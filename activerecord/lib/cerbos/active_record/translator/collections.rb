# frozen_string_literal: true

module Cerbos
  module ActiveRecord
    class Translator
      # The collection macros (`exists`, `all`, `exists_one`, `filter`, `map`) and `size()`.
      #
      # @private
      module Collections
        private

        def macro(operator, operands, environment)
          unless operands.length == 2
            raise InvalidPlanError, "#{operator} takes a collection and a lambda"
          end

          collection = evaluate(operands[0], environment)
          if collection.is_a?(Array)
            return value_list_macro(operator, collection, operands[1], environment)
          end

          scope = require_collection(operator, collection)
          body_node, iterator = lambda_parts(operands[1])
          inner = environment.bind(iterator, scope)

          case operator
          when "map"
            Values::MappedCollection.new(scope: scope, projection: evaluate(body_node, inner))
          when "filter"
            Values::FilteredCollection.new(scope: scope, body: predicate(body_node, inner))
          else
            quantifier(operator, scope, predicate(body_node, inner))
          end
        end

        # Each quantifier treats an element that errors differently:
        #
        # * `exists` ignores errors if any element is true;
        # * `all` ignores errors if any element is false;
        # * `exists_one` never ignores them, since it counts every element.
        def quantifier(operator, scope, body)
          error_witness = scope.exists(ArelSupport.is_null(body))

          quantified =
            case operator
            when "exists"
              ArelSupport.case_node(
                [[scope.exists(body), true], [error_witness, nil]], else_value: false
              )
            when "all"
              ArelSupport.case_node(
                [[scope.exists(ArelSupport.not_node(body)), false], [error_witness, nil]],
                else_value: true
              )
            when "exists_one"
              ArelSupport.case_node(
                [[error_witness, nil]],
                else_value: ArelSupport.comparison("eq", scope.count(body), 1)
              )
            else
              raise UnsupportedOperatorError, "Unsupported collection macro: #{operator}"
            end

          # Require the parent hops, or `all` over a missing parent is TRUE and returns a
          # denied row. See {Relations::Scope#guarded}.
          scope.guarded(quantified)
        end

        # A macro over a constant list, such as a principal attribute the planner inlined.
        # Expands the body per element and joins with OR (`exists`) or AND (`all`). SQL's
        # three-valued OR/AND already match how CEL's quantifiers treat errors.
        def value_list_macro(operator, values, lambda_node, environment)
          body_node, iterator = lambda_parts(lambda_node)
          bodies = values.map { |value| predicate(body_node, environment.bind(iterator, value)) }

          case operator
          when "exists" then ArelSupport.or_node(bodies)
          when "all" then ArelSupport.and_node(bodies)
          when "exists_one" then exactly_one_of(bodies)
          else
            # `filter`/`map` here would need a list-valued `size`/`hasIntersection`. No corpus
            # shape needs it, so refuse.
            raise UnsupportedOperatorError,
              "#{operator} over a list of constants is not supported: only exists, all and " \
              "exists_one have a translation for that shape"
          end
        end

        # UNKNOWN if any element errors, else true when exactly one element is true.
        def exactly_one_of(bodies)
          matches = bodies
            .map { |body| ArelSupport.case_node([[body, 1]], else_value: 0) }
            .reduce { |left, right| ArelSupport.infix("+", left, right) }

          ArelSupport.case_node(
            [[ArelSupport.or_node(bodies.map { |body| ArelSupport.is_null(body) }), nil]],
            else_value: ArelSupport.comparison("eq", matches, 1)
          )
        end

        def lambda_parts(node)
          unless node.is_a?(Plan::Expression) && node.operator == "lambda" && node.operands.length == 2
            raise InvalidPlanError, "Expected a lambda operand, got #{node.inspect}"
          end

          body, iterator = node.operands
          unless iterator.is_a?(Plan::Variable)
            raise InvalidPlanError, "Lambda iterator must be a variable, got #{iterator.inspect}"
          end

          [body, iterator.name]
        end

        def require_collection(operator, value)
          return value.scope if value.is_a?(Values::Collection)

          raise UnmappedAttributeError,
            "#{operator} needs a collection, but its operand resolved to #{describe(value)}; " \
            "map that attribute with Cerbos::ActiveRecord.relation"
        end

        def size(target)
          case target
          when Values::Collection
            # Counting never errors, so NULL members count too. The hop guard is still needed:
            # a missing parent counts 0, and `== 0` would return a denied row (#309, #316).
            target.scope.guarded(target.scope.count)
          when Values::FilteredCollection
            # Unlike exists(), filter() never ignores an error: one UNKNOWN body makes the
            # count UNKNOWN.
            target.scope.guarded(
              ArelSupport.case_node(
                [[target.scope.exists(ArelSupport.is_null(target.body)), nil]],
                else_value: target.scope.count(target.body)
              )
            )
          when ::String
            target.length
          else
            require_string_operand("size", target)
            dialect.char_length(target)
          end
        end
      end
    end
  end
end
