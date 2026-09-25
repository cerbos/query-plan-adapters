# frozen_string_literal: true

module Cerbos
  module Sequel
    class Translator
      # The collection macros (+exists+, +all+, +exists_one+, +filter+, +map+) and +size()+.
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
            projection = evaluate(body_node, inner)
            reject_double_text("map", projection)
            Values::MappedCollection.new(scope: scope, projection: projection)
          when "filter"
            Values::FilteredCollection.new(scope: scope, body: predicate(body_node, inner))
          else
            quantifier(operator, scope, predicate(body_node, inner))
          end
        end

        # The three CEL quantifiers are different in one important way. Each one has different
        # behaviour for an element whose body made an error. Thus each one gets its own guard
        # for that error, and they do not share one guard:
        #
        # * +exists+ ignores the errors if one element gives true;
        # * +all+ ignores the errors if one element gives false;
        # * +exists_one+ never ignores them, because it must count all the elements.
        def quantifier(operator, scope, body)
          error_witness = scope.exists(SqlSupport.is_null(body))

          quantified =
            case operator
            when "exists"
              SqlSupport.case_node(
                [[scope.exists(body), true], [error_witness, nil]], else_value: false
              )
            when "all"
              SqlSupport.case_node(
                [[scope.exists(SqlSupport.not_node(body)), false], [error_witness, nil]],
                else_value: true
              )
            when "exists_one"
              SqlSupport.case_node(
                [[error_witness, nil]],
                else_value: SqlSupport.comparison("eq", scope.count(body), 1)
              )
            else
              raise UnsupportedOperatorError, "Unsupported collection macro: #{operator}"
            end

          # A chain must require its parent hops. Without that, `all` over an absent parent is
          # vacuously TRUE and gives back a row that the PDP denies. See {Relations::Scope#guarded}.
          scope.guarded(quantified)
        end

        # A macro over a list of constants. The planner sends the list itself when the collection
        # is a principal attribute, because it knows those values when it makes the plan.
        #
        # The elements are known here, so the translator evaluates the body one time for each
        # element and joins the results. SQL gives the correct answer without more work: OR and
        # AND obey the same three-valued logic as the CEL quantifiers. OR is TRUE if one element
        # is true, UNKNOWN if no element is true and one is unknown, and FALSE if all are false.
        # That is exactly `exists`. AND is the same for `all`.
        def value_list_macro(operator, values, lambda_node, environment)
          body_node, iterator = lambda_parts(lambda_node)
          bodies = values.map { |value| predicate(body_node, environment.bind(iterator, value)) }

          case operator
          when "exists" then SqlSupport.or_node(bodies)
          when "all" then SqlSupport.and_node(bodies)
          when "exists_one" then exactly_one_of(bodies)
          else
            # `filter` and `map` give a list, and the operator that uses it — `size` or
            # `hasIntersection` — would need a second list-valued form. No corpus shape needs it,
            # so the adapter refuses instead of keeping code that nothing proves.
            raise UnsupportedOperatorError,
              "#{operator} over a list of constants is not supported: only exists, all and " \
              "exists_one have a translation for that shape"
          end
        end

        # `exists_one` never ignores an element that made an error, so the guard for the error
        # comes first. After that it is an exact count of the elements that are true.
        def exactly_one_of(bodies)
          matches = bodies
            .map { |body| SqlSupport.case_node([[body, 1]], else_value: 0) }
            .reduce { |left, right| SqlSupport.infix("+", left, right) }

          SqlSupport.case_node(
            [[SqlSupport.or_node(bodies.map { |body| SqlSupport.is_null(body) }), nil]],
            else_value: SqlSupport.comparison("eq", matches, 1)
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
            "map that attribute with Cerbos::Sequel.association"
        end

        # CEL's size() is an int, so the count can take `%`.
        def size(target)
          count = count_of(target)
          SqlSupport.sql_node?(count) ? record_cel_type(count, :int) : count
        end

        def count_of(target)
          case target
          when Values::Collection
            # size() counts the elements and does not evaluate them. Thus it also counts a
            # member column that is NULL, and no element can make an error. The hop guard is
            # still necessary: over an absent parent the count is 0, and `== 0`, `>= 0` and
            # `!(> 0)` each give back a row that the PDP denies (#309, #316).
            target.scope.guarded(target.scope.count)
          when Values::FilteredCollection
            # filter() is different from exists(). It never ignores an element that made an
            # error. Thus one body with an UNKNOWN result makes the full count unknown.
            target.scope.guarded(
              SqlSupport.case_node(
                [[target.scope.exists(SqlSupport.is_null(target.body)), nil]],
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
