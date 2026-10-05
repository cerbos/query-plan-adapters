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
          reject_two_variable_lambda(operator, operands[1])

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

          quantified =
            case operator
            when "exists" then ArelSupport.or_node(bodies)
            when "all" then ArelSupport.and_node(bodies)
            when "exists_one" then exactly_one_of(bodies)
            when "filter" then return Values::ConstantList.new(elements: values, keeps: bodies)
            when "map"
              projections = values.map { |value| evaluate(body_node, environment.bind(iterator, value)) }
              projections.each do |projection|
                reject_double_text("map", projection)
                reject_collection("map", projection)
                reject_deferred("map", projection)
              end
              return Values::ConstantProjection.new(projections: projections)
            else
              raise UnsupportedOperatorError, "Unsupported collection macro: #{operator}"
            end

          # A list built from attributes, `[R.attr.a, R.attr.b]`, errors as a whole when an
          # element is missing, before the macro sees any element: an OR of the bodies would
          # let a true body for the other element grant the row.
          missing = values.select { |value| ArelSupport.arel_node?(value) && null_convention(value) != :explicit }
          unknown_if_any(missing.map { |value| ArelSupport.is_null(value) }, quantified)
        end

        # CEL's two-variable comprehensions bind an element's position (over a list) or a key
        # (over a map) beside its value. A relation's rows have no position, and a row's
        # columns are not a map whose keys SQL can enumerate, so neither binding has a
        # translation.
        def reject_two_variable_lambda(operator, node)
          return unless node.is_a?(Plan::Expression) && node.operator == "lambda" && node.operands.length == 3

          raise UnsupportedOperatorError,
            "#{operator} with two variables binds each element's list position or map key: a " \
            "relation's rows have no position, and SQL cannot enumerate a row's columns as map " \
            "keys, so only one-variable comprehensions are translated"
        end

        # `left.except(right)`: the elements of `left` that no element of `right` equals, by CEL
        # equality, duplicates kept (Cerbos's `exceptList`). An error in either list, such as a
        # missing attribute inside `right`, is an error of the whole call.
        def except(left, right)
          unless right.is_a?(Array) && right.none? { |element| collection?(element) }
            raise UnsupportedOperatorError,
              "except is translated only with a list literal on its right, got #{describe(right)}"
          end

          case left
          when Values::Collection then except_from_relation(left.scope, right)
          when Array then except_from_constants(left, right)
          else
            raise UnsupportedOperatorError, "except needs a list on its left, got #{describe(left)}"
          end
        end

        # A relation's members that equal no constant on the right, as the body of a filtered
        # relation that `size()` counts. A member is a stored scalar list element, which holds null
        # values: `null` equals only a null constant, and a constant of another kind (or a list or
        # map) equals no member at all.
        def except_from_relation(scope, right)
          kind = member_kind(scope)
          unless kind && right.all? { |element| deep_constant?(element) }
            raise UnsupportedOperatorError,
              "except over a relation is translated only for a member column of a known type " \
              "against a list of constants"
          end

          member = scope.member_column
          candidates = right.reject { |element| composite?(element) || cross_type_literal?(element, kind) }
          present = candidates.compact
          removes_null = candidates.include?(nil)
          body =
            if present.empty?
              removes_null ? ArelSupport.comparison("ne", member, nil) : true
            else
              outside = ArelSupport.not_node(
                Arel::Nodes::In.new(member, present.map { |value| ArelSupport.quote(value) })
              )
              if removes_null
                ArelSupport.and_node([ArelSupport.comparison("ne", member, nil), outside])
              else
                ArelSupport.or_node([ArelSupport.comparison("eq", member, nil), outside])
              end
            end
          Values::FilteredCollection.new(scope: scope, body: body)
        end

        # Constants on the left, and on the right constants or columns. A column that is not
        # `:explicit`, or a computed node, is an error when NULL, which makes the list, and so the
        # call, an error: every keep is UNKNOWN there. Otherwise an element stays unless it equals
        # some right element, a null only equalling an explicit null.
        def except_from_constants(left, right)
          unless left.all? { |element| deep_constant?(element) }
            raise UnsupportedOperatorError, "except is translated only over a list of constants"
          end

          missing = right.filter_map { |element|
            ArelSupport.is_null(element) if ArelSupport.arel_node?(element) && null_convention(element) != :explicit
          }
          keeps = left.map do |element|
            equal = ArelSupport.or_node(right.map { |other| as_predicate(member_equality(element, other)) })
            unknown_if_any(missing, ArelSupport.not_node(equal))
          end
          Values::ConstantList.new(elements: left, keeps: keeps)
        end

        # How many keeps are TRUE, or UNKNOWN when one is: `filter()` never ignores an element's
        # error, and an error in `except()` is an error of the whole list.
        def count_constant_list(keeps)
          return keeps.count(true) if keeps.all? { |keep| keep == true || keep == false }

          total = keeps
            .map { |keep| ArelSupport.case_node([[keep, 1]], else_value: 0) }
            .reduce { |left, right| ArelSupport.infix("+", left, right) }
          ArelSupport.case_node(
            [[ArelSupport.or_node(keeps.map { |keep| ArelSupport.is_null(keep) }), nil]],
            else_value: total
          )
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

        # CEL's size() is an int, so the count can take `%`.
        def size(target)
          count = count_of(target)
          ArelSupport.arel_node?(count) ? record_cel_type(count, :int) : count
        end

        def count_of(target)
          case target
          when Values::ConstantList
            count_constant_list(target.keeps)
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
          when Values::SetOperation
            set_operation_size(target)
          when ::String
            target.length
          else
            return cel_type_error if known_non_string?(target)

            require_string_operand("size", target)
            dialect.char_length(target)
          end
        end
      end
    end
  end
end
