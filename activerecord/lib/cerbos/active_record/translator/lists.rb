# frozen_string_literal: true

module Cerbos
  module ActiveRecord
    class Translator
      # List values built in the plan: `+` over lists, `intersect`, `except`, `isSubset`, and
      # `index` into a map literal or by a constant key.
      #
      # A relation has no element order, so each list value is held until an operator gives it
      # a meaning that needs none (membership, a count, emptiness); every other use refuses.
      #
      # @private
      module Lists
        private

        # True for an operand `+` concatenates as a list rather than adds as a number.
        def list_operand?(value)
          case value
          when Array, Values::Collection, Values::ConcatenatedList then true
          when Values::ConditionalValue then list_operand?(value.then_value) || list_operand?(value.else_value)
          else false
          end
        end

        # `left + right` over lists. Two constant lists concatenate here; a ternary's arms each
        # take the other operand; a relation part is held as a {Values::ConcatenatedList}.
        def concatenate(left, right)
          if left.is_a?(Values::ConditionalValue)
            return Values::ConditionalValue.new(
              condition: left.condition,
              then_value: concatenate(left.then_value, right),
              else_value: concatenate(left.else_value, right)
            )
          end
          if right.is_a?(Values::ConditionalValue)
            return Values::ConditionalValue.new(
              condition: right.condition,
              then_value: concatenate(left, right.then_value),
              else_value: concatenate(left, right.else_value)
            )
          end
          return left + right if left.is_a?(Array) && right.is_a?(Array)

          parts = [left, right].flat_map { |part|
            part.is_a?(Values::ConcatenatedList) ? part.parts : [part]
          }
          unless parts.all? { |part| part.is_a?(Array) || part.is_a?(Values::Collection) }
            raise UnsupportedOperatorError,
              "+ of a list and #{describe(parts.find { |part| !part.is_a?(Array) && !part.is_a?(Values::Collection) })}: " \
              "only lists of constants and relations concatenate"
          end

          Values::ConcatenatedList.new(parts: parts)
        end

        # `intersect(relation, list)` and `except(relation, list)`. Two constant lists are
        # computed here, as CEL computes them.
        def set_operation(kind, left, right)
          if left.is_a?(Array) && right.is_a?(Array)
            require_scalar_constants(kind, left + right)
            return constant_set_operation(kind, left, right)
          end

          unless left.is_a?(Values::Collection) && left.scope.mapping&.member_field && right.is_a?(Array)
            raise UnsupportedOperatorError,
              "#{kind} is translated only over a relation of scalar members (a member_field " \
              "relation) and a list of constants, got #{describe(left)} and #{describe(right)}"
          end
          require_scalar_constants(kind, right)

          Values::SetOperation.new(kind: kind, scope: left.scope, values: right)
        end

        # Cerbos's `intersect` keeps the matching elements of the shorter list, so its
        # duplicates; `except` keeps the elements of the left list absent from the right.
        def constant_set_operation(kind, left, right)
          if kind == "except"
            left.reject { |element| right.include?(element) }
          else
            shorter, longer = (left.length > right.length) ? [right, left] : [left, right]
            shorter.select { |element| longer.include?(element) }
          end
        end

        # `isSubset(relation, list)`: every member of the relation is in the list. A relation
        # with no members is a subset of any list.
        def is_subset(left, right)
          if left.is_a?(Array) && right.is_a?(Array)
            require_scalar_constants("isSubset", left + right)
            return left.all? { |element| right.include?(element) }
          end

          unless left.is_a?(Values::Collection) && left.scope.mapping&.member_field && right.is_a?(Array)
            raise UnsupportedOperatorError,
              "isSubset is translated only over a relation of scalar members (a member_field " \
              "relation) and a list of constants, got #{describe(left)} and #{describe(right)}"
          end
          require_scalar_constants("isSubset", right)

          scope = left.scope
          scope.guarded(
            ArelSupport.not_node(scope.exists(ArelSupport.not_node(member_in_constants(scope, right))))
          )
        end

        # `intersect(...) == []` or `except(...) == []`, and their `!=`. Emptiness needs no
        # element order: `intersect` is empty when no member is in the list (whichever list
        # Cerbos walks), and `except` when every member is.
        def compare_set_operation(operator, left, right)
          operation, other = left.is_a?(Values::SetOperation) ? [left, right] : [right, left]
          unless %w[eq ne].include?(operator) && other == []
            raise UnsupportedOperatorError,
              "#{operator} of #{describe(operation)} against #{describe(other)}: only == and " \
              "!= against an empty list are translated, since a relation gives its members " \
              "no order to compare a non-empty list with"
          end

          scope = operation.scope
          contained = member_in_constants(scope, operation.values)
          witness = (operation.kind == "intersect") ? contained : ArelSupport.not_node(contained)
          empty = ArelSupport.not_node(scope.exists(witness))
          scope.guarded((operator == "eq") ? empty : ArelSupport.not_node(empty))
        end

        # `size()` of an intersect or except result.
        def set_operation_size(operation)
          scope = operation.scope
          values = operation.values
          contained = member_in_constants(scope, values)
          return scope.guarded(scope.count(ArelSupport.not_node(contained))) if operation.kind == "except"
          return 0 if values.empty?

          # Cerbos walks the shorter list and keeps each element the longer one contains, so
          # the duplicates come from whichever list is shorter on this row.
          from_list = values
            .map { |value| ArelSupport.case_node([[scope.exists(member_in_constants(scope, [value])), 1]], else_value: 0) }
            .reduce { |sum, term| ArelSupport.infix("+", sum, term) }
          scope.guarded(
            ArelSupport.case_node(
              [[ArelSupport.comparison("gt", scope.count, values.length), from_list]],
              else_value: scope.count(contained)
            )
          )
        end

        # A definite (never UNKNOWN) test that a relation's member equals one of the constants,
        # as CEL's list `contains` decides it: a null member matches only a null constant, and a
        # constant of another type matches nothing.
        def member_in_constants(scope, values)
          member = scope.member_column
          kind = member_kind(scope)
          present = values.compact.reject { |value| cross_type_literal?(value, kind) }

          tests = []
          unless present.empty?
            tests << ArelSupport.and_node([
              ArelSupport.comparison("ne", member, nil),
              Arel::Nodes::In.new(member, present.map { |value| ArelSupport.quote(value) })
            ])
          end
          tests << ArelSupport.comparison("eq", member, nil) if values.include?(nil)
          ArelSupport.or_node(tests)
        end

        def require_scalar_constants(operator, values)
          return if values.all? { |value| value.nil? || (constant?(value) && !value.is_a?(Array) && !value.is_a?(Hash)) }

          raise UnsupportedOperatorError,
            "#{operator} needs a list of scalar constants: a column, list or map element has no " \
            "set semantics in SQL"
        end

        # `index(container, key)`. Two shapes have a translation:
        #
        # * a map literal indexed by a string attribute, as a CASE with no ELSE, so a missing
        #   key is NULL, like CEL's no-such-key error;
        # * a constant string key into an attribute, which reads the same field as
        #   `container.key` and so takes that attribute's mapping.
        #
        # Every other index (list positions above all) keeps the generic refusal.
        def index_access(operands, environment)
          unless operands.length == 2
            raise InvalidPlanError, "index takes a container and a key, got #{operands.length} operands"
          end

          container, key = operands
          # Both shapes come first, even under an override of `index`: the generic walk below
          # would refuse each before an override saw it (`struct` and `set-field` have no
          # translation, and the container is unmapped).
          return map_literal_lookup(map_literal(container), evaluate(key, environment)) if map_literal?(container)

          if container.is_a?(Plan::Variable) && key.is_a?(Plan::Value) && key.value.is_a?(::String) &&
              attributes[container.name].nil? && attributes["#{container.name}.#{key.value}"].is_a?(AttributeMapping::Field)
            return environment.resolve("#{container.name}.#{key.value}")
          end

          apply("index", operands.map { |operand| evaluate(operand, environment) })
        end

        def map_literal?(node)
          node.is_a?(Plan::Expression) && node.operator == "struct" &&
            node.operands.all? { |entry|
              entry.is_a?(Plan::Expression) && entry.operator == "set-field" && entry.operands.length == 2 &&
                entry.operands.all?(Plan::Value)
            }
        end

        def map_literal(node)
          node.operands.to_h { |entry| entry.operands.map(&:value) }
        end

        def map_literal_lookup(map, key)
          unless ArelSupport.arel_node?(key) && scalar_kind(key) == :string && map.keys.all?(::String)
            raise UnsupportedOperatorError,
              "index into a map literal is translated only with string keys and a string " \
              "attribute as the key, got #{describe(key)}"
          end

          values = map.values
          value_kind =
            if values.all?(::String) then :string
            elsif values.all? { |value| value == true || value == false } then :bool
            end
          if value_kind.nil? || values.empty?
            raise UnsupportedOperatorError,
              "index into a map literal is translated only when every value is a string, or " \
              "every value a boolean: the CASE it becomes has one SQL type"
          end

          lookup = ArelSupport.case_node(map.map { |name, value| [ArelSupport.comparison("eq", key, name), value] })
          record_cel_type(lookup, value_kind)
        end
      end
    end
  end
end
