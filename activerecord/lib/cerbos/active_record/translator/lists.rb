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

        # `intersect(relation, list)`. Two constant lists are computed here, as CEL computes
        # them. (`except` is {Collections#except}.)
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
        # duplicates.
        def constant_set_operation(_kind, left, right)
          shorter, longer = (left.length > right.length) ? [right, left] : [left, right]
          shorter.select { |element| longer.include?(element) }
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

        # `intersect(...) == []` and its `!=`. Emptiness needs no element order: `intersect` is
        # empty when no member is in the list, whichever list Cerbos walks.
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
          empty = ArelSupport.not_node(scope.exists(contained))
          scope.guarded((operator == "eq") ? empty : ArelSupport.not_node(empty))
        end

        # `size()` of an intersect result.
        def set_operation_size(operation)
          scope = operation.scope
          values = operation.values
          contained = member_in_constants(scope, values)
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
          if map_literal?(container)
            key_value = evaluate(key, environment)
            return guard_positions([key_value], map_literal_lookup(evaluate(container, environment), key_value))
          end

          if container.is_a?(Plan::Variable) && key.is_a?(Plan::Value) && key.value.is_a?(::String) &&
              attributes[container.name].nil? && attributes["#{container.name}.#{key.value}"].is_a?(AttributeMapping::Field)
            return environment.resolve("#{container.name}.#{key.value}")
          end

          apply("index", operands.map { |operand| evaluate(operand, environment) })
        end

        # `list[position]` over a relation that declares `order_field`. The element is the
        # projection of the row at that position in the column's order, as a scalar subquery. A
        # position past the end is CEL's out-of-range error, so the element carries a guard
        # that {Translator#guard_positions} puts around every operator over it. A negative or
        # fractional position is that error on every row.
        def list_element(container, key)
          unless container.is_a?(Values::Collection)
            raise UnsupportedOperatorError,
              "index into #{describe(container)}: only a relation mapped with order_field, or " \
              "a map literal keyed by a string attribute, is translated. Supply an operator " \
              "override if the database can express it faithfully."
          end
          unless key.is_a?(Numeric)
            raise UnsupportedOperatorError,
              "index by #{describe(key)}: only a constant position into a relation is translated"
          end
          return cel_type_error unless key.finite? && key == key.truncate && key >= 0

          scope = container.scope
          position = key.to_i
          scope.order_column
          return Values::IndexedElement.new(scope: scope, position: position) unless scope.mapping.member_field

          element = scope.element_at(scope.member_column, position)
          register_column_type(element, scope.model, scope.mapping.member_field)
          register_null_representation(element, :explicit)
          @position_guards[element] = in_range(scope, position)
          element
        end

        # The guard of a position: the hops to the list exist, and the list is long enough.
        def in_range(scope, position, *conditions)
          scope.guarded(ArelSupport.comparison("gt", scope.count(*conditions), position))
        end

        # `list[position].field`: the member's column read from the element row. A missing
        # field on a struct element is CEL's no-such-field error, so the column keeps its
        # mapping's convention, as a macro body reads it.
        def field_access(operands, environment)
          unless operands.length == 2 && operands[1].is_a?(Plan::Variable)
            raise InvalidPlanError, "get-field takes a container and a field name, got #{operands.inspect}"
          end

          container = evaluate(operands[0], environment)
          unless container.is_a?(Values::IndexedElement)
            raise UnsupportedOperatorError,
              "get-field over #{describe(container)}: only a member of an element read by " \
              "position from a relation of structs is translated"
          end

          scope = container.scope
          name = operands[1].name
          mapping = scope.mapping.fields[name]
          unless mapping.is_a?(AttributeMapping::Field) && mapping.segments.length == 1
            raise UnsupportedOperatorError,
              "Relation #{scope.mapping.association.inspect} maps member field #{name.inspect} to " \
              "#{mapping.nil? ? "nothing" : "a relation or a path"}: an element read by " \
              "position gives up only a column of its own row"
          end

          element = scope.element_at(scope.table[mapping.path], container.position)
          register_column_type(element, scope.model, mapping.path)
          register_null_representation(element, mapping.null_representation)
          @position_guards[element] = in_range(scope, container.position)
          element
        end

        # `relation == [constants]` and its `!=` over an ordered relation: the same length, and
        # the element at each position equal to the literal's, as CEL compares lists. A relation
        # of scalar members holds a null as a value; a `map()` projection or a `filter()` body
        # that is NULL is an element's error, which errors the whole list.
        def ordered_list_equality(operator, value, literal)
          scope, projection, body = ordered_list_parts(value)
          unless literal.all? { |element| deep_constant?(element) }
            raise UnsupportedOperatorError,
              "#{operator} of #{describe(value)} against a list holding a column is not translated"
          end

          errors = []
          errors << scope.exists(ArelSupport.is_null(body)) unless body.nil?
          null_is_value = value.is_a?(Values::MappedCollection) ? null_convention(projection) == :explicit : true
          errors << scope.exists(ArelSupport.and_node([body, ArelSupport.is_null(projection)].compact)) unless null_is_value
          kind = value.is_a?(Values::MappedCollection) ? scalar_kind(projection) : member_kind(scope)

          matches = literal.each_with_index.map { |expected, position|
            element = scope.element_at(projection, position, body)
            if expected.nil?
              null_is_value ? ArelSupport.comparison("eq", element, nil) : false
            elsif composite?(expected) || cross_type_literal?(expected, kind)
              false
            elsif kind.nil?
              raise UnsupportedOperatorError,
                "#{operator} of #{describe(value)} against #{expected.inspect}: the elements' " \
                "type is not known, so the store could compare them across types"
            else
              ArelSupport.and_node([ArelSupport.comparison("ne", element, nil), ArelSupport.comparison("eq", element, expected)])
            end
          }
          equal = ArelSupport.and_node([ArelSupport.comparison("eq", scope.count(body), literal.length), *matches])
          result = (operator == "ne") ? ArelSupport.not_node(equal) : equal
          scope.guarded(unknown_if_any(errors, result))
        end

        def ordered_list_parts(value)
          case value
          when Values::Collection then [value.scope, value.scope.member_column, nil]
          when Values::FilteredCollection then [value.scope, value.scope.member_column, value.body]
          when Values::MappedCollection
            unless ArelSupport.arel_node?(value.projection)
              raise UnsupportedOperatorError,
                "== over a map() whose projection is #{describe(value.projection)}: only a " \
                "projection to a scalar value is translated"
            end
            [value.scope, value.projection, nil]
          end
        end

        # True for a list value whose order an `order_field` gives.
        def ordered_list?(value)
          case value
          when Values::Collection, Values::FilteredCollection, Values::MappedCollection then value.scope.ordered?
          else false
          end
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
