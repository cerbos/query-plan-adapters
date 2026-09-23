# frozen_string_literal: true

module Cerbos
  module ActiveRecord
    class Translator
      # `in` and `hasIntersection` against a constant list, a list holding a column, or a
      # mapped relation.
      #
      # @private
      module Membership
        private

        def membership(needle, haystack)
          if needle.is_a?(Array) || needle.is_a?(Hash) ||
              (haystack.is_a?(Array) && haystack.any? { |member| member.is_a?(Array) || member.is_a?(Hash) })
            raise UnsupportedOperatorError,
              "in requires scalar elements; SQL scalar membership cannot compare a list or map element"
          end
          return relation_membership(haystack.scope, needle) if haystack.is_a?(Values::Collection)
          return relation_membership(needle.scope, haystack) if needle.is_a?(Values::Collection)

          scalar_membership(needle, haystack)
        end

        # `value in R.attr.<relation>`, as an EXISTS over the related rows.
        def relation_membership(scope, value)
          member = scope.member_column
          condition =
            if explicit_null?(value)
              null_equality(member, value)
            elsif cross_type_literal?(value, member_kind(scope))
              # `"2" in [2]` is false in CEL. SQLite would coerce '2' to 2 (and true to 1).
              false
            else
              ArelSupport.comparison("eq", member, value)
            end

          # Without the guard, `!("x" in chain)` over a missing parent is TRUE and returns a
          # denied row (#315). The guard makes it NULL.
          result = scope.guarded(scope.exists(condition))
          if ArelSupport.arel_node?(value) && !explicit_null?(value)
            return unknown_if_any([ArelSupport.is_null(value)], result)
          end
          result
        end

        def scalar_membership(needle, values)
          members = values.is_a?(Array) ? values : [values]
          return false if members.empty?

          # Common case: a column against constants, as an IN clause.
          if ArelSupport.arel_node?(needle) && members.none? { |member| ArelSupport.arel_node?(member) }
            # Drop constants of another type: `aNumber in ["5"]` is false in CEL, but SQLite
            # reads '5' as 5 inside IN.
            kind = scalar_kind(needle)
            members = members.reject { |member| cross_type_literal?(member, kind) }
            if members.empty?
              return false if explicit_null?(needle)

              # A missing attribute is still an error, so `!(x in ["5"])` must not grant it.
              return unknown_if_any([ArelSupport.is_null(needle)], false)
            end

            present = members.compact

            predicates = []
            unless present.empty?
              predicates << Arel::Nodes::In.new(
                ArelSupport.quote(needle), present.map { |value| ArelSupport.quote(value) }
              )
            end
            # A null element matches a null attribute (a null value, not a missing one).
            predicates << ArelSupport.comparison("eq", needle, nil) if present.length != members.length

            return ArelSupport.or_node(predicates)
          end

          # A column in the list, or a constant needle: one comparison per element.
          # E.g. `null in [R.attr.x]` is true when the column is null.
          ArelSupport.or_node(members.map { |member| member_equality(needle, member) })
        end

        # CEL equality for one element. Two nulls are equal in CEL but UNKNOWN in SQL, so spell
        # that case out.
        def member_equality(needle, member)
          needle_is_node = ArelSupport.arel_node?(needle)
          member_is_node = ArelSupport.arel_node?(member)

          return ArelSupport.comparison("eq", member, nil) if needle.nil? && member_is_node
          return ArelSupport.comparison("eq", needle, nil) if member.nil? && needle_is_node

          return null_equality(needle, member) if needle_is_node && member_is_node

          return needle.nil? == member.nil? if needle.nil? || member.nil?

          ArelSupport.to_predicate(compare("eq", needle, member))
        end

        # The CEL kind of a `member_field` relation's values, from its column type. Nil if the
        # type is not classified.
        def member_kind(scope)
          kind_of_column_type(scope.model.columns_hash[scope.mapping.member_field.to_s]&.type)
        end

        # A constant whose CEL kind differs from the elements' can never equal one of them.
        def cross_type_literal?(value, kind)
          return false if kind.nil? || !constant?(value)

          literal_kind = scalar_kind(value)
          !literal_kind.nil? && literal_kind != kind
        end

        def has_intersection(left, right)
          # hasIntersection is symmetric and the planner keeps source order, so the literal list
          # can be on either side.
          left, right = right, left if left.is_a?(Array) && !right.is_a?(Array)
          values = right.is_a?(Array) ? right : [right]

          case left
          when Values::Collection
            # Guarded as in membership (#315). Literals of another type never intersect.
            kind = member_kind(left.scope)
            values = values.reject { |value| cross_type_literal?(value, kind) }
            left.scope.guarded(
              left.scope.exists(scalar_membership(left.scope.member_column, values))
            )
          when Values::MappedCollection
            # map() never ignores an element's error, so check for errors before matches.
            left.scope.guarded(
              ArelSupport.case_node(
                [
                  [left.scope.exists(ArelSupport.is_null(left.projection)), nil],
                  [left.scope.exists(scalar_membership(left.projection, values)), true]
                ],
                else_value: false
              )
            )
          else
            raise UnmappedAttributeError,
              "hasIntersection needs a collection, got #{describe(left)}"
          end
        end
      end
    end
  end
end
