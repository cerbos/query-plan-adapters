# frozen_string_literal: true

module Cerbos
  module ActiveRecord
    class Translator
      # +in+ and +hasIntersection+: a value against a list of constants, a list that holds a
      # column, or a mapped relation.
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

        # +value in R.attr.<relation>+. If the relation of a row is empty, the row stays out of
        # the result. This agrees with the CEL deny for a missing attribute.
        def relation_membership(scope, value)
          member = scope.member_column
          condition =
            if explicit_null?(value)
              null_equality(member, value)
            else
              ArelSupport.comparison("eq", member, value)
            end

          # A bare EXISTS has two values, so `!("x" in chain)` over an absent parent is TRUE and
          # gives back a row that the PDP denies (#315). The guard makes it NULL instead.
          result = scope.guarded(scope.exists(condition))
          if ArelSupport.arel_node?(value) && !explicit_null?(value)
            return unknown_if_any([ArelSupport.is_null(value)], result)
          end
          result
        end

        def scalar_membership(needle, values)
          members = values.is_a?(Array) ? values : [values]
          return false if members.empty?

          # The usual shape: a column against a list of constants. An IN clause reads better than
          # a chain of equality tests.
          if ArelSupport.arel_node?(needle) && members.none? { |member| ArelSupport.arel_node?(member) }
            present = members.compact

            predicates = []
            unless present.empty?
              predicates << Arel::Nodes::In.new(
                ArelSupport.quote(needle), present.map { |value| ArelSupport.quote(value) }
              )
            end
            # A null element makes the membership test true for an attribute that is null. The
            # attribute must be null and not only missing.
            predicates << ArelSupport.comparison("eq", needle, nil) if present.length != members.length

            return ArelSupport.or_node(predicates)
          end

          # A list that holds a column, or a needle that is a constant, needs one comparison for
          # each element. `null in [R.attr.x]` is the example: it is true when the column is null.
          ArelSupport.or_node(members.map { |member| member_equality(needle, member) })
        end

        # CEL equality for one element of a membership test. Two nulls are equal in CEL, but the
        # result of that comparison in SQL is UNKNOWN, so the adapter writes it out.
        def member_equality(needle, member)
          needle_is_node = ArelSupport.arel_node?(needle)
          member_is_node = ArelSupport.arel_node?(member)

          return ArelSupport.comparison("eq", member, nil) if needle.nil? && member_is_node
          return ArelSupport.comparison("eq", needle, nil) if member.nil? && needle_is_node

          return null_equality(needle, member) if needle_is_node && member_is_node

          return needle.nil? == member.nil? if needle.nil? || member.nil?

          ArelSupport.to_predicate(compare("eq", needle, member))
        end

        def has_intersection(left, right)
          # hasIntersection gives the same result if the operands change sides. The planner
          # keeps the order of the source. Thus the list of literals can come on each side.
          left, right = right, left if left.is_a?(Array) && !right.is_a?(Array)
          values = right.is_a?(Array) ? right : [right]

          case left
          when Values::Collection
            # As with membership: a bare EXISTS is FALSE for an absent parent, so
            # `!hasIntersection(chain, [...])` would be TRUE for it (#315).
            left.scope.guarded(
              left.scope.exists(scalar_membership(left.scope.member_column, values))
            )
          when Values::MappedCollection
            # map() makes an error for each element that makes an error, and it ignores no
            # errors. Thus the guard for the error must come before the test for a true
            # element.
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
