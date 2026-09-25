# frozen_string_literal: true

module Cerbos
  module Sequel
    class Translator
      # +in+ and +hasIntersection+: a value against a list of constants, a list that holds a
      # column, or a mapped relation.
      module Membership
        private

        def membership(needle, haystack)
          return composite_membership(needle, haystack) if composite?(needle)
          return relation_membership(haystack.scope, needle) if haystack.is_a?(Values::Collection)
          return relation_membership(needle.scope, haystack) if needle.is_a?(Values::Collection)

          scalar_membership(needle, haystack)
        end

        # A list or map literal.
        def composite?(value)
          value.is_a?(Array) || value.is_a?(Hash)
        end

        # A list or map needle. The members of a mapped association are scalars, which a list or
        # map never equals, so the answer is FALSE (the chain guard keeps an absent parent
        # UNKNOWN). Against a list of constants it is folded with CEL equality.
        def composite_membership(needle, haystack)
          unless deep_constant?(needle)
            raise UnsupportedOperatorError, "in with a list or map needle holding a column is not translated"
          end
          return haystack.scope.guarded(haystack.scope.exists(false)) if haystack.is_a?(Values::Collection)
          if haystack.is_a?(Array) && deep_constant?(haystack)
            return haystack.any? { |member| member == needle }
          end

          raise UnsupportedOperatorError,
            "in with a list or map needle is translated only against an association or a list of constants"
        end

        # +value in R.attr.<relation>+. If the relation of a row is empty, the row stays out of
        # the result. This agrees with the CEL deny for a missing attribute.
        def relation_membership(scope, value)
          member = scope.member_column
          condition =
            if explicit_null?(value)
              null_equality(member, value)
            elsif cross_type_literal?(value, member_kind(scope))
              # `"2" in [2]` is false in CEL, whose equality is heterogeneous. SQL would coerce
              # one side onto the other's type: SQLite's REAL affinity reads the literal '2' as
              # the number 2, MySQL reads 'true' as 0, and PostgreSQL rejects
              # `double precision = text` outright (#505).
              false
            else
              SqlSupport.comparison("eq", member, value)
            end

          # A bare EXISTS has two values, so `!("x" in chain)` over an absent parent is TRUE and
          # gives back a row that the PDP denies (#315). The guard makes it NULL instead.
          result = scope.guarded(scope.exists(condition))
          if SqlSupport.sql_node?(value) && !explicit_null?(value)
            return unknown_if_any([SqlSupport.is_null(value)], result)
          end
          result
        end

        def scalar_membership(needle, values)
          members = values.is_a?(Array) ? values : [values]
          return false if members.empty?

          # A list or map element never equals a scalar column, so it cannot match. A constant
          # needle is folded against it like any other element, below.
          if SqlSupport.sql_node?(needle) && members.any? { |member| composite?(member) }
            members = members.reject { |member| composite?(member) }
            if members.empty?
              return false if explicit_null?(needle)

              # A missing attribute is still an error.
              return unknown_if_any([SqlSupport.is_null(needle)], false)
            end
          end

          # The usual shape: a column against a list of constants. An IN clause reads better than
          # a chain of equality tests.
          if SqlSupport.sql_node?(needle) && members.none? { |member| SqlSupport.sql_node?(member) }
            # `R.attr.aNumber in ["5", 2]` is false for the string in CEL, whose equality is
            # heterogeneous. Inside IN, SQLite's NUMERIC affinity reads '5' as the number 5, so
            # the adapter drops each constant the column's kind can never equal, as it does for
            # the member column of an association (#505).
            kind = scalar_kind(needle)
            members = members.reject { |member| cross_type_literal?(member, kind) }
            if members.empty?
              return false if explicit_null?(needle)

              # A missing attribute is still an error, so `!(x in ["5"])` must not grant it.
              return unknown_if_any([SqlSupport.is_null(needle)], false)
            end

            present = members.compact

            predicates = []
            unless present.empty?
              predicates << SqlSupport.in_list(needle, present)
            end
            # A null element makes the membership test true for an attribute that is null. The
            # attribute must be null and not only missing.
            predicates << SqlSupport.comparison("eq", needle, nil) if present.length != members.length

            return SqlSupport.or_node(predicates)
          end

          # A list that holds a column, or a needle that is a constant, needs one comparison for
          # each element. `null in [R.attr.x]` is the example: it is true when the column is null.
          SqlSupport.or_node(members.map { |member| member_equality(needle, member) })
        end

        # CEL equality for one element of a membership test. Two nulls are equal in CEL, but the
        # result of that comparison in SQL is UNKNOWN, so the adapter writes it out.
        def member_equality(needle, member)
          needle_is_node = SqlSupport.sql_node?(needle)
          member_is_node = SqlSupport.sql_node?(member)

          return SqlSupport.comparison("eq", member, nil) if needle.nil? && member_is_node
          return SqlSupport.comparison("eq", needle, nil) if member.nil? && needle_is_node

          return null_equality(needle, member) if needle_is_node && member_is_node

          return needle.nil? == member.nil? if needle.nil? || member.nil?

          SqlSupport.to_predicate(compare("eq", needle, member))
        end

        # The CEL kind of the bare values in an association mapped by +member_field+, read from
        # the column that holds them; nil when the column's type is not one the adapter
        # classifies.
        def member_kind(scope)
          field = scope.mapping&.member_field
          return nil if field.nil? || scope.model.nil?

          kind_of_column_type(scope.model.db_schema.dig(field.to_sym, :type))
        end

        # A constant whose CEL kind differs from the elements' can never equal one of them.
        def cross_type_literal?(value, kind)
          return false if kind.nil? || !constant?(value)

          literal_kind = scalar_kind(value)
          !literal_kind.nil? && literal_kind != kind
        end

        def has_intersection(left, right)
          # hasIntersection gives the same result if the operands change sides. The planner
          # keeps the order of the source. Thus the list of literals can come on each side.
          left, right = right, left if left.is_a?(Array) && !right.is_a?(Array)
          values = right.is_a?(Array) ? right : [right]

          case left
          when Values::Collection
            # As with membership: a bare EXISTS is FALSE for an absent parent, so
            # `!hasIntersection(chain, [...])` would be TRUE for it (#315). Literals of another
            # type never intersect.
            kind = member_kind(left.scope)
            values = values.reject { |value| cross_type_literal?(value, kind) }
            left.scope.guarded(
              left.scope.exists(scalar_membership(left.scope.member_column, values))
            )
          when Values::MappedCollection
            # map() makes an error for each element that makes an error, and it ignores no
            # errors. Thus the guard for the error must come before the test for a true
            # element.
            left.scope.guarded(
              SqlSupport.case_node(
                [
                  [left.scope.exists(SqlSupport.is_null(left.projection)), nil],
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
