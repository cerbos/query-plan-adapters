# frozen_string_literal: true

module Cerbos
  module Sequel
    class Translator
      # +in+ and +hasIntersection+: a value against a list of constants, a list that holds a
      # column, or a mapped relation.
      module Membership
        private

        def membership(needle, haystack)
          # `x in map` tests the map's keys in CEL. The planner folds a literal map to `==`, but a
          # map can still arrive as a value; answering FALSE would grant its negation.
          haystack = haystack.keys if haystack.is_a?(Hash)
          return composite_membership(needle, haystack) if composite?(needle)
          return relation_membership(haystack.scope, needle) if haystack.is_a?(Values::Collection)
          return projection_membership(needle, haystack.projections) if haystack.is_a?(Values::ConstantProjection)
          return relation_membership(needle.scope, haystack) if needle.is_a?(Values::Collection)

          case haystack
          when Values::ConditionalValue
            # A list chosen by a ternary: test each branch. The CASE has no ELSE, so an UNKNOWN
            # condition stays UNKNOWN.
            return branches(haystack.condition, membership(needle, haystack.then_value), membership(needle, haystack.else_value))
          when Values::ConcatenatedList then return concatenated_membership(needle, haystack)
          when Values::FilteredCollection then return filtered_membership(needle, haystack)
          when Values::MappedCollection then return mapped_membership(needle, haystack)
          end
          # Any other filtered or projected association, or a filtered list, has no membership
          # translation.
          reject_collection("in", needle)
          reject_collection("in", haystack)
          scalar_membership(needle, haystack)
        end

        # `needle in list.map(t, ...)` over a list of constants. `map()` never ignores an
        # element's error, so a NULL projection (the translator's error) makes the list, and the
        # lookup, UNKNOWN. A needle that does not declare `:explicit` is a missing attribute when
        # NULL, UNKNOWN too. Otherwise it is an ordinary lookup in the projected values.
        def projection_membership(needle, projections)
          errors = projections.filter_map { |projection| SqlSupport.is_null(projection) if SqlSupport.sql_node?(projection) }
          errors << SqlSupport.is_null(needle) if SqlSupport.sql_node?(needle) && null_convention(needle) != :explicit
          unknown_if_any(errors, as_predicate(scalar_membership(needle, projections)))
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
          # A bare EXISTS has two values, so `!("x" in chain)` over an absent parent is TRUE and
          # gives back a row that the PDP denies (#315). The guard makes it NULL instead.
          needle_guard(value, scope.guarded(scope.exists(member_equals(scope, value))))
        end

        # An association's member equal to the needle, as CEL's `in` compares them.
        def member_equals(scope, value)
          member = scope.member_column
          if explicit_null?(value)
            explicit_null_equality(member, value)
          elsif cross_type_literal?(value, member_kind(scope))
            # `"2" in [2]` is false in CEL, whose equality is heterogeneous. SQL would coerce
            # one side onto the other's type: SQLite's REAL affinity reads the literal '2' as
            # the number 2, MySQL reads 'true' as 0, and PostgreSQL rejects
            # `double precision = text` outright (#505).
            false
          else
            SqlSupport.comparison("eq", member, value)
          end
        end

        # A missing needle is an error whatever the list holds, so the membership is UNKNOWN.
        def needle_guard(needle, result)
          return result unless SqlSupport.sql_node?(needle) && !explicit_null?(needle)

          unknown_if_any([SqlSupport.is_null(needle)], result)
        end

        # `value in association + [constants]`: in some part. A missing parent errors the whole
        # list, so each association's hop guard wraps the whole test, not just its own part.
        def concatenated_membership(needle, list)
          tests = list.parts.map { |part|
            next relation_membership(part.scope, needle) if part.is_a?(Values::Collection)

            test = membership(needle, part)
            explicit_null?(needle) ? in_with_present_guard(needle, part, test) : test
          }
          list.parts.grep(Values::Collection).reduce(SqlSupport.or_node(tests)) { |result, part| part.scope.guarded(result) }
        end

        # `value in association.filter(x, body)`. filter() never ignores an element's error, so
        # one UNKNOWN body makes the whole membership UNKNOWN, before any match counts.
        def filtered_membership(needle, filtered)
          scope = filtered.scope
          unless scope.mapping&.member_field
            raise UnsupportedOperatorError,
              "in over a filtered association of structs: only an association of scalar " \
              "members (a member_field association) has elements a value can equal"
          end

          result = scope.guarded(
            SqlSupport.case_node(
              [
                [scope.exists(SqlSupport.is_null(filtered.body)), nil],
                [scope.exists(SqlSupport.and_node([filtered.body, member_equals(scope, needle)])), true]
              ],
              else_value: false
            )
          )
          needle_guard(needle, result)
        end

        # `value in association.map(x, projection)`: {#has_intersection} with a one-element list.
        def mapped_membership(needle, mapped)
          unless SqlSupport.sql_node?(mapped.projection)
            raise UnsupportedOperatorError,
              "in over a map() whose projection is #{describe(mapped.projection)}: only a " \
              "projection to a scalar value is translated"
          end

          needle_guard(needle, has_intersection(mapped, [needle]))
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
            return SqlSupport.or_node(predicates) if present.length == members.length
            # A computed needle is never null in CEL; NULL is its error, which errors the whole
            # membership. See {Comparisons#computed_node?}.
            if computed_node?(needle)
              return unknown_if_any([SqlSupport.is_null(needle)], SqlSupport.or_node(predicates))
            end

            # A null element makes the membership test true for an attribute that is null. The
            # attribute must be null and not only missing.
            predicates << SqlSupport.comparison("eq", needle, nil)
            return SqlSupport.or_node(predicates)
          end

          # A list that holds a column, or a needle that is a constant, needs one comparison for
          # each element, each as `==` would compare it (cerbos/query-plan-adapters#574).
          # `null in [R.attr.x]` is the example: it is true when the column is null.
          columns = [needle, *members].select { |operand| SqlSupport.sql_node?(operand) }
          # Only the needle meets each member. A computed node has no declaration to align.
          needle_convention = SqlSupport.sql_node?(needle) && null_convention(needle)
          if needle_convention && members.any? { |member|
            SqlSupport.sql_node?(member) && ![nil, needle_convention].include?(null_convention(member))
          }
            raise mixed_null_conventions_error("in")
          end

          result = SqlSupport.or_node(members.map { |member| member_equality(needle, member) })
          # CEL builds the list before it compares, so a missing attribute or a computed error
          # anywhere in it errors the whole membership, whatever the other elements say, and
          # under `not` too.
          missing = columns.reject { |column| null_convention(column) == :explicit }
          unknown_if_any(missing.uniq.map { |column| SqlSupport.is_null(column) }, result)
        end

        # CEL equality for one element of a membership test. A NULL column under `:explicit` is a
        # null value, so a comparison against it must be definite: two nulls are equal, a null
        # and a value are not. A NULL column under `:omitted` is left UNKNOWN, and
        # {#scalar_membership} guards it.
        def member_equality(needle, member)
          needle_is_node = SqlSupport.sql_node?(needle)
          member_is_node = SqlSupport.sql_node?(member)

          return SqlSupport.comparison("eq", member, nil) if needle.nil? && member_is_node
          return SqlSupport.comparison("eq", needle, nil) if member.nil? && needle_is_node

          return needle.nil? == member.nil? if needle.nil? || member.nil?

          needle_explicit = needle_is_node && null_convention(needle) == :explicit
          member_explicit = member_is_node && null_convention(member) == :explicit
          definite = (needle_explicit || member_explicit) &&
            [needle, member].all? { |operand| SqlSupport.sql_node?(operand) || constant?(operand) }
          if definite
            return definite_equality("eq", needle, member, needle_explicit, member_explicit)
          end

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
          unless right.is_a?(Array)
            # hasIntersection takes two lists: a map or a scalar is CEL's no-overload error. A
            # column might hold an array the adapter cannot see, so it is refused.
            return cel_type_error if right.nil? || right.is_a?(Hash) || constant?(right)

            raise UnsupportedOperatorError,
              "hasIntersection is translated only against a list literal, got #{describe(right)}"
          end
          values = right

          case left
          when Values::Collection
            # As with membership: a bare EXISTS is FALSE for an absent parent, so
            # `!hasIntersection(chain, [...])` would be TRUE for it (#315). Literals of another
            # type never intersect.
            kind = member_kind(left.scope)
            values = values.reject { |value| cross_type_literal?(value, kind) }
            # A scalar list holds null values, as {Environment#element} registers it, so a null
            # literal matches a NULL element rather than reading as a computed error.
            member = register_null_representation(left.scope.member_column, :explicit)
            left.scope.guarded(left.scope.exists(scalar_membership(member, values)))
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
