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
          # `x in map` tests the map's keys in CEL. The planner folds a literal map to `==`, but a
          # map can still arrive as a value; answering FALSE would grant its negation.
          haystack = haystack.keys if haystack.is_a?(Hash)
          return composite_membership(needle, haystack) if composite?(needle)
          return relation_membership(haystack.scope, needle) if haystack.is_a?(Values::Collection)
          return projection_membership(needle, haystack.projections) if haystack.is_a?(Values::ConstantProjection)
          return relation_membership(needle.scope, haystack) if needle.is_a?(Values::Collection)

          # A filtered or projected relation, or a filtered list, has no membership translation.
          reject_collection("in", needle)
          reject_collection("in", haystack)
          scalar_membership(needle, haystack)
        end

        # `needle in list.map(t, ...)` over a list of constants. `map()` never ignores an element's
        # error, so a NULL projection (a computed error) makes the list, and the lookup, UNKNOWN.
        # A needle that is not `:explicit` is a missing attribute when NULL, UNKNOWN too.
        # Otherwise it is an ordinary lookup in the projected values.
        def projection_membership(needle, projections)
          errors = projections.filter_map { |projection| ArelSupport.is_null(projection) if ArelSupport.arel_node?(projection) }
          errors << ArelSupport.is_null(needle) if ArelSupport.arel_node?(needle) && null_convention(needle) != :explicit
          unknown_if_any(errors, as_predicate(scalar_membership(needle, projections)))
        end

        # A list or map literal.
        def composite?(value)
          value.is_a?(Array) || value.is_a?(Hash)
        end

        # A list or map needle. A relation's members are scalars, which a list or map never
        # equals, so the answer is FALSE (the chain guard keeps an absent parent UNKNOWN). Against
        # a list of constants it is folded with CEL equality.
        def composite_membership(needle, haystack)
          unless deep_constant?(needle)
            raise UnsupportedOperatorError, "in with a list or map needle holding a column is not translated"
          end
          return haystack.scope.guarded(haystack.scope.exists(false)) if haystack.is_a?(Values::Collection)
          return haystack.any? { |member| member == needle } if haystack.is_a?(Array) && deep_constant?(haystack)

          raise UnsupportedOperatorError,
            "in with a list or map needle is translated only against a relation or a list of constants"
        end

        # `value in R.attr.<relation>`, as an EXISTS over the related rows.
        def relation_membership(scope, value)
          member = scope.member_column
          condition =
            if explicit_null?(value)
              explicit_null_equality(member, value)
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

          # A list or map element never equals a scalar column, so it cannot match. A constant
          # needle is folded against it like any other element, below.
          if ArelSupport.arel_node?(needle) && members.any? { |member| composite?(member) }
            members = members.reject { |member| composite?(member) }
            if members.empty?
              return false if explicit_null?(needle)

              # A missing attribute is still an error.
              return unknown_if_any([ArelSupport.is_null(needle)], false)
            end
          end

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
            return ArelSupport.or_node(predicates) if present.length == members.length
            # A computed needle is never null in CEL; NULL is its error, which errors the whole
            # membership. See {Comparisons#computed_node?}.
            if computed_node?(needle)
              return unknown_if_any([ArelSupport.is_null(needle)], ArelSupport.or_node(predicates))
            end

            # A null element matches a null attribute (a null value, not a missing one).
            predicates << ArelSupport.comparison("eq", needle, nil)
            return ArelSupport.or_node(predicates)
          end

          # A column in the list, or a constant needle: one comparison per element, each as `==`
          # would compare it (#574). E.g. `null in [R.attr.x]` is true when the column is null.
          columns = [needle, *members].select { |operand| ArelSupport.arel_node?(operand) }
          # Only the needle meets each member. A computed node has no declaration to align.
          needle_convention = ArelSupport.arel_node?(needle) && null_convention(needle)
          if needle_convention && members.any? { |member|
            ArelSupport.arel_node?(member) && ![nil, needle_convention].include?(null_convention(member))
          }
            raise mixed_null_conventions_error("in")
          end

          result = ArelSupport.or_node(members.map { |member| member_equality(needle, member) })
          # CEL builds the list before it compares, so a missing attribute or a computed error
          # anywhere errors the whole membership, whatever the other elements say. Under `not` too.
          missing = columns.reject { |column| null_convention(column) == :explicit }
          unknown_if_any(missing.uniq.map { |column| ArelSupport.is_null(column) }, result)
        end

        # CEL equality for one element. A NULL column under `:explicit` is a null value, so a
        # comparison against it must be definite: two nulls are equal, a null and a value are
        # not. A NULL column under `:omitted` is left UNKNOWN; {#scalar_membership} guards it.
        def member_equality(needle, member)
          needle_is_node = ArelSupport.arel_node?(needle)
          member_is_node = ArelSupport.arel_node?(member)

          return ArelSupport.comparison("eq", member, nil) if needle.nil? && member_is_node
          return ArelSupport.comparison("eq", needle, nil) if member.nil? && needle_is_node

          return needle.nil? == member.nil? if needle.nil? || member.nil?

          needle_explicit = needle_is_node && null_convention(needle) == :explicit
          member_explicit = member_is_node && null_convention(member) == :explicit
          definite = (needle_explicit || member_explicit) &&
            [needle, member].all? { |operand| ArelSupport.arel_node?(operand) || constant?(operand) }
          if definite
            return definite_equality("eq", needle, member, needle_explicit, member_explicit)
          end

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
            # A scalar list holds null values, as {Environment#element} registers it, so a null
            # literal matches a NULL element rather than reading as a computed error.
            member = register_null_representation(left.scope.member_column, :explicit)
            left.scope.guarded(left.scope.exists(scalar_membership(member, values)))
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
