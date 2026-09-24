# frozen_string_literal: true

module Cerbos
  module Sequel
    class Translator
      # How a NULL column reaches Cerbos, and what that does to the translation.
      #
      # Under +:explicit+ a NULL column sends an attribute whose value is null, and CEL holds a
      # null VALUE. Under +:omitted+ it sends no attribute, and CEL raises a missing-attribute
      # error that denies the row. The call gives a fallback, and each mapping can declare its
      # own (cerbos/query-plan-adapters#302, #308).
      module NullConventions
        private

        # Refuses each null constant in the plan under the `omitted` representation.
        #
        # With that convention a NULL column sends no attribute. Thus CEL raises a
        # missing-attribute error and the PDP denies the row, but `IS NULL` would give exactly
        # those rows (cerbos/query-plan-adapters#302).
        #
        # The scan examines the OPERANDS and not a list of operators. A null constant can reach
        # a predicate that selects NULL through more shapes than `eq` and `ne`: `in` and
        # `hasIntersection` carry one in a list. A list of operators would also need a change
        # for each new operator.
        #
        # The scan is wider than the shapes that give too many rows. `ne(x, null)` is correct
        # by itself, but this adapter puts a negation around a predicate and does not push it
        # into the leaf. Thus a leaf cannot know that a `not` above it will make `IS NOT NULL`
        # into a predicate that selects NULL again. To refuse each null constant is correct for
        # all the shapes; a smaller rule needs the adapter to count the negations.
        def assert_no_null_operands(node)
          case node
          when Plan::Value
            raise null_operand_error if carries_null?(node) && null_attribute_representation == :omitted
          when Plan::Expression
            # A comparison between a mapped attribute and a constant is settled by the
            # declaration of that attribute, which is what lets one call carry both conventions
            # (cerbos/query-plan-adapters#308). The rule holds for that shape only: a null
            # inside a macro over a list of constants reaches a comparison long after this scan,
            # and nothing here can say which column it will meet, so those keep the convention
            # of the call.
            declared = declared_operand_convention(node)
            unless declared.nil?
              raise null_operand_error if declared == :omitted && node.operands.any? { |o| carries_null?(o) }
              return
            end

            node.operands.each { |operand| assert_no_null_operands(operand) }
          end
        end

        def carries_null?(node)
          return false unless node.is_a?(Plan::Value)

          node.value.nil? || (node.value.is_a?(Array) && node.value.any?(&:nil?))
        end

        # The convention that a binary equality-family comparison between one mapped attribute
        # and one constant declares, in either operand order. Returns nil when the node is not
        # that shape, or when the attribute declares nothing — the caller then falls back to the
        # convention of the call.
        def declared_operand_convention(node)
          return nil unless EQUALITY_FAMILY.include?(node.operator)
          return nil unless node.operands.length == 2

          variable, constant = node.operands
          variable, constant = constant, variable if constant.is_a?(Plan::Variable)
          return nil unless variable.is_a?(Plan::Variable) && constant.is_a?(Plan::Value)

          mapping = attributes[variable.name]
          return nil unless mapping.is_a?(AttributeMapping::Field)

          mapping.null_representation
        end

        def null_operand_error
          UnsupportedOperatorError.new(
            "Cannot translate a null constant with null_attribute_representation: :omitted. " \
            "A NULL column then sends no attribute, so Cerbos evaluates the comparison as a " \
            "missing-attribute error and denies the row, but a filter that selects NULL " \
            "would return that row. Send a NULL column as an explicit null and use " \
            ":explicit, or keep this shape out of the policy."
          )
        end

        # Two columns under different conventions have no faithful rendering, so the adapter
        # refuses the comparison instead of a direction.
        #
        # The declared side needs a DEFINITE answer for its NULL, because CEL holds a null value
        # there and `null != "x"` is TRUE. The other side needs UNKNOWN for its NULL, because
        # that is a missing attribute and CEL denies it under both polarities. A definite
        # predicate gives rows that the PDP refuses; a plain one loses rows that the PDP
        # permits. No one predicate is both. Declare the convention on both attributes, or on
        # neither (cerbos/query-plan-adapters#308).
        #
        # The arity of +operator+ is already asserted, so +values+ holds two operands.
        def assert_uniform_null_conventions(operator, values)
          return unless %w[eq ne].include?(operator)

          left, right = values
          return unless SqlSupport.sql_node?(left) && SqlSupport.sql_node?(right)
          return if explicit_null?(left) == explicit_null?(right)

          raise UnsupportedOperatorError,
            "Cannot translate #{operator} between two columns under mixed null conventions. " \
            "One attribute declares null_representation: :explicit and the other does not, so " \
            "one side must answer NULL definitely and the other must answer UNKNOWN, and no " \
            "one predicate does both. Declare null_representation on both attributes, or on " \
            "neither."
        end

        # The comparison with the declared conventions applied, or +plain+ when no attribute in
        # it declares +:explicit+.
        #
        # +plain+ is the usual translation, and it comes in as an argument and is not made again
        # here. Thus an operator override stays in effect on every path.
        def with_null_conventions(operator, values, plain, overridden:)
          return plain unless EQUALITY_FAMILY.include?(operator)

          left, right = values
          # A comparison against a null constant is already correct: `IS NULL` selects exactly
          # the rows where CEL holds a null value.
          return plain if left.nil? || right.nil?

          left_explicit = explicit_null?(left)
          right_explicit = explicit_null?(right)
          return plain unless left_explicit || right_explicit

          if operator == "in"
            return left_explicit ? in_with_present_guard(left, right, plain) : plain
          end

          # `eq` and `ne` RESTRUCTURE the comparison. An operator that the caller overrode is
          # thus left alone: to replace it would make this declaration discard the translation
          # of the caller in silence, which is not what the declaration says.
          return plain if overridden

          definite_equality(operator, left, right, left_explicit, right_explicit)
        end

        # An equality that can never be SQL UNKNOWN.
        #
        # An attribute that the caller sends as an explicit null holds a null VALUE in CEL. Thus
        # equality against a value that is not null is a definite FALSE, inequality is a definite
        # TRUE, and two nulls are EQUAL. SQL answers UNKNOWN to all three, and UNKNOWN keeps the
        # row out under BOTH polarities — so a NOT above it has nothing definite to invert.
        #
        # This is deliberately not a null-safe equality operator such as IS NOT DISTINCT FROM.
        # Two reasons, and the second one carries the weight. The expansion below needs no
        # knowledge of the dialect, and it must not be SYMMETRIC: when only one side declares
        # the convention the other side keeps propagating UNKNOWN for its NULL, and a null-safe
        # operator would match the two NULLs and give too many rows.
        def definite_equality(operator, left, right, left_explicit, right_explicit)
          if different_scalar_types?(left, right)
            return heterogeneous_comparison(operator, left, right, left_explicit, right_explicit)
          end

          present = []
          present << SqlSupport.comparison("ne", left, nil) if left_explicit
          present << SqlSupport.comparison("ne", right, nil) if right_explicit

          equal = SqlSupport.and_node(present + [SqlSupport.comparison("eq", left, right)])
          equal = SqlSupport.or_node([both_null(left, right), equal]) if left_explicit && right_explicit

          (operator == "ne") ? SqlSupport.not_node(equal) : equal
        end

        # `in` gains a presence guard beside whatever the membership translated to. It does not
        # replace it, so an operator override still composes.
        def in_with_present_guard(needle, haystack, plain)
          return plain unless SqlSupport.sql_node?(needle)
          # A stored COLLECTION and not a list of constants: a null element can exist at run
          # time, and `null in coll` is TRUE when it does, so the guard would remove exactly the
          # rows that CEL permits. The translation of the collection already handles the null
          # member.
          return plain unless haystack.is_a?(Array)
          # A null member already forces the `IS NULL` branch, which is definite by itself.
          return plain if haystack.any?(&:nil?)

          SqlSupport.and_node([SqlSupport.comparison("ne", needle, nil), plain])
        end

        # Equality between two columns for a membership test.
        #
        # With the `explicit` convention a NULL column sends an attribute whose value is null,
        # and two nulls are equal in CEL. The result of that comparison in SQL is UNKNOWN, so
        # the adapter writes the condition out.
        #
        # With the `omitted` convention a NULL column sends no attribute. Two NULL columns are
        # then two MISSING attributes, CEL raises a missing-attribute error, and the PDP denies
        # the row. Plain equality gives UNKNOWN for a NULL column and keeps the row out, which
        # is the correct answer for that convention.
        def null_equality(left, right)
          equal = SqlSupport.comparison("eq", left, right)
          return equal if null_attribute_representation == :omitted

          SqlSupport.or_node([equal, both_null(left, right)])
        end

        # +left IS NULL AND right IS NULL+: two explicit nulls, which CEL finds equal.
        def both_null(left, right)
          SqlSupport.and_node([
            SqlSupport.comparison("eq", left, nil),
            SqlSupport.comparison("eq", right, nil)
          ])
        end
      end
    end
  end
end
