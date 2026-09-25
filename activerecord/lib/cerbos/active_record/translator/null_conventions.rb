# frozen_string_literal: true

module Cerbos
  module ActiveRecord
    class Translator
      # How a NULL column reaches Cerbos, and how that changes the translation.
      #
      # `:explicit`: a NULL column is sent as a null value.
      # `:omitted`: no attribute is sent, so CEL errors and denies the row.
      # The call sets a default; each mapping can override it (#302, #308).
      #
      # @private
      module NullConventions
        private

        # Refuses a null constant under `:omitted`, where the PDP denies a NULL column but
        # `IS NULL` would return it (#302).
        #
        # One shape is allowed: a field attribute compared with a scalar null by the built-in
        # `eq` or `ne`. {#with_null_conventions} renders it UNKNOWN for a NULL column, the
        # missing-attribute error CEL raises, so a `not` above it cannot flip it (#551).
        #
        # Scans operands, not operators: `in` and `hasIntersection` can carry a null in a list.
        def assert_no_null_operands(node)
          case node
          when Plan::Value
            raise null_operand_error if carries_null?(node) && null_attribute_representation == :omitted
          when Plan::Expression
            # Attribute-vs-constant uses the attribute's own convention (#308). Other shapes,
            # such as a null inside a macro over a constant list, use the call's convention.
            convention = operand_convention(node)
            unless convention.nil?
              if convention == :omitted && node.operands.any? { |o| carries_null?(o) } &&
                  !unknown_when_null?(node)
                raise null_operand_error
              end
              return
            end

            node.operands.each { |operand| assert_no_null_operands(operand) }
          end
        end

        # True if the built-in `eq` or `ne` compares an attribute with a scalar null. An operator
        # override owns its comparison and would receive the null, so it keeps the refusal.
        def unknown_when_null?(node)
          return false unless %w[eq ne].include?(node.operator)
          return false if operator_overrides.key?(node.operator)

          node.operands.any? { |operand| operand.is_a?(Plan::Value) && operand.value.nil? }
        end

        def carries_null?(node)
          return false unless node.is_a?(Plan::Value)

          node.value.nil? || (node.value.is_a?(Array) && node.value.any?(&:nil?))
        end

        # The convention of the attribute in an attribute-vs-constant equality, in either order:
        # its own declaration, else the call's. Nil if the node is another shape.
        def operand_convention(node)
          return nil unless EQUALITY_FAMILY.include?(node.operator)
          return nil unless node.operands.length == 2

          variable, constant = node.operands
          variable, constant = constant, variable if constant.is_a?(Plan::Variable)
          return nil unless variable.is_a?(Plan::Variable) && constant.is_a?(Plan::Value)

          mapping = attributes[variable.name]
          return nil unless mapping.is_a?(AttributeMapping::Field)

          mapping.null_representation || null_attribute_representation
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

        # Refuses `eq`/`ne` between two columns with different null conventions (#308).
        # The explicit side needs a definite answer for NULL; the other side needs UNKNOWN.
        # No single predicate does both. `values` always holds two operands here.
        def assert_uniform_null_conventions(operator, values)
          return unless %w[eq ne].include?(operator)

          left, right = values
          return unless ArelSupport.arel_node?(left) && ArelSupport.arel_node?(right)
          return if explicit_null?(left) == explicit_null?(right)

          raise UnsupportedOperatorError,
            "Cannot translate #{operator} between two columns under mixed null conventions. " \
            "One attribute declares null_representation: :explicit and the other does not, so " \
            "one side must answer NULL definitely and the other must answer UNKNOWN, and no " \
            "one predicate does both. Declare null_representation on both attributes, or on " \
            "neither."
        end

        # Applies declared null conventions to `plain`, the normal translation. Returns `plain`
        # when no side is `:explicit`. Taking `plain` as input keeps operator overrides intact.
        def with_null_conventions(operator, values, plain, overridden:)
          return plain unless EQUALITY_FAMILY.include?(operator)

          left, right = values
          omitted = omitted_null_comparison(operator, left, right, overridden)
          return omitted if omitted

          # A null constant is already correct: `IS NULL` matches CEL's null value.
          return plain if left.nil? || right.nil?

          left_explicit = explicit_null?(left)
          right_explicit = explicit_null?(right)
          return plain unless left_explicit || right_explicit

          if operator == "in"
            return left_explicit ? in_with_present_guard(left, right, plain) : plain
          end

          # `eq`/`ne` get rebuilt below, so leave a caller override alone rather than drop it.
          return plain if overridden

          definite_equality(operator, left, right, left_explicit, right_explicit)
        end

        # `eq`/`ne` of an `:omitted` attribute against null. A NULL column sends no attribute,
        # so CEL errors and denies the row; a present one is never null. Hence
        # `CASE WHEN col IS NULL THEN NULL ELSE FALSE END` for `eq` and `... ELSE TRUE END` for
        # `ne`. UNKNOWN stays UNKNOWN under NOT, so the result is right under any nesting (#551).
        # Nil for any other shape.
        def omitted_null_comparison(operator, left, right, overridden)
          return nil if overridden || !%w[eq ne].include?(operator)
          return nil unless left.nil? || right.nil?

          column = right.nil? ? left : right
          return nil unless omitted_attribute?(column)

          ArelSupport.case_node(
            [[ArelSupport.comparison("eq", column, nil), nil]],
            else_value: operator == "ne"
          )
        end

        # An equality that is never SQL UNKNOWN on an explicit side. In CEL, null == value is
        # FALSE, null != value is TRUE, and null == null is TRUE; SQL says UNKNOWN to all three.
        #
        # Not IS NOT DISTINCT FROM: that is dialect-specific, and it is symmetric. If only one
        # side is explicit, the other must stay UNKNOWN for NULL, or two NULLs would match.
        def definite_equality(operator, left, right, left_explicit, right_explicit)
          if different_scalar_types?(left, right)
            return heterogeneous_comparison(operator, left, right, left_explicit, right_explicit)
          end

          present = []
          present << ArelSupport.comparison("ne", left, nil) if left_explicit
          present << ArelSupport.comparison("ne", right, nil) if right_explicit

          equal = ArelSupport.and_node(present + [ArelSupport.comparison("eq", left, right)])
          equal = ArelSupport.or_node([both_null(left, right), equal]) if left_explicit && right_explicit

          (operator == "ne") ? ArelSupport.not_node(equal) : equal
        end

        # Adds `needle IS NOT NULL` next to the `in` translation, so overrides still apply.
        def in_with_present_guard(needle, haystack, plain)
          return plain unless ArelSupport.arel_node?(needle)
          # A stored collection may hold a null, and then `null in coll` is TRUE. The guard
          # would drop those rows; the collection translation handles them already.
          return plain unless haystack.is_a?(Array)
          # A null member already adds a definite `IS NULL` branch.
          return plain if haystack.any?(&:nil?)

          ArelSupport.and_node([ArelSupport.comparison("ne", needle, nil), plain])
        end

        # Column-to-column equality for a membership test. Under `:explicit`, two NULLs are
        # equal in CEL, so add that case. Under `:omitted`, CEL denies the row, and plain
        # equality (UNKNOWN) already keeps it out.
        def null_equality(left, right)
          equal = ArelSupport.comparison("eq", left, right)
          return equal if null_attribute_representation == :omitted

          ArelSupport.or_node([equal, both_null(left, right)])
        end

        # `left IS NULL AND right IS NULL`: two explicit nulls are equal in CEL.
        def both_null(left, right)
          ArelSupport.and_node([
            ArelSupport.comparison("eq", left, nil),
            ArelSupport.comparison("eq", right, nil)
          ])
        end
      end
    end
  end
end
