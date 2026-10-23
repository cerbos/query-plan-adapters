# frozen_string_literal: true

module Cerbos
  module Sequel
    class Translator
      # +eq+, +ne+, +lt+, +gt+, +le+ and +ge+, in the order of the CEL source.
      module Comparisons
        private

        def compare(operator, left, right)
          if left.is_a?(Values::DoubleText) || right.is_a?(Values::DoubleText)
            return compare_double_text(operator, left, right)
          end

          # This is a ternary that the translator kept. It compares each arm and then makes the
          # branches again. This CASE also has no ELSE clause. Thus an UNKNOWN condition stays
          # UNKNOWN.
          if left.is_a?(Values::ConditionalValue)
            return branches(left.condition,
              compare(operator, left.then_value, right), compare(operator, left.else_value, right))
          end
          if right.is_a?(Values::ConditionalValue)
            return branches(right.condition,
              compare(operator, left, right.then_value), compare(operator, left, right.else_value))
          end

          if left.is_a?(Values::IEEEConstant) || right.is_a?(Values::IEEEConstant)
            return compare_non_finite(operator, left, right)
          end

          return compare_temporal(operator, left, right) if temporal_value?(left) || temporal_value?(right)
          if left.is_a?(Values::SetOperation) || right.is_a?(Values::SetOperation)
            return compare_set_operation(operator, left, right)
          end
          # `except(...) == []` and its `!=`: a list is empty exactly when its size is 0.
          if %w[eq ne].include?(operator) && [left, right].include?([]) && [left, right].any? { |value| filtered_list?(value) }
            return compare(operator, size(filtered_list?(left) ? left : right), 0)
          end

          reject_collection(operator, left)
          reject_collection(operator, right)
          assert_timestamp_wrapped(left, right)
          if [left, right].any? { |value| value.is_a?(Array) || value.is_a?(Hash) }
            return compare_list_literal(operator, left, right)
          end

          # A timestamp against a value CEL does not hold as one: a bare temporal column is the
          # RFC-3339 string the request carried. No ordering overload exists, so the comparison
          # is an error; `==` across types is false. Comparing the column as an instant would
          # grant the rows the error denies.
          if left.is_a?(::Time) ^ right.is_a?(::Time)
            other = left.is_a?(::Time) ? right : left
            if not_a_timestamp?(other)
              return heterogeneous_comparison(operator, left, right, explicit_null?(left), explicit_null?(right))
            end
          end

          # Both sides are constants. The translator calculates the result here. It does not
          # make SQL that is always true or always false.
          if constant?(left) && constant?(right)
            return fold_comparison(operator, left, right)
          end

          if (left.nil? || right.nil?) && %w[eq ne].include?(operator)
            operand = left.nil? ? right : left
            # CEL never holds a computed value as null, so a NULL one is an error, not a null.
            if computed_node?(operand)
              return unknown_if_any([SqlSupport.is_null(operand)], operator == "ne")
            end
            # `= NULL` is never true. In CEL, `null == x` is a usual equality. Thus the adapter
            # puts the null on the right, and SqlSupport.comparison makes IS NULL or IS NOT NULL.
            return SqlSupport.comparison(operator, right, nil) if left.nil?
          end

          if different_scalar_types?(left, right)
            return heterogeneous_comparison(operator, left, right, false, false)
          end

          SqlSupport.comparison(operator, left, right)
        end

        # A list or map literal. CEL compares lists element by element, in order, and maps key
        # by key; neither ever equals a scalar (heterogeneous equality is FALSE, not an error).
        # A column is always a scalar here: an association was refused above.
        def compare_list_literal(operator, left, right)
          literals = [left, right].select { |value| value.is_a?(Array) || value.is_a?(Hash) }
          unless literals.all? { |literal| deep_constant?(literal) } && %w[eq ne].include?(operator)
            raise UnsupportedOperatorError,
              "#{operator} with a list or map literal: only eq and ne against constants " \
              "are translated"
          end
          return fold_comparison(operator, left, right) unless SqlSupport.sql_node?(left) || SqlSupport.sql_node?(right)

          heterogeneous_comparison(operator, left, right, false, false)
        end

        # Two temporal columns compare as instants only when both went through `timestamp()`.
        # A raw comparison would lose the RFC-3339 spelling that CEL compares.
        def assert_timestamp_wrapped(left, right)
          operands = [left, right]
          return unless operands.all? { |value| TEMPORAL_COLUMN_TYPES.include?(column_type(value)) }
          return if operands.all? { |value| timestamp_operand?(value) }

          raise UnsupportedOperatorError,
            "Raw temporal column comparison loses RFC-3339 string spelling; wrap both operands in timestamp()"
        end

        # A constant that is not an instant, or a node of a known type that did not go through
        # `timestamp()`. A computed node of no recorded type (a ternary of instants) is left
        # alone.
        def not_a_timestamp?(value)
          return !value.is_a?(::Time) if constant?(value)
          return false unless SqlSupport.sql_node?(value)

          !timestamp_operand?(value) && !(column_type(value) || cel_type(value)).nil?
        end

        def scalar_kind(value)
          case value
          when ::String then :string
          when Numeric then :number
          when true, false then :boolean
          when Array then :list
          when Hash then :map
          else
            kind_of_column_type(column_type(value)) || kind_of_cel_type(cel_type(value))
          end
        end

        # The kind a computed node (arithmetic, a concatenation, a ternary) was recorded with.
        def kind_of_cel_type(type)
          case type
          when :int, :double, :ambiguous_number, :number then :number
          when :string then :string
          when :bool then :boolean
          end
        end

        def kind_of_column_type(type)
          if STRING_COLUMN_TYPES.include?(type) then :string
          elsif NUMERIC_COLUMN_TYPES.include?(type) then :number
          elsif type == :boolean then :boolean
          end
        end

        def different_scalar_types?(left, right)
          left_kind, right_kind = scalar_kind(left), scalar_kind(right)
          left_kind && right_kind && left_kind != right_kind
        end

        # SQL affinity can equate a string such as "0" with the number zero. CEL cannot.
        # Explicit nulls remain comparable values; an omitted operand stays an error.
        def heterogeneous_comparison(operator, left, right, left_explicit, right_explicit)
          return nil unless %w[eq ne].include?(operator)

          equal = (left_explicit && right_explicit) ? both_null(left, right) : false
          result = (operator == "ne") ? SqlSupport.not_node(equal) : equal
          missing = [[left, left_explicit], [right, right_explicit]]
            .filter_map { |operand, explicit| SqlSupport.is_null(operand) if !explicit && SqlSupport.sql_node?(operand) }

          unknown_if_any(missing, result)
        end

        # Cerbos 0.55 uses IEEE false for NaN equality and ordering, true for inequality.
        # PostgreSQL instead orders NaN above every finite number, so calculate comparisons
        # here while preserving missing-attribute errors independently.
        def compare_non_finite(operator, left, right)
          left_value = left.is_a?(Values::IEEEConstant) ? left.value : left
          right_value = right.is_a?(Values::IEEEConstant) ? right.value : right

          nan_side = [left_value, right_value].find { |v| v.is_a?(Float) && v.nan? }
          if nan_side
            other = left_value.equal?(nan_side) ? right_value : left_value
            result = (operator == "ne")

            return result if other.is_a?(Numeric)
            if SqlSupport.sql_node?(other)
              # The translator calculates the comparison for each value that is present. But a
              # missing attribute stays an error. It does not become the true result of `ne`.
              return unknown_if_any([SqlSupport.is_null(other)], result)
            end

            raise UnsupportedOperatorError,
              "NaN can only be compared with a number or a column, got #{describe(other)}"
          end

          unless left_value.is_a?(Numeric) && right_value.is_a?(Numeric)
            raise UnsupportedOperatorError,
              "Infinity can only be compared with a number, got " \
              "#{describe(left_value)} and #{describe(right_value)}"
          end

          fold_comparison(operator, left_value, right_value)
        end

        RUBY_COMPARISONS = {
          "eq" => :==, "ne" => :!=, "lt" => :<, "gt" => :>, "le" => :<=, "ge" => :>=
        }.freeze

        def fold_comparison(operator, left, right)
          left.public_send(RUBY_COMPARISONS.fetch(operator), right)
        end

        # A list a macro or `except()` filtered, whose size {#size} counts.
        def filtered_list?(value)
          value.is_a?(Values::FilteredCollection) || value.is_a?(Values::ConstantList)
        end

        # A node that is no attribute's column (arithmetic, a connective, a quantifier, a CASE).
        # It is NULL only where CEL errors; see {Translator#null_convention}.
        def computed_node?(value)
          SqlSupport.sql_node?(value) && null_convention(value).nil?
        end

        # +CASE WHEN <any of missing> THEN NULL ELSE result END+: +result+ for a row where every
        # operand is present, and UNKNOWN for one where an operand is missing, because a missing
        # attribute is an error in CEL and the PDP denies the row under both polarities.
        def unknown_if_any(missing, result)
          return result if missing.empty?

          SqlSupport.case_node([[SqlSupport.or_node(missing), nil]], else_value: result)
        end
      end
    end
  end
end
