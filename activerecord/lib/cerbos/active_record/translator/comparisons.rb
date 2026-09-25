# frozen_string_literal: true

module Cerbos
  module ActiveRecord
    class Translator
      # `eq`, `ne`, `lt`, `gt`, `le` and `ge`, in the order of the CEL source.
      #
      # @private
      module Comparisons
        private

        def compare(operator, left, right)
          if left.is_a?(Values::DoubleText) || right.is_a?(Values::DoubleText)
            return compare_double_text(operator, left, right)
          end

          # A kept ternary: compare each arm, then rebuild the branches. The CASE has no ELSE,
          # so an UNKNOWN condition stays UNKNOWN.
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

          reject_collection(operator, left)
          reject_collection(operator, right)
          assert_timestamp_wrapped(left, right)
          return compare_list_literal(operator, left, right) if left.is_a?(Array) || right.is_a?(Array)

          # Two constants: compute the result here instead of emitting constant SQL.
          if constant?(left) && constant?(right)
            return fold_comparison(operator, left, right)
          end

          # `= NULL` is never true in SQL. Put the null on the right so Arel emits IS [NOT] NULL.
          if left.nil? && %w[eq ne].include?(operator)
            return ArelSupport.comparison(operator, right, nil)
          end

          if different_scalar_types?(left, right)
            return heterogeneous_comparison(operator, left, right, false, false)
          end

          ArelSupport.comparison(operator, left, right)
        end

        # A list literal. CEL compares lists element by element, in order, and a list never
        # equals a scalar. A column is always a scalar here: a relation was refused above.
        def compare_list_literal(operator, left, right)
          constants = [left, right].grep(Array).flatten.all? { |element| element.nil? || constant?(element) }
          unless constants && %w[eq ne].include?(operator)
            raise UnsupportedOperatorError,
              "#{operator} with a list literal: only eq and ne against a list of constants " \
              "are translated"
          end
          return fold_comparison(operator, left, right) unless ArelSupport.arel_node?(left) || ArelSupport.arel_node?(right)

          heterogeneous_comparison(operator, left, right, false, false)
        end

        # Two temporal columns must both go through `timestamp()`. A raw comparison would
        # lose the RFC-3339 spelling that CEL compares.
        def assert_timestamp_wrapped(left, right)
          operands = [left, right]
          return unless operands.all? { |value| TEMPORAL_COLUMN_TYPES.include?(column_type(value)) }
          return if operands.all? { |value| timestamp_operand?(value) }

          raise UnsupportedOperatorError,
            "Raw temporal column comparison loses RFC-3339 string spelling; wrap both operands in timestamp()"
        end

        def scalar_kind(value)
          case value
          when ::String then :string
          when Numeric then :number
          when true, false then :boolean
          when Array then :list
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

        # Mixed types: SQL may equate "0" and 0, CEL never does. Explicit nulls still compare;
        # an omitted operand stays an error.
        def heterogeneous_comparison(operator, left, right, left_explicit, right_explicit)
          return nil unless %w[eq ne].include?(operator)

          equal = (left_explicit && right_explicit) ? both_null(left, right) : false
          result = (operator == "ne") ? ArelSupport.not_node(equal) : equal
          missing = [[left, left_explicit], [right, right_explicit]]
            .filter_map { |operand, explicit| ArelSupport.is_null(operand) if !explicit && ArelSupport.arel_node?(operand) }

          unknown_if_any(missing, result)
        end

        # In Cerbos 0.55, NaN compares false except `!=` (true). PostgreSQL orders NaN above
        # every number, so compute the result here and keep missing-attribute errors.
        def compare_non_finite(operator, left, right)
          left_value = left.is_a?(Values::IEEEConstant) ? left.value : left
          right_value = right.is_a?(Values::IEEEConstant) ? right.value : right

          nan_side = [left_value, right_value].find { |v| v.is_a?(Float) && v.nan? }
          if nan_side
            other = left_value.equal?(nan_side) ? right_value : left_value
            result = (operator == "ne")

            return result if other.is_a?(Numeric)
            if ArelSupport.arel_node?(other)
              # A missing attribute stays an error, not the TRUE of `ne`.
              return unknown_if_any([ArelSupport.is_null(other)], result)
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

        # `CASE WHEN <any of missing> THEN NULL ELSE result END`. A missing attribute is an
        # error in CEL, so the row must be UNKNOWN (denied under both polarities).
        def unknown_if_any(missing, result)
          return result if missing.empty?

          ArelSupport.case_node([[ArelSupport.or_node(missing), nil]], else_value: result)
        end
      end
    end
  end
end
