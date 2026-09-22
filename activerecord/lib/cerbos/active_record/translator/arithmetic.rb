# frozen_string_literal: true

module Cerbos
  module ActiveRecord
    class Translator
      # +add+, +sub+, +mult+, +mod+ and +div+.
      #
      # CEL arithmetic on attributes uses doubles, so a division by zero is not an error: it
      # gives NaN or an Infinity. SQL cannot hold those values. The translator keeps them as
      # {Values::IEEEConstant} and {Values::ConditionalValue} until the comparison around them
      # calculates each branch.
      module Arithmetic
        private

        def arithmetic(operator, left, right)
          # A division that can give a value which is not finite stays as branches until a
          # comparison calculates it. More arithmetic on those branches has no SQL equivalent,
          # so the adapter raises instead of making an incorrect filter.
          require_scalars(operator, left, right)

          # CEL uses `+` for strings and for numbers. SQL does not. On SQLite and MySQL,
          # `'a' + 'b'` is an addition of numbers, and it changes both sides into 0. Thus string
          # operands need the concatenation operation of the dialect.
          if operator == "add" && (string_valued?(left) || string_valued?(right))
            return left + right if left.is_a?(::String) && right.is_a?(::String)

            return dialect.concat(left, right)
          end

          if left.is_a?(Numeric) && right.is_a?(Numeric)
            return left.public_send(ARITHMETIC.fetch(operator), right)
          end

          ArelSupport.infix(ARITHMETIC.fetch(operator), left, right)
        end

        # Cerbos sends each number as a double, and CEL arithmetic on attributes uses doubles.
        # Thus the division must also use doubles. If it did not, SQLite and PostgreSQL would do
        # an integer division and change +5 / 2+ into +2+.
        def divide(numerator, denominator)
          require_scalars("div", numerator, denominator)

          if numerator.is_a?(Numeric) && denominator.is_a?(Numeric)
            return divide_constants(numerator.to_f, denominator.to_f)
          end

          # A constant denominator that is not zero can never divide by zero. Thus a plain
          # division is exact, and it keeps the SQL small.
          if denominator.is_a?(Numeric) && !denominator.to_f.zero?
            return ArelSupport.infix("/", as_double(numerator), denominator.to_f)
          end

          divide_with_zero_denominator(numerator, denominator)
        end

        def divide_constants(numerator, denominator)
          return Values::IEEEConstant.new(value: numerator / denominator) if denominator.zero?

          numerator / denominator
        end

        # A division by zero is not an error in CEL, because CEL arithmetic on attributes uses
        # doubles. IEEE-754 gives NaN for 0/0, +Infinity for a positive numerator, and -Infinity
        # for a negative one. SQL cannot hold those three values, and NULL is not equal to any of
        # them: `NaN != 1.0` is TRUE in CEL, but `NULL != 1.0` is UNKNOWN in SQL, and thus a
        # NULL would remove a row that the PDP permits.
        #
        # The translator keeps the three cases as branches. The comparison around the division
        # then calculates each branch, in the same way as any other constant that is not finite.
        def divide_with_zero_denominator(numerator, denominator)
          # The denominator is a constant zero, so the sign of that zero is known.
          return zero_denominator_value(numerator, zero_sign(denominator)) if denominator.is_a?(Numeric)

          # The denominator is row-dependent. SQL cannot tell -0.0 from 0.0 — both satisfy
          # `= 0` and no portable function reads the sign bit — so the sign of an Infinity is
          # unknowable here. The one shape that stays safe is a division of a value by itself:
          # the denominator can only be zero when the numerator is zero too, which gives NaN,
          # and NaN has no sign question.
          unless numerator == denominator
            raise UnsupportedOperatorError,
              "Cannot divide by a column that may be zero: IEEE-754 keeps the sign of a zero, " \
              "SQL cannot tell -0.0 from 0.0, and thus the sign of the Infinity is unknown. " \
              "Divide by a constant, or keep this shape out of the policy."
          end

          Values::ConditionalValue.new(
            condition: ArelSupport.comparison("eq", as_double(denominator), 0.0),
            then_value: Values::IEEEConstant.new(value: Float::NAN),
            else_value: ArelSupport.infix("/", as_double(numerator), as_double(denominator))
          )
        end

        # IEEE-754 keeps the sign of a zero. `2.0 / -0.0` is -Infinity, not +Infinity, because the
        # sign of the result is the sign of the numerator against the sign of the denominator.
        def zero_sign(denominator)
          (1.0 / denominator.to_f).negative? ? -1.0 : 1.0
        end

        def zero_denominator_value(numerator, sign)
          if numerator.is_a?(Numeric)
            return Values::IEEEConstant.new(value: numerator.to_f / (sign * 0.0))
          end

          Values::ConditionalValue.new(
            condition: ArelSupport.comparison("eq", as_double(numerator), 0.0),
            then_value: Values::IEEEConstant.new(value: Float::NAN),
            else_value: Values::ConditionalValue.new(
              condition: ArelSupport.comparison("gt", as_double(numerator), 0.0),
              then_value: Values::IEEEConstant.new(value: sign * Float::INFINITY),
              else_value: Values::IEEEConstant.new(value: -sign * Float::INFINITY)
            )
          )
        end
      end
    end
  end
end
