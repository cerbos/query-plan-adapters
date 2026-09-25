# frozen_string_literal: true

module Cerbos
  module ActiveRecord
    class Translator
      # `add`, `sub`, `mult`, `mod` and `div`.
      #
      # CEL divides doubles, so x / 0 gives NaN or Infinity, which SQL cannot hold. These stay
      # as {Values::IEEEConstant} and {Values::ConditionalValue} until a comparison resolves them.
      #
      # @private
      module Arithmetic
        private

        def arithmetic(operator, left, right)
          # Arithmetic on a NaN/Infinity branch has no SQL form, so raise.
          require_scalars(operator, left, right)
          reject_int_beside_non_int(operator, left, right)

          # SQLite and MySQL treat `'a' + 'b'` as numeric (0), so strings need the dialect's
          # concatenation.
          if operator == "add" && (string_valued?(left) || string_valued?(right))
            return left + right if left.is_a?(::String) && right.is_a?(::String)

            return record_cel_type(dialect.concat(left, right), :string)
          end

          if operator == "mod" && !(cel_int?(left) && cel_int?(right))
            raise UnsupportedOperatorError,
              "% has no double overload in CEL, and every number in a request attribute is a " \
              "double, so % over an attribute that has not gone through int() is an error that " \
              "denies the row. SQL computes a remainder instead. Wrap the operands in int()."
          end

          if left.is_a?(Numeric) && right.is_a?(Numeric)
            return left.public_send(ARITHMETIC.fetch(operator), right)
          end

          if cel_int?(left) && cel_int?(right)
            # CEL's `%` by zero is an error, which denies the row under either polarity.
            # PostgreSQL raises instead, failing the whole query; NULLIF makes it UNKNOWN, as
            # SQLite and MySQL already make it.
            right = ArelSupport.function("NULLIF", [right, 0]) if operator == "mod" && !right.is_a?(Numeric)
            return record_cel_type(ArelSupport.infix(ARITHMETIC.fetch(operator), left, right), :int)
          end

          # CEL holds every attribute number as a double. PostgreSQL and MySQL would compute an
          # integer or decimal column with a literal like 0.1 in exact decimal, so
          # `aNumber * 0.1 == 0.3` would hold for 3 where CEL computes 0.30000000000000004.
          left, right = [left, right].map { |operand| exact_numeric_column?(operand) ? as_double(operand) : operand }
          record_cel_type(ArelSupport.infix(ARITHMETIC.fetch(operator), left, right), :double)
        end

        def exact_numeric_column?(value)
          ArelSupport.arel_node?(value) && EXACT_NUMERIC_COLUMN_TYPES.include?(column_type(value))
        end

        # CEL has no overload mixing an int with a double: `int(x) + R.attr.d` is an error that
        # denies the row under either polarity, where SQL adds the two numbers and a negation
        # turns the sum into a grant. An int() result beside an operand that is not certainly an
        # int (a column, whose attribute is a double, or a fractional constant) is refused.
        def reject_int_beside_non_int(operator, left, right)
          mixed = (cel_type(left) == :int && !cel_int?(right)) ||
            (cel_type(right) == :int && !cel_int?(left))
          return unless mixed

          raise UnsupportedOperatorError,
            "#{operator} of an int() result and an operand that is not an int: CEL has no " \
            "overload mixing int and double, so the expression is an error that denies the row, " \
            "but SQL computes it. Every number in a request attribute is a double; wrap both " \
            "operands in int(), or neither."
        end

        # An operand CEL holds as an int: an int() result, or arithmetic on those. A whole
        # constant counts too, since the plan does not say whether a literal was `2` or `2.0`
        # and CEL's type checker rejects an int mixed with a double.
        def cel_int?(value)
          return value.finite? && value == value.truncate if value.is_a?(Float)
          return true if value.is_a?(Integer)

          cel_type(value) == :int
        end

        # Divides as doubles, like CEL. Otherwise SQLite and PostgreSQL make `5 / 2` into `2`.
        # Two ints are the exception: CEL's int division truncates toward zero.
        def divide(numerator, denominator)
          require_scalars("div", numerator, denominator)
          reject_int_beside_non_int("div", numerator, denominator)
          return int_divide(numerator, denominator) if int_division?(numerator, denominator)

          if numerator.is_a?(Numeric) && denominator.is_a?(Numeric)
            return divide_constants(numerator.to_f, denominator.to_f)
          end

          # A non-zero constant denominator is safe as a plain division.
          if denominator.is_a?(Numeric) && !denominator.to_f.zero?
            return record_cel_type(ArelSupport.infix("/", as_double(numerator), denominator.to_f), :double)
          end

          divide_with_zero_denominator(numerator, denominator)
        end

        # Int division: both operands are CEL ints and at least one is an int() result (or int
        # arithmetic on one), since a bare whole constant may have been written `2.0`.
        def int_division?(numerator, denominator)
          (cel_type(numerator) == :int || cel_type(denominator) == :int) &&
            cel_int?(numerator) && cel_int?(denominator)
        end

        # CEL's `int / int` truncates toward zero, as SQLite's and PostgreSQL's integer `/`
        # and MySQL's `DIV` do. A zero divisor is a CEL error that denies the row under either
        # polarity, where PostgreSQL aborts the query, so only a non-zero constant divisor is
        # translated.
        def int_divide(numerator, denominator)
          unless denominator.is_a?(Numeric) && !denominator.zero?
            raise UnsupportedOperatorError,
              "int division by a value that may be zero: CEL makes an error that denies the " \
              "row, but PostgreSQL aborts the whole query, so only a non-zero constant divisor " \
              "is translated"
          end

          return (numerator.to_i.to_r / denominator.to_i).truncate if numerator.is_a?(Numeric)

          record_cel_type(dialect.int_divide(numerator, denominator.to_i), :int)
        end

        def divide_constants(numerator, denominator)
          return Values::IEEEConstant.new(value: numerator / denominator) if denominator.zero?

          numerator / denominator
        end

        # Division by zero in CEL gives NaN (0/0) or +/-Infinity. SQL NULL is no substitute:
        # `NaN != 1.0` is TRUE in CEL but `NULL != 1.0` is UNKNOWN, which drops a permitted row.
        # So keep the cases as branches for the enclosing comparison to resolve.
        def divide_with_zero_denominator(numerator, denominator)
          # Constant zero: its sign is known.
          return zero_denominator_value(numerator, zero_sign(denominator)) if denominator.is_a?(Numeric)

          # Column denominator: SQL cannot tell -0.0 from 0.0, so the Infinity's sign is
          # unknown. Only x / x is safe: a zero there always gives NaN, which has no sign.
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

        # IEEE-754 zeros are signed: `2.0 / -0.0` is -Infinity.
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
