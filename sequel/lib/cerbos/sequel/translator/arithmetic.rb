# frozen_string_literal: true

module Cerbos
  module Sequel
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
          reject_int_beside_non_int(operator, left, right)

          # CEL uses `+` for strings and for numbers. SQL does not. On SQLite and MySQL,
          # `'a' + 'b'` is an addition of numbers, and it changes both sides into 0. Thus string
          # operands need the concatenation operation of the dialect.
          if operator == "add" && (string_valued?(left) || string_valued?(right))
            return left + right if left.is_a?(::String) && right.is_a?(::String)

            return dialect.concat(left, right)
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
            right = SqlSupport.function("NULLIF", [right, 0]) if operator == "mod" && !right.is_a?(Numeric)
            return record_cel_type(SqlSupport.infix(ARITHMETIC.fetch(operator), left, right), :int)
          end

          # CEL holds every attribute number as a double. PostgreSQL and MySQL would compute an
          # integer or decimal column with a literal like 0.1 in exact decimal, so
          # `aNumber * 0.1 == 0.3` would hold for 3 where CEL computes 0.30000000000000004.
          left, right = [left, right].map { |operand| exact_numeric_column?(operand) ? as_double(operand) : operand }
          record_cel_type(SqlSupport.infix(ARITHMETIC.fetch(operator), left, right), :double)
        end

        def exact_numeric_column?(value)
          SqlSupport.sql_node?(value) && EXACT_NUMERIC_COLUMN_TYPES.include?(column_type(value))
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

          # A constant denominator that is not zero can never divide by zero. Thus a plain
          # division is exact, and it keeps the SQL small.
          if denominator.is_a?(Numeric) && !denominator.to_f.zero?
            return record_cel_type(SqlSupport.infix("/", as_double(numerator), denominator.to_f), :double)
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
            condition: SqlSupport.comparison("eq", as_double(denominator), 0.0),
            then_value: Values::IEEEConstant.new(value: Float::NAN),
            else_value: SqlSupport.infix("/", as_double(numerator), as_double(denominator))
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
            condition: SqlSupport.comparison("eq", as_double(numerator), 0.0),
            then_value: Values::IEEEConstant.new(value: Float::NAN),
            else_value: Values::ConditionalValue.new(
              condition: SqlSupport.comparison("gt", as_double(numerator), 0.0),
              then_value: Values::IEEEConstant.new(value: sign * Float::INFINITY),
              else_value: Values::IEEEConstant.new(value: -sign * Float::INFINITY)
            )
          )
        end
      end
    end
  end
end
