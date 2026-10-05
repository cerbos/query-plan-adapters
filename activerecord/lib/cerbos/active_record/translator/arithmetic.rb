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
          return concatenate(left, right) if operator == "add" && (list_operand?(left) || list_operand?(right))
          if [left, right].any? { |operand| temporal_value?(operand) }
            return duration_arithmetic(operator, left, right)
          end
          if %w[add sub mult].include?(operator) && (deferred_value?(left) || deferred_value?(right))
            return deferred_arithmetic(operator, left, right)
          end

          # Any other arithmetic on a NaN/Infinity branch has no SQL form, so raise.
          require_scalars(operator, left, right)
          return cel_type_error if arithmetic_type_error?(operator, left, right)
          return cel_type_error if int_beside_non_int?(operator, left, right)

          # SQLite and MySQL treat `'a' + 'b'` as numeric (0), so strings need the dialect's
          # concatenation.
          if operator == "add" && (string_valued?(left) || string_valued?(right))
            return left + right if left.is_a?(::String) && right.is_a?(::String)

            return record_cel_type(dialect.concat(left, right), :string)
          end

          if operator == "mod" && !(cel_int?(left) && cel_int?(right))
            # `%` exists for ints only. An operand CEL certainly holds as something else (every
            # attribute number is a double) makes it an error on every row.
            return cel_type_error if certainly_non_int?(left) || certainly_non_int?(right)

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

        # `+`, `-` or `*` over a value that may be NaN or an Infinity. CEL carries the non-finite
        # value through the arithmetic; SQL has none to carry. So the arithmetic moves into the
        # branches the division left, and each branch is computed where it can be: a finite arm in
        # SQL, a non-finite constant in Ruby with IEEE-754 (`NaN * 0.0` is NaN, `Inf + 1` is Inf),
        # and NaN beside a double as NaN wherever that value is present (NaN absorbs every double)
        # and UNKNOWN where it is missing. An Infinity beside a column, whose stored value might be
        # the opposite Infinity, and two branching operands are refused. The comparison around the
        # result resolves its branches as before.
        def deferred_arithmetic(operator, left, right)
          if left.is_a?(Values::ConditionalValue) && right.is_a?(Values::ConditionalValue)
            raise UnsupportedOperatorError,
              "#{operator} of two values that may each be NaN or Infinity is not translated"
          end
          if left.is_a?(Values::ConditionalValue)
            return Values::ConditionalValue.new(
              condition: left.condition,
              then_value: arithmetic(operator, left.then_value, right),
              else_value: arithmetic(operator, left.else_value, right)
            )
          end
          if right.is_a?(Values::ConditionalValue)
            return Values::ConditionalValue.new(
              condition: right.condition,
              then_value: arithmetic(operator, left, right.then_value),
              else_value: arithmetic(operator, left, right.else_value)
            )
          end

          constant, other = left.is_a?(Values::IEEEConstant) ? [left, right] : [right, left]
          other_value = other.is_a?(Values::IEEEConstant) ? other.value : other
          if other_value.is_a?(Numeric)
            operands = left.is_a?(Values::IEEEConstant) ? [constant.value, other_value] : [other_value, constant.value]
            value = operands[0].to_f.public_send(ARITHMETIC.fetch(operator), operands[1].to_f)
            return value.finite? ? value : Values::IEEEConstant.new(value: value)
          end
          if constant.value.nan? && cel_double?(other)
            # `other = other` is TRUE wherever the value is present and UNKNOWN where it is NULL,
            # so the CASE the comparison builds is NaN or NULL, never the unreachable arm.
            present = ArelSupport.comparison("eq", other, other)
            return Values::ConditionalValue.new(condition: present, then_value: constant, else_value: constant)
          end

          raise UnsupportedOperatorError,
            "#{operator} cannot take an operand that may be NaN or Infinity beside " \
            "#{describe(other)}: only a number, or NaN beside a double, is carried"
        end

        # CEL has no arithmetic over a boolean, and over a string only `+` of two strings. Each
        # such operand is an error on every row, where SQL concatenates a string with a number,
        # and SQLite and MySQL read a boolean or a string as a number. An operand of unknown kind
        # decides nothing.
        def arithmetic_type_error?(operator, left, right)
          [left, right].each { |operand| reject_non_scalar_arithmetic(operator, operand) }
          kinds = [scalar_kind(left), scalar_kind(right)]
          return true if kinds.include?(:boolean)
          return false unless kinds.include?(:string)

          operator != "add" || kinds.include?(:number)
        end

        # List concatenation is valid CEL with no SQL form, and the adapter has no durations: a
        # temporal column is an RFC-3339 string to CEL unless timestamp() wraps it, and a
        # timestamp takes only a duration. Each is refused.
        def reject_non_scalar_arithmetic(operator, operand)
          if operand.is_a?(Array) || operand.is_a?(Hash)
            raise UnsupportedOperatorError, "#{operator} over a list or map literal is not translated"
          end
          return unless TEMPORAL_COLUMN_TYPES.include?(column_type(operand))

          raise UnsupportedOperatorError,
            "#{operator} over a temporal column is not translated: CEL reads it as an RFC-3339 " \
            "string, or as a timestamp that takes only a duration"
        end

        def exact_numeric_column?(value)
          ArelSupport.arel_node?(value) && EXACT_NUMERIC_COLUMN_TYPES.include?(column_type(value))
        end

        # CEL has no overload mixing an int with anything else: `int(x) + R.attr.d` is an error
        # that denies the row under either polarity, where SQL adds the two numbers and a negation
        # turns the sum into a grant. Beside an operand CEL certainly holds as something other than
        # an int, that error is on every row: true, and the caller renders it UNKNOWN. Beside one
        # whose CEL type the plan does not settle (a ternary of whole constants) it is refused.
        def int_beside_non_int?(operator, left, right)
          mixed = (cel_type(left) == :int && !cel_int?(right)) ||
            (cel_type(right) == :int && !cel_int?(left))
          return false unless mixed
          return true if certainly_non_int?(left) || certainly_non_int?(right)

          raise UnsupportedOperatorError,
            "#{operator} of an int() result and an operand that is not an int: CEL has no " \
            "overload mixing int and double, so the expression is an error that denies the row, " \
            "but SQL computes it. Every number in a request attribute is a double; wrap both " \
            "operands in int(), or neither."
        end

        # An operand CEL holds as something other than an int whatever the row: an attribute
        # column that has not gone through int() (a request attribute number is a double), a
        # computed double, string or boolean, a fractional or non-finite constant, a string or a
        # boolean. A whole constant, or a ternary of them, may be either, so it is not.
        def certainly_non_int?(value)
          return value != value.truncate || !value.finite? if value.is_a?(Float)
          return true if value.is_a?(::String) || value == true || value == false
          return false unless ArelSupport.arel_node?(value)
          return false if cel_type(value) == :int
          return true if %i[double string bool].include?(cel_type(value))

          !column_type(value).nil?
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
          return cel_type_error if arithmetic_type_error?("div", numerator, denominator)
          return cel_type_error if int_beside_non_int?("div", numerator, denominator)
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
