# frozen_string_literal: true

module Cerbos
  module ActiveRecord
    class Translator
      # `int()`, `double()`, `string()` and `timestamp()`.
      #
      # CEL casts exactly or errors (and the row is denied); SQL casts what it can. Each cast
      # is translated only where the two agree.
      #
      # @private
      module Casts
        # How CEL spells the doubles that SQL cannot hold.
        NON_FINITE_SPELLINGS = %w[NaN +Inf -Inf].freeze

        private

        # Only an integer column is safe, where the cast is a no-op.
        # - String: `int("1junk")` errors in CEL, but SQLite's CAST gives 1.
        # - Double: CEL truncates toward zero; PostgreSQL and MySQL round.
        # Each case has its own message because the corpus pins each one.
        def cast_to_int(value)
          return value.to_i if value.is_a?(Numeric)

          type = column_type(value)
          return record_cel_type(value, :int) if INTEGER_COLUMN_TYPES.include?(type)

          if NUMERIC_COLUMN_TYPES.include?(type)
            raise UnsupportedOperatorError,
              "int() applied to a double column is not portable: CEL removes the fraction " \
              "toward zero, and PostgreSQL and MySQL round a CAST to the nearest whole number " \
              "instead, so the two disagree for every value with a fraction of one half or " \
              "more. Give an operator override that removes the fraction the way your database " \
              "does it."
          end

          raise UnsupportedOperatorError,
            "int() applied to a #{type.inspect} column: CEL reads the WHOLE " \
            "string or makes an error, and Cerbos then denies the row, but SQL reads the digits " \
            "at the front and gives a number, so the filter would keep the row. Compare the " \
            "column directly, or give an operator override."
        end

        # Numeric columns only: `double("abc")` errors in CEL, but SQL gives 0.0.
        def cast_to_double(value)
          return value.to_f if value.is_a?(Numeric)
          return as_double(value) if NUMERIC_COLUMN_TYPES.include?(column_type(value))

          raise UnsupportedOperatorError,
            "double() needs a numeric column. CEL makes an error for a string that is not a " \
            "number, and Cerbos denies the row, but SQL gives 0.0 and the filter would keep it. " \
            "Compare the column directly, or give an operator override."
        end

        # `CAST(x AS TEXT)` matches CEL except for booleans and doubles. SQLite and MySQL give
        # "1" for a boolean, not "true", so anything CEL holds as a boolean goes through
        # {#boolean_to_string}: a boolean column, and every comparison, logical operator,
        # membership test or string predicate (see {#boolean_value?}). A double is held as a
        # {Values::DoubleText} until a comparison resolves it: see {#compare_double_text}.
        def cast_to_string(value)
          return value.to_s if value == true || value == false
          return boolean_to_string(value) if boolean_value?(value)
          if cel_type(value) == :ambiguous_number
            raise UnsupportedOperatorError,
              "string() over a ternary of whole-number constants: CEL spells the int 1000000 as " \
              "\"1000000\" and the double 1000000.0 as \"1e+06\", and the query plan carries both " \
              "as the same number, so the spelling cannot be known. Put int() or double() on one " \
              "arm, or compare the ternary's value instead of its string."
          end
          return Values::DoubleText.new(value) if cel_double?(value)

          cast(value, dialect.text_type)
        end

        # A resolved Arel node that CEL holds as a boolean: a boolean column, or the result of
        # an operator that returns one (recorded by {Translator#evaluate_expression}).
        def boolean_value?(value)
          ArelSupport.arel_node?(value) &&
            (column_type(value) == :boolean || cel_type(value) == :bool)
        end

        # A column or a computed value that CEL holds as a double. Every number in a request
        # attribute is a double, whatever the column holds; only `int()` makes it an int.
        def cel_double?(value)
          return false unless ArelSupport.arel_node?(value)
          return false if cel_type(value) == :int

          NUMERIC_COLUMN_TYPES.include?(column_type(value)) || cel_type(value) == :double
        end

        # `string(x) == "literal"` over a double. SQL spells a double its own way ("2.0",
        # "1000000.0", "-9.5E18") where CEL writes "2", "1e+06" and "-9.5e+18", so the text is
        # never compared. The literal is read back as the one double CEL spells that way, if
        # any, and the column is compared with that number.
        def compare_double_text(operator, left, right)
          text, other = left.is_a?(Values::DoubleText) ? [left, right] : [right, left]
          reject_double_text(operator, text) unless %w[eq ne].include?(operator) && other.is_a?(::String)
          if NON_FINITE_SPELLINGS.include?(other)
            raise UnsupportedOperatorError,
              "string() over a double compared with #{other.inspect}: the adapter does not " \
              "bind a NaN or an Infinity into SQL"
          end

          number = Float(other, exception: false)
          unless number&.finite? && cel_double_spelling(number) == other
            # No double prints as this text. A missing attribute stays an error.
            return unknown_if_any([ArelSupport.is_null(text.value)], operator == "ne")
          end

          if number.zero?
            raise UnsupportedOperatorError,
              "string() over a double compared with #{other.inspect}: CEL prints 0.0 as \"0\" " \
              "and -0.0 as \"-0\", and SQL cannot tell -0.0 from 0.0"
          end

          ArelSupport.comparison(operator, text.value, number)
        end

        def reject_double_text(operator, value)
          return unless value.is_a?(Values::DoubleText)

          raise UnsupportedOperatorError,
            "#{operator} cannot take string() of a double: SQL spells a double differently " \
            "from CEL (\"2.0\" where CEL writes \"2\", \"1000000.0\" where it writes " \
            "\"1e+06\"), so only eq and ne against a string literal are translated"
        end

        # How CEL (Go's `strconv.FormatFloat(f, 'g', -1, 64)`) spells a finite double: the
        # shortest digits that round-trip, in exponent form when the exponent is below -4 or
        # at least 6.
        def cel_double_spelling(number)
          return ((1.0 / number).negative? ? "-0" : "0") if number.zero?

          match = number.abs.to_s.match(/\A(\d+)\.(\d+)(?:e([+-]\d+))?\z/)
          digits = match[1] + match[2]
          point = match[1].length + match[3].to_i
          stripped = digits.sub(/\A0+/, "")
          point -= digits.length - stripped.length
          digits = stripped.sub(/0+\z/, "")
          exponent = point - 1
          sign = number.negative? ? "-" : ""

          if exponent < -4 || exponent >= 6
            mantissa = (digits.length > 1) ? "#{digits[0]}.#{digits[1..]}" : digits
            return format("%s%se%s%02d", sign, mantissa, (exponent < 0) ? "-" : "+", exponent.abs)
          end

          sign + if point <= 0
            "0.#{"0" * -point}#{digits}"
          elsif point >= digits.length
            digits + "0" * (point - digits.length)
          else
            "#{digits[0, point]}.#{digits[point..]}"
          end
        end

        # `string()` over a boolean, portable across dialects (#418, #471):
        #
        #   CASE WHEN b THEN 'true' WHEN NOT (b) THEN 'false' END
        #
        # No ELSE, as in {Translator#branches}: CEL errors on a null or a missing attribute, so
        # the row must stay out under both polarities, and an UNKNOWN `b` matches neither WHEN.
        # An ELSE would make it "false", and `string(x) != "true"` would return a denied row.
        # `b` is used as a condition rather than tested with `IS NULL`: PostgreSQL binds
        # `IS NULL` tighter than `>`, so `a > 3 IS NULL` would not parse as meant.
        #
        # The result is recorded as a string column. MySQL compares the literals in the
        # connection collation (README, "The collation is part of the contract").
        def boolean_to_string(value)
          text = ArelSupport.case_node(
            [[value, "true"], [ArelSupport.not_node(value), "false"]]
          )
          record_column_type(text, :string)
        end

        # A value as an IEEE-754 double: a Ruby Float for a constant, and a CAST for a column.
        def as_double(value)
          return value.to_f if value.is_a?(Numeric)

          cast(value, dialect.double_type)
        end

        def cast(value, type)
          return value if value.nil?

          Arel::Nodes::NamedFunction.new(
            "CAST", [Arel::Nodes::As.new(ArelSupport.quote(value), Arel.sql(type))]
          )
        end

        def timestamp(value)
          return value if value.is_a?(::Time)
          return Timestamps.parse(value) if value.is_a?(::String)

          unless ArelSupport.arel_node?(value)
            raise UnsupportedOperatorError,
              "timestamp() needs an RFC-3339 literal or a temporal column, got #{describe(value)}"
          end

          type = column_type(value)
          if TEMPORAL_COLUMN_TYPES.include?(type)
            @timestamp_operands[value] = true
            return value
          end

          # A string column would compare as text: ActiveRecord writes `2025-01-01 00:00:00`
          # but the column holds `2025-01-01T00:00:00Z`. Refuse.
          raise UnsupportedOperatorError,
            "timestamp() applied to a #{type.inspect} column: this adapter compares instants " \
            "using the database's own temporal type, so the attribute must map to a datetime " \
            "column rather than to a column holding a formatted timestamp string"
        end

        # Records that CEL holds a computed value as an `:int` or a `:double`. Columns carry
        # only their storage type, which CEL never sees.
        def record_cel_type(node, type)
          @cel_types[node] = type
          node
        end

        def cel_type(node)
          @cel_types[node]
        end

        # True if this column went through `timestamp()`.
        # See {Comparisons#assert_timestamp_wrapped}.
        def timestamp_operand?(value)
          @timestamp_operands.key?(value)
        end
      end
    end
  end
end
