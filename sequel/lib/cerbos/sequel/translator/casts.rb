# frozen_string_literal: true

module Cerbos
  module Sequel
    class Translator
      # +int()+, +double()+, +string()+ and +timestamp()+.
      #
      # CEL converts a value exactly or makes an error, and Cerbos then denies the row. SQL
      # converts what it can. Each cast here is translated only where the two agree.
      module Casts
        # How CEL spells the doubles that SQL cannot hold.
        NON_FINITE_SPELLINGS = %w[NaN +Inf -Inf].freeze

        private

        # CEL reads a whole string or it makes an error: `int("1junk")` is an error and Cerbos
        # denies the row. `CAST('1junk' AS INTEGER)` gives 1 on SQLite, so the filter would give
        # a row that the PDP denies. No portable SQL reads a number the way CEL does.
        #
        # A cast from a double is also not portable, and for a different reason. CEL removes the
        # fraction toward zero, SQLite does the same, but PostgreSQL and MySQL round. Thus only an
        # integer column is safe, and there the cast has nothing to do.
        #
        # Each of the two failures says its own reason. A message that named both would not show
        # which mechanism stopped the translation, and the corpus pins these messages precisely so
        # that a refusal proves the limitation it declares.
        def cast_to_int(value)
          return value.to_i if value.is_a?(Numeric)

          type = column_type(value)
          return record_cel_type(value, :int) if INTEGER_COLUMN_TYPES.include?(type)

          return int_of_double(value) if type == :float

          if NUMERIC_COLUMN_TYPES.include?(type)
            raise UnsupportedOperatorError,
              "int() applied to a #{type.inspect} column: the attribute CEL truncates is the " \
              "double nearest the stored exact value, which can truncate to a different whole " \
              "number than the exact value does. Map a double column, or give an operator " \
              "override."
          end

          raise UnsupportedOperatorError,
            "int() applied to a #{type.inspect} column: CEL reads the WHOLE " \
            "string or makes an error, and Cerbos then denies the row, but SQL reads the digits " \
            "at the front and gives a number, so the filter would keep the row. Compare the " \
            "column directly, or give an operator override."
        end

        # CEL's int() of a double truncates toward zero, and raises when the double is NaN, an
        # Infinity, or outside (-2^63, 2^63) — cel-go's `doubleToInt64Checked` rejects both
        # bounds themselves. The CASE keeps exactly that range and is NULL (the error, UNKNOWN)
        # outside it; a NaN, which only PostgreSQL stores and orders above every number, fails
        # the upper bound. The bounds are compared as doubles: 2^63 is exact in binary64.
        # Only a double column: a decimal's attribute is the double nearest it, which can
        # truncate to a different whole number than the exact decimal does.
        def int_of_double(value)
          bound = cast(2.0**63, dialect.double_type)
          lower = cast(-(2.0**63), dialect.double_type)
          in_range = SqlSupport.and_node([
            SqlSupport.comparison("gt", value, lower), SqlSupport.comparison("lt", value, bound)
          ])
          record_cel_type(SqlSupport.case_node([[in_range, dialect.truncate_to_int(value)]]), :int)
        end

        # The same reason as `int()`: `double("abc")` is an error in CEL and Cerbos denies the
        # row, but SQL gives 0.0 and the filter would keep the row.
        def cast_to_double(value)
          return value.to_f if value.is_a?(Numeric)
          return as_double(value) if NUMERIC_COLUMN_TYPES.include?(column_type(value))

          raise UnsupportedOperatorError,
            "double() needs a numeric column. CEL makes an error for a string that is not a " \
            "number, and Cerbos denies the row, but SQL gives 0.0 and the filter would keep it. " \
            "Compare the column directly, or give an operator override."
        end

        # `CAST(x AS TEXT)` gives what CEL gives for an int and for a string. It does not for a
        # boolean or a double. SQLite and MySQL have no boolean type and keep 1 and 0, so the
        # CAST makes "1" where CEL makes "true": anything CEL holds as a boolean — a boolean
        # column, and every comparison, logical operator, membership test or string predicate
        # (see {#boolean_value?}) — goes through {#boolean_to_string}. A double is held as a
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

        # A resolved expression that CEL holds as a boolean: a boolean column, or the result of
        # an operator that returns one (recorded by {Translator#evaluate_expression}).
        def boolean_value?(value)
          SqlSupport.sql_node?(value) &&
            (column_type(value) == :boolean || cel_type(value) == :bool)
        end

        # A column or a computed value that CEL holds as a double. Every number in a request
        # attribute is a double, whatever the column holds; only `int()` makes it an int.
        def cel_double?(value)
          return false unless SqlSupport.sql_node?(value)
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
            return unknown_if_any([SqlSupport.is_null(text.value)], operator == "ne")
          end

          if number.zero?
            raise UnsupportedOperatorError,
              "string() over a double compared with #{other.inspect}: CEL prints 0.0 as \"0\" " \
              "and -0.0 as \"-0\", and SQL cannot tell -0.0 from 0.0"
          end

          SqlSupport.comparison(operator, text.value, number)
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

        # +string()+ over a boolean, as a CASE that spells the two words of CEL itself:
        #
        #   CASE WHEN b THEN 'true' WHEN NOT (b) THEN 'false' ELSE NULL END
        #
        # CASE WHEN is standard SQL, so one translation is correct on all three dialects, where
        # a CAST is correct on one of them (cerbos/query-plan-adapters#418, #471).
        #
        # No value in the ELSE, as in {Translator#branches}: CEL has no +string()+ for a null or
        # a missing attribute, so it makes an error and the row must stay out under both
        # polarities. An UNKNOWN `b` matches neither WHEN, and the CASE is NULL. A value there
        # would make it "false", and `string(x) != "true"` would give a row that the PDP denies.
        #
        # The result is text, and the translator records it as a string. Thus the operators that
        # examine the kind of an operand (a comparison, `+`, a string match, `size()`) treat it as
        # they treat any other string. The two words are literals and not a column, so MySQL
        # compares them in the collation of the connection (see "The collation is part of the
        # contract" in the README).
        def boolean_to_string(value)
          condition = as_predicate(value)
          text = SqlSupport.case_node(
            [[condition, "true"], [SqlSupport.not_node(condition), "false"]]
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

          ::Sequel.cast(value, type)
        end

        def timestamp(value)
          return value if value.is_a?(::Time)
          return Timestamps.parse(value) if value.is_a?(::String)

          unless SqlSupport.sql_node?(value)
            raise UnsupportedOperatorError,
              "timestamp() needs an RFC-3339 literal or a temporal column, got #{describe(value)}"
          end

          type = column_type(value)
          if TEMPORAL_COLUMN_TYPES.include?(type)
            @timestamp_operands[value] = true
            return value
          end

          # A comparison between a string column and a Time compares two different text
          # formats. Sequel makes `2025-01-01 00:00:00.000000`, but an RFC-3339 column holds
          # `2025-01-01T00:00:00Z`. Thus the order of the results comes from the text and not
          # from the instants. The adapter refuses this shape and does not make the SQL.
          raise UnsupportedOperatorError,
            "timestamp() applied to a #{type.inspect} column: this adapter compares instants " \
            "using the database's own temporal type, so the attribute must map to a datetime " \
            "column rather than to a column holding a formatted timestamp string"
        end

        # Records that CEL holds a computed value as an `:int`, a `:double` or a `:bool`. Columns
        # carry only their storage type, which CEL never sees.
        def record_cel_type(node, type)
          @cel_types[node] = type
          node
        end

        def cel_type(node)
          @cel_types[node]
        end

        # True if this temporal column went through `timestamp()`, and thus compares as an
        # instant. See {Comparisons#assert_timestamp_wrapped}.
        def timestamp_operand?(value)
          @timestamp_operands.key?(value)
        end
      end
    end
  end
end
