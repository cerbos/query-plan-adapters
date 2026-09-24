# frozen_string_literal: true

module Cerbos
  module Sequel
    class Translator
      # +int()+, +double()+, +string()+ and +timestamp()+.
      #
      # CEL converts a value exactly or makes an error, and Cerbos then denies the row. SQL
      # converts what it can. Each cast here is translated only where the two agree.
      module Casts
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
          return value if INTEGER_COLUMN_TYPES.include?(type)

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

        # `CAST(x AS TEXT)` gives what CEL gives for a number and for a string. It does not for a
        # boolean. SQLite and MySQL have no boolean type and keep 1 and 0, so the CAST makes "1"
        # where CEL makes "true", and only PostgreSQL agrees. Thus a boolean column does not go
        # through the CAST. See {#boolean_to_string}.
        def cast_to_string(value)
          return boolean_to_string(value) if column_type(value) == :boolean

          cast(value, dialect.text_type)
        end

        # +string()+ over a boolean column, as a CASE that spells the two words of CEL itself:
        #
        #   CASE WHEN col IS NULL THEN NULL WHEN col THEN 'true' ELSE 'false' END
        #
        # CASE WHEN is standard SQL, and SQLite, MySQL and PostgreSQL each read a boolean column
        # as a condition. Thus one translation is correct on all three dialects, where a CAST is
        # correct on one of them (cerbos/query-plan-adapters#418).
        #
        # The IS NULL arm is necessary. A NULL boolean is a missing attribute, or a null value,
        # and CEL has no +string()+ for either: it makes an error and Cerbos denies the row. A
        # NULL column makes `WHEN col` UNKNOWN, so without that arm the CASE would go to its ELSE
        # and give "false". Then `string(x) != "true"` would give a row that the PDP denies. With
        # the arm the result is NULL, and the row stays out under both polarities.
        #
        # The result is text, and the translator records it as a string. Thus the operators that
        # examine the kind of an operand (a comparison, `+`, a string match, `size()`) treat it as
        # they treat any other string. The two words are literals and not a column, so MySQL
        # compares them in the collation of the connection (see "The collation is part of the
        # contract" in the README).
        def boolean_to_string(column)
          text = SqlSupport.case_node(
            [[SqlSupport.comparison("eq", column, nil), nil], [column, "true"]],
            else_value: "false"
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

        # True if this temporal column went through `timestamp()`, and thus compares as an
        # instant. See {Comparisons#assert_timestamp_wrapped}.
        def timestamp_operand?(value)
          @timestamp_operands.key?(value)
        end
      end
    end
  end
end
