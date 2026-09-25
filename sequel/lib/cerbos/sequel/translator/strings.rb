# frozen_string_literal: true

module Cerbos
  module Sequel
    class Translator
      # +contains+, +startsWith+ and +endsWith+, and the checks for a string operand. See
      # {StringMatcher} for the LIKE escaping.
      module Strings
        private

        def string_match(operator, receiver, needle)
          reject_collection(operator, receiver)
          reject_collection(operator, needle)
          return cel_type_error if known_non_string?(receiver) || known_non_string?(needle)

          require_string_operand(operator, receiver)
          require_string_operand(operator, needle)
          matcher.match(receiver, needle, **STRING_MATCHES.fetch(operator))
        end

        # A value CEL certainly holds as a number or a boolean: a constant, a numeric or boolean
        # column, or a computed int, double or boolean. `contains`, `startsWith`, `endsWith` and
        # `size()` have no overload for either, so CEL raises a no-such-overload error on every
        # row, which is decided by the declared type and not by the row's value. A temporal
        # column is NOT one of these: its attribute is an RFC-3339 string in CEL, and its SQL
        # text is a different spelling, so it stays refused below.
        def known_non_string?(value)
          return true if value.is_a?(Numeric) || value == true || value == false
          return false unless SqlSupport.sql_node?(value)

          type = column_type(value)
          NUMERIC_COLUMN_TYPES.include?(type) || type == :boolean ||
            %i[int double bool].include?(cel_type(value))
        end

        # The value of an expression that is a CEL error on every row: SQL NULL, a fresh node
        # each time so nothing recorded against it by identity leaks to another use. UNKNOWN
        # denies under both polarities, as the error does: `NOT NULL` is NULL, `NULL OR TRUE` is
        # TRUE as `error || true` is, and `NULL AND FALSE` is FALSE as `error && false` is.
        def cel_type_error
          ::Sequel::SQL::Constant.new(:NULL)
        end

        def require_string_operand(operator, value)
          type = column_type(value)
          return if value.is_a?(::String) || (SqlSupport.sql_node?(value) && (type.nil? || STRING_COLUMN_TYPES.include?(type)))

          raise UnsupportedOperatorError,
            "#{operator} requires a string operand, got #{type || describe(value)}"
        end

        def string_valued?(value)
          value.is_a?(::String) || STRING_COLUMN_TYPES.include?(column_type(value))
        end
      end
    end
  end
end
