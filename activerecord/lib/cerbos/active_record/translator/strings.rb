# frozen_string_literal: true

module Cerbos
  module ActiveRecord
    class Translator
      # `contains`, `startsWith` and `endsWith`, and the checks for a string operand. See
      # {StringMatcher} for the LIKE escaping.
      #
      # @private
      module Strings
        private

        def string_match(operator, receiver, needle)
          reject_collection(operator, receiver)
          reject_collection(operator, needle)

          require_string_operand(operator, receiver)
          require_string_operand(operator, needle)
          matcher.match(receiver, needle, **STRING_MATCHES.fetch(operator))
        end

        # CEL's `upperAscii()` folds only `a`-`z`. SQL `UPPER` follows the database's locale
        # and folds `é` to `É` too, so each ASCII letter is replaced on its own; `REPLACE`
        # matches exactly, whatever the column's collation.
        def upper_ascii(value)
          require_string_operand("upperAscii", value)
          return value.tr("a-z", "A-Z") if value.is_a?(::String)

          folded = ("a".."z").reduce(value) { |text, letter| ArelSupport.function("REPLACE", [text, letter, letter.upcase]) }
          record_cel_type(folded, :string)
        end

        def require_string_operand(operator, value)
          type = column_type(value)
          return if value.is_a?(::String) || (ArelSupport.arel_node?(value) && (type.nil? || STRING_COLUMN_TYPES.include?(type)))

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
