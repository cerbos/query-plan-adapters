# frozen_string_literal: true

module Cerbos
  module ActiveRecord
    class Translator
      # +contains+, +startsWith+ and +endsWith+, and the checks for a string operand. See
      # {StringMatcher} for the LIKE escaping.
      module Strings
        private

        def string_match(operator, receiver, needle)
          reject_collection(operator, receiver)
          reject_collection(operator, needle)

          require_string_operand(operator, receiver)
          require_string_operand(operator, needle)
          matcher.match(receiver, needle, **STRING_MATCHES.fetch(operator))
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
