# frozen_string_literal: true

require_relative "errors"

module Cerbos
  module MongoDB
    # CEL patterns are RE2; MongoDB's +$regex+ is PCRE2. Only a common subset reads the same in
    # both, and anything outside it is refused rather than guessed at.
    module Regex
      METACHARACTERS = /[.*+?^${}()|\[\]\\]/
      ESCAPABLE = "\\.^$*+?()[]{}|".chars.freeze
      UNSUPPORTED = "()[]{}|".chars.freeze

      module_function

      # Escapes every regex metacharacter so the value matches itself literally.
      def escape(value)
        value.gsub(METACHARACTERS) { |character| "\\#{character}" }
      end

      # Rewrites a CEL (RE2) pattern into the subset RE2 and PCRE2 read identically: literal
      # escapes, +^+ only at the start, +$+ only at the end (as +\z+, because PCRE2's +$+ also
      # matches before a final newline), and the +*+/+++/+?+ quantifiers.
      def normalise_re2(pattern)
        normalised = +""
        can_quantify = false
        index = 0
        while index < pattern.length
          character = pattern[index]
          if character == "\\"
            escaped = pattern[index + 1]
            unless escaped && ESCAPABLE.include?(escaped)
              raise UnsupportedError, "matches supports only literal escapes in the common RE2/PCRE2 subset"
            end

            normalised << "\\" << escaped
            can_quantify = true
            index += 2
            next
          end

          case character
          when "^"
            raise UnsupportedError, "matches supports ^ only at the start of the pattern" unless index.zero?

            normalised << character
            can_quantify = false
          when "$"
            unless index == pattern.length - 1
              raise UnsupportedError, "matches supports $ only at the end of the pattern"
            end

            normalised << "\\z"
            can_quantify = false
          when "*", "+", "?"
            raise UnsupportedError, "matches has an invalid #{character} quantifier" unless can_quantify

            normalised << character
            can_quantify = false
          else
            if UNSUPPORTED.include?(character) || character.ord < 0x20
              raise UnsupportedError, "matches pattern is outside the supported common RE2/PCRE2 subset"
            end

            normalised << character
            can_quantify = true
          end
          index += 1
        end
        normalised
      end
    end
  end
end
