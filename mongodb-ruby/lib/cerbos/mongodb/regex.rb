# frozen_string_literal: true

require_relative "errors"

module Cerbos
  module MongoDB
    # CEL patterns are RE2; MongoDB's $regex is PCRE2 (with UTF, without UCP). This module parses
    # an RE2 pattern and writes the PCRE2 pattern that matches exactly the same strings, or says
    # the pattern is invalid RE2 (which CEL raises on at evaluation), or refuses.
    #
    # Where the two dialects differ, the PCRE2 spelling is explicit: `$` is `\z` (PCRE2's `$` also
    # matches before a final newline), `.` is `[^\n]`, `\s` is RE2's `[\t\n\f\r ]` (PCRE2's also
    # holds the vertical tab), `\v` and `\a` are code points (PCRE2's `\v` is a class), a POSIX
    # class is its ASCII set, a class member is written as a code point, and a brace that is not a
    # repetition is a literal brace. `\d`, `\w` and `\b` are ASCII in both, and `.` reads one code
    # point in both. A case-insensitive pattern is translated only when every character it names
    # is ASCII: RE2 folds `k` with the Kelvin sign and `s` with the long s, which PCRE2 does too,
    # and no other ASCII letter has a non-ASCII fold.
    module Regex
      METACHARACTERS = /[.*+?^${}()|\[\]\\]/

      # The RE2 pattern does not compile: CEL raises where it is evaluated.
      class Invalid < StandardError; end

      POSIX = {
        "alnum" => "0-9A-Za-z", "alpha" => "A-Za-z", "ascii" => "\\x{0}-\\x{7F}", "blank" => "\\t ",
        "cntrl" => "\\x{0}-\\x{1F}\\x{7F}", "digit" => "0-9", "graph" => "!-~", "lower" => "a-z",
        "print" => " -~", "punct" => "!-/:-@\\[-`{-~", "space" => "\\t\\n\\x{0B}\\f\\r ", "upper" => "A-Z",
        "word" => "0-9A-Za-z_", "xdigit" => "0-9A-Fa-f"
      }.freeze

      # RE2's limit on a repetition count.
      MAX_REPEAT = 1000

      # Beyond these, RE2 may reject a pattern as too large or too deep where PCRE2 compiles it,
      # so the adapter refuses rather than decide which.
      MAX_LENGTH = 1000
      MAX_DEPTH = 100

      module_function

      # Escapes every regex metacharacter so the value matches itself literally.
      def escape(value)
        value.gsub(METACHARACTERS) { |character| "\\#{character}" }
      end

      # @return [String] the PCRE2 pattern
      # @raise [Invalid] if the pattern is not valid RE2
      # @raise [UnsupportedError] if the pattern is valid RE2 outside what this translates
      def translate(pattern)
        Parser.new(pattern).translate
      end

      # The pattern, or nil when it is invalid RE2 (so CEL raises).
      def translate_or_invalid(pattern)
        translate(pattern)
      rescue Invalid
        nil
      end

      # Kept for callers that only accept a translatable pattern.
      def normalise_re2(pattern)
        translate(pattern)
      rescue Invalid
        raise UnsupportedError, "matches pattern is not valid RE2"
      end

      # A recursive-descent reader of RE2 syntax.
      class Parser
        def initialize(pattern)
          @chars = pattern.chars
          @index = 0
          @caseless = false
        end

        def translate
          unsupported("a pattern longer than #{MAX_LENGTH} characters") if @chars.length > MAX_LENGTH
          @depth = 0
          @counted = 0
          if @chars.first(4).join == "(?i)"
            @caseless = true
            @index = 4
          end
          body = alternation
          raise Invalid, "unexpected )" if @index < @chars.length

          @caseless ? "(?i)#{body}" : body
        end

        private

        def peek(offset = 0) = @chars[@index + offset]

        def find_from(character, start) = (start...@chars.length).find { |position| @chars[position] == character }

        def unsupported(what) = raise(UnsupportedError, "matches pattern uses #{what}, outside what the adapter translates from RE2 to PCRE2")

        def alternation
          branches = [concatenation]
          while peek == "|"
            @index += 1
            branches << concatenation
          end
          branches.join("|")
        end

        def concatenation
          out = +""
          loop do
            character = peek
            break if character.nil? || character == "|" || character == ")"

            counted_before = @counted
            atom, quantifiable = self.atom
            out << atom
            out << quantifiers(quantifiable, @counted > counted_before)
          end
          out
        end

        # [PCRE2 text, whether a repetition may follow]
        def atom
          character = peek
          case character
          when "(" then group
          when "[" then [character_class, true]
          when "." then @index += 1
                        ["[^\\n]", true]
          when "^" then @index += 1
                        ["^", false]
          when "$" then @index += 1
                        ["\\z", false]
          when "\\" then escape_atom
          when "*", "+", "?" then raise Invalid, "missing argument to repetition operator"
          when "{"
            raise Invalid, "missing argument to repetition operator" if repeat_at(@index)

            @index += 1
            ["\\{", true]
          else
            @index += 1
            [literal(character), true]
          end
        end

        def group
          @index += 1
          @depth += 1
          unsupported("groups nested more than #{MAX_DEPTH} deep") if @depth > MAX_DEPTH
          if peek == "?"
            if peek(1) == ":"
              @index += 2
            elsif ["=", "!"].include?(peek(1)) || (peek(1) == "<" && ["=", "!"].include?(peek(2)))
              raise Invalid, "invalid or unsupported Perl syntax"
            else
              unsupported("a group flag or a named group")
            end
          end
          inner = alternation
          raise Invalid, "missing closing )" unless peek == ")"

          @index += 1
          @depth -= 1
          ["(?:#{inner})", true]
        end

        def escape_atom
          escaped = peek(1)
          raise Invalid, "trailing backslash" if escaped.nil?

          @index += 2
          case escaped
          when "d", "D", "w", "W" then ["\\#{escaped}", true]
          when "s" then ["[\\t\\n\\f\\r ]", true]
          when "S" then ["[^\\t\\n\\f\\r ]", true]
          when "b", "B" then ["\\#{escaped}", false]
          when "A" then ["\\A", false]
          when "z" then ["\\z", false]
          when "t" then ["\\t", true]
          when "n" then ["\\n", true]
          when "r" then ["\\r", true]
          when "f" then ["\\f", true]
          when "v" then ["\\x{0B}", true]
          when "a" then ["\\x{07}", true]
          when "1".."9" then raise Invalid, "invalid escape sequence"
          when "x" then [code_point(hex_escape), true]
          else
            unsupported("the escape \\#{escaped}") if escaped.match?(/[[:alnum:]]/) || escaped.ord >= 0x80

            [literal(escaped), true]
          end
        end

        # \xHH or \x{H...}, after the `x`.
        def hex_escape
          if peek == "{"
            closing = find_from("}", @index)
            raise Invalid, "invalid escape sequence" if closing.nil?

            digits = @chars[(@index + 1)...closing].join
            @index = closing + 1
          else
            digits = @chars[@index, 2].join
            @index += 2
          end
          raise Invalid, "invalid escape sequence" unless digits.match?(/\A\h{1,8}\z/)

          value = digits.to_i(16)
          raise Invalid, "invalid escape sequence" if value > 0x10FFFF
          unsupported("a surrogate code point") if value.between?(0xD800, 0xDFFF)

          value
        end

        def literal(character)
          assert_caseless_ascii(character.ord)
          return "\\#{character}" if character.match?(METACHARACTERS)
          return code_point(character.ord) if character.ord < 0x20 || character.ord == 0x7F

          character
        end

        def code_point(value)
          assert_caseless_ascii(value)
          "\\x{#{value.to_s(16).upcase}}"
        end

        def assert_caseless_ascii(value)
          unsupported("case-insensitive matching of a non-ASCII character") if @caseless && value >= 0x80
        end

        # Zero or one repetition, then an optional lazy `?`. A second repetition is invalid RE2.
        def quantifiers(quantifiable, holds_counted)
          text = repetition
          return "" if text.nil?

          unsupported("a repetition of an assertion") unless quantifiable
          # RE2 bounds the product of nested counts; refuse rather than reproduce the bound.
          if text.start_with?("{") && holds_counted
            unsupported("a counted repetition nested in a counted repetition")
          end
          if peek == "?"
            @index += 1
            text += "?"
          end
          raise Invalid, "invalid nested repetition operator" if ["*", "+", "?"].include?(peek) || repeat_at(@index)

          text
        end

        def repetition
          character = peek
          if ["*", "+", "?"].include?(character)
            @index += 1
            return character
          end
          repeat = repeat_at(@index)
          return nil if repeat.nil?

          minimum, maximum, length = repeat
          if minimum > MAX_REPEAT || (maximum && (maximum > MAX_REPEAT || maximum < minimum))
            raise Invalid, "invalid repeat count"
          end

          @index += length
          @counted += 1
          if maximum.nil?
            "{#{minimum},}"
          else
            (maximum == minimum) ? "{#{minimum}}" : "{#{minimum},#{maximum}}"
          end
        end

        # [min, max or nil for open, length] for a `{n}`, `{n,}` or `{n,m}` at +index+, else nil.
        # RE2 reads any other brace as a literal.
        def repeat_at(index)
          rest = @chars[index..].join
          match = /\A\{(\d+)(,(\d*))?\}/.match(rest)
          return nil if match.nil?

          minimum = match[1].to_i
          maximum = if match[2].nil?
            minimum
          else
            match[3].empty? ? nil : match[3].to_i
          end
          [minimum, maximum, match[0].length]
        end

        def character_class
          @index += 1
          negated = false
          if peek == "^"
            negated = true
            @index += 1
          end
          items = +""
          first = true
          loop do
            character = peek
            raise Invalid, "missing closing ]" if character.nil?
            break if character == "]" && !first

            first = false
            if character == "[" && peek(1) == ":"
              items << posix_class
              next
            end
            low = class_member
            if low.is_a?(Integer) && peek == "-" && peek(1) && peek(1) != "]"
              @index += 1
              high = class_member
              raise Invalid, "invalid character class range" unless high.is_a?(Integer)
              raise Invalid, "invalid character class range" if high < low

              items << "#{code_point(low)}-#{code_point(high)}"
            elsif low.is_a?(Integer)
              items << code_point(low)
            else
              items << low
            end
          end
          @index += 1
          negated ? "[^#{items}]" : "[#{items}]"
        end

        # A code point, or the PCRE2 text of an escaped class (`\d`).
        def class_member
          character = peek
          @index += 1
          return character.ord unless character == "\\"

          escaped = peek
          raise Invalid, "missing closing ]" if escaped.nil?

          @index += 1
          case escaped
          when "d", "w", "D", "W" then "\\#{escaped}"
          when "s" then "\\t\\n\\f\\r "
          when "t" then 0x09
          when "n" then 0x0A
          when "r" then 0x0D
          when "f" then 0x0C
          when "v" then 0x0B
          when "a" then 0x07
          when "x" then hex_escape
          when "1".."9" then raise Invalid, "invalid escape sequence"
          else
            unsupported("the escape \\#{escaped} in a class") if escaped.match?(/[[:alnum:]]/) || escaped.ord >= 0x80

            escaped.ord
          end
        end

        def posix_class
          closing = find_from("]", @index + 2)
          raise Invalid, "missing closing ]" if closing.nil? || @chars[closing - 1] != ":"

          name = @chars[(@index + 2)...(closing - 1)].join
          set = POSIX[name]
          raise Invalid, "invalid character class range" if set.nil? && name.match?(/\A[a-z]+\z/)

          unsupported("the POSIX class [:#{name}:]") if set.nil?
          @index = closing + 1
          set
        end
      end
    end
  end
end
