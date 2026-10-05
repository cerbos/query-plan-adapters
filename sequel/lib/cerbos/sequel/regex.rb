# frozen_string_literal: true

require_relative "errors"

module Cerbos
  module Sequel
    # CEL's `matches()` without a SQL regex engine.
    #
    # CEL matches with RE2 (Go's `regexp`), and no store's regex dialect is RE2: MySQL's ICU engine
    # lets `$` match before a trailing newline, PostgreSQL's and ICU's classes and flags differ
    # from RE2's, and SQLite has no regex operator at all. So a pattern is never handed to the
    # store. It is parsed here, and lowered only when what it matches can be said with the
    # adapter's exact string predicates: equality, `startsWith`, `endsWith` and `contains` over a
    # FINITE set of literals, plus two infinite forms with an exact reading — every character
    # drawn from a small set (`^[ab]+$`), and a prefix and a suffix around a run of non-newline
    # characters (`^a.*b$`).
    #
    # {.compile} returns one plan per top-level alternative, which the caller ORs together;
    # `:error` for a pattern RE2 rejects (CEL raises at evaluation, which denies under both
    # polarities); and raises {UnsupportedOperatorError} for anything else. The same lowering, on
    # the same three stores, is drizzle's (drizzle/src/regex.ts).
    #
    # @api private
    module Regex
      # The most literals one pattern may expand to before it is refused rather than enumerated.
      MAX_LITERALS = 256
      # The most characters a set may hold for the every-character-in-a-set form.
      MAX_SET_SIZE = 64
      # RE2's own bound on a counted repetition.
      MAX_REPEAT = 1000

      # The string is one of +literals+ (`:equals`), or some literal is a prefix, suffix or
      # substring of it (`:starts_with`, `:ends_with`, `:contains`).
      Literals = Struct.new(:kind, :literals)
      # Every character of the string is in +characters+, and there are at least +min+.
      AllCharactersIn = Struct.new(:characters, :min)
      # No character is a newline, and there are at least +min+ of them.
      NoNewline = Struct.new(:min)
      # For some pair, the string starts with the prefix, ends with the suffix, holds at least
      # `prefix + min + suffix` characters, and contains no newline.
      PrefixSuffix = Struct.new(:pairs, :min)

      CharSet = Struct.new(:characters)
      Dot = Struct.new(:unused)
      Group = Struct.new(:alternatives)
      Repeat = Struct.new(:node, :min, :max)
      Begin = Struct.new(:unused)
      End = Struct.new(:unused)

      # A pattern RE2 itself rejects.
      class InvalidPattern < StandardError; end

      def self.range(from, to) = (from.ord..to.ord).map { |code| code.chr(Encoding::UTF_8) }

      DIGITS = range("0", "9").freeze
      LOWER = range("a", "z").freeze
      UPPER = range("A", "Z").freeze
      WORD = (DIGITS + UPPER + LOWER + ["_"]).freeze
      SPACE = ["\t", "\n", "\f", "\r", " "].freeze

      # RE2's ASCII POSIX classes, `[[:name:]]`.
      POSIX_CLASSES = {
        "alnum" => DIGITS + UPPER + LOWER,
        "alpha" => UPPER + LOWER,
        "blank" => [" ", "\t"],
        "digit" => DIGITS,
        "lower" => LOWER,
        "space" => SPACE + ["\v"],
        "upper" => UPPER,
        "word" => WORD,
        "xdigit" => DIGITS + range("A", "F") + range("a", "f")
      }.freeze

      # Perl classes RE2 accepts, outside and inside brackets. ASCII only in RE2.
      PERL_CLASSES = {"d" => DIGITS, "s" => SPACE, "w" => WORD}.freeze

      LITERAL_ESCAPES = {"a" => "\a", "f" => "\f", "n" => "\n", "r" => "\r", "t" => "\t", "v" => "\v"}.freeze

      # ASCII punctuation, which RE2 reads escaped as itself.
      PUNCTUATION = /\A[!-\/:-@\[-`{-~]\z/

      module_function

      # @return [Array, Symbol] the plans to OR together, or `:error`
      def compile(pattern)
        alternatives =
          begin
            Parser.new(pattern).parse
          rescue InvalidPattern
            return :error
          end
        alternatives.filter_map { |alternative|
          plan = plan_alternative(pattern, alternative)
          plan unless plan == :never
        }
      end

      def unsupported(pattern, why)
        UnsupportedOperatorError.new(
          "Cannot translate matches(#{pattern.inspect}): #{why}. The adapter lowers only " \
          "patterns whose matches are a finite set of literals under anchors, every character " \
          "from a small set, or a prefix and suffix around non-newline characters, because no " \
          "store's regex dialect is RE2"
        )
      end

      # Every character RE2's `(?i)` treats as the same as +character+. Its case folding follows
      # Unicode simple folding, so beyond the two ASCII cases `k` also matches KELVIN SIGN and `s`
      # matches LONG S. A non-ASCII letter's orbit is not tabulated here, so it is refused.
      def fold_case(character, pattern)
        return ["k", "K", "K"] if %w[k K].include?(character)
        return ["s", "S", "ſ"] if %w[s S].include?(character)
        return [character.downcase, character.upcase] if character.match?(/\A[a-zA-Z]\z/)
        if character.ord > 0x7f && character.downcase != character.upcase
          raise unsupported(pattern, "case-insensitive matching of '#{character}' is not tabulated")
        end

        [character]
      end

      # A recursive-descent parser for the RE2 syntax the lowering can use.
      class Parser
        def initialize(pattern)
          @pattern = pattern
          @characters = pattern.chars
          @position = 0
          @case_insensitive = false
        end

        def parse
          if @pattern.start_with?("(?i)")
            @case_insensitive = true
            @position = 4
          end
          alternatives = alternation
          # Only an unbalanced `)` stops the top level early.
          raise InvalidPattern if @position < @characters.length

          alternatives
        end

        private

        def peek(offset = 0) = @characters[@position + offset]

        def rest = @characters[@position..].join

        def alternation
          alternatives = [sequence]
          while peek == "|"
            @position += 1
            alternatives << sequence
          end
          alternatives
        end

        def sequence
          nodes = []
          loop do
            following = peek
            return nodes if following.nil? || following == "|" || following == ")"

            nodes << quantified(atom)
          end
        end

        def quantified(atom)
          node = atom
          loop do
            following = peek
            bounds =
              case following
              when "*" then [0, nil]
              when "+" then [1, nil]
              when "?" then [0, 1]
              when "{" then counted_repeat
              end
            return node if bounds.nil?

            @position += 1 unless following == "{"
            # RE2 rejects a repeated repetition (`a**`, "invalid nested repetition operator").
            raise InvalidPattern if node.is_a?(Repeat)
            if node.is_a?(Begin) || node.is_a?(End)
              raise Regex.unsupported(@pattern, "a repeated anchor is not read")
            end

            # A trailing `?` makes the repetition lazy, which changes WHERE it matches, not WHETHER.
            @position += 1 if peek == "?"
            node = Repeat.new(node, bounds[0], bounds[1])
          end
        end

        # `{n}`, `{n,}` or `{n,m}`; anything else starting with `{` is a literal brace in RE2.
        def counted_repeat
          match = /\A\{(\d+)(,(\d*))?\}/.match(rest)
          return nil if match.nil?

          min = match[1].to_i
          max =
            if match[2].nil? then min
            elsif match[3].empty? then nil
            else match[3].to_i
            end
          raise InvalidPattern if min > MAX_REPEAT || (!max.nil? && (max > MAX_REPEAT || max < min))

          @position += match[0].length
          [min, max]
        end

        def literal(character)
          CharSet.new(@case_insensitive ? Regex.fold_case(character, @pattern) : [character])
        end

        def atom
          character = peek
          @position += 1
          case character
          when "^" then Begin.new
          when "$" then End.new
          when "." then Dot.new
          when "(" then group
          when "[" then CharSet.new(character_class)
          when "\\" then escape
          # "missing argument to repetition operator"
          when "*", "+", "?" then raise InvalidPattern
          when "{"
            # A brace that opens a valid count with nothing before it is the same error; any
            # other brace is a literal in RE2.
            @position -= 1
            raise InvalidPattern unless counted_repeat.nil?

            @position += 1
            literal(character)
          else literal(character)
          end
        end

        def group
          if peek == "?"
            # Lookaround is not RE2 syntax: `regexp.Compile` rejects it, so CEL raises.
            raise InvalidPattern if rest.match?(/\A\?(=|!|<=|<!)/)
            raise Regex.unsupported(@pattern, "only (?i) at the start and (?:...) groups are read") unless rest.start_with?("?:")

            @position += 2
          end
          alternatives = alternation
          raise InvalidPattern unless peek == ")" # "missing closing )"

          @position += 1
          Group.new(alternatives)
        end

        def escape
          character = peek
          raise InvalidPattern if character.nil? # "trailing backslash"

          @position += 1
          return Begin.new if character == "A"
          return End.new if character == "z"
          return CharSet.new(PERL_CLASSES[character]) if PERL_CLASSES.key?(character)
          return literal(LITERAL_ESCAPES[character]) if LITERAL_ESCAPES.key?(character)
          return literal(character) if character.match?(PUNCTUATION)
          # A lone \1-\7 would be a backreference and \8, \9 are nothing: RE2 rejects both, and
          # reads \1-\7 before another octal digit as an octal escape, which is not read here.
          if character.match?(/\A[89]\z/) || (character.match?(/\A[1-7]\z/) && !peek.to_s.match?(/\A[0-7]\z/))
            raise InvalidPattern # "invalid escape sequence"
          end

          raise Regex.unsupported(@pattern, "the escape \\#{character} is not read")
        end

        # `[...]`, just after its `[`.
        def character_class
          if peek == "^"
            raise Regex.unsupported(@pattern, "a negated class matches an unbounded set of characters")
          end

          members = []
          first = true
          loop do
            character = peek
            raise InvalidPattern if character.nil? # "missing closing ]"

            if character == "]" && !first
              @position += 1
              break
            end
            first = false
            if character == "[" && peek(1) == ":"
              match = /\A\[:([a-z]+):\]/.match(rest)
              posix = match && POSIX_CLASSES[match[1]]
              raise Regex.unsupported(@pattern, "only ASCII POSIX classes are read") if posix.nil?

              members.concat(posix)
              @position += match[0].length
              next
            end
            start = class_character
            if start.is_a?(Array)
              members.concat(start)
              next
            end
            if peek == "-" && peek(1) != "]" && !peek(1).nil?
              @position += 1
              finish = class_character
              raise InvalidPattern if finish.is_a?(Array) || finish.ord < start.ord

              span = Regex.range(start, finish)
              raise Regex.unsupported(@pattern, "a class range is too wide to enumerate") if span.length > MAX_SET_SIZE * 4

              members.concat(span)
            else
              members << start
            end
          end
          folded = @case_insensitive ? members.flat_map { |member| Regex.fold_case(member, @pattern) } : members
          folded.uniq
        end

        # One class member: a character, or a Perl class's characters.
        def class_character
          character = peek
          @position += 1
          return character unless character == "\\"

          escaped = peek
          raise InvalidPattern if escaped.nil?

          @position += 1
          return PERL_CLASSES[escaped] if PERL_CLASSES.key?(escaped)
          return LITERAL_ESCAPES[escaped] if LITERAL_ESCAPES.key?(escaped)
          return escaped if escaped.match?(PUNCTUATION)

          raise Regex.unsupported(@pattern, "the escape \\#{escaped} is not read")
        end
      end

      # Every string a node sequence matches, when that is a finite set of at most MAX_LITERALS.
      def finite_sequence(nodes)
        strings = [""]
        nodes.each do |node|
          following = finite_node(node)
          return nil if following.nil?

          product = []
          strings.each do |head|
            following.each do |tail|
              product << head + tail
              return nil if product.length > MAX_LITERALS
            end
          end
          strings = product.uniq
        end
        strings
      end

      def finite_node(node)
        case node
        when CharSet then node.characters
        when Group
          union = []
          node.alternatives.each do |alternative|
            strings = finite_sequence(alternative)
            return nil if strings.nil?

            union.concat(strings)
            return nil if union.length > MAX_LITERALS
          end
          union.uniq
        when Repeat
          return nil if node.max.nil?

          union = []
          (node.min..node.max).each do |count|
            strings = finite_sequence(Array.new(count, node.node))
            return nil if strings.nil?

            union.concat(strings)
            return nil if union.length > MAX_LITERALS
          end
          union.uniq
        end
      end

      # One top-level alternative, its anchors removed. An end without an anchor is free: a match
      # may start (or stop) anywhere, so a repetition there needs only its minimum number of
      # copies — `a+b` somewhere is exactly `ab` somewhere — and `.*` there needs none.
      def plan_alternative(pattern, sequence)
        nodes = sequence.dup
        anchored_start = false
        anchored_end = false
        while nodes.first.is_a?(Begin)
          anchored_start = true
          nodes.shift
        end
        while nodes.last.is_a?(End)
          anchored_end = true
          nodes.pop
        end
        if nodes.any? { |node| node.is_a?(Begin) || node.is_a?(End) }
          raise unsupported(pattern, "an anchor inside the pattern is not read")
        end

        at_least = ->(node) { node.is_a?(Repeat) ? Repeat.new(node.node, node.min, node.min) : node }
        unless anchored_start
          nodes.shift while nodes.first.is_a?(Repeat) && nodes.first.min.zero?
          nodes[0] = at_least.call(nodes[0]) unless nodes.empty?
        end
        unless anchored_end
          nodes.pop while nodes.last.is_a?(Repeat) && nodes.last.min.zero?
          nodes[-1] = at_least.call(nodes[-1]) unless nodes.empty?
        end

        literals = finite_sequence(nodes)
        unless literals.nil?
          return :never if literals.empty?

          kind =
            if anchored_start
              anchored_end ? :equals : :starts_with
            else
              anchored_end ? :ends_with : :contains
            end
          return Literals.new(kind, literals)
        end

        if anchored_start && anchored_end
          # `^X*$`, `^X+$`, `^X{n,}$` over a set or `.`.
          only = nodes.first
          if nodes.length == 1 && only.is_a?(Repeat) && only.max.nil?
            return NoNewline.new(only.min) if only.node.is_a?(Dot)
            if only.node.is_a?(CharSet) && only.node.characters.length <= MAX_SET_SIZE
              return AllCharactersIn.new(only.node.characters, only.min)
            end
          end
          # `^P.*S$`: finite prefix and suffix around one unbounded run of `.`.
          runs = nodes.each_index.select { |index|
            node = nodes[index]
            node.is_a?(Repeat) && node.node.is_a?(Dot) && node.max.nil?
          }
          if runs.length == 1
            run = runs.first
            prefixes = finite_sequence(nodes[0...run])
            suffixes = finite_sequence(nodes[(run + 1)..])
            if prefixes && suffixes
              pairs = prefixes.product(suffixes)
              return :never if pairs.empty?
              if pairs.length <= MAX_LITERALS && pairs.none? { |prefix, suffix| (prefix + suffix).include?("\n") }
                return PrefixSuffix.new(pairs, nodes[run].min)
              end
            end
          end
        end
        raise unsupported(pattern, "what it matches is not a finite set of literals")
      end
    end
  end
end
