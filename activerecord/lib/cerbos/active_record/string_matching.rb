# frozen_string_literal: true

require_relative "arel_support"

module Cerbos
  module ActiveRecord
    # Translates CEL `contains`, `startsWith` and `endsWith` into `LIKE ... ESCAPE`.
    #
    # The needle is literal in CEL, but `LIKE` treats `%` and `_` as wildcards. Unescaped,
    # `contains("a_b")` would also match `aXb` and return rows the PDP denies.
    #
    # @private
    class StringMatcher
      ESCAPE_CHAR = "\\"

      # Some dialects (e.g. SQL Server) read `[` as a character class, so escape it too.
      METACHARACTERS = [ESCAPE_CHAR, "%", "_", "["].freeze

      def initialize(dialect)
        @dialect = dialect
      end

      attr_reader :dialect

      # @param receiver [Object] the haystack, as in the CEL source. May be a constant.
      # @param needle [Object] the needle. May be a column, to compare two fields.
      # @param prefix [Boolean] permit any characters before the needle
      # @param suffix [Boolean] permit any characters after the needle
      def match(receiver, needle, prefix:, suffix:)
        if needle.is_a?(::String) && needle.include?("\0")
          raise UnsupportedOperatorError,
            "Cannot translate a string match whose needle holds a NUL character: PostgreSQL " \
            "text cannot hold one, and MySQL's LIKE stops reading the pattern at it"
        end
        return sqlite_match(receiver, needle, prefix: prefix, suffix: suffix) if dialect.sqlite?

        pattern =
          if needle.is_a?(::String)
            literal = escape_literal(needle)
            literal = "%#{literal}" if prefix
            literal = "#{literal}%" if suffix
            ArelSupport.quote(literal)
          else
            column_pattern(needle, prefix: prefix, suffix: suffix)
          end

        Arel::Nodes::Matches.new(
          ArelSupport.quote(receiver),
          pattern,
          Arel::Nodes.build_quoted(ESCAPE_CHAR),
          # Case-sensitive, like CEL. False would make PostgreSQL use ILIKE.
          true
        )
      end

      # Escapes each LIKE metacharacter in a literal needle.
      #
      # Use gsub's block form: a String replacement re-reads backslash sequences, so replacing
      # `\` with `\\` would still yield a single `\`.
      def escape_literal(needle)
        METACHARACTERS.reduce(needle) do |escaped, metacharacter|
          escaped.gsub(metacharacter) { ESCAPE_CHAR + metacharacter }
        end
      end

      private

      # SQLite's LIKE reads a string only up to its first NUL. instr() and = compare every byte,
      # and hex() keeps a suffix aligned to whole bytes; each is case-sensitive, as CEL is.
      def sqlite_match(receiver, needle, prefix:, suffix:)
        haystack = ArelSupport.quote(receiver)
        needle = ArelSupport.quote(needle)
        position = ArelSupport.function("instr", [haystack, needle])

        if prefix && suffix
          Arel::Nodes::GreaterThan.new(position, ArelSupport.quote(0))
        elsif suffix
          Arel::Nodes::Equality.new(position, ArelSupport.quote(1))
        elsif prefix
          # hex() reads NULL as '', so NULL is propagated by hand.
          hex = ->(node) { ArelSupport.function("hex", [node]) }
          suffix_match = Arel::Nodes::Matches.new(hex.call(haystack), dialect.concat(ArelSupport.quote("%"), hex.call(needle)), nil, true)
          both_present = Arel::Nodes::And.new([Arel::Nodes::NotEqual.new(haystack, nil), Arel::Nodes::NotEqual.new(needle, nil)])
          ArelSupport.case_node([[both_present, suffix_match]])
        else
          Arel::Nodes::Equality.new(haystack, needle)
        end
      end

      # Escapes LIKE metacharacters in a column needle, in SQL at query time.
      #
      # A NULL needle makes the pattern NULL, so LIKE is unknown and the row is excluded,
      # matching CEL's missing-attribute deny.
      def column_pattern(needle, prefix:, suffix:)
        pattern = METACHARACTERS.reduce(ArelSupport.quote(needle)) do |escaped, metacharacter|
          ArelSupport.function(
            "REPLACE", [escaped, metacharacter, ESCAPE_CHAR + metacharacter]
          )
        end

        pattern = dialect.concat(ArelSupport.quote("%"), pattern) if prefix
        pattern = dialect.concat(pattern, ArelSupport.quote("%")) if suffix
        pattern
      end
    end
  end
end
