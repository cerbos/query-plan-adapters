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
          ArelSupport.quote(ESCAPE_CHAR),
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
