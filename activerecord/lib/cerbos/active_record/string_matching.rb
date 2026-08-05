# frozen_string_literal: true

require_relative "arel_support"

module Cerbos
  module ActiveRecord
    # Translates CEL's receiver-style string matches (+contains+, +startsWith+, +endsWith+)
    # into +LIKE+ with an explicit +ESCAPE+ clause.
    #
    # The escaping is the whole point. CEL's needle is a literal string, but +LIKE+ reads
    # +%+ and +_+ as wildcards, so an unescaped translation of
    # <tt>R.attr.name.contains("a_b")</tt> also matches +aXb+ — a filter that returns rows
    # the PDP denies.
    class StringMatcher
      ESCAPE_CHAR = "\\"

      # +[+ opens a character class on SQL Server *even when an ESCAPE clause is declared*,
      # so it is escaped alongside the portable wildcards rather than left to the dialect.
      METACHARACTERS = [ESCAPE_CHAR, "%", "_", "["].freeze

      def initialize(dialect)
        @dialect = dialect
      end

      attr_reader :dialect

      # @param receiver [Object] the haystack, in CEL source order (may be a constant)
      # @param needle [Object] the needle (may be a column, for field-to-field matching)
      # @param prefix [Boolean] allow anything before the needle
      # @param suffix [Boolean] allow anything after the needle
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
          true # case-sensitive: CEL string matching is, and PostgreSQL would otherwise ILIKE
        )
      end

      # Escape LIKE metacharacters in a literal needle.
      #
      # The block form of gsub is required, not stylistic: a String replacement re-reads
      # backslash sequences, so replacing "\\" with "\\" + "\\" collapses back to a single
      # backslash and silently leaves the needle unescaped.
      def escape_literal(needle)
        METACHARACTERS.reduce(needle) do |escaped, metacharacter|
          escaped.gsub(metacharacter) { ESCAPE_CHAR + metacharacter }
        end
      end

      private

      # Escape a column-valued needle at query time.
      #
      # A NULL needle propagates through REPLACE to a NULL pattern, leaving the LIKE UNKNOWN
      # and the row excluded — which is exactly CEL's missing-attribute error (a deny) for
      # the same row.
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
