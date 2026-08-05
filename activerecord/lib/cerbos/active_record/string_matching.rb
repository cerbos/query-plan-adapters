# frozen_string_literal: true

require_relative "arel_support"

module Cerbos
  module ActiveRecord
    # Changes the CEL string operators +contains+, +startsWith+ and +endsWith+ into +LIKE+
    # with an ESCAPE clause.
    #
    # The ESCAPE clause is the important part. The needle in CEL is a literal string, but
    # +LIKE+ reads +%+ and +_+ as wildcards. Thus a translation without an escape character
    # changes the meaning. For <tt>R.attr.name.contains("a_b")</tt>, such a filter also
    # matches +aXb+, and it gives rows that the PDP denies.
    class StringMatcher
      ESCAPE_CHAR = "\\"

      # SQL Server reads +[+ as the start of a character class. It does this even when an
      # ESCAPE clause is present. Thus the adapter escapes +[+ with the two portable
      # wildcards, and it does not leave +[+ to the dialect.
      METACHARACTERS = [ESCAPE_CHAR, "%", "_", "["].freeze

      def initialize(dialect)
        @dialect = dialect
      end

      attr_reader :dialect

      # @param receiver [Object] the haystack, in the order of the CEL source. It can be a
      #   constant.
      # @param needle [Object] the needle. It can be a column, for a comparison between two
      #   fields.
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
          # CEL compares strings with attention to the case of the letters. If this flag were
          # false, PostgreSQL would use ILIKE.
          true
        )
      end

      # Puts an escape character before each LIKE metacharacter in a literal needle.
      #
      # The block form of gsub is necessary. It is not only a preference. A String replacement
      # reads the backslash sequences again. Thus a replacement of "\\" with "\\" + "\\" gives
      # one backslash, and the needle stays without an escape character.
      def escape_literal(needle)
        METACHARACTERS.reduce(needle) do |escaped, metacharacter|
          escaped.gsub(metacharacter) { ESCAPE_CHAR + metacharacter }
        end
      end

      private

      # Puts an escape character before each LIKE metacharacter in a needle that is a column.
      # The database does this when it runs the query.
      #
      # If the needle is NULL, REPLACE gives NULL, and thus the pattern is NULL. The result of
      # the LIKE is unknown and the row stays out of the result. This is the same result as
      # the CEL error for a missing attribute, which is a deny for that row.
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
