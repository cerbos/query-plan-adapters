# frozen_string_literal: true

require_relative "arel_support"

module Cerbos
  module ActiveRecord
    # The handful of places where portable SQL does not exist and guessing would change
    # filter semantics rather than merely the generated text.
    class Dialect
      MYSQL_ADAPTERS = %w[mysql mysql2 trilogy].freeze

      def initialize(adapter_name)
        @adapter_name = adapter_name.to_s.downcase
      end

      attr_reader :adapter_name

      def mysql?
        MYSQL_ADAPTERS.include?(adapter_name)
      end

      def self.for(model)
        new(model.connection.adapter_name)
      rescue => e
        # A model without a live connection can still be translated: only the two helpers
        # below need the dialect, and both have a sane majority default.
        raise e if e.is_a?(Error)
        new("unknown")
      end

      # MySQL has no +||+ string concatenation operator under its default
      # +sql_mode+; everything else this adapter supports lacks +CONCAT+ (SQLite gained it
      # only in 3.44).
      def concat(left, right)
        if mysql?
          ArelSupport.function("CONCAT", [left, right])
        else
          ArelSupport.infix("||", left, right)
        end
      end

      # CEL +size()+ over a string counts characters. MySQL's +LENGTH+ counts bytes, so a
      # multi-byte string would report the wrong size; +CHAR_LENGTH+ is its character-wise
      # equivalent. SQLite and PostgreSQL +LENGTH+ already count characters.
      def char_length(expression)
        ArelSupport.function(mysql? ? "CHAR_LENGTH" : "LENGTH", [expression])
      end

      # The type name a CAST must use to reach an IEEE-754 binary64. Spelling this wrong is
      # not cosmetic: PostgreSQL's +numeric+ is exact decimal, so casting to it would make
      # fractional arithmetic disagree with CEL's doubles.
      def double_type
        case adapter_name
        when "sqlite", "sqlite3" then "REAL"
        when *MYSQL_ADAPTERS then "DOUBLE"
        else "double precision"
        end
      end
    end
  end
end
