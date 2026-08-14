# frozen_string_literal: true

require_relative "arel_support"

module Cerbos
  module ActiveRecord
    # The small number of operations for which no portable SQL is available. For each of them,
    # an incorrect selection changes the meaning of the filter. It does not change only the
    # text of the SQL.
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
      end

      # MySQL has no +||+ operator for the concatenation of strings with its default
      # +sql_mode+. The other databases that this adapter supports have no +CONCAT+ function.
      # SQLite got +CONCAT+ only in version 3.44.
      def concat(left, right)
        if mysql?
          ArelSupport.function("CONCAT", [left, right])
        else
          ArelSupport.infix("||", left, right)
        end
      end

      # CEL +size()+ counts the characters of a string. The +LENGTH+ function of MySQL counts
      # the bytes. Thus it gives the wrong size for a string with multi-byte characters. The
      # +CHAR_LENGTH+ function of MySQL counts the characters. The +LENGTH+ function of SQLite
      # and PostgreSQL counts the characters.
      def char_length(expression)
        ArelSupport.function(mysql? ? "CHAR_LENGTH" : "LENGTH", [expression])
      end

      # The type name that a CAST must use to get an IEEE-754 binary64 value. The correct name
      # is important. The +numeric+ type of PostgreSQL is an exact decimal type. If the
      # adapter used it, arithmetic with fractions would not agree with the doubles of CEL.
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
