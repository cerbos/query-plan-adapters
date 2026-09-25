# frozen_string_literal: true

require_relative "arel_support"

module Cerbos
  module ActiveRecord
    # The few operations with no portable SQL. Picking the wrong form changes what the filter
    # means, not just how it reads.
    #
    # @private
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

      # MySQL reads `||` as OR by default. Elsewhere use `||`: SQLite before 3.44 has no
      # `CONCAT`.
      def concat(left, right)
        if mysql?
          ArelSupport.function("CONCAT", [left, right])
        else
          ArelSupport.infix("||", left, right)
        end
      end

      # CEL `size()` counts characters. MySQL's `LENGTH` counts bytes, so MySQL uses
      # `CHAR_LENGTH`. SQLite and PostgreSQL `LENGTH` already count characters.
      def char_length(expression)
        ArelSupport.function(mysql? ? "CHAR_LENGTH" : "LENGTH", [expression])
      end

      # Integer division truncating toward zero, as CEL's `int / int` does. SQLite's and
      # PostgreSQL's `/` over two integers already truncates; MySQL's `/` gives a decimal, and
      # its `DIV` truncates.
      def int_divide(left, right)
        ArelSupport.infix(mysql? ? "DIV" : "/", left, right)
      end

      # The CAST type for an IEEE-754 double. Not PostgreSQL `numeric`: it is exact decimal, so
      # fractional arithmetic would not match CEL doubles.
      def double_type
        case adapter_name
        when "sqlite", "sqlite3" then "REAL"
        when *MYSQL_ADAPTERS then "DOUBLE"
        else "double precision"
        end
      end

      # The CAST type for a string. MySQL casts only to `CHAR`: `TEXT` and `VARCHAR` are
      # syntax errors there.
      def text_type
        mysql? ? "CHAR" : "TEXT"
      end
    end
  end
end
