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

      # A double truncated toward zero, as a whole-number type: CEL's `int()` of a double in its
      # range. SQLite's CAST of a REAL to INTEGER already truncates; PostgreSQL's and MySQL's CAST
      # round, so the fraction is dropped first with TRUNC and TRUNCATE, and the CAST is exact.
      def truncate_to_int(expression)
        case adapter_name
        when "sqlite", "sqlite3" then ArelSupport.cast(expression, "INTEGER")
        when *MYSQL_ADAPTERS then ArelSupport.cast(ArelSupport.function("TRUNCATE", [expression, 0]), "SIGNED")
        else ArelSupport.cast(ArelSupport.function("TRUNC", [expression]), "BIGINT")
        end
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

      # The CAST type for a 64-bit whole number.
      def int_type
        case adapter_name
        when "sqlite", "sqlite3" then "INTEGER"
        when *MYSQL_ADAPTERS then "SIGNED"
        else "BIGINT"
        end
      end

      # True where a string is spelled as Go's `strconv.ParseInt` reads it, which CEL's `int()`
      # over a string requires: an optional sign and then ASCII digits only, nothing else, not
      # even a trailing newline. SQLite has no regex, so it strips the digits from both ends and
      # checks that only a sign at the front is left; PostgreSQL does the same with BTRIM; MySQL's
      # ICU regex anchors with `\A` and `\z`, since `$` would match before a final newline.
      def integer_text(expression)
        if mysql?
          return ArelSupport.infix("REGEXP", expression, "\\A[+-]?[0-9]+\\z")
        end

        trim = ArelSupport.function(postgres? ? "BTRIM" : "TRIM", [expression, "0123456789"])
        ArelSupport.and_node([
          Arel::Nodes::In.new(trim, ["", "+", "-"].map { |sign| ArelSupport.quote(sign) }),
          ArelSupport.comparison("eq", ArelSupport.function("SUBSTR", [expression, 1, ArelSupport.function("LENGTH", [trim])]), trim),
          ArelSupport.comparison("gt", ArelSupport.function("LENGTH", [expression]), ArelSupport.function("LENGTH", [trim]))
        ])
      end

      # The digits of a string {#integer_text} accepted, without its sign and leading zeros, so
      # their count and text order give the number's magnitude.
      def integer_digits(expression)
        return ArelSupport.function("REGEXP_REPLACE", [expression, "\\A[+-]?0*", ""]) if mysql?

        ArelSupport.function("LTRIM", [ArelSupport.function("LTRIM", [expression, "+-"]), "0"])
      end

      # True on PostgreSQL, where TRIM with a character set is spelled BTRIM.
      def postgres?
        adapter_name.start_with?("postg")
      end
    end
  end
end
