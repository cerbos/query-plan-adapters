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

      # Another store's collations, types and functions are untested, and a case-folding
      # default collation (SQL Server, Oracle) matches rows CEL does not. Every MySQL string
      # comparison is quoted under utf8mb4_0900_bin, which MariaDB has from 11.4.5.
      def self.for(model)
        connection = model.connection
        dialect = new(connection.adapter_name)
        dialect.check_supported(connection)
        dialect
      end

      SUPPORTED = "use SQLite, PostgreSQL, MySQL 8.0 or later, or MariaDB 11.4.5 or later"

      def check_supported(connection)
        return if sqlite? || postgres?

        unless mysql?
          raise UnsupportedDatabaseError,
            "The #{connection.adapter_name} database adapter is not supported: #{SUPPORTED}"
        end
        server, minimum = connection.mariadb? ? ["MariaDB", "11.4.5"] : ["MySQL", "8.0"]
        return if connection.database_version >= minimum

        raise UnsupportedDatabaseError, "#{server} #{connection.database_version} is not supported: #{SUPPORTED}"
      end

      def concat(left, right)
        if mysql?
          ArelSupport.function("CONCAT", [left, right])
        else
          ArelSupport.infix("||", left, right)
        end
      end

      # CEL `size()` counts characters. MySQL's `LENGTH` counts bytes, so MySQL uses
      # `CHAR_LENGTH`. PostgreSQL's `LENGTH` already counts characters. SQLite's stops at the
      # first NUL, and its `REPLACE` cannot match one, so each NUL becomes a space inside the
      # string's JSON text, where it is the escape \u0000.
      def char_length(expression)
        return ArelSupport.function(mysql? ? "CHAR_LENGTH" : "LENGTH", [expression]) unless sqlite?

        json = ArelSupport.function("REPLACE", [ArelSupport.function("JSON_QUOTE", [expression]), "\\\\", "\u0001"])
        json = ArelSupport.function("REPLACE", [json, "\\u0000", " "])
        json = ArelSupport.function("REPLACE", [json, "\u0001", "\\\\"])
        ArelSupport.function("LENGTH", [ArelSupport.function("JSON_EXTRACT", [json, "$"])])
      end

      def sqlite?
        %w[sqlite sqlite3].include?(adapter_name)
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
