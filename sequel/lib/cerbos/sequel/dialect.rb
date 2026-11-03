# frozen_string_literal: true

require_relative "sql_support"

module Cerbos
  module Sequel
    # The small number of operations for which no portable SQL is available. For each of them,
    # an incorrect selection changes the meaning of the filter. It does not change only the
    # text of the SQL.
    #
    # Sequel already hides two of the differences that the ActiveRecord adapter handles itself:
    # +Sequel.join+ writes +CONCAT+ on MySQL and +||+ elsewhere, and +Sequel.char_length+ counts
    # characters on every dialect (MySQL's +LENGTH+ counts bytes). What is left is the name of
    # the type a CAST must use.
    class Dialect
      def initialize(database_type)
        @database_type = database_type.to_s.downcase
      end

      attr_reader :database_type

      def mysql?
        database_type == "mysql"
      end

      def self.for(model)
        new(model.db.database_type)
      end

      def concat(left, right)
        ::Sequel.join([left, right])
      end

      # CEL's +int / int+, which truncates toward zero. SQLite's and PostgreSQL's +/+ over two
      # integers already does; MySQL's +/+ gives a decimal, and its +DIV+ truncates.
      def int_divide(left, right)
        return ::Sequel.lit("(? DIV ?)", left, right) if mysql?

        SqlSupport.infix("/", left, right)
      end

      # A double truncated toward zero, as a whole-number type: CEL's +int()+ of a double in
      # its range. SQLite's CAST of a REAL to INTEGER already truncates; PostgreSQL's and
      # MySQL's CAST round, so the fraction is dropped first with TRUNC and TRUNCATE, after
      # which the CAST is exact.
      def truncate_to_int(expression)
        case database_type
        when "sqlite" then ::Sequel.cast(expression, "INTEGER")
        when "mysql" then ::Sequel.cast(::Sequel.function(:TRUNCATE, expression, 0), "SIGNED")
        else ::Sequel.cast(::Sequel.function(:trunc, expression), "bigint")
        end
      end

      # CEL +size()+ counts the characters of a string.
      def char_length(expression)
        ::Sequel.char_length(expression)
      end

      # The type name that a CAST must use to get an IEEE-754 binary64 value. The correct name
      # is important. The +numeric+ type of PostgreSQL is an exact decimal type. If the
      # adapter used it, arithmetic with fractions would not agree with the doubles of CEL.
      def double_type
        case database_type
        when "sqlite" then "REAL"
        when "mysql" then "DOUBLE"
        else "double precision"
        end
      end

      # The type name that a CAST to a string must use. MySQL has no +TEXT+ target for a CAST.
      def text_type
        mysql? ? "CHAR" : "TEXT"
      end
    end
  end
end
