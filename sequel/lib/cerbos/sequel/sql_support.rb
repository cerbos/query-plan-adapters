# frozen_string_literal: true

require "sequel"

require_relative "errors"
require_relative "timestamps"

module Cerbos
  module Sequel
    # Helper functions that make Sequel expression objects.
    #
    # They are together in one module so every node the translator builds goes through one
    # place, and so the rules that decide the meaning of a filter — where a NULL goes, which
    # CASE has no ELSE arm, how a NOT wraps its operand — are written down once.
    #
    # The translator builds each node with its constructor and never with the operator methods
    # of Sequel (+~+, +&+, +|+). Those methods simplify: +~(a = b)+ becomes +a != b+ and
    # +~(a & b)+ is pushed through De Morgan. Each rewrite is sound under three-valued logic, but
    # an explicit +NOT+ keeps the emitted SQL in the shape of the plan, so it reads as the plan it
    # came from.
    module SqlSupport
      module_function

      BooleanExpression = ::Sequel::SQL::BooleanExpression
      NumericExpression = ::Sequel::SQL::NumericExpression
      CaseExpression = ::Sequel::SQL::CaseExpression

      # True for a value that the database calculates: a column, an expression, or a subquery.
      #
      # +Sequel::LiteralString+ is a subclass of +String+, and the translator reads a +String+
      # as a CEL string constant. This adapter never makes one, so it is not listed here, and a
      # literal string that an operator override gives back reads as the expression it is only
      # because of the check below it.
      def sql_node?(value)
        value.is_a?(::Sequel::SQL::Expression) ||
          value.is_a?(::Sequel::Dataset) ||
          value.is_a?(::Sequel::LiteralString)
      end

      # Changes an operand into a value that a boolean position can use. The translator can
      # calculate a full subtree and get a Ruby boolean. Two constant hierarchies are an
      # example. Sequel literalizes +true+, +false+ and +nil+ in the spelling of the dialect.
      def to_predicate(value)
        value
      end

      def and_node(nodes)
        return true if nodes.empty?
        return nodes.first if nodes.size == 1

        BooleanExpression.new(:AND, *nodes)
      end

      def or_node(nodes)
        return false if nodes.empty?
        return nodes.first if nodes.size == 1

        BooleanExpression.new(:OR, *nodes)
      end

      # Sequel writes +NOT x+ with no parentheses around it, and every store binds +IS+ and the
      # comparison operators tighter than +NOT+: +NOT x IS NULL+ reads as +NOT (x IS NULL)+. A
      # negation that becomes the operand of +IS NULL+ (an element guard in a macro) or of a
      # comparison would test the wrong expression, so the negation carries its own parentheses.
      def not_node(value)
        ::Sequel::SQL::PlaceholderLiteralString.new(["(NOT ", ")"], [value])
      end

      # Makes +CASE WHEN c1 THEN v1 [WHEN c2 THEN v2 ...] ELSE e END+.
      #
      # Sequel always writes an ELSE arm. When the caller gives none, the arm is +ELSE NULL+,
      # which is the same as no arm at all, and that is important: if all the conditions of a
      # CASE are unknown, the result of the CASE is NULL. This is the same result as a CEL error
      # for an element. Thus the row stays out of the result, and it also stays out when a NOT
      # operator is around the CASE. An ELSE arm with a value would put those rows into it.
      def case_node(whens, else_value: nil)
        CaseExpression.new(whens.map { |condition, result| [condition, bindable(result)] }, bindable(else_value))
      end

      COMPARISON_OPERATORS = {
        "eq" => :"=",
        "ne" => :"!=",
        "lt" => :<,
        "gt" => :>,
        "le" => :<=,
        "ge" => :>=
      }.freeze

      # A comparison in the order of the operands. A +nil+ on the right side of +eq+ or +ne+
      # becomes +IS NULL+ or +IS NOT NULL+: +\= NULL+ is always unknown.
      def comparison(operator, left, right)
        sql_operator = COMPARISON_OPERATORS.fetch(operator) do
          raise UnsupportedOperatorError, "Unsupported comparison operator: #{operator}"
        end

        if right.nil? && operator == "eq"
          return BooleanExpression.new(:IS, left, nil)
        end
        if right.nil? && operator == "ne"
          return BooleanExpression.new(:"IS NOT", left, nil)
        end

        BooleanExpression.new(sql_operator, bindable(left), bindable(right))
      end

      # A Ruby value about to go into SQL, refused if Sequel would literalize it as another
      # value. A Time keeps six fractional digits there. See {Timestamps.assert_bindable}.
      def bindable(value)
        Timestamps.assert_bindable(value) if value.is_a?(::Time)
        value
      end

      def infix(operator, left, right)
        NumericExpression.new(operator.to_sym, left, right)
      end

      def function(name, args)
        ::Sequel.function(name, *args)
      end

      # Makes +IS NULL+ for an expression of any type. A collection macro uses this to find
      # the elements for which the body of the lambda made an error.
      def is_null(expression)
        BooleanExpression.new(:IS, expression, nil)
      end

      def in_list(needle, values)
        BooleanExpression.new(:IN, needle, values.map { |value| bindable(value) })
      end
    end
  end
end
