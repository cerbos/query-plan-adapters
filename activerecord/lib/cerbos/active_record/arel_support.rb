# frozen_string_literal: true

require "arel"

module Cerbos
  module ActiveRecord
    # Helper functions that make Arel nodes.
    #
    # They are together in one module for two reasons. Some of them hide a difference between
    # the Arel interfaces of the supported Rails versions (7.0 to 8.x). The others hide a
    # difference between the SQL dialects. If the adapter did not hide these differences, the
    # meaning of a filter could change.
    module ArelSupport
      module_function

      # Rails 7.1 made +Arel::Nodes::Or+ n-ary, with one array argument. Before that version
      # it was binary. The adapter examines the interface and does not use a rescue clause.
      # Thus a true argument error is still visible.
      OR_IS_NARY = (Arel::Nodes::Or.instance_method(:initialize).arity == 1)

      TRUE_SQL = Arel.sql("TRUE")
      FALSE_SQL = Arel.sql("FALSE")
      NULL_SQL = Arel.sql("NULL")

      # Puts a Ruby value into an Arel node. A node that already exists does not change.
      #
      # A +nil+ stays a +nil+. The Arel visitors for Equality and NotEqual change a +nil+ on
      # the right side into +IS NULL+ or +IS NOT NULL+. A quoted +nil+ would make
      # +\= NULL+ instead, and the result of that comparison is always unknown.
      def quote(value)
        return value if value.nil? || arel_node?(value)

        Arel::Nodes.build_quoted(value)
      end

      def arel_node?(value)
        value.is_a?(Arel::Nodes::Node) ||
          value.is_a?(Arel::Attributes::Attribute) ||
          value.is_a?(Arel::Nodes::SqlLiteral)
      end

      # Changes an operand into a value that a boolean position can use. The translator can
      # calculate a full subtree and get a Ruby boolean. Two constant hierarchies are an
      # example. Such a result must become SQL.
      def to_predicate(value)
        case value
        when true then TRUE_SQL
        when false then FALSE_SQL
        when nil then NULL_SQL
        else value
        end
      end

      def and_node(nodes)
        nodes = nodes.map { |n| to_predicate(n) }
        return TRUE_SQL if nodes.empty?
        return nodes.first if nodes.size == 1

        Arel::Nodes::Grouping.new(Arel::Nodes::And.new(nodes))
      end

      def or_node(nodes)
        nodes = nodes.map { |n| to_predicate(n) }
        return FALSE_SQL if nodes.empty?
        return nodes.first if nodes.size == 1

        combined =
          if OR_IS_NARY
            Arel::Nodes::Or.new(nodes)
          else
            nodes.reduce { |left, right| Arel::Nodes::Or.new(left, right) }
          end
        Arel::Nodes::Grouping.new(combined)
      end

      def not_node(value)
        Arel::Nodes::Not.new(Arel::Nodes::Grouping.new(to_predicate(value)))
      end

      # Makes +CASE WHEN c1 THEN v1 [WHEN c2 THEN v2 ...] [ELSE e] END+.
      #
      # The absence of the ELSE clause is important. If all the conditions of a CASE are
      # unknown, the result of the CASE is NULL. This is the same result as a CEL error for an
      # element. Thus the row stays out of the result, and it also stays out when a NOT
      # operator is around the CASE. An ELSE clause would put those rows into the else branch.
      def case_node(whens, else_value: :__omitted__)
        node = Arel::Nodes::Case.new
        whens.each do |condition, result|
          node = node.when(to_predicate(condition)).then(quote(to_predicate(result)))
        end
        node = node.else(quote(to_predicate(else_value))) unless else_value == :__omitted__
        Arel::Nodes::Grouping.new(node)
      end

      COMPARISON_NODES = {
        "eq" => Arel::Nodes::Equality,
        "ne" => Arel::Nodes::NotEqual,
        "lt" => Arel::Nodes::LessThan,
        "gt" => Arel::Nodes::GreaterThan,
        "le" => Arel::Nodes::LessThanOrEqual,
        "ge" => Arel::Nodes::GreaterThanOrEqual
      }.freeze

      def comparison(operator, left, right)
        node_class = COMPARISON_NODES.fetch(operator) do
          raise UnsupportedOperatorError, "Unsupported comparison operator: #{operator}"
        end
        node_class.new(quote(left), quote(right))
      end

      def infix(operator, left, right)
        Arel::Nodes::Grouping.new(
          Arel::Nodes::InfixOperation.new(operator, quote(left), quote(right))
        )
      end

      def function(name, args)
        Arel::Nodes::NamedFunction.new(name, args.map { |arg| quote(arg) })
      end

      # Makes +IS NULL+ for an expression of any type. A collection macro uses this to find
      # the elements for which the body of the lambda made an error.
      def is_null(expression)
        Arel::Nodes::Equality.new(Arel::Nodes::Grouping.new(to_predicate(expression)), nil)
      end
    end
  end
end
