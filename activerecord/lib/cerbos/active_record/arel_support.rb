# frozen_string_literal: true

require "arel"

module Cerbos
  module ActiveRecord
    # Arel construction helpers, kept in one place because several of them paper over Arel
    # API differences across the supported Rails versions (7.0 – 8.x) or over SQL dialect
    # differences that would otherwise silently change filter semantics.
    module ArelSupport
      module_function

      # Rails 7.1 made +Arel::Nodes::Or+ n-ary (one array argument); before that it was
      # binary. Detect rather than rescue, so a genuine argument error still surfaces.
      OR_IS_NARY = (Arel::Nodes::Or.instance_method(:initialize).arity == 1)

      TRUE_SQL = Arel.sql("TRUE")
      FALSE_SQL = Arel.sql("FALSE")
      NULL_SQL = Arel.sql("NULL")

      # Wrap a Ruby value as an Arel node, leaving existing nodes untouched.
      #
      # +nil+ stays +nil+: Arel's Equality/NotEqual visitors turn a literal +nil+ right-hand
      # side into +IS NULL+ / +IS NOT NULL+, whereas a quoted +nil+ would emit the always-
      # unknown +\= NULL+.
      def quote(value)
        return value if value.nil? || arel_node?(value)

        Arel::Nodes.build_quoted(value)
      end

      def arel_node?(value)
        value.is_a?(Arel::Nodes::Node) ||
          value.is_a?(Arel::Attributes::Attribute) ||
          value.is_a?(Arel::Nodes::SqlLiteral)
      end

      # Convert a translated operand into something usable in a boolean position. Constant
      # folding can reduce a whole subtree to a Ruby boolean (two constant hierarchies, for
      # example), which must still render as SQL.
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

      # +CASE WHEN c1 THEN v1 [WHEN c2 THEN v2 ...] [ELSE e] END+.
      #
      # Omitting ELSE is load-bearing: a CASE whose conditions are all UNKNOWN evaluates to
      # NULL, which is exactly CEL's "this element errored" outcome and stays excluded under
      # both polarities of a surrounding NOT.
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

      # +IS NULL+ against an arbitrary expression, used to detect "this element's lambda body
      # errored" inside collection macros.
      def is_null(expression)
        Arel::Nodes::Equality.new(Arel::Nodes::Grouping.new(to_predicate(expression)), nil)
      end
    end
  end
end
