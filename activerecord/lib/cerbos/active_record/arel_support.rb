# frozen_string_literal: true

require "arel"

require_relative "errors"
require_relative "timestamps"

module Cerbos
  module ActiveRecord
    # Arel node builders. They hide differences between Rails versions (7.1 to 8.x) and between
    # SQL dialects that could otherwise change what a filter means.
    #
    # @private
    module ArelSupport
      module_function

      # Rails 7.2 made `Arel::Nodes::Or` take one array; 7.1 is binary. Detected by arity, not
      # rescue, so real argument errors still surface.
      OR_IS_NARY = (Arel::Nodes::Or.instance_method(:initialize).arity == 1)

      TRUE_SQL = Arel.sql("TRUE")
      FALSE_SQL = Arel.sql("FALSE")
      NULL_SQL = Arel.sql("NULL")

      # Wraps a Ruby value in an Arel node; existing nodes pass through.
      #
      # `nil` stays `nil` so Arel renders `IS NULL` / `IS NOT NULL`. A quoted `nil` would give
      # `= NULL`, which is always unknown.
      def quote(value)
        return value if value.nil? || arel_node?(value)

        Timestamps.assert_bindable(value) if value.is_a?(::Time)
        quoted = Arel::Nodes.build_quoted(value)
        return quoted unless value.is_a?(::String) && current_dialect&.mysql?

        converted = Arel::Nodes::InfixOperation.new("USING", quoted, Arel.sql("utf8mb4"))
        exact_mysql(Arel::Nodes::NamedFunction.new("CONVERT", [converted]))
      end

      # The dialect of the translation in progress, so every string literal and string ordering
      # is built for it: CEL compares strings byte for byte and orders them by code point, which
      # MySQL's default collation and PostgreSQL's linguistic ones do not.
      def with_dialect(dialect, column_type, schema_column = nil)
        previous = Thread.current[:cerbos_active_record_dialect]
        Thread.current[:cerbos_active_record_dialect] = [dialect, column_type, schema_column]
        yield
      ensure
        Thread.current[:cerbos_active_record_dialect] = previous
      end

      def current_dialect
        Thread.current[:cerbos_active_record_dialect]&.first
      end

      MYSQL_EXACT_COLLATION = Arel.sql("utf8mb4_0900_bin")
      CODE_POINT_COLLATION = Arel.sql('"C"')

      def exact_mysql(node)
        collate(node, MYSQL_EXACT_COLLATION)
      end

      def collate(node, collation)
        Arel::Nodes::Grouping.new(Arel::Nodes::InfixOperation.new("COLLATE", node, collation))
      end

      def collated?(node)
        node.is_a?(Arel::Nodes::Grouping) && node.expr.is_a?(Arel::Nodes::InfixOperation) &&
          node.expr.operator == "COLLATE"
      end

      def string_operand?(node)
        return true if collated?(node)
        return true if node.is_a?(Arel::Nodes::Quoted) && node.value.is_a?(::String)

        column_type = Thread.current[:cerbos_active_record_dialect]&.[](1)
        %i[string text].include?(column_type&.call(node))
      end

      # A string comparison under one explicit collation: MySQL's byte-exact one on a string
      # column when no literal already carries it, and "C" on PostgreSQL for an ordering.
      def exact_operands(operator, left, right)
        dialect = current_dialect
        sides = [left, right]
        return sides unless dialect && sides.any? { |side| string_operand?(side) }

        if dialect.mysql?
          return sides if sides.any? { |side| collated?(side) }

          return sides.map { |side| string_operand?(side) ? exact_mysql(side) : side }
        end
        return sides unless dialect.postgres? && ORDERING_OPERATORS.include?(operator)

        sides.map { |side| string_operand?(side) ? collate(side, CODE_POINT_COLLATION) : side }
      end

      ORDERING_OPERATORS = %w[lt gt le ge].freeze

      def arel_node?(value)
        value.is_a?(Arel::Nodes::Node) ||
          value.is_a?(Arel::Attributes::Attribute) ||
          value.is_a?(Arel::Nodes::SqlLiteral)
      end

      # Turns a Ruby boolean or nil (e.g. a subtree folded in Ruby) into SQL for a boolean
      # position.
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

      # Makes `CASE WHEN c1 THEN v1 [WHEN c2 THEN v2 ...] [ELSE e] END`.
      #
      # Omit ELSE on purpose: with no matching WHEN the CASE is NULL, like a CEL error, so the
      # row is excluded even under NOT. An ELSE would let those rows in.
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
        node = node_class.new(*exact_operands(operator, quote(left), quote(right)))
        (operator == "eq") ? indexable(node) : node
      end

      def infix(operator, left, right)
        Arel::Nodes::Grouping.new(
          Arel::Nodes::InfixOperation.new(operator, quote(left), quote(right))
        )
      end

      def cast(value, type)
        Arel::Nodes::NamedFunction.new("CAST", [Arel::Nodes::As.new(quote(value), Arel.sql(type))])
      end

      def function(name, args)
        Arel::Nodes::NamedFunction.new(name, args.map { |arg| quote(arg) })
      end

      # MySQL reads a string column against a utf8mb4_0900_bin literal with no index on a column of
      # another collation, and loses the index's order for ORDER BY ... LIMIT. Equal under the
      # column's own collation holds whenever byte-equal does, so that test goes first, where the
      # index serves it, and the byte-exact one keeps only CEL's rows. Each of `=`, `IN` and a
      # LIKE prefix gets this; the pair has the same three-valued result as the exact test alone.
      def indexable(node)
        return node unless current_dialect&.mysql?

        column, exact = node.left, node.right
        column, exact = exact, column if node.is_a?(Arel::Nodes::Equality) && !column.is_a?(Arel::Attributes::Attribute)
        collation = schema_column(column)&.collation
        return node unless collation&.start_with?("utf8mb4_") && collation != MYSQL_EXACT_COLLATION.to_s

        literals = exact.is_a?(Array) ? exact : [exact]
        relaxed = literals.map { |literal| relaxed_literal(literal, collation) }
        return node if relaxed.any?(&:nil?)

        prefilter =
          case node
          when Arel::Nodes::In then Arel::Nodes::In.new(column, relaxed)
          when Arel::Nodes::Equality then Arel::Nodes::Equality.new(column, relaxed.first)
          when Arel::Nodes::Matches
            return node if literals.first.expr.left.expressions.first.left.value.match?(/\A[%_]/)

            Arel::Nodes::Matches.new(column, relaxed.first, node.escape, true)
          else return node
          end
        Arel::Nodes::Grouping.new(Arel::Nodes::And.new([prefilter, node]))
      end

      # True when a predicate can be proven never to be NULL, so a quantifier needs no subquery
      # looking for an element whose body errored. Conservative: an unrecognised node may be.
      # Inside an AND, a conjunct may be NULL only where an `IS NOT NULL` before it is false.
      def never_null?(node, known = [])
        case node
        when Arel::Nodes::Grouping then never_null?(node.expr, known)
        when Arel::Nodes::SqlLiteral then node.equal?(TRUE_SQL) || node.equal?(FALSE_SQL)
        when Arel::Nodes::Exists then true
        when Arel::Nodes::Not then never_null?(node.expr, known)
        when Arel::Nodes::And
          known = known.dup
          node.children.all? do |child|
            never_null?(child, known).tap { known << bare(child.left) if child.is_a?(Arel::Nodes::NotEqual) && child.right.nil? }
          end
        when Arel::Nodes::Or
          (node.respond_to?(:children) ? node.children : [node.left, node.right]).all? { |child| never_null?(child, known) }
        when Arel::Nodes::In
          value_never_null?(node.left, known) && Array(node.right).all? { |value| value_never_null?(value, known) }
        when Arel::Nodes::Equality, Arel::Nodes::NotEqual, Arel::Nodes::LessThan, Arel::Nodes::GreaterThan,
          Arel::Nodes::LessThanOrEqual, Arel::Nodes::GreaterThanOrEqual, Arel::Nodes::Matches
          node.right.nil? || (value_never_null?(node.left, known) && value_never_null?(node.right, known))
        else false
        end
      end

      def value_never_null?(value, known)
        value = bare(value)
        case value
        when Arel::Attributes::Attribute then known.include?(value) || schema_column(value)&.null == false
        when Arel::Nodes::Quoted, Arel::Nodes::Casted then !value.value.nil?
        when Arel::Nodes::NamedFunction
          value.name == "CONVERT" && value_never_null?(value.expressions.first.left, known)
        else false
        end
      end

      # A node without the groupings and COLLATE this module wraps around it.
      def bare(node)
        loop do
          if collated?(node) then node = node.expr.left
          elsif node.is_a?(Arel::Nodes::Grouping) then node = node.expr
          else return node
          end
        end
      end

      # The bare column a node reads, as the schema declares it, or nil for anything else.
      def schema_column(node)
        return unless node.is_a?(Arel::Attributes::Attribute)

        Thread.current[:cerbos_active_record_dialect]&.[](2)&.call(node)
      end

      # `quote`'s `(CONVERT('x' USING utf8mb4) COLLATE utf8mb4_0900_bin)` under another collation.
      def relaxed_literal(literal, collation)
        return unless collated?(literal) && literal.expr.right == MYSQL_EXACT_COLLATION

        converted = literal.expr.left
        return unless converted.is_a?(Arel::Nodes::NamedFunction) && converted.name == "CONVERT"

        collate(converted, Arel.sql(collation))
      end

      # `IS NULL` for any expression. Collection macros use it to find elements whose lambda
      # body errored.
      def is_null(expression)
        Arel::Nodes::Equality.new(Arel::Nodes::Grouping.new(to_predicate(expression)), nil)
      end
    end
  end
end
