# frozen_string_literal: true

require_relative "errors"
require_relative "planner_operators"

module Cerbos
  module MongoDB
    # Reads a +PlanResources+ response into one small syntax tree for the translator.
    #
    # A response arrives in more than one shape: an output object of the official Ruby SDK
    # (https://github.com/cerbos/cerbos-sdk-ruby), or the JSON of a REST or gRPC response after a
    # parse, with the plan either at the top level or under +filter+.
    module Plan
      Expression = Struct.new(:operator, :operands)
      Value = Struct.new(:value)
      Variable = Struct.new(:name)

      ALWAYS_ALLOWED = "KIND_ALWAYS_ALLOWED"
      ALWAYS_DENIED = "KIND_ALWAYS_DENIED"
      CONDITIONAL = "KIND_CONDITIONAL"
      KINDS = [ALWAYS_ALLOWED, ALWAYS_DENIED, CONDITIONAL].freeze

      # BSON has no integer wider than 64 bits. The planner's wire value is a protobuf double, so
      # an integral literal outside that range was always a double; a JSON reader that turned it
      # into a Ruby Integer would otherwise make the driver refuse the whole filter.
      INT64_RANGE = (-(2**63)..(2**63 - 1))

      Normalised = Struct.new(:kind, :condition)

      module_function

      # @return [Normalised]
      def normalise(plan)
        kind, condition = extract(plan)
        kind = kind.to_s
        raise InvalidPlanError, "Unrecognised query plan kind: #{kind.inspect}" unless KINDS.include?(kind)
        if kind == CONDITIONAL && condition.nil?
          raise InvalidPlanError, "Conditional query plan has no condition"
        end

        Normalised.new(kind, (kind == CONDITIONAL) ? rewrite_set_functions(assert_planner_operators(node(condition))) : nil)
      end

      # @api private
      def assert_planner_operators(operand)
        return operand unless operand.is_a?(Expression)
        raise UnsupportedError, "Unsupported operator: #{operand.operator}" unless PLANNER_OPERATORS.include?(operand.operator)

        operand.operands.each { |child| assert_planner_operators(child) }
        operand
      end

      # @api private
      def extract(plan)
        if plan.is_a?(Hash)
          plan = symbolish(plan)
          return extract(plan[:filter]) if plan[:filter]

          return [plan[:kind], plan[:condition]]
        end
        return extract(plan.filter) if plan.respond_to?(:filter) && !plan.respond_to?(:kind)

        unless plan.respond_to?(:kind) && plan.respond_to?(:condition)
          raise InvalidPlanError,
            "Cannot read a query plan from #{plan.class}: expected a Cerbos::Output::PlanResources, " \
            "a Hash, or an object responding to #kind and #condition"
        end

        [plan.kind, plan.condition]
      end

      # @api private
      def node(operand)
        case operand
        when Expression, Variable then operand
        when Value then Value.new(literal(operand.value))
        when Hash then hash_node(symbolish(operand))
        else object_node(operand)
        end
      end

      # @api private
      def hash_node(operand)
        return node(operand[:expression]) if operand.key?(:expression)
        # `{"value": null}` is a real constant, so the key is tested, not the value.
        return Value.new(literal(operand[:value])) if operand.key?(:value) && !operand.key?(:operator)
        return Variable.new(operand[:variable].to_s) if operand.key?(:variable)
        if operand.key?(:operator)
          return Expression.new(operand[:operator].to_s, Array(operand[:operands]).map { |child| node(child) })
        end

        raise InvalidPlanError, "Unrecognised query plan operand: #{operand.inspect}"
      end

      # @api private
      def object_node(operand)
        if operand.respond_to?(:operator) && operand.respond_to?(:operands)
          return Expression.new(operand.operator.to_s, Array(operand.operands).map { |child| node(child) })
        end
        return Variable.new(operand.name.to_s) if operand.respond_to?(:name)
        return Value.new(literal(operand.value)) if operand.respond_to?(:value)

        raise InvalidPlanError, "Unrecognised query plan operand: #{operand.inspect}"
      end

      # @api private
      def literal(value)
        case value
        when Integer then INT64_RANGE.cover?(value) ? value : value.to_f
        when Array then value.map { |element| literal(element) }
        when Hash then value.to_h { |key, element| [key.to_s, literal(element)] }
        else value
        end
      end

      # @api private
      def symbolish(hash)
        hash.transform_keys { |key| key.respond_to?(:to_sym) ? key.to_sym : key }
      end

      SET_FUNCTIONS = %w[hasAll hasAny hasOnly hasExactly].freeze

      # Rewrites hasAll, hasAny, hasOnly and hasExactly (CEL list extensions, Cerbos 0.58) into
      # hasIntersection and isSubset, bottom-up. Each rewrite is exact in CEL, errors included; a
      # call without exactly one literal list operand is left for the translator to refuse.
      #
      # @api private
      def rewrite_set_functions(operand)
        return operand unless operand.is_a?(Expression)

        rewritten = Expression.new(operand.operator, operand.operands.map { |child| rewrite_set_functions(child) })
        set_function(rewritten) || rewritten
      end

      # @api private
      def set_function(expr)
        return nil unless SET_FUNCTIONS.include?(expr.operator) && expr.operands.length == 2

        literal_list = ->(o) { o.is_a?(Value) && o.value.is_a?(Array) }
        first, second = expr.operands
        first_literal = literal_list.call(first)
        return nil if first_literal == literal_list.call(second)

        list, literal = first_literal ? [second, first] : [first, second]
        operator = expr.operator
        operator = {"hasAll" => "hasOnly", "hasOnly" => "hasAll"}.fetch(operator, operator) if first_literal
        contains_all = lambda do
          next Expression.new("not", [Expression.new("hasIntersection", [list, Value.new([])])]) if literal.value.empty?

          tests = literal.value.map { |v| Expression.new("hasIntersection", [list, Value.new([v])]) }
          (tests.length == 1) ? tests.first : Expression.new("and", tests)
        end
        case operator
        when "hasAny" then Expression.new("hasIntersection", [list, literal])
        when "hasAll" then contains_all.call
        when "hasOnly" then Expression.new("isSubset", [list, literal])
        else Expression.new("and", [contains_all.call, Expression.new("isSubset", [list, literal])])
        end
      end
    end
  end
end
