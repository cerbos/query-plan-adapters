# frozen_string_literal: true

require_relative "errors"

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

        Normalised.new(kind, (kind == CONDITIONAL) ? node(condition) : nil)
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
    end
  end
end
