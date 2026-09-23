# frozen_string_literal: true

require_relative "errors"

module Cerbos
  module ActiveRecord
    # Normalises a `PlanResources` response into one AST for the translator. Accepts a Ruby
    # SDK output object, or the JSON / protobuf-JSON from REST or gRPC.
    #
    # @private
    module Plan
      # @private
      Expression = Struct.new(:operator, :operands)
      # @private
      Value = Struct.new(:value)
      # @private
      Variable = Struct.new(:name)

      ALWAYS_ALLOWED = "KIND_ALWAYS_ALLOWED"
      ALWAYS_DENIED = "KIND_ALWAYS_DENIED"
      CONDITIONAL = "KIND_CONDITIONAL"

      # @private
      Normalised = Struct.new(:kind, :condition) do
        def always_allowed?
          kind == ALWAYS_ALLOWED
        end

        def always_denied?
          kind == ALWAYS_DENIED
        end
      end

      module_function

      # @param plan [Object] a `Cerbos::Output::PlanResources`, a Hash, or an object that has
      #   `kind` and `condition` (or `filter`) in one of those shapes
      # @return [Normalised]
      def normalise(plan)
        kind, condition = extract(plan)
        kind = kind.to_s

        unless [ALWAYS_ALLOWED, ALWAYS_DENIED, CONDITIONAL].include?(kind)
          raise InvalidPlanError, "Unrecognised query plan kind: #{kind.inspect}"
        end

        if kind == CONDITIONAL && condition.nil?
          raise InvalidPlanError, "Conditional query plan has no condition"
        end

        Normalised.new(kind: kind, condition: (kind == CONDITIONAL) ? node(condition) : nil)
      end

      # @private
      def extract(plan)
        if plan.is_a?(Hash)
          plan = symbolish(plan)
          filter = plan[:filter]
          return extract(filter) if filter
          return [plan[:kind], plan[:condition]]
        end

        # Protobuf responses nest the plan in `filter`; SDK output objects have `kind` directly.
        return extract(plan.filter) if plan.respond_to?(:filter) && !plan.respond_to?(:kind)

        unless plan.respond_to?(:kind) && plan.respond_to?(:condition)
          raise InvalidPlanError,
            "Cannot read a query plan from #{plan.class}: expected a Cerbos::Output::PlanResources, " \
            "a Hash, or an object responding to #kind and #condition"
        end

        [plan.kind, plan.condition]
      end

      # @private
      def node(operand)
        return nil if operand.nil?

        case operand
        when Expression, Value, Variable then operand
        when Hash then hash_node(operand)
        else object_node(operand)
        end
      end

      # @private
      def hash_node(operand)
        operand = symbolish(operand)

        if operand.key?(:expression)
          return node(operand[:expression])
        end
        # Check for the key, not the value: `{"value": null}` is a valid constant.
        if operand.key?(:value) && !operand.key?(:operator)
          return Value.new(value: operand[:value])
        end
        if operand.key?(:variable)
          return Variable.new(name: operand[:variable].to_s)
        end
        if operand.key?(:operator)
          return Expression.new(
            operator: operand[:operator].to_s,
            operands: Array(operand[:operands]).map { |child| node(child) }
          )
        end

        raise InvalidPlanError, "Unrecognised query plan operand: #{operand.inspect}"
      end

      # @private
      def object_node(operand)
        if operand.respond_to?(:operator) && operand.respond_to?(:operands)
          return Expression.new(
            operator: operand.operator.to_s,
            operands: Array(operand.operands).map { |child| node(child) }
          )
        end
        if operand.respond_to?(:name)
          return Variable.new(name: operand.name.to_s)
        end
        if operand.respond_to?(:value)
          return Value.new(value: operand.value)
        end

        raise InvalidPlanError, "Unrecognised query plan operand: #{operand.inspect}"
      end

      # @private
      def symbolish(hash)
        return hash if hash.empty? || hash.first.first.is_a?(Symbol)

        hash.transform_keys { |key| key.respond_to?(:to_sym) ? key.to_sym : key }
      end
    end
  end
end
