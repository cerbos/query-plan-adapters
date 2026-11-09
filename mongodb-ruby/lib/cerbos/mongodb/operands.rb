# frozen_string_literal: true

require_relative "errors"
require_relative "plan"

module Cerbos
  module MongoDB
    # Small predicates over plan nodes, shared by every translation step.
    module Operands
      module_function

      def expression?(operand) = operand.is_a?(Plan::Expression)

      def value?(operand) = operand.is_a?(Plan::Value)

      def variable?(operand) = operand.is_a?(Plan::Variable)

      def expression_with?(operand, operator)
        expression?(operand) && operand.operator == operator
      end

      # CEL's number: an Integer or a Float, never a boolean (Ruby booleans are not Numeric).
      def number?(value) = value.is_a?(Numeric)

      def string?(value) = value.is_a?(String)

      def boolean?(value) = value == true || value == false

      # A whole number, whatever its Ruby class: the gRPC SDK delivers every plan number as a
      # Float, and the JSON wire as an Integer when it is integral.
      def integral?(value)
        value.is_a?(Integer) || (value.is_a?(Float) && value.finite? && value == value.floor)
      end

      def operand_at(operands, index, message)
        operand = operands[index]
        raise InvalidPlanError, message if operand.nil?

        operand
      end

      # Every variable name referenced anywhere below +operand+, in plan order.
      def variable_names(operand)
        return [operand.name] if variable?(operand)
        return operand.operands.flat_map { |child| variable_names(child) } if expression?(operand)

        []
      end

      # Whether a comparison constant is a plan-level null, or a list holding one.
      def carries_null?(value)
        value.nil? || (value.is_a?(Array) && value.include?(nil))
      end

      def matches_type?(value, value_type)
        case value_type
        when :number then number?(value)
        when :string then string?(value)
        when :boolean then boolean?(value)
        else true
        end
      end
    end
  end
end
