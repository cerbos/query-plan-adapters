# frozen_string_literal: true

require_relative "errors"
require_relative "guards"
require_relative "operands"
require_relative "regex"

module Cerbos
  module MongoDB
    # +ancestorOf+, +descendentOf+ and +overlaps+ between one field and one constant path, as a
    # literal prefix +$regex+ and a list of ancestors.
    module Hierarchy
      extend Operands

      Operand = Struct.new(:kind, :name, :value, :separator)

      module_function

      def build(operator, operands, mapper)
        left_operand, right_operand = operands
        raise InvalidPlanError, "#{operator} requires two hierarchy operands" unless left_operand && right_operand

        left = parse(left_operand)
        right = parse(right_operand)
        [left, right].each do |operand|
          next unless operand.kind == :field

          type = mapper.value_type(operand.name)
          unless type.nil? || type == :string
            raise UnsupportedError,
              "hierarchy requires a string field: the declared scalar type cannot be compared with a path prefix"
          end
        end
        if left.separator != right.separator
          raise UnsupportedError, "#{operator} requires one field and one value with the same separator"
        end

        field, value = if left.kind == :field && right.kind == :value
          [left, right]
        elsif left.kind == :value && right.kind == :field
          [right, left]
        else
          raise UnsupportedError, "#{operator} requires one field and one value"
        end
        resolved = mapper.resolve_field(field.name)
        raise UnsupportedError, "Hierarchy fields cannot be collection relations" if resolved.relation&.type == :many

        path = resolved.path
        ancestors = prefixes(value.value, value.separator)
        descendent = Guards.field_filter(path, {"$regex" => "^#{Regex.escape(value.value + value.separator)}"})

        filter = if operator == "overlaps"
          {"$or" => [Guards.field_filter(path, {"$in" => ancestors + [value.value]}), descendent]}
        elsif (operator == "ancestorOf") == (left.kind == :field)
          # ancestorOf(field, value) and descendentOf(value, field) both ask for a field that is
          # an ancestor of the constant; the other two spellings ask for a descendent.
          Guards.field_filter(path, {"$in" => ancestors})
        else
          descendent
        end
        Guards.with_nullable(filter, operands, mapper)
      end

      def parse(operand)
        unless expression_with?(operand, "hierarchy")
          raise UnsupportedError, "Hierarchy operators require hierarchy() operands"
        end

        path, separator_operand = operand.operands
        raise InvalidPlanError, "hierarchy operator requires a path operand" if path.nil?

        # An omitted separator is CEL's default; a present one must be a non-empty string constant.
        separator = "."
        if separator_operand
          unless value?(separator_operand) && string?(separator_operand.value) && !separator_operand.value.empty?
            raise UnsupportedError, "hierarchy separator must be a non-empty string"
          end

          separator = separator_operand.value
        end
        return Operand.new(:field, path.name, nil, separator) if variable?(path)
        return Operand.new(:value, nil, path.value, separator) if value?(path) && string?(path.value)

        raise UnsupportedError, "hierarchy path must be a field or string value"
      end

      # "a.b.c" → ["a", "a.b"]: every proper ancestor of the path.
      def prefixes(value, separator)
        segments = value.split(separator, -1)
        (1...segments.length).map { |count| segments.first(count).join(separator) }
      end
    end
  end
end
