# frozen_string_literal: true

require_relative "errors"
require_relative "operands"

module Cerbos
  module MongoDB
    # `has(x)`, the presence test the planner emits from Cerbos 0.57.0 for an attribute it does
    # not fold into a guard: whether the caller's attributes carry the key at all.
    #
    # Presence is a property of the NULL convention, not of the field. A document field is
    # present when its path exists ($exists); a field declared +nullable+ (a stored null is a
    # missing attribute) also needs a non-null value. A to-one relation is present when its
    # subdocument is stored and not null. A to-many relation is a list CEL always holds, but
    # nothing says the application sends one for every document, so a presence test over one is
    # refused.
    #
    # `has()` never errors on a missing value, so it is two-valued and its negation is a plain
    # $nor. The one error it raises is selecting through a missing to-one parent (`has(parent.x)`
    # on a parentless document), which denies under both polarities: a negation requires the
    # parent OUTSIDE its $nor, and the three-valued evaluation (Logic) answers null there.
    module Presence
      extend Operands

      # +path+ is the document path tested, +not_null+ whether a stored null counts as absent,
      # and +receiver+ the path of the to-one parent the selection reads through, if any.
      Subject = Struct.new(:path, :not_null, :receiver)

      module_function

      # The single attribute reference +operands+ hold.
      def reference(operands)
        operand = operands.first
        unless operands.length == 1 && variable?(operand)
          raise InvalidPlanError, "has takes exactly one attribute reference"
        end

        operand
      end

      # @return [Subject]
      # @raise [UnsupportedError] for a to-many relation, or a path reached through one
      def subject(name, mapper)
        resolved = mapper.resolve_field(name)
        relation = resolved.relation
        if relation && relation.type != :one
          raise UnsupportedError,
            "Cannot translate has(#{name}): a presence test over a to-many relation has no filter " \
            "that matches what the application sends for it"
        end

        path = resolved.path.join(".")
        names_relation = !relation.nil? && path == relation.name
        receiver = if relation && !names_relation
          relation.name
        elsif names_relation
          # `has(parent.inner)`: the selection reads through `parent`, when that is a relation.
          parent, _, = name.rpartition(".")
          outer = parent.empty? ? nil : mapper.relation_of(parent)
          (outer&.type == :one) ? outer.name : nil
        end
        Subject.new(path, names_relation || mapper.nullable?(name), receiver)
      end

      # The filter selecting the documents on which `has(name)` is true, or, +negated+, false.
      def filter(name, mapper, negated)
        subject = subject(name, mapper)
        presence = {subject.path => subject.not_null ? {"$ne" => nil} : {"$exists" => true}}
        return presence unless negated

        absence = {"$nor" => [presence]}
        subject.receiver ? {"$and" => [{subject.receiver => {"$ne" => nil}}, absence]} : absence
      end

      # Whether +operand+ holds a presence test anywhere below it.
      def within?(operand)
        expression?(operand) && (operand.operator == "has" || operand.operands.any? { |child| within?(child) })
      end
    end
  end
end
