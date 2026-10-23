# frozen_string_literal: true

module Cerbos
  module ActiveRecord
    class Translator
      # `has(x)`, the presence test the planner emits from Cerbos 0.57.0 for an attribute it does
      # not fold into a guard: whether the caller's attributes carry the key at all.
      #
      # Presence is a property of the NULL convention, not of the column. An attribute sent as an
      # explicit null is present on every row; one omitted when its column is NULL is present
      # exactly when the column is not NULL. A relation member's field follows `:omitted` unless it
      # declares otherwise, as the collection macros already assume. A to-one association is
      # present when its row exists.
      #
      # `has()` never errors on a missing value, so the predicate is two-valued and NOT over it is
      # exact. The one error it raises is selecting through a missing to-one parent
      # (`has(parent.x)` on a parentless row), which denies under both polarities, so a missing hop
      # gives NULL.
      #
      # @private
      module Presence
        private

        def presence(operands, environment)
          unless operands.length == 1 && operands.first.is_a?(Plan::Variable)
            raise InvalidPlanError, "has takes exactly one attribute reference"
          end

          name = operands.first.name
          mapping = attributes[name]
          return field_presence(name, mapping, environment) if mapping.is_a?(AttributeMapping::Field)
          return relation_presence(name, mapping) if mapping

          member_presence(name, environment)
        end

        # A to-one association is present when its row exists. A collection is a list CEL always
        # holds, empty or not, but nothing says the caller sends one for every row, so it is refused.
        def relation_presence(name, mapping)
          reflection = model.reflect_on_association(mapping.association.to_sym)
          if reflection.nil? || reflection.collection?
            raise UnsupportedOperatorError,
              "has(#{name}) tests a collection association: a presence test over one has no " \
              "SQL form that matches what the caller sends for it"
          end

          Relations.build_path(
            owner_model: model,
            owner_table: root_table,
            association_names: [mapping.association],
            aliaser: aliaser
          ).exists
        end

        def field_presence(name, mapping, environment)
          omitted = (mapping.null_representation || null_attribute_representation) == :omitted
          *associations, column = mapping.segments
          return column_presence(environment.resolve(name), omitted) if associations.empty?

          scope = Relations.build_path(
            owner_model: model,
            owner_table: root_table,
            association_names: associations,
            aliaser: aliaser
          )
          present = omitted ? column_presence(scope.scalar(column), true) : true
          ArelSupport.case_node([[scope.exists, present]])
        end

        def member_presence(name, environment)
          head, rest = name.split(".", 2)
          scope = environment.bindings[head]
          return environment.resolve(name) if rest.nil? || scope.nil?

          if scope.is_a?(Hash)
            *path, key = rest.split(".")
            parent = path.empty? ? scope : constant_field(scope, path.join("."))
            return cel_type_error unless parent.is_a?(Hash)

            return parent.key?(key)
          end
          member = scope.is_a?(Relations::Scope) && scope.mapping&.fields&.[](rest)
          unless member.is_a?(AttributeMapping::Field)
            raise UnsupportedOperatorError,
              "has(#{name}) does not read a mapped field of #{head.inspect}"
          end

          omitted = (member.null_representation || :omitted) == :omitted
          column_presence(environment.resolve(name), omitted)
        end

        def column_presence(node, omitted)
          omitted ? Arel::Nodes::NotEqual.new(node, nil) : true
        end
      end
    end
  end
end
