# frozen_string_literal: true

module Cerbos
  module Sequel
    class Translator
      # +has(x)+, the presence test that the planner emits from Cerbos 0.57.0 for an attribute it
      # does not fold into a guard: whether the attributes of the caller carry the key at all.
      #
      # Presence is a property of the NULL convention, not of the column. An attribute sent as an
      # explicit null is present on every row. An attribute omitted when its column is NULL is
      # present exactly when the column is not NULL. A field of an association member follows
      # +:omitted+ unless it declares otherwise, as the collection macros already assume. A to-one
      # association is present when its row exists.
      #
      # +has()+ never makes an error over a missing value, so the predicate has two values and a
      # NOT over it is exact. The one error that it makes is a selection through a missing to-one
      # parent (+has(parent.x)+ on a row with no parent). That error denies under both
      # polarities, so a missing hop gives NULL.
      module Presence
        private

        def presence(operands, environment)
          unless operands.length == 1 && operands.first.is_a?(Plan::Variable)
            raise InvalidPlanError, "has takes exactly one attribute reference"
          end

          name = operands.first.name
          mapping = attributes[name]
          return field_presence(name, mapping, environment) if mapping.is_a?(AttributeMapping::Field)
          return association_presence(name, mapping) if mapping

          member_presence(name, environment)
        end

        # A to-one association is present when its row exists. A collection is a list that CEL
        # always holds, empty or not, but nothing says that the caller sends one for every row,
        # so the adapter refuses it.
        def association_presence(name, mapping)
          reflection = model.association_reflection(mapping.association)
          unless reflection && Relations::TO_ONE_TYPES.include?(reflection[:type])
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
          SqlSupport.case_node([[scope.exists, present]])
        end

        def member_presence(name, environment)
          head, rest = name.split(".", 2)
          scope = environment.bindings[head]
          return unmapped_presence(name, environment) if rest.nil? || scope.nil?

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

        # A name that the attribute map does not hold and no macro binds. Resolving it raises
        # for a missing mapping; a chain through a mapped association resolves to a collection,
        # whose presence depends on every hop the caller sends, so it is refused.
        def unmapped_presence(name, environment)
          resolved = environment.resolve(name)
          return resolved unless collection?(resolved)

          raise UnsupportedOperatorError,
            "has(#{name}) tests a collection reached through an association: a presence test " \
            "over one has no SQL form that matches what the caller sends for it"
        end

        def column_presence(node, omitted)
          omitted ? SqlSupport.comparison("ne", node, nil) : true
        end
      end
    end
  end
end
