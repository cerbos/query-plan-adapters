# frozen_string_literal: true

module Cerbos
  module ActiveRecord
    class Translator
      # Resolves plan variables through the caller's attribute map, and macro iterator
      # variables through their bound scope.
      #
      # @private
      class Environment
        def initialize(translator:, bindings:)
          @translator = translator
          @bindings = bindings
        end

        attr_reader :translator, :bindings

        def bind(name, scope)
          self.class.new(translator: translator, bindings: bindings.merge(name => scope))
        end

        def resolve(name)
          mapping = translator.attributes[name]
          return resolve_mapping(mapping, translator.model, translator.root_table) if mapping

          head, rest = name.split(".", 2)
          unless bindings.key?(head)
            chained = resolve_chain(name)
            return chained unless chained.nil?

            raise UnmappedAttributeError,
              "No mapping for attribute #{name.inspect}. Add it to the attributes hash " \
              "passed to Cerbos::ActiveRecord.query_plan_to_relation."
          end

          scope = bindings[head]

          # Bound to a constant list element, which has no fields.
          unless scope.is_a?(Relations::Scope)
            return scope if rest.nil?

            raise UnmappedAttributeError,
              "#{name.inspect} reads the field #{rest.inspect} from #{head.inspect}, but " \
              "#{head.inspect} is an element of a list of constants and has no fields"
          end

          return element(scope) if rest.nil?

          member = scope.mapping&.fields&.[](rest)
          unless member
            raise UnmappedAttributeError,
              "Relation #{scope.mapping&.association.inspect} has no mapping for member " \
              "field #{rest.inspect} (referenced as #{name.inspect})"
          end

          resolve_mapping(member, scope.model, scope.table)
        end

        private

        # Resolves a path into a mapped relation, such as `R.attr.mainCategory.subCategories`:
        # the longest mapped prefix starts the chain, and each later part is a nested relation.
        #
        # @return [Values::Collection, nil] nil when no prefix is a mapped relation
        def resolve_chain(name)
          segments = name.split(".")

          (segments.length - 1).downto(1) do |length|
            mapping = translator.attributes[segments.take(length).join(".")]
            next unless mapping.is_a?(AttributeMapping::Relation)

            scope = Relations.build(
              owner_model: translator.model,
              owner_table: translator.root_table,
              mapping: mapping,
              aliaser: translator.aliaser
            )
            return walk_members(scope, segments.drop(length), name)
          end

          nil
        end

        # Each part must be a nested relation. A scalar field would mean picking one row of a
        # collection, which the adapter will not guess.
        def walk_members(scope, segments, name)
          segments.each do |segment|
            member = scope.mapping.fields[segment]
            unless member.is_a?(AttributeMapping::Relation)
              raise UnmappedAttributeError,
                "#{name.inspect} reads #{segment.inspect} from relation " \
                "#{scope.mapping.association.inspect}, which maps it to " \
                "#{member.nil? ? "nothing" : "a scalar field"}. Every step of a path through " \
                "a relation must name a nested relation mapping."
            end

            scope = Relations.chain(
              outer_scope: scope, mapping: member, aliaser: translator.aliaser
            )
          end

          Values::Collection.new(scope: scope)
        end

        def element(scope)
          return Values::Collection.new(scope: scope) unless scope.mapping&.member_field

          # A scalar list holds null values, unlike a missing field on a struct element.
          translator.register_null_representation(scope.member_column, :explicit)
        end

        def resolve_mapping(mapping, owner_model, owner_table)
          case mapping
          when AttributeMapping::Field then resolve_field(mapping, owner_model, owner_table)
          when AttributeMapping::Relation
            Values::Collection.new(
              scope: Relations.build(
                owner_model: owner_model,
                owner_table: owner_table,
                mapping: mapping,
                aliaser: translator.aliaser
              )
            )
          else
            raise UnmappedAttributeError, "Unrecognised attribute mapping: #{mapping.inspect}"
          end
        end

        def resolve_field(mapping, owner_model, owner_table)
          *associations, column = mapping.segments
          node, model =
            if associations.empty?
              [owner_table[column], owner_model]
            else
              scope = Relations.build_path(
                owner_model: owner_model,
                owner_table: owner_table,
                association_names: associations,
                aliaser: translator.aliaser
              )
              [scope.scalar(column), scope.model]
            end

          translator.register_null_representation(
            translator.register_column_type(node, model, column), mapping.null_representation
          )
        end
      end
    end
  end
end
