# frozen_string_literal: true

module Cerbos
  module ActiveRecord
    class Translator
      # Resolves the plan variables with the attribute map from the caller. It also resolves
      # the iterator variables that the collection macros around them connected to a scope.
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

          # A macro over a list of constants binds the iterator to an element of that list.
          # An element is a value and has no fields, so a reference with a dot is an error.
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

        # Resolves a plan variable that walks INTO a mapped relation, for example
        # <tt>request.resource.attr.mainCategory.subCategories</tt>. The longest part of the
        # name that the attribute map holds is the start of the chain, and each remaining part
        # names a nested relation in the +fields:+ of the part before it.
        #
        # @return [Values::Collection, nil] nil when no part of the name is a mapped relation,
        #   so the caller can raise the message for an attribute that has no mapping at all
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

        # Each part after the mapped start must name a nested relation. A nested field would be
        # a scalar read from a collection, and the adapter does not choose one row of a
        # collection by itself.
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

          # A scalar list contains null VALUES, unlike an absent field on a struct element.
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
