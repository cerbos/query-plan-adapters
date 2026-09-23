# frozen_string_literal: true

require_relative "errors"

module Cerbos
  module ActiveRecord
    # Maps a plan `variable` name (e.g. `"request.resource.attr.ownerId"`) to the model, as a
    # scalar {Field} or a collection {Relation}.
    #
    #     MAPPING = {
    #       "request.resource.attr.ownerId"    => Cerbos::ActiveRecord.field("owner_id"),
    #       "request.resource.attr.department" => Cerbos::ActiveRecord.field("owner.department"),
    #       "request.resource.attr.tags"       => Cerbos::ActiveRecord.relation(
    #         :tags, member_field: "name", fields: {"name" => Cerbos::ActiveRecord.field("name")}
    #       )
    #     }
    #
    # An unmapped variable, or a mapping its operator cannot use, raises
    # {UnmappedAttributeError}. The adapter never guesses a column.
    module AttributeMapping
      # How the caller sends a NULL column to Cerbos. See {Cerbos::ActiveRecord.field}.
      NULL_REPRESENTATIONS = %i[explicit omitted].freeze

      # A scalar mapping.
      #
      # `path` is a column, or a dotted path through `belongs_to` / `has_one` associations
      # (e.g. `"owner.department"`). A path becomes a correlated scalar subquery, so it cannot
      # add rows.
      #
      # `null_representation` overrides the call's `null_attribute_representation` for this
      # attribute; `nil` keeps the call's value.
      #
      # Make one with {Cerbos::ActiveRecord.field}.
      #
      # @attr path [String] the column, or a dotted path through to-one associations.
      # @attr null_representation [Symbol, nil] `:explicit`, `:omitted`, or `nil` for the
      #   convention of the call.
      Field = Struct.new(:path, :null_representation) do
        # Specify a scalar mapping.
        #
        # @param path [String, Symbol] the column, or a dotted path through to-one associations.
        # @param null_representation [Symbol, String, nil] `:explicit`, `:omitted`, or `nil`.
        #
        # @raise [ArgumentError] when `path` is `nil` or `null_representation` is invalid.
        def initialize(path:, null_representation: nil)
          raise ArgumentError, "path is required" if path.nil?

          unless null_representation.nil? ||
              NULL_REPRESENTATIONS.include?(null_representation.to_sym)
            raise ArgumentError,
              "null_representation must be :explicit or :omitted, got " \
              "#{null_representation.inspect}"
          end

          super(path: path.to_s, null_representation: null_representation&.to_sym)
        end

        # @return [Array<String>] the path in parts: the association hops and then the column.
        def segments
          @segments ||= path.split(".").freeze
        end
      end

      # A collection mapping.
      #
      # `association` is a collection association on the owning model. `through:` chains
      # become joins in one correlated subquery. Map a `has_one` as a dotted {Field} path.
      #
      # `member_field` is the column used when the collection is a list of plain values, so
      # `"urgent" in R.attr.tags` compares `tag.name`, not the `Tag` record.
      #
      # `fields` maps member names used in lambda bodies, e.g. `"name"` for
      # `R.attr.tags.exists(t, t.name == "x")`. An entry can be a relation, for multi-hop
      # chains like `R.attr.categories.exists(c, c.subCategories.exists(s, ...))`.
      #
      # Make one with {Cerbos::ActiveRecord.relation}.
      #
      # @attr association [Symbol] the association name.
      # @attr member_field [String, nil] the column for a bare element value.
      # @attr fields [Hash{String => Field, Relation}] the member mappings for lambda bodies.
      Relation = Struct.new(:association, :member_field, :fields) do
        # Specify a collection mapping.
        #
        # @param association [String, Symbol] the association name.
        # @param member_field [String, Symbol, nil] the column for a bare element value.
        # @param fields [Hash{String, Symbol => Field, Relation}] the member mappings for lambda
        #   bodies.
        #
        # @raise [ArgumentError] when `association` is `nil` or a `fields` value is not a mapping.
        def initialize(association:, member_field: nil, fields: {})
          raise ArgumentError, "association is required" if association.nil?

          fields.each do |name, mapping|
            unless mapping.is_a?(Field) || mapping.is_a?(Relation)
              raise ArgumentError, "fields[#{name.inspect}] must be a field or relation mapping"
            end
          end

          super(
            association: association.to_sym,
            member_field: member_field&.to_s,
            fields: fields.transform_keys(&:to_s).freeze
          )
        end
      end
    end

    # Makes a scalar {AttributeMapping::Field} mapping.
    #
    # @param path [String, Symbol] a column, or a dotted path through to-one associations.
    # @param null_representation [Symbol, nil] how a NULL column is sent: `:explicit` (as a
    #   null value) or `:omitted` (not sent). `nil` (default) uses the call's value. Declaring
    #   it makes `eq`, `ne` and `in` match the PDP on NULL rows
    #   (cerbos/query-plan-adapters#308).
    #
    # @return [AttributeMapping::Field]
    #
    # @raise [ArgumentError] when `path` is `nil` or `null_representation` is invalid.
    #
    # @example A column on the model
    #   Cerbos::ActiveRecord.field("owner_id")
    #
    # @example A column on a to-one parent, sent as an explicit null
    #   Cerbos::ActiveRecord.field("owner.department", null_representation: :explicit)
    def self.field(path, null_representation: nil)
      AttributeMapping::Field.new(path: path, null_representation: null_representation)
    end

    # Makes a collection {AttributeMapping::Relation} mapping.
    #
    # @param association [String, Symbol] the association name on the model that owns it.
    # @param member_field [String, Symbol, nil] the column for a bare element value.
    # @param fields [Hash{String => AttributeMapping::Field, AttributeMapping::Relation}]
    #   the member mappings for lambda bodies.
    #
    # @return [AttributeMapping::Relation]
    #
    # @raise [ArgumentError] when `association` is `nil` or a `fields` value is not a mapping.
    #
    # @example Tags compared by name
    #   Cerbos::ActiveRecord.relation(
    #     :tags, member_field: "name", fields: {"name" => Cerbos::ActiveRecord.field("name")}
    #   )
    #
    # @example A chain through a parent
    #   Cerbos::ActiveRecord.relation(:categories, fields: {
    #     "subCategories" => Cerbos::ActiveRecord.relation(:sub_categories, fields: {
    #       "name" => Cerbos::ActiveRecord.field("name")
    #     })
    #   })
    def self.relation(association, member_field: nil, fields: {})
      AttributeMapping::Relation.new(association: association, member_field: member_field, fields: fields)
    end
  end
end
