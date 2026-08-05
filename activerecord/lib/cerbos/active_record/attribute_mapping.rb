# frozen_string_literal: true

require_relative "errors"

module Cerbos
  module ActiveRecord
    # Maps one Cerbos attribute reference — the +variable+ name in a query plan, e.g.
    # <tt>"request.resource.attr.ownerId"</tt> — onto the ActiveRecord model, either as a
    # scalar {Field} or as a collection {Relation}.
    #
    #   MAPPING = {
    #     "request.resource.attr.ownerId"    => Cerbos::ActiveRecord.field("owner_id"),
    #     "request.resource.attr.department" => Cerbos::ActiveRecord.field("owner.department"),
    #     "request.resource.attr.tags"       => Cerbos::ActiveRecord.relation(
    #       :tags, member_field: "name", fields: {"name" => Cerbos::ActiveRecord.field("name")}
    #     )
    #   }
    #
    # Plan variables missing from the mapping, or mapped to something the surrounding
    # operator cannot use, raise {UnmappedAttributeError} — the adapter never guesses a
    # column.
    module AttributeMapping
      # A scalar mapping.
      #
      # +path+ is a column on the model, or a dot-separated path through +belongs_to+ /
      # +has_one+ associations (<tt>"owner.department"</tt>), which the adapter resolves as a
      # correlated scalar subquery so it can never multiply result rows.
      Field = Struct.new(:path) do
        def initialize(path:)
          raise ArgumentError, "path is required" if path.nil?
          super(path: path.to_s)
        end

        # @return [Array<String>] the path split into association hops plus a final column.
        def segments
          @segments ||= path.split(".").freeze
        end
      end

      # A collection mapping.
      #
      # +association+ names a +has_many+ / +has_one+ association on the owning model
      # (including +through:+ chains, which the adapter expands into joins inside one
      # correlated subquery).
      #
      # +member_field+ stands in for the element wherever the policy treats the collection as
      # a list of bare values — <tt>"urgent" in R.attr.tags</tt> compares against
      # <tt>tag.name</tt> rather than the +Tag+ record.
      #
      # +fields+ maps the policy-facing member names used inside lambda bodies
      # (<tt>R.attr.tags.exists(t, t.name == "x")</tt> needs <tt>"name"</tt>) onto the member
      # model. Entries may themselves be relations, which is how multi-hop chains such as
      # <tt>R.attr.categories.exists(c, c.subCategories.exists(s, ...))</tt> resolve.
      Relation = Struct.new(:association, :member_field, :fields) do
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

    # Build a scalar {AttributeMapping::Field} mapping.
    #
    # @param path [String, Symbol] column name, or a dotted path through to-one associations
    # @return [AttributeMapping::Field]
    def self.field(path)
      AttributeMapping::Field.new(path: path)
    end

    # Build a collection {AttributeMapping::Relation} mapping.
    #
    # @param association [String, Symbol] the owning model's association name
    # @param member_field [String, Symbol, nil] member column standing in for a bare element
    # @param fields [Hash{String => AttributeMapping::Field, AttributeMapping::Relation}]
    #   policy-facing member names referenced inside lambda bodies
    # @return [AttributeMapping::Relation]
    def self.relation(association, member_field: nil, fields: {})
      AttributeMapping::Relation.new(association: association, member_field: member_field, fields: fields)
    end
  end
end
