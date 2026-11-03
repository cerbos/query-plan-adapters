# frozen_string_literal: true

require_relative "errors"

module Cerbos
  module Sequel
    # Maps one Cerbos attribute reference to the Sequel model. The attribute reference is the
    # +variable+ name in a query plan, for example <tt>"request.resource.attr.ownerId"</tt>.
    # The mapping is a scalar {Field} or a collection {Association}.
    #
    #   MAPPING = {
    #     "request.resource.attr.ownerId"    => Cerbos::Sequel.field("owner_id"),
    #     "request.resource.attr.department" => Cerbos::Sequel.field("owner.department"),
    #     "request.resource.attr.tags"       => Cerbos::Sequel.association(
    #       :tags, member_field: "name", fields: {"name" => Cerbos::Sequel.field("name")}
    #     )
    #   }
    #
    # If the attribute map does not contain a plan variable, the adapter raises
    # {UnmappedAttributeError}. It also raises that error if the operator around the variable
    # cannot use the mapping. The adapter never selects a column by itself.
    module AttributeMapping
      # How the caller sends a NULL column to Cerbos. See
      # {Cerbos::Sequel.field} and {Cerbos::Sequel.query_plan_to_dataset}.
      NULL_REPRESENTATIONS = %i[explicit omitted].freeze

      # A scalar mapping.
      #
      # +path+ is a column on the model. It can also be a path with dots through +many_to_one+
      # or +one_to_one+ associations, for example <tt>"owner.department"</tt>. The adapter
      # translates such a path into a correlated scalar subquery. Thus the path cannot
      # increase the number of rows in the result.
      #
      # +null_representation+ declares how the caller sends THIS column when it is NULL. It
      # overrides the +null_attribute_representation+ of the call for this attribute only.
      # A mapping that declares nothing keeps the value of the call.
      Field = Struct.new(:path, :null_representation) do
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
      # +association+ is the name of a +one_to_many+ or +many_to_many+ association on the model
      # that owns it. The adapter opens a +many_to_many+ into its join table and its target, in
      # one correlated subquery.
      #
      # +member_field+ replaces the element when the policy uses the collection as a list of
      # simple values. Thus <tt>"urgent" in R.attr.tags</tt> compares with <tt>tag.name</tt>
      # and not with the +Tag+ record.
      #
      # +fields+ maps the member names in the bodies of lambdas. For example,
      # <tt>R.attr.tags.exists(t, t.name == "x")</tt> needs <tt>"name"</tt>. An entry in
      # +fields+ can be an association. This is how the adapter resolves a chain with more
      # than one hop, for example
      # <tt>R.attr.categories.exists(c, c.subCategories.exists(s, ...))</tt>.
      Association = Struct.new(:association, :member_field, :fields) do
        def initialize(association:, member_field: nil, fields: {})
          raise ArgumentError, "association is required" if association.nil?

          fields.each do |name, mapping|
            unless mapping.is_a?(Field) || mapping.is_a?(Association)
              raise ArgumentError, "fields[#{name.inspect}] must be a field or association mapping"
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
    # @param path [String, Symbol] a column name, or a path with dots through to-one
    #   associations
    # @param null_representation [Symbol, nil] +:explicit+ if the caller sends this column as
    #   an attribute whose value is null when the column is NULL, +:omitted+ if the caller
    #   sends no attribute at all then. The default, +nil+, keeps the value that the call
    #   gives. Declare it to make +eq+, +ne+ and +in+ against this column agree with the PDP
    #   for the rows where it is NULL (cerbos/query-plan-adapters#308).
    # @return [AttributeMapping::Field]
    def self.field(path, null_representation: nil)
      AttributeMapping::Field.new(path: path, null_representation: null_representation)
    end

    # Makes a collection {AttributeMapping::Association} mapping.
    #
    # @param association [String, Symbol] the association name on the model that owns it
    # @param member_field [String, Symbol, nil] the member column that replaces a simple
    #   element value
    # @param fields [Hash{String => AttributeMapping::Field, AttributeMapping::Association}]
    #   the member names that the bodies of lambdas use
    # @return [AttributeMapping::Association]
    def self.association(association, member_field: nil, fields: {})
      AttributeMapping::Association.new(
        association: association, member_field: member_field, fields: fields
      )
    end
  end
end
