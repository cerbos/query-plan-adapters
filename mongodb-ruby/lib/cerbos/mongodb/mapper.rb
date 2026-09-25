# frozen_string_literal: true

require_relative "errors"

module Cerbos
  module MongoDB
    # How Cerbos attribute references map onto document paths.
    #
    # The caller supplies either a Hash keyed by the plan's variable name
    # (+"request.resource.attr.title"+) or anything that responds to +call(reference)+ and
    # returns the same config Hash or +nil+. A config is:
    #
    #   {
    #     field: "title",                 # the document path, dotted for a subdocument
    #     nullable: true,                 # a stored null IS a missing Cerbos attribute; left
    #                                     # undeclared, it follows the call's convention
    #     value_parser: ->(v) { ... },    # rewrites each constant compared with this field
    #     value_type: :string,            # :number, :string, :boolean or :date_time
    #     relation: {
    #       name: "tags",                 # the path of the subdocument or array
    #       type: :many,                  # :one (subdocument) or :many (array of subdocuments)
    #       field: "name",                # the element field the relation stands for, if any
    #       requires_parent: "categories",# an optional to-ONE parent array this path goes through
    #       fields: {"name" => {field: "name"}}
    #     }
    #   }
    #
    # Keys may be Symbols or Strings. An unknown key raises {MapperError}: a misspelt +nullable+
    # silently ignored would drop a guard, which is an over-grant rather than a typo.
    #
    # An entry that does not declare +nullable+ takes the mapper's +nullable_default+, which is
    # the call's +null_attribute_representation+: false under +:explicit+, true under +:omitted+.
    # Under +:omitted+ a NULL field sends no attribute and CEL denies the document on a
    # missing-attribute error, while MongoDB's +$ne+ and +$nor+ match a document the path is
    # absent from or null in, so refusing null operands alone left <tt>R.attr.x != "a"</tt>
    # returning those documents (cerbos/query-plan-adapters#493). +nullable: false+ still opts an
    # entry out.
    class Mapper
      Config = Struct.new(:field, :nullable, :value_parser, :value_type, :relation)
      Relation = Struct.new(:name, :type, :field, :requires_parent, :fields)

      CONFIG_KEYS = %i[field nullable value_parser value_type relation].freeze
      RELATION_KEYS = %i[name type field requires_parent fields].freeze
      VALUE_TYPES = %i[number string boolean date_time].freeze
      RELATION_TYPES = %i[one many].freeze

      # @param source [Hash, #call, Mapper]
      # @param nullable_default [Boolean] whether an entry that does not declare +nullable+ is
      #   nullable
      def self.wrap(source, nullable_default: false)
        return source.with_nullable_default(nullable_default) if source.is_a?(Mapper)
        if source.respond_to?(:call)
          return new(->(reference) { source.call(reference) }, nullable_default: nullable_default)
        end
        raise MapperError, "mapper must be a Hash or respond to #call, got #{source.class}" unless source.is_a?(Hash)

        entries = source.to_h { |key, config| [key.to_s, normalise_config(config, key.to_s)] }
        new(->(reference) { entries[reference] }, nullable_default: nullable_default)
      end

      # @return [Config, nil]
      def self.normalise_config(config, label)
        return nil if config.nil?
        return config if config.is_a?(Config)
        raise MapperError, "mapper entry #{label} must be a Hash, got #{config.class}" unless config.is_a?(Hash)

        config = symbolise(config, CONFIG_KEYS, "mapper entry #{label}")
        field = config[:field]
        raise MapperError, "mapper entry #{label}: field must be a String" unless field.nil? || field.is_a?(String)
        nullable = config[:nullable]
        raise MapperError, "mapper entry #{label}: nullable must be true or false" unless [true, false, nil].include?(nullable)
        parser = config[:value_parser]
        unless parser.nil? || parser.respond_to?(:call)
          raise MapperError, "mapper entry #{label}: value_parser must respond to #call"
        end
        value_type = config[:value_type]&.to_sym
        unless value_type.nil? || VALUE_TYPES.include?(value_type)
          raise MapperError, "mapper entry #{label}: value_type must be one of #{VALUE_TYPES.inspect}"
        end

        Config.new(field, nullable, parser, value_type, normalise_relation(config[:relation], label))
      end

      def self.normalise_relation(relation, label)
        return nil if relation.nil?
        return relation if relation.is_a?(Relation)
        raise MapperError, "mapper entry #{label}: relation must be a Hash" unless relation.is_a?(Hash)

        relation = symbolise(relation, RELATION_KEYS, "mapper entry #{label} relation")
        name = relation[:name]
        raise MapperError, "mapper entry #{label}: relation name must be a non-empty String" unless name.is_a?(String) && !name.empty?
        type = relation[:type]&.to_sym
        unless RELATION_TYPES.include?(type)
          raise MapperError, "mapper entry #{label}: relation type must be :one or :many"
        end
        fields = (relation[:fields] || {}).to_h { |key, nested|
          [key.to_s, normalise_config(nested, "#{label}.fields.#{key}")]
        }

        Relation.new(name, type, relation[:field]&.to_s, relation[:requires_parent]&.to_s, fields)
      end

      def self.symbolise(hash, allowed, label)
        hash = hash.transform_keys(&:to_sym)
        unknown = hash.keys - allowed
        raise MapperError, "#{label} carries unknown keys #{unknown.inspect}" unless unknown.empty?

        hash
      end

      attr_reader :nullable_default

      def initialize(lookup, nullable_default: false)
        @lookup = lookup
        @nullable_default = nullable_default
      end

      # This mapper with +nullable_default+ for every entry that does not declare +nullable+.
      def with_nullable_default(nullable_default)
        return self if nullable_default == @nullable_default

        Mapper.new(@lookup, nullable_default: nullable_default)
      end

      # The caller's entry for exactly +reference+.
      # @return [Config, nil]
      def lookup(reference)
        self.class.normalise_config(@lookup.call(reference), reference)
      end

      # +a.b+ → the +b+ entry in the +fields+ of the relation +a+ maps to, if any.
      def relation_field_config(reference)
        parts = reference.split(".")
        last = parts.pop
        return nil if parts.empty? || last.nil? || last.empty?

        lookup(parts.join("."))&.relation&.fields&.[](last)
      end

      # The mapper entry for a reference: its own, or the one its parent relation declares.
      def resolve_config(reference)
        lookup(reference) || relation_field_config(reference)
      end

      # Whether a stored null in +reference+ is a missing Cerbos attribute: its own declaration,
      # or the mapper's default when it declares none. An unmapped reference is never nullable.
      def nullable?(reference)
        config = resolve_config(reference)
        return false if config.nil?

        config.nullable.nil? ? @nullable_default : config.nullable
      end

      def value_type(reference)
        resolve_config(reference)&.value_type
      end

      def apply_value_parser(reference, value)
        # Unlike resolve_config, a reference's own entry does not shadow its relation's parser.
        parser = lookup(reference)&.value_parser || relation_field_config(reference)&.value_parser
        parser ? parser.call(value) : value
      end

      Resolved = Struct.new(:path, :relation)
      ResolvedRelation = Struct.new(:name, :type, :requires_parent)

      # Resolves a plan variable to a document path, through the relation it belongs to if any.
      # A to-many relation keeps its array segment first so it can be split off.
      #
      # An unmapped reference raises rather than being used verbatim as a document path. A plan
      # reference such as +request.resource.attr.status+ names no field any real document has,
      # and MongoDB's negations match a document the path is absent from: +$ne+ and +$nor+ over a
      # path nothing stores select every document, so a typo or a missing mapper entry turned
      # <tt>R.attr.status != "x"</tt> into a filter returning the whole collection
      # (cerbos/query-plan-adapters#492). A caller whose documents really are shaped like the plan
      # path opts in per reference with an entry that names no field (+{}+ or
      # <tt>{nullable: true}</tt>), or a callable mapper that returns one.
      #
      # @raise [MapperError] if the mapper has no entry for +reference+
      def resolve_field(reference)
        lookup_field(reference) or raise MapperError,
          "No mapper entry for #{reference}: an unmapped reference is not used verbatim as a " \
          "document path, because MongoDB's $ne and $nor match every document a path is absent " \
          "from. Map it to a field, or declare it with an entry to use the plan path as-is."
      end

      # The relation a plan variable is reached through, if any. Unlike {#resolve_field} it does
      # not refuse an unmapped name: the guards that ask it walk every variable in an operand,
      # and the emission site is what refuses the references.
      def relation_of(reference)
        lookup_field(reference)&.relation
      end

      # The mapper a collection macro's lambda body is translated with: the iteration variable
      # (and +variable.field+) resolve against the relation's element +fields+, relative to the
      # element; every other key falls through to this mapper, unmapped if it is unmapped here.
      def scoped(collection_path, variable)
        outer = self
        Mapper.new(lambda { |key|
          next outer.lookup(key) unless key == variable || key.start_with?("#{variable}.")

          relation = outer.lookup(collection_path)&.relation
          if key == variable
            field = relation&.field
            next Config.new(key, nil, nil, nil, nil) if field.nil?

            next relation.fields[field] || Config.new(field, nil, nil, nil, nil)
          end
          element_field = key[(variable.length + 1)..]
          relation&.fields&.[](element_field) || Config.new(element_field, nil, nil, nil, nil)
        }, nullable_default: @nullable_default)
      end

      private

      # The document path +reference+ maps to, or nil when the mapper has no entry for it.
      def lookup_field(reference)
        config = lookup(reference)
        return relation_reference(config.relation, config.relation.field) if config&.relation
        return Resolved.new([config.field], nil) if config&.field

        parts = reference.split(".")
        last = parts.pop
        if !parts.empty? && last && !last.empty?
          parent = lookup(parts.join("."))&.relation
          return relation_reference(parent, parent.fields[last]&.field || last) if parent
        end

        # An entry with neither `field` nor `relation` is the caller's opt-in to the plan path.
        Resolved.new([reference], nil) if config
      end

      def relation_reference(relation, field)
        path = if field.nil?
          [relation.name]
        elsif relation.type == :one
          ["#{relation.name}.#{field}"]
        else
          [relation.name, field]
        end
        Resolved.new(path, ResolvedRelation.new(relation.name, relation.type, relation.requires_parent))
      end
    end
  end
end
