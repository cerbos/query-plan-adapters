# frozen_string_literal: true

require "mongoid"

require_relative "adversarial_store"
require_relative "corpus_mapper"

# The corpus collection as an application would declare it in Mongoid: typed scalar fields and
# embedded relations for the nested documents. The types are what make it a test — Mongoid
# converts a query constant to the declared type of its field, and a type is exactly what
# turns `flag == 1` into `flag == true`. spec/adversarial_conformance_spec.rb runs every corpus
# action through Cerbos::MongoDB::Mongoid.criteria on these models against the real server.
#
# Loading this file opens no connection: Mongoid connects on the first query, and the offline
# suites never issue one.
module AdversarialMongoid
  class Tag
    include ::Mongoid::Document

    embedded_in :resource, class_name: "AdversarialMongoid::Resource"
    field :id, type: String
    field :name, type: String
  end

  class Label
    include ::Mongoid::Document

    embedded_in :sub_category, class_name: "AdversarialMongoid::SubCategory"
    field :name, type: String
  end

  class SubCategory
    include ::Mongoid::Document

    embedded_in :category, class_name: "AdversarialMongoid::Category"
    field :name, type: String
    embeds_many :labels, class_name: "AdversarialMongoid::Label"
  end

  class Category
    include ::Mongoid::Document

    embedded_in :resource, class_name: "AdversarialMongoid::Resource"
    field :name, type: String
    embeds_many :subCategories, class_name: "AdversarialMongoid::SubCategory"
  end

  class Inner
    include ::Mongoid::Document

    embedded_in :parent, class_name: "AdversarialMongoid::Parent"
    field :aBool, type: ::Mongoid::Boolean
    field :aString, type: String
    field :aNumber, type: Integer
    field :aOptionalString, type: String
  end

  class Parent
    include ::Mongoid::Document

    embedded_in :resource, class_name: "AdversarialMongoid::Resource"
    field :aBool, type: ::Mongoid::Boolean
    field :aString, type: String
    field :aNumber, type: Integer
    field :aOptionalString, type: String
    embeds_one :inner, class_name: "AdversarialMongoid::Inner"
  end

  class Resource
    include ::Mongoid::Document

    store_in collection: AdversarialStore::COLLECTION

    field :resourceId, type: String
    field :aBool, type: ::Mongoid::Boolean
    field :aString, type: String
    field :aNumber, type: Integer
    field :aDouble, type: Float
    field :aOptionalString, type: String
    field :createdBy, type: String
    field :scope, type: String
    field :createdAt, type: Time
    field :updatedAt, type: Time
    field :aNumberList, type: Array
    field :aBoolList, type: Array
    embeds_many :tags, class_name: "AdversarialMongoid::Tag"
    embeds_many :categories, class_name: "AdversarialMongoid::Category"
    embeds_one :parent, class_name: "AdversarialMongoid::Parent"
  end

  module_function

  # Points Mongoid at +uri+. The offline suites pass an address nothing listens on, so a query
  # issued by mistake fails instead of reaching a server.
  def configure!(uri)
    ::Mongoid.configure do |config|
      config.clients.default = {uri: uri, options: {server_selection_timeout: 10}}
      config.logger = Logger.new(File::NULL)
    end
  end

  # Cast probes: a constant of another type than the field it is compared with, on fields the
  # mapper does NOT declare a value_type for. CEL's heterogeneous equality is false (and an
  # ordering across types an error), so these select nothing — or, negated, everything — while
  # a Mongoid query that converted the constant to the field's type selects something else.
  UNTYPED_MAPPER = CorpusMapper::MAPPER.transform_values { |config| config.except(:value_type) }.freeze

  def self.probe(operator, attribute, constant)
    {"expression" => {"operator" => operator, "operands" => [
      {"variable" => "request.resource.attr.#{attribute}"}, {"value" => constant}
    ]}}
  end

  CAST_PROBES = {
    "aBool == 1" => probe("eq", "aBool", 1),
    "aBool == \"true\"" => probe("eq", "aBool", "true"),
    "aBool != 0" => probe("ne", "aBool", 0),
    "aString == 5" => probe("eq", "aString", 5),
    "aNumber == \"5\"" => probe("eq", "aNumber", "5"),
    "aNumber < \"3\"" => probe("lt", "aNumber", "3"),
    "aDouble > -1e19" => probe("gt", "aDouble", -1.0e19),
    "createdAt == \"2024-01-01T00:00:00Z\"" => probe("eq", "createdAt", "2024-01-01T00:00:00Z")
  }.freeze
end
