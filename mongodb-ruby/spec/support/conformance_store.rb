# frozen_string_literal: true

# The corpus seeds as MongoDB documents, written with the official driver and no ODM, so what is
# stored is exactly what this file builds: no schema casts a value on the way in.
#
# Only the conformance harness opens a connection. The unit suites never call `collection`, and
# scripts/test.sh starts no server for them.
module ConformanceStore
  COLLECTION = "conformance_resources"

  module_function

  def client
    @client ||= Mongo::Client.new(
      ENV.fetch("MONGODB_URI") { raise "MONGODB_URI is not set: run this suite through scripts/test.sh" },
      server_selection_timeout: 10,
      logger: Logger.new(File::NULL)
    )
  end

  def collection = client[COLLECTION]

  def establish!
    collection.drop
    collection.insert_many(ConformanceCorpus::SEEDS.map { |seed| document(seed) })
  end

  # @return [Array<String>] the corpus ids of the documents +filter+ selects, sorted
  def ids(filter)
    collection.find(filter, projection: {resourceId: 1, _id: 0}).map { |doc| doc.fetch("resourceId") }.sort
  end

  def document(seed)
    created_at = ConformanceCorpus.derived(seed, "createdAt")
    updated_at = ConformanceCorpus.derived(seed, "updatedAt")
    {
      "resourceId" => seed.fetch("id"),
      "aBool" => seed.fetch("aBool"),
      "aString" => seed.fetch("aString"),
      "aNumber" => seed.fetch("aNumber"),
      "aDouble" => ConformanceCorpus.derived(seed, "aDouble")&.to_f,
      "aOptionalString" => seed.fetch("aOptionalString"),
      "createdBy" => ConformanceCorpus.derived(seed, "createdBy"),
      "scope" => ConformanceCorpus.derived(seed, "scope"),
      "createdAt" => created_at && Time.iso8601(created_at).utc,
      "updatedAt" => updated_at && Time.iso8601(updated_at).utc,
      "tags" => seed.fetch("tags").map { |tag| {"id" => tag.fetch("id"), "name" => tag.fetch("name")} },
      "categories" => seed.fetch("subCategoryNames").map { |name|
        {
          "name" => "business",
          "subCategories" => [
            {"name" => name, "labels" => ConformanceCorpus.derived(seed, "labels").map { |label| {"name" => label} }}
          ]
        }
      },
      "parent" => stored_parent(seed),
      # Verbatim, null elements included: a null element is a value CEL compares.
      "aNumberList" => seed.fetch("aNumberList"),
      "aBoolList" => seed.fetch("aBoolList")
    }
  end

  # The to-one chain as embedded subdocuments (ADR 0005). Every document owns a FRESH copy of
  # the named seed's scalars, so a filter that read the parent instead of the child cannot agree
  # with the recorded decisions by accident. A seed with no parent stores `parent: null`, a missing path to
  # a filter exactly as the absent attribute is to check().
  def stored_parent(seed)
    parent = ConformanceCorpus.parent_seed_of(seed)
    return nil if parent.nil?

    inner = ConformanceCorpus.parent_seed_of(parent)
    relation_level(parent).merge("inner" => inner && relation_level(inner))
  end

  def relation_level(seed)
    %w[aBool aString aNumber aOptionalString].to_h { |key| [key, seed.fetch(key)] }
  end
end
