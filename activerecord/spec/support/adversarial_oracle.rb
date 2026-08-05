# frozen_string_literal: true

# The oracle side of the differential harness: for each seeded row, ask the SAME PDP that
# produced the query plan whether it allows the action, with attributes mirroring that row
# exactly. There are no hand-written expectations on either side.
module AdversarialOracle
  module_function

  def client
    @client ||= Cerbos::Client.new(ENV.fetch("CERBOS_HOST", "cerbos:3593"), tls: false)
  end

  def principal
    ConformanceCorpus::PRINCIPAL.slice("id", "roles", "attr").transform_keys(&:to_sym)
  end

  def allowed_ids(action)
    ConformanceCorpus::SEEDS.select { |seed|
      client.allow?(principal: principal, resource: check_resource(seed), action: action)
    }.map { |seed| seed.fetch("id") }.sort
  end

  def plan(action)
    client.plan_resources(
      principal: principal,
      resource: {kind: ConformanceCorpus::RESOURCE_KIND},
      action: action
    )
  end

  # A DB NULL is a MISSING attribute on the check side. CEL raises a missing-attribute error
  # for a condition that touches one, which Cerbos treats as a deny — the same three-valued
  # logic SQL applies when a NULL participates in a comparison. In particular `NOT (NULL = x)`
  # stays UNKNOWN, not TRUE.
  #
  # `owner` and `tagNames` are the deliberate exceptions: they carry EXPLICIT nulls, because
  # CEL membership distinguishes a null element from an absent one.
  def check_resource(seed)
    attr = {
      "aBool" => seed.fetch("aBool"),
      "aString" => seed.fetch("aString"),
      "aNumber" => seed.fetch("aNumber"),
      "createdBy" => ConformanceCorpus.created_by(seed),
      "obj" => {"inner" => seed.fetch("aString")},
      "tags" => seed.fetch("tags").map { |tag| tag_attr(tag) },
      "owner" => seed.fetch("aOptionalString"),
      "tagNames" => seed.fetch("tags").map { |tag| tag.fetch("name") },
      "categories" => seed.fetch("subCategoryNames").map { |name| category_attr(seed, name) }
    }

    optional_string = seed.fetch("aOptionalString")
    attr["aOptionalString"] = optional_string unless optional_string.nil?

    double = ConformanceCorpus.a_double(seed)
    attr["aDouble"] = double unless double.nil?

    scope = ConformanceCorpus.scope(seed)
    attr["scope"] = scope unless scope.nil?

    created_at = ConformanceCorpus.created_at(seed)
    attr["createdAt"] = created_at unless created_at.nil?

    # mainCategory mirrors the row's category graph as ONE nested object (the seeder creates
    # at most one category per seed). A row with no category gets NO attribute at all — a
    # missing-attribute deny, matching the adapter's empty join chain excluding the row.
    unless seed.fetch("subCategoryNames").empty?
      attr["mainCategory"] = {
        "name" => "business",
        "subCategories" => seed.fetch("subCategoryNames").map { |name| {"name" => name} },
        "subNames" => seed.fetch("subCategoryNames")
      }
    end

    {kind: ConformanceCorpus::RESOURCE_KIND, id: seed.fetch("id"), attr: attr}
  end

  # A NULL tag name in the database is a MISSING element attribute on the check side.
  def tag_attr(tag)
    attr = {"id" => tag.fetch("id")}
    name = tag.fetch("name")
    attr["name"] = name unless name.nil?
    attr
  end

  def label_attr(name)
    name.nil? ? {} : {"name" => name}
  end

  def category_attr(seed, sub_name)
    {
      "name" => "business",
      "subCategories" => [
        {
          "name" => sub_name,
          "labels" => ConformanceCorpus.labels(seed).map { |label| label_attr(label) }
        }
      ]
    }
  end
end
