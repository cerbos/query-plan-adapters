# frozen_string_literal: true

# The oracle part of the differential harness. For each row, it asks the same PDP that made
# the query plan if the action is permitted. The attributes in that question are the same as
# the data in the row. No person writes the expected results for either side.
module AdversarialOracle
  module_function

  def client
    @client ||= Cerbos::Client.new(ENV.fetch("CERBOS_HOST", "cerbos:3593"), tls: false)
  end

  # The principal goes through without a change. An allowlist of keys here would drop the
  # attribute that a new action discriminates on from the plan and from the oracle at the same
  # time, and the action would then agree with itself and prove nothing.
  def principal
    ConformanceCorpus::PRINCIPAL.transform_keys(&:to_sym)
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

  # A NULL in the database is a missing attribute for the check call. If a condition uses a
  # missing attribute, CEL makes an error, and Cerbos denies the row. SQL has the same
  # three-valued logic when a NULL is in a comparison. Note that `NOT (NULL = x)` stays
  # UNKNOWN and does not become TRUE.
  #
  # `owner`, `coOwner` and `tagNames` are the three exceptions. They hold explicit nulls,
  # because a CEL membership test finds a difference between a null element and a missing
  # element, and because the equality family answers a null VALUE definitely while a missing
  # attribute denies under both polarities (cerbos/query-plan-adapters#308).
  def check_resource(seed)
    attr = {
      "aBool" => seed.fetch("aBool"),
      "aString" => seed.fetch("aString"),
      "aNumber" => seed.fetch("aNumber"),
      "createdBy" => ConformanceCorpus.created_by(seed),
      "obj" => {"inner" => seed.fetch("aString")},
      "tags" => seed.fetch("tags").map { |tag| tag_attr(tag) },
      "owner" => seed.fetch("aOptionalString"),
      # The explicit-null alias of the `scope` column. `scope` below is omitted when it is
      # NULL, so the same column reaches the PDP under both conventions.
      "coOwner" => ConformanceCorpus.scope(seed),
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

    # mainCategory shows the category graph of the row as one nested object. The seed code
    # makes a maximum of one category for each row. A row without a category gets no attribute.
    # Thus CEL denies it because the attribute is missing. The adapter also keeps that row out
    # of the result, because its chain of joins is empty.
    unless seed.fetch("subCategoryNames").empty?
      attr["mainCategory"] = {
        "name" => "business",
        "subCategories" => seed.fetch("subCategoryNames").map { |name| {"name" => name} },
        "subNames" => seed.fetch("subCategoryNames")
      }
    end

    {kind: ConformanceCorpus::RESOURCE_KIND, id: seed.fetch("id"), attr: attr}
  end

  # A NULL tag name in the database is a missing element attribute for the check call.
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
