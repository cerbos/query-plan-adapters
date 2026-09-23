# frozen_string_literal: true

# The oracle: asks the PDP that made the plan whether each row is allowed, using the row's own
# data as attributes. No expected results are hand-written.
module AdversarialOracle
  module_function

  def client
    @client ||= Cerbos::Client.new(ENV.fetch("CERBOS_HOST", "cerbos:3593"), tls: false)
  end

  # Passed through verbatim. An allowlist would drop a new attribute from both the plan and the
  # oracle, and the action would pass without proving anything.
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

  # A NULL column is sent as a missing attribute. CEL errors on it and Cerbos denies, which
  # matches SQL's UNKNOWN (`NOT (NULL = x)` is still UNKNOWN, not TRUE).
  #
  # Exceptions: `owner`, `coOwner`, `tagNames`, `aNumberList` and `aBoolList` send explicit
  # nulls. CEL treats a null value differently from a missing one (#308).
  def check_resource(seed)
    attr = {
      "aBool" => seed.fetch("aBool"),
      "aString" => seed.fetch("aString"),
      "aNumber" => seed.fetch("aNumber"),
      "createdBy" => ConformanceCorpus.created_by(seed),
      "obj" => {"inner" => seed.fetch("aString")},
      "tags" => seed.fetch("tags").map { |tag| tag_attr(tag) },
      "owner" => seed.fetch("aOptionalString"),
      # Explicit-null alias of `scope`, which is omitted when NULL. Same column, both
      # conventions.
      "coOwner" => ConformanceCorpus.scope(seed),
      "tagNames" => seed.fetch("tags").map { |tag| tag.fetch("name") },
      # Verbatim, nulls and order kept: on a6, `[null, 2]` starts with a null value, and
      # `null == 2` is false, not an error.
      "aNumberList" => seed.fetch("aNumberList"),
      "aBoolList" => seed.fetch("aBoolList"),
      "categories" => seed.fetch("subCategoryNames").map { |name| category_attr(seed, name) }
    }

    # The real to-one chain (ADR 0005). A missing level sends no attribute, so check() denies,
    # like a NULL column on the root row.
    parent_seed = ConformanceCorpus.parent_seed_of(seed)
    if parent_seed
      parent_attr = ConformanceCorpus.relation_attr(parent_seed)
      inner_seed = ConformanceCorpus.parent_seed_of(parent_seed)
      parent_attr["inner"] = ConformanceCorpus.relation_attr(inner_seed) if inner_seed
      attr["parent"] = parent_attr
    end

    optional_string = seed.fetch("aOptionalString")
    attr["aOptionalString"] = optional_string unless optional_string.nil?

    double = ConformanceCorpus.a_double(seed)
    attr["aDouble"] = double unless double.nil?

    scope = ConformanceCorpus.scope(seed)
    attr["scope"] = scope unless scope.nil?

    created_at = ConformanceCorpus.created_at(seed)
    attr["createdAt"] = created_at unless created_at.nil?
    updated_at = ConformanceCorpus.updated_at(seed)
    attr["updatedAt"] = updated_at unless updated_at.nil?

    # The row's category graph as one object (each row has at most one category). A row with
    # no category gets no attribute, so CEL denies it; the adapter's join finds nothing either.
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
