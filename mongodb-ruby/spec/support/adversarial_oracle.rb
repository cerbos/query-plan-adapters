# frozen_string_literal: true

# The oracle part of the differential harness. For each document, it asks the same PDP that made
# the query plan if the action is permitted. The attributes in that question are the same as
# the data in the document. No person writes the expected results for either side.
module AdversarialOracle
  module_function

  # The PDP scripts/test.sh started for THIS run, on a port Docker chose. There is deliberately
  # no default: a fixed address is how a suite ends up planning against another run's PDP
  # (cerbos/query-plan-adapters#476), possibly on another corpus revision or evaluation mode.
  def client
    @client ||= Cerbos::Client.new(
      ENV.fetch("CERBOS_HOST") { raise "CERBOS_HOST is not set: run this suite through scripts/test.sh" },
      tls: false
    )
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

  # A stored null is a missing attribute for the check call. If a condition uses a missing
  # attribute, CEL raises, and Cerbos denies the document under BOTH polarities. A MongoDB
  # filter has no UNKNOWN, which is why the adapter's `nullable` guards exist.
  #
  # `owner`, `coOwner`, `tagNames`, `aNumberList` and `aBoolList` are the exceptions. They hold
  # explicit nulls, because a CEL membership test finds a difference between a null element and
  # a missing element, and because the equality family answers a null VALUE definitely while a
  # missing attribute denies under both polarities (cerbos/query-plan-adapters#308).
  def check_resource(seed)
    attr = {
      "aBool" => seed.fetch("aBool"),
      "aString" => seed.fetch("aString"),
      "aNumber" => seed.fetch("aNumber"),
      "createdBy" => ConformanceCorpus.created_by(seed),
      "obj" => {"inner" => seed.fetch("aString")},
      "tags" => seed.fetch("tags").map { |tag| tag_attr(tag) },
      "owner" => seed.fetch("aOptionalString"),
      # The explicit-null alias of the `scope` field. `scope` below is omitted when it is
      # NULL, so the same field reaches the PDP under both conventions.
      "coOwner" => ConformanceCorpus.scope(seed),
      "tagNames" => seed.fetch("tags").map { |tag| tag.fetch("name") },
      # Verbatim, null elements and element order included: `[null, 2]` on a6 is a list whose
      # first element is the null VALUE, and `null == 2` is false rather than an error.
      "aNumberList" => seed.fetch("aNumberList"),
      "aBoolList" => seed.fetch("aBoolList"),
      "categories" => seed.fetch("subCategoryNames").map { |name| category_attr(seed, name) }
    }

    # The real to-one chain (ADR 0005). A level that does not exist sends NO attribute, so CEL
    # raises a missing-path error and check() denies — the same rule the root document follows for a
    # NULL field.
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

    # mainCategory shows the category graph of the document as one nested object. The seeder
    # makes at most one category for each document. A document without a category gets no
    # attribute, so CEL denies it because the attribute is missing; the mapper declares
    # `requires_parent` so the filter keeps that document out as well (#309).
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
