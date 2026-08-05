# frozen_string_literal: true

# Oracle for the shared policy suite: the same differential technique the adversarial
# harness uses, applied to /policies/resource.yaml. Expected id sets are computed by asking
# the PDP about each fixture row rather than being written by hand.
module SharedOracle
  KIND = "resource"

  PRINCIPAL = {
    id: "user1",
    roles: ["USER"],
    attr: {
      "tags" => ["public", "internal"],
      "teams" => ["string", "platform"],
      "context" => "projects:resource4",
      "scope" => "acme.dept"
    }
  }.freeze

  module_function

  def client
    AdversarialOracle.client
  end

  def plan(action)
    client.plan_resources(principal: PRINCIPAL, resource: {kind: KIND}, action: action)
  end

  def allowed_ids(action)
    SharedModels::FIXTURES.select { |fixture|
      client.allow?(principal: PRINCIPAL, resource: check_resource(fixture), action: action)
    }.map { |fixture| fixture[:id] }.sort
  end

  NESTED = {
    "nested1" => {
      "aString" => "test string", "aNumber" => 1, "aBool" => true,
      "aOptionalString" => "nested-optional",
      "nextlevel" => {"aString" => "string next", "aNumber" => 1, "aBool" => true}
    },
    "nested2" => {
      "aString" => "other", "aNumber" => 2, "aBool" => false,
      "aOptionalString" => nil,
      "nextlevel" => {"aString" => "other next", "aNumber" => 2, "aBool" => false}
    },
    "nested3" => {
      "aString" => "test string three", "aNumber" => 3, "aBool" => true,
      "aOptionalString" => "third",
      "nextlevel" => {"aString" => "string next", "aNumber" => 1, "aBool" => true}
    }
  }.freeze

  TAG_NAMES = {"tag1" => "public", "tag2" => "draft", "tag3" => "private"}.freeze

  CATEGORIES = {
    "cat-business" => {
      "name" => "business",
      "subCategories" => [{"name" => "finance", "labels" => [{"name" => "important"}]}]
    },
    "cat-tech" => {
      "name" => "tech",
      "subCategories" => [{"name" => "devops", "labels" => [{"name" => "minor"}]}]
    }
  }.freeze

  # Nullable columns are sent as EXPLICIT nulls, mirroring what a nullable column actually
  # holds. `is-not-set` (`aOptionalString == null`) distinguishes the two: an omitted
  # attribute would make CEL raise (a deny) where SQL's IS NULL is true.
  def check_resource(fixture)
    {
      kind: KIND,
      id: fixture[:id],
      attr: {
        "id" => fixture[:id],
        "aString" => fixture[:a_string],
        "aNumber" => fixture[:a_number],
        "aBool" => fixture[:a_bool],
        "aOptionalString" => fixture[:a_optional_string],
        "scope" => fixture[:scope],
        "createdBy" => fixture[:creator_id],
        "ownedBy" => fixture[:owners],
        "tags" => fixture[:tags].map { |id| {"id" => id, "name" => TAG_NAMES.fetch(id)} },
        "nested" => NESTED.fetch(fixture[:nested]),
        "categories" => fixture[:categories].map { |id| CATEGORIES.fetch(id) }
      }
    }
  end
end
