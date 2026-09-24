# frozen_string_literal: true

# The one mapper every corpus case is translated through (conformance/README.md: one mapping,
# no per-case options). The conformance harness runs it against a real MongoDB, and
# spec/mongoid_spec.rb reuses it offline, so there is one copy.
#
# The stored document shape it describes is spec/support/conformance_store.rb.
module CorpusMapper
  LABELS = {relation: {name: "labels", type: :many, fields: {"name" => {field: "name", nullable: true}}}}.freeze

  SUB_CATEGORIES = {
    relation: {
      name: "subCategories", type: :many, field: "name",
      fields: {"name" => {field: "name"}, "labels" => LABELS}
    }
  }.freeze

  RELATION_LEVEL_FIELDS = {
    "aBool" => {field: "aBool"},
    "aString" => {field: "aString"},
    "aNumber" => {field: "aNumber"},
    "aOptionalString" => {field: "aOptionalString", nullable: true}
  }.freeze

  MAPPER = {
    # The primary key, reached as `request.resource.id` rather than through `attr`. It maps to
    # `resourceId`, the string field the harness carries the corpus id in, not to `_id`: three of
    # the `identifier/*` cases compare the key with a STRING field, so a single mapping cannot be an
    # ObjectId and satisfy them. An ObjectId `value_parser` is pinned in the contract suite.
    "request.resource.id" => {field: "resourceId"},
    "request.resource.attr.aBool" => {field: "aBool"},
    "request.resource.attr.aString" => {field: "aString", value_type: :string},
    "request.resource.attr.aNumber" => {field: "aNumber", value_type: :number},
    "request.resource.attr.aDouble" => {field: "aDouble", nullable: true},
    "request.resource.attr.aOptionalString" => {field: "aOptionalString", nullable: true},
    "request.resource.attr.createdBy" => {field: "createdBy"},
    "request.resource.attr.scope" => {field: "scope", nullable: true},
    "request.resource.attr.createdAt" => {field: "createdAt", value_type: :date_time, nullable: true},
    "request.resource.attr.updatedAt" => {field: "updatedAt", value_type: :date_time, nullable: true},
    "request.resource.attr.owner" => {field: "aOptionalString"},
    # `coOwner` aliases the `scope` field under the explicit-null convention: check() is sent a
    # real null attribute for it. The driver stores an explicit null and MongoDB compares it as
    # a value, so no `nullable` flag applies — the flag means the opposite.
    "request.resource.attr.coOwner" => {field: "scope"},
    # obj.inner is not a real nested path — it mirrors aString. `parent.inner` below is the
    # opposite: a real two-level to-one chain. The two are kept side by side on purpose.
    "request.resource.attr.obj.inner" => {field: "aString"},
    # The corpus's one REAL to-one chain (the `relation/*` cases), stored as an embedded subdocument
    # per level. `type: :one` flattens the path AND declares the level absent-able, which is
    # what makes the adapter require it outside any $nor.
    "request.resource.attr.parent" => {relation: {name: "parent", type: :one, fields: RELATION_LEVEL_FIELDS}},
    "request.resource.attr.parent.inner" => {relation: {name: "parent.inner", type: :one, fields: RELATION_LEVEL_FIELDS}},
    "request.resource.attr.tags" => {
      relation: {name: "tags", type: :many, fields: {"id" => {field: "id"}, "name" => {field: "name", nullable: true}}}
    },
    "request.resource.attr.tagNames" => {
      relation: {name: "tags", type: :many, field: "name", fields: {"name" => {field: "name"}}}
    },
    # Homogeneous scalar lists stored as native arrays: a plain field, not a relation, because
    # an element is a scalar with no field of its own.
    "request.resource.attr.aNumberList" => {field: "aNumberList"},
    "request.resource.attr.aBoolList" => {field: "aBoolList"},
    "request.resource.attr.categories" => {
      relation: {name: "categories", type: :many, fields: {"name" => {field: "name"}, "subCategories" => SUB_CATEGORIES}}
    },
    "request.resource.attr.mainCategory" => {
      relation: {
        name: "categories", type: :many,
        fields: {"name" => {field: "name"}, "subCategories" => SUB_CATEGORIES, "subNames" => SUB_CATEGORIES}
      }
    },
    # mainCategory is a to-ONE parent on the check side: a seed with no subCategoryNames sends
    # NO mainCategory attribute, so CEL raises and check() denies. The flattened path cannot see
    # that on its own — an absent and a childless parent both give an empty array — so the
    # mapping declares the parent (cerbos/query-plan-adapters#309).
    "request.resource.attr.mainCategory.subCategories" => {
      relation: {name: "categories.subCategories", type: :many, requires_parent: "categories", fields: {"name" => {field: "name"}}}
    },
    "request.resource.attr.mainCategory.subNames" => {
      relation: {
        name: "categories.subCategories", type: :many, field: "name", requires_parent: "categories",
        fields: {"name" => {field: "name"}}
      }
    }
  }.freeze
end
