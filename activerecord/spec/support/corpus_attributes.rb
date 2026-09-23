# frozen_string_literal: true

# The one attribute map this adapter is classified against in conformance/actions.json.
# The conformance harness and the translator unit test both use it, so they cannot disagree.
module CorpusAttributes
  def self.field(path, **kwargs) = Cerbos::ActiveRecord.field(path, **kwargs)

  def self.relation(*args, **kwargs) = Cerbos::ActiveRecord.relation(*args, **kwargs)

  ATTRIBUTES = {
    "request.resource.attr.aBool" => field("a_bool"),
    "request.resource.attr.aString" => field("a_string"),
    "request.resource.attr.aNumber" => field("a_number"),
    "request.resource.attr.aDouble" => field("a_double"),
    "request.resource.attr.aOptionalString" => field("a_optional_string"),
    "request.resource.attr.createdBy" => field("created_by"),
    "request.resource.attr.scope" => field("scope"),
    "request.resource.attr.createdAt" => field("created_at"),
    "request.resource.attr.updatedAt" => field("updated_at"),
    # `owner` and `coOwner` alias the `aOptionalString` and `scope` columns under the explicit
    # null convention: the oracle sends a real null for them (#308).
    "request.resource.attr.owner" => field("a_optional_string", null_representation: :explicit),
    # The second half of `null-value-f2f`, which compares two explicit nulls. Not another
    # alias of `a_optional_string`: a column compared with itself is always TRUE, and the
    # degeneracy guard rejects a total oracle.
    "request.resource.attr.coOwner" => field("scope", null_representation: :explicit),
    # Not a real nested column: it reuses aString's column, as other harnesses do.
    "request.resource.attr.obj.inner" => field("a_string"),

    # `R.id` arrives as its own variable, not under `R.attr`, so it needs its own mapping.
    "request.resource.id" => field("id"),

    # The corpus's one real to-one relation (ADR 0005). Unlike `obj.inner`, this is a join:
    # each hop becomes a scalar subquery, and a missing parent gives NULL, which denies like
    # check()'s missing-path error. No null convention needed: NULL denies either way.
    "request.resource.attr.parent.aBool" => field("parent.a_bool"),
    "request.resource.attr.parent.aString" => field("parent.a_string"),
    "request.resource.attr.parent.aNumber" => field("parent.a_number"),
    "request.resource.attr.parent.aOptionalString" => field("parent.a_optional_string"),
    "request.resource.attr.parent.inner.aBool" => field("parent.inner.a_bool"),
    "request.resource.attr.parent.inner.aString" => field("parent.inner.a_string"),
    "request.resource.attr.parent.inner.aNumber" => field("parent.inner.a_number"),
    "request.resource.attr.parent.inner.aOptionalString" =>
      field("parent.inner.a_optional_string"),

    "request.resource.attr.tags" => relation(
      :tags, fields: {"id" => field("tag_id"), "name" => field("name")}
    ),
    # tags[].name as a list. A NULL name stays in the list as a null element.
    "request.resource.attr.tagNames" => relation(:tags, member_field: "name"),

    # Every action on these lists indexes them, and a relation has no order, so the translator
    # refuses them at `index`. Mapped anyway so the refusal names that reason and not a
    # missing mapping.
    "request.resource.attr.aNumberList" => relation(:number_list_elements, member_field: "value"),
    "request.resource.attr.aBoolList" => relation(:bool_list_elements, member_field: "value"),

    "request.resource.attr.categories" => relation(:categories, fields: {
      # Read by a lambda body in `rel-hop2-or-exists`.
      "name" => field("name"),
      "subCategories" => relation(:sub_categories, fields: {
        "name" => field("name"),
        "labels" => relation(:labels, fields: {"name" => field("name")})
      })
    }),

    # The same two hops as a chain from the root. Each seed has at most one category, so
    # check() sees one object; 16 seeds have none and get no attribute.
    #
    # Nested on purpose. A flat `has_many :through` cannot tell a missing parent from a parent
    # with no children, so `all`, `!exists` and counts would return the 16 rows the PDP denies
    # (w1-*-chain, #309).
    "request.resource.attr.mainCategory" => relation(:categories, fields: {
      "subCategories" => relation(:sub_categories, fields: {"name" => field("name")}),
      "subNames" => relation(:sub_categories, member_field: "name")
    })
  }.freeze

  # The same map without per-attribute declarations, so a per-call convention reaches every
  # attribute. Derived, so it cannot drift from ATTRIBUTES.
  UNDECLARED = ATTRIBUTES.transform_values { |mapping|
    mapping.is_a?(Cerbos::ActiveRecord::AttributeMapping::Field) ? field(mapping.path) : mapping
  }.freeze
end
