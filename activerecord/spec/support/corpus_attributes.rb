# frozen_string_literal: true

# The one attribute map the conformance harness uses for every case (conformance/README.md,
# "The harness contract"). Read back through it, each stored row is its resource in
# conformance/resources.json.
#
# Null conventions (ADR 0004): a nullable scalar that resources.json omits when NULL declares
# `:omitted`, and one it sends as an explicit null declares `:explicit`. The collections carry
# null elements as explicit nulls, which is the call's default.
module CorpusAttributes
  def self.field(path, **kwargs) = Cerbos::ActiveRecord.field(path, **kwargs)

  def self.omitted(path) = field(path, null_representation: :omitted)

  def self.relation(*args, **kwargs) = Cerbos::ActiveRecord.relation(*args, **kwargs)

  ATTRIBUTES = {
    "request.resource.attr.aBool" => omitted("a_bool"),
    "request.resource.attr.aString" => omitted("a_string"),
    "request.resource.attr.aNumber" => omitted("a_number"),
    "request.resource.attr.aDouble" => omitted("a_double"),
    "request.resource.attr.aOptionalString" => omitted("a_optional_string"),
    "request.resource.attr.createdBy" => field("created_by"),
    "request.resource.attr.scope" => omitted("scope"),
    "request.resource.attr.createdAt" => omitted("created_at"),
    "request.resource.attr.updatedAt" => omitted("updated_at"),
    # `owner` and `coOwner` alias the `aOptionalString` and `scope` columns under the explicit
    # null convention: resources.json sends a real null for them (#308).
    "request.resource.attr.owner" => field("a_optional_string", null_representation: :explicit),
    # The second half of `null/equals/field-to-field-both-explicit-null`. Not another
    # alias of `a_optional_string`: a column compared with itself is always TRUE.
    "request.resource.attr.coOwner" => field("scope", null_representation: :explicit),
    # Not a real nested column: it reuses aString's column, as other harnesses do.
    "request.resource.attr.obj.inner" => omitted("a_string"),

    # `R.id` arrives as its own variable, not under `R.attr`, so it needs its own mapping.
    "request.resource.id" => field("id"),

    # The corpus's one real to-one relation (ADR 0005). Unlike `obj.inner`, this is a join:
    # each hop becomes a scalar subquery, and a missing parent gives NULL, which denies like
    # check()'s missing-path error. An absent level is a missing attribute, hence `:omitted`.
    # Read whole, the parent is a map (collection/exists/map-keys). Mapped as what it is, a
    # to-one association, which the adapter refuses as a macro's collection.
    "request.resource.attr.parent" => relation(:parent),
    "request.resource.attr.parent.aBool" => omitted("parent.a_bool"),
    "request.resource.attr.parent.aString" => omitted("parent.a_string"),
    "request.resource.attr.parent.aNumber" => omitted("parent.a_number"),
    "request.resource.attr.parent.aOptionalString" => omitted("parent.a_optional_string"),
    "request.resource.attr.parent.inner.aBool" => omitted("parent.inner.a_bool"),
    "request.resource.attr.parent.inner.aString" => omitted("parent.inner.a_string"),
    "request.resource.attr.parent.inner.aNumber" => omitted("parent.inner.a_number"),
    "request.resource.attr.parent.inner.aOptionalString" =>
      omitted("parent.inner.a_optional_string"),

    "request.resource.attr.tags" => relation(
      :tags, fields: {"id" => field("tag_id"), "name" => field("name")}
    ),
    # tags[].name as a list. A NULL name stays in the list as a null element.
    "request.resource.attr.tagNames" => relation(:tags, member_field: "name"),

    # A relation has no order, so the translator refuses indexing these lists at `index`.
    # Mapped anyway so the refusal names that reason and not a missing mapping.
    "request.resource.attr.aNumberList" => relation(:number_list_elements, member_field: "value"),
    "request.resource.attr.aBoolList" => relation(:bool_list_elements, member_field: "value"),

    "request.resource.attr.categories" => relation(:categories, fields: {
      "name" => field("name"),
      "subCategories" => relation(:sub_categories, fields: {
        "name" => field("name"),
        "labels" => relation(:labels, fields: {"name" => field("name")})
      })
    }),

    # The same two hops as a chain from the root. Each seed has at most one category, so
    # check() sees one object; a seed with none gets no attribute.
    #
    # Nested on purpose. A flat `has_many :through` cannot tell a missing parent from a parent
    # with no children, so `all`, `!exists` and counts would return the category-less rows the
    # PDP denies (#309).
    "request.resource.attr.mainCategory" => relation(:categories, fields: {
      "subCategories" => relation(:sub_categories, fields: {"name" => field("name")}),
      "subNames" => relation(:sub_categories, member_field: "name")
    })
  }.freeze

  # The same map without per-attribute declarations, so a per-call convention reaches every
  # attribute (spec/adapter_contract_spec.rb). Derived, so it cannot drift from ATTRIBUTES.
  UNDECLARED = ATTRIBUTES.transform_values { |mapping|
    mapping.is_a?(Cerbos::ActiveRecord::AttributeMapping::Field) ? field(mapping.path) : mapping
  }.freeze
end
