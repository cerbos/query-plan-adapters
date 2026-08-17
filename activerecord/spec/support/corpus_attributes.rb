# frozen_string_literal: true

# The one attribute map this adapter is classified against.
#
# Both corpus suites read it: the conformance harness, which plans against a real PDP and
# compares rows, and the translator unit test, which replays conformance/wire-fixtures/ offline.
# adapterctl.json classifies each action against ONE mapping per adapter, so the two
# suites disagreeing about the mapping would make the classification true of neither.
#
# This is sharing WITHIN one adapter, which is what ADR 0007 asks for. The thing that must not
# be extracted is the corpus LOADER, and that stays duplicated per adapter on purpose.
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
    # `owner` and `coOwner` alias the columns that `aOptionalString` and `scope` also map,
    # under the OTHER null convention: the oracle sends a real null attribute for them and
    # does not remove it. The declaration here is what makes the equality family definite for
    # these two attributes and leaves every other mapping alone
    # (cerbos/query-plan-adapters#308).
    "request.resource.attr.owner" => field("a_optional_string", null_representation: :explicit),
    # The explicit-null alias of the `scope` column, and the second half of `null-value-f2f`.
    # `scope` itself is omitted when NULL, so the corpus holds the same column under both
    # conventions and the field-to-field test has two explicit nulls to compare. It is NOT a
    # second alias of `a_optional_string`: a column compared with itself is TRUE for every
    # seed, and the degeneracy guard refuses a total oracle.
    "request.resource.attr.coOwner" => field("scope", null_representation: :explicit),
    # obj.inner is not a true nested column. It uses the same column as aString. The
    # spring-data, prisma and sqlalchemy harnesses use the same substitute for the p-struct
    # test.
    "request.resource.attr.obj.inner" => field("a_string"),

    # The primary key as an attribute. `R.id` reaches the wire as its own variable and not as
    # `R.attr.*`, so it needs a mapping of its own.
    "request.resource.id" => field("id"),

    # The one REAL to-one relation of the corpus (ADR 0005), and the counterpart of
    # `obj.inner` above: this one IS a join. A path with dots through a to-one association
    # becomes a correlated scalar subquery, and each hop is required to exist — an absent
    # parent gives NULL, which matches the missing-path error that check() denies with.
    #
    # No declaration of a null convention here. A NULL column one hop out is a MISSING
    # attribute for check(), and SQL UNKNOWN denies under both polarities in the same way.
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
    # The scalar values of tags[].name. A NULL name stays in the list as a null element.
    "request.resource.attr.tagNames" => relation(:tags, member_field: "name"),

    "request.resource.attr.categories" => relation(:categories, fields: {
      # The category itself carries a name, and a lambda body can read it
      # (`rel-hop2-or-exists`). Only the sub-category name was mapped before that action
      # existed.
      "name" => field("name"),
      "subCategories" => relation(:sub_categories, fields: {
        "name" => field("name"),
        "labels" => relation(:labels, fields: {"name" => field("name")})
      })
    }),

    # The same two hops, but from the root and written as a chain: `mainCategory` is one
    # relation, and `subCategories`/`subNames` are nested relations in its fields. Each seed
    # holds at most one category, so the check side sees `mainCategory` as ONE object, and 16
    # of the seeds have no category and thus no attribute at all.
    #
    # The nesting is deliberate. A flat `has_many :through` under the full name with dots gives
    # the same joins, but it does not say which hop is the parent. Then an absent parent and a
    # parent with no children look the same, and `all`, `!exists` and every count over the
    # chain give back the 16 rows that the PDP denies (w1-*-chain, #309/#315/#316).
    "request.resource.attr.mainCategory" => relation(:categories, fields: {
      "subCategories" => relation(:sub_categories, fields: {"name" => field("name")}),
      "subNames" => relation(:sub_categories, member_field: "name")
    })
  }.freeze

  # The same map with every per-attribute declaration removed, so a test can make the
  # convention of the call reach every attribute. It is DERIVED from the map above and is not
  # written out a second time: a new attribute cannot arrive in one and not the other.
  UNDECLARED = ATTRIBUTES.transform_values { |mapping|
    mapping.is_a?(Cerbos::ActiveRecord::AttributeMapping::Field) ? field(mapping.path) : mapping
  }.freeze
end
