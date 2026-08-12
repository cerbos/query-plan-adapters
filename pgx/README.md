# Cerbos query plan adapter for pgx

Translates a [Cerbos](https://cerbos.dev) query plan
([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan))
into a PostgreSQL `WHERE` fragment and its bound arguments, ready to hand to
[pgx](https://github.com/jackc/pgx).

```
go get github.com/cerbos/query-plan-adapters/pgx
```

## Usage

```go
import (
    "github.com/cerbos/cerbos-sdk-go/cerbos"
    cerbospgx "github.com/cerbos/query-plan-adapters/pgx"
)

plan, err := client.PlanResources(ctx, principal, cerbos.NewResource("contact", ""), "read")
if err != nil {
    return err
}

mapper := cerbospgx.MapperMap{
    "request.resource.attr.ownerId": {Column: "owner_id"},
    "request.resource.attr.status":  {Column: "status"},
}

result, err := cerbospgx.Translate(plan.PlanResourcesResponse, "contact", mapper)
if err != nil {
    return err
}

switch result.Kind {
case cerbospgx.KindAlwaysDenied:
    return nil // no rows are accessible; skip the query entirely
case cerbospgx.KindAlwaysAllowed:
    rows, err = pool.Query(ctx, `SELECT * FROM contact`)
case cerbospgx.KindConditional:
    rows, err = pool.Query(ctx, `SELECT * FROM contact WHERE `+result.Where, result.Args...)
}
```

`Where` is a bare boolean expression — it carries no `WHERE` keyword — so it composes with your own
predicates. Use `WithPlaceholderOffset(n)` when appending it to a query that already binds `n`
arguments.

Every value from the plan is bound as a parameter; nothing from the policy is ever interpolated
into SQL text.

## Mapping attributes

`Mapper` resolves the plan's attribute references onto storage. Resolution is **fail-closed**: an
unmapped reference is an error, never a guessed column name.

```go
tags := &cerbospgx.Relation{
    Table:        "contact_tag",
    SourceColumn: "id",         // column on the parent row
    TargetColumn: "contact_id", // matching column on contact_tag
    Field:        &cerbospgx.Entry{Column: "name"},               // scalar projection
    Fields:       map[string]cerbospgx.Entry{"name": {Column: "name"}}, // object fields
}

mapper := cerbospgx.MapperMap{
    "request.resource.attr.tags":      {Relation: tags},
    "request.resource.attr.createdAt": {Column: "created_at", ValueType: cerbospgx.ValueTimestamp},
}
```

Collection attributes lower into correlated subqueries. A relation may reach through intermediate
tables with `Via`, so a flattened chain such as `R.attr.mainCategory.subCategories` joins its
intermediate table inside the subquery while only the resource row correlates outwards.

Those subqueries read the mapped table bare. If your own reads of it apply a predicate — a
soft-delete flag, a tenant column, a subtype discriminator — declare it as `SubqueryFilter` on the
relation so the subquery sees the same rows the application serialised. See
[Mapping hazards](#mapping-hazards).

### NULL representation

The planner emits the same `eq(attr, null)` node whether the caller sends a NULL column as an
explicit `null` attribute or omits it, so the plan cannot reveal which convention is in use and the
adapter has to be told.

- `NullExplicit` (default) — a NULL column is sent to `check()` as an explicit null. `IS NULL`
  then selects exactly the rows `check()` allows.
- `NullOmitted` — a NULL column sends no attribute, so CEL raises a missing-attribute error (a
  deny) and a NULL-selecting filter would return rows the PDP refuses. Null operands are rejected
  rather than translated.

Pass `cerbospgx.WithNullRepresentation(cerbospgx.NullOmitted)` if your attributes omit NULL columns.

### Declare the convention per attribute

The option above is a whole-call default, and one policy suite can legitimately use both
conventions: the same column mapped twice, sent as an explicit null under one attribute name and
omitted under another. Declare it per attribute instead and the call-level option only covers what
the mapping does not:

```go
mapper := cerbospgx.MapperMap{
    // sent as an explicit null when the column is NULL
    "request.resource.attr.owner": {
        Column:         "owner_id",
        NullConvention: cerbospgx.NullConventionExplicit,
    },
    // omitted when the column is NULL — the call-level default applies
    "request.resource.attr.department": {Column: "department"},
}
```

Declaring the explicit convention asserts two things: the column can be NULL, **and** a NULL reaches
`check()` as an explicit null. The equality family (`eq`, `ne`, `in`) over that attribute is then
rendered so it can never be SQL UNKNOWN — CEL holds a null *value* under this convention, so
`null != "x"` is TRUE and the row must come back, while UNKNOWN would drop it under *both*
polarities. Ordering and string operators are left alone: a null receiver raises a no-overload error
in CEL, which denies exactly as UNKNOWN does.

Leaving an attribute undeclared keeps the historical rendering — so nothing changes for a mapping
that says nothing, and `!=` against a constant keeps under-granting the NULL rows until you declare
it.

**Declare both sides of a field-to-field comparison, or neither.** Mixing the conventions across one
comparison has no faithful rendering — the declared side needs a definite answer for its NULL, the
undeclared side needs UNKNOWN — so the adapter throws rather than picking a direction. See
[#308](https://github.com/cerbos/query-plan-adapters/issues/308) and
[ADR 0004](../docs/adr/0004-the-null-convention-is-a-property-of-the-attribute.md).

See [#302](https://github.com/cerbos/query-plan-adapters/issues/302).

## Conformance contract

The adapter is differentially tested against Cerbos PDP 0.54.0 `checkResource` decisions using 21
hostile seed rows and real PostgreSQL queries. The Spring Data adapter defines the reference
semantics for this compatibility snapshot.

| Classification | Coverage |
| --- | --- |
| Oracle-tested | 174 reference conformance actions |
| Fail-closed corpus shapes | Regex `matches()`, ordered list indexing/`get-field`, `timestamp()` over an untyped string field, `int()`/`double()` casts (SQL `CAST` reads a numeric prefix where CEL demands the whole string, and rounds where CEL truncates toward zero) `filter()`/`map()` used as a condition (both return a list, not a boolean), `string()` over a column declared `ValueBool` (rejected in the shared vendored translator, which serves MySQL and SQLite too, even though PostgreSQL alone would render it correctly), and a hierarchy path constructed by `list()` rather than read from a column (11 actions) |
| Operand types the plan does not carry | CEL overloads `+` on strings, and a query plan names no operand types. One string operand settles it, so `R.attr.a + "x"` and `"x" + R.attr.a` translate on their own. Between **two columns** neither does: declare the string column with `ValueType: cerbospgx.ValueString` and the adapter emits concatenation, or it fails closed rather than emitting a numeric `+` — which is a hard error on PostgreSQL, `0` on SQLite, and on MySQL a silent match against every row (cerbos/query-plan-adapters#391) |
| Representation-dependent | `null-eq-missing` — rejected under `NullOmitted`; translated as `IS NULL` under the default, which over-grants if the caller omits attributes for NULL columns |
| Attribute NULL convention | The equality family (`eq`, `ne`, `in`) over an attribute the caller sends as an explicit null renders definitely, so a NULL row is included where CEL's null *value* says it should be. Declare it per attribute — `NullConvention: NullConventionExplicit` on the mapper `Entry` — or the historical rendering applies and `!=` against a constant under-grants those rows (cerbos/query-plan-adapters#308) |
| Known planner divergence | `has()` on a missing attribute is folded by the Cerbos planner to `ALWAYS_ALLOWED`, while `checkResource` denies the missing-attribute rows. Until the planner is fixed, use `R.attr.x != null` for database-backed attributes instead of `has(R.attr.x)` |

The oracle coverage includes value-first and field-to-field comparisons, escaped string predicates,
relation counts and nested collection macros, null/error propagation, arithmetic and ternaries,
hierarchy operations, typed timestamps, and multi-hop relations. Unlike the Python and TypeScript
adapters, sub-millisecond `now()` thresholds (`ts-window`, `ts-vf`) are **not** fail-closed here:
Go's `time.Time` carries nanoseconds, so those instants survive translation exactly.

**Breaking change (#391).** `R.attr.a + R.attr.b` between two columns now returns an error unless one column is declared `ValueString`. It previously emitted a numeric `+`; on PostgreSQL that is a hard `operator does not exist: text + text` for text columns, so the change turns a runtime failure into a translation-time one and refuses the shape the shared translator cannot type.

The eleven fail-closed shapes return an error wrapping `ErrUnsupported` rather than a broader SQL
filter. `matches()` is rejected because SQL regex dialects do not guarantee CEL/RE2 semantics.

Every fail-closed shape's error message is pinned in the shared corpus (`conformance/actions.json`) and asserted by this adapter's conformance run, so a classification proves the throw names its declared mechanism rather than merely that something threw.

### Mapping hazards

The conformance contract above proves the *plan* side — given a policy shape, does the filter select
the rows `check()` allows. The other half is the *mapping*: **the rows the subquery reads must be
the rows the application put into the resource attributes.** Six ways that can break are catalogued
in the shared corpus, and every adapter has to record a position on each of them.

This adapter builds a **bare-table subquery.** A `Relation` is a table name plus a source and a
target column; there is no association metadata for the translator to consult, so nothing the
application applies to its own reads reaches the generated subquery. Where the application narrows
those reads, declare the same predicate as `SubqueryFilter` on the relation (or on the `Hop`, for an
intermediate table) and the translator reproduces it. Declaring nothing emits exactly the SQL this
adapter emitted before the field existed — it cannot detect the omission, so silence is not a
warning.

| Hazard | Position | Mechanism to check |
|---|---|---|
| Filtered association | **Caller-owned**, reproducible with `SubqueryFilter` | The `WHERE` clause of the query your application runs to load the relation when it builds the resource attributes. There is no ORM here to consult, so that clause exists only in your own code — the translator is given `Table`, `SourceColumn` and `TargetColumn` and reads the table bare |
| Default scope on the target model | **Caller-owned**, reproducible with `SubqueryFilter` | A soft-delete column (`deleted_at IS NULL`), a tenant column, a `published` flag — anything every application read of that table filters on. Go has no default-scope construct here, so the convention lives in your own query code and only you can see it |
| Subtype discrimination | **Caller-owned**, reproducible with `SubqueryFilter` | A `type`/`kind` discriminator column where one table holds several row kinds. Declare `{Column: "kind", Value: "…"}`, or `RestrictIn` over the kinds the association admits |
| To-one relation used as a collection | **Caller-owned** | A relation whose `TargetColumn` has no unique index. The mapping carries no cardinality at all — every relation lowers to the same correlated subquery — so nothing makes the database enforce the single row the application saw. Add the unique constraint, or accept that the subquery examines every matching row |
| Composite association key | **Rejected by the type system** | `SourceColumn` and `TargetColumn` are each one `string`, so a two-column key cannot be expressed. This is a compile error, not a wrong join |
| Absent to-one parent | **Reproduced**, and proved by the corpus (`w1-all-chain`, `rel-not-bool-hop` and siblings) | None — every operator reached through a `Via` chain requires its intermediate hops separately, so a missing parent is UNKNOWN under both polarities ([#309](https://github.com/cerbos/query-plan-adapters/issues/309), [#315](https://github.com/cerbos/query-plan-adapters/issues/315)). Declaring `SubqueryFilter` on a `Hop` extends that to a parent the application *hides*, which for this purpose is equally absent. A SCALAR read through a to-one hop is `Entry.ScalarRelation`, new in [#375](https://github.com/cerbos/query-plan-adapters/issues/375): it renders a correlated scalar subquery, which is NULL when no row correlates and so needs no separate hop guard |

#### Declaring the application's own predicate

```go
tags := &cerbospgx.Relation{
    Table:        "contact_tag",
    SourceColumn: "id",
    TargetColumn: "contact_id",
    Field:        &cerbospgx.Entry{Column: "name"},
    // Exactly the predicate your own reads of contact_tag apply.
    SubqueryFilter: []cerbospgx.Restriction{
        {Column: "deleted_at", Op: cerbospgx.RestrictIsNull},
        {Column: "kind", Value: "label"},
    },
}
```

The vocabulary is deliberately narrow — `RestrictEq`, `RestrictNe`, `RestrictIsNull`,
`RestrictIsNotNull`, `RestrictIn`, `RestrictNotIn` over a single column — because that is what these
hazards look like in practice. A predicate that does not fit is a signal that the mapping cannot
faithfully reproduce the application's read, and the honest response is to not map that relation
rather than to declare an approximation.

Restrictions are ANDed into the subquery's correlation predicate, so they narrow the rows the
subquery *examines* rather than the rows it returns. That is what keeps them right under negation:
`all` lowers to a false-witness `EXISTS`, and restricting the scan turns it into "every visible row
satisfies the body" instead of "every row in the table does". They apply to every shape built on the
relation — the truth witnesses, the UNKNOWN witness, the counts, and the hop-existence guard.

Values are bound as query parameters, never interpolated. An empty `Values` list is read as CEL
reads it: `RestrictIn` then hides every row, `RestrictNotIn` hides none.

### Known gaps

An adversarial review found these. They are real but unfixed here, because each either needs a
corpus action first (this repository's rule is that translation changes start in the shared corpus,
not in one adapter) or is shared with the reference adapters and should be fixed across all of them
at once. Treat them as constraints on the policies you write.

| Gap | Effect |
| --- | --- |
| A NaN stored in a floating-point column | Ordered comparisons follow the database's NaN ordering rather than IEEE's. Only NaNs the adapter folds itself are handled exactly. |
| Division by a stored negative zero | The sign of the resulting infinity is taken from the numerator alone, so `1.0 / -0.0` classifies as `+Inf` where CEL gives `-Inf`. |
| Timestamp literals finer than a microsecond | PostgreSQL stores microsecond resolution, so a sub-microsecond bound is silently truncated and a boundary comparison can flip. Keep policy timestamps at microsecond precision or coarser. |
| `!=` / `not in` against an explicit null under `NullExplicit` | CEL evaluates `null != "x"` as true; SQL leaves it UNKNOWN and excludes the row. This under-grants — it fails closed — but is not exact equivalence. |

Two gaps listed here previously are now closed and pinned by the corpus rather than documented as
constraints: an absent to-one parent is no longer indistinguishable from an empty collection (the
chain requires its intermediate hop, and `w1-all-chain`/`w1-not-exists-chain`/`w1-size-zero-chain`/
`w1-size-nonneg-chain`/`w1-not-in-chain`/`w1-not-hasint-chain`/`w1-not-size-chain` are
oracle-compared here — membership and the negated count spelling route through the same guarded
existence construction as the macros, which is why this adapter needed no change when those holes
were found elsewhere), and `int()`/`double()` no longer lower to SQL
`CAST` at all — they fail closed, because CEL reads a whole string or raises where `CAST` reads a
numeric prefix, and CEL truncates toward zero where PostgreSQL rounds (`cast-int-string`,
`cast-double-string`, `cast-int-double`).

### Collation

CEL string comparison and matching are case-sensitive and byte-exact. `LIKE` collation is
controlled by the database, so a case-insensitive column collation will match strings CEL would
reject — an over-grant the adapter cannot detect. Treat collation as part of your policy contract
and use a case-sensitive (e.g. `C` or a `_cs_` ICU) collation on columns policies compare.

## Development

```bash
go test -skip TestAdversarialConformance ./...   # unit suite, no Docker
go test ./...                                    # adds the adversarial conformance suite (Docker)
golangci-lint run ./...
golangci-lint fmt ./...
```

The adversarial suite starts its own PostgreSQL and Cerbos containers, reading the pinned PDP
version from `conformance/CERBOS_VERSION`.

The unit suite is everything else, and needs nothing running. It covers what the corpus structurally
cannot: malformed and hostile plans no planner emits. CI runs it as its own step before the
container-backed one, so "these tests need no Docker" stays a checked claim.

The translator under `internal/queryplan` is vendored byte-for-byte into the
[ent module](../ent) as well, so that a consumer of either pulls in only the one.
`conformance/scripts/validate-corpus.sh` diffs the two trees and fails on any difference: a semantic
fix has to land in both copies. Anything genuinely per-engine belongs in `render.go`, which is
outside the shared tree.

## License

Apache 2.0
