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
See [#302](https://github.com/cerbos/query-plan-adapters/issues/302).

## Conformance contract

The adapter is differentially tested against Cerbos PDP 0.54.0 `checkResource` decisions using 20
hostile seed rows and real PostgreSQL queries. The Spring Data adapter defines the reference
semantics for this compatibility snapshot.

| Classification | Coverage |
| --- | --- |
| Oracle-tested | 130 reference conformance actions — every conformance shape in the corpus |
| Fail-closed corpus shapes | Regex `matches()`, ordered list indexing/`get-field`, `timestamp()` over an untyped string field, `int()`/`double()` casts (SQL `CAST` reads a numeric prefix where CEL demands the whole string, and rounds where CEL truncates toward zero) and `filter()`/`map()` used as a condition (both return a list, not a boolean) (8 actions) |
| Representation-dependent | `null-eq-missing` — rejected under `NullOmitted`; translated as `IS NULL` under the default, which over-grants if the caller omits attributes for NULL columns |
| Known planner divergence | `has()` on a missing attribute is folded by the Cerbos planner to `ALWAYS_ALLOWED`, while `checkResource` denies the missing-attribute rows. Until the planner is fixed, use `R.attr.x != null` for database-backed attributes instead of `has(R.attr.x)` |

The oracle coverage includes value-first and field-to-field comparisons, escaped string predicates,
relation counts and nested collection macros, null/error propagation, arithmetic and ternaries,
hierarchy operations, typed timestamps, and multi-hop relations. Unlike the Python and TypeScript
adapters, sub-millisecond `now()` thresholds (`ts-window`, `ts-vf`) are **not** fail-closed here:
Go's `time.Time` carries nanoseconds, so those instants survive translation exactly.

The three fail-closed shapes return an error wrapping `ErrUnsupported` rather than a broader SQL
filter. `matches()` is rejected because SQL regex dialects do not guarantee CEL/RE2 semantics.

### Known gaps

An adversarial review found these. They are real but unfixed here, because each either needs a
corpus action first (this repository's rule is that translation changes start in the shared corpus,
not in one adapter) or is shared with the reference adapters and should be fixed across all of them
at once. Treat them as constraints on the policies you write.

| Gap | Effect |
| --- | --- |
| An absent to-one parent is indistinguishable from an empty collection | `R.attr.parent.children.all(...)`, `!exists(...)` or `size(...) == 0` on a row with no parent returns TRUE/zero, while CEL raises a missing-path error and denies. Affects every relational adapter in this repository, not just this one. |
| `int()` over a non-numeric string | CEL raises a conversion error and denies; SQL coerces (SQLite yields 0, PostgreSQL errors). Avoid `int()` on free-text columns. |
| A NaN stored in a floating-point column | Ordered comparisons follow the database's NaN ordering rather than IEEE's. Only NaNs the adapter folds itself are handled exactly. |
| Division by a stored negative zero | The sign of the resulting infinity is taken from the numerator alone, so `1.0 / -0.0` classifies as `+Inf` where CEL gives `-Inf`. |
| Timestamp literals finer than a microsecond | PostgreSQL stores microsecond resolution, so a sub-microsecond bound is silently truncated and a boundary comparison can flip. Keep policy timestamps at microsecond precision or coarser. |
| `!=` / `not in` against an explicit null under `NullExplicit` | CEL evaluates `null != "x"` as true; SQL leaves it UNKNOWN and excludes the row. This under-grants — it fails closed — but is not exact equivalence. |

### Collation

CEL string comparison and matching are case-sensitive and byte-exact. `LIKE` collation is
controlled by the database, so a case-insensitive column collation will match strings CEL would
reject — an over-grant the adapter cannot detect. Treat collation as part of your policy contract
and use a case-sensitive (e.g. `C` or a `_cs_` ICU) collation on columns policies compare.

## Development

```bash
go test ./...          # adversarial conformance suite (needs Docker for testcontainers)
golangci-lint run ./...
golangci-lint fmt ./...
```

The suite starts its own PostgreSQL and Cerbos containers, reading the pinned PDP version from
`conformance/CERBOS_VERSION`.

## License

Apache 2.0
