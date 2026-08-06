# Cerbos query plan adapter for Ent

Translates a [Cerbos](https://cerbos.dev) query plan
([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan))
into an [Ent](https://entgo.io) predicate you can apply to any generated query.

```
go get github.com/cerbos/query-plan-adapters/ent
```

## Usage

```go
import (
    "entgo.io/ent/dialect"
    entsql "entgo.io/ent/dialect/sql"
    "github.com/cerbos/cerbos-sdk-go/cerbos"
    cerbosent "github.com/cerbos/query-plan-adapters/ent"
)

plan, err := client.PlanResources(ctx, principal, cerbos.NewResource("contact", ""), "read")
if err != nil {
    return err
}

mapper := cerbosent.MapperMap{
    "request.resource.attr.ownerId": {Column: "owner_id"},
    "request.resource.attr.status":  {Column: "status"},
}

result, err := cerbosent.Translate(plan.PlanResourcesResponse, contact.Table, mapper,
    cerbosent.WithDialect(dialect.Postgres))
if err != nil {
    return err
}

switch result.Kind {
case cerbosent.KindAlwaysDenied:
    return nil // no rows are accessible; skip the query entirely
case cerbosent.KindAlwaysAllowed:
    contacts, err = client.Contact.Query().All(ctx)
case cerbosent.KindConditional:
    contacts, err = client.Contact.Query().
        Where(func(s *entsql.Selector) { s.Where(result.Predicate) }).
        All(ctx)
}
```

The predicate is rendered through Ent's own `sql.Builder`, so identifier quoting and placeholder
syntax follow the dialect rather than being assembled by hand. Every value from the plan is bound
as a parameter; nothing from the policy is ever interpolated into SQL text.

`WithDialect` must match the dialect of the Ent client the predicate is handed to — cast spellings
and timestamp storage differ between SQLite, PostgreSQL and MySQL. It defaults to
`dialect.SQLite`, matching Ent's own default.

### Timestamps on SQLite

SQLite has no temporal type, so a timestamp column is text and comparisons are lexicographic. That
only agrees with chronological order if every value is fixed width and in one zone, so this adapter
binds time values in `cerbosent.SQLiteTimestampLayout` (`2006-01-02T15:04:05.000000000Z`). Store
your timestamps in that layout — Go's `time.RFC3339Nano` trims trailing zeros from the fraction and
would sort `…:05.5Z` after `…:05.12Z`.

## Mapping attributes

`Mapper` resolves the plan's attribute references onto storage. Resolution is **fail-closed**: an
unmapped reference is an error, never a guessed column name.

```go
tags := &cerbosent.Relation{
    Table:        "contact_tag",
    SourceColumn: "id",         // column on the parent row
    TargetColumn: "contact_id", // matching column on contact_tag
    Field:        &cerbosent.Entry{Column: "name"},               // scalar projection
    Fields:       map[string]cerbosent.Entry{"name": {Column: "name"}}, // object fields
}

mapper := cerbosent.MapperMap{
    "request.resource.attr.tags":      {Relation: tags},
    "request.resource.attr.createdAt": {Column: "created_at", ValueType: cerbosent.ValueTimestamp},
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

Pass `cerbosent.WithNullRepresentation(cerbosent.NullOmitted)` if your attributes omit NULL columns.
See [#302](https://github.com/cerbos/query-plan-adapters/issues/302).

## Conformance contract

The adapter is differentially tested against Cerbos PDP 0.54.0 `checkResource` decisions using 20
hostile seed rows and real Ent-built queries. The whole corpus is replayed against **SQLite,
PostgreSQL and MySQL**, so the dialect-sensitive choices this adapter makes are proved rather than
assumed. The Spring Data adapter defines the reference semantics for this compatibility snapshot.

| Classification | Coverage |
| --- | --- |
| Oracle-tested | 133 reference conformance actions — every conformance shape in the corpus, on SQLite, PostgreSQL and MySQL |
| Fail-closed corpus shapes | Regex `matches()`, ordered list indexing/`get-field`, `timestamp()` over an untyped string field, `int()`/`double()` casts (SQL `CAST` reads a numeric prefix where CEL demands the whole string, and rounds where CEL truncates toward zero) and `filter()`/`map()` used as a condition (both return a list, not a boolean) (8 actions) |
| Representation-dependent | `null-eq-missing` — rejected under `NullOmitted`; translated as `IS NULL` under the default, which over-grants if the caller omits attributes for NULL columns |
| Known planner divergence | `has()` on a missing attribute is folded by the Cerbos planner to `ALWAYS_ALLOWED`, while `checkResource` denies the missing-attribute rows. Until the planner is fixed, use `R.attr.x != null` for database-backed attributes instead of `has(R.attr.x)` |

The oracle coverage includes value-first and field-to-field comparisons, escaped string predicates,
relation counts and nested collection macros, null/error propagation, arithmetic and ternaries,
hierarchy operations, typed timestamps, and multi-hop relations. Unlike the Python and TypeScript
adapters, sub-millisecond `now()` thresholds (`ts-window`, `ts-vf`) are **not** fail-closed here:
Go's `time.Time` carries nanoseconds, so those instants survive translation exactly.

The eight fail-closed shapes return an error wrapping `ErrUnsupported` rather than a broader
predicate. `matches()` is rejected because SQL regex dialects do not guarantee CEL/RE2 semantics.

### Dialect coverage

| Dialect | Status |
| --- | --- |
| SQLite | Proved — full corpus, text timestamps compared lexicographically |
| PostgreSQL | Proved — full corpus, native `boolean` and `timestamptz` columns |
| MySQL | Proved — full corpus, `DATETIME(6)` columns, binary collation |

The three proved dialects are not the same test three times: SQLite stores instants as text and
booleans as integers, PostgreSQL has real types for both, MySQL needs `CONCAT` rather than `||`
(which it reads as logical OR outside `PIPES_AS_CONCAT`), `CHAR_LENGTH` rather than `LENGTH` (which
counts bytes), and `TRUNCATE` before an integer cast — and each needs a different null-safe
equality operator and different cast spellings. Running all three is what makes `WithDialect` a
checked claim rather than an assertion.

The MySQL schema pins a **binary collation** on every string column. MySQL's default
`utf8mb4_0900_ai_ci` is both case- and accent-insensitive, which over-grants on `cs-eq`,
`unicode-eq` and every hierarchy prefix probe — see [Collation](#collation) below.

### Known gaps

An adversarial review found these. They are real but unfixed here, because each either needs a
corpus action first (this repository's rule is that translation changes start in the shared corpus,
not in one adapter) or is shared with the reference adapters and should be fixed across all of them
at once. Treat them as constraints on the policies you write.

| Gap | Effect |
| --- | --- |
| A NaN stored in a floating-point column | Ordered comparisons follow the database's NaN ordering rather than IEEE's. Only NaNs the adapter folds itself are handled exactly. |
| Division by a **stored** negative zero | SQL cannot tell `-0.0` from `0.0` — both satisfy `= 0` and no portable function reads the sign bit — so the sign of the resulting infinity is unknowable when the denominator is a column. A constant denominator is handled exactly: the planner ships the sign and the adapter applies it (`cr-div-neg-zero`). |
| `!=` / `not in` against an explicit null under `NullExplicit` | CEL evaluates `null != "x"` as true; SQL leaves it UNKNOWN and excludes the row. This under-grants — it fails closed — but is not exact equivalence. See cerbos/query-plan-adapters#308. |

Two gaps listed here previously are now closed and pinned by the corpus rather than documented as
constraints: an absent to-one parent is no longer indistinguishable from an empty collection (the
chain requires its intermediate hop, `w1-all-chain`/`w1-not-exists-chain`/`w1-size-zero-chain`/
`w1-size-nonneg-chain`/`w1-not-in-chain`/`w1-not-hasint-chain`/`w1-not-size-chain` — membership
and the negated count spelling route through the same guarded existence construction as the
macros, which is why this adapter needed no change when those holes were found elsewhere), and
`int()`/`double()` no longer lower to SQL `CAST` at all — they fail
closed, because CEL reads a whole string or raises where `CAST` reads a numeric prefix, and CEL
truncates toward zero where PostgreSQL and MySQL round (`cast-int-string`, `cast-double-string`,
`cast-int-double`).

### Collation

CEL string comparison and matching are case-sensitive and byte-exact, while `LIKE` collation is
controlled by the database. The suite sets `PRAGMA case_sensitive_like = ON` on SQLite, relies on
PostgreSQL's default deterministic collation, and pins `utf8mb4_bin` on every MySQL string column.
On MySQL's **default** `utf8mb4_0900_ai_ci` — which is both case- and accent-insensitive — or a
`_CI_` SQL Server collation, string predicates will match strings CEL would reject: an over-grant
the adapter cannot detect. Treat collation as part of your policy contract.

## Development

```bash
go test ./...          # adversarial conformance suite (needs Docker for testcontainers)
golangci-lint run ./...
golangci-lint fmt ./...
```

The suite starts one Cerbos container, reading the pinned PDP version from
`conformance/CERBOS_VERSION`, then replays the whole corpus against an in-memory SQLite database
and a PostgreSQL testcontainer in turn.

## License

Apache 2.0
