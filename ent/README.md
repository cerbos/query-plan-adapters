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
hostile seed rows and real Ent-built queries. The whole corpus is replayed against **both SQLite
and PostgreSQL**, so the dialect-sensitive choices this adapter makes are proved rather than
assumed. The Spring Data adapter defines the reference semantics for this compatibility snapshot.

| Classification | Coverage |
| --- | --- |
| Oracle-tested | 122 reference conformance actions — every conformance shape in the corpus, on SQLite and PostgreSQL |
| Fail-closed corpus shapes | Regex `matches()`, ordered list indexing/`get-field`, and `timestamp()` over an untyped string field (3 actions) |
| Representation-dependent | `null-eq-missing` — rejected under `NullOmitted`; translated as `IS NULL` under the default, which over-grants if the caller omits attributes for NULL columns |
| Known planner divergence | `has()` on a missing attribute is folded by the Cerbos planner to `ALWAYS_ALLOWED`, while `checkResource` denies the missing-attribute rows. Until the planner is fixed, use `R.attr.x != null` for database-backed attributes instead of `has(R.attr.x)` |

The oracle coverage includes value-first and field-to-field comparisons, escaped string predicates,
relation counts and nested collection macros, null/error propagation, arithmetic and ternaries,
hierarchy operations, typed timestamps, and multi-hop relations. Unlike the Python and TypeScript
adapters, sub-millisecond `now()` thresholds (`ts-window`, `ts-vf`) are **not** fail-closed here:
Go's `time.Time` carries nanoseconds, so those instants survive translation exactly.

The three fail-closed shapes return an error wrapping `ErrUnsupported` rather than a broader
predicate. `matches()` is rejected because SQL regex dialects do not guarantee CEL/RE2 semantics.

### Dialect coverage

| Dialect | Status |
| --- | --- |
| SQLite | Proved — full corpus, text timestamps compared lexicographically |
| PostgreSQL | Proved — full corpus, native `boolean` and `timestamptz` columns |
| MySQL | Supported by construction, **not** exercised by the differential suite |

The two proved dialects are not the same test twice: SQLite stores instants as text and booleans
as integers, PostgreSQL has real types for both, and each needs a different null-safe equality
operator and different cast spellings. Running both is what makes `WithDialect` a checked claim.

MySQL goes through the same code paths, but until it joins the suite treat it as untested.

### Collation

CEL string comparison and matching are case-sensitive and byte-exact, while `LIKE` collation is
controlled by the database. The suite sets `PRAGMA case_sensitive_like = ON` on SQLite and relies
on PostgreSQL's default deterministic collation; on MySQL's default `utf8mb4_0900_ai_ci` or a
`_CI_` SQL Server collation, string predicates will match strings CEL would reject — an over-grant
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
