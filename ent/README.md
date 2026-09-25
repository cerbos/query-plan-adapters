# Cerbos query plan adapter for Ent

Translates a [Cerbos](https://cerbos.dev) query plan
([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan))
into an [Ent](https://entgo.io) predicate you can apply to any generated query.

## Install

```bash
go get github.com/cerbos/query-plan-adapters/ent
```

- Go 1.26.4 or later (the module's `go` directive).
- `entgo.io/ent` v0.14 and `github.com/cerbos/cerbos-sdk-go` v0.4 (the versions `go.mod` requires).
- SQLite, PostgreSQL or MySQL 8.0.17+. See [Dialects](#dialects).
- The module is standalone: it vendors its own translator and depends on nothing else in this
  repository. Release notes are in [CHANGELOG.md](CHANGELOG.md).

## Quick start

```go
import (
    "context"
    "fmt"

    "entgo.io/ent/dialect"
    entsql "entgo.io/ent/dialect/sql"
    "github.com/cerbos/cerbos-sdk-go/cerbos"
    cerbosent "github.com/cerbos/query-plan-adapters/ent"

    "example.com/app/ent"
    "example.com/app/ent/contact"
)

var mapper = cerbosent.MapperMap{
    "request.resource.attr.ownerId": {Column: contact.FieldOwnerID},
    "request.resource.attr.status":  {Column: contact.FieldStatus},
}

func listContacts(ctx context.Context, c *cerbos.GRPCClient, db *ent.Client, principal *cerbos.Principal) ([]*ent.Contact, error) {
    plan, err := c.PlanResources(ctx, principal, cerbos.NewResource("contact", ""), "read")
    if err != nil {
        return nil, err
    }

    result, err := cerbosent.Translate(plan.PlanResourcesResponse, contact.Table, mapper,
        cerbosent.WithDialect(dialect.Postgres)) // must match the ent client's dialect
    if err != nil {
        return nil, err // wraps cerbosent.ErrUnsupported for a shape SQL cannot express
    }

    switch result.Kind {
    case cerbosent.KindAlwaysDenied:
        return nil, nil // no row is accessible: skip the query
    case cerbosent.KindAlwaysAllowed:
        return db.Contact.Query().All(ctx)
    case cerbosent.KindConditional:
        return db.Contact.Query().
            Where(func(s *entsql.Selector) { s.Where(result.Predicate) }).
            All(ctx)
    }
    return nil, fmt.Errorf("unexpected plan kind %d", result.Kind) // fail closed
}
```

`Translate` takes the resource's table name (ent's generated `<entity>.Table`) and a mapper.
`Result.Predicate` is set only for `KindConditional`; it is an ordinary `*sql.Predicate`, so it
composes with ent's own predicates, `Limit` and `Offset`. The predicate is rendered through ent's
`sql.Builder`, so quoting and placeholders follow the dialect, and every value from the plan is a
bound parameter.

Every shape the adapter refuses returns an error wrapping `cerbosent.ErrUnsupported` — never a broader
predicate. Use `errors.Is(err, cerbosent.ErrUnsupported)` to tell "the policy asks for something SQL
cannot express" from a mapping or configuration error. The corpus cases it refuses are listed in
[`conformance-ledger.json`](conformance-ledger.json) (see [Conformance contract](#conformance-contract)).

## Mapping attributes

A `Mapper` resolves each attribute reference in the plan to storage. Use `MapperMap` for a static
table or `MapperFunc` for a function. Resolution is **fail-closed**: an unmapped reference is an
error, never a guessed column name.

`Entry` fields:

| Field | Use |
| --- | --- |
| `Column` | Column on the row being read (the resource row, or the element row inside a collection). |
| `ValueType` | `ValueTimestamp`, `ValueBool`, `ValueString`, `ValueNumber`, or `ValueDefault`. The plan carries no types; declare them where storage needs it (see [Declaring value types](#declaring-value-types)). |
| `NullConvention` | Per-attribute NULL convention. See [NULL representation](#null-representation). |
| `Relation` | A collection- or object-valued attribute stored in another table. |
| `ScalarRelation` | A scalar read through a **to-one** relation (`R.attr.parent.name`), rendered as a correlated scalar subquery. Declaring it asserts your schema makes the match unique. |
| `Qualifier` | Table or alias to read `Column` from. Normally left empty; the translator fills it in. |

```go
tags := &cerbosent.Relation{
    Table:        "contact_tag",
    SourceColumn: "id",         // column on the parent row
    TargetColumn: "contact_id", // matching column on contact_tag
    Field:        &cerbosent.Entry{Column: "name"},                     // scalar elements
    Fields:       map[string]cerbosent.Entry{"name": {Column: "name"}}, // object elements (t.name)
}

mapper := cerbosent.MapperMap{
    "request.resource.attr.tags":      {Relation: tags},
    "request.resource.attr.createdAt": {Column: "created_at", ValueType: cerbosent.ValueTimestamp},
    "request.resource.attr.company": {
        Column:         "name",
        ScalarRelation: &cerbosent.Relation{Table: "company", SourceColumn: "company_id", TargetColumn: "id"},
    },
}
```

Collection attributes lower into correlated subqueries. A relation can reach through intermediate
tables with `Via []Hop` (innermost first; each `Hop` has `Table`, `ChildColumn`, `JoinColumn`), so a
flattened chain such as `R.attr.mainCategory.subCategories` joins its intermediate table inside the
subquery while only the resource row correlates outwards. Those subqueries read the mapped table
bare; if your own reads apply a predicate, declare it as `SubqueryFilter` (see
[Mapping hazards](#mapping-hazards)).

### Declaring value types

- `ValueTimestamp` on every timestamp column.
- `ValueString` on text columns used in `+`: between two columns, `R.attr.a + R.attr.b` is refused
  unless one side is declared `ValueString`
  ([#391](https://github.com/cerbos/query-plan-adapters/issues/391)).
- `ValueNumber`, `ValueString` and `ValueBool` prevent database coercion in heterogeneous
  comparisons and string operations. Undeclared columns keep the historical rendering.
- `ValueBool` lets `string(R.attr.flag)` translate to CEL's `"true"`/`"false"`. The same spelling
  covers a `ValueBool` column read through a to-one `ScalarRelation` (`string(R.attr.parent.flag)`)
  and a boolean-valued expression (`string(R.attr.n > 3)`). An undeclared column, through a hop or
  not, keeps the plain text `CAST`: the plan carries no types, so declaring `ValueBool` is what
  tells the adapter the column holds a boolean. Declare every boolean column you pass to `string()`,
  since SQLite and MySQL render a stored boolean as `"1"`/`"0"` under a `CAST`.

### Timestamps on SQLite

SQLite compares timestamp text lexicographically, which matches chronological order only if every
value is fixed width and in one zone. The adapter binds times in `cerbosent.SQLiteTimestampLayout`
(`2006-01-02T15:04:05.000000000Z`); store yours in the same layout. `time.RFC3339Nano` trims
trailing zeros and would sort `…:05.5Z` after `…:05.12Z`.

## NULL representation

The planner emits the same `eq(attr, null)` node whether a NULL column is sent to `check()` as an
explicit `null` or omitted, so you have to tell the adapter which you do
([#302](https://github.com/cerbos/query-plan-adapters/issues/302)).

| Call option | When a column is NULL, your attributes… | Effect |
| --- | --- | --- |
| `NullExplicit` (default) | send an explicit `null` | `== null` translates to `IS NULL`. |
| `NullOmitted` | omit the attribute | Null operands are rejected: CEL raises a missing-attribute error (a deny), so an `IS NULL` filter would over-grant. |

```go
cerbosent.Translate(plan.PlanResourcesResponse, contact.Table, mapper,
    cerbosent.WithNullRepresentation(cerbosent.NullOmitted))
```

### Declare the convention per attribute

One policy can use both conventions, so you can declare it per attribute on the `Entry`; the call
option then covers only undeclared attributes
([#308](https://github.com/cerbos/query-plan-adapters/issues/308),
[ADR 0004](../docs/adr/0004-the-null-convention-is-a-property-of-the-attribute.md)):

```go
mapper := cerbosent.MapperMap{
    // NULL is sent as an explicit null
    "request.resource.attr.owner": {Column: "owner_id", NullConvention: cerbosent.NullConventionExplicit},
    // NULL is omitted, whatever the call option says
    "request.resource.attr.team": {Column: "team", NullConvention: cerbosent.NullConventionOmitted},
    // undeclared (NullConventionUnset): treated as NOT NULL; the call option governs null operands
    "request.resource.attr.department": {Column: "department"},
}
```

- `NullConventionExplicit` asserts the column can be NULL **and** a NULL reaches `check()` as
  `null`. The equality family (`eq`, `ne`, `in`) then never renders SQL UNKNOWN, so
  `null != "x"` includes the row as CEL does. Ordering and string operators are unchanged (a null
  receiver is a CEL error, which denies like UNKNOWN).
- Undeclared attributes keep the historical rendering, where `!=` against a constant under-grants
  the NULL rows.
- **Declare both sides of a field-to-field equality, or neither.** Mixing conventions on operands
  of the same or undeclared scalar type is rejected.

## Dialects

`WithDialect` accepts only `dialect.SQLite` (the default), `dialect.Postgres` and `dialect.MySQL`,
and must match the ent client the predicate is handed to. Any other value, including `""` and
aliases such as `postgresql`, is a configuration error, even for a constant plan.

| Dialect | Status |
| --- | --- |
| SQLite | Proved — full corpus, text timestamps compared lexicographically |
| PostgreSQL | Proved — full corpus, native `boolean` and `timestamptz` columns |
| MySQL | Proved — full corpus, `DATETIME(6)` columns, byte-exact NO PAD collation (`utf8mb4_0900_bin`) |

The dialect changes more than quoting: MySQL needs `CONCAT` (its `||` is logical OR) and
`CHAR_LENGTH`; each engine has its own null-safe equality (`IS`, `IS NOT DISTINCT FROM`, `<=>`) and
cast spellings. On MySQL, `string()` results are cast with `COLLATE utf8mb4_0900_bin` so the
connection collation cannot make `string(x) == "set"` match `"Set"`.

## Collation

CEL string comparison is case-sensitive and byte-exact; `=` and `LIKE` follow the database's
collation. A looser collation is an **over-grant the adapter cannot detect**, so treat collation as
part of your policy contract:

- **SQLite:** set `PRAGMA case_sensitive_like = ON`.
- **PostgreSQL:** use a deterministic collation (the default).
- **MySQL:** use `utf8mb4_0900_bin` (8.0.17+) on every string column policies read. The default
  `utf8mb4_0900_ai_ci` is case- and accent-insensitive. Even `utf8mb4_0900_as_cs` is not
  byte-exact: it ignores a soft hyphen (U+00AD), so `"o­ne"` equals `"one"` — an over-grant on
  `==`/`in` and an under-grant on `!=` ([#474](https://github.com/cerbos/query-plan-adapters/issues/474)).
- A `_CI_` SQL Server collation has the same problem.

## Identifier quoting

Table, column and qualifier names go through ent's `sql.Builder.Ident`, which treats a name already
containing the dialect's quote character (a backtick on SQLite/MySQL, `"` on PostgreSQL) as
pre-quoted and writes it unchanged. **Name your columns with ordinary identifiers.** No plan data
ever reaches an identifier, so this is a naming constraint on your own `Mapper`, not an injection
surface. The [pgx adapter](../pgx) quotes defensively instead.

## Conformance contract

The adapter is proved against the shared [conformance corpus](../conformance/README.md): the plans
Cerbos PDP 0.55.0 recorded for each case are translated, run against the corpus dataset in real
Ent-built queries on **SQLite, PostgreSQL and MySQL**, and the returned ids are compared with the
recorded `check()` decisions. The previous PDP's goldens (0.54.0) are replayed too. The counts are
the same on all three databases. The total is every golden case in the tier; a case whose golden
records a planner divergence (the plan and `check()` disagree, so no adapter can pass) is skipped
and counts toward the total but not toward passed — on 0.55.0 that is four extended cases.

| Tier | Passed / total (PDP 0.55.0) |
| --- | --- |
| core | 26 / 26 |
| extended | 56 / 80 |
| adversarial | 204 / 269 |

Every case that does not pass is either refused with `ErrUnsupported` or a recorded divergence;
[`conformance-ledger.json`](conformance-ledger.json) lists each one with its reason, and one ledger
holds for every dialect. The one case no adapter can pass is `null/has/missing-attribute`: the
Cerbos planner folds `has()` on a missing attribute to `ALWAYS_ALLOWED` while `check()` denies those
rows, so use `R.attr.x != null` for database-backed attributes instead of `has(R.attr.x)`.

### Known gaps

Real but unfixed; each needs a corpus case first. Treat them as constraints on your policies.

| Gap | Effect |
| --- | --- |
| A NaN stored in a floating-point column | Ordered comparisons follow the database's NaN ordering, not CEL's IEEE semantics. Only NaNs the adapter folds itself are exact. |
| `!=` / `not in` against an explicit null, on an attribute not declared `NullConventionExplicit` | CEL says `null != "x"` is true; SQL leaves it UNKNOWN and excludes the row. Under-grants (fails closed). See cerbos/query-plan-adapters#308. |

## Mapping hazards

The conformance contract proves the *plan* side. The *mapping* side is yours: **the rows a subquery
reads must be the rows your application put into the resource attributes.** The shared corpus
catalogues six ways that breaks.

A `Relation` is a table name and two columns, with no association metadata, so the adapter builds
a **bare-table subquery**: nothing your application applies to its own reads reaches it, and the
adapter cannot detect the omission. Where your reads narrow a table, declare the same predicate as
`SubqueryFilter` on the `Relation` (or on a `Hop`).

| Hazard | Position | Mechanism to check |
|---|---|---|
| Filtered association | **Caller-owned**, reproducible with `SubqueryFilter` | Ent interceptors (`client.Intercept(intercept.TraverseFunc(…))` calling `q.WhereP`), privacy filter rules (`privacy.FilterFunc`), and `.Where(...)` predicates a repository helper always applies. They rewrite the ent query they are attached to, not the raw correlated subquery this adapter builds |
| Default scope on the target model | **Caller-owned**, reproducible with `SubqueryFilter` | A soft-delete column (`deleted_at IS NULL`), a tenant column, a `published` flag — anything every read of that table filters on. Go has no default-scope construct, so it lives in your query code |
| Subtype discrimination | **Caller-owned**, reproducible with `SubqueryFilter` | A `type`/`kind` discriminator column. Declare `{Column: "kind", Value: "…"}`, or `RestrictIn` over the kinds the association admits |
| To-one relation used as a collection | **Caller-owned** | A relation whose `TargetColumn` has no unique index. The mapping carries no cardinality, so nothing enforces the single row the application saw. Add the unique constraint, or accept that the subquery examines every matching row |
| Composite association key | **Rejected by the type system** | `SourceColumn` and `TargetColumn` are each one `string`; a two-column key is a compile error, not a wrong join |
| Absent to-one parent | **Reproduced**, and proved by the corpus (`relation/all/to-one-chain`, `relation/bare-attribute/negated-one-hop-boolean` and siblings) | None — every operator reached through a `Via` chain requires its intermediate hops, so a missing parent is UNKNOWN under both polarities ([#309](https://github.com/cerbos/query-plan-adapters/issues/309), [#315](https://github.com/cerbos/query-plan-adapters/issues/315)). `SubqueryFilter` on a `Hop` extends that to a parent the application *hides*. A scalar read through a to-one hop is `Entry.ScalarRelation` ([#375](https://github.com/cerbos/query-plan-adapters/issues/375)), which is NULL when no row correlates and needs no hop guard |

### Declaring the application's own predicate

```go
tags := &cerbosent.Relation{
    Table:        "contact_tag",
    SourceColumn: "id",
    TargetColumn: "contact_id",
    Field:        &cerbosent.Entry{Column: "name"},
    // Exactly the predicate your own reads of contact_tag apply.
    SubqueryFilter: []cerbosent.Restriction{
        {Column: "deleted_at", Op: cerbosent.RestrictIsNull},
        {Column: "kind", Value: "label"}, // Op defaults to RestrictEq
    },
}
```

- Operators: `RestrictEq`, `RestrictNe`, `RestrictIsNull`, `RestrictIsNotNull`, `RestrictIn`,
  `RestrictNotIn` (with `Values`), each over one column. If your read does not fit, don't map that
  relation rather than declaring an approximation.
- Restrictions are ANDed into the correlation predicate, so they narrow the rows the subquery
  *examines*. That keeps negation right (`all` means "every visible row", not "every row in the
  table") and applies to every shape built on the relation, including counts and hop guards.
- Values are bound parameters. An empty `Values` reads as CEL does: `RestrictIn` hides every row,
  `RestrictNotIn` hides none.

## Behaviour changes

- **Membership across types:** a literal whose type differs from a relation element's declared
  `ValueType` no longer matches it. `"2" in R.attr.numbers` and `hasIntersection(R.attr.flags,
  ["true"])` are false, as CEL says, and a mixed literal list keeps only the members of the
  element's type; the same rule applies to `in` against a literal list over a declared scalar
  column. SQLite and MySQL used to coerce `'2'` onto a number column (and MySQL read `'true'` as
  `0`), an over-grant; PostgreSQL failed the query. An undeclared element keeps the old rendering.
- **Breaking:** `WithDialect` rejects anything but `dialect.SQLite`, `dialect.Postgres` and
  `dialect.MySQL` with a configuration error, including `""` and aliases. Previous versions accepted
  them and could render inconsistent SQL.
- **Breaking ([#391](https://github.com/cerbos/query-plan-adapters/issues/391)):** `R.attr.a + R.attr.b`
  between two columns returns an error unless one is declared `ValueString`. It used to emit a
  numeric `+`, which was wrong for non-numeric columns.
- **Breaking ([#414](https://github.com/cerbos/query-plan-adapters/issues/414)):** bare comparisons
  between declared temporal columns fail closed (storage loses the RFC 3339 spelling; wrap both
  operands in `timestamp()`), and two-list `except`, structured list operands and constructor
  expressions are refused. Membership now keeps the needle's per-attribute NULL convention even
  over an empty collection, and `ValueNumber`/`ValueString`/`ValueBool` declarations now prevent
  database coercion; undeclared columns behave as before.
- **Breaking:** `int()` and `double()` casts fail closed instead of lowering to SQL `CAST`, which
  reads a numeric prefix and rounds where CEL reads the whole string and truncates
  (`cast/int/malformed-string`, `cast/double/malformed-string`, `cast/int/negative-fraction`).
- [#387](https://github.com/cerbos/query-plan-adapters/issues/387): a `filter()`/`map()` result in a
  value position (`map(R.attr.tags, t.id) == [...]`) fails at translation with a named error instead
  of at execution in the driver.
- [#418](https://github.com/cerbos/query-plan-adapters/issues/418): `string()` over a `ValueBool`
  column translates (it used to fail closed), via
  `CASE WHEN col IS NULL THEN NULL WHEN col THEN 'true' ELSE 'false' END` cast to text, giving CEL's
  words on every dialect; a NULL column stays NULL and the row is excluded. On MySQL the cast
  carries `COLLATE utf8mb4_0900_bin`.
- [#470](https://github.com/cerbos/query-plan-adapters/issues/470): `string()` over a boolean-valued
  expression (`string(R.attr.n > 3)`), or over a `ValueBool` column read through a to-one
  `ScalarRelation`, is spelled through the same `CASE` as a plain `ValueBool` column, instead of a
  plain `CAST`. On SQLite and MySQL that `CAST` said `"1"`/`"0"`, so `!(string(x) == "true")`
  returned rows whose `x` is true (`cast/string/negated-from-boolean-expression`,
  `cast/string/negated-from-boolean-through-relation`).
- **Breaking:** three shapes that used to emit a filter now fail closed, because the filter
  disagreed with CEL. `%` over an attribute (`R.attr.n % 2`) is a CEL no-overload error — every
  attribute number is a double — so the PDP denies every row
  (`arithmetic/modulo/negated-double-operand`). A comparison decided by the sign of an infinity
  from a zero column denominator (`R.attr.a / R.attr.b > 0.0`) is refused, since CEL's `x / -0.0` is
  the opposite infinity from `x / 0.0` and the sign of a stored zero cannot be read
  (`arithmetic/divide/field-by-field`); a self-division, and a comparison both signs answer alike,
  still translate. `string()` over a `ValueNumber` column translates only as `==`/`!=` against a
  string constant, lowered to a numeric comparison with the double CEL spells that way (Go's `%g`:
  `"1e+06"`, `"2"`), and a zero spelling is refused (`cast/string/from-double-spellings`,
  `cast/string/from-negative-zero-double`).
- A list literal compared with a column declared `ValueString`, `ValueNumber` or `ValueBool` is
  unequal wherever the column is present (`type-mismatch/equals/string-field-against-list-literal`);
  against an undeclared column it fails closed. It used to bind the list as a parameter, which the
  driver rejected at execution.
- Cerbos 0.55: folded NaN ordered comparisons return false (so their negation returns true),
  matching the updated CEL evaluator; this differs from Cerbos 0.54. Missing attributes still
  propagate errors.

## Example application

[`example/`](example/) runs the adapter against a live PDP over the shared
[demo domain](../demo/README.md), with a **generated** ent client:

```bash
# from the repository root
demo/scripts/run-example.sh ent
```

It is the only place in this repository where `Result.Predicate` is handed to a generated query's
`Where(func(*sql.Selector))` and combined with ent's `Limit`/`Offset` and generated predicates. It
proves usage shapes only, not packaging: it resolves this module through a `replace` directive
([ADR 0002](../docs/adr/0002-examples-install-the-packed-artifact.md)).

## Development

```bash
go test -skip TestAdversarialConformance ./...   # unit suite, no Docker
go test ./...                                    # adds the conformance suite (Docker)
golangci-lint run ./...
golangci-lint fmt ./...
```

- The conformance suite (`TestAdversarialConformance`) starts no PDP: it replays the recorded
  goldens in `conformance/golden/` against in-memory SQLite, then PostgreSQL and MySQL
  testcontainers. A case it cannot pass goes in [`conformance-ledger.json`](conformance-ledger.json):
  `unsupported` (translation must return `ErrUnsupported`) or `divergent` (the result must differ,
  with an issue).
- The unit suite needs nothing running. It covers what the corpus cannot: malformed and hostile
  plans no planner emits, and per-dialect spellings where a wrong choice is still valid SQL. CI runs
  it as a separate step before the Docker-backed one.
- `internal/queryplan` is vendored byte-for-byte into the [pgx module](../pgx);
  `conformance/scripts/validate-corpus.sh` fails on any difference, so a semantic fix must land in
  both. Per-engine code belongs in `render.go`, outside the shared tree.

See [conformance/README.md](../conformance/README.md) before changing how a shape is translated.

## License

Apache 2.0
