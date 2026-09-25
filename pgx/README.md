# Cerbos query plan adapter for pgx

Translates a [Cerbos](https://cerbos.dev) query plan
([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan))
into a PostgreSQL `WHERE` fragment and its bound arguments, ready to hand to
[pgx](https://github.com/jackc/pgx).

## Install

```bash
go get github.com/cerbos/query-plan-adapters/pgx
```

- Go 1.26.4 or later (the module's `go` directive).
- `github.com/jackc/pgx/v5` and `github.com/cerbos/cerbos-sdk-go` v0.4 (the versions `go.mod`
  requires). The fragment is plain SQL text with `$n` placeholders, so any driver that speaks
  PostgreSQL's ordinal parameters can run it.
- PostgreSQL. The tested server is pinned in [`POSTGRES_IMAGE`](POSTGRES_IMAGE).
- The module is standalone: it vendors its own translator and depends on nothing else in this
  repository.

## Quick start

```go
import (
    "context"
    "fmt"

    "github.com/cerbos/cerbos-sdk-go/cerbos"
    cerbospgx "github.com/cerbos/query-plan-adapters/pgx"
    "github.com/jackc/pgx/v5"
    "github.com/jackc/pgx/v5/pgxpool"
)

var mapper = cerbospgx.MapperMap{
    "request.resource.attr.ownerId": {Column: "owner_id"},
    "request.resource.attr.status":  {Column: "status"},
}

func listContacts(ctx context.Context, c *cerbos.GRPCClient, pool *pgxpool.Pool, principal *cerbos.Principal) (pgx.Rows, error) {
    plan, err := c.PlanResources(ctx, principal, cerbos.NewResource("contact", ""), "read")
    if err != nil {
        return nil, err
    }

    result, err := cerbospgx.Translate(plan.PlanResourcesResponse, "contact", mapper)
    if err != nil {
        return nil, err // wraps cerbospgx.ErrUnsupported for a shape PostgreSQL cannot express
    }

    switch result.Kind {
    case cerbospgx.KindAlwaysDenied:
        return nil, nil // no row is accessible: skip the query
    case cerbospgx.KindAlwaysAllowed:
        return pool.Query(ctx, `SELECT * FROM contact`)
    case cerbospgx.KindConditional:
        return pool.Query(ctx, `SELECT * FROM contact WHERE `+result.Where, result.Args...)
    }
    return nil, fmt.Errorf("unexpected plan kind %d", result.Kind) // fail closed
}
```

`Translate` takes the resource's table name and a mapper. `Result.Where` is a bare boolean
expression (no `WHERE` keyword) with placeholders numbered from `$1`, and `Result.Args` binds them
in order; both are set only for `KindConditional`. Every value from the plan is a bound parameter
with an explicit cast (`$1::text`), and every identifier is double-quoted.

Every shape the adapter refuses returns an error wrapping `cerbospgx.ErrUnsupported` — never a broader
filter. Use `errors.Is(err, cerbospgx.ErrUnsupported)` to tell "the policy asks for something PostgreSQL
cannot express" from a mapping or configuration error. The corpus cases it refuses are listed in
[`conformance-ledger.json`](conformance-ledger.json) (see [Conformance contract](#conformance-contract)).

## Composing the fragment with your own predicates

PostgreSQL placeholders are **ordinal**: `$1` is the first argument sent with the statement, not
the first in the fragment. Once the fragment goes anywhere but straight after `WHERE`, you own the
numbering, and a mistake is not a compile error.

**Your predicate first:** pass `WithPlaceholderOffset(len(yourArgs))` and send the fragment's
arguments after yours.

```go
where, args := `archived = $1 AND region = $2`, []any{false, "emea"}

result, err := cerbospgx.Translate(plan.PlanResourcesResponse, "contact", mapper,
    cerbospgx.WithPlaceholderOffset(len(args))) // the fragment now starts at $3
if err != nil {
    return err
}

if result.Kind == cerbospgx.KindAlwaysDenied {
    return nil // don't run the application-only query
}
if result.Kind == cerbospgx.KindConditional {
    where += " AND (" + result.Where + ")"
    args = append(args, result.Args...) // Result.Args must follow yours
}
rows, err := pool.Query(ctx, `SELECT id FROM contact WHERE `+where, args...)
```

**The fragment first** (pagination is the usual case): there is no option for this side. The
fragment keeps `$1…`, and you number your parameters from `len(result.Args)+1`. This assumes you
have already handled the unconditional kinds:

```go
stmt := fmt.Sprintf(`SELECT id FROM contact WHERE %s ORDER BY id LIMIT $%d OFFSET $%d`,
    result.Where, len(result.Args)+1, len(result.Args)+2)
rows, err := pool.Query(ctx, stmt, append(slices.Clone(result.Args), limit, offset)...)
```

Two things the offset does **not** do:

- **It does not move your arguments.** It renumbers the fragment's placeholders; putting
  `Result.Args` at positions `n+1…` of the slice you send is up to you. Two swapped same-typed
  arguments bind without error and answer the wrong question. (A count or type mismatch is refused
  by pgx or the server.)
- **It does not apply to a non-conditional plan.** `Where` is empty and `Args` is nil for
  `KindAlwaysAllowed` and `KindAlwaysDenied`, so branch on `Kind` before splicing. An empty `Where`
  spliced after `AND` is a syntax error at run time.

[`example/`](example/) runs both directions against a real PostgreSQL server.

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
tags := &cerbospgx.Relation{
    Table:        "contact_tag",
    SourceColumn: "id",         // column on the parent row
    TargetColumn: "contact_id", // matching column on contact_tag
    Field:        &cerbospgx.Entry{Column: "name"},                     // scalar elements
    Fields:       map[string]cerbospgx.Entry{"name": {Column: "name"}}, // object elements (t.name)
}

mapper := cerbospgx.MapperMap{
    "request.resource.attr.tags":      {Relation: tags},
    "request.resource.attr.createdAt": {Column: "created_at", ValueType: cerbospgx.ValueTimestamp},
    "request.resource.attr.company": {
        Column:         "name",
        ScalarRelation: &cerbospgx.Relation{Table: "company", SourceColumn: "company_id", TargetColumn: "id"},
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
  covers a `ValueBool` column read through a to-one `ScalarRelation` (`string(R.attr.parent.flag)`),
  a boolean-valued expression (`string(R.attr.n > 3)`) and a ternary whose arms are all boolean
  (`string(R.attr.n > 3 ? R.attr.flag : false)`). An undeclared column, through a hop or
  not, keeps the plain text `CAST`: the plan carries no types, so declaring `ValueBool` is what
  tells the adapter the column holds a boolean. PostgreSQL's own `CAST` of a `boolean` column already
  says `"true"`/`"false"`, so an undeclared boolean column is right here too.

## NULL representation

The planner emits the same `eq(attr, null)` node whether a NULL column is sent to `check()` as an
explicit `null` or omitted, so you have to tell the adapter which you do
([#302](https://github.com/cerbos/query-plan-adapters/issues/302)).

| Call option | When a column is NULL, your attributes… | Effect |
| --- | --- | --- |
| `NullExplicit` (default) | send an explicit `null` | `== null` translates to `IS NULL`. |
| `NullOmitted` | omit the attribute | Null operands are rejected: CEL raises a missing-attribute error (a deny), so an `IS NULL` filter would over-grant. |

```go
cerbospgx.Translate(plan.PlanResourcesResponse, "contact", mapper,
    cerbospgx.WithNullRepresentation(cerbospgx.NullOmitted))
```

### Declare the convention per attribute

One policy can use both conventions, so you can declare it per attribute on the `Entry`; the call
option then covers only undeclared attributes
([#308](https://github.com/cerbos/query-plan-adapters/issues/308),
[ADR 0004](../docs/adr/0004-the-null-convention-is-a-property-of-the-attribute.md)):

```go
mapper := cerbospgx.MapperMap{
    // NULL is sent as an explicit null
    "request.resource.attr.owner": {Column: "owner_id", NullConvention: cerbospgx.NullConventionExplicit},
    // NULL is omitted, whatever the call option says
    "request.resource.attr.team": {Column: "team", NullConvention: cerbospgx.NullConventionOmitted},
    // undeclared (NullConventionUnset): treated as NOT NULL; the call option governs null operands
    "request.resource.attr.department": {Column: "department"},
}
```

- `NullConventionExplicit` asserts the column can be NULL **and** a NULL reaches `check()` as
  `null`. The equality family (`eq`, `ne`, `in`) then never renders SQL UNKNOWN, so
  `null != "x"` includes the row as CEL does. Ordering and string operators are unchanged (a null
  receiver is a CEL error, which denies like UNKNOWN).
- `NullConventionOmitted` renders `== null` as `CASE WHEN col IS NULL THEN NULL ELSE FALSE END`
  and `!= null` with `ELSE TRUE`: a NULL column is CEL's missing-attribute error, so it stays
  UNKNOWN under any `NOT`, and a true sibling in an `||` still absorbs it. A column read through
  a to-one hop is NULL for an absent parent too, and renders the same way. Every other null
  operand against it (a null in an `in` list, say) is rejected.
- Undeclared attributes keep the historical rendering, where `!=` against a constant under-grants
  the NULL rows.
- **Declare both sides of a field-to-field equality, or neither.** Mixing conventions on operands
  of the same or undeclared scalar type is rejected.

> [!WARNING]
> **Do not guard with `has()`: write `R.attr.x != null`.** An attribute the plan request omits is
> unknown to the planner, which assumes the data layer supplies it: for a table, the column exists
> and only its value is open. So the planner reads `has(R.attr.x)` as the guard for the `x` access
> beside it and folds it to true by design: alone it plans as `ALWAYS_ALLOWED`, and
> `has(R.attr.x) && R.attr.y > 0` plans as `R.attr.y > 0`. It never excludes a row whose `x` is NULL.
> `R.attr.x != null` stays in the plan, where the adapter translates it or refuses it, and agrees
> with `check()` whether `x` is missing, null or present.

## Collation

CEL string comparison is case-sensitive and byte-exact; `=` and `LIKE` follow the column's
collation. A case-insensitive collation is an **over-grant the adapter cannot detect**, so treat
collation as part of your policy contract: use a deterministic collation (the PostgreSQL default)
on every column policies compare. Nondeterministic ICU collations and `citext` are not safe.

**String ordering needs more.** CEL orders strings by code point, and `<`, `<=`, `>` and `>=`
follow the column's collation. A linguistic collation such as glibc's `en_US.UTF-8` (the usual
default on Debian images and managed services) or ICU's `en-US` is deterministic, yet sorts
`"One"` after `"a"`, so `R.attr.name > "a"` over-grants it
([#489](https://github.com/cerbos/query-plan-adapters/issues/489)). Use a byte-order collation,
`"C"`, on every column a policy orders: create the database with `LC_COLLATE 'C'`, or declare
`COLLATE "C"` on the column.

The conformance harness initialises its database with `--lc-collate=C` rather than trusting the
Alpine image, whose musl libc orders by byte only by accident.
`ADAPTER_TEST_POSTGRES_INITDB_ARGS="--locale-provider=icu --icu-locale=en-US" go test -run
TestAdversarialConformance ./...` replays the corpus under a linguistic order and fails both
`comparison/*/string-code-point-order` cases.

## Conformance contract

The adapter is proved against the shared [conformance corpus](../conformance/README.md): the plans
Cerbos PDP 0.55.0 recorded for each case are translated, run against the corpus dataset in real
PostgreSQL, and the returned ids are compared with the recorded `check()` decisions. The previous
PDP's goldens (0.54.0) are replayed too.

| Tier | Passed / total (PDP 0.55.0) |
| --- | --- |
| core | 26 / 26 |
| extended | 56 / 80 |
| adversarial | 235 / 314 |

The total is every golden case in the tier for PDP 0.55.0. A case whose golden records a
`plannerDivergence` is skipped rather than compared, and counts as not passed.

Every case that does not pass is either refused with `ErrUnsupported` or a recorded divergence;
[`conformance-ledger.json`](conformance-ledger.json) lists each one with its reason. Two of the
skipped cases, `null/has/missing-attribute` and `null/has/composed-with-comparison`, are the two
calls answering different questions: the plan request leaves an omitted attribute unknown, so the
planner folds `has()` to true by design, while `check()` receives the omission as absent and denies
the row. Use `R.attr.x != null` instead of `has(R.attr.x)`. Two more, `arithmetic/add/int-literal-plus-constant` and `arithmetic/add/int-literal-negated`, are the
planner dropping the int type of the literal in `R.attr.x + 1`: the plan is the double spelling's,
while `check()` has no double + int overload and denies every row, so write `1.0`.

### Known gaps

Real but unfixed; each needs a corpus case first. Treat them as constraints on your policies.

| Gap | Effect |
| --- | --- |
| A NaN stored in a floating-point column | Ordered comparisons follow the database's NaN ordering, not CEL's IEEE semantics. Only NaNs the adapter folds itself are exact. |
| Timestamp literals finer than a microsecond | PostgreSQL stores microseconds, so a sub-microsecond bound is truncated and a boundary comparison can flip. Keep policy timestamps at microsecond precision or coarser. |
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
| Filtered association | **Caller-owned**, reproducible with `SubqueryFilter` | The `WHERE` clause of the query your application runs to load the relation when it builds the resource attributes. There is no ORM to consult, so it exists only in your code |
| Default scope on the target model | **Caller-owned**, reproducible with `SubqueryFilter` | A soft-delete column (`deleted_at IS NULL`), a tenant column, a `published` flag — anything every read of that table filters on. Go has no default-scope construct, so it lives in your query code |
| Subtype discrimination | **Caller-owned**, reproducible with `SubqueryFilter` | A `type`/`kind` discriminator column. Declare `{Column: "kind", Value: "…"}`, or `RestrictIn` over the kinds the association admits |
| To-one relation used as a collection | **Caller-owned** | A relation whose `TargetColumn` has no unique index. The mapping carries no cardinality, so nothing enforces the single row the application saw. Add the unique constraint, or accept that the subquery examines every matching row |
| Composite association key | **Rejected by the type system** | `SourceColumn` and `TargetColumn` are each one `string`; a two-column key is a compile error, not a wrong join |
| Absent to-one parent | **Reproduced**, and proved by the corpus (`relation/all/to-one-chain`, `relation/bare-attribute/negated-one-hop-boolean` and the other `relation/*` cases) | None — every operator reached through a `Via` chain requires its intermediate hops, so a missing parent is UNKNOWN under both polarities ([#309](https://github.com/cerbos/query-plan-adapters/issues/309), [#315](https://github.com/cerbos/query-plan-adapters/issues/315)). `SubqueryFilter` on a `Hop` extends that to a parent the application *hides*. A scalar read through a to-one hop is `Entry.ScalarRelation` ([#375](https://github.com/cerbos/query-plan-adapters/issues/375)), which is NULL when no row correlates and needs no hop guard |

### Declaring the application's own predicate

```go
tags := &cerbospgx.Relation{
    Table:        "contact_tag",
    SourceColumn: "id",
    TargetColumn: "contact_id",
    Field:        &cerbospgx.Entry{Column: "name"},
    // Exactly the predicate your own reads of contact_tag apply.
    SubqueryFilter: []cerbospgx.Restriction{
        {Column: "deleted_at", Op: cerbospgx.RestrictIsNull},
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
  column. PostgreSQL used to fail the query (`operator does not exist: double precision = text`); the
  shared translator's SQLite and MySQL renderings (the ent module) coerced `'2'` and over-granted. An undeclared element keeps the old rendering.
- **Breaking ([#391](https://github.com/cerbos/query-plan-adapters/issues/391)):** `R.attr.a + R.attr.b`
  between two columns returns an error unless one is declared `ValueString`. It used to emit a
  numeric `+`, which PostgreSQL rejects at run time for text columns (`operator does not exist:
  text + text`); the error now arrives at translation.
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
  `CASE WHEN col IS NULL THEN NULL WHEN col THEN 'true' ELSE 'false' END` cast to text. A NULL
  column stays NULL and the row is excluded.
- [#470](https://github.com/cerbos/query-plan-adapters/issues/470): `string()` over a boolean-valued
  expression (`string(R.attr.n > 3)`), or over a `ValueBool` column read through a to-one
  `ScalarRelation`, is spelled through the same `CASE` as a plain `ValueBool` column, instead of a
  plain `CAST`. PostgreSQL's `CAST` already said `"true"`/`"false"`, so results do not change;
  the vendored translator is shared with the ent module, where SQLite and MySQL said `"1"`/`"0"`.
- [#538](https://github.com/cerbos/query-plan-adapters/issues/538): `string()` over a ternary whose
  arms are all boolean (`string(R.attr.n > 3 ? R.attr.flag : false)`) is spelled through the same
  `CASE`. PostgreSQL's `CAST` was already right, so results do not change
  (`cast/string/negated-from-boolean-ternary`).
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

[`example/`](example/) runs the adapter against a live PDP and a real PostgreSQL server over the
shared [demo domain](../demo/README.md):

```bash
# from the repository root
demo/scripts/run-example.sh pgx
```

It is the only place in this repository where
[composition](#composing-the-fragment-with-your-own-predicates) runs end to end: an application
predicate ahead of the fragment with `WithPlaceholderOffset`, and pagination parameters numbered
after it. It proves usage shapes only, not packaging: it resolves this module through a `replace`
directive ([ADR 0002](../docs/adr/0002-examples-install-the-packed-artifact.md)).

## Development

```bash
go test -skip TestAdversarialConformance ./...   # unit suite, no Docker
go test ./...                                    # adds the conformance suite (Docker)
golangci-lint run ./...
golangci-lint fmt ./...
```

- The conformance suite (`TestAdversarialConformance`) starts no PDP: it replays the recorded
  goldens in `conformance/golden/` against a PostgreSQL container from
  [`POSTGRES_IMAGE`](POSTGRES_IMAGE), the same file `example/run.sh` reads. A case it cannot pass
  goes in [`conformance-ledger.json`](conformance-ledger.json): `unsupported` (translation must
  return `ErrUnsupported`) or `divergent` (the result must differ, with an issue).
- The unit suite needs nothing running. It covers what the corpus cannot: malformed and hostile
  plans no planner emits. CI runs it as a separate step before the Docker-backed one.
- `internal/queryplan` is vendored byte-for-byte into the [ent module](../ent);
  `conformance/scripts/validate-corpus.sh` fails on any difference, so a semantic fix must land in
  both. Per-engine code belongs in `render.go`, outside the shared tree.

See [conformance/README.md](../conformance/README.md) before changing how a shape is translated.

## License

Apache 2.0
