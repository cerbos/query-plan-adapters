# Cerbos + Drizzle ORM Adapter

An adapter library that takes a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into a [Drizzle ORM](https://orm.drizzle.team/) SQL expression. This allows you to use Cerbos query plans directly inside your Drizzle queries.

## Features

- Supports logical operators: `and`, `or`, `not`
- Supports comparison operators: `eq`, `ne`, `lt`, `gt`, `le`, `ge`, `in`
- Supports string operators: `contains`, `startsWith`, `endsWith`
- Supports nullability checks: `eq`/`ne` against a null value map to `IS NULL` / `IS NOT NULL`
  (the planner emits no existence operator)
- Supports set-aware operators such as `hasIntersection`, `exists`, `exists_one`, and `all`
- Supports relation-aware mappings, including nested relations and many-to-many joins
- Works with Drizzle SQLite, PostgreSQL, MySQL and PlanetScale drivers

## NULL attribute representation

`R.attr.x == null` compiles to the same `eq(x, null)` plan node however your application represents
a NULL column in the attributes it sends to `check()`, so the adapter cannot infer the convention
and has to be told which one you use.

| attributes you send for a NULL column | `check()` on that row | null-matching filter |
| --- | --- | --- |
| `{"x": null}` — explicit null | allow | selects it — aligned |
| `{}` — attribute omitted | **deny** (CEL missing-attribute error) | selects it — **over-grants** |

``nullAttributeRepresentation`` defaults to ``"explicit"``, preserving the historical translation. If your application
omits attributes for NULL columns, set it to ``"omitted"``: the adapter then rejects every null
comparison operand instead of emitting a filter that returns rows the PDP denies.

```ts
queryPlanToDrizzle({ queryPlan, mapper, nullAttributeRepresentation: "omitted" });
```

The rejection is deliberately wider than the shapes that actually over-grant — `x != null` and
`!(x == null)` are aligned under both conventions — because negation is applied by wrapping the
built condition rather than pushing it into the leaf, so a leaf cannot tell whether an enclosing
`not` will flip a not-null predicate back into a null-selecting one. Rejecting every null operand
is correct under any nesting. See
[#302](https://github.com/cerbos/query-plan-adapters/issues/302).

### Declare the convention per attribute

The option above is a whole-call default, and one policy suite can legitimately use both
conventions: the same column mapped twice, sent as an explicit null under one attribute name and
omitted under another. Declare it on the mapping instead and the call-level option only covers what
the mapping does not:

```ts
const mapper = {
  // sent as an explicit null when the column is NULL
  "request.resource.attr.owner": {
    column: resources.ownerId,
    nullAttributeRepresentation: "explicit",
  },
  // omitted when the column is NULL — the call-level default applies
  "request.resource.attr.department": resources.department,
};
```

Declaring `"explicit"` asserts two things: the column can be NULL, **and** a NULL reaches `check()`
as an explicit null. The equality family (`eq`, `ne`, `in`) over that attribute is then rendered so
it can never be SQL UNKNOWN — CEL holds a null *value* under this convention, so `null != "x"` is
TRUE and the row must come back, while UNKNOWN would drop it under *both* polarities. Ordering and
string operators are left alone: a null receiver raises a no-overload error in CEL, which denies
exactly as UNKNOWN does.

Leaving an attribute undeclared keeps the historical rendering — so nothing changes for a mapping
that says nothing, and `!=` against a constant keeps under-granting the NULL rows until you declare
it.

**Declare both sides of a field-to-field comparison, or neither.** Mixing the conventions across one
comparison has no faithful rendering — the declared side needs a definite answer for its NULL, the
undeclared side needs UNKNOWN — so the adapter throws rather than picking a direction. See
[#308](https://github.com/cerbos/query-plan-adapters/issues/308) and
[ADR 0004](../docs/adr/0004-the-null-convention-is-a-property-of-the-attribute.md).


## Conformance contract

The adapter is differentially tested against Cerbos PDP 0.54.0 `checkResource` decisions using 21 hostile seed rows and real Drizzle queries, executed on both SQLite and PostgreSQL. The Spring Data adapter defines the reference semantics for this compatibility snapshot.

| Classification | Coverage |
| --- | --- |
| Oracle-tested | 177 reference conformance actions |
| Fail-closed corpus shapes | Sub-millisecond `now()` thresholds, regex `matches()`, ordered list indexing/`get-field`, `timestamp()` over an untyped string field, `int()`/`double()` casts (SQL `CAST` reads a numeric prefix where CEL demands the whole string, and rounds where CEL truncates toward zero) `filter()`/`map()` used as a condition (both return a list, not a boolean), `string()` over a boolean column (SQLite and MySQL store 1/0 and render `"1"` where CEL and PostgreSQL render `"true"`), CEL's `+` over strings (`||` concatenates on SQLite and PostgreSQL but is logical OR on MySQL, and the numeric `+` this adapter emits coerces the operands to 0 rather than failing), a hierarchy path constructed by `list()` rather than read from a column, `mod` (reached through the `int()` cast that gives `%` an integer operand), a positional read of a scalar list (row order in a SQL relation is not defined), and list equality over a `map()` projection (20 actions) |
| Representation-dependent | `null-eq-missing` — rejected under `nullAttributeRepresentation: "omitted"`; translated as `IS NULL` under the default, which over-grants if the caller omits attributes for NULL columns |
| Attribute NULL convention | The equality family (`eq`, `ne`, `in`) over an attribute the caller sends as an explicit null renders definitely, so a NULL row is included where CEL's null *value* says it should be. Declare it per attribute — `nullAttributeRepresentation: "explicit"` on the mapper entry — or the historical rendering applies and `!=` against a constant under-grants those rows (cerbos/query-plan-adapters#308) |
| Known planner divergence | `has()` on a missing attribute is folded by the Cerbos planner to `ALWAYS_ALLOWED`, while `checkResource` denies the missing-attribute rows. Until the planner is fixed, use `R.attr.x != null` for database-backed attributes instead of `has(R.attr.x)` |

The oracle coverage includes value-first and field-to-field comparisons, escaped string predicates, relation counts and nested collection macros, null/error propagation, arithmetic and ternaries, hierarchy operations, typed timestamps, and multi-hop relations. The fail-closed shapes throw rather than return a broader SQL filter. `matches()` is rejected because SQL regex dialects do not guarantee CEL/RE2 semantics. Every fail-closed shape's error message is pinned in the shared corpus (`conformance/actions.json`) and asserted by this adapter's conformance run, so a classification proves the throw names its declared mechanism rather than merely that something threw.

The SQL each of these actions produces is pinned separately, in the translator unit test (`npm test`) — see [Testing](#testing). That is what makes a change to the emitted SQL show up as a diff even when it selects the same rows from the corpus seeds, and it is the only place the parts of the mapper contract no policy can reach are asserted at all: function mappers, `transform`, `subqueryFilter`, the `nullAttributeRepresentation` boundary, the timestamp literal contract, and malformed input.

### Dialects the contract is proved on

The classification above holds where the corpus is **executed**, not where the emitted SQL merely looks plausible. Until [#320](https://github.com/cerbos/query-plan-adapters/issues/320) the corpus ran on SQLite only and PostgreSQL support was pinned at the rendered-string level; it now runs end to end on both:

```bash
npm run test:adversarial            # SQLite
npm run test:adversarial:postgres   # PostgreSQL, via testcontainers
```

The PostgreSQL leg is what proves the typed paths SQLite cannot reach — a real `boolean` where SQLite stores an integer, a real `timestamptz` where SQLite compares text, a hard error on division by zero where SQLite returns NULL, and a parameter typed from the column it is compared with rather than from the value. **MySQL and PlanetScale are not executed anywhere.** The translator unit test (`npm test`) renders every corpus action through `MySqlDialect` as well and holds it to the dialect rules — no SQLite-only string function, no single-precision cast, no integer `CASE` arm outside a counting aggregate, no fold that collapses a NULL operand to FALSE. That is renderability, not evaluation, and it is why the golden expectations pin bytes for the two executed stores and rules for the third.

**Behaviour change.** `hasIntersection` now normalizes its operand order, so the value-first spelling — `hasIntersection(["a","b"], R.attr.list)`, which the planner preserves from policy source order — translates instead of silently becoming `FALSE`. The same change makes an operand pair with **no** literal list throw rather than emit that `FALSE`: a shape that returned a filter now raises, which is a consumer-visible break, but the filter it returned selected no rows and the corpus forbids emitting one for a shape the adapter cannot express ([#387](https://github.com/cerbos/query-plan-adapters/issues/387)).

## Mapping hazards

The conformance contract above proves the *plan* side — given a policy shape, does the filter select the rows `check()` allows. The other half is the *mapping*: **the rows the subquery reads must be the rows the application put into the resource attributes.** Six ways that can break are catalogued in the shared corpus, and every adapter has to record a position on each of them.

This adapter builds a **bare-table subquery.** A relation mapping is a table plus a source and a target column; Drizzle has no association metadata for the adapter to consult, so nothing the application applies to its own reads reaches the generated `EXISTS`. Where the application narrows those reads, declare the same predicate as [`subqueryFilter`](#declaring-the-applications-own-predicate) on the relation and the adapter reproduces it. Declaring nothing emits exactly the SQL this adapter emitted before the field existed — it cannot detect the omission, so silence is not a warning.

| Hazard | Position | Mechanism to check |
|---|---|---|
| Filtered association | **Caller-owned**, reproducible with `subqueryFilter` | The `where` you pass to Drizzle's relational query builder (`db.query.<table>.findMany({ with: { rel: { where } } })`), and any repository helper that appends one. Drizzle applies those to the query you call them on; the adapter is given `table`, `sourceColumn` and `targetColumn` and reads the table bare |
| Default scope on the target model | **Caller-owned**, reproducible with `subqueryFilter` | A soft-delete column (`deletedAt IS NULL`), a tenant column, a `published` flag — anything every application read of that table filters on. Drizzle has no `default_scope` construct, so the convention lives in your own query code and only you can see it |
| Subtype discrimination | **Caller-owned**, reproducible with `subqueryFilter` | A `type`/`kind` discriminator column where one table holds several row kinds. Declare `eq(table.type, "…")` |
| To-one relation used as a collection | **Caller-owned** | A `type: "one"` relation whose target column has no unique index. `type` is declarative — the adapter emits the same correlated `EXISTS` for either value — so nothing makes the database enforce the single row the application saw. Add the unique constraint, or accept that the subquery examines every matching row |
| Composite association key | **Rejected by the type system** | `sourceColumn`/`targetColumn` are each a single `AnyColumn`, so a two-column key cannot be expressed. This is a compile error, not a wrong join |
| Absent to-one parent | **Reproduced**, and proved by the corpus (`w1-all-chain`, `rel-not-bool-hop` and siblings) | None — every operator reached through a relation requires its to-one hops separately, so a missing parent is UNKNOWN under both polarities ([#309](https://github.com/cerbos/query-plan-adapters/issues/309), [#315](https://github.com/cerbos/query-plan-adapters/issues/315), [#375](https://github.com/cerbos/query-plan-adapters/issues/375)). **Behaviour change in #375:** this previously held only for a chain of two or more relations, and a bare boolean read through a hop bypassed the guard entirely. A negation over a SINGLE to-one hop returned every row whose relation was absent; it now returns fewer rows — an over-grant fix, consumer-visible for any policy with that shape |

### Declaring the application's own predicate

```ts
import { and, eq, isNull } from "drizzle-orm";

const result = queryPlanToDrizzle({
  queryPlan,
  mapper: {
    "request.resource.attr.tags": {
      relation: {
        type: "many",
        table: tags,
        sourceColumn: resources.id,
        targetColumn: tags.resourceId,
        field: tags.name,
        // Exactly the predicate your own reads of `tags` apply.
        subqueryFilter: and(isNull(tags.deletedAt), eq(tags.kind, "label")),
      },
    },
  },
});
```

`subqueryFilter` is ANDed into the correlated subquery alongside the join, so it narrows the rows the subquery *examines* rather than the rows it returns. That is what makes it right under negation as well: `all()` compiles to a `NOT EXISTS` over a false witness, and restricting the scan turns it into "every visible row satisfies the predicate" instead of "every row in the table does". It applies to every operator reached through the relation — `exists`, `all`, `except`, membership, `hasIntersection`, `size` — and to the hop-existence guard, so an intermediate hop must exist *and* be visible.

## How it works

Cerbos can respond to a `PlanResources` request with one of three plan kinds. The adapter mirrors that API:

- `PlanKind.ALWAYS_ALLOWED`: The user can access the resource without any extra filtering.
- `PlanKind.ALWAYS_DENIED`: The user cannot access the resource at all.
- `PlanKind.CONDITIONAL`: Cerbos returns an expression tree that must be applied when reading data. The adapter converts this expression into a Drizzle SQL filter.

`queryPlanToDrizzle` walks the Cerbos expression, resolves every attribute reference through the mapper, and produces a Drizzle `SQL` fragment. That fragment can then be composed with the rest of your query builder chain (`db.select().from(table).where(result.filter)`).

## Installation

```bash
npm install @cerbos/orm-drizzle
```

## Usage

```ts
import { queryPlanToDrizzle, PlanKind } from "@cerbos/orm-drizzle";
import { eq, and } from "drizzle-orm";
import { resources } from "./schema";

const plan = await cerbos.planResources({
  principal,
  resource,
  action,
});

const result = queryPlanToDrizzle({
  queryPlan: plan,
  mapper: {
    "request.resource.attr.status": resources.status,
    "request.resource.attr.owner": resources.ownerId,
  },
});

if (result.kind === PlanKind.CONDITIONAL) {
  const rows = await db
    .select()
    .from(resources)
    .where(and(eq(resources.deleted, false), result.filter));
}
```

### Handling different plan kinds

```ts
const evaluation = queryPlanToDrizzle({ queryPlan: plan, mapper });

switch (evaluation.kind) {
  case PlanKind.ALWAYS_ALLOWED:
    // run the query without extra filters
    break;
  case PlanKind.ALWAYS_DENIED:
    // return an empty result immediately
    break;
  case PlanKind.CONDITIONAL:
    const rows = await db
      .select()
      .from(resources)
      .where(evaluation.filter);
    break;
}
```

Cerbos plans reference both resources (`request.resource.attr.*`) and principals (`request.principal.attr.*`), so include the paths your policies emit in the mapper.

### Database collation requirement

> **Every mapped string column must use a binary or case-sensitive collation.** CEL
> string comparison is exact and case-sensitive, while MySQL and PlanetScale commonly
> default to case-insensitive collations. With a CI collation, a database predicate can
> return `"Finance"` for a policy that allowed only `"finance"`, silently over-granting
> access compared with the PDP's `check()` decision.

On MySQL use a collation such as `utf8mb4_bin` or `utf8mb4_0900_as_cs`. PostgreSQL is
case-sensitive by default, but nondeterministic ICU collations and `citext` are not safe
for mapped policy attributes. On SQLite, do not apply `COLLATE NOCASE` to mapped columns.
This requirement covers equality and ordering, `in`, intersections, string matching, and
hierarchy prefix/ancestor comparisons.

**`contains`/`startsWith`/`endsWith` are the exception, and in your favour.** This adapter
lowers them to `REPLACE` rather than `LIKE` — chosen so a column-valued needle cannot be
reinterpreted as pattern syntax — and `REPLACE` is case-sensitive on SQLite, PostgreSQL and
MySQL alike. That also sidesteps a hazard `LIKE`-based adapters have to configure around:
SQLite's `LIKE` is case-insensitive for ASCII *regardless of collation*, so nothing but
`PRAGMA case_sensitive_like = ON` makes it exact. Here the collation requirement above is
about equality, ordering, membership and the hierarchy operators; the string operators are
correct without it. The corpus proves both halves — `cs-eq` for equality, and `cs-contains`,
`cs-startswith` and `cs-endswith` for string matching, on both the SQLite and PostgreSQL legs.

### Mapper options

The mapper associates Cerbos attribute references with Drizzle columns. It can be:

- A plain object where keys are Cerbos attribute references and values are Drizzle columns or SQL expressions.
- A function receiving the attribute reference and returning the column/expression.
- An object with a `column` property and optional metadata or a `transform` function to customize how operator/value pairs are converted into SQL.

```ts
const result = queryPlanToDrizzle({
  queryPlan,
  mapper: {
    "request.resource.attr.custom": {
      column: sql`lower(${resources.title})`,
      transform: ({ operator, value }) => {
        if (operator !== "eq") throw new Error("Unsupported");
        return eq(sql`lower(${resources.title})`, value.toLowerCase());
      },
    },
  },
});
```

### Attribute references and functions

- Plain values: map `request.resource.attr.field` to a column (`resources.field`).
- Nested attributes: map longer paths such as `request.resource.attr.owner.email`.
- Principal attributes: map `request.principal.attr.role` or similar paths when policies check the caller.
- Dynamic resolution: pass a mapper function `(reference) => ...` to compute mappings at runtime.

Every mapper entry can be:

- A column or SQL fragment.
- An object with `column` and/or `transform` to customize how each operator is translated.
- A relation mapping (described below) for nested resource structures.

Fields used through CEL's `timestamp()` must opt in with `valueType: "timestamp"`.
The adapter then validates strict RFC 3339 constants and normalizes them to UTC before comparing them. The instant must fall inside CEL's supported year 0001–9999 range and be exactly representable at millisecond precision: fractional digits after the third must be zero. The mapped column and database must preserve the same precision.

```ts
"request.resource.attr.createdAt": {
  column: resources.createdAt,
  valueType: "timestamp",
}
```

### Mapping relations

Relations can be described using the `relation` option, mirroring the structure of the Prisma adapter. The adapter will wrap
comparisons in `EXISTS` subqueries and automatically infer relation fields when they match the column names on the related table.

```ts
const result = queryPlanToDrizzle({
  queryPlan,
  mapper: {
    "request.resource.attr.owner": {
      relation: {
        type: "one",
        table: owners,
        sourceColumn: resources.ownerId,
        targetColumn: owners.id,
        fields: {
          email: owners.email,
        },
      },
    },
    "request.resource.attr.tags": {
      relation: {
        type: "many",
        table: resourceTags,
        sourceColumn: resources.id,
        targetColumn: resourceTags.resourceId,
        fields: {
          name: {
            relation: {
              type: "one",
              table: tags,
              sourceColumn: resourceTags.tagId,
              targetColumn: tags.id,
              field: tags.name,
            },
          },
        },
      },
    },
  },
});
```

With the above mapper, query plan references such as `request.resource.attr.owner.email` and `request.resource.attr.tags.name`
are translated into `EXISTS` expressions that join the `owners` and `tags` tables respectively.

Those `EXISTS` expressions read the mapped table bare. If your own reads of that table apply a predicate — a soft-delete flag, a tenant column, a subtype discriminator — declare it as `subqueryFilter` on the relation so the subquery sees the same rows the application serialised. See [Mapping hazards](#mapping-hazards).

### Working with collections

- `hasIntersection`: Use for multi-valued attributes such as tags. When Cerbos emits `hasIntersection(map(resource.tags, lambda t => t.name), ["tag"])`, the mapper looks up the nested field and the adapter converts it into a `column IN (...)` condition.
- `exists`, `exists_one`, and `all`: When policies reference array attributes (e.g., `request.resource.attr.tags`), mark the mapper entry as a relation. The adapter scopes the lambda variable, generates the `EXISTS` subquery, and correlates it with the parent table automatically.
- Scalar collections stored through a relation can opt in with `collectionValueType: "scalar"` and set the relation's `field`. This enables direct membership such as `R.attr.owner in R.attr.tagNames`, including explicit `null` elements.
- `filter`: Cerbos uses `filter` during plan construction. The adapter discards those lambdas because the entire filter is rerun in Drizzle land.

## Example application

This repository carries a runnable [`example/`](example/), which installs the adapter from the
artifact `npm publish` would upload and exercises it against a live PDP over the shared
[demo domain](../demo/README.md):

```bash
# from the repository root
demo/scripts/run-example.sh drizzle
```

Unlike the test suites, it resolves the adapter through its **published** surface — the `exports`
map, `types`, the `files` allowlist, and the peer range — and covers usage shapes past a single
flat query: pagination, and the adapter's filter composed with an application-owned filter.

## Testing

| Command | What it proves | What it needs |
| --- | --- | --- |
| `npm test` | **The SQL this adapter emits.** The translator unit test: every corpus action, classified exactly once as a golden expectation or as a throw | Nothing but Node — no Cerbos sidecar, no database, no Docker |
| `npm run test:adversarial` | **The rows that SQL returns**, against real SQLite with `check()` as the oracle | Cerbos CLI |
| `npm run test:adversarial:postgres` | The same corpus against real PostgreSQL | Cerbos CLI, Docker |
| `npm run golden:update` | — | Rewrites `golden/expectations.json` from what the translator emits today. Review the diff |

### The golden expectations

`npm test` reads its plans from `../conformance/wire-fixtures/` — the golden `PlanResources`
responses captured against the pinned Cerbos version — and asserts them against
`golden/expectations.json`, a **golden expectation** file this adapter owns. One entry per corpus
action, keyed by action name:

```jsonc
{
  "adapter": "drizzle",
  "regenerate": "npm run golden:update",
  "expectations": {
    "in-empty":  { "kind": "KIND_ALWAYS_DENIED" },
    "arith-add": {
      "kind": "KIND_CONDITIONAL",
      "rendered": {
        // One per store the adversarial suite executes, because what the driver is asked to bind
        // depends on the column type: a PostgreSQL `boolean` binds `true`, a SQLite one binds 1.
        "postgresql": { "sql": "\"adversarial_resources\".\"a_number\" + $1 > $2", "params": [1, 2] },
        "sqlite":     { "sql": "\"adversarial_resources\".\"a_number\" + ? > ?",   "params": [1, 2] }
      }
    }
  }
}
```

An action this adapter refuses carries **no entry**: its pinned message is corpus data, in
`conformance/actions.json`, and duplicating it here would be two places to change one string. A
wire fixture that is neither in this file nor declared unsupported fails the suite, which is what
makes a new corpus action land as a failure rather than as silence
([ADR 0006](../docs/adr/0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md),
[ADR 0007](../docs/adr/0007-adapters-share-data-not-code.md), and the "Golden expectations" section
of [conformance/README.md](../conformance/README.md)).

Whether those filters return the rows the PDP allows is a separate question, answered by the
adversarial suite, which does need a Cerbos sidecar and (for the PostgreSQL leg) Docker.
