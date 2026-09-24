# Cerbos + Drizzle ORM Adapter

Translates a [Cerbos](https://cerbos.dev) query plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan))
into a [Drizzle ORM](https://orm.drizzle.team/) `SQL` expression you can pass to `.where()`.

## Install

```bash
npm install @cerbos/orm-drizzle @cerbos/core
npm install @cerbos/grpc   # or @cerbos/http — any Cerbos client that can call planResources
```

- Node 22+
- Peer dependencies: `drizzle-orm` `^0.44.0 || ^0.45.0`, `@cerbos/core` `^0.32.0 || ^0.33.0`.
  Install `@cerbos/core` yourself so your Cerbos client and the adapter share one copy of the plan
  types (npm 7+ does this automatically; pnpm and Yarn need it declared).
- The adapter depends on no Cerbos client, so an HTTP application does not pull in gRPC.
- Stores executed in CI: SQLite, PostgreSQL, MySQL 8.0.17+. PlanetScale emits the same SQL as MySQL
  but is **not executed anywhere**, so the conformance contract does not cover it.
- String columns need a byte-exact collation — see
  [Database collation requirement](#database-collation-requirement).

## Quick start

```ts
import { GRPC } from "@cerbos/grpc";
import { PlanKind, queryPlanToDrizzle, type Mapper } from "@cerbos/orm-drizzle";
import { and, eq } from "drizzle-orm";
import { db } from "./db";
import { resources } from "./schema";

const cerbos = new GRPC("localhost:3593", { tls: false });

const mapper: Mapper = {
  "request.resource.attr.status": resources.status,
  "request.resource.attr.owner": resources.ownerId,
};

const queryPlan = await cerbos.planResources({
  principal: { id: "user1", roles: ["user"] },
  resource: { kind: "resource" },
  action: "view",
});

const result = queryPlanToDrizzle({ queryPlan, mapper });

let rows;
switch (result.kind) {
  case PlanKind.ALWAYS_DENIED:
    rows = []; // never run the query
    break;
  case PlanKind.ALWAYS_ALLOWED:
    rows = await db.select().from(resources).where(eq(resources.deleted, false));
    break;
  case PlanKind.CONDITIONAL:
    rows = await db
      .select()
      .from(resources)
      .where(and(eq(resources.deleted, false), result.filter));
    break;
}
```

`queryPlanToDrizzle` returns `{ kind }`, plus a `filter` (a Drizzle `SQL`) when the kind is
`CONDITIONAL`. Compose `filter` with your own predicates using `and()`. Handle `ALWAYS_DENIED` by
returning nothing: if you turn it into `undefined` and pass it to `and()`, Drizzle drops it and your
query returns rows the PDP denied. Any attribute the plan references but the mapper does not resolve
throws.

## Mapping attributes

The mapper maps Cerbos attribute references (`request.resource.attr.*`, `request.principal.attr.*`,
including nested paths such as `request.resource.attr.owner.email`) to Drizzle columns. It is either
an object keyed by reference, or a function `(reference) => MapperEntry | undefined`.

Each entry is one of:

| Entry | Use |
| --- | --- |
| A Drizzle column | The common case |
| `{ column, ...options }` | A column plus options: `valueType`, `indexable`, `nullAttributeRepresentation` |
| `{ transform }` or a bare function | You build the SQL for each comparison yourself |
| `{ relation: { ... } }` | The attribute lives on a related table |

### Custom transforms

A transform receives `{ operator, value }` (`operator` is one of `eq`, `ne`, `lt`, `le`, `gt`, `ge`,
`in`, `contains`, `startsWith`, `endsWith`) and returns `SQL`:

```ts
import { eq, sql } from "drizzle-orm";

const mapper: Mapper = {
  "request.resource.attr.title": {
    transform: ({ operator, value }) => {
      if (operator !== "eq") throw new Error(`Unsupported operator ${operator}`);
      return eq(sql`lower(${resources.title})`, String(value).toLowerCase());
    },
  },
};
```

A transform mapping cannot be used as a value expression (for example, the other side of a
field-to-field comparison) or with index access; those throw.

### Relations

Relations become correlated `EXISTS` subqueries. `type` is `"one"` or `"many"`; name the related
table and the join columns, then either a single `field` or a `fields` map. A path segment that is
not in `fields` falls back to the related table's column of that name.

```ts
const mapper: Mapper = {
  "request.resource.attr.owner": {
    relation: {
      type: "one",
      table: owners,
      sourceColumn: resources.ownerId,
      targetColumn: owners.id,
      fields: { email: owners.email },
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
};
```

`request.resource.attr.owner.email` and `request.resource.attr.tags.name` now translate into
`EXISTS` subqueries over `owners` and `tags`.

The subquery reads the table **bare**. If your own reads of that table filter it (soft delete,
tenant, subtype), declare the same predicate as [`subqueryFilter`](#declaring-the-applications-own-predicate).
See [Mapping hazards](#mapping-hazards).

### Collections

- `hasIntersection(map(R.attr.tags, t, t.name), ["a"])` becomes `column IN (...)` over the mapped
  field. Either operand order works; a pair with no literal list throws.
- `exists`, `exists_one` and `all` over a relation-mapped attribute become correlated subqueries.
  Over a literal list (a principal attribute the planner folded) each element is substituted into
  the lambda: `exists` and `all` become an `OR` / `AND` of the results, and `exists_one` a count of
  the TRUE ones that is NULL if any element's condition is UNKNOWN, since CEL's `exists_one`
  absorbs no error.
- `filter()` is supported inside `size(filter(...))`. On its own it returns a list, not a boolean,
  and throws.
- For a relation that stores scalar values, set `collectionValueType: "scalar"` and the relation's
  `field`. This enables membership such as `R.attr.owner in R.attr.tagNames`, including explicit
  `null` elements.

## Timestamps

Attributes compared through CEL's `timestamp()` must opt in:

```ts
"request.resource.attr.createdAt": { column: resources.createdAt, valueType: "timestamp" },
```

Constants must be strict RFC 3339, within years 0001–9999, and exactly representable at millisecond
precision (digits after the third fractional digit must be zero). They are normalized to UTC. Your
column and database must keep the same precision. Sub-millisecond `now()` thresholds and
`timestamp()` over an untyped string throw.

## Indexed collection columns

To translate `R.attr.tags[0] == "public"`, declare ordered column storage:

```ts
const mapper = {
  "request.resource.attr.tags": {
    column: resources.tags,
    indexable: "json", // or "pgArray" for a PostgreSQL array column
  },
} satisfies Mapper;
```

| `indexable` | Supported columns |
| --- | --- |
| `"json"` | PostgreSQL JSON/JSONB, SQLite JSON text, MySQL JSON |
| `"pgArray"` | PostgreSQL arrays of `text`, `varchar`, `boolean`, `integer`, `smallint` |

- Supported: `==` / `!=` against a scalar literal (string, finite number, boolean, null), in either
  operand order, under any logical operator. The index must be a constant non-negative 32-bit
  integer. Positions are zero-based, including PostgreSQL arrays with a nonstandard lower bound.
  Values and paths are bound parameters.
- Throws: dynamic or negative indexes, object-field projection (`get-field`), ordered comparisons,
  indexes nested inside other value expressions, a mapping with a `transform`, and undeclared
  storage (a related table does not define list order).
- Refused array types: numeric, bigint, temporal and custom decoders (their application values can
  differ from SQL), and floating-point arrays (PostgreSQL serializes NaN/infinity elements as JSON
  strings).
- An absent element, NULL collection or non-array JSON value is SQL UNKNOWN, so the row stays
  excluded under negation. A null **element** is a value: `[null][0] == null` and
  `[null][0] != "public"` are both true. `nullAttributeRepresentation` does not affect elements.
- The element's JSON type is checked before comparing, because CEL equality is heterogeneous
  (`[true][0] == 1` is false) while SQLite and MySQL read a JSON `true` back as `1`.
- Membership: without a `relation`, the column is also the collection that `x in R.attr.list`
  and `hasIntersection(R.attr.list, [...])` search. The adapter walks its elements (`json_each` on
  SQLite, `jsonb_array_elements` on PostgreSQL, `JSON_TABLE` on MySQL) with the same JSON-type
  check, so `"2" in [2]` matches nothing even where the store would equate the two. A NULL or
  non-array column is SQL UNKNOWN. Any other scalar comparison against such a column throws.
- An entry may carry both `column` + `indexable` and a `relation`: indexing reads the column,
  collection predicates read the relation. Both must hold exactly the list you send to Cerbos,
  including null elements; do not apply custom decoding that changes the values.

## NULL attribute representation

The planner emits the same `eq(x, null)` node whether your application sends a NULL column to
`check()` as an explicit `null` or omits the attribute, so you must tell the adapter which you do.

| attributes you send for a NULL column | `check()` on that row | null-matching filter |
| --- | --- | --- |
| `{"x": null}` — explicit null | allow | selects it — aligned |
| `{}` — attribute omitted | **deny** (CEL missing-attribute error) | selects it — **over-grants** |

The call-level `nullAttributeRepresentation` defaults to `"explicit"`. If you omit attributes for
NULL columns, set it to `"omitted"`: every null comparison operand then throws instead of producing a
filter that returns rows the PDP denies.

```ts
queryPlanToDrizzle({ queryPlan, mapper, nullAttributeRepresentation: "omitted" });
```

This rejects more than the shapes that actually over-grant (`x != null` is fine either way), because
a leaf cannot see whether an enclosing `not` will flip it
([#302](https://github.com/cerbos/query-plan-adapters/issues/302)). The option is scoped to each
call, including when a mapper starts another translation.

### Declare the convention per attribute

One policy suite can use both conventions (the same column sent as explicit null under one name,
omitted under another). Declare it on the mapper entry; the call-level option covers undeclared
entries:

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

Declaring `"explicit"` asserts that the column can be NULL **and** that NULL reaches `check()` as an
explicit null. The equality family (`eq`, `ne`, `in`) is then rendered so it never yields SQL
UNKNOWN: CEL's `null != "x"` is true and the row must come back. Ordering and string operators are
unchanged, since CEL errors (and denies) on a null receiver. Declaring `"omitted"` on an entry
applies the null-operand rejection to that attribute only.

- Undeclared attributes keep the default rendering, so `!=` against a constant under-grants NULL
  rows until you declare them.
- **Declare both sides of a field-to-field comparison, or neither.** Mixing conventions in one
  comparison throws.

See [#308](https://github.com/cerbos/query-plan-adapters/issues/308) and
[ADR 0004](../docs/adr/0004-the-null-convention-is-a-property-of-the-attribute.md).

## Database collation requirement

> **Every mapped string column must use a byte-exact collation.** CEL compares strings byte for
> byte. With a case-insensitive collation, a filter can return `"Finance"` for a policy that
> allowed only `"finance"` — an over-grant.

- **MySQL / PlanetScale:** use `utf8mb4_0900_bin` (MySQL 8.0.17+), which is byte-exact and NO PAD.
  Case-sensitive is not enough: `utf8mb4_0900_as_cs` ignores a soft hyphen (`'o­ne' = 'one'`),
  and `utf8mb4_bin` is PAD SPACE (`'a' = 'a '`). Replaying the corpus on `mysql:8.4`, the default
  `utf8mb4_0900_ai_ci` makes **62 of the 268 compared cases** disagree with the PDP, and
  `utf8mb4_0900_as_cs` makes **16** disagree, all on the soft-hyphen seed `h6`
  ([#474](https://github.com/cerbos/query-plan-adapters/issues/474)).
- **PostgreSQL:** the default is fine. Do not use nondeterministic ICU collations or `citext` for
  mapped attributes.
- **SQLite:** do not apply `COLLATE NOCASE` to mapped columns.

This covers equality, ordering, `in`, intersections and hierarchy comparisons.

`contains` / `startsWith` / `endsWith` do not depend on it: they are lowered to `REPLACE` (so a
column-valued needle is never read as `LIKE` pattern syntax), which is case-sensitive on all three
stores. That also avoids SQLite's `LIKE`, which is ASCII case-insensitive regardless of collation.

`string()` over a boolean column produces the literals `'true'` / `'false'`. On MySQL a literal
compares in the *connection's* collation (mysql2 defaults to `utf8mb4_unicode_ci`), so the adapter
renders them as `_utf8mb4'true' COLLATE utf8mb4_0900_bin`. That explicit collation also applies to
the other operand, so `string(R.attr.flag) == R.attr.label` compares `label` byte-exactly. SQLite
and PostgreSQL literals carry no collation.

## Supported operators

| Kind | Operators |
| --- | --- |
| Logical | `and`, `or`, `not` |
| Comparison | `eq`, `ne`, `lt`, `gt`, `le`, `ge`, `in` |
| String | `contains`, `startsWith`, `endsWith` (via `REPLACE`), `size()` over a string, `+` (concatenation) |
| Null | `eq` / `ne` against null become `IS NULL` / `IS NOT NULL` (the planner has no existence operator) |
| Collections | `hasIntersection`, `exists`, `exists_one`, `all`, `size`, `size(filter(...))`, `except`, membership |
| Other | arithmetic, ternaries, hierarchy operations, typed timestamps, index access, `string()` over a boolean or text column, `string()` of a number compared for equality with a string |

Shapes the adapter cannot express throw `UnsupportedQueryPlanError` rather than emit a broader
filter. It is exported and extends `Error`, so existing `catch` blocks keep working:

```ts
import { queryPlanToDrizzle, UnsupportedQueryPlanError } from "@cerbos/orm-drizzle";

try {
  const result = queryPlanToDrizzle({ queryPlan, mapper });
} catch (error) {
  if (error instanceof UnsupportedQueryPlanError) {
    // The policy uses a shape this adapter cannot translate faithfully: deny, or fall back to
    // per-row check() calls.
  }
  throw error;
}
```

A mapper misconfiguration, such as a reference with no mapping or a column of the wrong type for
its declaration, stays a plain `Error`. The shapes this adapter refuses are listed, with reasons, in
[`conformance-ledger.json`](conformance-ledger.json).

## Conformance contract

The adapter is replayed against the shared [conformance corpus](../conformance/README.md): the plans
and `check()` decisions recorded from Cerbos PDP 0.55.0 (and 0.54.0), executed as real Drizzle
queries over the corpus's 29 seed rows on SQLite, PostgreSQL and MySQL (under `utf8mb4_0900_bin`).
Passed cases on the current PDP, 0.55.0, identical on all three stores. The total is every golden
case in the tier; planner-divergence cases are skipped, not run, and count as not passed:

| Tier | Passed / total |
| --- | --- |
| core | 26 / 26 |
| extended | 64 / 80 |
| adversarial | 190 / 227 |

Every case that runs and does not pass is refused with `UnsupportedQueryPlanError`; none returns
wrong rows on 0.55.0. [`conformance-ledger.json`](conformance-ledger.json) lists each one with its
reason. One extended case, `null/has/missing-attribute`, is a known Cerbos planner divergence and is
skipped: the planner folds it to `ALWAYS_ALLOWED` while `checkResource` denies the missing-attribute
rows, so use `R.attr.x != null` for database-backed attributes instead of `has(R.attr.x)`.

## Mapping hazards

The conformance contract proves the *plan* side. The *mapping* side is on you: **the rows the
subquery reads must be the rows your application put into the resource attributes.** The shared
corpus catalogues six ways that can break.

This adapter builds a **bare-table subquery**: a relation mapping is a table plus two columns, and
Drizzle has no association metadata, so nothing your own reads apply reaches the `EXISTS`. Declare
that narrowing as [`subqueryFilter`](#declaring-the-applications-own-predicate). The adapter cannot
detect a missing one.

| Hazard | Position | Mechanism to check |
|---|---|---|
| Filtered association | **Caller-owned**, reproducible with `subqueryFilter` | The `where` you pass to Drizzle's relational query builder (`db.query.<table>.findMany({ with: { rel: { where } } })`), and any repository helper that appends one. The adapter reads `table` bare |
| Default scope on the target model | **Caller-owned**, reproducible with `subqueryFilter` | A soft-delete column (`deletedAt IS NULL`), a tenant column, a `published` flag — anything every read of that table filters on. Drizzle has no `default_scope`, so only you can see it |
| Subtype discrimination | **Caller-owned**, reproducible with `subqueryFilter` | A `type`/`kind` discriminator column where one table holds several row kinds. Declare `eq(table.type, "…")` |
| To-one relation used as a collection | **Caller-owned** | A `type: "one"` relation whose target column has no unique index. `type` is declarative and emits the same `EXISTS` either way, so add the unique constraint, or accept that the subquery examines every matching row |
| Composite association key | **Rejected by the type system** | `sourceColumn`/`targetColumn` are each a single `AnyColumn`, so a two-column key is a compile error, not a wrong join |
| Absent to-one parent | **Reproduced**, and proved by the corpus (`relation/all/to-one-chain`, `relation/bare-attribute/negated-one-hop-boolean` and the rest of the `relation/*` cases) | None — every operator reached through a relation requires each to-one hop, so a missing parent is UNKNOWN under both polarities ([#309](https://github.com/cerbos/query-plan-adapters/issues/309), [#315](https://github.com/cerbos/query-plan-adapters/issues/315), [#375](https://github.com/cerbos/query-plan-adapters/issues/375), [#430](https://github.com/cerbos/query-plan-adapters/issues/430)) |

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

`subqueryFilter` is ANDed into the correlated subquery next to the join, so it narrows the rows the
subquery examines. That keeps negation correct (`all()` becomes "every *visible* row matches"). It
applies to every operator reached through the relation — `exists`, `all`, `except`, membership,
`hasIntersection`, `size` — and to the hop-existence guard.

## Behaviour changes

- A hierarchy built from segments — `hierarchy(["projects", R.id])` — now translates against a
  constant hierarchy or another built one: its length is known, so `ancestorOf`, `descendentOf` and
  `overlaps` become equalities between the segments of the shared prefix. A NULL column segment is
  an error in CEL, so it leaves the result UNKNOWN. A built path against a column-backed hierarchy
  still throws.
- `int()` over an integer column (`integer`, `smallint`, `int`, `serial`, `bigint` in `number`
  mode, …) now translates, as the column itself (`CAST(… AS INTEGER)` on SQLite, whose INTEGER
  affinity can keep a fraction). `int()` of any other column still throws.
- **Breaking:** `%` translates only as `int(<integer column>) % <non-zero whole constant>`, and
  throws otherwise. It used to be emitted for any operands, but CEL's `%` has no double overload —
  `R.attr.aNumber % 2` is an error the PDP denies, which the old filter answered — and a zero
  divisor raises on PostgreSQL.
- String `+` now translates instead of throwing: `||` on SQLite and PostgreSQL, `CONCAT()` on
  MySQL, where `||` is logical OR. As for `size()` and indexed storage, the dialect is read off the
  Drizzle class of a column among the operands, so a concatenation reaching no Drizzle column
  (only callback mappings, or columns of two dialects) still throws. A NULL operand makes the
  concatenation NULL on all three stores, so a missing attribute stays excluded under both
  polarities. A `+` nested inside another (`(a + b) + (c + d)`) is recognized as concatenation too.
- A shape the adapter refuses now throws `UnsupportedQueryPlanError`, an exported subclass of
  `Error`. What it translates is unchanged, and existing `catch` blocks keep working; mapper
  misconfiguration stays a plain `Error`.
- [#509](https://github.com/cerbos/query-plan-adapters/issues/509): a collection macro nested over
  the relation an enclosing macro iterates — `tags.exists(t, tags.exists(u, u.name != t.name))` —
  now gives the inner subquery its own alias (`cerbos_<table>_<n>`), so the lambda compares its
  element with the enclosing one. Both subqueries used to range over the bare table name, SQL
  resolved `t.name` to the inner row, and the body compared each element with itself: an
  under-grant, and an over-grant under negation. An inner element read that cannot be rebound to
  the alias — a transform, a further relation hop, or a relation carrying a `subqueryFilter` —
  now throws, as does testing an enclosing element's column for membership in a collection stored
  in the same table.
- An `in` list or `hasIntersection` list against a string, number or boolean column now drops the
  constants of another type before binding them, because CEL's `5 in ["5", 2]` never matches the
  `"5"`. The store used to convert it: SQLite and PostgreSQL read `'5'` as 5, and MySQL reads a
  non-numeric string as 0, so `aNumber in ["5", 2]` returned rows with `aNumber` 5, and
  `hasIntersection(tags.map(t, t.name), ["public", 0])` returned every tag on MySQL. Both were
  over-grants (`type-mismatch/in/number-field-in-mixed-literal-list`, `type-mismatch/has-intersection/mapped-names-against-mixed-literal-list`). A list left with no
  constant of the column's type is false.
- **Breaking:** membership in an `indexable` column without a `relation` used to compare the whole
  column with the literal as one scalar, so `2 in R.attr.list` matched no row and its negation
  matched every row. It now searches the list's elements, and any other scalar comparison against
  such a column throws.
- **Breaking** (Cerbos 0.55): ordered comparisons involving NaN evaluate to false, so their negation
  can allow a row. Cerbos 0.54 denied it. Use Cerbos 0.55 if policies can produce NaN in a negated
  comparison.
- **Breaking:** `string()` over a number or text column no longer emits `CAST(… AS TEXT)`, a syntax
  error on MySQL ([#340](https://github.com/cerbos/query-plan-adapters/issues/340)). It translates
  without a cast where it can: over a text column it is the column itself, and over a number column
  compared with `==` / `!=` against a string (`string(R.attr.aDouble) == "-0.6"`) it becomes a
  numeric comparison against the one double CEL spells that way — so `"1e+06"` matches `1000000`,
  and `"2.0"`, which CEL never produces, matches no row. Any other `string()` of a number (an
  ordering, `"0"`, `"NaN"`, a column-valued other side) throws.
- `string()` over a **boolean** column now translates instead of throwing, as a `CASE` whose
  `IS NULL` arm keeps NULL rows excluded under both polarities
  ([#418](https://github.com/cerbos/query-plan-adapters/issues/418)).
- **Breaking:** `hasIntersection` normalizes operand order, so the value-first spelling translates
  instead of becoming `FALSE`; an operand pair with no literal list now throws
  ([#387](https://github.com/cerbos/query-plan-adapters/issues/387)).
- **Breaking:** a hierarchy with an empty delimiter (`hierarchy(R.attr.scope, "")`) throws. Its old
  filter matched the path itself (`hierarchy/descendent-of/empty-delimiter` returned a denied row).
- **Breaking:** bare temporal field comparisons, whole-list equality, list-valued membership
  needles, and nested division where an inner zero divisor could be evaluated before the outer
  guard now throw before returning SQL. They previously emitted incorrect filters or failed in the
  database.
- On MySQL columns, string lengths use `CHAR_LENGTH` instead of `LENGTH` (`size()` over a string,
  and the `SUBSTR` bounds for `startsWith`/`endsWith` and hierarchy prefixes). `LENGTH` counts
  bytes; this fixed an over-grant in `string/starts-with/negated-field-to-field`
  ([#473](https://github.com/cerbos/query-plan-adapters/issues/473),
  [#474](https://github.com/cerbos/query-plan-adapters/issues/474)). A non-column mapping keeps
  `length()`.
- Negated scalar comparisons, string matches and hierarchy predicates through a to-one relation now
  exclude rows whose parent is absent — an over-grant fix
  ([#430](https://github.com/cerbos/query-plan-adapters/issues/430)).
- The absent-parent guard now applies over a **single** to-one hop, including a bare boolean read
  through it; a negation over one hop used to return every row with no parent — an over-grant fix
  ([#375](https://github.com/cerbos/query-plan-adapters/issues/375)).
- Indexed scalar equality (and its negated, null, number and boolean variants) is now executed
  against the corpus oracle on SQLite, PostgreSQL (JSON, JSONB and zero-based native arrays) and
  MySQL. Each query still runs entirely in the database.

## Example application

[`example/`](example/) installs the packed adapter and runs the shared
[demo domain](../demo/README.md) against a live PDP, including pagination and the adapter's filter
composed with an application filter:

```bash
# from the repository root
demo/scripts/run-example.sh drizzle
```

## Testing

| Command | What it proves | What it needs |
| --- | --- | --- |
| `npm test` | Caller-supplied options the corpus cannot vary (mapper forms, `transform`, `subqueryFilter`, declared index storage, `nullAttributeRepresentation`), the refusal type, the timestamp literal contract and malformed input | Node only — no Cerbos, database or Docker |
| `npm run test:adversarial` | The rows each recorded plan returns on real SQLite equal the recorded `check()` decisions, for both pinned PDPs | Node only |
| `npm run test:adversarial:postgres` | The same corpus on real PostgreSQL, plus the list cases under `pgArray` and plain `json` storage | Docker |
| `npm run test:adversarial:mysql` | The same corpus on real MySQL under `utf8mb4_0900_bin`. Set `ADAPTER_TEST_MYSQL_COLLATION=utf8mb4_0900_ai_ci` to measure MySQL's default | Docker |

No suite starts a PDP. The harness reads the golden files under `../conformance/golden/` and applies
[`conformance-ledger.json`](conformance-ledger.json): a case with no entry must return exactly the
recorded allowed ids, and an `unsupported` case must throw `UnsupportedQueryPlanError`. See
"The harness contract" in [conformance/README.md](../conformance/README.md).
