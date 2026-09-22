# Cerbos + Prisma ORM Adapter

Converts a [Cerbos](https://cerbos.dev) query plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan))
into a [Prisma](https://prisma.io) `where` input, so `findMany` returns only the records a principal
is allowed to see. Use it with the [Cerbos JavaScript SDK](https://github.com/cerbos/cerbos-sdk-javascript).

## Install

```bash
npm install @cerbos/orm-prisma @cerbos/core
npm install @cerbos/grpc   # or @cerbos/http — whichever client your deployment uses
```

- **Node.js** 22 or later.
- **`@prisma/client`** 5, 6 or 7 (peer dependency). CI runs Prisma 6 and 7.
- **`@cerbos/core`** `^0.32.0 || ^0.33.0` (peer dependency). It carries the query plan types, and
  your application and the adapter must share one copy. npm 7+ installs peers automatically; pnpm
  and Yarn expect you to declare it.
- **A Cerbos client** (`@cerbos/grpc` or `@cerbos/http`). The adapter depends on neither.
- **Cerbos PDP** 0.55 or later is recommended; the contract below is tested against 0.55.0.
- **A byte-exact database collation** on mapped columns, and on SQLite
  `PRAGMA case_sensitive_like = ON`. See
  [Database collation is an authorization invariant](#database-collation-is-an-authorization-invariant).

## Quick start

```ts
import { GRPC } from "@cerbos/grpc";
import { PlanKind, queryPlanToPrisma } from "@cerbos/orm-prisma";

import { prisma } from "./db"; // your PrismaClient instance

const cerbos = new GRPC("localhost:3593", { tls: false });

const queryPlan = await cerbos.planResources({
  principal: { id: "user1", roles: ["USER"] },
  resource: { kind: "document" },
  action: "view",
});

const result = queryPlanToPrisma({
  queryPlan,
  mapper: {
    "request.resource.attr.ownerId": { field: "ownerId" },
    "request.resource.attr.status": { field: "status" },
  },
});

let documents;
switch (result.kind) {
  case PlanKind.ALWAYS_DENIED:
    documents = [];
    break;
  case PlanKind.ALWAYS_ALLOWED:
    documents = await prisma.document.findMany({ where: { archived: false } });
    break;
  case PlanKind.CONDITIONAL:
    documents = await prisma.document.findMany({
      where: { AND: [result.filters, { archived: false }] },
    });
    break;
}
```

`queryPlanToPrisma` returns `{ kind }` for `ALWAYS_ALLOWED` / `ALWAYS_DENIED` and
`{ kind: CONDITIONAL, filters }` otherwise. `filters` is a plain where-input for the model, so you
can `AND` it with your own predicates, paginate, or add `select`/`orderBy`. Short-circuit
`ALWAYS_DENIED` instead of querying. A shape the adapter cannot express throws an `Error`; it never
returns a broader filter.

### Arguments

| Argument | Required | Description |
| --- | --- | --- |
| `queryPlan` | yes | The `PlanResourcesResponse` from `planResources`. |
| `mapper` | no | Maps Cerbos attribute paths to Prisma fields and relations. Defaults to `{}`. |
| `model` | no | Prisma model name of the queried model (e.g. `"Resource"`). Needed only for [field-to-field comparisons](#field-to-field-comparisons) between root columns. |
| `nullAttributeRepresentation` | no | `"explicit"` (default) or `"omitted"`. See [NULL attribute representation](#null-attribute-representation). |

Exported types: `QueryPlanToPrismaArgs`, `QueryPlanToPrismaResult`, `Mapper`, `MapperConfig`,
`NullAttributeRepresentation`, `PrismaFilter`, and `PlanKind` (re-exported from `@cerbos/core`).

## Mapping attributes

Cerbos attribute paths are not column names, so you map them. The mapper is an object keyed by the
full attribute path, or a function from path to config:

```ts
// Object
mapper: {
  "request.resource.attr.title": { field: "title" },
  "request.resource.attr.createdAt": { field: "createdAt", valueType: "dateTime" },
}

// Function
mapper: (path) => ({ field: path.replace("request.resource.attr.", "") })
```

An unmapped path is used verbatim as the Prisma field name, which makes the query fail.

### `MapperConfig`

| Key | Description |
| --- | --- |
| `field` | Prisma field name. |
| `valueType` | `"string"`, `"number"`, `"boolean"` or `"dateTime"`. Lets the adapter reject incompatible comparisons and string operations before Prisma sees them. Required (`"dateTime"`) for `timestamp()` comparisons. Undeclared keeps the untyped behaviour; the adapter cannot read your schema. |
| `nullable` | `false` declares the column cannot be NULL, which drops the NULL guards on [relation elements](#relation-element-nullability) and hierarchy segments. |
| `nullAttributeRepresentation` | Per-attribute NULL convention. See [Declare the convention per attribute](#declare-the-convention-per-attribute). |
| `relation.name` | Prisma relation field name. |
| `relation.type` | `"one"` (compiles to `is`/`isNot`) or `"many"` (`some`/`every`/`none`). |
| `relation.field` | Column to compare when the policy compares the relation itself (e.g. `"x" in R.attr.tags`). |
| `relation.fields` | Mappings for fields of the related model, recursively (a nested entry can itself have a `relation`). Unmapped fields are inferred from the path. |
| `relation.model` | Prisma model name of the related model. Needed only for field-to-field comparisons inside a collection expression. |
| `relation.subqueryFilter` | A where-input over the related model that your application applies to its own reads. See [Declaring the application's own predicate](#declaring-the-applications-own-predicate). |

### Relations

```ts
mapper: {
  // to-one; nested fields are inferred from the path (owner.id -> owner.is.id)
  "request.resource.attr.owner": { relation: { name: "owner", type: "one" } },

  // to-many compared as a list of names: "x" in R.attr.tags
  "request.resource.attr.tags": { relation: { name: "tags", type: "many", field: "name" } },

  // to-many used in collection macros: R.attr.comments.exists(c, c.status == "approved")
  "request.resource.attr.comments": {
    relation: {
      name: "comments",
      type: "many",
      fields: {
        id: { field: "id", nullable: false },
        status: { field: "status" },
        author: { relation: { name: "author", type: "one" } },
      },
    },
  },
}
```

A policy like `R.attr.status == "active" && R.attr.owner.id == P.id && "tag1" in R.attr.tags`
translates to:

```ts
{
  AND: [
    { status: { equals: "active" } },
    { owner: { is: { id: { equals: "user1" } } } },
    { tags: { some: { name: { equals: "tag1" } } } },
  ],
}
```

The nested filters read the related model **unfiltered**. If your own reads of that model apply a
predicate (soft delete, tenant, subtype), declare it as `subqueryFilter`. See
[Mapping hazards](#mapping-hazards).

### Field-to-field comparisons

Comparisons between two columns of the same model compile to
[Prisma field references](https://www.prisma.io/docs/orm/reference/prisma-client-reference#compare-columns-in-the-same-table).
Pass `model` for root columns, or `relation.model` for columns of a related model inside a
collection expression. Comparisons across models throw: Prisma field references are same-model only.

```ts
queryPlanToPrisma({ queryPlan, mapper, model: "Resource" });
```

### Timestamps

Mark `DateTime` columns with `valueType: "dateTime"` and compare them with `timestamp()`.
`timestamp()` over an untyped or string mapping throws, and so does a bare comparison between two
mapped `DateTime` columns (use `timestamp()` on both sides). Literals must be strict RFC 3339
instants in CEL's year 0001–9999 range and exactly representable in milliseconds (digits after the
third fractional digit must be zero). Your column must preserve millisecond precision.

## NULL attribute representation

`R.attr.x == null` produces the same plan however your application represents a NULL column in the
attributes it sends to `check()`, so tell the adapter which convention you use:

| Attributes you send for a NULL column | `check()` on that row | `IS NULL` filter |
| --- | --- | --- |
| `{"x": null}` — explicit null | allow | selects it — aligned |
| `{}` — attribute omitted | **deny** (CEL missing-attribute error) | selects it — **over-grants** |

The default is `"explicit"` (`IS NULL`). If you omit attributes for NULL columns, set `"omitted"`:
the adapter then rejects every null comparison operand rather than emit a filter that returns
denied rows.

```ts
queryPlanToPrisma({ queryPlan, mapper, nullAttributeRepresentation: "omitted" });
```

The rejection covers every null operand, including `x != null`, which is aligned under both
conventions: Prisma negates by wrapping in `{ NOT: … }`, so a leaf cannot tell whether an enclosing
`not` will flip it back. See [#302](https://github.com/cerbos/query-plan-adapters/issues/302).

### Declare the convention per attribute

One policy suite can use both conventions (the same column mapped under two attribute names). Set
the convention on the mapper entry; the call-level option covers only undeclared attributes:

```ts
const mapper = {
  // sent as an explicit null when the column is NULL
  "request.resource.attr.owner": { field: "ownerId", nullAttributeRepresentation: "explicit" },
  // omitted when the column is NULL — the call-level default applies
  "request.resource.attr.department": { field: "department" },
};
```

Declaring `"explicit"` asserts the column can be NULL **and** that NULL reaches `check()` as an
explicit null. The equality family (`eq`, `ne`, `in`) is then rendered so it is never SQL UNKNOWN:
`null != "x"` is true in CEL, so the row is returned. Ordering and string operators are unchanged,
because CEL denies them on a null receiver anyway. An undeclared attribute keeps the old rendering,
under which `!=` against a constant under-grants NULL rows.

Declare both sides of a field-to-field comparison, or neither. Mixed conventions throw. See
[#308](https://github.com/cerbos/query-plan-adapters/issues/308) and
[ADR 0004](../docs/adr/0004-the-null-convention-is-a-property-of-the-attribute.md).

## Relation element nullability

Collection macros lower to `some`/`every`/`none`, which treat SQL UNKNOWN as false. Without a guard,
`!R.attr.tags.exists(t, t.name == "x")`, `all()` and `hasIntersection()` over `map()` would return
rows holding a NULL element that `check()` denies. So a relation element column is treated as
nullable and guarded **unless its mapping says `nullable: false`**.

Declare `nullable: false` on every required element column a policy reaches through a macro:

```ts
"request.resource.attr.tags": {
  relation: {
    name: "tags",
    type: "many",
    fields: {
      id: { field: "id", nullable: false }, // String   — required
      name: { field: "name" },              // String?  — nullable, guarded
    },
  },
},
```

If you leave a required column undeclared, Prisma rejects the guard's `null` comparison
(``Argument `id` must not be null``) and the query fails. This fails closed, but it still fails.
Declarations are read from nested `relation.fields` for chained collections
(`R.attr.a.b.exists(...)`).

## Database collation is an authorization invariant

Cerbos string comparisons are byte-exact. Prisma leaves comparison semantics to the database
collation, so a case- or accent-insensitive collation makes the filter return rows Cerbos denies.
The adapter cannot set a collation or a pragma from inside a `where`.

- **PostgreSQL:** a deterministic, case-sensitive collation. No `citext`, no `mode: "insensitive"`
  on mapped fields.
- **MySQL:** `utf8mb4_0900_bin` (MySQL 8.0.17+), the only collation that is byte-exact and NO PAD.
  `utf8mb4_0900_as_cs` is not enough: it ignores default-ignorable code points, so
  `'o­ne' = 'one'` ([#474](https://github.com/cerbos/query-plan-adapters/issues/474)).
  `utf8mb4_bin` is PAD SPACE, so `'a' = 'a '`. On MariaDB the equivalent is `utf8mb4_nopad_bin`
  (not executed here).

  **You must convert the tables yourself.** Prisma's migration engine writes
  `COLLATE utf8mb4_unicode_ci` into every `CREATE TABLE`, ignores the server default, and has no
  schema attribute to override it, so a Prisma-managed MySQL database is case- and
  accent-insensitive out of the box. After migrating:

  ```sql
  ALTER TABLE `YourModel` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
  ```

  Measured on `mysql:8.4`: under `utf8mb4_unicode_ci`, **58 of the 172 oracle-tested actions
  disagree with the PDP** (`cs-eq` returns `"One"` for a policy that allowed `"one"`); under
  `utf8mb4_0900_as_cs`, **17** disagree, all on the soft-hyphen seed `h6`.
- **SQL Server:** a case-sensitive (`_CS_`) collation, not `_CI_`.
- **SQLite:** no `COLLATE NOCASE` on mapped fields, **and** `PRAGMA case_sensitive_like = ON` on
  every connection. `contains`/`startsWith`/`endsWith` lower to `LIKE`, which is ASCII
  case-insensitive on SQLite regardless of column collation. The pragma is per connection, not per
  schema. Without it the adapter over-grants the corpus's `cs-contains`, `cs-startswith` and
  `cs-endswith` actions.

See Prisma's [case-sensitivity documentation](https://docs.prisma.io/docs/orm/v6/prisma-client/queries/case-sensitivity).

## Supported operators

| Category | Operators |
| --- | --- |
| Logical | `and`, `or`, `not` |
| Comparison | `eq`, `ne`, `lt`, `gt`, `lte`, `gte`, `in` |
| Null checks | `== null` / `!= null` → `{ equals: null }` / `{ not: null }` (the planner has no existence operator) |
| Strings | `startsWith`, `endsWith`, `contains`; a constant receiver with a column needle (`"a-b".startsWith(R.attr.x)`) becomes an `in` over the candidate needles |
| Relations | to-one `is`/`isNot`; to-many `some`/`every`/`none` |
| Collections | `exists`, `all`, lambda `except`, `hasIntersection`, `map`/`filter` inside another expression, emptiness of a mapped relation |
| Arithmetic | `add`, `sub`, `mult`, `div` against a constant, solved to a plain comparison (`R.attr.n + 1 > 2` → `{ n: { gt: 1 } }`; negative multipliers flip the direction); string concatenation solving (`P.attr.ctx == "projects:" + R.attr.id` → `{ id: { equals: "…" } }`) |
| Hierarchy | `hierarchy(string)`, `hierarchy(string, delimiter)`, `hierarchy([segments])`, `overlaps`, `ancestorOf`, `descendentOf` |
| Timestamps | `timestamp()` over `valueType: "dateTime"` columns |

Outer-column references inside a macro (`R.attr.tags.exists(t, t.name == "x" && R.attr.aBool)`)
are hoisted or case-split so each filter lands on its own model.

### What throws

Loud failures, never silently wrong filters:

- **LIKE metacharacters.** Prisma emits `LIKE` without `ESCAPE`, so `contains`/`startsWith`/
  `endsWith` with a needle containing `%`, `_` or `\`, or a column-valued needle, throws. Hierarchy
  prefixes (`ancestorOf`, `descendentOf`, `overlaps`) throw on `%`, `_`, `\` or `[` (SQL Server
  opens a character class on `[` even with `ESCAPE`). If you match on backslashes, compare the
  whole value with `==`.
- **Counting.** `exists_one`, `size()` other than empty/non-empty on a mapped relation, and string
  length.
- **Cross-model column comparisons**, including membership between an outer scalar column and a
  related collection column.
- **Unsolvable arithmetic.** Arithmetic on both sides, division *by* a column, and `==`/`!=` over
  fractional addition (not reversible in IEEE-754).
- **Other malformed or non-boolean shapes:** the two-list `except` function, `filter()` or `map()`
  as a standalone condition, `all()` over a multi-hop chain, an empty hierarchy delimiter,
  sub-millisecond `now()` thresholds, non-scalar comparison literals, empty `and`/`or`, and
  negating a sub-condition that translates to `{}` (Prisma reads `{ NOT: {} }` as true).

#### Operators Prisma `where` cannot express

Prisma 6.19 and 7.9 expose the same filter surface on SQLite, PostgreSQL and MySQL
([#224](https://github.com/cerbos/query-plan-adapters/issues/224)): scalar filters take a constant
or a same-model field reference, never an expression. These shapes throw:

| CEL shape | Corpus actions | Why Prisma cannot express it |
| --- | --- | --- |
| `a % b` | `arith-mod` | No modulo operator, and `%` is not invertible, so it cannot be solved into a plain comparison. |
| Arithmetic on both sides | `arith-both` | Field references compare two columns as they are; no operand is an expression. |
| `matches` | `regex-*`, `p-matches`, `matches-alt` | No regex filter ([prisma/prisma#18481](https://github.com/prisma/prisma/issues/18481)). Full-text `search` matches lexemes, not patterns, and no provider here has RE2. |
| List index `l[i]` | `index-*` | List filters test membership, emptiness or equality, never a position. |
| `int()`, `double()`, `string()` | `cast-*` | No cast operator, and SQL `CAST` would not reproduce CEL's conversion errors (SQLite reads `CAST('abc' AS INTEGER)` as `0`). |
| `size()` of a string, or of a list that isn't a mapped relation | `string-size-gt0`, `type-size-*`, `pv-filter`, `size-filter-count`, `pv-except`, `except-size` | No length filter; `_count` exists only in `orderBy`, `select` and aggregates ([prisma/prisma#8935](https://github.com/prisma/prisma/issues/8935)). |

Raw SQL fragments, an id subquery and an in-memory post-filter were considered and rejected: Prisma
5–7 has no raw predicate inside `where` ([prisma/prisma#5560](https://github.com/prisma/prisma/issues/5560),
[prisma/prisma#11568](https://github.com/prisma/prisma/issues/11568)), and the other two would stop
the result being a composable where-input.

**Workaround: a derived column.** Compute the value when you write the row and have the policy read
it: `isEven` instead of `R.attr.n % 2 == 0`, `nameLength` instead of `size(R.attr.name) > 0`,
`primaryTag` instead of `R.attr.tags[0]`.

Prisma 8 (a release candidate as of September 2026) replaces `findMany({ where })` with a new client
API and is out of scope.

## Conformance contract

The adapter is differentially tested against Cerbos PDP 0.55.0 `checkResource` decisions in both evaluation modes using 29 hostile seed rows, both Prisma 6 and 7, and each of SQLite, PostgreSQL and MySQL. The Spring Data adapter defines the reference semantics for this compatibility snapshot.

| Classification | Coverage |
| --- | --- |
| Oracle-tested | 172 reference actions |
| Fail-closed | 116 reference actions plus the 11 reference-unsupported shapes (127 actions total) |
| Representation-dependent | `null-eq-missing` — rejected under `nullAttributeRepresentation: "omitted"`; translated as `IS NULL` under the default, which over-grants if the caller omits attributes for NULL columns |
| Attribute NULL convention | The equality family (`eq`, `ne`, `in`) over an attribute sent as an explicit null renders definitely, so a NULL row is included where CEL's null *value* says it should be. Declare `nullAttributeRepresentation: "explicit"` on the mapper entry, or `!=` against a constant under-grants those rows (cerbos/query-plan-adapters#308) |
| Known planner divergence | `has()` on a missing attribute is folded by the Cerbos planner to `ALWAYS_ALLOWED`, while `checkResource` denies the missing-attribute rows. Until the planner is fixed, use `R.attr.x != null` for database-backed attributes instead of `has(R.attr.x)` |

The fail-closed set is: `LIKE` needles Prisma cannot escape, cross-model field references, relation
counts and string lengths, `exists_one`, unsolved column arithmetic, sub-millisecond `now()`
thresholds, and the reference probes for regex, ordered indexing, `timestamp()` over a string
field, `mod`, a positional read of a scalar list, and list equality over a `map()` projection. Each
one's error message is pinned in [`conformance/actions.json`](../conformance/actions.json) and
asserted by the conformance run.

**Providers.** The corpus is executed on SQLite, PostgreSQL and MySQL, each with Prisma 6 and 7, in
both evaluation modes (see [Development](#development)). The MySQL legs run under
`utf8mb4_0900_bin`; `ADAPTER_TEST_MYSQL_COLLATION` replays them under another collation, which is
how the figures in the collation section were measured. MySQL adds no fail-closed shape. SQL Server
and CockroachDB are **not** executed: fail-closed reasons naming them are reasoned from documented
`LIKE` behaviour.

## Mapping hazards

The contract above proves the *plan* side. The other half is the *mapping*: **the records the
nested filter reads must be the records your application put into the resource attributes.** The
shared corpus catalogues six ways that can break.

Prisma has no filtered relation and no `@Where` equivalent, and a `where` injected by a client
extension or middleware (`$extends`/`$use`) rewrites only the top-level query. So although the
mapping names a relation, this adapter is corpus class 1 — a **bare-table subquery**. Declare your
own narrowing as `subqueryFilter`; the adapter cannot detect an omission.

| Hazard | Position | Mechanism to check |
|---|---|---|
| Filtered association | **Caller-owned**, reproducible with `subqueryFilter` | A `$extends`/`$use` client extension or middleware that injects a `where` for the related model, or a repository helper that always appends one. None of them rewrite the nested filter this adapter returns |
| Default scope on the target model | **Caller-owned**, reproducible with `subqueryFilter` | A soft-delete column (`deletedAt: null`), a tenant column, a `published` flag — anything every application read of the related model filters on. Prisma has no default-scope construct |
| Subtype discrimination | **Caller-owned**, reproducible with `subqueryFilter` | A `type`/`kind` discriminator column where one model holds several row kinds. Declare `{ type: "…" }` |
| To-one relation used as a collection | **Rejected by Prisma** | `type: "one"` compiles to `is`, which Prisma accepts only on a relation its schema declares to-one. Mapping a to-many relation as `type: "one"` is a Prisma validation error, not a wider subquery |
| Composite association key | **Reproduced by Prisma** | Prisma resolves multi-column keys from `@relation(fields: […], references: […])`. The mapping names the relation, never its columns |
| Absent to-one parent | **Reproduced**, and proved by the corpus (`w1-all-chain`, `rel-not-bool-hop` and siblings) | None — every operator reached through a relation requires its to-one hops separately, so a missing parent stays denied under both polarities ([#309](https://github.com/cerbos/query-plan-adapters/issues/309), [#315](https://github.com/cerbos/query-plan-adapters/issues/315), [#375](https://github.com/cerbos/query-plan-adapters/issues/375)) |

### Declaring the application's own predicate

```ts
queryPlanToPrisma({
  queryPlan,
  mapper: {
    "request.resource.attr.tags": {
      relation: {
        name: "tags",
        type: "many",
        field: "name",
        // Exactly the predicate your own reads of `Tag` apply.
        subqueryFilter: { deletedAt: null, kind: "label" },
      },
    },
  },
});
```

`subqueryFilter` is a where-input over the related model, ANDed into the nested filter. It narrows
the records the subquery *examines*, for every operator reached through the relation (`exists`,
`all`, `except`, membership, `hasIntersection`, emptiness) and for the hop-existence guard.
`all()` is rewritten from `every: AND(declared, P)` to `none: AND(declared, NOT P)`, so hidden
records are ignored rather than required. If the declaration hides every record, `all()` is
vacuously true, matching the empty list your application would send to `check()`.

## Behaviour changes

- **Breaking ([#495](https://github.com/cerbos/query-plan-adapters/issues/495)):** relation element
  columns are now guarded as nullable unless declared `nullable: false` (previously opt-in with
  `nullable: true`, which over-granted on NULL elements). Nullable columns left undeclared now return
  fewer rows; required columns left undeclared need `nullable: false`. `nullable: true` mappings
  are unchanged. Declarations now reach chained collections via nested `relation.fields`.
- **Breaking ([#495](https://github.com/cerbos/query-plan-adapters/issues/495)):** an empty
  `and`/`or` (previously `{ AND: [] }` / `{ OR: [] }`, or folded to an unconditional filter) and a
  negated sub-condition that translates to `{}` now throw. Two constants related by a hierarchy
  operator, or an `overlaps` whose segments are all `nullable: false`, produce `{}`.
- **Breaking ([#414](https://github.com/cerbos/query-plan-adapters/issues/414)):** non-scalar
  comparison/membership literals and bare comparisons between mapped `DateTime` columns now throw
  (use `timestamp()` on both operands). Negated ternary comparisons and unsolvable string
  concatenation now keep CEL's error behaviour.
- **Breaking:** `contains`/`startsWith`/`endsWith` needles and hierarchy prefixes containing a
  backslash now throw. A backslash is a `LIKE` escape on PostgreSQL and MySQL and literal on SQLite,
  so `contains("a\\b")` matched `"ab"` on PostgreSQL and `endsWith("\\")` failed with
  `SQLSTATE 22025`.
- **Breaking:** a hierarchy with an empty delimiter (`hierarchy(R.attr.scope, "")`) now throws. It
  was lowered as a `startsWith` that also matched the path itself (corpus action
  `hier-empty-delim` over-granted).
- **Breaking (Cerbos 0.55 compatibility):** ordered comparisons involving NaN evaluate to false, so
  their negation can allow a row; Cerbos 0.54 denied it. Use Cerbos 0.55 when a policy can negate a
  NaN comparison. Missing attributes and nulls are unchanged.
- **[#375](https://github.com/cerbos/query-plan-adapters/issues/375):** a negation over a single
  to-one hop (`!R.attr.parent.aBool` with `type: "one"`) no longer returns rows whose relation is
  absent. It emits `AND: [{ parent: { is: {} } }, { NOT: … }]` and returns fewer rows (over-grant
  fix).
- Projection relations resolve the scalar lambda variable to the mapped column. Negated equality in
  `tagNames.exists(name, !(name == "public"))` now includes null list elements, as CEL does.
  Function mappers may call `queryPlanToPrisma` recursively without clobbering the outer call's
  model, null convention or collection scope.

## Example application

[`example/`](example/) installs the packed adapter and runs it against a live PDP over the shared
[demo domain](../demo/README.md), including pagination and composition with an application filter:

```bash
# from the repository root
demo/scripts/run-example.sh prisma
```

A fuller app: [cerbos/express-prisma-cerbos](https://github.com/cerbos/express-prisma-cerbos).

## Development

| Command | What it runs | Needs |
| --- | --- | --- |
| `npm test` | Translator unit test | Nothing |
| `npm run typecheck` | `tsc` against Prisma 7 and 6 | Nothing |
| `npm run test:adversarial:v7` / `:v6` | Corpus on SQLite, Prisma 7 / 6 | Cerbos PDP (started by the script) |
| `npm run test:adversarial:postgres:v7` / `:v6` | Corpus on PostgreSQL | Docker |
| `npm run test:adversarial:mysql:v7` / `:v6` | Corpus on MySQL | Docker |

`npm run test:adversarial`, `…:postgres` and `…:mysql` alias the v7 leg. Set
`ADAPTER_TEST_STRICT_EVALUATION=true` to run the corpus with strict evaluation for both planning and
the `check()` oracle (`false` is the default; other values are rejected). CI runs all six
store/Prisma combinations in both modes. The SQLite legs reset `prisma/dev-adversarial.db` with
`prisma db push --force-reset`, so point them only at disposable databases.

`npm test` reads every plan from `conformance/wire-fixtures/`, so it needs no PDP, database or
generated client, and has no v6/v7 split. It pins the `where` input for every corpus action
(classified exactly once as a filter, a plan kind or a throw, with the message
`conformance/actions.json` records), plus mapper contracts no policy can reach:
`nullAttributeRepresentation`, `subqueryFilter` and malformed input. Its expectations are still
inline rather than in a `golden/expectations.json`; see
[ADR 0006](../docs/adr/0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md) and
"Golden expectations" in [conformance/README.md](../conformance/README.md).

## Resources

- [Cerbos documentation](https://docs.cerbos.dev) and the [query plan API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)
- [Prisma filtering guide](https://www.prisma.io/docs/concepts/components/prisma-client/filtering-and-sorting)
- [Cerbos Slack](https://community.cerbos.dev) · [GitHub Discussions](https://github.com/cerbos/cerbos/discussions)

## License

Apache 2.0.
