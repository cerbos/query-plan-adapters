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
| `valueType` | `"string"`, `"number"`, `"boolean"` or `"dateTime"`. Lets the adapter settle comparisons the type already decides before Prisma sees them: CEL answers `R.attr.aNumber == "5"` false for every row and `R.attr.aNumber.contains("2")` with an error, so neither literal is bound for the store to coerce. Required (`"dateTime"`) for `timestamp()` comparisons. Undeclared keeps the untyped behaviour; the adapter cannot read your schema. |
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
instants in CEL's year 0001–9999 range. Your column must preserve millisecond precision, and the
attribute you send must be the millisecond Prisma returns for it. An instant between two
milliseconds — what the planner folds `now()` into — is compared against the next millisecond,
which is exact for a whole-millisecond attribute: `a < T` and `a <= T` become `< ceil(T)`, `a > T`
and `a >= T` become `>= ceil(T)`, and `a == T` never holds. Anywhere else (a list, arithmetic, two
literals) such an instant still throws.

## NULL attribute representation

`R.attr.x == null` produces the same plan however your application represents a NULL column in the
attributes it sends to `check()`, so tell the adapter which convention you use:

| Attributes you send for a NULL column | `check()` on that row | `IS NULL` filter |
| --- | --- | --- |
| `{"x": null}` — explicit null | allow | selects it — aligned |
| `{}` — attribute omitted | **deny** (CEL missing-attribute error) | selects it — **over-grants** |

The default is `"explicit"` (`IS NULL`). If you omit attributes for NULL columns, set `"omitted"`:
the adapter then never emits a filter that selects the NULL rows. `x == null` is FALSE for a
present value and an error for an absent one, so it becomes `NOT startsWith(x, "")` (for a string
column): FALSE when present, UNKNOWN when NULL, under either polarity. `x != null` is the
presence test itself. Every other null comparison operand is rejected.

A typed column read through to-one relations (`parent.x`) is also missing when a relation on the way is
absent, and a relation filter cannot promise UNKNOWN on the rows it fails. So the rendering follows
the comparison's polarity, which the enclosing `!`, `&&`, `||`, `exists` and `all` fix: where the
comparison needs to be TRUE, `parent.x != null` is the relation-reached presence test (the parent
exists and `x` is not NULL) and `parent.x == null` is never true; where it needs to be FALSE,
`parent.x == null` is the negated presence test and `parent.x != null` is never false. A missing
parent or a NULL `x` is then denied under any nesting of `!`. A null comparison whose polarity is
not fixed (a ternary condition, an operand of `==`, the body of `exists_one`) is still rejected.

```ts
queryPlanToPrisma({ queryPlan, mapper, nullAttributeRepresentation: "omitted" });
```

The rejection covers every other null operand, including `x != null`, which is aligned under both
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

A field-to-field `==`/`!=` between an explicit-null attribute and one that is not keeps both: the
explicit side's NULL compares as a value, the other side's NULL stays an error. See
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

  Measured on `mysql:8.4` against PDP 0.55.0's goldens: under `utf8mb4_unicode_ci`, **58 of the
  184 compared cases disagree with the PDP** (`string/equals/case-sensitive` returns `"One"` for a
  policy that allowed `"one"`); under `utf8mb4_0900_as_cs`, **17** disagree, all on the
  soft-hyphen seed `h6`.
- **SQL Server:** a case-sensitive (`_CS_`) collation, not `_CI_`.
- **SQLite:** no `COLLATE NOCASE` on mapped fields, **and** `PRAGMA case_sensitive_like = ON` on
  every connection. `contains`/`startsWith`/`endsWith` lower to `LIKE`, which is ASCII
  case-insensitive on SQLite regardless of column collation. The pragma is per connection, not per
  schema, and Prisma 6's query engine pools SQLite connections, so one `$executeRawUnsafe` reaches
  only one of them: pin `connection_limit=1` in the datasource url, as this repository's Prisma 6
  SQLite leg does. Without it the adapter over-grants the corpus's
  `string/contains/case-sensitive`, `string/starts-with/case-sensitive` and
  `string/ends-with/case-sensitive` cases.

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
| Arithmetic | `add`, `sub`, `mult`, `div` against a constant, solved exactly over IEEE-754 doubles to a range of the column (`R.attr.n + 0.5 == 0.75` → `0.24999999999999994 <= n <= 0.25000000000000006`, every double whose rounded sum is 0.75; negative multipliers flip the direction); `x / x` and `x / ±0` split on the sign of `x`; string concatenation solving (`P.attr.ctx == "projects:" + R.attr.id` → `{ id: { equals: "…" } }`), and a concatenation of two string columns against a literal as one arm per split of it |
| Regex | `matches` against a literal RE2 pattern whose language LIKE decides exactly: literals, `^`/`$`, alternation, groups, `?` and bounded `{n,m}`, finite classes, `\d`, `[[:digit:]]`, leading `(?i)` over ASCII, and `.`/one `.*` in a pattern anchored at both ends (the value then holds no newline, as RE2's `.` requires). A pattern RE2 rejects (lookaround, a backreference) is an error on every row |
| Hierarchy | `hierarchy(string)`, `hierarchy(string, delimiter)` (an empty delimiter splits per code point, so a descendant is `startsWith(path + "_")`), `hierarchy([segments])`, `overlaps`, `ancestorOf`, `descendentOf` |
| Timestamps | `timestamp()` over `valueType: "dateTime"` columns |

Outer-column references inside a macro (`R.attr.tags.exists(t, t.name == "x" && R.attr.aBool)`)
are hoisted or case-split so each filter lands on its own model.

### What throws

Loud failures, never silently wrong filters. A shape the adapter cannot express throws
`UnsupportedQueryPlanError`. It is exported and extends `Error`, so existing `catch` blocks keep
working:

```ts
import { queryPlanToPrisma, UnsupportedQueryPlanError } from "@cerbos/orm-prisma";

try {
  const result = queryPlanToPrisma({ queryPlan, mapper, model: "Resource" });
} catch (error) {
  if (error instanceof UnsupportedQueryPlanError) {
    // The policy uses a shape this adapter cannot translate faithfully: deny, or fall back to
    // per-row check() calls.
  }
  throw error;
}
```

A mapper misconfiguration, such as a field-to-field comparison without the `model` option or
`relation.model`, stays a plain `Error`. The refused shapes include:

- **LIKE metacharacters.** Prisma emits `LIKE` without `ESCAPE`, so `contains`/`startsWith`/
  `endsWith` with a needle containing `%`, `_` or `\`, or a column-valued needle, throws. Hierarchy
  prefixes (`ancestorOf`, `descendentOf`, `overlaps`) throw on `%`, `_`, `\` or `[` (SQL Server
  opens a character class on `[` even with `ESCAPE`). If you match on backslashes, compare the
  whole value with `==`.
- **Counting.** `exists_one` over a relation, and `size()` of a mapped relation other than empty, non-empty or (on
  a chain) reachable.
- **Cross-model column comparisons**, including membership between an outer scalar column and a
  related collection column.
- **Unsolvable arithmetic.** Arithmetic on both sides, and division *by* a column other than
  `x / x`.
- **Other malformed shapes:** the two-list `except` function compared to a list or counted past
  emptiness, non-scalar comparison literals, empty `and`/`or`, and
  negating a sub-condition that translates to `{}` (Prisma reads `{ NOT: {} }` as true).

#### Operators Prisma `where` cannot express

Prisma 6.19 and 7.9 expose the same filter surface on SQLite, PostgreSQL and MySQL
([#224](https://github.com/cerbos/query-plan-adapters/issues/224)): scalar filters take a constant
or a same-model field reference, never an expression. These shapes throw:

| CEL shape | Corpus cases | Why Prisma cannot express it |
| --- | --- | --- |
| `a % b` | `arithmetic/modulo/*` | No modulo operator, and `%` is not invertible, so it cannot be solved into a plain comparison. |
| Arithmetic on both sides | `arithmetic/add/on-both-sides` | Field references compare two columns as they are; no operand is an expression. |
| `matches` beyond the LIKE-decidable subset | `regex/matches/lucene-reserved-characters-in-class` | No regex filter ([prisma/prisma#18481](https://github.com/prisma/prisma/issues/18481)), and no provider here has RE2. A repeated class (`[ab]+`), a negated class, a wildcard in a pattern not anchored at both ends, or a literal `%`/`_`/`\` inside a LIKE has no exact LIKE spelling. |
| List index `l[i]` | `collection/index/*` | List filters test membership, emptiness or equality, never a position. |
| `int()`, `double()` or `timestamp()` parsing a string; an `int()` threshold | `cast/int/malformed-string`, `cast/double/malformed-string`, `cast/timestamp/malformed-string` and their negations | No cast operator, and SQL `CAST` would not reproduce CEL's conversion errors (SQLite reads `CAST('abc' AS INTEGER)` as `0`). `int()` of a number is translated only as `==` (or `!=` under a `!`): a threshold would need CEL's ±2^63 overflow bound spelled out, which an `Int` column rejects. The invertible casts are solved for the column instead — `string()` of a boolean or number, `int(d) == k` as the interval truncation maps to `k`, and `string()`/`double()` of a column already of that type. |
| `size()` of `filter()` over a relation | `size/equals/filtered-collection` | No count filter; `_count` exists only in `orderBy`, `select` and aggregates ([prisma/prisma#8935](https://github.com/prisma/prisma/issues/8935)). |

Raw SQL fragments, an id subquery and an in-memory post-filter were considered and rejected: Prisma
5–7 has no raw predicate inside `where` ([prisma/prisma#5560](https://github.com/prisma/prisma/issues/5560),
[prisma/prisma#11568](https://github.com/prisma/prisma/issues/11568)), and the other two would stop
the result being a composable where-input.

**Workaround: a derived column.** Compute the value when you write the row and have the policy read
it: `isEven` instead of `R.attr.n % 2 == 0`, `tagCount` instead of `size(R.attr.tags) > 1`,
`primaryTag` instead of `R.attr.tags[0]`.

Prisma 8 (a release candidate as of September 2026) replaces `findMany({ where })` with a new client
API and is out of scope.

## Conformance contract

The adapter is replayed against the shared [conformance corpus](../conformance/README.md): the plans
and `check()` decisions recorded from Cerbos PDP 0.55.0 (and 0.54.0), executed as real Prisma
queries over the corpus's 41 seed rows with Prisma 6 and 7 on SQLite, PostgreSQL and MySQL (under
`utf8mb4_0900_bin`). Passed cases on the current PDP, 0.55.0, identical on all six combinations,
out of every golden case in the tier:

| Tier | Passed / total |
| --- | --- |
| core | 26 / 26 |
| extended | 60 / 80 |
| adversarial | 213 / 270 |

Every case that does not pass is refused with `UnsupportedQueryPlanError`; none returns wrong rows
on 0.55.0. [`conformance-ledger.json`](conformance-ledger.json) lists each one with its reason.
Cases the corpus marks as a planner divergence are skipped, not failed, and count in the total but
never as passed. On 0.55.0 that is four extended cases. `null/has/missing-attribute`: the planner
folds `has()` on a missing attribute to `ALWAYS_ALLOWED` while `checkResource` denies the
missing-attribute rows, so use `R.attr.x != null` for database-backed attributes instead of
`has(R.attr.x)`. And the three `composition/*` cases with a conditional `DENY` rule: a deny
condition that errors on a missing attribute does not deny in `checkResource`, while the plan
negates it as an ordinary condition, which a missing attribute leaves unsatisfiable
([#530](https://github.com/cerbos/query-plan-adapters/issues/530)).

**Providers.** `ADAPTER_TEST_MYSQL_COLLATION` replays the MySQL legs under another collation, which
is how the figures in the collation section were measured. MySQL adds no refused shape. SQL Server
and CockroachDB are **not** executed: refusal reasons naming them are reasoned from documented
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
| Absent to-one parent | **Reproduced**, and proved by the corpus (`relation/all/to-one-chain`, `relation/bare-attribute/negated-one-hop-boolean` and siblings) | None — every operator reached through a relation requires its to-one hops separately, so a missing parent stays denied under both polarities ([#309](https://github.com/cerbos/query-plan-adapters/issues/309), [#315](https://github.com/cerbos/query-plan-adapters/issues/315), [#375](https://github.com/cerbos/query-plan-adapters/issues/375)) |

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

- A comparison against NaN settles (`==` and orderings false, `!=` true, as the current PDP
  evaluates them even against a string), and a comparison over `c ? a : b` with `c` a boolean
  column that is never missing distributes over the branches when one of them then settles.
- `hierarchy(x, "")` translates: Cerbos splits on an empty delimiter per code point, so a
  descendant of `C` is `startsWith(C + "_")`, and the empty path is an ancestor of every other.
- `matches()` translates for the RE2 patterns a combination of LIKE filters decides exactly (see
  [Supported operators](#supported-operators)); `b == true` over a boolean expression is `b`.
- Arithmetic against a constant is solved exactly over IEEE-754 doubles: `x + c CMP v` (and `-`,
  `*`, `/` by a constant) becomes the range of doubles `x` for which the rounded result compares,
  found by bisecting the doubles in order, where it used to compute `v - c` — inexact for a
  fraction, and refused for `==`/`!=`. `x + 1 == 3` now also admits `1.9999999999999998`, which CEL
  rounds to 3. A comparison whose one column appears only as `x / x` or `x / ±0` splits on the sign
  of `x` (those divisions are 1, ±Inf or NaN, never an error, on doubles).
- `all()` over a multi-hop chain translates (the hops must exist, and the predicate holds at the
  end of every path). **Fix:** a negated `all()` over a chain put its negation outside the inner
  hops (`some { NOT { some P } }`), which admitted a hop with no elements — where `all()` is
  vacuously true — and missed a hop whose elements only partly matched; the false witness is now
  an element at the end of the chain.
- A field-to-field `==`/`!=` across mixed null conventions translates instead of throwing, and
  `a + b == "lit"` over two string columns is one arm per split of the literal.
- Macros over a literal list (a folded principal attribute) are expanded before translation:
  `exists`/`all` over a `list(...)` of structs as well as of values, and `exists_one`,
  `size(filter(...))` and `size(except(list, [x]))` whose body is `t == x` or `t != x`, which count
  the multiplicity of `x` and become membership. `size(except(R.attr.list, literals))` against an
  emptiness threshold becomes `exists`/`all` over the relation.
- Casts that can be inverted are solved for the column rather than refused: `string(aBool) ==
  "true"` is `aBool == true`, `string(aDouble) == "-0.6"` is `aDouble == -0.6` (and false when no
  double prints as the literal, as cel-go's `%g` decides), `int(aDouble) == 0` is
  `-1 < aDouble < 1`, and `string()`/`double()` of a column of that type is the column, with a NULL
  kept an error.
- A timestamp literal with digits past the millisecond — what the planner folds `now()` into — is
  no longer refused when compared against a column: it is compared against the next millisecond
  (see [Timestamps](#timestamps)).
- A list-valued `filter()`, `map()` or `except()` where a boolean is required is a CEL error, and
  settles as one: a whole condition that is one returns `ALWAYS_DENIED` rather than throwing. Under
  `nullAttributeRepresentation: "omitted"`, `x == null` and `x != null` over a typed column
  translate to presence tests instead of being refused. A column reached through to-one relations
  (`parent.x != null`) now translates too, by the comparison's polarity (see
  [NULL attribute representation](#null-attribute-representation)), where it used to be refused
  ([#551](https://github.com/cerbos/query-plan-adapters/issues/551)).
- `size()` of a `valueType: "string"` column translates for any threshold, as `LIKE` patterns of
  `_` (`size(x) > 4` is `startsWith: "_____"`). A threshold of 2^32 or more, which no store can
  hold, needs no pattern; one past 1024 characters short of that is refused. Fractional and
  negative thresholds are normalised to the integer count they mean for relations too, and
  `size(chain) >= 0` on a multi-hop chain is "the chain exists".
- A comparison whose outcome the declared `valueType` settles is no longer refused. CEL's
  heterogeneous equality answers `==` false and `!=` true across types, so `aNumber == "5"`,
  `aString == {"a": 1}` and `"2" in aNumberList` settle, and an `in`/`hasIntersection`
  list drops the literals that can never match (`aNumber in ["5", 2]` is `aNumber in [2]`). A
  no-overload call (`aNumber.contains("2")`, `size(aBool)`, `hierarchy(aNumber)`) is a CEL error,
  which is settled only where the enclosing `!`, `&&`, `||`, `exists` and `all` fix how an error
  decides the row. A column is trusted never to be missing only when its mapping says
  `nullable: false` (or it is an explicit-null attribute); otherwise the settled comparison is a
  presence test on it (`startsWith(x, "")`, `x > 0 || x <= 0`, `x || !x`), TRUE when the value is
  there and UNKNOWN on NULL, so a missing attribute stays denied under a negation. A column reached
  through `relation.fields` and a projected list element now carry the `valueType` declared for
  them. A conditional plan that folds to `false` returns `ALWAYS_DENIED` instead of
  throwing. This supersedes the `in value type does not match mapped <type> field` refusal below
  for scalar literals.
- A shape the adapter refuses now throws `UnsupportedQueryPlanError`, an exported subclass of
  `Error`. What it translates is unchanged, and existing `catch` blocks keep working; mapper
  misconfiguration stays a plain `Error`.
- **Breaking:** `hasIntersection` over a relation-backed list checks its literals against the
  `valueType` declared on the list's mapper entry, as `in` already did, and throws `in value type
  does not match mapped <type> field` on a mismatch. It used to drop the declared type and hand the
  literal to Prisma.
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
  was lowered as a `startsWith` that also matched the path itself (corpus case
  `hierarchy/descendent-of/empty-delimiter` over-granted).
- **Breaking (Cerbos 0.55 compatibility):** ordered comparisons involving NaN evaluate to false, so
  their negation can allow a row; Cerbos 0.54 denied it. Use Cerbos 0.55 when a policy can negate a
  NaN comparison. Missing attributes and nulls are unchanged.
- **[#375](https://github.com/cerbos/query-plan-adapters/issues/375):** a negation over a single
  to-one hop (`!R.attr.parent.aBool` with `type: "one"`) no longer returns rows whose relation is
  absent. It emits `AND: [{ parent: { is: {} } }, { NOT: … }]` and returns fewer rows (over-grant
  fix).
- **Fix:** a negated `&&`/`||` that reads a single to-one hop is pushed down with De Morgan, so each
  operand requires only the hops it reads. `!(!aBool && parent.aString == "one")` used to require
  the parent around the whole negation and denied a parentless row with `aBool` true, which CEL
  allows because `!aBool` is false and decides the `&&` (under-grant fix).
- **Breaking:** a `matches()` pattern opening with a repetition count (`{2}a`, or one right after
  `(` or `|`) is a pattern RE2 rejects, and settles as a CEL error on every row. It used to be read
  as a literal brace, so its negation matched every row (over-grant fix).
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
| `npm test` | Offline unit tests: caller-supplied options the corpus cannot vary (mapper forms, `subqueryFilter`, element nullability, `nullAttributeRepresentation`), the refusal type, the timestamp literal contract and malformed input | Nothing |
| `npm run typecheck` | `tsc` against Prisma 7 and 6 | Nothing |
| `npm run test:adversarial:v7` / `:v6` | Conformance harness on SQLite, Prisma 7 / 6 | Nothing |
| `npm run test:adversarial:postgres:v7` / `:v6` | Conformance harness on PostgreSQL | Docker |
| `npm run test:adversarial:mysql:v7` / `:v6` | Conformance harness on MySQL | Docker |

`npm run test:adversarial`, `…:postgres` and `…:mysql` alias the v7 leg. CI runs all six
store/Prisma combinations. The SQLite legs reset `prisma/dev-adversarial.db` with
`prisma db push --force-reset`, so point them only at disposable databases.

No suite starts a PDP. The harness reads the golden files under `../conformance/golden/` for both
pinned PDPs and applies [`conformance-ledger.json`](conformance-ledger.json): a case with no entry
must return exactly the recorded allowed ids, and an `unsupported` case must throw
`UnsupportedQueryPlanError`. See "The harness contract" in
[conformance/README.md](../conformance/README.md). `npm test` reads its plans from the same golden
files but pins no corpus case's filter; it needs no database or generated client, and has no v6/v7
split.

## Resources

- [Cerbos documentation](https://docs.cerbos.dev) and the [query plan API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)
- [Prisma filtering guide](https://www.prisma.io/docs/concepts/components/prisma-client/filtering-and-sorting)
- [Cerbos Slack](https://community.cerbos.dev) · [GitHub Discussions](https://github.com/cerbos/cerbos/discussions)

## License

Apache 2.0.
