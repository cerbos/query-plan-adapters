# Cerbos + Convex Adapter

An adapter library that takes a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into a [Convex](https://convex.dev/) filter function. It is designed to run alongside a project that is already using the [Cerbos JavaScript SDK](https://github.com/cerbos/cerbos-sdk-javascript) to fetch query plans so that authorization logic can be pushed down to Convex queries.

## How it works

1. Use a Cerbos client (`@cerbos/http` or `@cerbos/grpc`) to call `planResources` and obtain a `PlanResourcesResponse`.
2. Provide `queryPlanToConvex` with that plan and an optional mapper that describes how Cerbos attribute paths relate to your Convex document fields.
3. The adapter walks the Cerbos expression tree and returns `{ kind, filter?, postFilter? }`:
   - `filter` is a Convex-native filter function `(q) => Expression<boolean>` pushed to the DB.
   - `postFilter` is a JS predicate `(doc) => boolean` for operators Convex can't express natively (string ops, collection ops). **Requires `allowPostFilter: true`** and must run in trusted backend code before any document is returned — see below.
4. Inspect `result.kind`:
   - `ALWAYS_ALLOWED`: the caller can query without any additional filters.
   - `ALWAYS_DENIED`: short-circuit and return an empty result set.
   - `CONDITIONAL`: apply `result.filter` in the Convex query, then apply `result.postFilter` to every candidate inside the trusted backend before returning results (see usage example below).

## Supported operators

| Category | Operators | Behavior |
| --- | --- | --- |
| Logical | `and`, `or`, `not` | Builds `q.and(...)`, `q.or(...)`, `q.not(...)` groups. |
| Comparisons | `eq`, `ne`, `lt`, `le`, `gt`, `ge` | Emits `q.eq`, `q.neq`, `q.lt`, `q.lte`, `q.gt`, `q.gte` against the mapped field. |
| Membership | `in` | Composed as `q.or(q.eq(field, v1), q.eq(field, v2), ...)`. |
| Existence | `eq`/`ne` against `null` | Null checks arrive as `eq`/`ne` against a null value — the planner emits no existence operator. |

### Post-filter operators

The following operators cannot be expressed as Convex DB filters. When the adapter encounters them, it returns a `postFilter` function that evaluates them in JavaScript against each document:

| Category | Operators | JS Behavior |
| --- | --- | --- |
| String | `contains`, `startsWith`, `endsWith` | `String.prototype.includes` / `startsWith` / `endsWith` |
| Collection | `hasIntersection`, `index`, `get-field`, `size` | CEL-compatible collection and nested-field evaluation |
| Quantifiers | `exists`, `exists_one`, `all` | CEL-compatible lambda evaluation, including empty collections, missing members, and literal value-list collections |
| Higher-order | `filter`, `map`, `lambda` | Collection filtering and projection inside larger expressions |
| Arithmetic | `add`, `sub`, `mult`, `div`, `mod` | Numeric evaluation with CEL error propagation |
| Conversion | `string`, `double`, `int`, `timestamp` | Scalar conversion and RFC 3339 timestamp comparison |
| Conditional | `if` | Evaluates only the selected branch |
| Pattern | `matches` | Constant patterns in the safe common RE2/JavaScript subset: literals, `^`/`$` anchors, and a trailing `.*` when no `$` end anchor is present |
| Hierarchy | `hierarchy`, `ancestorOf`, `descendentOf`, `overlaps` | Delimiter-aware hierarchy comparison |

For mixed expressions (e.g. `and(eq(...), contains(...))`), the adapter splits the tree: DB-pushable children go to `filter`, the rest go to `postFilter`. For `or(...)` with any unsupported child, the entire expression goes to `postFilter` (partial OR push-down would miss results).

#### Quantifiers over known collections

A quantifier whose collection the PDP resolves at plan time — typically a
principal attribute, as in `P.attr.teams.exists(t, R.attr.team == t)` — reaches
the adapter in one of two wire shapes. The Cerbos planner unrolls it into a
plain `or`/`and` chain at 10 elements or fewer, which pushes into the DB filter,
and ships the lambda with a literal value-list collection above that
(`maxItems = 10` in the planner's struct matcher; cerbos/cerbos#2570,
cerbos/cerbos#2817), which evaluates as a `postFilter`. Because the adapter is
an interpreter rather than a compiler, the literal collection is bound
per-element like any other, so both shapes yield the same decisions — including
`exists_one` cardinality, which compiling adapters cannot express. Only the
push-down changes: above the threshold the predicate no longer reduces the
documents read, so `allowPostFilter: true` is required.

### `allowPostFilter` opt-in

By default, `queryPlanToConvex` throws an error when the query plan requires a `postFilter`. This is because post-filter operators cause documents to be read before the complete authorization predicate is applied — the DB-level filter alone may not fully enforce the authorization policy.

To enable post-filtering, pass `allowPostFilter: true`:

```ts
const { kind, filter, postFilter } = queryPlanToConvex({
  queryPlan,
  mapper,
  allowPostFilter: true,
});
```

> **Security requirement:** `postFilter` is part of the authorization predicate. Run it in the same trusted Convex/backend function and apply it to every candidate before serialization or return. Never send unfiltered candidates to a browser or other untrusted client for filtering.

If your Cerbos policies only use operators that Convex supports natively (comparisons, `in`, null checks, logical combinators), you don't need this flag — `filter` alone will enforce the full policy at the DB level.

## NULL attribute representation

`R.attr.x == null` compiles to the same `eq(x, null)` plan node however your application represents
a NULL field in the attributes it sends to `check()`, so the adapter cannot infer the convention
and has to be told which one you use.

| attributes you send for a NULL field | `check()` on that document | `q.eq(field, null)` |
| --- | --- | --- |
| `{"x": null}` — explicit null | allow | selects it — aligned |
| `{}` — attribute omitted | **deny** (CEL missing-attribute error) | selects it if the field is stored as null — **over-grants** |

`nullAttributeRepresentation` defaults to `"explicit"`, preserving the historical translation. If
your application omits attributes for NULL fields, set it to `"omitted"`: the adapter then rejects
every null comparison operand — in the pushed-down filter *and* in the in-memory `postFilter` —
instead of returning documents the PDP denies.

```ts
queryPlanToConvex({ queryPlan, mapper, nullAttributeRepresentation: "omitted" });
```

Storing the field as absent rather than as an explicit `null` also aligns the two, but not through
the filter: a field that can be absent must be `nullable: true` in the mapper, and that makes the
adapter refuse the push-down and evaluate the predicate in the `postFilter`, where an absent path
is a CEL missing-attribute error and denies — the same three-valued logic `check()` applied. That
is a property of your document shape, not of the plan, so the option remains the reliable guard.

The rejection is deliberately wider than the shapes that actually over-grant — `x != null` and
`!(x == null)` are aligned under both conventions — because negation is applied around the built
predicate rather than pushed into the leaf, so a leaf cannot tell whether an enclosing `not` will
flip a not-null predicate back into a null-selecting one. Rejecting every null operand is correct
under any nesting. See [#302](https://github.com/cerbos/query-plan-adapters/issues/302).

## Conformance contract

The adapter is differentially tested with the canonical check resources against Cerbos PDP 0.54.0 `checkResource` decisions: each query plan is translated by the adapter and executed inside a Convex query function, and the returned document IDs must equal the PDP's per-document decisions. How much of that execution is Convex's filter engine and how much is the adapter's `postFilter` is set out below.

| Classification | Coverage |
| --- | --- |
| Oracle-tested | Every catalog action with a `matched` direct outcome in `adapterctl.json`; catalog cardinality expectations guard empty, total, and proper-subset oracles |
| Fail-closed | Every catalog action with a `rejected` direct outcome in `adapterctl.json`; its pinned message substring is asserted. These include non-boolean `filter()`/`map()` conditions, signed-zero divisors whose sign the JSON hop discards, and hierarchy paths constructed by an unknown `list()` operator |
| Explicit opt-in | Any plan that cannot be represented entirely as a Convex database filter requires `allowPostFilter: true` |
| Representation-dependent | `null-eq-missing` — rejected under `nullAttributeRepresentation: "omitted"`. Under the default it already returns the empty set the PDP demands *when the document omits the field for a NULL value*, which is what the conformance harness seeds. The alignment is the `postFilter`'s doing, not a Convex filter's: the field is `nullable: true`, so the predicate is evaluated in JavaScript and the absent path raises the same CEL missing-attribute error that made `check()` deny. A deployment that stores explicit nulls while omitting the attribute would over-grant |
| Attribute NULL convention | Needs no declaration: Convex stores the value the caller sent, so a stored null already compares as a null *value* exactly as CEL does, and a stored null stays distinguishable from an absent field. Every `null-value-*` corpus probe for the explicit convention (cerbos/query-plan-adapters#308) was aligned before that option existed — including `null-value-f2f-mixed`, which Convex and Mongoose are the only two adapters to translate rather than refuse |
| Known planner divergence | `has()` on a missing attribute is currently folded by the Cerbos planner to `ALWAYS_ALLOWED`; `checkResource` still denies documents where the attribute is missing. Until the planner is fixed, use `R.attr.x != null` for database-backed attributes instead of `has(R.attr.x)` |

This support statement includes value-first comparisons, field-to-field expressions, null and missing-attribute behavior, nested lambdas, collection macros, string and arithmetic expressions, timestamps, hierarchy operations, and chained nested fields. Fields that may be absent must be marked `nullable: true` in the mapper so the adapter evaluates their predicates with CEL-compatible missing-value semantics instead of pushing them to a Convex filter.

Every fail-closed shape's error message is pinned in this adapter's direct-outcome manifest (`adapterctl.json`) and asserted by its conformance run, so an outcome proves the throw names its declared mechanism rather than merely that something threw.

The filter each of these actions produces is pinned separately, in the translator unit test (`npm test`) — see [Testing](#testing). That is what makes a change to the emitted filter show up as a diff even when it selects the same documents from the corpus seeds, and it is the only place the parts of the contract no policy can reach are asserted at all: the `allowPostFilter` gate, function mappers, the identity fallback for an unmapped reference, the `nullAttributeRepresentation` boundary, and malformed input.

### What the differential proves, and what it does not

Convex's filter API has no string, collection, arithmetic or cast operators, so most of the corpus
is decided by the adapter's in-memory `postFilter` after an unfiltered `.collect()`. That is the
adapter's design rather than a gap in the harness, but it changes what the numbers above mean, so
the split is pinned by the conformance run instead of being left to inference:

| Decided by | Default mapper | Pushdown mapper |
| --- | --- | --- |
| Convex's filter engine, alone | 22 | 33 |
| the engine narrowing and the `postFilter` deciding (`rel-hop-and-root`) | 1 | 1 |
| the adapter's `postFilter`, alone | 167 | 156 |
| folded to `ALWAYS_DENIED` before any filter exists (`in-empty`) | 1 | 1 |

For the 167 post-filtered actions the differential compares the adapter's CEL evaluator against the
PDP's CEL evaluator; Convex's own comparison and ordering semantics only get a say on the 22.
The **pushdown mapper** is a second leg that clears `nullable` on `owner` — the one nullable field
the seeded documents always carry, since the table declares it `v.union(v.string(), v.null())`
rather than `v.optional(...)`. That moves the null-comparison family (`null-eq`, `null-ne`,
`null-not-eq`, `vf-null-ne`, the four `in-null-elem-*` shapes and the three `null-value-*-const`
probes) into the engine, where `q.eq(field, null)` against a stored explicit null is proved against
the same oracle. Both legs run in CI; the leg re-executes only those eleven, because `nullable` is
read in exactly one place in the adapter and an action whose execution path both mappers agree on
is translated identically by both — a claim the suites pin rather than assume, offline in
`npm test` and again against the running backend.

It cannot go further without lying about the documents: the other `nullable` fields
(`aOptionalString`, `aDouble`, `createdAt`, `scope`, `mainCategory` and its two chained paths) are
genuinely **absent** from some seeds, and a comparison against an absent path has CEL
missing-attribute semantics that a Convex filter cannot reproduce — which is exactly what
`nullable: true` exists to prevent.

The harness runs against a self-hosted `convex-backend` container, pinned by tag and digest in
`docker-compose.yml`. Convex Cloud is not exercised, so any divergence between the two — the
filter engine, value ordering, or the `undefined`/`null` distinction — is outside what this
contract proves.

**Behaviour change.** `filter()` and `map()` are now refused in **every** boolean position, not only at the root of the condition. `all: [R.attr.tags.filter(...), R.attr.aBool]` used to translate: the post-filter read the held list through a boolean coercion, got an evaluation error and denied every row — an emitted filter for a shape with no boolean meaning, which happened to agree with the PDP for the wrong reason. It now throws, which is a consumer-visible break for anyone relying on the empty result ([#387](https://github.com/cerbos/query-plan-adapters/issues/387)).

## Mapping hazards

The conformance contract above proves the *plan* side — given a policy shape, does the filter select the documents `check()` allows. The other half is the *mapping*: **the documents the filter reads must be the documents the application put into the resource attributes.** Six ways that can break are catalogued in the shared corpus, and every adapter has to record a position on each of them.

This adapter **builds no subquery.** Convex has no joins, so the mapper resolves every Cerbos path — including chained ones such as `mainCategory.subCategories` — to a path inside the same document, and the returned `filter`/`postFilter` pair reads exactly the document the application serialised.

| Hazard | Position | Mechanism to check |
|---|---|---|
| Filtered association | Not applicable — no subquery | — |
| Default scope on the target model | Not applicable — no second table is read | — |
| Subtype discrimination | **Caller-owned** | The table you pass to `ctx.db.query()`. The adapter is handed a plan, never a table, so it cannot check that the table you filter is the one the resource attributes were read from. If one Convex table holds several document shapes distinguished by a field, that field is not in the returned filter and you have to add it yourself |
| To-one relation used as a collection | Not applicable — a document path holds exactly what the application stored | — |
| Composite association key | Not applicable — no join, so no key to compose | — |
| Absent to-one parent | **Reproduced**, and proved by the corpus (`w1-all-chain`, `rel-not-bool-hop` and siblings) | None — the post-filter evaluates a missing path as a CEL error, which denies, so an absent parent is excluded under both polarities rather than reading as an empty collection ([#309](https://github.com/cerbos/query-plan-adapters/issues/309), [#375](https://github.com/cerbos/query-plan-adapters/issues/375)). **Behaviour change in #375:** a BARE variable in boolean position was pushed to Convex's filter engine regardless of `nullable`, and the engine cannot tell an absent path from a false one, so a negation over one readmitted every row missing that path. A `nullable` bare variable is now answered by the adapter's own evaluator instead — correct rows, and one more shape scanned rather than indexed |

## Example application

This repository carries a runnable [`example/`](example/), which installs the adapter from the
artifact `npm publish` would upload and exercises it against a live PDP over the shared
[demo domain](../demo/README.md):

```bash
# from the repository root
demo/scripts/run-example.sh convex
```

Unlike the test suites, it resolves the adapter through its **published** surface — the `exports`
map, `types`, the `files` allowlist, and the `@cerbos/core` peer range — and covers usage shapes
past a single flat query: `.paginate()` on top of the filter, and the adapter's filter composed
with an application-owned filter. It is also the one place a plan makes the round trip a Convex
consumer's plan actually makes, JSON-encoded into the backend and back out again.

## Testing

| Command | What it proves | What it needs |
| --- | --- | --- |
| `npm test` | **The filter this adapter emits.** The translator unit test: every corpus action, classified exactly once as a golden expectation or as a throw — plus the execution-path distribution over the whole corpus, the rules every pushed-down filter obeys, and the `allowPostFilter` gate, none of which needs a backend | Nothing but Node — no Cerbos sidecar, no Convex backend, no Docker |
| `npm run test:adversarial` | **The documents that filter returns**, inside a real Convex backend with `check()` as the oracle — and, per action, which half of the output selected them, so a pushed-down filter is proved against Convex's real filter engine rather than against a recorder | Cerbos CLI, Docker |
| `npm run golden:update` | — | Rewrites `golden/expectations.json` from what the translator emits today. Review the diff |

### The golden expectations

`npm test` reads its plans from `../conformance/wire-fixtures/` — the golden `PlanResources`
responses captured against the pinned Cerbos version — and asserts them against
`golden/expectations.json`, a **golden expectation** file this adapter owns. One entry per corpus
action, keyed by action name:

```jsonc
{
  "adapter": "convex",
  "regenerate": "npm run golden:update",
  "expectations": {
    "in-empty": { "kind": "KIND_ALWAYS_DENIED" },
    // Answered entirely by the adapter's own evaluator: there is no filter to record, and what
    // that evaluator decides is a question about documents, which the adversarial suite answers.
    "null-eq":  { "kind": "KIND_CONDITIONAL", "path": "post" },
    "cs-eq": {
      "kind": "KIND_CONDITIONAL",
      "path": "db",
      // The adapter emits a FUNCTION, `(q) => Expression<boolean>`, so there is no query text to
      // pin. What is recorded is the calls that function makes against the `FilterBuilder` it is
      // handed — here `q.eq(q.field("aString"), "one")`.
      "filter": {
        "op": "eq",
        "args": [{ "op": "field", "args": ["aString"] }, "one"]
      }
    }
  }
}
```

`path` is the routing decision — `db`, `post`, or `split` for a root `and` whose pushable conjuncts
narrow before the post-filter decides. It is pinned because it is the most consequential thing this
translator chooses and the least visible: both halves are supposed to return the same documents, so
an action that silently crossed the boundary would pass the adversarial suite unchanged while
altering what the database is asked to do.

An action this adapter refuses carries **no entry**: its pinned message is corpus data, in
`adapterctl.json`, and duplicating it here would be two places to change one string. A
wire fixture that is neither in this file nor declared unsupported fails the suite, which is what
makes a new corpus action land as a failure rather than as silence
([ADR 0006](../docs/adr/0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md),
[ADR 0007](../docs/adr/0007-adapters-share-data-not-code.md), and the "Golden expectations" section
of [conformance/README.md](../conformance/README.md)).

Whether those filters return the documents the PDP allows is a separate question, answered by the
adversarial suite, which does need a Cerbos sidecar and a Convex backend.

## Requirements

- Cerbos > v0.16 plus either the `@cerbos/http` or `@cerbos/grpc` client

## System Requirements

- Node.js >= 22.0.0
- Convex 1.x

## Installation

```bash
npm install @cerbos/orm-convex @cerbos/core
```

`@cerbos/core` is a peer dependency: it carries the query plan types, and your application and
this adapter have to share one copy of them. Installing it yourself is what keeps that true — with
a second copy in the tree an operand built by your Cerbos client is not the same object the adapter
inspects. npm 7+ installs missing peers automatically; pnpm and Yarn expect it to be declared.

You also need a Cerbos client to obtain a query plan in the first place — [`@cerbos/grpc`](https://www.npmjs.com/package/@cerbos/grpc)
or [`@cerbos/http`](https://www.npmjs.com/package/@cerbos/http). Install whichever your deployment
uses; this adapter deliberately depends on neither, so it does not pull a gRPC stack into an
application that talks HTTP.

## API

```ts
import {
  queryPlanToConvex,
  PlanKind,
  type Mapper,
} from "@cerbos/orm-convex";
import type { Expression, FilterBuilder } from "convex/server";
import type { DataModel } from "./_generated/dataModel";

const { kind, filter, postFilter } = queryPlanToConvex<
  FilterBuilder<DataModel["myTable"]>,
  Expression<boolean>
>({
  queryPlan, // PlanResourcesResponse obtained by trusted backend code
  mapper, // optional Mapper - see below
  allowPostFilter: true, // opt in to trusted-backend filtering (see note below)
});

if (kind === PlanKind.ALWAYS_DENIED) return [];
if (kind === PlanKind.ALWAYS_ALLOWED && !postFilter) {
  return await ctx.db.query("myTable").collect();
}

let query = ctx.db.query("myTable");
if (filter) query = query.filter(filter);
let results = await query.collect();
if (postFilter) results = results.filter(postFilter);
```

`PlanKind` is re-exported from `@cerbos/core`:

```ts
export enum PlanKind {
  ALWAYS_ALLOWED = "KIND_ALWAYS_ALLOWED",
  ALWAYS_DENIED = "KIND_ALWAYS_DENIED",
  CONDITIONAL = "KIND_CONDITIONAL",
}
```

### Mapper configuration

The Cerbos query plan references fields using paths such as `request.resource.attr.title`. Use a mapper to translate those names to the field names in your Convex documents.

```ts
export type MapperConfig = {
  field?: string;
  nullable?: boolean;
};

export type Mapper =
  | Record<string, MapperConfig>
  | ((key: string) => MapperConfig);
```

- `field` rewrites a single Cerbos path to a different field name in your Convex document. Dot-notation is supported for nested fields.
- `nullable` declares that the document field may be absent. Predicates involving that field are evaluated by the post-filter so missing values cannot be mistaken for ordinary Convex comparison results.

If you omit the mapper the adapter will use the query plan paths verbatim.

#### Direct fields

```ts
const mapper: Mapper = {
  "request.resource.attr.aBool": { field: "aBool" },
  "request.resource.attr.title": { field: "title" },
  "request.resource.attr.nested.value": { field: "metadata.value" },
  "request.resource.attr.optionalOwner": {
    field: "optionalOwner",
    nullable: true,
  },
};
```

#### Mapper functions

You can also supply a function if your mappings follow a predictable pattern:

```ts
const mapper: Mapper = (path) => ({
  field: path.replace("request.resource.attr.", ""),
});
```

## Trusted usage pattern

Call Cerbos from a trusted Convex action or from your application backend. Pass the resulting query plan to an `internalQuery` that applies both the Convex filter and any `postFilter`. Do not expose a public query that accepts a caller-supplied plan: an untrusted caller could submit an `ALWAYS_ALLOWED` plan.

```ts
import type { PlanResourcesResponse } from "@cerbos/core";
import {
  queryPlanToConvex,
  PlanKind,
  type Mapper,
} from "@cerbos/orm-convex";
import { internalQuery, type Expression, type FilterBuilder } from "convex/server";
import { v } from "convex/values";
import type { DataModel } from "./_generated/dataModel";

const mapper: Mapper = {
  "request.resource.attr.title": { field: "title" },
  "request.resource.attr.status": { field: "status" },
  "request.resource.attr.priority": { field: "priority" },
};

export const executePlan = internalQuery({
  args: { queryPlan: v.any() },
  handler: async (ctx, { queryPlan }) => {
    // This internal-only argument must be the response returned directly by Cerbos.
    const { kind, filter, postFilter } = queryPlanToConvex<
      FilterBuilder<DataModel["documents"]>,
      Expression<boolean>
    >({
      queryPlan: queryPlan as PlanResourcesResponse,
      mapper,
      allowPostFilter: true,
    });

    if (kind === PlanKind.ALWAYS_DENIED) return [];
    if (kind === PlanKind.ALWAYS_ALLOWED && !postFilter) {
      return await ctx.db.query("documents").collect();
    }

    let query = ctx.db.query("documents");
    if (filter) query = query.filter(filter);
    let results = await query.collect();
    if (postFilter) results = results.filter(postFilter);
    return results;
  },
});
```

## Error handling

`queryPlanToConvex` throws descriptive errors in the following scenarios:

- The plan kind is not one of the Cerbos `PlanKind` values (`Invalid query plan.`).
- A conditional plan omits the `operator`/`operands` structure (`Invalid Cerbos expression structure`).
- An operator listed in the plan is not implemented by this adapter (`Unsupported operator for Convex: <name>` or `Unsupported operator: <name>`).
- The `in` operator is given a non-array value.
- The query plan requires trusted-backend filtering and `allowPostFilter` is not set to `true`.

## Limitations

- String and collection operators (`contains`, `startsWith`, `endsWith`, `hasIntersection`, `exists`, `all`, etc.) are evaluated as a JavaScript `postFilter` after the DB query returns candidates. This means these conditions do not reduce the number of documents read from the database; they must still run in trusted backend code before any candidate is returned.
- For `or(...)` expressions where any child uses an unsupported operator, the entire OR is evaluated in the trusted backend via `postFilter`. Only `and(...)` expressions can be split between DB filter and post-filter.
- The `in` operator is composed as multiple `eq` comparisons joined with `or`, which may be less efficient for large value lists.
- `matches` rejects dynamic patterns and regex syntax outside the documented safe subset. This avoids JavaScript-only regex semantics and regular-expression denial of service in the trusted backend.
- Type conversions accept only CEL-compatible source types and strict string formats. JavaScript-only coercions such as `Number(true)`, `Number("")`, or `String(null)` fail closed.
