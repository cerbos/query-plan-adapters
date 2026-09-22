# Cerbos + Convex Adapter

Translates a [Cerbos](https://cerbos.dev) query plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan))
into a [Convex](https://convex.dev/) filter function `(q) => Expression<boolean>`, plus an in-memory
`postFilter` for the conditions Convex's filter engine cannot express.

## Install

```bash
npm install @cerbos/orm-convex @cerbos/core
npm install @cerbos/http   # or @cerbos/grpc — whichever client your deployment uses
```

- Node.js >= 22, Convex 1.x
- `@cerbos/core` `^0.32.0 || ^0.33.0` is a peer dependency. Install it yourself so your Cerbos
  client and the adapter share one copy of the plan types (npm 7+ adds it automatically; pnpm and
  Yarn do not).
- The adapter depends on neither Cerbos client, so you only pull in the transport you use.
- Cerbos PDP 0.55 or later if your policies can produce NaN inside a negated comparison (see
  [Behaviour changes](#behaviour-changes)); otherwise Cerbos > 0.16.

## Quick start

The filter is a function of Convex's `FilterBuilder`, which only exists inside a Convex query. So
you plan in trusted code (an action or your backend), then translate and query in an
`internalQuery`:

```ts
// convex/documents.ts
import type { PlanResourcesResponse } from "@cerbos/core";
import { HTTP } from "@cerbos/http";
import { PlanKind, queryPlanToConvex, type Mapper } from "@cerbos/orm-convex";
import type { Expression, FilterBuilder } from "convex/server";
import { v } from "convex/values";

import { internal } from "./_generated/api";
import type { DataModel, Doc } from "./_generated/dataModel";
import { action, internalQuery } from "./_generated/server";

const cerbos = new HTTP("http://localhost:3592");

const mapper: Mapper = {
  "request.resource.attr.ownerId": { field: "ownerId" },
  "request.resource.attr.status": { field: "status" },
};

export const listDocuments = action({
  args: {},
  handler: async (ctx): Promise<Doc<"documents">[]> => {
    const identity = await ctx.auth.getUserIdentity();
    if (!identity) throw new Error("Unauthenticated");

    const plan = await cerbos.planResources({
      principal: { id: identity.subject, roles: ["user"] },
      resource: { kind: "document" },
      action: "view",
    });

    // Convex arguments reject class instances; send the plan as plain JSON.
    return await ctx.runQuery(internal.documents.listAllowed, {
      queryPlan: JSON.parse(JSON.stringify(plan)),
    });
  },
});

export const listAllowed = internalQuery({
  args: { queryPlan: v.any() }, // must be the response Cerbos returned — never client input
  handler: async (ctx, { queryPlan }) => {
    const result = queryPlanToConvex<
      FilterBuilder<DataModel["documents"]>,
      Expression<boolean>
    >({
      queryPlan: queryPlan as PlanResourcesResponse,
      mapper,
      allowPostFilter: true, // only if your policies need it — see below
    });

    if (result.kind === PlanKind.ALWAYS_DENIED) return [];

    // ALWAYS_ALLOWED carries no filter; CONDITIONAL carries filter, postFilter, or both.
    const table = ctx.db.query("documents");
    let docs = await (result.filter ? table.filter(result.filter) : table).collect();
    if (result.postFilter) docs = docs.filter(result.postFilter);
    return docs;
  },
});
```

`queryPlanToConvex` returns `{ kind, path?, filter?, postFilter? }`. After narrowing `kind` to
`PlanKind.CONDITIONAL`, `path` says which fields are present: `"db"` → `filter`, `"post"` →
`postFilter`, `"split"` → both. `PlanKind` is re-exported from `@cerbos/core`. The plan survives the
JSON round trip because the adapter classifies operands by shape, not with `instanceof`
([#419](https://github.com/cerbos/query-plan-adapters/issues/419)).

## Trusted usage pattern

- Call Cerbos from trusted code: a Convex action, or your own backend.
- Translate and query in an **`internalQuery`**. Never expose a public query that accepts a plan: a
  caller who can hand you a plan can hand you an `ALWAYS_ALLOWED` one.
- `postFilter` is part of the authorization predicate. Apply it to every candidate in the same
  trusted function, before anything is serialized or returned. Never ship unfiltered candidates to
  a browser to filter there.

## Mapping attributes

Every attribute path the plan references must have a mapper entry, or translation throws
`No mapper entry for <reference>`. The one exception is a lambda's own iteration variable.

```ts
type MapperConfig = { field?: string; nullable?: boolean };
type Mapper = Record<string, MapperConfig> | ((key: string) => MapperConfig);
```

| Option | Meaning |
| --- | --- |
| `field` | The document field the Cerbos path maps to. Dot notation reaches nested fields (`metadata.value`), including chained paths such as `mainCategory.subCategories`. Omit it (`{}`) if the document field really is named like the plan path. |
| `nullable` | The field may be **absent** from a document. Predicates on it are evaluated by `postFilter`, where an absent path is a CEL missing-attribute error (deny), instead of in Convex's filter engine, which cannot tell absent from null or false. Required for every field that can be absent. |

```ts
const mapper: Mapper = {
  "request.resource.attr.title": { field: "title" },
  "request.resource.attr.nested.value": { field: "metadata.value" },
  "request.resource.attr.optionalOwner": { field: "optionalOwner", nullable: true },
};

// Or a function, when the mapping follows a pattern:
const mapper: Mapper = (path) => ({ field: path.replace("request.resource.attr.", "") });
```

Convex has no joins: every path, relations included, resolves to a path inside the same document.

## `allowPostFilter`

Convex's filter engine only has comparisons and `and`/`or`/`not`. Anything else — string,
collection, arithmetic, cast operators — is evaluated in JavaScript by `postFilter` after the
documents are read. Because that reads documents before the full authorization predicate has run,
`queryPlanToConvex` **throws** whenever a plan needs a `postFilter`, unless you pass
`allowPostFilter: true`.

If your policies only use comparisons, `in`, null checks and logical operators, leave it off:
`filter` alone enforces the whole policy in the database.

How the tree is split:

- A root `and` is split: pushable conjuncts go to `filter` (narrowing the read), the rest to
  `postFilter` (`path: "split"`).
- An `or` with any non-pushable child goes entirely to `postFilter`; a partial push-down would drop
  matches.
- Post-filtered conditions do not reduce the number of documents read.

### Quantifiers over known collections

A quantifier whose collection the PDP resolves at plan time (for example
`P.attr.teams.exists(t, R.attr.team == t)`) arrives in one of two shapes. At 10 elements or fewer
the planner unrolls it into an `or`/`and` chain, which pushes into `filter`. Above that it ships the
lambda over a literal list, which is evaluated by `postFilter` and so needs `allowPostFilter: true`
(cerbos/cerbos#2570, cerbos/cerbos#2817). Both give the same decisions, including `exists_one`.

## NULL attribute representation

The planner emits the same `eq(x, null)` node for `R.attr.x == null` however you represent a NULL
field in the attributes you send to `check()`, so tell the adapter which convention you use:

| Attributes you send for a NULL field | `check()` | `q.eq(field, null)` |
| --- | --- | --- |
| `{"x": null}` — explicit null | allow | selects it — aligned |
| `{}` — attribute omitted | **deny** (missing-attribute error) | selects a stored null — **over-grants** |

`nullAttributeRepresentation` defaults to `"explicit"`. If you omit attributes for NULL fields, set
`"omitted"`: the adapter then rejects every null comparison operand, in both `filter` and
`postFilter`, rather than return documents the PDP denies.

```ts
queryPlanToConvex({ queryPlan, mapper, nullAttributeRepresentation: "omitted" });
```

The rejection is wider than the shapes that actually over-grant (`x != null` is aligned under both
conventions), because a leaf cannot see whether an enclosing `not` will flip it. Storing the field
as absent (with `nullable: true`) also aligns the two via `postFilter`, but that depends on your
document shape; the option is the reliable guard. See
[#302](https://github.com/cerbos/query-plan-adapters/issues/302).

## Supported operators

Pushed to Convex's filter engine (`filter`):

| Category | Operators | Emits |
| --- | --- | --- |
| Logical | `and`, `or`, `not` | `q.and`, `q.or`, `q.not` |
| Comparison | `eq`, `ne`, `lt`, `le`, `gt`, `ge` | `q.eq`, `q.neq`, `q.lt`, `q.lte`, `q.gt`, `q.gte` |
| Membership | `in` | `q.or(q.eq(field, v1), q.eq(field, v2), …)` — can be slow for long lists |
| Null checks | `eq`/`ne` against `null` | The planner has no existence operator |

Evaluated in JavaScript (`postFilter`, needs `allowPostFilter: true`):

| Category | Operators | Behaviour |
| --- | --- | --- |
| String | `contains`, `startsWith`, `endsWith` | `includes` / `startsWith` / `endsWith` |
| Collection | `hasIntersection`, `index`, `get-field`, `size` | CEL-compatible collection and nested-field evaluation |
| Quantifiers | `exists`, `exists_one`, `all` | CEL lambda semantics, including empty collections, missing members and literal lists |
| Higher-order | `filter`, `map`, `lambda` | Inside larger expressions only — see below |
| Arithmetic | `add`, `sub`, `mult`, `div`, `mod` | Numeric, with CEL error propagation |
| Conversion | `string`, `double`, `int`, `timestamp` | CEL source types and strict formats only; JS coercions like `Number(true)`, `Number("")`, `String(null)` fail closed. RFC 3339 timestamps |
| Conditional | `if` | Evaluates only the selected branch |
| Pattern | `matches` | Constant patterns in the safe RE2/JS subset: literals, `^`/`$` anchors, a trailing `.*` when there is no `$`. Dynamic patterns and anything else throw (avoids JS-only regex semantics and ReDoS) |
| Hierarchy | `hierarchy`, `ancestorOf`, `descendentOf`, `overlaps` | Delimiter-aware comparison |

### What throws

Translation throws, before any filter exists, when:

- the plan kind is unknown (`Invalid query plan.`);
- a conditional plan lacks the `operator`/`operands` structure (`Invalid Cerbos expression structure`);
- an operator is not implemented (`Unsupported operator: <name>`);
- `in` is not given one field and one array value (`in operator requires one field and one array value`);
- the plan needs a `postFilter` and `allowPostFilter` is not `true`;
- a referenced attribute has no mapper entry (`No mapper entry for <reference>`);
- `nullAttributeRepresentation: "omitted"` and the plan has a null comparison operand;
- the shape is one of the fail-closed classes in the [Conformance contract](#conformance-contract):
  `filter()`/`map()` in a boolean position; `list`, `struct`, `set-field` and `except` forms without
  a lowering; regex outside the supported subset; a constant zero divisor whose sign JSON discards;
  a division used as another division's denominator.

## Conformance contract

The adapter is differentially tested with 29 hostile seed documents against Cerbos PDP 0.55.0
`checkResource` decisions, in both evaluation modes: each plan is translated, executed inside a
Convex query function, and the returned IDs must equal the PDP's per-document decisions. The Spring
Data adapter defines the reference semantics for this snapshot.

| Classification | Coverage |
| --- | --- |
| Oracle-tested | 262 reference conformance actions, plus `matches()`, list indexing/`get-field`, `timestamp()`, and `int()`/`double()` cast plans that the Spring Data reference adapter rejects — the post-filter reimplements CEL cast semantics exactly (whole-string parse, truncation toward zero), so the SQL divergences do not apply (269 actions total). A positional read of a number or boolean list keeps the element's JSON type, so `true` never equals `1` and a null element is a value that negation admits (the `index-number-list*` and `index-bool-list*` actions) |
| Fail-closed | `filter()`/`map()` used as a condition or conjunct; `list`, `struct`, and `except` constructor/operator forms without a lowering; regex patterns outside the supported RE2 subset; a constant zero divisor whose sign the JSON hop discards; and a nested division denominator whose numeric type the plan does not preserve (30 actions). All 30 throw during translation, before any filter exists; unknown operators and invalid expression structures still throw |
| Explicit opt-in | Any plan that cannot be represented entirely as a Convex database filter requires `allowPostFilter: true` |
| Representation-dependent | `null-eq-missing` — rejected under `nullAttributeRepresentation: "omitted"`. Under the default it returns the empty set the PDP demands when the document omits the field for a NULL value (what the harness seeds), because the field is `nullable: true` and `postFilter` raises the same missing-attribute error `check()` does. A deployment that stores explicit nulls while omitting the attribute would over-grant |
| Attribute NULL convention | Needs no declaration: Convex stores the value the caller sent, so a stored null compares as a null value exactly as CEL does, and stays distinguishable from an absent field. Every `null-value-*` probe for the explicit convention (cerbos/query-plan-adapters#308) is aligned — including `null-value-f2f-mixed`, which Convex and Mongoose are the only two adapters to translate rather than refuse |
| Known planner divergence | `has()` on a missing attribute is currently folded by the Cerbos planner to `ALWAYS_ALLOWED`; `checkResource` still denies documents where the attribute is missing. Until the planner is fixed, use `R.attr.x != null` for database-backed attributes instead of `has(R.attr.x)` |

Coverage includes value-first comparisons, field-to-field expressions, null and missing-attribute
behaviour, nested lambdas, collection macros, string and arithmetic expressions, timestamps,
hierarchy operations and chained nested fields. Every fail-closed message is pinned in
`conformance/actions.json` and asserted. The emitted filters themselves are pinned by the
translator unit test (see [Development](#development)), which is also the only place the
`allowPostFilter` gate, function mappers, unmapped-reference refusal, the
`nullAttributeRepresentation` boundary and malformed input are asserted.

### What the differential proves, and what it does not

Most of the corpus is decided by `postFilter`, not by Convex. The split is pinned by the
conformance run:

| Decided by | Default mapper | Pushdown mapper |
| --- | --- | --- |
| Convex's filter engine, alone | 29 | 40 |
| the engine narrowing and the `postFilter` deciding (`rel-hop-and-root`) | 1 | 1 |
| the adapter's `postFilter`, alone | 233 | 222 |
| folded to an unconditional plan before any filter exists | 6 | 6 |

For the 233 post-filtered actions the differential compares the adapter's CEL evaluator against the
PDP's; Convex's own comparison semantics only decide the 29. The **pushdown mapper** leg clears
`nullable` on `owner` (always present, stored as `v.union(v.string(), v.null())`), which moves 11
null-comparison actions (`null-eq`, `null-ne`, `null-not-eq`, `vf-null-ne`, the four
`in-null-elem-*` and the three `null-value-*-const`) into the engine, proving `q.eq(field, null)`
against a stored null. The other `nullable` fields (`aOptionalString`, `aDouble`, `createdAt`,
`updatedAt`, `scope`, `mainCategory` and its two chained paths) are genuinely absent from some
seeds, so they cannot be pushed down.

The harness runs against a self-hosted `convex-backend` container pinned in `docker-compose.yml`.
Convex Cloud is not exercised: any difference in its filter engine, value ordering or
`undefined`/`null` handling is outside this contract.

## Mapping hazards

The contract above proves the *plan* side. The other half is the *mapping*: the documents the filter
reads must be the documents whose attributes you sent to Cerbos. The shared corpus catalogues six
ways that can break. This adapter builds no subquery — every path resolves inside the same
document — so most do not apply.

| Hazard | Position | Mechanism to check |
|---|---|---|
| Filtered association | Not applicable — no subquery | — |
| Default scope on the target model | Not applicable — no second table is read | — |
| Subtype discrimination | **Caller-owned** | The table you pass to `ctx.db.query()`. The adapter never sees it. If one table holds several document shapes distinguished by a field, add that field to the query yourself |
| To-one relation used as a collection | Not applicable — a document path holds exactly what the application stored | — |
| Composite association key | Not applicable — no join, so no key to compose | — |
| Absent to-one parent | **Reproduced**, proved by the corpus (`w1-all-chain`, `rel-not-bool-hop` and siblings) | None — `postFilter` evaluates a missing path as a CEL error, which denies, so an absent parent is excluded under both polarities ([#309](https://github.com/cerbos/query-plan-adapters/issues/309), [#375](https://github.com/cerbos/query-plan-adapters/issues/375)) |

## Behaviour changes

See also [CHANGELOG.md](CHANGELOG.md).

- **Breaking (0.3.0):** `QueryPlanToConvexResult` is a discriminated union. Conditional results
  carry `path` (`db` → `filter`, `post` → `postFilter`, `split` → both); unconditional results carry
  neither. Destructuring still works; code that *constructs* a result must supply `path`.
- **Breaking:** an unmapped reference now throws `No mapper entry for <reference>` instead of being
  read as a document path. That path was absent everywhere, so a negated comparison matched every
  document. Callers passing no mapper are affected; if your documents really are shaped like plan
  paths, map each reference to `{}` (or `{ nullable: true }`)
  ([#492](https://github.com/cerbos/query-plan-adapters/issues/492)).
- **Breaking:** `filter()` and `map()` are refused in every boolean position, not only at the root.
  `all: [R.attr.tags.filter(...), R.attr.aBool]` used to translate and deny every row by accident;
  it now throws ([#387](https://github.com/cerbos/query-plan-adapters/issues/387)).
- **Breaking:** `struct`, `set-field`, `list` and `except` constructors the evaluator does not
  implement, regex outside the supported subset, and a division used as another division's
  denominator now throw at translation (a zero divisor previously failed only while evaluating a
  document).
- **Breaking:** a `nullable` bare boolean variable is now answered by `postFilter` rather than
  pushed to Convex, whose engine cannot tell an absent path from `false`, so a negation over it
  readmitted every document missing the path. Correct rows, but one more shape scanned, and such
  plans now need `allowPostFilter: true`
  ([#375](https://github.com/cerbos/query-plan-adapters/issues/375)).
- **Breaking (Cerbos 0.55 compatibility):** ordered comparisons involving NaN evaluate to false, so
  their negation can allow a row; Cerbos 0.54 denied it. The adapter follows 0.55 — use it with
  Cerbos 0.55 when policies can produce NaN in a negated comparison. Missing attributes and nulls
  are unchanged.

## Example application

[`example/`](example/) installs the packed adapter and runs it against a live PDP over the shared
[demo domain](../demo/README.md), including `.paginate()` over the filter and the filter composed
with an application-owned predicate:

```bash
# from the repository root
demo/scripts/run-example.sh convex
```

## Development

| Command | What it proves | Needs |
| --- | --- | --- |
| `npm test` | The translator unit test: the filter emitted for every corpus action (golden expectation or pinned throw), the execution-path distribution, the rules every pushed-down filter obeys, the `allowPostFilter` gate | Node only |
| `npm run typecheck` | `src/` and the tests type-check | A deployed backend with `convex/_generated` for the harness |
| `npm run test:adversarial` | The documents each filter returns, in a real Convex backend with `check()` as the oracle, and which half of the output selected them | Cerbos CLI, Docker (`npm run convex:up`, then deploy the functions in `convex/`), `CONVEX_URL` |
| `npm run golden:update` | — | Rewrites `golden/expectations.json` from what the translator emits today. Review the diff |

`ADAPTER_TEST_STRICT_EVALUATION=true|false` (default `false`; other values are rejected) selects
the PDP's evaluation mode for both planning and the `check()` oracle. CI runs both. See the
Convex job in [`.github/workflows/convex.yaml`](../.github/workflows/convex.yaml) for the full
backend bring-up sequence.

### The golden expectations

`npm test` reads plans from `../conformance/wire-fixtures/` and compares them with
`golden/expectations.json`, one entry per corpus action. Because the adapter emits a function, an
entry records the calls that function makes against a recording `FilterBuilder`, plus `path` — the
routing decision between `db`, `post` and `split`, pinned because an action that silently crossed
the boundary would still pass the adversarial suite:

```jsonc
"cs-eq": {
  "kind": "KIND_CONDITIONAL",
  "path": "db",
  "filter": { "op": "eq", "args": [{ "op": "field", "args": ["aString"] }, "one"] }
},
"null-eq":  { "kind": "KIND_CONDITIONAL", "path": "post" },
"in-empty": { "kind": "KIND_ALWAYS_DENIED" }
```

A refused action has no entry (its message is in `conformance/actions.json`); a fixture with
neither fails the suite, so a new corpus action cannot land silently. See "Golden expectations" in
[conformance/README.md](../conformance/README.md),
[ADR 0006](../docs/adr/0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md) and
[ADR 0007](../docs/adr/0007-adapters-share-data-not-code.md).
