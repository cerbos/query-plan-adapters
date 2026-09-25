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
| `nullable` | The field may be **absent** from a document. Predicates on it are evaluated by `postFilter`, where an absent path is a CEL missing-attribute error (deny), instead of in Convex's filter engine, which cannot tell absent from null or false. Required for every field that can be absent. Left undeclared, it follows the call's [`nullAttributeRepresentation`](#null-attribute-representation): off under `"explicit"`, on under `"omitted"`. |

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
`"omitted"`:

```ts
queryPlanToConvex({ queryPlan, mapper, nullAttributeRepresentation: "omitted", allowPostFilter: true });
```

The call-level option is the default for every mapper entry that does not declare `nullable`, so
under `"omitted"` the adapter:

- rejects every null comparison operand, in both `filter` and `postFilter`. The rejection is wider
  than the shapes that actually over-grant (`x != null` is aligned under both conventions), because
  a leaf cannot see whether an enclosing `not` will flip it
  ([#302](https://github.com/cerbos/query-plan-adapters/issues/302));
- treats every entry that does not declare `nullable` as `nullable: true`, so its comparisons are
  answered by `postFilter` and need `allowPostFilter: true`. Convex's engine cannot guard them:
  `q.neq(...)` and a negated comparison match a document the field is absent from, which `check()`
  denies ([#493](https://github.com/cerbos/query-plan-adapters/issues/493));
- has `postFilter` read a stored `null` as a missing attribute, which denies under both polarities,
  since under this convention the application sends no attribute for it.

`nullable: false` opts an entry out: it asserts the field is always stored and never null, and its
comparisons go to Convex's filter engine as they do under `"explicit"`.

## Supported operators

Pushed to Convex's filter engine (`filter`):

| Category | Operators | Emits |
| --- | --- | --- |
| Logical | `and`, `or`, `not` | `q.and`, `q.or`, `q.not` |
| Comparison | `eq`, `ne` | `q.eq`, `q.neq` |
| Ordering | `lt`, `le`, `gt`, `ge` | `q.lt`, `q.lte`, `q.gt`, `q.gte`, each inside a guard confining the field to the literal's type ([#516](https://github.com/cerbos/query-plan-adapters/issues/516)) |
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

Shapes the adapter cannot express throw `UnsupportedQueryPlanError` rather than emit a broader
filter. It is exported and extends `Error`, so existing `catch` blocks keep working:

```ts
import { queryPlanToConvex, UnsupportedQueryPlanError } from "@cerbos/orm-convex";

try {
  const result = queryPlanToConvex({ queryPlan, mapper, allowPostFilter: true });
} catch (error) {
  if (error instanceof UnsupportedQueryPlanError) {
    // The policy uses a shape this adapter cannot translate faithfully: deny, or fall back to
    // per-document check() calls.
  }
  throw error;
}
```

It is raised, before any filter exists, when:

- the plan kind is unknown (`Invalid query plan.`);
- a conditional plan lacks the `operator`/`operands` structure (`Invalid Cerbos expression structure`);
- an operator is not implemented (`Unsupported operator: <name>`) — including the `list`, `struct`,
  `set-field` and `except` forms without a lowering;
- `filter()`/`map()` sits in a boolean position;
- a `matches` pattern is outside the supported subset;
- a constant zero divisor whose sign JSON discards, or a division used as another division's
  denominator;
- `nullAttributeRepresentation: "omitted"` and the plan has a null comparison operand.

Two failures are deliberately a plain `Error`, because they are about the call rather than the
policy: a referenced attribute with no mapper entry (`No mapper entry for <reference>`), and a plan
that needs a `postFilter` when `allowPostFilter` is not `true`.

## Conformance contract

The adapter is replayed against the shared [conformance corpus](../conformance/README.md): the plans
and `check()` decisions recorded from Cerbos PDP 0.55.0 (and 0.54.0), executed inside a Convex query
function over the corpus's 41 seed documents. Passed cases on the current PDP, 0.55.0, where the
total is every golden case in that tier:

| Tier | Passed / total |
| --- | --- |
| core | 26 / 26 |
| extended | 67 / 80 |
| adversarial | 242 / 270 |

Cases the golden marks as a Cerbos planner divergence are skipped, not compared: no adapter can pass
them, because the plan and `check()` disagree. On 0.55.0 there are four, all extended, which is why
that tier's passed and refused cases add up to four fewer than its total. `null/has/missing-attribute`:
the planner folds `has()` on a missing attribute to `ALWAYS_ALLOWED` while `checkResource` denies
the missing-attribute documents, so use `R.attr.x != null` for database-backed attributes instead
of `has(R.attr.x)`. And three `composition/*` cases whose DENY rule reads `aNumber`: on the
missing-`aNumber` document the DENY condition errors, so `check()` does not deny it, while the plan
negates that condition and the negation is itself a missing-attribute error.

Every other case that does not pass is refused with `UnsupportedQueryPlanError`; none returns wrong
documents. [`conformance-ledger.json`](conformance-ledger.json) lists each one with its reason.

The corpus's two NULL conventions need no option here, only the mapping and the document shape: an
attribute the caller omits for a NULL value is absent from the document and declared
`nullable: true`, so `postFilter` raises the same missing-attribute error `check()` does; an
explicit-null attribute (`owner`, `coOwner`) is stored as a null value and is not `nullable`, so
Convex's engine compares it as a value, exactly as CEL does.

### What the conformance run proves, and what it does not

Most of the corpus is decided by `postFilter`, not by Convex. Of the 332 cases that pass on 0.55.0,
the harness reports:

| Decided by | Cases |
| --- | --- |
| Convex's filter engine, alone | 18 |
| the adapter's `postFilter`, alone | 308 |
| folded to an unconditional plan before any filter exists | 6 |

For the post-filtered cases the run compares the adapter's CEL evaluator against the PDP's;
Convex's own comparison semantics only decide the 18, which include the null comparisons against
the explicit-null `owner` field (`q.eq(field, null)` against a stored null) and its orderings
against a string, where a stored null must not sort below `"m"`.
The corpus's three most-read scalars, `aBool`, `aString` and `aNumber`, can each be absent (one
seed apiece), so the harness declares them `nullable` and their predicates are post-filtered too:
Convex's filter engine cannot tell an absent field from a present one the way CEL does.

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
| Absent to-one parent | **Reproduced**, proved by the corpus (`relation/all/to-one-chain`, `relation/bare-attribute/negated-one-hop-boolean` and siblings) | None — `postFilter` evaluates a missing path as a CEL error, which denies, so an absent parent is excluded under both polarities ([#309](https://github.com/cerbos/query-plan-adapters/issues/309), [#375](https://github.com/cerbos/query-plan-adapters/issues/375)) |

## Behaviour changes

See also [CHANGELOG.md](CHANGELOG.md).

- **Breaking:** under `nullAttributeRepresentation: "omitted"`, a mapper entry that does not
  declare `nullable` is treated as `nullable: true`, and `postFilter` reads a stored `null` as a
  missing attribute. Comparisons over such an entry move to `postFilter` and need
  `allowPostFilter: true`; the old pushed-down `q.neq(...)` and negations matched documents the
  field was missing from, which `check()` denies. Declare `nullable: false` on an entry that is
  always stored and never null to keep it on Convex's engine. `"explicit"` output is unchanged
  ([#493](https://github.com/cerbos/query-plan-adapters/issues/493)).
- A shape the adapter refuses now throws `UnsupportedQueryPlanError`, an exported subclass of
  `Error`. What it translates is unchanged, and existing `catch` blocks keep working; an unmapped
  reference and a missing `allowPostFilter` opt-in stay a plain `Error`.
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
- An ordering against a literal is pushed to Convex only inside a guard confining the field to the
  literal's type, and a `not` above it is pushed inward so the guard is never negated. Convex
  orders values across types (null < number < boolean < string), so `q.lt(field, "5")` held for
  every number, where CEL's `5 < "5"` is an error that denies; under `!`, `!(x >= "5")`
  returned every number. An ordering against a null, a list or a map is now a constant false.
  Filters change shape; nothing that translated now throws
  ([#516](https://github.com/cerbos/query-plan-adapters/issues/516)).
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

| Command | What it does | Needs |
| --- | --- | --- |
| `npm test` | Caller-supplied options the corpus cannot vary (function mappers, the unmapped-reference refusal, the `allowPostFilter` gate, the `nullAttributeRepresentation` boundary), the refusal type, the rules every filter handed to Convex obeys, and malformed input | Node only |
| `npm run typecheck` | Type-checks `src/`, the tests and the backend functions | A deployed backend with `convex/_generated` for the harness |
| `npm run convex:up` | Starts the Convex backend pinned in `docker-compose.yml` on port 3210 | Docker |
| `npm run test:adversarial` | The documents each recorded golden plan returns in a real Convex backend equal the recorded `check()` decisions, for both pinned PDPs | `npm run convex:up`, then `npx convex deploy` and `npx convex codegen`; `CONVEX_URL` if not on 3210 |

No suite starts a PDP. The harness reads the golden files under `../conformance/golden/` and applies
[`conformance-ledger.json`](conformance-ledger.json): a case with no entry must return exactly the
recorded allowed ids, and an `unsupported` case must throw `UnsupportedQueryPlanError`. See
"The harness contract" in [conformance/README.md](../conformance/README.md), and the
`integration-test` job in [`.github/workflows/convex.yaml`](../.github/workflows/convex.yaml) for the
full backend bring-up sequence.
