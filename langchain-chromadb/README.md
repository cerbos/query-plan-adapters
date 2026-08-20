# Cerbos + LangChain.js ChromaDB Adapter

An adapter library that takes a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into a [ChromaDB](https://www.trychroma.com/) filter object that can be passed to the LangChain.js Chroma vector store. This is designed to work alongside a project using the [Cerbos Javascript SDK](https://github.com/cerbos/cerbos-sdk-javascript).

## How it works

1. Use a Cerbos client (`@cerbos/http` or `@cerbos/grpc`) to call `planResources` and obtain a `PlanResourcesResponse`.
2. Provide `queryPlanToChromaDB` with that plan and a `fieldNameMapper` that describes how Cerbos attribute paths relate to your ChromaDB metadata fields.
3. The adapter walks the Cerbos expression tree, translates supported operators to ChromaDB `Where` filter syntax, and returns `{ kind, filters? }`.
4. Inspect `result.kind`:
   - `ALWAYS_ALLOWED`: the caller can query without any additional filters.
   - `ALWAYS_DENIED`: short-circuit and return an empty result set.
   - `CONDITIONAL`: execute the query with `result.filters`.

## Supported operators

| Category | Cerbos operators | ChromaDB output |
| --- | --- | --- |
| Logical | `and`, `or` | `$and`, `$or` |
| Negation | `not` | Operator inversion and De Morgan's law (see below) |
| Comparisons | `eq`, `ne`, `lt`, `le`, `gt`, `ge` | `$eq`, `$ne`, `$lt`, `$lte`, `$gt`, `$gte` |
| Membership | `in` | `$in` |

### Negation handling

ChromaDB's `Where` filter does not support `$not` or `$nor`. The adapter handles `not` expressions by inverting the inner operator:

- `not(eq)` → `$ne`, `not(ne)` → `$eq`
- `not(lt)` → `$gte`, `not(gt)` → `$lte`, `not(le)` → `$gt`, `not(ge)` → `$lt`
- `not(in)` → `$nin`
- `not(and(A, B))` → `$or[not(A), not(B)]` (De Morgan's law)
- `not(or(A, B))` → `$and[not(A), not(B)]` (De Morgan's law)
- `not(not(X))` → `X` (double negation elimination)

### Not supported

ChromaDB stores flat scalar metadata, so the following Cerbos operators cannot be mapped:

- String helpers: `contains`, `startsWith`, `endsWith`
- Null checks: `eq`/`ne` against a null value (Chroma metadata values are non-null scalars, so a
  comparison against null cannot be represented)
- Array/collection: `hasIntersection`, `exists`, `exists_one`, `all`, `filter`, `map`, `lambda`, `size`

Any unsupported operator in the plan causes `queryPlanToChromaDB` to throw an error.

### Write membership as `in`, not as a collection macro

A policy that checks a resource field against a principal collection has two
equivalent spellings, and only one of them translates reliably here:

```yaml
# Fragile — translates for some principals and throws for others
expr: P.attr.teams.exists(t, R.attr.team == t)

# Portable — always a single $in
expr: R.attr.team in P.attr.teams
```

The macro form is a trap on ChromaDB. `P.attr.*` is folded to a known value at
plan time, and the Cerbos planner unrolls the macro into a plain `or` chain of
equality comparisons — which this adapter translates — but only up to 10
elements (`maxItems = 10` in the planner's struct matcher; cerbos/cerbos#2570,
cerbos/cerbos#2817). Above that it ships the lambda with a literal value-list
collection, which has no ChromaDB metadata translation and throws. The result is
a data-dependent cliff: the same policy works for a principal with 10 teams and
fails for one with 11.

`all` is worse still: below the cap it unrolls to a chain of `ne`, which this
adapter only accepts when the mapper declares `required: true` for the field,
because a document missing the metadata key would otherwise match `$ne` and be
over-granted.

The membership form has no such threshold. It reaches the adapter as `in`
against a literal list at every collection size and maps to a single `$in`
filter, which is also cheaper for ChromaDB to evaluate than an `or` chain, and
needs no `required` assertion.

The cliff itself is pinned in the shared corpus rather than here, by a pair of
actions over the same policy at two collection sizes: `pv-exists-unrolled`
(three elements, below the cap) translates to a nested `$or` chain, and
`pv-exists` (eleven elements, above it) throws. `pv-all-unrolled` and `pv-all`
are the `all` half of the same pair, and both throw — the unrolled `$ne` chain
targets an optional metadata key, which is the over-grant `required: true`
exists to prevent.

**The membership spelling has no such pair, because it has no boundary to
straddle.** `P.attr.*` is folded before the adapter sees anything, so
`R.attr.x in P.attr.xs` reaches the wire as `in(key, [literals])` at every
collection size — structurally the corpus's `p-in-null-multi`, which is
oracle-tested — and there is no size at which the planner emits something else.
What the retired suite pinned by planning at 9, 10, 11 and 40 elements was
therefore a **planner** property, not a translator one, and a wire fixture
cannot carry it. A corpus action that spells the recommendation directly is
tracked in
[#411](https://github.com/cerbos/query-plan-adapters/issues/411).

## NULL attribute representation

Other adapters in this repo take a `nullAttributeRepresentation` option, because `R.attr.x == null`
compiles to the same `eq(x, null)` plan node whether the caller sends a NULL column as an explicit
`null` attribute (where `check()` allows the row) or omits the attribute entirely (where CEL raises
a missing-attribute error and `check()` denies it) — and an `IS NULL`-shaped filter over-grants in
the second case.

**This adapter needs no such option.** Chroma metadata holds only finite numbers, strings and
booleans, so it cannot store an explicit null distinguishably from an absent key: every null
comparison operand is rejected outright, under either convention. `null-eq`, `null-ne`,
`vf-null-ne`, `null-not-eq`, the `in-null-elem-*` family and `null-eq-missing` are all fail-closed
for that reason. See [#302](https://github.com/cerbos/query-plan-adapters/issues/302).

## Conformance contract

The adapter is differentially tested against Cerbos PDP 0.54.0 `checkResource` decisions using the canonical check resources and real ChromaDB metadata queries.

| Classification | Coverage |
| --- | --- |
| Oracle-tested | Every catalog action with a `matched` direct outcome in `adapterctl.json`; catalog cardinality expectations guard empty, total, and proper-subset oracles |
| Fail-closed | Every catalog action with a `rejected` direct outcome in `adapterctl.json`; its pinned message substring is asserted. Chroma's flat scalar metadata model rejects nested collections, computed operands, patterns, temporal values, nullable inequality, and other shapes it cannot express faithfully |
| Representation-independent | `null-eq-missing` — rejected like every other null comparison operand, so no `nullAttributeRepresentation` option is required |
| Attribute NULL convention | Also representation-independent, and for the same reason: Chroma metadata has no null value, so a NULL column is stored as an ABSENT key and `$ne`/`$nin` match absent records. All five `null-value-*` probes for the explicit convention (cerbos/query-plan-adapters#308) are refused rather than answered narrowly |
| Known planner divergence | `has()` on a missing attribute is folded by the Cerbos planner to `ALWAYS_ALLOWED`, while `checkResource` denies the missing-attribute documents. Until the planner is fixed, use `R.attr.x != null` for database-backed attributes instead of `has(R.attr.x)` |

Chroma metadata filters are limited to flat scalar comparisons and membership. Nested collections, field-to-field and arithmetic expressions, string helpers, hierarchy/timestamp operations, ternaries, nullable inequality, and other shapes that cannot be represented faithfully throw before a filter is returned. Every fail-closed shape's error message is pinned in this adapter's direct-outcome manifest (`adapterctl.json`) and asserted by its conformance run, so an outcome proves the throw names its declared mechanism rather than merely that something threw.

The `Where` document each translated action produces is pinned separately, in the translator unit test (`npm test`) — see [Testing](#testing). That is what makes a change to the emitted filter show up as a diff even when it selects the same documents from the corpus seeds, and it is the only place the parts of the mapper contract no policy can reach are asserted at all: function mappers, the `required` and `numericType` declarations, the fallback for an unmapped reference, and malformed input.

That test also pins **where** each refusal happens. Most of this catalog is fail-closed here, so the interesting property is not that a shape throws but which rejection site it reaches. The distribution is derived from the direct outcomes at runtime, while the test still pins the expected site totals because arithmetic, casts, ternaries, projections and above-cap collection macros all arrive at the wire as the same thing: an operand that is neither a bare metadata key nor a literal.

## Mapping hazards

The conformance contract above proves the *plan* side — given a policy shape, does the filter select the documents `check()` allows. The other half is the *mapping*: **the documents the filter reads must be the documents the application put into the resource attributes.** Six ways that can break are catalogued in the shared corpus, and every adapter has to record a position on each of them.

This adapter **builds no subquery**, and has nothing to build one over: a Chroma `where` clause compares flat scalar metadata on the document being matched. Every shape that would need to reach a second record is already in the fail-closed set above.

| Hazard | Position | Mechanism to check |
|---|---|---|
| Filtered association | Not applicable — no subquery | — |
| Default scope on the target model | Not applicable — no second collection is read | — |
| Subtype discrimination | **Caller-owned** | The Chroma collection you pass the `where` clause to. The adapter is handed a plan, never a collection, so it cannot check that the collection you query is the one whose metadata became the resource attributes. If one collection mixes document kinds, add the discriminating metadata key to the `where` yourself |
| To-one relation used as a collection | Not applicable — a metadata key holds exactly what the application stored | — |
| Composite association key | Not applicable — no join, so no key to compose | — |
| Absent to-one parent | **Rejected** — `w1-all-chain`, `w1-not-exists-chain` and the eight other chained shapes have `rejected` direct outcomes and throw | None — Chroma metadata is flat, so a chain has nowhere to resolve to and the adapter refuses the plan rather than flattening it ([#309](https://github.com/cerbos/query-plan-adapters/issues/309)) |

## Requirements

- Cerbos > v0.16
- `@cerbos/http` or `@cerbos/grpc` client
- Node.js >= 22.0.0
- ChromaDB 3.x

## Installation

```bash
npm install @cerbos/langchain-chromadb @cerbos/core
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
import { queryPlanToChromaDB, PlanKind } from "@cerbos/langchain-chromadb";

const result = queryPlanToChromaDB({
  queryPlan, // PlanResourcesResponse from Cerbos
  fieldNameMapper, // map or function - see below
});

if (result.kind === PlanKind.CONDITIONAL) {
  // use result.filters as the `where` property of a ChromaDB query
}
```

`QueryPlanToChromaDBArgs` and `QueryPlanToChromaDBResult` are exported alongside the function, so a
caller passing either one to a function of its own has a name to write.

`PlanKind` is re-exported from `@cerbos/core`:

```ts
export enum PlanKind {
  ALWAYS_ALLOWED = "KIND_ALWAYS_ALLOWED",
  ALWAYS_DENIED = "KIND_ALWAYS_DENIED",
  CONDITIONAL = "KIND_CONDITIONAL",
}
```

### Field name mapper

The Cerbos query plan references fields using paths such as `request.resource.attr.title`. Use a mapper to translate those to the metadata field names in your ChromaDB collection. Map entries can be a field name or a configuration object:

```ts
type FieldNameMapperConfig = {
  field: string;
  required?: boolean;
  numericType?: "integer" | "float";
};
```

- Fields are optional by default. Chroma's `$ne` and `$nin` operators match missing metadata, unlike a missing Cerbos attribute, which produces an evaluation error and denies access. The adapter rejects those operators unless the field is declared `required: true`, instead of over-authorizing.
- Set `required: true` to assert that the metadata key is present on every record in the collection. That assertion is what permits `$ne` and `$nin` against the field; declaring it for a field that may be absent reintroduces the over-grant.
- Set `numericType: "float"` when a field is always stored as floating-point metadata and must be compared with fractional thresholds. Without that declaration, fractional ordered comparisons are rejected because Chroma distinguishes integer and floating-point metadata.

As a map:

```ts
const result = queryPlanToChromaDB({
  queryPlan,
  fieldNameMapper: {
    "request.resource.attr.aBool": "aBool",
    "request.resource.attr.aString": "title",
    "request.resource.attr.score": {
      field: "score",
      required: true,
      numericType: "float",
    },
  },
});
```

As a function, return either a field name or a configuration object:

```ts
const result = queryPlanToChromaDB({
  queryPlan,
  fieldNameMapper: (fieldName: string): string => {
    return fieldName.replace("request.resource.attr.", "");
  },
});
```

If a field is not found in the map, the original path is used as-is and treated as optional, so `$ne` and `$nin` against it are rejected. A mapper that returns a plain string is also treated as optional; return a configuration object with `required: true` to permit those operators.

## Usage example

```ts
import { GRPC as Cerbos } from "@cerbos/grpc";
import { Chroma } from "@langchain/community/vectorstores/chroma";
import { OpenAIEmbeddings } from "@langchain/openai";
import { queryPlanToChromaDB, PlanKind } from "@cerbos/langchain-chromadb";

const cerbos = new Cerbos("localhost:3592", { tls: false });

const queryPlan = await cerbos.planResources({
  principal: { id: "user1", roles: ["USER"] },
  resource: { kind: "document" },
  action: "view",
});

const result = queryPlanToChromaDB({
  queryPlan,
  fieldNameMapper: {
    "request.resource.attr.department": "department",
    "request.resource.attr.public": "public",
  },
});

if (result.kind === PlanKind.ALWAYS_DENIED) {
  return [];
}

const chroma = await Chroma.fromExistingCollection(new OpenAIEmbeddings(), {
  collectionName: "my_collection",
});

const filters =
  result.kind === PlanKind.CONDITIONAL ? result.filters : undefined;

const matches = await chroma.similaritySearch("query", 10, filters);
```

## Error handling

`queryPlanToChromaDB` throws descriptive errors when:

- The plan kind is not a valid `PlanKind` value.
- A conditional plan contains an operand that is not a `PlanExpression`.
- An operator in the plan is not supported by ChromaDB's filter syntax.
- A comparison operator is missing a variable or field name.
- A `not` expression wraps an operator that cannot be negated.
- A filter literal is null, nested, non-finite, or otherwise invalid for Chroma metadata.
- `$ne` or `$nin` targets a field that is not declared `required: true`.
- A fractional ordered comparison targets a field that is not configured with `numericType: "float"`.

## Example application

This repository carries a runnable [`example/`](example/), which installs the adapter from the
artifact `npm publish` would upload and exercises it against a live PDP and a real ChromaDB server
over the shared [demo domain](../demo/README.md):

```bash
# from the repository root
demo/scripts/run-example.sh langchain-chromadb
```

Unlike the test suites, it resolves the adapter through its **published** surface — the `exports`
map, `types`, the `files` allowlist, and the `@cerbos/core` peer range — and covers usage shapes
past a single flat query: a limit walked to the end of the result set through `collection.get`, and
the adapter's `Where` clause composed with an application-owned one under `$and`. It is also where
the empty clause an `ALWAYS_ALLOWED` plan returns meets Chroma's own validator, which rejects `{}`
— so a caller omits `where` rather than forwarding it.

## Testing

| Command | What it does | What it needs |
| --- | --- | --- |
| `npm test` | Proves **the `Where` filter this adapter emits.** The translator unit test: every corpus action, classified exactly once as a golden expectation or as a throw — plus the set of logical operators the emitted clauses ever use, and the golden asset's own invariants, neither of which needs a store | Nothing but Node — no Cerbos sidecar, no ChromaDB, no Docker |
| `npm run test:adversarial` | Proves **the documents that filter returns**, against a real ChromaDB collection with `check()` as the oracle | Cerbos CLI, Docker |
| `npm run golden:update` | Rewrites `golden/expectations.json` from what the translator emits today, preserving every `note`. Review the diff — CI never regenerates | Nothing but Node |

### The golden expectations

`npm test` reads its plans from `../conformance/wire-fixtures/` — the golden `PlanResources`
responses captured against the pinned Cerbos version — and asserts them against
`golden/expectations.json`, a **golden expectation** file this adapter owns. One entry per corpus
action, keyed by action name.

The value schema is the cheapest one in the repository, because a Chroma `Where` clause **is** a JSON
document: the entry holds the translator's whole `{ kind, filters? }` result verbatim, with no
rendering step and no dialect. Nothing is normalised on the way in — a literal JSON cannot carry (a
non-finite number, a negative zero) fails regeneration rather than being rewritten, because such a
literal is one the deployed adapter could not have put in a query body either.

```jsonc
{
  "adapter": "langchain-chromadb",
  "regenerate": "npm run golden:update",
  "expectations": {
    "in-empty": { "kind": "KIND_ALWAYS_DENIED" },
    "vf-le": {
      "note": "optional, human, preserved across regeneration",
      "kind": "KIND_CONDITIONAL",
      // Value-first: the plan reads `3 <= R.attr.aNumber`, and the emitted operator is mirrored.
      "filters": { "aNumber": { "$gte": 3 } }
    }
  }
}
```

An action this adapter refuses carries **no entry**: its pinned message is corpus data, in
`adapterctl.json`, and duplicating it here would be two places to change one string — which
on an adapter that refuses most catalog shapes would make the asset almost entirely
restatement. A wire fixture that is neither in this file nor declared unsupported fails the suite,
which is what makes a new corpus action land as a failure rather than as silence
([ADR 0006](../docs/adr/0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md),
[ADR 0007](../docs/adr/0007-adapters-share-data-not-code.md), and the "Golden expectations" section
of [conformance/README.md](../conformance/README.md)).

Alongside the pinned bytes the suite states the properties a regeneration must not silently accept,
as rules over every translated action rather than over chosen shapes: every field a filter names is a
metadata key the mapper declares, no `$not`/`$nor` survives into a filter Chroma would reject at
query time, an inequality is emitted only over a field declared `required` — and clearing `required`
moves exactly the actions that emit one — and an ordered comparison binds a fractional threshold only
where `numericType: "float"` is declared.

Whether those filters return the documents the PDP allows is a separate question, answered by the
adversarial suite, which does need a Cerbos sidecar and a ChromaDB container:

```bash
npm run chroma            # in one shell: the pinned ChromaDB image on port 8234
npm run test:adversarial  # in another
```
