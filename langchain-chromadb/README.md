# Cerbos + LangChain.js ChromaDB Adapter

Converts a [Cerbos](https://cerbos.dev) query plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan))
into a [ChromaDB](https://www.trychroma.com/) `Where` metadata filter, for a Chroma collection or the
LangChain.js Chroma vector store.

## Install

```bash
npm install @cerbos/langchain-chromadb @cerbos/core
npm install @cerbos/grpc   # or @cerbos/http — whichever client your deployment uses
```

- `@cerbos/core` (`^0.32.0 || ^0.33.0`) is a peer dependency. Install it yourself so your Cerbos
  client and the adapter share one copy of the query plan types (npm 7+ adds it automatically;
  pnpm and Yarn need it declared).
- The adapter depends on no Cerbos client. It depends on the `chromadb` 3.x JS client for its
  `Where` type.
- Requirements: Node.js >= 22, Cerbos > v0.16.

## Quick start

```ts
import { GRPC as Cerbos } from "@cerbos/grpc";
import { Chroma } from "@langchain/community/vectorstores/chroma";
import { OpenAIEmbeddings } from "@langchain/openai";
import { queryPlanToChromaDB, PlanKind } from "@cerbos/langchain-chromadb";

const cerbos = new Cerbos("localhost:3593", { tls: false });
const chroma = await Chroma.fromExistingCollection(new OpenAIEmbeddings(), {
  collectionName: "my_collection",
});

async function search(principalId: string, query: string) {
  const queryPlan = await cerbos.planResources({
    principal: { id: principalId, roles: ["USER"] },
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

  switch (result.kind) {
    case PlanKind.ALWAYS_DENIED:
      return [];
    case PlanKind.ALWAYS_ALLOWED:
      return chroma.similaritySearch(query, 10); // omit the filter: Chroma rejects `{}`
    case PlanKind.CONDITIONAL:
      return chroma.similaritySearch(query, 10, result.filters);
  }
}
```

`result` is a discriminated union: `CONDITIONAL` always carries `filters` (a `Where`),
`ALWAYS_ALLOWED` carries `filters: {}`, `ALWAYS_DENIED` carries none. **Do not pass the empty `{}`
to Chroma** — its validator rejects it ("Expected 'where' to have exactly one operator, but got 0");
omit `where` instead, and do not wrap `{}` in an `$and` with your own clause either. `PlanKind`,
`QueryPlanToChromaDBArgs`, `QueryPlanToChromaDBResult`, `FieldMapper`, `FieldNameMapperConfig` and
`UnsupportedOperatorError` are exported.

## Field name mapper

`fieldNameMapper` maps plan paths (`request.resource.attr.title`) to metadata keys. Each entry is a
key name or a config object; a function mapper returns either.

```ts
interface FieldNameMapperConfig {
  field: string;
  required?: boolean;
  numericType?: "integer" | "float";
}
```

| Option | What it does |
| --- | --- |
| `field` | The metadata key. |
| `required: true` | Asserts the key is present on **every** record. Required to permit `$ne` and `$nin`: Chroma matches records missing the key, whereas Cerbos denies a missing attribute. Declaring it for a key that can be absent reintroduces that over-grant. |
| `numericType: "float"` | The key is always stored as floating-point metadata. Required for ordered comparisons against a fractional threshold, because Chroma distinguishes integer from float metadata. |

```ts
queryPlanToChromaDB({
  queryPlan,
  fieldNameMapper: {
    "request.resource.attr.aBool": "aBool",
    "request.resource.attr.aString": "title",
    "request.resource.attr.score": { field: "score", required: true, numericType: "float" },
  },
});

queryPlanToChromaDB({
  queryPlan,
  fieldNameMapper: (path) => path.replace("request.resource.attr.", ""),
});
```

A plain string entry, a string from a function, and an unmapped path are all treated as optional
(no `required`). An unmapped path is used as-is as the metadata key — which no record normally
carries, so the filter silently selects nothing; map every attribute your policies reference.

## NULL attribute representation

Other adapters take a `nullAttributeRepresentation` option because `R.attr.x == null` produces the
same plan whether a NULL field is sent to `check()` as an explicit `null` or omitted. **This adapter
needs none**: Chroma metadata holds only finite numbers, strings and booleans, so every null
comparison operand is rejected under either convention (`null-eq`, `null-ne`, `vf-null-ne`,
`null-not-eq`, the `in-null-elem-*` family and `null-eq-missing` all throw). See
[#302](https://github.com/cerbos/query-plan-adapters/issues/302).

## Write membership as `in`, not as a collection macro

```yaml
# Fragile — translates for some principals and throws for others
expr: P.attr.teams.exists(t, R.attr.team == t)

# Direct membership — a non-empty list maps to one $in
expr: R.attr.team in P.attr.teams
```

The planner unrolls a macro over a principal attribute into an `or` chain of equalities (which
translates) only up to 10 elements (cerbos/cerbos#2570, cerbos/cerbos#2817). Above that it ships a
lambda over a literal list, which has no Chroma form and throws — so the same policy works for a
principal with 10 teams and fails for one with 11. `all` is worse: its unrolled `ne` chain needs
`required: true` on the field. The `in` spelling has no threshold, needs no `required`, and an empty
list folds to `ALWAYS_DENIED`.

The corpus pins both sides: `pv-exists-unrolled` (3 elements) translates and `pv-exists` (11)
throws; `pv-all-unrolled` and `pv-all` both throw; `pv-in` (11) and `pv-in-unrolled` (3) both emit
`in(key, [literals])` and are oracle-tested, including records missing the key.

## Supported operators

| Category | Cerbos operators | ChromaDB output |
| --- | --- | --- |
| Logical | `and`, `or` | `$and`, `$or` |
| Negation | `not` | Operator inversion and De Morgan's law (below) |
| Comparisons | `eq`, `ne`, `lt`, `le`, `gt`, `ge` | `$eq`, `$ne`, `$lt`, `$lte`, `$gt`, `$gte` |
| Membership | `in` | `$in` |

Chroma has no `$not` or `$nor`, so `not` is pushed inward: `not(eq)` → `$ne`, `not(ne)` → `$eq`,
`not(lt)` → `$gte`, `not(gt)` → `$lte`, `not(le)` → `$gt`, `not(ge)` → `$lt`, `not(in)` → `$nin`,
`not(and(A, B))` → `$or[not A, not B]`, `not(or(A, B))` → `$and[not A, not B]`, `not(not(X))` → `X`.
Resulting `$ne`/`$nin` still need `required: true`. Value-first comparisons (`3 <= R.attr.n`) are
mirrored.

Metadata is flat scalars, so these throw: string helpers (`contains`, `startsWith`, `endsWith`),
null comparisons, collection operators (`hasIntersection`, `exists`, `exists_one`, `all`, `filter`,
`map`, `lambda`, `size`), field-to-field comparisons, arithmetic, casts, ternaries, hierarchy and
timestamp operations, and relation chains.

## Error handling

The adapter fails closed. When a well-formed plan asks for something Chroma cannot express, it
throws `UnsupportedOperatorError`, whose `operator` names the plan operator concerned:

```ts
import { queryPlanToChromaDB, UnsupportedOperatorError } from "@cerbos/langchain-chromadb";

try {
  const result = queryPlanToChromaDB({ queryPlan, fieldNameMapper });
  // ...
} catch (error) {
  if (error instanceof UnsupportedOperatorError) {
    // error.operator: e.g. "contains", "size", "ne". Deny, fall back to a broader search, etc.
  }
  throw error;
}
```

`UnsupportedOperatorError` is thrown when:

- an operator has no Chroma filter form;
- a comparison operand is a computed expression, not a bare metadata key or literal (`operator` is
  that expression's, e.g. `add`, `size`; for a collection macro, the macro itself);
- a comparison is between two keys or two literals, or tests a literal contained in a field;
- `not` wraps an operator that cannot be negated;
- a literal is null, nested, non-finite or otherwise invalid metadata;
- `$ne`/`$nin` targets a field not declared `required: true`;
- a fractional ordered comparison targets a field without `numericType: "float"`.

A malformed plan or mapper misconfiguration is a plain `Error`, so a fallback keyed on
`UnsupportedOperatorError` does not swallow it: an invalid plan kind, a non-`PlanExpression`
operand, wrong operand counts on `and`/`or`/`not`/comparisons, or a mapper resolving to an empty
field name. Messages match those pinned in `conformance/actions.json`.

## Conformance contract

Select the PDP engine mode with `ADAPTER_TEST_STRICT_EVALUATION=false` (default) or `=true`; other
values are rejected. For example, `ADAPTER_TEST_STRICT_EVALUATION=true npm run test:adversarial`
enables strict evaluation for both planning and the `check()` oracle. CI runs both modes for each
adversarial store and client-version combination.

The adapter is differentially tested against Cerbos PDP 0.55.0 `checkResource` decisions in both evaluation modes using 29 hostile seed documents and real ChromaDB metadata queries. The Spring Data adapter defines the reference semantics for this compatibility snapshot.

| Classification | Coverage |
| --- | --- |
| Oracle-tested | 62 reference actions: directional and inequality comparisons, single/empty membership, Unicode and empty strings, negative numbers, n-ary/double/triple negation, membership on an optional resource field, mapped nested-field equality, case-sensitive equality, the primary key against a literal, and the root-position and bare-operand forms (bare `>`/`<=` on a metadata key, either ordering under a negation, a bare boolean key as the whole condition, a disjunction of two scalar predicates); plus the De Morgan branch over a conjunction, a value-first ordering against a metadata key, the below-cliff unroll of a principal collection, membership in a map literal (folded by the planner to its key list), a double literal beyond int64 on a double field, and a literal of the wrong type against a scalar key (`aNumber == "5"`, `aString == 0`, `aBool == "true"`, `aNumber != "5"`, `aNumber in ["5", 2]`), which Chroma answers as CEL does because its comparisons are type-exact |
| Fail-closed | 255 reference conformance actions — among them positional access into the number and boolean lists, which has no `Where` form, and membership (`x in list`, `null in list`, `hasIntersection`) in those lists, whose empty lists and null elements the pinned Chroma stack refuses to store — plus regex, ordered indexing/`get-field`, timestamp, cast and non-boolean-macro probes (266 actions total) |
| Representation-independent | `null-eq-missing` — rejected like every other null comparison operand, so no `nullAttributeRepresentation` option is required |
| Attribute NULL convention | Also representation-independent: Chroma metadata has no null value, so a NULL column is stored as an absent key and `$ne`/`$nin` match absent records. All five `null-value-*` probes for the explicit convention (cerbos/query-plan-adapters#308) are refused |
| Known planner divergence | `has()` on a missing attribute is folded by the Cerbos planner to `ALWAYS_ALLOWED`, while `checkResource` denies the missing-attribute documents. Until the planner is fixed, use `R.attr.x != null` for database-backed attributes instead of `has(R.attr.x)` |

Every fail-closed shape's error message is pinned in `conformance/actions.json` and asserted here.
The translator unit test also pins **where** each of the 267 refusals (the 266 fail-closed actions
plus `null-eq-missing`) is raised across the adapter's nine rejection sites; `binaryOperands`
refusing a computed operand accounts for 160 of them, since arithmetic, casts, ternaries,
projections and above-cap macros all reach the wire as an operand that is neither a key nor a
literal.

## Mapping hazards

The contract above proves the plan side. The other half is the mapping: **the documents the filter
reads must be the documents the application put into the resource attributes.** The shared corpus
catalogues six ways that can break. This adapter **builds no subquery**: a `where` clause compares
flat metadata on the record being matched, and every shape that would reach a second record throws.

| Hazard | Position | Mechanism to check |
|---|---|---|
| Filtered association | Not applicable — no subquery | — |
| Default scope on the target model | Not applicable — no second collection is read | — |
| Subtype discrimination | **Caller-owned** | The Chroma collection you pass the `where` clause to. The adapter never sees the collection, so it cannot check that it is the one whose metadata became the resource attributes. If one collection mixes document kinds, add the discriminating metadata key to the `where` yourself |
| To-one relation used as a collection | Not applicable — a metadata key holds exactly what the application stored | — |
| Composite association key | Not applicable — no join, so no key to compose | — |
| Absent to-one parent | **Rejected** — `w1-all-chain`, `w1-not-exists-chain` and the eight other chained shapes are in `adapterUnsupported` and throw | None — Chroma metadata is flat, so a chain has nowhere to resolve and the plan is refused ([#309](https://github.com/cerbos/query-plan-adapters/issues/309)) |

## Behaviour changes

- **Widening:** a membership list mixing scalar types (`R.attr.x in ["5", 2]`) is split into one
  `$in` per type under an `$or` (`$nin` per type under an `$and` when negated), because Chroma
  rejects a mixed `$in`/`$nin` list. It used to be refused with `in requires a list whose values
  have one scalar type`. The split is exact: Chroma's comparisons are type-exact, so `"5"` never
  matches a stored 5.
- **Breaking (types, 0.2.0)** — `QueryPlanToChromaDBResult` is a discriminated union; code that
  constructs result objects must supply the payload for its kind. Runtime output is unchanged.
- Refusals of a well-formed plan throw `UnsupportedOperatorError` (with `operator`) instead of a
  plain `Error`. Messages are unchanged, but `String(error)` and stack traces now print
  `UnsupportedOperatorError` ([#228](https://github.com/cerbos/query-plan-adapters/issues/228)).

## Example application

[`example/`](example/) installs the packed adapter and runs it against a live PDP and a real ChromaDB
server over the shared [demo domain](../demo/README.md), including pagination via `collection.get`
and composition with an application-owned `where` under `$and`:

```bash
# from the repository root
demo/scripts/run-example.sh langchain-chromadb
```

## Development

| Command | What it does | Needs |
| --- | --- | --- |
| `npm test` | Translator unit test: every corpus action classified once as a golden expectation or a pinned throw, plus the refusal sites and the mapper contract no policy can reach (function mappers, `required`, `numericType`, the unmapped fallback, malformed input) | Node only |
| `npm run typecheck` | Type-checks `src/` and the tests | Node only |
| `npm run golden:update` | Rewrites `golden/expectations.json` from what the translator emits, preserving each `note`. Review the diff; CI never regenerates | Node only |
| `npm run test:adversarial` | Starts the pinned ChromaDB ([`CHROMA_IMAGE`](CHROMA_IMAGE)) on port 8234 and runs the shared corpus against it, with `check()` as the oracle | Cerbos CLI, Docker |

`npm test` reads its plans from `../conformance/wire-fixtures/` and asserts them against
`golden/expectations.json`. Because a `Where` clause is JSON, each entry is the translator's
`{ kind, filters? }` result verbatim, keyed by action name; a literal JSON cannot carry fails
regeneration. A refused action has no entry (its message lives in `conformance/actions.json`) —
that is 267 of the corpus's 330 shapes. A wire fixture in neither place fails the suite. The suite
also asserts, across every translated action, that each field is a mapped key, no `$not`/`$nor`
is emitted, inequalities appear only on `required` fields, and fractional thresholds only on
`numericType: "float"` fields. See "Golden expectations" in
[conformance/README.md](../conformance/README.md),
[ADR 0006](../docs/adr/0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md) and
[ADR 0007](../docs/adr/0007-adapters-share-data-not-code.md).

```jsonc
{
  "adapter": "langchain-chromadb",
  "regenerate": "npm run golden:update",
  "expectations": {
    "in-empty": { "kind": "KIND_ALWAYS_DENIED" },
    "vf-le": {
      "note": "optional, human, preserved across regeneration",
      "kind": "KIND_CONDITIONAL",
      "filters": { "aNumber": { "$gte": 3 } }
    }
  }
}
```
