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
comparison operand is rejected under either convention (every `null/*` corpus case that compares a
null literal throws, including `null/equals/null-literal-on-missing-attribute`). See
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

The corpus pins both sides: `principal/exists/short-list` (3 elements) translates and
`principal/exists/long-list` (11) throws; `principal/all/short-list` and `principal/all/long-list`
both throw; `principal/in/long-list` (11) and `principal/in/short-list` (3) both emit
`in(key, [literals])` and pass, including records missing the key.

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
field name.

## Conformance contract

The adapter is replayed against the shared [conformance corpus](../conformance/README.md): the plans
and `check()` decisions recorded from Cerbos PDP 0.55.0 (and 0.54.0), executed as real ChromaDB
metadata queries over the corpus's 29 seed records. Passed cases on the current PDP, 0.55.0, out of
every golden case in the tier:

| Tier | Passed / total |
| --- | --- |
| core | 19 / 26 |
| extended | 13 / 80 |
| adversarial | 30 / 227 |

Every case that does not pass is refused with `UnsupportedOperatorError`; none returns wrong
records. [`conformance-ledger.json`](conformance-ledger.json) lists each one with its reason.
Planner-divergence cases are skipped, and count in the total but never as passed. On 0.55.0 that is
one extended case, `null/has/missing-attribute`: the planner folds `has()` on a missing attribute to
`ALWAYS_ALLOWED` while `checkResource` denies the missing-attribute documents, so use
`R.attr.x != null` for database-backed attributes instead of `has(R.attr.x)`.

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
| Absent to-one parent | **Rejected** — `relation/all/to-one-chain`, `relation/exists/negated-to-one-chain` and the other chained shapes are `unsupported` in the ledger and throw | None — Chroma metadata is flat, so a chain has nowhere to resolve and the plan is refused ([#309](https://github.com/cerbos/query-plan-adapters/issues/309)) |

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
| `npm test` | Offline unit suite: the refusal type, the rules every emitted filter obeys (each field is a mapped key, no `$not`/`$nor`, inequalities only on `required` fields, fractional thresholds only on `numericType: "float"` fields), the mapper contract no policy can reach (function mappers, `required`, `numericType`, the unmapped fallback) and malformed input | Node only |
| `npm run typecheck` | Type-checks `src/` and the tests | Node only |
| `npm run chroma` | Starts the pinned ChromaDB ([`CHROMA_IMAGE`](CHROMA_IMAGE)) on port 8234 | Docker |
| `npm run test:adversarial` | Replays the recorded conformance goldens (`../conformance/golden/`) against the ChromaDB on `CHROMA_URL` (default `http://127.0.0.1:8234`); no PDP | A running ChromaDB |
