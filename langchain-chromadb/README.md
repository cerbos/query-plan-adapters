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
`QueryPlanToChromaDBArgs`, `QueryPlanToChromaDBResult`, `PostFilter`, `FieldMapper`,
`FieldNameMapperConfig` and `UnsupportedOperatorError` are exported.

A call that passes `allowPostFilter: true` can also get a `postFilter` back, and then `filters` may
be absent; see [Post-filtering](#post-filtering).

## Field name mapper

`fieldNameMapper` maps plan paths (`request.resource.attr.title`) to metadata keys. Each entry is a
key name or a config object; a function mapper returns either.

```ts
interface FieldNameMapperConfig {
  field: string;
  required?: boolean;
  numericType?: "integer" | "float";
  valueType?: "boolean";
}
```

| Option | What it does |
| --- | --- |
| `field` | The metadata key. |
| `required: true` | Asserts the key is present on **every** record. Required to permit `$ne` and `$nin`: Chroma matches records missing the key, whereas Cerbos denies a missing attribute. Declaring it for a key that can be absent reintroduces that over-grant. |
| `numericType: "float"` | The key is always stored as floating-point metadata. Required for ordered comparisons against a fractional threshold, because Chroma distinguishes integer from float metadata. |
| `numericType: "integer"` | Every value stored under the key is an integer. An inequality then needs no `$ne` and no `required`: `x != 5` becomes `$or` of `$lt: 5` and `$gt: 5`, and `x != "5"` (a string or boolean literal, which no integer equals) becomes `$or` of `$lt: 0` and `$gte: 0`. A fractional literal (`x != 2.5`) still takes `$ne` and needs `required: true`, because Chroma compares a fractional threshold with integer metadata inexactly. |
| `valueType: "boolean"` | Every value stored under the key is a boolean. An inequality against a boolean then needs no `$ne` and no `required`: `x != true` and `!x` become `$eq: false`. Cannot be combined with `numericType`. |

Chroma's `$eq`, `$lt`, `$gt` and `$gte` never match a record missing the key, so these spellings
are sound over an optional key. They rely on the declaration holding for every record: a string
stored under a key declared boolean or integer is never matched by them. A string key has no such
spelling (Chroma's `$lt`/`$gt` reject a string operand), so its inequality still needs
`required: true`.

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
carries, so the filter silently selects nothing; map every attribute your policies reference. The
post-filter does not share that fallback: it refuses a reference the mapper has no entry for (a
function mapper declares one by returning a value).

## NULL attribute representation

Other adapters take a `nullAttributeRepresentation` option because `R.attr.x == null` produces the
same plan whether a NULL field is sent to `check()` as an explicit `null` or omitted. **This adapter
needs none**: Chroma metadata has no null (it rejects a null value or list element), so every null
comparison operand is rejected under either convention (every `null/*` corpus case that compares a
null literal throws, including `null/equals/null-literal-on-missing-attribute`). See
[#302](https://github.com/cerbos/query-plan-adapters/issues/302).

> [!WARNING]
> **Do not guard with `has()`: write `R.attr.x != null`.** The Cerbos planner folds `has(R.attr.x)`
> to true and drops it from the plan: alone it plans as `ALWAYS_ALLOWED`, and
> `has(R.attr.x) && R.attr.y > 0` plans as `R.attr.y > 0`. The filter then returns records missing `x`
> that `check()` denies, and the adapter, which only sees the plan, cannot restore the guard.
> `R.attr.x != null` stays in the plan, where the adapter translates it or refuses it.

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

Without `allowPostFilter`, the corpus pins both sides: `principal/exists/short-list` (3 elements)
translates and `principal/exists/long-list` (11) throws; `principal/all/short-list` and
`principal/all/long-list` both throw; `principal/in/long-list` (11) and `principal/in/short-list`
(3) both emit `in(key, [literals])` and pass, including records missing the key. With it, the
post-filter answers the lambda and the `ne` chain, so every one of them passes, but a long list is
then evaluated in memory rather than narrowing the search: `in` stays the better spelling.

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
Resulting `$ne`/`$nin` still need `required: true`, except where a `valueType: "boolean"` or
`numericType: "integer"` declaration spells the inequality without `$ne` (see the mapper table). Value-first comparisons (`3 <= R.attr.n`) are
mirrored.

A `Where` clause compares one metadata key with a literal, so these throw unless
[`allowPostFilter`](#post-filtering) is set, in which case the post-filter answers many of them:

- string helpers (`contains`, `startsWith`, `endsWith`): Chroma has no prefix, substring or pattern
  operator on a string value, and its `$contains` tests list membership, not a substring;
- null comparisons: Chroma metadata has no null, and rejects a null value or list element;
- collection operators (`hasIntersection`, `exists`, `exists_one`, `all`, `filter`, `map`, `lambda`,
  `size`), positional reads (`R.attr.list[0]`) and value-first membership in a list-valued key
  (`"x" in R.attr.list`). The pinned server stores a homogeneous list, but `$eq` on a list key and a
  dotted `key.N` both match nothing, there is no count function, and there is no quantifier over a
  list's elements. Its position-blind `$contains` is not translated: Chroma stores an empty list as
  an absent key and rejects null elements and mixed types, so a list cannot always be stored as the
  PDP sees it ([#475](https://github.com/cerbos/query-plan-adapters/issues/475));
- collections reached through a relation, and lists of objects: Chroma metadata has no relation or array-of-object model;
- field-to-field comparisons, arithmetic, casts, ternaries, hierarchy and timestamp operations: an
  operand must be a bare key or a literal, never a computed value or a second key.

## Post-filtering

Chroma's `Where` grammar compares one metadata key with a literal, so most of CEL has no filter form.
Setting `allowPostFilter: true` lets the adapter answer those parts itself: the result carries a
`postFilter`, a predicate over one record's metadata that evaluates the rest of the plan in memory.
It is off by default, and without it a plan that would need one throws `UnsupportedOperatorError`
exactly as before.

```ts
const result = queryPlanToChromaDB({ queryPlan, fieldNameMapper, allowPostFilter: true });

switch (result.kind) {
  case PlanKind.ALWAYS_DENIED:
    return [];
  case PlanKind.ALWAYS_ALLOWED:
    return collection.query({ queryTexts: [query], nResults: k });
  case PlanKind.CONDITIONAL: {
    // Over-fetch: the post-filter drops records after Chroma has already ranked and cut them.
    const rows = await collection.query({
      queryTexts: [query],
      nResults: result.postFilter ? k * 4 : k,
      where: result.filters, // absent when nothing in the plan has a `Where` form
      include: ["metadatas", "documents"],
    });
    const metadatas = rows.metadatas[0] ?? [];
    return rows.ids[0]!
      .map((id, i) => ({ id, metadata: metadatas[i] }))
      .filter(({ metadata }) => !result.postFilter || result.postFilter(metadata))
      .slice(0, k);
  }
}
```

- **`postFilter` is part of the authorization predicate.** A record is allowed only when it
  matches `filters` **and** `postFilter` returns `true`. Apply it to every candidate before the
  record is used, returned or passed to a model, and ask Chroma for `metadatas` so it has something
  to read.
- **A top-k search can return fewer than k results.** Chroma ranks and truncates before the
  post-filter runs, so records it drops are not replaced. Over-fetch (a larger `nResults` or
  `limit`) and cut back to k afterwards, or page with `collection.get` until you have enough.
- **Whatever Chroma can express stays in `filters`.** A plan Chroma can express whole returns
  `filters` alone, exactly as without the option. A root `and` keeps its expressible conjuncts in
  `filters` and post-filters the rest, so the vector search still narrows the candidates. Anything
  else, including an `or` with any inexpressible child, goes to `postFilter` whole, with no
  `filters`: pushing half of a disjunction would drop the records only the other half admits.
- **Post-filtered conditions do not narrow the search.** They cost a read of every candidate
  Chroma returns.

The predicate evaluates CEL, not an approximation of it. A key the record does not carry is a
missing-attribute error; an error propagates through `!`, and through `&&` and `||` unless another
operand decides them; and a result that is not `true` denies. Equality is CEL's heterogeneous
equality (`1 == 1.0`, `"1" != 1`), ordering across types is an error, and strings compare by code
point. It evaluates arithmetic, string helpers (`contains`, `startsWith`, `endsWith`, `+`), casts
(`int`, `double`, `string`, `timestamp`), field-to-field comparisons, ternaries, `size`,
hierarchies, and the collection macros over a literal list (a principal attribute's list, which the
planner inlines). `matches` is answered for a literal pattern with optional anchors and a trailing
`.*` only.

It reads only the metadata the mapper declares, and only scalars: a key holding a list or anything
else that is not a string, finite number or boolean reads as missing, which denies. Everything it
cannot evaluate exactly is still refused with `UnsupportedOperatorError`, at translation, never from
the predicate:

- a reference the mapper has no entry for: an undeclared attribute is one the record may not
  store (a list, a relation), and reading it as missing would deny records the PDP allows;
- a null literal: Chroma metadata has no null, so a record cannot tell an explicit null attribute
  from a missing one;
- arithmetic between a stored number and an integral literal, or between two integral literals.
  A plan carries `1` whether the policy wrote `1` or `1.0`, and CEL arithmetic has no int/double
  overload: `R.attr.x + 1` is an error on every record, so `!(R.attr.x + 1 > 2)` denies every
  record, while `R.attr.x + 1.0` is not. Write a fractional literal, or cast the attribute;
- a divisor that reads metadata, and `string()` over a stored number (a key not declared
  `valueType: "boolean"`): the chromadb client stores metadata through JSON, which writes -0.0 as
  0, so the sign of a stored zero is lost, and it decides both the infinity a division by zero
  gives and `string()`'s `"-0"`;
- any other `matches` pattern, `filter` or `map` used as a condition, and the operators it has no
  evaluation for (`except`, list and map literals built by the plan).

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
- `$ne`/`$nin` targets a field not declared `required: true`, and no type declaration spells it
  another way;
- a fractional ordered comparison targets a field without `numericType: "float"`;
- under `allowPostFilter`, the post-filter cannot evaluate the rest exactly
  ([Post-filtering](#post-filtering) lists why).

A malformed plan or mapper misconfiguration is a plain `Error`, so a fallback keyed on
`UnsupportedOperatorError` does not swallow it: an invalid plan kind, a non-`PlanExpression`
operand, wrong operand counts on `and`/`or`/`not`/comparisons, or a mapper resolving to an empty
field name.

## Conformance contract

The adapter is replayed against the shared [conformance corpus](../conformance/README.md): the plans
and `check()` decisions recorded from Cerbos PDP 0.55.0 (and 0.54.0), executed as real ChromaDB
metadata queries over the corpus's 41 seed records. The harness translates every case with
`allowPostFilter: true` and applies the `postFilter` to every record the `where` returns, as a
caller must. Passed cases on the current PDP, 0.55.0, out of every golden case in the tier:

| Tier | Passed / total |
| --- | --- |
| core | 20 / 26 |
| extended | 39 / 80 |
| adversarial | 134 / 286 |

Without `allowPostFilter`, the 129 cases the post-filter answers throw `UnsupportedOperatorError`
instead, as they did before the option existed (`src/translator.test.ts` pins that), leaving 64
passing: 18, 10 and 36 in the three tiers.

Every case that does not pass is refused with `UnsupportedOperatorError`; none returns wrong
records. [`conformance-ledger.json`](conformance-ledger.json) lists each one with its reason.
Planner-divergence cases are skipped, and count in the total but never as passed. On 0.55.0 that is
four extended cases and three adversarial cases. `null/has/missing-attribute` and
`null/has/composed-with-comparison`: the planner drops `has()` from the plan while `checkResource`
denies the missing-attribute documents, so use `R.attr.x != null` for database-backed attributes
instead of `has(R.attr.x)`. `arithmetic/add/int-literal-plus-constant` and `arithmetic/add/int-literal-negated`: the planner drops the int type of the literal in `R.attr.x + 1`, so the plan is the double spelling's, while `check()` has no double + int overload and denies every row; write `1.0`. Three `composition/*`
cases whose DENY condition reads a missing attribute: the plan negates the deny condition with the
same `not` as CEL's `!`, while `checkResource` treats the erroring deny rule as not matching
([#530](https://github.com/cerbos/query-plan-adapters/issues/530)).

The harness mapping declares no metadata key but the id `required: true`: every other scalar the
corpus reads through a filter is missing on some seed. It declares the boolean keys
`valueType: "boolean"` and the integer keys `numericType: "integer"`, so their inequalities are
spelled without `$ne` and proved by the corpus (`logic/not/bare-boolean-attribute`,
`type-mismatch/not-equals/number-field-against-string-literal`). An inequality over a string key has
no `Where` form, so the post-filter answers it. The corpus therefore proves no `$ne`/`$nin` filter;
`src/translator.test.ts` pins that `required` gates them, and that the type declarations are what
spell the others.

The harness stores every scalar the dataset has, `createdBy` and `scope` included, and maps each. It
stores no list (#475), and not `owner` or `coOwner`, which reach `check()` as an explicit null on
some rows, a value Chroma metadata cannot hold; the mapping declares none of them, so every case
reading one is refused.

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
| Absent to-one parent | **Handled** for a scalar reached through the parent, **rejected** for a collection — `relation/contains/one-hop`, `relation/and/negated-conjunction-short-circuits-absent-parent` and the other scalar hops pass; `relation/all/to-one-chain`, `relation/exists/negated-to-one-chain` and the other chained collections are `unsupported` in the ledger and throw | A scalar hop is stored flattened onto a dotted key (`parent.aString`) that an absent parent leaves out, which the post-filter reads as CEL's missing-attribute error. The dotted key must be written exactly when the parent exists. Chroma metadata has no relation or nested-object model, so a collection behind a relation has nowhere to resolve and the plan is refused ([#309](https://github.com/cerbos/query-plan-adapters/issues/309)) |

## Behaviour changes

- **Widening, opt-in:** `allowPostFilter: true` answers in memory, with CEL's semantics, the parts
  of a plan Chroma's `Where` cannot express, and returns them as a `postFilter`
  ([#228](https://github.com/cerbos/query-plan-adapters/issues/228)). See
  [Post-filtering](#post-filtering). Without the option nothing changes: the same plans throw
  `UnsupportedOperatorError`, and `QueryPlanToChromaDBResult`'s default type argument keeps
  `filters` a `Where` on every conditional result.

- **Widening:** an inequality over a key declared `valueType: "boolean"` (new) or
  `numericType: "integer"` is spelled without `$ne`, so it no longer needs `required: true`
  ([#531](https://github.com/cerbos/query-plan-adapters/issues/531)). Over such a key already
  declared `required`, the emitted filter changes shape (`$eq: false` for `$ne: true`; `$or` of
  `$lt`/`$gt` for `$ne: 5`) but selects the same records as long as the declaration holds.

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
| `npm test` | Offline unit suite: the refusal type, the rules every emitted filter obeys (each field is a mapped key, no `$not`/`$nor`, `$ne`/`$nin` only on `required` fields and never on a boolean or integer key, fractional thresholds only on `numericType: "float"` fields), the mapper contract no policy can reach (function mappers, `required`, `numericType`, `valueType`, the unmapped fallback), what `allowPostFilter` changes and what it leaves alone, and malformed input | Node only |
| `npm run typecheck` | Type-checks `src/` and the tests | Node only |
| `npm run chroma` | Starts the pinned ChromaDB ([`CHROMA_IMAGE`](CHROMA_IMAGE)) on port 8234 | Docker |
| `npm run test:adversarial` | Replays the recorded conformance goldens (`../conformance/golden/`) against the ChromaDB on `CHROMA_URL` (default `http://127.0.0.1:8234`); no PDP | A running ChromaDB |
