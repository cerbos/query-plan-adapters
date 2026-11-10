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
      return chroma.similaritySearch(query, 10);
    case PlanKind.CONDITIONAL:
      return chroma.similaritySearch(query, 10, result.filters);
  }
}
```

`result` is a discriminated union on `kind`: `CONDITIONAL` carries `filters` (a `Where`), and
`ALWAYS_ALLOWED` and `ALWAYS_DENIED` carry nothing else. TypeScript will not let you read `filters`
until you have narrowed on `kind`, so a denial cannot reach Chroma as an absent `where`, which
Chroma reads as "every record". `PlanKind`,
`QueryPlanToChromaDBArgs`, `QueryPlanToChromaDBResult`, `PostFilter`, `FieldMapper`,
`FieldNameMapperConfig` and `UnsupportedOperatorError` are exported.

A call that passes `allowPostFilter: true` can also get a conditional result with a `postFilter`
and no `filters`; see [Post-filtering](#post-filtering).

## Field name mapper

`fieldNameMapper` maps plan paths (`request.resource.attr.title`) to metadata keys. Each entry is a
key name or a config object; a function mapper returns either.

```ts
interface FieldNameMapperConfig {
  field: string;
  required?: boolean;
  numericType?: "integer" | "float";
  valueType?: "boolean" | "json";
  listElements?: { field: string; type: "string" | "integer" };
}
```

| Option | What it does |
| --- | --- |
| `field` | The metadata key. |
| `required: true` | Asserts the key is present on **every** record. Required to permit `$ne` and `$nin`: Chroma matches records missing the key, whereas Cerbos denies a missing attribute. Declaring it for a key that can be absent reintroduces that over-grant. |
| `numericType: "float"` | The key is always stored as floating-point metadata. Required for ordered comparisons against a fractional threshold, because Chroma distinguishes integer from float metadata. |
| `numericType: "integer"` | Every value stored under the key is an integer. An inequality then needs no `$ne` and no `required`: `x != 5` becomes `$or` of `$lt: 5` and `$gt: 5`, and `x != "5"` (a string or boolean literal, which no integer equals) becomes `$or` of `$lt: 0` and `$gte: 0`. A fractional literal (`x != 2.5`) still takes `$ne` and needs `required: true`, because Chroma compares a fractional threshold with integer metadata inexactly. |
| `valueType: "boolean"` | Every value stored under the key is a boolean. An inequality against a boolean then needs no `$ne` and no `required`: `x != true` and `!x` become `$eq: false`. Cannot be combined with `numericType`. |
| `valueType: "json"` | The key holds the attribute as JSON text (`JSON.stringify(value)`): a list, a map, a null, or a scalar, exactly as the application sends it to the PDP. Chroma metadata cannot hold a list with a null element or an empty list, a map, or a null, and this is how such an attribute reaches the [post-filter](#post-filtering), which parses it. No `Where` clause reads it: a comparison against it is refused, or answered by the post-filter under `allowPostFilter`. A reference below the attribute (`R.attr.doc.tags`) resolves through the entry for `R.attr.doc`. Cannot be combined with `numericType`. |
| `listElements` | With `valueType: "json"` only, for a list of strings or integers: a second key holding the list's non-null elements as native Chroma list metadata, so membership narrows the search. See [List membership](#list-membership). |

A key declared `"json"` must hold what the PDP saw, as text: a key that holds the JSON of a list
but is declared as a plain key reads as a string, so `size(R.attr.list)` would count its characters.
Declare the encoding wherever it is used.

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

## List membership

Chroma stores a list of strings or integers as metadata and tests membership with `$contains`, but
it cannot hold the list the PDP sees: it rejects an empty list, a null element and mixed types, and
has no positional read. So the list stays under its `valueType: "json"` key, which the post-filter
reads, and `listElements` names a second key the application writes beside it:

- the list's non-null elements, in any order, all of the declared `type`;
- **no key at all** when the list has no non-null element (empty, all null), or is null or missing.

```ts
"request.resource.attr.teams": {
  field: "teams",
  valueType: "json",
  required: true,
  listElements: { field: "teams.elements", type: "string" },
},
```

```ts
metadata["teams"] = JSON.stringify(teams);
const elements = (teams ?? []).filter((team) => team !== null);
if (elements.length > 0) metadata["teams.elements"] = elements;
```

With that convention, `"x" in R.attr.teams` becomes `{ "teams.elements": { $contains: "x" } }` and
`hasIntersection(R.attr.teams, ["x", "y"])` an `$or` of `$contains`, with or without
`allowPostFilter`. An absent elements key holds no non-null needle, so the positive forms are exact
over an optional attribute. The negated forms become `$not_contains`, which also matches a record
missing the key: it stands for an empty list only when the attribute is a list on every record, so
negated membership is emitted only when the entry declares `required: true`, and refused (or
post-filtered) otherwise. A null needle, a needle of another type than `type` (a fractional or
unsafe integer against `"integer"`), an empty literal list, a list of objects, a reference below
the attribute, `size()` and positional reads keep their refusals.

## NULL attribute representation

Other adapters take a `nullAttributeRepresentation` option because `R.attr.x == null` produces the
same plan whether a NULL field is sent to `check()` as an explicit `null` or omitted. This adapter
reads the convention off the mapping instead. A scalar metadata key cannot hold a null (Chroma
rejects a null value), so an attribute stored under one is either present or **missing**: the
post-filter reads an absent key as CEL's missing-attribute error, which denies under both
polarities, exactly as `check()` does for an attribute the request omits. An attribute the
application sends as an **explicit null** must be stored under a key declared
`valueType: "json"`, as the text `null`: the post-filter then compares it as CEL does (`null != "x"`
is true, `has()` is true). The corpus pins both: `null/equals/null-literal-on-missing-attribute`
reads a scalar key and denies, and the `null/*` cases over `owner` read a JSON key. No `Where`
clause compares a null, so every null comparison is answered by the post-filter and refused without
`allowPostFilter` ([#302](https://github.com/cerbos/query-plan-adapters/issues/302)).

> [!WARNING]
> **On Cerbos 0.56.0 and earlier, do not guard with `has()`: write `R.attr.x != null`.** An
> attribute the plan request omits is unknown to those planners, which assume the data layer
> supplies it, so they fold `has(R.attr.x)` to true: alone it plans as `ALWAYS_ALLOWED`, and
> `has(R.attr.x) && R.attr.y > 0` plans as `R.attr.y > 0`, which never excludes a record missing
> `x`. From 0.57.0 the planner keeps `has(R.attr.x)` in the plan, and the post-filter checks whether
> the record's metadata carries the key, or refuses an attribute the mapping does not declare. The
> planner still drops a `has(R.attr.x)` that only guards an access to `x`, as in `has(R.attr.x) &&
> R.attr.x == "a"`, because that access already denies a record without `x`.

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
[`allowPostFilter`](#post-filtering) is set, in which case the post-filter answers them:

- string helpers (`contains`, `startsWith`, `endsWith`) and `matches`: Chroma has no prefix,
  substring or pattern operator on a string value, and its `$contains` tests list membership, not
  a substring;
- null comparisons: Chroma metadata has no null (see above);
- collection operators (`hasIntersection`, `exists`, `exists_one`, `all`, `filter`, `map`,
  `except`, `intersect`, `isSubset`, `size`), positional reads (`R.attr.list[0]`), value-first
  membership in a list-valued key (`"x" in R.attr.list`) and whole-list equality. The pinned
  server stores a homogeneous list, but `$eq` on a list key and a dotted `key.N` both match
  nothing, there is no count function, and there is no quantifier over a list's elements. Its
  `$contains` answers membership against a literal, and `hasIntersection` with a literal list,
  only over a key declaring [`listElements`](#list-membership)
  ([#475](https://github.com/cerbos/query-plan-adapters/issues/475)). The post-filter reads a list,
  a map or a list of objects from a key declared `valueType: "json"`;
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
    if ("filters" in result) {
      return collection.query({ queryTexts: [query], nResults: k, where: result.filters });
    }
    const { prefilter, postFilter } = result;
    // Over-fetch: the post-filter drops records after Chroma has already ranked and cut them.
    const rows = await collection.query({
      queryTexts: [query],
      nResults: k * 4,
      where: prefilter, // absent when nothing in the plan has a `Where` form
      include: ["metadatas", "documents"],
    });
    const metadatas = rows.metadatas[0] ?? [];
    return rows.ids[0]!
      .map((id, i) => ({ id, metadata: metadatas[i] }))
      .filter(({ metadata }) => postFilter(metadata))
      .slice(0, k);
  }
}
```

- **`postFilter` is the authorization predicate.** It evaluates the whole condition, so a record is
  allowed exactly when `postFilter` returns `true`. Apply it to every candidate before the record is
  used, returned or passed to a model, and ask Chroma for `metadatas` so it has something to read.
  `prefilter` only narrows the candidates: it is never enough on its own, which is why it is not
  called `filters`.
- **A top-k search can return fewer than k results.** Chroma ranks and truncates before the
  post-filter runs, so records it drops are not replaced. Over-fetch (a larger `nResults` or
  `limit`) and cut back to k afterwards, or page with `collection.get` until you have enough.
- **Whatever Chroma can express still reaches Chroma.** A plan Chroma can express whole returns
  `filters` alone, exactly as without the option. A root `and` hands its expressible conjuncts to
  Chroma as `prefilter`, so the vector search still narrows the candidates. Anything else, including
  an `or` with any inexpressible child, carries `postFilter` alone: pushing half of a disjunction
  would drop the records only the other half admits.
- **Post-filtered conditions do not narrow the search.** They cost a read of every candidate
  Chroma returns.

The predicate evaluates CEL, not an approximation of it. A key the record does not carry is a
missing-attribute error; an error propagates through `!`, and through `&&` and `||` unless another
operand decides them; and a result that is not `true` denies. Equality is CEL's heterogeneous
equality (`1 == 1.0`, `"1" != 1`), ordering across types is an error, and strings compare by code
point. It evaluates arithmetic, string helpers (`contains`, `startsWith`, `endsWith`, `upperAscii`,
`+`), casts (`int`, `double`, `string`, `timestamp`, `duration`), timestamp and duration
arithmetic and `timeSince` (against the clock when the plan is translated), field-to-field
comparisons, ternaries, `size`, hierarchies (built from a string or a list of segments), list
literals and map literals built from expressions, an index into a list or a map, the collection
macros (one- and two-variable) and Cerbos's `except`, `intersect` and `isSubset` over any list, and
null comparisons. `matches` is evaluated by [re2js](https://github.com/le0pard/re2js), a port of
RE2, so a pattern means what it means to the PDP: no lookaround, a repeat count of at most 1000,
`$` at the absolute end, `(?i)` by Unicode simple case folding; a pattern RE2 rejects is an
evaluation error, as it is in Cerbos.

It reads only the metadata the mapper declares. A plain key must hold a scalar: a list or anything
else that is not a string, finite number or boolean reads as missing, which denies. A key declared
`valueType: "json"` is parsed, and holds whatever the PDP saw: a list, a list of objects, a map, a
null. Everything the post-filter cannot evaluate exactly is still refused with
`UnsupportedOperatorError`, at translation, never from the predicate:

- a reference the mapper has no entry for: an undeclared attribute is one the record may not
  store (a list, a relation), and reading it as missing would deny records the PDP allows;
- arithmetic between a stored number and an integral literal, or between two integral literals.
  A plan carries `1` whether the policy wrote `1` or `1.0`, and CEL arithmetic has no int/double
  overload: `R.attr.x + 1` is an error on every record, so `!(R.attr.x + 1 > 2)` denies every
  record, while `R.attr.x + 1.0` is not. Write a fractional literal, or cast the attribute;
- a divisor that reads metadata, and `string()` over a stored number (a key not declared
  `valueType: "boolean"`): the chromadb client stores metadata through JSON, which writes -0.0 as
  0, so the sign of a stored zero is lost, and it decides both the infinity a division by zero
  gives and `string()`'s `"-0"`;
- a map literal with a key that is not a string literal, and the operators it has no evaluation
  for.

A list-valued expression (`filter`, `map`, `except`, `intersect`) in a boolean position has no
overload, so CEL errors on every record and the PDP denies it; the predicate denies it the same
way, as the other adapters do.

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
and `check()` decisions recorded from Cerbos PDP 0.57.0 (and 0.56.0), executed as real ChromaDB
metadata queries over the corpus's 44 seed records. The harness translates every case with
`allowPostFilter: true` and applies the `postFilter` to every record the `where` returns, as a
caller must. Passed cases on the current PDP, 0.57.0, out of every golden case in the tier:

| Tier | Passed / total |
| --- | --- |
| core | 31 / 31 |
| extended | 96 / 110 |
| adversarial | 454 / 518 |

Without `allowPostFilter`, the 384 cases the post-filter answers throw `UnsupportedOperatorError`
instead, as they did before the option existed (`src/translator.test.ts` pins that), leaving 78
passing: 22, 12 and 44 in the three tiers.

Every case that does not pass is refused with `UnsupportedOperatorError`; none returns wrong
records. [`conformance-ledger.json`](conformance-ledger.json) lists each one with its reason.
Planner-divergence cases are skipped, and count in the total but never as passed. On 0.57.0 that is
three extended cases and four adversarial cases. On 0.56.0, `null/has/missing-attribute` and `null/has/composed-with-comparison` are skipped too: that planner folds `has()` on an omitted attribute to true, while `check()` denies the record; 0.57.0 keeps `has()` in the plan. `arithmetic/add/int-literal-plus-constant` and `arithmetic/add/int-literal-negated`: the planner drops the int type of the literal in `R.attr.x + 1`, so the plan is the double spelling's, while `check()` has no double + int overload and denies every row; write `1.0`. Three `composition/*`
cases whose DENY condition reads `aNumber`, which j2 lacks: the plan's `not(...)` of it denies j2,
while `checkResource` receives `aNumber` as absent and treats the erroring deny rule as not matching
([#530](https://github.com/cerbos/query-plan-adapters/issues/530)). Two `type-mismatch/in/*` cases
whose container is a scalar: the planner rewrites `in` over it to `eq`, while `check()` has no `in`
overload for that container and errors
([#596](https://github.com/cerbos/query-plan-adapters/issues/596)).

The harness mapping declares no metadata key but the id `required: true`: every other scalar the
corpus reads through a filter is missing on some seed. It declares the boolean keys
`valueType: "boolean"` and the integer keys `numericType: "integer"`, so their inequalities are
spelled without `$ne` and proved by the corpus (`logic/not/bare-boolean-attribute`,
`type-mismatch/not-equals/number-field-against-string-literal`). An inequality over a string key has
no `Where` form, so the post-filter answers it. The corpus therefore proves no `$ne`/`$nin` filter;
`src/translator.test.ts` pins that `required` gates them, and that the type declarations are what
spell the others.

The harness stores every scalar the dataset has, `createdBy` and `scope` included, and maps each.
What a scalar key cannot hold it stores as JSON text under a key declared
`valueType: "json"`: the lists (`tagNames`, `aNumberList`, `aBoolList`, `tags`), `categories` and
`mainCategory`, `owner` and `coOwner`, which reach `check()` as an explicit null on some rows, and
`parent` and `obj` whole, beside their flattened scalar keys. Each is written as the generator
builds the resource, so the record read back is the resource. `tagNames` and `aNumberList` also
carry their non-null elements under a [`listElements`](#list-membership) key, so membership against
a literal is a `$contains` filter (`membership/in/literal-in-resource-number-list`,
`collection/has-intersection/value-first`). Only `tagNames` is declared `required`, since
`aNumberList` is null on j4: `membership/in/negated-literal-in-resource-list` is a `$not_contains`
filter, while `membership/in/negated-literal-in-null-resource-list` is refused by the `Where` and
answered by the post-filter. The 35 cases that remain refused
turn on a type the plan does not carry (an integral literal next to a stored number, `0 / 0` in a
ternary) or on the sign of a stored zero.

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
| Absent to-one parent | **Handled** — `relation/contains/one-hop`, `relation/and/negated-conjunction-short-circuits-absent-parent` and the other scalar hops pass over flattened keys; `relation/all/to-one-chain`, `relation/exists/negated-to-one-chain` and the other chained collections pass over a JSON key | A scalar hop is stored flattened onto a dotted key (`parent.aString`) that an absent parent leaves out, which the post-filter reads as CEL's missing-attribute error. The dotted key must be written exactly when the parent exists. A collection behind a relation is read from the parent stored whole under a key declared `valueType: "json"`, which an absent parent leaves out ([#309](https://github.com/cerbos/query-plan-adapters/issues/309)) |

## Behaviour changes

- **Breaking:** `ALWAYS_ALLOWED` and `ALWAYS_DENIED` carry no `filters` field. `ALWAYS_ALLOWED` used
  to carry `filters: {}`, which Chroma's validator rejects, and `ALWAYS_DENIED` declared
  `filters?: undefined`, which a caller that skipped the switch on `kind` passed to Chroma as no
  `where`, returning every record
  ([#494](https://github.com/cerbos/query-plan-adapters/issues/494),
  [ADR 0011](../docs/adr/0011-denial-fails-closed-in-the-result.md)).
- **Breaking, opt-in only:** under `allowPostFilter: true`, a post-filtered result carries
  `postFilter` and, for a root `and`, the expressible conjuncts as `prefilter`; it never carries
  `filters`. `postFilter` now evaluates the whole condition, so it is correct on its own. A caller
  that applied `filters` and dropped `postFilter` used to over-grant.
- **Widening, opt-in:** `allowPostFilter: true` answers in memory, with CEL's semantics, the parts
  of a plan Chroma's `Where` cannot express, and returns them as a `postFilter`
  ([#228](https://github.com/cerbos/query-plan-adapters/issues/228)). See
  [Post-filtering](#post-filtering). Without the option nothing changes: the same plans throw
  `UnsupportedOperatorError`, and `QueryPlanToChromaDBResult`'s default type argument keeps
  `filters` a `Where` on every conditional result.

- **Widening, opt-in:** a key declared `valueType: "json"` (new) holds an attribute as JSON text,
  which the post-filter parses, so a list, a list of objects, a map and an explicit null attribute
  can be read under `allowPostFilter`; a reference below it resolves through its entry. The
  post-filter now also evaluates null comparisons, two-variable comprehensions, `except`,
  `intersect` and `isSubset`, a map literal as a value, `has()` of a nested attribute under
  negation when the enclosing attribute is a JSON key, and every RE2 `matches` pattern (through
  re2js, a new dependency). Each used to throw `UnsupportedOperatorError`, as did `filter()`,
  `map()`, `except()` or `intersect()` in a boolean position, which the predicate now denies on
  every record, as the PDP does. A null comparison over
  a plain key reads an absent key as a missing attribute, which denies: an attribute sent to
  `check()` as an explicit null belongs under a JSON key (see
  [NULL attribute representation](#null-attribute-representation)).
- **Widening, opt-in:** under `allowPostFilter`, the post-filter now evaluates `upperAscii`,
  `duration()`, `timeSince`, timestamp and duration arithmetic, list literals built from
  expressions (`R.attr.a in [R.attr.b]`), an index into a map literal, and a hierarchy built from a
  list of segments. Each used to throw `UnsupportedOperatorError`. A two-variable comprehension,
  which used to fail with a plain `Error`, now throws `UnsupportedOperatorError`.
- **Fix, opt-in:** the post-filter compares hierarchies segment by segment, as Cerbos does, so
  `hierarchy("a:b", ":")` overlaps `hierarchy("a.b")`, and `size()` of a hierarchy counts its
  segments. It used to require both sides to share a delimiter, and denied such a pair.

- **Widening:** a key declared `valueType: "json"` can name a native list key with `listElements`
  (new), which turns `"x" in R.attr.list` and `hasIntersection` against a literal list into
  `$contains`, and their negations into `$not_contains` when the entry is `required`, with or
  without `allowPostFilter`. Each used to throw `UnsupportedOperatorError`, or be answered only by
  the post-filter ([#475](https://github.com/cerbos/query-plan-adapters/issues/475)).

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

Edit `src/`. `npm run build` (`tsc --build`) emits the published surface to `lib/`, which is
gitignored and leaves the tests out; `npm run typecheck` is the check that covers `src/` and every
`*.test.ts`.

| Command | What it does | Needs |
| --- | --- | --- |
| `npm test` | Offline unit suite: the refusal type, the rules every emitted filter obeys (each field is a mapped key, no `$not`/`$nor`, `$ne`/`$nin` only on `required` fields and never on a boolean or integer key, fractional thresholds only on `numericType: "float"` fields), the mapper contract no policy can reach (function mappers, `required`, `numericType`, `valueType`, the unmapped fallback), what `allowPostFilter` changes and what it leaves alone, and malformed input | Node only |
| `npm run typecheck` | Type-checks `src/` and the tests | Node only |
| `npm run chroma` | Starts the pinned ChromaDB ([`CHROMA_IMAGE`](CHROMA_IMAGE)) on port 8234 | Docker |
| `npm run test:adversarial` | Replays the recorded conformance goldens (`../conformance/golden/`) against the ChromaDB on `CHROMA_URL` (default `http://127.0.0.1:8234`); no PDP | A running ChromaDB |
