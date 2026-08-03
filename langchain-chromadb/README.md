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
- Existence: `isSet`
- Array/collection: `hasIntersection`, `exists`, `exists_one`, `all`, `filter`, `map`, `lambda`, `size`

Any unsupported operator in the plan causes `queryPlanToChromaDB` to throw an error.

## Conformance contract

The adapter is differentially tested against Cerbos PDP 0.54.0 `checkResource` decisions using 20 hostile seed documents and real ChromaDB metadata queries. The Spring Data adapter defines the reference semantics for this compatibility snapshot.

| Classification | Coverage |
| --- | --- |
| Oracle-tested | 15 reference actions: directional and inequality comparisons, single/empty membership, Unicode and empty strings, negative numbers, n-ary/double/triple negation, membership on an optional resource field, mapped nested-field equality, and case-sensitive equality |
| Fail-closed | 99 reference conformance actions plus regex, ordered indexing/`get-field`, and timestamp probes (102 actions total) |
| Known planner divergence | `has()` on a missing attribute is folded by the Cerbos planner to `ALWAYS_ALLOWED`, while `checkResource` denies the missing-attribute documents. Until the planner is fixed, use `R.attr.x != null` for database-backed attributes instead of `has(R.attr.x)` |

Chroma metadata filters are limited to flat scalar comparisons and membership. Nested collections, field-to-field and arithmetic expressions, string helpers, hierarchy/timestamp operations, ternaries, nullable inequality, and other shapes that cannot be represented faithfully throw before a filter is returned.

## Requirements

- Cerbos > v0.16
- `@cerbos/http` or `@cerbos/grpc` client
- Node.js >= 22.0.0
- ChromaDB 3.x

## Installation

```bash
npm install @cerbos/langchain-chromadb
```

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
