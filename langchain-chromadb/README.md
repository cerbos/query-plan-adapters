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

## Requirements

- Cerbos > v0.16
- `@cerbos/http` or `@cerbos/grpc` client
- Node.js >= 20.0.0
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

The Cerbos query plan references fields using paths such as `request.resource.attr.title`. Use a mapper to translate those to the metadata field names in your ChromaDB collection.

As a map:

```ts
const result = queryPlanToChromaDB({
  queryPlan,
  fieldNameMapper: {
    "request.resource.attr.aBool": "aBool",
    "request.resource.attr.aString": "title",
  },
});
```

As a function:

```ts
const result = queryPlanToChromaDB({
  queryPlan,
  fieldNameMapper: (fieldName: string): string => {
    return fieldName.replace("request.resource.attr.", "");
  },
});
```

If a field is not found in the map, the original path is used as-is.

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
