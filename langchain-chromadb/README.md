# Cerbos + LangChain.js ChromaDB Adapter

An adapter library that takes a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into a [ChromaDB](https://www.trychroma.com/) filter object that can be passed to the LangChain.js Chroma vector store. This is designed to work alongside a project using the [Cerbos Javascript SDK](https://github.com/cerbos/cerbos-sdk-javascript).

The following conditions are supported: `and`, `or`, `not`, `eq`, `ne`, `lt`, `gt`, `le`, `ge` and `in`.

Not Supported:

- `every`
- `contains`
- `search`
- `mode`
- `startsWith`
- `endsWith`
- `isSet`
- Scalar filters
- Atomic number operations
- Composite keys

## Requirements

- Cerbos > v0.16
- `@cerbos/http` or `@cerbos/grpc` client

## Usage

```
npm install @cerbos/lanchain-chromadb
```

This package exports a function:

```ts
import { queryPlanToChromaDB, PlanKind } from "@cerbos/lanchain-chromadb";

queryPlanToChromaDB({ queryPlan, fieldNameMapper }): {
  kind: PlanKind,
  filters?: any // a filter to pass as the `where` property of a ChromaDB query
}
```

where `PlanKind` is:

```ts
export enum PlanKind {
  /**
   * The specified action is always allowed for the principal on resources matching the input.
   */
  ALWAYS_ALLOWED = "KIND_ALWAYS_ALLOWED",

  /**
   * The specified action is always denied for the principal on resources matching the input.
   */
  ALWAYS_DENIED = "KIND_ALWAYS_DENIED",

  /**
   * The specified action is conditionally allowed for the principal on resources matching the input.
   */
  CONDITIONAL = "KIND_CONDITIONAL",
}
```

The function requires the full query plan from Cerbos to be passed in an object along with a `fieldNameMapper`.

A basic implementation can be as simple as:

```ts
import { GRPC as Cerbos } from "@cerbos/grpc";
import { Chroma } from "@langchain/community/vectorstores/chroma";
import { OpenAIEmbeddings } from "@langchain/openai";

import { queryPlanToChromaDB, PlanKind } from "@cerbos/lanchain-chromadb";

const cerbos = new Cerbos("localhost:3592", { tls: false });

// Fetch the query plan from Cerbos passing in the principal
// resource type and action
const queryPlan = await cerbos.planResources({
  principal: {....},
  resource: { kind: "resourceKind" },
  action: "view"
})

const filterResult = queryPlanToChromaDB({
  queryPlan,
  fieldNameMapper: {
    "request.resource.attr.aFieldName": "metadataFieldName"
  }
});

if(filterResult.kind === PlanKind.ALWAYS_DENIED) {
  // return empty or throw an error depending on your app.
  return [];
}

const chroma = await Chroma.fromExistingCollection(new OpenAIEmbeddings(), {
  collectionName: "my_collection",
});

const matches = await chroma.similaritySearch("query", 10, {
  ...filterResult.filters,
});
```

The `fieldNameMapper` is used to convert the field names in the query plan response to names of fields in your ChromaDB metadata - this can be done as a map or a function:

```ts
const filters = queryPlanToChromaDB({
  queryPlan,
  fieldNameMapper: {
    "request.resource.attr.aFieldName": "metadataFieldName",
  },
});

//or

const filters = queryPlanToChromaDB({
  queryPlan,
  fieldNameMapper: (fieldName: string): string => {
    if (fieldName.indexOf("request.resource.") > 0) {
      return fieldName.replace("request.resource.attr", "metadata");
    }

    if (fieldName.indexOf("request.principal.") > 0) {
      return fieldName.replace("request.principal.attr", "principal");
    }

    return fieldName;
  },
});
```
