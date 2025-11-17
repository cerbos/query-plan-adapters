# Cerbos + Mongoose ORM Adapter

An adapter library that takes a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into a [Mongoose](https://mongoosejs.com/) filter. It is designed to run alongside a project that is already using the [Cerbos JavaScript SDK](https://github.com/cerbos/cerbos-sdk-javascript) to fetch query plans.

## Supported conditions

The adapter currently understands the following Cerbos plan operators:

- Logical: `and`, `or`, `not`
- Comparisons: `eq`, `ne`, `lt`, `le`, `gt`, `ge`, `in`
- String helpers: `contains`, `startsWith`, `endsWith`
- Existence helpers: `isSet`, `exists`, `exists_one`
- Collection helpers: `filter`, `hasIntersection`, `lambda`, `map`, `all`

Any operator not listed above throws `Unsupported operator`. `exists_one` is treated like `exists` (at least one matching element) because Mongoose find queries cannot express “exactly one” matches without switching to aggregation.

## Requirements

- Cerbos > v0.16 plus either the `@cerbos/http` or `@cerbos/grpc` client

## System Requirements

- Node.js >= 20.0.0
- Mongoose 8.x
- A MongoDB-compatible server (MongoDB 5.0+ is recommended for compatibility with Mongoose 8)

## Installation

```bash
npm install @cerbos/orm-mongoose
```

## API

```ts
import {
  queryPlanToMongoose,
  PlanKind,
  type Mapper,
} from "@cerbos/orm-mongoose";

const result = queryPlanToMongoose({
  queryPlan, // PlanResourcesResponse from Cerbos
  mapper, // optional Mapper - see below
});

if (result.kind === PlanKind.CONDITIONAL) {
  await MyModel.find(result.filters);
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

### Mapper configuration

The Cerbos query plan references fields using paths such as `request.resource.attr.title`. Use a mapper to translate those names to the paths in your Mongoose models and to describe relations/collections so the adapter can generate `$elemMatch` filters when needed.

```ts
export type MapperConfig = {
  field?: string;
  relation?: {
    name: string;
    type: "one" | "many";
    field?: string;
    fields?: Record<string, MapperConfig>;
  };
};

export type Mapper =
  | Record<string, MapperConfig>
  | ((key: string) => MapperConfig);
```

If you omit the mapper the adapter will use the query plan paths verbatim, which only works when your Mongo documents follow the Cerbos naming convention.

#### Direct fields

```ts
const mapper: Mapper = {
  "request.resource.attr.aBool": { field: "aBool" },
  "request.resource.attr.aString": { field: "title" },
  "request.principal.attr.department": { field: "principalDepartment" },
};
```

#### Relations and collections

Use `relation` when mapping nested objects or arrays. `type: "one"` maps to embedded/single relations; `type: "many"` maps to arrays. The optional `fields` map lets you rename nested properties.

```ts
const mapper: Mapper = {
  "request.resource.attr.createdBy": {
    relation: {
      name: "createdBy",
      type: "one",
      field: "id",
    },
  },
  "request.resource.attr.tags": {
    relation: {
      name: "tags",
      type: "many",
      fields: {
        id: { field: "id" },
        name: { field: "name" },
      },
    },
  },
};
```

#### Mapper functions

You can also supply a function if your mappings follow a predictable pattern:

```ts
const mapper: Mapper = (path) => {
  if (path.startsWith("request.resource.attr.")) {
    return { field: path.replace("request.resource.attr.", "") };
  }
  if (path.startsWith("request.principal.attr.")) {
    return { field: `principal.${path.replace("request.principal.attr.", "")}` };
  }
  return { field: path };
};
```

## Usage example

```ts
import { GRPC as Cerbos } from "@cerbos/grpc";
import mongoose from "mongoose";
import {
  queryPlanToMongoose,
  PlanKind,
  type Mapper,
} from "@cerbos/orm-mongoose";

await mongoose.connect("mongodb://127.0.0.1:27017/test");
const cerbos = new Cerbos("localhost:3592", { tls: false });
const MyModel = mongoose.model("MyModel", /* ... schema ... */);

const mapper: Mapper = {
  "request.resource.attr.title": { field: "title" },
  "request.resource.attr.owner": {
    relation: { name: "owner", type: "one", field: "id" },
  },
  "request.resource.attr.tags": {
    relation: {
      name: "tags",
      type: "many",
      fields: { name: { field: "name" } },
    },
  },
};

const queryPlan = await cerbos.planResources({
  principal: { id: "user1", roles: ["USER"] },
  resource: { kind: "document" },
  action: "view",
});

const result = queryPlanToMongoose({ queryPlan, mapper });

if (result.kind === PlanKind.ALWAYS_DENIED) {
  return [];
}

const filters = result.kind === PlanKind.CONDITIONAL ? result.filters : {};
const records = await MyModel.find(filters);
```

If you already have application-specific criteria you can combine them using `$and`:

```ts
const filters = result.kind === PlanKind.CONDITIONAL ? result.filters : {};
await MyModel.find({ $and: [filters ?? {}, { archived: false }] });
```

## Limitations

- `exists_one` behaves like “at least one element matches” because counting matches requires an aggregation pipeline.
- Operators not enumerated in the **Supported conditions** section (such as `search`, `mode`, scalar filters, atomic number operations, composite keys, etc.) are not implemented and will throw `Unsupported operator`.
