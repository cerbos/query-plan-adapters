# Cerbos + Mongoose ORM Adapter

An adapter library that takes a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into a [Mongoose](https://mongoosejs.com/) filter. It is designed to run alongside a project that is already using the [Cerbos JavaScript SDK](https://github.com/cerbos/cerbos-sdk-javascript) to fetch query plans so that authorization logic can be pushed down to MongoDB.

## How it works

1. Use a Cerbos client (`@cerbos/http` or `@cerbos/grpc`) to call `planResources` and obtain a `PlanResourcesResponse`.
2. Provide `queryPlanToMongoose` with that plan and an optional mapper that describes how Cerbos attribute paths relate to your document schema.
3. The adapter walks the Cerbos expression tree, translates supported operators to MongoDB syntax, and returns `{ kind, filters? }`.
4. Inspect `result.kind`:
   - `ALWAYS_ALLOWED`: the caller can query without any additional filters.
   - `ALWAYS_DENIED`: short-circuit and return an empty result set.
   - `CONDITIONAL`: execute the query with `result.filters`.

You can merge the adapter output with existing application filters (for example, via `$and`) before issuing the Mongoose query.

## Supported operators

| Category | Operators | Behavior |
| --- | --- | --- |
| Logical | `and`, `or`, `not` | Builds `$and`, `$or`, and `$nor` groups. |
| Comparisons | `eq`, `ne`, `lt`, `le`, `gt`, `ge` | Emits `$eq`, `$ne`, `$lt`, `$lte`, `$gt`, `$gte` checks against the mapped field. |
| Membership | `in`, `hasIntersection` | `$in` on simple lists, or `$elemMatch` when targeting array relations; `hasIntersection` supports either a direct array field or a `map` projection inside the plan. |
| String helpers | `contains`, `startsWith`, `endsWith` | Generates escaped regular expressions that target substrings, prefixes, or suffixes. |
| Existence helpers | `isSet`, `exists`, `exists_one` | Uses `$exists`/`$ne: null` for scalars and `$elemMatch` for collections. |
| Collection helpers | `filter`, `lambda`, `map`, `all` | Translates Cerbos collection expressions into scoped `$elemMatch` filters and maps lambda variables to the correct nested paths. |

Any operator not listed above causes `queryPlanToMongoose` to throw `Unsupported operator: <name>`.

> **Note:** `exists_one` currently behaves like “at least one element matches”. Enforcing “exactly one” requires an aggregation pipeline, which is outside the scope of this adapter.

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
  valueParser?: (value: any) => any;
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

- `field` rewrites a single Cerbos path to a different field in MongoDB.
- `valueParser` transforms leaf values during filter construction. This is useful when the Cerbos plan contains string representations that need to be converted to MongoDB-specific types (for example, converting a string to an `ObjectId`). The parser is applied to each value in `eq`, `ne`, `lt`, `le`, `gt`, `ge`, and `in` operators. It also works on nested relation fields via the `fields` map.
- `relation` describes embedded documents (`type: "one"`) or arrays (`type: "many"`). When `field` is provided on a relation it identifies the property inside that relation that should be used for comparisons (for example, matching `createdBy.id` without an `$elemMatch`).
- `fields` supplies nested overrides so lambda expressions such as `tag.name` can be mapped to the correct property.

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

Use `relation` when mapping nested objects or arrays. `type: "one"` maps to embedded/single relations and results in dotted field paths, while `type: "many"` maps to arrays and lets the adapter emit `$elemMatch` conditions. The optional `fields` map lets you rename nested properties referenced in lambda expressions.

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

#### Collection operators in practice

Collection-aware operators (`filter`, `exists`, `exists_one`, `hasIntersection`, `map`, and `all`) require the mapper to declare the relation with `type: "many"`. The adapter automatically scopes lambda variables and uses the `fields` map when translating expressions such as `tag.name`:

```ts
const mapper: Mapper = {
  "request.resource.attr.tags": {
    relation: {
      name: "tags",
      type: "many",
      fields: {
        name: { field: "name" },
      },
    },
  },
};
```

- `exists`, `exists_one`, and `filter` wrap the translated condition in `$elemMatch`.
- `hasIntersection` works for both scalar arrays and arrays of objects; when the plan uses `map(lambda(tag.name))` the adapter projects `tag.name` to `tags.$elemMatch.name`.
- `all` converts the lambda condition into a negated `$elemMatch` so that all elements must satisfy the predicate.
- A bare `map` expression verifies that the referenced nested path exists inside each element.

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

#### Value parsing

Use `valueParser` to convert values from the Cerbos plan into types that MongoDB expects. A common use case is converting string IDs to `ObjectId`:

```ts
import { Types } from "mongoose";

const mapper: Mapper = {
  "request.resource.attr.id": {
    field: "_id",
    valueParser: (value) => new Types.ObjectId(value),
  },
};
```

`valueParser` also works on nested relation fields via the `fields` map:

```ts
const mapper: Mapper = {
  "request.resource.attr.createdBy": {
    relation: {
      name: "createdBy",
      type: "one",
      field: "id",
      fields: {
        id: {
          field: "id",
          valueParser: (value) => new Types.ObjectId(value),
        },
      },
    },
  },
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

## Error handling

`queryPlanToMongoose` throws descriptive errors in the following scenarios:

- The plan kind is not one of the Cerbos `PlanKind` values (`Invalid query plan.`).
- A conditional plan omits the `operator`/`operands` structure (`Invalid Cerbos expression structure`).
- An operator listed in the plan is not implemented (`Unsupported operator: <name>`).
- Collection-oriented operators (`map`, `filter`, `exists`, `all`, etc.) are used without a `relation` mapper, or with a mapper that declares `type: "one"` where `type: "many"` is required (errors such as `map operator requires a relation mapping`).
- Lambda expressions in the plan are malformed (for example, missing a variable operand results in `Lambda variable must have a name`).
- Value operands do not match the expected type, e.g., `hasIntersection` supplies a non-array value.

Surfacing these errors early helps keep the adapter and your Cerbos policies in sync.

## Limitations

- `exists_one` behaves like “at least one element matches” because counting matches requires an aggregation pipeline.
- Operators not enumerated in **Supported operators** (such as search, mode, scalar math helpers, atomic number operations, composite keys, etc.) are not implemented and will throw `Unsupported operator`.
- All translations target standard MongoDB find filters; anything that would require `$expr` or a multi-stage aggregation pipeline is currently out of scope.
