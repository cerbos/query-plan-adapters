# Cerbos + Mongoose ORM Adapter

An adapater library that takes a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into a [Mongoose](https://mongoosejs.com/) filter. This is designed to work alongside a project using the [Cerbos Javascript SDK](https://github.com/cerbos/cerbos-sdk-javascript).

The following conditions are supported: `and`, `or`, `eq`, `ne`, `lt`, `gt`, `lte`, `gte` and `in`.

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
npm install @cerbos/orm-mongoose
```

This package exports a function:

```ts
import { queryPlanToMongoose, PlanKind } from "@cerbos/orm-mongoose";

queryPlanToMongoose({ queryPlan, fieldNameMapper }): {
  kind: PlanKind,
  filters?: any // a filter to pass to the find() function of a Mongoose model
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

The function reqiures the full query plan from Cerbos to be passed in an object along with a `fieldNameMapper`.

A basic implementation can be as simple as:

```js
import { GRPC as Cerbos } from "@cerbos/grpc";
import mongoose from "mongoose";

import { queryPlanToMongoose, PlanKind } from "@cerbos/orm-mongoose";

// connect to mongo
await mongoose.connect("mongodb://127.0.0.1:27017/test");
// connect to Cerbos PDP
const cerbos = new Cerbos("localhost:3592", { tls: false });

// Mongoose models (schema excluded for brevity)
const MyModel = mongoose.model("MyModel", ....);

// Fetch the query plan from Cerbos passing in the principal
// resource type and action
const queryPlan = await cerbos.planResources({
  principal: {....},
  resource: { kind: "resourceKind" },
  action: "view"
})

// Generate the mongoose filter from the query plan
const result = queryPlanToMongoose({
  queryPlan,
  fieldNameMapper: {
    "request.resource.attr.aFieldName": "mongooseModelFieldName"
  }
});

// The query plan says the user would always be denied
// return empty or throw an error depending on your app.
if(result.kind == PlanKind.ALWAYS_DENIED) {
  return console.log([]);
}

// Pass the filters in as where conditions
// If you have prexisting where conditions, you can pass them in an $and clause
const result = await MyModel.find({
  ...result.filters
});

console.log(result)
```

The `fieldNameMapper` is used to convert the field names in the query plan response to names of fields in the Mongoose model - this can be done as a map or a function:

```js
const filters = queryPlanToMongoose({
  queryPlan,
  fieldNameMapper: {
    "request.resource.attr.aFieldName": "mongooseModelFieldName",
  },
});

//or

const filters = queryPlanToMongoose({
  queryPlan,
  fieldNameMapper: (fieldName: string): string => {
    if (fieldName.indexOf("request.resource.") > 0) {
      return fieldName.replace("request.resource.attr", "");
    }

    if (fieldName.indexOf("request.principal.") > 0) {
      return fieldName.replace("request.principal.attr", "");
    }
  },
});
```
