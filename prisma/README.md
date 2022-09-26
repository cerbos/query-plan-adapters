# Cerbos + Prisma ORM Adapter

An adapater library that takes a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into a [Prisma](https://prisma.io) where class object. This is designed to work alongside a project using the [Cerbos Javascript SDK](https://github.com/cerbos/cerbos-sdk-javascript).

The following conditions are supported: `and`, `or`, `eq`, `ne`, `lt`, `gt`, `lte`, `gte` and `in`, as well as relational filters `some`, `none`, `is` and `isNot`.

Not Supported:

- `every`
- `contains`
- `search`
- `mode`
- `startsWith`
- `endsWith`
- Scalar filters
- Composite type filters
- Atomic number operations
- Composite keys

## Requirements
- Cerbos > v0.16
- `@cerbos/http` or `@cerbos/grpc` client

## Usage

```
npm install @cerbos/orm-prisma
```

This package exports a function:

```ts
import { queryPlanToPrisma, PlanKind } from "@cerbos/orm-prisma";

queryPlanToPrisma({ queryPlan, fieldNameMapper, relationMapper }): {
  kind: PlanKind,
  filters?: any // a filter to pass to the findMany() function of Prisma
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

The function reqiures the full query plan from Cerbos to be passed in an object along with a `fieldNameMapper` and option `relationMapper` if the model has relations.

A basic implementation can be as simple as:

```js
import { GRPC as Cerbos } from "@cerbos/grpc";
import { PrismaClient } from "@prisma/client";

import { queryPlanToPrisma, PlanKind } from "@cerbos/orm-prisma";

const prisma = new PrismaClient();
const cerbos = new Cerbos("localhost:3592", { tls: false });

// Fetch the query plan from Cerbos passing in the principal
// resource type and action
const queryPlan = await cerbos.planResources({
  principal: {....},
  resource: { kind: "resourceKind" },
  action: "view"
})

// Generate the prisma filter from the query plan
const result = queryPlanToPrisma({
  queryPlan,
  fieldNameMapper: {
    "request.resource.attr.aFieldName": "prismaModelFieldName"
  },
  relationMapper: {
    "request.resource.attr.aRelatedModel": {
      "relation": "aRelatedModel",
      "field": "id" // the column it is joined on
    }
  }
});

// The query plan says the user would always be denied
// return empty or throw an error depending on your app.
if(result.kind == PlanKind.ALWAYS_DENIED) {
  return console.log([]);
}

// Pass the filters in as where conditions
// If you have prexisting where conditions, you can pass them in an AND clause
const result = await prisma.myModel.findMany({
  where: {
    AND: result.filters
  },
});

console.log(result)
```

The `fieldNameMapper` is used to convert the field names in the query plan response to names of fields in the Prisma model - this can be done as a map or a function:

```js
const filters = queryPlanToPrisma({
  queryPlan,
  fieldNameMapper: {
    "request.resource.attr.aFieldName": "prismaModelFieldName"
  }
});

//or

const filters = queryPlanToPrisma({
  queryPlan,
  fieldNameMapper: (fieldName: string): string => {
    if(fieldName.indexOf("request.resource.") > 0) {
      return fieldName.replace("request.resource.attr", "")
    }

    if(fieldName.indexOf("request.principal.") > 0) {
      return fieldName.replace("request.principal.attr", "")
    }
  }
});
```


The `relationMapper` is used to convert references to fields which are joins at the database level

```js
const filters = queryPlanToPrisma({
  queryPlan,
  fieldNameMapper: {},
  relationMapper: {
    "request.resource.attr.aRelatedModel": {
      "relation": "aRelatedModel",
      "field": "id" // the column it is joined on
    }
  }
});

//or

const filters = queryPlanToPrisma({
  queryPlan,
  fieldNameMapper: {},
  relationMapper: (fieldName: string): string => {
    if(fieldName.indexOf("request.resource.") > 0) {
      return {
        "relation": fieldName.replace("request.resource.attr.", ""),
        "field": "id" // the column it is joined on
      }
    }
  }
});
```

## Full Example

A full Prisma application making use of this adapater can be found at [https://github.com/cerbos/express-prisma-cerbos](https://github.com/cerbos/express-prisma-cerbos)