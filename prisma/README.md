# Cerbos + Prisma ORM Adapter

An adapter library that takes a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into a [Prisma](https://prisma.io) where class object. This is designed to work alongside a project using the [Cerbos Javascript SDK](https://github.com/cerbos/cerbos-sdk-javascript).

## Features

### Supported Operators

- Logical operators: `and`, `or`, `not`
- Comparison operators: `eq`, `ne`, `lt`, `gt`, `lte`, `gte`, `in`
- Relation operators: `some`, `none`, `is`, `isNot`, `hasIntersection`
- String operations: `startsWith`, `endsWith`, `contains`, `isSet`
- Support for nested fields and relations
- Support for both one-to-one and one-to-many relationships

## Requirements

- Cerbos > v0.40
- `@cerbos/http` or `@cerbos/grpc` client

## Installation

```bash
npm install @cerbos/orm-prisma
```

## Usage

The package exports a function:

```ts
import { queryPlanToPrisma, PlanKind } from "@cerbos/orm-prisma";

queryPlanToPrisma({
  queryPlan,                // The Cerbos query plan response
  fieldNameMapper,         // Map Cerbos field names to Prisma field names
  relationMapper?: {}      // Optional: Map relation fields and their types
}): {
  kind: PlanKind,
  filters?: any           // Prisma where conditions
}
```

### Basic Example

```ts
import { GRPC as Cerbos } from "@cerbos/grpc";
import { PrismaClient } from "@prisma/client";
import { queryPlanToPrisma, PlanKind } from "@cerbos/orm-prisma";

const prisma = new PrismaClient();
const cerbos = new Cerbos("localhost:3592", { tls: false });

// Fetch query plan from Cerbos
const queryPlan = await cerbos.planResources({
  principal: { id: "user1", roles: ["USER"] },
  resource: { kind: "resource" },
  action: "view",
});

// Convert query plan to Prisma filters
const result = queryPlanToPrisma({
  queryPlan,
  fieldNameMapper: {
    "request.resource.attr.title": "title",
    "request.resource.attr.status": "status",
  },
});

if (result.kind === PlanKind.ALWAYS_DENIED) {
  return [];
}

// Use filters in Prisma query
const records = await prisma.resource.findMany({
  where: result.filters,
});
```

### Field Name Mapping

Field names can be mapped using either an object or a function:

```ts
// Object mapping
const result = queryPlanToPrisma({
  queryPlan,
  fieldNameMapper: {
    "request.resource.attr.fieldName": "prismaFieldName",
  },
});

// Function mapping
const result = queryPlanToPrisma({
  queryPlan,
  fieldNameMapper: (fieldName) =>
    fieldName.replace("request.resource.attr.", ""),
});
```

### Relations Mapping

Relations can be mapped with their types (one-to-one or one-to-many). The `field` property is only required when using direct field comparisons and can be omitted when using lambda expressions or exists operators:

```ts
const result = queryPlanToPrisma({
  queryPlan,
  fieldNameMapper: {},
  relationMapper: {
    "request.resource.attr.owner": {
      relation: "owner",
      field: "id",
      type: "one", // "one" for one-to-one, "many" for one-to-many
    },
    "request.resource.attr.tags": {
      relation: "tags",
      field: "name",
      type: "many",
    },
  },
});

// Function-based relation mapping
const result = queryPlanToPrisma({
  queryPlan,
  fieldNameMapper: {},
  relationMapper: (fieldName) => {
    if (fieldName.includes("owner")) {
      return {
        relation: "owner",
        field: "id",
        type: "one",
      };
    }
    // ...
  },
});
```

### Complex Examples

#### Multiple Conditions

```ts
// Combining multiple conditions with AND
const result = queryPlanToPrisma({
  queryPlan,
  fieldNameMapper: {},
  relationMapper: {
    "request.resource.attr.status": {
      relation: "status",
      field: "value",
      type: "one",
    },
    "request.resource.attr.owner": {
      relation: "owner",
      field: "id",
      type: "one",
    },
  },
});

// Results in Prisma filters like:
{
  AND: [
    { status: { is: { value: { equals: "active" } } } },
    { owner: { is: { id: { equals: "user1" } } } },
  ];
}
```

#### Nested Relations

```ts
const result = queryPlanToPrisma({
  queryPlan,
  fieldNameMapper: {
    "request.resource.attr.nested.field": "nestedModel.field",
  },
});

// Results in Prisma filters like:
{
  nestedModel: {
    field: {
      equals: "value";
    }
  }
}
```

## Full Example

A complete example application using this adapter can be found at [https://github.com/cerbos/express-prisma-cerbos](https://github.com/cerbos/express-prisma-cerbos)
