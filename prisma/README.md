# Cerbos + Prisma ORM Adapter

An adapter library that takes a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into a [Prisma](https://prisma.io) where class object. This is designed to work alongside a project using the [Cerbos Javascript SDK](https://github.com/cerbos/cerbos-sdk-javascript).

## Features

### Supported Operators

- Logical operators: `and`, `or`, `not`
- Comparison operators: `eq`, `ne`, `lt`, `gt`, `lte`, `gte`, `in`
- Relation operators: `some`, `none`, `is`, `isNot`, `hasIntersection`
- String operations: `startsWith`, `endsWith`, `contains`, `isSet`
- Support for nested fields and relations
- Support for both one-to-one, one-to-many and many-to-many relationships

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
  mapper,                   // Map Cerbos field names to Prisma field names
}): {
  kind: PlanKind,
  filters?: any             // Prisma where conditions
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
  mapper: {
    "request.resource.attr.title": { field: "title" },
    "request.resource.attr.status": { field: "status" },
  },
});

if (result.kind === PlanKind.ALWAYS_DENIED) {
  return [];
}

// Use filters in Prisma query
const records = await prisma.resource.findMany({
  where: result.filters,
});

// Use filters in Prisma query with other conditions
const records = await prisma.resource.findMany({
  where: {
    AND: [
      {
        status: "DRAFT"
      },
      result.filters,
    ]
});
```

### Field Name Mapping

Fields can be mapped using either an object or a function:

```ts
// Object mapping
const result = queryPlanToPrisma({
  queryPlan,
  mapper: {
    "request.resource.attr.fieldName": { field: "prismaFieldName" },
  },
});

// Function mapping
const result = queryPlanToPrisma({
  queryPlan,
  mapper: (fieldName) => ({
    field: fieldName.replace("request.resource.attr.", ""),
  }),
});
```

### Relations Mapping

Relations are mapped with their types and optional field configurations. Fields can be automatically inferred from the path if not explicitly mapped:

```ts
const result = queryPlanToPrisma({
  queryPlan,
  mapper: {
    // Simple relation mapping - fields will be inferred
    "request.resource.attr.owner": {
      relation: {
        name: "owner",
        type: "one", // "one" for one-to-one, "many" for one-to-many
      },
    },

    // Relation with explicit field mapping
    "request.resource.attr.tags": {
      relation: {
        name: "tags",
        type: "many",
        field: "name", // Optional: specify field for direct comparisons
      },
    },

    // Relation with nested field mappings
    "request.resource.attr.nested": {
      relation: {
        name: "nested",
        type: "one",
        fields: {
          // Optional: specify mappings for nested fields
          aBool: { field: "aBool" },
          aNumber: { field: "aNumber" },
        },
      },
    },
  },
});
```

### Field Inference Example

When using relations, fields are automatically inferred from the path unless explicitly mapped:

```ts
// These mappers are equivalent for handling: request.resource.attr.nested.aNumber
{
  "request.resource.attr.nested": {
    relation: {
      name: "nested",
      type: "one",
      fields: {
        aNumber: { field: "aNumber" }
      }
    }
  }
}

// Shorter version - aNumber will be inferred from the path
{
  "request.resource.attr.nested": {
    relation: {
      name: "nested",
      type: "one"
    }
  }
}
```

### Complex Example with Multiple Relations and Direct Fields

```ts
const result = queryPlanToPrisma({
  queryPlan,
  mapper: {
    "request.resource.attr.status": { field: "status" },
    "request.resource.attr.owner": {
      relation: {
        name: "owner",
        type: "one",
      },
    },
    "request.resource.attr.tags": {
      relation: {
        name: "tags",
        type: "many",
        field: "name",
      },
    },
  },
});

// Results in Prisma filters like:
const result = await primsa.resource.findMany({
  where: {
    AND: [
      { status: { equals: "active" } },
      { owner: { is: { id: { equals: "user1" } } } },
      { tags: { some: { name: { in: ["tag1", "tag2"] } } } },
    ];
  }
})
```

### Complex Examples

#### Lambda Expression Examples

```ts
// Using exists with lambda expressions
const result = queryPlanToPrisma({
  queryPlan,
  mapper: {
    "request.resource.attr.comments": {
      relation: {
        name: "comments",
        type: "many",
        fields: {
          author: {
            relation: {
              name: "author",
              type: "one",
            },
          },
          status: { field: "status" },
        },
      },
    },
  },
});

// This can handle complex exists queries like:
// "Does the resource have any approved comments by specific users?"
const result = await primsa.resource.findMany({
  where: {
    comments: {
      some: {
        AND: [
          { status: { equals: "approved" } },
          {
            author: {
              is: {
                id: { in: ["user1", "user2"] },
              },
            },
          },
        ],
      },
    },
  },
});
```

## Full Example

A complete example application using this adapter can be found at [https://github.com/cerbos/express-prisma-cerbos](https://github.com/cerbos/express-prisma-cerbos)
