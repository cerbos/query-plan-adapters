# Cerbos + Prisma ORM Adapter

An adapter library that takes a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into a [Prisma](https://prisma.io) where clause object. This is designed to work alongside a project using the [Cerbos Javascript SDK](https://github.com/cerbos/cerbos-sdk-javascript).

## Features

### Supported Operators

#### Basic Operators

- Logical operators: `and`, `or`, `not`
- Comparison operators: `eq`, `ne`, `lt`, `gt`, `lte`, `gte`, `in`
- String operations: `startsWith`, `endsWith`, `contains`, `isSet`

#### Relation Operators

- One-to-one: `is`, `isNot`
- One-to-many/Many-to-many: `some`, `none`, `every`
- Collection operators: `exists`, `exists_one`, `all`, `filter`, `except`
- Set operations: `hasIntersection`

#### Advanced Features

- Deep nested relations support
- Automatic field inference
- Collection mapping and filtering
- Complex condition combinations
- Type-safe field mappings

## Requirements

- Cerbos > v0.40
- `@cerbos/http` or `@cerbos/grpc` client
- Prisma > v6.0

## System Requirements

- Node.js >= 20.0.
- Prisma CLI & Client >= 6.0
- A database supported by Prisma (SQLite/PostgreSQL/MySQL/etc.) so the Prisma client can communicate with stored data

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

1. Create a basic policy file in the `policies` directory:

```yaml
apiVersion: api.cerbos.dev/v1
resourcePolicy:
  resource: resource
  version: default
  rules:
    - actions: ["view"]
      effect: EFFECT_ALLOW
      roles: ["USER"]
      condition:
        match:
          expr: request.resource.attr.status == "active"
```

2. Start Cerbos PDP:

```bash
docker run --rm -i -p 3592:3592 -v $(pwd)/policies:/policies ghcr.io/cerbos/cerbos:latest
```

3. Create Prisma schema (`prisma/schema.prisma`):

```prisma
datasource db {
  provider = "sqlite"
  url      = env("DATABASE_URL")
}

generator client {
  provider = "prisma-client-js"
}

model Resource {
  id     Int     @id @default(autoincrement())
  title  String
  status String
}
```

4. Implement the mapper

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

### Collection Operators

The adapter understands the full Cerbos collection operator set, including `except`. For example, the configuration below ensures a resource’s categories do not have any sub-category named `finance`:

```ts
const result = queryPlanToPrisma({
  queryPlan,
  mapper: {
    "request.resource.attr.categories": {
      relation: {
        name: "categories",
        type: "many",
        fields: {
          subCategories: {
            relation: {
              name: "subCategories",
              type: "many",
              fields: {
                name: { field: "name" },
              },
            },
          },
        },
      },
    },
  },
});
```

`queryPlanToPrisma` emits the necessary nested `NOT` structure so Prisma receives a valid filter for the entire relation chain.

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

### Handling `in` Operators

`queryPlanToPrisma` normalises Cerbos `in` expressions to match Prisma expectations:

- Single values become equality comparisons (`{ field: "value" }`).
- Arrays remain `{ field: { in: [...] } }`.
- Relation-backed fields retain their relation structure while still applying the appropriate equality or `in` operator at the leaf.

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

## Development

### Running Tests

```bash
npm test
```

> **Note:** The suite seeds `prisma/dev.db` and invokes `prisma db push --force-reset`. Only run it against disposable development databases.

The tests populate Prisma with fixture data and assert query results directly against those fixtures, covering scalar and relation `in` operators, collection behaviour (including `except`), nested relations, and lambda expressions.

## Types

### Query Plan Response Types

The adapter is fully typed and provides clear type definitions for all responses:

```ts
import { PlanKind, QueryPlanToPrismaResult } from "@cerbos/orm-prisma";

// The result will be one of these types:
type QueryPlanToPrismaResult =
  | {
      kind: PlanKind.ALWAYS_ALLOWED | PlanKind.ALWAYS_DENIED;
    }
  | {
      kind: PlanKind.CONDITIONAL;
      filters: Record<string, any>;
    };

// Example usage with type narrowing:
const result = queryPlanToPrisma({ queryPlan });

if (result.kind === PlanKind.CONDITIONAL) {
  // TypeScript knows `filters` exists here
  const records = await prisma.resource.findMany({
    where: result.filters,
  });
} else if (result.kind === PlanKind.ALWAYS_ALLOWED) {
  // No filters needed
  const records = await prisma.resource.findMany();
} else {
  // Must be ALWAYS_DENIED
  return [];
}
```

### Mapper Types

The mapper configuration is also fully typed:

```ts
type MapperConfig = {
  field?: string;
  relation?: {
    name: string;
    type: "one" | "many";
    field?: string;
    fields?: {
      [key: string]: MapperConfig; // Recursive for nested fields
    };
  };
};

type Mapper = { [key: string]: MapperConfig } | ((key: string) => MapperConfig);
```

## Full Example

A complete example application using this adapter can be found at [https://github.com/cerbos/express-prisma-cerbos](https://github.com/cerbos/express-prisma-cerbos)

## Resources

### Documentation

- [Cerbos Documentation](https://docs.cerbos.dev)
- [Prisma Documentation](https://www.prisma.io/docs)
- [Query Plan API Reference](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)

### Examples and Tutorials

- [Express + Prisma + Cerbos Example](https://github.com/cerbos/express-prisma-cerbos)
- [Cerbos Query Planning Guide](https://docs.cerbos.dev/cerbos/latest/policies/compile.html)
- [Prisma Filtering Guide](https://www.prisma.io/docs/concepts/components/prisma-client/filtering-and-sorting)

### Related Projects

- [Cerbos JavaScript SDK](https://github.com/cerbos/cerbos-sdk-javascript)

### Community

- [Cerbos Slack](https://community.cerbos.dev)
- [Cerbos GitHub Discussions](https://github.com/cerbos/cerbos/discussions)

## License

Apache 2.0 - See [LICENSE](../LICENSE) for more information.
