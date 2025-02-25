# Cerbos + MikroORM Adapter

An adapter library that takes a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into a [MikroORM](https://mikro-orm.io) filter object. This is designed to work alongside a project using the [Cerbos Javascript SDK](https://github.com/cerbos/cerbos-sdk-javascript).

## Features

### Supported Operators

#### Basic Operators

- Logical operators: `and` ($and), `or` ($or), `not` ($not)
- Comparison operators: `eq`, `ne` ($ne), `lt` ($lt), `gt` ($gt), `lte` ($lte), `gte` ($gte), `in` ($in)
- String operations: `startsWith` ($like), `endsWith` ($like), `contains` ($like), `isSet` ($ne null)

#### Relation Operators

- One-to-one: dot notation (e.g., `nested.id`)
- One-to-many/Many-to-many: `$exists`, `$in`
- Collection operators: `exists`, `exists_one`, `all`, `filter`
- Set operations: `hasIntersection` ($in)

#### Advanced Features

- Deep nested relations support using dot notation
- Automatic field inference
- Collection mapping and filtering
- Complex condition combinations
- Type-safe field mappings

## Requirements

- Cerbos > v0.40
- `@cerbos/http` or `@cerbos/grpc` client
- MikroORM > v5.0

## Installation

```bash
npm install @cerbos/orm-mikroorm
```

## Usage

The package exports a function:

```ts
import { queryPlanToMikroORM, PlanKind } from "@cerbos/orm-mikroorm";

queryPlanToMikroORM({
  queryPlan,                // The Cerbos query plan response
  mapper,                   // Map Cerbos field names to MikroORM field names
}): {
  kind: PlanKind,
  filters?: any             // MikroORM filter conditions
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

3. Create MikroORM entities:

```typescript
import { Entity, PrimaryKey, Property } from "@mikro-orm/core";

@Entity()
export class Resource {
  @PrimaryKey()
  id!: number;

  @Property()
  title!: string;

  @Property()
  status!: string;
}
```

4. Implement the mapper:

```ts
import { GRPC as Cerbos } from "@cerbos/grpc";
import { MikroORM } from "@mikro-orm/core";
import { queryPlanToMikroORM, PlanKind } from "@cerbos/orm-mikroorm";
import { Resource } from "./entities";

const orm = await MikroORM.init({
  entities: [Resource],
  dbName: "my-db",
  type: "postgresql",
});
const em = orm.em.fork();
const cerbos = new Cerbos("localhost:3592", { tls: false });

// Fetch query plan from Cerbos
const queryPlan = await cerbos.planResources({
  principal: { id: "user1", roles: ["USER"] },
  resource: { kind: "resource" },
  action: "view",
});

// Convert query plan to MikroORM filters
const result = queryPlanToMikroORM({
  queryPlan,
  mapper: {
    "request.resource.attr.title": { field: "title" },
    "request.resource.attr.status": { field: "status" },
  },
});

if (result.kind === PlanKind.ALWAYS_DENIED) {
  return [];
}

// Use filters in MikroORM query
const records = await em.find(Resource, result.filters);

// Use filters in MikroORM query with other conditions
const records = await em.find(Resource, {
  $and: [{ status: "DRAFT" }, result.filters],
});
```

### Field Name Mapping

Fields can be mapped using either an object or a function:

```ts
// Object mapping
const result = queryPlanToMikroORM({
  queryPlan,
  mapper: {
    "request.resource.attr.fieldName": { field: "mikroormFieldName" },
  },
});

// Function mapping
const result = queryPlanToMikroORM({
  queryPlan,
  mapper: (fieldName) => ({
    field: fieldName.replace("request.resource.attr.", ""),
  }),
});
```

### Relations Mapping

Relations are mapped with their types and optional field configurations. MikroORM uses dot notation for relation paths:

```ts
const result = queryPlanToMikroORM({
  queryPlan,
  mapper: {
    // Simple relation mapping - fields will be inferred
    "request.resource.attr.owner": {
      relation: {
        name: "owner",
        type: "one",
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

Fields are automatically inferred from the path unless explicitly mapped:

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
const result = queryPlanToMikroORM({
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

// Results in MikroORM filters like:
const records = await em.find(Resource, {
  $and: [
    { status: "active" },
    { "owner.id": "user1" },
    { "tags.name": { $in: ["tag1", "tag2"] } },
  ],
});
```

## Types

### Query Plan Response Types

```ts
import { PlanKind, QueryPlanToMikroORMResult } from "@cerbos/orm-mikroorm";

type QueryPlanToMikroORMResult =
  | {
      kind: PlanKind.ALWAYS_ALLOWED | PlanKind.ALWAYS_DENIED;
    }
  | {
      kind: PlanKind.CONDITIONAL;
      filters: Record<string, any>;
    };

// Example usage with type narrowing:
const result = queryPlanToMikroORM({ queryPlan });

if (result.kind === PlanKind.CONDITIONAL) {
  const records = await em.find(Resource, result.filters);
} else if (result.kind === PlanKind.ALWAYS_ALLOWED) {
  const records = await em.find(Resource, {});
} else {
  return [];
}
```

### Mapper Types

```ts
type MapperConfig = {
  field?: string;
  relation?: {
    name: string;
    type: "one" | "many";
    field?: string;
    fields?: {
      [key: string]: MapperConfig;
    };
  };
};

type Mapper = { [key: string]: MapperConfig } | ((key: string) => MapperConfig);
```

## Resources

### Documentation

- [Cerbos Documentation](https://docs.cerbos.dev)
- [MikroORM Documentation](https://mikro-orm.io/docs)
- [Query Plan API Reference](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)

### Related Projects

- [Cerbos JavaScript SDK](https://github.com/cerbos/cerbos-sdk-javascript)
- [MikroORM](https://github.com/mikro-orm/mikro-orm)

### Community

- [Cerbos Slack](https://community.cerbos.dev)
- [Cerbos GitHub Discussions](https://github.com/cerbos/cerbos/discussions)
- [MikroORM Discord](https://discord.gg/6BSV5SE)

## License

Apache 2.0 - See [LICENSE](../LICENSE) for more information.
