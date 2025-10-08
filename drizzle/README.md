# Cerbos + Drizzle ORM Adapter

An adapter library that takes a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into a [Drizzle ORM](https://orm.drizzle.team/) SQL expression. This allows you to use Cerbos query plans directly inside your Drizzle queries.

## Features

- Supports logical operators: `and`, `or`, `not`
- Supports comparison operators: `eq`, `ne`, `lt`, `gt`, `le`, `ge`, `in`
- Supports string operators: `contains`, `startsWith`, `endsWith`
- Supports nullability checks via the `isSet` operator
- Supports relation-aware mappings, including nested relations and many-to-many joins
- Works with Drizzle SQLite, PostgreSQL, MySQL and PlanetScale drivers

## Installation

```bash
npm install @cerbos/orm-drizzle
```

## Usage

```ts
import { queryPlanToDrizzle, PlanKind } from "@cerbos/orm-drizzle";
import { eq, and } from "drizzle-orm";
import { resources } from "./schema";

const plan = await cerbos.planResources({
  principal,
  resource,
  action,
});

const result = queryPlanToDrizzle({
  queryPlan: plan,
  mapper: {
    "request.resource.attr.status": resources.status,
    "request.resource.attr.owner": resources.ownerId,
  },
});

if (result.kind === PlanKind.CONDITIONAL) {
  const rows = await db
    .select()
    .from(resources)
    .where(and(eq(resources.deleted, false), result.filter));
}
```

### Mapper options

The mapper associates Cerbos attribute references with Drizzle columns. It can be:

- A plain object where keys are Cerbos attribute references and values are Drizzle columns or SQL expressions.
- A function receiving the attribute reference and returning the column/expression.
- An object with a `column` property and an optional `transform` function to customize how operator/value pairs are converted into SQL.

```ts
const result = queryPlanToDrizzle({
  queryPlan,
  mapper: {
    "request.resource.attr.custom": {
      column: sql`lower(${resources.title})`,
      transform: ({ operator, value }) => {
        if (operator !== "eq") throw new Error("Unsupported");
        return eq(sql`lower(${resources.title})`, value.toLowerCase());
      },
    },
  },
});
```

### Mapping relations

Relations can be described using the `relation` option, mirroring the structure of the Prisma adapter. The adapter will wrap
comparisons in `EXISTS` subqueries and automatically infer relation fields when they match the column names on the related table.

```ts
const result = queryPlanToDrizzle({
  queryPlan,
  mapper: {
    "request.resource.attr.owner": {
      relation: {
        type: "one",
        table: owners,
        sourceColumn: resources.ownerId,
        targetColumn: owners.id,
        fields: {
          email: owners.email,
        },
      },
    },
    "request.resource.attr.tags": {
      relation: {
        type: "many",
        table: resourceTags,
        sourceColumn: resources.id,
        targetColumn: resourceTags.resourceId,
        fields: {
          name: {
            relation: {
              type: "one",
              table: tags,
              sourceColumn: resourceTags.tagId,
              targetColumn: tags.id,
              field: tags.name,
            },
          },
        },
      },
    },
  },
});
```

With the above mapper, query plan references such as `request.resource.attr.owner.email` and `request.resource.attr.tags.name`
are translated into `EXISTS` expressions that join the `owners` and `tags` tables respectively.

## Testing

The project ships with a comprehensive test suite that exercises all supported operators using an in-memory SQLite database and the official Drizzle ORM query builder.
