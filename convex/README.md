# Cerbos + Convex Adapter

An adapter library that takes a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into a [Convex](https://convex.dev/) filter function. It is designed to run alongside a project that is already using the [Cerbos JavaScript SDK](https://github.com/cerbos/cerbos-sdk-javascript) to fetch query plans so that authorization logic can be pushed down to Convex queries.

## How it works

1. Use a Cerbos client (`@cerbos/http` or `@cerbos/grpc`) to call `planResources` and obtain a `PlanResourcesResponse`.
2. Provide `queryPlanToConvex` with that plan and an optional mapper that describes how Cerbos attribute paths relate to your Convex document fields.
3. The adapter walks the Cerbos expression tree, translates supported operators into a Convex filter function `(q) => Expression<boolean>`, and returns `{ kind, filter? }`.
4. Inspect `result.kind`:
   - `ALWAYS_ALLOWED`: the caller can query without any additional filters.
   - `ALWAYS_DENIED`: short-circuit and return an empty result set.
   - `CONDITIONAL`: pass `result.filter` to `ctx.db.query("table").filter(result.filter)`.

## Supported operators

| Category | Operators | Behavior |
| --- | --- | --- |
| Logical | `and`, `or`, `not` | Builds `q.and(...)`, `q.or(...)`, `q.not(...)` groups. |
| Comparisons | `eq`, `ne`, `lt`, `le`, `gt`, `ge` | Emits `q.eq`, `q.neq`, `q.lt`, `q.lte`, `q.gt`, `q.gte` against the mapped field. |
| Membership | `in` | Composed as `q.or(q.eq(field, v1), q.eq(field, v2), ...)`. |
| Existence | `isSet` | Uses `q.neq(field, undefined)` for set, `q.eq(field, undefined)` for unset. |

### Unsupported operators

The following Cerbos operators are not supported by the Convex filter API and will throw an error:

- **String helpers:** `contains`, `startsWith`, `endsWith`
- **Collection helpers:** `hasIntersection`, `exists`, `exists_one`, `all`, `filter`, `map`, `lambda`

If your Cerbos policies use these operators, consider restructuring the policy conditions to use only supported operators, or filtering results in application code after the Convex query.

## Requirements

- Cerbos > v0.16 plus either the `@cerbos/http` or `@cerbos/grpc` client

## System Requirements

- Node.js >= 20.0.0
- Convex 1.x

## Installation

```bash
npm install @cerbos/orm-convex
```

## API

```ts
import {
  queryPlanToConvex,
  PlanKind,
  type Mapper,
} from "@cerbos/orm-convex";

const result = queryPlanToConvex({
  queryPlan, // PlanResourcesResponse from Cerbos
  mapper, // optional Mapper - see below
});

if (result.kind === PlanKind.CONDITIONAL) {
  const documents = await ctx.db
    .query("myTable")
    .filter(result.filter)
    .collect();
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

The Cerbos query plan references fields using paths such as `request.resource.attr.title`. Use a mapper to translate those names to the field names in your Convex documents.

```ts
export type MapperConfig = {
  field?: string;
};

export type Mapper =
  | Record<string, MapperConfig>
  | ((key: string) => MapperConfig);
```

- `field` rewrites a single Cerbos path to a different field name in your Convex document. Dot-notation is supported for nested fields.

If you omit the mapper the adapter will use the query plan paths verbatim.

#### Direct fields

```ts
const mapper: Mapper = {
  "request.resource.attr.aBool": { field: "aBool" },
  "request.resource.attr.title": { field: "title" },
  "request.resource.attr.nested.value": { field: "metadata.value" },
};
```

#### Mapper functions

You can also supply a function if your mappings follow a predictable pattern:

```ts
const mapper: Mapper = (path) => ({
  field: path.replace("request.resource.attr.", ""),
});
```

## Usage example

```ts
import { GRPC as Cerbos } from "@cerbos/grpc";
import {
  queryPlanToConvex,
  PlanKind,
  type Mapper,
} from "@cerbos/orm-convex";

const cerbos = new Cerbos("localhost:3592", { tls: false });

const mapper: Mapper = {
  "request.resource.attr.title": { field: "title" },
  "request.resource.attr.status": { field: "status" },
  "request.resource.attr.priority": { field: "priority" },
};

// Inside a Convex query function:
const queryPlan = await cerbos.planResources({
  principal: { id: "user1", roles: ["USER"] },
  resource: { kind: "document" },
  action: "view",
});

const result = queryPlanToConvex({ queryPlan, mapper });

if (result.kind === PlanKind.ALWAYS_DENIED) {
  return [];
}

if (result.kind === PlanKind.CONDITIONAL) {
  return await ctx.db
    .query("documents")
    .filter(result.filter)
    .collect();
}

return await ctx.db.query("documents").collect();
```

## Error handling

`queryPlanToConvex` throws descriptive errors in the following scenarios:

- The plan kind is not one of the Cerbos `PlanKind` values (`Invalid query plan.`).
- A conditional plan omits the `operator`/`operands` structure (`Invalid Cerbos expression structure`).
- An operator listed in the plan is not implemented by this adapter (`Unsupported operator for Convex: <name>` or `Unsupported operator: <name>`).
- The `in` operator is given a non-array value.

## Limitations

- Convex filter expressions do not support string operations (`contains`, `startsWith`, `endsWith`), so policies using those operators cannot be translated.
- Collection operators (`exists`, `all`, `filter`, `map`, `lambda`, `hasIntersection`) are not supported because Convex is document-oriented and does not support joins or array sub-queries in its filter API.
- The `in` operator is composed as multiple `eq` comparisons joined with `or`, which may be less efficient for large value lists.
