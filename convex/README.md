# Cerbos + Convex Adapter

An adapter library that takes a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into a [Convex](https://convex.dev/) filter function. It is designed to run alongside a project that is already using the [Cerbos JavaScript SDK](https://github.com/cerbos/cerbos-sdk-javascript) to fetch query plans so that authorization logic can be pushed down to Convex queries.

## How it works

1. Use a Cerbos client (`@cerbos/http` or `@cerbos/grpc`) to call `planResources` and obtain a `PlanResourcesResponse`.
2. Provide `queryPlanToConvex` with that plan and an optional mapper that describes how Cerbos attribute paths relate to your Convex document fields.
3. The adapter walks the Cerbos expression tree and returns `{ kind, filter?, postFilter? }`:
   - `filter` is a Convex-native filter function `(q) => Expression<boolean>` pushed to the DB.
   - `postFilter` is a JS predicate `(doc) => boolean` for operators Convex can't express natively (string ops, collection ops).
4. Inspect `result.kind`:
   - `ALWAYS_ALLOWED`: the caller can query without any additional filters.
   - `ALWAYS_DENIED`: short-circuit and return an empty result set.
   - `CONDITIONAL`: apply `result.filter` server-side and `result.postFilter` client-side (see usage example below).

## Supported operators

| Category | Operators | Behavior |
| --- | --- | --- |
| Logical | `and`, `or`, `not` | Builds `q.and(...)`, `q.or(...)`, `q.not(...)` groups. |
| Comparisons | `eq`, `ne`, `lt`, `le`, `gt`, `ge` | Emits `q.eq`, `q.neq`, `q.lt`, `q.lte`, `q.gt`, `q.gte` against the mapped field. |
| Membership | `in` | Composed as `q.or(q.eq(field, v1), q.eq(field, v2), ...)`. |
| Existence | `isSet` | Uses `q.neq(field, undefined)` for set, `q.eq(field, undefined)` for unset. |

### Post-filter operators

The following operators cannot be expressed as Convex DB filters. When the adapter encounters them, it returns a `postFilter` function that evaluates them in JavaScript against each document:

| Category | Operators | JS Behavior |
| --- | --- | --- |
| String | `contains`, `startsWith`, `endsWith` | `String.prototype.includes` / `startsWith` / `endsWith` |
| Collection | `hasIntersection` | `a.some(v => b.includes(v))` |
| Quantifiers | `exists`, `exists_one`, `all` | `Array.prototype.some` / filter-count / `every` with lambda |
| Higher-order | `filter`, `map`, `lambda` | Used internally by quantifier operators |

For mixed expressions (e.g. `and(eq(...), contains(...))`), the adapter splits the tree: DB-pushable children go to `filter`, the rest go to `postFilter`. For `or(...)` with any unsupported child, the entire expression goes to `postFilter` (partial OR push-down would miss results).

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

const { kind, filter, postFilter } = queryPlanToConvex({
  queryPlan, // PlanResourcesResponse from Cerbos
  mapper, // optional Mapper - see below
});

if (kind === PlanKind.ALWAYS_DENIED) return [];
if (kind === PlanKind.ALWAYS_ALLOWED && !postFilter) {
  return await ctx.db.query("myTable").collect();
}

let query = ctx.db.query("myTable");
if (filter) query = query.filter(filter);
let results = await query.collect();
if (postFilter) results = results.filter(postFilter);
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

const { kind, filter, postFilter } = queryPlanToConvex({ queryPlan, mapper });

if (kind === PlanKind.ALWAYS_DENIED) {
  return [];
}

if (kind === PlanKind.ALWAYS_ALLOWED && !postFilter) {
  return await ctx.db.query("documents").collect();
}

let query = ctx.db.query("documents");
if (filter) query = query.filter(filter);
let results = await query.collect();
if (postFilter) results = results.filter(postFilter);
return results;
```

## Error handling

`queryPlanToConvex` throws descriptive errors in the following scenarios:

- The plan kind is not one of the Cerbos `PlanKind` values (`Invalid query plan.`).
- A conditional plan omits the `operator`/`operands` structure (`Invalid Cerbos expression structure`).
- An operator listed in the plan is not implemented by this adapter (`Unsupported operator for Convex: <name>` or `Unsupported operator: <name>`).
- The `in` operator is given a non-array value.

## Limitations

- String and collection operators (`contains`, `startsWith`, `endsWith`, `hasIntersection`, `exists`, `all`, etc.) are evaluated as a JavaScript `postFilter` after the DB query returns. This means these conditions do not reduce the number of documents read from the database.
- For `or(...)` expressions where any child uses an unsupported operator, the entire OR is evaluated client-side via `postFilter`. Only `and(...)` expressions can be split between DB filter and post-filter.
- The `in` operator is composed as multiple `eq` comparisons joined with `or`, which may be less efficient for large value lists.
