# Cerbos + Convex Adapter

An adapter library that takes a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into a [Convex](https://convex.dev/) filter function. It is designed to run alongside a project that is already using the [Cerbos JavaScript SDK](https://github.com/cerbos/cerbos-sdk-javascript) to fetch query plans so that authorization logic can be pushed down to Convex queries.

## How it works

1. Use a Cerbos client (`@cerbos/http` or `@cerbos/grpc`) to call `planResources` and obtain a `PlanResourcesResponse`.
2. Provide `queryPlanToConvex` with that plan and an optional mapper that describes how Cerbos attribute paths relate to your Convex document fields.
3. The adapter walks the Cerbos expression tree and returns `{ kind, filter?, postFilter? }`:
   - `filter` is a Convex-native filter function `(q) => Expression<boolean>` pushed to the DB.
   - `postFilter` is a JS predicate `(doc) => boolean` for operators Convex can't express natively (string ops, collection ops). **Requires `allowPostFilter: true`** and must run in trusted backend code before any document is returned — see below.
4. Inspect `result.kind`:
   - `ALWAYS_ALLOWED`: the caller can query without any additional filters.
   - `ALWAYS_DENIED`: short-circuit and return an empty result set.
   - `CONDITIONAL`: apply `result.filter` in the Convex query, then apply `result.postFilter` to every candidate inside the trusted backend before returning results (see usage example below).

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
| Collection | `hasIntersection`, `index`, `get-field`, `size` | CEL-compatible collection and nested-field evaluation |
| Quantifiers | `exists`, `exists_one`, `all` | CEL-compatible lambda evaluation, including empty collections, missing members, and literal value-list collections |
| Higher-order | `filter`, `map`, `lambda` | Collection filtering and projection inside larger expressions |
| Arithmetic | `add`, `sub`, `mult`, `div`, `mod` | Numeric evaluation with CEL error propagation |
| Conversion | `string`, `double`, `int`, `timestamp` | Scalar conversion and RFC 3339 timestamp comparison |
| Conditional | `if` | Evaluates only the selected branch |
| Pattern | `matches` | Constant patterns in the safe common RE2/JavaScript subset: literals, `^`/`$` anchors, and a trailing `.*` when no `$` end anchor is present |
| Hierarchy | `hierarchy`, `ancestorOf`, `descendentOf`, `overlaps` | Delimiter-aware hierarchy comparison |

For mixed expressions (e.g. `and(eq(...), contains(...))`), the adapter splits the tree: DB-pushable children go to `filter`, the rest go to `postFilter`. For `or(...)` with any unsupported child, the entire expression goes to `postFilter` (partial OR push-down would miss results).

#### Quantifiers over known collections

A quantifier whose collection the PDP resolves at plan time — typically a
principal attribute, as in `P.attr.teams.exists(t, R.attr.team == t)` — reaches
the adapter in one of two wire shapes. The Cerbos planner unrolls it into a
plain `or`/`and` chain at 10 elements or fewer, which pushes into the DB filter,
and ships the lambda with a literal value-list collection above that
(`maxItems = 10` in the planner's struct matcher; cerbos/cerbos#2570,
cerbos/cerbos#2817), which evaluates as a `postFilter`. Because the adapter is
an interpreter rather than a compiler, the literal collection is bound
per-element like any other, so both shapes yield the same decisions — including
`exists_one` cardinality, which compiling adapters cannot express. Only the
push-down changes: above the threshold the predicate no longer reduces the
documents read, so `allowPostFilter: true` is required.

### `allowPostFilter` opt-in

By default, `queryPlanToConvex` throws an error when the query plan requires a `postFilter`. This is because post-filter operators cause documents to be read before the complete authorization predicate is applied — the DB-level filter alone may not fully enforce the authorization policy.

To enable post-filtering, pass `allowPostFilter: true`:

```ts
const { kind, filter, postFilter } = queryPlanToConvex({
  queryPlan,
  mapper,
  allowPostFilter: true,
});
```

> **Security requirement:** `postFilter` is part of the authorization predicate. Run it in the same trusted Convex/backend function and apply it to every candidate before serialization or return. Never send unfiltered candidates to a browser or other untrusted client for filtering.

If your Cerbos policies only use operators that Convex supports natively (comparisons, `in`, `isSet`, logical combinators), you don't need this flag — `filter` alone will enforce the full policy at the DB level.

## Conformance contract

The adapter is differentially tested with 20 hostile seed documents against Cerbos PDP 0.54.0 `checkResource` decisions: each query plan is executed by Convex, and the returned document IDs must equal the PDP's per-document decisions. The Spring Data adapter defines the reference semantics for this compatibility snapshot.

| Classification | Coverage |
| --- | --- |
| Oracle-tested | All 118 reference conformance actions, plus `matches()`, list indexing/`get-field`, and `timestamp()` plans that the Spring Data reference adapter rejects (121 actions total) |
| Fail-closed | No corpus shape when `allowPostFilter: true`; unknown operators and invalid expression structures still throw |
| Explicit opt-in | Any plan that cannot be represented entirely as a Convex database filter requires `allowPostFilter: true` |
| Known planner divergence | `has()` on a missing attribute is currently folded by the Cerbos planner to `ALWAYS_ALLOWED`; `checkResource` still denies documents where the attribute is missing. Until the planner is fixed, use `R.attr.x != null` for database-backed attributes instead of `has(R.attr.x)` |

This support statement includes value-first comparisons, field-to-field expressions, null and missing-attribute behavior, nested lambdas, collection macros, string and arithmetic expressions, timestamps, hierarchy operations, and chained nested fields. Fields that may be absent must be marked `nullable: true` in the mapper so the adapter evaluates their predicates with CEL-compatible missing-value semantics instead of pushing them to a Convex filter.

## Requirements

- Cerbos > v0.16 plus either the `@cerbos/http` or `@cerbos/grpc` client

## System Requirements

- Node.js >= 22.0.0
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
import type { Expression, FilterBuilder } from "convex/server";
import type { DataModel } from "./_generated/dataModel";

const { kind, filter, postFilter } = queryPlanToConvex<
  FilterBuilder<DataModel["myTable"]>,
  Expression<boolean>
>({
  queryPlan, // PlanResourcesResponse obtained by trusted backend code
  mapper, // optional Mapper - see below
  allowPostFilter: true, // opt in to trusted-backend filtering (see note below)
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
  nullable?: boolean;
};

export type Mapper =
  | Record<string, MapperConfig>
  | ((key: string) => MapperConfig);
```

- `field` rewrites a single Cerbos path to a different field name in your Convex document. Dot-notation is supported for nested fields.
- `nullable` declares that the document field may be absent. Predicates involving that field are evaluated by the post-filter so missing values cannot be mistaken for ordinary Convex comparison results.

If you omit the mapper the adapter will use the query plan paths verbatim.

#### Direct fields

```ts
const mapper: Mapper = {
  "request.resource.attr.aBool": { field: "aBool" },
  "request.resource.attr.title": { field: "title" },
  "request.resource.attr.nested.value": { field: "metadata.value" },
  "request.resource.attr.optionalOwner": {
    field: "optionalOwner",
    nullable: true,
  },
};
```

#### Mapper functions

You can also supply a function if your mappings follow a predictable pattern:

```ts
const mapper: Mapper = (path) => ({
  field: path.replace("request.resource.attr.", ""),
});
```

## Trusted usage pattern

Call Cerbos from a trusted Convex action or from your application backend. Pass the resulting query plan to an `internalQuery` that applies both the Convex filter and any `postFilter`. Do not expose a public query that accepts a caller-supplied plan: an untrusted caller could submit an `ALWAYS_ALLOWED` plan.

```ts
import type { PlanResourcesResponse } from "@cerbos/core";
import {
  queryPlanToConvex,
  PlanKind,
  type Mapper,
} from "@cerbos/orm-convex";
import { internalQuery, type Expression, type FilterBuilder } from "convex/server";
import { v } from "convex/values";
import type { DataModel } from "./_generated/dataModel";

const mapper: Mapper = {
  "request.resource.attr.title": { field: "title" },
  "request.resource.attr.status": { field: "status" },
  "request.resource.attr.priority": { field: "priority" },
};

export const executePlan = internalQuery({
  args: { queryPlan: v.any() },
  handler: async (ctx, { queryPlan }) => {
    // This internal-only argument must be the response returned directly by Cerbos.
    const { kind, filter, postFilter } = queryPlanToConvex<
      FilterBuilder<DataModel["documents"]>,
      Expression<boolean>
    >({
      queryPlan: queryPlan as PlanResourcesResponse,
      mapper,
      allowPostFilter: true,
    });

    if (kind === PlanKind.ALWAYS_DENIED) return [];
    if (kind === PlanKind.ALWAYS_ALLOWED && !postFilter) {
      return await ctx.db.query("documents").collect();
    }

    let query = ctx.db.query("documents");
    if (filter) query = query.filter(filter);
    let results = await query.collect();
    if (postFilter) results = results.filter(postFilter);
    return results;
  },
});
```

## Error handling

`queryPlanToConvex` throws descriptive errors in the following scenarios:

- The plan kind is not one of the Cerbos `PlanKind` values (`Invalid query plan.`).
- A conditional plan omits the `operator`/`operands` structure (`Invalid Cerbos expression structure`).
- An operator listed in the plan is not implemented by this adapter (`Unsupported operator for Convex: <name>` or `Unsupported operator: <name>`).
- The `in` operator is given a non-array value.
- The query plan requires trusted-backend filtering and `allowPostFilter` is not set to `true`.

## Limitations

- String and collection operators (`contains`, `startsWith`, `endsWith`, `hasIntersection`, `exists`, `all`, etc.) are evaluated as a JavaScript `postFilter` after the DB query returns candidates. This means these conditions do not reduce the number of documents read from the database; they must still run in trusted backend code before any candidate is returned.
- For `or(...)` expressions where any child uses an unsupported operator, the entire OR is evaluated in the trusted backend via `postFilter`. Only `and(...)` expressions can be split between DB filter and post-filter.
- The `in` operator is composed as multiple `eq` comparisons joined with `or`, which may be less efficient for large value lists.
- `matches` rejects dynamic patterns and regex syntax outside the documented safe subset. This avoids JavaScript-only regex semantics and regular-expression denial of service in the trusted backend.
- Type conversions accept only CEL-compatible source types and strict string formats. JavaScript-only coercions such as `Number(true)`, `Number("")`, or `String(null)` fail closed.
