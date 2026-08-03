# Cerbos + Drizzle ORM Adapter

An adapter library that takes a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into a [Drizzle ORM](https://orm.drizzle.team/) SQL expression. This allows you to use Cerbos query plans directly inside your Drizzle queries.

## Features

- Supports logical operators: `and`, `or`, `not`
- Supports comparison operators: `eq`, `ne`, `lt`, `gt`, `le`, `ge`, `in`
- Supports string operators: `contains`, `startsWith`, `endsWith`
- Supports nullability checks via the `isSet` operator
- Supports set-aware operators such as `hasIntersection`, `exists`, `exists_one`, and `all`
- Supports relation-aware mappings, including nested relations and many-to-many joins
- Works with Drizzle SQLite, PostgreSQL, MySQL and PlanetScale drivers

## Conformance contract

The adapter is differentially tested against Cerbos PDP 0.54.0 `checkResource` decisions using 20 hostile seed rows and real Drizzle queries. The Spring Data adapter defines the reference semantics for this compatibility snapshot.

| Classification | Coverage |
| --- | --- |
| Oracle-tested | 112 reference conformance actions |
| Fail-closed corpus shapes | Sub-millisecond `now()` thresholds plus regex `matches()`, ordered list indexing/`get-field`, and `timestamp()` over an untyped string field (5 actions) |
| Known planner divergence | `has()` on a missing attribute is folded by the Cerbos planner to `ALWAYS_ALLOWED`, while `checkResource` denies the missing-attribute rows. Until the planner is fixed, use `R.attr.x != null` for database-backed attributes instead of `has(R.attr.x)` |

The oracle coverage includes value-first and field-to-field comparisons, escaped string predicates, relation counts and nested collection macros, null/error propagation, arithmetic and ternaries, hierarchy operations, typed timestamps, and multi-hop relations. The five fail-closed shapes throw rather than return a broader SQL filter. `matches()` is rejected because SQL regex dialects do not guarantee CEL/RE2 semantics.

## How it works

Cerbos can respond to a `PlanResources` request with one of three plan kinds. The adapter mirrors that API:

- `PlanKind.ALWAYS_ALLOWED`: The user can access the resource without any extra filtering.
- `PlanKind.ALWAYS_DENIED`: The user cannot access the resource at all.
- `PlanKind.CONDITIONAL`: Cerbos returns an expression tree that must be applied when reading data. The adapter converts this expression into a Drizzle SQL filter.

`queryPlanToDrizzle` walks the Cerbos expression, resolves every attribute reference through the mapper, and produces a Drizzle `SQL` fragment. That fragment can then be composed with the rest of your query builder chain (`db.select().from(table).where(result.filter)`).

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

### Handling different plan kinds

```ts
const evaluation = queryPlanToDrizzle({ queryPlan: plan, mapper });

switch (evaluation.kind) {
  case PlanKind.ALWAYS_ALLOWED:
    // run the query without extra filters
    break;
  case PlanKind.ALWAYS_DENIED:
    // return an empty result immediately
    break;
  case PlanKind.CONDITIONAL:
    const rows = await db
      .select()
      .from(resources)
      .where(evaluation.filter);
    break;
}
```

Cerbos plans reference both resources (`request.resource.attr.*`) and principals (`request.principal.attr.*`), so include the paths your policies emit in the mapper.

### Database collation requirement

> **Every mapped string column must use a binary or case-sensitive collation.** CEL
> string comparison is exact and case-sensitive, while MySQL and PlanetScale commonly
> default to case-insensitive collations. With a CI collation, a database predicate can
> return `"Finance"` for a policy that allowed only `"finance"`, silently over-granting
> access compared with the PDP's `check()` decision.

On MySQL use a collation such as `utf8mb4_bin` or `utf8mb4_0900_as_cs`. PostgreSQL is
case-sensitive by default, but nondeterministic ICU collations and `citext` are not safe
for mapped policy attributes. On SQLite, do not apply `COLLATE NOCASE` to mapped columns.
This requirement covers equality and ordering, `in`, intersections, string matching, and
hierarchy prefix/ancestor comparisons.

### Mapper options

The mapper associates Cerbos attribute references with Drizzle columns. It can be:

- A plain object where keys are Cerbos attribute references and values are Drizzle columns or SQL expressions.
- A function receiving the attribute reference and returning the column/expression.
- An object with a `column` property and optional metadata or a `transform` function to customize how operator/value pairs are converted into SQL.

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

### Attribute references and functions

- Plain values: map `request.resource.attr.field` to a column (`resources.field`).
- Nested attributes: map longer paths such as `request.resource.attr.owner.email`.
- Principal attributes: map `request.principal.attr.role` or similar paths when policies check the caller.
- Dynamic resolution: pass a mapper function `(reference) => ...` to compute mappings at runtime.

Every mapper entry can be:

- A column or SQL fragment.
- An object with `column` and/or `transform` to customize how each operator is translated.
- A relation mapping (described below) for nested resource structures.

Fields used through CEL's `timestamp()` must opt in with `valueType: "timestamp"`.
The adapter then validates strict RFC 3339 constants and normalizes them to UTC before comparing them. The instant must fall inside CEL's supported year 0001–9999 range and be exactly representable at millisecond precision: fractional digits after the third must be zero. The mapped column and database must preserve the same precision.

```ts
"request.resource.attr.createdAt": {
  column: resources.createdAt,
  valueType: "timestamp",
}
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

### Working with collections

- `hasIntersection`: Use for multi-valued attributes such as tags. When Cerbos emits `hasIntersection(map(resource.tags, lambda t => t.name), ["tag"])`, the mapper looks up the nested field and the adapter converts it into a `column IN (...)` condition.
- `exists`, `exists_one`, and `all`: When policies reference array attributes (e.g., `request.resource.attr.tags`), mark the mapper entry as a relation. The adapter scopes the lambda variable, generates the `EXISTS` subquery, and correlates it with the parent table automatically.
- Scalar collections stored through a relation can opt in with `collectionValueType: "scalar"` and set the relation's `field`. This enables direct membership such as `R.attr.owner in R.attr.tagNames`, including explicit `null` elements.
- `filter`: Cerbos uses `filter` during plan construction. The adapter discards those lambdas because the entire filter is rerun in Drizzle land.

## Testing

The project ships with a comprehensive test suite that exercises all supported operators using an in-memory SQLite database and the official Drizzle ORM query builder.
