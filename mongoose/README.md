# Cerbos + Mongoose ORM Adapter

An adapter library that takes a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into a [Mongoose](https://mongoosejs.com/) filter. It is designed to run alongside a project that is already using the [Cerbos JavaScript SDK](https://github.com/cerbos/cerbos-sdk-javascript) to fetch query plans so that authorization logic can be pushed down to MongoDB.

## How it works

1. Use a Cerbos client (`@cerbos/http` or `@cerbos/grpc`) to call `planResources` and obtain a `PlanResourcesResponse`.
2. Provide `queryPlanToMongoose` with that plan and an optional mapper that describes how Cerbos attribute paths relate to your document schema.
3. The adapter walks the Cerbos expression tree, translates supported operators to MongoDB syntax, and returns `{ kind, filters? }`.
4. Inspect `result.kind`:
   - `ALWAYS_ALLOWED`: the caller can query without any additional filters.
   - `ALWAYS_DENIED`: short-circuit and return an empty result set.
   - `CONDITIONAL`: execute the query with `result.filters`.

You can merge the adapter output with existing application filters (for example, via `$and`) before issuing the Mongoose query.

## Supported operators

| Category | Operators | Behavior |
| --- | --- | --- |
| Logical | `and`, `or`, `not` | Builds `$and`, `$or`, and `$nor` groups. |
| Comparisons | `eq`, `ne`, `lt`, `le`, `gt`, `ge` | Emits `$eq`, `$ne`, `$lt`, `$lte`, `$gt`, `$gte` checks against the mapped field. |
| Membership | `in`, `hasIntersection` | `$in` on simple lists, or `$elemMatch` when targeting array relations; `hasIntersection` supports either a direct array field or a `map` projection inside the plan. |
| String helpers | `contains`, `startsWith`, `endsWith` | Generates escaped regular expressions that target substrings, prefixes, or suffixes. |
| Existence helpers | `eq`/`ne` against `null`, `exists` | Null checks arrive as `eq`/`ne` against a null value — the planner emits no existence operator. Uses `$eq: null`/`$ne: null` for scalars and `$elemMatch` for collections. |
| Collection helpers | `filter`, `lambda`, `map`, `all` | Translates Cerbos collection expressions into scoped `$elemMatch` filters and maps lambda variables to the correct nested paths. `all` requires the stored field to be an array. `exists`/`all` over a *literal* value-list collection (a folded principal attribute above the planner's 10-element unroll cap) fold to `$or`/`$and` of the substituted lambda body instead — no relation mapping needed. |
| Arithmetic and values | `add`, `sub`, `mult`, `div`, `mod`, `if`, `size`, `index`, `get-field` | Uses document-level MongoDB `$expr` expressions. Division requires a non-zero constant denominator; indexing requires a non-negative integer constant and adds a per-document bounds check. |
| Conversions and matching | `string`, `double`, `int`, `timestamp`, `matches` | Uses guarded MongoDB conversion and regular-expression expressions. Timestamps accept BSON dates or millisecond-exact RFC 3339 strings in the CEL instant range. Regex matching accepts a validated common RE2/PCRE2 subset. |
| Hierarchies | `hierarchy`, `ancestorOf`, `descendentOf`, `overlaps` | Translates a mapped scalar hierarchy path using literal prefix and ancestor-list filters. |

Any operator not listed above causes `queryPlanToMongoose` to throw `Unsupported operator: <name>`.

`exists_one` fails loudly because a normal match filter cannot preserve exact-match cardinality and CEL error semantics for nullable elements.

Conversions fail closed when the stored BSON type is outside CEL's compatible source types or when parsing fails. `double` and `int` accept strings and numeric BSON values, but not booleans; `int` also rejects BSON dates rather than interpreting them as milliseconds. `string` accepts strings, booleans, and numeric BSON values. A failed conversion remains denied under negation.

Timestamp values must fall in CEL's UTC instant range (`0001-01-01T00:00:00Z` through `9999-12-31T23:59:59.999Z`). Strings must use RFC 3339 syntax with no more than three fractional-second digits, matching BSON Date's millisecond precision. Higher-precision or out-of-range strings and BSON dates fail closed instead of being silently truncated into a different CEL instant.

`matches` supports literals, `.`, the `*`, `+`, and `?` quantifiers, leading `^`, terminal `$`, and escaped regex metacharacters. Other constructs fail closed. A terminal `$` is translated to PCRE2's absolute end-of-text anchor so MongoDB cannot match before a final newline, preserving RE2 semantics.

## NULL attribute representation

`R.attr.x == null` compiles to the same `eq(x, null)` plan node however your application represents
a NULL field in the attributes it sends to `check()`, so the adapter cannot infer the convention
and has to be told which one you use.

| attributes you send for a NULL field | `check()` on that document | null-matching filter |
| --- | --- | --- |
| `{"x": null}` — explicit null | allow | selects it — aligned |
| `{}` — attribute omitted | **deny** (CEL missing-attribute error) | selects it — **over-grants** |

``nullAttributeRepresentation`` defaults to ``"explicit"``, preserving the historical translation. If your application
omits attributes for NULL fields, set it to ``"omitted"``: the adapter then rejects every null
comparison operand instead of emitting a filter that returns documents the PDP denies.

```ts
queryPlanToMongoose({ queryPlan, mapper, nullAttributeRepresentation: "omitted" });
```

The rejection is deliberately wider than the shapes that actually over-grant — `x != null` and
`!(x == null)` are aligned under both conventions — because negation is applied by wrapping the
built filter rather than pushing it into the leaf, so a leaf cannot tell whether an enclosing
`not` will flip a not-null predicate back into a null-selecting one. Rejecting every null operand
is correct under any nesting. See
[#302](https://github.com/cerbos/query-plan-adapters/issues/302).

## Conformance contract

The adapter is differentially tested against Cerbos PDP 0.54.0 `checkResource` decisions using 20 hostile seed documents and real MongoDB 7 and 8 queries. The Spring Data adapter defines the reference semantics for this compatibility snapshot.

| Classification | Coverage |
| --- | --- |
| Oracle-tested | 103 reference conformance actions plus regex, ordered indexing/`get-field`, and timestamp probes (106 actions) |
| Fail-closed | 38 reference actions plus the 5 reference-unsupported shapes (43 actions total) |
| Representation-dependent | `null-eq-missing` — rejected under `nullAttributeRepresentation: "omitted"`. Under the default it already returns the empty set the PDP demands, because `nullable: true` on a mapper entry declares per-attribute that a stored null is a missing Cerbos attribute; the global option is the backstop for mappings that do not declare it |
| Attribute NULL convention | Needs no declaration: Mongoose stores the value the caller sent, so a stored null already compares as a null *value* exactly as CEL does. The four `null-value-*` corpus probes for the explicit convention (cerbos/query-plan-adapters#308) were aligned before that option existed; the fifth is refused by the pre-existing negated-collection-macro limitation, not by the null convention |
| Known planner divergence | `has()` on a missing attribute is folded by the Cerbos planner to `ALWAYS_ALLOWED`, while `checkResource` denies the missing-attribute documents. Until the planner is fixed, use `R.attr.x != null` for database-backed attributes instead of `has(R.attr.x)` |

The fail-closed set covers exact-one cardinality, aggregation expressions or outer-document references inside `$elemMatch`, nested collection counts, correlated variable-in-variable membership, unsafe division/non-finite arithmetic, and negated nullable collection predicates that cannot preserve CEL's three-valued error semantics. These plans throw instead of silently degrading to a weaker MongoDB filter. Every fail-closed shape's error message is pinned in the shared corpus (`conformance/actions.json`) and asserted by this adapter's conformance run, so a classification proves the throw names its declared mechanism rather than merely that something threw.

## Mapping hazards

The conformance contract above proves the *plan* side — given a policy shape, does the filter select the documents `check()` allows. The other half is the *mapping*: **the documents the filter reads must be the documents the application put into the resource attributes.** Six ways that can break are catalogued in the shared corpus, and every adapter has to record a position on each of them.

This adapter **builds no subquery.** A relation is a path inside the same document, so `find()` reads exactly the document the application serialised. The adapter emits no `$lookup` and no `$graphLookup`, and never calls `populate()` or `aggregate()` — `src/adversarial.test.ts` ("emits no `$lookup` and reaches no second collection") asserts that against both the source and every emitted filter, because five of the six rows below are only "not applicable" for as long as it stays true.

| Hazard | Position | Mechanism to check |
|---|---|---|
| Filtered association | Not applicable — no subquery | — |
| Default scope on the target model | Not applicable — no second collection is read | — |
| Subtype discrimination | **Caller-owned** | `Model.discriminator(...)`. Run the filter on the same model the application read the attributes from. Discriminated models share one collection, so a filter executed against the *base* model matches the other subtypes' documents — the `__t` criterion Mongoose adds for the discriminator model is not in the filter the adapter returns, and cannot be: the plan does not say which model the caller will use |
| To-one relation used as a collection | Not applicable — a document path holds exactly what the application stored | — |
| Composite association key | Not applicable — no join, so no key to compose | — |
| Absent to-one parent | **Reproduced**, and proved by the corpus (`w1-all-chain` and siblings) | `relation.requiresParent` — declare the optional to-one parent a flattened path is reached through, so `size(chain)` comparisons yield null rather than 0 for a document that has no parent ([#309](https://github.com/cerbos/query-plan-adapters/issues/309)) |

## Requirements

- Cerbos > v0.16 plus either the `@cerbos/http` or `@cerbos/grpc` client

## System Requirements

- Node.js >= 22.0.0
- Mongoose 9.x
- MongoDB 7.0 or newer

## Installation

```bash
npm install @cerbos/orm-mongoose
```

## API

```ts
import {
  queryPlanToMongoose,
  PlanKind,
  type Mapper,
} from "@cerbos/orm-mongoose";

const result = queryPlanToMongoose({
  queryPlan, // PlanResourcesResponse from Cerbos
  mapper, // optional Mapper - see below
});

if (result.kind === PlanKind.CONDITIONAL) {
  await MyModel.find(result.filters);
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

The Cerbos query plan references fields using paths such as `request.resource.attr.title`. Use a mapper to translate those names to the paths in your Mongoose models and to describe relations/collections so the adapter can generate `$elemMatch` filters when needed.

```ts
export type MapperConfig = {
  field?: string;
  nullable?: boolean;
  valueParser?: (value: any) => any;
  relation?: {
    name: string;
    type: "one" | "many";
    field?: string;
    fields?: Record<string, MapperConfig>;
  };
};

export type Mapper =
  | Record<string, MapperConfig>
  | ((key: string) => MapperConfig);
```

- `field` rewrites a single Cerbos path to a different field in MongoDB.
- `nullable` declares that a stored `null` represents a missing Cerbos attribute. Comparisons add a non-null guard so MongoDB does not turn a CEL evaluation error into an authorized match. Do not set it for fields where `null` is an explicit Cerbos value.
- `valueParser` transforms leaf values during filter construction. This is useful when the Cerbos plan contains string representations that need to be converted to MongoDB-specific types (for example, converting a string to an `ObjectId`). The parser is applied to each value in `eq`, `ne`, `lt`, `le`, `gt`, `ge`, and `in` operators. It also works on nested relation fields via the `fields` map.
- `relation` describes embedded documents (`type: "one"`) or arrays (`type: "many"`). When `field` is provided on a relation it identifies the property inside that relation that should be used for comparisons (for example, matching `createdBy.id` without an `$elemMatch`).
- `fields` supplies nested overrides so lambda expressions such as `tag.name` can be mapped to the correct property.

If you omit the mapper the adapter will use the query plan paths verbatim, which only works when your Mongo documents follow the Cerbos naming convention.

#### Direct fields

```ts
const mapper: Mapper = {
  "request.resource.attr.aBool": { field: "aBool" },
  "request.resource.attr.aString": { field: "title" },
  "request.principal.attr.department": { field: "principalDepartment" },
};
```

#### Relations and collections

Use `relation` when mapping nested objects or arrays. `type: "one"` maps to embedded/single relations and results in dotted field paths, while `type: "many"` maps to arrays and lets the adapter emit `$elemMatch` conditions. The optional `fields` map lets you rename nested properties referenced in lambda expressions.

```ts
const mapper: Mapper = {
  "request.resource.attr.createdBy": {
    relation: {
      name: "createdBy",
      type: "one",
      field: "id",
    },
  },
  "request.resource.attr.tags": {
    relation: {
      name: "tags",
      type: "many",
      fields: {
        id: { field: "id" },
        name: { field: "name" },
      },
    },
  },
};
```

#### Collection operators in practice

Collection-aware operators (`filter`, `exists`, `hasIntersection`, `map`, and `all`) over a *resource* collection require the mapper to declare the relation with `type: "many"`. The adapter automatically scopes lambda variables and uses the `fields` map when translating expressions such as `tag.name`:

```ts
const mapper: Mapper = {
  "request.resource.attr.tags": {
    relation: {
      name: "tags",
      type: "many",
      fields: {
        name: { field: "name", nullable: true },
      },
    },
  },
};
```

- `exists` and `filter` wrap the translated condition in `$elemMatch`.
- `hasIntersection` works for both scalar arrays and arrays of objects; when the plan uses `map(lambda(tag.name))` the adapter projects `tag.name` to `tags.$elemMatch.name`.
- `all` converts the lambda condition into a negated `$elemMatch` so that all elements must satisfy the predicate.
- A bare `map` expression verifies that the referenced nested path exists inside each element.

#### Collection macros over known values

`exists`/`all` over a collection the PDP resolves at plan time — typically a
principal attribute, as in `P.attr.teams.exists(t, R.attr.team == t)` — needs no
relation mapping. The Cerbos planner unrolls it into a plain `or`/`and` chain at
10 elements or fewer and ships the lambda with a literal value-list collection
above that (`maxItems = 10` in the planner's struct matcher; cerbos/cerbos#2570,
cerbos/cerbos#2817). The adapter applies the same fold, uncapped, so the emitted
filter is equivalent on both sides of that threshold rather than depending on how
many teams a given principal happens to hold.

Each element is substituted into the lambda body — a bare `t` becomes the
element, `t.name` drills into it — and the per-element filters combine with
`$or` (`exists`) or `$and` (`all`). An empty collection keeps CEL identity
semantics: `exists` emits `{ $expr: false }` (matches nothing) and `all` emits
`{ $expr: true }` (matches everything), since MongoDB rejects an empty
`$or`/`$and`. `exists_one`, `filter`, `map` and `except` have no flat equivalent
and throw over a literal value list, as does a `t.path` reference the element
does not carry.

#### Mapper functions

You can also supply a function if your mappings follow a predictable pattern:

```ts
const mapper: Mapper = (path) => {
  if (path.startsWith("request.resource.attr.")) {
    return { field: path.replace("request.resource.attr.", "") };
  }
  if (path.startsWith("request.principal.attr.")) {
    return { field: `principal.${path.replace("request.principal.attr.", "")}` };
  }
  return { field: path };
};
```

#### Value parsing

Use `valueParser` to convert values from the Cerbos plan into types that MongoDB expects. A common use case is converting string IDs to `ObjectId`:

```ts
import { Types } from "mongoose";

const mapper: Mapper = {
  "request.resource.attr.id": {
    field: "_id",
    valueParser: (value) => new Types.ObjectId(value),
  },
};
```

`valueParser` also works on nested relation fields via the `fields` map:

```ts
const mapper: Mapper = {
  "request.resource.attr.createdBy": {
    relation: {
      name: "createdBy",
      type: "one",
      field: "id",
      fields: {
        id: {
          field: "id",
          valueParser: (value) => new Types.ObjectId(value),
        },
      },
    },
  },
};
```

## Usage example

```ts
import { GRPC as Cerbos } from "@cerbos/grpc";
import mongoose from "mongoose";
import {
  queryPlanToMongoose,
  PlanKind,
  type Mapper,
} from "@cerbos/orm-mongoose";

await mongoose.connect("mongodb://127.0.0.1:27017/test");
const cerbos = new Cerbos("localhost:3592", { tls: false });
const MyModel = mongoose.model("MyModel", /* ... schema ... */);

const mapper: Mapper = {
  "request.resource.attr.title": { field: "title" },
  "request.resource.attr.owner": {
    relation: { name: "owner", type: "one", field: "id" },
  },
  "request.resource.attr.tags": {
    relation: {
      name: "tags",
      type: "many",
      fields: { name: { field: "name" } },
    },
  },
};

const queryPlan = await cerbos.planResources({
  principal: { id: "user1", roles: ["USER"] },
  resource: { kind: "document" },
  action: "view",
});

const result = queryPlanToMongoose({ queryPlan, mapper });

if (result.kind === PlanKind.ALWAYS_DENIED) {
  return [];
}

const filters = result.kind === PlanKind.CONDITIONAL ? result.filters : {};
const records = await MyModel.find(filters);
```

If you already have application-specific criteria you can combine them using `$and`:

```ts
const filters = result.kind === PlanKind.CONDITIONAL ? result.filters : {};
await MyModel.find({ $and: [filters ?? {}, { archived: false }] });
```

## Error handling

`queryPlanToMongoose` throws descriptive errors in the following scenarios:

- The plan kind is not one of the Cerbos `PlanKind` values (`Invalid query plan.`).
- A conditional plan omits the `operator`/`operands` structure (`Invalid Cerbos expression structure`).
- An operator listed in the plan is not implemented (`Unsupported operator: <name>`).
- Collection-oriented operators (`map`, `filter`, `exists`, `all`, etc.) are used without a `relation` mapper, or with a mapper that declares `type: "one"` where `type: "many"` is required (errors such as `map operator requires a relation mapping`).
- Lambda expressions in the plan are malformed (for example, missing a variable operand results in `Lambda variable must have a name`).
- Value operands do not match the expected type, e.g., `hasIntersection` supplies a non-array value.

Surfacing these errors early helps keep the adapter and your Cerbos policies in sync.

## Limitations

- `exists_one` throws rather than silently degrading to `exists`.
- Aggregation expressions inside collection predicates, outer-document references from lambdas, and negation of nullable collection macros fail closed because `$elemMatch` cannot preserve their CEL scoping and three-valued error semantics.
- Operators not enumerated in **Supported operators** (such as search, mode, scalar math helpers, atomic number operations, composite keys, etc.) are not implemented and will throw `Unsupported operator`.
- Translations may use document-level `$expr`, but never require a multi-stage aggregation pipeline.
