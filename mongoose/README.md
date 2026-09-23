# Cerbos + Mongoose ORM Adapter

Converts a [Cerbos](https://cerbos.dev) query plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan))
into a [Mongoose](https://mongoosejs.com/) filter, so authorization is pushed down to MongoDB.

## Install

```bash
npm install @cerbos/orm-mongoose @cerbos/core
npm install @cerbos/grpc   # or @cerbos/http — whichever client your deployment uses
```

- `@cerbos/core` (`^0.32.0 || ^0.33.0`) is a peer dependency. Install it yourself so your Cerbos
  client and the adapter share one copy of the query plan types (npm 7+ adds it automatically;
  pnpm and Yarn need it declared).
- The adapter depends on no Cerbos client and does not import `mongoose`; it returns a plain filter
  object.
- Requirements: Node.js >= 22, Mongoose 9.x, MongoDB 7.0 or newer, Cerbos > v0.16.

## Quick start

```ts
import { GRPC as Cerbos } from "@cerbos/grpc";
import mongoose from "mongoose";
import { queryPlanToMongoose, PlanKind, type Mapper } from "@cerbos/orm-mongoose";

await mongoose.connect("mongodb://127.0.0.1:27017/test");
const cerbos = new Cerbos("localhost:3593", { tls: false });
const Document = mongoose.model("Document", /* ... schema ... */);

const mapper: Mapper = {
  "request.resource.attr.title": { field: "title" },
  "request.resource.attr.owner": { relation: { name: "owner", type: "one", field: "id" } },
  "request.resource.attr.tags": {
    relation: { name: "tags", type: "many", fields: { name: { field: "name" } } },
  },
};

async function listDocuments(principalId: string) {
  const queryPlan = await cerbos.planResources({
    principal: { id: principalId, roles: ["USER"] },
    resource: { kind: "document" },
    action: "view",
  });

  const result = queryPlanToMongoose({ queryPlan, mapper });

  switch (result.kind) {
    case PlanKind.ALWAYS_DENIED:
      return [];
    case PlanKind.ALWAYS_ALLOWED:
      return Document.find({ archived: false });
    case PlanKind.CONDITIONAL:
      return Document.find({ $and: [result.filters!, { archived: false }] });
  }
}
```

`queryPlanToMongoose` returns `{ kind, filters? }`; `filters` is set only for `CONDITIONAL`.
`PlanKind` is re-exported from `@cerbos/core`. Combine the adapter's filter with your own criteria
under `$and`, as above. Unsupported shapes throw rather than returning a weaker filter.

## Mapping attributes

The plan names attributes by path (`request.resource.attr.title`). The mapper tells the adapter
where each lives in your documents.

```ts
type MapperConfig = {
  field?: string;
  nullable?: boolean;
  valueParser?: (value: any) => any;
  valueType?: "number" | "string" | "boolean" | "dateTime";
  relation?: {
    name: string;
    type: "one" | "many";
    field?: string;
    requiresParent?: string;
    fields?: Record<string, MapperConfig>;
  };
};

type Mapper = Record<string, MapperConfig> | ((key: string) => MapperConfig);
```

| Option | What it does |
| --- | --- |
| `field` | Document path for this attribute. |
| `nullable` | A stored `null` means a *missing* Cerbos attribute. Comparisons add a non-null guard, so a CEL evaluation error is not turned into a match. Do not set it where `null` is an explicit Cerbos value. |
| `valueParser` | Converts plan literals before they reach the filter (for example string → `ObjectId`). Applied to `eq`, `ne`, `lt`, `le`, `gt`, `ge` and `in` values, and inside relation `fields`. |
| `valueType` | The stored scalar type. Declare number, string and boolean fields — top-level, and inside relation `fields` — so Mongoose does not cast a mismatched CEL literal into the field's type: without it, `R.attr.flag == "true"` is sent as `true` and matches. Declare stored `Date` fields as `dateTime` (see [Timestamps](#timestamps-and-conversions)). `valueParser` still overrides. |
| `relation` | An embedded document (`type: "one"`, dotted paths) or an array (`type: "many"`, `$elemMatch`). `relation.field` names the property compared inside it (e.g. `createdBy.id`). |
| `relation.fields` | Mappings for properties inside the relation, as referenced by lambda variables (`tag.name`). |
| `relation.requiresParent` | Document path of an optional to-one parent an array is reached through, so `size(chain)` on a document with no parent yields null instead of 0 ([#309](https://github.com/cerbos/query-plan-adapters/issues/309)). A `type: "one"` relation needs no declaration. |

**Every attribute a plan references needs a mapper entry** — its own, or the relation it is reached
through — or translation throws `No mapper entry for <reference>`. If your documents really are
shaped like the plan paths, opt in per reference with an entry that names no `field` (`{}` or
`{ nullable: true }`), or a function mapper that returns one.

### Direct fields

```ts
const mapper: Mapper = {
  "request.resource.attr.aBool": { field: "aBool", valueType: "boolean" },
  "request.resource.attr.aString": { field: "title", valueType: "string" },
  "request.principal.attr.department": { field: "principalDepartment" },
};
```

### Relations and collections

```ts
const mapper: Mapper = {
  "request.resource.attr.createdBy": {
    relation: { name: "createdBy", type: "one", field: "id" },
  },
  "request.resource.attr.tags": {
    relation: {
      name: "tags",
      type: "many",
      fields: { id: { field: "id" }, name: { field: "name", nullable: true } },
    },
  },
};
```

Collection operators (`filter`, `exists`, `hasIntersection`, `map`, `all`) over a *resource*
collection need a `type: "many"` relation:

- `exists` and `filter` wrap the condition in `$elemMatch`.
- `hasIntersection` works on scalar arrays and arrays of objects; `map(lambda(tag.name))` projects to
  `tags` `$elemMatch` on `name`.
- `all` becomes a negated `$elemMatch`, and requires the stored field to be an array.
- A bare `map` checks that the nested path exists in each element.

Collection predicates cannot reference anything outside their lambda: `$elemMatch` cannot evaluate
the root document, so such references throw.

### Collection macros over known values

`exists`/`all` over a collection the PDP resolves at plan time (typically a principal attribute,
`P.attr.teams.exists(t, R.attr.team == t)`) needs no relation mapping. The planner unrolls it into
an `or`/`and` chain at 10 elements or fewer and ships a literal value-list above that
(cerbos/cerbos#2570, cerbos/cerbos#2817); the adapter folds both forms the same way, so the filter
does not depend on how many values the principal holds.

Each element is substituted into the lambda body (`t` → the element, `t.name` → its field) and the
results combine with `$or` (`exists`) or `$and` (`all`). An empty collection emits
`{ $expr: false }` for `exists` and `{ $expr: true }` for `all`. `exists_one`, `filter`, `map` and
`except` over a literal list throw, as does a `t.path` the element does not carry.

### Mapper functions

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

### Value parsing

```ts
import { Types } from "mongoose";

const mapper: Mapper = {
  "request.resource.attr.id": {
    field: "_id",
    valueParser: (value) => new Types.ObjectId(value),
  },
  "request.resource.attr.createdBy": {
    relation: {
      name: "createdBy",
      type: "one",
      field: "id",
      fields: { id: { field: "id", valueParser: (value) => new Types.ObjectId(value) } },
    },
  },
};
```

## NULL attribute representation

`R.attr.x == null` produces the same plan however your application sends a NULL field to
`check()`, so tell the adapter which convention you use:

| Attributes you send for a NULL field | `check()` on that document | Null-matching filter |
| --- | --- | --- |
| `{"x": null}` — explicit null | allow | selects it — aligned |
| `{}` — attribute omitted | **deny** (missing-attribute error) | selects it — **over-grants** |

`nullAttributeRepresentation` defaults to `"explicit"`. If you omit NULL attributes, set
`"omitted"`: the adapter then rejects every null comparison operand instead of emitting a filter
that returns documents the PDP denies.

```ts
queryPlanToMongoose({ queryPlan, mapper, nullAttributeRepresentation: "omitted" });
```

The rejection is wider than the shapes that actually over-grant, because a leaf cannot tell whether
an enclosing `not` will flip it. See [#302](https://github.com/cerbos/query-plan-adapters/issues/302).
For a single field, `nullable: true` on its mapper entry is the per-attribute alternative.

## Timestamps and conversions

- **Timestamps** accept BSON dates or RFC 3339 strings with at most 3 fractional-second digits, in
  CEL's range `0001-01-01T00:00:00Z` to `9999-12-31T23:59:59.999Z`. Higher-precision or
  out-of-range values fail closed rather than being truncated.
- A bare comparison of two `dateTime` fields throws, because MongoDB no longer holds the original
  strings CEL compares. Use `timestamp(...)` on both operands when the policy compares instants.
- **Conversions** fail closed when the stored BSON type is not a CEL-compatible source or parsing
  fails, and stay denied under negation. `double`/`int` accept strings and numbers, not booleans;
  `int` rejects BSON dates. `string` accepts strings, booleans and numbers.
- **`matches`** supports literals, `.`, `*`, `+`, `?`, leading `^`, terminal `$` and escaped
  metacharacters; anything else fails closed. A terminal `$` becomes PCRE2's absolute end anchor so
  MongoDB cannot match before a trailing newline, as RE2 would not.

## Supported operators

| Category | Operators | Behaviour |
| --- | --- | --- |
| Logical | `and`, `or`, `not` | `$and`, `$or`, `$nor`. |
| Comparisons | `eq`, `ne`, `lt`, `le`, `gt`, `ge` | `$eq`, `$ne`, `$lt`, `$lte`, `$gt`, `$gte` on the mapped field. |
| Membership | `in`, `hasIntersection` | `$in`, or `$elemMatch` on array relations. `hasIntersection` takes an array field or a `map` projection, in either operand order. |
| String helpers | `contains`, `startsWith`, `endsWith` | Escaped regular expressions. |
| Null checks | `eq`/`ne` against `null`, `exists` | `$eq: null`/`$ne: null` on scalars, `$elemMatch` on collections. |
| Collections | `filter`, `lambda`, `map`, `all` | Scoped `$elemMatch`; over a literal value list, `$or`/`$and` of the substituted body. |
| Arithmetic and values | `add`, `sub`, `mult`, `div`, `mod`, `if`, `size`, `index`, `get-field` | Document-level `$expr`. Division needs a non-zero constant denominator; `index` needs a non-negative integer constant and adds a bounds check. |
| Conversions and matching | `string`, `double`, `int`, `timestamp`, `matches` | Guarded conversion and regex expressions (see above). |
| Hierarchies | `hierarchy`, `ancestorOf`, `descendentOf`, `overlaps` | Literal prefix and ancestor-list filters on a mapped scalar path, including through to-one relations and under negation. |

Translations may use `$expr` but never need an aggregation pipeline.

### What throws

- `Invalid query plan.` — the plan kind is not a `PlanKind`.
- `Invalid Cerbos expression structure` — a conditional plan lacks `operator`/`operands`.
- `Unsupported operator: <name>` — anything not in the table above.
- `No mapper entry for <reference>` — an unmapped attribute.
- Collection operators without a `type: "many"` relation (e.g. `map operator requires a relation mapping`).
- Malformed lambdas (`Lambda variable must have a name`) and mistyped operands (e.g. a non-array
  `hasIntersection` value).
- Shapes `$elemMatch` or `$expr` cannot express faithfully: `exists_one`, aggregation expressions or
  outer-document references inside a collection predicate, nested collection counts, correlated
  variable-in-variable membership, unsafe division or non-finite arithmetic, negated collection
  macros over nullable fields (including a negated string match against a nullable field needle),
  whole-list equality (including over a `map()` projection), list-valued membership needles, and
  `+` between two field paths (see the contract table).

## Conformance contract

Select the PDP engine mode with `ADAPTER_TEST_STRICT_EVALUATION=false` (default) or `=true`; other
values are rejected. For example, `ADAPTER_TEST_STRICT_EVALUATION=true npm run test:adversarial`
enables strict evaluation for both planning and the `check()` oracle. CI runs both modes for each
adversarial store and client-version combination.

The adapter is differentially tested against Cerbos PDP 0.55.0 `checkResource` decisions in both evaluation modes using 29 hostile seed documents and real MongoDB 7 and 8 queries. The Spring Data adapter defines the reference semantics for this compatibility snapshot.

| Classification | Coverage |
| --- | --- |
| Oracle-tested | 221 reference conformance actions plus regex, ordered indexing/`get-field`, timestamp and mixed-null field-to-field probes (225 actions) |
| Literal of another type than a declared field | `R.attr.aNumber == "5"`, `R.attr.aBool == "true"`, `R.attr.aString == 0`, `R.attr.aNumber in ["5", 2]`, and the same over a typed subdocument field (`t.name == 0`, `hasIntersection(tags.map(t, t.name), ["public", 0])`) are answered as CEL answers them — `==` and membership false, `!=` true where the field is present — because Mongoose casts a query literal to the schema type (`"5"` is sent as `5`). This needs the field's `valueType`; the literal of the declared type stays a plain, indexable leaf. No seed tag name spells a number, so on MongoDB the subdocument probes return the same rows under the cast filter; the translator unit test pins the uncast one |
| Membership in a native array field | `x in R.attr.list` and `hasIntersection(R.attr.list, [...])` over an array stored on the document (not a relation) are answered inside `$expr` with `$literal` needles, because Mongoose casts a query-level literal to the schema's element type (`{ list: "2" }` is sent as `2` over `[Number]`) while CEL's `"2" in [2]` is false |
| Fail-closed | 96 reference actions plus the 7 reference-unsupported shapes (103 actions total) |
| Operand types the plan does not carry | CEL overloads `+` on strings and a plan names no field types. One string operand settles it, so `R.attr.a + "x"` translates as `$concat`. Between **two field paths** it cannot be decided, and MongoDB's `$add` accepts only numbers and dates, so the shape is refused at translation rather than aborting the query on the server (cerbos/query-plan-adapters#391) |
| Representation-dependent | `null-eq-missing` — rejected under `nullAttributeRepresentation: "omitted"`. Under the default it already returns the empty set the PDP demands, because `nullable: true` on a mapper entry declares that a stored null is a missing attribute; the global option is the backstop for mappings that do not declare it |
| Attribute NULL convention | Needs no declaration: Mongoose stores the value the caller sent, so a stored null compares as a null *value* exactly as CEL does. Four `null-value-*` probes (cerbos/query-plan-adapters#308) are aligned; the fifth is refused by the negated-collection-macro limitation, not by the null convention |
| Known planner divergence | `has()` on a missing attribute is folded by the Cerbos planner to `ALWAYS_ALLOWED`, while `checkResource` denies the missing-attribute documents. Until the planner is fixed, use `R.attr.x != null` for database-backed attributes instead of `has(R.attr.x)` |

Every fail-closed shape's error message is pinned in `conformance/actions.json` and asserted here,
so a classification proves the throw names its declared mechanism. The emitted filter for every
corpus action is pinned separately by the offline translator unit test (see
[Development](#development)).

## Mapping hazards

The contract above proves the plan side. The other half is the mapping: **the documents the filter
reads must be the documents the application put into the resource attributes.** The shared corpus
catalogues six ways that can break.

This adapter **builds no subquery**: a relation is a path inside the same document, and it never
emits `$lookup`/`$graphLookup` or calls `populate()`/`aggregate()`. `src/adversarial.test.ts`
asserts that, since five of the rows below depend on it.

| Hazard | Position | Mechanism to check |
|---|---|---|
| Filtered association | Not applicable — no subquery | — |
| Default scope on the target model | Not applicable — no second collection is read | — |
| Subtype discrimination | **Caller-owned** | `Model.discriminator(...)`. Run the filter on the same model the application read the attributes from. Discriminated models share one collection, so a filter run against the *base* model matches other subtypes' documents — the `__t` criterion Mongoose adds for a discriminator model is not in the adapter's filter, and cannot be: the plan does not say which model you will use |
| To-one relation used as a collection | Not applicable — a document path holds exactly what the application stored | — |
| Composite association key | Not applicable — no join, so no key to compose | — |
| Absent to-one parent | **Reproduced**, and proved by the corpus (`w1-all-chain`, `rel-not-bool-hop` and siblings) | `relation.requiresParent` for a flattened array parent, so `size(chain)` comparisons yield null rather than 0 ([#309](https://github.com/cerbos/query-plan-adapters/issues/309)). A `type: "one"` relation needs no declaration: it ANDs `{ <path>: { $ne: null } }` outside any `$nor`, so a negation never matches a document whose subdocument is absent ([#375](https://github.com/cerbos/query-plan-adapters/issues/375)) |

## Behaviour changes

- **Breaking:** value-first membership and `hasIntersection` over a native array field (a mapper
  entry with no `relation`) emit an `$expr` instead of `{ list: x }` / `{ list: { $in: [...] } }`.
  The old filter over-granted whenever the literal's type differed from the schema's element type,
  since Mongoose cast it first. The new filter is an aggregation expression, so MongoDB cannot
  answer it from a multikey index on that field. Relation-mapped collections are unchanged.
- **Breaking** — an unmapped reference throws `No mapper entry for <reference>` instead of being
  used verbatim as a document path, which made `$ne`/`$nor` match every document. Callers relying
  on the fallback (including passing no mapper) must add entries
  ([#492](https://github.com/cerbos/query-plan-adapters/issues/492)).
- **Breaking** — collection predicates reject references outside their lambda scope for every leaf
  comparison operator; MongoDB's `$elemMatch` cannot evaluate them against the root document.
- **Breaking** — whole-list equality and list-valued membership needles throw; they previously
  produced invalid or incorrect filters.
- **Breaking** — `+` between two field paths throws instead of reaching the server as `$add`
  ([#391](https://github.com/cerbos/query-plan-adapters/issues/391)).
- **Breaking** — a bare comparison of two `dateTime` fields throws.
- A `type: "one"` relation ANDs a non-null guard outside any negation, so a negation over it no
  longer matches documents where the subdocument is absent (over-grant fix); a bare boolean read
  through a to-one hop now translates instead of throwing "Bare collection variables are
  unsupported" ([#375](https://github.com/cerbos/query-plan-adapters/issues/375)).
- A literal of another type than a field's declared `valueType` is kept away from Mongoose's cast
  in every position, not only in `==`/`!=` at the top level: it is dropped from a field-in-list
  `$in` and from `hasIntersection` values, answers value-first membership over a relation false,
  and answers `==`/`!=` inside `$elemMatch` as `{ $in: [] }` / `{ $exists: true }` (MongoDB
  refuses `$expr` there). Previously `R.attr.n in ["5", 2]` matched `n == 5` and
  `tags.exists(t, t.name == 0)` matched a tag named `"0"` (over-grant fix). Nothing newly throws.
- `hasIntersection` accepts the value-first operand order (`hasIntersection(["a","b"], R.attr.list)`)
  instead of throwing "Invalid operands". Widening only
  ([#387](https://github.com/cerbos/query-plan-adapters/issues/387)).

## Example application

[`example/`](example/) installs the packed adapter and runs it against a live PDP and MongoDB over
the shared [demo domain](../demo/README.md), including pagination and composition with an
application filter:

```bash
# from the repository root
demo/scripts/run-example.sh mongoose
```

## Development

| Command | What it does | Needs |
| --- | --- | --- |
| `npm test` | Translator unit test: every corpus action's emitted filter, plan kind or pinned refusal, plus the mapper contract no policy can reach (`valueParser` incl. `ObjectId` coercion, function mappers, the `nullAttributeRepresentation` boundary, malformed input) | Node only |
| `npm run typecheck` | Type-checks `src/` and the tests | Node only |
| `npm run mongo` | Starts the pinned MongoDB ([`MONGO_IMAGE`](MONGO_IMAGE)) on port 27017 | Docker |
| `npm run test:adversarial` | Runs the shared corpus against real MongoDB with `check()` as the oracle | Cerbos CLI, `npm run mongo` in another shell |

`npm test` reads its plans from `conformance/wire-fixtures/`, so a change to the emitted query shows
up as a diff even when it selects the same seed documents, and a new corpus action fails it until
its filter is recorded ([ADR 0006](../docs/adr/0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md)).
Mongoose still keeps its expectations inline rather than in a `golden/expectations.json`; see
"Golden expectations" in [conformance/README.md](../conformance/README.md). CI also runs the
adversarial suite against [`MONGO_NEXT_IMAGE`](MONGO_NEXT_IMAGE).
