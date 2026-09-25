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
| `nullable` | A stored `null` means a *missing* Cerbos attribute. Comparisons add a non-null guard, so a CEL evaluation error is not turned into a match. Do not set it where `null` is an explicit Cerbos value. Left undeclared, it follows the call's [`nullAttributeRepresentation`](#null-attribute-representation): off under `"explicit"`, on under `"omitted"`. |
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
`"omitted"`:

```ts
queryPlanToMongoose({ queryPlan, mapper, nullAttributeRepresentation: "omitted" });
```

The call-level option is the default for every mapper entry that does not declare `nullable`, so
under `"omitted"` the adapter:

- rejects every null comparison operand. The rejection is wider than the shapes that actually
  over-grant, because a leaf cannot tell whether an enclosing `not` will flip it
  ([#302](https://github.com/cerbos/query-plan-adapters/issues/302));
- treats every entry that does not declare `nullable` as `nullable: true`, relation `fields`
  included. A comparison ANDs `{ field: { $ne: null } }` in front of it, so `R.attr.x != "a"` no
  longer returns the documents `x` is missing or null in, which `check()` denies. A `not` over such a
  field is handled as it is for a declared-nullable field: the guard is ANDed outside the `$nor`,
  and where some path through the negation can leave the field unread (one side of `&&`/`||`, a
  ternary branch) the adapter throws `UnsupportedQueryPlanError` instead
  ([#493](https://github.com/cerbos/query-plan-adapters/issues/493)).

`nullable: false` opts an entry out: it asserts the field is always stored and never null, and the
entry translates as it does under `"explicit"`. Declare it where you know that, because a nullable
guard is a `$ne: null`, which MongoDB applies per element to an array field: an array that holds a
`null` element is excluded too. Under `"explicit"`, `nullable: true` on one entry is the
per-attribute way to declare the omitted convention.

> [!WARNING]
> **Do not guard with `has()`: write `R.attr.x != null`.** An attribute the plan request omits is
> unknown to the planner, which assumes the data layer supplies it, as a table column always does.
> So the planner reads `has(R.attr.x)` as the guard for the `x` access beside it and folds it to true
> by design: alone it plans as `ALWAYS_ALLOWED`, and `has(R.attr.x) && R.attr.y > 0` plans as
> `R.attr.y > 0`. A document can lack a field, so the filter then returns documents missing `x` that
> `check()` denies, and the adapter, which only sees the plan, cannot restore the guard.
> `R.attr.x != null` stays in the plan, where the adapter translates it or refuses it, and agrees
> with `check()` whether `x` is missing, null or present.

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

Shapes the adapter cannot express throw `UnsupportedQueryPlanError` rather than emit a broader
filter. It is exported and extends `Error`, so existing `catch` blocks keep working:

```ts
import { queryPlanToMongoose, UnsupportedQueryPlanError } from "@cerbos/orm-mongoose";

try {
  const result = queryPlanToMongoose({ queryPlan, mapper });
} catch (error) {
  if (error instanceof UnsupportedQueryPlanError) {
    // The policy uses a shape this adapter cannot translate faithfully: deny, or fall back to
    // per-document check() calls.
  }
  throw error;
}
```

A mapper misconfiguration — an unmapped reference, or a collection operator over a reference with
no relation mapping — stays a plain `Error`. A macro over a `type: "one"` relation is a refusal: CEL
reads that attribute as a map and ranges over its keys, which no filter can iterate. The messages:

- `Invalid query plan.` — the plan kind is not a `PlanKind`.
- `Invalid Cerbos expression structure` — a conditional plan lacks `operator`/`operands`.
- `Unsupported operator: <name>` — anything not in the table above.
- `No mapper entry for <reference>` — an unmapped attribute.
- Collection operators over a reference with no relation mapping (e.g. `map operator requires a relation mapping`).
- Malformed lambdas (`Lambda variable must have a name`) and mistyped operands (e.g. a non-array
  `hasIntersection` value).
- Shapes `$elemMatch` or `$expr` cannot express faithfully: `exists_one`, aggregation expressions or
  outer-document references inside a collection predicate, nested collection counts, correlated
  variable-in-variable membership, unsafe division or non-finite arithmetic, `%` over anything but `size()` or by anything but a
  non-zero integer constant, negated collection
  macros, a negation over a nullable field that some path through it can leave unread (one side of
  `&&` or `||`, a ternary branch, a lambda body) or that sits on a to-many relation, a negated
  membership whose list CEL may not evaluate, or that sits inside a collection predicate,
  whole-list equality (including over a `map()` projection), list-valued membership needles, and
  `+` between two field paths ([`conformance-ledger.json`](conformance-ledger.json) lists every
  refused corpus case with its reason).

## Conformance contract

The adapter is replayed against the shared [conformance corpus](../conformance/README.md): the plans
and `check()` decisions recorded from Cerbos PDP 0.55.0 (and 0.54.0), executed as real MongoDB
queries over the corpus's 41 seed documents on MongoDB 7 and 8. Passed cases on the current PDP,
0.55.0, identical on both servers, where the total is every golden case in that tier:

| Tier | Passed / total |
| --- | --- |
| core | 26 / 26 |
| extended | 49 / 80 |
| adversarial | 202 / 308 |

Cases marked as a planner divergence in their golden file are skipped, not compared: no adapter can
pass them. On 0.55.0 that is four extended cases and three adversarial cases.
`null/has/missing-attribute` and `null/has/composed-with-comparison`: the plan request leaves an
omitted attribute unknown, so the planner folds `has()` to true by design, while `checkResource`
receives the omission as absent and denies the document; use `R.attr.x != null` instead of
`has(R.attr.x)`. `arithmetic/add/int-literal-plus-constant` and `arithmetic/add/int-literal-negated`: the planner drops the int type of the literal in `R.attr.x + 1`, so the plan is the double spelling's, while `check()` has no double + int overload and denies every row; write `1.0`. Three `composition/*` cases whose DENY condition reads `aNumber`, which j2
lacks: the plan's `not(...)` of it denies j2, while `checkResource` receives `aNumber` as absent,
treats the erroring deny rule as not matching and allows the document
([#530](https://github.com/cerbos/query-plan-adapters/issues/530)). Every other case that does not pass is refused with `UnsupportedQueryPlanError`;
none returns wrong documents. [`conformance-ledger.json`](conformance-ledger.json) lists each one
with its reason.

Two behaviours the corpus relies on that a caller's mapping has to provide:

- **Declared `valueType`.** Mongoose casts a query literal to the schema type (`"5"` is sent as
  `5`), so `R.attr.aNumber == "5"` would match `5`. Declaring the field's `valueType` lets the
  adapter answer a literal of another type as CEL does — `==` and membership false, `!=` true where
  the field is present, and an ordering (`<`, `<=`, `>`, `>=`) false under either polarity —
  including over a typed subdocument field. Membership in a native array
  field is answered inside `$expr` with `$literal` needles for the same reason.
- **`nullable: true`** declares that a stored null is a *missing* attribute (the caller omits it
  from `check()`), so `== null` against it selects nothing, as CEL's missing-attribute error
  demands. Under a negation its non-null guard is ANDed outside the `$nor`, so `!(x > 3)` denies
  a document with no `x` as CEL does, where a bare `$nor` would match it. A negated ordering
  against a constant is translated as its complement (`!(x > 3)` as `x <= 3`), whose MongoDB
  comparison only matches values of the constant's own type, so a stored null or a value of
  another type is denied under both polarities. A field that does not declare it takes the call's
  `nullAttributeRepresentation`: under `"explicit"` it compares a stored null as a null *value*;
  under `"omitted"` it is nullable, and every null operand is refused
  ([NULL attribute representation](#null-attribute-representation)).

A document whose number field holds NaN is outside this contract, since the PDP cannot receive one
([conformance mapping hazards](../conformance/README.md#mapping-hazards)). Mongoose's `Number` schema
type already refuses NaN on save, so it only arrives through the raw driver or another writer: a
negated ordering such as `!(x >= 1)` denies it, and a comparison inside `$expr` may match it.

## Mapping hazards

The contract above proves the plan side. The other half is the mapping: **the documents the filter
reads must be the documents the application put into the resource attributes.** The shared corpus
catalogues six ways that can break.

This adapter **builds no subquery**: a relation is a path inside the same document, and it never
emits `$lookup`/`$graphLookup` or calls `populate()`/`aggregate()`. `src/translator.test.ts`
asserts that, since five of the rows below depend on it.

| Hazard | Position | Mechanism to check |
|---|---|---|
| Filtered association | Not applicable — no subquery | — |
| Default scope on the target model | Not applicable — no second collection is read | — |
| Subtype discrimination | **Caller-owned** | `Model.discriminator(...)`. Run the filter on the same model the application read the attributes from. Discriminated models share one collection, so a filter run against the *base* model matches other subtypes' documents — the `__t` criterion Mongoose adds for a discriminator model is not in the adapter's filter, and cannot be: the plan does not say which model you will use |
| To-one relation used as a collection | Not applicable — a document path holds exactly what the application stored | — |
| Composite association key | Not applicable — no join, so no key to compose | — |
| Absent to-one parent | **Reproduced**, and proved by the corpus (`relation/all/to-one-chain`, `relation/bare-attribute/negated-one-hop-boolean` and siblings) | `relation.requiresParent` for a flattened array parent, so `size(chain)` comparisons yield null rather than 0 ([#309](https://github.com/cerbos/query-plan-adapters/issues/309)). A `type: "one"` relation needs no declaration: it ANDs `{ <path>: { $ne: null } }` outside any `$nor`, so a negation never matches a document whose subdocument is absent ([#375](https://github.com/cerbos/query-plan-adapters/issues/375)) |

## Behaviour changes

- **Breaking** — under `nullAttributeRepresentation: "omitted"`, a mapper entry that does not
  declare `nullable` is treated as `nullable: true`. Comparisons on it gain a
  `{ field: { $ne: null } }` guard, and a `not` over it carries that guard outside the `$nor`, or
  throws `UnsupportedQueryPlanError` where some path through it can leave the field unread. The old
  filter's `$ne` and `$nor` returned documents the field was missing or null in, which `check()`
  denies. Declare `nullable: false` on an entry that is always stored and never null to keep its old
  translation. `"explicit"` output is unchanged
  ([#493](https://github.com/cerbos/query-plan-adapters/issues/493)).
- **Breaking:** `string()` over an integral constant of 1e6 or more, bare or as a ternary branch
  (`string(R.attr.flag ? 1000000 : 0)`), throws `UnsupportedQueryPlanError`. CEL renders the int
  as `"1000000"` and the double as `"1e+06"`, and the plan ships both as the same number; the old
  filter rendered the double and denied what the PDP allowed, or allowed it under negation. A
  literal `in` over a `type: "one"` relation (`"k" in R.attr.parent`), which CEL answers from the
  subdocument's keys, throws `UnsupportedQueryPlanError` instead of reaching Mongoose, which failed
  the query with a `CastError` ([#554](https://github.com/cerbos/query-plan-adapters/issues/554)).
- A macro (`exists`, `all`, `filter`, `map`, …) over a `type: "one"` relation throws
  `UnsupportedQueryPlanError` instead of a plain `Error` ("requires a collection relation"). CEL
  ranges a macro over a map's keys, and a filter has no form that iterates a subdocument's field
  names ([#545](https://github.com/cerbos/query-plan-adapters/issues/545)). It threw before too;
  only the error type changes.
- A shape the adapter refuses now throws `UnsupportedQueryPlanError`, an exported subclass of
  `Error`. What it translates is unchanged, and existing `catch` blocks keep working; mapper
  misconfiguration stays a plain `Error`.
- An ordering against a constant of another type than the field's declared `valueType`, or
  against null, is `false` under either polarity instead of being cast by Mongoose (`aNumber < "5"`
  compared `5`). A negated ordering against a constant is translated as its complement rather than
  a `$nor`, which matched a stored null or a value of another type
  ([#516](https://github.com/cerbos/query-plan-adapters/issues/516)). Nothing that translated now
  throws.
- A negated membership or `hasIntersection` over a list (a native array field, or a to-many
  relation's array) requires `{ <list>: { $type: "array" } }` outside the `$nor`. CEL raises on a
  null or absent list (`2 in null` has no overload) and denies the document, where the bare `$nor`
  matched it (over-grant fix,
  [#534](https://github.com/cerbos/query-plan-adapters/issues/534)). **Breaking:** the same
  negation throws when its list sits under `&&`, `||` or a ternary that CEL may short-circuit, or
  inside a collection predicate, where the guard has no faithful position.
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
- **Breaking** — `%` throws unless its dividend is `size()` and its divisor a non-zero integer
  constant. CEL's `%` has no double overload and every attribute number reaches CEL as a double, so
  `R.attr.n % 2` is an error that denies every row; the old `$mod` computed a floating remainder,
  which a negation turned into an over-grant.
- `string()` of a number follows CEL's (Go's shortest `%g`) spelling: `1e+06`, `1.234567e+06`,
  `+Inf` and `-Inf` where `$convert` wrote `1000000`, `1234567` and `Infinity` (under-grant fix).
- `size()` of a `requiresParent` chain counts the children of the parent, not the parent elements:
  `$parent.children` is one array per stored parent element, so a parent holding two children
  counted 1.
- A negated `&&`/`||` is pushed down to its operands (De Morgan), so each carries its own
  absent-parent and evaluation guards: `!(!aBool && parent.x == "one")` now allows a parentless
  document whose `aBool` is true, as CEL does (under-grant fix).
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
| `npm test` | Caller-supplied options the corpus cannot vary (`valueParser` incl. `ObjectId` coercion, function mappers, the `nullAttributeRepresentation` boundary), the refusal type, the timestamp literal contract, the no-`$lookup` source scan and malformed input | Node only |
| `npm run typecheck` | Type-checks `src/` and the tests | Node only |
| `npm run mongo` | Starts the pinned MongoDB ([`MONGO_IMAGE`](MONGO_IMAGE)) on port 27017 | Docker |
| `npm run test:adversarial` | The documents each recorded golden plan returns on real MongoDB equal the recorded `check()` decisions, for both pinned PDPs | `npm run mongo` in another shell |

No suite starts a PDP. The harness reads the golden files under `../conformance/golden/` and applies
[`conformance-ledger.json`](conformance-ledger.json): a case with no entry must return exactly the
recorded allowed ids, and an `unsupported` case must throw `UnsupportedQueryPlanError`. See
"The harness contract" in [conformance/README.md](../conformance/README.md). CI also runs the
harness against [`MONGO_NEXT_IMAGE`](MONGO_NEXT_IMAGE).
