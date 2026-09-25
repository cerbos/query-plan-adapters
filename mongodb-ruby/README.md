# Cerbos + MongoDB Ruby driver adapter

> [!WARNING]
> **Work-in-progress prototype.** This gem has not been released, has not been used in
> production, and its interface can still change without a deprecation. Do not depend on it to
> enforce access control in a live system yet.

`cerbos-mongodb` takes a [Cerbos](https://cerbos.dev) query plan
([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan))
and turns it into a MongoDB query filter for the official
[Ruby driver](https://www.mongodb.com/docs/ruby-driver/current/), so the authorization rules in
your Cerbos policies are enforced by MongoDB instead of by application code.

The filter is a plain `Hash` with `String` keys whose values are Strings, numbers, booleans, `nil`,
Arrays, Hashes and `Time` — nothing a BSON encoder does not take — so it works with whichever
version of the `mongo` gem your application already uses. The gem has **no runtime dependency**.

## Requirements

- Ruby 3.2 or newer
- MongoDB 7.0 or newer (CI runs the conformance suite against real 7.0 and 8.0 servers)
- The official `mongo` driver (2.x) in your application, and a Cerbos client to obtain a plan —
  the [Cerbos Ruby SDK](https://github.com/cerbos/cerbos-sdk-ruby) (`cerbos` gem)

## Usage

```ruby
require "cerbos"
require "cerbos/mongodb"
require "mongo"

cerbos = Cerbos::Client.new("localhost:3593", tls: false)
documents = Mongo::Client.new("mongodb://localhost:27017/app")[:documents]

plan = cerbos.plan_resources(
  principal: {id: "alice", roles: ["user"]},
  resource: {kind: "document"},
  action: "view"
)

result = Cerbos::MongoDB.query_plan_to_filter(
  plan: plan,
  mapper: {
    "request.resource.attr.ownerId" => {field: "owner_id"},
    "request.resource.attr.public" => {field: "is_public"}
  }
)

visible = result.always_denied? ? [] : documents.find(result.filter).to_a
```

`result.kind` is `"KIND_ALWAYS_ALLOWED"`, `"KIND_ALWAYS_DENIED"` or `"KIND_CONDITIONAL"`
(`always_allowed?`, `always_denied?`, `conditional?`). `result.filter` is always safe to run as-is:
`{}` for an unconditional allow, `{"$expr" => false}` for an unconditional deny, and the
translated filter otherwise. Skipping the query on a denial is an optimisation, never required.

Combine it with your own predicate under `$and`, and page, sort or count as usual:

```ruby
documents.find("$and" => [result.filter, {"archived" => false}]).sort(_id: 1).skip(20).limit(10)
documents.count_documents(result.filter)
documents.aggregate([{"$match" => result.filter}, ...])
```

### Mongoid

For a [Mongoid](https://www.mongodb.com/docs/mongoid/current/) model, use the optional helper, which
returns an ordinary chainable criteria:

```ruby
require "cerbos/mongodb/mongoid"

result = Cerbos::MongoDB.query_plan_to_filter(plan: plan, mapper: mapper)
Cerbos::MongoDB::Mongoid.criteria(Document, result).where(archived: false).order(title: 1).skip(20).limit(10)
```

It returns `Document.none` for an unconditional deny and the scope unchanged for an unconditional
allow, and accepts a criteria as well as a model (`criteria(current_user.documents, result)`).

**Do not pass the filter to `Model.where` yourself.** Mongoid converts every constant in a query
to the declared type of the field it is compared with, before the query is sent; CEL never does.
Measured against a real MongoDB with typed fields:

| policy | bare `Model.where(filter)` sends | result |
| --- | --- | --- |
| `R.attr.flag == 1` (`Boolean` field) | `flag == true` | returns documents the PDP denies |
| `R.attr.n < "3"` (`Integer` field) | `n < 3` | returns documents the PDP denies |
| `R.attr.flag != 0` (`Boolean` field) | `flag != false` | drops documents the PDP allows |
| `R.attr.x > -1e19` (`Float` field) | an Integer too large for BSON | raises |

The helper wraps each top-level condition in `Mongoid::RawValue`, Mongoid's own opt-out from
that conversion, so the criteria's selector is the adapter's filter verbatim (Mongoid still drops
a repeated clause from `$and`/`$or`/`$nor`, which changes nothing). Both are asserted for every
corpus case. Mongoid is not a dependency of the gem: only `cerbos/mongodb/mongoid` loads it, and
it is tested against Mongoid 9. Mongoid treats `id` as an alias of `_id`, so map a plan variable to
`"_id"` rather than to `"id"`.

`plan:` accepts a `Cerbos::Output::PlanResources` from the Ruby SDK, the parsed JSON of a
`PlanResources` response (with the plan at the top level or under `filter`), or any object with
`kind` and `condition`.

### Mapper

A mapper tells the adapter where each plan variable lives in your documents. It is a Hash keyed
by the variable name, or anything responding to `call(name)` that returns the same config (or
`nil`). Keys may be Symbols or Strings; **an unknown key raises**, because a misspelt `nullable`
silently ignored would drop a guard.

```ruby
{
  "request.resource.id" => {field: "_id", value_parser: ->(id) { BSON::ObjectId.from_string(id) }},
  "request.resource.attr.title" => {field: "title", value_type: :string},
  "request.resource.attr.deletedAt" => {field: "deleted_at", value_type: :date_time, nullable: true},
  "request.resource.attr.owner" => {relation: {name: "owner", type: :one, fields: {"id" => {field: "_id"}}}},
  "request.resource.attr.tags" => {relation: {name: "tags", type: :many, field: "name"}}
}
```

| Key | Meaning |
| --- | --- |
| `field` | The document path; dotted for a subdocument. |
| `nullable` | A stored `null` is a **missing** Cerbos attribute (the application omits it from `check()`). Comparisons then keep null documents out, and a `not` CEL is certain to evaluate keeps the guard outside its `$nor`; a `not` that may skip the field (a ternary branch, a lambda body) or reads it through a to-many relation is refused. Do not set it where `null` is an explicit Cerbos value. Left undeclared, it follows the call's [`null_attribute_representation:`](#null-attribute-representation): off under `:explicit`, on under `:omitted`. |
| `value_parser` | Rewrites each constant compared with the field — for example a string id into a `BSON::ObjectId`. |
| `value_type` | `:number`, `:string`, `:boolean` or `:date_time`. Settles an equality or an ordering against a constant of another type without a query, and refuses shapes a stored `Date` cannot answer (a bare comparison of two date fields: MongoDB has discarded the strings CEL compares). |
| `relation` | `type: :one` for an embedded subdocument (a to-one hop, required to be present outside any negation), `type: :many` for an array of subdocuments (`$elemMatch`). `field` names the element field the relation stands for, `fields` maps element fields, `requires_parent` names an optional to-one parent array the path is reached through. |

**An unmapped reference raises `Cerbos::MongoDB::MapperError`** rather than being used verbatim as
a document path. No real document stores `request.resource.attr.status`, and MongoDB's `$ne` and
`$nor` match every document a path is absent from, so a missing entry used to turn
`R.attr.status != "x"` into a filter returning the whole collection
([#492](https://github.com/cerbos/query-plan-adapters/issues/492)). If your documents really do use
Cerbos's attribute paths as field names, declare each one with an entry that names no field (`{}`,
or `{nullable: true}`), or return one from a callable mapper.

### NULL attribute representation

`R.attr.x == null` reaches the adapter as the same plan node however your application sends a NULL
field to `check()`, so it has to be told:

| attributes you send for a NULL field | `check()` on that document | null-matching filter |
| --- | --- | --- |
| `{"x" => nil}` — explicit null | allow | selects it — aligned |
| `{}` — attribute omitted | **deny** (missing attribute) | selects it — **over-grants** |

`null_attribute_representation:` defaults to `:explicit`. If you omit NULL attributes, pass
`:omitted`:

```ruby
Cerbos::MongoDB.query_plan_to_filter(plan: plan, mapper: mapper, null_attribute_representation: :omitted)
```

The call-level option is the default for every mapper entry that does not declare `nullable`, so
under `:omitted` the adapter:

- refuses every null comparison operand instead of translating it. The refusal is wider than the
  shapes that actually over-grant, because a leaf cannot tell whether an enclosing `not` will flip
  it ([#302](https://github.com/cerbos/query-plan-adapters/issues/302));
- treats every entry that does not declare `nullable` as `nullable: true`, relation `fields` and
  the element fields of a collection macro included. A comparison ANDs `{field => {"$ne" => nil}}`
  in front of it, so `R.attr.x != "a"` no longer returns the documents `x` is missing or null in,
  which `check()` denies. A `not` over such a field is handled as it is for a declared-nullable
  field: the guard is ANDed outside the `$nor`, and where CEL may leave the field unread (a ternary
  branch, a lambda body) the adapter raises `Cerbos::MongoDB::UnsupportedError` instead
  ([#493](https://github.com/cerbos/query-plan-adapters/issues/493)).

`nullable: false` opts an entry out: it asserts the field is always stored and never null, and the
entry translates as it does under `:explicit`. Declare it where you know that, because a nullable
guard is a `$ne: null`, which MongoDB applies per element to an array field: an array that holds a
`null` element is excluded too. Under `:explicit`, `nullable: true` on one entry is the
per-attribute way to declare the omitted convention, and is what lets one mapping mix the two.

## Supported operators

| Category | Operators | MongoDB |
| --- | --- | --- |
| Logical | `and`, `or`, `not` | `$and`, `$or`, `$nor` — with every evaluation guard ANDed *outside* the `$nor` |
| Comparisons | `eq`, `ne`, `lt`, `le`, `gt`, `ge` | query operators against a constant (value-first spellings are mirrored); `$expr` between two fields or around an expression |
| Membership | `in`, `hasIntersection` | `$in`, or `$elemMatch` over a `type: :many` relation; `hasIntersection(map(...), [...])` over one |
| Strings | `contains`, `startsWith`, `endsWith`, `matches` | an escaped, `\z`-anchored `$regex`; `matches` accepts the RE2/PCRE2 common subset (literals, `.`, `*`, `+`, `?`, leading `^`, trailing `$`) |
| Collections | `exists`, `all`, `lambda` | `$elemMatch` scoped to the element; over a literal list above the planner's unroll limit, folded to `$or`/`$and` |
| Expressions | `add`, `sub`, `mult`, `div`, `mod`, `if`, `size`, `index`, `get-field`, `string`, `timestamp` | aggregation operators inside `$expr`, each with the guard that keeps out documents on which CEL raises |
| Hierarchies | `hierarchy`, `ancestorOf`, `descendentOf`, `overlaps` | a literal prefix `$regex` and an ancestor `$in` |

Anything else raises `Cerbos::MongoDB::UnsupportedError`. Every error is a `Cerbos::MongoDB::Error`.

## Conformance contract

The adapter is replayed against the shared [conformance corpus](../conformance/README.md): the plans
and `check()` decisions recorded from Cerbos PDP 0.55.0 (and 0.54.0), executed as real MongoDB
queries over the corpus's 41 seed documents on MongoDB 7.0 and 8.0, once through the Ruby driver
and once through `Cerbos::MongoDB::Mongoid.criteria` on typed Mongoid models. Passed cases on the
current PDP, 0.55.0, identical on both servers and through both, where the total is every golden
case in that tier:

| Tier | Passed / total |
| --- | --- |
| core | 26 / 26 |
| extended | 53 / 80 |
| adversarial | 217 / 308 |

Cases marked as a planner divergence in their golden file are skipped, not compared: no adapter can
pass them. On 0.55.0 that is four extended cases and three adversarial cases.
`null/has/missing-attribute` and `null/has/composed-with-comparison`: the plan request leaves an
omitted attribute unknown, so the planner folds `has()` to true, while `checkResource` receives the
omission as absent and denies the document; use `R.attr.x != null` instead of `has(R.attr.x)`.
`arithmetic/add/int-literal-plus-constant` and `arithmetic/add/int-literal-negated`: the planner
drops the int type of the literal in `R.attr.x + 1`, while `check()` has no double + int overload
and denies every row; write `1.0`. Three `composition/*` cases whose DENY condition reads
`aNumber`, which j2 lacks: the plan's `not(...)` of it denies j2, while `checkResource` treats the
erroring deny rule as not matching and allows the document
([#530](https://github.com/cerbos/query-plan-adapters/issues/530)). Every other case that does not pass is refused with a `Cerbos::MongoDB::Error`;
none returns wrong documents. [`conformance-ledger.json`](conformance-ledger.json) lists each one
with its reason.

The refused set is exact-one cardinality beyond an element field compared with a scalar constant (over a relation it is a `$size` of a `$filter`; over a literal list of up to 32 elements, and not under a negation, it expands to "this one and no other"), aggregation expressions or outer-document references
inside `$elemMatch` (MongoDB accepts `$expr` only at the top level), CEL's `int()`/`double()`
(`$convert` parses a numeric prefix and rounds where CEL raises and truncates), division by
anything but a non-zero constant (`$divide` by zero aborts the query), `%` over anything but an
integer `size()` (CEL's `%` has no double overload), `string()` over an untyped integral constant
of 1e6 or more, `+` between two fields (nothing tells `$add` from `$concat`), negations over
collection macros or over a nullable field CEL may not evaluate (a filter has no UNKNOWN), macros
and `in` over a to-one relation (CEL iterates a map's keys), an empty hierarchy separator, regular
expressions outside the common subset, and a comparison with a map constant or with a list holding
a list, a map or NaN (MongoDB compares embedded documents in stored field order and NaN equal to
NaN). A list constant is compared whole inside `$expr` with a field, a to-many relation's
projection or a `map()` over one, element by element and in order, as CEL does.

It shares its MongoDB semantics with the [Mongoose adapter](../mongoose/), and its ledger is the
same but for one case: the driver sends a filter to the server untouched, so a comparison between
two conditionals (`conditional/ternary/on-both-sides`) translates here, where Mongoose's `$expr`
caster fails to build it.

The harness uses the `nullable: true` mapper flag for the attributes whose NULL the corpus sends
as a *missing* attribute (`aString`, `aNumber` and `aBool` among them, which seeds j1, j2 and j3
leave NULL), so `== null` against them selects nothing, as CEL's missing-attribute error demands.
Under a negation the non-null guard is ANDed outside the `$nor`, and a negated ordering against a
constant is translated as its complement (`!(x > 3)` as `x <= 3`), so a missing field is denied
under both polarities. A declared `value_type` answers an ordering against a constant of another
type as CEL does: false under either polarity. The call-wide `null_attribute_representation: :omitted` has no case spelling, since
the harness uses one mapping, so the contract suite covers it.

### How it is tested

```bash
./scripts/test.sh                                                        # everything (needs Docker)
./scripts/test.sh spec/adapter_contract_spec.rb spec/mongoid_spec.rb     # offline
ADAPTER_TEST_MONGO_IMAGE_FILE=MONGO_NEXT_IMAGE ./scripts/test.sh spec/conformance_spec.rb
```

No suite starts a PDP.

- `spec/conformance_spec.rb` is the conformance harness. It replays every golden plan for both
  pinned PDPs against a real MongoDB server (`MONGO_IMAGE`, or `MONGO_NEXT_IMAGE`), through the
  driver and through Mongoid, and compares the ids with the recorded `check()` decisions.
  `scripts/test.sh` starts the server in Docker, pinned by tag and digest, on a port Docker chooses.
- `spec/mongoid_spec.rb` asserts, offline, that the Mongoid criteria's selector is the emitted
  filter for every corpus case, and that a bare `where` is not.
- `spec/adapter_contract_spec.rb` covers what the corpus cannot vary: `value_parser`, callable
  mappers, unmapped references, mapper validation, the per-call null representation, the plan
  shapes accepted, the line between a refusal and a mapping mistake, and that no source file
  reaches a second collection.

## Mapping hazards

This adapter **builds no subquery**: a relation is a path inside the same document, so the filter
reads exactly the document the application stored. It emits no `$lookup`, `$graphLookup` or
`$unionWith`, and the contract suite asserts that against the source.

| Hazard | Position |
| --- | --- |
| Filtered association | Not applicable — no subquery |
| Default scope on the target model | Not applicable — no second collection is read |
| Subtype discrimination | **Caller-owned.** If several types share a collection, AND your own type predicate with the filter |
| To-one relation used as a collection | Not applicable — a document path holds exactly what the application stored |
| Composite association key | Not applicable — no join |
| Absent to-one parent | **Reproduced**, and proved by the corpus: a `type: :one` relation is required present outside any `$nor`, and `requires_parent` makes a count over a flattened parent UNKNOWN rather than 0 |

## Example

[`example/`](example/) installs the packed gem and runs the shared demo domain's five usage shapes
against a real MongoDB: `../demo/scripts/run-example.sh mongodb-ruby`.
