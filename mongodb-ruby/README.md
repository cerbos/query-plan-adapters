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
corpus action. Mongoid is not a dependency of the gem: only `cerbos/mongodb/mongoid` loads it, and
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
| `nullable` | A stored `null` is a **missing** Cerbos attribute (the application omits it from `check()`). Comparisons then keep null documents out, and `not` over the field is refused. Do not set it where `null` is an explicit Cerbos value. |
| `value_parser` | Rewrites each constant compared with the field — for example a string id into a `BSON::ObjectId`. |
| `value_type` | `:number`, `:string`, `:boolean` or `:date_time`. Settles an equality against a constant of another type without a query, and refuses shapes a stored `Date` cannot answer (a bare comparison of two date fields: MongoDB has discarded the strings CEL compares). |
| `relation` | `type: :one` for an embedded subdocument (a to-one hop, required to be present outside any negation), `type: :many` for an array of subdocuments (`$elemMatch`). `field` names the element field the relation stands for, `fields` maps element fields, `requires_parent` names an optional to-one parent array the path is reached through. |

Without a mapper the plan's paths are used verbatim, which only works if your documents use
Cerbos's attribute paths as field names.

### NULL attribute representation

`R.attr.x == null` reaches the adapter as the same plan node however your application sends a NULL
field to `check()`, so it has to be told:

| attributes you send for a NULL field | `check()` on that document | null-matching filter |
| --- | --- | --- |
| `{"x" => nil}` — explicit null | allow | selects it — aligned |
| `{}` — attribute omitted | **deny** (missing attribute) | selects it — **over-grants** |

`null_attribute_representation:` defaults to `:explicit`. Pass `:omitted` and every null comparison
operand is refused instead of translated
([#302](https://github.com/cerbos/query-plan-adapters/issues/302)). `nullable: true` states the
same thing per field, and is what lets one mapping mix the two conventions.

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

The adapter is differentially tested against Cerbos PDP 0.55.0 `checkResource` decisions, in both
evaluation modes, using the shared hostile corpus's 27 seed documents executed with the Ruby driver
against **real MongoDB 7.0 and 8.0 servers**. The Spring Data adapter defines the reference
semantics.

| Classification | Coverage |
| --- | --- |
| Oracle-tested | 199 reference conformance actions plus the 4 reference-unsupported shapes MongoDB can express — regular expressions, positional reads (`$arrayElemAt`/`$getField`), RFC 3339 timestamps, and a field-to-field comparison under mixed null conventions (203 actions) |
| Fail-closed | 89 reference actions plus the 7 reference-unsupported shapes this adapter does not promote (96 actions) |
| Representation-dependent | `null-eq-missing` — refused under `:omitted`. Under the default it already returns the empty set the PDP demands, because `nullable: true` declares that a stored null is a missing attribute |
| Through Mongoid | The same 203 actions, run through `Cerbos::MongoDB::Mongoid.criteria` on typed Mongoid models (embedded relations included), match the oracle too |
| Known planner divergence | `has()` on a missing attribute is folded by the planner to `ALWAYS_ALLOWED` while `checkResource` denies. Use `R.attr.x != null` instead of `has(R.attr.x)` until the planner is fixed |

The fail-closed set is exact-one cardinality, aggregation expressions or outer-document references
inside `$elemMatch` (MongoDB accepts `$expr` only at the top level), CEL's `int()`/`double()`
(`$convert` parses a numeric prefix and rounds where CEL raises and truncates), division by
anything but a non-zero constant (`$divide` by zero aborts the query), `+` between two fields
(nothing tells `$add` from `$concat`), negations over nullable fields or collection macros (a
filter has no UNKNOWN), regular expressions outside the common subset, whole-list comparisons and
list equality over a `map()` projection. Every refusal's message is pinned in
`conformance/actions.json` and asserted by both the conformance run and the translator unit test.

It shares its MongoDB semantics with the [Mongoose adapter](../mongoose/) and differs in one place:
the driver sends a filter to the server untouched, so a comparison between two conditionals
(`p-ternary-vs-ternary`) translates here, where Mongoose's `$expr` caster fails to build it.

### How it is tested

```bash
./scripts/test.sh                                         # everything (needs Docker)
./scripts/test.sh spec/translator_spec.rb spec/adapter_contract_spec.rb   # offline
ADAPTER_TEST_STRICT_EVALUATION=true ADAPTER_TEST_MONGO_IMAGE_FILE=MONGO_NEXT_IMAGE \
  ./scripts/test.sh spec/adversarial_conformance_spec.rb
```

- `spec/adversarial_conformance_spec.rb` plans every corpus action against a real PDP, runs the
  filter against a real MongoDB server (`MONGO_IMAGE`, or `MONGO_NEXT_IMAGE`) and compares the ids
  with per-document `check()` decisions. `scripts/test.sh` starts both in Docker, pinned by tag and
  digest, on ports Docker chooses.
- `spec/translator_spec.rb` is the translator unit test: it replays `conformance/wire-fixtures/`
  offline and pins every emitted filter in `golden/expectations.json` — the translator's return
  value verbatim, with a `Time` written as `{"$date": "<ISO 8601>"}`. Regenerate with
  `./scripts/golden-update.sh` and review the diff; CI never regenerates.
- `spec/mongoid_spec.rb` asserts, offline, that the Mongoid criteria's selector is the emitted
  filter for every corpus action, and that a bare `where` is not.
- `spec/adapter_contract_spec.rb` covers what the corpus cannot vary: `value_parser`, callable
  mappers, mapper validation, the per-call null representation and the plan shapes accepted.

## Mapping hazards

This adapter **builds no subquery**: a relation is a path inside the same document, so the filter
reads exactly the document the application stored. It emits no `$lookup`, `$graphLookup` or
`$unionWith`, and the conformance suite asserts that against both the source and every emitted
filter.

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
