# Cerbos ActiveRecord Query Plan Adapter

An adapter to convert a [Cerbos](https://cerbos.dev) query plan (`PlanResources`) into an
`ActiveRecord::Relation`, so authorization rules written as Cerbos policies are enforced by the
database instead of by application code.

The result is an ordinary relation, so it composes with scopes, ordering, pagination and eager
loading:

```ruby
documents = Cerbos::ActiveRecord.query_plan_to_relation(
  plan: plan, model: Document, attributes: MAPPING
)

documents.where(archived: false).order(:created_at).limit(20)
```

## Fail-closed by design

A plan shape this adapter cannot express faithfully **raises** rather than returning a
best-effort filter. That is the central guarantee: a wrong filter is an authorization bug that
returns rows the PDP denies, whereas a raise is a bug report. The adapter never degrades an
operator into a weaker one — `exists_one` never becomes `exists`, an inexpressible `LIKE`
needle never becomes an unescaped wildcard.

### Conformance contract

The adapter is differentially tested against Cerbos PDP `0.54.0`: every action is planned
against a real PDP, translated, executed against 20 hostile seed rows, and the returned ids are
compared with per-row `checkResource` decisions. The PDP is the oracle for both sides — there
are no hand-written expectations. The Spring Data adapter defines the reference semantics.

| Classification | Coverage |
| --- | --- |
| Oracle-tested | 112 reference actions |
| Fail-closed | 2 reference actions plus the 3 reference-unsupported shapes (5 actions total) |
| Known planner divergence | `has()` on a missing attribute is folded by the Cerbos planner to `ALWAYS_ALLOWED`, while `checkResource` denies the missing-attribute rows. Until the planner is fixed, use `R.attr.x != null` for database-backed attributes instead of `has(R.attr.x)` |

The fail-closed set is small because SQL can express most of the corpus directly: `LIKE` is
emitted with an explicit `ESCAPE` clause, relation counts and `exists_one` become correlated
`COUNT` subqueries, column arithmetic and string lengths are computed in the database, and
cross-model comparisons are ordinary correlated predicates. What remains:

| Action | Why it raises |
| --- | --- |
| `ts-window`, `ts-vf` | The planner emits a nanosecond-precision `now()` literal. ActiveRecord binds a `Time` into SQL at microsecond precision, so translating it would compare against a different instant than the policy specifies. |
| `p-matches` | `matches()` is RE2. No SQL dialect guarantees RE2 semantics, and `LIKE` cannot express a regex. |
| `p-index` | `tags[0]` indexes a list positionally; a relation has no inherent order. |
| `p-timestamp` | `timestamp()` over a column holding a *formatted timestamp string*. Comparing that against a bound `Time` compares two different textual formats, so the ordering would be lexicographic accident. Map the attribute to a `datetime` column instead. |

The adapter also raises for a bare `filter()` or `map()` used as a condition, which evaluate to
a list rather than a boolean.

### Collation is part of the contract

CEL string matching is case-sensitive. `LIKE` collation is dialect-controlled, so a
case-insensitive configuration will make `contains`/`startsWith`/`endsWith` match more rows
than the policy allows. On SQLite, set `PRAGMA case_sensitive_like = ON`; on MySQL, use a
`_bin` or `_cs` collation for the columns a policy matches against.

The suites here run on SQLite only. A dialect the suite does not exercise is not covered.

## Requirements

- Ruby >= 3.2
- ActiveRecord >= 7.0, < 9.0 (CI exercises 7.1 and 8.0)
- Cerbos > v0.40
- The official [Cerbos Ruby SDK](https://github.com/cerbos/cerbos-sdk-ruby)
  ([`cerbos`](https://rubygems.org/gems/cerbos) gem)

### Why the SDK is not a hard dependency

This gem declares no runtime dependency on `cerbos`, because that SDK is gRPC-based and would
pull a native `grpc` build into applications that talk to the PDP over REST instead. The SDK
is nonetheless the expected client and the shape the adapter is built around: `plan:` accepts
a `Cerbos::Output::PlanResources` directly, both test suites drive real
`Cerbos::Client#plan_resources` responses through the adapter, and the SDK's output types are
asserted against by name in `spec/translator_spec.rb`.

If you obtain plans another way — the REST API, a cached response — pass the decoded JSON, or
any object exposing `kind` and `condition`, and it will translate identically.

## Installation

```bash
bundle add cerbos-activerecord cerbos
```

## Usage

```ruby
require "cerbos"
require "cerbos/active_record"

cerbos = Cerbos::Client.new("localhost:3593", tls: false)

plan = cerbos.plan_resources(
  principal: {id: "user@example.com", roles: ["USER"]},
  resource: {kind: "document"},
  action: "view"
)

MAPPING = {
  "request.resource.attr.ownerId" => Cerbos::ActiveRecord.field("owner_id"),
  "request.resource.attr.status" => Cerbos::ActiveRecord.field("status"),
  "request.resource.attr.department" => Cerbos::ActiveRecord.field("owner.department"),
  "request.resource.attr.tags" => Cerbos::ActiveRecord.relation(
    :tags,
    member_field: "name",
    fields: {"name" => Cerbos::ActiveRecord.field("name")}
  )
}

documents = Cerbos::ActiveRecord.query_plan_to_relation(
  plan: plan, model: Document, attributes: MAPPING
)
```

`plan` may be a `Cerbos::Output::PlanResources` from the
[Ruby SDK](https://github.com/cerbos/cerbos-sdk-ruby), the decoded JSON of a `PlanResources`
response, or any object exposing `kind` and `condition` in those shapes.

The three plan kinds map onto relations directly:

| Plan kind | Result |
| --- | --- |
| `KIND_ALWAYS_ALLOWED` | `model.all` |
| `KIND_ALWAYS_DENIED` | `model.none` |
| `KIND_CONDITIONAL` | `model.where(<translated condition>)` |

### Mapping attributes

Every plan variable must be mapped, or translation raises. Nothing is inferred from column
names, because guessing a column is how an authorization filter silently starts matching the
wrong data.

#### `field` — scalar columns

```ruby
Cerbos::ActiveRecord.field("status")            # a column on the model
Cerbos::ActiveRecord.field("owner.department")  # through a belongs_to / has_one
```

A dotted path traverses to-one associations and is emitted as a **correlated scalar
subquery**, so it can never multiply the result set the way a join can. A collection
association in a dotted path raises — a scalar comparison against "some element" is not what
the policy asked for.

#### `relation` — collections

```ruby
Cerbos::ActiveRecord.relation(
  :tags,
  member_field: "name",
  fields: {"name" => Cerbos::ActiveRecord.field("name"),
           "id" => Cerbos::ActiveRecord.field("id")}
)
```

- `member_field` stands in for the element wherever the policy treats the collection as a list
  of bare values, so `"urgent" in R.attr.tags` compares against `tag.name`.
- `fields` maps the member names used inside lambda bodies, so
  `R.attr.tags.exists(t, t.name == "x")` can resolve `t.name`. Entries may themselves be
  relations, which is how multi-hop chains resolve:

```ruby
"request.resource.attr.categories" => Cerbos::ActiveRecord.relation(:categories, fields: {
  "subCategories" => Cerbos::ActiveRecord.relation(:sub_categories, fields: {
    "name" => Cerbos::ActiveRecord.field("name")
  })
})
```

`has_many :through` is supported and expands into joins **inside one correlated subquery**,
rather than an `EXISTS` nested in an `EXISTS`. That distinction matters for counting operators:
`size(R.attr.categories.subCategories)` must count leaf rows per resource, not per category.

Every subquery gets fresh table aliases, so a macro nested over the same association as its
parent correlates against the outer row rather than its own.

Two association shapes raise rather than guess: a polymorphic `belongs_to` (its target table
is unknown until a row is read) and an association carrying a scope (whose conditions cannot be
re-bound onto the generated alias). Map the attribute onto a concrete, unscoped association, or
supply an operator override.

#### `operator_overrides` — schema-specific translations

```ruby
Cerbos::ActiveRecord.query_plan_to_relation(
  plan: plan, model: Document, attributes: MAPPING,
  operator_overrides: {
    "matches" => ->(column, pattern) { column.matches_regexp(pattern) }
  }
)
```

An override receives the resolved operands and returns an Arel node. Use it where a particular
database can express a shape faithfully that portable SQL cannot — a dialect regex, a JSON
containment operator, a full-text index. Structural operators (`and`, `or`, `not`, `if`,
`lambda`, and the collection macros) cannot be overridden, because their operands are
deliberately not resolved before they run.

## How three-valued logic is preserved

CEL denies a resource when evaluating its condition raises — a missing attribute, an element
whose field is absent. SQL's `UNKNOWN` behaves the same way: it is excluded by a predicate
*and* by that predicate's negation, so `NOT (NULL = x)` stays `UNKNOWN` rather than becoming
true.

The translation preserves `UNKNOWN` rather than collapsing it. Two consequences are visible in
the generated SQL:

- **Ternaries compile to a `CASE` with no `ELSE`.** When the condition is `UNKNOWN`, the
  `CASE` yields `NULL`, keeping the row excluded under both polarities. An `ELSE` would leak
  those rows into the else-branch.
- **Collection macros compile to `CASE` expressions, not bare `EXISTS`.** CEL's quantifiers
  differ precisely in how they treat an element whose body errored: `exists` absorbs errors
  behind a true witness, `all` absorbs them behind a false witness, and `exists_one` never
  absorbs them. Each gets its own error guard.

## Development

Everything runs in Docker against a PDP pinned to `conformance/CERBOS_VERSION` — no Ruby
toolchain on the host is needed.

```bash
./scripts/test.sh                                   # both suites
./scripts/test.sh spec/adversarial_conformance_spec.rb
RUBY_VERSION=3.2 ./scripts/test.sh                  # a different Ruby
./scripts/lint.sh
```

The `tests` service mounts the **repository root**, because the adversarial harness reads the
shared corpus at `../conformance/` (`seeds.json`, `actions.json`, `CERBOS_VERSION`).

Two suites run:

- `spec/shared_policy_spec.rb` — the shared policy suite (`/policies/resource.yaml`) that every
  adapter in this repository is exercised against. Actions are discovered from the policy file,
  so an action added there cannot silently go untested.
- `spec/adversarial_conformance_spec.rb` — the shared adversarial corpus (`/conformance/`),
  implementing the oracle recipe in
  [conformance/README.md](../conformance/README.md).
