# Cerbos ActiveRecord Query Plan Adapter

> [!WARNING]
> **A work-in-progress prototype. Do not use this to enforce access control in a live system.**
>
> - **Not released.** No version of this gem is on RubyGems. Version `0.1.0` is a placeholder.
> - **No real-world use.** Nobody runs this in production. Every result below comes from the
>   test corpus in this repository, and a corpus cannot find the shapes of policy, schema and
>   mapping that real applications have and this one has never seen.
> - **The interface can change without warning.** Method names, arguments and the shapes the
>   adapter accepts or refuses can all change, with no deprecation cycle before a first release.
> - **The mapping is yours to get right.** The conformance results prove the *translation*. They
>   cannot prove that your attribute map points at the rows your application put into the Cerbos
>   attributes — see [Mapping hazards](#mapping-hazards). A mistake there is an authorization bug
>   that no test in this repository can see.
>
> Read it, try it, and report what breaks. Do not put it in front of your data yet.

Translates a [Cerbos](https://cerbos.dev) query plan (`PlanResources`) into an
`ActiveRecord::Relation`, so the database applies your Cerbos policies. The result is an ordinary
relation: add scopes, ordering, pagination and eager loading as usual.

The adapter is **fail-closed**: a plan shape it cannot translate exactly raises a
`Cerbos::ActiveRecord::Error`. It never emits an approximate filter, never weakens an operator
(`exists_one` never becomes `exists`), and never lets an unescaped `LIKE` wildcard through.

## Install

The gem is not on RubyGems yet. Install it from this repository:

```ruby
# Gemfile
gem "cerbos" # the official Cerbos Ruby SDK
gem "cerbos-activerecord",
  git: "https://github.com/cerbos/query-plan-adapters.git",
  glob: "activerecord/*.gemspec"
```

Requirements:

- Ruby 3.2+
- ActiveRecord `>= 7.1, < 9.0` (CI tests 7.1 and 8.0)
- Cerbos PDP after v0.40
- The [Cerbos Ruby SDK](https://github.com/cerbos/cerbos-sdk-ruby) (`cerbos` gem) is the expected
  client but not a runtime dependency, because it pulls in a native `grpc` build. `plan:` also
  accepts a parsed `PlanResources` JSON Hash (from the REST API or a cache), or any object with
  `kind` and `condition`.

## Quick start

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
    :tags, member_field: "name", fields: {"name" => Cerbos::ActiveRecord.field("name")}
  )
}

begin
  documents = Cerbos::ActiveRecord.query_plan_to_relation(
    plan: plan, model: Document, attributes: MAPPING
  )
rescue Cerbos::ActiveRecord::Error => e
  # The plan has a shape the adapter cannot translate exactly. Deny, and report it.
  warn "Cerbos plan not translatable: #{e.message}"
  documents = Document.none
end

documents.where(archived: false).order(:created_at).limit(20)
```

You do not need to branch on the plan kind — every kind becomes a relation:

| Plan kind | Result |
| --- | --- |
| `KIND_ALWAYS_ALLOWED` | `model.all` |
| `KIND_ALWAYS_DENIED` | `model.none` |
| `KIND_CONDITIONAL` | `model.where(<translated condition>)` |

If you want to skip the database on a denial, check `plan.kind == :KIND_ALWAYS_DENIED` (the SDK
returns a symbol) before translating.

Errors, all subclasses of `Cerbos::ActiveRecord::Error`:

| Error | Raised when |
| --- | --- |
| `UnmappedAttributeError` | A plan variable is missing from the map, or used where its mapping cannot go (e.g. a relation where a column is needed) |
| `UnsupportedOperatorError` | An operator or operand shape has no exact SQL translation |
| `InvalidPlanError` | The plan is malformed, or holds a literal the adapter cannot represent (a nanosecond timestamp, an empty hierarchy delimiter) |
| `UnsupportedAssociationError` | An attribute maps to an association the adapter cannot turn into a correlated subquery |

## Mapping attributes

Every plan variable must be in the map, or translation raises. The adapter never guesses a column
from an attribute name. The key is the plan variable; the value describes where it lives on your
model, so policy names and column names are independent.

### `field` for scalar columns

```ruby
Cerbos::ActiveRecord.field("status")                 # a column on the model
Cerbos::ActiveRecord.field("owner.department")       # through belongs_to / has_one
Cerbos::ActiveRecord.field("parent.inner.a_string")  # several hops, each to-one
```

- A dotted path becomes a correlated scalar subquery, so it cannot multiply rows. A collection
  association anywhere in the path raises.
- A missing hop gives NULL, so the row is excluded — matching Cerbos, which denies on the missing
  path.
- `R.id` is its own plan variable. Map it if a policy reads it:
  `"request.resource.id" => Cerbos::ActiveRecord.field("id")`.
- `null_representation:` — see [Declare the convention on the attribute](#declare-the-convention-on-the-attribute).

### `relation` for collections

```ruby
Cerbos::ActiveRecord.relation(
  :tags,
  member_field: "name",
  fields: {"name" => Cerbos::ActiveRecord.field("name"),
           "id" => Cerbos::ActiveRecord.field("id")}
)
```

- `member_field` is the column compared when the policy treats the collection as a list of
  values: `"urgent" in R.attr.tags` compares `tag.name`.
- `fields` maps member names used inside macro bodies: `R.attr.tags.exists(t, t.name == "x")`.
  An entry can itself be a `relation`, for multi-hop chains:

```ruby
"request.resource.attr.categories" => Cerbos::ActiveRecord.relation(:categories, fields: {
  "subCategories" => Cerbos::ActiveRecord.relation(:sub_categories, fields: {
    "name" => Cerbos::ActiveRecord.field("name")
  })
})
```

A `has_many :through` is allowed and is expanded into joins inside **one** correlated subquery, so
`size(R.attr.categories.subCategories)` counts the final rows per resource. Each subquery gets
fresh aliases, so nested macros over the same association correlate correctly.

### A chain through a parent

For a dotted policy path such as `R.attr.mainCategory.subCategories`, map the **start** of the
path and nest each later step in `fields`:

```ruby
"request.resource.attr.mainCategory" => Cerbos::ActiveRecord.relation(:categories, fields: {
  "subCategories" => Cerbos::ActiveRecord.relation(:sub_categories, fields: {
    "name" => Cerbos::ActiveRecord.field("name")
  }),
  "subNames" => Cerbos::ActiveRecord.relation(:sub_categories, member_field: "name")
})
```

**Do not map the full path onto one flat `has_many :through`.** The joins are the same, but only
the nested form tells the adapter which hops are to-one parents. When a parent is absent, Cerbos
denies the row (missing path), while a subquery sees "no children" — so `all(...)`,
`!exists(...)` and `size(...) == 0` would read TRUE and return denied rows. With the nested form
the adapter requires the parent hops to exist. A directly mapped relation keeps the normal
empty-collection meaning (`!R.attr.tags.exists(...)` over zero tags is TRUE).

### Principal-attribute collections need no mapping

When a macro iterates a principal attribute, the plan carries the values, so
`P.attr.teams.exists(t, R.attr.owner == t)` becomes `owner = 'team-a' OR owner = 'team-b' ...`
(AND for `all`).

### Associations the adapter refuses

These raise `UnsupportedAssociationError`, because the association returns different rows from
the ones a subquery would read:

| Shape | Why |
| --- | --- |
| Polymorphic `belongs_to` | The target table is unknown until a row is read |
| Association with a scope (including the outer association of a `through:` chain) | The scope's conditions cannot be applied to the subquery's alias |
| Target model with a `default_scope` | Rows the scope hides are absent from the attributes Cerbos evaluates |
| `has_one` used as a collection | Nothing enforces one row; map it as a dotted `field` path instead |
| Association to an STI subclass | It filters on the inheritance column, whose value set depends on which subclasses Ruby has loaded. Point at the base class instead |
| Composite-key association | The correlated subquery joins on one column only |

Map the attribute to a plain, unscoped association, or use an operator override.

## Operator overrides

For a shape your database can express but portable SQL cannot (a dialect regex, JSON containment,
a full-text index), pass a callable. It receives the resolved operands and returns an Arel node:

```ruby
Cerbos::ActiveRecord.query_plan_to_relation(
  plan: plan, model: Document, attributes: MAPPING,
  operator_overrides: {
    "matches" => ->(column, pattern) { column.matches_regexp(pattern) }
  }
)
```

Structural operators cannot be overridden: `and`, `or`, `not`, `if`, `lambda` and the collection
macros.

## The NULL convention of the caller

Your application can send a NULL column to Cerbos in two ways, and the plan looks identical for
both, so you must tell the adapter which one you use:

| Convention | What you send for a NULL column | `R.attr.x == null` |
| --- | --- | --- |
| `:explicit` (default) | An attribute whose value is null | true in Cerbos; `IS NULL` agrees |
| `:omitted` | No attribute | Missing-attribute error; Cerbos denies the row |

Set the fallback for the whole call with `null_attribute_representation:`. Under `:omitted` the
adapter refuses every null constant in the plan (including `!= null`, because a `not` above it
could flip it back into a NULL-selecting predicate):

```ruby
Cerbos::ActiveRecord.query_plan_to_relation(
  plan: plan, model: Document, attributes: MAPPING,
  null_attribute_representation: :omitted
)
# => Cerbos::ActiveRecord::UnsupportedOperatorError when the plan contains a null constant
```

See [#302](https://github.com/cerbos/query-plan-adapters/issues/302).

### Declare the convention on the attribute

One policy suite can use both conventions, so declare it per nullable attribute:

```ruby
MAPPING = {
  "request.resource.attr.owner" => Cerbos::ActiveRecord.field("owner", null_representation: :explicit),
  "request.resource.attr.tag" => Cerbos::ActiveRecord.field("tag", null_representation: :omitted),
  "request.resource.attr.title" => Cerbos::ActiveRecord.field("title") # NOT NULL: inherits the call's value
}
```

A declaration affects only `eq`, `ne` and `in`, which CEL evaluates to a definite boolean over
null. With `:explicit` they are written so they are never SQL UNKNOWN:

```
eq(col, c)     ->  col IS NOT NULL AND col = c
ne(col, c)     ->  NOT (col IS NOT NULL AND col = c)
in(col, [cs])  ->  col IS NOT NULL AND col IN (cs)
eq(a, b)       ->  (a IS NULL AND b IS NULL) OR (a IS NOT NULL AND b IS NOT NULL AND a = b)
```

Undeclared, `NULL != 'x'` is UNKNOWN and excludes the row under both polarities — narrower than
the decision, so safe but not in agreement. Ordering and string operators are unchanged: CEL
raises no-overload on a null receiver, which denies exactly like UNKNOWN.

**Do not mix conventions in a column-to-column comparison.** If one side declares `:explicit` and
the other declares nothing, the adapter raises `UnsupportedOperatorError`. Declare both or
neither. See [#308](https://github.com/cerbos/query-plan-adapters/issues/308) and
[ADR 0004](../docs/adr/0004-the-null-convention-is-a-property-of-the-attribute.md).

## The collation is part of the contract

CEL string comparison is byte-exact. A case-insensitive or otherwise lenient collation makes
`==`, `contains`, `startsWith` and `endsWith` match more rows than the policy allows.

- **SQLite:** set `PRAGMA case_sensitive_like = ON`.
- **MySQL:** use `utf8mb4_0900_bin` (MySQL 8.0.17+) on every column your policies read. `_cs` is
  not enough — `utf8mb4_0900_as_cs` ignores a soft hyphen (U+00AD), and `utf8mb4_bin` is PAD
  SPACE (`'a' = 'a '` is TRUE) ([#474](https://github.com/cerbos/query-plan-adapters/issues/474)).
  Make the **connection** collation byte-exact too: `string()` over a boolean column compares two
  literals, so it uses the connection collation, and under the default `utf8mb4_0900_ai_ci`
  `string(R.attr.flag) == "TRUE"` matches rows CEL does not.

The suites here run on SQLite only; other dialects have no test coverage.

## How the adapter keeps the three-valued logic

CEL denies a row whose condition errors (missing attribute, missing field). SQL UNKNOWN behaves
the same — neither a predicate nor its negation selects it — so the translation preserves UNKNOWN
instead of collapsing it to a boolean. You will see this in the SQL:

- A ternary becomes a `CASE` with **no `ELSE`**, so an UNKNOWN condition yields NULL even under a
  `NOT`.
- Each collection macro becomes a `CASE` with its own error guard: `exists` ignores errors if any
  element is true, `all` if any element is false, `exists_one` never does.
- `string()` over a boolean column becomes a `CASE` starting `WHEN col IS NULL THEN NULL`, then
  spells `'true'`/`'false'` (a plain `CAST` gives `"1"` on SQLite and MySQL).

## Supported operators

Most of the corpus translates directly: `LIKE … ESCAPE` for string operators, correlated `COUNT`
subqueries for relation sizes and `exists_one`, arithmetic and string length computed in the
database, and plain correlated predicates for model-to-model comparisons. What raises:

| Action | Why the adapter raises |
| --- | --- |
| `ts-window`, `ts-vf` | The planner emits a nanosecond `now()` literal; ActiveRecord binds `Time` at microseconds, so the query would compare a different instant. |
| `cr-div-other-column` | Division by another column. The sign of a zero denominator decides ±Infinity, and SQL cannot tell `-0.0` from `0.0`. Dividing a value by itself, or by a constant, is fine. |
| `cr-div-then-add`, `cr-div-then-add-ne` | Arithmetic on a division result that may be non-finite. SQL has no NaN or signed Infinity; a NULL would propagate where CEL propagates NaN. |
| `p-matches` | `matches()` is RE2; no SQL dialect matches it. Use an operator override. |
| `p-index` | `tags[0]` needs row order, which a relation does not have (falls through to the generic unsupported-operator refusal). Use an operator override if you have an ordering column. |
| `p-timestamp` | `timestamp()` on a text column would order by text, not by instant. Map a `datetime` column. |
| `cast-int-string`, `cast-double-string` | CEL parses the whole string or errors; SQL reads leading digits (`CAST('1junk' AS INTEGER)` is `1` on SQLite). |
| `cast-int-double` | CEL truncates toward zero; PostgreSQL and MySQL round. |
| `filter-as-condition`, `map-as-condition` | `filter()`/`map()` as the whole condition is a list, not a boolean. Only `size(filter(...))` and `hasIntersection(map(...), [...])` are boolean. |
| `filter-as-conjunct` | The same, one level below the root (`filter(...) && R.attr.aBool`). Dropping the untranslatable conjunct would over-grant. |
| `index-scalar-list`, `index-number-list`, `index-number-list-not-eq`, `index-bool-list`, `index-bool-list-not-eq`, `index-bool-list-vs-number`, `index-number-list-vs-bool` | Positional access into a relation mapped by member field — no row order, as with `p-index`. The last two compare a boolean with `1` / a number with `true`, which CEL answers false; SQLite stores booleans as 1 and would match. |
| `map-eq-list` | A `map()` projection compared with `==` to a literal list; a correlated subquery has no order to compare element-wise. |
| `hier-empty-delim` | An empty hierarchy delimiter turns `descendentOf` into a prefix test whose `LIKE` would also match the path itself. |

The adapter also raises on an `and`/`or` with no operands and on any operator with the wrong
number of operands. The planner never emits these, but the adapter accepts plans from any source.

## Conformance contract

The tests compare this adapter with the PDP pinned in `../conformance/CERBOS_VERSION` and
`../conformance/CERBOS_IMAGE_DIGEST` (Cerbos 0.55.0), with strict evaluation both disabled and
enabled. For each action the harness plans against a real PDP, translates the plan, runs it
against 27 hostile rows, and compares the returned ids with per-row `checkResource` decisions —
the PDP is the oracle for both sides. The Spring Data adapter is the reference behaviour.

The harness reads `ADAPTER_TEST_STRICT_EVALUATION=false` (default) or `true` and rejects anything
else; CI runs both, each against a PDP configured with the same mode.

| Classification | Coverage |
| --- | --- |
| Tested against the oracle | 236 corpus actions |
| Fail-closed | 72 actions: 61 that this adapter cannot express, and the 11 that the reference adapter does not support either. Each must raise an error whose message the corpus pins, so a typo or a transport error cannot pass as the refusal |
| Refused under the `omitted` NULL convention | 1 action — see [The NULL convention of the caller](#the-null-convention-of-the-caller) |
| Known difference in the planner | The Cerbos planner folds `has()` on a missing attribute to `ALWAYS_ALLOWED`, but `checkResource` denies rows where the attribute is missing. Until the planner is fixed, use `R.attr.x != null` instead of `has(R.attr.x)` for database attributes |

## Mapping hazards

The conformance table covers the **plan**. The other half of the contract is the **mapping**:

> The rows that a subquery of the adapter sees must be the same rows that your application put
> into the resource attributes.

If they differ, the filter returns rows the PDP denies, and no corpus action can see it. Each
hazard below was a real over-grant found while building this adapter
([#314](https://github.com/cerbos/query-plan-adapters/issues/314)):

| Hazard | Position | Mechanism to check |
| --- | --- | --- |
| A filtered association | **Rejected** | `has_many …, -> { where(…) }`. The scope cannot be applied to the subquery's alias. `through:` chains are expanded first, so a scope on the outer association is caught too. |
| A default scope on the target model | **Rejected** | `default_scope` on the association's target. The application applies it on every read; the subquery would not. |
| Subtype discrimination | **Rejected** | An association to an STI subclass also filters on the inheritance column, whose value set depends on which subclasses are loaded. Map onto the base class, or use an operator override. |
| A to-one relation used as a collection | **Rejected** | `has_one`. The application reads one row; the subquery would see all of them. Map it as a dotted field path. |
| A composite association key | **Rejected** | The adapter builds a single-column equality and refuses rather than join on the first column only. |
| An absent to-one parent | **Proved by the corpus** | Write `R.attr.parent.children` as a nested `relation` mapping, not one flat `has_many :through`, so the adapter requires the parent hops to exist. See [A chain through a parent](#a-chain-through-a-parent). The `w1-*-chain` actions hold it under every polarity. |

Five of the six are rejected because the adapter builds its subquery from the association
reflection and can detect them there.

## Behaviour changes

- **Breaking** ([#414](https://github.com/cerbos/query-plan-adapters/issues/414)): size and string
  operators reject numeric and boolean columns.
- **Breaking** ([#414](https://github.com/cerbos/query-plan-adapters/issues/414)): comparisons on
  raw temporal columns require a `timestamp()` wrapper, because SQL discards the RFC 3339 spelling.
- **Breaking** ([#414](https://github.com/cerbos/query-plan-adapters/issues/414)): nested list
  membership is refused before SQL rendering.
- ([#414](https://github.com/cerbos/query-plan-adapters/issues/414)) Negated scalar-list macros,
  omitted scalar membership, hierarchy prefix shortcuts and NaN ordering now preserve CEL's
  null/error behaviour through negation.
- ([#414](https://github.com/cerbos/query-plan-adapters/issues/414)) Comparisons between known
  different scalar types follow CEL equality and missing-value rules instead of letting SQL coerce
  (e.g. `"0"` to a number). Two declared explicit nulls still compare equal.
- Constant NaN ordering follows Cerbos 0.55: an unordered comparison is false, so its negation is
  true. Under Cerbos 0.54 it was an evaluation error and stayed denied under negation. Missing
  attributes and other evaluation errors are unchanged.

## Example application

[`example/`](example/) runs the shared demo domain against the packed gem:
`../demo/scripts/run-example.sh activerecord` from this directory. See
[example/README.md](example/README.md).

## Development

Everything runs in Docker; you do not need Ruby locally. The PDP version comes from
`conformance/CERBOS_VERSION`.

```bash
./scripts/test.sh                                   # all suites
./scripts/test.sh spec/translator_spec.rb           # offline: no PDP, no database server
./scripts/golden-update.sh                          # rewrite golden/expectations.json
RUBY_VERSION=3.2 ACTIVERECORD_VERSION=7.1 ./scripts/test.sh
./scripts/lint.sh
```

The `tests` service mounts the repository root, because the suites read `../conformance/`.

| Suite | What it covers | Needs |
| --- | --- | --- |
| `spec/translator_spec.rb` | Translator unit test: replays every plan in `../conformance/wire-fixtures/` and asserts the SQL against [`golden/expectations.json`](golden/expectations.json), plus corpus-wide rules (every `LIKE` has an `ESCAPE`, no self-join of the resource table, every identifier names a declared table) | Nothing |
| `spec/adapter_contract_spec.rb` | What a caller supplies: mapper forms, operator overrides, the per-call NULL convention, the four plan transports, refused association shapes | Nothing |
| `spec/adversarial_conformance_spec.rb` | Differential harness over [`../conformance/`](../conformance/README.md) | Docker (starts a pinned PDP) |

**Golden expectations.** `golden/expectations.json` records each action's relation rendered with
`to_sql` on SQLite, literals inlined. Because ActiveRecord's renderer shapes those bytes, the file
declares `"activerecord": "8.0"`, `golden-update.sh` refuses to run under another minor series,
and the 7.1 leg asserts a pinned divergence list. `./scripts/test.sh` never regenerates it: run
`./scripts/golden-update.sh` and review the diff. See
[conformance/README.md, "Golden expectations"](../conformance/README.md#golden-expectations).
