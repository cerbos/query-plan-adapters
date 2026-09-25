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

- Ruby 3.3+
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
| `InvalidPlanError` | The plan is malformed, or holds a literal the adapter cannot represent (a nanosecond timestamp, a hierarchy delimiter that is not a string) |
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

Set the fallback for the whole call with `null_attribute_representation:`. Under `:omitted` a
NULL column is a missing attribute, which CEL answers with an error, and a present column is never
null. So `==` and `!=` between a field attribute and `null` are rendered UNKNOWN for a NULL column,
which stays UNKNOWN under any `not` above it:

```
eq(col, null)  ->  CASE WHEN col IS NULL THEN NULL ELSE FALSE END
ne(col, null)  ->  CASE WHEN col IS NULL THEN NULL ELSE TRUE END
```

This holds for a column reached through a to-one path such as `parent.tag`, where an absent parent
is NULL too. Every other null constant is refused under `:omitted`: a null in an `in` or
`hasIntersection` list, and a null given to an operator override of `eq` or `ne`, since the
override would receive it:

```ruby
Cerbos::ActiveRecord.query_plan_to_relation(
  plan: plan, model: Document, attributes: MAPPING,
  null_attribute_representation: :omitted
)
# => Cerbos::ActiveRecord::UnsupportedOperatorError when the plan holds such a null constant
```

See [#302](https://github.com/cerbos/query-plan-adapters/issues/302) and
[#551](https://github.com/cerbos/query-plan-adapters/issues/551).

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

> [!WARNING]
> **Do not guard with `has()`: write `R.attr.x != null`.** An attribute the plan request omits is
> unknown to the planner, which assumes the data layer supplies it: for a table, the column exists
> and only its value is open. So the planner reads `has(R.attr.x)` as the guard for the `x` access
> beside it and folds it to true by design: alone it plans as `ALWAYS_ALLOWED`, and
> `has(R.attr.x) && R.attr.y > 0` plans as `R.attr.y > 0`. It never excludes a row whose `x` is NULL.
> `R.attr.x != null` stays in the plan, where the adapter translates it or refuses it, and agrees
> with `check()` whether `x` is missing, null or present.

## The collation is part of the contract

CEL string comparison is byte-exact. A case-insensitive or otherwise lenient collation makes
`==`, `contains`, `startsWith` and `endsWith` match more rows than the policy allows. CEL also
orders strings by code point, and `<`, `<=`, `>` and `>=` follow the collation.

- **SQLite:** set `PRAGMA case_sensitive_like = ON`. The default `BINARY` collation orders by code
  point.
- **PostgreSQL:** every collation is deterministic, so equality is exact, but string ordering needs
  a byte-order collation, `"C"`. A linguistic one such as glibc's `en_US.UTF-8` (the usual default
  on Debian images and managed services) or ICU's `en-US` sorts `"One"` after `"a"`, so
  `R.attr.name > "a"` over-grants it
  ([#489](https://github.com/cerbos/query-plan-adapters/issues/489)). Create the database with
  `LC_COLLATE 'C'`, or declare `COLLATE "C"` on each column a policy orders.
- **MySQL:** use `utf8mb4_0900_bin` (MySQL 8.0.17+) on every column your policies read. `_cs` is
  not enough — `utf8mb4_0900_as_cs` ignores a soft hyphen (U+00AD), and `utf8mb4_bin` is PAD
  SPACE (`'a' = 'a '` is TRUE) ([#474](https://github.com/cerbos/query-plan-adapters/issues/474)).
  `utf8mb4_0900_bin` orders by code point. Make the **connection** collation byte-exact too
  (`collation: utf8mb4_0900_bin` in the mysql2 config, which ActiveRecord applies with
  `SET NAMES`): `string()` of a column is a `CAST` or a `CASE` over literals, so it takes the
  connection collation, and under the default `utf8mb4_0900_ai_ci` `string(R.attr.owner) == "set"`
  also matches `Set`.

The conformance suite runs on SQLite, PostgreSQL (initialised with `--lc-collate=C`) and MySQL
(`utf8mb4_0900_bin` on the columns and the connection). The other suites run on SQLite.

## How the adapter keeps the three-valued logic

CEL denies a row whose condition errors (missing attribute, missing field). SQL UNKNOWN behaves
the same — neither a predicate nor its negation selects it — so the translation preserves UNKNOWN
instead of collapsing it to a boolean. You will see this in the SQL:

- A ternary becomes a `CASE` with **no `ELSE`**, so an UNKNOWN condition yields NULL even under a
  `NOT`.
- Each collection macro becomes a `CASE` with its own error guard: `exists` ignores errors if any
  element is true, `all` if any element is false, `exists_one` never does.
- `string()` over a boolean — a boolean column, or any comparison, logical operator, `in` or
  string predicate — becomes `CASE WHEN b THEN 'true' WHEN NOT (b) THEN 'false' END`, again with
  no `ELSE`, so a NULL stays NULL (a plain `CAST` gives `"1"` on SQLite and MySQL). Any other
  `string()` is a `CAST` to `TEXT`, or to `CHAR` on MySQL, whose `CAST` accepts no `TEXT`.

## Supported operators

Most of the corpus translates directly: `LIKE … ESCAPE` for string operators, correlated `COUNT`
subqueries for relation sizes and `exists_one`, arithmetic and string length computed in the
database, and plain correlated predicates for model-to-model comparisons. What raises (the
full list, with reasons, is [`conformance-ledger.json`](conformance-ledger.json)):

| Case | Why the adapter raises |
| --- | --- |
| `timestamp/less-than/relative-window`, `timestamp/greater-than/relative-window-value-first` | The planner emits a nanosecond `now()` literal; ActiveRecord binds `Time` at microseconds, so the query would compare a different instant. |
| `arithmetic/divide/field-by-field` | Division by another column. The sign of a zero denominator decides ±Infinity, and SQL cannot tell `-0.0` from `0.0`. Dividing a value by itself, or by a constant, is fine. |
| `arithmetic/add/self-division-plus-constant-greater-than`, `arithmetic/add/self-division-plus-constant-not-equals` | Arithmetic on a division result that may be non-finite. SQL has no NaN or signed Infinity; a NULL would propagate where CEL propagates NaN. |
| `regex/matches/anchored-prefix` | `matches()` is RE2; no SQL dialect matches it. Use an operator override. |
| `collection/index/first-element-of-object-list` | `tags[0]` needs row order, which a relation does not have (falls through to the generic unsupported-operator refusal). Use an operator override if you have an ordering column. |
| `cast/timestamp/malformed-string` | `timestamp()` on a text column would order by text, not by instant. Map a `datetime` column. |
| `cast/int/malformed-string`, `cast/double/malformed-string` | CEL parses the whole string or errors; SQL reads leading digits (`CAST('1junk' AS INTEGER)` is `1` on SQLite). |
| `cast/int/negative-fraction` | CEL truncates toward zero; PostgreSQL and MySQL round. |
| `cast/string/from-int-beyond-double-precision`, `cast/string/from-int-past-exponent-threshold`, `cast/string/negated-from-int-past-exponent-threshold` | `string()` over `int()` of a string or double column: the `int()` is refused for the reasons above. |
| `collection/exists/map-keys`, `collection/exists/negated-map-keys` | A macro over the to-one `parent` ranges over a map's keys. A to-one association is not a collection, and SQL cannot list which of a row's columns are non-NULL as keys. |
| `collection/filter/as-whole-condition`, `collection/map/as-whole-condition` | `filter()`/`map()` as the whole condition is a list, not a boolean. Only `size(filter(...))` and `hasIntersection(map(...), [...])` are boolean. |
| `collection/filter/as-conjunct` | The same, one level below the root (`filter(...) && R.attr.aBool`). Dropping the untranslatable conjunct would over-grant. |
| `collection/index/first-element-of-string-list`, `collection/index/first-element-of-number-list`, `collection/index/negated-first-element-of-number-list`, `collection/index/first-element-of-boolean-list`, `collection/index/negated-first-element-of-boolean-list`, `type-mismatch/equals/boolean-list-element-against-number-literal`, `type-mismatch/equals/number-list-element-against-boolean-literal` | Positional access into a relation mapped by member field — no row order, as with `collection/index/first-element-of-object-list`. The last two compare a boolean with `1` / a number with `true`, which CEL answers false; SQLite stores booleans as 1 and would match. |
| `collection/map/equals-list-literal` | A `map()` projection compared with `==` to a literal list; a correlated subquery has no order to compare element-wise. |
| `arithmetic/modulo/negated-double-operand` | `%` over an attribute that has not gone through `int()`. Every number in a request attribute is a double and CEL's `%` has no double overload, so the row errors; SQL would compute a remainder. |
| `cast/string/from-negative-zero-double` | `string()` over a double is compared as the number its literal spells in CEL (`"1e+06"` is `1000000.0`), since SQL spells doubles differently. `"-0"` and `"0"` are refused: SQL cannot tell `-0.0` from `0.0`. |

The adapter also raises on an `and`/`or` with no operands and on any operator with the wrong
number of operands. The planner never emits these, but the adapter accepts plans from any source.

## Conformance contract

`spec/conformance_spec.rb` replays every plan recorded in
[`../conformance/golden/`](../conformance/README.md), for both pinned PDPs, against the corpus
rows and compares the ids with the ones `check()` allowed. It needs no PDP. It runs on SQLite,
PostgreSQL and MySQL, and every store gives the same results. On the current PDP (Cerbos 0.55.0),
cases that return exactly the allowed rows, out of every golden case in the tier:

| Tier | Passed / total |
| --- | --- |
| core | 26 / 26 |
| extended | 58 / 80 |
| adversarial | 233 / 314 |

Every other case is either refused with a `Cerbos::ActiveRecord::Error`, which the harness
asserts, or listed as a known wrong result. [`conformance-ledger.json`](conformance-ledger.json)
gives the reason for each. A case whose golden file records a `plannerDivergence` for the PDP is
skipped, because the plan and `check()` disagree and no adapter can pass it. On 0.55.0 those are
four extended cases and three adversarial cases. In `null/has/missing-attribute` and
`null/has/composed-with-comparison` the plan request leaves an omitted attribute unknown, so the
planner folds `has()` to true by design, while `check()` receives the omission as absent and denies
the row. Use `R.attr.x != null` instead of `has(R.attr.x)`. In `arithmetic/add/int-literal-plus-constant` and `arithmetic/add/int-literal-negated` the
planner drops the int type of the literal in `R.attr.x + 1`, so the plan is the double spelling's, while `check()` has no double + int overload and denies every row; write `1.0`. In the other three, all `composition/*`, a DENY condition reads `aNumber`, which j2 lacks: the plan's `not(...)` of it
denies j2, while `check()` receives `aNumber` as absent, treats the erroring DENY as not matching
and lets the ALLOW stand ([#530](https://github.com/cerbos/query-plan-adapters/issues/530)).

## Mapping hazards

The conformance table covers the **plan**. The other half of the contract is the **mapping**:

> The rows that a subquery of the adapter sees must be the same rows that your application put
> into the resource attributes.

If they differ, the filter returns rows the PDP denies, and no corpus case can see it. Each
hazard below was a real over-grant found while building this adapter
([#314](https://github.com/cerbos/query-plan-adapters/issues/314)):

| Hazard | Position | Mechanism to check |
| --- | --- | --- |
| A filtered association | **Rejected** | `has_many …, -> { where(…) }`. The scope cannot be applied to the subquery's alias. `through:` chains are expanded first, so a scope on the outer association is caught too. |
| A default scope on the target model | **Rejected** | `default_scope` on the association's target. The application applies it on every read; the subquery would not. |
| Subtype discrimination | **Rejected** | An association to an STI subclass also filters on the inheritance column, whose value set depends on which subclasses are loaded. Map onto the base class, or use an operator override. |
| A to-one relation used as a collection | **Rejected** | `has_one`. The application reads one row; the subquery would see all of them. Map it as a dotted field path. |
| A composite association key | **Rejected** | The adapter builds a single-column equality and refuses rather than join on the first column only. |
| An absent to-one parent | **Proved by the corpus** | Write `R.attr.parent.children` as a nested `relation` mapping, not one flat `has_many :through`, so the adapter requires the parent hops to exist. See [A chain through a parent](#a-chain-through-a-parent). The `relation/*/…to-one-chain` cases hold it under every polarity. |

Five of the six are rejected because the adapter builds its subquery from the association
reflection and can detect them there.

## Behaviour changes

See [CHANGELOG.md](CHANGELOG.md).

## Example application

[`example/`](example/) runs the shared demo domain against the packed gem:
`../demo/scripts/run-example.sh activerecord` from this directory. See
[example/README.md](example/README.md).

## Development

Everything runs in Docker; you do not need Ruby locally, and no suite needs a PDP.

```bash
./scripts/test.sh                                   # all suites
./scripts/test.sh spec/conformance_spec.rb          # the conformance harness alone
ADAPTER_TEST_DB=postgres ./scripts/test.sh spec/conformance_spec.rb   # on PostgreSQL (or mysql)
RUBY_VERSION=3.3 ACTIVERECORD_VERSION=7.1 ./scripts/test.sh
./scripts/lint.sh                                   # RuboCop on Standard, via `rake lint`
./scripts/docs.sh                                   # YARD, failing on a warning or an undocumented object
```

The `tests` service mounts the repository root, because the suites read `../conformance/`.
`ADAPTER_TEST_DB` picks the store: `sqlite` (the default, in memory), `postgres` or `mysql`, which
`scripts/test.sh` starts from `docker-compose.yaml` with the images pinned in
[`POSTGRES_IMAGE`](POSTGRES_IMAGE) and [`MYSQL_IMAGE`](MYSQL_IMAGE). Any other value fails.
Specs run in random order; rerun a failure with the seed RSpec prints (`--seed N`).

| Suite | What it covers |
| --- | --- |
| `spec/conformance_spec.rb` | The conformance harness over [`../conformance/`](../conformance/README.md): one mapping, every recorded plan, and the ledger |
| `spec/adapter_contract_spec.rb` | What a caller supplies: mapper forms, operator overrides, the per-call NULL convention, the four plan transports, refused association shapes |
