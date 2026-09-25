# Cerbos Sequel Query Plan Adapter

> [!WARNING]
> **A work-in-progress prototype. Do not use this to enforce access control in a live system.**
>
> - **Not released.** No version of this gem is on RubyGems. Version `0.1.0` is a placeholder.
> - **No real-world use.** Nobody runs this in production. Every result below comes from the
>   test corpus in this repository, and a corpus is not a deployment: it cannot find the shapes
>   of policy, schema and mapping that real applications have and this one has never seen.
> - **The interface can change without warning.** Method names, arguments and the shapes that
>   the adapter accepts or refuses can all still change, and there is no deprecation cycle
>   until a first release.
> - **The mapping is yours to get right.** The conformance results below prove the
>   *translation*. They cannot prove that your attribute map points at the rows your
>   application put into the Cerbos attributes — see [Mapping hazards](#mapping-hazards). A
>   mistake there is an authorization bug that no test in this repository can see.
>
> Read it, try it, and report what breaks. Do not put it in front of your data yet.

An adapter that changes a [Cerbos](https://cerbos.dev) query plan (`PlanResources`) into a
filtered [Sequel](https://sequel.jeremyevans.net) `Dataset`. Thus the database applies the
authorization rules from your Cerbos policies, and your application code does not.

The result is a usual dataset of your model. Thus you can add filters, an order, pagination
and eager loading to it:

```ruby
documents = Cerbos::Sequel.query_plan_to_dataset(
  plan: plan, model: Document, attributes: MAPPING
)

documents.where(archived: false).order(:created_at).limit(20).all
```

## The adapter is fail-closed

If the adapter cannot translate a shape of plan correctly, it **raises an error**. It does not
give a filter that is only approximately correct. This is the primary guarantee of the adapter.
An incorrect filter is an authorization bug, because it gives rows that the PDP denies. An
error is a bug report.

The adapter never changes an operator into a weaker operator. It never changes `exists_one`
into `exists`. If it cannot escape a `LIKE` needle, it never lets the wildcards stay.

### Conformance contract

`spec/conformance_spec.rb` replays every plan recorded in
[`../conformance/golden/`](../conformance/README.md), for both pinned PDPs, against the corpus
rows and compares the ids with the ones `check()` allowed. It needs no PDP. It runs on SQLite,
PostgreSQL and MySQL, and every store gives the same results. On the current PDP (Cerbos 0.55.0),
cases that return exactly the allowed rows, out of every golden case in the tier:

| Tier | Passed / total |
| --- | --- |
| core | 26 / 26 |
| extended | 58 / 80 |
| adversarial | 238 / 308 |

Every other case is refused with a `Cerbos::Sequel::Error`, which the harness asserts.
[`conformance-ledger.json`](conformance-ledger.json) gives the reason for each. None is a known
wrong result. A case whose golden file records a `plannerDivergence` for the PDP is skipped,
because the plan and `check()` disagree and no adapter can pass it: on 0.55.0 those are four
extended cases and three adversarial cases. In `null/has/missing-attribute` and
`null/has/composed-with-comparison` the planner folds `has()` to true by design, while `check()`
denies the row whose attribute is absent: write `R.attr.x != null` instead of `has(R.attr.x)`.
In `arithmetic/add/int-literal-plus-constant` and `arithmetic/add/int-literal-negated` the plan
drops the int type of the literal in `R.attr.x + 1`, while `check()` has no double + int overload
and denies every row: write `1.0`. In the three `composition/*` cases a DENY condition reads an
attribute one row lacks, which the plan and `check()` treat differently
([#530](https://github.com/cerbos/query-plan-adapters/issues/530)).

Some shapes that are CEL errors on every row are translated rather than refused, as SQL
UNKNOWN, which denies under both polarities exactly as the error does:

- `size()`, `contains`, `startsWith` or `endsWith` over a number or a boolean (a numeric or
  boolean column, a computed number or boolean, or a constant). CEL has no such overload, so
  the declared type decides the error, whatever the row holds.
- A collection where a boolean belongs: `filter()`, `map()` or a mapped association as a
  condition, a conjunct or the operand of `!`. CEL's logical operators take only a boolean.

The refusals fall into a few mechanisms:

| Shape | Why the adapter raises an error |
| --- | --- |
| `timestamp(...)` against `now() - duration(...)` | The planner folds `now()` into a literal with nanoseconds. Sequel puts a `Time` into SQL with microseconds at best, so the query would compare with a different instant from the one in the policy. |
| A division whose denominator is a second column | IEEE-754 keeps the sign of a zero, and `2.0 / -0.0` is -Infinity while `2.0 / 0.0` is +Infinity. SQL cannot tell `-0.0` from `0.0`. A division of a value by itself stays safe, and so does a constant denominator. |
| More arithmetic on a division that can give NaN or Infinity | SQL has no NaN and no signed Infinity, so the adapter resolves such a division only where it is the comparison operand. |
| `int()` beside a double, `%` over a bare attribute | CEL has no overload mixing int and double, and no `%` over doubles; every number in a request attribute is a double. SQL computes both. |
| `matches()` | RE2 has no portable SQL form, and `LIKE` cannot show a regular expression. |
| `list[i]` | An association has no order of its own, so `index` has no case in the operator dispatch. A caller with a deterministic ordering column can supply an operator override. |
| `int()`, `double()` or `timestamp()` over a text column; `int()` over a double column | CEL reads the WHOLE string or makes an error, but SQL reads the digits at the front. CEL truncates a double toward zero, and PostgreSQL and MySQL round a `CAST`. |
| `string()` over a ternary of whole constants, or a double compared with `"0"`/`"-0"` | The plan carries `1000000` and `1000000.0` as the same number, which CEL spells differently; SQL cannot tell `-0.0` from `0.0`. |
| A whole collection compared with `==` | A correlated subquery has no ordered list to compare element by element. |
| A list or map as a list element, a struct built in the policy, a list difference, `filter`/`map` over a list of constants, a map's keys | None has a scalar SQL form. |
| Two raw temporal columns, or two columns under mixed NULL conventions | CEL compares RFC-3339 spellings without `timestamp()`; a mixed pair needs both a definite and an UNKNOWN answer for NULL. |

The adapter also raises an error for a plan whose `and` or `or` carries no operands, and for any
operator that carries the wrong number of operands. The planner does not make those shapes, but
this adapter accepts a plan from any source, and a plan that lost or gained an operand must not
become a wider filter.

### Mapping hazards

The table above is about the **plan**. The other half of the contract is the **mapping**:

> The rows that a subquery of the adapter sees must be the same rows that your application put
> into the resource attributes.

When the two differ, the filter gives rows that the PDP denies and no action in the corpus can
see it ([#314](https://github.com/cerbos/query-plan-adapters/issues/314)). This adapter builds
each subquery from the association reflection, so it can see most of these hazards and refuses
them:

| Hazard | Position | Mechanism to check |
| --- | --- | --- |
| A filtered association | **Rejected** | `conditions:`, a block, a custom `dataset:` or `limit:` on the association. The adapter cannot put those onto the alias that it makes for the correlated subquery. |
| A target model over a filtered dataset | **Rejected** | `Sequel::Model(DB[:tags].where(visible: true))`. Every read of the application applies the filter and the subquery would not. |
| Subtype discrimination | **Rejected** | The `single_table_inheritance` plugin filters the dataset of a subclass on its key column, so an association that points at a subclass is refused by the same check. An association that points at the base class is permitted. |
| A to-one association used as a collection | **Rejected** | `one_to_one`. Nothing makes the database keep one row. Map a to-one association as a field path with dots. |
| A `one_through_one` in a field path | **Rejected** | Nothing makes the join table hold one row per owner, so a scalar subquery through it could read any of several. |
| A composite association key | **Rejected** | The adapter builds one equality for the correlated subquery and refuses the association instead of joining on the first column only. |
| An absent to-one parent | **Proved by the corpus** | Write a path such as `R.attr.parent.children` as a NESTED `association` mapping. See [A chain through a parent](#a-chain-through-a-parent). The `relation/*/…to-one-chain` cases hold it under every polarity. |

### The NULL convention of the caller

There are two ways to send a NULL column to Cerbos, and the query plan looks the same for both.
You must tell the adapter which one your application uses.

| `null_attribute_representation:` | What your application sends for a NULL column | `R.attr.x == null` |
| --- | --- | --- |
| `:explicit` (the default) | An attribute whose value is null | Cerbos gives true, and `IS NULL` agrees |
| `:omitted` | No attribute at all | CEL raises a missing-attribute error, and Cerbos denies the row |

Under `:omitted` a NULL column is a missing attribute, which CEL answers with an error, and a
present column is never null. So `==` and `!=` between a field attribute and `null` are rendered
UNKNOWN for a NULL column, which stays UNKNOWN under any `not` above it:

```
eq(col, null)  ->  CASE WHEN col IS NULL THEN NULL ELSE FALSE END
ne(col, null)  ->  CASE WHEN col IS NULL THEN NULL ELSE TRUE END
```

This holds for a column reached through a to-one path such as `parent.tag`, where an absent parent
is NULL too. Every other null constant is refused under `:omitted`: a null in an `in` or
`hasIntersection` list, and a null given to an operator override of `eq` or `ne`, since the
override would receive it. See [#302](https://github.com/cerbos/query-plan-adapters/issues/302)
and [#551](https://github.com/cerbos/query-plan-adapters/issues/551).

`null_attribute_representation:` is the fallback for the whole call. Declare the convention on
each attribute that can be NULL, with `null_representation:` on the mapping:

```ruby
MAPPING = {
  "request.resource.attr.owner" => Cerbos::Sequel.field("owner", null_representation: :explicit),
  "request.resource.attr.tag" => Cerbos::Sequel.field("tag", null_representation: :omitted),
  "request.resource.attr.title" => Cerbos::Sequel.field("title")  # NOT NULL: declares nothing
}
```

A declaration of `:explicit` changes only `eq`, `ne` and `in`, the operators that CEL
calculates to a definite boolean over a null value:

```
eq(col, c)     ->  col IS NOT NULL AND col = c
ne(col, c)     ->  NOT (col IS NOT NULL AND col = c)
in(col, [cs])  ->  col IS NOT NULL AND col IN (cs)
eq(a, b)       ->  (a IS NULL AND b IS NULL) OR (a IS NOT NULL AND b IS NOT NULL AND a = b)
```

A comparison between two columns must not mix the conventions, and the adapter refuses one
that does. Refer to [#308](https://github.com/cerbos/query-plan-adapters/issues/308) and
[ADR 0004](../docs/adr/0004-the-null-convention-is-a-property-of-the-attribute.md).

### The collation is part of the contract

CEL string comparison is byte-exact. A case-insensitive or otherwise lenient collation makes
`==`, `contains`, `startsWith` and `endsWith` match more rows than the policy allows. CEL also
orders strings by code point, and `<`, `<=`, `>` and `>=` follow the collation.

- **SQLite:** set `PRAGMA case_sensitive_like = ON`. The default `BINARY` collation orders by code
  point.
- **PostgreSQL:** equality is exact, but string ordering needs a byte-order collation, `"C"`. A
  linguistic one such as glibc's `en_US.UTF-8` sorts `"One"` after `"a"`, so `R.attr.name > "a"`
  over-grants it ([#489](https://github.com/cerbos/query-plan-adapters/issues/489)). Create the
  database with `LC_COLLATE 'C'`, or declare `COLLATE "C"` on each column a policy orders.
- **MySQL:** use `utf8mb4_0900_bin` on every column your policies read. `_cs` is not enough —
  `utf8mb4_0900_as_cs` ignores a soft hyphen (U+00AD), and `utf8mb4_bin` is PAD SPACE
  ([#474](https://github.com/cerbos/query-plan-adapters/issues/474)). Make the **connection**
  collation byte-exact too: `string()` of a column is a `CAST` or a `CASE` over literals, so it
  takes the connection collation. With the `trilogy` driver, set it with
  `connect_sqls: ["SET collation_connection = utf8mb4_0900_bin"]`; `SET NAMES ... COLLATE`
  crashes that driver.

### Timestamps are compared in the database timezone

The adapter reads each `timestamp()` literal as a UTC `Time`, and Sequel converts it into
`Sequel.database_timezone` when it writes the SQL. Set that to the zone your columns hold — on
SQLite a datetime is text, and a literal in another zone compares as the wrong instant.

The conformance harness replays the corpus on SQLite, PostgreSQL (initialised with
`--lc-collate=C`) and MySQL (`utf8mb4_0900_bin` on the columns and the connection). The contract
suite runs on SQLite only.

## Requirements

- Ruby 3.3 or a later version
- Sequel 5.69 or a later 5.x (CI tests 5.69 and the newest release). 5.69 is the first release with the trilogy adapter, the MySQL driver the conformance harness runs on
- Cerbos after v0.40
- The official [Cerbos Ruby SDK](https://github.com/cerbos/cerbos-sdk-ruby)
  (the [`cerbos`](https://rubygems.org/gems/cerbos) gem)

This gem has no runtime dependency on `cerbos`: that SDK uses gRPC, and a dependency on it
would install a native `grpc` build in applications that speak to the PDP with REST. The
`plan:` parameter accepts a `Cerbos::Output::PlanResources` directly, the JSON of a
`PlanResources` response after a parse, or any object that has `kind` and `condition`.

## Installation

```bash
bundle add cerbos-sequel cerbos
```

## Usage

```ruby
require "cerbos"
require "cerbos/sequel"

cerbos = Cerbos::Client.new("localhost:3593", tls: false)

plan = cerbos.plan_resources(
  principal: {id: "user@example.com", roles: ["USER"]},
  resource: {kind: "document"},
  action: "view"
)

MAPPING = {
  "request.resource.attr.ownerId" => Cerbos::Sequel.field("owner_id"),
  "request.resource.attr.status" => Cerbos::Sequel.field("status"),
  "request.resource.attr.department" => Cerbos::Sequel.field("owner.department"),
  "request.resource.attr.tags" => Cerbos::Sequel.association(
    :tags,
    member_field: "name",
    fields: {"name" => Cerbos::Sequel.field("name")}
  )
}

documents = Cerbos::Sequel.query_plan_to_dataset(
  plan: plan, model: Document, attributes: MAPPING
)
```

Inside `Cerbos`, the constant `Sequel` is this adapter. Write `::Sequel` for the library in code
that is nested in a `Cerbos` module.

`model:` is a `Sequel::Model` subclass, or a dataset of one such as
`Document.where(tenant_id: tenant)`; the filter is then added to that dataset. The associations
and the column types are read from the model, so a plain `DB[:documents]` is refused.

For a runnable application that uses the published gem, refer to [`example/`](example/).

| Kind of plan | Result |
| --- | --- |
| `KIND_ALWAYS_ALLOWED` | the dataset, unfiltered |
| `KIND_ALWAYS_DENIED` | `dataset.where(false)`, which selects no row and still composes |
| `KIND_CONDITIONAL` | `dataset.where(<the condition after the translation>)` |

### The attribute map

The map must contain each plan variable. If it does not, the translation raises an error. The
adapter does not select a column from the name of an attribute.

#### `field` for scalar columns

```ruby
Cerbos::Sequel.field("status")            # a column on the model
Cerbos::Sequel.field("owner.department")  # through a many_to_one or a one_to_one
```

A path with dots goes through to-one associations, and each hop must be to-one. The adapter
makes a **correlated scalar subquery** for it, so the path cannot increase the number of rows
in the result. A hop that does not exist gives NULL, the comparison is UNKNOWN, and the row
stays out of the result — as it does in Cerbos, which denies a missing path.

The primary key is its own plan variable, so map it by name if a policy reads `R.id`:

```ruby
"request.resource.id" => Cerbos::Sequel.field("id")
```

#### `association` for collections

```ruby
Cerbos::Sequel.association(
  :tags,
  member_field: "name",
  fields: {"name" => Cerbos::Sequel.field("name")}
)
```

- `association` names a `one_to_many` or a `many_to_many`. A `many_to_many` becomes its join
  table and its target in one correlated subquery.
- `member_field` replaces the element when the policy uses the collection as a list of simple
  values. Thus `"urgent" in R.attr.tags` compares with `tag.name`.
- `fields` maps the member names in the bodies of the lambdas, and an entry can be another
  `association`. This is how the adapter resolves a chain with more than one hop.

Each subquery gets new table aliases. Thus a macro on an association inside another macro on
the same association correlates to the outer row.

#### A chain through a parent

A path that the policy writes with dots, such as `R.attr.mainCategory.subCategories`, is mapped
from its START, with each step after it in `fields`:

```ruby
"request.resource.attr.mainCategory" => Cerbos::Sequel.association(:categories, fields: {
  "subCategories" => Cerbos::Sequel.association(:sub_categories, fields: {
    "name" => Cerbos::Sequel.field("name")
  }),
  "subNames" => Cerbos::Sequel.association(:sub_categories, member_field: "name")
})
```

CEL cannot read a field from a list, so every step before the last one is a to-ONE parent. When
that parent is absent, your application sends no attribute, and Cerbos denies the row. A
subquery cannot tell an absent parent from a parent with no children, so the adapter requires
the parent hops to exist and a row without a parent stays out of the result under **both**
polarities. An association that you map directly keeps the usual meaning of an empty
collection: `!R.attr.tags.exists(...)` over zero tags is still TRUE.

#### A macro over a principal attribute

When a collection is a principal attribute, the planner sends the list itself. The adapter
evaluates the body of the lambda one time for each element and joins the results with OR for
`exists`, or with AND for `all`. You need no mapping for such a collection.

#### `operator_overrides` for translations that are specific to your schema

```ruby
Cerbos::Sequel.query_plan_to_dataset(
  plan: plan, model: Document, attributes: MAPPING,
  operator_overrides: {
    "matches" => ->(column, pattern) { Sequel.like(column, Regexp.new(pattern)) }
  }
)
```

The adapter gives the operands to an override after it resolves them, and the override gives a
Sequel expression. You cannot override the structural operators: `and`, `or`, `not`, `if`,
`lambda` and the collection macros.

## How the adapter keeps the three-valued logic

CEL denies a resource if the evaluation of its condition makes an error. The UNKNOWN value of
SQL has the same behaviour: a predicate does not select it, and the negation of that predicate
does not select it. The translation keeps UNKNOWN and does not change it into a boolean:

- **A ternary becomes a `CASE` whose `ELSE` is `NULL`.** If the condition is UNKNOWN, the `CASE`
  gives NULL, and the row stays out of the result under a `NOT` too.
- **A collection macro becomes a `CASE` expression and not only an `EXISTS` subquery**, with a
  guard for an element whose body made an error that matches what each CEL quantifier does with
  one.
- **A negation is an explicit `NOT`.** The adapter builds its nodes with their constructors and
  never with Sequel's `~`, which rewrites `NOT (a = b)` into `a != b` and pushes `NOT` through De
  Morgan. Both rewrites are sound, but the explicit form keeps the SQL in the shape of the plan.

## Development

Everything runs in Docker; you do not need Ruby locally, and no suite needs a PDP.

```bash
./scripts/test.sh                                   # all suites
./scripts/test.sh spec/conformance_spec.rb          # the conformance harness alone
ADAPTER_TEST_DB=postgres ./scripts/test.sh spec/conformance_spec.rb   # on PostgreSQL (or mysql)
RUBY_VERSION=3.3 SEQUEL_VERSION="= 5.69.0" ./scripts/test.sh
./scripts/lint.sh                                   # standardrb
```

The `tests` service mounts the repository root, because the suites read `../conformance/`.
`ADAPTER_TEST_DB` picks the store: `sqlite` (the default, in memory), `postgres` or `mysql`, which
`scripts/test.sh` starts from `docker-compose.yaml` with the images pinned in
[`POSTGRES_IMAGE`](POSTGRES_IMAGE) and [`MYSQL_IMAGE`](MYSQL_IMAGE). Any other value fails.

| Suite | What it covers |
| --- | --- |
| `spec/conformance_spec.rb` | The conformance harness: every golden plan of both pinned PDPs, against the corpus rows, on the store `ADAPTER_TEST_DB` names. |
| `spec/adapter_contract_spec.rb` | What a **caller** supplies and the corpus therefore cannot vary: the mapper forms, a `many_to_many`, operator overrides, the per-call NULL convention, the four transports a plan can arrive over, and the association shapes the adapter refuses to guess at. SQLite only. |

The example application in [`example/`](example/) runs the shared demo domain. Start it with
`../demo/scripts/run-example.sh sequel` — see [example/README.md](example/README.md).
