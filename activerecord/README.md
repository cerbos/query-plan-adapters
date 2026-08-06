# Cerbos ActiveRecord Query Plan Adapter

An adapter that changes a [Cerbos](https://cerbos.dev) query plan (`PlanResources`) into an
`ActiveRecord::Relation`. Thus the database applies the authorization rules from your Cerbos
policies, and your application code does not.

The result is a usual relation. Thus you can add scopes, an order, pagination and eager loading
to it:

```ruby
documents = Cerbos::ActiveRecord.query_plan_to_relation(
  plan: plan, model: Document, attributes: MAPPING
)

documents.where(archived: false).order(:created_at).limit(20)
```

## The adapter is fail-closed

If the adapter cannot translate a shape of plan correctly, it **raises an error**. It does not
give a filter that is only approximately correct. This is the primary guarantee of the adapter.
An incorrect filter is an authorization bug, because it gives rows that the PDP denies. An
error is a bug report.

The adapter never changes an operator into a weaker operator. It never changes `exists_one`
into `exists`. If it cannot escape a `LIKE` needle, it never lets the wildcards stay.

### Conformance contract

The tests compare this adapter with Cerbos PDP `0.54.0`. For each action, the test makes a
plan with a real PDP, translates the plan, runs the query against 20 difficult rows, and
compares the ids in the result with the decisions of `checkResource` for each row. The PDP
gives the results for both sides. No person writes the expected results. The Spring Data
adapter gives the reference behaviour.

| Classification | Coverage |
| --- | --- |
| Tested against the oracle | 120 reference actions |
| Fail-closed | 2 reference actions, and the 3 shapes that the reference adapter does not support (5 actions in total) |
| Refused under the `omitted` NULL convention | 1 action — see [The NULL convention of the caller](#the-null-convention-of-the-caller) |
| Known difference in the planner | The Cerbos planner changes `has()` on a missing attribute into `ALWAYS_ALLOWED`, but `checkResource` denies the rows in which the attribute is missing. Until the planner has a correction, use `R.attr.x != null` and not `has(R.attr.x)` for the attributes in your database |

The fail-closed set is small, because SQL can show most of the corpus directly. The adapter
makes `LIKE` with an ESCAPE clause. It makes correlated `COUNT` subqueries for the relation
counts and for `exists_one`. The database calculates the arithmetic on columns and the lengths
of the strings. A comparison between two models is a usual correlated predicate. These shapes
stay:

| Action | Why the adapter raises an error |
| --- | --- |
| `ts-window`, `ts-vf` | The planner makes a `now()` literal with nanoseconds. ActiveRecord puts a `Time` into SQL with microseconds. Thus the query would compare with a different instant from the instant in the policy. |
| `p-matches` | `matches()` uses RE2. No SQL dialect gives the behaviour of RE2, and `LIKE` cannot show a regular expression. |
| `p-index` | `tags[0]` selects an element of a list by its position. A relation has no order of its own. |
| `p-timestamp` | `timestamp()` on a column that holds a timestamp in text. A comparison between that column and a `Time` compares two different text formats. Thus the order of the results comes from the text and not from the instants. Map the attribute to a `datetime` column. |

The adapter also raises an error for the shapes below. The shared corpus does not cover them
yet, and each has an issue to add it — a refusal that only one adapter enforces leaves the same
defect live in the others:

- A `filter()` or a `map()` that a policy uses as a condition. Those operations give a list
  and not a boolean.
  ([#313](https://github.com/cerbos/query-plan-adapters/issues/313))
- A division whose denominator is a column, unless the numerator is the same column. IEEE-754
  keeps the sign of a zero, and `2.0 / -0.0` is -Infinity while `2.0 / 0.0` is +Infinity. SQL
  cannot tell `-0.0` from `0.0`, because both satisfy `= 0` and no portable function reads the
  sign bit. Thus the sign of the Infinity is unknown. A division of a value by itself stays
  safe: the denominator can only be zero when the numerator is zero too, and that gives NaN,
  which has no sign. Divide by a constant to keep the shape.
  ([#312](https://github.com/cerbos/query-plan-adapters/issues/312))
- More arithmetic on the result of a division that can give a value which is not finite. Only
  a comparison can resolve NaN or an Infinity without putting it into SQL.
  ([#312](https://github.com/cerbos/query-plan-adapters/issues/312))
- An `and` or an `or` that carries no operands, and any operator that carries the wrong number
  of operands. The planner does not make those shapes, so the corpus cannot hold them: it is
  built from real plans. But this adapter accepts a plan from any source, and a plan that lost
  or gained an operand must not become a wider filter.
- `int()` over a column that is not an integer, and `double()` over a column that is not
  numeric. CEL reads a whole string or makes an error, and Cerbos then denies the row, but SQL
  reads the digits at the front: `CAST('1junk' AS INTEGER)` is `1` on SQLite. A cast from a
  double is also not portable, because SQLite removes the fraction while PostgreSQL rounds.
  Compare the column directly, or give an operator override.
  ([#311](https://github.com/cerbos/query-plan-adapters/issues/311))

### The NULL convention of the caller

There are two ways to send a NULL column to Cerbos, and the query plan looks the same for both.
You must tell the adapter which one your application uses.

| `null_attribute_representation:` | What your application sends for a NULL column | `R.attr.x == null` |
| --- | --- | --- |
| `:explicit` (the default) | An attribute whose value is null | Cerbos gives true, and `IS NULL` agrees |
| `:omitted` | No attribute at all | CEL raises a missing-attribute error, and Cerbos denies the row |

With `:omitted`, a filter that selects NULL would give exactly the rows that the PDP denies.
The adapter cannot read the convention from the plan, because the planner makes the same
`eq(attr, null)` node for both. Thus the adapter refuses each null constant in the plan under
`:omitted`:

```ruby
Cerbos::ActiveRecord.query_plan_to_relation(
  plan: plan, model: Document, attributes: MAPPING,
  null_attribute_representation: :omitted
)
# => Cerbos::ActiveRecord::UnsupportedOperatorError
```

The refusal is wider than the shapes that give too many rows. `R.attr.x != null` is correct by
itself, but this adapter puts a negation around a predicate and does not push it into the leaf.
Thus a leaf cannot know that a `not` above it will make `IS NOT NULL` into a predicate that
selects NULL again. To refuse each null constant is correct for all the shapes. Refer to
[cerbos/query-plan-adapters#302](https://github.com/cerbos/query-plan-adapters/issues/302).

#### A known limit of the `explicit` convention

Under `:explicit`, CEL holds a null *value*, and thus `null != "x"` is true and `null == "x"` is
false. SQL does not agree: `NULL != 'x'` is UNKNOWN, and an UNKNOWN keeps the row out. Thus the
adapter gives fewer rows than the PDP permits when a policy compares a column that holds NULL
with a constant that is not null:

```cel
R.attr.owner != "x"        # Cerbos permits a row whose owner is null; the filter does not
!(R.attr.owner == "x")     # the same
!P.attr.teams.exists(t, R.attr.owner == t)
R.attr.owner == R.attr.otherOwner   # two nulls are equal in CEL; `a = b` is UNKNOWN in SQL
```

The direction is safe — the filter is narrower than the decision, and never wider — but the two
do not agree. A row with a NULL column is absent from the result although the policy permits it.

The adapter does not correct this today, because the correction would need the convention for
each attribute and not one setting for the whole call. The shared corpus uses both conventions
in one policy suite: `owner` sends an explicit null while `aOptionalString` sends no attribute.
One setting cannot be right for both. Refer to
[cerbos/query-plan-adapters#308](https://github.com/cerbos/query-plan-adapters/issues/308).

Until then: keep a column that a policy compares with a constant `NOT NULL`, or use `:omitted`
and send no attribute for a NULL column, which the adapter already translates correctly.

### The collation is part of the contract

CEL compares strings with attention to the case of the letters. The dialect controls the
collation of `LIKE`. Thus a collation without attention to the case makes `contains`,
`startsWith` and `endsWith` select more rows than the policy permits. On SQLite, set
`PRAGMA case_sensitive_like = ON`. On MySQL, use a `_bin` collation or a `_cs` collation for
the columns in your policies.

The suites here use SQLite only. This adapter has no test coverage for the other dialects.

## Requirements

- Ruby 3.2 or a later version
- ActiveRecord 7.0 or a later version, but before 9.0 (CI tests 7.1 and 8.0)
- Cerbos after v0.40
- The official [Cerbos Ruby SDK](https://github.com/cerbos/cerbos-sdk-ruby)
  (the [`cerbos`](https://rubygems.org/gems/cerbos) gem)

### Why the SDK is not a hard dependency

This gem has no runtime dependency on `cerbos`. That SDK uses gRPC. Thus a dependency on it
would install a native `grpc` build in the applications that speak to the PDP with REST.

But the SDK is the expected client, and the adapter is built around its shapes. The `plan:`
parameter accepts a `Cerbos::Output::PlanResources` directly. The two test suites send real
responses from `Cerbos::Client#plan_resources` through the adapter. The tests in
`spec/translator_spec.rb` also use the output types of the SDK by name.

If you get your plans in a different way, from the REST interface or from a cache, give the
JSON after a parse operation. You can also give an object that has `kind` and `condition`. The
result of the translation is the same.

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

For a full application that uses the adapter, refer to [`example/`](example/).

The three kinds of plan become relations directly:

| Kind of plan | Result |
| --- | --- |
| `KIND_ALWAYS_ALLOWED` | `model.all` |
| `KIND_ALWAYS_DENIED` | `model.none` |
| `KIND_CONDITIONAL` | `model.where(<the condition after the translation>)` |

### The attribute map

The map must contain each plan variable. If it does not, the translation raises an error. The
adapter does not select a column from the name of an attribute. If it did that, an
authorization filter could quietly use the wrong data.

#### `field` for scalar columns

```ruby
Cerbos::ActiveRecord.field("status")            # a column on the model
Cerbos::ActiveRecord.field("owner.department")  # through a belongs_to or a has_one
```

A path with dots goes through to-one associations. The adapter makes a **correlated scalar
subquery** for it. Thus the path cannot increase the number of rows in the result, but a join
can do that. If a path with dots contains a collection association, the adapter raises an
error. A scalar comparison with "one of the elements" is not the request of the policy.

#### `relation` for collections

```ruby
Cerbos::ActiveRecord.relation(
  :tags,
  member_field: "name",
  fields: {"name" => Cerbos::ActiveRecord.field("name"),
           "id" => Cerbos::ActiveRecord.field("id")}
)
```

- `member_field` replaces the element when the policy uses the collection as a list of simple
  values. Thus `"urgent" in R.attr.tags` compares with `tag.name`.
- `fields` maps the member names in the bodies of the lambdas. Thus
  `R.attr.tags.exists(t, t.name == "x")` can resolve `t.name`. An entry in `fields` can be a
  relation. This is how the adapter resolves a chain with more than one hop:

```ruby
"request.resource.attr.categories" => Cerbos::ActiveRecord.relation(:categories, fields: {
  "subCategories" => Cerbos::ActiveRecord.relation(:sub_categories, fields: {
    "name" => Cerbos::ActiveRecord.field("name")
  })
})
```

#### A macro over a principal attribute

When a collection is a principal attribute, the planner knows its values and sends the list
itself. The adapter evaluates the body of the lambda one time for each element and joins the
results with OR for `exists`, or with AND for `all`. SQL gives the correct answer without more
work, because OR and AND obey the same three-valued logic as the CEL quantifiers.

```cel
P.attr.teams.exists(t, R.attr.owner == t)
```

becomes `owner = 'team-a' OR owner = 'team-b' OR ...`. You need no mapping for such a
collection, because the values are in the plan.

A `has_many :through` association is permitted. The adapter opens it into joins **in one
correlated subquery**. It does not make an `EXISTS` inside an `EXISTS`. This difference is
important for the operators that count. `size(R.attr.categories.subCategories)` must count the
last rows for each resource and not for each category.

Each subquery gets new table aliases. Thus a macro on an association inside another macro on
the same association correlates to the outer row.

The adapter refuses an association whose rows it cannot reproduce exactly. In each case the
association gives a different set of rows from the table itself. Thus the attributes that
Cerbos evaluates and the rows that a subquery finds would not agree, and the filter would
select a row that the decision did not.

| Shape | Why the adapter refuses it |
| --- | --- |
| A polymorphic `belongs_to` | The target table is not known until the query reads a row |
| An association with a scope, including the outer association of a `through` chain | The adapter cannot put the conditions of that scope onto the alias that it makes |
| A target model with a `default_scope` | The rows that the scope removes are absent from the attributes that Cerbos evaluates |
| A `has_one` used as a collection | ActiveRecord does not make the database enforce that a `has_one` has only one row, so the association gives one row while a subquery examines every row. Map a to-one association as a field path with dots. |
| An association that points at a subclass in a single-table hierarchy | Such an association also filters on the inheritance column. The adapter does not add that condition, because the set of subclasses depends on which of them Ruby has loaded. An association that points at the base class needs no condition and is permitted. |
| An association that joins on more than one column | The adapter builds one equality for the correlated subquery and cannot express a composite key |

Map the attribute to a concrete association without a scope, or give an operator override.

The corpus cannot hold these either. It proves the *plan* side — that a filter returns the rows
`check()` allows — and a mapping is not a policy condition. But the invariant is the same one:
the rows that the subquery of the adapter sees must equal the rows that the application put
into the attributes it sent to Cerbos.
[#314](https://github.com/cerbos/query-plan-adapters/issues/314) proposes a shared way to cover
them.

#### `operator_overrides` for translations that are specific to your schema

```ruby
Cerbos::ActiveRecord.query_plan_to_relation(
  plan: plan, model: Document, attributes: MAPPING,
  operator_overrides: {
    "matches" => ->(column, pattern) { column.matches_regexp(pattern) }
  }
)
```

The adapter gives the operands to an override after it resolves them, and the override gives an
Arel node. Use an override when your database can show a shape correctly but portable SQL
cannot. A regular expression of a dialect, a JSON containment operator and a full-text index
are three examples.

You cannot override the structural operators: `and`, `or`, `not`, `if`, `lambda` and the
collection macros. The adapter does not resolve their operands before they run, and this is
necessary for their behaviour.

## How the adapter keeps the three-valued logic

CEL denies a resource if the evaluation of its condition makes an error. A missing attribute is
one cause. An element without a field is another cause. The UNKNOWN value of SQL has the same
behaviour: a predicate does not select it, and the negation of that predicate does not select
it. Thus `NOT (NULL = x)` stays UNKNOWN and does not become true.

The translation keeps UNKNOWN and does not change it into a boolean. You can see two results of
this rule in the SQL:

- **A ternary becomes a `CASE` without an `ELSE` clause.** If the condition is UNKNOWN, the
  `CASE` gives NULL. Thus the row stays out of the result, and it also stays out when a NOT
  operator is around the `CASE`. An `ELSE` clause would put those rows into the else branch.
- **A collection macro becomes a `CASE` expression and not only an `EXISTS` subquery.** The
  three CEL quantifiers have different behaviour for an element whose body made an error.
  `exists` ignores the errors if one element gives true. `all` ignores them if one element
  gives false. `exists_one` never ignores them. Thus each quantifier gets its own guard for the
  error.

## Development

All the components run in Docker. The version of the PDP comes from
`conformance/CERBOS_VERSION`. You do not need Ruby on your computer.

```bash
./scripts/test.sh                                   # all the suites
./scripts/test.sh spec/adversarial_conformance_spec.rb
RUBY_VERSION=3.2 ./scripts/test.sh                  # a different version of Ruby
./scripts/lint.sh
```

The `tests` service mounts the **root directory of the repository**, because the adversarial
harness reads the shared corpus at `../conformance/` (`seeds.json`, `actions.json`,
`CERBOS_VERSION`).

There are three suites:

- `spec/translator_spec.rb` examines the public interface, the shapes of plan that the adapter
  accepts, the SQL that it makes, and the shapes that it must refuse. It needs no PDP.
- `spec/shared_policy_spec.rb` is the shared policy suite (`/policies/resource.yaml`) for all
  the adapters in this repository. It reads the actions from the policy file. Thus a new action
  in that file cannot stay without a test.
- `spec/adversarial_conformance_spec.rb` is the shared adversarial corpus (`/conformance/`). It
  obeys the oracle procedure in [conformance/README.md](../conformance/README.md).

The example application in [`example/`](example/) has its own smoke tests. Refer to
[example/README.md](example/README.md).
