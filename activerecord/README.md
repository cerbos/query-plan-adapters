# Cerbos ActiveRecord Query Plan Adapter

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

The tests compare this adapter with the PDP pinned in `../conformance/CERBOS_VERSION` and
`../conformance/CERBOS_IMAGE_DIGEST`. For each action, the test makes a
plan with a real PDP, translates the plan, runs the query against 21 difficult rows, and
compares the ids in the result with the decisions of `checkResource` for each row. The PDP
gives the results for both sides. No person writes the expected results. The Spring Data
adapter gives the reference behaviour.

A second suite, `spec/translator_spec.rb`, replays the same corpus OFFLINE from
`../conformance/wire-fixtures/` and pins the SQL this adapter emits for each action in
[`golden/expectations.json`](golden/expectations.json). It needs no PDP and no database
server. Rewrite it with `./scripts/golden-update.sh` and review the diff.

| Classification | Coverage |
| --- | --- |
| Tested against the oracle | 178 corpus actions |
| Fail-closed | 18 actions: 8 that this adapter cannot show, and the 10 that the reference adapter does not support either. Each one must raise an error whose message the corpus pins, so a typo or a transport error cannot pass as the refusal |
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
| `cr-div-other-column` | A division whose denominator is a second column. IEEE-754 keeps the sign of a zero, and `2.0 / -0.0` is -Infinity while `2.0 / 0.0` is +Infinity. SQL cannot tell `-0.0` from `0.0`, because both satisfy `= 0` and no portable function reads the sign bit, so the sign of the Infinity is unknown. A division of a value by ITSELF stays safe — the denominator can only be zero when the numerator is zero too, and that gives NaN, which has no sign — and so does a constant denominator, whose sign the plan carries. Divide by a constant to keep the shape. |
| `cr-div-then-add`, `cr-div-then-add-ne` | More arithmetic on the result of a division that can give a value which is not finite. The adapter keeps such a division as branches until a comparison resolves them, because SQL has no NaN and no signed Infinity to bind. An addition on those branches has no SQL form: a NULL would go through the sum where CEL takes NaN through it. |
| `p-matches` | `matches()` uses RE2. No SQL dialect gives the behaviour of RE2, and `LIKE` cannot show a regular expression. |
| `p-index` | `tags[0]` selects an element of a list by its position. A relation has no order of its own, so `index` has no case in the operator dispatch and the walk falls through to the generic unsupported-operator refusal. A caller whose table has a deterministic ordering column can supply an operator override. |
| `p-timestamp` | `timestamp()` on a column that holds a timestamp in text. A comparison between that column and a `Time` compares two different text formats. Thus the order of the results comes from the text and not from the instants. Map the attribute to a `datetime` column. |
| `cast-int-string`, `cast-double-string` | `int()` and `double()` over a text column. CEL reads the WHOLE string or makes an error, and Cerbos then denies the row, but SQL reads the digits at the front: `CAST('1junk' AS INTEGER)` is `1` on SQLite. Compare the column directly, or give an operator override. |
| `cast-int-double` | `int()` over a double column. CEL removes the fraction toward zero. PostgreSQL and MySQL round a `CAST` to the nearest whole number, so the two disagree for every value with a fraction of one half or more. |
| `cast-string-bool` | `string()` over a **boolean** column. SQLite and MySQL have no boolean type and keep 1 or 0, so `CAST(col AS TEXT)` gives `"1"` where CEL gives `"true"`, and the filter would then remove every row. PostgreSQL alone gives `"true"`. The limitation is the CAST rather than the dialect: this adapter does branch per dialect elsewhere (`Dialect#concat`, `#char_length`, `#double_type`), and a three-valued `CASE WHEN col IS NULL THEN NULL WHEN col THEN 'true' ELSE 'false' END` would agree with CEL on every engine at once — it is simply not what the cast path builds today. `string()` over a number or a text column translates, because `CAST` agrees on all three. Compare the boolean column directly, or give an operator override that spells the two words your database uses. |
| `filter-as-condition`, `map-as-condition` | A `filter()` or a `map()` that a policy uses as the whole condition. Those operations give a list and not a boolean, and only `size(filter(...))` or `hasIntersection(map(...), [...])` has a boolean meaning. |
| `filter-as-conjunct` | The same list-where-a-boolean-belongs, one level BELOW the root: `filter(...) && R.attr.aBool`. The other conjunct is one the adapter can certainly express, so dropping the one it cannot would emit a filter that returns rows the PDP denies for every seed. |
| `index-scalar-list` | `tagNames[0]`, positional access into a scalar list. The same missing row order as `p-index`, reached through a relation mapped by member field rather than through a principal attribute. |
| `map-eq-list` | A `map()` projection compared with `==` to a literal list. The projection is held until `size()` or `hasIntersection()` gives it a scalar meaning; comparing the ordered projection itself gives it none, and a correlated subquery has no ordering to compare element-wise against. |

The adapter also raises an error for a plan whose `and` or `or` carries no operands, and for any
operator that carries the wrong number of operands. The planner does not make those shapes, so
the corpus cannot hold them: it is built from real plans. But this adapter accepts a plan from
any source, and a plan that lost or gained an operand must not become a wider filter.

### Mapping hazards

The table above is about the **plan**: given a shape of policy, does the filter give the rows
that `checkResource` allows. The other half of the contract is the **mapping**:

> The rows that a subquery of the adapter sees must be the same rows that your application put
> into the resource attributes.

When the two differ, the filter gives rows that the PDP denies and no action in the corpus can
see it, because the oracle reads the attributes while the adapter reads the tables. Each hazard
below was a real over-grant, found while building this adapter
([#314](https://github.com/cerbos/query-plan-adapters/issues/314)). This is the position of this
adapter on each one:

| Hazard | Position | Mechanism to check |
| --- | --- | --- |
| A filtered association | **Rejected** | `has_many …, -> { where(…) }`. The adapter cannot put the conditions of the scope onto the alias that it makes for the correlated subquery, so it refuses the mapping. A `through:` chain is opened into its parts first, so a scope on the outer association is also found. |
| A default scope on the target model | **Rejected** | `default_scope` on the model that the association points at. Every read of the application applies it and the subquery would not. |
| Subtype discrimination | **Rejected** | Single-table inheritance. An association that points at a subclass also filters on the inheritance column. The adapter does not add that condition itself, because the set of subclasses depends on which of them Ruby has loaded, and a short condition would give the wrong answer in the other direction. Map the attribute onto the base class, or give an operator override. |
| A to-one relation used as a collection | **Rejected** | `has_one`. Nothing makes the database keep one row, so the application reads one and the subquery would examine all of them. Map a to-one association as a field path with dots. |
| A composite association key | **Rejected** | ActiveRecord gives an ARRAY for the keys of such an association. The adapter builds one equality for the correlated subquery and refuses the mapping instead of joining on the first column only. |
| An absent to-one parent | **Proved by the corpus** | Write a path such as `R.attr.parent.children` as a NESTED `relation` mapping and not as one flat `has_many :through`. The nesting is what tells the adapter which hops are the parent, and the adapter then requires them to exist. See [A chain through a parent](#a-chain-through-a-parent). The `w1-*-chain` actions hold it under every polarity. |

Five of the six are rejected rather than reproduced, because this adapter builds the subquery
from the association itself and can therefore see the hazard in the reflection. "The caller owns
it" is only honest for a hazard that the adapter cannot detect, and none of these is one.

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

#### Declare the convention on the attribute

`null_attribute_representation:` is the fallback for the whole call. Declare the convention on
each attribute that can be NULL, with `null_representation:` on the mapping:

```ruby
MAPPING = {
  # This column sends an attribute whose value is null.
  "request.resource.attr.owner" => Cerbos::ActiveRecord.field(
    "owner", null_representation: :explicit
  ),
  # This column sends no attribute at all.
  "request.resource.attr.tag" => Cerbos::ActiveRecord.field(
    "tag", null_representation: :omitted
  ),
  # This column is NOT NULL, so it declares nothing and keeps the value of the call.
  "request.resource.attr.title" => Cerbos::ActiveRecord.field("title")
}
```

One setting for the whole call cannot be correct, because one policy suite can correctly use
both conventions. The shared corpus does exactly that: `owner` sends an explicit null while
`aOptionalString` sends no attribute, and the two are the same column.

A declaration says two things at the same time: that the column can be NULL, **and** how that
NULL goes to `checkResource`. A mapping that declares nothing keeps the behaviour it always
had, so no filter of an application changes without a change to its mapping.

**What the declaration changes.** Only `eq`, `ne` and `in`. Those are the operators that CEL
calculates to a definite boolean over a null value, and thus the only ones whose SQL must also
be definite. With `:explicit`, the adapter writes them out:

```
eq(col, c)     ->  col IS NOT NULL AND col = c
ne(col, c)     ->  NOT (col IS NOT NULL AND col = c)
in(col, [cs])  ->  col IS NOT NULL AND col IN (cs)
eq(a, b)       ->  (a IS NULL AND b IS NULL) OR (a IS NOT NULL AND b IS NOT NULL AND a = b)
```

Without the declaration, `NULL != 'x'` is UNKNOWN in SQL, an UNKNOWN keeps the row out under
**both** polarities, and the filter is thus narrower than the decision. The direction is safe,
but the two do not agree, and to agree is the property that the corpus holds.

`lt`, `le`, `gt`, `ge` and the string operators do not change. A null receiver raises a
no-overload error in CEL, which denies under both polarities — the same result as UNKNOWN. To
make them definite would break them.

**A comparison between two columns must not mix the conventions.** The declared side needs a
definite answer for its NULL. The other side needs UNKNOWN for its NULL, because that is a
missing attribute. No one predicate is both, so the adapter refuses the comparison:

```ruby
# owner declares :explicit, scope declares nothing
# R.attr.owner != R.attr.scope
# => Cerbos::ActiveRecord::UnsupportedOperatorError
```

Declare the convention on both attributes, or on neither. Refer to
[cerbos/query-plan-adapters#308](https://github.com/cerbos/query-plan-adapters/issues/308) and
[ADR 0004](../docs/adr/0004-the-null-convention-is-a-property-of-the-attribute.md).

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

For a runnable application that uses the published gem, refer to [`example/`](example/).

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

More than one hop is permitted, and each hop must be to-one:

```ruby
# R.attr.parent.inner.aString
"request.resource.attr.parent.inner.aString" =>
  Cerbos::ActiveRecord.field("parent.inner.a_string")
```

The KEY is the plan variable, and the PATH is the association chain on your model. The two are
independent, so the names in your policy do not have to be the names of your columns.

A hop that does not exist gives NULL, and the comparison is then UNKNOWN and the row stays out
of the result. This agrees with Cerbos: your application sends no `parent` attribute for that
row, so CEL raises a missing-path error and `checkResource` denies it. The corpus holds this
under one hop and two (`rel-*-hop`).

The primary key is its own plan variable and not an attribute, so map it by name if a policy
reads `R.id`:

```ruby
"request.resource.id" => Cerbos::ActiveRecord.field("id")
```

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

#### A chain through a parent

The same nesting also carries a path that the policy writes with dots, such as
`R.attr.mainCategory.subCategories`. Map the START of the path, and put each step after it in
`fields`:

```ruby
"request.resource.attr.mainCategory" => Cerbos::ActiveRecord.relation(:categories, fields: {
  "subCategories" => Cerbos::ActiveRecord.relation(:sub_categories, fields: {
    "name" => Cerbos::ActiveRecord.field("name")
  }),
  "subNames" => Cerbos::ActiveRecord.relation(:sub_categories, member_field: "name")
})
```

**Write the path this way and do not map the full name onto one flat `has_many :through`.**
Both give the same joins, but only the nested form says which hops are the parent, and the
adapter needs that to keep an authorization guarantee.

CEL cannot read a field from a list, so every step before the last one is a to-ONE parent. When
that parent is absent your application sends no attribute at all, CEL makes a missing-path
error, and Cerbos denies the row. A subquery from the resource row cannot see the difference,
because an absent parent and a parent with no children both give no rows. Then
`all(...)` reads TRUE, `!exists(...)` reads TRUE, `size(...) == 0` reads TRUE, and each one of
those gives back the rows that the PDP denies.

With the nested form the adapter requires the parent hops to exist, so a row without a parent
stays out of the result under **both** polarities. A relation that you map directly keeps the
usual meaning of an empty collection: `!R.attr.tags.exists(...)` over zero tags is still TRUE.
That is why the adapter cannot decide this for you — a `has_many :through` is also how a plain
join table is written, and Cerbos never sees such a table.

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
./scripts/test.sh spec/translator_spec.rb           # offline: no PDP, no database server
./scripts/golden-update.sh                          # rewrite golden/expectations.json
RUBY_VERSION=3.2 ACTIVERECORD_VERSION=7.1 ./scripts/test.sh
./scripts/lint.sh
```

The `tests` service mounts the **root directory of the repository**, because both corpus suites
read shared data at `../conformance/` (`seeds.json`, `actions.json`, `derived-fields.json`,
`wire-fixtures/`, `CERBOS_VERSION`, `CERBOS_IMAGE_DIGEST`).

There are three suites:

- `spec/translator_spec.rb` is the **translator unit test**. It replays every plan in
  `../conformance/wire-fixtures/` and asserts the SQL against
  [`golden/expectations.json`](golden/expectations.json), plus a set of rules stated over the
  whole corpus that survive a regeneration — every `LIKE` carries an `ESCAPE`, no statement joins
  the resource table to itself, every identifier names a declared table. Needs no PDP.
- `spec/adapter_contract_spec.rb` covers what a **caller** supplies and the corpus therefore
  cannot vary: the mapper forms, operator overrides, the per-call NULL convention, the four
  transports a plan can arrive over, and the association shapes the adapter refuses to guess at.
  Needs no PDP.
- `spec/adversarial_conformance_spec.rb` is the differential harness over the shared corpus
  (`../conformance/`). It obeys the oracle procedure in
  [conformance/README.md](../conformance/README.md) and starts a pinned PDP.

The example application in [`example/`](example/) runs the shared demo domain. Start it with
`../demo/scripts/run-example.sh activerecord` — see [example/README.md](example/README.md).
