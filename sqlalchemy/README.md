# Cerbos + SQLAlchemy Adapter

An adapter library that takes a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into a [SQLAlchemy](https://docs.sqlalchemy.org/en/14/) Select instance. This is designed to work alongside a project using the [Cerbos Python SDK](https://github.com/cerbos/cerbos-sdk-python).

The adapter supports logical and comparison operators, value-first and field-to-field comparisons, literal-safe string helpers, arithmetic and conditional expressions, scalar casts and sizes, timestamps, and hierarchy comparisons. `operator_override_fns` can provide database- or schema-specific translations for collection and other non-portable shapes.

## NULL attribute representation

`R.attr.x == null` compiles to the same `eq(x, null)` plan node however your application represents
a NULL column in the attributes it sends to `check()`, so the adapter cannot infer the convention
and has to be told which one you use.

| attributes you send for a NULL column | `check()` on that row | `IS NULL` filter |
| --- | --- | --- |
| `{"x": None}` — explicit null | allow | selects it — aligned |
| `{}` — attribute omitted | **deny** (CEL missing-attribute error) | selects it — **over-grants** |

`null_attribute_representation` defaults to `"explicit"`, preserving the historical `IS NULL`
translation. If your application omits attributes for NULL columns, pass `"omitted"`: the adapter
then raises on every null comparison operand instead of emitting a filter that returns rows the PDP
denies.

```python
get_query(
    plan,
    Resource,
    attr_map,
    null_attribute_representation="omitted",
)
```

The rejection is deliberately wider than the shapes that actually over-grant — `x != null` and
`!(x == null)` are aligned under both conventions — because negation is applied around the built
predicate rather than pushed into the leaf, so a leaf cannot tell whether an enclosing `not` will
flip `IS NOT NULL` back into a NULL-selecting predicate. Rejecting every null operand is correct
under any nesting. It also fires ahead of `operator_override_fns`, since an override cannot
recover a representation the plan never carried. See
[#302](https://github.com/cerbos/query-plan-adapters/issues/302).

### Declare the convention per attribute

The option above is a whole-call default, and one policy suite can legitimately use both
conventions: the same column mapped twice, sent as an explicit null under one attribute name and
omitted under another. Declare it per attribute instead and the call-level option only covers what
the mapping does not:

```python
get_query(
    query_plan,
    Resource,
    attr_map,
    attribute_null_representation={
        # sent as an explicit null when the column is NULL
        "request.resource.attr.owner": "explicit",
        # `request.resource.attr.department` is omitted for a NULL column, so it is
        # left undeclared and the call-level default applies
    },
)
```

Declaring the explicit convention asserts two things: the column can be NULL, **and** a NULL reaches
`check()` as an explicit null. The equality family (`eq`, `ne`, `in`) over that attribute is then
rendered so it can never be SQL UNKNOWN — CEL holds a null *value* under this convention, so
`null != "x"` is TRUE and the row must come back, while UNKNOWN would drop it under *both*
polarities. Ordering and string operators are left alone: a null receiver raises a no-overload error
in CEL, which denies exactly as UNKNOWN does.

Leaving an attribute undeclared keeps the historical rendering — so nothing changes for a mapping
that says nothing, and `!=` against a constant keeps under-granting the NULL rows until you declare
it.

**Declare both sides of a field-to-field comparison, or neither.** Mixing the conventions across one
comparison has no faithful rendering — the declared side needs a definite answer for its NULL, the
undeclared side needs UNKNOWN — so the adapter throws rather than picking a direction. See
[#308](https://github.com/cerbos/query-plan-adapters/issues/308) and
[ADR 0004](../docs/adr/0004-the-null-convention-is-a-property-of-the-attribute.md).

## How this adapter is tested

Three suites, each answering a different question, and only one of them needs anything running.

| suite | question | needs |
| --- | --- | --- |
| `tests/test_translator.py` | what SQL does `get_query` emit for a plan? | nothing — plans come from `conformance/wire-fixtures/`, expectations from `golden/expectations.json` |
| `tests/test_query.py`, `tests/test_relations.py` | what does `get_query` do with a plan the planner cannot produce, or an option no policy can reach? | nothing |
| `tests/test_adversarial_conformance.py` | do the rows that query returns match `check()`? | Docker: a pinned Cerbos PDP, plus in-memory SQLite |

```bash
pdm install
pdm run test            # all three
pdm run golden:update   # rewrite golden/expectations.json, then review the diff
pdm run format          # isort + black
```

`golden/expectations.json` is a **golden expectation** file: for every one of the corpus's actions
this adapter translates, the `WHERE` clause it emits on SQLite and on PostgreSQL, plus the
parameters it binds. It is regenerated with the command above and reviewed as a diff — CI never
regenerates it, so a change to how a shape is translated fails there and shows up as the list of
statements it moved. The format is shared across adapters and documented under "Golden
expectations" in [`conformance/README.md`](../conformance/README.md); what an entry *holds* is this
adapter's own, and three things about the choice made here are worth knowing:

- **The whole `Select` is compiled, not the bare `WHERE` clause.** Correlation is only observable
  inside the enclosing SELECT: compiled on its own, a correlated subquery renders the outer table
  into its own `FROM` and silently compares against every row of it. The recorded value is the part
  after `WHERE`, and the suite asserts the rest of the statement is the same `SELECT … FROM` every
  time.
- **Two dialects, because they genuinely differ.** SQLite is what the conformance harness executes;
  PostgreSQL is executed by nothing in this repository and is the dialect this adapter's own source
  reasons about most (NaN ordering, `CAST` rounding, `string()` over a boolean). They are not close
  to identical — SQLite has no boolean type, so a `CASE` in a `WHERE` needs `= 1` and a negation
  renders as `= 0`, and the two spell float division differently.
- **The asset declares which SQLAlchemy major compiled it.** SQL text is the adapter's expression
  tree *plus* SQLAlchemy's compiler, and 1.4 and 2.x do not render every tree the same way: 2.x
  parenthesises a concatenation used as a comparison operand and adds SQLite's `+ 0.0`
  float-division coercion. Neither is a translation decision, so the asset is generated under 2.x
  and the 1.4 leg asserts a pinned list of exactly which shapes render differently — in both
  directions, so a shape that starts or stops diverging fails. `pdm run golden:update` refuses to
  run under the other major rather than rewriting the file with a compiler swap dressed up as a
  translation change.

## Conformance contract

The adapter is differentially tested against Cerbos PDP 0.54.0 `check()` decisions using 21 hostile seed rows and executable SQLAlchemy queries. The Spring Data adapter defines the reference semantics for this compatibility snapshot.

| Classification | Coverage |
| --- | --- |
| Oracle-tested | 178 reference conformance actions |
| Fail-closed corpus shapes | Nanosecond `now()` thresholds, regex `matches()`, ordered list indexing/`get-field`, `timestamp()` over an ambiguous string column, `int()`/`double()` casts (SQL `CAST` reads a numeric prefix where CEL demands the whole string, and rounds where CEL truncates toward zero) and `filter()`/`map()` used as a condition (both return a list, not a boolean), a constant zero divisor whose sign the HTTP transport discards, `string()` over a boolean column (SQLite and MySQL store 1/0 and render `'1'` where CEL and PostgreSQL render `'true'`), a hierarchy path constructed by `list()` rather than read from a column, `mod` (reached through the `int()` cast that gives `%` an integer operand), a positional read of a scalar list (row order in a SQL relation is not defined), and list equality over a `map()` projection, whose deferred intermediate no enclosing override consumes (19 actions) |
| Representation-dependent | `null-eq-missing` — raises under `null_attribute_representation="omitted"`; translated as `IS NULL` under the default, which over-grants if the caller omits attributes for NULL columns |
| Attribute NULL convention | The equality family (`eq`, `ne`, `in`) over an attribute the caller sends as an explicit null renders definitely, so a NULL row is included where CEL's null *value* says it should be. Declare it per attribute — `attribute_null_representation={reference: "explicit"}` — or the historical rendering applies and `!=` against a constant under-grants those rows (cerbos/query-plan-adapters#308) |
| Known planner divergence | `has()` on a missing attribute is folded by the Cerbos planner to `ALWAYS_ALLOWED`, while `check()` denies the missing-attribute rows. Until the planner is fixed, use `R.attr.x != null` for database-backed attributes instead of `has(R.attr.x)` |

The conformance harness supplies the same public `operator_override_fns` mechanism available to applications for schema-specific collection translations. Regex `matches()` fails closed by default because SQL dialect regex engines do not guarantee CEL/RE2 semantics; applications may provide an override only when their database translation is known to be equivalent. Timestamp literals must use strict RFC 3339 grammar, resolve inside CEL's supported year 0001–9999 instant range, and be exactly representable at Python/SQLAlchemy microsecond precision: discarded fractional digits must be zero, and the mapped column/database must preserve microseconds. Unsupported shapes raise instead of producing a broader query. Every fail-closed shape's error message is pinned in the shared corpus (`conformance/actions.json`) and asserted by this adapter's conformance run, so a classification proves the throw names its declared mechanism rather than merely that something threw.

The root-condition check that rejects `filter()`/`map()` accepts a bare boolean **column**. A policy whose whole condition is `R.attr.aBool` plans to a condition with no expression wrapper at all, and the ORM attribute it resolves to is a descriptor rather than a Core `ColumnElement` — so it used to be refused at the root while the identical operand was accepted one level down, as an `and`/`or`/`not` child. The position, not the shape, was deciding ([#388](https://github.com/cerbos/query-plan-adapters/issues/388)). This is a widening: a shape that raised now returns a filter, and nothing that previously returned a filter has changed.

**Behaviour changes** ([#387](https://github.com/cerbos/query-plan-adapters/issues/387)). Three, all narrowing or widening rather than silent:

- `hasIntersection` joins `eq`/`ne`/`in` as order-insensitive, so the value-first spelling — which the planner preserves from policy source order — reaches an override with the column first instead of handing it a literal list where it expected a relation. A widening.
- An operator override's intermediate value reaching a **default** handler now raises. Python compared it with `==` and produced a bare `False`, which `where()` accepts as a valid boolean and which filters out every row — an emitted filter for a shape the adapter cannot express.
- The non-boolean check that refuses `filter()`/`map()` at the root now also runs on every `and`/`or`/`not` operand. A macro one level down used to reach `and_()` and raise SQLAlchemy's own WHERE/HAVING-role error, which is fail-closed but names a coercion rather than the mechanism.

## Mapping hazards

The conformance contract above proves the *plan* side — given a policy shape, does the query select the rows `check()` allows. The other half is the *mapping*: **the rows a subquery reads must be the rows the application put into the resource attributes.** Six ways that can break are catalogued in the shared corpus, and every adapter has to record a position on each of them.

**`get_query` has no relation model.** `attr_map` maps attribute references to columns; a collection-valued attribute reaches its rows entirely through [`operator_override_fns`](#overriding-default-predicates), which means *you* write the correlated subquery. Every hazard below is therefore caller-owned here, and which of them apply depends on how you write it:

- Through a mapped **`relationship()`** — `Model.rel.any(...)`, `Model.rel.has(...)`, `select(...).join(Model.rel)` — SQLAlchemy applies the relationship's `primaryjoin` and, for a single-table-inheritance target, the discriminator criteria. Those hazards are closed by the ORM.
- Through a **hand-written correlated `select()`** over columns, which is what the adversarial harness does, none of that applies. You are reading the table bare and the invariant is yours end to end.

The single-table-inheritance half of that is [documented here](https://docs.sqlalchemy.org/en/20/orm/queryguide/inheritance.html#single-inheritance-mappings) — a `select(Subclass)` adds the discriminator to the `WHERE`. Check both against the SQLAlchemy version you actually run before relying on a row below.

| Hazard | Position | Mechanism to check |
|---|---|---|
| Filtered association | **Caller-owned** | `relationship(primaryjoin=…)` and `relationship(secondaryjoin=…)`. Going through `.any()`/`.has()` applies them; a hand-written `select()` over the target's columns does not, and must repeat the predicate in its own `where()` |
| Default scope on the target model | **Caller-owned** | A soft-delete column (`deleted_at IS NULL`), a tenant column, a `published` flag, or a `with_loader_criteria` you register on the session. SQLAlchemy applies none of those to a subquery you build yourself |
| Subtype discrimination | **Caller-owned** | `polymorphic_identity` on a single-table-inheritance subclass. `select(Subclass)` carries the discriminator; `select(literal(1)).where(subclass_table.c.x == …)` over the shared table does not, and sees the sibling subtypes |
| To-one relation used as a collection | **Caller-owned** | A `relationship(uselist=False)` whose foreign key has no unique constraint. Nothing in the override mechanism makes the database enforce the single row the application saw — add the constraint |
| Composite association key | **Caller-owned** | A multi-column foreign key. Unlike the adapters that take one source and one target column, an override is arbitrary SQLAlchemy, so a composite key *is* expressible — which also means nothing stops you writing half of it. Conjoin every column pair |
| Absent to-one parent | **Reproduced by `require_hops`**, and proved by the corpus (`w1-all-chain`, `rel-not-bool-hop` and siblings) | `cerbos_sqlalchemy.require_hops` — see below. Call it from every override that reaches a COLLECTION through an intermediate to-one hop. A SCALAR read through a to-one hop needs nothing: map it to a correlated scalar subquery and an absent hop is already SQL NULL, excluded under both polarities because `NOT NULL` is still NULL. `get_query` accepts any SQLAlchemy column expression in the attribute map for exactly this, and such an expression carries its own correlation so it needs no `table_mapping` ([#375](https://github.com/cerbos/query-plan-adapters/issues/375)) |

### `require_hops`: the one hazard with a library helper

CEL cannot dot through a list, so every intermediate segment of `a.b.c` is a to-ONE parent. When it is absent the application sends no attribute at all and CEL raises a missing-path error, which denies — but a subquery rooted at the resource row cannot tell an absent parent from a childless one, so `all` reads TRUE, `!exists` reads TRUE and the count reads 0, each admitting rows the PDP denies ([#309](https://github.com/cerbos/query-plan-adapters/issues/309)).

That requirement is mechanical, identical for every caller, and easy to get subtly wrong, so it ships in the library rather than being left as advice:

```python
from cerbos_sqlalchemy import get_query, require_hops
from sqlalchemy import exists, literal, select

# `mainCategory.subCategories`: the collection is reached THROUGH the category hop.
HOP = [Category.resource_id == Resource.id]
CORRELATE = [Resource]

def sub_categories_exists(collection, body):
    subquery = (
        select(literal(1))
        .where(SubCategory.category_id == Category.id)
        .where(Category.resource_id == Resource.id)
        .where(body)
        .correlate(*CORRELATE)
    )
    return require_hops(exists(subquery), HOP, CORRELATE)

query = get_query(
    plan, Resource, attr_map, operator_override_fns={"exists": sub_categories_exists}
)
```

`require_hops` wraps the answer in a `CASE` with **no `ELSE`**: a missing hop yields NULL, and `NOT NULL` is still NULL, so the row stays excluded under both polarities. A direct relation — an empty `hop_correlation` — is returned unchanged, so `!tags.exists(...)` over zero tags is still TRUE.

Every operator whose answer comes off a chain has to go through it, not just the collection macros. A bare `EXISTS` is two-valued, so it is FALSE for an absent parent and its negation is TRUE — which is how plain membership and `hasIntersection` kept readmitting every parentless row after the macros alone were fixed ([#315](https://github.com/cerbos/query-plan-adapters/issues/315)), and how `!(size(chain) > 0)` did the same ([#316](https://github.com/cerbos/query-plan-adapters/issues/316)).

It is **optional**, and calling it is not enforced: a caller wiring a join chain today gets exactly the query it got before the helper existed. Making it mandatory would mean raising for every such caller, a consumer-visible break to guard a hazard many of them do not have.

## Requirements
- Cerbos > v0.16
- SQLAlchemy >= 1.4 / 2.0

### Model styles

`get_query`'s `table` argument accepts a Core `Table`, a legacy
`declarative_base()` model, or a SQLAlchemy 2.0 `DeclarativeBase` subclass. The
2.0 style is not a relabelling of the legacy one — its metaclass sits outside the
`DeclarativeMeta` hierarchy — so both are named explicitly in the accepted type.

Passing an ORM model returns `Select[Tuple[Model]]` and passing a Core `Table`
returns `Select[Any]`, so the row type reaches the caller instead of being erased
to a bare `Select`.

### Database collation requirements

Cerbos CEL string and hierarchy comparisons are case-sensitive. The database
columns used in `attr_map` must therefore use a case-sensitive collation for
equality, membership, and the `LIKE` operations emitted by
`contains`/`startsWith`/`endsWith` and hierarchy-prefix predicates.

This is an authorization invariant: a case-insensitive database collation can
silently over-grant access (for example, treating `One` as equal to `one`, or
`Dept.Eng` as overlapping `dept.eng`). MySQL's common default `_ci` collations
are case-insensitive; configure a case-sensitive or binary collation for mapped
authorization columns. The adapter cannot enforce one portably because
collation selection belongs to the database schema and dialect.

## Usage

```
pip install cerbos-sqlalchemy
```

```python
from cerbos.sdk.client import CerbosClient
from cerbos.sdk.model import Principal, ResourceDesc

from cerbos_sqlalchemy import get_query
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import Select

Base = declarative_base()


class LeaveRequest(Base):
    __tablename__ = "leave_request"

    id = Column(Integer, primary_key=True)
    department = Column(String(225))
    geography = Column(String(225))
    team = Column(String(225))
    priority = Column(Integer)


with CerbosClient(host="http://localhost:3592") as c:
    p = Principal(
        "john",
        roles={"employee"},
        policy_version="20210210",
        attr={"department": "marketing", "geography": "GB", "team": "design"},
    )

    # Get the query plan for "view" action
    rd = ResourceDesc("leave_request", policy_version="20210210")
    plan = c.plan_resources("view", p, rd)


# the attr_map arg of get_query expects a map[string, InstrumentedAttribute | Column], with cerbos attribute strings mapped to the column/attr instances
attr_map = {
    "request.resource.attr.department": LeaveRequest.department,  # LeaveRequest.__table__.c.department is also allowed
    "request.resource.attr.geography": LeaveRequest.geography,
    "request.resource.attr.team": LeaveRequest.team,
    "request.resource.attr.priority": LeaveRequest.priority,
}


# `get_query` supports both `Table` instances and ORM entities:
# ORM entity - honouring object level relationships via the sqlalchemy ORM.
# Passing a model returns `Select[Tuple[LeaveRequest]]`, so the row type survives
# into `session.execute(query).scalars()` without a manual annotation.
query = get_query(plan, LeaveRequest, attr_map)
# Alternatively it can generate legacy queries by passing the Table instance,
# which returns `Select[Any]` — a Core table carries no row type.
query: Select = get_query(plan, LeaveRequest.__table__, attr_map)


# NOTE: if columns defined within the attr_map originate from more than one table, we need to define a mapping as the optional 4th positional arg to `get_query`.
# The argument is in the form:
#   `list[tuple[Table | DeclarativeMeta | type[DeclarativeBase], BinaryExpression | ColumnOperators]]`
# e.g.:
query: Select = get_query(
    plan,
    Table1,
    {
        "request.resource.attr.foo": Table1.foo,  # or `Table1.__table__.c.foo`
        "request.resource.attr.bar": Table2.bar,
        "request.resource.attr.bosh": Table3.bosh,
    },
    [
        (Table2, Table1.table2_id == Table2.id),  # or (Table2.__table__, Table1.__table__.c.table2_id == Table2.__table__.c.id)
        (Table3, Table1.table3_id == Table3.id),
    ]
)


# optionally extend the query
query = query.where(LeaveRequest.priority < 5)

# or return a subset of the selected columns (via a new `select`)
# NOTE: this is wise to do as standard, to avoid implicit joins generated by sqla `relationship()` usage, if present
query = query.with_only_columns(
    LeaveRequest.department,
    LeaveRequest.geography,
)

# Print the compiled query (for debug purposes)
print(query.compile(compile_kwargs={"literal_binds": True}))
```

### Overriding default predicates

By default, the library provides a base set of operators which are widely supported across a range of SQL dialects. However, in some cases, users may wish to override a particular operator for a more idiomatic/optimised alternative for a given database. An example of this could be postgres users preferring to use `= ANY` over `IN`:

```python
from sqlalchemy.sql.expression import any_

query = get_query(
    plan_resource_resp,
    some_table,
    attr_map={
        "request.resource.attr.foo": Table1.foo,
    },
    # override handler functions in the map below
    operator_override_fns={
        "in": lambda c, v: c == any_(v),
    },
)
```

The types are as follows:

```python
from sqlalchemy import Column
from sqlalchemy.orm import InstrumentedAttribute
from sqlalchemy.sql.expression import BinaryExpression, ColumnOperators

GenericColumn = Column | InstrumentedAttribute
GenericExpression = BinaryExpression | ColumnOperators
# and the actual map arg to `get_query` ⬇️
OperatorFnMap = dict[str, Callable[[GenericColumn, Any], GenericExpression]]
```

### Collection macros over known values

`exists`/`all` over a *known* collection — one whose elements the PDP resolves
at plan time, typically a principal attribute — is translated without any
override or relation mapping:

```yaml
condition:
  match:
    expr: P.attr.teams.exists(t, R.attr.team == t)
```

The Cerbos planner unrolls this into a plain `or`/`and` chain at 10 elements or
fewer and ships the lambda with a literal value-list collection above that
(`maxItems = 10` in the planner's struct matcher; cerbos/cerbos#2570,
cerbos/cerbos#2817). The adapter applies the same fold, uncapped, so the
generated SQL is equivalent on both sides of that threshold rather than
depending on how many teams a given principal happens to hold. Elements are
substituted into the lambda body — a bare `t` becomes the element, `t.name`
drills into it — and each substituted body is translated through the ordinary
pipeline, so overrides and NULL handling apply exactly as they do to a
planner-unrolled chain. An empty collection keeps CEL identity semantics:
`exists` matches nothing, `all` matches everything.

`exists_one`, `filter`, `map` and `except` have no flat equivalent and raise
over a literal value list, as does a `t.path` reference that the element does
not carry.

Macros over a collection *column or relation* (`R.attr.tags.exists(...)`) still
require an `operator_override_fns` entry — the adapter has no portable
correlated-subquery translation for them.
