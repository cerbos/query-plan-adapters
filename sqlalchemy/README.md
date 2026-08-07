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

## Conformance contract

The adapter is differentially tested against Cerbos PDP 0.54.0 `check()` decisions using 20 hostile seed rows and executable SQLAlchemy queries. The Spring Data adapter defines the reference semantics for this compatibility snapshot.

| Classification | Coverage |
| --- | --- |
| Oracle-tested | 132 reference conformance actions |
| Fail-closed corpus shapes | Nanosecond `now()` thresholds, regex `matches()`, ordered list indexing/`get-field`, `timestamp()` over an ambiguous string column, `int()`/`double()` casts (SQL `CAST` reads a numeric prefix where CEL demands the whole string, and rounds where CEL truncates toward zero) and `filter()`/`map()` used as a condition (both return a list, not a boolean), and a constant zero divisor whose sign the HTTP transport discards (12 actions) |
| Representation-dependent | `null-eq-missing` — raises under `null_attribute_representation="omitted"`; translated as `IS NULL` under the default, which over-grants if the caller omits attributes for NULL columns |
| Known planner divergence | `has()` on a missing attribute is folded by the Cerbos planner to `ALWAYS_ALLOWED`, while `check()` denies the missing-attribute rows. Until the planner is fixed, use `R.attr.x != null` for database-backed attributes instead of `has(R.attr.x)` |

The conformance harness supplies the same public `operator_override_fns` mechanism available to applications for schema-specific collection translations. Regex `matches()` fails closed by default because SQL dialect regex engines do not guarantee CEL/RE2 semantics; applications may provide an override only when their database translation is known to be equivalent. Timestamp literals must use strict RFC 3339 grammar, resolve inside CEL's supported year 0001–9999 instant range, and be exactly representable at Python/SQLAlchemy microsecond precision: discarded fractional digits must be zero, and the mapped column/database must preserve microseconds. Unsupported shapes raise instead of producing a broader query. Every fail-closed shape's error message is pinned in the shared corpus (`conformance/actions.json`) and asserted by this adapter's conformance run, so a classification proves the throw names its declared mechanism rather than merely that something threw.

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
| Absent to-one parent | **Reproduced by `require_hops`**, and proved by the corpus (`w1-all-chain` and siblings) | `cerbos_sqlalchemy.require_hops` — see below. Call it from every override that reaches a collection through an intermediate to-one hop |

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
