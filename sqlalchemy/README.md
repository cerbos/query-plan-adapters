# Cerbos + SQLAlchemy Adapter

An adapter library that takes a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into a [SQLAlchemy](https://docs.sqlalchemy.org/en/14/) Select instance. This is designed to work alongside a project using the [Cerbos Python SDK](https://github.com/cerbos/cerbos-sdk-python).

The adapter supports logical and comparison operators, value-first and field-to-field comparisons, literal-safe string helpers, arithmetic and conditional expressions, scalar casts and sizes, timestamps, and hierarchy comparisons. `operator_override_fns` can provide database- or schema-specific translations for collection and other non-portable shapes.

## Conformance contract

The adapter is differentially tested against Cerbos PDP 0.54.0 `check()` decisions using 20 hostile seed rows and executable SQLAlchemy queries. The Spring Data adapter defines the reference semantics for this compatibility snapshot.

| Classification | Coverage |
| --- | --- |
| Oracle-tested | 114 reference conformance actions |
| Fail-closed corpus shapes | Nanosecond `now()` thresholds plus regex `matches()`, ordered list indexing/`get-field`, and `timestamp()` over an ambiguous string column (5 actions) |
| Known planner divergence | `has()` on a missing attribute is folded by the Cerbos planner to `ALWAYS_ALLOWED`, while `check()` denies the missing-attribute rows. Until the planner is fixed, use `R.attr.x != null` for database-backed attributes instead of `has(R.attr.x)` |

The conformance harness supplies the same public `operator_override_fns` mechanism available to applications for schema-specific collection translations. Regex `matches()` fails closed by default because SQL dialect regex engines do not guarantee CEL/RE2 semantics; applications may provide an override only when their database translation is known to be equivalent. Timestamp literals must use strict RFC 3339 grammar, resolve inside CEL's supported year 0001–9999 instant range, and be exactly representable at Python/SQLAlchemy microsecond precision: discarded fractional digits must be zero, and the mapped column/database must preserve microseconds. Unsupported shapes raise instead of producing a broader query.

## Requirements
- Cerbos > v0.16
- SQLAlchemy >= 1.4 / 2.0

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
# ORM entity - honouring object level relationships via the sqlalchemy ORM
query: Select = get_query(plan, LeaveRequest, attr_map)
# Alternatively it can generate legacy queries by passing the Table instance
query: Select = get_query(plan, LeaveRequest.__table__, attr_map)


# NOTE: if columns defined within the attr_map originate from more than one table, we need to define a mapping as the optional 4th positional arg to `get_query`.
# The argument is in the form:
#   `list[tuple[Table | DeclarativeMeta, BinaryExpression | ColumnOperators]]`
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
