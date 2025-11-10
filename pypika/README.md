# Cerbos + PyPika Adapter

An adapter library that takes a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into a [PyPika](https://pypika.readthedocs.io/) Criterion for use in WHERE clauses. This is designed to work alongside a project using the [Cerbos Python SDK](https://github.com/cerbos/cerbos-sdk-python).

The following conditions are supported: `and`, `or`, `not`, `eq`, `ne`, `lt`, `gt`, `le`, `ge` and `in`. Other operators (eg LIKE, regex) can be implemented programatically and attached via the `operator_override_fns` parameter.

## Requirements
- Cerbos > v0.16
- PyPika >= 0.48

## Installation

```bash
pip install cerbos-pypika
```

> **Note**: Package is not yet published to PyPI. For now, install from source or use local development setup.

## Usage

```python
from cerbos.sdk.client import CerbosClient
from cerbos.sdk.model import Principal, ResourceDesc

from cerbos_pypika import cerbos_plan_criterion
from pypika import Query, Table, Order

# Define your table
leave_request = Table('leave_request')

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


# Map Cerbos attribute paths to PyPika Field objects
attr_map = {
    "request.resource.attr.department": leave_request.department,
    "request.resource.attr.geography": leave_request.geography,
    "request.resource.attr.team": leave_request.team,
    "request.resource.attr.priority": leave_request.priority,
}

# Convert the Cerbos plan to a PyPika Criterion
criterion = cerbos_plan_criterion(plan, attr_map)

# Use the criterion in your PyPika query builder
query = (Query
    .from_(leave_request)
    .select(leave_request.star)
    .where(criterion)
    .where(leave_request.priority < 5)  # Combine with additional filters
    .orderby(leave_request.id, order=Order.desc)
    .limit(10))

# Get the SQL string
sql = query.get_sql()
print(sql)
```

### Combining with other filters

The returned Criterion can be combined with other PyPika criteria using `&` (AND) and `|` (OR):

```python
criterion = cerbos_plan_criterion(plan, attr_map)

# Combine with AND
custom_filter = leave_request.active == True
combined = criterion & custom_filter
query = Query.from_(leave_request).select('*').where(combined)

# Combine with OR
alternate_filter = leave_request.department == 'engineering'
combined = criterion | alternate_filter
query = Query.from_(leave_request).select('*').where(combined)

# Can also negate
negated = criterion.negate()
query = Query.from_(leave_request).select('*').where(negated)
```

### Multi-table queries with joins

For queries spanning multiple tables, build your joins manually using PyPika:

```python
users = Table('users')
resources = Table('resources')

attr_map = {
    "request.resource.attr.status": resources.status,
    "request.principal.attr.role": users.role,
}

criterion = cerbos_plan_criterion(plan, attr_map)

query = (Query
    .from_(resources)
    .join(users).on(resources.user_id == users.id)
    .select(resources.star)
    .where(criterion)
    .orderby(resources.created_at))
```

**Note**: Unlike the SQLAlchemy adapter, this library does not provide automatic JOIN management. You are responsible for constructing the JOIN clauses in your PyPika query. The `cerbos_plan_criterion` function only generates the WHERE clause criterion.

### Overriding default operators

By default, the library provides a base set of operators which are widely supported across a range of SQL dialects. However, in some cases, users may wish to override a particular operator for a more idiomatic/optimised alternative for a given database. An example of this could be implementing a LIKE operator for substring matching:

```python
from pypika.terms import ValueWrapper

criterion = cerbos_plan_criterion(
    plan,
    attr_map={
        "request.resource.attr.name": some_table.name,
    },
    operator_override_fns={
        "like": lambda f, v: f.like(ValueWrapper(f"%{v}%")),
    },
)
```

The types are as follows:

```python
from typing import Any, Callable
from pypika import Field
from pypika.terms import Criterion

GenericField = Field
GenericCriterion = Criterion
OperatorFnMap = dict[str, Callable[[GenericField, Any], GenericCriterion]]
```

### Handling ALWAYS_ALLOWED and ALWAYS_DENIED

The function handles special Cerbos filter kinds:

- `ALWAYS_ALLOWED`: Returns `None` (no filtering needed - can omit `.where()` entirely)
- `ALWAYS_DENIED`: Returns a criterion that's always false (`1 = 0`)

```python
criterion = cerbos_plan_criterion(plan, attr_map)

if criterion is None:
    # ALWAYS_ALLOWED - no filtering needed
    query = Query.from_(table).select('*')
else:
    # Apply the criterion
    query = Query.from_(table).select('*').where(criterion)
```
