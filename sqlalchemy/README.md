# Cerbos + SQLAlchemy Adapter

An adapter library that takes a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into a [SQLAlchemy](https://docs.sqlalchemy.org/en/14/) Query object. This is designed to work alongside a project using the [Cerbos Python SDK](https://github.com/cerbos/cerbos-sdk-python).

The following conditions are supported: `and`, `or`, `eq`, `ne`, `lt`, `gt`, `lte`, `gte` and `in`. Other operators (eg math operators) can be implemented programatically, and attached to the query object via the `query.where(...)` API.

## Requirements
- Cerbos > v0.16

## Usage

```
pip install cerbos-sqlalchemy
```

```python
from cerbos.sdk.client import CerbosClient
from cerbos.sdk.model import Principal, ResourceDesc

from cerbos_sqlalchemy import get_query
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base, Query

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


# the attr_map arg of get_query expects a map[string, string], with cerbos attribute strings mapped to table column names
attr_map = {
    "request.resource.attr.department": "department",
    "request.resource.attr.geography": "geography",
    "request.resource.attr.team": "team",
    "request.resource.attr.priority": "priority",
}
query: Query = get_query(plan, LeaveRequest.__table__, attr_map)

# optionally extend the query
query = query.where(LeaveRequest.__table__.c.priority < 5)
```
