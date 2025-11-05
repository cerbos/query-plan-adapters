# Cerbos + PyPika Adapter

Convert Cerbos Query Plan responses into PyPika queries.

## Installation

```bash
pip install cerbos-pypika
```

## Usage

```python
from pypika import Table
from cerbos.sdk.client import CerbosClient
from cerbos.sdk.model import Principal, ResourceDesc
from cerbos_pypika import get_query

# Define table
resources = Table('resources')

# Get query plan from Cerbos
with CerbosClient(host="http://localhost:3592") as client:
    principal = Principal("user123", roles={"user"})
    resource_desc = ResourceDesc("resource")
    plan = client.plan_resources("view", principal, resource_desc)

# Convert to PyPika query
attr_map = {
    "request.resource.attr.status": resources.status,
    "request.resource.attr.owner": resources.owner,
}

query = get_query(plan, resources, attr_map)
sql = query.get_sql()
print(sql)  # SELECT * FROM resources WHERE status='active'
```

## Features

- All Cerbos operators: eq, ne, lt, gt, le, ge, in
- Logical operators: and, or, not
- Multi-table joins
- Custom operator overrides
