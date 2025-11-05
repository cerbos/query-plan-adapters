from cerbos.sdk.model import (
    PlanResourcesFilter,
    PlanResourcesFilterKind,
    PlanResourcesResponse,
)
from cerbos_pypika import get_query


def test_integration_simple_filter(cursor, resource_table):
    """Test query execution with simple eq filter returns correct rows."""
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.CONDITIONAL,
        "condition": {
            "expression": {
                "operator": "eq",
                "operands": [
                    {"variable": "request.resource.attr.aNumber"},
                    {"value": 2},
                ],
            },
        },
    })
    plan = PlanResourcesResponse(
        filter=plan_filter,
        request_id="1",
        action="view",
        resource_kind="resource",
        policy_version="default",
    )
    
    attr_map = {"request.resource.attr.aNumber": resource_table.aNumber}
    query = get_query(plan, resource_table, attr_map)
    sql = query.get_sql()
    
    cursor.execute(sql)
    rows = cursor.fetchall()
    
    assert len(rows) == 1
    assert rows[0]["name"] == "resource2"
    assert rows[0]["aNumber"] == 2


def test_integration_numeric_range(cursor, resource_table):
    """Test query with numeric comparisons (gt, le)."""
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.CONDITIONAL,
        "condition": {
            "expression": {
                "operator": "gt",
                "operands": [
                    {"variable": "request.resource.attr.aNumber"},
                    {"value": 1},
                ],
            },
        },
    })
    plan = PlanResourcesResponse(
        filter=plan_filter,
        request_id="1",
        action="view",
        resource_kind="resource",
        policy_version="default",
    )
    
    attr_map = {"request.resource.attr.aNumber": resource_table.aNumber}
    query = get_query(plan, resource_table, attr_map)
    sql = query.get_sql()
    
    cursor.execute(sql)
    rows = cursor.fetchall()
    
    assert len(rows) == 2
    names = sorted([row["name"] for row in rows])
    assert names == ["resource2", "resource3"]


def test_integration_boolean_filter(cursor, resource_table):
    """Test query with boolean field."""
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.CONDITIONAL,
        "condition": {
            "expression": {
                "operator": "eq",
                "operands": [
                    {"variable": "request.resource.attr.aBool"},
                    {"value": True},
                ],
            },
        },
    })
    plan = PlanResourcesResponse(
        filter=plan_filter,
        request_id="1",
        action="view",
        resource_kind="resource",
        policy_version="default",
    )
    
    attr_map = {"request.resource.attr.aBool": resource_table.aBool}
    query = get_query(plan, resource_table, attr_map)
    sql = query.get_sql()
    
    cursor.execute(sql)
    rows = cursor.fetchall()
    
    assert len(rows) == 2
    names = sorted([row["name"] for row in rows])
    assert names == ["resource1", "resource3"]


def test_integration_string_comparison(cursor, resource_table):
    """Test query with string ne operator."""
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.CONDITIONAL,
        "condition": {
            "expression": {
                "operator": "ne",
                "operands": [
                    {"variable": "request.resource.attr.aString"},
                    {"value": "string"},
                ],
            },
        },
    })
    plan = PlanResourcesResponse(
        filter=plan_filter,
        request_id="1",
        action="view",
        resource_kind="resource",
        policy_version="default",
    )
    
    attr_map = {"request.resource.attr.aString": resource_table.aString}
    query = get_query(plan, resource_table, attr_map)
    sql = query.get_sql()
    
    cursor.execute(sql)
    rows = cursor.fetchall()
    
    assert len(rows) == 2
    names = sorted([row["name"] for row in rows])
    assert names == ["resource2", "resource3"]


def test_integration_always_allow(cursor, resource_table):
    """Test ALWAYS_ALLOWED returns all rows."""
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.ALWAYS_ALLOWED,
    })
    plan = PlanResourcesResponse(
        filter=plan_filter,
        request_id="1",
        action="view",
        resource_kind="resource",
        policy_version="default",
    )
    
    query = get_query(plan, resource_table, {})
    sql = query.get_sql()
    
    cursor.execute(sql)
    rows = cursor.fetchall()
    
    assert len(rows) == 3
    names = sorted([row["name"] for row in rows])
    assert names == ["resource1", "resource2", "resource3"]


def test_integration_always_deny(cursor, resource_table):
    """Test ALWAYS_DENIED returns no rows."""
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.ALWAYS_DENIED,
    })
    plan = PlanResourcesResponse(
        filter=plan_filter,
        request_id="1",
        action="view",
        resource_kind="resource",
        policy_version="default",
    )
    
    query = get_query(plan, resource_table, {})
    sql = query.get_sql()
    
    cursor.execute(sql)
    rows = cursor.fetchall()
    
    assert len(rows) == 0
