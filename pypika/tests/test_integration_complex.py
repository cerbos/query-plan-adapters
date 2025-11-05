"""
Complex integration tests for multi-operator queries.

These tests verify that logical operators (and, or, not) work correctly
with actual database execution, testing complex real-world scenarios.

Test Data Reference:
| Resource   | name         | aBool | aString        | aNumber | ownedBy |
|------------|--------------|-------|----------------|---------|---------|
| resource1  | "resource1"  | 1     | "string"       | 1       | "1"     |
| resource2  | "resource2"  | 0     | "amIAString?"  | 2       | "1"     |
| resource3  | "resource3"  | 1     | "anotherString"| 3       | "2"     |
"""

import pytest
from cerbos.sdk.model import (
    PlanResourcesFilter,
    PlanResourcesFilterKind,
    PlanResourcesResponse,
)
from cerbos_pypika import get_query


@pytest.mark.skip(reason="Waiting for AND operator implementation")
def test_integration_and_multiple_conditions(cursor, resource_table):
    """
    Test AND operator with multiple conditions.
    Query: aBool == True AND aNumber > 1
    Expected: 1 row (resource3)
    """
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.CONDITIONAL,
        "condition": {
            "expression": {
                "operator": "and",
                "operands": [
                    {
                        "expression": {
                            "operator": "eq",
                            "operands": [
                                {"variable": "request.resource.attr.aBool"},
                                {"value": True},
                            ],
                        }
                    },
                    {
                        "expression": {
                            "operator": "gt",
                            "operands": [
                                {"variable": "request.resource.attr.aNumber"},
                                {"value": 1},
                            ],
                        }
                    },
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
    
    attr_map = {
        "request.resource.attr.aBool": resource_table.aBool,
        "request.resource.attr.aNumber": resource_table.aNumber,
    }
    
    query = get_query(plan, resource_table, attr_map)
    sql = query.get_sql()
    
    cursor.execute(sql)
    rows = cursor.fetchall()
    
    assert len(rows) == 1
    assert rows[0]["name"] == "resource3"
    assert rows[0]["aBool"] == 1
    assert rows[0]["aNumber"] == 3


@pytest.mark.skip(reason="Waiting for AND operator implementation")
def test_integration_range_query(cursor, resource_table):
    """
    Test range query using AND with ge and le.
    Query: aNumber >= 1 AND aNumber <= 2
    Expected: 2 rows (resource1, resource2)
    """
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.CONDITIONAL,
        "condition": {
            "expression": {
                "operator": "and",
                "operands": [
                    {
                        "expression": {
                            "operator": "ge",
                            "operands": [
                                {"variable": "request.resource.attr.aNumber"},
                                {"value": 1},
                            ],
                        }
                    },
                    {
                        "expression": {
                            "operator": "le",
                            "operands": [
                                {"variable": "request.resource.attr.aNumber"},
                                {"value": 2},
                            ],
                        }
                    },
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
    assert names == ["resource1", "resource2"]


@pytest.mark.skip(reason="Waiting for OR operator implementation")
def test_integration_or_condition(cursor, resource_table):
    """
    Test OR operator.
    Query: aNumber == 1 OR aNumber == 3
    Expected: 2 rows (resource1, resource3)
    """
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.CONDITIONAL,
        "condition": {
            "expression": {
                "operator": "or",
                "operands": [
                    {
                        "expression": {
                            "operator": "eq",
                            "operands": [
                                {"variable": "request.resource.attr.aNumber"},
                                {"value": 1},
                            ],
                        }
                    },
                    {
                        "expression": {
                            "operator": "eq",
                            "operands": [
                                {"variable": "request.resource.attr.aNumber"},
                                {"value": 3},
                            ],
                        }
                    },
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
    assert names == ["resource1", "resource3"]


@pytest.mark.skip(reason="Waiting for NOT operator implementation")
def test_integration_not_condition(cursor, resource_table):
    """
    Test NOT operator.
    Query: NOT (aBool == False)
    Expected: 2 rows (resource1, resource3 where aBool is True)
    """
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.CONDITIONAL,
        "condition": {
            "expression": {
                "operator": "not",
                "operands": [
                    {
                        "expression": {
                            "operator": "eq",
                            "operands": [
                                {"variable": "request.resource.attr.aBool"},
                                {"value": False},
                            ],
                        }
                    },
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


@pytest.mark.skip(reason="Waiting for AND/OR operator implementation")
def test_integration_complex_nested(cursor, resource_table):
    """
    Test complex nested logic with AND/OR combination.
    Query: (aBool == True AND aNumber > 1) OR (aString == "string")
    Expected: 2 rows (resource1 matches string condition, resource3 matches bool+number)
    """
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.CONDITIONAL,
        "condition": {
            "expression": {
                "operator": "or",
                "operands": [
                    {
                        "expression": {
                            "operator": "and",
                            "operands": [
                                {
                                    "expression": {
                                        "operator": "eq",
                                        "operands": [
                                            {"variable": "request.resource.attr.aBool"},
                                            {"value": True},
                                        ],
                                    }
                                },
                                {
                                    "expression": {
                                        "operator": "gt",
                                        "operands": [
                                            {"variable": "request.resource.attr.aNumber"},
                                            {"value": 1},
                                        ],
                                    }
                                },
                            ],
                        }
                    },
                    {
                        "expression": {
                            "operator": "eq",
                            "operands": [
                                {"variable": "request.resource.attr.aString"},
                                {"value": "string"},
                            ],
                        }
                    },
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
    
    attr_map = {
        "request.resource.attr.aBool": resource_table.aBool,
        "request.resource.attr.aNumber": resource_table.aNumber,
        "request.resource.attr.aString": resource_table.aString,
    }
    
    query = get_query(plan, resource_table, attr_map)
    sql = query.get_sql()
    
    cursor.execute(sql)
    rows = cursor.fetchall()
    
    assert len(rows) == 2
    names = sorted([row["name"] for row in rows])
    assert names == ["resource1", "resource3"]
