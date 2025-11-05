def test_fixtures_work(resource_table):
    """Verify test fixtures are set up correctly."""
    assert resource_table is not None
    assert str(resource_table) == '"resource"'


def test_get_query_is_importable():
    """Verify get_query can be imported."""
    from cerbos_pypika import get_query
    assert callable(get_query)


def test_always_allow(resource_table):
    """Test ALWAYS_ALLOWED returns unfiltered query."""
    from cerbos.sdk.model import (
        PlanResourcesFilter,
        PlanResourcesFilterKind,
        PlanResourcesResponse,
    )
    from cerbos_pypika import get_query
    
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
    
    assert "WHERE" not in sql


def test_always_deny(resource_table):
    """Test ALWAYS_DENIED returns impossible condition."""
    from cerbos.sdk.model import (
        PlanResourcesFilter,
        PlanResourcesFilterKind,
        PlanResourcesResponse,
    )
    from cerbos_pypika import get_query
    
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
    
    assert "WHERE" in sql


def test_eq_operator(resource_table):
    """Test eq operator with simple condition."""
    from cerbos.sdk.model import (
        PlanResourcesFilter,
        PlanResourcesFilterKind,
        PlanResourcesResponse,
    )
    from cerbos_pypika import get_query
    
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.CONDITIONAL,
        "condition": {
            "expression": {
                "operator": "eq",
                "operands": [
                    {"variable": "request.resource.attr.name"},
                    {"value": "test"},
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
        "request.resource.attr.name": resource_table.name,
    }
    
    query = get_query(plan, resource_table, attr_map)
    sql = query.get_sql()
    
    assert "WHERE" in sql
    assert "name" in sql.lower()


def test_ne_operator(resource_table):
    """Test ne operator."""
    from cerbos.sdk.model import (
        PlanResourcesFilter,
        PlanResourcesFilterKind,
        PlanResourcesResponse,
    )
    from cerbos_pypika import get_query
    
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.CONDITIONAL,
        "condition": {
            "expression": {
                "operator": "ne",
                "operands": [
                    {"variable": "request.resource.attr.name"},
                    {"value": "test"},
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
    
    attr_map = {"request.resource.attr.name": resource_table.name}
    query = get_query(plan, resource_table, attr_map)
    sql = query.get_sql()
    
    assert "WHERE" in sql
    assert ("<>" in sql or "!=" in sql)


def test_lt_operator(resource_table):
    """Test lt operator."""
    from cerbos.sdk.model import (
        PlanResourcesFilter,
        PlanResourcesFilterKind,
        PlanResourcesResponse,
    )
    from cerbos_pypika import get_query
    
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.CONDITIONAL,
        "condition": {
            "expression": {
                "operator": "lt",
                "operands": [
                    {"variable": "request.resource.attr.aNumber"},
                    {"value": 5},
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
    
    assert "WHERE" in sql
    assert "<" in sql


def test_gt_operator(resource_table):
    """Test gt operator."""
    from cerbos.sdk.model import (
        PlanResourcesFilter,
        PlanResourcesFilterKind,
        PlanResourcesResponse,
    )
    from cerbos_pypika import get_query
    
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.CONDITIONAL,
        "condition": {
            "expression": {
                "operator": "gt",
                "operands": [
                    {"variable": "request.resource.attr.aNumber"},
                    {"value": 5},
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
    
    assert "WHERE" in sql
    assert ">" in sql


def test_le_operator(resource_table):
    """Test le operator."""
    from cerbos.sdk.model import (
        PlanResourcesFilter,
        PlanResourcesFilterKind,
        PlanResourcesResponse,
    )
    from cerbos_pypika import get_query
    
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.CONDITIONAL,
        "condition": {
            "expression": {
                "operator": "le",
                "operands": [
                    {"variable": "request.resource.attr.aNumber"},
                    {"value": 5},
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
    
    assert "WHERE" in sql
    assert "<=" in sql


def test_ge_operator(resource_table):
    """Test ge operator."""
    from cerbos.sdk.model import (
        PlanResourcesFilter,
        PlanResourcesFilterKind,
        PlanResourcesResponse,
    )
    from cerbos_pypika import get_query
    
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.CONDITIONAL,
        "condition": {
            "expression": {
                "operator": "ge",
                "operands": [
                    {"variable": "request.resource.attr.aNumber"},
                    {"value": 5},
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
    
    assert "WHERE" in sql
    assert ">=" in sql


def test_and_operator(resource_table):
    """Test AND logical operator."""
    from cerbos.sdk.model import (
        PlanResourcesFilter,
        PlanResourcesFilterKind,
        PlanResourcesResponse,
    )
    from cerbos_pypika import get_query
    
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
    
    assert "WHERE" in sql
    assert "AND" in sql


def test_or_operator(resource_table):
    """Test OR logical operator."""
    from cerbos.sdk.model import (
        PlanResourcesFilter,
        PlanResourcesFilterKind,
        PlanResourcesResponse,
    )
    from cerbos_pypika import get_query
    
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
    
    attr_map = {
        "request.resource.attr.aNumber": resource_table.aNumber,
    }
    
    query = get_query(plan, resource_table, attr_map)
    sql = query.get_sql()
    
    assert "WHERE" in sql
    assert "OR" in sql


def test_not_operator(resource_table):
    """Test NOT logical operator."""
    from cerbos.sdk.model import (
        PlanResourcesFilter,
        PlanResourcesFilterKind,
        PlanResourcesResponse,
    )
    from cerbos_pypika import get_query
    
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
    
    attr_map = {
        "request.resource.attr.aBool": resource_table.aBool,
    }
    
    query = get_query(plan, resource_table, attr_map)
    sql = query.get_sql()
    
    assert "WHERE" in sql
    assert "NOT" in sql


def test_in_operator(resource_table):
    """Test IN operator for list membership."""
    from cerbos.sdk.model import (
        PlanResourcesFilter,
        PlanResourcesFilterKind,
        PlanResourcesResponse,
    )
    from cerbos_pypika import get_query
    
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.CONDITIONAL,
        "condition": {
            "expression": {
                "operator": "in",
                "operands": [
                    {"variable": "request.resource.attr.aNumber"},
                    {"value": [1, 3, 5]},
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
        "request.resource.attr.aNumber": resource_table.aNumber,
    }
    
    query = get_query(plan, resource_table, attr_map)
    sql = query.get_sql()
    
    assert "WHERE" in sql
    assert "IN" in sql


def test_in_operator_single_value(resource_table):
    """Test IN operator handles single non-list value."""
    from cerbos.sdk.model import (
        PlanResourcesFilter,
        PlanResourcesFilterKind,
        PlanResourcesResponse,
    )
    from cerbos_pypika import get_query
    
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.CONDITIONAL,
        "condition": {
            "expression": {
                "operator": "in",
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
    
    attr_map = {
        "request.resource.attr.aNumber": resource_table.aNumber,
    }
    
    query = get_query(plan, resource_table, attr_map)
    sql = query.get_sql()
    
    assert "WHERE" in sql
    assert "IN" in sql
def test_not_operator_empty_operands(resource_table):
    """Test NOT operator raises error with empty operands."""
    from cerbos.sdk.model import (
        PlanResourcesFilter,
        PlanResourcesFilterKind,
        PlanResourcesResponse,
    )
    from cerbos_pypika import get_query
    import pytest
    
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.CONDITIONAL,
        "condition": {
            "expression": {
                "operator": "not",
                "operands": [],
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
    
    attr_map = {}
    
    with pytest.raises(ValueError, match="NOT operator requires exactly one operand"):
        get_query(plan, resource_table, attr_map)
def test_join_support(resource_table):
    """Test join support with multi-table queries."""
    from cerbos.sdk.model import (
        PlanResourcesFilter,
        PlanResourcesFilterKind,
        PlanResourcesResponse,
    )
    from cerbos_pypika import get_query
    from pypika import Table
    
    # Create a user table for the join
    user_table = Table("user")
    
    # Query: resource.ownedBy = "1" AND user.role = "admin"
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
                                {"variable": "request.resource.attr.ownedBy"},
                                {"value": "1"},
                            ],
                        }
                    },
                    {
                        "expression": {
                            "operator": "eq",
                            "operands": [
                                {"variable": "request.principal.attr.role"},
                                {"value": "admin"},
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
        "request.resource.attr.ownedBy": resource_table.ownedBy,
        "request.principal.attr.role": user_table.role,
    }
    
    # Define join: resource.ownedBy = user.id
    joins = [(user_table, resource_table.ownedBy == user_table.id)]
    
    query = get_query(plan, resource_table, attr_map, joins=joins)
    sql = query.get_sql()
    
    assert "JOIN" in sql
    assert "user" in sql.lower()
    assert "resource" in sql.lower()
    assert "WHERE" in sql
def test_error_unknown_attribute():
    """Test error handling for unknown attribute in attr_map."""
    from cerbos.sdk.model import (
        PlanResourcesFilter,
        PlanResourcesFilterKind,
        PlanResourcesResponse,
    )
    from cerbos_pypika import get_query
    from pypika import Table
    import pytest
    
    resource_table = Table("resource")
    
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.CONDITIONAL,
        "condition": {
            "expression": {
                "operator": "eq",
                "operands": [
                    {"variable": "request.resource.attr.nonexistent"},
                    {"value": "test"},
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
    
    attr_map = {}  # Empty attr_map - attribute not found
    
    with pytest.raises(KeyError, match="Attribute does not exist in the attribute column map: request.resource.attr.nonexistent"):
        get_query(plan, resource_table, attr_map)


def test_error_unknown_operator():
    """Test error handling for unknown operator."""
    from cerbos.sdk.model import (
        PlanResourcesFilter,
        PlanResourcesFilterKind,
        PlanResourcesResponse,
    )
    from cerbos_pypika import get_query
    from pypika import Table
    import pytest
    
    resource_table = Table("resource")
    
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.CONDITIONAL,
        "condition": {
            "expression": {
                "operator": "unknown_op",
                "operands": [
                    {"variable": "request.resource.attr.name"},
                    {"value": "test"},
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
    
    attr_map = {"request.resource.attr.name": resource_table.name}
    
    with pytest.raises(ValueError, match="Unknown operator: unknown_op"):
        get_query(plan, resource_table, attr_map)
def test_operator_override_custom():
    """Test custom operator with override."""
    from cerbos.sdk.model import (
        PlanResourcesFilter,
        PlanResourcesFilterKind,
        PlanResourcesResponse,
    )
    from cerbos_pypika import get_query
    from pypika import Table
    
    resource_table = Table("resource")
    
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.CONDITIONAL,
        "condition": {
            "expression": {
                "operator": "custom_contains",
                "operands": [
                    {"variable": "request.resource.attr.name"},
                    {"value": "test"},
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
    
    attr_map = {"request.resource.attr.name": resource_table.name}
    
    # Define custom operator that uses LIKE
    custom_ops = {
        "custom_contains": lambda field, value: field.like(f"%{value}%")
    }
    
    query = get_query(plan, resource_table, attr_map, operator_override_fns=custom_ops)
    sql = query.get_sql()
    
    assert "WHERE" in sql
    assert "LIKE" in sql


def test_operator_override_replace_default():
    """Test overriding default operator behavior."""
    from cerbos.sdk.model import (
        PlanResourcesFilter,
        PlanResourcesFilterKind,
        PlanResourcesResponse,
    )
    from cerbos_pypika import get_query
    from pypika import Table
    
    resource_table = Table("resource")
    
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.CONDITIONAL,
        "condition": {
            "expression": {
                "operator": "eq",
                "operands": [
                    {"variable": "request.resource.attr.name"},
                    {"value": "test"},
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
    
    attr_map = {"request.resource.attr.name": resource_table.name}
    
    # Override eq to use case-insensitive comparison
    override_ops = {
        "eq": lambda field, value: field.ilike(value)
    }
    
    query = get_query(plan, resource_table, attr_map, operator_override_fns=override_ops)
    sql = query.get_sql()
    
    assert "WHERE" in sql
    assert "ILIKE" in sql
