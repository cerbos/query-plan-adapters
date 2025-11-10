import pytest
from cerbos.sdk.model import (
    PlanResourcesFilter,
    PlanResourcesFilterKind,
    PlanResourcesResponse,
)

from cerbos_pypika import cerbos_plan_criterion
from pypika import Query


def _default_resp_params():
    return {
        "request_id": "1",
        "action": "action",
        "resource_kind": "resource",
        "policy_version": "default",
    }


class TestCerbosPlanCriterion:
    def test_always_allow(
        self, cerbos_client, principal, resource_desc, resource_table, cursor
    ):
        plan = cerbos_client.plan_resources("always-allow", principal, resource_desc)
        criterion = cerbos_plan_criterion(plan, {})
        query = Query.from_(resource_table).select('*')
        if criterion is not None:
            query = query.where(criterion)
        sql = query.get_sql()
        
        cursor.execute(sql)
        res = cursor.fetchall()
        assert len(res) == 3

    def test_always_deny(
        self, cerbos_client, principal, resource_desc, resource_table, cursor
    ):
        plan = cerbos_client.plan_resources("always-deny", principal, resource_desc)
        criterion = cerbos_plan_criterion(plan, {})
        query = Query.from_(resource_table).select('*')
        if criterion is not None:
            query = query.where(criterion)
        sql = query.get_sql()
        
        cursor.execute(sql)
        res = cursor.fetchall()
        assert len(res) == 0

    def test_equals(
        self, cerbos_client, principal, resource_desc, resource_table, cursor
    ):
        plan = cerbos_client.plan_resources("equal", principal, resource_desc)
        attr = {
            "request.resource.attr.aBool": resource_table.aBool,
        }
        criterion = cerbos_plan_criterion(plan, attr)
        query = Query.from_(resource_table).select('*').where(criterion)
        sql = query.get_sql()
        
        cursor.execute(sql)
        res = cursor.fetchall()
        assert len(res) == 2
        assert all(row["name"] in {"resource1", "resource3"} for row in res)

    def test_not_equals(
        self, cerbos_client, principal, resource_desc, resource_table, cursor
    ):
        plan = cerbos_client.plan_resources("ne", principal, resource_desc)
        attr = {
            "request.resource.attr.aString": resource_table.aString,
        }
        criterion = cerbos_plan_criterion(plan, attr)
        query = Query.from_(resource_table).select('*').where(criterion)
        sql = query.get_sql()
        
        cursor.execute(sql)
        res = cursor.fetchall()
        assert len(res) == 2
        assert all(row["name"] in {"resource2", "resource3"} for row in res)

    def test_and(self, cerbos_client, principal, resource_desc, resource_table, cursor):
        plan = cerbos_client.plan_resources("and", principal, resource_desc)
        attr = {
            "request.resource.attr.aBool": resource_table.aBool,
            "request.resource.attr.aString": resource_table.aString,
        }
        criterion = cerbos_plan_criterion(plan, attr)
        query = Query.from_(resource_table).select('*').where(criterion)
        sql = query.get_sql()
        
        cursor.execute(sql)
        res = cursor.fetchall()
        assert len(res) == 1
        assert res[0]["name"] == "resource3"

    def test_not_and(
        self, cerbos_client, principal, resource_desc, resource_table, cursor
    ):
        plan = cerbos_client.plan_resources("nand", principal, resource_desc)
        attr = {
            "request.resource.attr.aBool": resource_table.aBool,
            "request.resource.attr.aString": resource_table.aString,
        }
        criterion = cerbos_plan_criterion(plan, attr)
        query = Query.from_(resource_table).select('*').where(criterion)
        sql = query.get_sql()
        
        cursor.execute(sql)
        res = cursor.fetchall()
        assert len(res) == 2
        assert all(row["name"] in {"resource1", "resource2"} for row in res)

    def test_or(self, cerbos_client, principal, resource_desc, resource_table, cursor):
        plan = cerbos_client.plan_resources("or", principal, resource_desc)
        attr = {
            "request.resource.attr.aBool": resource_table.aBool,
            "request.resource.attr.aString": resource_table.aString,
        }
        criterion = cerbos_plan_criterion(plan, attr)
        query = Query.from_(resource_table).select('*').where(criterion)
        sql = query.get_sql()
        
        cursor.execute(sql)
        res = cursor.fetchall()
        assert len(res) == 3

    def test_not_or(
        self, cerbos_client, principal, resource_desc, resource_table, cursor
    ):
        plan = cerbos_client.plan_resources("nor", principal, resource_desc)
        attr = {
            "request.resource.attr.aBool": resource_table.aBool,
            "request.resource.attr.aString": resource_table.aString,
        }
        criterion = cerbos_plan_criterion(plan, attr)
        query = Query.from_(resource_table).select('*').where(criterion)
        sql = query.get_sql()
        
        cursor.execute(sql)
        res = cursor.fetchall()
        assert len(res) == 0

    def test_in(self, cerbos_client, principal, resource_desc, resource_table, cursor):
        plan = cerbos_client.plan_resources("in", principal, resource_desc)
        attr = {
            "request.resource.attr.aString": resource_table.aString,
        }
        criterion = cerbos_plan_criterion(plan, attr)
        query = Query.from_(resource_table).select('*').where(criterion)
        sql = query.get_sql()
        
        cursor.execute(sql)
        res = cursor.fetchall()
        assert len(res) == 2
        assert all(row["name"] in {"resource1", "resource3"} for row in res)

    def test_lt(self, cerbos_client, principal, resource_desc, resource_table, cursor):
        plan = cerbos_client.plan_resources("lt", principal, resource_desc)
        attr = {
            "request.resource.attr.aNumber": resource_table.aNumber,
        }
        criterion = cerbos_plan_criterion(plan, attr)
        query = Query.from_(resource_table).select('*').where(criterion)
        sql = query.get_sql()
        
        cursor.execute(sql)
        res = cursor.fetchall()
        assert len(res) == 1
        assert res[0]["name"] == "resource1"

    def test_gt(self, cerbos_client, principal, resource_desc, resource_table, cursor):
        plan = cerbos_client.plan_resources("gt", principal, resource_desc)
        attr = {
            "request.resource.attr.aNumber": resource_table.aNumber,
        }
        criterion = cerbos_plan_criterion(plan, attr)
        query = Query.from_(resource_table).select('*').where(criterion)
        sql = query.get_sql()
        
        cursor.execute(sql)
        res = cursor.fetchall()
        assert len(res) == 2
        assert all(row["name"] in {"resource2", "resource3"} for row in res)

    def test_lte(self, cerbos_client, principal, resource_desc, resource_table, cursor):
        plan = cerbos_client.plan_resources("lte", principal, resource_desc)
        attr = {
            "request.resource.attr.aNumber": resource_table.aNumber,
        }
        criterion = cerbos_plan_criterion(plan, attr)
        query = Query.from_(resource_table).select('*').where(criterion)
        sql = query.get_sql()
        
        cursor.execute(sql)
        res = cursor.fetchall()
        assert len(res) == 2
        assert all(row["name"] in {"resource1", "resource2"} for row in res)

    def test_gte(self, cerbos_client, principal, resource_desc, resource_table, cursor):
        plan = cerbos_client.plan_resources("gte", principal, resource_desc)
        attr = {
            "request.resource.attr.aNumber": resource_table.aNumber,
        }
        criterion = cerbos_plan_criterion(plan, attr)
        query = Query.from_(resource_table).select('*').where(criterion)
        sql = query.get_sql()
        
        cursor.execute(sql)
        res = cursor.fetchall()
        assert len(res) == 3


class TestCerbosPlanCriterionOverrides:
    def test_in_single_query(self, resource_table, cursor):
        plan_resources_filter = PlanResourcesFilter.from_dict({
            "kind": PlanResourcesFilterKind.CONDITIONAL,
            "condition": {
                "expression": {
                    "operator": "in",
                    "operands": [
                        {"variable": "request.resource.attr.name"},
                        {"value": "resource1"},
                    ],
                },
            },
        })
        plan_resource_resp = PlanResourcesResponse(
            filter=plan_resources_filter,
            **_default_resp_params(),
        )
        attr = {
            "request.resource.attr.name": resource_table.name,
        }
        criterion = cerbos_plan_criterion(plan_resource_resp, attr)
        query = Query.from_(resource_table).select('*').where(criterion)
        sql = query.get_sql()
        
        cursor.execute(sql)
        res = cursor.fetchall()
        assert len(res) == 1
        assert res[0]["name"] == "resource1"

    def test_in_multiple_query(self, resource_table, cursor):
        plan_resources_filter = PlanResourcesFilter.from_dict({
            "kind": PlanResourcesFilterKind.CONDITIONAL,
            "condition": {
                "expression": {
                    "operator": "in",
                    "operands": [
                        {"variable": "request.resource.attr.name"},
                        {"value": ["resource1", "resource2"]},
                    ],
                },
            },
        })
        plan_resource_resp = PlanResourcesResponse(
            filter=plan_resources_filter,
            **_default_resp_params(),
        )
        attr = {
            "request.resource.attr.name": resource_table.name,
        }
        criterion = cerbos_plan_criterion(plan_resource_resp, attr)
        query = Query.from_(resource_table).select('*').where(criterion)
        sql = query.get_sql()
        
        cursor.execute(sql)
        res = cursor.fetchall()
        assert len(res) == 2
        assert all(row["name"] in {"resource1", "resource2"} for row in res)

    def test_unrecognised_response_attribute(self, resource_table):
        unknown_attribute = "request.resource.attr.foo"
        plan_resources_filter = PlanResourcesFilter.from_dict({
            "kind": PlanResourcesFilterKind.CONDITIONAL,
            "condition": {
                "expression": {
                    "operator": "eq",
                    "operands": [
                        {"variable": unknown_attribute},
                        {"value": 1},
                    ],
                },
            },
        })
        plan_resource_resp = PlanResourcesResponse(
            filter=plan_resources_filter,
            **_default_resp_params(),
        )
        attr = {
            "request.resource.attr.ownedBy": resource_table.ownedBy,
        }
        with pytest.raises(KeyError) as exc_info:
            cerbos_plan_criterion(plan_resource_resp, attr)
        assert (
            exc_info.value.args[0]
            == f"Attribute does not exist in the attribute column map: {unknown_attribute}"
        )

    def test_unrecognised_filter(self, resource_table):
        unknown_op = "unknown"
        plan_resources_filter = PlanResourcesFilter.from_dict({
            "kind": PlanResourcesFilterKind.CONDITIONAL,
            "condition": {
                "expression": {
                    "operator": unknown_op,
                    "operands": [
                        {"variable": "request.resource.attr.ownedBy"},
                        {"value": "1"},
                    ],
                },
            },
        })
        plan_resource_resp = PlanResourcesResponse(
            filter=plan_resources_filter,
            **_default_resp_params(),
        )
        attr = {
            "request.resource.attr.ownedBy": resource_table.ownedBy,
        }
        with pytest.raises(ValueError) as exc_info:
            cerbos_plan_criterion(plan_resource_resp, attr)
        assert exc_info.value.args[0] == f"Unrecognised operator: {unknown_op}"

    def test_in_equals_override(self, resource_table, cursor):
        plan_resources_filter = PlanResourcesFilter.from_dict({
            "kind": PlanResourcesFilterKind.CONDITIONAL,
            "condition": {
                "expression": {
                    "operator": "in",
                    "operands": [
                        {"variable": "request.resource.attr.name"},
                        {"value": "resource1"},
                    ],
                },
            },
        })
        plan_resource_resp = PlanResourcesResponse(
            filter=plan_resources_filter,
            **_default_resp_params(),
        )
        attr = {
            "request.resource.attr.name": resource_table.name,
        }
        operator_override_fns = {
            "in": lambda f, v: f == v,
        }
        criterion = cerbos_plan_criterion(plan_resource_resp, attr, operator_override_fns)
        query = Query.from_(resource_table).select('*').where(criterion)
        sql = query.get_sql()
        
        cursor.execute(sql)
        res = cursor.fetchall()
        assert len(res) == 1
        assert res[0]["name"] == "resource1"

    def test_in_override(self, cerbos_client, principal, resource_desc, resource_table):
        plan = cerbos_client.plan_resources("in", principal, resource_desc)
        attr = {
            "request.resource.attr.aString": resource_table.aString,
        }
        operator_override_fns = {
            "in": lambda f, v: f.isin(v) if isinstance(v, list) else f == v,
        }
        criterion = cerbos_plan_criterion(plan, attr, operator_override_fns)
        query = Query.from_(resource_table).select('id').where(criterion)
        sql = query.get_sql()
        assert "IN" in sql.upper()


class TestUserQueries:
    """Tests for filtering users by department - demonstrates adapter works on any table."""

    def test_user_department_engineering(
        self, cerbos_client, principal, user_desc, user_table, cursor
    ):
        plan = cerbos_client.plan_resources("department-engineering", principal, user_desc)
        attr = {
            "request.resource.attr.department": user_table.department,
        }
        criterion = cerbos_plan_criterion(plan, attr)
        query = Query.from_(user_table).select('*').where(criterion)
        sql = query.get_sql()
        
        cursor.execute(sql)
        res = cursor.fetchall()
        assert len(res) == 1
        assert res[0]["department"] == "engineering"
        assert res[0]["id"] == 1

    def test_user_department_in_list(
        self, cerbos_client, principal, user_desc, user_table, cursor
    ):
        plan = cerbos_client.plan_resources("department-in-list", principal, user_desc)
        attr = {
            "request.resource.attr.department": user_table.department,
        }
        criterion = cerbos_plan_criterion(plan, attr)
        query = Query.from_(user_table).select('*').where(criterion)
        sql = query.get_sql()
        
        cursor.execute(sql)
        res = cursor.fetchall()
        assert len(res) == 2
        departments = {row["department"] for row in res}
        assert departments == {"engineering", "marketing"}

    def test_user_engineering_admin(
        self, cerbos_client, principal, user_desc, user_table, cursor
    ):
        plan = cerbos_client.plan_resources("engineering-admin", principal, user_desc)
        attr = {
            "request.resource.attr.department": user_table.department,
            "request.resource.attr.role": user_table.role,
        }
        criterion = cerbos_plan_criterion(plan, attr)
        query = Query.from_(user_table).select('*').where(criterion)
        sql = query.get_sql()
        
        cursor.execute(sql)
        res = cursor.fetchall()
        assert len(res) == 1
        assert res[0]["id"] == 1
        assert res[0]["department"] == "engineering"
        assert res[0]["role"] == "admin"
