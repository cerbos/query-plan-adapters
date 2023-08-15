import pytest
from cerbos.sdk.model import (
    PlanResourcesFilter,
    PlanResourcesFilterKind,
    PlanResourcesResponse,
)

from cerbos_sqlalchemy import get_query
from sqlalchemy import any_


def _default_resp_params():
    return {
        "request_id": "1",
        "action": "action",
        "resource_kind": "resource",
        "policy_version": "default",
    }


class TestGetQuery:
    def test_always_allow(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        plan = cerbos_client.plan_resources("always-allow", principal, resource_desc)
        query = get_query(plan, resource_table, {})
        res = conn.execute(query).fetchall()
        assert len(res) == 3

    def test_always_deny(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        plan = cerbos_client.plan_resources("always-deny", principal, resource_desc)
        query = get_query(plan, resource_table, {})
        res = conn.execute(query).fetchall()
        assert len(res) == 0

    def test_equals(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        plan = cerbos_client.plan_resources("equal", principal, resource_desc)
        attr = {
            "request.resource.attr.aBool": resource_table.aBool,
        }
        query = get_query(plan, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 2
        assert all(map(lambda x: x.name in {"resource1", "resource3"}, res))

    def test_not_equals(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        plan = cerbos_client.plan_resources("ne", principal, resource_desc)
        attr = {
            "request.resource.attr.aString": resource_table.aString,
        }
        query = get_query(plan, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 2
        assert all(map(lambda x: x.name in {"resource2", "resource3"}, res))

    def test_and(self, cerbos_client, principal, resource_desc, resource_table, conn):
        plan = cerbos_client.plan_resources("and", principal, resource_desc)
        attr = {
            "request.resource.attr.aBool": resource_table.aBool,
            "request.resource.attr.aString": resource_table.aString,
        }
        query = get_query(plan, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 1
        assert res[0].name == "resource3"

    def test_not_and(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        plan = cerbos_client.plan_resources("nand", principal, resource_desc)
        attr = {
            "request.resource.attr.aBool": resource_table.aBool,
            "request.resource.attr.aString": resource_table.aString,
        }
        query = get_query(plan, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 2
        assert all(map(lambda x: x.name in {"resource1", "resource2"}, res))

    def test_or(self, cerbos_client, principal, resource_desc, resource_table, conn):
        plan = cerbos_client.plan_resources("or", principal, resource_desc)
        attr = {
            "request.resource.attr.aBool": resource_table.aBool,
            "request.resource.attr.aString": resource_table.aString,
        }
        query = get_query(plan, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 3

    def test_not_or(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        plan = cerbos_client.plan_resources("nor", principal, resource_desc)
        attr = {
            "request.resource.attr.aBool": resource_table.aBool,
            "request.resource.attr.aString": resource_table.aString,
        }
        query = get_query(plan, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 0

    def test_in(self, cerbos_client, principal, resource_desc, resource_table, conn):
        plan = cerbos_client.plan_resources("in", principal, resource_desc)
        attr = {
            "request.resource.attr.aString": resource_table.aString,
        }
        query = get_query(plan, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 2
        assert all(map(lambda x: x.name in {"resource1", "resource3"}, res))

    def test_lt(self, cerbos_client, principal, resource_desc, resource_table, conn):
        plan = cerbos_client.plan_resources("lt", principal, resource_desc)
        attr = {
            "request.resource.attr.aNumber": resource_table.aNumber,
        }
        query = get_query(plan, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 1
        assert res[0].name == "resource1"

    def test_gt(self, cerbos_client, principal, resource_desc, resource_table, conn):
        plan = cerbos_client.plan_resources("gt", principal, resource_desc)
        attr = {
            "request.resource.attr.aNumber": resource_table.aNumber,
        }
        query = get_query(plan, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 2
        assert all(map(lambda x: x.name in {"resource2", "resource3"}, res))

    def test_lte(self, cerbos_client, principal, resource_desc, resource_table, conn):
        plan = cerbos_client.plan_resources("lte", principal, resource_desc)
        attr = {
            "request.resource.attr.aNumber": resource_table.aNumber,
        }
        query = get_query(plan, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 2
        assert all(map(lambda x: x.name in {"resource1", "resource2"}, res))

    def test_gte(self, cerbos_client, principal, resource_desc, resource_table, conn):
        plan = cerbos_client.plan_resources("gte", principal, resource_desc)
        attr = {
            "request.resource.attr.aNumber": resource_table.aNumber,
        }
        query = get_query(plan, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 3

    def test_relation_some(
        self, cerbos_client, principal, resource_desc, user_table, resource_table, conn
    ):
        plan = cerbos_client.plan_resources("relation-some", principal, resource_desc)
        attr = {
            "request.resource.attr.ownedBy": resource_table.ownedBy,
        }
        table_mapping = [(user_table, resource_table.ownedBy == user_table.id)]
        query = get_query(plan, resource_table, attr, table_mapping)
        res = conn.execute(query).fetchall()
        assert len(res) == 2
        assert all(map(lambda x: x.name in {"resource1", "resource2"}, res))

    def test_relation_none(
        self, cerbos_client, principal, resource_desc, user_table, resource_table, conn
    ):
        plan = cerbos_client.plan_resources("relation-none", principal, resource_desc)
        attr = {
            "request.resource.attr.ownedBy": resource_table.ownedBy,
        }
        table_mapping = [(user_table, resource_table.ownedBy == user_table.id)]
        query = get_query(plan, resource_table, attr, table_mapping)
        res = conn.execute(query).fetchall()
        assert len(res) == 1
        assert res[0].name == "resource3"

    def test_relation_is(
        self, cerbos_client, principal, resource_desc, user_table, resource_table, conn
    ):
        plan = cerbos_client.plan_resources("relation-is", principal, resource_desc)
        attr = {
            "request.resource.attr.createdBy": resource_table.createdBy,
        }
        table_mapping = [(user_table, resource_table.ownedBy == user_table.id)]
        query = get_query(plan, resource_table, attr, table_mapping)
        res = conn.execute(query).fetchall()
        assert len(res) == 1
        assert res[0].name == "resource1"

    def test_relation_is_not(
        self, cerbos_client, principal, resource_desc, user_table, resource_table, conn
    ):
        plan = cerbos_client.plan_resources("relation-is-not", principal, resource_desc)
        attr = {
            "request.resource.attr.createdBy": resource_table.createdBy,
        }
        table_mapping = [(user_table, resource_table.ownedBy == user_table.id)]
        query = get_query(plan, resource_table, attr, table_mapping)
        res = conn.execute(query).fetchall()
        assert len(res) == 2
        assert all(map(lambda x: x.name in {"resource2", "resource3"}, res))


class TestGetQueryOverrides:
    def test_in_single_query(self, resource_table, conn):
        plan_resources_filter = PlanResourcesFilter.from_dict(
            {
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
            }
        )
        plan_resource_resp = PlanResourcesResponse(
            filter=plan_resources_filter,
            **_default_resp_params(),
        )
        attr = {
            "request.resource.attr.name": resource_table.name,
        }
        query = get_query(plan_resource_resp, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 1
        assert res[0].name == "resource1"

    def test_in_multiple_query(self, resource_table, conn):
        plan_resources_filter = PlanResourcesFilter.from_dict(
            {
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
            }
        )
        plan_resource_resp = PlanResourcesResponse(
            filter=plan_resources_filter,
            **_default_resp_params(),
        )
        attr = {
            "request.resource.attr.name": resource_table.name,
        }
        query = get_query(plan_resource_resp, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 2
        assert all(map(lambda x: x.name in {"resource1", "resource2"}, res))

    def test_unrecognised_response_attribute(self, resource_table):
        unknown_attribute = "request.resource.attr.foo"
        plan_resources_filter = PlanResourcesFilter.from_dict(
            {
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
            }
        )
        plan_resource_resp = PlanResourcesResponse(
            filter=plan_resources_filter,
            **_default_resp_params(),
        )
        attr = {
            "request.resource.attr.ownedBy": resource_table.ownedBy,
        }
        with pytest.raises(KeyError) as exc_info:
            get_query(plan_resource_resp, resource_table, attr)
        assert (
            exc_info.value.args[0]
            == f"Attribute does not exist in the attribute column map: {unknown_attribute}"
        )

    def test_unrecognised_filter(self, resource_table):
        unknown_op = "unknown"
        plan_resources_filter = PlanResourcesFilter.from_dict(
            {
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
            }
        )
        plan_resource_resp = PlanResourcesResponse(
            filter=plan_resources_filter,
            **_default_resp_params(),
        )
        attr = {
            "request.resource.attr.ownedBy": resource_table.ownedBy,
        }
        with pytest.raises(ValueError) as exc_info:
            get_query(plan_resource_resp, resource_table, attr)
        assert exc_info.value.args[0] == f"Unrecognised operator: {unknown_op}"

    def test_in_equals_override(self, resource_table, conn):
        plan_resources_filter = PlanResourcesFilter.from_dict(
            {
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
            }
        )
        plan_resource_resp = PlanResourcesResponse(
            filter=plan_resources_filter,
            **_default_resp_params(),
        )
        attr = {
            "request.resource.attr.name": resource_table.name,
        }
        operator_override_fns = {
            "in": lambda c, v: c == v,
        }
        query = get_query(
            plan_resource_resp,
            resource_table,
            attr,
            operator_override_fns=operator_override_fns,
        )
        res = conn.execute(query).fetchall()
        assert len(res) == 1
        assert res[0].name == "resource1"

    def test_in_override(self, cerbos_client, principal, resource_desc, resource_table):
        plan = cerbos_client.plan_resources("in", principal, resource_desc)
        attr = {
            "request.resource.attr.aString": resource_table.aString,
        }
        operator_override_fns = {
            "in": lambda c, v: c == any_(v),
        }
        query = get_query(
            plan,
            resource_table,
            attr,
            operator_override_fns=operator_override_fns,
        )
        query = query.with_only_columns(resource_table.id)
        assert "= ANY (" in str(query)
