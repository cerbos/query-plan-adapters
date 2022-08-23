import pytest
from cerbos.sdk.model import (
    PlanResourcesFilter,
    PlanResourcesFilterKind,
    PlanResourcesResponse,
)

from cerbos_sqlalchemy import get_query


def _default_resp_params():
    return {
        "request_id": "1",
        "action": "action",
        "resource_kind": "contact",
        "policy_version": "default",
    }


class TestGetQuery:
    def test_always_allow(self, contact_table, session):
        plan_resource_resp = PlanResourcesResponse(
            filter=PlanResourcesFilter(
                PlanResourcesFilterKind.ALWAYS_ALLOWED,
            ),
            **_default_resp_params(),
        )
        query = get_query(plan_resource_resp, contact_table, {})
        res = session.execute(query).fetchall()
        assert len(res) == 3

    def test_always_deny(self, contact_table, session):
        plan_resource_resp = PlanResourcesResponse(
            filter=PlanResourcesFilter(
                PlanResourcesFilterKind.ALWAYS_DENIED,
            ),
            **_default_resp_params(),
        )
        query = get_query(plan_resource_resp, contact_table, {})
        res = session.execute(query).fetchall()
        assert len(res) == 0

    def test_get_is_user(self, contact_table, conn):
        plan_resources_filter = PlanResourcesFilter.from_dict(
            {
                "kind": PlanResourcesFilterKind.CONDITIONAL,
                "condition": {
                    "expression": {
                        "operator": "eq",
                        "operands": [
                            {"variable": "request.resource.attr.user"},
                            {"value": "user1"},
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
            "request.resource.attr.user": "user",
        }
        query = get_query(plan_resource_resp, contact_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 2
        assert all(map(lambda x: x.name in {"contact1", "contact3"}, res))

    def test_get_is_not_user(self, contact_table, conn):
        plan_resources_filter = PlanResourcesFilter.from_dict(
            {
                "kind": PlanResourcesFilterKind.CONDITIONAL,
                "condition": {
                    "expression": {
                        "operator": "ne",
                        "operands": [
                            {"variable": "request.resource.attr.user"},
                            {"value": "user1"},
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
            "request.resource.attr.user": "user",
        }
        query = get_query(plan_resource_resp, contact_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 1
        assert res[0].name == "contact2"

    def test_get_negating_and_query(self, contact_table, conn):
        plan_resources_filter = PlanResourcesFilter.from_dict(
            {
                "kind": PlanResourcesFilterKind.CONDITIONAL,
                "condition": {
                    "expression": {
                        "operator": "and",
                        "operands": [
                            {
                                "expression": {
                                    "operator": "eq",
                                    "operands": [
                                        {"variable": "request.resource.attr.user"},
                                        {"value": "user1"},
                                    ],
                                },
                            },
                            {
                                "expression": {
                                    "operator": "ne",
                                    "operands": [
                                        {"variable": "request.resource.attr.user"},
                                        {"value": "user1"},
                                    ],
                                },
                            },
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
            "request.resource.attr.user": "user",
        }
        query = get_query(plan_resource_resp, contact_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 0

    def test_get_union_or_query(self, contact_table, conn):
        plan_resources_filter = PlanResourcesFilter.from_dict(
            {
                "kind": PlanResourcesFilterKind.CONDITIONAL,
                "condition": {
                    "expression": {
                        "operator": "or",
                        "operands": [
                            {
                                "expression": {
                                    "operator": "eq",
                                    "operands": [
                                        {"variable": "request.resource.attr.user"},
                                        {"value": "user1"},
                                    ],
                                },
                            },
                            {
                                "expression": {
                                    "operator": "ne",
                                    "operands": [
                                        {"variable": "request.resource.attr.user"},
                                        {"value": "user1"},
                                    ],
                                },
                            },
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
            "request.resource.attr.user": "user",
        }
        query = get_query(plan_resource_resp, contact_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 3

    def test_get_lt_query(self, contact_table, conn):
        plan_resources_filter = PlanResourcesFilter.from_dict(
            {
                "kind": PlanResourcesFilterKind.CONDITIONAL,
                "condition": {
                    "expression": {
                        "operator": "lt",
                        "operands": [
                            {"variable": "request.resource.attr.age"},
                            {"value": 52},
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
            "request.resource.attr.age": "age",
        }
        query = get_query(plan_resource_resp, contact_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 2
        assert all(map(lambda x: x.name in {"contact1", "contact2"}, res))

    def test_get_gt_query(self, contact_table, conn):
        plan_resources_filter = PlanResourcesFilter.from_dict(
            {
                "kind": PlanResourcesFilterKind.CONDITIONAL,
                "condition": {
                    "expression": {
                        "operator": "gt",
                        "operands": [
                            {"variable": "request.resource.attr.age"},
                            {"value": 42},
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
            "request.resource.attr.age": "age",
        }
        query = get_query(plan_resource_resp, contact_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 1
        assert res[0].name == "contact3"

    def test_get_lte_query(self, contact_table, conn):
        plan_resources_filter = PlanResourcesFilter.from_dict(
            {
                "kind": PlanResourcesFilterKind.CONDITIONAL,
                "condition": {
                    "expression": {
                        "operator": "lte",
                        "operands": [
                            {"variable": "request.resource.attr.age"},
                            {"value": 42},
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
            "request.resource.attr.age": "age",
        }
        query = get_query(plan_resource_resp, contact_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 2
        assert all(map(lambda x: x.name in {"contact1", "contact2"}, res))

    def test_get_gte_query(self, contact_table, conn):
        plan_resources_filter = PlanResourcesFilter.from_dict(
            {
                "kind": PlanResourcesFilterKind.CONDITIONAL,
                "condition": {
                    "expression": {
                        "operator": "gte",
                        "operands": [
                            {"variable": "request.resource.attr.age"},
                            {"value": 42},
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
            "request.resource.attr.age": "age",
        }
        query = get_query(plan_resource_resp, contact_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 2
        assert all(map(lambda x: x.name in {"contact2", "contact3"}, res))

    def test_get_in_query(self, contact_table, conn):
        plan_resources_filter = PlanResourcesFilter.from_dict(
            {
                "kind": PlanResourcesFilterKind.CONDITIONAL,
                "condition": {
                    "expression": {
                        "operator": "in",
                        "operands": [
                            {"variable": "request.resource.attr.name"},
                            {"value": ["contact1", "contact2"]},
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
            "request.resource.attr.name": "name",
        }
        query = get_query(plan_resource_resp, contact_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 2
        assert all(map(lambda x: x.name in {"contact1", "contact2"}, res))

    def test_get_unrecognised_response_attribute(self, contact_table):
        unknown_attribute = "request.resource.attr.foo"
        plan_resources_filter = PlanResourcesFilter.from_dict(
            {
                "kind": PlanResourcesFilterKind.CONDITIONAL,
                "condition": {
                    "expression": {
                        "operator": "eq",
                        "operands": [
                            {"variable": unknown_attribute},
                            {"value": "user1"},
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
            "request.resource.attr.user": "user",
        }
        with pytest.raises(KeyError) as exc_info:
            get_query(plan_resource_resp, contact_table, attr)
        assert (
            exc_info.value.args[0]
            == f"Attribute does not exist in the attribute column map: {unknown_attribute}"
        )

    def test_get_unrecognised_map_attribute(self, contact_table):
        plan_resources_filter = PlanResourcesFilter.from_dict(
            {
                "kind": PlanResourcesFilterKind.CONDITIONAL,
                "condition": {
                    "expression": {
                        "operator": "eq",
                        "operands": [
                            {"variable": "request.resource.attr.user"},
                            {"value": "user1"},
                        ],
                    },
                },
            }
        )
        plan_resource_resp = PlanResourcesResponse(
            filter=plan_resources_filter,
            **_default_resp_params(),
        )
        unknown_column = "foo"
        attr = {
            "request.resource.attr.user": unknown_column,
        }
        with pytest.raises(AttributeError) as exc_info:
            get_query(plan_resource_resp, contact_table, attr)
        assert (
            exc_info.value.args[0]
            == f"Table column name does not match key in attribute column map: {unknown_column}"
        )

    def test_get_unrecognised_filter(self, contact_table):
        unknown_op = "unknown"
        plan_resources_filter = PlanResourcesFilter.from_dict(
            {
                "kind": PlanResourcesFilterKind.CONDITIONAL,
                "condition": {
                    "expression": {
                        "operator": unknown_op,
                        "operands": [
                            {"variable": "request.resource.attr.user"},
                            {"value": "user1"},
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
            "request.resource.attr.user": "user",
        }
        with pytest.raises(ValueError) as exc_info:
            get_query(plan_resource_resp, contact_table, attr)
        assert exc_info.value.args[0] == f"Unrecognised operator: {unknown_op}"
