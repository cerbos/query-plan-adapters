import pytest
from cerbos.sdk.model import (
    PlanResourcesFilter,
    PlanResourcesFilterKind,
    PlanResourcesResponse,
)

from cerbos_sqlalchemy import get_query
from sqlalchemy import any_, func, literal


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

    def test_not_and_demorgan(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        plan = cerbos_client.plan_resources("not-and", principal, resource_desc)
        attr = {
            "request.resource.attr.aBool": resource_table.aBool,
            "request.resource.attr.aString": resource_table.aString,
        }
        query = get_query(plan, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 2
        assert all(map(lambda x: x.name in {"resource1", "resource2"}, res))

    def test_not_or_demorgan(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        plan = cerbos_client.plan_resources("not-or", principal, resource_desc)
        attr = {
            "request.resource.attr.aBool": resource_table.aBool,
            "request.resource.attr.aString": resource_table.aString,
        }
        query = get_query(plan, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 0

    def test_not_gt(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        plan = cerbos_client.plan_resources("not-gt", principal, resource_desc)
        attr = {
            "request.resource.attr.aNumber": resource_table.aNumber,
        }
        query = get_query(plan, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 1
        assert res[0].name == "resource1"

    def test_not_lt(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        plan = cerbos_client.plan_resources("not-lt", principal, resource_desc)
        attr = {
            "request.resource.attr.aNumber": resource_table.aNumber,
        }
        query = get_query(plan, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 2
        assert all(map(lambda x: x.name in {"resource2", "resource3"}, res))

    def test_not_contains(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        plan = cerbos_client.plan_resources("not-contains", principal, resource_desc)
        attr = {
            "request.resource.attr.aString": resource_table.aString,
        }
        # `contains` is not a default operator in the SQLAlchemy adapter; supply
        # an override. We use `instr` rather than `LIKE` because SQLite's `LIKE`
        # is case-insensitive by default whereas CEL `String.contains` is
        # case-sensitive (matching e.g. Postgres `LIKE`).
        operator_override_fns = {
            "contains": lambda c, v: func.instr(c, v) > 0,
        }
        query = get_query(
            plan,
            resource_table,
            attr,
            operator_override_fns=operator_override_fns,
        )
        res = conn.execute(query).fetchall()
        assert len(res) == 2
        assert all(map(lambda x: x.name in {"resource2", "resource3"}, res))

    def test_not_starts_with(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        plan = cerbos_client.plan_resources(
            "not-starts-with", principal, resource_desc
        )
        attr = {
            "request.resource.attr.aString": resource_table.aString,
        }
        # `startsWith` is not a default operator in the SQLAlchemy adapter;
        # supply a case-sensitive override (CEL `String.startsWith` semantics).
        operator_override_fns = {
            "startsWith": lambda c, v: func.substr(c, 1, func.length(v)) == v,
        }
        query = get_query(
            plan,
            resource_table,
            attr,
            operator_override_fns=operator_override_fns,
        )
        res = conn.execute(query).fetchall()
        assert len(res) == 2
        assert all(map(lambda x: x.name in {"resource2", "resource3"}, res))

    def test_arith_add(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        plan = cerbos_client.plan_resources("arith-add", principal, resource_desc)
        attr = {"request.resource.attr.aNumber": resource_table.aNumber}
        query = get_query(plan, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 2
        assert all(map(lambda x: x.name in {"resource2", "resource3"}, res))

    def test_arith_sub(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        plan = cerbos_client.plan_resources("arith-sub", principal, resource_desc)
        attr = {"request.resource.attr.aNumber": resource_table.aNumber}
        query = get_query(plan, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 2
        assert all(map(lambda x: x.name in {"resource1", "resource2"}, res))

    def test_arith_mult(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        plan = cerbos_client.plan_resources("arith-mult", principal, resource_desc)
        attr = {"request.resource.attr.aNumber": resource_table.aNumber}
        query = get_query(plan, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 2
        assert all(map(lambda x: x.name in {"resource2", "resource3"}, res))

    def test_arith_div(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        # Cerbos transports numeric literals as protobuf doubles, so SQLite
        # performs float division here. `aNumber / 2.0 > 0` matches every row.
        plan = cerbos_client.plan_resources("arith-div", principal, resource_desc)
        attr = {"request.resource.attr.aNumber": resource_table.aNumber}
        query = get_query(plan, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 3

    def test_arith_mod(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        plan = cerbos_client.plan_resources("arith-mod", principal, resource_desc)
        attr = {"request.resource.attr.aNumber": resource_table.aNumber}
        query = get_query(plan, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 1
        assert res[0].name == "resource2"

    def test_matches_regex(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        plan = cerbos_client.plan_resources("matches-regex", principal, resource_desc)
        attr = {"request.resource.attr.aString": resource_table.aString}
        query = get_query(plan, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 1
        assert res[0].name == "resource1"

    def test_index_list(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        # `ownedBy` is modelled as a scalar foreign-key string here, but the
        # policy uses `ownedBy[0]`, so callers must supply an `index`
        # override that knows how to translate indexed access for their
        # storage shape. We treat the scalar as a single-element list and
        # match against the user id "1" (stored value for "user1").
        plan = cerbos_client.plan_resources("index-list", principal, resource_desc)
        attr = {"request.resource.attr.ownedBy": resource_table.ownedBy}
        operator_override_fns = {
            # Treat the scalar column as the indexed-into element directly.
            "index": lambda c, _: c,
            # Map the policy literal "user1" to the FK value "1".
            "eq": lambda c, v: c == ("1" if v == "user1" else v),
        }
        query = get_query(
            plan,
            resource_table,
            attr,
            operator_override_fns=operator_override_fns,
        )
        res = conn.execute(query).fetchall()
        assert len(res) == 2
        assert all(map(lambda x: x.name in {"resource1", "resource2"}, res))

    def test_convert_string(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        plan = cerbos_client.plan_resources("convert-string", principal, resource_desc)
        attr = {"request.resource.attr.aNumber": resource_table.aNumber}
        query = get_query(plan, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 1
        assert res[0].name == "resource1"

    def test_convert_double(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        plan = cerbos_client.plan_resources("convert-double", principal, resource_desc)
        attr = {"request.resource.attr.aNumber": resource_table.aNumber}
        query = get_query(plan, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 2
        assert all(map(lambda x: x.name in {"resource2", "resource3"}, res))

    def test_convert_int(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        # All `aString` values are non-numeric, so SQLite casts them to 0 and
        # the predicate `int(aString) > 0` matches no rows.
        plan = cerbos_client.plan_resources("convert-int", principal, resource_desc)
        attr = {"request.resource.attr.aString": resource_table.aString}
        query = get_query(plan, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 0

    def test_ternary(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        plan = cerbos_client.plan_resources("ternary", principal, resource_desc)
        attr = {
            "request.resource.attr.aBool": resource_table.aBool,
            "request.resource.attr.aNumber": resource_table.aNumber,
        }
        query = get_query(plan, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 2
        assert all(map(lambda x: x.name in {"resource1", "resource3"}, res))

    def test_string_size(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        plan = cerbos_client.plan_resources("string-size", principal, resource_desc)
        attr = {"request.resource.attr.aString": resource_table.aString}
        query = get_query(plan, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 3

    def test_empty_collection(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        # `tags` is not a real column on the test schema. The caller supplies
        # a `size` override that reports the collection's length given
        # whatever storage representation it uses. Here we pretend every row
        # has zero tags so the predicate `size(tags) == 0` matches all rows.
        plan = cerbos_client.plan_resources(
            "empty-collection", principal, resource_desc
        )
        attr = {"request.resource.attr.tags": resource_table.name}
        operator_override_fns = {
            "size": lambda c, _: literal(0),
        }
        query = get_query(
            plan,
            resource_table,
            attr,
            operator_override_fns=operator_override_fns,
        )
        res = conn.execute(query).fetchall()
        assert len(res) == 3


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
