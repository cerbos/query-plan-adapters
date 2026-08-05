import math

import pytest
from cerbos.response.v1 import response_pb2
from cerbos.sdk.model import (
    PlanResourcesFilter,
    PlanResourcesFilterKind,
    PlanResourcesResponse,
)
from google.protobuf.json_format import MessageToDict

from cerbos_sqlalchemy import get_query
from sqlalchemy import DateTime, String, any_, column, func, literal, table
from sqlalchemy.dialects import postgresql


def _default_resp_params():
    return {
        "request_id": "1",
        "action": "action",
        "resource_kind": "resource",
        "policy_version": "default",
    }


def _condition_to_dict(plan):
    # The HTTP client surfaces `to_dict()`; the gRPC client surfaces a raw
    # protobuf which needs `MessageToDict`. Mirrors the adapter's own
    # dual-mode handling so AST probes work under either transport.
    if isinstance(plan, response_pb2.PlanResourcesResponse):
        return MessageToDict(plan.filter.condition)
    return plan.filter.condition.to_dict()


def _conditional_plan(expression):
    return PlanResourcesResponse(
        filter=PlanResourcesFilter.from_dict(
            {
                "kind": PlanResourcesFilterKind.CONDITIONAL,
                "condition": {"expression": expression},
            }
        ),
        **_default_resp_params(),
    )


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

    def test_value_first_lt(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        # The planner preserves policy source order: `1 < R.attr.aNumber` arrives as
        # lt(value(1), variable(aNumber)) — the constant is the FIRST operand. The operator
        # must be mirrored (aNumber > 1) or the filter is silently inverted (#257).
        plan = cerbos_client.plan_resources("value-first-lt", principal, resource_desc)
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
        plan = cerbos_client.plan_resources("not-starts-with", principal, resource_desc)
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
        with pytest.raises(ValueError, match="Unrecognised operator: matches"):
            get_query(plan, resource_table, attr)

        query = get_query(
            plan,
            resource_table,
            attr,
            operator_override_fns={"matches": lambda c, v: c.regexp_match(v)},
        )
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

    def test_is_not_set(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        # `aOptionalString == null` -> Cerbos emits `eq(var, null)`. The
        # adapter resolves this to `col == None`, which SQLAlchemy lowers to
        # `IS NULL`. The test schema has no nullable optional column, so we
        # map onto `aString` (always populated): the predicate emits valid
        # SQL but matches no rows.
        plan = cerbos_client.plan_resources("is-not-set", principal, resource_desc)
        attr = {
            "request.resource.attr.aOptionalString": resource_table.aString,
        }
        query = get_query(plan, resource_table, attr)
        assert "IS NULL" in str(query.compile(compile_kwargs={"literal_binds": True}))
        res = conn.execute(query).fetchall()
        assert len(res) == 0

    def test_equal_field_to_field(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        # `aString == id` -> both operands are `variable`. The adapter resolves
        # variable-vs-variable comparisons as a column-to-column predicate
        # (previously it raised `KeyError: 'value'`; fixed as part of the
        # adversarial conformance work, #263 — see the `field-to-field` corpus
        # action). No row's aString equals its numeric id, so the filter
        # matches nothing.
        plan = cerbos_client.plan_resources(
            "equal-field-to-field", principal, resource_desc
        )
        attr = {
            "request.resource.attr.aString": resource_table.aString,
            "request.resource.attr.id": resource_table.id,
        }
        query = get_query(plan, resource_table, attr)
        compiled = str(query.compile(compile_kwargs={"literal_binds": True}))
        assert 'resource."aString" = resource.id' in compiled
        res = conn.execute(query).fetchall()
        assert len(res) == 0

    def test_equal_bool_false(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        plan = cerbos_client.plan_resources(
            "equal-bool-false", principal, resource_desc
        )
        attr = {
            "request.resource.attr.aBool": resource_table.aBool,
        }
        query = get_query(plan, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 1
        assert res[0].name == "resource2"

    def test_in_number(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        plan = cerbos_client.plan_resources("in-number", principal, resource_desc)
        attr = {
            "request.resource.attr.aNumber": resource_table.aNumber,
        }
        query = get_query(plan, resource_table, attr)
        res = conn.execute(query).fetchall()
        assert len(res) == 3

    def test_or_leaf_exists(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        # `aBool == true || tags.exists(t, t.name == "public")`. `tags` is
        # not a real column on the test schema; the caller supplies an
        # `exists` override that reports membership against whatever shape
        # they store. The adapter resolves operands eagerly before calling
        # an override, so the comprehension variable (`t.name`) must also
        # be present in the attr map. Here we pretend no row has a matching
        # tag so the `or` collapses to `aBool == true` and matches
        # resource1+resource3.
        plan = cerbos_client.plan_resources("or-leaf-exists", principal, resource_desc)
        attr = {
            "request.resource.attr.aBool": resource_table.aBool,
            "request.resource.attr.tags": resource_table.name,
            # Cerbos emits the comprehension iterator as a bare `t`
            # variable and a `t.name` field reference. We dummy both onto
            # an existing column; the `exists` override never reads them.
            "t": resource_table.name,
            "t.name": resource_table.name,
        }
        operator_override_fns = {
            # The `lambda` carries the predicate body for `exists`; we
            # discard it and let the outer `exists` override decide the
            # result.
            "lambda": lambda *_: literal(False),
            "exists": lambda *_: literal(False),
        }
        query = get_query(
            plan,
            resource_table,
            attr,
            operator_override_fns=operator_override_fns,
        )
        res = conn.execute(query).fetchall()
        assert len(res) == 2
        assert all(map(lambda x: x.name in {"resource1", "resource3"}, res))

    def test_all_nested(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        # `R.attr.tags.all(tag, tag.name == "public" && tag.id != "tag1")`.
        # AST: all(var:tags, lambda(and(eq(tag.name, "public"),
        #                               ne(tag.id, "tag1")), var:tag)).
        # TODO(#232): the SQLAlchemy adapter has no built-in handler for the
        # CEL `all` collection macro (nor for `lambda`); without an override
        # the default path fails loudly on the unsupported `lambda` operator.
        # (Previously it tripped earlier, on the `and` INSIDE the lambda body;
        # boolean combinators nested in value expressions are now translatable
        # as part of the adversarial conformance work, #263, so the traversal
        # reaches the comprehension itself before raising.) Locks in current
        # behavior.
        plan = cerbos_client.plan_resources("all-nested", principal, resource_desc)
        cond = _condition_to_dict(plan)
        assert cond["expression"]["operator"] == "all"
        all_operands = cond["expression"]["operands"]
        assert all_operands[0] == {"variable": "request.resource.attr.tags"}
        lambda_expr = all_operands[1]["expression"]
        assert lambda_expr["operator"] == "lambda"
        body_expr = lambda_expr["operands"][0]["expression"]
        assert body_expr["operator"] == "and"
        assert lambda_expr["operands"][1] == {"variable": "tag"}

        attr = {
            "request.resource.attr.tags": resource_table.name,
            "tag": resource_table.name,
            "tag.id": resource_table.id,
            "tag.name": resource_table.name,
        }
        with pytest.raises(ValueError, match="Unrecognised operator: lambda"):
            get_query(plan, resource_table, attr)

    def test_map_compared(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        # `R.attr.tags.map(t, t.id) == ["tag1", "tag2"]`.
        # AST: eq(map(var:tags, lambda(var:t.id, var:t)),
        #        value:["tag1","tag2"]).
        # TODO(#232): the SQLAlchemy adapter has no built-in handler for
        # `map` or `lambda`. The outer `eq` resolves its operands eagerly,
        # which descends into the `map` expression and trips on the
        # `lambda` child first. Locks in current behavior.
        plan = cerbos_client.plan_resources("map-compared", principal, resource_desc)
        cond = _condition_to_dict(plan)
        assert cond["expression"]["operator"] == "eq"
        eq_operands = cond["expression"]["operands"]
        map_expr = eq_operands[0]["expression"]
        assert map_expr["operator"] == "map"
        assert map_expr["operands"][0] == {"variable": "request.resource.attr.tags"}
        lambda_expr = map_expr["operands"][1]["expression"]
        assert lambda_expr["operator"] == "lambda"
        assert lambda_expr["operands"][0] == {"variable": "t.id"}
        assert lambda_expr["operands"][1] == {"variable": "t"}
        assert eq_operands[1] == {"value": ["tag1", "tag2"]}

        attr = {
            "request.resource.attr.tags": resource_table.name,
            "t": resource_table.name,
            "t.id": resource_table.id,
        }
        with pytest.raises(ValueError, match="Unrecognised operator: lambda"):
            get_query(plan, resource_table, attr)

    def test_filter_count_gt(
        self, cerbos_client, principal, resource_desc, resource_table, conn
    ):
        # `size(R.attr.tags.filter(t, t.name == "public")) > 0`.
        # AST: gt(size(filter(var:tags, lambda(eq(t.name,"public"), var:t))),
        #        value:0).
        # TODO(#232): the SQLAlchemy adapter ships a `size` default that
        # calls `func.length`, but it has no built-in handler for `filter`
        # or `lambda`. The default path descends into the nested `filter`
        # expression and raises on the unsupported `lambda` operator
        # before any composition can occur. Locks in current behavior.
        plan = cerbos_client.plan_resources("filter-count-gt", principal, resource_desc)
        cond = _condition_to_dict(plan)
        assert cond["expression"]["operator"] == "gt"
        gt_operands = cond["expression"]["operands"]
        size_expr = gt_operands[0]["expression"]
        assert size_expr["operator"] == "size"
        filter_expr = size_expr["operands"][0]["expression"]
        assert filter_expr["operator"] == "filter"
        assert filter_expr["operands"][0] == {"variable": "request.resource.attr.tags"}
        lambda_expr = filter_expr["operands"][1]["expression"]
        assert lambda_expr["operator"] == "lambda"
        body_expr = lambda_expr["operands"][0]["expression"]
        assert body_expr["operator"] == "eq"
        assert lambda_expr["operands"][1] == {"variable": "t"}
        assert gt_operands[1] == {"value": 0}

        attr = {
            "request.resource.attr.tags": resource_table.name,
            "t": resource_table.name,
            "t.name": resource_table.name,
        }
        with pytest.raises(ValueError, match="Unrecognised operator: lambda"):
            get_query(plan, resource_table, attr)


class TestSemanticEdgeTranslations:
    def test_in_list_with_explicit_null_uses_is_null_disjunct(
        self, resource_table, conn
    ):
        plan = _conditional_plan(
            {
                "operator": "in",
                "operands": [
                    {"variable": "request.resource.attr.name"},
                    {"value": ["resource1", None]},
                ],
            }
        )
        query = get_query(
            plan,
            resource_table,
            {"request.resource.attr.name": resource_table.name},
        )

        compiled = str(query.compile(compile_kwargs={"literal_binds": True}))
        assert " IN " in compiled
        assert " IS NULL" in compiled
        assert [row.name for row in conn.execute(query)] == ["resource1"]

    def test_constant_zero_division_preserves_nan_and_infinity(
        self, resource_table, conn
    ):
        attr = {"request.resource.attr.enabled": resource_table.aBool}

        nan_plan = _conditional_plan(
            {
                "operator": "gt",
                "operands": [
                    {
                        "expression": {
                            "operator": "if",
                            "operands": [
                                {"variable": "request.resource.attr.enabled"},
                                {"value": 1},
                                {
                                    "expression": {
                                        "operator": "div",
                                        "operands": [{"value": 0}, {"value": 0}],
                                    }
                                },
                            ],
                        }
                    },
                    {"value": 0.5},
                ],
            }
        )
        infinity_plan = _conditional_plan(
            {
                "operator": "gt",
                "operands": [
                    {
                        "expression": {
                            "operator": "if",
                            "operands": [
                                {"variable": "request.resource.attr.enabled"},
                                {
                                    "expression": {
                                        "operator": "div",
                                        "operands": [{"value": 1}, {"value": 0}],
                                    }
                                },
                                {
                                    "expression": {
                                        "operator": "div",
                                        "operands": [{"value": -1}, {"value": 0}],
                                    }
                                },
                            ],
                        }
                    },
                    {"value": 0.5},
                ],
            }
        )

        for plan in (nan_plan, infinity_plan):
            query = get_query(plan, resource_table, attr)
            assert {row.name for row in conn.execute(query)} == {
                "resource1",
                "resource3",
            }

        # PostgreSQL gives NaN a total ordering above finite numbers. Letting a
        # raw NaN bind reach that dialect turns the false CEL comparison into
        # true; the adapter must fold it before SQL compilation.
        nan_query = get_query(nan_plan, resource_table, attr)
        compiled = nan_query.compile(dialect=postgresql.dialect())
        assert not any(
            isinstance(value, float) and math.isnan(value)
            for value in compiled.params.values()
        )

    @pytest.mark.parametrize("field_first", (True, False))
    def test_direct_field_nan_ordering_is_folded_in_both_orders(
        self, field_first, resource_table, conn
    ):
        field = {"variable": "request.resource.attr.number"}
        nan = {
            "expression": {
                "operator": "div",
                "operands": [{"value": 0}, {"value": 0}],
            }
        }
        plan = _conditional_plan(
            {
                "operator": "gt" if field_first else "lt",
                "operands": [field, nan] if field_first else [nan, field],
            }
        )
        query = get_query(
            plan,
            resource_table,
            {"request.resource.attr.number": resource_table.aNumber},
        )

        assert conn.execute(query).fetchall() == []
        compiled = query.compile(dialect=postgresql.dialect())
        assert not any(
            isinstance(value, float) and not math.isfinite(value)
            for value in compiled.params.values()
        )

    def test_hierarchy_field_as_strict_ancestor(self, resource_table, conn):
        plan = _conditional_plan(
            {
                "operator": "ancestorOf",
                "operands": [
                    {
                        "expression": {
                            "operator": "hierarchy",
                            "operands": [{"variable": "request.resource.attr.path"}],
                        }
                    },
                    {
                        "expression": {
                            "operator": "hierarchy",
                            "operands": [{"value": "resource1.child"}],
                        }
                    },
                ],
            }
        )
        query = get_query(
            plan,
            resource_table,
            {"request.resource.attr.path": resource_table.name},
        )

        assert [row.name for row in conn.execute(query)] == ["resource1"]

    def test_timestamp_requires_temporal_column_and_normalizes_offset(self):
        temporal_table = table("events", column("created_at", DateTime(timezone=True)))
        plan = _conditional_plan(
            {
                "operator": "eq",
                "operands": [
                    {
                        "expression": {
                            "operator": "timestamp",
                            "operands": [
                                {"variable": "request.resource.attr.createdAt"}
                            ],
                        }
                    },
                    {
                        "expression": {
                            "operator": "timestamp",
                            "operands": [{"value": "2024-06-01T02:00:00+02:00"}],
                        }
                    },
                ],
            }
        )

        query = get_query(
            plan,
            temporal_table,
            {"request.resource.attr.createdAt": temporal_table.c.created_at},
        )
        compiled = str(query.compile(compile_kwargs={"literal_binds": True}))
        assert "2024-06-01 00:00:00" in compiled

        string_table = table("events", column("created_at", String))
        with pytest.raises(ValueError, match="DateTime column"):
            get_query(
                plan,
                string_table,
                {"request.resource.attr.createdAt": string_table.c.created_at},
            )

    def test_timestamp_rejects_inexact_nanosecond_precision(self):
        temporal_table = table("events", column("created_at", DateTime(timezone=True)))
        plan = _conditional_plan(
            {
                "operator": "eq",
                "operands": [
                    {
                        "expression": {
                            "operator": "timestamp",
                            "operands": [
                                {"variable": "request.resource.attr.createdAt"}
                            ],
                        }
                    },
                    {
                        "expression": {
                            "operator": "timestamp",
                            "operands": [{"value": "2024-06-01T00:00:00.123456789Z"}],
                        }
                    },
                ],
            }
        )

        with pytest.raises(ValueError, match="precision"):
            get_query(
                plan,
                temporal_table,
                {"request.resource.attr.createdAt": temporal_table.c.created_at},
            )

    def test_timestamp_accepts_exact_trailing_nanosecond_zeroes(self):
        temporal_table = table("events", column("created_at", DateTime(timezone=True)))
        plan = _conditional_plan(
            {
                "operator": "eq",
                "operands": [
                    {
                        "expression": {
                            "operator": "timestamp",
                            "operands": [
                                {"variable": "request.resource.attr.createdAt"}
                            ],
                        }
                    },
                    {
                        "expression": {
                            "operator": "timestamp",
                            "operands": [{"value": "2024-06-01T00:00:00.123456000Z"}],
                        }
                    },
                ],
            }
        )

        query = get_query(
            plan,
            temporal_table,
            {"request.resource.attr.createdAt": temporal_table.c.created_at},
        )
        compiled = str(query.compile(compile_kwargs={"literal_binds": True}))
        assert "2024-06-01 00:00:00.123456" in compiled

    @pytest.mark.parametrize(
        "value",
        [
            "2024-01-01",
            "2024-W01-1T00:00:00Z",
            "2024-01-01 00:00:00Z",
            "0000-01-01T00:00:00Z",
            "2024-02-30T00:00:00Z",
            "9999-12-31T23:00:00-02:00",
        ],
    )
    def test_timestamp_rejects_non_rfc3339_or_out_of_range_literals(self, value):
        temporal_table = table("events", column("created_at", DateTime(timezone=True)))
        plan = _conditional_plan(
            {
                "operator": "eq",
                "operands": [
                    {
                        "expression": {
                            "operator": "timestamp",
                            "operands": [
                                {"variable": "request.resource.attr.createdAt"}
                            ],
                        }
                    },
                    {
                        "expression": {
                            "operator": "timestamp",
                            "operands": [{"value": value}],
                        }
                    },
                ],
            }
        )

        with pytest.raises(ValueError, match="RFC-3339|instant range"):
            get_query(
                plan,
                temporal_table,
                {"request.resource.attr.createdAt": temporal_table.c.created_at},
            )


class TestGetQueryOverrides:
    def test_unrelated_override_does_not_bypass_table_mapping_validation(
        self, resource_table, user_table
    ):
        plan = _conditional_plan(
            {
                "operator": "eq",
                "operands": [
                    {"variable": "request.resource.attr.externalOwner"},
                    {"value": 1},
                ],
            }
        )

        with pytest.raises(TypeError, match="table_mapping"):
            get_query(
                plan,
                resource_table,
                {"request.resource.attr.externalOwner": user_table.id},
                operator_override_fns={"size": lambda *_: literal(0)},
            )

    def test_used_override_owns_foreign_operand_without_flat_mapping(
        self, resource_table, user_table, conn
    ):
        plan = _conditional_plan(
            {
                "operator": "eq",
                "operands": [
                    {"variable": "request.resource.attr.externalOwner"},
                    {"value": 1},
                ],
            }
        )
        query = get_query(
            plan,
            resource_table,
            {"request.resource.attr.externalOwner": user_table.id},
            operator_override_fns={
                # The override deliberately consumes the foreign marker and
                # rewrites it to a predicate on the root table.
                "eq": lambda _column, value: resource_table.ownedBy
                == str(value)
            },
        )

        assert {row.name for row in conn.execute(query)} == {
            "resource1",
            "resource2",
        }

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


class TestKnownValueCollections:
    """The planner unroll cliff (cerbos/cerbos#2570, #2817).

    `exists`/`all` over a known collection — typically a folded principal
    attribute — is unrolled by the planner into an or/and chain at 10 elements
    or fewer, and shipped as a lambda over a literal value list above that
    (`maxItems = 10` in the planner's struct matcher). The adapter must
    translate both shapes identically, or support becomes a data-dependent
    cliff that small-seed tests never cross.
    """

    @staticmethod
    def _teams(size):
        # "string" and "anotherString" match seeded rows; the rest are filler
        # that only moves the collection across the unroll threshold.
        teams = ["string", "anotherString"]
        while len(teams) < size:
            teams.append(f"filler-{len(teams)}")
        return teams

    @staticmethod
    def _assert_wire_shape(plan, size, unrolled_operator, macro_operator):
        # Supported PDPs are >= 0.54, where both macros unroll at <= 10
        # elements and ship the value-list lambda above that. Pinning the
        # shape per leg keeps each side of the cliff provably exercised: a
        # future planner threshold change fails here instead of silently
        # leaving one shape untested.
        expression = _condition_to_dict(plan)["expression"]
        assert expression["operator"] == (
            unrolled_operator if size <= 10 else macro_operator
        )

    @pytest.mark.parametrize("size", [9, 10, 11])
    def test_principal_exists_across_unroll_threshold(
        self,
        cerbos_client,
        principal_with_attr,
        resource_desc,
        resource_table,
        conn,
        size,
    ):
        teams = self._teams(size)
        plan = cerbos_client.plan_resources(
            "principal-exists", principal_with_attr({"teams": teams}), resource_desc
        )
        self._assert_wire_shape(plan, size, "or", "exists")

        query = get_query(
            plan,
            resource_table,
            {"request.resource.attr.aString": resource_table.aString},
        )
        assert sorted(row.name for row in conn.execute(query)) == [
            "resource1",
            "resource3",
        ]

    @pytest.mark.parametrize("size", [9, 10, 11])
    def test_principal_all_across_unroll_threshold(
        self,
        cerbos_client,
        principal_with_attr,
        resource_desc,
        resource_table,
        conn,
        size,
    ):
        teams = self._teams(size)
        plan = cerbos_client.plan_resources(
            "principal-all", principal_with_attr({"teams": teams}), resource_desc
        )
        self._assert_wire_shape(plan, size, "and", "all")

        query = get_query(
            plan,
            resource_table,
            {"request.resource.attr.aString": resource_table.aString},
        )
        assert sorted(row.name for row in conn.execute(query)) == ["resource2"]

    @staticmethod
    def _value_list_plan(operator, elements, body, variable="t"):
        return _conditional_plan(
            {
                "operator": operator,
                "operands": [
                    {"value": elements},
                    {
                        "expression": {
                            "operator": "lambda",
                            "operands": [body, {"variable": variable}],
                        }
                    },
                ],
            }
        )

    @staticmethod
    def _eq_body(variable="t"):
        return {
            "expression": {
                "operator": "eq",
                "operands": [
                    {"variable": "request.resource.attr.aString"},
                    {"variable": variable},
                ],
            }
        }

    def test_exists_over_value_list_folds_to_or(self, resource_table, conn):
        plan = self._value_list_plan(
            "exists", ["string", "anotherString"], self._eq_body()
        )
        query = get_query(
            plan,
            resource_table,
            {"request.resource.attr.aString": resource_table.aString},
        )
        assert sorted(row.name for row in conn.execute(query)) == [
            "resource1",
            "resource3",
        ]

    def test_all_over_value_list_folds_to_and(self, resource_table, conn):
        plan = self._value_list_plan(
            "all",
            ["string", "anotherString"],
            {
                "expression": {
                    "operator": "ne",
                    "operands": [
                        {"variable": "request.resource.attr.aString"},
                        {"variable": "t"},
                    ],
                }
            },
        )
        query = get_query(
            plan,
            resource_table,
            {"request.resource.attr.aString": resource_table.aString},
        )
        assert sorted(row.name for row in conn.execute(query)) == ["resource2"]

    def test_variable_path_drills_into_element_fields(self, resource_table, conn):
        plan = self._value_list_plan(
            "exists",
            [{"name": "string", "meta": {"rank": 1}}, {"name": "nope"}],
            {
                "expression": {
                    "operator": "eq",
                    "operands": [
                        {"variable": "request.resource.attr.aString"},
                        {"variable": "t.name"},
                    ],
                }
            },
        )
        query = get_query(
            plan,
            resource_table,
            {"request.resource.attr.aString": resource_table.aString},
        )
        assert [row.name for row in conn.execute(query)] == ["resource1"]

    def test_empty_value_list_keeps_cel_identity_semantics(self, resource_table, conn):
        # exists over [] matches nothing; all over [] matches everything.
        exists_query = get_query(
            self._value_list_plan("exists", [], self._eq_body()),
            resource_table,
            {"request.resource.attr.aString": resource_table.aString},
        )
        assert conn.execute(exists_query).fetchall() == []

        all_query = get_query(
            self._value_list_plan("all", [], self._eq_body()),
            resource_table,
            {"request.resource.attr.aString": resource_table.aString},
        )
        assert len(conn.execute(all_query).fetchall()) == 3

    def test_nested_lambda_rebinding_the_variable_shadows_substitution(
        self, resource_table
    ):
        # The inner lambda rebinds `t`, so its body must keep referencing the
        # inner binding; only the inner collection operand is substituted.
        plan = self._value_list_plan(
            "exists",
            [["a"], ["b"]],
            {
                "expression": {
                    "operator": "exists",
                    "operands": [
                        {"variable": "t"},
                        {
                            "expression": {
                                "operator": "lambda",
                                "operands": [
                                    {
                                        "expression": {
                                            "operator": "eq",
                                            "operands": [
                                                {
                                                    "variable": (
                                                        "request.resource.attr.aString"
                                                    )
                                                },
                                                {"variable": "t"},
                                            ],
                                        }
                                    },
                                    {"variable": "t"},
                                ],
                            }
                        },
                    ],
                }
            },
        )
        query = get_query(
            plan,
            resource_table,
            {"request.resource.attr.aString": resource_table.aString},
        )
        compiled = str(query.compile(compile_kwargs={"literal_binds": True}))
        assert "'a'" in compiled and "'b'" in compiled

    def test_missing_element_field_fails_closed(self, resource_table):
        plan = self._value_list_plan(
            "exists",
            [{"name": "string"}],
            {
                "expression": {
                    "operator": "eq",
                    "operands": [
                        {"variable": "request.resource.attr.aString"},
                        {"variable": "t.missing"},
                    ],
                }
            },
        )
        with pytest.raises(ValueError) as exc_info:
            get_query(
                plan,
                resource_table,
                {"request.resource.attr.aString": resource_table.aString},
            )
        assert 'Cannot resolve "t.missing"' in exc_info.value.args[0]

    @pytest.mark.parametrize("operator", ["exists_one", "filter", "map"])
    def test_unfoldable_macros_over_value_lists_fail_closed(
        self, resource_table, operator
    ):
        plan = self._value_list_plan(operator, ["string"], self._eq_body())
        with pytest.raises(ValueError) as exc_info:
            get_query(
                plan,
                resource_table,
                {"request.resource.attr.aString": resource_table.aString},
            )
        assert (
            f"{operator} over a literal collection value is not supported"
            in exc_info.value.args[0]
        )

    def test_non_list_collection_value_fails_closed(self, resource_table):
        plan = self._value_list_plan("exists", {"not": "a list"}, self._eq_body())
        with pytest.raises(ValueError) as exc_info:
            get_query(
                plan,
                resource_table,
                {"request.resource.attr.aString": resource_table.aString},
            )
        assert (
            "exists over a literal collection requires a list value"
            in exc_info.value.args[0]
        )

    def test_value_list_fold_precedes_operator_overrides(self, resource_table, conn):
        # An override exists to translate relation/column collections; a
        # literal value list can never be one, so the fold must win rather
        # than handing the override an unresolvable lambda.
        plan = self._value_list_plan("exists", ["string"], self._eq_body())
        query = get_query(
            plan,
            resource_table,
            {"request.resource.attr.aString": resource_table.aString},
            operator_override_fns={"exists": lambda c, v: literal(True)},
        )
        assert [row.name for row in conn.execute(query)] == ["resource1"]
