import pytest
from cerbos.response.v1 import response_pb2
from cerbos.sdk.model import (
    PlanResourcesFilter,
    PlanResourcesFilterKind,
    PlanResourcesResponse,
)
from google.protobuf.json_format import MessageToDict

from cerbos_sqlalchemy import get_query
from sqlalchemy import any_, func, literal


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
