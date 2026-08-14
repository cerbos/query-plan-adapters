"""``get_query``'s contract for plans the planner cannot produce, and for options no
policy can reach.

Every plan here is built by hand, and that is deliberate rather than an oversight: each
one is either malformed by construction — an operand shape the planner never emits, a
lambda reading a path no element carries — or a call-level argument the corpus has no
action for, such as an unknown ``null_attribute_representation`` or a model built with
the SQLAlchemy 2.0 declarative style. Neither can come from a wire fixture, because
neither corresponds to a policy.

What used to sit above all of this was 48 tests that planned corpus-adjacent shapes
against a live PDP loaded with the shared policy suite, executed the query against three
seeded rows and compared the result with a hardcoded count. Those are retired: the shapes are
all corpus actions now, ``test_translator.py`` pins the SQL each one emits and
``test_adversarial_conformance.py`` proves the rows against ``check()`` over 21 hostile
seeds instead of 3 friendly ones. A shape CEL *can* express belongs there, not here,
whatever its plan looks like — see
`ADR 0006 <../../docs/adr/0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md>`_.

Nothing in this file starts a PDP or a container.
"""

import math

import pytest
from cerbos.sdk.model import (
    PlanResourcesFilter,
    PlanResourcesFilterKind,
    PlanResourcesResponse,
)

from cerbos_sqlalchemy import get_query
from sqlalchemy import Boolean, DateTime, String, column, func, literal, table
from sqlalchemy.dialects import postgresql


def _default_resp_params():
    return {
        "request_id": "1",
        "action": "action",
        "resource_kind": "resource",
        "policy_version": "default",
    }


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


class TestNullAttributeRepresentation:
    """cerbos/query-plan-adapters#302.

    Both NULL-column conventions produce the identical ``eq(attr, null)`` wire
    node, so the adapter cannot infer which one the caller uses. Under
    ``"omitted"`` a NULL column carries no attribute at all, CEL raises a
    missing-attribute error, and ``check()`` denies every row -- an ``IS NULL``
    filter would return precisely the rows the PDP refuses.
    """

    @staticmethod
    def _null_eq_plan():
        return _conditional_plan(
            {
                "operator": "eq",
                "operands": [
                    {"variable": "request.resource.attr.name"},
                    {"value": None},
                ],
            }
        )

    def test_explicit_is_the_default_and_keeps_is_null(self, resource_table):
        attr = {"request.resource.attr.name": resource_table.name}
        default = get_query(self._null_eq_plan(), resource_table, attr)
        explicit = get_query(
            self._null_eq_plan(),
            resource_table,
            attr,
            null_attribute_representation="explicit",
        )

        compiled = str(default.compile(compile_kwargs={"literal_binds": True}))
        assert " IS NULL" in compiled
        assert compiled == str(explicit.compile(compile_kwargs={"literal_binds": True}))

    def test_omitted_rejects_eq_against_null(self, resource_table):
        with pytest.raises(ValueError, match="missing-attribute"):
            get_query(
                self._null_eq_plan(),
                resource_table,
                {"request.resource.attr.name": resource_table.name},
                null_attribute_representation="omitted",
            )

    def test_omitted_rejects_ne_against_null(self, resource_table):
        # Conservatively rejected too: `ne` alone is aligned under "omitted",
        # but negation wraps the built predicate, so a leaf cannot see whether
        # an enclosing `not` will flip IS NOT NULL back into IS NULL.
        plan = _conditional_plan(
            {
                "operator": "ne",
                "operands": [
                    {"variable": "request.resource.attr.name"},
                    {"value": None},
                ],
            }
        )
        with pytest.raises(ValueError, match="missing-attribute"):
            get_query(
                plan,
                resource_table,
                {"request.resource.attr.name": resource_table.name},
                null_attribute_representation="omitted",
            )

    def test_omitted_rejects_null_element_in_in_list(self, resource_table):
        plan = _conditional_plan(
            {
                "operator": "in",
                "operands": [
                    {"variable": "request.resource.attr.name"},
                    {"value": ["resource1", None]},
                ],
            }
        )
        with pytest.raises(ValueError, match="missing-attribute"):
            get_query(
                plan,
                resource_table,
                {"request.resource.attr.name": resource_table.name},
                null_attribute_representation="omitted",
            )

    def test_omitted_rejects_a_null_operand_nested_under_and(self, resource_table):
        plan = _conditional_plan(
            {
                "operator": "and",
                "operands": [
                    {
                        "expression": {
                            "operator": "not",
                            "operands": [
                                {
                                    "expression": {
                                        "operator": "eq",
                                        "operands": [
                                            {"variable": "request.resource.attr.name"},
                                            {"value": None},
                                        ],
                                    }
                                }
                            ],
                        }
                    },
                    {
                        "expression": {
                            "operator": "eq",
                            "operands": [
                                {"variable": "request.resource.attr.name"},
                                {"value": "resource1"},
                            ],
                        }
                    },
                ],
            }
        )
        with pytest.raises(ValueError, match="missing-attribute"):
            get_query(
                plan,
                resource_table,
                {"request.resource.attr.name": resource_table.name},
                null_attribute_representation="omitted",
            )

    def test_omitted_leaves_null_free_comparisons_untouched(self, resource_table, conn):
        plan = _conditional_plan(
            {
                "operator": "eq",
                "operands": [
                    {"variable": "request.resource.attr.name"},
                    {"value": "resource1"},
                ],
            }
        )
        query = get_query(
            plan,
            resource_table,
            {"request.resource.attr.name": resource_table.name},
            null_attribute_representation="omitted",
        )
        assert [row.name for row in conn.execute(query)] == ["resource1"]

    def test_unknown_representation_is_rejected(self, resource_table):
        with pytest.raises(ValueError, match="must be 'explicit' or 'omitted'"):
            get_query(
                self._null_eq_plan(),
                resource_table,
                {"request.resource.attr.name": resource_table.name},
                null_attribute_representation="sometimes",
            )


class TestAttributeNullRepresentation:
    """cerbos/query-plan-adapters#308.

    The per-attribute half of the same option. A call-level flag cannot express
    a policy suite that mixes the two conventions -- the same column mapped
    twice, sent explicitly under one attribute name and omitted under another --
    so the declaration is keyed by attribute and the call-level option is only
    its default.
    """

    @staticmethod
    def _attr_map(resource_table):
        return {
            "request.resource.attr.owner": resource_table.name,
            "request.resource.attr.coOwner": resource_table.aString,
            "request.resource.attr.plain": resource_table.name,
        }

    @staticmethod
    def _declared():
        return {
            "request.resource.attr.owner": "explicit",
            "request.resource.attr.coOwner": "explicit",
        }

    def _compiled(self, resource_table, condition):
        query = get_query(
            _conditional_plan(condition),
            resource_table,
            self._attr_map(resource_table),
            attribute_null_representation=self._declared(),
        )
        return str(query.compile(compile_kwargs={"literal_binds": True}))

    @staticmethod
    def _comparison(operator, variable, value):
        return {
            "operator": operator,
            "operands": [{"variable": variable}, {"value": value}],
        }

    # A null VALUE is not equal to "x", so CEL returns a definite FALSE and its
    # negation a definite TRUE. `name != 'x'` is UNKNOWN instead, which excludes
    # the row under BOTH polarities -- the row the PDP allows never comes back.
    def test_ne_against_a_constant_includes_a_null_row(self, resource_table):
        compiled = self._compiled(
            resource_table,
            self._comparison("ne", "request.resource.attr.owner", "x"),
        )
        assert "IS NOT NULL" in compiled
        assert compiled.startswith("SELECT") and " NOT (" in compiled

    def test_eq_against_a_constant_is_definite(self, resource_table):
        compiled = self._compiled(
            resource_table,
            self._comparison("eq", "request.resource.attr.owner", "x"),
        )
        assert "IS NOT NULL" in compiled

    # The equality family only. An ordering comparison against a null receiver
    # is a no-overload error in CEL, which denies under both polarities --
    # exactly what UNKNOWN already does -- so it keeps propagating it.
    def test_ordering_comparisons_are_left_alone(self, resource_table):
        compiled = self._compiled(
            resource_table,
            self._comparison("gt", "request.resource.attr.owner", "x"),
        )
        assert "IS NOT NULL" not in compiled

    def test_membership_without_a_null_element_is_definite(self, resource_table):
        compiled = self._compiled(
            resource_table,
            self._comparison("in", "request.resource.attr.owner", ["x", "y"]),
        )
        assert "IS NOT NULL" in compiled

    def test_two_explicit_nulls_match_field_to_field(self, resource_table):
        compiled = self._compiled(
            resource_table,
            {
                "operator": "eq",
                "operands": [
                    {"variable": "request.resource.attr.owner"},
                    {"variable": "request.resource.attr.coOwner"},
                ],
            },
        )
        assert compiled.count("IS NULL") == 2
        assert compiled.count("IS NOT NULL") == 2

    # An attribute the declaration does not name keeps the historical
    # rendering, so declaring the convention for one cannot change the SQL
    # emitted for any other mapping.
    def test_an_undeclared_attribute_is_untouched(self, resource_table):
        compiled = self._compiled(
            resource_table,
            self._comparison("ne", "request.resource.attr.plain", "x"),
        )
        assert "IS NOT NULL" not in compiled

    # The declaration overrides the call-level default in both directions,
    # which is the whole point: one call, two conventions.
    def test_declaring_omitted_rejects_a_null_operand_under_the_explicit_default(
        self, resource_table
    ):
        with pytest.raises(ValueError, match="null operand"):
            get_query(
                _conditional_plan(
                    self._comparison("eq", "request.resource.attr.owner", None)
                ),
                resource_table,
                self._attr_map(resource_table),
                null_attribute_representation="explicit",
                attribute_null_representation={
                    "request.resource.attr.owner": "omitted"
                },
            )

    def test_declaring_explicit_translates_a_null_operand_under_the_omitted_default(
        self, resource_table
    ):
        query = get_query(
            _conditional_plan(
                self._comparison("eq", "request.resource.attr.owner", None)
            ),
            resource_table,
            self._attr_map(resource_table),
            null_attribute_representation="omitted",
            attribute_null_representation=self._declared(),
        )
        assert " IS NULL" in str(query.compile(compile_kwargs={"literal_binds": True}))

    def test_an_unmapped_attribute_is_rejected(self, resource_table):
        with pytest.raises(ValueError, match="not in the attribute column map"):
            get_query(
                _conditional_plan(
                    self._comparison("eq", "request.resource.attr.owner", "x")
                ),
                resource_table,
                self._attr_map(resource_table),
                attribute_null_representation={
                    "request.resource.attr.absent": "explicit"
                },
            )

    def test_an_unknown_convention_is_rejected(self, resource_table):
        with pytest.raises(ValueError, match="must be 'explicit' or 'omitted'"):
            get_query(
                _conditional_plan(
                    self._comparison("eq", "request.resource.attr.owner", "x")
                ),
                resource_table,
                self._attr_map(resource_table),
                attribute_null_representation={
                    "request.resource.attr.owner": "sometimes"
                },
            )


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

        query = get_query(nan_plan, resource_table, attr)
        assert {row.name for row in conn.execute(query)} == {
            "resource1",
            "resource3",
        }

        # #312: the infinity plan divides a NON-zero numerator by a constant zero, and
        # the sign of that zero decides which infinity CEL produced. Over the HTTP
        # transport the operand arrives as the INTEGER 0 — Cerbos renders the double
        # -0.0 as `-0` and json.loads("-0") is 0 — so the sign bit is already gone and
        # the adapter cannot tell +Infinity from -Infinity. It fails closed rather than
        # assume the positive zero. The NaN plan above is unaffected: 0/0 is NaN under
        # either sign.
        with pytest.raises(ValueError, match="sign is indeterminate"):
            get_query(infinity_plan, resource_table, attr)

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


class TestKnownValueCollections:
    """The planner unroll cliff (cerbos/cerbos#2570, #2817), on the side no policy reaches.

    `exists`/`all` over a known collection — typically a folded principal attribute —
    is unrolled by the planner into an or/and chain at 10 elements or fewer, and
    shipped as a lambda over a literal value list above that (`maxItems = 10` in the
    planner's struct matcher). The adapter must translate both shapes identically, or
    support becomes a data-dependent cliff that small-seed tests never cross.

    **Both sides of the cliff are corpus actions**: `pv-exists`/`pv-all` ship the
    value-list lambda and `pv-exists-unrolled`/`pv-all-unrolled` the or/and chain, each
    with a wire fixture, a golden expectation and an oracle comparison. Two tests here
    used to plan a principal with 9, 10 and 11 teams against a live PDP to cross it by
    hand; the corpus crosses it with a real principal instead.

    What remains is the fold's own edge cases — an empty collection, a lambda rebinding
    its own variable, a `t.path` no element carries, a collection value that is not a
    list — which are malformed or degenerate plans rather than policy shapes.
    """

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


class TestDeclarativeStyles:
    """`get_query` accepts a model declared either declarative way, plus a Core `Table`.

    SQLAlchemy 2.0's `DeclarativeBase` is not a `declarative_base()` model in
    disguise: its metaclass sits outside the `DeclarativeMeta` hierarchy, so a
    2.0-style model is a distinct arm of `GenericTable` rather than a relabelled
    one (cerbos/query-plan-adapters#181).
    """

    @staticmethod
    def _eq_bool_plan():
        return _conditional_plan(
            {
                "operator": "eq",
                "operands": [
                    {"variable": "request.resource.attr.aBool"},
                    {"value": True},
                ],
            }
        )

    def test_declarative_base_is_not_a_declarative_meta(self, modern_resource_table):
        # Pins *why* `GenericTable` needs the extra member: if SQLAlchemy ever
        # folds the 2.0 metaclass back under `DeclarativeMeta`, this fails and
        # the member becomes removable.
        from sqlalchemy.orm import DeclarativeBase, DeclarativeMeta

        assert issubclass(modern_resource_table, DeclarativeBase)
        assert not isinstance(modern_resource_table, DeclarativeMeta)

    def test_declarative_base_model_filters(self, modern_resource_table, conn):
        query = get_query(
            self._eq_bool_plan(),
            modern_resource_table,
            {"request.resource.attr.aBool": modern_resource_table.aBool},
        )
        assert {row.name for row in conn.execute(query)} == {"resource1", "resource3"}

    def test_declarative_base_cross_table_mapping(
        self, modern_resource_table, modern_user_table, conn
    ):
        # Exercises `_get_table_name` on both sides of the mapping: the root
        # model and the joined one are both 2.0-style.
        plan = _conditional_plan(
            {
                "operator": "eq",
                "operands": [
                    {"variable": "request.resource.attr.ownerId"},
                    {"value": 1},
                ],
            }
        )
        query = get_query(
            plan,
            modern_resource_table,
            {"request.resource.attr.ownerId": modern_user_table.id},
            [
                (
                    modern_user_table,
                    modern_resource_table.ownedBy == modern_user_table.id,
                )
            ],
        )
        assert {row.name for row in conn.execute(query)} == {"resource1", "resource2"}

    def test_declarative_base_missing_table_mapping_still_fails_closed(
        self, modern_resource_table, modern_user_table
    ):
        plan = _conditional_plan(
            {
                "operator": "eq",
                "operands": [
                    {"variable": "request.resource.attr.ownerId"},
                    {"value": 1},
                ],
            }
        )
        with pytest.raises(TypeError, match="table_mapping"):
            get_query(
                plan,
                modern_resource_table,
                {"request.resource.attr.ownerId": modern_user_table.id},
            )

    def test_core_table_still_supported(self, conn):
        core_resource = table(
            "resource",
            column("name", String),
            column("aBool", Boolean),
        )
        query = get_query(
            self._eq_bool_plan(),
            core_resource,
            {"request.resource.attr.aBool": core_resource.c.aBool},
        )
        assert {row.name for row in conn.execute(query)} == {"resource1", "resource3"}
