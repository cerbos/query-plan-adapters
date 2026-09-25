# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""``get_query`` contracts for caller options and plans the planner cannot produce.

The corpus cannot vary caller arguments such as operator overrides or model styles,
so they are tested here. Policy-reachable shapes belong in the corpus. No PDP needed.
"""

import math
import warnings

import pytest
from cerbos.sdk.model import (
    PlanResourcesFilter,
    PlanResourcesFilterKind,
    PlanResourcesResponse,
)
from sqlalchemy import Boolean, DateTime, String, column, create_engine, literal, table
from sqlalchemy.dialects import postgresql

from cerbos_sqlalchemy import UnsupportedPlanError, get_query


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
    """The call-level NULL convention (#302). The plan cannot tell the two apart.

    Under "omitted", CEL errors on the missing attribute and denies the row, so
    ``IS NULL`` would return exactly the rows the PDP refuses.
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

    @pytest.mark.parametrize(
        "operator, negated, expected",
        [
            ("eq", False, []),
            ("eq", True, [1]),
            ("ne", False, [1]),
            ("ne", True, []),
        ],
    )
    def test_omitted_answers_eq_and_ne_against_null_as_unknown_for_a_null_column(
        self, resource_table, operator, negated, expected
    ):
        # #551. A NULL column is a missing attribute, a CEL error that denies the
        # row under any number of enclosing NOTs; a present one is never null.
        comparison = {
            "operator": operator,
            "operands": [
                {"variable": "request.resource.attr.name"},
                {"value": None},
            ],
        }
        if negated:
            comparison = {"operator": "not", "operands": [{"expression": comparison}]}
        engine = create_engine("sqlite://")
        resource_table.metadata.create_all(engine)
        with engine.begin() as connection:
            connection.execute(
                resource_table.__table__.insert(),
                [{"id": 1, "name": "present"}, {"id": 2, "name": None}],
            )
            query = get_query(
                _conditional_plan(comparison),
                resource_table,
                {"request.resource.attr.name": resource_table.name},
                null_attribute_representation="omitted",
            )
            assert [row.id for row in connection.execute(query)] == expected
        engine.dispose()

    def test_omitted_rejects_an_overridden_eq_against_null(self, resource_table):
        # The override owns `eq`, so the UNKNOWN-when-NULL rendering cannot be applied.
        with pytest.raises(ValueError, match="missing-attribute"):
            get_query(
                self._null_eq_plan(),
                resource_table,
                {"request.resource.attr.name": resource_table.name},
                operator_override_fns={"eq": lambda left, right: left == right},
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
                                        "operator": "in",
                                        "operands": [
                                            {"variable": "request.resource.attr.name"},
                                            {"value": ["resource1", None]},
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
    """The per-attribute NULL convention (#308), for callers that mix both.

    The call-level option is only the default for undeclared attributes.
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

    # In CEL, null != "x" is TRUE. In SQL it is UNKNOWN, which would drop the row.
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

    # Ordering against null is a CEL error, which denies like UNKNOWN does.
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

    @pytest.mark.parametrize("operator, expected", [("eq", []), ("ne", [1, 2])])
    def test_explicit_null_does_not_enable_string_number_coercion(
        self, resource_table, operator, expected
    ):
        engine = create_engine("sqlite://")
        resource_table.metadata.create_all(engine)
        with engine.begin() as connection:
            connection.execute(
                resource_table.__table__.insert(),
                [{"id": 1, "name": "0"}, {"id": 2, "name": None}],
            )
            query = get_query(
                _conditional_plan(
                    self._comparison(operator, "request.resource.attr.owner", 0)
                ),
                resource_table,
                self._attr_map(resource_table),
                attribute_null_representation=self._declared(),
            )
            assert [row.id for row in connection.execute(query)] == expected
        engine.dispose()

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

    # Declaring one attribute must not change the SQL for any other.
    def test_an_undeclared_attribute_is_untouched(self, resource_table):
        compiled = self._compiled(
            resource_table,
            self._comparison("ne", "request.resource.attr.plain", "x"),
        )
        assert "IS NOT NULL" not in compiled

    # The declaration overrides the call-level default in both directions.
    def test_declaring_omitted_rejects_a_null_operand_under_the_explicit_default(
        self, resource_table
    ):
        with pytest.raises(ValueError, match="null operand"):
            get_query(
                _conditional_plan(
                    self._comparison("in", "request.resource.attr.owner", ["x", None])
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

        # #312: x/0 is +Inf or -Inf depending on the zero's sign, which JSON loses
        # (-0.0 arrives as 0). So the adapter fails closed. 0/0 is NaN either way.
        with pytest.raises(ValueError, match="sign is indeterminate"):
            get_query(infinity_plan, resource_table, attr)

        # PostgreSQL orders NaN above every number, which would turn CEL's FALSE
        # into TRUE. No NaN may reach the bound parameters.
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
    @pytest.mark.parametrize("overrides", [{}, {"eq": None, "add": None}])
    def test_none_override_uses_default_for_nested_expression(
        self, resource_table, conn, overrides
    ):
        plan = _conditional_plan(
            {
                "operator": "eq",
                "operands": [
                    {
                        "expression": {
                            "operator": "add",
                            "operands": [
                                {"variable": "request.resource.attr.aNumber"},
                                {"value": 0},
                            ],
                        }
                    },
                    {"value": 1},
                ],
            }
        )
        query = get_query(
            plan,
            resource_table,
            {"request.resource.attr.aNumber": resource_table.aNumber},
            operator_override_fns=overrides,
        )
        expected = get_query(
            plan,
            resource_table,
            {"request.resource.attr.aNumber": resource_table.aNumber},
        )
        assert conn.execute(query).fetchall() == conn.execute(expected).fetchall()
        assert str(query) == str(expected)

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
                # Rewrites the foreign column to a predicate on the root table.
                "eq": lambda _column, value: resource_table.ownedBy == str(value)
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
        assert all(x.name in {"resource1", "resource2"} for x in res)

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
    """Edge cases of folding `exists`/`all` over a literal value list.

    The planner unrolls up to 10 elements and sends a value-list lambda above that
    (cerbos/cerbos#2570). The `principal/*` corpus cases cover both sides. These
    cover degenerate or malformed plans the corpus cannot.
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
        # The inner lambda rebinds `t`, so only the inner collection operand is
        # substituted, not the inner body.
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
        # A literal list is never a relation, so the fold runs before any
        # override could receive an unresolvable lambda.
        plan = self._value_list_plan("exists", ["string"], self._eq_body())
        query = get_query(
            plan,
            resource_table,
            {"request.resource.attr.aString": resource_table.aString},
            operator_override_fns={"exists": lambda c, v: literal(True)},
        )
        assert [row.name for row in conn.execute(query)] == ["resource1"]


class TestDeclarativeStyles:
    """`get_query` accepts both declarative styles and a Core `Table`.

    A 2.0 `DeclarativeBase` model is not a `DeclarativeMeta` instance, so it takes
    its own arm of `GenericTable` (#181).
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
        # Both the root and the joined model are 2.0-style.
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


class TestPlanOperandBoundary:
    @pytest.mark.parametrize(
        "operand",
        [
            {"value": False, "variable": "request.resource.attr.aBool"},
            {"expression": {"value": True}, "value": None},
            {"operator": "eq", "operands": None},
            {"variable": ["request.resource.attr.aBool"]},
        ],
    )
    def test_malformed_nodes_are_rejected_before_semantic_traversal(self, operand):
        # The planner never sends these. Reject them rather than pick a branch
        # by key order.
        from cerbos_sqlalchemy._plan import parse_operand

        with pytest.raises(ValueError, match="Unrecognised operand shape"):
            parse_operand(operand)


_A_BOOL = {"variable": "request.resource.attr.aBool"}


class TestEmptyBooleanOperators:
    """A zero-operand ``and``/``or`` is refused, never rendered as an empty clause.

    The planner never sends this shape. ``and_()``/``or_()`` with no arguments
    render nothing, ``.where()`` drops it and every row comes back, even for an
    empty ``or``, which CEL evaluates as false. See #498.
    """

    @pytest.mark.parametrize(
        "expression",
        [
            pytest.param({"operator": "or", "operands": []}, id="or"),
            pytest.param({"operator": "and", "operands": []}, id="and"),
            pytest.param(
                {
                    "operator": "and",
                    "operands": [_A_BOOL, {"operator": "or", "operands": []}],
                },
                id="or-under-and",
            ),
            pytest.param(
                {
                    "operator": "or",
                    "operands": [_A_BOOL, {"operator": "and", "operands": []}],
                },
                id="and-under-or",
            ),
            pytest.param(
                {"operator": "not", "operands": [{"operator": "or", "operands": []}]},
                id="or-under-not",
            ),
        ],
    )
    def test_zero_operand_boolean_is_refused(self, resource_table, expression):
        with warnings.catch_warnings():
            # Nor may it warn about an argument-less and_()/or_() on the way.
            warnings.simplefilter("error")
            with pytest.raises(UnsupportedPlanError):
                get_query(
                    _conditional_plan(expression),
                    resource_table,
                    {"request.resource.attr.aBool": resource_table.aBool},
                )
