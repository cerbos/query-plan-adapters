# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""``get_filter`` contracts for caller arguments the corpus cannot vary.

The harness uses one schema form, one mapping and fixed column types, so the other forms a
caller may pass are tested here. Policy-reachable shapes belong in the corpus.
"""

import polars as pl
import pytest
from cerbos.sdk.model import (
    PlanResourcesFilter,
    PlanResourcesFilterKind,
    PlanResourcesResponse,
)

from cerbos_polars import UnsupportedPlanError, get_filter

FRAME = pl.DataFrame(
    {
        "id": ["1", "2", "3"],
        "owner_id": ["alice", "bob", None],
        "org": [{"region": "emea"}, {"region": None}, None],
        "tags": [["a", "b"], ["c"], []],
    }
)


def _plan(expression=None, kind=PlanResourcesFilterKind.CONDITIONAL):
    body = {"kind": kind}
    if expression is not None:
        body["condition"] = {"expression": expression}
    return PlanResourcesResponse(
        filter=PlanResourcesFilter.from_dict(body),
        request_id="1",
        action="view",
        resource_kind="document",
        policy_version="default",
    )


def _op(operator, *operands):
    return {"operator": operator, "operands": list(operands)}


def _attr(name):
    return {"variable": f"request.resource.attr.{name}"}


def _value(value):
    return {"value": value}


def _ids(frame, expr):
    return sorted(frame.filter(expr)["id"].to_list())


OWNER_IS_ALICE = _op("eq", _attr("owner"), _value("alice"))
OWNER_MAP = {"request.resource.attr.owner": "owner_id"}


@pytest.mark.parametrize(
    "schema",
    [FRAME, FRAME.lazy(), FRAME.schema, dict(FRAME.schema)],
    ids=["DataFrame", "LazyFrame", "Schema", "Mapping"],
)
def test_every_schema_form_filters_the_same_rows(schema):
    assert _ids(FRAME, get_filter(_plan(OWNER_IS_ALICE), schema, OWNER_MAP)) == ["1"]


def test_a_schema_of_another_type_is_rejected():
    with pytest.raises(TypeError):
        get_filter(_plan(OWNER_IS_ALICE), ["owner_id"], OWNER_MAP)


def test_an_attr_map_value_may_be_an_expression():
    attr_map = {"request.resource.attr.owner": pl.col("owner_id").str.to_lowercase()}
    assert _ids(FRAME, get_filter(_plan(OWNER_IS_ALICE), FRAME, attr_map)) == ["1"]


def test_a_mapped_prefix_reads_struct_fields_of_its_expression():
    plan = _plan(_op("eq", _attr("department.region"), _value("emea")))
    attr_map = {"request.resource.attr.department": pl.col("org")}
    assert _ids(FRAME, get_filter(plan, FRAME, attr_map)) == ["1"]


def test_an_attribute_with_no_column_raises_key_error():
    with pytest.raises(KeyError):
        get_filter(_plan(OWNER_IS_ALICE), FRAME)
    with pytest.raises(KeyError):
        get_filter(
            _plan(OWNER_IS_ALICE), FRAME, {"request.resource.attr.owner": "nope"}
        )


def test_an_attr_map_key_must_be_a_full_attribute_reference():
    with pytest.raises(ValueError):
        get_filter(_plan(OWNER_IS_ALICE), FRAME, {"owner": "owner_id"})


class TestNullRepresentation:
    """The plan's ``x != "alice"`` is the same whichever way the caller sends a null."""

    OWNER_IS_NOT_ALICE = _op("ne", _attr("owner"), _value("alice"))

    def test_omitted_is_the_default_and_denies_the_null_row(self):
        expr = get_filter(_plan(self.OWNER_IS_NOT_ALICE), FRAME, OWNER_MAP)
        assert _ids(FRAME, expr) == ["2"]

    def test_explicit_per_attribute_grants_the_null_row(self):
        expr = get_filter(
            _plan(self.OWNER_IS_NOT_ALICE),
            FRAME,
            OWNER_MAP,
            attribute_null_representation={"request.resource.attr.owner": "explicit"},
        )
        assert _ids(FRAME, expr) == ["2", "3"]

    def test_explicit_for_every_attribute(self):
        expr = get_filter(
            _plan(self.OWNER_IS_NOT_ALICE),
            FRAME,
            OWNER_MAP,
            null_attribute_representation="explicit",
        )
        assert _ids(FRAME, expr) == ["2", "3"]

    def test_an_explicit_struct_field_still_errors_under_a_null_struct(self):
        plan = _plan(_op("ne", _attr("org.region"), _value("apac")))
        expr = get_filter(plan, FRAME, null_attribute_representation="explicit")
        assert _ids(FRAME, expr) == ["1", "2"]

    def test_an_unknown_representation_is_rejected(self):
        with pytest.raises(ValueError):
            get_filter(
                _plan(OWNER_IS_ALICE),
                FRAME,
                OWNER_MAP,
                null_attribute_representation="none",  # type: ignore[arg-type]
            )
        with pytest.raises(ValueError):
            get_filter(
                _plan(OWNER_IS_ALICE),
                FRAME,
                OWNER_MAP,
                attribute_null_representation={"request.resource.attr.owner": "none"},  # type: ignore[dict-item]
            )


@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        (PlanResourcesFilterKind.ALWAYS_ALLOWED, ["1", "2", "3"]),
        (PlanResourcesFilterKind.ALWAYS_DENIED, []),
    ],
)
def test_unconditional_plans_compose_with_a_caller_predicate(kind, expected):
    expr = get_filter(_plan(kind=kind), FRAME)
    combined = FRAME.lazy().filter(expr & pl.col("id").is_not_null()).collect()
    assert sorted(combined["id"].to_list()) == expected


def test_a_plan_kind_the_adapter_does_not_know_is_refused():
    plan = _plan(OWNER_IS_ALICE)
    plan.filter.kind = "KIND_SOMETHING_NEW"  # type: ignore[assignment]
    with pytest.raises(UnsupportedPlanError):
        get_filter(plan, FRAME, OWNER_MAP)


class TestColumnTypes:
    """Dtypes a caller may choose that read as CEL strings and lists."""

    def test_enum_and_categorical_compare_as_strings(self):
        frame = pl.DataFrame(
            {
                "id": ["1", "2"],
                "level": pl.Series(["a", "b"], dtype=pl.Enum(["b", "a"])),
                "team": pl.Series(["x", "y"], dtype=pl.Categorical()),
            }
        )
        later = _plan(_op("gt", _attr("level"), _value("a")))
        assert _ids(frame, get_filter(later, frame)) == ["2"]
        prefix = _plan(_op("startsWith", _attr("team"), _value("x")))
        assert _ids(frame, get_filter(prefix, frame)) == ["1"]

    def test_an_array_column_is_a_list(self):
        frame = pl.DataFrame(
            {
                "id": ["1", "2"],
                "pair": pl.Series(
                    [["a", "b"], ["c", "d"]], dtype=pl.Array(pl.String, 2)
                ),
            }
        )
        plan = _plan(_op("in", _value("b"), _attr("pair")))
        assert _ids(frame, get_filter(plan, frame)) == ["1"]
