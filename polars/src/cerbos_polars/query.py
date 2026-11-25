# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Mapping
from typing import Any, Literal

import polars as pl
from cerbos.engine.v1 import engine_pb2
from cerbos.response.v1 import response_pb2
from cerbos.sdk.model import PlanResourcesFilterKind, PlanResourcesResponse

from cerbos_polars._plan import plan_condition
from cerbos_polars._translator import RESOURCE_ATTR, RESOURCE_ID, Settings, Translator
from cerbos_polars.errors import UnsupportedPlanError

NullRepresentation = Literal["omitted", "explicit"]

_DENY_KINDS = frozenset(
    [
        PlanResourcesFilterKind.ALWAYS_DENIED,
        engine_pb2.PlanResourcesFilter.KIND_ALWAYS_DENIED,
    ]
)
_ALLOW_KINDS = frozenset(
    [
        PlanResourcesFilterKind.ALWAYS_ALLOWED,
        engine_pb2.PlanResourcesFilter.KIND_ALWAYS_ALLOWED,
    ]
)
_CONDITIONAL_KINDS = frozenset(
    [
        PlanResourcesFilterKind.CONDITIONAL,
        engine_pb2.PlanResourcesFilter.KIND_CONDITIONAL,
    ]
)


def _schema(source: Any) -> pl.Schema:
    if isinstance(source, pl.Schema):
        return source
    if isinstance(source, pl.LazyFrame):
        return source.collect_schema()
    if isinstance(source, pl.DataFrame):
        return source.schema
    if isinstance(source, Mapping):
        return pl.Schema(source)
    raise TypeError(
        "schema must be a polars Schema, DataFrame, LazyFrame or a mapping of column "
        f"names to dtypes, got {type(source).__name__}"
    )


def _attribute_key(name: str) -> str:
    if name.startswith(RESOURCE_ATTR) or name == RESOURCE_ID:
        return name
    raise ValueError(
        f"{name!r} is not a resource attribute reference: write it in full, as "
        f"'{RESOURCE_ATTR}<name>'"
    )


def get_filter(
    query_plan: PlanResourcesResponse | response_pb2.PlanResourcesResponse,
    schema: pl.Schema | pl.DataFrame | pl.LazyFrame | Mapping[str, Any],
    attr_map: Mapping[str, pl.Expr | str] | None = None,
    *,
    null_attribute_representation: NullRepresentation = "omitted",
    attribute_null_representation: Mapping[str, NullRepresentation] | None = None,
) -> pl.Expr:
    """A Polars expression that keeps exactly the rows the query plan allows.

    ``schema`` is the frame the expression will filter, or its schema: the translation
    reads column types from it. ``attr_map`` maps an attribute reference
    (``request.resource.attr.department``) to a column name or any expression over the
    frame; an attribute it leaves out is read from the column of the same name, and a
    dotted one through struct fields. Always returns an expression, ``pl.lit(False)`` for
    an always-denied plan, and raises ``UnsupportedPlanError`` for a condition it cannot
    express exactly.
    """
    if null_attribute_representation not in ("omitted", "explicit"):
        raise ValueError(
            "null_attribute_representation must be 'omitted' or 'explicit', got "
            f"{null_attribute_representation!r}"
        )
    resolved_schema = _schema(schema)
    columns: dict[str, pl.Expr] = {}
    for key, value in (attr_map or {}).items():
        columns[_attribute_key(key)] = (
            pl.col(value) if isinstance(value, str) else value
        )
    per_attribute = {
        _attribute_key(key): value
        for key, value in (attribute_null_representation or {}).items()
    }
    for key, value in per_attribute.items():
        if value not in ("omitted", "explicit"):
            raise ValueError(
                f"attribute_null_representation[{key!r}] must be 'omitted' or "
                f"'explicit', got {value!r}"
            )

    if query_plan.filter is None or query_plan.filter.kind in _DENY_KINDS:
        return pl.lit(False)
    if query_plan.filter.kind in _ALLOW_KINDS:
        return pl.lit(True)
    if query_plan.filter.kind not in _CONDITIONAL_KINDS:
        raise UnsupportedPlanError(f"Unknown plan kind: {query_plan.filter.kind!r}")

    condition = plan_condition(query_plan)
    explicit = _ExplicitNames(null_attribute_representation, per_attribute)
    translator = Translator(
        Settings(schema=resolved_schema, attr_map=columns, explicit=explicit)
    )
    return translator.predicate(condition).fill_null(False)


class _ExplicitNames:
    """The attributes whose NULL reaches ``check()`` as an explicit null."""

    def __init__(self, default: str, per_attribute: Mapping[str, str]) -> None:
        self._default = default
        self._per_attribute = per_attribute

    def __contains__(self, name: object) -> bool:
        if not isinstance(name, str):
            return False
        return self._per_attribute.get(name, self._default) == "explicit"
