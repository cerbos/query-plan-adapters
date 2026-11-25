# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Translates a Cerbos query plan into a DuckDB ``WHERE`` fragment and its parameters."""

import enum
import importlib.metadata
from dataclasses import dataclass, field
from typing import Any

from cerbos.engine.v1 import engine_pb2
from cerbos.response.v1 import response_pb2
from cerbos.sdk.model import PlanResourcesFilterKind, PlanResourcesResponse
from google.protobuf.json_format import MessageToDict

from cerbos_duckdb._plan import decode
from cerbos_duckdb._render import Renderer
from cerbos_duckdb._translate import Builder
from cerbos_duckdb.errors import UnsupportedPlanError
from cerbos_duckdb.mapping import (
    Entry,
    Hop,
    Join,
    ListOf,
    Mapper,
    NullConvention,
    NullRepresentation,
    Restriction,
    RestrictOp,
    ValueType,
)

__version__ = importlib.metadata.version(__package__ or __name__)


class PlanKind(enum.Enum):
    ALWAYS_DENIED = "always_denied"
    ALWAYS_ALLOWED = "always_allowed"
    CONDITIONAL = "conditional"


@dataclass(frozen=True)
class Result:
    """A translated plan.

    ``sql`` is a boolean expression to put after ``WHERE``, for every kind: ``FALSE`` when the plan
    denies every row and ``TRUE`` when it allows every row. ``params`` binds its ``?`` placeholders,
    in order. ``sql`` is self-delimiting, so it joins your own predicates with ``AND`` as it stands.
    """

    kind: PlanKind
    sql: str
    params: list[Any] = field(default_factory=list)


_DENIED_KINDS = frozenset(
    {
        PlanResourcesFilterKind.ALWAYS_DENIED,
        engine_pb2.PlanResourcesFilter.KIND_ALWAYS_DENIED,
    }
)
_ALLOWED_KINDS = frozenset(
    {
        PlanResourcesFilterKind.ALWAYS_ALLOWED,
        engine_pb2.PlanResourcesFilter.KIND_ALWAYS_ALLOWED,
    }
)
_CONDITIONAL_KINDS = frozenset(
    {
        PlanResourcesFilterKind.CONDITIONAL,
        engine_pb2.PlanResourcesFilter.KIND_CONDITIONAL,
    }
)


def translate(
    plan: PlanResourcesResponse | response_pb2.PlanResourcesResponse,
    table: str,
    mapper: Mapper,
    *,
    null_representation: NullRepresentation = NullRepresentation.OMITTED,
) -> Result:
    """Translate a ``PlanResources`` response from either Cerbos SDK client.

    ``table`` is the name or alias that qualifies the resource row's columns in your query.
    ``mapper`` resolves each attribute reference in the plan, such as
    ``request.resource.attr.status``, to an ``Entry``. An unmapped reference is refused.

    Raises:
        UnsupportedPlanError: the plan holds a shape DuckDB cannot be asked exactly.
    """
    if not table:
        raise ValueError("table is required")
    plan_filter = plan.filter
    if plan_filter is None or plan_filter.kind in _DENIED_KINDS:
        return Result(PlanKind.ALWAYS_DENIED, "FALSE")
    if plan_filter.kind in _ALLOWED_KINDS:
        return Result(PlanKind.ALWAYS_ALLOWED, "TRUE")
    if plan_filter.kind not in _CONDITIONAL_KINDS:
        raise UnsupportedPlanError(f"unrecognised filter kind {plan_filter.kind!r}")

    if isinstance(plan, response_pb2.PlanResourcesResponse):
        raw = MessageToDict(plan_filter.condition)
    else:
        raw = plan_filter.condition.to_dict()
    expr = Builder(table, null_representation).build(decode(raw), mapper)
    renderer = Renderer()
    sql = renderer.render(expr)
    return Result(PlanKind.CONDITIONAL, sql, renderer.params)


__all__ = [
    "Entry",
    "Hop",
    "Join",
    "ListOf",
    "Mapper",
    "NullConvention",
    "NullRepresentation",
    "PlanKind",
    "RestrictOp",
    "Restriction",
    "Result",
    "UnsupportedPlanError",
    "ValueType",
    "translate",
]
