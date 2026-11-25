# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""The plan's condition tree, decoded from either SDK's wire format into three node types."""

from dataclasses import dataclass
from typing import Any

from google.protobuf.json_format import MessageToDict

from cerbos_polars._planner_operators import PLANNER_OPERATORS
from cerbos_polars.errors import UnsupportedPlanError


@dataclass(frozen=True)
class Value:
    value: Any


@dataclass(frozen=True)
class Variable:
    name: str


@dataclass(frozen=True)
class Expr:
    operator: str
    operands: tuple["Operand", ...]


Operand = Value | Variable | Expr


def parse_operand(node: object) -> Operand:
    if isinstance(node, dict):
        if set(node) == {"expression"}:
            return parse_operand(node["expression"])
        if set(node) == {"value"}:
            return Value(node["value"])
        if set(node) == {"variable"} and isinstance(node["variable"], str):
            return Variable(node["variable"])
        if (
            set(node) == {"operator", "operands"}
            and isinstance(node["operator"], str)
            and isinstance(node["operands"], list)
        ):
            if node["operator"] not in PLANNER_OPERATORS:
                raise UnsupportedPlanError(f"Unsupported operator: {node['operator']}")
            return Expr(
                node["operator"],
                tuple(parse_operand(child) for child in node["operands"]),
            )
        if set(node) == {"operator"} and node["operator"] in PLANNER_OPERATORS:
            return Expr(node["operator"], ())
    raise UnsupportedPlanError(f"Unrecognised operand shape: {node}")


def plan_condition(query_plan: Any) -> Operand:
    condition = query_plan.filter.condition
    if hasattr(condition, "DESCRIPTOR"):
        return parse_operand(MessageToDict(condition))
    return parse_operand(condition.to_dict())
