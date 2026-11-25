# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""The plan's condition tree, decoded from either SDK client's response."""

from dataclasses import dataclass
from typing import Any

from cerbos_duckdb._planner_operators import PLANNER_OPERATORS
from cerbos_duckdb.errors import UnsupportedPlanError


@dataclass(frozen=True)
class Node:
    kind: str
    value: Any = None
    variable: str = ""
    operator: str = ""
    operands: tuple["Node", ...] = ()

    @property
    def is_value(self) -> bool:
        return self.kind == "value"

    @property
    def is_variable(self) -> bool:
        return self.kind == "variable"

    @property
    def is_expr(self) -> bool:
        return self.kind == "expr"


def value_node(v: Any) -> Node:
    return Node("value", value=v)


def variable_node(name: str) -> Node:
    return Node("variable", variable=name)


def expr_node(operator: str, *operands: Node) -> Node:
    return Node("expr", operator=operator, operands=tuple(operands))


def _number(v: Any) -> Any:
    if isinstance(v, bool) or v is None or isinstance(v, str):
        return v
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, list):
        return [_number(x) for x in v]
    if isinstance(v, dict):
        return {k: _number(x) for k, x in v.items()}
    raise UnsupportedPlanError(f"unrecognised value {v!r} in query plan")


_SPELLINGS = {"has_intersection": "hasIntersection"}


def decode(raw: Any) -> Node:
    if isinstance(raw, dict):
        if set(raw) == {"expression"}:
            return decode(raw["expression"])
        if set(raw) == {"value"}:
            return value_node(_number(raw["value"]))
        if set(raw) == {"variable"} and isinstance(raw["variable"], str):
            return variable_node(raw["variable"])
        if isinstance(raw.get("operator"), str) and set(raw) <= {
            "operator",
            "operands",
        }:
            operator = raw["operator"]
            if operator not in PLANNER_OPERATORS:
                raise UnsupportedPlanError(f"unsupported operator: {operator}")
            operands = raw.get("operands", [])
            if not isinstance(operands, list):
                raise UnsupportedPlanError("operands must be a list in query plan")
            decoded = tuple(decode(o) for o in operands)
            # dyn() only relaxes CEL's type check, which the planner has already run.
            if operator == "dyn" and len(decoded) == 1:
                return decoded[0]
            return _rewrite_set_function(
                expr_node(_SPELLINGS.get(operator, operator), *decoded)
            )
    raise UnsupportedPlanError(f"unrecognised operand shape in query plan: {raw!r}")


def _rewrite_set_function(n: Node) -> Node:
    if (
        n.operator not in ("hasAll", "hasAny", "hasOnly", "hasExactly")
        or len(n.operands) != 2
    ):
        return n

    def literal_list(o: Node) -> bool:
        return o.is_value and isinstance(o.value, list)

    lst, literal = n.operands
    first_literal = literal_list(lst)
    if first_literal == literal_list(literal):
        return n
    if first_literal:
        lst, literal = literal, lst
    values = literal.value

    def contains_all() -> Node:
        if not values:
            return expr_node("not", expr_node("hasIntersection", lst, value_node([])))
        tests = [expr_node("hasIntersection", lst, value_node([v])) for v in values]
        return tests[0] if len(tests) == 1 else expr_node("and", *tests)

    operator = n.operator
    if first_literal and operator == "hasAll":
        operator = "hasOnly"
    elif first_literal and operator == "hasOnly":
        operator = "hasAll"
    if operator == "hasAny":
        return expr_node("hasIntersection", lst, literal)
    if operator == "hasAll":
        return contains_all()
    if operator == "hasOnly":
        return expr_node("isSubset", lst, literal)
    return expr_node("and", contains_all(), expr_node("isSubset", lst, literal))
