"""The plan's condition tree: decoding it from the wire, and the queries asked of its shape.

Both SDK clients' spellings are normalised here, once, into three node types, so nothing
downstream has to know which transport a plan came through.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, FrozenSet, Tuple, Union

_INT64_MAX = 2**63 - 1
_INT64_MIN = -(2**63)

#: Operators whose second operand is a lambda that binds an iteration variable.
LAMBDA_BINDING_OPERATORS = frozenset(
    {"exists", "exists_one", "all", "filter", "map", "except"}
)

#: The operators whose collection operand is read from its declared storage rather than from
#: ``attr_map``: the only two a collection's storage decides.
COLLECTION_STORAGE_OPERATORS = frozenset({"size", "index"})


@dataclass(frozen=True)
class Value:
    value: Any


@dataclass(frozen=True)
class Variable:
    name: str


@dataclass(frozen=True)
class Expr:
    operator: str
    operands: Tuple[Operand, ...]


Operand = Union[Value, Variable, Expr]


def _widen_integral_literals(node: Any) -> Any:
    """Rebind wire integers that do not fit int64 as floats.

    A plan literal is a protobuf ``Value.number_value`` — always a double — but
    the JSON path renders an integral double without a fraction, so ``-1e19``
    arrives as the Python int ``-10000000000000000000``. Binding that int is a
    driver error on SQLite (``OverflowError: Python int too large to convert to
    SQLite INTEGER``) rather than the comparison the policy wrote. Every int
    outside int64 is such a double exactly, so widening it back is lossless;
    ints inside int64 are left alone so nothing else this module emits moves.
    """
    if isinstance(node, dict):
        return {key: _widen_integral_literals(value) for key, value in node.items()}
    if isinstance(node, list):
        return [_widen_integral_literals(value) for value in node]
    if (
        isinstance(node, int)
        and not isinstance(node, bool)
        and (node > _INT64_MAX or node < _INT64_MIN)
    ):
        return float(node)
    return node


def parse_operand(node: object) -> Operand:
    """Normalize the HTTP/gRPC wire spellings once, before semantic traversal."""
    if isinstance(node, dict):
        if set(node) == {"expression"}:
            return parse_operand(node["expression"])
        if set(node) == {"value"}:
            return Value(_widen_integral_literals(node["value"]))
        if set(node) == {"variable"} and isinstance(node["variable"], str):
            return Variable(node["variable"])
        if (
            set(node) == {"operator", "operands"}
            and isinstance(node["operator"], str)
            and isinstance(node["operands"], list)
        ):
            return Expr(
                node["operator"],
                tuple(parse_operand(child) for child in node["operands"]),
            )
    raise ValueError(f"Unrecognised operand shape: {node}")


def substitute_lambda_variable(
    operand: Operand, variable_name: str, element: Any
) -> Operand:
    """Substitute a concrete element without crossing a shadowing lambda binding."""
    if isinstance(operand, Value):
        return operand
    if isinstance(operand, Variable):
        name = operand.name
        if name == variable_name:
            return Value(element)
        if name.startswith(f"{variable_name}."):
            current = element
            for segment in name[len(variable_name) + 1 :].split("."):
                if not isinstance(current, dict) or segment not in current:
                    raise ValueError(
                        f'Cannot resolve "{name}": collection element has no field '
                        f'"{segment}"'
                    )
                current = current[segment]
            return Value(current)
        return operand

    operator = operand.operator
    children = operand.operands
    if operator in LAMBDA_BINDING_OPERATORS and len(children) == 2:
        nested_collection, nested_lambda = children
        if (
            isinstance(nested_lambda, Expr)
            and nested_lambda.operator == "lambda"
            and len(nested_lambda.operands) == 2
            and isinstance(nested_lambda.operands[1], Variable)
            and nested_lambda.operands[1].name == variable_name
        ):
            return Expr(
                operator,
                (
                    substitute_lambda_variable(
                        nested_collection, variable_name, element
                    ),
                    nested_lambda,
                ),
            )

    return Expr(
        operator,
        tuple(
            substitute_lambda_variable(child, variable_name, element)
            for child in children
        ),
    )


def declared_collection_name(
    expression: Expr, declared: FrozenSet[str]
) -> Union[str, None]:
    """The attribute ``expression`` reads through its declared storage, if it reads one."""
    if expression.operator not in COLLECTION_STORAGE_OPERATORS:
        return None
    collection = expression.operands[0] if expression.operands else None
    if isinstance(collection, Variable) and collection.name in declared:
        return collection.name
    return None
