# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Default SQL lowering for each plan operator.

To add an operator, write a handler, register it in ``OPERATOR_FNS``, and add it to
the operand tables below if it is unary or order-insensitive.
"""

import math
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any, NoReturn

from sqlalchemy import (
    ARRAY,
    JSON,
    Boolean,
    DateTime,
    Float,
    Integer,
    Numeric,
    String,
    TypeDecorator,
    case,
    cast,
    false,
    func,
    literal,
    literal_column,
    not_,
    null,
    or_,
)
from sqlalchemy.orm import InstrumentedAttribute
from sqlalchemy.sql.expression import ColumnElement

from cerbos_sqlalchemy.errors import UnsupportedPlanError

_LIKE_ESCAPE_CHAR = "\\"
_RFC3339_TIMESTAMP = re.compile(
    r"^((?!0000)\d{4})-(\d{2})-(\d{2})[Tt]"
    r"(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d"
    r"(?:\.(\d{1,9}))?(?:[Zz]|[+-](?:[01]\d|2[0-3]):[0-5]\d)$"
)
_EXCESS_RFC3339_PRECISION = re.compile(r"(\.\d{6})\d+(?=(?:[Zz]|[+-]\d{2}:\d{2})$)")
_MIN_CEL_TIMESTAMP = datetime(1, 1, 1, tzinfo=timezone.utc)
_MAX_CEL_TIMESTAMP = datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)


# -- symbolic values ---------------------------------------------------------------------------


@dataclass(frozen=True)
class IEEEConstant:
    """A non-finite CEL double that must not be bound into dialect SQL."""

    value: float


@dataclass(frozen=True)
class ConditionalValue:
    """A ternary retained until comparison so non-finite arms can be folded."""

    condition: Any
    then_value: Any
    else_value: Any


#: Possibly non-finite numbers, folded by the enclosing comparison instead of bound into SQL.
SYMBOLIC_NUMBERS = (IEEEConstant, ConditionalValue)


@dataclass(frozen=True)
class Hierarchy:
    value: Any
    delimiter: str


def _is_number(value: Any) -> bool:
    """True for an int or float literal, never a bool."""
    return not isinstance(value, bool) and isinstance(value, (int, float))


def scalar_kind(value: Any) -> str:
    if isinstance(value, str) or isinstance(getattr(value, "type", None), String):
        return "string"
    if isinstance(value, bool) or isinstance(getattr(value, "type", None), Boolean):
        return "bool"
    if isinstance(value, (int, float)) or isinstance(
        getattr(value, "type", None), (Integer, Numeric)
    ):
        return "number"
    return ""


def _base_type(type_: Any) -> Any:
    """Unwrap type decorators, including SQLAlchemy 1.4's ``Variant`` from ``with_variant()``."""
    while isinstance(type_, TypeDecorator):
        type_ = type_.impl
    return type_


# -- strings -----------------------------------------------------------------------------------


def _escape_like_literal(needle: str) -> str:
    """Escape LIKE metacharacters in a literal needle.

    ``[`` is escaped too because SQL Server treats it as a character class.
    """
    return (
        needle.replace(_LIKE_ESCAPE_CHAR, _LIKE_ESCAPE_CHAR * 2)
        .replace("%", _LIKE_ESCAPE_CHAR + "%")
        .replace("_", _LIKE_ESCAPE_CHAR + "_")
        .replace("[", _LIKE_ESCAPE_CHAR + "[")
    )


def _escape_like_column(needle: Any) -> Any:
    """Escape LIKE metacharacters in a column-valued needle at query time.

    A NULL needle gives a NULL pattern, so the row is excluded, as CEL denies it.
    """
    escaped = func.replace(needle, _LIKE_ESCAPE_CHAR, _LIKE_ESCAPE_CHAR * 2)
    escaped = func.replace(escaped, "%", _LIKE_ESCAPE_CHAR + "%")
    escaped = func.replace(escaped, "_", _LIKE_ESCAPE_CHAR + "_")
    return func.replace(escaped, "[", _LIKE_ESCAPE_CHAR + "[", type_=String)


def _string_match(receiver: Any, needle: Any, *, prefix: bool, suffix: bool) -> Any:
    """Translate CEL contains/startsWith/endsWith to an escaped LIKE.

    Either side may be a constant or a column. CEL matching is case-sensitive, so
    a case-insensitive LIKE (SQLite by default) must be configured to match.
    """
    if any(scalar_kind(value) not in ("", "string") for value in (receiver, needle)):
        return null()
    if isinstance(receiver, str):
        receiver = literal(receiver, String)
    if isinstance(needle, str):
        pattern: Any = (
            ("%" if prefix else "")
            + _escape_like_literal(needle)
            + ("%" if suffix else "")
        )
    else:
        pattern = _escape_like_column(needle)
        if prefix:
            pattern = literal("%", String) + pattern
        if suffix:
            pattern = pattern + literal("%", String)
    return receiver.like(pattern, escape=_LIKE_ESCAPE_CHAR)


def _string_size(value: Any, _: Any) -> Any:
    if isinstance(_base_type(getattr(value, "type", None)), (JSON, ARRAY)):
        # LENGTH() would measure the text, not count elements. Counting depends on
        # the storage, so the caller must declare it.
        raise UnsupportedPlanError(
            "size() over a collection-typed column needs its storage declared: map the "
            'attribute in collection_columns with storage "json" or "pgArray"'
        )
    kind = scalar_kind(value)
    return null() if kind and kind != "string" else func.length(value)


# -- casts -------------------------------------------------------------------------------------


def _reject_numeric_cast(operator: str) -> NoReturn:
    """Fail closed on CEL's int()/double().

    CAST reads a numeric prefix (``'100%_done'`` is 100 on SQLite) where CEL denies,
    and PostgreSQL and MySQL round where CEL truncates. See #311.
    """
    raise UnsupportedPlanError(
        f"'{operator}()' cannot be lowered to SQL CAST: CAST reads a numeric prefix "
        "where CEL requires the whole string and raises otherwise, and PostgreSQL and "
        "MySQL round where CEL truncates toward zero"
    )


def _string_cast(c: Any) -> Any:
    """CEL's ``string()``.

    Numbers use CAST, which formats the shortest round-trip decimal on SQLite,
    PostgreSQL 12+ and MySQL. Booleans use a CASE because SQLite and MySQL cast
    them to ``'1'``/``'0'`` (#376, #418). A NULL boolean must stay NULL, since CEL
    denies it. On MySQL the literals use the connection collation, which must be
    case-sensitive.
    """
    if isinstance(_base_type(getattr(c, "type", None)), Boolean):
        return case(
            (c.is_(None), null()),
            (c, literal_column("'true'", String)),
            else_=literal_column("'false'", String),
        )
    return cast(c, String)


# -- arithmetic --------------------------------------------------------------------------------


def _require_signed_zero(denominator: Any) -> None:
    """Reject an integer zero denominator, whose sign is unknown.

    ``n / -0.0`` and ``n / 0.0`` are opposite infinities. Over HTTP, ``-0.0``
    arrives as ``-0``, which JSON decodes to the int ``0``, losing the sign.
    gRPC keeps it as a float. See #312.
    """
    if isinstance(denominator, bool) or not isinstance(denominator, int):
        return
    if denominator != 0:
        return
    raise UnsupportedPlanError(
        "division by a constant zero whose sign is indeterminate: the HTTP transport "
        "renders -0.0 as `-0`, which JSON decodes to the integer 0, so the adapter "
        "cannot tell +Infinity from -Infinity. Use the gRPC client, which preserves "
        "the sign bit, or avoid a literal zero denominator"
    )


def _float_div(c: Any, v: Any) -> Any:
    """Divide as doubles, as CEL does. SQLite and PostgreSQL would truncate integer ``/``."""
    if _is_number(c) and _is_number(v):
        numerator = float(c)
        denominator = float(v)
        if denominator == 0.0:
            if numerator == 0.0 or math.isnan(numerator):
                # NaN has no sign, so an indeterminate zero cannot change the answer.
                return IEEEConstant(math.nan)
            _require_signed_zero(v)
            sign = math.copysign(1.0, numerator) * math.copysign(1.0, denominator)
            return IEEEConstant(math.copysign(math.inf, sign))
        return numerator / denominator

    numerator = float(c) if _is_number(c) else cast(c, Float)
    denominator = float(v) if _is_number(v) else cast(v, Float)

    # In CEL, x/0 is NaN or a signed infinity, not an error. SQL NULL would be
    # wrong (`NaN != 1.0` is TRUE), so keep these arms symbolic for the enclosing
    # comparison to fold. A NULL operand still makes the whole CASE NULL.
    # NULLIF stops dialects that evaluate CASE arms eagerly from failing on /0.
    # A constant denominator's sign is applied. SQL cannot read a column's
    # zero sign, so a column is assumed +0.0. See #312.
    denominator_sign = 1.0
    if _is_number(v):
        _require_signed_zero(v)
        denominator_sign = math.copysign(1.0, float(v))

    return ConditionalValue(
        condition=denominator == 0.0,
        then_value=ConditionalValue(
            condition=numerator == 0.0,
            then_value=IEEEConstant(math.nan),
            else_value=ConditionalValue(
                condition=numerator > 0.0,
                then_value=IEEEConstant(math.copysign(math.inf, denominator_sign)),
                else_value=IEEEConstant(math.copysign(math.inf, -denominator_sign)),
            ),
        ),
        else_value=numerator / func.nullif(denominator, 0.0),
    )


def arith_over_conditional(op_fn: Callable[[Any, Any], Any], left: Any, right: Any):
    """Distribute a binary arithmetic operator across a retained ternary.

    Keeps a NaN arm symbolic, since ``NULL + 1 != 2.0`` is UNKNOWN where CEL's
    ``NaN + 1 != 2.0`` is TRUE. See #312.
    """
    if isinstance(left, ConditionalValue):
        return ConditionalValue(
            condition=left.condition,
            then_value=arith_over_conditional(op_fn, left.then_value, right),
            else_value=arith_over_conditional(op_fn, left.else_value, right),
        )
    if isinstance(right, ConditionalValue):
        return ConditionalValue(
            condition=right.condition,
            then_value=arith_over_conditional(op_fn, left, right.then_value),
            else_value=arith_over_conditional(op_fn, left, right.else_value),
        )
    if isinstance(left, IEEEConstant) or isinstance(right, IEEEConstant):
        left_value = left.value if isinstance(left, IEEEConstant) else left
        right_value = right.value if isinstance(right, IEEEConstant) else right
        if not (_is_number(left_value) and _is_number(right_value)):
            raise UnsupportedPlanError(
                "arithmetic combines a non-finite value with a column, which SQL "
                "cannot carry"
            )
        # A non-finite operand keeps the result non-finite under + - * /.
        result = op_fn(float(left_value), float(right_value))
        if isinstance(result, IEEEConstant):
            return result
        return IEEEConstant(float(result))
    return op_fn(left, right)


# -- comparisons -------------------------------------------------------------------------------


def _apply_comparison(operator: str, left: Any, right: Any) -> Any:
    if operator == "eq":
        return left == right
    if operator == "ne":
        return left != right
    if operator == "lt":
        return left < right
    if operator == "gt":
        return left > right
    if operator == "le":
        return left <= right
    if operator == "ge":
        return left >= right
    raise KeyError(operator)


def _compare_leaf(operator: str, left: Any, right: Any) -> Any:
    left_kind, right_kind = scalar_kind(left), scalar_kind(right)
    if left_kind and right_kind and left_kind != right_kind:
        result = literal(operator == "ne") if operator in ("eq", "ne") else null()
        for value in (left, right):
            if hasattr(value, "is_"):
                result = case((value.isnot(None), result))
        return result
    left_is_ieee = isinstance(left, IEEEConstant)
    right_is_ieee = isinstance(right, IEEEConstant)
    if left_is_ieee or right_is_ieee:
        left_value = left.value if left_is_ieee else left
        right_value = right.value if right_is_ieee else right
        left_is_nan = left_is_ieee and math.isnan(left_value)
        right_is_nan = right_is_ieee and math.isnan(right_value)
        if left_is_nan or right_is_nan:
            # IEEE: NaN is unequal and unordered, even under NOT (as in Cerbos 0.55).
            other = right_value if left_is_nan else left_value
            if isinstance(other, (int, float)):
                return operator == "ne"
            if hasattr(other, "is_"):
                # NULL stays UNKNOWN, as CEL errors on a missing attribute.
                return case(
                    (other.is_(None), null()),
                    else_=(operator == "ne"),
                )
            raise UnsupportedPlanError(
                "NaN can only be compared with numeric constants or SQLAlchemy "
                "expressions"
            )
        if not isinstance(left_value, (int, float)) or not isinstance(
            right_value, (int, float)
        ):
            raise UnsupportedPlanError(
                "Non-finite numeric constants can only be compared with numeric "
                "constants"
            )
        return _apply_comparison(operator, left_value, right_value)

    return _apply_comparison(operator, left, right)


def _compare(operator: str, left: Any, right: Any) -> Any:
    """Compare values without leaking PostgreSQL's non-IEEE NaN ordering."""
    if isinstance(left, ConditionalValue):
        return case(
            (left.condition, _compare(operator, left.then_value, right)),
            (not_(left.condition), _compare(operator, left.else_value, right)),
        )
    if isinstance(right, ConditionalValue):
        return case(
            (right.condition, _compare(operator, left, right.then_value)),
            (not_(right.condition), _compare(operator, left, right.else_value)),
        )
    return _compare_leaf(operator, left, right)


def _comparison(operator: str) -> Callable[[Any, Any], Any]:
    return lambda left, right: _compare(operator, left, right)


def _in(c: Any, values: Any) -> Any:
    """CEL membership, including explicit-null list elements."""
    members = values if isinstance(values, list) else [values]
    non_nulls = [member for member in members if member is not None]
    # `5 in ["5"]` is false in CEL. Drop members of another type rather than let
    # the store coerce '5' to 5.
    column_kind = scalar_kind(c)
    comparable = [
        member
        for member in non_nulls
        if not column_kind or scalar_kind(member) in ("", column_kind)
    ]
    predicates = []
    if comparable:
        predicates.append(c.in_(comparable))
    elif non_nulls and hasattr(c, "isnot"):
        # All members dropped: FALSE if present, NULL if absent, like `c IN (...)`.
        predicates.append(case((c.isnot(None), false())))
    if len(non_nulls) != len(members):
        predicates.append(c.is_(None))
    if not predicates:
        return false()
    return or_(*predicates)


# -- timestamps --------------------------------------------------------------------------------


def _parse_rfc3339(value: str) -> datetime:
    match = _RFC3339_TIMESTAMP.fullmatch(value)
    if match is None:
        raise UnsupportedPlanError(f"Invalid RFC-3339 timestamp literal: {value}")
    digits = match.group(4) or ""
    if len(digits) > 6 and any(d != "0" for d in digits[6:]):
        raise UnsupportedPlanError(
            f"Timestamp literal precision exceeds the exact microsecond range: {value}"
        )
    try:
        normalized = _EXCESS_RFC3339_PRECISION.sub(r"\1", value)
        normalized = normalized.replace("t", "T")
        normalized = normalized.replace("z", "+00:00").replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise UnsupportedPlanError(
            f"Invalid RFC-3339 timestamp literal: {value}"
        ) from exc


def _timestamp(value: Any, _: Any) -> Any:
    """Unwrap a temporal column or parse an RFC-3339 planner constant."""
    if isinstance(getattr(value, "type", None), DateTime):
        return value
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        parsed = _parse_rfc3339(value)
    else:
        raise UnsupportedPlanError(
            "timestamp() requires an RFC-3339 literal or a SQLAlchemy DateTime column"
        )
    if parsed.tzinfo is None:
        raise UnsupportedPlanError(f"Timestamp literal must include an offset: {value}")
    try:
        normalized = parsed.astimezone(timezone.utc)
    except (OverflowError, ValueError) as exc:
        raise UnsupportedPlanError(
            f"Timestamp literal is outside CEL's supported instant range: {value}"
        ) from exc
    if normalized < _MIN_CEL_TIMESTAMP or normalized > _MAX_CEL_TIMESTAMP:
        raise UnsupportedPlanError(
            f"Timestamp literal is outside CEL's supported instant range: {value}"
        )
    return normalized


# -- hierarchies -------------------------------------------------------------------------------


def _hierarchy(value: Any, delimiter: Any) -> Hierarchy:
    delimiter = "." if delimiter is None else delimiter
    if not isinstance(delimiter, str) or not delimiter:
        raise UnsupportedPlanError("hierarchy() delimiter must be a non-empty string")
    return Hierarchy(value, delimiter)


def _matching_hierarchies(left: Any, right: Any) -> tuple[Hierarchy, Hierarchy]:
    if not isinstance(left, Hierarchy) or not isinstance(right, Hierarchy):
        raise UnsupportedPlanError("Hierarchy operator requires hierarchy() operands")
    if left.delimiter != right.delimiter:
        raise UnsupportedPlanError("Hierarchy operands must use the same delimiter")
    return left, right


def _ancestor_of(left: Any, right: Any) -> Any:
    ancestor, descendent = _matching_hierarchies(left, right)
    ancestor_value = ancestor.value
    descendent_value = descendent.value
    delimiter = ancestor.delimiter

    if isinstance(ancestor_value, str) and isinstance(descendent_value, str):
        return descendent_value.startswith(ancestor_value + delimiter)
    if isinstance(descendent_value, str):
        parts = descendent_value.split(delimiter)
        prefixes = [delimiter.join(parts[:i]) for i in range(1, len(parts))]
        return ancestor_value.in_(prefixes)
    if isinstance(ancestor_value, str):
        return _string_match(
            descendent_value,
            ancestor_value + delimiter,
            prefix=False,
            suffix=True,
        )
    raise UnsupportedPlanError(
        "Hierarchy comparison between two columns is not supported"
    )


def _descendent_of(left: Any, right: Any) -> Any:
    return _ancestor_of(right, left)


def _hierarchy_overlaps(left: Any, right: Any) -> Any:
    left_hierarchy, right_hierarchy = _matching_hierarchies(left, right)
    left_value = left_hierarchy.value
    right_value = right_hierarchy.value
    if isinstance(left_value, str) and isinstance(right_value, str):
        return (
            left_value == right_value
            or _ancestor_of(left_hierarchy, right_hierarchy)
            or _ancestor_of(right_hierarchy, left_hierarchy)
        )
    return or_(
        left_value == right_value,
        _ancestor_of(left_hierarchy, right_hierarchy),
        _ancestor_of(right_hierarchy, left_hierarchy),
    )


# -- the registry ------------------------------------------------------------------------------

# Read-only so no caller can change the defaults process-wide. Override per call.
OPERATOR_FNS = MappingProxyType(
    {
        "eq": _comparison("eq"),
        "ne": _comparison("ne"),
        "lt": _comparison("lt"),
        "gt": _comparison("gt"),
        "le": _comparison("le"),
        "ge": _comparison("ge"),
        "in": _in,
        # Arithmetic returns values, composed inside comparisons.
        "add": lambda c, v: c + v,
        "sub": lambda c, v: c - v,
        "mult": lambda c, v: c * v,
        "div": _float_div,
        "mod": lambda c, v: c % v,
        # Receiver-style string matches. Operands arrive receiver first.
        "contains": lambda c, v: _string_match(c, v, prefix=True, suffix=True),
        "startsWith": lambda c, v: _string_match(c, v, prefix=False, suffix=True),
        "endsWith": lambda c, v: _string_match(c, v, prefix=True, suffix=False),
        # int() and double() always throw. See _reject_numeric_cast.
        "string": lambda c, _: _string_cast(c),
        "double": lambda *_: _reject_numeric_cast("double"),
        "int": lambda *_: _reject_numeric_cast("int"),
        # String size only. Declared collections never reach this handler.
        "size": _string_size,
        "timestamp": _timestamp,
        "hierarchy": _hierarchy,
        "ancestorOf": _ancestor_of,
        "descendentOf": _descendent_of,
        "overlaps": _hierarchy_overlaps,
    }
)

#: Single-operand operators, called as ``handler(operand, None)``.
UNARY_VALUE_OPERATORS = frozenset({"string", "double", "int", "size", "timestamp"})

#: Operators to mirror when a value comes first: `1 < R.attr.x` becomes `x > 1`. See #257.
MIRRORED_OPERATORS: dict[str, str] = {"lt": "gt", "gt": "lt", "le": "ge", "ge": "le"}

#: Operators normalised to column-first. All others keep source order, or
#: `"const".contains(R.attr.x)` would swap haystack and needle.
ORDER_INSENSITIVE_OPERATORS = frozenset({"eq", "ne", "in", "hasIntersection"})

#: Macros that fold into a boolean over their per-element bodies. Others fail closed.
FOLDABLE_COLLECTION_OPERATORS = frozenset({"exists", "all"})

# What a default handler can lower: SQL, plan literals, and this module's symbolic
# values. An override's intermediate value (e.g. a deferred tuple) must throw here,
# or Python's `==` turns it into a bare False that silently filters. See #387.
_LOWERABLE_OPERAND_TYPES = (
    ColumnElement,
    InstrumentedAttribute,
    IEEEConstant,
    ConditionalValue,
    Hierarchy,
    str,
    bool,
    int,
    float,
    list,
    dict,
    datetime,
    type(None),
)


def require_lowerable(operator: str, operand: Any) -> None:
    if not isinstance(operand, _LOWERABLE_OPERAND_TYPES):
        raise UnsupportedPlanError(
            f"`{operator}` received an operand of type "
            f"{type(operand).__name__!r}, which is not a SQL expression or a plan "
            "literal: an operator override returning an intermediate value must be "
            "consumed by an enclosing override, and no default handler can lower one"
        )
