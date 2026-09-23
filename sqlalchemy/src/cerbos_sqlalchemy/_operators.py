"""The default lowering of every plan operator the adapter translates without help.

``OPERATOR_FNS`` at the bottom is the registry, and the operator tables beside it say how the
translator hands each operator its operands. Adding an operator is an edit to this module
alone: a handler, a registry entry, and -- if it takes one operand, or its operands may be
swapped -- an entry in the matching table.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any, Callable, Dict, NoReturn, Tuple

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


#: The values that carry a possibly non-finite number symbolically, to be folded by the
#: enclosing comparison rather than bound into SQL.
SYMBOLIC_NUMBERS = (IEEEConstant, ConditionalValue)


@dataclass(frozen=True)
class Hierarchy:
    value: Any
    delimiter: str


def _is_number(value: Any) -> bool:
    """A numeric plan literal: an int or a float, and never a bool."""
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
    """``type_`` with decorators unwrapped -- including SQLAlchemy 1.4's ``with_variant()``,
    which returns a ``Variant`` decorator where 2.x returns a copy of the base type."""
    while isinstance(type_, TypeDecorator):
        type_ = type_.impl
    return type_


# -- strings -----------------------------------------------------------------------------------


def _escape_like_literal(needle: str) -> str:
    """Escape LIKE metacharacters in a literal needle.

    ``[`` is a character-class opener on SQL Server even with an ESCAPE clause,
    so escape it alongside the portable ``%``/``_`` wildcards.
    """
    return (
        needle.replace(_LIKE_ESCAPE_CHAR, _LIKE_ESCAPE_CHAR * 2)
        .replace("%", _LIKE_ESCAPE_CHAR + "%")
        .replace("_", _LIKE_ESCAPE_CHAR + "_")
        .replace("[", _LIKE_ESCAPE_CHAR + "[")
    )


def _escape_like_column(needle: Any) -> Any:
    """Escape LIKE metacharacters in a column-valued needle at query time.

    A NULL needle propagates through REPLACE to a NULL pattern, so the LIKE
    stays UNKNOWN and the row is excluded — matching CEL's missing-attribute
    error (deny) for the same row.
    """
    escaped = func.replace(needle, _LIKE_ESCAPE_CHAR, _LIKE_ESCAPE_CHAR * 2)
    escaped = func.replace(escaped, "%", _LIKE_ESCAPE_CHAR + "%")
    escaped = func.replace(escaped, "_", _LIKE_ESCAPE_CHAR + "_")
    return func.replace(escaped, "[", _LIKE_ESCAPE_CHAR + "[", type_=String)


def _string_match(receiver: Any, needle: Any, *, prefix: bool, suffix: bool) -> Any:
    """Translate CEL contains/startsWith/endsWith to an escaped LIKE.

    The receiver (haystack) is the first operand and the needle the second, in
    CEL source order — the receiver may be a constant (`"const".contains(col)`)
    and the needle may be a column (field-to-field), so both sides accept
    either shape. `prefix`/`suffix` add `%` before/after the escaped needle.

    NOTE: `LIKE` collation is dialect-controlled; CEL string matching is
    case-sensitive, so case-insensitive dialects (e.g. SQLite without
    `PRAGMA case_sensitive_like`) need it configured for exact semantics.
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
        # A JSON or array column holds a collection, and LENGTH() of it is a number that
        # answers a different question -- the length of its text, or nothing CEL means. Which
        # SQL counts its elements depends on how it is stored, so the caller has to say.
        raise ValueError(
            "size() over a collection-typed column needs its storage declared: map the "
            'attribute in collection_columns with storage "json" or "pgArray"'
        )
    kind = scalar_kind(value)
    return null() if kind and kind != "string" else func.length(value)


# -- casts -------------------------------------------------------------------------------------


def _reject_numeric_cast(operator: str) -> NoReturn:
    """Fail closed on CEL's int()/double().

    CEL reads a WHOLE string or raises, and an error denies the row; SQL reads
    whatever numeric prefix parses, so ``CAST('100%_done' AS INTEGER)`` is 100 on
    SQLite and the filter returns rows the PDP denies. The numeric direction is no
    safer: CEL truncates toward zero where PostgreSQL and MySQL round, so
    ``int(-0.6)`` is 0 to CEL and -1 to them. Nothing in the plan says what type the
    operand's column holds, so no lowering is faithful for every row.
    """
    raise ValueError(
        f"'{operator}()' cannot be lowered to SQL CAST: CAST reads a numeric prefix "
        "where CEL requires the whole string and raises otherwise, and PostgreSQL and "
        "MySQL round where CEL truncates toward zero"
    )


def _string_cast(c: Any) -> Any:
    """CEL's ``string()``.

    Numeric and text columns lower to a CAST: CEL formats the shortest decimal that
    round-trips, and so do SQLite, PostgreSQL (12+, where that became the default) and
    MySQL.

    A BOOLEAN column does not go through a CAST. SQLite and MySQL have no boolean type and
    store 1/0, so ``CAST(a_bool AS VARCHAR)`` is ``'1'`` where CEL's ``string(true)`` is
    ``'true'`` -- the same query would return every matching row on PostgreSQL and none on
    SQLite (cerbos/query-plan-adapters#376). A CASE spells CEL's two words on every
    dialect instead (cerbos/query-plan-adapters#418), and its first arm is load-bearing: a
    NULL boolean is a missing attribute or a null value, CEL has no ``string()`` for either
    and denies the row, so the result must stay NULL rather than fall through to
    ``'false'``. The two words are literals, so on MySQL they compare in the connection's
    collation, which has to be case-sensitive as the columns' does.
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
    """Reject a zero denominator whose sign the adapter cannot observe.

    IEEE-754 keeps the sign of a zero, so ``n / -0.0`` is the OPPOSITE infinity from
    ``n / 0.0``. The planner does ship the sign — the wire operand for ``-0.0`` is
    ``-0`` — but Cerbos's HTTP transport renders a whole double without a decimal
    point and Python's ``json.loads("-0")`` returns the **int** ``0``, discarding the
    sign bit. A float operand keeps it (``json.loads("-0.0")`` is ``-0.0``), which is
    what the gRPC client delivers.

    So when the denominator arrives as an integer zero the adapter cannot tell which
    infinity CEL produced, and guessing returns rows the PDP denies. Fail closed
    instead (cerbos/query-plan-adapters#312).
    """
    if isinstance(denominator, bool) or not isinstance(denominator, int):
        return
    if denominator != 0:
        return
    raise ValueError(
        "division by a constant zero whose sign is indeterminate: the HTTP transport "
        "renders -0.0 as `-0`, which JSON decodes to the integer 0, so the adapter "
        "cannot tell +Infinity from -Infinity. Use the gRPC client, which preserves "
        "the sign bit, or avoid a literal zero denominator"
    )


def _float_div(c: Any, v: Any) -> Any:
    """CEL numeric attribute arithmetic is double-typed (Cerbos transports all
    numbers as doubles), so force float division: dialects with integer `/`
    (SQLite, PostgreSQL) would otherwise truncate `3 / 2.0` to `1`."""
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

    # A zero denominator is NOT an error in CEL: attribute arithmetic is
    # double-typed, so `0/0` is NaN and `x/0` is a signed infinity. Lowering
    # that to SQL NULL loses the distinction — `NULL != 1.0` is UNKNOWN and
    # excludes the row, while `NaN != 1.0` is TRUE and the PDP allows it.
    # Keep the three IEEE cases symbolic and let the enclosing comparison fold
    # each arm (see `_compare`/`_compare_leaf`), which is exact for ordered and
    # equality comparisons alike.
    #
    # A NULL numerator or denominator makes every branch condition UNKNOWN, so
    # the folded CASE yields NULL and the row stays excluded under BOTH
    # polarities — the correct outcome for a CEL missing-attribute error.
    #
    # The finite arm keeps a NULLIF guard: it can never be selected when the
    # denominator is zero, but dialects that evaluate CASE arms eagerly would
    # otherwise abort the whole query on a division by zero.
    #
    # IEEE-754 keeps the sign of a zero, so `n / -0.0` is the OPPOSITE infinity from
    # `n / 0.0`. A CONSTANT denominator carries its sign on the wire (the planner ships
    # `-0` verbatim and protobuf doubles preserve the sign bit), so it must be applied.
    # A COLUMN denominator does not: SQL cannot tell -0.0 from 0.0 and no portable
    # function reads the sign bit, so the positive-zero reading is assumed and
    # documented (cerbos/query-plan-adapters#312).
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

    ``R.attr.aNumber / R.attr.aNumber + 1.0`` composes addition on top of a division
    that is NaN for a zero row. Lowering that arm to SQL makes it ``NULL + 1``, and
    ``NULL != 2.0`` is UNKNOWN where CEL's ``NaN != 2.0`` is TRUE — the row the PDP
    allows would be dropped (cerbos/query-plan-adapters#312). Keeping the arms
    symbolic lets the enclosing comparison fold each one exactly.
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
            raise ValueError(
                "arithmetic combines a non-finite value with a column, which SQL "
                "cannot carry"
            )
        # A non-finite operand absorbs every finite one under +, -, * and /, so the
        # result is always non-finite and stays symbolic.
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
            # Cerbos 0.55 uses IEEE false for unordered comparisons, including under NOT.
            other = right_value if left_is_nan else left_value
            if isinstance(other, (int, float)):
                # CEL follows IEEE: NaN is unequal to everything and unordered.
                return operator == "ne"
            if hasattr(other, "is_"):
                # Preserve CEL missing-attribute errors as SQL UNKNOWN while
                # folding every present numeric value dialect-independently.
                return case(
                    (other.is_(None), null()),
                    else_=(operator == "ne"),
                )
            raise ValueError(
                "NaN can only be compared with numeric constants or SQLAlchemy "
                "expressions"
            )
        if not isinstance(left_value, (int, float)) or not isinstance(
            right_value, (int, float)
        ):
            raise ValueError(
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
    # CEL's equality is heterogeneous: `5 in ["5"]` is false. A member the column's
    # type cannot equal is dropped here, as `_compare_leaf` answers the same pair
    # under `==`, rather than handed to a store that converts '5' to 5.
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
        # Every member was dropped: false for a present value, and NULL for an
        # absent one, exactly as `c IN (...)` would have answered.
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
        raise ValueError(f"Invalid RFC-3339 timestamp literal: {value}")
    digits = match.group(4) or ""
    if len(digits) > 6 and any(d != "0" for d in digits[6:]):
        raise ValueError(
            "Timestamp literal precision exceeds the exact microsecond range: "
            f"{value}"
        )
    try:
        normalized = _EXCESS_RFC3339_PRECISION.sub(r"\1", value)
        normalized = normalized.replace("t", "T")
        normalized = normalized.replace("z", "+00:00").replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"Invalid RFC-3339 timestamp literal: {value}") from exc


def _timestamp(value: Any, _: Any) -> Any:
    """Unwrap a temporal column or parse an RFC-3339 planner constant."""
    if isinstance(getattr(value, "type", None), DateTime):
        return value
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        parsed = _parse_rfc3339(value)
    else:
        raise ValueError(
            "timestamp() requires an RFC-3339 literal or a SQLAlchemy DateTime column"
        )
    if parsed.tzinfo is None:
        raise ValueError(f"Timestamp literal must include an offset: {value}")
    try:
        normalized = parsed.astimezone(timezone.utc)
    except (OverflowError, ValueError) as exc:
        raise ValueError(
            f"Timestamp literal is outside CEL's supported instant range: {value}"
        ) from exc
    if normalized < _MIN_CEL_TIMESTAMP or normalized > _MAX_CEL_TIMESTAMP:
        raise ValueError(
            f"Timestamp literal is outside CEL's supported instant range: {value}"
        )
    return normalized


# -- hierarchies -------------------------------------------------------------------------------


def _hierarchy(value: Any, delimiter: Any) -> Hierarchy:
    delimiter = "." if delimiter is None else delimiter
    if not isinstance(delimiter, str) or not delimiter:
        raise ValueError("hierarchy() delimiter must be a non-empty string")
    return Hierarchy(value, delimiter)


def _matching_hierarchies(left: Any, right: Any) -> Tuple[Hierarchy, Hierarchy]:
    if not isinstance(left, Hierarchy) or not isinstance(right, Hierarchy):
        raise ValueError("Hierarchy operator requires hierarchy() operands")
    if left.delimiter != right.delimiter:
        raise ValueError("Hierarchy operands must use the same delimiter")
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
    raise ValueError("Hierarchy comparison between two columns is not supported")


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

# Read-only, so that no caller can change the defaults for every other `get_query` call in
# the process: an override is always explicit and per call (`operator_override_fns`).
OPERATOR_FNS = MappingProxyType(
    {
        "eq": _comparison("eq"),
        "ne": _comparison("ne"),
        "lt": _comparison("lt"),
        "gt": _comparison("gt"),
        "le": _comparison("le"),
        "ge": _comparison("ge"),
        "in": _in,
        # Arithmetic operators — return value expressions (not boolean), composed
        # inside parent comparisons like gt(add(col, 1), 2).
        "add": lambda c, v: c + v,
        "sub": lambda c, v: c - v,
        "mult": lambda c, v: c * v,
        "div": _float_div,
        "mod": lambda c, v: c % v,
        # CEL receiver-style string matches. Operands arrive in source order
        # (receiver first): the receiver may be a constant and the needle a
        # column, and LIKE metacharacters in the needle are always escaped.
        "contains": lambda c, v: _string_match(c, v, prefix=True, suffix=True),
        "startsWith": lambda c, v: _string_match(c, v, prefix=False, suffix=True),
        "endsWith": lambda c, v: _string_match(c, v, prefix=True, suffix=False),
        # Type conversions — value-returning expressions. Only string() survives: SQL CAST
        # does not reproduce CEL's int()/double(), which read a WHOLE string or raise where
        # CAST reads a numeric prefix, and truncate toward zero where PostgreSQL and MySQL
        # round (cerbos/query-plan-adapters#311).
        "string": lambda c, _: _string_cast(c),
        "double": lambda *_: _reject_numeric_cast("double"),
        "int": lambda *_: _reject_numeric_cast("int"),
        # size() over a string column. A collection is declared in `collection_columns` and
        # never reaches this handler; an undeclared JSON or array column is refused here.
        "size": _string_size,
        "timestamp": _timestamp,
        "hierarchy": _hierarchy,
        "ancestorOf": _ancestor_of,
        "descendentOf": _descendent_of,
        "overlaps": _hierarchy_overlaps,
    }
)

#: Value-returning operators that take a single operand, handed to their handler as
#: ``(operand, None)``.
UNARY_VALUE_OPERATORS = frozenset({"string", "double", "int", "size", "timestamp"})

#: Directional operators mirror when their operands swap sides; symmetric operators are
#: unchanged. The planner preserves policy source order, so `1 < R.attr.x` arrives as
#: lt(value(1), variable(x)) and must translate as `x > 1`, not `x < 1` (#257).
MIRRORED_OPERATORS: Dict[str, str] = {"lt": "gt", "gt": "lt", "le": "ge", "ge": "le"}

#: Operators whose semantics don't depend on which operand holds the column:
#: `eq`/`ne` are symmetric, value-first `in` (`value in R.attr.list`) still
#: means membership against the column, and set intersection is commutative, so
#: all four normalize to column-first. Every OTHER operator keeps its wire
#: (source) order when the value comes first — receiver-style string matches
#: (`"const".contains(R.attr.x)`) would otherwise silently swap haystack and
#: needle.
ORDER_INSENSITIVE_OPERATORS = frozenset({"eq", "ne", "in", "hasIntersection"})

#: Collection macros that fold into a flat boolean combination of their
#: per-element bodies. `exists_one`/`filter`/`map` have no such flattening and
#: fail closed instead.
FOLDABLE_COLLECTION_OPERATORS = frozenset({"exists", "all"})

# What a DEFAULT operator handler can lower: a SQL construct the mapper produced, a literal
# decoded from the plan (JSON carries no other kind of value), or one of THIS module's own
# symbolic values, which the handlers above know how to fold.
#
# Everything else is foreign. An operator override may return whatever it likes -- the
# corpus's `map` and `filter` overrides return deferred tuples -- but such a value only means
# something to the ENCLOSING override that consumes it, and once a default handler is running
# there is no enclosing override left to do so. Python then compares the intermediate with
# `==`, yields a bare `False`, and that reaches `where()` as a perfectly valid boolean which
# excludes every row: an emitted filter for a shape the adapter cannot express, and silent
# because it never threw (`map-eq-list`, cerbos/query-plan-adapters#387).
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
        raise ValueError(
            f"`{operator}` received an operand of type "
            f"{type(operand).__name__!r}, which is not a SQL expression or a plan "
            "literal: an operator override returning an intermediate value must be "
            "consumed by an enclosing override, and no default handler can lower one"
        )
