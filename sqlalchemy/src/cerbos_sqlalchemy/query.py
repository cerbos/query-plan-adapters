from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from types import MappingProxyType
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    List,
    Literal,
    NoReturn,
    Protocol,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

from cerbos.engine.v1 import engine_pb2
from cerbos.response.v1 import response_pb2
from cerbos.sdk.model import PlanResourcesFilterKind, PlanResourcesResponse
from google.protobuf.json_format import MessageToDict

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    String,
    Table,
    and_,
    case,
    cast,
    false,
    func,
    literal,
    not_,
    null,
    or_,
    select,
    true,
)
from sqlalchemy.orm import DeclarativeMeta, InstrumentedAttribute
from sqlalchemy.sql import Select
from sqlalchemy.sql.expression import (
    BinaryExpression,
    ColumnElement,
    ColumnOperators,
    FromClause,
)

try:  # SQLAlchemy >= 2.0
    from sqlalchemy.orm import DeclarativeBase
except ImportError:  # SQLAlchemy 1.4 predates the class-based declarative base.

    class DeclarativeBase:  # type: ignore[no-redef]
        """Stand-in so ``GenericTable`` stays constructible under SQLAlchemy 1.4."""


class _MappedClass(Protocol):
    """What `get_query` actually needs of an ORM model: a mapped `__table__`.

    Structural rather than nominal because the two declarative styles share no
    base class. Bounding the overload's TypeVar on it keeps unmapped classes out
    — unbounded, `Type[_ORMModel]` would admit any class at all, which is looser
    than the union it replaced.
    """

    __table__: ClassVar[FromClause]


_ORMModel = TypeVar("_ORMModel", bound=_MappedClass)

# A 2.0-style model's metaclass (`DeclarativeAttributeIntercept`) is *not* a
# `DeclarativeMeta`, so the legacy member alone does not admit it.
GenericTable = Union[Table, DeclarativeMeta, Type[DeclarativeBase]]
GenericColumn = Union[Column, InstrumentedAttribute]
GenericExpression = Union[BinaryExpression, ColumnOperators]
OperatorFnMap = Dict[str, Callable[[GenericColumn, Any], GenericExpression]]

# How the caller represents a NULL column when building the attributes it sends
# to check(). See get_query() and
# https://github.com/cerbos/query-plan-adapters/issues/302.
NullAttributeRepresentation = Literal["explicit", "omitted"]


_LIKE_ESCAPE_CHAR = "\\"
_RFC3339_TIMESTAMP = re.compile(
    r"^((?!0000)\d{4})-(\d{2})-(\d{2})[Tt]"
    r"(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d"
    r"(?:\.(\d{1,9}))?(?:[Zz]|[+-](?:[01]\d|2[0-3]):[0-5]\d)$"
)
_EXCESS_RFC3339_PRECISION = re.compile(r"(\.\d{6})\d+(?=(?:[Zz]|[+-]\d{2}:\d{2})$)")
_MIN_CEL_TIMESTAMP = datetime(1, 1, 1, tzinfo=timezone.utc)
_MAX_CEL_TIMESTAMP = datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)


@dataclass(frozen=True)
class _IEEEConstant:
    """A non-finite CEL double that must not be bound into dialect SQL."""

    value: float


@dataclass(frozen=True)
class _ConditionalValue:
    """A ternary retained until comparison so non-finite arms can be folded."""

    condition: Any
    then_value: Any
    else_value: Any


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
    if (
        not isinstance(c, bool)
        and isinstance(c, (int, float))
        and not isinstance(v, bool)
        and isinstance(v, (int, float))
    ):
        numerator = float(c)
        denominator = float(v)
        if denominator == 0.0:
            if numerator == 0.0 or math.isnan(numerator):
                # NaN has no sign, so an indeterminate zero cannot change the answer.
                return _IEEEConstant(math.nan)
            _require_signed_zero(v)
            sign = math.copysign(1.0, numerator) * math.copysign(1.0, denominator)
            return _IEEEConstant(math.copysign(math.inf, sign))
        return numerator / denominator

    numerator = (
        float(c)
        if not isinstance(c, bool) and isinstance(c, (int, float))
        else cast(c, Float)
    )
    denominator = (
        float(v)
        if not isinstance(v, bool) and isinstance(v, (int, float))
        else cast(v, Float)
    )

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
    if not isinstance(v, bool) and isinstance(v, (int, float)):
        _require_signed_zero(v)
        denominator_sign = math.copysign(1.0, float(v))

    return _ConditionalValue(
        condition=denominator == 0.0,
        then_value=_ConditionalValue(
            condition=numerator == 0.0,
            then_value=_IEEEConstant(math.nan),
            else_value=_ConditionalValue(
                condition=numerator > 0.0,
                then_value=_IEEEConstant(math.copysign(math.inf, denominator_sign)),
                else_value=_IEEEConstant(math.copysign(math.inf, -denominator_sign)),
            ),
        ),
        else_value=numerator / func.nullif(denominator, 0.0),
    )


def _arith_over_conditional(op_fn: Any, left: Any, right: Any) -> Any:
    """Distribute a binary arithmetic operator across a retained ternary.

    ``R.attr.aNumber / R.attr.aNumber + 1.0`` composes addition on top of a division
    that is NaN for a zero row. Lowering that arm to SQL makes it ``NULL + 1``, and
    ``NULL != 2.0`` is UNKNOWN where CEL's ``NaN != 2.0`` is TRUE — the row the PDP
    allows would be dropped (cerbos/query-plan-adapters#312). Keeping the arms
    symbolic lets the enclosing comparison fold each one exactly.
    """
    if isinstance(left, _ConditionalValue):
        return _ConditionalValue(
            condition=left.condition,
            then_value=_arith_over_conditional(op_fn, left.then_value, right),
            else_value=_arith_over_conditional(op_fn, left.else_value, right),
        )
    if isinstance(right, _ConditionalValue):
        return _ConditionalValue(
            condition=right.condition,
            then_value=_arith_over_conditional(op_fn, left, right.then_value),
            else_value=_arith_over_conditional(op_fn, left, right.else_value),
        )
    if isinstance(left, _IEEEConstant) or isinstance(right, _IEEEConstant):
        left_value = left.value if isinstance(left, _IEEEConstant) else left
        right_value = right.value if isinstance(right, _IEEEConstant) else right
        numeric = [
            value
            for value in (left_value, right_value)
            if not isinstance(value, bool) and isinstance(value, (int, float))
        ]
        if len(numeric) != 2:
            raise ValueError(
                "arithmetic combines a non-finite value with a column, which SQL "
                "cannot carry"
            )
        # A non-finite operand absorbs every finite one under +, -, * and /, so the
        # result is always non-finite and stays symbolic.
        result = op_fn(float(left_value), float(right_value))
        if isinstance(result, _IEEEConstant):
            return result
        return _IEEEConstant(float(result))
    return op_fn(left, right)


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
    """CEL's ``string()``, for the operand types where CAST reproduces it.

    Numeric and text columns lower cleanly: CEL formats the shortest decimal that
    round-trips, and so do SQLite, PostgreSQL (12+, where that became the default) and
    MySQL.

    A BOOLEAN column does not, and it is the one type where lowering is wrong rather
    than merely unproven. SQLite and MySQL have no boolean type and store 1/0, so
    ``CAST(a_bool AS VARCHAR)`` is ``'1'`` where CEL's ``string(true)`` is ``'true'`` —
    the same query returns every matching row on PostgreSQL and none on SQLite. One
    adapter serves every dialect SQLAlchemy does, so the shape fails closed rather than
    being silently dialect-dependent (cerbos/query-plan-adapters#376).
    """
    if isinstance(getattr(c, "type", None), Boolean):
        raise ValueError(
            "'string()' over a boolean column cannot be lowered to SQL CAST: SQLite "
            "and MySQL store a boolean as 1/0 and render '1', while CEL and PostgreSQL "
            "render 'true', so no single CAST is correct on every dialect"
        )
    return cast(c, String)


def _apply_comparison(operator: str, left: Any, right: Any) -> Any:
    comparisons = {
        "eq": lambda: left == right,
        "ne": lambda: left != right,
        "lt": lambda: left < right,
        "gt": lambda: left > right,
        "le": lambda: left <= right,
        "ge": lambda: left >= right,
    }
    return comparisons[operator]()


def _compare_leaf(operator: str, left: Any, right: Any) -> Any:
    left_is_ieee = isinstance(left, _IEEEConstant)
    right_is_ieee = isinstance(right, _IEEEConstant)
    if left_is_ieee or right_is_ieee:
        left_value = left.value if left_is_ieee else left
        right_value = right.value if right_is_ieee else right
        left_is_nan = left_is_ieee and math.isnan(left_value)
        right_is_nan = right_is_ieee and math.isnan(right_value)
        if left_is_nan or right_is_nan:
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
    if isinstance(left, _ConditionalValue):
        return case(
            (left.condition, _compare(operator, left.then_value, right)),
            (not_(left.condition), _compare(operator, left.else_value, right)),
        )
    if isinstance(right, _ConditionalValue):
        return case(
            (right.condition, _compare(operator, left, right.then_value)),
            (not_(right.condition), _compare(operator, left, right.else_value)),
        )
    return _compare_leaf(operator, left, right)


def _in(c: Any, values: Any) -> Any:
    """CEL membership, including explicit-null list elements."""
    members = values if isinstance(values, list) else [values]
    non_nulls = [member for member in members if member is not None]
    predicates = []
    if non_nulls:
        predicates.append(c.in_(non_nulls))
    if len(non_nulls) != len(members):
        predicates.append(c.is_(None))
    if not predicates:
        return false()
    return or_(*predicates)


def _timestamp(value: Any, _: Any) -> Any:
    """Unwrap a temporal column or parse an RFC-3339 planner constant."""
    column_type = getattr(value, "type", None)
    if isinstance(column_type, DateTime):
        return value
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
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
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError(f"Invalid RFC-3339 timestamp literal: {value}") from exc
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


@dataclass(frozen=True)
class _Hierarchy:
    value: Any
    delimiter: str


def _hierarchy(value: Any, delimiter: Any) -> _Hierarchy:
    delimiter = "." if delimiter is None else delimiter
    if not isinstance(delimiter, str) or not delimiter:
        raise ValueError("hierarchy() delimiter must be a non-empty string")
    return _Hierarchy(value, delimiter)


def _assert_matching_hierarchies(
    left: Any, right: Any
) -> Tuple[_Hierarchy, _Hierarchy]:
    if not isinstance(left, _Hierarchy) or not isinstance(right, _Hierarchy):
        raise ValueError("Hierarchy operator requires hierarchy() operands")
    if left.delimiter != right.delimiter:
        raise ValueError("Hierarchy operands must use the same delimiter")
    return left, right


def _ancestor_of(left: Any, right: Any) -> Any:
    ancestor, descendent = _assert_matching_hierarchies(left, right)
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
    left_hierarchy, right_hierarchy = _assert_matching_hierarchies(left, right)
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


# We want to make the base dict "immutable", and enforce explicit (optional) overrides on
# each call to `get_query` (rather than allowing keys in this dict to be overridden, which
# could wreak havoc if different calls from the same memory space weren't aware of each other's
# overrides)
__operator_fns: OperatorFnMap = {
    "eq": lambda c, v: _compare("eq", c, v),
    "ne": lambda c, v: _compare("ne", c, v),
    "lt": lambda c, v: _compare("lt", c, v),
    "gt": lambda c, v: _compare("gt", c, v),
    "le": lambda c, v: _compare("le", c, v),
    "ge": lambda c, v: _compare("ge", c, v),
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
    # size() over a string column — collection-typed columns require an override.
    "size": lambda c, _: func.length(c),
    "timestamp": _timestamp,
    "hierarchy": _hierarchy,
    "ancestorOf": _ancestor_of,
    "descendentOf": _descendent_of,
    "overlaps": _hierarchy_overlaps,
}
OPERATOR_FNS = MappingProxyType(__operator_fns)

# Directional operators mirror when their operands swap sides; symmetric operators are
# unchanged. The planner preserves policy source order, so `1 < R.attr.x` arrives as
# lt(value(1), variable(x)) and must translate as `x > 1`, not `x < 1` (#257).
_MIRRORED_OPERATORS = MappingProxyType(
    {
        "lt": "gt",
        "gt": "lt",
        "le": "ge",
        "ge": "le",
    }
)

# Operators whose semantics don't depend on which operand holds the column:
# `eq`/`ne` are symmetric, and value-first `in` (`value in R.attr.list`) still
# means membership against the column, so all three normalize to column-first.
# Every OTHER operator keeps its wire (source) order when the value comes
# first — receiver-style string matches (`"const".contains(R.attr.x)`) would
# otherwise silently swap haystack and needle.
_ORDER_INSENSITIVE_OPERATORS = frozenset({"eq", "ne", "in"})

# Unary value-returning operators take a single non-value input.
_UNARY_VALUE_OPERATORS = frozenset({"string", "double", "int", "size", "timestamp"})

# Operators whose second operand is a lambda that binds an iteration variable.
_LAMBDA_BINDING_OPERATORS = frozenset(
    {"exists", "exists_one", "all", "filter", "map", "except"}
)

# Collection macros that fold into a flat boolean combination of their
# per-element bodies. `exists_one`/`filter`/`map` have no such flattening and
# fail closed instead.
_FOLDABLE_COLLECTION_OPERATORS = frozenset({"exists", "all"})


def _unwrap_expression(operand: dict) -> dict:
    """Return the `{operator, operands}` node an operand carries, if any."""
    expression = operand.get("expression")
    return operand if expression is None else expression


def _carries_null_operand(operand: dict) -> bool:
    if "value" not in operand:
        return False
    value = operand["value"]
    if value is None:
        return True
    return isinstance(value, list) and any(member is None for member in value)


# The operators CEL evaluates to a definite boolean over a null value, and so the
# only ones an attribute's declared convention can settle. Anything else -- a
# collection macro, hasIntersection, a string match -- keeps using the call-level
# fallback, because the declaration says nothing about what its null means there.
_EQUALITY_FAMILY = frozenset({"eq", "ne", "in"})


def _compared_attribute_and_literal(node: dict):
    """Destructure a binary comparison between a plan variable and a literal.

    Returns ``(variable_name, literal_operand)`` in either operand order, or
    ``None`` when the node is not that shape.
    """
    expression = _unwrap_expression(node)
    if expression.get("operator") not in _EQUALITY_FAMILY:
        return None
    operands = expression.get("operands", [])
    if len(operands) != 2:
        return None
    left, right = operands
    variable, literal = left, right
    if "variable" in right:
        variable, literal = right, left
    if "variable" not in variable or "value" not in literal:
        return None
    return variable["variable"], literal


def _assert_no_null_comparison_operands(
    node: dict, declarations: Dict[str, "NullAttributeRepresentation"], fallback: str
) -> None:
    """Reject every null literal operand under the ``omitted`` representation.

    A NULL column then carries no attribute at all, so CEL raises a
    missing-attribute error and ``check()`` denies the row -- ``IS NULL`` would
    return exactly the rows the PDP refuses (cerbos/query-plan-adapters#302).

    The scan matches on the OPERAND, never on an allowlist of operators. A null
    constant reaches a NULL-selecting predicate through more shapes than the
    obvious ``eq``/``ne``/``in`` -- ``hasIntersection`` carries one in its value
    list too -- and any operator added later would silently escape a list that
    has to be maintained by hand.

    The rejection is also deliberately wider than the over-granting shapes:
    ``ne(x, null)`` on its own is aligned, but negation is applied around the
    built predicate rather than pushed into the leaf, so a leaf cannot tell
    whether an enclosing ``not`` will flip ``IS NOT NULL`` back into a
    NULL-selecting predicate. Rejecting every null operand is correct under any
    nesting; narrowing it requires negation-parity tracking.
    """
    expression = _unwrap_expression(node)
    operator = expression.get("operator")
    operands = expression.get("operands", [])

    # A comparison between a mapped attribute and a literal is decided by that
    # attribute's own declaration, which is what lets one call carry both
    # conventions (cerbos/query-plan-adapters#308). Confined to that shape: a
    # null buried in a macro over a literal list reaches a comparison long
    # after this scan, and nothing here can say which column it will land
    # against, so those keep using the call-level fallback.
    compared = _compared_attribute_and_literal(node)
    if compared is not None:
        variable, literal = compared
        declared = declarations.get(variable)
        if declared is not None:
            if declared == "omitted" and _carries_null_operand(literal):
                raise _null_operand_error(operator)
            return

    if (
        any(_carries_null_operand(_unwrap_expression(operand)) for operand in operands)
        and fallback == "omitted"
    ):
        raise _null_operand_error(operator)
    for operand in operands:
        _assert_no_null_comparison_operands(operand, declarations, fallback)


def _null_operand_error(operator) -> ValueError:
    return ValueError(
        f"Cannot translate `{operator}` against a null operand under "
        'null_attribute_representation="omitted": a NULL column sends no '
        "attribute, so Cerbos evaluates the comparison as a missing-attribute "
        "error (deny) while a NULL-selecting filter would return those rows. "
        'Send NULL columns as explicit nulls and use "explicit", or keep this '
        "shape out of the policy."
    )


def _substitute_lambda_variable(
    operand: dict, variable_name: str, element: Any
) -> dict:
    """Substitute a lambda iteration variable with a concrete collection element.

    A bare reference to the variable becomes the element itself; a
    ``variable.path.to.field`` reference drills into the element and fails
    closed when the path is missing. A nested collection macro whose lambda
    rebinds the same variable name shadows the outer variable, so substitution
    only descends into its collection operand.
    """
    if (expression := operand.get("expression")) is not None:
        return {
            "expression": _substitute_lambda_variable(
                expression, variable_name, element
            )
        }

    if (name := operand.get("variable")) is not None:
        if name == variable_name:
            return {"value": element}
        if name.startswith(f"{variable_name}."):
            current = element
            for segment in name[len(variable_name) + 1 :].split("."):
                if not isinstance(current, dict) or segment not in current:
                    raise ValueError(
                        f'Cannot resolve "{name}": collection element has no field '
                        f'"{segment}"'
                    )
                current = current[segment]
            return {"value": current}
        return operand

    if "operator" not in operand:
        return operand

    operator = operand["operator"]
    child_operands = operand.get("operands", [])

    if operator in _LAMBDA_BINDING_OPERATORS and len(child_operands) == 2:
        nested_collection, nested_lambda = child_operands
        nested_expression = _unwrap_expression(nested_lambda)
        nested_lambda_operands = nested_expression.get("operands", [])
        if (
            nested_expression.get("operator") == "lambda"
            and len(nested_lambda_operands) == 2
            and nested_lambda_operands[1].get("variable") == variable_name
        ):
            # The nested lambda rebinds our variable: it shadows the outer
            # binding, so only its collection operand may be substituted.
            return {
                "operator": operator,
                "operands": [
                    _substitute_lambda_variable(
                        nested_collection, variable_name, element
                    ),
                    nested_lambda,
                ],
            }

    return {
        "operator": operator,
        "operands": [
            _substitute_lambda_variable(child, variable_name, element)
            for child in child_operands
        ],
    }


# We support both the legacy HTTP and gRPC clients, so therefore we need to accept both input types
_deny_types = frozenset(
    [
        PlanResourcesFilterKind.ALWAYS_DENIED,
        engine_pb2.PlanResourcesFilter.KIND_ALWAYS_DENIED,
    ]
)
_allow_types = frozenset(
    [
        PlanResourcesFilterKind.ALWAYS_ALLOWED,
        engine_pb2.PlanResourcesFilter.KIND_ALWAYS_ALLOWED,
    ]
)


def _get_table_name(t: GenericTable) -> str:
    try:
        # ORM model — both declarative styles carry the mapped `Table` here
        return t.__table__.name
    except AttributeError:
        # Core `Table` type
        return t.name


def _variables_outside_overrides(
    operand: dict, override_operators: frozenset, override_owned: bool = False
) -> frozenset:
    """Find variables that still require an ordinary table mapping.

    An override owns its complete operand subtree: it may turn foreign columns
    or relation markers into a correlated subquery instead of a flat JOIN.
    Variables outside such a subtree retain the normal fail-closed
    ``table_mapping`` requirement. Boolean/ternary traversal is built in and
    cannot itself be overridden, so merely declaring those keys owns nothing.
    """
    if (expression := operand.get("expression")) is not None:
        return _variables_outside_overrides(
            expression, override_operators, override_owned
        )
    if "variable" in operand:
        return frozenset() if override_owned else frozenset({operand["variable"]})

    operator = operand.get("operator")
    operator_owns_children = override_owned or (
        operator in override_operators and operator not in {"and", "or", "not", "if"}
    )
    variables = frozenset()
    for child in operand.get("operands", []):
        variables |= _variables_outside_overrides(
            child, override_operators, operator_owns_children
        )
    return variables


# An ORM model class carries its row type; a Core `Table` does not. Overloading on
# that distinction lets callers infer the model rather than annotate the result.
@overload
def get_query(
    query_plan: Union[PlanResourcesResponse, response_pb2.PlanResourcesResponse],  # type: ignore (https://github.com/microsoft/pyright/issues/1035)
    table: Type[_ORMModel],
    attr_map: Dict[str, GenericColumn],
    table_mapping: Union[List[Tuple[GenericTable, GenericExpression]], None] = ...,
    operator_override_fns: Union[OperatorFnMap, None] = ...,
    null_attribute_representation: NullAttributeRepresentation = ...,
    attribute_null_representation: Union[
        Dict[str, NullAttributeRepresentation], None
    ] = ...,
) -> Select[Tuple[_ORMModel]]:
    ...


# Everything else `GenericTable` admits — a Core `Table`, and a legacy model
# under 1.4, whose stubs do not declare `__table__` so it cannot match the bound
# above. Row type unknown, but the call is still accepted: without this arm the
# overloads would be narrower than the union they replaced.
@overload
def get_query(
    query_plan: Union[PlanResourcesResponse, response_pb2.PlanResourcesResponse],  # type: ignore (https://github.com/microsoft/pyright/issues/1035)
    table: GenericTable,
    attr_map: Dict[str, GenericColumn],
    table_mapping: Union[List[Tuple[GenericTable, GenericExpression]], None] = ...,
    operator_override_fns: Union[OperatorFnMap, None] = ...,
    null_attribute_representation: NullAttributeRepresentation = ...,
    attribute_null_representation: Union[
        Dict[str, NullAttributeRepresentation], None
    ] = ...,
) -> Select[Any]:
    ...


def get_query(
    query_plan: Union[PlanResourcesResponse, response_pb2.PlanResourcesResponse],  # type: ignore (https://github.com/microsoft/pyright/issues/1035)
    table: GenericTable,
    attr_map: Dict[str, GenericColumn],
    table_mapping: Union[List[Tuple[GenericTable, GenericExpression]], None] = None,
    operator_override_fns: Union[OperatorFnMap, None] = None,
    null_attribute_representation: NullAttributeRepresentation = "explicit",
    attribute_null_representation: Union[
        Dict[str, NullAttributeRepresentation], None
    ] = None,
) -> Select[Any]:
    """Translate a Cerbos query plan into a SQLAlchemy ``Select``.

    ``null_attribute_representation`` declares how the caller represents a NULL
    column when building the attributes it sends to ``check()``. The planner
    emits the same ``eq(attr, null)`` node either way, so the plan cannot reveal
    which convention is in use and the adapter has to be told.

    - ``"explicit"`` (default) -- a NULL column is sent as an explicit ``null``
      attribute. CEL compares ``null == null``, so ``IS NULL`` selects exactly
      the rows ``check()`` allows.
    - ``"omitted"`` -- a NULL column sends no attribute at all. CEL then raises a
      missing-attribute error, which Cerbos treats as a deny, so a filter that
      *selects* NULL rows returns rows the PDP denies. Null comparison operands
      are rejected instead of translated.

    ``attribute_null_representation`` declares the same thing PER ATTRIBUTE,
    keyed by the references ``attr_map`` uses. It overrides
    ``null_attribute_representation`` for the attributes it names and asserts
    that their columns can be NULL; an attribute it does not name is treated as
    NOT NULL when rendering a comparison, which is the historical translation.

    It exists because one policy suite can legitimately mix the two conventions
    -- the same column can be mapped twice, sent as an explicit null under one
    attribute name and omitted under another -- which a single call-level
    option cannot express. Declaring an attribute ``"explicit"`` makes the
    equality family (``eq``, ``ne``, ``in``) render so it can never be SQL
    UNKNOWN: CEL holds a null VALUE under that convention, so ``null != "x"``
    is TRUE and ``null == "x"`` is FALSE, both definite, while UNKNOWN excludes
    the row under BOTH polarities. Ordering and string operators are left
    alone, because a null receiver raises a no-overload error in CEL, which
    denies exactly as UNKNOWN does.

    See https://github.com/cerbos/query-plan-adapters/issues/302 and
    https://github.com/cerbos/query-plan-adapters/issues/308.
    """
    if null_attribute_representation not in ("explicit", "omitted"):
        raise ValueError(
            "null_attribute_representation must be 'explicit' or 'omitted', got "
            f"{null_attribute_representation!r}"
        )
    null_conventions: Dict[str, NullAttributeRepresentation] = (
        attribute_null_representation or {}
    )
    for attribute, convention in null_conventions.items():
        if convention not in ("explicit", "omitted"):
            raise ValueError(
                "attribute_null_representation values must be 'explicit' or "
                f"'omitted', got {convention!r} for {attribute!r}"
            )
        if attribute not in attr_map:
            raise ValueError(
                f"attribute_null_representation names {attribute!r}, which is not "
                "in the attribute column map"
            )

    if query_plan.filter is None or query_plan.filter.kind in _deny_types:
        return select(table).where(False)

    if query_plan.filter.kind in _allow_types:
        return select(table)

    cond = (
        MessageToDict(query_plan.filter.condition)
        if isinstance(query_plan, response_pb2.PlanResourcesResponse)
        else query_plan.filter.condition.to_dict()
    )

    # Always: the call-level option is only the fallback now, and an attribute
    # can declare "omitted" while the call declares "explicit".
    _assert_no_null_comparison_operands(
        cond, null_conventions, null_attribute_representation
    )

    # Inspect columns that the normal translator owns. Override-owned operands
    # may legitimately be relation markers or columns translated into
    # correlated subqueries, but an unrelated override must never disable the
    # ordinary cross-table mapping requirement.
    if operator_override_fns is None:
        attributes_to_validate = attr_map.items()
    else:
        active_override_operators = frozenset(
            operator
            for operator, override in operator_override_fns.items()
            if override is not None
        )
        variables = _variables_outside_overrides(cond, active_override_operators)
        attributes_to_validate = (
            (variable, attr_map[variable])
            for variable in variables
            if variable in attr_map
        )

    required_tables = set()
    for variable, column in attributes_to_validate:
        column_table = getattr(column, "table", None)
        if column_table is None:
            # A self-contained SQL expression — canonically a correlated scalar
            # subquery — is how a caller reaches a scalar through a to-ONE hop
            # without a join (cerbos/query-plan-adapters#375). It carries its own
            # correlation, so it needs no `table_mapping`, and an absent hop makes
            # it SQL NULL: CEL's missing-path error, excluded under BOTH polarities
            # because NOT NULL is still NULL. Only a value that is neither a column
            # nor an expression — a bare relation marker used outside an override —
            # is a mapping error.
            if isinstance(column, ColumnElement):
                continue
            raise TypeError(
                f"Attribute '{variable}' must be handled by an operator override "
                "or map to a SQLAlchemy column"
            )
        if column_table.name != _get_table_name(table):
            required_tables.add(column_table.name)

    if required_tables:
        if table_mapping is None:
            raise TypeError(
                "get_query() missing 1 required positional argument: 'table_mapping'"
            )
        for mapped_table, _ in table_mapping:
            required_tables.discard(_get_table_name(mapped_table))
        if required_tables:
            raise TypeError(
                "positional argument 'table_mapping' missing mapping for table(s): '{0}'".format(
                    "', '".join(sorted(required_tables))
                )
            )

    def get_operator_fn(op: str, c: Any, v: Any) -> GenericExpression:
        # Check to see if the client has overridden the function
        if (
            operator_override_fns
            and (override_fn := operator_override_fns.get(op)) is not None
        ):
            return override_fn(c, v)

        # Otherwise, fall back to default handlers
        if (default_fn := OPERATOR_FNS.get(op)) is not None:
            return default_fn(c, v)

        raise ValueError(f"Unrecognised operator: {op}")

    def resolve_variable(variable: str) -> GenericColumn:
        try:
            return attr_map[variable]
        except KeyError:
            raise KeyError(
                f"Attribute does not exist in the attribute column map: {variable}"
            )

    def is_explicit_null(variable: str) -> bool:
        return null_conventions.get(variable) == "explicit"

    def definite_equality(
        operator: str,
        left_column: Any,
        right: Any,
        left_explicit: bool,
        right_explicit: bool,
    ) -> Any:
        """Render an equality that can never be SQL UNKNOWN.

        An attribute the caller sends as an explicit null holds a null VALUE in
        CEL, so equality against a non-null operand is a definite FALSE,
        inequality a definite TRUE, and two nulls are EQUAL. SQL answers
        UNKNOWN to all three, which excludes the row under BOTH polarities --
        so the NOT an enclosing negation applies has nothing definite to flip.

        Deliberately not ``is_distinct_from``. Two reasons, and the second is
        the load-bearing one: the same expression has to render on SQLite,
        PostgreSQL and MySQL -- and a null-safe equality is SYMMETRIC while this
        rewrite must not be. When only ONE side declares the convention, the
        other side's NULL is a MISSING attribute on the check side, so CEL raises
        an error and denies; only the asymmetric expansion below keeps
        propagating UNKNOWN for it. A null-safe operator would match the two
        NULLs and over-grant.
        """
        present = []
        if left_explicit:
            present.append(left_column.isnot(None))
        if right_explicit:
            present.append(right.isnot(None))
        equality = and_(*present, left_column == right)
        if left_explicit and right_explicit:
            equality = or_(and_(left_column.is_(None), right.is_(None)), equality)
        return not_(equality) if operator == "ne" else equality

    def with_null_conventions(
        operator: str,
        left: Any,
        right: Any,
        left_explicit: bool,
        right_explicit: bool,
        plain: Any,
    ) -> Any:
        """The comparison with the declared NULL conventions applied, else ``plain``.

        ``plain`` is the ordinary lowering the caller would otherwise return --
        passed in rather than rebuilt here, so a registered operator override is
        honoured on every path.

        ``eq``/``ne`` RESTRUCTURE the comparison, so an operator the caller
        overrode is left alone: replacing it would make this declaration silently
        discard the caller's own translation, which is not what it declares.
        ``in`` only gains a presence guard ANDed alongside whatever the
        membership lowered to, which composes with an override rather than
        replacing it.
        """
        if (left_explicit or right_explicit) and right is not None:
            if operator in ("eq", "ne"):
                overridden = (
                    operator_override_fns is not None
                    and operator_override_fns.get(operator) is not None
                )
                if not overridden:
                    return definite_equality(
                        operator, left, right, left_explicit, right_explicit
                    )
            elif (
                operator == "in"
                and left_explicit
                and hasattr(left, "isnot")
                # A stored COLLECTION, not a literal list: a null element can
                # exist at run time and `null in coll` is TRUE when it does, so
                # the presence guard would exclude exactly the rows CEL allows.
                # The collection's own lowering already handles the null member.
                and isinstance(right, list)
                # A null member already forces the `IS NULL` disjunct, which is
                # definite on its own.
                and not any(member is None for member in right)
            ):
                return and_(left.isnot(None), plain)
        return plain

    def fold_value_list_macro(operator: str, elements: Any, lambda_operand: dict):
        """Fold a collection macro whose collection operand is a literal value list.

        The planner emits this shape when a known-value collection (typically a
        folded principal attribute) has more than 10 elements — at 10 or fewer
        it unrolls `exists`/`all` into an or/and chain itself
        (cerbos/cerbos#2570, cerbos/cerbos#2817; `maxItems = 10` in the
        planner's struct matcher). Apply the same fold here, uncapped, so the
        translated query does not depend on which side of that threshold the
        collection lands: substitute each element into the lambda body and
        combine the per-element predicates with OR (`exists`) or AND (`all`).

        Each substituted body goes back through the ordinary traversal, so
        comparison semantics — operator overrides, three-valued NULL handling,
        value-first mirroring — are identical to a planner-unrolled chain of
        the same comparisons.
        """
        if operator not in _FOLDABLE_COLLECTION_OPERATORS:
            raise ValueError(
                f"{operator} over a literal collection value is not supported. "
                "Only exists() and all() can be folded into a flat filter."
            )
        if not isinstance(elements, list):
            raise ValueError(
                f"{operator} over a literal collection requires a list value"
            )

        lambda_expression = _unwrap_expression(lambda_operand)
        if lambda_expression.get("operator") != "lambda":
            raise ValueError(
                f"Second operand of {operator} must be a lambda expression"
            )
        lambda_operands = lambda_expression.get("operands", [])
        if len(lambda_operands) != 2:
            raise ValueError(
                f"{operator} over a literal collection supports single-variable "
                "lambdas only"
            )
        body, variable = lambda_operands
        variable_name = variable.get("variable")
        if not variable_name:
            raise ValueError("Lambda variable must have a name")

        predicates = [
            traverse_and_map_operands(
                _substitute_lambda_variable(body, variable_name, element)
            )
            for element in elements
        ]
        if not predicates:
            # CEL identity semantics over an empty collection: exists() matches
            # nothing, all() matches everything.
            return false() if operator == "exists" else true()
        return or_(*predicates) if operator == "exists" else and_(*predicates)

    def try_fold_value_list_macro(operator: str, child_operands: list):
        """Return the folded predicate for a value-list macro, else None.

        A literal value list can never be a relation marker or a column, so no
        operator override can meaningfully consume it — fold it here instead.
        """
        if operator not in _LAMBDA_BINDING_OPERATORS or len(child_operands) != 2:
            return None
        collection, lambda_operand = child_operands
        if "value" not in collection:
            return None
        return fold_value_list_macro(operator, collection["value"], lambda_operand)

    def resolve_operand(operand: dict) -> Any:
        """Resolve an operand to a SQL value/expression, descending into nested
        `expression` operands so that value-returning operators (arithmetic,
        casts, ternary, etc.) compose inside outer comparisons.
        """
        if "value" in operand:
            return operand["value"]
        if "variable" in operand:
            return resolve_variable(operand["variable"])
        if (exp := operand.get("expression")) is not None:
            return evaluate_expression(exp)
        raise ValueError(f"Unrecognised operand shape: {operand}")

    def evaluate_expression(expression: dict) -> Any:
        """Evaluate a value-producing expression node (an `{operator, operands}`
        dict) to a SQL expression. Used for nested non-boolean operators.
        """
        operator = expression["operator"]
        child_operands = expression["operands"]

        # Boolean combinators can appear nested inside value expressions
        # (e.g. a lambda body of `and(...)`); route them back through the
        # predicate traversal rather than treating them as binary operators.
        if operator in ("and", "or", "not"):
            return traverse_and_map_operands(expression)

        if operator == "if":
            # Ternary: if(cond, then, else). The condition may be either a
            # boolean expression or a bare boolean variable/value.
            #
            # Three-valued logic: when the condition is UNKNOWN (e.g. a NULL
            # column), CEL raises a missing-attribute error — a deny — so the
            # SQL result must be NULL, not the else-branch. A CASE with only a
            # WHEN-cond/WHEN-not-cond pair (no ELSE) yields NULL for UNKNOWN
            # conditions, keeping the row excluded under BOTH polarities
            # (`NOT (NULL > 1)` stays UNKNOWN instead of leaking to TRUE).
            first = child_operands[0]
            if "expression" in first:
                cond = traverse_and_map_operands(first["expression"])
            else:
                cond = resolve_operand(first)
            then_value = resolve_operand(child_operands[1])
            else_value = resolve_operand(child_operands[2])
            if isinstance(then_value, (_IEEEConstant, _ConditionalValue)) or isinstance(
                else_value, (_IEEEConstant, _ConditionalValue)
            ):
                return _ConditionalValue(cond, then_value, else_value)
            return case((cond, then_value), (not_(cond), else_value))

        folded = try_fold_value_list_macro(operator, child_operands)
        if folded is not None:
            return folded

        if operator == "hierarchy":
            target = resolve_operand(child_operands[0])
            delimiter = (
                resolve_operand(child_operands[1]) if len(child_operands) == 2 else None
            )
            return get_operator_fn(operator, target, delimiter)

        if operator in _UNARY_VALUE_OPERATORS:
            target = resolve_operand(child_operands[0])
            return get_operator_fn(operator, target, None)

        if len(child_operands) < 2:
            # e.g. timestamp(...) — a planner shape with no SQL translation.
            raise ValueError(f"Unrecognised unary operator: {operator}")

        # Binary value operators (add/sub/mult/div/mod, plus any user override).
        # Operands are passed in wire (source) order, which is significant for
        # non-commutative operators (sub/div) and receiver-style string ops.
        left = resolve_operand(child_operands[0])
        right = resolve_operand(child_operands[1])
        if isinstance(left, (_ConditionalValue, _IEEEConstant)) or isinstance(
            right, (_ConditionalValue, _IEEEConstant)
        ):
            if operator == "mod":
                # CEL's % is integer-only while Cerbos attribute values are always
                # doubles, so a modulus over this arithmetic is a no-overload error
                # that denies every row at check time. Folding it with Python's %
                # would answer a question CEL refused.
                raise ValueError(
                    "modulus over a division whose denominator may be zero is not "
                    "supported: CEL's % is integer-only and attribute values are "
                    "always doubles, so the condition can never be satisfied by the PDP"
                )
            # A retained ternary (a division that may be non-finite) must keep
            # propagating symbolically through the surrounding arithmetic.
            return _arith_over_conditional(
                lambda a, b: get_operator_fn(operator, a, b), left, right
            )
        return get_operator_fn(operator, left, right)

    def traverse_and_map_operands(operand: dict):
        if exp := operand.get("expression"):
            return traverse_and_map_operands(exp)

        # Bare leaf operands in a boolean position (e.g. `R.attr.aBool` as a
        # conjunct of an `and`): resolve directly.
        if "variable" in operand:
            return resolve_variable(operand["variable"])
        if "value" in operand:
            return operand["value"]

        operator = operand["operator"]
        child_operands = operand["operands"]

        # if `operator` in ["and", "or"], `child_operands` is a nested list of `expression` dicts (handled at the
        # beginning of this closure)
        if operator == "and":
            return and_(*[traverse_and_map_operands(o) for o in child_operands])
        if operator == "or":
            return or_(*[traverse_and_map_operands(o) for o in child_operands])
        if operator == "not":
            return not_(*[traverse_and_map_operands(o) for o in child_operands])
        if operator == "if":
            # A bare boolean-result ternary used directly as a predicate.
            return evaluate_expression(operand)

        # A literal value list arrives when the planner could not unroll a
        # macro over a known collection (more than 10 elements). Fold it before
        # override dispatch: overrides exist to translate relation/column
        # collections, which a literal can never be.
        folded = try_fold_value_list_macro(operator, child_operands)
        if folded is not None:
            return folded

        has_nested_expression = any("expression" in o for o in child_operands)

        # If the user has supplied an override for this operator and the
        # operands include a nested expression (e.g. size(tags) where tags is
        # a collection), or the operator isn't simple variable+value, resolve
        # operands and hand them to the override directly.
        if (
            operator_override_fns
            and operator in operator_override_fns
            and (
                has_nested_expression
                or len(child_operands) != 2
                or not all("variable" in o or "value" in o for o in child_operands)
            )
        ):
            resolved = [resolve_operand(o) for o in child_operands]
            if len(resolved) == 1:
                return operator_override_fns[operator](resolved[0], None)
            if len(resolved) == 2:
                return operator_override_fns[operator](resolved[0], resolved[1])
            return operator_override_fns[operator](*resolved)

        # Boolean leaf operators take exactly two operands. Either side may be
        # a nested value-producing expression (arithmetic, cast, ternary, ...).
        if len(child_operands) == 2 and has_nested_expression:
            left = resolve_operand(child_operands[0])
            right = resolve_operand(child_operands[1])
            return get_operator_fn(operator, left, right)

        # otherwise, they are a list[dict] (len==2), each operand a `variable` or a
        # `value`. The order is NOT guaranteed to be variable-first: the planner
        # preserves policy source order (`1 < R.attr.x` arrives value-first).
        left_operand, right_operand = child_operands

        # Field-to-field: both sides are columns (`R.attr.a == R.attr.b`).
        # Wire order is preserved; SQL three-valued logic keeps rows with a
        # NULL side excluded, matching CEL's missing-attribute deny.
        if "variable" in left_operand and "variable" in right_operand:
            left_column = resolve_variable(left_operand["variable"])
            right_column = resolve_variable(right_operand["variable"])
            # Mixing the two conventions across one comparison has no faithful
            # rendering. The declared side needs a definite answer for its NULL
            # (CEL holds a null VALUE); the undeclared side needs UNKNOWN for its
            # NULL (a missing attribute, which CEL denies under both polarities).
            # A definite predicate returns rows the PDP refuses; a plain one drops
            # rows the PDP allows. Refuse it rather than pick a direction --
            # declare both attributes, or neither.
            left_explicit = is_explicit_null(left_operand["variable"])
            right_explicit = is_explicit_null(right_operand["variable"])
            if left_explicit != right_explicit and operator in ("eq", "ne"):
                raise ValueError(
                    f"Cannot translate `{operator}` between two columns under mixed "
                    "null conventions: cannot compare an attribute declared "
                    "explicit-null with one on the omitted convention: the omitted "
                    "side is UNKNOWN for a NULL column while the declared side is "
                    "definite, and no single predicate is both. Declare "
                    "attribute_null_representation for both attributes, or for "
                    "neither."
                )
            both_explicit = left_explicit and right_explicit
            return with_null_conventions(
                operator,
                left_column,
                right_column,
                both_explicit,
                both_explicit,
                get_operator_fn(operator, left_column, right_column),
            )

        if "value" in left_operand and "variable" in right_operand:
            value = left_operand["value"]
            column = resolve_variable(right_operand["variable"])
            explicit = is_explicit_null(right_operand["variable"])
            if operator in _MIRRORED_OPERATORS:
                # Directional: `1 < R.attr.x` means `x > 1`.
                return get_operator_fn(_MIRRORED_OPERATORS[operator], column, value)
            if operator in _ORDER_INSENSITIVE_OPERATORS:
                return with_null_conventions(
                    operator,
                    column,
                    value,
                    explicit,
                    False,
                    get_operator_fn(operator, column, value),
                )
            # Receiver-sensitive (contains/startsWith/endsWith/...): keep wire
            # order — the value is the receiver, the column the argument.
            return get_operator_fn(operator, value, column)

        if "value" in left_operand and "value" in right_operand:
            # Both sides constant (rare; the planner usually folds these).
            return get_operator_fn(
                operator, left_operand["value"], right_operand["value"]
            )

        column = resolve_variable(left_operand["variable"])
        value = right_operand["value"]

        # the operator handlers here are the leaf nodes of the recursion
        return with_null_conventions(
            operator,
            column,
            value,
            is_explicit_null(left_operand["variable"]),
            False,
            get_operator_fn(operator, column, value),
        )

    condition = traverse_and_map_operands(cond)
    # The root of the plan must translate to a boolean SQL expression. A non-boolean root —
    # filter()/map() as the whole condition, or an operator override's intermediate value that
    # no enclosing override consumed — must be refused HERE, by the adapter, rather than left
    # for SQLAlchemy's where() coercion to trip over: a value that happened to coerce would
    # become a silently-wrong filter.
    #
    # `InstrumentedAttribute` is accepted alongside `ColumnElement` because a bare boolean
    # COLUMN is a legitimate root: `R.attr.aBool` alone plans to a condition that is a bare
    # `{"variable": ...}` with no expression wrapper at all (`root-bare-bool`). The ORM
    # attribute is a descriptor rather than a Core element, so it fails the ColumnElement
    # check while being exactly what `where()` wants — and the same operand has always been
    # accepted one level down, as an `and`/`or`/`not` child (`nary-and`). Refusing it only at
    # the root made the position, not the shape, decide (cerbos/query-plan-adapters#388).
    #
    # This widens nothing that a mapper could not already reach: `GenericColumn` is
    # `Column | InstrumentedAttribute`, and a Core `Column` IS a `ColumnElement`, so a mapper
    # holding one has always been accepted here. The check was discriminating by which of the
    # two flavours the mapper happened to hold, not by whether the root was boolean.
    if not isinstance(condition, (ColumnElement, InstrumentedAttribute, bool)):
        raise ValueError(
            f"the plan's condition translated to {type(condition).__name__!r}, which is not "
            "a boolean SQL expression. filter() and map() return a list, so they cannot be a "
            "condition on their own (only size(filter(...)) has a boolean meaning), and an "
            "operator override returning an intermediate value must be consumed by an "
            "enclosing override before the root"
        )
    q = select(table).where(condition)

    if table_mapping:
        q = q.select_from(table)
        for join_table, predicate in table_mapping:
            q = q.join(join_table, predicate)

    return q
