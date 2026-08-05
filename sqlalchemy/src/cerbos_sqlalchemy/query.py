import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any, Callable, Dict, List, Tuple, Union

from cerbos.engine.v1 import engine_pb2
from cerbos.response.v1 import response_pb2
from cerbos.sdk.model import PlanResourcesFilterKind, PlanResourcesResponse
from google.protobuf.json_format import MessageToDict

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Integer,
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
from sqlalchemy.sql.expression import BinaryExpression, ColumnOperators

GenericTable = Union[Table, DeclarativeMeta]
GenericColumn = Union[Column, InstrumentedAttribute]
GenericExpression = Union[BinaryExpression, ColumnOperators]
OperatorFnMap = Dict[str, Callable[[GenericColumn, Any], GenericExpression]]


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
                return _IEEEConstant(math.nan)
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
    return _ConditionalValue(
        condition=denominator == 0.0,
        then_value=_ConditionalValue(
            condition=numerator == 0.0,
            then_value=_IEEEConstant(math.nan),
            else_value=_ConditionalValue(
                condition=numerator > 0.0,
                then_value=_IEEEConstant(math.inf),
                else_value=_IEEEConstant(-math.inf),
            ),
        ),
        else_value=numerator / func.nullif(denominator, 0.0),
    )


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
    # Type conversions — value-returning expressions.
    "string": lambda c, _: cast(c, String),
    "double": lambda c, _: cast(c, Float),
    "int": lambda c, _: cast(c, Integer),
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
        # `DeclarativeMeta` type
        return t.__table__.name
    except AttributeError:
        # `Table` type
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


def get_query(
    query_plan: Union[PlanResourcesResponse, response_pb2.PlanResourcesResponse],  # type: ignore (https://github.com/microsoft/pyright/issues/1035)
    table: GenericTable,
    attr_map: Dict[str, GenericColumn],
    table_mapping: Union[List[Tuple[GenericTable, GenericExpression]], None] = None,
    operator_override_fns: Union[OperatorFnMap, None] = None,
) -> Select:
    if query_plan.filter is None or query_plan.filter.kind in _deny_types:
        return select(table).where(False)

    if query_plan.filter.kind in _allow_types:
        return select(table)

    cond = (
        MessageToDict(query_plan.filter.condition)
        if isinstance(query_plan, response_pb2.PlanResourcesResponse)
        else query_plan.filter.condition.to_dict()
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
            return get_operator_fn(
                operator,
                resolve_variable(left_operand["variable"]),
                resolve_variable(right_operand["variable"]),
            )

        if "value" in left_operand and "variable" in right_operand:
            value = left_operand["value"]
            column = resolve_variable(right_operand["variable"])
            if operator in _MIRRORED_OPERATORS:
                # Directional: `1 < R.attr.x` means `x > 1`.
                return get_operator_fn(_MIRRORED_OPERATORS[operator], column, value)
            if operator in _ORDER_INSENSITIVE_OPERATORS:
                return get_operator_fn(operator, column, value)
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
        return get_operator_fn(operator, column, value)

    q = select(table).where(traverse_and_map_operands(cond))

    if table_mapping:
        q = q.select_from(table)
        for join_table, predicate in table_mapping:
            q = q.join(join_table, predicate)

    return q
