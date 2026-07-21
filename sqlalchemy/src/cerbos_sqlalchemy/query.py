from types import MappingProxyType
from typing import Any, Callable, Dict, List, Tuple, Union

from cerbos.engine.v1 import engine_pb2
from cerbos.response.v1 import response_pb2
from cerbos.sdk.model import PlanResourcesFilterKind, PlanResourcesResponse
from google.protobuf.json_format import MessageToDict

from sqlalchemy import (
    Column,
    Float,
    Integer,
    String,
    Table,
    and_,
    case,
    cast,
    func,
    literal,
    not_,
    or_,
    select,
)
from sqlalchemy.orm import DeclarativeMeta, InstrumentedAttribute
from sqlalchemy.sql import Select
from sqlalchemy.sql.expression import BinaryExpression, ColumnOperators

GenericTable = Union[Table, DeclarativeMeta]
GenericColumn = Union[Column, InstrumentedAttribute]
GenericExpression = Union[BinaryExpression, ColumnOperators]
OperatorFnMap = Dict[str, Callable[[GenericColumn, Any], GenericExpression]]


_LIKE_ESCAPE_CHAR = "\\"


def _escape_like_literal(needle: str) -> str:
    """Escape LIKE metacharacters in a literal needle so `% _ \\` match literally."""
    return (
        needle.replace(_LIKE_ESCAPE_CHAR, _LIKE_ESCAPE_CHAR * 2)
        .replace("%", _LIKE_ESCAPE_CHAR + "%")
        .replace("_", _LIKE_ESCAPE_CHAR + "_")
    )


def _escape_like_column(needle: Any) -> Any:
    """Escape LIKE metacharacters in a column-valued needle at query time.

    A NULL needle propagates through REPLACE to a NULL pattern, so the LIKE
    stays UNKNOWN and the row is excluded — matching CEL's missing-attribute
    error (deny) for the same row.
    """
    escaped = func.replace(needle, _LIKE_ESCAPE_CHAR, _LIKE_ESCAPE_CHAR * 2)
    escaped = func.replace(escaped, "%", _LIKE_ESCAPE_CHAR + "%")
    return func.replace(escaped, "_", _LIKE_ESCAPE_CHAR + "_", type_=String)


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
    if isinstance(c, bool) or not isinstance(c, (int, float)):
        c = cast(c, Float)
    else:
        c = float(c)
    return c / v


# We want to make the base dict "immutable", and enforce explicit (optional) overrides on
# each call to `get_query` (rather than allowing keys in this dict to be overridden, which
# could wreak havoc if different calls from the same memory space weren't aware of each other's
# overrides)
__operator_fns: OperatorFnMap = {
    "eq": lambda c, v: c == v,  # c, v denotes column, value respectively
    "ne": lambda c, v: c != v,
    "lt": lambda c, v: c < v,
    "gt": lambda c, v: c > v,
    "le": lambda c, v: c <= v,
    "ge": lambda c, v: c >= v,
    "in": lambda c, v: c.in_([v]) if not isinstance(v, list) else c.in_(v),
    # Arithmetic operators — return value expressions (not boolean), composed
    # inside parent comparisons like gt(add(col, 1), 2).
    "add": lambda c, v: c + v,
    "sub": lambda c, v: c - v,
    "mult": lambda c, v: c * v,
    "div": _float_div,
    "mod": lambda c, v: c % v,
    # String matching — portable across most SQLAlchemy dialects.
    "matches": lambda c, v: c.regexp_match(v),
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
_UNARY_VALUE_OPERATORS = frozenset({"string", "double", "int", "size"})

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

    # Inspect passed columns. If > 1 origin table, assert that the mapping has been defined.
    # Skipped when operator overrides are supplied: overrides commonly translate
    # relation traversals into correlated subqueries (EXISTS/COUNT), where columns
    # from other tables — or non-column marker objects — are legitimate without a
    # flat join, and a forced `table_mapping` JOIN would change row multiplicity.
    if operator_override_fns is None:
        required_tables = set()
        for c in attr_map.values():
            # c is of type Union[Column, InstrumentedAttribute] - both have a `table` attribute returning a `Table` type
            if (n := c.table.name) != _get_table_name(table):
                required_tables.add(n)

        if len(required_tables):
            if table_mapping is None:
                raise TypeError(
                    "get_query() missing 1 required positional argument: 'table_mapping'"
                )
            for t, _ in table_mapping:
                required_tables.discard(_get_table_name(t))
            if len(required_tables):
                raise TypeError(
                    "positional argument 'table_mapping' missing mapping for table(s): '{0}'".format(
                        "', '".join(required_tables)
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
            return case((cond, then_value), (not_(cond), else_value))

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

    cond = (
        MessageToDict(query_plan.filter.condition)
        if isinstance(query_plan, response_pb2.PlanResourcesResponse)
        else query_plan.filter.condition.to_dict()
    )

    q = select(table).where(traverse_and_map_operands(cond))

    if table_mapping:
        q = q.select_from(table)
        for join_table, predicate in table_mapping:
            q = q.join(join_table, predicate)

    return q
