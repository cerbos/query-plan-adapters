# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Translates a plan's condition tree into one Polars expression.

A Polars null is CEL's error. Comparisons propagate it, ``&`` and ``|`` are Kleene, as
CEL's ``&&`` and ``||`` are over errors, and a filter drops a row whose predicate is null,
as Cerbos denies a row whose condition errors. Where CEL also has a null *value* (an
attribute the caller sends as an explicit null, or a null list element), an operand
carries ``err``: an expression that is true exactly where the operand is an error, so the
two nulls never meet in one column.
"""

from __future__ import annotations

import math
import re
import time
from collections.abc import Callable, Container
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any

import polars as pl
from polars.datatypes import DataTypeClass

from cerbos_polars._plan import Expr, Operand, Value, Variable
from cerbos_polars._regex import translate_regex
from cerbos_polars.errors import UnsupportedPlanError

_NO = object()

RESOURCE_ATTR = "request.resource.attr."
RESOURCE_ID = "request.resource.id"

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
_CEL_TS_MIN = -62135596800 * 10**9
_CEL_TS_MAX = 253402300799 * 10**9 + 999999999

_UPPER = [chr(c) for c in range(ord("A"), ord("Z") + 1)]
_LOWER = [chr(c) for c in range(ord("a"), ord("z") + 1)]


@dataclass(frozen=True)
class Op:
    """A translated operand.

    ``kind`` is the CEL type: ``bool``, ``double``, ``int``, ``string``, ``null``, ``list``,
    ``map``, ``timestamp``, ``duration``, ``hierarchy``, or ``tsattr`` (a datetime column
    standing for an attribute CEL reads as an RFC 3339 string). ``err`` is None when a
    null ``expr`` always means an error. ``depth`` is the lambda nesting the expression is
    valid at: 0 for the frame's own columns.
    """

    expr: pl.Expr | None
    kind: str
    dtype: DType | None = None
    err: pl.Expr | None = None
    const: Any = _NO
    elem: Op | None = None
    items: tuple[Op, ...] | None = None
    fields: dict[str, Op] | None = None
    depth: int = 0
    delimiter: str | None = None
    unit: str | None = None
    intlike: bool = False

    @property
    def is_const(self) -> bool:
        return self.const is not _NO


def _refuse(message: str) -> UnsupportedPlanError:
    return UnsupportedPlanError(message)


_BOOL_NULL = pl.lit(None, dtype=pl.Boolean)

DType = pl.DataType | DataTypeClass


def _kind_of(dtype: DType) -> str:
    if dtype == pl.Boolean:
        return "bool"
    if dtype.is_numeric():
        return "double"
    if dtype == pl.String:
        return "string"
    if isinstance(dtype, pl.List):
        return "list"
    if isinstance(dtype, pl.Struct):
        return "map"
    if isinstance(dtype, pl.Datetime):
        return "tsattr"
    if dtype == pl.Null:
        return "null"
    return "unknown"


def _error(op: Op) -> pl.Expr:
    """True where ``op`` is an error, never null."""
    assert op.expr is not None
    if op.is_const:
        return pl.lit(False)
    if op.err is None:
        return op.expr.is_null()
    return op.err.fill_null(True)


def _checks(op: Op) -> list[pl.Expr]:
    if op.is_const:
        return []
    if op.items is not None:
        return [check for item in op.items for check in _checks(item)]
    if op.fields is not None:
        return [check for item in op.fields.values() for check in _checks(item)]
    return [_error(op)]


def _any_error(*ops: Op) -> pl.Expr | None:
    checks = [check for op in ops for check in _checks(op)]
    if not checks:
        return None
    any_error = checks[0]
    for check in checks[1:]:
        any_error = any_error | check
    return any_error


def _guard(result: pl.Expr, *ops: Op) -> pl.Expr:
    """``result``, or null where any operand is an error."""
    any_error = _any_error(*ops)
    if any_error is None:
        return result
    return pl.when(any_error).then(None).otherwise(result)


def _strict(op: Op) -> Op:
    """``op`` where a null value is an error: every function but equality rejects null."""
    if op.err is None or op.is_const or op.expr is None:
        return op
    return replace(op, err=None)


def _value_nullable(op: Op) -> bool:
    return op.err is not None and not op.is_const


def _per_element(expr: pl.Expr) -> pl.Expr:
    """``expr`` evaluated once per list element, even when it does not read the element."""
    return pl.when(pl.element().is_null() | pl.element().is_not_null()).then(expr)


def _depth(*ops: Op) -> int:
    return max((op.depth for op in ops), default=0)


def _bool(expr: pl.Expr, *ops: Op) -> Op:
    return Op(expr, "bool", pl.Boolean, depth=_depth(*ops))


def _const_null_bool(*ops: Op) -> Op:
    return Op(_BOOL_NULL, "bool", pl.Boolean, depth=_depth(*ops))


def _is_numeric(op: Op) -> bool:
    return op.kind in ("double", "int")


def _integral_const(op: Op) -> bool:
    if op.intlike:
        return True
    value = op.const
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    return isinstance(value, float) and math.isfinite(value) and value.is_integer()


def _parse_timestamp(text: str) -> int | None:
    """RFC 3339 as Go's ``time.Parse(time.RFC3339, s)`` reads it, in Unix nanoseconds."""
    m = re.fullmatch(
        r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})",
        text,
    )
    if not m:
        return None
    year, month, day, hour, minute, second = (int(m.group(i)) for i in range(1, 7))
    try:
        base = datetime(year, month, day, hour, minute, second, tzinfo=UTC)
    except ValueError:
        return None
    fraction = m.group(7)
    nanos = int((fraction[1:] + "000000000")[:9]) if fraction else 0
    zone = m.group(8)
    offset = 0
    if zone != "Z":
        hours, minutes = int(zone[1:3]), int(zone[4:6])
        if hours > 23 or minutes > 59:
            return None
        offset = (hours * 3600 + minutes * 60) * (1 if zone[0] == "+" else -1)
    seconds = int((base - datetime(1970, 1, 1, tzinfo=UTC)).total_seconds()) - offset
    return seconds * 10**9 + nanos


def _parse_duration(text: str) -> int | None:
    m = re.fullmatch(r"(-?)(\d+)(?:\.(\d{1,9}))?s", text)
    if not m:
        return None
    nanos = int(m.group(2)) * 10**9 + int(((m.group(3) or "") + "000000000")[:9])
    return -nanos if m.group(1) else nanos


@dataclass
class Settings:
    schema: pl.Schema
    attr_map: dict[str, pl.Expr]
    explicit: Container[str]
    now_ns: int = field(default_factory=time.time_ns)


class Translator:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._frame = pl.LazyFrame(schema=settings.schema)

    def predicate(self, node: Operand) -> pl.Expr:
        op = self.translate(node, {}, 0)
        return self.as_bool(op)

    def as_bool(self, op: Op) -> pl.Expr:
        if op.kind == "bool":
            assert op.expr is not None
            return op.expr
        if op.kind == "null" or op.kind in ("double", "int", "string", "list", "map"):
            return _BOOL_NULL
        raise _refuse(f"Cannot use a {op.kind} value as a condition")

    def _dtype(self, expr: pl.Expr) -> pl.DataType:
        try:
            return self._frame.select(expr.alias("__cerbos__")).collect_schema()[
                "__cerbos__"
            ]
        except pl.exceptions.ColumnNotFoundError as error:
            raise KeyError(
                f"{expr} reads a column the frame does not have: {error}"
            ) from error
        except pl.exceptions.PolarsError as error:
            raise TypeError(f"Cannot resolve the type of {expr}: {error}") from error

    def _column(self, expr: pl.Expr, explicit: bool) -> Op:
        dtype = self._dtype(expr)
        return self._typed(expr, dtype, pl.lit(False) if explicit else None, 0)

    def _typed(
        self, expr: pl.Expr, dtype: DType, err: pl.Expr | None, depth: int
    ) -> Op:
        if isinstance(dtype, (pl.Categorical, pl.Enum)):
            expr, dtype = expr.cast(pl.String), pl.String()
        elif isinstance(dtype, pl.Array):
            expr, dtype = expr.arr.to_list(), pl.List(dtype.inner)
        kind = _kind_of(dtype)
        elem = None
        if isinstance(dtype, pl.List):
            elem = self._typed(pl.element(), dtype.inner, pl.lit(False), depth + 1)
        unit = dtype.time_unit if isinstance(dtype, pl.Datetime) else None
        return Op(expr, kind, dtype, err=err, elem=elem, depth=depth, unit=unit)

    def attribute(self, name: str) -> Op:
        explicit = name in self.settings.explicit
        attr_map = self.settings.attr_map
        if name == RESOURCE_ID:
            return self._column(attr_map.get(name, pl.col("id")), explicit)
        if not name.startswith(RESOURCE_ATTR):
            raise _refuse(
                f"Cannot translate a reference to {name}: only resource attributes are columns"
            )
        if name in attr_map:
            return self._column(attr_map[name], explicit)
        path = name[len(RESOURCE_ATTR) :].split(".")
        for cut in range(len(path) - 1, 0, -1):
            prefix = RESOURCE_ATTR + ".".join(path[:cut])
            if prefix in attr_map:
                base = self.attribute(prefix)
                return self._fields(base, path[cut:], name, explicit)
        column = path[0]
        if column not in self.settings.schema:
            raise KeyError(
                f"{name} is not in attr_map and the frame has no column {column!r}"
            )
        base = self._column(
            pl.col(column), RESOURCE_ATTR + column in self.settings.explicit
        )
        return self._fields(base, path[1:], name, explicit)

    def _fields(self, base: Op, names: list[str], full: str, explicit: bool) -> Op:
        op = base
        parent_error: pl.Expr = pl.lit(False)
        for segment in names:
            if op.kind != "map":
                raise KeyError(
                    f"{full}: {segment!r} is read from a {op.kind}, not a struct column"
                )
            assert op.expr is not None
            parent_error = parent_error | op.expr.is_null()
            op = self.field(op, segment, strict=True)
        if names and explicit:
            op = replace(op, err=parent_error)
        return op

    def field(self, op: Op, name: str, strict: bool = False) -> Op:
        if op.fields is not None:
            if name not in op.fields:
                return Op(pl.lit(None), "null", pl.Null, depth=op.depth)
            return op.fields[name]
        if op.kind != "map":
            if op.kind == "null":
                return Op(pl.lit(None), "null", pl.Null, depth=op.depth)
            raise _refuse(f"Cannot read field {name!r} of a {op.kind}")
        assert isinstance(op.dtype, pl.Struct) and op.expr is not None
        names = {f.name: f.dtype for f in op.dtype.fields}
        if name not in names:
            if strict:
                raise KeyError(f"The struct has no field {name!r}")
            return Op(pl.lit(None), "null", pl.Null, depth=op.depth)
        return self._typed(op.expr.struct.field(name), names[name], None, op.depth)

    def translate(self, node: Operand, env: dict[str, Op], depth: int) -> Op:
        if isinstance(node, Value):
            return self.literal(node.value)
        if isinstance(node, Variable):
            op = self.variable(node.name, env, depth)
            if 0 < op.depth < depth:
                raise _refuse(
                    "A lambda body reads the element of an enclosing lambda, and Polars' "
                    "list.eval cannot reach an outer element from an inner one"
                )
            return op
        handler = getattr(self, "_op_" + node.operator.replace("-", "_"), None)
        if handler is None:
            raise _refuse(f"Unsupported operator: {node.operator}")
        result = handler(node, env, depth)
        if result.depth not in (0, depth) and result.depth < depth:
            raise _refuse(
                "A lambda body reads the element of an enclosing lambda, and Polars' "
                "list.eval cannot reach an outer element from an inner one"
            )
        return result

    def args(self, node: Expr, env: dict[str, Op], depth: int, count: int) -> list[Op]:
        if len(node.operands) != count:
            raise _refuse(
                f"{node.operator} takes {count} operands, got {len(node.operands)}"
            )
        ops = [self.translate(child, env, depth) for child in node.operands]
        for op in ops:
            if 0 < op.depth < depth:
                raise _refuse(
                    "A lambda body reads the element of an enclosing lambda, and Polars' "
                    "list.eval cannot reach an outer element from an inner one"
                )
        return ops

    def variable(self, name: str, env: dict[str, Op], depth: int) -> Op:
        if name in env:
            return env[name]
        head, _, rest = name.partition(".")
        if head in env and rest:
            op = env[head]
            for segment in rest.split("."):
                op = self.field(op, segment)
            return op
        return self.attribute(name)

    def literal(self, value: Any) -> Op:
        if value is None:
            return Op(pl.lit(None), "null", pl.Null, err=pl.lit(False), const=None)
        if isinstance(value, bool):
            return Op(pl.lit(value, dtype=pl.Boolean), "bool", pl.Boolean, const=value)
        if isinstance(value, (int, float)):
            if isinstance(value, int) and not _INT64_MIN <= value <= _INT64_MAX:
                value = float(value)
            intlike = (
                isinstance(value, int)
                or math.isfinite(value)
                and float(value).is_integer()
            )
            return Op(
                pl.lit(float(value), dtype=pl.Float64),
                "double",
                pl.Float64,
                const=value,
                intlike=intlike,
            )
        if isinstance(value, str):
            return Op(pl.lit(value, dtype=pl.String), "string", pl.String, const=value)
        if isinstance(value, list):
            items = tuple(self.literal(v) for v in value)
            return Op(None, "list", None, const=value, items=items)
        if isinstance(value, dict):
            return Op(
                None,
                "map",
                None,
                const=value,
                fields={k: self.literal(v) for k, v in value.items()},
            )
        raise _refuse(f"Unsupported literal: {value!r}")

    def _op_and(self, node: Expr, env, depth) -> Op:
        return self._logic(node, env, depth, lambda a, b: a & b)

    def _op_or(self, node: Expr, env, depth) -> Op:
        return self._logic(node, env, depth, lambda a, b: a | b)

    def _logic(self, node: Expr, env, depth, combine) -> Op:
        if len(node.operands) < 2:
            raise _refuse(f"{node.operator} takes at least two operands")
        ops = [self.translate(child, env, depth) for child in node.operands]
        exprs = [self.as_bool(op) for op in ops]
        result = exprs[0]
        for expr in exprs[1:]:
            result = combine(result, expr)
        return _bool(result, *ops)

    def _op_not(self, node: Expr, env, depth) -> Op:
        (op,) = self.args(node, env, depth, 1)
        return _bool(~self.as_bool(op), op)

    def _op_if(self, node: Expr, env, depth) -> Op:
        cond, then, other = self.args(node, env, depth, 3)
        test = self.as_bool(cond)
        if (
            then.kind != other.kind
            and "null" not in (then.kind, other.kind)
            and not (
                _is_numeric(then)
                and _is_numeric(other)
                and (then.intlike or other.intlike)
            )
        ):
            raise _refuse(
                f"A ternary whose branches are {then.kind} and {other.kind} has no single Polars type"
            )
        if then.expr is None or other.expr is None:
            raise _refuse("A ternary over list or map literals is not translated")
        kind = then.kind if then.kind != "null" else other.kind
        if _is_numeric(then) and _is_numeric(other) and then.kind != other.kind:
            kind = then.kind if not then.intlike else other.kind
            then, other = self._numeric(then, kind), self._numeric(other, kind)
        if kind in ("list", "map", "hierarchy") and then.dtype != other.dtype:
            raise _refuse(
                "A ternary over two differently typed collections is not translated"
            )
        expr = (
            pl.when(test.is_null())
            .then(None)
            .when(test)
            .then(then.expr)
            .otherwise(other.expr)
        )
        err = None
        if any(_value_nullable(op) or op.kind == "null" for op in (then, other)):
            err = (
                pl.when(test.is_null())
                .then(True)
                .when(test)
                .then(_error(then))
                .otherwise(_error(other))
            )
        dtype = then.dtype if then.kind != "null" else other.dtype
        return Op(
            expr,
            kind,
            dtype,
            err=err,
            elem=then.elem or other.elem,
            depth=_depth(cond, then, other),
            delimiter=then.delimiter,
            unit=then.unit or other.unit,
            intlike=then.intlike and other.intlike,
        )

    def _op_dyn(self, node: Expr, env, depth) -> Op:
        (op,) = self.args(node, env, depth, 1)
        return op

    def _op_eq(self, node: Expr, env, depth) -> Op:
        a, b = self.args(node, env, depth, 2)
        return self.equals(a, b)

    def _op_ne(self, node: Expr, env, depth) -> Op:
        a, b = self.args(node, env, depth, 2)
        eq = self.equals(a, b)
        assert eq.expr is not None
        return _bool(~eq.expr, eq)

    def equals(self, a: Op, b: Op) -> Op:
        for op, other in ((a, b), (b, a)):
            if op.kind == "tsattr" and other.kind not in (
                "string",
                "tsattr",
                "null",
                "unknown",
            ):
                return _bool(_guard(pl.lit(False), a, b), a, b)
            if op.kind in ("tsattr",):
                raise _refuse(
                    "A datetime column stands for an RFC 3339 string attribute, and CEL "
                    "compares that string as written; wrap it in timestamp() to compare instants"
                )
        if a.kind == "null" or b.kind == "null":
            if a.kind == "null" and b.kind == "null":
                return _bool(_guard(pl.lit(True), a, b), a, b)
            other = b if a.kind == "null" else a
            null = a if a.kind == "null" else b
            if other.expr is None:
                return _bool(_guard(pl.lit(False), other, null), a, b)
            if _value_nullable(other):
                return _bool(_guard(other.expr.is_null(), other, null), a, b)
            return _bool(_guard(pl.lit(False), other, null), a, b)
        if a.kind != b.kind and not (_is_numeric(a) and _is_numeric(b)):
            if "unknown" in (a.kind, b.kind):
                raise _refuse("Cannot compare a value of an unsupported column type")
            return _bool(_guard(pl.lit(False), a, b), a, b)
        if a.kind == "list":
            return self._list_equals(a, b)
        if a.kind == "map":
            raise _refuse("Equality between two maps is not translated")
        if a.kind == "hierarchy" and a.delimiter != b.delimiter:
            raise _refuse("Hierarchies with different delimiters are not compared")
        if a.kind == "unknown":
            raise _refuse("Cannot compare a value of an unsupported column type")
        nan = None
        if _is_numeric(a):
            ae, be, nan = self._numeric_pair(a, b)
        else:
            ae, be = a.expr, b.expr
            if a.kind == "timestamp":
                ae, be = self._same_unit(a, b)
        assert ae is not None and be is not None
        if _value_nullable(a) or _value_nullable(b):
            result = ae.eq_missing(be)
        else:
            result = ae == be
        if nan is not None:
            result = pl.when(nan).then(False).otherwise(result)
        return _bool(_guard(result, a, b), a, b)

    def _list_equals(self, a: Op, b: Op) -> Op:
        """CEL list equality: the same length, and each position equal."""
        if a.items is None and b.items is None:
            raise _refuse("Equality between two list columns is not translated")
        literal, other = (a, b) if a.items is not None else (b, a)
        assert literal.items is not None
        size = len(literal.items)
        if other.items is not None:
            if len(other.items) != size:
                return _bool(_guard(pl.lit(False), a, b), a, b)
            tests = [self.equals(x, y) for x, y in zip(other.items, literal.items)]
            result: pl.Expr = pl.lit(True)
            for test in tests:
                assert test.expr is not None
                result = result & test.expr
            return _bool(_guard(result, a, b), a, b)
        assert other.expr is not None
        result = pl.lit(True)
        for i, item in enumerate(literal.items):
            element = self._list_index(
                other, Op(pl.lit(i, dtype=pl.Int64), "int", pl.Int64, const=i)
            )
            test = self.equals(replace(element, err=pl.lit(False)), item)
            assert test.expr is not None
            result = result & test.expr
        length = other.expr.list.len()
        expr = pl.when(length != size).then(False).otherwise(result)
        if _value_nullable(other):
            expr = pl.when(other.expr.is_null()).then(False).otherwise(expr)
        return _bool(_guard(expr, a, b), a, b)

    def _numeric(self, op: Op, kind: str) -> Op:
        assert op.expr is not None
        if kind == "int":
            if op.kind == "int":
                return op
            if op.is_const and _integral_const(op):
                return replace(
                    op,
                    expr=pl.lit(int(op.const), dtype=pl.Int64),
                    kind="int",
                    dtype=pl.Int64,
                )
            if op.intlike:
                return replace(
                    op, expr=op.expr.cast(pl.Int64), kind="int", dtype=pl.Int64
                )
            raise _refuse(
                "Cannot mix an int with a double that is not an integral literal"
            )
        return replace(
            op, expr=op.expr.cast(pl.Float64), kind="double", dtype=pl.Float64
        )

    def _numeric_pair(self, a: Op, b: Op) -> tuple[pl.Expr, pl.Expr, pl.Expr | None]:
        """Two numbers made comparable, and where either is NaN (which compares false).

        CEL compares an int with a double exactly. A Float64 cast would round an int past
        2^53, so a mixed pair compares the int with the double's integral part in Int128,
        and breaks a tie on the double's fraction.
        """
        assert a.expr is not None and b.expr is not None
        if a.kind == "int" and b.kind == "int":
            return a.expr, b.expr, None
        if (
            a.kind == "int"
            and b.intlike
            and b.is_const
            and _INT64_MIN <= b.const <= _INT64_MAX
        ):
            return a.expr, pl.lit(int(b.const), dtype=pl.Int64), None
        if (
            b.kind == "int"
            and a.intlike
            and a.is_const
            and _INT64_MIN <= a.const <= _INT64_MAX
        ):
            return pl.lit(int(a.const), dtype=pl.Int64), b.expr, None
        if a.kind == "int" or b.kind == "int":
            flip = b.kind == "int"
            i, d = (b.expr, a.expr) if flip else (a.expr, b.expr)
            d = d.cast(pl.Float64)
            whole = pl.when(d >= 0).then(d.floor()).otherwise(d.ceil())
            in_range = (d < 2.0**63) & (d >= -(2.0**63))
            diff = i.cast(pl.Int128) - pl.when(in_range).then(whole).otherwise(
                0.0
            ).cast(pl.Int128)
            sign = (
                pl.when(i.is_null() | d.is_null())
                .then(None)
                .when(d >= 2.0**63)
                .then(-1)
                .when(d < -(2.0**63))
                .then(1)
                .when(diff > 0)
                .then(1)
                .when(diff < 0)
                .then(-1)
                .otherwise((0.0 - (d - whole).sign()).cast(pl.Int64))
                .cast(pl.Int64)
            )
            if flip:
                sign = -sign
            return sign, pl.lit(0, dtype=pl.Int64), d.is_nan()
        ae, be = a.expr.cast(pl.Float64), b.expr.cast(pl.Float64)
        return ae, be, ae.is_nan() | be.is_nan()

    def _same_unit(self, a: Op, b: Op) -> tuple[pl.Expr, pl.Expr]:
        assert a.expr is not None and b.expr is not None
        return a.expr.cast(pl.Datetime("ns", "UTC")), b.expr.cast(
            pl.Datetime("ns", "UTC")
        )

    def _ordering(self, node: Expr, env, depth, compare) -> Op:
        a, b = self.args(node, env, depth, 2)
        for op, other in ((a, b), (b, a)):
            if op.kind == "tsattr" and other.kind not in (
                "string",
                "tsattr",
                "unknown",
            ):
                return self._unordered(a, b)
            if op.kind == "tsattr":
                raise _refuse(
                    "A datetime column stands for an RFC 3339 string attribute; wrap it in "
                    "timestamp() to compare instants"
                )
            if op.kind == "unknown":
                raise _refuse("Cannot order a value of an unsupported column type")
        comparable = (_is_numeric(a) and _is_numeric(b)) or (
            a.kind == b.kind and a.kind in ("string", "bool", "timestamp", "duration")
        )
        if not comparable:
            return self._unordered(a, b)
        nan = None
        if _is_numeric(a):
            ae, be, nan = self._numeric_pair(a, b)
        elif a.kind == "timestamp":
            ae, be = self._same_unit(a, b)
        else:
            ae, be = a.expr, b.expr
        assert ae is not None and be is not None
        result = compare(ae, be)
        if nan is not None:
            result = (
                pl.when(ae.is_null() | be.is_null())
                .then(None)
                .when(nan)
                .then(False)
                .otherwise(result)
            )
        return _bool(result, a, b)

    def _unordered(self, a: Op, b: Op) -> Op:
        """Ordering across types errors in CEL, except that a NaN orders false against anything."""
        result: pl.Expr = _BOOL_NULL
        for op in (a, b):
            if op.kind == "double" and op.expr is not None:
                result = pl.when(op.expr.is_nan()).then(False).otherwise(result)
        return _bool(result, a, b)

    def _op_lt(self, node, env, depth) -> Op:
        return self._ordering(node, env, depth, lambda a, b: a < b)

    def _op_le(self, node, env, depth) -> Op:
        return self._ordering(node, env, depth, lambda a, b: a <= b)

    def _op_gt(self, node, env, depth) -> Op:
        return self._ordering(node, env, depth, lambda a, b: a > b)

    def _op_ge(self, node, env, depth) -> Op:
        return self._ordering(node, env, depth, lambda a, b: a >= b)

    def _arith_kind(self, a: Op, b: Op) -> str | None:
        if a.kind == "int" and b.kind == "int":
            return "int"
        if a.kind == "int" and _integral_const(b) and b.kind == "double":
            return "int"
        if b.kind == "int" and _integral_const(a) and a.kind == "double":
            return "int"
        if a.kind == "double" and b.kind == "double":
            return "double"
        return None

    def _int_result(self, wide: pl.Expr, *ops: Op) -> Op:
        checked = (
            pl.when((wide < _INT64_MIN) | (wide > _INT64_MAX))
            .then(None)
            .otherwise(wide)
            .cast(pl.Int64)
        )
        return Op(checked, "int", pl.Int64, depth=_depth(*ops))

    def _op_add(self, node: Expr, env, depth) -> Op:
        a, b = self.args(node, env, depth, 2)
        if a.kind == "string" and b.kind == "string":
            assert a.expr is not None and b.expr is not None
            return Op(a.expr + b.expr, "string", pl.String, depth=_depth(a, b))
        if (
            a.kind == "timestamp"
            and b.kind == "duration"
            or a.kind == "duration"
            and b.kind == "timestamp"
        ):
            ts, dur = (a, b) if a.kind == "timestamp" else (b, a)
            return self._shift(ts, dur, 1)
        if a.kind == "duration" and b.kind == "duration":
            return self._duration_sum(a, b, 1)
        if a.kind == "list" and b.kind == "list":
            return self._concat(a, b, depth)
        return self._arith(a, b, "add")

    def _concat(self, a: Op, b: Op, depth: int) -> Op:
        if a.items is not None and b.items is not None:
            return Op(None, "list", None, items=a.items + b.items, depth=_depth(a, b))
        if a.items is not None and not a.items:
            return b
        if b.items is not None and not b.items:
            return a
        a, b = self._materialise(a, depth), self._materialise(b, depth)
        assert a.expr is not None and b.expr is not None
        if a.dtype != b.dtype or a.elem is None:
            raise _refuse(
                "Concatenating lists of different element types is not translated"
            )
        expr = pl.concat_list([a.expr, b.expr])
        expr = pl.when(a.expr.is_null() | b.expr.is_null()).then(None).otherwise(expr)
        return Op(expr, "list", a.dtype, elem=a.elem, depth=_depth(a, b))

    def _op_sub(self, node: Expr, env, depth) -> Op:
        a, b = self.args(node, env, depth, 2)
        if a.kind == "timestamp" and b.kind == "duration":
            return self._shift(a, b, -1)
        if a.kind == "duration" and b.kind == "duration":
            return self._duration_sum(a, b, -1)
        if a.kind == "timestamp" and b.kind == "timestamp":
            raise _refuse("Subtracting two timestamps is not translated")
        return self._arith(a, b, "sub")

    def _op_mult(self, node: Expr, env, depth) -> Op:
        a, b = self.args(node, env, depth, 2)
        return self._arith(a, b, "mult")

    def _op_div(self, node: Expr, env, depth) -> Op:
        a, b = self.args(node, env, depth, 2)
        return self._arith(a, b, "div")

    def _op_mod(self, node: Expr, env, depth) -> Op:
        a, b = self.args(node, env, depth, 2)
        return self._arith(a, b, "mod")

    def _arith(self, a: Op, b: Op, op: str) -> Op:
        if (
            "tsattr" in (a.kind, b.kind)
            and "unknown" not in (a.kind, b.kind)
            and (
                a.kind not in ("tsattr", "string") or b.kind not in ("tsattr", "string")
            )
        ):
            return Op(
                pl.lit(None, dtype=pl.Float64),
                "double",
                pl.Float64,
                depth=_depth(a, b),
            )
        for operand in (a, b):
            if operand.kind in ("tsattr", "unknown"):
                raise _refuse(f"Cannot do arithmetic on a {operand.kind} value")
            if operand.expr is None:
                raise _refuse("Arithmetic over a list or map literal is not translated")
        kind = self._arith_kind(a, b)
        if kind is None:
            if (_is_numeric(a) or a.kind in ("null", "string", "bool")) and (
                _is_numeric(b) or b.kind in ("null", "string", "bool")
            ):
                return Op(
                    pl.lit(None, dtype=pl.Float64),
                    "double",
                    pl.Float64,
                    depth=_depth(a, b),
                )
            raise _refuse(f"Arithmetic between {a.kind} and {b.kind} is not translated")
        if kind == "double":
            assert a.expr is not None and b.expr is not None
            ae, be = a.expr.cast(pl.Float64), b.expr.cast(pl.Float64)
            if op == "mod":
                return Op(
                    pl.lit(None, dtype=pl.Float64),
                    "double",
                    pl.Float64,
                    depth=_depth(a, b),
                )
            if op == "div" and b.is_const and b.const == 0:
                raise _refuse(
                    "Division by a literal zero: the plan's JSON writes -0.0 as -0, which the "
                    "HTTP SDK reads as the integer 0, so the infinity's sign cannot be known"
                )
            expr = {"add": ae + be, "sub": ae - be, "mult": ae * be, "div": ae / be}[op]
            return Op(expr, "double", pl.Float64, depth=_depth(a, b))
        a, b = self._numeric(a, "int"), self._numeric(b, "int")
        assert a.expr is not None and b.expr is not None
        ae, be = a.expr.cast(pl.Int128), b.expr.cast(pl.Int128)
        if op in ("add", "sub", "mult"):
            wide = {"add": ae + be, "sub": ae - be, "mult": ae * be}[op]
            return self._int_result(wide, a, b)
        quotient = (ae.abs() // be.abs()) * ae.sign() * be.sign()
        if op == "div":
            wide = pl.when(be == 0).then(None).otherwise(quotient)
        else:
            overflow = (ae == _INT64_MIN) & (be == -1)
            wide = (
                pl.when((be == 0) | overflow).then(None).otherwise(ae - be * quotient)
            )
        return self._int_result(wide, a, b)

    def _shift(self, ts: Op, dur: Op, sign: int) -> Op:
        assert ts.expr is not None and dur.expr is not None
        base = ts.expr.cast(pl.Datetime("ns", "UTC")).dt.epoch("ns").cast(pl.Int128)
        delta = dur.expr.cast(pl.Duration("ns")).dt.total_nanoseconds().cast(pl.Int128)
        wide = base + delta * sign
        checked = (
            pl.when((wide < _CEL_TS_MIN) | (wide > _CEL_TS_MAX))
            .then(None)
            .otherwise(wide)
        )
        expr = checked.cast(pl.Int64).cast(pl.Datetime("ns", "UTC"))
        return Op(
            expr,
            "timestamp",
            pl.Datetime("ns", "UTC"),
            depth=_depth(ts, dur),
            unit="ns",
        )

    def _duration_sum(self, a: Op, b: Op, sign: int) -> Op:
        assert a.expr is not None and b.expr is not None
        wide = (
            a.expr.cast(pl.Duration("ns")).dt.total_nanoseconds().cast(pl.Int128)
            + b.expr.cast(pl.Duration("ns")).dt.total_nanoseconds().cast(pl.Int128)
            * sign
        )
        checked = (
            pl.when((wide < _INT64_MIN) | (wide > _INT64_MAX))
            .then(None)
            .otherwise(wide)
        )
        return Op(
            checked.cast(pl.Int64).cast(pl.Duration("ns")),
            "duration",
            pl.Duration("ns"),
            depth=_depth(a, b),
        )

    def _strings(self, node: Expr, env, depth) -> tuple[Op, Op] | None:
        a, b = self.args(node, env, depth, 2)
        for op in (a, b):
            if op.kind == "tsattr":
                raise _refuse(
                    "A datetime column cannot be read as the RFC 3339 string CEL sees"
                )
            if op.kind == "unknown":
                raise _refuse(
                    "Cannot read a value of an unsupported column type as a string"
                )
        if a.kind != "string" or b.kind != "string":
            return None
        return a, b

    def _op_startsWith(self, node: Expr, env, depth) -> Op:
        pair = self._strings(node, env, depth)
        if pair is None:
            return _const_null_bool()
        a, b = pair
        assert a.expr is not None and b.expr is not None
        return _bool(a.expr.str.starts_with(b.expr), a, b)

    def _op_endsWith(self, node: Expr, env, depth) -> Op:
        pair = self._strings(node, env, depth)
        if pair is None:
            return _const_null_bool()
        a, b = pair
        assert a.expr is not None and b.expr is not None
        return _bool(a.expr.str.ends_with(b.expr), a, b)

    def _op_contains(self, node: Expr, env, depth) -> Op:
        pair = self._strings(node, env, depth)
        if pair is None:
            return _const_null_bool()
        a, b = pair
        assert a.expr is not None and b.expr is not None
        return _bool(a.expr.str.contains(b.expr, literal=True), a, b)

    def _op_matches(self, node: Expr, env, depth) -> Op:
        pair = self._strings(node, env, depth)
        if pair is None:
            return _const_null_bool()
        a, b = pair
        if not b.is_const:
            raise _refuse(
                "matches() with a pattern that is not a literal is not translated"
            )
        pattern = translate_regex(b.const)
        if pattern is None:
            return _const_null_bool(a)
        try:
            pl.select(pl.lit("").str.contains(pattern))
        except pl.exceptions.PolarsError as error:
            raise _refuse(
                f"Polars' regex engine rejects the translated pattern for {b.const!r}: {error}"
            ) from error
        assert a.expr is not None
        return _bool(a.expr.str.contains(pattern), a)

    def _case(self, node: Expr, env, depth, upper: bool) -> Op:
        (a,) = self.args(node, env, depth, 1)
        if a.kind == "tsattr":
            raise _refuse(
                "A datetime column cannot be read as the RFC 3339 string CEL sees"
            )
        if a.kind != "string":
            return Op(pl.lit(None, dtype=pl.String), "string", pl.String, depth=a.depth)
        assert a.expr is not None
        source, target = (_LOWER, _UPPER) if upper else (_UPPER, _LOWER)
        return Op(
            a.expr.str.replace_many(source, target), "string", pl.String, depth=a.depth
        )

    def _op_upperAscii(self, node: Expr, env, depth) -> Op:
        return self._case(node, env, depth, True)

    def _op_int(self, node: Expr, env, depth) -> Op:
        (a,) = self.args(node, env, depth, 1)
        a = _strict(a)
        if a.kind == "int":
            return a
        if a.kind == "double":
            assert a.expr is not None
            x = a.expr.cast(pl.Float64)
            bad = x.is_nan() | (x <= float(_INT64_MIN)) | (x >= float(_INT64_MAX))
            return Op(
                pl.when(bad).then(None).otherwise(x).cast(pl.Int64, strict=False),
                "int",
                pl.Int64,
                depth=a.depth,
            )
        if a.kind == "string":
            assert a.expr is not None
            ok = a.expr.str.contains(r"\A[+-]?[0-9]+\z")
            digits = a.expr.str.strip_prefix("+")
            return Op(
                pl.when(ok).then(digits).otherwise(None).cast(pl.Int64, strict=False),
                "int",
                pl.Int64,
                depth=a.depth,
            )
        if a.kind in ("null", "bool", "list", "map"):
            return Op(pl.lit(None, dtype=pl.Int64), "int", pl.Int64, depth=a.depth)
        raise _refuse(f"int() of a {a.kind} is not translated")

    def _op_double(self, node: Expr, env, depth) -> Op:
        (a,) = self.args(node, env, depth, 1)
        if a.kind in ("double", "int"):
            assert a.expr is not None
            return Op(a.expr.cast(pl.Float64), "double", pl.Float64, depth=a.depth)
        if a.kind in ("null", "bool", "list", "map"):
            return Op(
                pl.lit(None, dtype=pl.Float64), "double", pl.Float64, depth=a.depth
            )
        raise _refuse(
            f"double() of a {a.kind} is not translated: Go's ParseFloat accepts spellings "
            "(hex floats, underscores, Inf and NaN words) Polars' parser does not read the same way"
        )

    def _op_string(self, node: Expr, env, depth) -> Op:
        (a,) = self.args(node, env, depth, 1)
        a = _strict(a)
        if a.kind == "string":
            return a
        if a.kind == "bool":
            assert a.expr is not None
            return Op(
                pl.when(a.expr.is_null())
                .then(None)
                .when(a.expr)
                .then(pl.lit("true"))
                .otherwise(pl.lit("false")),
                "string",
                pl.String,
                depth=a.depth,
            )
        if a.kind == "int":
            assert a.expr is not None
            return Op(a.expr.cast(pl.String), "string", pl.String, depth=a.depth)
        if a.kind in ("null", "list", "map"):
            return Op(pl.lit(None, dtype=pl.String), "string", pl.String, depth=a.depth)
        raise _refuse(
            f"string() of a {a.kind} is not translated: CEL formats it as Go does, which "
            "Polars' cast to string does not reproduce"
        )

    def _op_timestamp(self, node: Expr, env, depth) -> Op:
        (a,) = self.args(node, env, depth, 1)
        a = _strict(a)
        if a.kind == "timestamp":
            return a
        if a.is_const and isinstance(a.const, str):
            nanos = _parse_timestamp(a.const)
            if nanos is None:
                return Op(
                    pl.lit(None, dtype=pl.Datetime("ns", "UTC")),
                    "timestamp",
                    pl.Datetime("ns", "UTC"),
                    unit="ns",
                )
            if not _CEL_TS_MIN <= nanos <= _CEL_TS_MAX:
                return Op(
                    pl.lit(None, dtype=pl.Datetime("ns", "UTC")),
                    "timestamp",
                    pl.Datetime("ns", "UTC"),
                    unit="ns",
                )
            if not _INT64_MIN <= nanos <= _INT64_MAX:
                raise _refuse(
                    f"timestamp({a.const!r}) is outside the range a nanosecond Polars Datetime holds"
                )
            return Op(
                pl.lit(nanos, dtype=pl.Int64).cast(pl.Datetime("ns", "UTC")),
                "timestamp",
                pl.Datetime("ns", "UTC"),
                const=nanos,
                unit="ns",
            )
        if a.kind == "tsattr":
            assert a.expr is not None and isinstance(a.dtype, pl.Datetime)
            expr = a.expr
            if a.dtype.time_zone is None:
                expr = expr.dt.replace_time_zone("UTC")
            else:
                expr = expr.dt.convert_time_zone("UTC")
            if a.dtype.time_unit != "ns":
                raise _refuse(
                    "timestamp() over a Datetime column needs nanosecond precision: CEL "
                    "timestamps carry nanoseconds, and a coarser column cannot be compared "
                    "exactly with them"
                )
            return Op(
                expr, "timestamp", pl.Datetime("ns", "UTC"), depth=a.depth, unit="ns"
            )
        if a.kind == "string":
            raise _refuse(
                "timestamp() over a string column is not translated: store the attribute "
                "in a Datetime column"
            )
        if a.kind in ("null", "bool", "double", "list", "map"):
            return Op(
                pl.lit(None, dtype=pl.Datetime("ns", "UTC")),
                "timestamp",
                pl.Datetime("ns", "UTC"),
                depth=a.depth,
                unit="ns",
            )
        raise _refuse(f"timestamp() of a {a.kind} is not translated")

    def _op_duration(self, node: Expr, env, depth) -> Op:
        (a,) = self.args(node, env, depth, 1)
        if not (a.is_const and isinstance(a.const, str)):
            raise _refuse(
                "duration() over a value that is not a literal is not translated"
            )
        nanos = _parse_duration(a.const)
        if nanos is None:
            raise _refuse(f"Cannot read the duration {a.const!r}")
        if not _INT64_MIN <= nanos <= _INT64_MAX:
            return Op(
                pl.lit(None, dtype=pl.Duration("ns")), "duration", pl.Duration("ns")
            )
        return Op(
            pl.lit(nanos, dtype=pl.Int64).cast(pl.Duration("ns")),
            "duration",
            pl.Duration("ns"),
            const=nanos,
        )

    def _op_timeSince(self, node: Expr, env, depth) -> Op:
        (a,) = self.args(node, env, depth, 1)
        if a.kind != "timestamp":
            raise _refuse(f"timeSince() of a {a.kind} is not translated")
        assert a.expr is not None
        wide = pl.lit(self.settings.now_ns, dtype=pl.Int128) - a.expr.cast(
            pl.Datetime("ns", "UTC")
        ).dt.epoch("ns").cast(pl.Int128)
        checked = (
            pl.when((wide < _INT64_MIN) | (wide > _INT64_MAX))
            .then(None)
            .otherwise(wide)
        )
        return Op(
            checked.cast(pl.Int64).cast(pl.Duration("ns")),
            "duration",
            pl.Duration("ns"),
            depth=a.depth,
        )

    def _op_hierarchy(self, node: Expr, env, depth) -> Op:
        if len(node.operands) not in (1, 2):
            raise _refuse("hierarchy() takes one or two operands")
        ops = [self.translate(child, env, depth) for child in node.operands]
        source = _strict(ops[0])
        delimiter = "."
        if len(ops) == 2:
            if not (ops[1].is_const and isinstance(ops[1].const, str)):
                raise _refuse(
                    "hierarchy() with a delimiter that is not a literal string is not translated"
                )
            delimiter = ops[1].const
        if len(delimiter) > 1:
            raise _refuse(
                "hierarchy() with a multi-character delimiter is not translated: Go splits it "
                "left to right, which a prefix test cannot reproduce"
            )
        if source.kind == "tsattr":
            raise _refuse(
                "A datetime column cannot be read as the RFC 3339 string CEL sees"
            )
        if source.kind != "string":
            if source.kind in ("list",):
                raise _refuse("hierarchy() of a list is not translated")
            return Op(
                pl.lit(None, dtype=pl.String),
                "hierarchy",
                pl.String,
                depth=source.depth,
                delimiter=delimiter,
            )
        return replace(source, kind="hierarchy", delimiter=delimiter, const=_NO)

    def _hier_pair(self, node: Expr, env, depth) -> tuple[Op, Op] | None:
        a, b = self.args(node, env, depth, 2)
        if a.kind != "hierarchy" or b.kind != "hierarchy":
            return None
        if a.delimiter != b.delimiter:
            raise _refuse("Hierarchies with different delimiters are not compared")
        return a, b

    def _ancestor(self, a: Op, b: Op) -> pl.Expr:
        assert a.expr is not None and b.expr is not None and a.delimiter is not None
        if a.delimiter == "":
            return b.expr.str.starts_with(a.expr) & (
                b.expr.str.len_chars() > a.expr.str.len_chars()
            )
        return b.expr.str.starts_with(a.expr + pl.lit(a.delimiter))

    def _op_ancestorOf(self, node: Expr, env, depth) -> Op:
        pair = self._hier_pair(node, env, depth)
        if pair is None:
            return _const_null_bool()
        a, b = pair
        return _bool(self._ancestor(a, b), a, b)

    def _op_descendentOf(self, node: Expr, env, depth) -> Op:
        pair = self._hier_pair(node, env, depth)
        if pair is None:
            return _const_null_bool()
        a, b = pair
        return _bool(self._ancestor(b, a), a, b)

    def _op_overlaps(self, node: Expr, env, depth) -> Op:
        pair = self._hier_pair(node, env, depth)
        if pair is None:
            return _const_null_bool()
        a, b = pair
        assert a.expr is not None and b.expr is not None
        if a.delimiter == "":
            return _bool(
                a.expr.str.starts_with(b.expr) | b.expr.str.starts_with(a.expr), a, b
            )
        return _bool(
            (a.expr == b.expr) | self._ancestor(a, b) | self._ancestor(b, a), a, b
        )

    def _op_struct(self, node: Expr, env, depth) -> Op:
        fields: dict[str, Op] = {}
        for child in node.operands:
            if not (
                isinstance(child, Expr)
                and child.operator == "set-field"
                and len(child.operands) == 2
            ):
                raise _refuse(
                    "A map literal holds something other than set-field entries"
                )
            key, value = child.operands
            if not isinstance(key, Value) or not isinstance(key.value, str):
                raise _refuse(
                    "A map literal with a key that is not a literal string is not translated"
                )
            fields[key.value] = self.translate(value, env, depth)
        return Op(None, "map", None, fields=fields, depth=_depth(*fields.values()))

    def _op_list(self, node: Expr, env, depth) -> Op:
        items = tuple(self.translate(child, env, depth) for child in node.operands)
        return Op(None, "list", None, items=items, depth=_depth(*items))

    def _op_get_field(self, node: Expr, env, depth) -> Op:
        if len(node.operands) != 2 or not isinstance(node.operands[1], Variable):
            raise _refuse("get-field takes a value and a field name")
        (base,) = [self.translate(node.operands[0], env, depth)]
        return self.field(base, node.operands[1].name)

    def _op_index(self, node: Expr, env, depth) -> Op:
        container, key = self.args(node, env, depth, 2)
        if container.kind == "map" or container.fields is not None:
            if container.fields is not None and not key.is_const:
                return self._map_literal_lookup(container, key)
            if not (key.is_const and isinstance(key.const, str)):
                if key.kind == "null" or key.is_const:
                    return Op(pl.lit(None), "null", pl.Null, depth=container.depth)
                raise _refuse(
                    "Indexing a struct column with a key that is not a literal string is not translated"
                )
            return self.field(container, key.const)
        if container.kind == "list":
            return self._list_index(container, key)
        if container.kind == "null":
            return Op(pl.lit(None), "null", pl.Null, depth=container.depth)
        raise _refuse(f"Indexing a {container.kind} is not translated")

    def _map_literal_lookup(self, container: Op, key: Op) -> Op:
        assert container.fields is not None
        if key.kind != "string":
            raise _refuse(
                "A map literal looked up by a key that is not a string is not translated"
            )
        values = list(container.fields.values())
        kinds = {v.kind for v in values}
        if len(kinds) != 1 or any(v.expr is None for v in values):
            raise _refuse("A map literal whose values differ in type is not translated")
        assert key.expr is not None
        chain: Any = pl
        errors: Any = pl
        for name, value in container.fields.items():
            chain = chain.when(key.expr == pl.lit(name)).then(value.expr)
            errors = errors.when(key.expr == pl.lit(name)).then(_error(value))
        expr = chain.otherwise(None) if container.fields else pl.lit(None)
        err = errors.otherwise(True) if container.fields else pl.lit(True)
        any_error = _any_error(*values)
        if any_error is not None:
            err = any_error | err
        sample = values[0]
        return Op(
            expr,
            sample.kind,
            sample.dtype,
            err=err,
            depth=_depth(container, key, *values),
        )

    def _list_index(self, container: Op, key: Op) -> Op:
        if key.kind == "int" or key.is_const and _integral_const(key):
            index = self._numeric(key, "int")
        elif key.kind == "null" or key.kind in ("string", "bool"):
            return Op(pl.lit(None), "null", pl.Null, depth=container.depth)
        else:
            raise _refuse(
                "A list indexed by a double is not translated: CEL accepts only an integral "
                "value there, which the column type cannot promise"
            )
        assert index.expr is not None
        if container.items is not None:
            if any(item.expr is None for item in container.items):
                raise _refuse(
                    "Indexing a list literal of collections is not translated"
                )
            kinds = {item.kind for item in container.items}
            if len(kinds) > 1:
                raise _refuse(
                    "Indexing a list literal whose elements differ in type is not translated"
                )
            if not container.items:
                return Op(pl.lit(None), "null", pl.Null, depth=index.depth)
            chain: Any = pl
            errors: Any = pl
            for i, item in enumerate(container.items):
                chain = chain.when(index.expr == i).then(item.expr)
                errors = errors.when(index.expr == i).then(_error(item))
            err = errors.otherwise(True)
            any_error = _any_error(*container.items)
            if any_error is not None:
                err = any_error | err
            sample = container.items[0]
            return Op(
                chain.otherwise(None),
                sample.kind,
                sample.dtype,
                err=err,
                depth=_depth(index, *container.items),
            )
        assert container.expr is not None and container.elem is not None
        length = container.expr.list.len()
        oob = (
            index.expr.is_null()
            | (index.expr < 0)
            | (index.expr >= length)
            | length.is_null()
        )
        value = container.expr.list.get(
            index.expr.clip(lower_bound=0), null_on_oob=True
        )
        elem = container.elem
        return Op(
            pl.when(oob).then(None).otherwise(value),
            elem.kind,
            elem.dtype,
            err=oob,
            elem=elem.elem,
            depth=_depth(container, index),
            unit=elem.unit,
        )

    def _op_in(self, node: Expr, env, depth) -> Op:
        needle, haystack = self.args(node, env, depth, 2)
        return self.member(needle, haystack, depth)

    def member(self, needle: Op, haystack: Op, depth: int) -> Op:
        if haystack.items is not None:
            if not haystack.items:
                return _bool(
                    _guard(pl.lit(False), needle)
                    if needle.expr is not None
                    else pl.lit(False),
                    needle,
                )
            fast = self._member_constants(needle, haystack.items)
            if fast is not None:
                return fast
            tests = [self.equals(needle, item) for item in haystack.items]
            result = tests[0].expr
            assert result is not None
            for test in tests[1:]:
                assert test.expr is not None
                result = result | test.expr
            return _bool(_guard(result, *haystack.items), needle, *haystack.items)
        if haystack.fields is not None or haystack.kind == "map":
            raise _refuse("Membership in a map is not translated")
        if haystack.kind == "null":
            return _const_null_bool(needle)
        if haystack.kind != "list":
            return _const_null_bool(needle, haystack)
        if needle.depth > 0 and needle.depth == depth and haystack.depth == 0:
            return self._member_broadcast(needle, haystack)
        if needle.depth > 0 and not needle.is_const:
            raise _refuse(
                "Membership of a lambda element in another list needs the element inside "
                "a second list.eval, which Polars cannot correlate"
            )
        result = self.exists(
            haystack, lambda element: self.equals(element, needle), depth
        )
        assert result.expr is not None
        return _bool(
            _guard(result.expr, needle)
            if needle.err is not None
            else self._error_or_if(result.expr, needle),
            needle,
            haystack,
        )

    def _error_or_if(self, value: pl.Expr, needle: Op) -> pl.Expr:
        if needle.is_const or needle.expr is None:
            return value
        return pl.when(needle.expr.is_null()).then(None).otherwise(value)

    def _member_constants(self, needle: Op, items: tuple[Op, ...]) -> Op | None:
        """``x in [literals]`` as one ``is_in`` for strings and booleans, whose equality is exact."""
        if (
            needle.kind not in ("string", "bool")
            or needle.expr is None
            or needle.is_const
        ):
            return None
        if not all(item.is_const and item.kind == needle.kind for item in items):
            return None
        dtype = pl.String if needle.kind == "string" else pl.Boolean
        found = needle.expr.is_in(
            pl.Series([item.const for item in items], dtype=dtype).implode()
        )
        if _value_nullable(needle):
            result = pl.when(needle.expr.is_null()).then(False).otherwise(found)
        else:
            result = pl.when(needle.expr.is_null()).then(None).otherwise(found)
        return _bool(_guard(result, needle), needle)

    def _member_broadcast(self, needle: Op, haystack: Op) -> Op:
        """``element in column`` inside a lambda, for strings and booleans, where equality is exact."""
        assert (
            needle.expr is not None
            and haystack.expr is not None
            and haystack.elem is not None
        )
        if needle.kind not in ("string", "bool") or haystack.elem.kind not in (
            "string",
            "bool",
            "null",
        ):
            raise _refuse(
                "Membership of a lambda element in a list column is translated only for "
                "strings and booleans, whose Polars equality is CEL's"
            )
        if needle.kind != haystack.elem.kind:
            return _bool(_guard(pl.lit(False), needle, haystack), needle, haystack)
        found = needle.expr.is_in(haystack.expr.explode().implode(), nulls_equal=True)
        result = (
            pl.when(haystack.expr.is_null())
            .then(None)
            .when(haystack.expr.list.len() == 0)
            .then(False)
            .otherwise(found)
        )
        if not _value_nullable(needle):
            result = pl.when(needle.expr.is_null()).then(None).otherwise(result)
        return Op(result, "bool", pl.Boolean, depth=needle.depth)

    def _lambda(self, node: Expr) -> tuple[Operand, Operand, str]:
        if len(node.operands) != 2:
            raise _refuse(f"{node.operator} takes a collection and a lambda")
        collection, fn = node.operands
        if (
            not isinstance(fn, Expr)
            or fn.operator != "lambda"
            or len(fn.operands) != 2
            or not isinstance(fn.operands[1], Variable)
        ):
            raise _refuse(
                f"{node.operator} without a one-variable lambda is not translated: a "
                "two-variable comprehension binds an index Polars' list.eval does not expose"
            )
        return collection, fn.operands[0], fn.operands[1].name

    def _materialise(self, op: Op, depth: int) -> Op:
        """A list operand as a Polars list expression, whatever it was built from."""
        if op.items is None:
            return op
        if not op.items:
            raise _refuse(
                "A macro over an empty list literal is folded by the planner; refusing the unfolded shape"
            )
        kinds = {item.kind for item in op.items if item.kind != "null"}
        if len(kinds) > 1 or any(item.expr is None for item in op.items):
            raise _refuse(
                "A list literal whose elements differ in type is not translated"
            )
        kind = kinds.pop() if kinds else "null"
        if kind not in ("string", "bool", "double", "int"):
            raise _refuse(f"A list literal of {kind} values is not translated")
        if any(0 < item.depth for item in op.items):
            raise _refuse(
                "A list literal built from a lambda element is not translated"
            )
        if any(_value_nullable(item) and not item.is_const for item in op.items):
            raise _refuse(
                "A list literal holding a value that may be an explicit null is not translated"
            )
        dtype = {
            "string": pl.String,
            "bool": pl.Boolean,
            "double": pl.Float64,
            "int": pl.Int64,
        }[kind]
        parts = [item.expr.cast(dtype) for item in op.items if item.expr is not None]
        expr = pl.concat_list(parts)
        any_error: pl.Expr | None = None
        for item in op.items:
            if not item.is_const:
                check = _error(item)
                any_error = check if any_error is None else any_error | check
        if any_error is not None:
            expr = pl.when(any_error).then(None).otherwise(expr)
        elem = Op(pl.element(), kind, dtype, err=pl.lit(False), depth=1)
        return Op(expr, "list", pl.List(dtype), elem=elem, depth=_depth(*op.items))

    def _body_env(
        self, collection: Op, variable: str, env: dict[str, Op], depth: int
    ) -> dict[str, Op]:
        assert collection.elem is not None
        element = replace(collection.elem, depth=depth + 1)
        if element.elem is not None:
            element = self._typed(pl.element(), element.dtype, pl.lit(False), depth + 1)  # type: ignore[arg-type]
        return {**{k: v for k, v in env.items() if k != variable}, variable: element}

    def _collection(self, node: Operand, env, depth) -> Op:
        op = self.translate(node, env, depth)
        if op.items is not None:
            op = self._materialise(op, depth)
        return op

    def exists(self, collection: Op, body: Callable[[Op], Op], depth: int) -> Op:
        collection = self._materialise(collection, depth)
        if collection.kind != "list":
            return _const_null_bool(collection)
        element = replace(collection.elem, depth=depth + 1) if collection.elem else None
        assert element is not None and collection.expr is not None
        result = body(element)
        pred = _per_element(self.as_bool(result))
        expr = collection.expr.list.eval(pred.any(ignore_nulls=False)).list.first()
        return _bool(expr, collection)

    def _macro(self, node: Expr, env, depth, reduce: str) -> Op:
        collection_node, body_node, variable = self._lambda(node)
        collection = self._collection(collection_node, env, depth)
        if collection.kind == "map":
            raise _refuse(f"{node.operator} over a map's keys is not translated")
        if collection.kind != "list":
            return _const_null_bool(collection)
        assert collection.expr is not None
        body_env = self._body_env(collection, variable, env, depth)
        body = self.translate(body_node, body_env, depth + 1)
        if 0 < body.depth <= depth:
            raise _refuse(
                "A lambda body reads the element of an enclosing lambda, and Polars' "
                "list.eval cannot reach an outer element from an inner one"
            )
        if reduce == "map":
            if body.expr is None:
                raise _refuse(
                    "A map() producing list or map literals is not translated"
                )
            if body.kind in ("list", "map", "tsattr", "unknown"):
                raise _refuse(f"A map() producing {body.kind} values is not translated")
            failed = collection.expr.list.eval(
                _per_element(_error(body)).any()
            ).list.first()
            values = collection.expr.list.eval(_per_element(body.expr))
            dtype = body.dtype if body.dtype is not None else pl.Null
            elem = Op(
                pl.element(),
                body.kind,
                dtype,
                err=pl.lit(False),
                depth=depth + 1,
                unit=body.unit,
                delimiter=body.delimiter,
            )
            return Op(
                pl.when(failed).then(None).otherwise(values),
                "list",
                pl.List(dtype) if dtype != pl.Null else None,
                elem=elem,
                depth=collection.depth,
            )
        pred = _per_element(self.as_bool(body))
        if reduce == "exists":
            expr = collection.expr.list.eval(pred.any(ignore_nulls=False)).list.first()
        elif reduce == "all":
            expr = collection.expr.list.eval(pred.all(ignore_nulls=False)).list.first()
        elif reduce == "exists_one":
            expr = collection.expr.list.eval(
                pl.when(pred.is_null().any()).then(None).otherwise(pred.sum() == 1)
            ).list.first()
        else:
            failed = collection.expr.list.eval(pred.is_null().any()).list.first()
            kept = collection.expr.list.eval(pl.element().filter(pred))
            assert collection.elem is not None
            return Op(
                pl.when(failed).then(None).otherwise(kept),
                "list",
                collection.dtype,
                elem=collection.elem,
                depth=collection.depth,
            )
        return _bool(expr, collection)

    def _op_exists(self, node, env, depth) -> Op:
        return self._macro(node, env, depth, "exists")

    def _op_all(self, node, env, depth) -> Op:
        return self._macro(node, env, depth, "all")

    def _op_exists_one(self, node, env, depth) -> Op:
        return self._macro(node, env, depth, "exists_one")

    def _op_filter(self, node, env, depth) -> Op:
        return self._macro(node, env, depth, "filter")

    def _op_map(self, node, env, depth) -> Op:
        return self._macro(node, env, depth, "map")

    def _op_lambda(self, node, env, depth) -> Op:
        raise _refuse("A lambda outside a macro is not translated")

    def _op_size(self, node: Expr, env, depth) -> Op:
        (a,) = self.args(node, env, depth, 1)
        if a.items is not None:
            if any(not item.is_const for item in a.items):
                errors = [_error(item) for item in a.items if not item.is_const]
                any_error = errors[0]
                for e in errors[1:]:
                    any_error = any_error | e
                return Op(
                    pl.when(any_error)
                    .then(None)
                    .otherwise(pl.lit(len(a.items), dtype=pl.Int64)),
                    "int",
                    pl.Int64,
                    depth=_depth(*a.items),
                )
            return Op(
                pl.lit(len(a.items), dtype=pl.Int64),
                "int",
                pl.Int64,
                const=len(a.items),
            )
        if a.fields is not None:
            return Op(pl.lit(len(a.fields), dtype=pl.Int64), "int", pl.Int64)
        if a.kind == "string":
            assert a.expr is not None
            return Op(
                a.expr.str.len_chars().cast(pl.Int64), "int", pl.Int64, depth=a.depth
            )
        if a.kind == "list":
            assert a.expr is not None
            return Op(a.expr.list.len().cast(pl.Int64), "int", pl.Int64, depth=a.depth)
        if a.kind == "tsattr":
            raise _refuse(
                "A datetime column cannot be read as the RFC 3339 string CEL sees"
            )
        if a.kind == "map":
            raise _refuse(
                "size() of a struct column is not translated: a struct holds every field on "
                "every row, where the map CEL sees holds only the keys the row sent"
            )
        if a.kind in ("null", "bool", "double", "int"):
            return Op(pl.lit(None, dtype=pl.Int64), "int", pl.Int64, depth=a.depth)
        raise _refuse(f"size() of a {a.kind} is not translated")

    def _op_has(self, node: Expr, env, depth) -> Op:
        if len(node.operands) != 1:
            raise _refuse("has() takes one operand")
        target = node.operands[0]
        if (
            isinstance(target, Expr)
            and target.operator == "get-field"
            and len(target.operands) == 2
            and isinstance(target.operands[1], Variable)
        ):
            base = self.translate(target.operands[0], env, depth)
            name = target.operands[1].name
            return self._has_field(base, name)
        if not isinstance(target, Variable):
            raise _refuse(
                "has() of something other than a field selection is not translated"
            )
        name = target.name
        head, _, rest = name.partition(".")
        if head in env and rest:
            parent_path, _, last = name.rpartition(".")
            base = self.variable(parent_path, env, depth)
            return self._has_field(base, last)
        if not name.startswith(RESOURCE_ATTR):
            raise _refuse(f"has({name}) is not translated")
        path = name[len(RESOURCE_ATTR) :]
        if "." not in path:
            op = self.attribute(name)
            assert op.expr is not None
            if name in self.settings.explicit:
                return _bool(pl.lit(True), op)
            return _bool(op.expr.is_not_null(), op)
        parent_path = name.rpartition(".")[0]
        if name in self.settings.explicit:
            parent = self.attribute(parent_path)
            assert parent.expr is not None
            return _bool(
                pl.when(parent.expr.is_null()).then(None).otherwise(True), parent
            )
        parent = self.attribute(parent_path)
        return self._has_field(parent, name.rpartition(".")[2])

    def _has_field(self, base: Op, name: str) -> Op:
        if base.fields is not None:
            return _bool(pl.lit(name in base.fields), base)
        if base.kind != "map":
            if base.kind == "null":
                return _const_null_bool(base)
            raise _refuse(f"has() on a {base.kind} is not translated")
        assert base.expr is not None and isinstance(base.dtype, pl.Struct)
        names = {f.name for f in base.dtype.fields}
        if name not in names:
            return _bool(pl.when(base.expr.is_null()).then(None).otherwise(False), base)
        value = base.expr.struct.field(name)
        return _bool(
            pl.when(base.expr.is_null()).then(None).otherwise(value.is_not_null()), base
        )

    def _filtered(self, source: Op, keep: Callable[[Op], Op], depth: int) -> Op:
        source = self._materialise(source, depth)
        assert source.expr is not None and source.elem is not None
        element = replace(source.elem, depth=depth + 1)
        test = keep(element)
        assert test.expr is not None
        pred = _per_element(test.expr)
        failed = source.expr.list.eval(pred.is_null().any()).list.first()
        kept = source.expr.list.eval(pl.element().filter(pred))
        return Op(
            pl.when(failed).then(None).otherwise(kept),
            "list",
            source.dtype,
            elem=source.elem,
            depth=source.depth,
        )

    def _op_except(self, node: Expr, env, depth) -> Op:
        a, b = self.args(node, env, depth, 2)
        if a.kind != "list" or b.kind != "list":
            if a.kind in (
                "list",
                "null",
                "string",
                "double",
                "int",
                "bool",
            ) and b.kind in ("list", "null", "string", "double", "int", "bool"):
                return Op(pl.lit(None), "null", pl.Null, depth=_depth(a, b))
            raise _refuse(f"except() of a {a.kind} and a {b.kind} is not translated")
        if (
            a.items is not None
            and b.items is not None
            and all(i.is_const for i in a.items + b.items)
        ):
            raise _refuse(
                "except() over two list literals is folded by the planner; refusing the unfolded shape"
            )
        result = self._filtered(
            a,
            lambda element: self._negated(self._plain_member(element, b, depth)),
            depth,
        )
        assert result.expr is not None
        return replace(
            result,
            expr=_guard(result.expr, b)
            if b.items is not None
            else self._list_guard(result.expr, b),
        )

    def _negated(self, op: Op) -> Op:
        assert op.expr is not None
        return replace(op, expr=~op.expr)

    def _op_intersect(self, node: Expr, env, depth) -> Op:
        a, b = self.args(node, env, depth, 2)
        if a.kind != "list" or b.kind != "list":
            if a.kind in (
                "list",
                "null",
                "string",
                "double",
                "int",
                "bool",
            ) and b.kind in ("list", "null", "string", "double", "int", "bool"):
                return Op(pl.lit(None), "null", pl.Null, depth=_depth(a, b))
            raise _refuse(f"intersect() of a {a.kind} and a {b.kind} is not translated")
        a_list, b_list = self._materialise(a, depth), self._materialise(b, depth)
        if a_list.dtype != b_list.dtype:
            raise _refuse(
                "intersect() of lists of different element types is not translated"
            )
        forward = self._filtered(
            a, lambda element: self._plain_member(element, b, depth), depth
        )
        backward = self._filtered(
            b, lambda element: self._plain_member(element, a, depth), depth
        )
        assert a_list.expr is not None and b_list.expr is not None
        assert forward.expr is not None and backward.expr is not None
        expr = (
            pl.when(a_list.expr.list.len() > b_list.expr.list.len())
            .then(backward.expr)
            .otherwise(forward.expr)
        )
        expr = _guard(expr, a, b)
        return Op(expr, "list", a_list.dtype, elem=a_list.elem, depth=_depth(a, b))

    def _quantify(self, source: Op, target: Op, depth: int, every: bool) -> Op:
        result = self._quantify_values(source, target, depth, every)
        items = (*(source.items or ()), *(target.items or ()))
        if not items:
            return result
        assert result.expr is not None
        return replace(result, expr=_guard(result.expr, *items))

    def _quantify_values(self, source: Op, target: Op, depth: int, every: bool) -> Op:
        if source.kind not in ("list",) and source.items is None:
            return _const_null_bool(source, target)
        if target.kind not in ("list",) and target.items is None:
            return _const_null_bool(source, target)
        if source.items is not None:
            tests = []
            for item in source.items:
                test = self.member(item, target, depth)
                assert test.expr is not None
                tests.append(test.expr.fill_null(False))
            if not tests:
                return _bool(
                    _guard(pl.lit(every), target)
                    if target.expr is not None
                    else pl.lit(every),
                    target,
                )
            result = tests[0]
            for t in tests[1:]:
                result = (result & t) if every else (result | t)
            return _bool(self._list_guard(result, target), source, target)
        assert source.expr is not None
        if target.items is not None and not target.items:
            return _bool(
                pl.when(source.expr.is_null())
                .then(None)
                .otherwise(source.expr.list.len() == 0 if every else pl.lit(False)),
                source,
            )
        element = replace(source.elem, depth=depth + 1) if source.elem else None
        assert element is not None
        result = self._plain_member(element, target, depth)
        assert result.expr is not None
        pred = _per_element(result.expr)
        reduced = pred.fill_null(False).all() if every else pred.fill_null(False).any()
        expr = source.expr.list.eval(reduced).list.first()
        return _bool(self._list_guard(expr, target), source, target)

    def _list_guard(self, value: pl.Expr, target: Op) -> pl.Expr:
        if target.items is not None or target.expr is None:
            return value
        return pl.when(target.expr.is_null()).then(None).otherwise(value)

    def _plain_member(self, element: Op, target: Op, depth: int) -> Op:
        if target.items is not None:
            fast = self._member_constants(element, target.items)
            if fast is not None:
                return fast
            tests = [self.equals(element, item) for item in target.items]
            result = tests[0].expr
            assert result is not None
            for test in tests[1:]:
                assert test.expr is not None
                result = result | test.expr
            return _bool(result, element)
        return self._member_broadcast(element, target)

    def _op_hasIntersection(self, node, env, depth) -> Op:
        a, b = self.args(node, env, depth, 2)
        return self._quantify(a, b, depth, every=False)

    def _op_has_intersection(self, node, env, depth) -> Op:
        return self._op_hasIntersection(node, env, depth)

    def _op_isSubset(self, node, env, depth) -> Op:
        a, b = self.args(node, env, depth, 2)
        return self._quantify(a, b, depth, every=True)

    def _op_hasAny(self, node, env, depth) -> Op:
        a, b = self.args(node, env, depth, 2)
        return self._quantify(b, a, depth, every=False)

    def _op_hasAll(self, node, env, depth) -> Op:
        a, b = self.args(node, env, depth, 2)
        return self._quantify(b, a, depth, every=True)

    def _op_hasOnly(self, node, env, depth) -> Op:
        a, b = self.args(node, env, depth, 2)
        return self._quantify(a, b, depth, every=True)

    def _op_hasExactly(self, node, env, depth) -> Op:
        a, b = self.args(node, env, depth, 2)
        both = [
            self._quantify(b, a, depth, every=True),
            self._quantify(a, b, depth, every=True),
        ]
        assert both[0].expr is not None and both[1].expr is not None
        return _bool(both[0].expr & both[1].expr, a, b)
