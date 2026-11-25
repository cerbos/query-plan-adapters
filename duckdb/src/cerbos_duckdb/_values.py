# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Operand values: plan constants, symbolic values the translator folds, and their lowering."""

import math
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from cerbos_duckdb._expr import (
    Anchor,
    Arith,
    ArithOp,
    BoolConst,
    Call,
    Case,
    Cast,
    Cmp,
    CmpOp,
    Col,
    Concat,
    Expr,
    FromItem,
    InList,
    IsNull,
    Lit,
    Not,
    StrMatch,
    Subquery,
    SubqueryKind,
    Truth,
    TruthTest,
    and_,
    or_,
    when,
)
from cerbos_duckdb.errors import UnsupportedPlanError as Unsupported
from cerbos_duckdb.mapping import ValueType

NS = 1_000_000_000
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


@dataclass(frozen=True, order=True)
class Instant:
    """A CEL timestamp, in nanoseconds since the epoch: DuckDB and ``datetime`` keep microseconds."""

    ns: int

    def truncated(self) -> "Instant":
        return Instant(self.ns - self.ns % 1000)


MIN_INSTANT = Instant(-62135596800 * NS)
MAX_INSTANT = Instant(253402300799 * NS + 999_999_999)


class Symbolic:
    __slots__ = ()


@dataclass(frozen=True)
class IEEEConst(Symbolic):
    v: float
    sign_unknown: bool = False


@dataclass(frozen=True)
class CondValue(Symbolic):
    cond: Expr
    then: Any
    els: Any


@dataclass(frozen=True)
class HierarchyValue(Symbolic):
    value: Any
    delimiter: str


@dataclass(frozen=True)
class NumberText(Symbolic):
    x: Expr


@dataclass(frozen=True)
class Scope:
    from_: tuple[FromItem, ...]
    correlate: Expr | None

    def exists(self, where: Expr | None) -> Subquery:
        return Subquery(SubqueryKind.EXISTS, self.from_, self.correlate, where)

    def count(self, where: Expr | None) -> Subquery:
        return Subquery(SubqueryKind.COUNT, self.from_, self.correlate, where)


@dataclass(frozen=True)
class PositionalElement(Symbolic):
    scope: Scope
    element: Col

    def scalar(self) -> Expr:
        return Subquery(
            SubqueryKind.SCALAR,
            self.scope.from_,
            self.scope.correlate,
            None,
            self.element,
        )

    def decided(self, body: Expr) -> Expr:
        return when(
            self.scope.exists(TruthTest(body, Truth.TRUE)),
            BoolConst(True),
            (self.scope.exists(TruthTest(body, Truth.FALSE)), BoolConst(False)),
        )


@dataclass(frozen=True)
class CountedList(Symbolic):
    size: Expr


@dataclass(frozen=True)
class ListValue(Symbolic):
    defined: Expr | None
    elements: tuple[Any, ...]
    nodes: tuple[Any, ...]

    def when_defined(self, e: Expr) -> Expr:
        if self.defined is None:
            return e
        return when(self.defined, e)


@dataclass(frozen=True)
class DeferredCollection(Symbolic):
    body: Expr
    element: Expr | None
    coll: Any
    scope: Scope
    is_map: bool

    def macro(self) -> str:
        return "map" if self.is_map else "filter"

    def exists(self, where: Expr | None) -> Subquery:
        return self.scope.exists(where)

    def count(self, where: Expr | None) -> Subquery:
        return self.scope.count(where)


@dataclass(frozen=True)
class DurationConst(Symbolic):
    ns: int


@dataclass(frozen=True)
class TimestampShift(Symbolic):
    x: Expr
    ns: int


@dataclass(frozen=True)
class TimeSinceValue(Symbolic):
    x: Expr


def is_float(v: Any) -> bool:
    return type(v) is float


def is_constant(v: Any) -> bool:
    return v is None or isinstance(v, (str, float, bool, list, dict))


# -- scalar kinds ------------------------------------------------------------------------------


def scalar_kind(v: Any) -> str:
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, str):
        return "string"
    if isinstance(v, float):
        return "number"
    if isinstance(v, list):
        return "list"
    if isinstance(v, dict):
        return "map"
    if isinstance(v, Subquery):
        return scalar_kind(v.select) if v.kind == SubqueryKind.SCALAR else ""
    if isinstance(v, PositionalElement):
        return scalar_kind(v.element)
    if isinstance(v, Col):
        return {
            ValueType.STRING: "string",
            ValueType.BOOL: "bool",
            ValueType.NUMBER: "number",
        }.get(v.type, "")
    if isinstance(v, Lit):
        return scalar_kind(v.v)
    if isinstance(v, Concat):
        return "string"
    if isinstance(v, Arith):
        if scalar_kind(v.l) == "number" or scalar_kind(v.r) == "number":
            return "number"
        return ""
    if isinstance(v, Case):
        kinds = case_arm_kinds(v)
        if kinds is not None and len(kinds) == 1:
            return next(iter(kinds))
    return ""


def case_arm_kinds(c: Case) -> set[str] | None:
    arms = [then for _, then in c.whens]
    if c.else_ is not None:
        arms.append(c.else_)
    kinds = set()
    for arm in arms:
        kind = scalar_kind(arm)
        if kind == "":
            return None
        kinds.add(kind)
    return kinds


def known_non_string(v: Any) -> bool:
    kind = scalar_kind(v)
    return kind not in ("", "string")


def explicit_null_column(e: Any) -> Col | None:
    if isinstance(e, Col) and e.explicit_null:
        return e
    return None


# -- lowering ----------------------------------------------------------------------------------

_ERR_NUMBER_TEXT = (
    "string() over a ValueType.NUMBER column translates only as == or != against a string "
    'constant: CEL spells a double with Go\'s %g ("1e+06", "-0") and no SQL CAST prints that spelling'
)


def as_expr(v: Any) -> Expr:
    if v is None:
        return Lit(None)
    if isinstance(v, Expr):
        return v
    if isinstance(v, IEEEConst):
        raise Unsupported(
            "CEL's infinity or NaN translates only where it is compared directly; DuckDB orders NaN "
            "above every number where CEL compares it false"
        )
    if isinstance(v, CondValue):
        raise Unsupported(
            "a quotient whose divisor can be zero is CEL's signed infinity or NaN there, which DuckDB "
            "cannot hold; it translates only where it is compared directly"
        )
    if isinstance(v, HierarchyValue):
        raise Unsupported("hierarchy() value used outside a hierarchy operator")
    if isinstance(v, NumberText):
        raise Unsupported(_ERR_NUMBER_TEXT)
    if isinstance(v, PositionalElement):
        return v.scalar()
    if isinstance(v, CountedList):
        raise Unsupported(
            "filter() over a literal list produces a collection rather than a plain value; it only "
            "translates inside size()"
        )
    if isinstance(v, DeferredCollection):
        raise Unsupported(
            f"'{v.macro()}' produces a collection rather than a plain value; it only translates "
            "inside size() or hasIntersection(), which give the collection a scalar meaning"
        )
    if isinstance(v, (list, dict, ListValue)):
        raise Unsupported(
            "a list or map literal has no SQL representation as a plain value"
        )
    if isinstance(v, Symbolic):
        raise Unsupported(
            f"symbolic value {type(v).__name__} has no SQL representation"
        )
    return Lit(v)


def as_float_expr(v: Any) -> Expr:
    if is_float(v):
        return Lit(v)
    return Cast(as_expr(v), "DOUBLE")


def negate(e: Expr, negated: bool) -> Expr:
    if not negated:
        return e
    if isinstance(e, BoolConst):
        return BoolConst(not e.v)
    return Not(e)


# -- string matching ---------------------------------------------------------------------------


def string_match(receiver: Any, needle: Any, anchor: Anchor) -> Expr:
    if known_non_string(receiver) or known_non_string(needle):
        return when(BoolConst(False), BoolConst(True))
    if isinstance(receiver, str) and isinstance(needle, str):
        if anchor == Anchor.CONTAINS:
            return BoolConst(needle in receiver)
        if anchor == Anchor.STARTS:
            return BoolConst(receiver.startswith(needle))
        return BoolConst(receiver.endswith(needle))
    return StrMatch(as_expr(receiver), as_expr(needle), anchor)


# -- division and IEEE values ------------------------------------------------------------------

_ERR_UNKNOWN_ZERO_SIGN = (
    "the sign of a zero column denominator decides this result: CEL's x / -0.0 is the opposite "
    "infinity from x / 0.0, and SQL cannot read the sign of a stored zero"
)
_ERR_NON_FINITE_WITH_COLUMN = (
    "arithmetic combines a non-finite value with a column, which SQL cannot carry"
)


def _same_float(a: float, b: float) -> bool:
    if math.isnan(a) and math.isnan(b):
        return True
    return a == b and math.copysign(1, a) == math.copysign(1, b)


def fold_ieee(c: IEEEConst, f) -> Any:
    out = f(c.v)
    if not c.sign_unknown:
        return IEEEConst(out)
    alt = f(-c.v)
    if _same_float(out, alt):
        return IEEEConst(out)
    if math.isinf(out) and alt == -out:
        return IEEEConst(out, True)
    raise Unsupported(_ERR_UNKNOWN_ZERO_SIGN)


def apply_ieee(op: ArithOp, l: float, r: float) -> float:
    try:
        if op == ArithOp.ADD:
            return l + r
        if op == ArithOp.SUB:
            return l - r
        if op == ArithOp.MULT:
            return l * r
        if op == ArithOp.DIV:
            return ieee_div(l, r)
    except (OverflowError, ValueError):
        return math.nan
    return math.nan


def ieee_div(l: float, r: float) -> float:
    if r == 0:
        if l == 0 or math.isnan(l):
            return math.nan
        return math.copysign(math.inf, math.copysign(1, l) * math.copysign(1, r))
    return l / r


def fold_arithmetic(op: ArithOp, l: float, r: float) -> float | None:
    if op == ArithOp.ADD:
        return l + r
    if op == ArithOp.SUB:
        return l - r
    if op == ArithOp.MULT:
        return l * r
    if op == ArithOp.MOD:
        if l != math.trunc(l) or r != math.trunc(r):
            raise Unsupported(f"modulus requires integer operands, got {l} % {r}")
        if r == 0:
            raise Unsupported("modulus by zero in query plan")
        li, ri = int(l), int(r)
        return float(int(math.fmod(li, ri)))
    return None


def float_div(l: Any, r: Any) -> Any:
    if is_float(l) and is_float(r):
        if r == 0:
            if l == 0 or math.isnan(l):
                return IEEEConst(math.nan)
            return IEEEConst(
                math.copysign(math.inf, math.copysign(1, l) * math.copysign(1, r))
            )
        return l / r

    numerator = as_float_expr(l)
    denominator = as_float_expr(r)
    zero = Lit(0.0)
    finite = Arith(ArithOp.DIV, numerator, Call("nullif", (denominator, zero)))
    if numerator == denominator:
        return CondValue(Cmp(CmpOp.EQ, denominator, zero), IEEEConst(math.nan), finite)

    sign = -1.0 if is_float(r) and math.copysign(1, r) < 0 else 1.0
    unknown = not is_float(r)
    return CondValue(
        Cmp(CmpOp.EQ, denominator, zero),
        CondValue(
            Cmp(CmpOp.EQ, numerator, zero),
            IEEEConst(math.nan),
            CondValue(
                Cmp(CmpOp.GT, numerator, zero),
                IEEEConst(math.copysign(math.inf, sign), unknown),
                IEEEConst(math.copysign(math.inf, -sign), unknown),
            ),
        ),
        finite,
    )


def _map_arms(cv: CondValue, f) -> CondValue:
    return CondValue(cv.cond, f(cv.then), f(cv.els))


def arith_over_conditional(op: ArithOp, l: Any, r: Any) -> tuple[Any, bool]:
    def combine(left: Any, right: Any) -> Any:
        if is_float(left) and is_float(right):
            folded = fold_arithmetic(op, left, right)
            if folded is not None:
                return folded
        if op == ArithOp.MOD:
            raise Unsupported(
                "modulus over a division whose denominator may be zero is not supported: CEL's % "
                "is integer-only and attribute values are always doubles"
            )
        inner, ok = arith_over_conditional(op, left, right)
        if ok:
            return inner
        if isinstance(left, IEEEConst):
            if not is_float(right):
                raise Unsupported(_ERR_NON_FINITE_WITH_COLUMN)
            return fold_ieee(left, lambda v: apply_ieee(op, v, right))
        if isinstance(right, IEEEConst):
            if not is_float(left):
                raise Unsupported(_ERR_NON_FINITE_WITH_COLUMN)
            return fold_ieee(right, lambda v: apply_ieee(op, left, v))
        return Arith(op, as_float_expr(left), as_float_expr(right))

    if isinstance(l, CondValue):
        return _map_arms(l, lambda arm: combine(arm, r)), True
    if isinstance(r, CondValue):
        return _map_arms(r, lambda arm: combine(l, arm)), True
    return None, False


# -- comparisons -------------------------------------------------------------------------------


def compare_ordered(op: CmpOp, l: Any, r: Any) -> bool:
    if op == CmpOp.EQ:
        return l == r
    if op == CmpOp.NE:
        return l != r
    if op == CmpOp.LT:
        return l < r
    if op == CmpOp.LE:
        return l <= r
    if op == CmpOp.GT:
        return l > r
    return l >= r


def compare(op: CmpOp, l: Any, r: Any) -> Expr:
    out, ok = compare_temporal(op, l, r)
    if ok:
        return out
    if isinstance(l, CondValue):
        return _distribute(op, l, r, True)
    if isinstance(r, CondValue):
        return _distribute(op, r, l, False)
    out, ok = positional_comparison(op, l, r)
    if ok:
        return out
    return compare_leaf(op, l, r)


def _distribute(op: CmpOp, cv: CondValue, other: Any, cond_on_left: bool) -> Expr:
    def build(arm: Any) -> Expr:
        return compare(op, arm, other) if cond_on_left else compare(op, other, arm)

    return when(cv.cond, build(cv.then), (Not(cv.cond), build(cv.els)))


def compare_leaf(op: CmpOp, l: Any, r: Any) -> Expr:
    l_ieee = isinstance(l, IEEEConst)
    r_ieee = isinstance(r, IEEEConst)
    if not l_ieee and not r_ieee:
        return apply_comparison(op, l, r)

    l_nan = l_ieee and math.isnan(l.v)
    r_nan = r_ieee and math.isnan(r.v)
    if l_nan or r_nan:
        other = l if r_nan else r
        if l_nan and r_nan:
            other = None
        result = BoolConst(op == CmpOp.NE)
        if is_float(other) or other is None:
            return result
        if isinstance(other, Expr):
            return when(IsNull(other), Lit(None), (IsNull(other, True), result))
        raise Unsupported(
            "NaN can only be compared with numeric constants or column expressions"
        )

    lv = l.v if l_ieee else l
    rv = r.v if r_ieee else r
    if not (l_ieee or is_float(l)) or not (r_ieee or is_float(r)):
        raise Unsupported(
            "non-finite numeric constants can only be compared with numeric constants"
        )
    result = compare_ordered(op, lv, rv)
    lu = l_ieee and l.sign_unknown
    ru = r_ieee and r.sign_unknown
    if (
        (lu and compare_ordered(op, -lv, rv) != result)
        or (ru and compare_ordered(op, lv, -rv) != result)
        or (lu and ru and compare_ordered(op, -lv, -rv) != result)
    ):
        raise Unsupported(_ERR_UNKNOWN_ZERO_SIGN)
    return BoolConst(result)


_ERR_LIST_OPERAND = (
    "a list literal compares only against an operand with a declared scalar ValueType, which it "
    "never equals; SQL has no list operand to compare an undeclared column with"
)


def apply_comparison(op: CmpOp, l: Any, r: Any) -> Expr:
    out = compare_number_text(op, l, r)
    if out is not None:
        return out
    out = distribute_mixed_case(op, l, r)
    if out is not None:
        return out
    out = compare_mixed_types(op, l, r)
    if out is not None:
        return out
    if isinstance(l, list):
        if isinstance(r, list) and op in (CmpOp.EQ, CmpOp.NE):
            return BoolConst(constant_lists_equal(l, r) == (op == CmpOp.EQ))
        raise Unsupported(_ERR_LIST_OPERAND)
    if isinstance(r, list):
        raise Unsupported(_ERR_LIST_OPERAND)
    out = null_comparison(op, l, r)
    if out is not None:
        return out
    if is_float(l) and is_float(r):
        return BoolConst(compare_ordered(op, l, r))
    if isinstance(l, str) and isinstance(r, str):
        return BoolConst(compare_ordered(op, l.encode("utf-8"), r.encode("utf-8")))
    if isinstance(l, bool) and isinstance(r, bool) and op in (CmpOp.EQ, CmpOp.NE):
        return BoolConst((l == r) == (op == CmpOp.EQ))
    if isinstance(l, Instant) and isinstance(r, Instant):
        return BoolConst(compare_ordered(op, l.ns, r.ns))

    le, re_ = as_expr(l), as_expr(r)
    definite = definite_equality(op, le, re_)
    if definite is not None:
        return definite
    return Cmp(op, le, re_)


def compare_number_text(op: CmpOp, l: Any, r: Any) -> Expr | None:
    if isinstance(l, NumberText):
        text, other = l, r
    elif isinstance(r, NumberText):
        text, other = r, l
    else:
        return None
    if not isinstance(other, str) or op not in (CmpOp.EQ, CmpOp.NE):
        raise Unsupported(_ERR_NUMBER_TEXT)
    f = cel_double_spelling(other)
    if f is None:
        return when(IsNull(text.x, True), BoolConst(op == CmpOp.NE))
    if f == 0:
        raise Unsupported(
            f'string() over a number compared with "{other}": CEL spells 0.0 "0" and -0.0 "-0", '
            "and SQL cannot read the sign of a stored zero"
        )
    if math.isinf(f) or math.isnan(f):
        raise Unsupported(
            f'string() over a number compared with "{other}": a non-finite value has no SQL literal'
        )
    return Cmp(op, text.x, Lit(f))


def go_format_g(f: float) -> str:
    if math.isnan(f):
        return "NaN"
    if math.isinf(f):
        return "+Inf" if f > 0 else "-Inf"
    sign = "-" if math.copysign(1, f) < 0 else ""
    if f == 0:
        return sign + "0"
    t = Decimal(repr(abs(f))).normalize().as_tuple()
    digits = "".join(map(str, t.digits))
    dp = len(digits) + int(t.exponent)
    exp = dp - 1
    if exp < -4 or exp >= 6:
        mantissa = digits[0] + ("." + digits[1:] if len(digits) > 1 else "")
        return f"{sign}{mantissa}e{'-' if exp < 0 else '+'}{abs(exp):02d}"
    if dp <= 0:
        return f"{sign}0.{'0' * -dp}{digits}"
    if dp >= len(digits):
        return sign + digits + "0" * (dp - len(digits))
    return f"{sign}{digits[:dp]}.{digits[dp:]}"


def cel_double_spelling(s: str) -> float | None:
    if not re.fullmatch(
        r"[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?|[+-]?(Inf|NaN)", s, re.ASCII
    ):
        return None
    try:
        f = float(s.replace("Inf", "inf"))
    except ValueError:
        return None
    return f if go_format_g(f) == s else None


def definite_equality(op: CmpOp, l: Expr, r: Expr) -> Expr | None:
    if op not in (CmpOp.EQ, CmpOp.NE):
        return None
    lcol, rcol = explicit_null_column(l), explicit_null_column(r)
    if lcol is None and rcol is None:
        return None
    missable = None
    if lcol is None and not isinstance(l, (Lit, BoolConst)):
        missable = l
    elif rcol is None and not isinstance(r, (Lit, BoolConst)):
        missable = r
    present: list[Expr] = []
    if lcol is not None:
        present.append(IsNull(lcol, True))
    if rcol is not None:
        present.append(IsNull(rcol, True))
    present.append(Cmp(CmpOp.EQ, l, r))
    equality = and_(*present)
    if lcol is not None and rcol is not None:
        equality = or_(and_(IsNull(lcol), IsNull(rcol)), equality)
    if op == CmpOp.NE:
        equality = Not(equality)
    if missable is not None:
        return when(IsNull(missable, True), equality)
    return equality


def null_comparison(op: CmpOp, l: Any, r: Any) -> Expr | None:
    if op not in (CmpOp.EQ, CmpOp.NE):
        return None
    if l is not None and r is not None:
        return None
    other = l if r is None else r
    if other is None:
        return BoolConst(op == CmpOp.EQ)
    return IsNull(as_expr(other), op == CmpOp.NE)


def omitted_null_comparison(op: CmpOp, x: Expr) -> Expr:
    return when(IsNull(x), Lit(None), else_=BoolConst(op == CmpOp.NE))


def distribute_mixed_case(op: CmpOp, l: Any, r: Any) -> Expr | None:
    if isinstance(l, Case):
        c, other, on_left = l, r, True
    elif isinstance(r, Case):
        c, other, on_left = r, l, False
    else:
        return None
    kinds = case_arm_kinds(c)
    if kinds is None or len(kinds) <= 1:
        return None

    def cmp(arm: Expr) -> Expr:
        return (
            apply_comparison(op, arm, other)
            if on_left
            else apply_comparison(op, other, arm)
        )

    whens = tuple((cond, cmp(then)) for cond, then in c.whens)
    els = cmp(c.else_) if c.else_ is not None else None
    out = Case(whens, els)
    if all(isinstance(t, Lit) and t.v is None for _, t in whens) and (
        els is None or (isinstance(els, Lit) and els.v is None)
    ):
        return Lit(None)
    return out


def compare_mixed_types(op: CmpOp, l: Any, r: Any) -> Expr | None:
    lk, rk = scalar_kind(l), scalar_kind(r)
    if lk == "" or rk == "" or lk == rk:
        return None
    if op not in (CmpOp.EQ, CmpOp.NE):
        return Lit(None)
    result = _mixed_type_equality(op, l, r)
    for operand in (l, r):
        if isinstance(operand, Col) and operand.explicit_null:
            continue
        if isinstance(operand, Expr):
            result = when(IsNull(operand, True), result)
    return result


def _mixed_type_equality(op: CmpOp, l: Any, r: Any) -> Expr:
    if not (
        isinstance(l, Col)
        and isinstance(r, Col)
        and l.explicit_null
        and r.explicit_null
    ):
        return BoolConst(op == CmpOp.NE)
    result = and_(IsNull(l), IsNull(r))
    return Not(result) if op == CmpOp.NE else result


def constants_equal(l: Any, r: Any) -> bool:
    if l is None:
        return r is None
    if isinstance(l, bool):
        return isinstance(r, bool) and l == r
    if isinstance(l, str):
        return isinstance(r, str) and l == r
    if isinstance(l, float):
        return is_float(r) and l == r
    if isinstance(l, list):
        return isinstance(r, list) and constant_lists_equal(l, r)
    raise Unsupported(
        f"list equality over a {type(l).__name__} element has no constant folding"
    )


def constant_lists_equal(l: list, r: list) -> bool:
    if len(l) != len(r):
        return False
    return all(constants_equal(a, b) for a, b in zip(l, r))


# -- membership --------------------------------------------------------------------------------


def membership(x: Any, values: Any) -> Expr:
    if isinstance(x, list):
        raise Unsupported(
            "membership with a list-valued element cannot be represented by scalar SQL IN"
        )
    if isinstance(x, PositionalElement):
        return x.decided(membership(x.element, values))
    members = values if isinstance(values, list) else [values]
    xe = as_expr(x)
    xkind = scalar_kind(x)
    non_null: list[Expr] = []
    has_null = dropped = False
    for m in members:
        if m is None:
            has_null = True
            continue
        if isinstance(m, (list, dict)):
            dropped = True
            continue
        kind = scalar_kind(m)
        if xkind and kind and kind != xkind:
            dropped = True
            continue
        non_null.append(Lit(m))
    predicates: list[Expr] = []
    if non_null:
        in_list: Expr = InList(xe, tuple(non_null))
        col = explicit_null_column(xe)
        if col is not None and not has_null:
            in_list = and_(IsNull(col, True), in_list)
        predicates.append(in_list)
    if has_null:
        predicates.append(IsNull(xe))
    if not predicates:
        if dropped and explicit_null_column(xe) is None:
            return when(IsNull(xe, True), BoolConst(False))
        return BoolConst(False)
    return or_(*predicates)


# -- timestamps --------------------------------------------------------------------------------

_RFC3339 = re.compile(
    r"(\d{4})-(\d{2})-(\d{2})[Tt]([01]\d|2[0-3]):([0-5]\d):([0-5]\d)"
    r"(?:\.(\d{1,9}))?([Zz]|[+-](?:[01]\d|2[0-3]):[0-5]\d)",
    re.ASCII,
)


def parse_instant(s: str) -> Instant | None:
    m = _RFC3339.fullmatch(s)
    if m is None or m.group(1) == "0000":
        return None
    year, month, day, hour, minute, second = (int(m.group(i)) for i in range(1, 7))
    try:
        base = datetime(year, month, day, hour, minute, second, tzinfo=UTC)
    except ValueError:
        return None
    offset = m.group(8)
    offset_s = 0
    if offset not in ("Z", "z"):
        sign = 1 if offset[0] == "+" else -1
        offset_s = sign * (int(offset[1:3]) * 3600 + int(offset[4:6]) * 60)
    seconds = (base - _EPOCH) // timedelta(seconds=1) - offset_s
    fraction = int((m.group(7) or "0").ljust(9, "0"))
    return Instant(seconds * NS + fraction)


def parse_timestamp(v: Any, entry_is_temporal: bool) -> Any:
    if entry_is_temporal:
        return v
    if not isinstance(v, str):
        if isinstance(v, Expr):
            raise Unsupported(
                "timestamp() over a column requires that column to be mapped with ValueType.TIMESTAMP"
            )
        raise Unsupported(
            "timestamp() requires an RFC-3339 literal or a temporal column"
        )
    t = parse_instant(v)
    if t is None:
        raise Unsupported(f"invalid RFC-3339 timestamp literal: {v}")
    if t < MIN_INSTANT or t > MAX_INSTANT:
        raise Unsupported(
            f"timestamp literal is outside CEL's supported instant range: {v}"
        )
    return t


_DURATION_UNITS = {
    "ns": 1,
    "us": 1_000,
    "µs": 1_000,
    "μs": 1_000,
    "ms": 1_000_000,
    "s": NS,
    "m": 60 * NS,
    "h": 3600 * NS,
}
_DURATION_PART = re.compile(r"(\d*)(?:\.(\d*))?(ns|us|µs|μs|ms|s|m|h)", re.ASCII)


def parse_duration(v: Any) -> DurationConst:
    if not isinstance(v, str):
        raise Unsupported("duration() translates only over a string literal")
    s = v
    neg = False
    if s[:1] in "+-" and s:
        neg = s[0] == "-"
        s = s[1:]
    if s == "0":
        return DurationConst(0)
    if not s:
        raise Unsupported(f'invalid duration literal "{v}"')
    total = 0
    pos = 0
    while pos < len(s):
        m = _DURATION_PART.match(s, pos)
        if m is None or (m.group(1) == "" and not m.group(2)):
            raise Unsupported(f'invalid duration literal "{v}"')
        unit = _DURATION_UNITS[m.group(3)]
        whole = int(m.group(1) or "0")
        frac = m.group(2) or ""
        total += whole * unit
        if frac:
            total += int(frac) * unit // 10 ** len(frac)
        pos = m.end()
    if total > 2**63 - 1 + (1 if neg else 0):
        raise Unsupported(f'invalid duration literal "{v}": out of range')
    return DurationConst(-total if neg else total)


def now_ns() -> int:
    return time.time_ns()


_ERR_TEMPORAL_ARITH = "timestamp arithmetic translates only as a timestamp() value plus or minus a constant duration"


def is_temporal(v: Any) -> bool:
    return isinstance(v, (DurationConst, TimestampShift, TimeSinceValue))


def temporal_arith(op: ArithOp, l: Any, r: Any) -> tuple[Any, bool]:
    if not is_temporal(l) and not is_temporal(r):
        return None, False
    if op == ArithOp.SUB:
        if not isinstance(r, DurationConst):
            raise Unsupported(_ERR_TEMPORAL_ARITH)
        r, op = DurationConst(-r.ns), ArithOp.ADD
    if op != ArithOp.ADD:
        raise Unsupported(_ERR_TEMPORAL_ARITH)
    if isinstance(l, DurationConst) and not isinstance(r, DurationConst):
        l, r = r, l
    if not isinstance(r, DurationConst):
        raise Unsupported(_ERR_TEMPORAL_ARITH)
    if isinstance(l, DurationConst):
        return DurationConst(l.ns + r.ns), True
    if isinstance(l, Instant):
        shifted = Instant(l.ns + r.ns)
        if shifted < MIN_INSTANT or shifted > MAX_INSTANT:
            raise Unsupported(
                "timestamp arithmetic leaves CEL's supported instant range"
            )
        return shifted, True
    if isinstance(l, TimestampShift):
        return TimestampShift(l.x, l.ns + r.ns), True
    if isinstance(l, Col) and l.type == ValueType.TIMESTAMP:
        return TimestampShift(l, r.ns), True
    if (
        isinstance(l, Subquery)
        and l.kind == SubqueryKind.SCALAR
        and isinstance(l.select, Col)
        and l.select.type == ValueType.TIMESTAMP
    ):
        return TimestampShift(l, r.ns), True
    raise Unsupported(_ERR_TEMPORAL_ARITH)


def compare_temporal(op: CmpOp, l: Any, r: Any) -> tuple[Expr | None, bool]:
    if not is_temporal(l) and not is_temporal(r):
        return None, False
    if is_temporal(r) and not is_temporal(l):
        return _compare_temporal_left(op.mirror(), r, l), True
    return _compare_temporal_left(op, l, r), True


def _compare_temporal_left(op: CmpOp, l: Any, r: Any) -> Expr:
    if isinstance(l, DurationConst):
        if isinstance(r, DurationConst):
            return BoolConst(compare_ordered(op, l.ns, r.ns))
        if isinstance(r, TimeSinceValue):
            return _compare_temporal_left(op.mirror(), r, l)
    elif isinstance(l, TimestampShift) and isinstance(r, Instant):
        return compare(op, l.x, Instant(r.ns - l.ns))
    elif isinstance(l, TimeSinceValue) and isinstance(r, DurationConst):
        return compare(op.mirror(), l.x, Instant(now_ns() - r.ns))
    raise Unsupported(
        "a duration compares only with a duration, and a shifted timestamp() only with a timestamp constant"
    )


def time_since(operand_is_timestamp_call: bool, v: Any) -> Any:
    if isinstance(v, Instant):
        return DurationConst(now_ns() - v.ns)
    if operand_is_timestamp_call and isinstance(v, Expr):
        return TimeSinceValue(v)
    raise Unsupported("timeSince() translates only over a timestamp() value")


def on_microsecond_grid(c: Cmp) -> Cmp:
    x, op = c.l, c.op
    lit = c.r
    if not isinstance(lit, Lit):
        if not isinstance(c.l, Lit):
            return c
        lit, x, op = c.l, c.r, op.mirror()
    t = lit.v
    if not isinstance(t, Instant) or t.ns % 1000 == 0:
        return c
    floor = Lit(t.truncated())
    if op == CmpOp.EQ:
        return Cmp(CmpOp.NE, x, x)
    if op == CmpOp.NE:
        return Cmp(CmpOp.EQ, x, x)
    if op in (CmpOp.GT, CmpOp.GE):
        return Cmp(CmpOp.GT, x, floor)
    return Cmp(CmpOp.LE, x, floor)


# -- hierarchies -------------------------------------------------------------------------------


def new_hierarchy(v: Any, delimiter: Any) -> HierarchyValue:
    if isinstance(v, (list, ListValue)):
        raise Unsupported(
            "hierarchy() over a list of segments has no lowering: it compares segment by segment, "
            "and this translator lowers only a delimited string"
        )
    d = "."
    if delimiter is not None:
        if not isinstance(delimiter, str):
            raise Unsupported("hierarchy() delimiter must be a string constant")
        d = delimiter
    return HierarchyValue(v, d)


def _matching(l: Any, r: Any) -> tuple[HierarchyValue, HierarchyValue]:
    if not isinstance(l, HierarchyValue) or not isinstance(r, HierarchyValue):
        raise Unsupported("hierarchy operator requires hierarchy() operands")
    if l.delimiter != r.delimiter:
        raise Unsupported("hierarchy operands must use the same delimiter")
    return l, r


def ancestor_of(l: Any, r: Any) -> Expr:
    ancestor, descendent = _matching(l, r)
    a, d, delim = ancestor.value, descendent.value, ancestor.delimiter
    if isinstance(a, str) and isinstance(d, str):
        if delim == "":
            return BoolConst(d.startswith(a) and d != a)
        return BoolConst(d.startswith(a + delim))
    if isinstance(d, str):
        parts = list(d) if delim == "" else d.split(delim)
        if delim == "":
            prefixes = ["".join(parts[:i]) for i in range(len(parts))]
        else:
            prefixes = [delim.join(parts[:i]) for i in range(1, len(parts))]
        if not prefixes:
            return when(IsNull(as_expr(a), True), BoolConst(False))
        return membership(a, prefixes)
    if isinstance(a, str):
        prefixed = string_match(d, a + delim, Anchor.STARTS)
        if delim != "":
            return prefixed
        return and_(prefixed, Cmp(CmpOp.NE, as_expr(d), Lit(a)))
    raise Unsupported("hierarchy comparison between two columns is not supported")


def descendent_of(l: Any, r: Any) -> Expr:
    return ancestor_of(r, l)


def hierarchy_overlaps(l: Any, r: Any) -> Expr:
    lh, rh = _matching(l, r)
    return or_(
        compare(CmpOp.EQ, lh.value, rh.value), ancestor_of(lh, rh), ancestor_of(rh, lh)
    )


def upper_ascii(v: Any) -> Any:
    if isinstance(v, str):
        return "".join(chr(ord(c) - 32) if "a" <= c <= "z" else c for c in v)
    if v is None or known_non_string(v):
        return Lit(None)
    return Call(
        "translate",
        (
            as_expr(v),
            Lit("abcdefghijklmnopqrstuvwxyz"),
            Lit("ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
        ),
    )


# -- positional reads --------------------------------------------------------------------------


def positional_comparison(op: CmpOp, l: Any, r: Any) -> tuple[Expr | None, bool]:
    if isinstance(l, PositionalElement):
        return l.decided(compare(op, l.element, r)), True
    if isinstance(r, PositionalElement):
        return r.decided(compare(op, l, r.element)), True
    return None, False


def strict_count(parts: list[Expr]) -> Expr:
    if not parts:
        return Lit(0.0)
    unknown = [TruthTest(p, Truth.UNKNOWN) for p in parts]
    total: Expr | None = None
    for p in parts:
        one = when(p, Lit(1.0), else_=Lit(0.0))
        total = one if total is None else Arith(ArithOp.ADD, total, one)
    return when(or_(*unknown), Lit(None), else_=total)


def strictly_one(parts: list[Expr]) -> Expr:
    if not parts:
        return BoolConst(False)
    unknown = [TruthTest(p, Truth.UNKNOWN) for p in parts]
    return when(
        or_(*unknown),
        Lit(None),
        (Cmp(CmpOp.EQ, strict_count(parts), Lit(1.0)), BoolConst(True)),
        else_=BoolConst(False),
    )
