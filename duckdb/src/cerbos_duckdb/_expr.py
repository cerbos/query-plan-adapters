# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""The SQL expression tree the translator builds and the renderer spells for DuckDB."""

import enum
from dataclasses import dataclass
from typing import Any

from cerbos_duckdb.mapping import ValueType


class CmpOp(enum.Enum):
    EQ = "eq"
    NE = "ne"
    LT = "lt"
    LE = "le"
    GT = "gt"
    GE = "ge"

    def mirror(self) -> "CmpOp":
        return _MIRROR.get(self, self)


_MIRROR = {
    CmpOp.LT: CmpOp.GT,
    CmpOp.GT: CmpOp.LT,
    CmpOp.LE: CmpOp.GE,
    CmpOp.GE: CmpOp.LE,
}


class ArithOp(enum.Enum):
    ADD = "add"
    SUB = "sub"
    MULT = "mult"
    DIV = "div"
    MOD = "mod"


class Truth(enum.Enum):
    TRUE = "true"
    FALSE = "false"
    UNKNOWN = "unknown"


class Anchor(enum.Enum):
    CONTAINS = "contains"
    STARTS = "starts_with"
    ENDS = "ends_with"


class SubqueryKind(enum.Enum):
    EXISTS = "exists"
    COUNT = "count"
    SCALAR = "scalar"


class Expr:
    __slots__ = ()


@dataclass(frozen=True)
class Col(Expr):
    qualifier: str
    path: tuple[str, ...]
    explicit_null: bool = False
    type: ValueType = ValueType.DEFAULT


@dataclass(frozen=True)
class Lit(Expr):
    v: Any


@dataclass(frozen=True)
class BoolConst(Expr):
    v: bool


@dataclass(frozen=True)
class Cmp(Expr):
    op: CmpOp
    l: Expr
    r: Expr


@dataclass(frozen=True)
class Arith(Expr):
    op: ArithOp
    l: Expr
    r: Expr


@dataclass(frozen=True)
class Concat(Expr):
    l: Expr
    r: Expr


@dataclass(frozen=True)
class Logic(Expr):
    is_and: bool
    xs: tuple[Expr, ...]


@dataclass(frozen=True)
class Not(Expr):
    x: Expr


@dataclass(frozen=True)
class IsNull(Expr):
    x: Expr
    negate: bool = False


@dataclass(frozen=True)
class TruthTest(Expr):
    x: Expr
    want: Truth


@dataclass(frozen=True)
class StrMatch(Expr):
    receiver: Expr
    needle: Expr
    anchor: Anchor


@dataclass(frozen=True)
class Regex(Expr):
    x: Expr
    pattern: Expr


@dataclass(frozen=True)
class NotDistinct(Expr):
    l: Expr
    r: Expr


@dataclass(frozen=True)
class InList(Expr):
    x: Expr
    vs: tuple[Expr, ...]


@dataclass(frozen=True)
class Case(Expr):
    whens: tuple[tuple[Expr, Expr], ...]
    else_: Expr | None = None


@dataclass(frozen=True)
class Call(Expr):
    name: str
    args: tuple[Expr, ...]


@dataclass(frozen=True)
class Cast(Expr):
    x: Expr
    to: str


@dataclass(frozen=True)
class TableItem:
    table: str
    alias: str


@dataclass(frozen=True)
class UnnestItem:
    """``unnest(source) WITH ORDINALITY AS alias(v, o)``: ``v`` is the element, ``o`` from 1."""

    source: Expr
    alias: str


FromItem = TableItem | UnnestItem


@dataclass(frozen=True)
class Subquery(Expr):
    kind: SubqueryKind
    from_: tuple[FromItem, ...]
    correlate: Expr | None
    where: Expr | None = None
    select: Expr | None = None


def combine(is_and: bool, xs: list[Expr] | tuple[Expr, ...]) -> Expr:
    if not xs:
        return BoolConst(is_and)
    if len(xs) == 1:
        return xs[0]
    return Logic(is_and, tuple(xs))


def and_(*xs: Expr) -> Expr:
    return combine(True, xs)


def or_(*xs: Expr) -> Expr:
    return combine(False, xs)


def when(
    cond: Expr, then: Expr, *more: tuple[Expr, Expr], else_: Expr | None = None
) -> Case:
    return Case(((cond, then), *more), else_)
