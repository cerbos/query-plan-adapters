# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Spells the expression tree as DuckDB SQL with ``?`` placeholders."""

from typing import Any

from cerbos_duckdb._expr import (
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
    InList,
    IsNull,
    Lit,
    Logic,
    Not,
    NotDistinct,
    Regex,
    StrMatch,
    Subquery,
    SubqueryKind,
    TableItem,
    Truth,
    TruthTest,
    UnnestItem,
)
from cerbos_duckdb._values import Instant, on_microsecond_grid
from cerbos_duckdb.errors import UnsupportedPlanError as Unsupported
from cerbos_duckdb.mapping import ValueType

_CMP = {
    CmpOp.EQ: "=",
    CmpOp.NE: "<>",
    CmpOp.LT: "<",
    CmpOp.LE: "<=",
    CmpOp.GT: ">",
    CmpOp.GE: ">=",
}
_ARITH = {
    ArithOp.ADD: "+",
    ArithOp.SUB: "-",
    ArithOp.MULT: "*",
    ArithOp.DIV: "/",
    ArithOp.MOD: "%",
}
_TRUTH = {Truth.TRUE: "IS TRUE", Truth.FALSE: "IS FALSE", Truth.UNKNOWN: "IS NULL"}
_FUNCTIONS = frozenset({"len", "length", "nullif", "translate"})


def quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _is_text(e: Expr) -> bool:
    if isinstance(e, Lit):
        return isinstance(e.v, str)
    if isinstance(e, Col):
        return e.type == ValueType.STRING
    if isinstance(e, Concat):
        return True
    if isinstance(e, Cast):
        return e.to == "VARCHAR"
    if isinstance(e, Call):
        return e.name == "translate"
    if isinstance(e, Subquery):
        return (
            e.kind == SubqueryKind.SCALAR
            and e.select is not None
            and _is_text(e.select)
        )
    return False


class Renderer:
    def __init__(self) -> None:
        self.params: list[Any] = []

    def render(self, e: Expr) -> str:
        if isinstance(e, Col):
            col = ".".join(quote(p) for p in (e.qualifier, *e.path))
            # Instants compare as epoch microseconds: a TIMESTAMP column then reads as UTC, not in
            # the session's TimeZone, and a DATE column as its midnight UTC.
            return f"epoch_us({col})" if e.type == ValueType.TIMESTAMP else col
        if isinstance(e, Lit):
            return self.bind(e.v)
        if isinstance(e, BoolConst):
            return "TRUE" if e.v else "FALSE"
        if isinstance(e, Cmp):
            e = on_microsecond_grid(e)
            l, r = self.collated_pair(e.l, e.r)
            return f"({l} {_CMP[e.op]} {r})"
        if isinstance(e, Arith):
            return f"({self.render(e.l)} {_ARITH[e.op]} {self.render(e.r)})"
        if isinstance(e, Concat):
            return f"({self.render(e.l)} || {self.render(e.r)})"
        if isinstance(e, Logic):
            sep = " AND " if e.is_and else " OR "
            return "(" + sep.join(self.render(x) for x in e.xs) + ")"
        if isinstance(e, Not):
            return f"(NOT {self.render(e.x)})"
        if isinstance(e, IsNull):
            return f"({self.render(e.x)} {'IS NOT NULL' if e.negate else 'IS NULL'})"
        if isinstance(e, TruthTest):
            return f"({self.render(e.x)} {_TRUTH[e.want]})"
        if isinstance(e, StrMatch):
            receiver, needle = self.collated_pair(e.receiver, e.needle)
            return f"{e.anchor.value}({receiver}, {needle})"
        if isinstance(e, Regex):
            return f"regexp_matches({self.collated(e.x)}, {self.render(e.pattern)})"
        if isinstance(e, NotDistinct):
            l, r = self.collated_pair(e.l, e.r)
            return f"({l} IS NOT DISTINCT FROM {r})"
        if isinstance(e, InList):
            text = any(_is_text(v) for v in e.vs) or _is_text(e.x)
            x = self.collated(e.x) if text else self.render(e.x)
            return f"({x} IN ({', '.join(self.render(v) for v in e.vs)}))"
        if isinstance(e, Case):
            parts = ["CASE"]
            for cond, then in e.whens:
                parts.append(f"WHEN {self.render(cond)} THEN {self.render(then)}")
            if e.else_ is not None:
                parts.append(f"ELSE {self.render(e.else_)}")
            return "(" + " ".join(parts) + " END)"
        if isinstance(e, Call):
            if e.name not in _FUNCTIONS:
                raise Unsupported(f'cannot render function "{e.name}"')
            return f"{e.name}({', '.join(self.render(a) for a in e.args)})"
        if isinstance(e, Cast):
            if e.to not in ("VARCHAR", "DOUBLE"):
                raise Unsupported(f'cannot render cast to "{e.to}"')
            return f"CAST({self.render(e.x)} AS {e.to})"
        if isinstance(e, Subquery):
            return self.subquery(e)
        raise Unsupported(f"cannot render expression of type {type(e).__name__}")

    def bind(self, v: Any) -> str:
        if v is None:
            return "NULL"
        if isinstance(v, bool):
            return "TRUE" if v else "FALSE"
        if isinstance(v, Instant):
            self.params.append(v.ns // 1000)
            return "CAST(? AS BIGINT)"
        if isinstance(v, str):
            cast = "VARCHAR"
        elif isinstance(v, float):
            cast = "DOUBLE"
        elif isinstance(v, int):
            cast = "BIGINT"
        else:
            raise Unsupported(f"cannot bind a {type(v).__name__} parameter")
        self.params.append(v)
        return f"CAST(? AS {cast})"

    def collated(self, e: Expr) -> str:
        # The cast lets ENUM, UUID and JSON columns declared as strings take a collation.
        text = (
            self.render(e)
            if isinstance(e, Lit)
            else f"CAST({self.render(e)} AS VARCHAR)"
        )
        return f'({text} COLLATE "binary")'

    def collated_pair(self, l: Expr, r: Expr) -> tuple[str, str]:
        lt, rt = _is_text(l), _is_text(r)
        return (
            self.collated(l) if lt else self.render(l),
            self.collated(r) if rt else self.render(r),
        )

    def subquery(self, s: Subquery) -> str:
        if s.kind == SubqueryKind.EXISTS:
            select = "SELECT 1"
        elif s.kind == SubqueryKind.COUNT:
            select = "SELECT count(*)"
        else:
            assert s.select is not None
            select = f"SELECT {self.render(s.select)}"
        items = []
        for item in s.from_:
            if isinstance(item, TableItem):
                items.append(f"{quote(item.table)} AS {quote(item.alias)}")
            elif isinstance(item, UnnestItem):
                items.append(
                    f'unnest({self.render(item.source)}) WITH ORDINALITY AS {quote(item.alias)}("v", "o")'
                )
        conds = [self.render(c) for c in (s.correlate, s.where) if c is not None]
        where = f" WHERE {' AND '.join(conds)}" if conds else ""
        body = f"{select} FROM {', '.join(items)}{where}"
        return f"(EXISTS ({body}))" if s.kind == SubqueryKind.EXISTS else f"({body})"
