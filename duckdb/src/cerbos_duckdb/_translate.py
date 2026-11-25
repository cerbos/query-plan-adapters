# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Walks the plan's condition tree and lowers it into the expression tree in ``_expr``."""

import functools
import math
import re
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from typing import Any

import duckdb

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
    and_,
    or_,
    when,
)
from cerbos_duckdb._plan import Node, expr_node, value_node, variable_node
from cerbos_duckdb._values import (
    CondValue,
    CountedList,
    DeferredCollection,
    IEEEConst,
    Instant,
    ListValue,
    NumberText,
    PositionalElement,
    Scope,
    ancestor_of,
    arith_over_conditional,
    as_expr,
    as_float_expr,
    compare,
    constants_equal,
    descendent_of,
    explicit_null_column,
    float_div,
    fold_arithmetic,
    hierarchy_overlaps,
    is_constant,
    is_float,
    known_non_string,
    membership,
    negate,
    new_hierarchy,
    omitted_null_comparison,
    parse_duration,
    parse_timestamp,
    scalar_kind,
    strict_count,
    strictly_one,
    string_match,
    temporal_arith,
    time_since,
    upper_ascii,
)
from cerbos_duckdb.errors import UnsupportedPlanError as Unsupported
from cerbos_duckdb.mapping import (
    Entry,
    Join,
    ListOf,
    NullConvention,
    NullRepresentation,
    Restriction,
    RestrictOp,
    ValueType,
)

ALIAS_PREFIX = "cerbos_rel_"

_COMPARISON = {
    "eq": CmpOp.EQ,
    "ne": CmpOp.NE,
    "lt": CmpOp.LT,
    "le": CmpOp.LE,
    "gt": CmpOp.GT,
    "ge": CmpOp.GE,
}
_ARITHMETIC = {
    "add": ArithOp.ADD,
    "sub": ArithOp.SUB,
    "mult": ArithOp.MULT,
    "div": ArithOp.DIV,
    "mod": ArithOp.MOD,
}
_BINARY: dict[str, Callable[[Any, Any], Expr]] = {
    "in": membership,
    "contains": lambda l, r: string_match(l, r, Anchor.CONTAINS),
    "startsWith": lambda l, r: string_match(l, r, Anchor.STARTS),
    "endsWith": lambda l, r: string_match(l, r, Anchor.ENDS),
    "ancestorOf": ancestor_of,
    "descendentOf": descendent_of,
    "overlaps": hierarchy_overlaps,
}
_LAMBDA_BINDING = frozenset({"exists", "exists_one", "all", "filter", "map"})
_FOLDABLE = frozenset({"exists", "all", "exists_one"})
_NON_BOOLEAN = frozenset({ValueType.STRING, ValueType.NUMBER, ValueType.TIMESTAMP})
_COLLECTION_VALUED = frozenset({"filter", "map", "except", "intersect"})
_NULL_FREE = frozenset(
    {
        "add", "sub", "mult", "div", "mod", "size", "int", "double", "string", "not", "and",
        "or", "eq", "ne", "lt", "le", "gt", "ge", "in", "contains", "startsWith", "endsWith",
        "matches", "exists", "exists_one", "all", "hasIntersection",
    }
)  # fmt: skip
_LIST_ELEMENT = "__cerbos_list_element"


# -- mappers -----------------------------------------------------------------------------------


class _Mapper:
    def resolve(self, reference: str) -> Entry | None:
        raise NotImplementedError


class _CallerMapper(_Mapper):
    def __init__(
        self, mapper: Mapping[str, Entry] | Callable[[str], Entry | None]
    ) -> None:
        self._mapper = mapper

    def resolve(self, reference: str) -> Entry | None:
        if isinstance(self._mapper, Mapping):
            entry = self._mapper.get(reference)
        else:
            entry = self._mapper(reference)
        if entry is None:
            return None
        if not isinstance(entry, Entry):
            raise TypeError(
                f"the mapper returned {type(entry).__name__} for {reference!r}, not an Entry"
            )
        if entry.qualifier is not None and entry.qualifier.startswith(ALIAS_PREFIX):
            return None
        return entry


class _RootMapper(_Mapper):
    def __init__(self, parent: _Mapper, table: str) -> None:
        self.parent, self.table = parent, table

    def resolve(self, reference: str) -> Entry | None:
        entry = self.parent.resolve(reference)
        if entry is not None and entry.qualifier is None:
            entry = replace(entry, qualifier=self.table)
        return entry


class _ScopedMapper(_Mapper):
    def __init__(self, parent: _Mapper, of: ListOf, variable: str, alias: str) -> None:
        self.parent, self.of, self.variable, self.alias = parent, of, variable, alias

    def element(self, e: Entry) -> Entry:
        return replace(e, qualifier=self.alias, column=("v", *e.path))

    def resolve(self, reference: str) -> Entry | None:
        if reference == self.variable:
            return (
                self.element(self.of.element) if self.of.element is not None else None
            )
        prefix = self.variable + "."
        if reference.startswith(prefix):
            e = self.of.fields.get(reference[len(prefix) :])
            return self.element(e) if e is not None else None
        return self.parent.resolve(reference)


@dataclass(frozen=True)
class _Coll:
    """A list-valued attribute: the ``LIST`` expression on the enclosing row, and its elements."""

    source: Expr
    of: ListOf


def _require_value_type(reference: str, e: Entry) -> None:
    if e.value_type == ValueType.DEFAULT:
        raise Unsupported(
            f'attribute "{reference}" maps to a column with no value_type: DuckDB casts between '
            "types implicitly where CEL compares them unequal ('5' = 5 is true), so declare it as "
            "ValueType.STRING, NUMBER, BOOL or TIMESTAMP"
        )


def _element_col(alias: str, e: Entry) -> Col:
    _require_value_type("a list element", e)
    return Col(
        alias,
        ("v", *e.path),
        e.null_convention == NullConvention.EXPLICIT,
        e.value_type,
    )


def _restrictions(alias: str, restrictions: tuple[Restriction, ...]) -> list[Expr]:
    preds: list[Expr] = []
    for r in restrictions:
        col = Col(alias, (r.column,))
        if r.op in (RestrictOp.EQ, RestrictOp.NE) and r.value is None:
            raise ValueError(
                f"Restriction on {r.column!r} compares with None, which matches no row: "
                "use RestrictOp.IS_NULL or RestrictOp.IS_NOT_NULL"
            )
        if r.op == RestrictOp.EQ:
            preds.append(Cmp(CmpOp.EQ, col, Lit(r.value)))
        elif r.op == RestrictOp.NE:
            preds.append(Cmp(CmpOp.NE, col, Lit(r.value)))
        elif r.op == RestrictOp.IS_NULL:
            preds.append(IsNull(col))
        elif r.op == RestrictOp.IS_NOT_NULL:
            preds.append(IsNull(col, True))
        else:
            if not r.values:
                preds.append(BoolConst(r.op == RestrictOp.NOT_IN))
                continue
            in_list: Expr = InList(col, tuple(Lit(v) for v in r.values))
            preds.append(Not(in_list) if r.op == RestrictOp.NOT_IN else in_list)
    return preds


def _is_element_reference(m: _Mapper, reference: str) -> bool:
    while isinstance(m, _ScopedMapper):
        if reference.startswith(m.variable + "."):
            return True
        m = m.parent
    return False


# -- the builder -------------------------------------------------------------------------------


class Builder:
    def __init__(self, table: str, null_representation: NullRepresentation) -> None:
        self.table = table
        self.null_representation = null_representation
        self._aliases = 0

    def new_alias(self) -> str:
        self._aliases += 1
        return f"{ALIAS_PREFIX}{self._aliases}"

    def build(
        self, root: Node, mapper: Mapping[str, Entry] | Callable[[str], Entry | None]
    ) -> Expr:
        if self.table.startswith(ALIAS_PREFIX):
            raise Unsupported(
                f'table "{self.table}" collides with the "{ALIAS_PREFIX}" prefix reserved for '
                "generated subquery aliases"
            )
        m: _Mapper = _CallerMapper(mapper)
        _assert_no_null_operands(root, m, self.null_representation)
        return self.predicate(root, _RootMapper(m, self.table), False)

    # -- relations -----------------------------------------------------------------------------

    def _join_scope(self, join: Join, alias: str, parent: str) -> Scope:
        return self._join_hops(
            join,
            parent,
            alias,
            [TableItem(join.table, alias)],
            _restrictions(alias, join.subquery_filter),
        )

    def _join_hops(
        self, join: Join, parent: str, inner: str, from_: list, preds: list[Expr]
    ) -> Scope:
        for hop in join.via:
            hop_alias = self.new_alias()
            from_.append(TableItem(hop.table, hop_alias))
            if inner:
                preds.append(
                    Cmp(
                        CmpOp.EQ,
                        Col(inner, (hop.child_column,)),
                        Col(hop_alias, (hop.join_column,)),
                    )
                )
            preds.extend(_restrictions(hop_alias, hop.subquery_filter))
            inner = hop_alias
        preds.append(
            Cmp(
                CmpOp.EQ,
                Col(inner, (join.target_column,)),
                Col(parent, (join.source_column,)),
            )
        )
        return Scope(tuple(from_), and_(*preds))

    def _require_join_hops(self, join: Join, parent: str, expr: Expr) -> Expr:
        if not join.via:
            return expr
        return when(self._join_hops(join, parent, "", [], []).exists(None), expr)

    def scalar_through_hop(self, entry: Entry) -> Expr:
        assert entry.join is not None and entry.qualifier is not None
        alias = self.new_alias()
        s = self._join_scope(entry.join, alias, entry.qualifier)
        select = Col(
            alias,
            entry.path,
            entry.null_convention == NullConvention.EXPLICIT,
            entry.value_type,
        )
        return Subquery(SubqueryKind.SCALAR, s.from_, s.correlate, None, select)

    def _coll_of(self, entry: Entry) -> _Coll:
        assert entry.list is not None and entry.qualifier is not None
        if entry.join is not None:
            source = self.scalar_through_hop(
                replace(entry, null_convention=NullConvention.UNSET)
            )
        else:
            source = Col(entry.qualifier, entry.path)
        return _Coll(source, entry.list)

    def relation_for(self, m: _Mapper, n: Node) -> _Coll | None:
        if not n.is_variable:
            return None
        entry = m.resolve(n.variable)
        if entry is None or entry.list is None:
            return None
        return self._coll_of(entry)

    def require_relation(self, m: _Mapper, reference: str) -> _Coll:
        entry = m.resolve(reference)
        if entry is None:
            raise Unsupported(f'no mapping for collection attribute "{reference}"')
        if entry.list is None and entry.join is not None:
            raise Unsupported(
                f'attribute "{reference}" is a to-one relation, one joined row, but is used as a '
                "collection: a CEL macro over a map ranges over its keys, and SQL has no form for "
                "the set of a row's present columns"
            )
        if entry.list is None:
            raise Unsupported(
                f'attribute "{reference}" is mapped to a column but is used as a collection; map it '
                "as a LIST column with ListOf"
            )
        return self._coll_of(entry)

    @staticmethod
    def scope(coll: _Coll, alias: str) -> Scope:
        return Scope((UnnestItem(coll.source, alias),), None)

    @staticmethod
    def require_list(coll: _Coll, expr: Expr) -> Expr:
        return when(IsNull(coll.source, True), expr)

    def tri_state_exists(
        self, coll: _Coll, alias: str, body: Expr, exists: bool, negated: bool
    ) -> Expr:
        s = self.scope(coll, alias)
        witness, witness_result, default = (
            (Truth.TRUE, True, False) if exists else (Truth.FALSE, False, True)
        )
        tri = when(
            s.exists(TruthTest(body, witness)),
            BoolConst(witness_result),
            (s.exists(TruthTest(body, Truth.UNKNOWN)), Lit(None)),
            else_=BoolConst(default),
        )
        return negate(self.require_list(coll, tri), negated)

    # -- predicates ----------------------------------------------------------------------------

    def predicate(self, n: Node, m: _Mapper, negated: bool) -> Expr:
        if n.is_variable:
            entry = m.resolve(n.variable)
            if entry is not None and entry.value_type in _NON_BOOLEAN:
                return Lit(None)
            return negate(as_expr(self.resolve_variable(n.variable, m)), negated)
        if n.is_value:
            if not isinstance(n.value, bool):
                raise Unsupported(
                    f"non-boolean constant {n.value!r} in a boolean position"
                )
            return BoolConst(n.value != negated)

        op = n.operator
        if op in ("and", "or"):
            if not n.operands:
                raise Unsupported(f"'{op}' requires at least one operand")
            parts = [self.predicate(c, m, negated) for c in n.operands]
            return and_(*parts) if (op == "and") != negated else or_(*parts)
        if op == "not":
            if len(n.operands) != 1:
                raise Unsupported("'not' requires exactly one operand")
            return self.predicate(n.operands[0], m, not negated)
        if op == "if":
            return self.ternary_predicate(n, m, negated)
        if op in _COLLECTION_VALUED:
            if op in ("filter", "map"):
                self.value(n, m)
            return Lit(None)
        if op in _LAMBDA_BINDING:
            return self.collection_macro(n, m, negated)
        if op in _COMPARISON or op in _BINARY:
            return self.binary_predicate(n, m, negated)
        if op == "has":
            return self.presence(n, m, negated)
        if op == "hasIntersection":
            return self.has_intersection(n, m, negated)
        if op in ("index", "get-field"):
            v = self.value(n, m)
            if isinstance(v, PositionalElement):
                kind = scalar_kind(v.element)
                if kind not in ("", "bool"):
                    return Lit(None)
                return negate(v.decided(v.element), negated)
            return negate(as_expr(v), negated)
        if op == "isSubset":
            return self.is_subset(n, m, negated)
        if op == "matches":
            return self.matches(n, m, negated)
        if op == "lambda":
            raise Unsupported("lambda expression outside a collection macro")
        raise Unsupported(f"unsupported operator: {op}")

    def ternary_predicate(self, n: Node, m: _Mapper, negated: bool) -> Expr:
        if len(n.operands) != 3:
            raise Unsupported("'if' requires exactly three operands")
        cond = self.predicate(n.operands[0], m, False)
        then = self.predicate(n.operands[1], m, negated)
        els = self.predicate(n.operands[2], m, negated)
        return when(cond, then, (Not(cond), els))

    def binary_predicate(self, n: Node, m: _Mapper, negated: bool) -> Expr:
        if len(n.operands) != 2:
            raise Unsupported(
                f'expected a binary operation: op = "{n.operator}", # of operands = {len(n.operands)}'
            )

        lifted = _lift_list_ternary(n)
        if lifted is not None:
            return self.ternary_predicate(lifted, m, negated)
        rewritten = _empty_list_comparison(n)
        if rewritten is not None:
            return self.predicate(rewritten, m, negated)
        rewritten = self._whole_list_equality(n, m)
        if rewritten is not None:
            return self.predicate(rewritten, m, negated)
        if n.operator in _COMPARISON:
            out = self._map_literal_lookup(_COMPARISON[n.operator], n, m)
            if out is not None:
                return negate(out, negated)

        left, right = n.operands
        operator = n.operator
        comparison = operator in _COMPARISON
        cmp_op = _COMPARISON.get(operator, CmpOp.EQ)
        if comparison and left.is_variable and right.is_variable:
            le, re_ = m.resolve(left.variable), m.resolve(right.variable)
            if (
                le is not None
                and re_ is not None
                and ValueType.TIMESTAMP in (le.value_type, re_.value_type)
            ):
                raise Unsupported(
                    "bare temporal attributes compare RFC 3339 strings in CEL; stored timestamps lose "
                    "the original spelling; use timestamp() on both operands"
                )

        if operator == "in":
            parts = self._concatenated_haystack(m, right)
            if parts is not None:
                return self.predicate(
                    expr_node("or", *(expr_node("in", left, p) for p in parts)),
                    m,
                    negated,
                )
            coll = self.relation_for(m, right)
            if coll is not None:
                return self.membership_over_relation(left, coll, m, negated)
            coll = self.relation_for(m, left)
            if coll is not None:
                return self.membership_over_relation(right, coll, m, negated)
            if _is_scalar_column(m, right):
                return Lit(None)

        if left.is_value and right.is_variable and (comparison or operator == "in"):
            left, right = right, left
            cmp_op = cmp_op.mirror()

        if comparison:
            out = self._bare_temporal_against_timestamp(cmp_op, left, right, m)
            if out is not None:
                return negate(out, negated)
            if _omitted_null_operand(cmp_op, left, right, m):
                x = as_expr(self.value(left, m))
                return negate(omitted_null_comparison(cmp_op, x), negated)

        if operator == "in" and _never_null(left):
            right, emptied = _without_null_members(right)
            if emptied:
                x = as_expr(self.value(left, m))
                return negate(when(IsNull(x, True), BoolConst(False)), negated)

        lv = self.value(left, m)
        rv = self.value(right, m)
        if comparison:
            out = compare(cmp_op, lv, rv)
        elif operator == "in" and isinstance(rv, DeferredCollection):
            out = self.membership_in_deferred(lv, rv)
        elif operator == "in" and isinstance(rv, ListValue):
            out = _membership_in_list(lv, rv)
        else:
            out = _BINARY[operator](lv, rv)
        return negate(out, negated)

    def membership_over_relation(
        self, needle: Node, coll: _Coll, m: _Mapper, negated: bool
    ) -> Expr:
        if coll.of.element is None:
            raise Unsupported(
                "membership against a list requires its element to be mapped (set ListOf.element)"
            )
        needle_value = self.value(needle, m)
        alias = self.new_alias()
        element = _element_col(alias, coll.of.element)
        omitted = self.null_representation == NullRepresentation.OMITTED
        if needle.is_variable:
            entry = m.resolve(needle.variable)
            if entry is not None and entry.null_convention != NullConvention.UNSET:
                omitted = entry.null_convention == NullConvention.OMITTED
        null_safe = (
            not omitted or coll.of.element.null_convention == NullConvention.EXPLICIT
        )
        body = _element_matches(element, needle_value, null_safe)
        result = self.tri_state_exists(coll, alias, body, True, negated)
        if isinstance(needle_value, Expr) and omitted:
            return when(IsNull(needle_value, True), result)
        return result

    def collection_macro(self, n: Node, m: _Mapper, negated: bool) -> Expr:
        if len(n.operands) != 2:
            raise Unsupported(f"'{n.operator}' requires exactly two operands")
        collection, lam = n.operands
        if collection.is_value:
            return self._fold_value_list_macro(
                n.operator, collection.value, lam, m, negated
            )
        if collection.is_expr and collection.operator == "list":
            v = self.list_constructor(collection, m)
            if isinstance(v, ListValue):
                return self._fold_list_constructor_macro(n.operator, v, lam, m, negated)
            return self._fold_value_list_macro(n.operator, v, lam, m, negated)
        if not collection.is_variable:
            raise Unsupported(
                f"'{n.operator}' requires a collection attribute or a literal list"
            )

        body, variable = _lambda_parts(lam, n.operator)
        coll = self.require_relation(m, collection.variable)
        alias = self.new_alias()
        body_expr = self.predicate(
            body, _ScopedMapper(m, coll.of, variable, alias), False
        )

        if n.operator == "exists":
            return self.tri_state_exists(coll, alias, body_expr, True, negated)
        if n.operator == "all":
            return self.tri_state_exists(coll, alias, body_expr, False, negated)
        if n.operator == "exists_one":
            s = self.scope(coll, alias)
            count = s.count(TruthTest(body_expr, Truth.TRUE))
            tri = when(
                s.exists(TruthTest(body_expr, Truth.UNKNOWN)),
                Lit(None),
                (Cmp(CmpOp.EQ, count, Lit(1.0)), BoolConst(True)),
                else_=BoolConst(False),
            )
            return negate(self.require_list(coll, tri), negated)
        raise Unsupported(
            f"'{n.operator}' produces a collection rather than a boolean; it only translates inside "
            "size() or hasIntersection(), which give the collection a scalar meaning"
        )

    def _fold_value_list_macro(
        self, operator: str, elements: Any, lam: Node, m: _Mapper, negated: bool
    ) -> Expr:
        if operator not in _FOLDABLE:
            raise Unsupported(
                f"{operator} over a literal collection value is not supported; only exists(), all() "
                "and exists_one() fold into a flat filter"
            )
        if not isinstance(elements, list):
            raise Unsupported(
                f"{operator} over a literal collection requires a list value"
            )
        body, variable = _lambda_parts(lam, operator)
        inner_negated = negated and operator != "exists_one"
        parts = [
            self.predicate(
                _substitute_lambda_variable(body, variable, e), m, inner_negated
            )
            for e in elements
        ]
        if operator == "exists_one":
            return negate(strictly_one(parts), negated)
        return or_(*parts) if (operator == "exists") != negated else and_(*parts)

    def _fold_list_constructor_macro(
        self, operator: str, lv: ListValue, lam: Node, m: _Mapper, negated: bool
    ) -> Expr:
        if operator not in _FOLDABLE:
            raise Unsupported(
                f"{operator} over a list() of computed elements is not supported; only exists(), "
                "all() and exists_one() fold into a flat filter"
            )
        body, variable = _lambda_parts(lam, operator)
        inner_negated = negated and operator != "exists_one"
        parts = [
            self.predicate(_substitute_lambda_node(body, variable, e), m, inner_negated)
            for e in lv.nodes
        ]
        if operator == "exists_one":
            return negate(lv.when_defined(strictly_one(parts)), negated)
        if (operator == "exists") != negated:
            return lv.when_defined(or_(*parts))
        return lv.when_defined(and_(*parts))

    def has_intersection(self, n: Node, m: _Mapper, negated: bool) -> Expr:
        if len(n.operands) != 2:
            raise Unsupported("'hasIntersection' requires exactly two operands")
        sides = list(n.operands)
        for i, side in enumerate(sides):
            if side.is_expr and side.operator == "list":
                v = self.list_constructor(side, m)
                if isinstance(v, list):
                    sides[i] = value_node(v)
        left, right = sides
        list_node, other = (left, right) if left.is_value else (right, left)
        if not list_node.is_value:
            raise Unsupported(
                "hasIntersection between two stored collections requires a set-valued join that SQL "
                "cannot express as a row filter; one side must be a literal list"
            )
        values = list_node.value
        if not isinstance(values, list):
            raise Unsupported("hasIntersection requires a list value")

        coll = self.relation_for(m, other)
        if coll is not None:
            if coll.of.element is None:
                raise Unsupported(
                    "hasIntersection against a list requires its element to be mapped"
                )
            alias = self.new_alias()
            body = membership(_element_col(alias, coll.of.element), values)
            return self.tri_state_exists(coll, alias, body, True, negated)
        if _is_scalar_column(m, other):
            return Lit(None)
        ov = self.value(other, m)
        if isinstance(ov, DeferredCollection):
            if not ov.is_map:
                raise Unsupported(
                    "hasIntersection() over filter() is not supported; project with map() instead"
                )
            member = membership(ov.body, values)
            tri = when(
                ov.exists(IsNull(ov.body)),
                Lit(None),
                (ov.exists(TruthTest(member, Truth.TRUE)), BoolConst(True)),
                else_=BoolConst(False),
            )
            return negate(self.require_list(ov.coll, tri), negated)
        return negate(membership(ov, values), negated)

    def is_subset(self, n: Node, m: _Mapper, negated: bool) -> Expr:
        if len(n.operands) != 2:
            raise Unsupported("'isSubset' requires exactly two operands")
        sub, sup = n.operands
        coll = self.relation_for(m, sub)
        if coll is None or not sup.is_value:
            raise Unsupported(
                "isSubset translates only a stored list against a literal list: SQL has no set "
                "containment between two stored collections or over a computed list"
            )
        if not isinstance(sup.value, list):
            raise Unsupported("isSubset requires a list value")
        if coll.of.element is None:
            raise Unsupported(
                "isSubset against a list requires its element to be mapped"
            )
        alias = self.new_alias()
        body = membership(_element_col(alias, coll.of.element), sup.value)
        return self.tri_state_exists(coll, alias, body, False, negated)

    def membership_in_deferred(self, needle: Any, d: DeferredCollection) -> Expr:
        if not isinstance(needle, (str, float, bool)):
            raise Unsupported(
                f"membership in a {d.macro()}() result translates only for a constant string, number "
                "or boolean needle"
            )
        if d.is_map:
            match = compare(CmpOp.EQ, d.body, needle)
            unknown: Expr = IsNull(d.body)
            witness: Expr = TruthTest(match, Truth.TRUE)
        else:
            if d.element is None:
                raise Unsupported(
                    "membership in a filter() result requires the list's element to be mapped"
                )
            match = compare(CmpOp.EQ, d.element, needle)
            unknown = TruthTest(d.body, Truth.UNKNOWN)
            witness = and_(TruthTest(d.body, Truth.TRUE), TruthTest(match, Truth.TRUE))
        tri = when(
            d.exists(unknown),
            Lit(None),
            (d.exists(witness), BoolConst(True)),
            else_=BoolConst(False),
        )
        return self.require_list(d.coll, tri)

    def presence(self, n: Node, m: _Mapper, negated: bool) -> Expr:
        if len(n.operands) != 1 or not n.operands[0].is_variable:
            raise Unsupported("'has' requires exactly one attribute reference")
        reference = n.operands[0].variable
        entry = m.resolve(reference)
        if entry is None:
            raise Unsupported(
                f"attribute does not exist in the attribute column map: {reference}"
            )
        omitted = entry.null_convention == NullConvention.OMITTED or (
            entry.null_convention == NullConvention.UNSET
            and (
                self.null_representation == NullRepresentation.OMITTED
                or _is_element_reference(m, reference)
            )
        )
        join = entry.join
        if join is not None and entry.column is None:
            alias = self.new_alias()
            out: Expr = self._require_join_hops(
                join,
                entry.qualifier,
                self._join_scope(join, alias, entry.qualifier).exists(None),
            )
        elif join is not None:
            then: Expr = BoolConst(True)
            if omitted:
                then = IsNull(self.scalar_through_hop(entry), True)
            row_exists = self._join_scope(
                join, self.new_alias(), entry.qualifier
            ).exists(None)
            out = when(row_exists, then)
        elif omitted:
            out = IsNull(Col(entry.qualifier, entry.path), True)
        else:
            out = BoolConst(True)
        return negate(out, negated)

    def matches(self, n: Node, m: _Mapper, negated: bool) -> Expr:
        if len(n.operands) != 2:
            raise Unsupported("'matches' requires exactly two operands")
        receiver = self.value(n.operands[0], m)
        pattern = self.value(n.operands[1], m)
        if known_non_string(receiver) or known_non_string(pattern):
            return negate(when(BoolConst(False), BoolConst(True)), negated)
        if not isinstance(pattern, str):
            raise Unsupported(
                "matches() translates only a constant pattern: one read from a column could hold a "
                "pattern DuckDB's RE2 and CEL's read differently, and only a constant can be checked"
            )
        _check_pattern(pattern)
        if not _compiles(pattern):
            # CEL raises on a pattern RE2 rejects, and UNKNOWN keeps the row out under both polarities.
            return negate(when(BoolConst(False), BoolConst(True)), negated)
        return negate(Regex(as_expr(receiver), as_expr(pattern)), negated)

    # -- values --------------------------------------------------------------------------------

    def value(self, n: Node, m: _Mapper) -> Any:
        if n.is_value:
            return n.value
        if n.is_variable:
            return self.resolve_variable(n.variable, m)
        op = n.operator
        if op in ("and", "or", "not"):
            return self.predicate(n, m, False)
        if op == "if":
            return self.ternary_value(n, m)
        if op == "hierarchy":
            if not 1 <= len(n.operands) <= 2:
                raise Unsupported("'hierarchy' requires one or two operands")
            target = self.value(n.operands[0], m)
            delimiter = self.value(n.operands[1], m) if len(n.operands) == 2 else None
            return new_hierarchy(target, delimiter)
        if op == "size":
            return self.size(n, m)
        if op in ("index", "get-field"):
            return self.index(n, m)
        if op == "list":
            return self.list_constructor(n, m)
        if op == "struct":
            return _struct_literal(n)
        if op == "upperAscii":
            _arity(n, 1)
            return upper_ascii(self.value(n.operands[0], m))
        if op == "duration":
            _arity(n, 1)
            return parse_duration(self.value(n.operands[0], m))
        if op == "timeSince":
            _arity(n, 1)
            operand = n.operands[0]
            return time_since(
                operand.is_expr and operand.operator == "timestamp",
                self.value(operand, m),
            )
        if op in ("except", "intersect"):
            raise Unsupported(
                f"'{op}' produces a collection rather than a plain value; it translates inside size(), "
                "compared with an empty list, or (except) compared with a one-element list"
            )
        if op in ("filter", "map"):
            if len(n.operands) == 2 and n.operands[0].is_value:
                if not isinstance(n.operands[0].value, list):
                    raise Unsupported(
                        f"'{op}' over a literal collection requires a list value"
                    )
                return self._fold_literal_projection(
                    n, n.operands[0].value, n.operands[1], m
                )
            return self.deferred_collection(n, m)
        if op == "timestamp":
            _arity(n, 1)
            operand = n.operands[0]
            temporal = False
            if operand.is_variable:
                entry = m.resolve(operand.variable)
                temporal = entry is not None and entry.value_type == ValueType.TIMESTAMP
            return parse_timestamp(self.value(operand, m), temporal)
        if op in ("double", "int"):
            raise Unsupported(
                f"'{op}()' cannot be lowered to a DuckDB CAST, which reads strings CEL rejects "
                "('1e3', ' 12 ' and '1_000' all cast to BIGINT) and rounds a double where CEL's int() "
                "truncates toward zero (CAST(-0.6 AS BIGINT) is -1, int(-0.6) is 0)"
            )
        if op == "string":
            _arity(n, 1)
            if _renders_numeric_constant(n.operands[0]):
                raise Unsupported(
                    "string() over a numeric constant cannot be lowered: the plan ships an int and a "
                    'double constant as the same number, which CEL renders differently ("1000000" '
                    'and "1e+06")'
                )
            return _cast_value(self.value(n.operands[0], m))
        if op in _ARITHMETIC:
            return self.arithmetic(n, m)
        if op in _LAMBDA_BINDING:
            return self.collection_macro(n, m, False)
        return self.predicate(n, m, False)

    def ternary_value(self, n: Node, m: _Mapper) -> Any:
        if len(n.operands) != 3:
            raise Unsupported("'if' requires exactly three operands")
        cond = self.predicate(n.operands[0], m, False)
        then = self.value(n.operands[1], m)
        els = self.value(n.operands[2], m)
        if isinstance(then, (IEEEConst, CondValue)) or isinstance(
            els, (IEEEConst, CondValue)
        ):
            return CondValue(cond, then, els)
        return when(cond, as_expr(then), (Not(cond), as_expr(els)))

    def index(self, n: Node, m: _Mapper) -> Any:
        if len(n.operands) != 2:
            raise Unsupported(f"'{n.operator}' requires exactly two operands")
        read = self._positional_read(n, m)
        if read is not None:
            return self.positional(*read)
        container, key = n.operands
        if n.operator == "index" and container.is_variable and key.is_value:
            k = key.value
            if isinstance(k, str) and k and "." not in k:
                return self.resolve_variable(f"{container.variable}.{k}", m)
        raise Unsupported(
            "index() translates only a constant string key into a map attribute whose member is "
            "mapped, or a constant position into a LIST column: a map literal indexed by an "
            "attribute has no SQL lookup"
        )

    def _positional_read(self, n: Node, m: _Mapper) -> tuple[_Coll, float, str] | None:
        if not n.is_expr or len(n.operands) != 2:
            return None
        if n.operator == "get-field":
            read, name = n.operands
            if (
                not name.is_variable
                or not name.variable
                or not read.is_expr
                or read.operator != "index"
            ):
                return None
            inner = self._positional_read(read, m)
            if inner is None:
                return None
            return inner[0], inner[1], name.variable
        if n.operator != "index":
            return None
        container, key = n.operands
        if not key.is_value or not is_float(key.value):
            return None
        coll = self.relation_for(m, container)
        if coll is None:
            return None
        return coll, key.value, ""

    def positional(self, coll: _Coll, k: float, field: str) -> Any:
        if field == "":
            entry = coll.of.element
            if entry is None:
                raise Unsupported(
                    "an object list element read by position has no scalar value of its own; select "
                    "one of its fields"
                )
        else:
            entry = coll.of.fields.get(field)
            if entry is None or entry.list is not None:
                raise Unsupported(
                    f'list element field "{field}" is not mapped to a scalar field of the element'
                )
        if k < 0 or k != math.trunc(k):
            return Lit(None)
        alias = self.new_alias()
        s = self.scope(coll, alias)
        s = Scope(s.from_, Cmp(CmpOp.EQ, Col(alias, ("o",)), Lit(k + 1)))
        return PositionalElement(s, _element_col(alias, entry))

    def size(self, n: Node, m: _Mapper) -> Any:
        _arity(n, 1)
        operand = n.operands[0]
        if operand.is_expr and operand.operator == "except":
            return self._difference_size(operand, m)
        if operand.is_expr and operand.operator == "intersect":
            return self._intersection_size(operand, m)
        if operand.is_variable:
            entry = m.resolve(operand.variable)
            if entry is not None and entry.list is not None:
                return Call("len", (self._coll_of(entry).source,))
        v = self.value(operand, m)
        if isinstance(v, DeferredCollection):
            if v.is_map:
                raise Unsupported(
                    "size() over map() is not supported; project with filter() instead"
                )
            return self.require_list(
                v.coll,
                when(
                    v.exists(TruthTest(v.body, Truth.UNKNOWN)),
                    Lit(None),
                    else_=v.count(TruthTest(v.body, Truth.TRUE)),
                ),
            )
        if isinstance(v, str):
            return float(len(v))
        if isinstance(v, list):
            return float(len(v))
        if isinstance(v, CountedList):
            return v.size
        if isinstance(v, ListValue):
            return v.when_defined(Lit(float(len(v.elements))))
        e = as_expr(v)
        if known_non_string(v):
            return Lit(None)
        return Call("length", (e,))

    def arithmetic(self, n: Node, m: _Mapper) -> Any:
        if len(n.operands) != 2:
            raise Unsupported(f"'{n.operator}' requires exactly two operands")
        lv = self.value(n.operands[0], m)
        rv = self.value(n.operands[1], m)
        op = _ARITHMETIC[n.operator]
        if op != ArithOp.ADD and (_non_numeric(lv) or _non_numeric(rv)):
            raise Unsupported(
                f"'{n.operator}' over a string, boolean or bare timestamp operand is a CEL no-overload error"
            )
        if op == ArithOp.DIV:
            return float_div(lv, rv)
        out, ok = temporal_arith(op, lv, rv)
        if ok:
            if n.operands[0].is_variable or n.operands[1].is_variable:
                raise Unsupported(
                    "a duration shifts only a timestamp() value, not a bare attribute, which CEL holds as a string"
                )
            return out
        if op == ArithOp.ADD:
            return _add_value(lv, rv)
        return _numeric_arith(op, lv, rv)

    def list_constructor(self, n: Node, m: _Mapper) -> Any:
        elements = [self.value(o, m) for o in n.operands]
        if all(is_constant(v) for v in elements):
            return elements
        defined: list[Expr] = []
        for v in elements:
            if is_constant(v):
                continue
            x = as_expr(v)
            if explicit_null_column(x) is not None:
                continue
            defined.append(IsNull(x, True))
        return ListValue(
            and_(*defined) if defined else None, tuple(elements), n.operands
        )

    def deferred_collection(self, n: Node, m: _Mapper) -> DeferredCollection:
        if len(n.operands) != 2:
            raise Unsupported(f"'{n.operator}' requires exactly two operands")
        collection, lam = n.operands
        if not collection.is_variable:
            raise Unsupported(f"'{n.operator}' requires a collection attribute")
        coll = self.require_relation(m, collection.variable)
        body, variable = _lambda_parts(lam, n.operator)
        alias = self.new_alias()
        inner = _ScopedMapper(m, coll.of, variable, alias)
        if n.operator == "map":
            body_expr = as_expr(self.value(body, inner))
        else:
            body_expr = self.predicate(body, inner, False)
        element = (
            _element_col(alias, coll.of.element)
            if coll.of.element is not None
            else None
        )
        return DeferredCollection(
            body_expr, element, coll, self.scope(coll, alias), n.operator == "map"
        )

    def resolve_variable(self, reference: str, m: _Mapper) -> Any:
        entry = m.resolve(reference)
        if entry is None:
            raise Unsupported(
                f"attribute does not exist in the attribute column map: {reference}"
            )
        if entry.list is not None:
            raise Unsupported(
                f'attribute "{reference}" maps to a collection and cannot be used as a scalar value'
            )
        if entry.join is not None and entry.column is None:
            raise Unsupported(
                f'attribute "{reference}" is a to-one relation, one joined row, but is used as a value: '
                "a CEL map's keys are the row's present columns, and SQL has no form for the set of a "
                "row's present columns"
            )
        if entry.column is None:
            raise Unsupported(f'attribute "{reference}" maps to no column')
        _require_value_type(reference, entry)
        if entry.join is not None:
            return self.scalar_through_hop(entry)
        return Col(
            entry.qualifier,
            entry.path,
            entry.null_convention == NullConvention.EXPLICIT,
            entry.value_type,
        )

    # -- list shapes ---------------------------------------------------------------------------

    def _concatenated_haystack(self, m: _Mapper, n: Node) -> list[Node] | None:
        if not n.is_expr or n.operator != "add":
            return None
        parts: list[Node] = []
        stored = False

        def walk(part: Node) -> bool:
            nonlocal stored
            if part.is_expr and part.operator == "add" and len(part.operands) == 2:
                return walk(part.operands[0]) and walk(part.operands[1])
            if part.is_value:
                if not isinstance(part.value, list):
                    return False
            else:
                if self.relation_for(m, part) is None:
                    return False
                stored = True
            parts.append(part)
            return True

        if not walk(n) or not stored:
            return None
        return parts

    def _whole_list_equality(self, n: Node, m: _Mapper) -> Node | None:
        if n.operator not in ("eq", "ne"):
            return None
        coll, literal = n.operands
        if coll.is_value:
            coll, literal = literal, coll
        if not literal.is_value or not isinstance(literal.value, list):
            return None
        elements = literal.value
        equality = None
        if coll.is_variable:
            equality = self._relation_equality(coll, "", elements, m)
        elif (
            coll.is_expr
            and coll.operator == "map"
            and len(coll.operands) == 2
            and coll.operands[0].is_variable
        ):
            body, variable = _lambda_parts(coll.operands[1], "map")
            if not body.is_variable:
                return None
            field = ""
            if body.variable != variable:
                prefix = variable + "."
                rest = (
                    body.variable[len(prefix) :]
                    if body.variable.startswith(prefix)
                    else ""
                )
                if not rest or "." in rest:
                    return None
                field = rest
            equality = self._relation_equality(coll.operands[0], field, elements, m)
        elif (
            coll.is_expr
            and coll.operator == "except"
            and len(coll.operands) == 2
            and len(elements) == 1
        ):
            equality = self._difference_equality(coll, elements[0], m)
        if equality is None:
            return None
        return expr_node("not", equality) if n.operator == "ne" else equality

    def _relation_equality(
        self, list_var: Node, field: str, elements: list, m: _Mapper
    ) -> Node | None:
        if self.relation_for(m, list_var) is None:
            return None
        size = expr_node(
            "eq", expr_node("size", list_var), value_node(float(len(elements)))
        )
        if not elements:
            return size
        if len(elements) == 1:
            element = variable_node(_LIST_ELEMENT + ("." + field if field else ""))
            body = expr_node("eq", element, value_node(elements[0]))
            exists = expr_node(
                "exists",
                list_var,
                expr_node("lambda", body, variable_node(_LIST_ELEMENT)),
            )
            return expr_node("and", size, exists)
        conjuncts = [size]
        for k, element in enumerate(elements):
            read = expr_node("index", list_var, value_node(float(k)))
            if field:
                read = expr_node("get-field", read, variable_node(field))
            conjuncts.append(expr_node("eq", read, value_node(element)))
        return expr_node("and", *conjuncts)

    def _difference_equality(self, except_: Node, v: Any, m: _Mapper) -> Node | None:
        list_var, other = except_.operands
        if self.relation_for(m, list_var) is None:
            return None
        size = expr_node("eq", expr_node("size", except_), value_node(1.0))
        element = variable_node(_LIST_ELEMENT)
        kept = expr_node(
            "and",
            expr_node("eq", element, value_node(v)),
            expr_node("not", expr_node("in", element, other)),
        )
        exists = expr_node(
            "exists", list_var, expr_node("lambda", kept, variable_node(_LIST_ELEMENT))
        )
        return expr_node("and", size, exists)

    def _map_literal_lookup(self, op: CmpOp, n: Node, m: _Mapper) -> Expr | None:
        lookup, other = n.operands
        if not other.is_value:
            lookup, other = other, lookup
        if (
            not other.is_value
            or not lookup.is_expr
            or lookup.operator != "index"
            or len(lookup.operands) != 2
        ):
            return None
        container = lookup.operands[0]
        if not container.is_expr or container.operator != "struct":
            return None
        entries = _struct_literal(container)
        key = self.value(lookup.operands[1], m)
        keys = sorted(entries)
        matching = [k for k in keys if constants_equal(entries[k], other.value)]
        is_key = membership(key, list(keys))
        hit = membership(key, matching)
        return when(is_key, negate(hit, op == CmpOp.NE))

    def _fold_literal_projection(
        self, n: Node, elements: list, lam: Node, m: _Mapper
    ) -> Any:
        body, variable = _lambda_parts(lam, n.operator)
        substituted = [_substitute_lambda_variable(body, variable, e) for e in elements]
        if n.operator == "map":
            return self.list_constructor(expr_node("list", *substituted), m)
        return CountedList(
            strict_count([self.predicate(s, m, False) for s in substituted])
        )

    def _difference_size(self, except_: Node, m: _Mapper) -> Any:
        if len(except_.operands) != 2:
            raise Unsupported("'except' requires exactly two operands")
        left, right = except_.operands
        coll = self.relation_for(m, left)
        if coll is not None:
            if not right.is_value:
                raise Unsupported(
                    "size() over except() translates a stored list only against a literal list"
                )
            if not isinstance(right.value, list):
                raise Unsupported("except() requires a list value")
            if coll.of.element is None:
                raise Unsupported(
                    "except() over a list requires its element to be mapped"
                )
            alias = self.new_alias()
            member = membership(_element_col(alias, coll.of.element), right.value)
            s = self.scope(coll, alias)
            count = when(
                s.exists(TruthTest(member, Truth.UNKNOWN)),
                Lit(None),
                else_=s.count(TruthTest(member, Truth.FALSE)),
            )
            return self.require_list(coll, count)
        lv = self.value(left, m)
        if not isinstance(lv, list):
            raise Unsupported(
                "size() over except() translates a stored list or a literal list only"
            )
        rv = self.value(right, m)
        if isinstance(rv, list):
            kept = sum(1 for e in lv if not any(constants_equal(e, c) for c in rv))
            return float(kept)
        if isinstance(rv, ListValue):
            excluded = [_membership_in_list(e, rv) for e in lv]
            return strict_count([Not(x) for x in excluded])
        raise Unsupported(
            "size() over except() translates only against a literal list or a list() of computed elements"
        )

    def _intersection_size(self, intersect: Node, m: _Mapper) -> Any:
        if len(intersect.operands) != 2:
            raise Unsupported("'intersect' requires exactly two operands")
        left, right = intersect.operands
        if left.is_value and right.is_value:
            lv, rv = left.value, right.value
            if not isinstance(lv, list) or not isinstance(rv, list):
                raise Unsupported("intersect() requires list values")
            shorter, longer = (rv, lv) if len(lv) > len(rv) else (lv, rv)
            return float(
                sum(1 for e in shorter if any(constants_equal(e, c) for c in longer))
            )

        stored, literal = left, right
        coll = self.relation_for(m, stored)
        if coll is None:
            stored, literal = right, left
            coll = self.relation_for(m, stored)
            if coll is None:
                raise Unsupported(
                    "size() over intersect() translates a stored list against a literal list only"
                )
        if not literal.is_value:
            raise Unsupported(
                "size() over intersect() translates a stored list against a literal list only"
            )
        members = literal.value
        if not isinstance(members, list):
            raise Unsupported("intersect() requires a list value")
        if coll.of.element is None:
            raise Unsupported(
                "intersect() over a list requires its element to be mapped"
            )

        contained = []
        for member in members:
            alias = self.new_alias()
            match = _element_matches(_element_col(alias, coll.of.element), member, True)
            contained.append(self.tri_state_exists(coll, alias, match, True, False))
        literal_walk = strict_count(contained)

        alias = self.new_alias()
        member_expr = membership(_element_col(alias, coll.of.element), members)
        s = self.scope(coll, alias)
        stored_walk = when(
            s.exists(TruthTest(member_expr, Truth.UNKNOWN)),
            Lit(None),
            else_=s.count(TruthTest(member_expr, Truth.TRUE)),
        )
        size = Call("len", (coll.source,))
        op = CmpOp.LE if stored is left else CmpOp.LT
        walk_stored = Cmp(op, size, Lit(float(len(members))))
        return self.require_list(
            coll, when(walk_stored, stored_walk, else_=literal_walk)
        )

    def _bare_temporal_against_timestamp(
        self, op: CmpOp, left: Node, right: Node, m: _Mapper
    ) -> Expr | None:
        bare, other = (left, right) if left.is_variable else (right, left)
        if not bare.is_variable or not _yields_timestamp(other):
            return None
        entry = m.resolve(bare.variable)
        if (
            entry is None
            or entry.list is not None
            or entry.value_type != ValueType.TIMESTAMP
        ):
            return None
        if op not in (CmpOp.EQ, CmpOp.NE):
            return Lit(None)
        v = self.resolve_variable(bare.variable, m)
        result: Expr = BoolConst(op == CmpOp.NE)
        if isinstance(v, Col) and v.explicit_null:
            return result
        return when(IsNull(as_expr(v), True), result)


# -- free helpers ------------------------------------------------------------------------------


def _arity(n: Node, count: int) -> None:
    if len(n.operands) != count:
        raise Unsupported(f"'{n.operator}' requires exactly {count} operand(s)")


_RE2_LOCK = threading.Lock()
_RE2: list[duckdb.DuckDBPyConnection] = []


@functools.lru_cache(maxsize=256)
def _compiles(pattern: str) -> bool:
    with _RE2_LOCK:
        if not _RE2:
            _RE2.append(duckdb.connect(config={"threads": 1}))
        try:
            _RE2[0].execute("SELECT regexp_matches('', ?)", [pattern])
        except duckdb.Error:
            return False
    return True


_COUNT = re.compile(r"\{(\d+)(?:,(\d*))?\}")


def _check_pattern(pattern: str) -> None:
    i, in_class = 0, False
    while i < len(pattern):
        c = pattern[i]
        if c == "\\" and i + 1 < len(pattern):
            if pattern[i + 1] == "C":
                raise Unsupported(
                    f'cannot translate matches("{pattern}"): \\C matches one byte in DuckDB\'s RE2 and '
                    "is an error in CEL's"
                )
            i += 2
            continue
        if c == "[":
            in_class = True
        elif c == "]":
            in_class = False
        elif (
            c == "("
            and not in_class
            and pattern.startswith("(?<", i)
            and pattern[i + 3 : i + 4] not in ("=", "!")
        ):
            raise Unsupported(
                f'cannot translate matches("{pattern}"): DuckDB\'s RE2 rejects the (?<name>...) '
                "group that CEL's accepts"
            )
        elif c == "{" and not in_class:
            count = _COUNT.match(pattern, i)
            if count is not None and any(len(g) > 4 for g in count.groups() if g):
                raise Unsupported(
                    f'cannot translate matches("{pattern}"): DuckDB\'s RE2 reads a count past 1000 '
                    "that overflows 32 bits as literal text, where CEL's rejects the pattern"
                )
        i += 1


def _is_scalar_column(m: _Mapper, n: Node) -> bool:
    if not n.is_variable:
        return False
    entry = m.resolve(n.variable)
    return (
        entry is not None
        and entry.list is None
        and entry.join is None
        and entry.column is not None
    )


def _element_matches(element: Col, needle: Any, null_safe: bool) -> Expr:
    if needle is None:
        return IsNull(element)
    if not isinstance(needle, Col):
        return compare(CmpOp.EQ, element, needle)
    if not null_safe:
        return Cmp(CmpOp.EQ, element, needle)
    return NotDistinct(element, needle)


def _never_null(n: Node) -> bool:
    if n.is_value:
        return n.value is not None
    if n.is_variable:
        return False
    if n.operator == "if" and len(n.operands) == 3:
        return _never_null(n.operands[1]) and _never_null(n.operands[2])
    return n.operator in _NULL_FREE


def _without_null_members(n: Node) -> tuple[Node, bool]:
    if not n.is_value or not isinstance(n.value, list) or None not in n.value:
        return n, False
    kept = [v for v in n.value if v is not None]
    return value_node(kept), not kept


def _omitted_null_operand(op: CmpOp, left: Node, right: Node, m: _Mapper) -> bool:
    if (
        op not in (CmpOp.EQ, CmpOp.NE)
        or not left.is_variable
        or not right.is_value
        or right.value is not None
    ):
        return False
    entry = m.resolve(left.variable)
    return entry is not None and entry.null_convention == NullConvention.OMITTED


def _membership_in_list(needle: Any, lv: ListValue) -> Expr:
    return lv.when_defined(or_(*(compare(CmpOp.EQ, needle, e) for e in lv.elements)))


def _lambda_parts(operand: Node, operator: str) -> tuple[Node, str]:
    if not operand.is_expr or operand.operator != "lambda":
        raise Unsupported(f"second operand of {operator} must be a lambda expression")
    if len(operand.operands) != 2:
        raise Unsupported(
            f"{operator} supports single-variable lambdas only: the two-variable form binds a list "
            "index or a map key"
        )
    body, variable = operand.operands
    if not variable.is_variable or not variable.variable:
        raise Unsupported("lambda variable must have a name")
    return body, variable.variable


def _rebinds(n: Node, variable: str) -> bool:
    if n.operator not in _LAMBDA_BINDING or len(n.operands) != 2:
        return False
    nested = n.operands[1]
    if not nested.is_expr or nested.operator != "lambda" or len(nested.operands) != 2:
        return False
    v = nested.operands[1]
    return v.is_variable and v.variable == variable


def _substitute_lambda_variable(n: Node, variable: str, element: Any) -> Node:
    if n.is_value:
        return n
    if n.is_variable:
        if n.variable == variable:
            return value_node(element)
        prefix = variable + "."
        if n.variable.startswith(prefix) and len(n.variable) > len(prefix):
            current = element
            for segment in n.variable[len(prefix) :].split("."):
                if not isinstance(current, dict) or segment not in current:
                    raise Unsupported(
                        f'cannot resolve "{n.variable}": collection element has no field "{segment}"'
                    )
                current = current[segment]
            return value_node(current)
        return n
    if _rebinds(n, variable):
        return expr_node(
            n.operator,
            _substitute_lambda_variable(n.operands[0], variable, element),
            n.operands[1],
        )
    return expr_node(
        n.operator,
        *(_substitute_lambda_variable(c, variable, element) for c in n.operands),
    )


def _substitute_lambda_node(n: Node, variable: str, element: Node) -> Node:
    if n.is_value:
        return n
    if n.is_variable:
        if n.variable == variable:
            return element
        if n.variable.startswith(variable + "."):
            raise Unsupported(
                f'cannot select "{n.variable}" from a computed list element'
            )
        return n
    if _rebinds(n, variable):
        return expr_node(
            n.operator,
            _substitute_lambda_node(n.operands[0], variable, element),
            n.operands[1],
        )
    return expr_node(
        n.operator, *(_substitute_lambda_node(c, variable, element) for c in n.operands)
    )


def _struct_literal(n: Node) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for f in n.operands:
        if not f.is_expr or f.operator != "set-field" or len(f.operands) != 2:
            raise Unsupported(
                "struct() translates only as a map literal of set-field() entries"
            )
        key, value = f.operands
        if not key.is_value or not isinstance(key.value, str):
            raise Unsupported("a map literal translates only with constant string keys")
        if not value.is_value:
            raise Unsupported("a map literal translates only with constant values")
        if key.value in out:
            raise Unsupported(
                f'map literal repeats the key "{key.value}", which CEL rejects'
            )
        out[key.value] = value.value
    return out


def _yields_list(n: Node) -> bool:
    if n.is_value:
        return isinstance(n.value, list)
    if not n.is_expr:
        return False
    if n.operator == "add":
        return any(_yields_list(o) for o in n.operands)
    if n.operator == "if":
        return len(n.operands) == 3 and (
            _yields_list(n.operands[1]) or _yields_list(n.operands[2])
        )
    return False


def _lift_list_ternary(n: Node) -> Node | None:
    if not n.is_expr:
        return None
    for i, operand in enumerate(n.operands):
        if operand.is_expr and operand.operator == "add":
            lifted = _lift_list_ternary(operand)
            if lifted is not None:
                operand = lifted
        if (
            not operand.is_expr
            or operand.operator != "if"
            or len(operand.operands) != 3
        ):
            continue
        if not _yields_list(operand.operands[1]) and not _yields_list(
            operand.operands[2]
        ):
            continue

        def with_arm(arm: Node, i: int = i) -> Node:
            operands = list(n.operands)
            operands[i] = arm
            return expr_node(n.operator, *operands)

        return expr_node(
            "if",
            operand.operands[0],
            with_arm(operand.operands[1]),
            with_arm(operand.operands[2]),
        )
    return None


def _empty_list_comparison(n: Node) -> Node | None:
    if n.operator not in ("eq", "ne"):
        return None
    set_op, other = n.operands
    if not set_op.is_expr:
        set_op, other = other, set_op
    if not other.is_value or not set_op.is_expr or len(set_op.operands) != 2:
        return None
    if not isinstance(other.value, list) or other.value:
        return None
    if set_op.operator == "except":
        empty = expr_node("isSubset", *set_op.operands)
    elif set_op.operator == "intersect":
        empty = expr_node("not", expr_node("hasIntersection", *set_op.operands))
    else:
        return None
    return expr_node("not", empty) if n.operator == "ne" else empty


def _yields_timestamp(n: Node) -> bool:
    if not n.is_expr:
        return False
    if n.operator == "timestamp":
        return True
    if n.operator in ("add", "sub"):
        return len(n.operands) == 2 and _yields_timestamp(n.operands[0])
    return False


def _renders_numeric_constant(n: Node) -> bool:
    if n.is_value:
        return is_float(n.value)
    if not n.is_expr or n.operator != "if" or len(n.operands) != 3:
        return False
    return _renders_numeric_constant(n.operands[1]) or _renders_numeric_constant(
        n.operands[2]
    )


def _non_numeric(v: Any) -> bool:
    if isinstance(v, (str, bool)):
        return True
    return isinstance(v, Col) and v.type in (
        ValueType.STRING,
        ValueType.BOOL,
        ValueType.TIMESTAMP,
    )


def _non_string(v: Any) -> bool:
    if isinstance(v, (float, bool, Instant)):
        return True
    return isinstance(v, Col) and v.type in (
        ValueType.NUMBER,
        ValueType.BOOL,
        ValueType.TIMESTAMP,
    )


def _is_string_operand(v: Any) -> bool:
    return isinstance(v, str) or (isinstance(v, Col) and v.type == ValueType.STRING)


def _is_attribute(v: Any) -> bool:
    if isinstance(v, (Col, PositionalElement)):
        return True
    return isinstance(v, Subquery) and v.kind == SubqueryKind.SCALAR


def _add_value(lv: Any, rv: Any) -> Any:
    if lv is None or rv is None:
        raise Unsupported("`+` with a null operand is a CEL no-overload error")
    if isinstance(lv, list) and isinstance(rv, list):
        return lv + rv
    if _is_string_operand(lv) or _is_string_operand(rv):
        if _non_string(lv) or _non_string(rv):
            raise Unsupported(
                "`+` between a string and a non-string operand is a CEL no-overload error"
            )
        return Concat(as_expr(lv), as_expr(rv))
    if _non_numeric(lv) or _non_numeric(rv):
        raise Unsupported(
            "`+` over a boolean or bare timestamp operand is a CEL no-overload error"
        )
    return _numeric_arith(ArithOp.ADD, lv, rv)


def _numeric_arith(op: ArithOp, lv: Any, rv: Any) -> Any:
    out, ok = arith_over_conditional(op, lv, rv)
    if ok:
        return out
    if is_float(lv) and is_float(rv):
        folded = fold_arithmetic(op, lv, rv)
        if folded is not None:
            return folded
    if op == ArithOp.MOD and (_is_attribute(lv) or _is_attribute(rv)):
        raise Unsupported(
            "modulus over an attribute is a CEL no-overload error: Cerbos sends every attribute number "
            "as a double and CEL's % is integer-only, so the PDP denies every row"
        )
    if op == ArithOp.MOD:
        lv, rv = _integral(lv), _integral(rv)
        if rv == 0:
            raise Unsupported("modulus by zero is a CEL error")
        return Arith(op, as_expr(lv), as_expr(rv))
    # CEL computes in double; DuckDB would compute in the column's type, so FLOAT overflows to
    # infinity and INTEGER raises.
    return Arith(op, as_float_expr(lv), as_float_expr(rv))


def _integral(v: Any) -> Any:
    if not is_float(v):
        return v
    if v != math.trunc(v) or abs(v) > 2**53:
        raise Unsupported(f"modulus requires an integer operand, got {v}")
    return int(v)


def _cast_value(v: Any) -> Any:
    e = as_expr(v)
    if not is_float(v) and scalar_kind(v) == "number":
        return NumberText(e)
    if _bool_operand(e):
        e = when(IsNull(e), Lit(None), (e, Lit("true")), else_=Lit("false"))
    return Cast(e, "VARCHAR")


def _bool_operand(e: Expr) -> bool:
    if isinstance(e, Col):
        return e.type == ValueType.BOOL
    if isinstance(e, Subquery):
        if e.kind == SubqueryKind.EXISTS:
            return True
        return (
            e.kind == SubqueryKind.SCALAR
            and isinstance(e.select, Col)
            and e.select.type == ValueType.BOOL
        )
    if isinstance(e, Case):
        arms = [then for _, then in e.whens] + (
            [e.else_] if e.else_ is not None else []
        )
        return all(_bool_arm(a) for a in arms)
    return isinstance(
        e,
        (
            Cmp,
            Logic,
            Not,
            IsNull,
            TruthTest,
            StrMatch,
            Regex,
            NotDistinct,
            InList,
            BoolConst,
        ),
    )


def _bool_arm(e: Expr) -> bool:
    if isinstance(e, Lit):
        return isinstance(e.v, bool)
    return _bool_operand(e)


# -- null operands -----------------------------------------------------------------------------


def _carries_null(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, list):
        return any(_carries_null(x) for x in v)
    if isinstance(v, dict):
        return any(_carries_null(x) for x in v.values())
    return False


_ERR_NULL_UNDER_OMITTED = (
    "cannot translate a null operand under NullRepresentation.OMITTED: a NULL column sends no "
    "attribute, so Cerbos evaluates the comparison as a missing-attribute error (deny) while a "
    "NULL-selecting filter would return those rows. Send NULL columns as explicit nulls and declare "
    "NullRepresentation.EXPLICIT, or keep this shape out of the policy"
)


def _compared_attribute_and_literal(n: Node) -> tuple[str, Node] | None:
    if not n.is_expr or len(n.operands) != 2:
        return None
    if n.operator not in _COMPARISON and n.operator != "in":
        return None
    left, right = n.operands
    variable, literal = (right, left) if right.is_variable else (left, right)
    if not variable.is_variable or not literal.is_value:
        return None
    return variable.variable, literal


def _assert_no_null_operands(n: Node, m: _Mapper, fallback: NullRepresentation) -> None:
    if n.is_value:
        if _carries_null(n.value) and fallback == NullRepresentation.OMITTED:
            raise Unsupported(_ERR_NULL_UNDER_OMITTED)
        return
    compared = _compared_attribute_and_literal(n)
    if compared is not None:
        variable, literal = compared
        entry = m.resolve(variable)
        if entry is not None and entry.null_convention != NullConvention.UNSET:
            renders_unknown = literal.value is None and n.operator in ("eq", "ne")
            if (
                _carries_null(literal.value)
                and entry.null_convention == NullConvention.OMITTED
                and not renders_unknown
            ):
                raise Unsupported(_ERR_NULL_UNDER_OMITTED)
            return
    for child in n.operands:
        _assert_no_null_operands(child, m, fallback)
