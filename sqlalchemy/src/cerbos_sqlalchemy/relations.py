# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Collection macros over a related table, as ``operator_override_fns``.

See "Collections in a related table" and "Mapping hazards" in the README.
"""

from collections.abc import Callable, Sequence
from typing import Any

from sqlalchemy import (
    and_,
    case,
    exists,
    false,
    func,
    literal,
    not_,
    null,
    or_,
    select,
    true,
)

from cerbos_sqlalchemy._operators import OPERATOR_FNS, require_lowerable
from cerbos_sqlalchemy.errors import UnsupportedPlanError

__all__ = ["Relation", "relation_overrides", "require_hops"]


def require_hops(
    expression: Any,
    hop_correlation: Sequence[Any],
    correlate: Sequence[Any] = (),
) -> Any:
    """Make ``expression`` UNKNOWN unless every intermediate to-one hop exists.

    CEL denies when an intermediate parent in ``a.b.c`` is absent, but a subquery
    cannot tell an absent parent from a childless one, so ``all``, ``!exists`` and
    a zero count would admit rows the PDP denies. The ``CASE`` has no ``ELSE``: a
    missing hop yields NULL, which stays excluded under negation too. Route every
    operator that reads a chain through this, not just the macros, because a bare
    ``EXISTS`` is two-valued. See #309, #315 and #316.

    Args:
        expression: The answer the chain produces, e.g. an ``EXISTS``, a scalar
            count subquery, or a ``CASE`` over either.
        hop_correlation: Join predicates for the intermediate hops only, not the
            element table. If empty, ``expression`` is returned unchanged so a
            direct collection keeps its empty-collection semantics.
        correlate: Entities the guard subquery must correlate against. SQLAlchemy
            auto-correlates only with the immediately enclosing SELECT, so an
            outer entity referenced from a nested subquery would otherwise become
            a cartesian product.

    Returns:
        ``expression`` wrapped in a ``CASE`` that is NULL when a hop is missing.
    """
    if not hop_correlation:
        return expression

    guard = select(literal(1))
    for predicate in hop_correlation:
        guard = guard.where(predicate)
    return case((exists(guard.correlate(*correlate)), expression))


class Relation:
    """A collection attribute held in a related table, as an ``attr_map`` value.

    Map the collection attribute to a ``Relation`` and each lambda variable that
    ranges over it to the element table's columns, then pass
    ``relation_overrides()`` as ``operator_override_fns``.

    Args:
        correlation: Predicates joining an element row to the row that owns the
            collection, through every intermediate hop.
        correlate: Entities the subqueries correlate against: the queried table,
            and every enclosing lambda's table for a nested collection.
            SQLAlchemy auto-correlates only with the immediately enclosing SELECT.
        member: The element column compared by ``in`` and ``hasIntersection``
            when the collection is a list of scalars.
        hop_correlation: The predicates of ``correlation`` that cross an
            intermediate to-one hop (``a`` in ``a.b``), passed to ``require_hops``.
    """

    def __init__(
        self,
        correlation: Sequence[Any],
        correlate: Sequence[Any] = (),
        *,
        member: Any = None,
        hop_correlation: Sequence[Any] = (),
    ) -> None:
        self.correlation = tuple(correlation)
        self.correlate = tuple(correlate)
        self.member = member
        self.hop_correlation = tuple(hop_correlation)

    def __repr__(self) -> str:
        return f"Relation({', '.join(str(p) for p in self.correlation)})"

    def _exists(self, *conditions: Any) -> Any:
        query = select(literal(1))
        for predicate in (*self.correlation, *conditions):
            query = query.where(predicate)
        return exists(query.correlate(*self.correlate))

    def _count(self, *conditions: Any) -> Any:
        query = select(func.count())
        for predicate in (*self.correlation, *conditions):
            query = query.where(predicate)
        return query.correlate(*self.correlate).scalar_subquery()

    def _guarded(self, expression: Any) -> Any:
        return require_hops(expression, self.hop_correlation, self.correlate)

    def _errors(self, body: Any) -> Any:
        return self._exists(body.is_(None))


class _Deferred:
    def __init__(self, kind: str, relation: Relation, body: Any) -> None:
        self.kind = kind
        self.relation = relation
        self.body = body

    def __repr__(self) -> str:
        return f"{self.kind}({self.relation!r})"


def _relation(operator: str, operand: Any) -> Relation:
    if not isinstance(operand, Relation):
        raise UnsupportedPlanError(
            f"{operator} is translated by relation_overrides() only over an attribute "
            f"mapped to a Relation, not {operand!r}"
        )
    return operand


def _default(operator: str, left: Any, right: Any) -> Any:
    require_lowerable(operator, left)
    require_lowerable(operator, right)
    return OPERATOR_FNS[operator](left, right)


def _exists_fn(collection: Any, body: Any) -> Any:
    relation = _relation("exists", collection)
    return relation._guarded(
        case(
            (relation._exists(body), true()),
            (relation._errors(body), null()),
            else_=false(),
        )
    )


def _all_fn(collection: Any, body: Any) -> Any:
    relation = _relation("all", collection)
    return relation._guarded(
        case(
            (relation._exists(not_(body)), false()),
            (relation._errors(body), null()),
            else_=true(),
        )
    )


def _exists_one_fn(collection: Any, body: Any) -> Any:
    relation = _relation("exists_one", collection)
    return relation._guarded(
        case(
            (relation._errors(body), null()),
            else_=(relation._count(body) == 1),
        )
    )


def _filter_fn(collection: Any, body: Any) -> _Deferred:
    return _Deferred("filter", _relation("filter", collection), body)


def _map_fn(collection: Any, projected: Any) -> _Deferred:
    return _Deferred("map", _relation("map", collection), projected)


def _size_fn(target: Any, _: Any) -> Any:
    if isinstance(target, Relation):
        return target._guarded(target._count())
    if isinstance(target, _Deferred) and target.kind == "filter":
        relation, body = target.relation, target.body
        return relation._guarded(
            case((relation._errors(body), null()), else_=relation._count(body))
        )
    if isinstance(target, _Deferred):
        raise UnsupportedPlanError(f"size over {target!r} is not translated")
    require_lowerable("size", target)
    return OPERATOR_FNS["size"](target, None)


def _members(relation: Relation, operator: str) -> Any:
    if relation.member is None:
        raise UnsupportedPlanError(
            f"{operator} over {relation!r} needs the Relation's member column"
        )
    return relation.member


def _has_intersection_fn(left: Any, right: Any) -> Any:
    mapped, values = (right, left) if isinstance(left, list) else (left, right)
    if not isinstance(values, list):
        raise UnsupportedPlanError(
            "hasIntersection is translated only against a literal list"
        )
    if isinstance(mapped, Relation):
        if not values:
            return mapped._guarded(false())
        member = _members(mapped, "hasIntersection")
        return mapped._guarded(mapped._exists(OPERATOR_FNS["in"](member, values)))
    if isinstance(mapped, _Deferred) and mapped.kind == "map":
        relation, projected = mapped.relation, mapped.body
        if not values:
            return relation._guarded(false())
        return relation._guarded(
            case(
                (relation._errors(projected), null()),
                (relation._exists(OPERATOR_FNS["in"](projected, values)), true()),
                else_=false(),
            )
        )
    raise UnsupportedPlanError(
        "hasIntersection is translated only over a Relation or its map() projection"
    )


def _relation_membership(relation: Relation, value: Any) -> Any:
    if isinstance(value, (list, dict)):
        raise UnsupportedPlanError(
            "Membership with a structured element has no scalar SQL lowering"
        )
    member = _members(relation, "in")
    if value is None:
        predicate = member.is_(None)
    elif hasattr(value, "is_"):
        predicate = or_(member == value, and_(member.is_(None), value.is_(None)))
    else:
        predicate = member == value
    return relation._guarded(relation._exists(predicate))


def _deferred_membership(needle: Any, deferred: _Deferred) -> Any:
    if needle is None or not isinstance(needle, (str, int, float, bool)):
        raise UnsupportedPlanError(
            f"in over {deferred.kind}() is lowered for a scalar literal needle only"
        )
    if deferred.kind == "map":
        return _has_intersection_fn(deferred, [needle])
    relation, body = deferred.relation, deferred.body
    member = _members(relation, "in")
    return relation._guarded(
        case(
            (relation._errors(body), null()),
            (relation._exists(body, OPERATOR_FNS["in"](member, [needle])), true()),
            else_=false(),
        )
    )


def _in_fn(needle: Any, haystack: Any) -> Any:
    if isinstance(haystack, _Deferred):
        return _deferred_membership(needle, haystack)
    if isinstance(needle, Relation):
        return _relation_membership(needle, haystack)
    if isinstance(haystack, Relation):
        return _relation_membership(haystack, needle)
    return _default("in", needle, haystack)


def relation_overrides() -> dict[str, Callable[[Any, Any], Any]]:
    """The ``operator_override_fns`` that translate macros over a ``Relation``.

    ``exists``, ``all``, ``exists_one``, ``size``, ``in``, ``hasIntersection``,
    ``size(filter(...))``, ``x in filter(...)``, ``x in map(...)`` and
    ``hasIntersection(map(...), [...])`` are three-valued: a body that is SQL NULL
    is a CEL error, which ``exists`` absorbs only beside a true element and ``all``
    only beside a false one. Every answer goes through ``require_hops``. ``in``,
    ``size`` and ``hasIntersection`` over anything else fall back to the default
    handler.

    Returns:
        A new dict, so it can be merged with other overrides.
    """
    return {
        "lambda": lambda body, _variable: body,
        "exists": _exists_fn,
        "all": _all_fn,
        "exists_one": _exists_one_fn,
        "filter": _filter_fn,
        "map": _map_fn,
        "size": _size_fn,
        "hasIntersection": _has_intersection_fn,
        "in": _in_fn,
    }
