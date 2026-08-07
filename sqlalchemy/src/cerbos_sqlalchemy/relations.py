"""Helpers for the parts of a mapping this adapter cannot own.

``get_query`` has no relation model. A collection-valued attribute reaches its rows
entirely through ``operator_override_fns``: the caller writes the correlated
subquery, so the caller owns the invariant that the subquery sees exactly the rows
the application serialised into the resource attributes. That is the right split —
only the caller knows whether its association is a ``relationship()``, a Core join
or a soft-deleted table — but one part of it is not a judgement call, it is a
mechanical requirement every join chain has, and every caller has to get it right
the same way. This module holds that part.

See "Mapping hazards" in the README, and ``conformance/README.md``.
"""

from __future__ import annotations

from typing import Any, Sequence

from sqlalchemy import case, exists, literal, select

__all__ = ["require_hops"]


def require_hops(
    expression: Any,
    hop_correlation: Sequence[Any],
    correlate: Sequence[Any] = (),
) -> Any:
    """Make ``expression`` UNKNOWN unless every intermediate to-one hop exists.

    CEL cannot dot through a list, so every intermediate segment of ``a.b.c`` is a
    to-ONE parent: when it is absent the application sends no attribute at all and
    CEL raises a missing-path error, which denies. A subquery rooted at the resource
    row cannot see that -- an absent parent and a childless parent both return
    nothing -- so ``all`` reads TRUE, ``!exists`` reads TRUE and the count reads 0,
    each admitting rows the PDP denies (cerbos/query-plan-adapters#309).

    Wrapping the answer in this guard restores the distinction. The ``CASE`` has no
    ``ELSE`` on purpose: a missing hop yields NULL, and ``NOT NULL`` is still NULL,
    so the row stays excluded under BOTH polarities.

    **Every** operator whose answer comes off a chain has to go through here, not
    just the collection macros. A bare ``EXISTS`` is two-valued, so it is FALSE for
    an absent to-one parent and its negation is TRUE -- which is how plain
    membership and ``hasIntersection`` kept readmitting every parentless row after
    the macros alone were fixed (cerbos/query-plan-adapters#315), and how the
    negated count spelling ``!(size(chain) > 0)`` did the same (#316). Guarding the
    chain rather than each operator is what closes all of them at once.

    :param expression: the answer the chain produces -- an ``EXISTS``, a scalar
        count subquery, a ``CASE`` over either.
    :param hop_correlation: the join predicates for the INTERMEDIATE hops alone,
        excluding the element table. Empty for a direct relation, in which case
        ``expression`` is returned unchanged so a direct collection keeps its
        empty-collection semantics (``!tags.exists(...)`` over zero tags is still
        TRUE).
    :param correlate: entities the guard subquery must correlate against
        explicitly. SQLAlchemy's auto-correlation only reaches the immediately
        enclosing SELECT, so a reference to an outer entity from inside a nested
        lambda subquery would otherwise pull that entity into the inner FROM as a
        cartesian product -- silently comparing against EVERY row of it.

    Optional by design: a caller wiring a join chain today gets the same query it
    got before this helper existed. Making it mandatory would mean throwing for
    every such caller, which is a consumer-visible break to guard a hazard many of
    them do not have.
    """
    if not hop_correlation:
        return expression

    guard = select(literal(1))
    for predicate in hop_correlation:
        guard = guard.where(predicate)
    return case((exists(guard.correlate(*correlate)), expression))
