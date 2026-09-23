# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Helpers for the join chains a caller writes in ``operator_override_fns``.

See "Mapping hazards" in the README.
"""

from collections.abc import Sequence
from typing import Any

from sqlalchemy import case, exists, literal, select

__all__ = ["require_hops"]


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
