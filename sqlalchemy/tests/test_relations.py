# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Structural tests for ``require_hops``; the adversarial suite proves its semantics.

These pin what a refactor could quietly break: the guard is a ``CASE`` with no
``ELSE``, and a direct relation gets no guard.
"""

from sqlalchemy import Column, Integer, MetaData, String, Table, literal, select
from sqlalchemy.sql.elements import Case

from cerbos_sqlalchemy import require_hops

_metadata = MetaData()
resource = Table(
    "hazard_resource",
    _metadata,
    Column("id", Integer, primary_key=True),
)
category = Table(
    "hazard_category",
    _metadata,
    Column("id", Integer, primary_key=True),
    Column("resource_id", Integer),
    Column("kind", String),
)


def _sql(expression) -> str:
    return str(expression.compile(compile_kwargs={"literal_binds": True}))


def test_direct_relation_is_returned_unchanged():
    # `!tags.exists(...)` over zero tags is TRUE. A guard would make it UNKNOWN.
    answer = literal(True)
    assert require_hops(answer, []) is answer
    assert require_hops(answer, ()) is answer


def test_guard_has_no_else_branch():
    # A missing hop must yield NULL so the row stays excluded under negation.
    # An ELSE FALSE would become TRUE under NOT: the #309 over-grant.
    guarded = require_hops(literal(True), [category.c.resource_id == resource.c.id])
    assert isinstance(guarded, Case)
    assert guarded.else_ is None

    sql = _sql(guarded)
    assert "CASE WHEN (EXISTS" in sql
    assert "ELSE" not in sql
    assert "hazard_category.resource_id = hazard_resource.id" in sql


def test_every_hop_predicate_is_required():
    # All predicates go in one EXISTS, so the chain is required as a whole.
    guarded = require_hops(
        literal(True),
        [
            category.c.resource_id == resource.c.id,
            category.c.kind == "main",
        ],
    )
    sql = _sql(guarded)
    assert sql.count("EXISTS") == 1
    assert "hazard_category.resource_id = hazard_resource.id" in sql
    assert "hazard_category.kind = 'main'" in sql


def test_correlate_targets_keep_the_outer_entity_out_of_the_inner_from():
    # Auto-correlation only reaches the immediately enclosing SELECT. Without an
    # explicit correlate, the outer table joins the guard's FROM as a cross join.
    def outer(*correlate):
        guarded = require_hops(
            literal(True), [category.c.resource_id == resource.c.id], correlate
        )
        return _sql(select(resource.c.id).where(guarded))

    uncorrelated = outer()
    correlated = outer(resource)

    assert "FROM hazard_category, hazard_resource" in uncorrelated
    assert "FROM hazard_category, hazard_resource" not in correlated
    assert "FROM hazard_category \n" in correlated or correlated.endswith(
        "FROM hazard_category"
    )
