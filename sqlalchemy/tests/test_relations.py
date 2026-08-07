"""Unit tests for the mapping helpers a caller wires into ``operator_override_fns``.

The adversarial harness is the semantic proof of ``require_hops`` — every chained
corpus action is an oracle comparison against a real PDP that routes through it.
These pin the two structural properties that proof depends on and that a refactor
could quietly break: the guard is a ``CASE`` with no ``ELSE``, and a direct relation
gets no guard at all.
"""

from cerbos_sqlalchemy import require_hops
from sqlalchemy import Column, Integer, MetaData, String, Table, literal, select
from sqlalchemy.sql.elements import Case

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
    # A direct collection keeps its empty-collection semantics: `!tags.exists(...)`
    # over zero tags is TRUE, and a guard here would wrongly turn it UNKNOWN.
    answer = literal(True)
    assert require_hops(answer, []) is answer
    assert require_hops(answer, ()) is answer


def test_guard_has_no_else_branch():
    # The whole point. A missing hop must yield NULL, because NOT NULL is still
    # NULL and that is what keeps the row excluded under BOTH polarities. An ELSE
    # of FALSE would be TRUE once negated — the #309 over-grant, restored.
    guarded = require_hops(literal(True), [category.c.resource_id == resource.c.id])
    assert isinstance(guarded, Case)
    assert guarded.else_ is None

    sql = _sql(guarded)
    assert "CASE WHEN (EXISTS" in sql
    assert "ELSE" not in sql
    assert "hazard_category.resource_id = hazard_resource.id" in sql


def test_every_hop_predicate_is_required():
    # Several predicates conjoin inside one guard subquery rather than producing
    # several guards, so a chain is required whole.
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
    # SQLAlchemy's auto-correlation only reaches the immediately enclosing SELECT.
    # Without the explicit correlate, the outer table joins into the guard's FROM
    # as a cartesian product — silently comparing against EVERY resource row.
    # Correlation is only observable once the guard sits inside an enclosing SELECT,
    # which is where a real override puts it.
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
