"""Adversarial differential conformance harness (cerbos/query-plan-adapters#263).

Every action in the shared repo-level ``conformance/`` corpus is planned against a
REAL Cerbos PDP (a dedicated testcontainer pinned to ``conformance/CERBOS_VERSION``,
loaded with ``conformance/policies/adversarial.yaml``), translated through this
adapter's public ``get_query`` API, and executed against seeded SQLite rows — then
the filtered id set is compared against an oracle computed by calling the check API
for each seed row with attributes mirroring that row exactly.

No hand-computed expectations: if the adapter's filter semantics diverge from
Cerbos's own evaluation for any row, the mismatch surfaces mechanically. See
``conformance/README.md`` for the oracle recipe (NULL-as-missing-attribute, the
degeneracy guard). This file owns only the SQLAlchemy-specific translation
configuration: the schema, the attribute map, and the operator overrides that
express relation traversals as correlated subqueries with CEL-faithful
three-valued logic (an element whose column is NULL is a CEL missing-attribute
error — UNKNOWN in SQL — and must stay excluded under BOTH polarities).
"""

import json
import os
from typing import Any, Dict, List, Set

import pytest
from cerbos.sdk.client import CerbosClient
from cerbos.sdk.container import CerbosContainer
from cerbos.sdk.model import Principal, Resource, ResourceDesc

from cerbos_sqlalchemy import get_query
from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Integer,
    String,
    case,
    create_engine,
    event,
    exists,
    false,
    func,
    insert,
    literal,
    not_,
    null,
    select,
    true,
)
from sqlalchemy.orm import declarative_base

CONFORMANCE_DIR = os.path.realpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "conformance")
)

with open(os.path.join(CONFORMANCE_DIR, "seeds.json"), encoding="utf-8") as f:
    SEEDS_FILE = json.load(f)
with open(os.path.join(CONFORMANCE_DIR, "actions.json"), encoding="utf-8") as f:
    ACTIONS_FILE = json.load(f)
with open(os.path.join(CONFORMANCE_DIR, "CERBOS_VERSION"), encoding="utf-8") as f:
    CERBOS_VERSION = f.read().strip()

SEEDS: List[Dict[str, Any]] = SEEDS_FILE["seeds"]
RESOURCE_KIND: str = SEEDS_FILE["resourceKind"]

# Conformance actions this adapter genuinely cannot express, asserted as loud
# failures instead of oracle matches. Currently EMPTY: SQLAlchemy expresses the
# full conformance list (correlated subqueries, COUNT thresholds, CASE
# ternaries, field-to-field ESCAPE'd LIKE) that Prisma's filter language could
# not.
#
# TODO: if this ever gains entries, move them to conformance/actions.json
# `adapterUnsupported.sqlalchemy` (the shared corpus is not editable from this
# change).
SQLALCHEMY_UNSUPPORTED: Dict[str, str] = {}

# Globally-`expectedUnsupported` planner shapes that this adapter DOES
# translate: `matches` maps to SQLAlchemy's regexp_match, and on this harness's
# SQLite backend SQLAlchemy registers a Python-`re` REGEXP implementation, so
# the probe executes with CEL-compatible semantics. Verified differentially
# against the oracle instead of asserted as a throw. (Regex dialects differ
# across backends — CEL is RE2 — so on other databases semantics are
# best-effort; recorded for corpus triage rather than regressing a documented
# adapter feature to a throw.)
SQLALCHEMY_SUPPORTED_DESPITE_EXPECTED_UNSUPPORTED = {"p-matches"}

ORACLE_ACTIONS = [
    a for a in ACTIONS_FILE["conformance"] if a not in SQLALCHEMY_UNSUPPORTED
] + sorted(SQLALCHEMY_SUPPORTED_DESPITE_EXPECTED_UNSUPPORTED)
# Globally-unsupported planner shapes plus this adapter's own unsupported list:
# translation (or execution) must fail loudly, never produce a silently-wrong
# filter.
THROWING_ACTIONS = sorted(
    (
        {u["action"] for u in ACTIONS_FILE["expectedUnsupported"]}
        | set(SQLALCHEMY_UNSUPPORTED)
    )
    - SQLALCHEMY_SUPPORTED_DESPITE_EXPECTED_UNSUPPORTED
)


def _iso_for(seed: Dict[str, Any]) -> str:
    """Deterministic ISO instant per seed for the timestamp probe (see
    conformance/README.md): split around the probe's 2025-01-01 threshold."""
    return "2024-06-01T00:00:00Z" if seed["aNumber"] >= 2 else "2026-06-01T00:00:00Z"


# ---------------------------------------------------------------------------
# Schema: dedicated tables so hostile seeds (NULL element columns, duplicate
# names, LIKE metacharacters) are all representable.
# ---------------------------------------------------------------------------

AdvBase = declarative_base()


class AdvResource(AdvBase):
    __tablename__ = "adversarial_resource"

    id = Column(String, primary_key=True)
    a_bool = Column(Boolean, nullable=False)
    a_string = Column(String, nullable=False)
    a_number = Column(Integer, nullable=False)
    a_optional_string = Column(String, nullable=True)
    created_by = Column(String, nullable=False)


class AdvTag(AdvBase):
    __tablename__ = "adversarial_tag"

    pk = Column(Integer, primary_key=True, autoincrement=True)
    tag_id = Column(String, nullable=False)
    name = Column(String, nullable=True)
    resource_id = Column(String, ForeignKey("adversarial_resource.id"), nullable=False)


class AdvCategory(AdvBase):
    __tablename__ = "adversarial_category"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    resource_id = Column(String, ForeignKey("adversarial_resource.id"), nullable=False)


class AdvSubCategory(AdvBase):
    __tablename__ = "adversarial_sub_category"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    category_id = Column(String, ForeignKey("adversarial_category.id"), nullable=False)


# ---------------------------------------------------------------------------
# Relation markers + operator overrides: the adapter's attribute map points
# relation-valued attributes at marker objects; the overrides translate the
# collection macros over them into correlated subqueries. Three-valued logic:
# CEL's exists/all absorb an erroring element only through a true/false
# witness; exists_one/map/filter never do. An erroring element is a row whose
# lambda body evaluates to SQL UNKNOWN (NULL), detected with `body IS NULL`.
# ---------------------------------------------------------------------------


class _Relation:
    """Marker standing in for a relation path in the attribute map."""

    def __init__(
        self,
        description: str,
        correlation: List[Any],
        correlate_targets: List[Any],
        member_field=None,
    ):
        self.description = description
        self.correlation = correlation
        # Entities the subquery must correlate against explicitly: SQLAlchemy's
        # auto-correlation only reaches the immediate enclosing SELECT, so an
        # outer-resource reference inside a depth-2 lambda subquery would
        # otherwise pull the resource table into the inner FROM as a cartesian
        # product (silently comparing against EVERY resource row).
        self.correlate_targets = correlate_targets
        # For plain `in` membership over a chained string list (w1-in-chain).
        self.member_field = member_field

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return f"_Relation({self.description})"


TAGS = _Relation(
    "tags",
    [AdvTag.resource_id == AdvResource.id],
    # The root resource may be any number of lambda scopes up (w2-outer-relation
    # plans a tags exists INSIDE the categories lambda).
    correlate_targets=[AdvResource],
)
CATEGORIES = _Relation(
    "categories",
    [AdvCategory.resource_id == AdvResource.id],
    correlate_targets=[AdvResource],
)
# c.subCategories: correlates to the *category* the enclosing lambda is scoped
# to, never to the root resource — but its lambda body may still reference
# outer resource columns (outer-attr-depth2), so both entities correlate.
SUB_OF_CATEGORY = _Relation(
    "c.subCategories",
    [AdvSubCategory.category_id == AdvCategory.id],
    correlate_targets=[AdvCategory, AdvResource],
)
# mainCategory.subCategories: the same two-hop chain flattened from the root —
# the subquery must join THROUGH the intermediate category hop (which stays in
# the subquery FROM; only the root resource correlates).
MAIN_SUB = _Relation(
    "mainCategory.subCategories",
    [
        AdvSubCategory.category_id == AdvCategory.id,
        AdvCategory.resource_id == AdvResource.id,
    ],
    correlate_targets=[AdvResource],
)
MAIN_SUBNAMES = _Relation(
    "mainCategory.subNames",
    [
        AdvSubCategory.category_id == AdvCategory.id,
        AdvCategory.resource_id == AdvResource.id,
    ],
    correlate_targets=[AdvResource],
    member_field=AdvSubCategory.name,
)


def _exists_where(rel: _Relation, *conds: Any):
    q = select(literal(1))
    for pred in rel.correlation:
        q = q.where(pred)
    for cond in conds:
        q = q.where(cond)
    return exists(q.correlate(*rel.correlate_targets))


def _count_subquery(rel: _Relation, *conds: Any):
    q = select(func.count())
    for pred in rel.correlation:
        q = q.where(pred)
    for cond in conds:
        q = q.where(cond)
    return q.correlate(*rel.correlate_targets).scalar_subquery()


def _require_relation(op: str, coll: Any) -> _Relation:
    if not isinstance(coll, _Relation):
        raise ValueError(f"{op} over unsupported collection operand: {coll!r}")
    return coll


def _exists_fn(coll: Any, body: Any):
    # CEL exists: true on any true witness (absorbing errors), error if any
    # element errors without one, false otherwise (incl. empty).
    rel = _require_relation("exists", coll)
    return case(
        (_exists_where(rel, body), true()),
        (_exists_where(rel, body.is_(None)), null()),
        else_=false(),
    )


def _all_fn(coll: Any, body: Any):
    # CEL all: false on any false witness (absorbing errors), error if any
    # element errors without one, true otherwise (incl. empty).
    rel = _require_relation("all", coll)
    return case(
        (_exists_where(rel, not_(body)), false()),
        (_exists_where(rel, body.is_(None)), null()),
        else_=true(),
    )


def _exists_one_fn(coll: Any, body: Any):
    # CEL exists_one never absorbs an erroring element, even next to a true
    # witness; otherwise it's an exact count-of-matches == 1.
    rel = _require_relation("exists_one", coll)
    return case(
        (_exists_where(rel, body.is_(None)), null()),
        else_=(_count_subquery(rel, body) == 1),
    )


def _filter_fn(coll: Any, body: Any):
    # Deferred: consumed by the `size` override (size(filter(...)) shape).
    return ("filter", _require_relation("filter", coll), body)


def _map_fn(coll: Any, projected: Any):
    # Deferred: consumed by the `hasIntersection` override.
    return ("map", _require_relation("map", coll), projected)


def _size_fn(target: Any, _: Any):
    if isinstance(target, _Relation):
        # size() counts elements without evaluating them, so NULL element
        # columns still count — no error guard needed.
        return _count_subquery(target)
    if isinstance(target, tuple) and target[0] == "filter":
        # CEL filter never absorbs an erroring element: any UNKNOWN body row
        # poisons the whole count.
        _, rel, body = target
        return case(
            (_exists_where(rel, body.is_(None)), null()),
            else_=_count_subquery(rel, body),
        )
    return func.length(target)


def _has_intersection_fn(mapped: Any, values: Any):
    # hasIntersection(map(coll, x), list): map errors on any erroring element
    # (no absorption), so the error guard comes FIRST.
    if not (isinstance(mapped, tuple) and mapped[0] == "map"):
        raise ValueError(f"hasIntersection over unsupported operand: {mapped!r}")
    _, rel, projected = mapped
    return case(
        (_exists_where(rel, projected.is_(None)), null()),
        (_exists_where(rel, projected.in_(values)), true()),
        else_=false(),
    )


def _in_fn(column: Any, value: Any):
    if isinstance(column, _Relation):
        # `value in R.attr.<chain>`: membership against the relation's member
        # column; rows with an empty chain are simply excluded (CEL
        # missing-attribute error → deny).
        if column.member_field is None:
            raise ValueError(f"in over relation without member field: {column!r}")
        return _exists_where(column, column.member_field == value)
    return column.in_(value) if isinstance(value, list) else column.in_([value])


OPERATOR_OVERRIDES = {
    # The lambda's first (resolved) operand is its body predicate; the iterator
    # variable resolves through the attribute map and is discarded.
    "lambda": lambda body, _var: body,
    "exists": _exists_fn,
    "all": _all_fn,
    "exists_one": _exists_one_fn,
    "filter": _filter_fn,
    "map": _map_fn,
    "size": _size_fn,
    "hasIntersection": _has_intersection_fn,
    "in": _in_fn,
}

ATTR_MAP = {
    "request.resource.attr.aBool": AdvResource.a_bool,
    "request.resource.attr.aString": AdvResource.a_string,
    "request.resource.attr.aNumber": AdvResource.a_number,
    "request.resource.attr.aOptionalString": AdvResource.a_optional_string,
    "request.resource.attr.createdBy": AdvResource.created_by,
    # obj.inner is not a real nested column — mirrors aString, the same trick
    # the spring-data and prisma reference harnesses use for the p-struct probe.
    "request.resource.attr.obj.inner": AdvResource.a_string,
    "request.resource.attr.tags": TAGS,
    "t": TAGS,
    "t.id": AdvTag.tag_id,
    "t.name": AdvTag.name,
    "request.resource.attr.categories": CATEGORIES,
    "c": CATEGORIES,
    "c.subCategories": SUB_OF_CATEGORY,
    "s": SUB_OF_CATEGORY,
    "s.name": AdvSubCategory.name,
    "request.resource.attr.mainCategory.subCategories": MAIN_SUB,
    "request.resource.attr.mainCategory.subNames": MAIN_SUBNAMES,
}


# ---------------------------------------------------------------------------
# Fixtures: a dedicated in-memory DB seeded from seeds.json, and a dedicated
# Cerbos container (random host port) pinned to conformance/CERBOS_VERSION.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def adv_engine():
    engine = create_engine("sqlite://")

    @event.listens_for(engine, "connect")
    def _configure(dbapi_conn, _):
        # CEL string matching is case-sensitive; SQLite's LIKE is
        # case-insensitive by default. No REGEXP function is registered on
        # purpose: `matches` (expectedUnsupported) must fail loudly.
        dbapi_conn.execute("PRAGMA case_sensitive_like = ON")

    AdvBase.metadata.create_all(engine)

    resource_rows = []
    tag_rows = []
    category_rows = []
    sub_category_rows = []
    for seed in SEEDS:
        resource_rows.append(
            {
                "id": seed["id"],
                "a_bool": seed["aBool"],
                "a_string": seed["aString"],
                "a_number": seed["aNumber"],
                "a_optional_string": seed["aOptionalString"],
                "created_by": _iso_for(seed),
            }
        )
        for tag in seed["tags"]:
            tag_rows.append(
                {"tag_id": tag["id"], "name": tag["name"], "resource_id": seed["id"]}
            )
        # Distinct category graphs per seed (one category per sub-name, same
        # shape the prisma reference harness seeds) so no rows share relations.
        for i, sub_name in enumerate(seed["subCategoryNames"]):
            category_id = f"{seed['id']}-cat{i}"
            category_rows.append(
                {"id": category_id, "name": "business", "resource_id": seed["id"]}
            )
            sub_category_rows.append(
                {
                    "id": f"{seed['id']}-sub{i}",
                    "name": sub_name,
                    "category_id": category_id,
                }
            )

    with engine.begin() as conn:
        conn.execute(insert(AdvResource.__table__), resource_rows)
        if tag_rows:
            conn.execute(insert(AdvTag.__table__), tag_rows)
        if category_rows:
            conn.execute(insert(AdvCategory.__table__), category_rows)
        if sub_category_rows:
            conn.execute(insert(AdvSubCategory.__table__), sub_category_rows)

    yield engine


@pytest.fixture
def adv_conn(adv_engine):
    with adv_engine.connect() as conn:
        yield conn


@pytest.fixture(scope="module")
def adv_cerbos_client():
    container = CerbosContainer(image=f"ghcr.io/cerbos/cerbos:{CERBOS_VERSION}")
    container.with_volume_mapping(
        os.path.join(CONFORMANCE_DIR, "policies"), "/policies"
    )
    container.with_env("CERBOS_NO_TELEMETRY", "1")
    container.with_command("server")
    container.start()
    container.wait_until_ready()
    try:
        with CerbosClient(container.http_host(), tls_verify=False) as client:
            yield client
    finally:
        container.stop()


def _principal() -> Principal:
    p = SEEDS_FILE["principal"]
    return Principal(id=p["id"], roles=set(p["roles"]), attr=p["attr"])


def _tag_attr(tag: Dict[str, Any]) -> Dict[str, Any]:
    """A NULL tag name in the DB is a MISSING element attribute on the check side."""
    attr: Dict[str, Any] = {"id": tag["id"]}
    if tag["name"] is not None:
        attr["name"] = tag["name"]
    return attr


def _check_resource(seed: Dict[str, Any]) -> Resource:
    """Cerbos attributes mirroring exactly what the seeded DB row holds."""
    attr: Dict[str, Any] = {
        "aBool": seed["aBool"],
        "aString": seed["aString"],
        "aNumber": seed["aNumber"],
        "createdBy": _iso_for(seed),
        "obj": {"inner": seed["aString"]},
        "tags": [_tag_attr(t) for t in seed["tags"]],
        "categories": [
            {"name": "business", "subCategories": [{"name": n}]}
            for n in seed["subCategoryNames"]
        ],
    }
    # A DB NULL is a missing attribute on the check side — conditions touching
    # it must deny (CEL error), matching SQL three-valued logic excluding the row.
    if seed["aOptionalString"] is not None:
        attr["aOptionalString"] = seed["aOptionalString"]
    # mainCategory mirrors the row's category graph as ONE nested object (the
    # seeder creates at most one category per seed); rows without a category get
    # NO attribute — a CEL missing-attr error (deny), matching the adapter's
    # empty join chain excluding the row.
    if seed["subCategoryNames"]:
        attr["mainCategory"] = {
            "name": "business",
            "subCategories": [{"name": n} for n in seed["subCategoryNames"]],
            "subNames": list(seed["subCategoryNames"]),
        }
    return Resource(id=seed["id"], kind=RESOURCE_KIND, attr=attr)


# -- oracle: ask the PDP itself, row by row --


def _oracle_allowed_ids(client: CerbosClient, action: str) -> Set[str]:
    return {
        seed["id"]
        for seed in SEEDS
        if client.is_allowed(action, _principal(), _check_resource(seed))
    }


# -- adapter execution through the public get_query path --


def _adapter_filtered_ids(client: CerbosClient, conn, action: str) -> Set[str]:
    plan = client.plan_resources(action, _principal(), ResourceDesc(RESOURCE_KIND))
    query = get_query(
        plan,
        AdvResource,
        ATTR_MAP,
        operator_override_fns=OPERATOR_OVERRIDES,
    )
    return {row.id for row in conn.execute(query).fetchall()}


# A cartesian-product warning from SQLAlchemy means a subquery failed to
# correlate (comparing against EVERY row of a table instead of the current
# one) — a silent-wrongness bug class, so escalate it to an error.
@pytest.mark.filterwarnings("error::sqlalchemy.exc.SAWarning")
class TestAdversarialConformance:
    @pytest.mark.parametrize("action", ORACLE_ACTIONS)
    def test_matches_check_oracle(self, action, adv_cerbos_client, adv_conn):
        oracle = _oracle_allowed_ids(adv_cerbos_client, action)
        filtered = _adapter_filtered_ids(adv_cerbos_client, adv_conn, action)
        assert sorted(filtered) == sorted(oracle)

    @pytest.mark.parametrize("action", THROWING_ACTIONS)
    def test_fails_loudly(self, action, adv_cerbos_client, adv_conn):
        # A loud failure — at translation or at query execution — is required;
        # a silently-wrong filter is the only unacceptable outcome.
        with pytest.raises(Exception):
            _adapter_filtered_ids(adv_cerbos_client, adv_conn, action)

    def test_oracle_is_not_degenerate(self, adv_cerbos_client):
        # Guard the guard: these actions must produce a non-empty, non-total
        # oracle set, otherwise the differential comparison could pass
        # vacuously (e.g. a PDP that denies everything).
        for action in ("vf-le", "like-percent", "all-on-empty"):
            ids = _oracle_allowed_ids(adv_cerbos_client, action)
            assert 0 < len(ids) < len(SEEDS)
