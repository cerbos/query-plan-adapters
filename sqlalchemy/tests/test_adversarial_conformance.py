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
import math
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Set, Union

import pytest
from cerbos.sdk.client import CerbosClient
from cerbos.sdk.container import CerbosContainer
from cerbos.sdk.model import PlanResourcesFilterKind, Principal, Resource, ResourceDesc
from cerbos_image import CERBOS_IMAGE, CONFORMANCE_DIR

from cerbos_sqlalchemy import get_query, require_hops
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    and_,
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
    or_,
    select,
    true,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import declarative_base

with open(os.path.join(CONFORMANCE_DIR, "seeds.json"), encoding="utf-8") as f:
    SEEDS_FILE = json.load(f)
with open(os.path.join(CONFORMANCE_DIR, "actions.json"), encoding="utf-8") as f:
    ACTIONS_FILE = json.load(f)
with open(os.path.join(CONFORMANCE_DIR, "derived-fields.json"), encoding="utf-8") as f:
    DERIVED_FILE = json.load(f)

SEEDS: List[Dict[str, Any]] = SEEDS_FILE["seeds"]
RESOURCE_KIND: str = SEEDS_FILE["resourceKind"]

# -- corpus coverage guards -------------------------------------------------
#
# The same parsed seed feeds the stored row AND the check() oracle, so a corpus
# field this harness does not consume is dropped from both sides at once and the
# differential agrees for the wrong reason — the projection trap
# conformance/README.md describes for actions.json, applied to the seeds.
# Asserting set equality catches both directions: a corpus key nothing here
# reads, and a key this harness reads that the corpus no longer carries.
SEED_KEYS = {
    "id",
    "aBool",
    "aString",
    "aNumber",
    "aOptionalString",
    "tags",
    "subCategoryNames",
    "parentSeedId",
}
# Corpus prose, never read by a harness: the one documented exclusion.
SEED_NOTE_KEY = "note"
# The one nested object array a seed carries. A key added inside an element is
# dropped from both sides of the differential just as silently as a top-level
# one, so it is guarded the same way.
TAG_KEYS = {"id", "name"}
DERIVED_KEYS = {"createdBy", "aDouble", "createdAt", "scope", "labels"}


def _assert_keys(
    label: str,
    got: Set[str],
    want: Set[str],
    optional: Set[str] = frozenset(),
) -> None:
    unconsumed = got - want - optional
    if unconsumed:
        raise AssertionError(
            f"{label} carries {sorted(unconsumed)}, which this harness does not "
            "consume: an unconsumed corpus field is dropped from the stored row "
            "and the check() oracle at once"
        )
    missing = want - got
    if missing:
        raise AssertionError(
            f"{label} is missing {sorted(missing)}, which this harness consumes"
        )


for _index, _seed in enumerate(SEEDS):
    _label = f"seeds.json seeds[{_index}]"
    _assert_keys(_label, set(_seed), SEED_KEYS, {SEED_NOTE_KEY})
    for _tag_index, _tag in enumerate(_seed["tags"]):
        _assert_keys(f"{_label}.tags[{_tag_index}]", set(_tag), TAG_KEYS)

DERIVED: Dict[str, Dict[str, Any]] = DERIVED_FILE["derived"]
_assert_keys("derived-fields.json fields", set(DERIVED_FILE["fields"]), DERIVED_KEYS)
if set(DERIVED) != {seed["id"] for seed in SEEDS}:
    raise AssertionError(
        "derived-fields.json must carry exactly one entry per seeds.json id"
    )
for _id, _entry in DERIVED.items():
    _assert_keys(f'derived-fields.json derived["{_id}"]', set(_entry), DERIVED_KEYS)

# Capability classifications come from the shared manifest. Unsupported
# conformance actions must throw; globally-unsupported actions promoted for
# this adapter are instead checked against the PDP oracle.
SQLALCHEMY_UNSUPPORTED: Dict[str, str] = {
    item["action"]: item["reason"]
    for item in ACTIONS_FILE["adapterUnsupported"].get("sqlalchemy", [])
}

# Globally expected-unsupported shapes promoted by this adapter. Regex is not
# promoted because SQL dialect regex engines do not guarantee CEL/RE2 semantics.
SQLALCHEMY_SUPPORTED_EXPECTED = {
    item["action"]
    for item in ACTIONS_FILE.get("adapterSupportedExpected", {}).get("sqlalchemy", [])
}

ORACLE_ACTIONS = [
    a for a in ACTIONS_FILE["conformance"] if a not in SQLALCHEMY_UNSUPPORTED
] + sorted(SQLALCHEMY_SUPPORTED_EXPECTED)


def _require_message(label: str, message: Any) -> str:
    """The substring this adapter's error must contain, or a loud failure.

    The message is what turns "it raised" into "it raised for the declared
    reason": without it a mapper typo or an unrelated validation satisfies the
    throw suite just as well as the documented limitation
    (cerbos/query-plan-adapters#326).
    """
    if not isinstance(message, str) or not message:
        raise AssertionError(
            f"actions.json pins no throw message for {label}: the throw suite "
            "would accept a failure for any reason"
        )
    return message


# Globally-unsupported planner shapes plus this adapter's own unsupported list:
# translation (or execution) must fail loudly, never produce a silently-wrong
# filter. Each carries the substring the raised error must contain.
THROWING_ACTIONS = sorted(
    [
        (
            item["action"],
            _require_message(
                f'adapterUnsupported.sqlalchemy.{item["action"]}',
                item.get("message"),
            ),
        )
        for item in ACTIONS_FILE["adapterUnsupported"].get("sqlalchemy", [])
    ]
    + [
        (
            item["action"],
            _require_message(
                f'expectedUnsupported.{item["action"]}.messages.sqlalchemy',
                item.get("messages", {}).get("sqlalchemy"),
            ),
        )
        for item in ACTIONS_FILE["expectedUnsupported"]
        if item["action"] not in SQLALCHEMY_SUPPORTED_EXPECTED
    ]
)
THROWING_ACTION_NAMES = {action for action, _ in THROWING_ACTIONS}

# Actions whose `== null` probe targets an attribute the oracle OMITS for NULL
# columns. They carry no oracle comparison: under the omitted representation
# check() denies every row, so the adapter must reject the shape rather than
# emit a filter (#302).
# Every adapter must reject these, so the message map names the whole roster and
# this harness resolves its own entry exactly as it does for a throwing action.
NULL_REPRESENTATION_OMITTED = [
    (
        item["action"],
        item["reason"],
        _require_message(
            f'nullRepresentationOmitted.{item["action"]}.messages.sqlalchemy',
            item.get("messages", {}).get("sqlalchemy"),
        ),
    )
    for item in ACTIONS_FILE["nullRepresentationOmitted"]
]
# The one message every null-carrying action must be rejected with under
# ``omitted``.
NULL_OMITTED_MESSAGE = NULL_REPRESENTATION_OMITTED[0][2]

# Every classified action across all four manifest groups. Read each group
# explicitly: a group this expression does not name is dropped silently, and a
# dropped group makes its actions vanish from every count at once (the
# projection trap conformance/README.md warns about).
MANIFEST_ACTIONS = (
    set(ACTIONS_FILE["conformance"])
    | {u["action"] for u in ACTIONS_FILE["expectedUnsupported"]}
    | {n["action"] for n in ACTIONS_FILE["nullRepresentationOmitted"]}
    | {d["action"] for d in ACTIONS_FILE["knownDivergences"]}
)
SQLALCHEMY_SKIPPED_DIVERGENCES = {
    d["action"]
    for d in ACTIONS_FILE["knownDivergences"]
    if "sqlalchemy" in d["adapters"]
}

# -- the degeneracy guard (conformance/README.md, "The degeneracy guard") ----
#
# A representative sample of the actions this adapter ORACLE-COMPARES, one per
# hostile group it can express. The two lists are asserted to be complements of
# ORACLE_ACTIONS, so neither can drift into the other unnoticed.
#
# w1-size-zero-chain, w1-not-size-chain, w1-size-frac-chain and the two
# string-cast actions are deliberately absent: their oracles are empty by
# CONSTRUCTION (no seed holds a to-one parent with zero children, nor one with
# two or more; every seed's aString raises in int()/double()), so they cannot
# satisfy this guard.
DEGENERACY_GUARD_ACTIONS = (
    "vf-le",
    "like-percent",
    "all-on-empty",
    "pv-exists",
    "pv-all",
    "null-eq",
    "null-ne",
    # The explicit-null convention against a non-null operand (#308). All five are
    # compared rather than raised, because the attribute map declares the convention
    # per attribute; every one of them under-granted by exactly the NULL-column rows
    # before that declaration existed.
    "null-value-ne-const",
    "null-value-not-eq-const",
    "null-value-not-in-const",
    "null-value-f2f",
    "null-value-pv-not-exists",
    # The absent to-one parent (#309/#315/#316/#333/#334).
    "w1-all-chain",
    "w1-not-exists-chain",
    "w1-size-nonneg-chain",
    "w1-not-in-chain",
    "w1-not-hasint-chain",
    "w1-ternary-chain-cond",
    "w1-size-frac-le-chain",
    # Column arithmetic under a division (#311); the zero-denominator arm is a
    # liveness probe below.
    "cr-div-other-column",
    "cr-div-then-add",
    "cr-div-then-add-ne",
    # The real to-one join (#375): one per hazard — the negated hop, the null
    # comparison, two-level depth, the root conjunction, and the disjunction,
    # whose failure direction is an under-grant.
    "rel-not-bool-hop",
    "rel-ne-null-hop",
    "rel-bool-hop2",
    "rel-hop-and-root",
    "rel-hop2-or-exists",
    # Case sensitivity in STRING MATCHING, a different mechanism from cs-eq:
    # collation governs `=`, and on SQLite only `PRAGMA case_sensitive_like`
    # governs LIKE.
    "cs-contains",
    # The primary key as a filterable attribute (#376): against a constant,
    # against a column under negation, and inside a concatenation in both
    # operand orders. SQLAlchemy renders CEL's string `+` as `||` through the
    # column's own type, so both concatenations compare rather than raise.
    "id-eq-const",
    "id-f2f-ne",
    "id-concat",
    "id-concat-vf",
    # string() over a NUMERIC column, the half this adapter lowers. Its boolean
    # sibling is refused instead, so this entry proves the supported half still
    # compares rather than joining the probes below.
    "cast-string-double",
    # CEL's `+` between two COLUMNS (#391). SQLAlchemy renders it through the
    # columns' own String type, so it emits `||` (or CONCAT on MySQL) without
    # needing the plan to say which overload it is.
    "concat-f2f",
)

# Shapes this adapter refuses to translate: they have no oracle comparison to
# guard, and stay here as PDP/policy liveness probes for a group the list above
# cannot cover. See cerbos/query-plan-adapters#324.
DEGENERACY_LIVENESS_PROBES = (
    # json.loads renders the wire's -0 as the integer 0, so the sign of a zero
    # denominator is gone before the adapter sees it.
    "cr-div-neg-zero",
    # int() over a numeric column: truncation-versus-rounding, unsupported for
    # every adapter but convex, which promotes it in adapterSupportedExpected.
    "cast-int-double",
    # string() over a BOOLEAN column, where CAST is dialect-dependent (#376).
    "cast-string-bool",
    # `list` has no operator-table entry, so the constructed hierarchy path is
    # refused before the hierarchy operators around it are reached.
    "hier-list-id",
)


# -- deterministic derived fields (conformance/README.md) --------------------
#
# Read from conformance/derived-fields.json rather than restated here. The same
# value feeds the stored row and the check() oracle, so a transcription error
# would be self-consistent and invisible to the differential; one
# machine-readable definition is what makes that impossible.


def _derived_for(seed: Dict[str, Any]) -> Dict[str, Any]:
    entry = DERIVED.get(seed["id"])
    if entry is None:
        raise AssertionError(
            f'derived-fields.json has no entry for seed "{seed["id"]}"'
        )
    return entry


def _iso_for(seed: Dict[str, Any]) -> str:
    """Deterministic ISO instant per seed for the timestamp probe (see
    conformance/README.md): split around the probe's 2025-01-01 threshold."""
    return _derived_for(seed)["createdBy"]


def _double_for(seed: Dict[str, Any]):
    return _derived_for(seed)["aDouble"]


def _timestamp_for(seed: Dict[str, Any]):
    value = _derived_for(seed)["createdAt"]
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


def _scope_for(seed: Dict[str, Any]):
    return _derived_for(seed)["scope"]


def _labels_for(seed: Dict[str, Any]):
    return _derived_for(seed)["labels"]


# -- the real to-one relation (conformance/README.md, "The real to-one relation")
#
# `parentSeedId` names the seed whose four scalars this row's `parent` carries,
# and that seed's own `parentSeedId` names the ones `parent.inner` carries. The
# chain is cut at two levels. Every resource owns a FRESH parent (and inner) row
# rather than pointing at the named seed's own row, so no two resources share one
# and a filter that returned the parent instead of the child cannot agree with
# the oracle by accident.

_SEEDS_BY_ID: Dict[str, Dict[str, Any]] = {seed["id"]: seed for seed in SEEDS}


def _parent_seed_of(seed):
    if seed is None or seed["parentSeedId"] is None:
        return None
    parent = _SEEDS_BY_ID.get(seed["parentSeedId"])
    if parent is None:
        raise AssertionError(
            f'seeds.json: "{seed["id"]}" names parent "{seed["parentSeedId"]}", '
            "which is not a seed id"
        )
    return parent


def _relation_attr(seed: Dict[str, Any]) -> Dict[str, Any]:
    """The four scalars as check() attributes: a NULL column is MISSING, one hop out."""
    attr: Dict[str, Any] = {
        "aBool": seed["aBool"],
        "aString": seed["aString"],
        "aNumber": seed["aNumber"],
    }
    if seed["aOptionalString"] is not None:
        attr["aOptionalString"] = seed["aOptionalString"]
    return attr


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
    a_double = Column(Float(precision=53), nullable=True)
    a_optional_string = Column(String, nullable=True)
    created_by = Column(String, nullable=False)
    scope = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=True)


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


class AdvLabel(AdvBase):
    __tablename__ = "adversarial_label"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=True)
    sub_category_id = Column(
        String, ForeignKey("adversarial_sub_category.id"), nullable=False
    )


# The corpus's one REAL to-one relation (conformance/seeds.json `parentSeedId`).
# `parent` and `parent.inner` are separate rows reached through a join, unlike
# `obj.inner`, which is a flat column wearing a dotted name. A resource owns its
# own parent chain — the unique foreign key is what makes it to-ONE — so a filter
# that returned the parent instead of the child could not agree with the oracle
# by accident.
#
# ATTR_MAP deliberately carries no entry for these yet. This adapter has no
# relation model of its own: reaching a scalar through a to-one hop is a
# correlated subquery the CALLER supplies, so the mapping is designed alongside
# the actions that need it rather than guessed here. Seeding it now is the expand
# half of cerbos/query-plan-adapters#372's expand-contract.
class AdvParent(AdvBase):
    __tablename__ = "adversarial_parent"

    id = Column(String, primary_key=True)
    a_bool = Column(Boolean, nullable=False)
    a_string = Column(String, nullable=False)
    a_number = Column(Integer, nullable=False)
    a_optional_string = Column(String, nullable=True)
    resource_id = Column(
        String, ForeignKey("adversarial_resource.id"), nullable=False, unique=True
    )


class AdvInner(AdvBase):
    __tablename__ = "adversarial_inner"

    id = Column(String, primary_key=True)
    a_bool = Column(Boolean, nullable=False)
    a_string = Column(String, nullable=False)
    a_number = Column(Integer, nullable=False)
    a_optional_string = Column(String, nullable=True)
    parent_id = Column(
        String, ForeignKey("adversarial_parent.id"), nullable=False, unique=True
    )


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
        hop_correlation: Union[List[Any], None] = None,
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
        # Correlation for the INTERMEDIATE hops alone, when the collection is
        # reached through an optional to-one parent. CEL cannot dot through a
        # list, so `mainCategory.subCategories` reaches its tail through a to-one
        # parent: absent, the application sends no `mainCategory` attribute and
        # CEL raises a missing-path error, which denies. A subquery rooted at the
        # resource row cannot see that — an absent parent and a childless parent
        # both return nothing — so `all` reads TRUE, `!exists` reads TRUE and the
        # count reads 0, each admitting rows the PDP denies
        # (cerbos/query-plan-adapters#309). Requiring the hop separately restores
        # the distinction. The requirement itself is `cerbos_sqlalchemy.
        # require_hops`; what stays in the MAPPING is only which predicates the
        # hops are, because the SQLAlchemy adapter has no relation model of its
        # own: collection semantics are entirely caller-supplied through operator
        # overrides, so the caller owns the invariant that its subquery sees
        # exactly the rows the application serialised into the resource
        # attributes.
        self.hop_correlation = hop_correlation or []

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return f"_Relation({self.description})"


TAGS = _Relation(
    "tags",
    [AdvTag.resource_id == AdvResource.id],
    # The root resource may be any number of lambda scopes up (w2-outer-relation
    # plans a tags exists INSIDE the categories lambda).
    correlate_targets=[AdvResource],
)
TAG_NAMES = _Relation(
    "tagNames",
    [AdvTag.resource_id == AdvResource.id],
    correlate_targets=[AdvResource],
    member_field=AdvTag.name,
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
LABELS_OF_SUB = _Relation(
    "s.labels",
    [AdvLabel.sub_category_id == AdvSubCategory.id],
    correlate_targets=[AdvSubCategory, AdvCategory, AdvResource],
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
    hop_correlation=[AdvCategory.resource_id == AdvResource.id],
)
MAIN_SUBNAMES = _Relation(
    "mainCategory.subNames",
    [
        AdvSubCategory.category_id == AdvCategory.id,
        AdvCategory.resource_id == AdvResource.id,
    ],
    correlate_targets=[AdvResource],
    member_field=AdvSubCategory.name,
    hop_correlation=[AdvCategory.resource_id == AdvResource.id],
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


def _require_hops(rel: _Relation, expr: Any):
    """Make ``expr`` UNKNOWN unless every intermediate to-one hop exists.

    The invariant lives in the library as ``cerbos_sqlalchemy.require_hops``; this
    is only the unpacking of the harness's ``_Relation`` marker into its arguments.
    The harness using the shipped helper rather than a private copy is what proves
    the helper: every chained corpus action — ``w1-all-chain``,
    ``w1-not-exists-chain``, ``w1-size-zero-chain``, ``w1-not-in-chain``,
    ``w1-not-hasint-chain`` and the rest — is an oracle comparison against a real
    PDP that runs through this call.
    """
    return require_hops(expr, rel.hop_correlation, rel.correlate_targets)


def _require_relation(op: str, coll: Any) -> _Relation:
    if not isinstance(coll, _Relation):
        raise ValueError(f"{op} over unsupported collection operand: {coll!r}")
    return coll


def _exists_fn(coll: Any, body: Any):
    # CEL exists: true on any true witness (absorbing errors), error if any
    # element errors without one, false otherwise (incl. empty).
    rel = _require_relation("exists", coll)
    return _require_hops(
        rel,
        case(
            (_exists_where(rel, body), true()),
            (_exists_where(rel, body.is_(None)), null()),
            else_=false(),
        ),
    )


def _all_fn(coll: Any, body: Any):
    # CEL all: false on any false witness (absorbing errors), error if any
    # element errors without one, true otherwise (incl. empty).
    rel = _require_relation("all", coll)
    return _require_hops(
        rel,
        case(
            (_exists_where(rel, not_(body)), false()),
            (_exists_where(rel, body.is_(None)), null()),
            else_=true(),
        ),
    )


def _exists_one_fn(coll: Any, body: Any):
    # CEL exists_one never absorbs an erroring element, even next to a true
    # witness; otherwise it's an exact count-of-matches == 1.
    rel = _require_relation("exists_one", coll)
    return _require_hops(
        rel,
        case(
            (_exists_where(rel, body.is_(None)), null()),
            else_=(_count_subquery(rel, body) == 1),
        ),
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
        # columns still count — no error guard needed. An absent to-one parent
        # still has to count as UNKNOWN rather than 0 (#309).
        return _require_hops(target, _count_subquery(target))
    if isinstance(target, tuple) and target[0] == "filter":
        # CEL filter never absorbs an erroring element: any UNKNOWN body row
        # poisons the whole count.
        _, rel, body = target
        return _require_hops(
            rel,
            case(
                (_exists_where(rel, body.is_(None)), null()),
                else_=_count_subquery(rel, body),
            ),
        )
    return func.length(target)


def _has_intersection_fn(mapped: Any, values: Any):
    if isinstance(mapped, _Relation):
        if mapped.member_field is None:
            raise ValueError(
                f"hasIntersection over relation without member field: {mapped!r}"
            )
        return _require_hops(
            mapped,
            _exists_where(mapped, _scalar_membership(mapped.member_field, values)),
        )

    # hasIntersection(map(coll, x), list): map errors on any erroring element
    # (no absorption), so the error guard comes FIRST.
    if not (isinstance(mapped, tuple) and mapped[0] == "map"):
        raise ValueError(f"hasIntersection over unsupported operand: {mapped!r}")
    _, rel, projected = mapped
    return _require_hops(
        rel,
        case(
            (_exists_where(rel, projected.is_(None)), null()),
            (_exists_where(rel, _scalar_membership(projected, values)), true()),
            else_=false(),
        ),
    )


def _scalar_membership(column: Any, values: Any):
    members = values if isinstance(values, list) else [values]
    non_nulls = [member for member in members if member is not None]
    predicates = []
    if non_nulls:
        predicates.append(column.in_(non_nulls))
    if len(non_nulls) != len(members):
        predicates.append(column.is_(None))
    return or_(*predicates) if predicates else false()


def _relation_membership(relation: _Relation, value: Any):
    if relation.member_field is None:
        raise ValueError(f"in over relation without member field: {relation!r}")
    member = relation.member_field
    if value is None:
        predicate = member.is_(None)
    elif hasattr(value, "is_"):
        predicate = or_(
            member == value,
            and_(member.is_(None), value.is_(None)),
        )
    else:
        predicate = member == value
    return _require_hops(relation, _exists_where(relation, predicate))


def _in_fn(column: Any, value: Any):
    if isinstance(column, _Relation):
        # `value in R.attr.<chain>`: membership against the relation's member
        # column; rows with an empty chain are simply excluded (CEL
        # missing-attribute error → deny).
        return _relation_membership(column, value)
    if isinstance(value, _Relation):
        return _relation_membership(value, column)
    return _scalar_membership(column, value)


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

# `owner` and `coOwner` alias columns that `aOptionalString` and `scope` also map,
# under the OTHER null convention: the oracle sends a real null attribute for them
# rather than omitting it. Declaring that here is what makes the equality family
# definite for these two attributes and leaves it untouched for every other
# mapping (cerbos/query-plan-adapters#308).
ATTRIBUTE_NULL_REPRESENTATION = {
    "request.resource.attr.owner": "explicit",
    "request.resource.attr.coOwner": "explicit",
}


def _parent_scalar(column):
    """One scalar of the to-one `parent`, as a correlated scalar subquery.

    The resource owns at most one parent row (``resource_id`` is UNIQUE), so this
    yields that row's value, or SQL NULL when the resource has no parent at all.
    NULL is precisely what the check side means: an absent level sends no
    attribute, CEL raises a missing-path error, and the PDP denies. Because
    ``NOT NULL`` is still NULL, the row stays excluded under both polarities
    without the explicit ``require_hops`` guard the COLLECTION chains need
    (cerbos/query-plan-adapters#375).
    """
    return (
        select(column)
        .where(AdvParent.resource_id == AdvResource.id)
        .correlate(AdvResource)
        .scalar_subquery()
    )


def _inner_scalar(column):
    """The same, one level further out: `parent.inner`.

    Nesting the parent's own lookup inside the correlation is what keeps the two
    levels distinct — reading off the parent, or off the resource, gives a
    different row set for every action in the group.
    """
    return (
        select(column)
        .where(
            AdvInner.parent_id
            == select(AdvParent.id)
            .where(AdvParent.resource_id == AdvResource.id)
            .correlate(AdvResource)
            .scalar_subquery()
        )
        .correlate(AdvResource)
        .scalar_subquery()
    )


ATTR_MAP = {
    # The primary key, reached as `request.resource.id` rather than through `attr` (the `id-*`
    # actions). An adapter that resolves references by stripping a `request.resource.attr.`
    # prefix never sees this name.
    "request.resource.id": AdvResource.id,
    "request.resource.attr.aBool": AdvResource.a_bool,
    "request.resource.attr.aString": AdvResource.a_string,
    "request.resource.attr.aNumber": AdvResource.a_number,
    "request.resource.attr.aDouble": AdvResource.a_double,
    "request.resource.attr.aOptionalString": AdvResource.a_optional_string,
    "request.resource.attr.createdBy": AdvResource.created_by,
    "request.resource.attr.owner": AdvResource.a_optional_string,
    "request.resource.attr.coOwner": AdvResource.scope,
    "request.resource.attr.scope": AdvResource.scope,
    "request.resource.attr.createdAt": AdvResource.created_at,
    # obj.inner is not a real nested column — mirrors aString, the same trick
    # the spring-data and prisma reference harnesses use for the p-struct probe.
    "request.resource.attr.obj.inner": AdvResource.a_string,
    # The corpus's one REAL to-one chain (the `rel-*` actions). This adapter has no
    # relation model, so the caller supplies the hop as a correlated scalar
    # subquery — and that spelling needs no separate hop guard: an absent parent
    # makes the subquery SQL NULL, which is CEL's missing-path error, and NOT NULL
    # is still NULL, so the row stays excluded under BOTH polarities.
    "request.resource.attr.parent.aBool": _parent_scalar(AdvParent.a_bool),
    "request.resource.attr.parent.aString": _parent_scalar(AdvParent.a_string),
    "request.resource.attr.parent.aNumber": _parent_scalar(AdvParent.a_number),
    "request.resource.attr.parent.aOptionalString": _parent_scalar(
        AdvParent.a_optional_string
    ),
    "request.resource.attr.parent.inner.aBool": _inner_scalar(AdvInner.a_bool),
    "request.resource.attr.parent.inner.aString": _inner_scalar(AdvInner.a_string),
    "request.resource.attr.parent.inner.aNumber": _inner_scalar(AdvInner.a_number),
    "request.resource.attr.parent.inner.aOptionalString": _inner_scalar(
        AdvInner.a_optional_string
    ),
    "request.resource.attr.tags": TAGS,
    "request.resource.attr.tagNames": TAG_NAMES,
    "t": TAGS,
    "t.id": AdvTag.tag_id,
    "t.name": AdvTag.name,
    "request.resource.attr.categories": CATEGORIES,
    "c": CATEGORIES,
    # The category's own name, read inside the categories lambda. Only
    # rel-hop2-or-exists reaches it — every other categories probe dots straight
    # through to subCategories — so it is mapped here rather than alongside them.
    "c.name": AdvCategory.name,
    "c.subCategories": SUB_OF_CATEGORY,
    "s": SUB_OF_CATEGORY,
    "s.name": AdvSubCategory.name,
    "s.labels": LABELS_OF_SUB,
    "l": LABELS_OF_SUB,
    "l.name": AdvLabel.name,
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
        # case-insensitive by default.
        dbapi_conn.execute("PRAGMA case_sensitive_like = ON")

    AdvBase.metadata.create_all(engine)

    resource_rows = []
    parent_rows = []
    inner_rows = []
    tag_rows = []
    category_rows = []
    sub_category_rows = []
    label_rows = []
    for seed in SEEDS:
        resource_rows.append(
            {
                "id": seed["id"],
                "a_bool": seed["aBool"],
                "a_string": seed["aString"],
                "a_number": seed["aNumber"],
                "a_double": _double_for(seed),
                "a_optional_string": seed["aOptionalString"],
                "created_by": _iso_for(seed),
                "scope": _scope_for(seed),
                "created_at": _timestamp_for(seed),
            }
        )
        # The to-one chain, one owned row per level. A seed with no parent gets no
        # row at all, which is what makes the absent-parent hazard reachable
        # through a SCALAR rather than only through mainCategory's collection.
        if (parent_seed := _parent_seed_of(seed)) is not None:
            parent_id = f"{seed['id']}-parent"
            parent_rows.append(
                {
                    "id": parent_id,
                    "a_bool": parent_seed["aBool"],
                    "a_string": parent_seed["aString"],
                    "a_number": parent_seed["aNumber"],
                    "a_optional_string": parent_seed["aOptionalString"],
                    "resource_id": seed["id"],
                }
            )
            if (inner_seed := _parent_seed_of(parent_seed)) is not None:
                inner_rows.append(
                    {
                        "id": f"{parent_id}-inner",
                        "a_bool": inner_seed["aBool"],
                        "a_string": inner_seed["aString"],
                        "a_number": inner_seed["aNumber"],
                        "a_optional_string": inner_seed["aOptionalString"],
                        "parent_id": parent_id,
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
                    "id": (sub_category_id := f"{seed['id']}-sub{i}"),
                    "name": sub_name,
                    "category_id": category_id,
                }
            )
            for label_index, label_name in enumerate(_labels_for(seed)):
                label_rows.append(
                    {
                        "id": f"{seed['id']}-label{i}-{label_index}",
                        "name": label_name,
                        "sub_category_id": sub_category_id,
                    }
                )

    with engine.begin() as conn:
        conn.execute(insert(AdvResource.__table__), resource_rows)
        if parent_rows:
            conn.execute(insert(AdvParent.__table__), parent_rows)
        if inner_rows:
            conn.execute(insert(AdvInner.__table__), inner_rows)
        if tag_rows:
            conn.execute(insert(AdvTag.__table__), tag_rows)
        if category_rows:
            conn.execute(insert(AdvCategory.__table__), category_rows)
        if sub_category_rows:
            conn.execute(insert(AdvSubCategory.__table__), sub_category_rows)
        if label_rows:
            conn.execute(insert(AdvLabel.__table__), label_rows)

    yield engine


@pytest.fixture
def adv_conn(adv_engine):
    with adv_engine.connect() as conn:
        yield conn


@pytest.fixture(scope="module")
def adv_cerbos_client():
    container = CerbosContainer(image=CERBOS_IMAGE)
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


def _label_attr(name: Any) -> Dict[str, Any]:
    """A NULL label name in the DB is a MISSING element attribute."""
    return {"name": name} if name is not None else {}


def _check_resource(seed: Dict[str, Any]) -> Resource:
    """Cerbos attributes mirroring exactly what the seeded DB row holds."""
    attr: Dict[str, Any] = {
        "aBool": seed["aBool"],
        "aString": seed["aString"],
        "aNumber": seed["aNumber"],
        "createdBy": _iso_for(seed),
        "obj": {"inner": seed["aString"]},
        "tags": [_tag_attr(t) for t in seed["tags"]],
        # These two attributes deliberately use EXPLICIT nulls. Unlike the
        # optional field above, CEL membership distinguishes null from missing.
        "owner": seed["aOptionalString"],
        # `coOwner` is the explicit-null alias of the `scope` column, the second
        # half of `null-value-f2f`: `scope` itself is omitted when NULL (below),
        # so the corpus carries the same column under both conventions and the
        # field-to-field probe has two explicit nulls to compare.
        "coOwner": _scope_for(seed),
        "tagNames": [tag["name"] for tag in seed["tags"]],
        "categories": [
            {
                "name": "business",
                "subCategories": [
                    {
                        "name": n,
                        "labels": [_label_attr(label) for label in _labels_for(seed)],
                    }
                ],
            }
            for n in seed["subCategoryNames"]
        ],
    }
    # A DB NULL is a missing attribute on the check side — conditions touching
    # it must deny (CEL error), matching SQL three-valued logic excluding the row.
    if seed["aOptionalString"] is not None:
        attr["aOptionalString"] = seed["aOptionalString"]
    if (a_double := _double_for(seed)) is not None:
        attr["aDouble"] = a_double
    if (scope := _scope_for(seed)) is not None:
        attr["scope"] = scope
    if (created_at := _timestamp_for(seed)) is not None:
        attr["createdAt"] = created_at.isoformat().replace("+00:00", "Z")
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
    # The real to-one chain, mirroring the seeded rows exactly. A row with no
    # parent sends NO `parent` attribute — a CEL missing-path error (deny) —
    # matching the adapter's join finding nothing; likewise for `parent.inner`.
    if (parent_seed := _parent_seed_of(seed)) is not None:
        parent_attr = _relation_attr(parent_seed)
        if (inner_seed := _parent_seed_of(parent_seed)) is not None:
            parent_attr["inner"] = _relation_attr(inner_seed)
        attr["parent"] = parent_attr
    return Resource(id=seed["id"], kind=RESOURCE_KIND, attr=attr)


# -- oracle: ask the PDP itself, row by row --


def _oracle_allowed_ids(client: CerbosClient, action: str) -> Set[str]:
    return {
        seed["id"]
        for seed in SEEDS
        if client.is_allowed(action, _principal(), _check_resource(seed))
    }


def _plan_carries_null_literal(node) -> bool:
    """Whether any operand anywhere in the plan is a literal null, or a list containing one."""
    if not isinstance(node, dict):
        return False
    if "value" in node:
        value = node["value"]
        return value is None or (
            isinstance(value, list) and any(member is None for member in value)
        )
    expression = node.get("expression", node)
    return any(
        _plan_carries_null_literal(operand)
        for operand in expression.get("operands", [])
    )


# -- adapter execution through the public get_query path --


def _adapter_filtered_ids(
    client: CerbosClient,
    conn,
    action: str,
    null_attribute_representation: str = "explicit",
    attribute_null_representation=ATTRIBUTE_NULL_REPRESENTATION,
) -> Set[str]:
    plan = client.plan_resources(action, _principal(), ResourceDesc(RESOURCE_KIND))
    query = get_query(
        plan,
        AdvResource,
        ATTR_MAP,
        operator_override_fns=OPERATOR_OVERRIDES,
        null_attribute_representation=null_attribute_representation,
        attribute_null_representation=attribute_null_representation,
    )
    return {row.id for row in conn.execute(query).fetchall()}


# A cartesian-product warning from SQLAlchemy means a subquery failed to
# correlate (comparing against EVERY row of a table instead of the current
# one) — a silent-wrongness bug class, so escalate it to an error.
@pytest.mark.filterwarnings("error::sqlalchemy.exc.SAWarning")
class TestAdversarialConformance:
    def test_throwing_action_with_no_pinned_message_fails_classification(self):
        # Adding a throwing action without pinning its message must fail this
        # harness rather than silently degrade the throw suite to a bare "it
        # raised" (cerbos/query-plan-adapters#326).
        for absent in (None, "", 42):
            with pytest.raises(AssertionError, match="pins no throw message"):
                _require_message("synthetic-entry", absent)

    def test_manifest_assigns_every_action_exactly_one_outcome(self):
        oracle = set(ORACLE_ACTIONS)
        throwing = THROWING_ACTION_NAMES
        null_omitted = {action for action, _, _ in NULL_REPRESENTATION_OMITTED}
        misclassified = [
            action
            for action in sorted(MANIFEST_ACTIONS)
            if [
                action in oracle,
                action in throwing,
                action in null_omitted,
                action in SQLALCHEMY_SKIPPED_DIVERGENCES,
            ].count(True)
            != 1
        ]

        # Deliberate tripwires: a corpus edit must bump these in the same
        # change, so a new hostile action cannot join (or vanish) silently.
        assert len(MANIFEST_ACTIONS) == 179
        assert len(SEEDS) == 21
        # Each of these carries a pinned message, so a shape gained or lost has
        # to be re-triaged here rather than joining the throw suite unnoticed.
        assert len(THROWING_ACTIONS) == 15
        assert misclassified == []
        assert SQLALCHEMY_SUPPORTED_EXPECTED <= {
            u["action"] for u in ACTIONS_FILE["expectedUnsupported"]
        }

    @pytest.mark.parametrize("action", ORACLE_ACTIONS)
    def test_matches_check_oracle(self, action, adv_cerbos_client, adv_conn):
        oracle = _oracle_allowed_ids(adv_cerbos_client, action)
        filtered = _adapter_filtered_ids(adv_cerbos_client, adv_conn, action)
        assert sorted(filtered) == sorted(oracle)

    @pytest.mark.parametrize("action,message", THROWING_ACTIONS)
    def test_fails_loudly(self, action, message, adv_cerbos_client):
        # The plan is fetched OUTSIDE the assertion so a PDP failure fails the
        # test instead of passing it, and nothing executes — the invariant is
        # that the shape throws during translation, BEFORE a filter exists, so
        # the database rejecting a wrongly emitted query afterwards cannot
        # masquerade as the adapter refusing to translate.
        plan = adv_cerbos_client.plan_resources(
            action, _principal(), ResourceDesc(RESOURCE_KIND)
        )
        # The adapter's translation-time refusals: ValueError (unsupported
        # operator/cast/timestamp shapes), KeyError (attribute missing from the
        # map), TypeError (attribute needs an operator override to be
        # expressible). Anything else — connection errors, SQLAlchemy runtime
        # errors — must fail the test, not satisfy it.
        #
        # The exception type alone is not enough: it scopes the failure to the
        # adapter but says nothing about WHICH refusal fired, so the corpus
        # message pins the mechanism too (cerbos/query-plan-adapters#326).
        with pytest.raises((ValueError, KeyError, TypeError), match=re.escape(message)):
            get_query(
                plan,
                AdvResource,
                ATTR_MAP,
                operator_override_fns=OPERATOR_OVERRIDES,
                null_attribute_representation="explicit",
                # The per-attribute declarations belong here too: a shape whose
                # refusal depends on them (null-value-f2f-mixed) would otherwise
                # translate cleanly and read as a missing throw.
                attribute_null_representation=ATTRIBUTE_NULL_REPRESENTATION,
            )

    # #302. `null-eq-missing` probes `aOptionalString == null`, and
    # `aOptionalString` follows the corpus default: a NULL column sends NO
    # attribute. Both halves are asserted because the rejection alone would pass
    # vacuously if the adapter raised for an unrelated reason — the over-grant
    # under the default representation is what makes the rejection necessary.
    @pytest.mark.parametrize("action,reason,message", NULL_REPRESENTATION_OMITTED)
    def test_null_representation_omitted_is_rejected(
        self, action, reason, message, adv_cerbos_client, adv_conn
    ):
        assert _oracle_allowed_ids(adv_cerbos_client, action) == set()

        # The default translation emits IS NULL and returns exactly the rows the
        # PDP denies.
        over_granted = _adapter_filtered_ids(adv_cerbos_client, adv_conn, action)
        assert len(over_granted) > 0, reason

        with pytest.raises(ValueError, match=re.escape(message)):
            _adapter_filtered_ids(
                adv_cerbos_client,
                adv_conn,
                action,
                null_attribute_representation="omitted",
            )

    # #302 completeness guard. The rejection must key off the null OPERAND, not off a
    # list of operators: `hasIntersection(tagNames, ["public", None])` carries one in
    # its value list, and an allowlist of eq/ne/in silently misses it. Enumerating the
    # corpus rather than naming shapes means a newly added action carrying a null
    # constant is covered automatically.
    # #308. The per-attribute declaration overrides the call-level option, which is
    # the property that makes a suite mixing both conventions expressible at all.
    # Asserted in both directions against the SAME action and the SAME call-level
    # option, varying only whether the attribute map declares the convention -- so a
    # declaration that did nothing would show up here as the two runs agreeing. It
    # also proves the completeness guard below is not quietly running against the
    # same declarations.
    def test_attribute_declaration_overrides_the_call_level_representation(
        self, adv_cerbos_client, adv_conn
    ):
        # `owner` declares "explicit", so the call-level "omitted" does not reach it.
        assert _adapter_filtered_ids(
            adv_cerbos_client,
            adv_conn,
            "null-eq",
            null_attribute_representation="omitted",
        ) == _oracle_allowed_ids(adv_cerbos_client, "null-eq")

        with pytest.raises(ValueError, match="null operand"):
            _adapter_filtered_ids(
                adv_cerbos_client,
                adv_conn,
                "null-eq",
                null_attribute_representation="omitted",
                attribute_null_representation=None,
            )

    def test_every_null_carrying_action_is_rejected_under_omitted(
        self, adv_cerbos_client, adv_conn
    ):
        null_carrying = []
        for action in sorted(MANIFEST_ACTIONS):
            plan = adv_cerbos_client.plan_resources(
                action, _principal(), ResourceDesc(RESOURCE_KIND)
            )
            if (
                plan.filter is None
                or plan.filter.kind != PlanResourcesFilterKind.CONDITIONAL
            ):
                continue
            if _plan_carries_null_literal(plan.filter.condition.to_dict()):
                null_carrying.append(action)

        # Guard the guard: if the walk stopped finding null operands the loop is vacuous.
        assert "null-eq-missing" in null_carrying
        assert "in-null-elem-hasint" in null_carrying

        not_rejected = []
        for action in null_carrying:
            try:
                _adapter_filtered_ids(
                    adv_cerbos_client,
                    adv_conn,
                    action,
                    null_attribute_representation="omitted",
                    attribute_null_representation=None,
                )
                not_rejected.append(action)
            except Exception as exc:  # noqa: BLE001 - triaged below
                # The rejection must be the null-operand check talking, not an
                # incidental failure: a transport error or attr-map typo counting
                # as the required rejection is the silent pass the corpus README
                # warns about.
                if NULL_OMITTED_MESSAGE not in str(exc):
                    not_rejected.append(
                        f"{action} (rejected for the wrong reason: {exc})"
                    )
        assert not_rejected == []

    # nan-ord-inf is absent: its 1.0/0.0 and -1.0/0.0 branches carry a CONSTANT zero
    # denominator, and over the HTTP transport that arrives as the integer 0 with the
    # sign bit already gone, so the adapter now rejects the shape rather than guess
    # which infinity CEL produced. It is declared in adapterUnsupported[sqlalchemy]
    # and asserted as a throw by test_fails_loudly (cerbos/query-plan-adapters#312).
    @pytest.mark.parametrize(
        "action",
        (
            "nan-ord-ternary",
            "nan-ord-ternary-vf",
            "nan-ord-le",
        ),
    )
    def test_nonfinite_ordering_is_folded_before_postgresql_compilation(
        self, action, adv_cerbos_client
    ):
        plan = adv_cerbos_client.plan_resources(
            action, _principal(), ResourceDesc(RESOURCE_KIND)
        )
        query = get_query(
            plan,
            AdvResource,
            ATTR_MAP,
            operator_override_fns=OPERATOR_OVERRIDES,
        )
        compiled = query.compile(dialect=postgresql.dialect())

        assert not any(
            isinstance(value, float) and not math.isfinite(value)
            for value in compiled.params.values()
        )

    def test_upstream_has_fold_overgrant_tripwire(self, adv_cerbos_client, adv_conn):
        """Pin the PDP planner's known has() fold until the upstream fix lands.

        The check API denies rows where ``aOptionalString`` is missing, while
        the planner currently folds the same condition to ALWAYS_ALLOWED. The
        adapter must translate that plan faithfully; this test keeps the one
        intentional oracle divergence visible and fails when the image changes
        so ``p-has`` can move back into the differential run.
        """
        action = "p-has"
        plan = adv_cerbos_client.plan_resources(
            action, _principal(), ResourceDesc(RESOURCE_KIND)
        )
        oracle = _oracle_allowed_ids(adv_cerbos_client, action)
        all_ids = {seed["id"] for seed in SEEDS}

        assert plan.filter.kind == PlanResourcesFilterKind.ALWAYS_ALLOWED
        assert 0 < len(oracle) < len(all_ids)
        assert _adapter_filtered_ids(adv_cerbos_client, adv_conn, action) == all_ids

    def test_seeded_to_one_chain_matches_the_corpus_relation(self, adv_conn):
        """The relation carries no corpus action yet — this is the expand half of
        cerbos/query-plan-adapters#372's expand-contract — so nothing else in this
        file would notice a seeder that stored no chain at all, or one that
        attached every parent to the wrong resource. Read the two hops back
        through a real join rather than counting rows: a count cannot tell an
        inner row carrying the corpus's values from one carrying the root's own
        columns, which is exactly the flat-column-alias failure this relation
        exists to make visible.
        """
        with_parent = [s for s in SEEDS if _parent_seed_of(s) is not None]
        with_inner = [
            s for s in SEEDS if _parent_seed_of(_parent_seed_of(s)) is not None
        ]
        assert with_parent
        assert with_inner
        assert len(with_parent) < len(SEEDS)

        joined = (
            select(
                AdvResource.id,
                AdvParent.a_string.label("parent"),
                AdvInner.a_string.label("inner"),
            )
            .select_from(AdvResource.__table__)
            .outerjoin(AdvParent.__table__, AdvParent.resource_id == AdvResource.id)
            .outerjoin(AdvInner.__table__, AdvInner.parent_id == AdvParent.id)
        )
        stored = {row.id: (row.parent, row.inner) for row in adv_conn.execute(joined)}

        def a_string_of(seed) -> Union[str, None]:
            return None if seed is None else seed["aString"]

        assert stored == {
            seed["id"]: (
                a_string_of(_parent_seed_of(seed)),
                a_string_of(_parent_seed_of(_parent_seed_of(seed))),
            )
            for seed in SEEDS
        }

    def test_oracle_is_not_degenerate(self, adv_cerbos_client):
        # Guard the guard: these actions must produce a non-empty, non-total
        # oracle set, otherwise the differential comparison could pass
        # vacuously (e.g. a PDP that denies everything).
        #
        # Every entry is asserted to be an action this adapter actually
        # oracle-compares. A list copied from another harness drifts into naming
        # shapes it never compares, which guard nothing
        # (cerbos/query-plan-adapters#324); the membership assertion turns moving
        # an action into adapterUnsupported into a failure here rather than a
        # silent no-op.
        def assert_non_degenerate(action: str) -> None:
            ids = _oracle_allowed_ids(adv_cerbos_client, action)
            assert 0 < len(ids) < len(SEEDS), f"{action} has a degenerate oracle"

        for action in DEGENERACY_GUARD_ACTIONS:
            assert action in ORACLE_ACTIONS, f"{action} is not oracle-compared"
            assert_non_degenerate(action)
        # Asserting the complement keeps the split honest — an action this
        # adapter gains support for must move up into the guard proper.
        for action in DEGENERACY_LIVENESS_PROBES:
            assert action not in ORACLE_ACTIONS, f"{action} is now oracle-compared"
            assert_non_degenerate(action)
