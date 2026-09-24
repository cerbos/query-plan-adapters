# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""The shared ``../conformance/`` corpus as this adapter's suites read it.

``test_adversarial_conformance.py`` replays the recorded plans against real stores;
``test_translator.py`` asks the same mapping the questions a store cannot answer. Both need
the one mapping -- the schema, ``ATTR_MAP``, ``OPERATOR_OVERRIDES``, the collection storage
and the NULL conventions -- so it lives here rather than in either suite.

The code in this file is duplicated across adapters **on purpose** -- adapters share data,
not code, so that every adapter stays standalone. See
`ADR 0007 <../../docs/adr/0007-adapters-share-data-not-code.md>`_.

Test-only: it lives under ``tests/`` and never reaches the published package.
"""

import glob
import json
import os
import time
from datetime import datetime, timezone
from typing import Any

from cerbos.response.v1 import response_pb2
from cerbos.sdk.model import PlanResourcesResponse
from google.protobuf.json_format import ParseDict
from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
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
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import declarative_base

from cerbos_sqlalchemy import CollectionColumn, UnsupportedPlanError, require_hops
from cerbos_sqlalchemy.query import OPERATOR_FNS

CONFORMANCE_DIR = os.path.realpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "conformance")
)


def read_corpus_json(name: str) -> Any:
    with open(os.path.join(CONFORMANCE_DIR, name), encoding="utf-8") as f:
        return json.load(f)


# -- the golden files -------------------------------------------------------

#: Both recorded PDPs, current first.
PDP_TAGS: list[str] = [
    read_corpus_json("pdp-versions.json")[which]["tag"]
    for which in ("current", "previous")
]


def golden_cases(tag: str) -> list[dict[str, Any]]:
    """Every golden file recorded for one PDP tag, sorted by case id."""
    root = os.path.join(CONFORMANCE_DIR, "golden", tag)
    paths = glob.glob(os.path.join(root, "**", "*.json"), recursive=True)
    cases = []
    for path in paths:
        with open(path, encoding="utf-8") as f:
            cases.append(json.load(f))
    return sorted(cases, key=lambda case: case["id"])


def golden_case(case_id: str, tag: str | None = None) -> dict[str, Any]:
    """One golden file, from the current PDP unless ``tag`` says otherwise."""
    path = os.path.join(
        CONFORMANCE_DIR, "golden", tag or PDP_TAGS[0], case_id + ".json"
    )
    with open(path, encoding="utf-8") as f:
        return json.load(f)


_NOW_MINUS_24H = "__NOW_MINUS_24H__"


def now_minus_24h() -> str:
    """``now() - duration("24h")`` as the PDP folds it: RFC 3339 at nanosecond precision.

    The precision is the point. The planner folds the literal with Go's nanosecond clock, and
    a ``DateTime`` column holds microseconds, which is why this adapter refuses those plans.
    A clock that happened to land on a whole microsecond would hide that, so the last digit
    is forced non-zero.
    """
    ns = time.time_ns() - 24 * 3600 * 10**9
    seconds, fraction = divmod(ns, 10**9)
    if fraction % 1000 == 0:
        fraction += 1
    stamp = datetime.fromtimestamp(seconds, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{stamp}.{fraction:09d}Z"


def _substitute(node: Any, planned_at: str) -> Any:
    if isinstance(node, dict):
        return {k: _substitute(v, planned_at) for k, v in node.items()}
    if isinstance(node, list):
        return [_substitute(v, planned_at) for v in node]
    return planned_at if node == _NOW_MINUS_24H else node


def _response_dict(case: dict[str, Any], planned_at: str | None) -> dict[str, Any]:
    return {
        "requestId": "",
        "action": case["request"]["action"],
        "resourceKind": case["request"]["resourceKind"],
        "policyVersion": "default",
        "filter": _substitute(case["plan"], planned_at or now_minus_24h()),
    }


def plan_from_golden(
    case: dict[str, Any], planned_at: str | None = None
) -> PlanResourcesResponse:
    """The recorded plan as the HTTP SDK hands it to a caller.

    The golden ``plan`` is the PDP's ``filter`` object as the HTTP API returns it, so the
    decoding is the one ``cerbos.sdk.client.CerbosClient`` performs.
    """
    return PlanResourcesResponse.from_dict(_response_dict(case, planned_at))


def grpc_plan_from_golden(
    case: dict[str, Any], planned_at: str | None = None
) -> response_pb2.PlanResourcesResponse:
    """The same plan re-encoded as the gRPC client's protobuf response.

    JSON has already lost what the two transports disagree about (the sign of a ``-0``), so
    this exercises the protobuf decoding arm without standing in for a real gRPC frame.
    """
    return ParseDict(
        _response_dict(case, planned_at), response_pb2.PlanResourcesResponse()
    )


# ---------------------------------------------------------------------------
# Schema: dedicated tables so hostile seeds (NULL element columns, duplicate
# names, LIKE metacharacters) are all representable.
# ---------------------------------------------------------------------------

AdvBase = declarative_base()

# The ordered copies of the resource's collections, for `collection_columns` (#227). JSON text on
# SQLite and JSONB on PostgreSQL; `none_as_null` so an absent collection is SQL NULL rather than
# the JSON document `null`.
_COLLECTION_JSON = JSON(none_as_null=True).with_variant(
    JSONB(none_as_null=True), "postgresql"
)
# The same collections as native PostgreSQL arrays. Only the PostgreSQL leg declares them; on
# SQLite, which has no array type, the variant falls back to JSON text nothing reads.
_COLLECTION_ARRAY = JSON(none_as_null=True).with_variant(ARRAY(String), "postgresql")
_NUMBER_ARRAY = JSON(none_as_null=True).with_variant(ARRAY(Integer), "postgresql")
_BOOL_ARRAY = JSON(none_as_null=True).with_variant(ARRAY(Boolean), "postgresql")


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
    updated_at = Column(DateTime(timezone=True), nullable=True)
    tags_json = Column(_COLLECTION_JSON, nullable=True)
    tag_names_json = Column(_COLLECTION_JSON, nullable=True)
    main_sub_categories_json = Column(_COLLECTION_JSON, nullable=True)
    tags_array = Column(_COLLECTION_ARRAY, nullable=True)
    tag_names_array = Column(_COLLECTION_ARRAY, nullable=True)
    main_sub_categories_array = Column(_COLLECTION_ARRAY, nullable=True)
    # The corpus's two scalar lists of numbers and booleans, which exist to prove an element's
    # JSON type survives the comparison (conformance/README.md, "Number and boolean list
    # elements"). No relation backs them: the declared column is their only storage.
    a_number_list_json = Column(_COLLECTION_JSON, nullable=True)
    a_bool_list_json = Column(_COLLECTION_JSON, nullable=True)
    a_number_list_array = Column(_NUMBER_ARRAY, nullable=True)
    a_bool_list_array = Column(_BOOL_ARRAY, nullable=True)


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
# that returned the parent instead of the child could not agree with the recorded
# decisions by accident.
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
        correlation: list[Any],
        correlate_targets: list[Any],
        member_field=None,
        hop_correlation: list[Any] | None = None,
    ):
        self.description = description
        self.correlation = correlation
        # Entities the subquery must correlate against explicitly: SQLAlchemy's
        # auto-correlation only reaches the immediate enclosing SELECT, so an
        # outer-resource reference inside a depth-2 lambda subquery would
        # otherwise pull the resource table into the inner FROM as a cartesian
        # product (silently comparing against EVERY resource row).
        self.correlate_targets = correlate_targets
        # For plain `in` membership over a chained string list (relation/in/to-one-chain).
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
    # The root resource may be any number of lambda scopes up
    # (collection/exists/outer-collection-inside-lambda plans a tags exists INSIDE the
    # categories lambda).
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
# outer resource columns (collection/exists/nested-with-outer-attribute), so both
# entities correlate.
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
    the helper: every chained corpus case over ``mainCategory`` is compared with the
    PDP's recorded decisions through this call.
    The ``size()`` chains are the exception: ``mainCategory.subCategories`` is
    declared in :data:`COLLECTION_COLUMNS`, and a NULL column is what makes an
    absent parent UNKNOWN there.
    """
    return require_hops(expr, rel.hop_correlation, rel.correlate_targets)


def _require_relation(op: str, coll: Any) -> _Relation:
    if not isinstance(coll, _Relation):
        raise UnsupportedPlanError(
            f"{op} over unsupported collection operand: {coll!r}"
        )
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
    return OPERATOR_FNS["size"](target, None)


def _has_intersection_fn(mapped: Any, values: Any):
    if isinstance(mapped, list):
        mapped, values = values, mapped
    if isinstance(values, list) and not values:
        rel = mapped if isinstance(mapped, _Relation) else mapped[1]
        return _require_hops(rel, false())
    if isinstance(mapped, _Relation):
        if mapped.member_field is None:
            raise UnsupportedPlanError(
                f"hasIntersection over relation without member field: {mapped!r}"
            )
        return _require_hops(
            mapped,
            _exists_where(mapped, _scalar_membership(mapped.member_field, values)),
        )

    # hasIntersection(map(coll, x), list): map errors on any erroring element
    # (no absorption), so the error guard comes FIRST.
    if not (isinstance(mapped, tuple) and mapped[0] == "map"):
        raise UnsupportedPlanError(
            f"hasIntersection over unsupported operand: {mapped!r}"
        )
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
    # The adapter's own lowering, not a copy of it: it keeps a null member as
    # `IS NULL` and drops a member the column's type cannot equal, which a
    # store would otherwise convert ('5' = 5 on SQLite) where CEL says false.
    return OPERATOR_FNS["in"](column, values)


def _relation_membership(relation: _Relation, value: Any):
    if isinstance(value, (list, dict)):
        raise UnsupportedPlanError(
            "Membership with a structured element has no scalar SQL lowering"
        )
    if relation.member_field is None:
        raise UnsupportedPlanError(
            f"in over relation without member field: {relation!r}"
        )
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

# How the collections are STORED, read by `size()` and `index` alone (#227). The three with a
# relation keep their marker in ATTR_MAP too, because every collection macro still reads the
# relation: a declaration answers only the questions a collection's own storage decides. The
# harness stores exactly the list `check()` is sent, so the ordered copy and the relation cannot
# disagree about a row.
#
# `mainCategory.subCategories` is the one that proves "absent is not empty": a resource with no
# category sends no `mainCategory` at all, so its column is NULL and `size() >= 0` must still
# exclude it (`relation/size/non-negative-to-one-chain`). `tags` proves the other half, since
# every seed carries the attribute and 14 carry it empty.
COLLECTION_COLUMNS = {
    "request.resource.attr.tags": CollectionColumn(AdvResource.tags_json, "json"),
    "request.resource.attr.tagNames": CollectionColumn(
        AdvResource.tag_names_json, "json"
    ),
    "request.resource.attr.mainCategory.subCategories": CollectionColumn(
        AdvResource.main_sub_categories_json, "json"
    ),
    "request.resource.attr.aNumberList": CollectionColumn(
        AdvResource.a_number_list_json, "json"
    ),
    "request.resource.attr.aBoolList": CollectionColumn(
        AdvResource.a_bool_list_json, "json"
    ),
}

#: The same five, declared as PostgreSQL arrays. The corpus classifies each action against one
#: mapping, so the translator unit test and the SQLite harness use :data:`COLLECTION_COLUMNS`;
#: this one is executed by the PostgreSQL leg of the harness alone.
PG_ARRAY_COLLECTION_COLUMNS = {
    "request.resource.attr.tags": CollectionColumn(AdvResource.tags_array, "pgArray"),
    "request.resource.attr.tagNames": CollectionColumn(
        AdvResource.tag_names_array, "pgArray"
    ),
    "request.resource.attr.mainCategory.subCategories": CollectionColumn(
        AdvResource.main_sub_categories_array, "pgArray"
    ),
    "request.resource.attr.aNumberList": CollectionColumn(
        AdvResource.a_number_list_array, "pgArray"
    ),
    "request.resource.attr.aBoolList": CollectionColumn(
        AdvResource.a_bool_list_array, "pgArray"
    ),
}


def reads_declared_collection(case: dict[str, Any]) -> bool:
    """Whether a golden case's plan reads a declared collection's storage.

    That is ``size()`` or ``index`` over any declared collection, and ``in`` or
    ``hasIntersection`` over one :data:`ATTR_MAP` does not map -- the attributes whose membership
    the adapter answers from the declaration rather than from a relation override.

    Read off the recorded plan rather than listed, so a case added to the corpus over one of
    these attributes joins the PostgreSQL leg without anyone remembering to add it.
    """

    def walk(node: Any) -> bool:
        if not isinstance(node, dict):
            return False
        expression = node.get("expression")
        if expression is None:
            return False
        operands = expression["operands"]
        if (
            expression["operator"] in ("size", "index")
            and operands
            and operands[0].get("variable") in COLLECTION_COLUMNS
        ):
            return True
        if expression["operator"] in ("in", "hasIntersection") and any(
            operand.get("variable") in COLLECTION_COLUMNS
            and operand.get("variable") not in ATTR_MAP
            for operand in operands
        ):
            return True
        return any(walk(operand) for operand in operands)

    return walk((case["plan"] or {}).get("condition", {}))


# `owner` and `coOwner` alias columns that `aOptionalString` and `scope` also map,
# under the OTHER null convention: `resources.json` sends a real null attribute for
# them rather than omitting it. Declaring that here is what makes the equality family
# definite for these two attributes (cerbos/query-plan-adapters#308).
# `aOptionalString` itself is omitted when NULL, so `== null` against it is a CEL
# missing-attribute error, not a match, and is declared that way (#302).
ATTRIBUTE_NULL_REPRESENTATION = {
    "request.resource.attr.aOptionalString": "omitted",
    "tagName": "explicit",
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
    # The primary key, reached as `request.resource.id` rather than through `attr` (the
    # `identifier/*` cases). An adapter that resolves references by stripping a
    # `request.resource.attr.` prefix never sees this name.
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
    "request.resource.attr.updatedAt": AdvResource.updated_at,
    # obj.inner is not a real nested column — mirrors aString, the same trick
    # the spring-data and prisma reference harnesses use for the
    # comparison/equals/nested-map-member case.
    "request.resource.attr.obj.inner": AdvResource.a_string,
    # The corpus's one REAL to-one chain (the `relation/*` cases). This adapter has no
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
    "tagName": AdvTag.name,
    "t.id": AdvTag.tag_id,
    "t.name": AdvTag.name,
    "request.resource.attr.categories": CATEGORIES,
    "c": CATEGORIES,
    # The category's own name, read inside the categories lambda. Only
    # relation/or/two-hops-or-collection-exists reaches it — every other categories probe
    # dots straight through to subCategories — so it is mapped here rather than alongside
    # them.
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


# -- rendering an emitted Select --------------------------------------------


def dialect(name: str):
    from sqlalchemy.dialects import postgresql, sqlite

    return {"sqlite": sqlite.dialect(), "postgresql": postgresql.dialect()}[name]


def render(query, dialect_name: str) -> tuple[str, dict[str, Any]]:
    """Compile ``query`` for one dialect, as ``(statement, parameters)``.

    The WHOLE ``Select`` is compiled, never the bare ``WHERE`` clause, because correlation is
    only observable inside the enclosing SELECT. Whitespace is collapsed because SQLAlchemy's
    compiler breaks clauses across lines, and every value is a bind parameter.
    """
    compiled = query.compile(
        dialect=dialect(dialect_name), compile_kwargs={"render_postcompile": True}
    )
    return " ".join(str(compiled).split()), dict(compiled.params)
