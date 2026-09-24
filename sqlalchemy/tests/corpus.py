# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""The shared ``../conformance/`` corpus, schema and mapping for both corpus suites.

Both suites must use the same mapping: the harness replays recorded plans against real
stores, and the translator unit test asks what a store cannot. Each adapter keeps its own
copy of this file on purpose (ADR 0007). Test-only.
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
    """``now() - duration("24h")`` as the PDP folds it: RFC 3339 in nanoseconds."""
    ns = time.time_ns() - 24 * 3600 * 10**9
    seconds, fraction = divmod(ns, 10**9)
    # Sub-microsecond precision is why this adapter refuses these plans. Force it, so a clock
    # landing on a whole microsecond cannot hide that.
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
    """The recorded plan, decoded as the HTTP SDK client decodes it."""
    return PlanResourcesResponse.from_dict(_response_dict(case, planned_at))


def grpc_plan_from_golden(
    case: dict[str, Any], planned_at: str | None = None
) -> response_pb2.PlanResourcesResponse:
    """The same plan as the gRPC client's protobuf response."""
    # Tests the protobuf decoding path only. JSON has already lost a `-0`'s sign, so this
    # is not a real gRPC frame.
    return ParseDict(
        _response_dict(case, planned_at), response_pb2.PlanResourcesResponse()
    )


# -- schema -----------------------------------------------------------------
# Dedicated tables, so hostile seeds (NULL elements, duplicate names, LIKE
# metacharacters) are all representable.

AdvBase = declarative_base()

# Ordered copies of the collections for `collection_columns` (#227). `none_as_null` makes an
# absent collection SQL NULL, not the JSON document `null`.
_COLLECTION_JSON = JSON(none_as_null=True).with_variant(
    JSONB(none_as_null=True), "postgresql"
)
# The same collections as PostgreSQL arrays, used only by the PostgreSQL leg. SQLite has no
# array type, so there the variant is unused JSON.
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
    # Number and boolean lists prove an element's JSON type survives comparison.
    # No relation backs them; the column is their only storage.
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


# The corpus's one real to-one relation (ADR 0005). Unlike `obj.inner`, `parent` and
# `parent.inner` are separate rows. The unique foreign key makes it to-one.
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


# -- relation markers and operator overrides --------------------------------
# ATTR_MAP points relation attributes at markers; the overrides turn collection
# macros over them into correlated subqueries. An element whose body is SQL NULL
# is a CEL error: exists/all absorb it only given a true/false witness, and
# exists_one/map/filter never do.


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
        # Auto-correlation only reaches the immediate enclosing SELECT. Without
        # explicit targets, a nested subquery would cross-join every resource row.
        self.correlate_targets = correlate_targets
        # The column compared by `in` over a string list (relation/in/to-one-chain).
        self.member_field = member_field
        # Predicates for the intermediate to-one hops. An absent parent is a CEL
        # error (deny), but a plain subquery sees it as empty, so `all` and
        # `!exists` would over-grant (#309). `require_hops` uses these to tell
        # the two apart.
        self.hop_correlation = hop_correlation or []

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return f"_Relation({self.description})"


TAGS = _Relation(
    "tags",
    [AdvTag.resource_id == AdvResource.id],
    # The resource may be several lambdas up: collection/exists/outer-collection-inside-lambda
    # nests tags in categories.
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
# Correlates to the enclosing category, but the body may read resource columns
# (collection/exists/nested-with-outer-attribute), so the resource correlates too.
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
# The same two-hop chain from the root. The category hop stays in the subquery's
# FROM; only the resource correlates.
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
    """Make ``expr`` UNKNOWN unless every intermediate to-one hop exists."""
    # Calls the shipped helper, so the chained corpus cases test it. `size()` chains
    # skip it: a NULL declared column already yields UNKNOWN.
    return require_hops(expr, rel.hop_correlation, rel.correlate_targets)


def _require_relation(op: str, coll: Any) -> _Relation:
    if not isinstance(coll, _Relation):
        raise UnsupportedPlanError(
            f"{op} over unsupported collection operand: {coll!r}"
        )
    return coll


def _exists_fn(coll: Any, body: Any):
    # True on any true witness, else NULL if any element errors, else false.
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
    # False on any false witness, else NULL if any element errors, else true.
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
    # Any erroring element makes it NULL, even beside a true witness.
    rel = _require_relation("exists_one", coll)
    return _require_hops(
        rel,
        case(
            (_exists_where(rel, body.is_(None)), null()),
            else_=(_count_subquery(rel, body) == 1),
        ),
    )


def _filter_fn(coll: Any, body: Any):
    # Deferred: consumed by the `size` override.
    return ("filter", _require_relation("filter", coll), body)


def _map_fn(coll: Any, projected: Any):
    # Deferred: consumed by the `hasIntersection` override.
    return ("map", _require_relation("map", coll), projected)


def _size_fn(target: Any, _: Any):
    if isinstance(target, _Relation):
        # size() never evaluates elements, so no error guard. An absent parent
        # must still be UNKNOWN, not 0 (#309).
        return _require_hops(target, _count_subquery(target))
    if isinstance(target, tuple) and target[0] == "filter":
        # filter never absorbs errors: any NULL body makes the count NULL.
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

    # map never absorbs errors, so the error check comes first.
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
    # Reuse the adapter's lowering: it drops members of the wrong type, which
    # SQLite would otherwise coerce ('5' = 5).
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
        # Rows with an empty chain are excluded, matching CEL's missing-attribute deny.
        return _relation_membership(column, value)
    if isinstance(value, _Relation):
        return _relation_membership(value, column)
    return _scalar_membership(column, value)


OPERATOR_OVERRIDES = {
    # Keep the body; the iterator variable is already resolved via ATTR_MAP.
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

# Collection storage, read only by `size()` and `index` (#227). Collections with a relation
# keep their ATTR_MAP marker too, since the macros still use the relation.
# `mainCategory.subCategories` is NULL when absent, so `size() >= 0` must exclude it
# (relation/size/non-negative-to-one-chain); `tags` covers the empty-but-present case.
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

#: The same five as PostgreSQL arrays. Only the harness's PostgreSQL leg uses this.
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
    """Whether a golden case's plan reads a declared collection's storage."""
    # `size()`/`index` over a declared collection, or `in`/`hasIntersection` over one not in
    # ATTR_MAP. Read off the plan, so a new corpus case joins the PostgreSQL leg unasked.

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


# `owner` and `coOwner` reuse columns under the other null convention: the corpus
# sends an explicit null instead of omitting the attribute (#308). `aOptionalString`
# is omitted when NULL, so `== null` against it is a CEL error, not a match (#302).
ATTRIBUTE_NULL_REPRESENTATION = {
    "request.resource.attr.aOptionalString": "omitted",
    "tagName": "explicit",
    "request.resource.attr.owner": "explicit",
    "request.resource.attr.coOwner": "explicit",
}


def _parent_scalar(column):
    """One scalar of the to-one `parent`, as a correlated scalar subquery."""
    # A missing parent gives NULL, which stays excluded under negation too, so no
    # `require_hops` guard is needed (#375).
    return (
        select(column)
        .where(AdvParent.resource_id == AdvResource.id)
        .correlate(AdvResource)
        .scalar_subquery()
    )


def _inner_scalar(column):
    """The same for `parent.inner`, looked up through the parent."""
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
    # Not under `attr` (the `identifier/*` cases).
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
    # Not a real nested column; aliases aString for comparison/equals/nested-map-member.
    "request.resource.attr.obj.inner": AdvResource.a_string,
    # The real to-one chain (`relation/*` cases), as correlated scalar subqueries.
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
    # Only relation/or/two-hops-or-collection-exists reads the category's own name.
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
    """Compile ``query`` for one dialect, as ``(statement, parameters)``."""
    # Compile the whole Select, not the WHERE clause: correlation only shows inside it.
    compiled = query.compile(
        dialect=dialect(dialect_name), compile_kwargs={"render_postcompile": True}
    )
    return " ".join(str(compiled).split()), dict(compiled.params)
