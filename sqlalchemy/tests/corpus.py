# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Corpus loading, schema and mapping shared by both of this adapter's corpus suites.

Both suites must use the same mapping: the unit test pins the SQL, the adversarial suite
proves that SQL returns the rows the PDP allows. Seeds and the oracle stay in the harness.
Each adapter keeps its own copy of this loader on purpose (ADR 0007). Test-only.
"""

import json
import math
import os
from collections.abc import Sequence
from datetime import datetime
from importlib.metadata import version as sqlalchemy_version
from typing import Any

from cerbos.response.v1 import response_pb2
from cerbos.sdk.model import PlanResourcesResponse
from cerbos_image import CONFORMANCE_DIR
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

from cerbos_sqlalchemy import CollectionColumn, require_hops
from cerbos_sqlalchemy.query import OPERATOR_FNS

ADAPTER = "sqlalchemy"

WIRE_FIXTURES_DIR = os.path.join(CONFORMANCE_DIR, "wire-fixtures")

#: This adapter's golden expectations. Never under ``conformance/`` (ADR 0007).
GOLDEN_FILE = os.path.realpath(
    os.path.join(os.path.dirname(__file__), "..", "golden", "expectations.json")
)

#: The command that rewrites :data:`GOLDEN_FILE`; recorded in the file itself.
GOLDEN_REGENERATE_COMMAND = "pdm run golden:update"

#: The installed SQLAlchemy major. CI runs both 1.4 and 2.x.
INSTALLED_SQLALCHEMY_MAJOR = (
    "1.4" if sqlalchemy_version("sqlalchemy").startswith("1.4") else "2.x"
)

#: The major :data:`GOLDEN_FILE` is generated under. The two majors compile some trees
#: differently (e.g. 2.x adds SQLite's ``+ 0.0`` float division), so the other major asserts
#: a pinned divergence set instead of the bytes.
GOLDEN_SQLALCHEMY_MAJOR = "2.x"


def read_corpus_json(name: str) -> Any:
    with open(os.path.join(CONFORMANCE_DIR, name), encoding="utf-8") as f:
        return json.load(f)


# -- actions.json -----------------------------------------------------------
#
# Every group is parsed explicitly. A silently dropped group would remove its actions from
# every count at once and the suite would pass vacuously.


class ActionsFile:
    """``conformance/actions.json``, parsed group by group."""

    def __init__(self, raw: dict[str, Any]) -> None:
        self.adapters: list[str] = _require_list(raw, "adapters")
        self.conformance: list[str] = _require_list(raw, "conformance")
        self.adapter_unsupported: dict[str, list[dict[str, Any]]] = _require_dict(
            raw, "adapterUnsupported"
        )
        self.adapter_supported_expected: dict[str, list[dict[str, Any]]] = (
            _require_dict(raw, "adapterSupportedExpected")
        )
        self.expected_unsupported: list[dict[str, Any]] = _require_list(
            raw, "expectedUnsupported"
        )
        self.null_representation_omitted: list[dict[str, Any]] = _require_list(
            raw, "nullRepresentationOmitted"
        )
        self.known_divergences: list[dict[str, Any]] = _require_list(
            raw, "knownDivergences"
        )
        self.degenerate_oracles: dict[str, str] = _degenerate_oracles(
            _require_list(raw, "degenerateOracles")
        )

    def manifest_actions(self) -> set[str]:
        """Every action the corpus classifies, across every group."""
        return (
            set(self.conformance)
            | {entry["action"] for entry in self.expected_unsupported}
            | {entry["action"] for entry in self.null_representation_omitted}
            | {entry["action"] for entry in self.known_divergences}
        )

    def skipped_divergences(self, adapter: str) -> set[str]:
        return {
            entry["action"]
            for entry in self.known_divergences
            if adapter in entry["adapters"]
        }


def _require_list(raw: dict[str, Any], key: str) -> list[Any]:
    value = raw.get(key)
    if not isinstance(value, list):
        raise AssertionError(f"actions.json {key} must be an array")
    return value


def _require_dict(raw: dict[str, Any], key: str) -> dict[str, Any]:
    value = raw.get(key)
    if not isinstance(value, dict):
        raise AssertionError(f"actions.json {key} must be an object")
    return value


DEGENERATE_ORACLE_SHAPES = ("empty", "total")


def _degenerate_oracles(entries: list[Any]) -> dict[str, str]:
    """``degenerateOracles`` as ``action -> "empty" | "total"``.

    Any other value would silently exempt an action from the degeneracy sweep, so it fails.
    """
    oracles: dict[str, str] = {}
    for index, entry in enumerate(entries):
        label = f"actions.json degenerateOracles[{index}]"
        if not isinstance(entry, dict) or not isinstance(entry.get("action"), str):
            raise AssertionError(f"{label} must be an object with a string action")
        if entry.get("oracle") not in DEGENERATE_ORACLE_SHAPES:
            raise AssertionError(
                f"{label} ({entry['action']}) oracle must be one of "
                f"{DEGENERATE_ORACLE_SHAPES}, got {entry.get('oracle')!r}"
            )
        if entry["action"] in oracles:
            raise AssertionError(f"{label} repeats {entry['action']}")
        oracles[entry["action"]] = entry["oracle"]
    return oracles


def parse_actions_file(raw: dict[str, Any]) -> ActionsFile:
    return ActionsFile(raw)


def require_message(label: str, message: Any) -> str:
    """The substring this adapter's error must contain, or a loud failure.

    Without it, any unrelated error would satisfy the throw suite (#326).
    """
    if not isinstance(message, str) or not message:
        raise AssertionError(
            f"actions.json pins no throw message for {label}: the throw suite "
            "would accept a failure for any reason"
        )
    return message


class Classification:
    """How the corpus classifies every action for this adapter.

    ``nullRepresentationOmitted`` stays separate so the harness's one-outcome-per-action
    guard works; read it with :func:`null_representation_throws`.
    """

    def __init__(
        self,
        oracle_actions: list[str],
        throwing_actions: list[tuple[str, str]],
        supported_expected: set[str],
    ) -> None:
        self.oracle_actions = oracle_actions
        #: ``(action, pinned message substring)``, sorted.
        self.throwing_actions = throwing_actions
        self.supported_expected = supported_expected


def classify_actions_for_adapter(manifest: ActionsFile, adapter: str) -> Classification:
    unsupported = manifest.adapter_unsupported.get(adapter, [])
    unsupported_actions = {entry["action"] for entry in unsupported}
    supported_expected = {
        entry["action"]
        for entry in manifest.adapter_supported_expected.get(adapter, [])
    }
    oracle_actions = [
        action for action in manifest.conformance if action not in unsupported_actions
    ] + sorted(supported_expected)
    throwing_actions = sorted(
        [
            (
                entry["action"],
                require_message(
                    f"adapterUnsupported.{adapter}.{entry['action']}",
                    entry.get("message"),
                ),
            )
            for entry in unsupported
        ]
        + [
            (
                entry["action"],
                require_message(
                    f"expectedUnsupported.{entry['action']}.messages.{adapter}",
                    entry.get("messages", {}).get(adapter),
                ),
            )
            for entry in manifest.expected_unsupported
            if entry["action"] not in supported_expected
        ]
    )
    return Classification(oracle_actions, throwing_actions, supported_expected)


def null_representation_throws(
    manifest: ActionsFile, adapter: str
) -> list[tuple[str, str, str]]:
    """The ``nullRepresentationOmitted`` actions as ``(action, reason, message)``.

    Every adapter must reject these: both NULL conventions look alike on the wire (#302).
    """
    return [
        (
            entry["action"],
            entry["reason"],
            require_message(
                f"nullRepresentationOmitted.{entry['action']}.messages.{adapter}",
                entry.get("messages", {}).get(adapter),
            ),
        )
        for entry in manifest.null_representation_omitted
    ]


# -- the golden wire fixtures -----------------------------------------------

#: The value substituted for the fixtures' ``__NOW_MINUS_24H__`` placeholder.
#: It must keep nanosecond precision like the real PDP output: that precision is why this
#: adapter refuses ``ts-window`` and ``ts-vf``, and a millisecond value would translate.
PLANNED_AT = "2026-08-11T09:13:39.123456789Z"

_NOW_MINUS_24H = "__NOW_MINUS_24H__"


def wire_fixture_actions() -> list[str]:
    """Every action the corpus has a golden wire fixture for, sorted."""
    return sorted(
        name[: -len(".json")]
        for name in os.listdir(WIRE_FIXTURES_DIR)
        if name.endswith(".json")
    )


def _substitute_planned_at(node: Any, planned_at: str) -> Any:
    if isinstance(node, dict):
        return {k: _substitute_planned_at(v, planned_at) for k, v in node.items()}
    if isinstance(node, list):
        return [_substitute_planned_at(v, planned_at) for v in node]
    return planned_at if node == _NOW_MINUS_24H else node


def _fixture_response_dict(action: str, planned_at: str) -> dict[str, Any]:
    with open(os.path.join(WIRE_FIXTURES_DIR, f"{action}.json"), encoding="utf-8") as f:
        fixture = json.load(f)
    return {
        "requestId": "",
        "action": fixture["action"],
        "resourceKind": fixture["resourceKind"],
        "policyVersion": "default",
        "filter": _substitute_planned_at(fixture["filter"], planned_at),
    }


def plan_from_wire_fixture(
    action: str, planned_at: str = PLANNED_AT
) -> PlanResourcesResponse:
    """The pinned PDP's plan for ``action``, decoded as the HTTP SDK client decodes it.

    Real planner output, not a hand-built plan (ADR 0006).
    """
    return PlanResourcesResponse.from_dict(_fixture_response_dict(action, planned_at))


def grpc_plan_from_wire_fixture(
    action: str, planned_at: str = PLANNED_AT
) -> response_pb2.PlanResourcesResponse:
    """The same fixture decoded into the protobuf response the gRPC client returns.

    ``get_query`` reads this form through ``MessageToDict``, a second decoding path.
    """
    return ParseDict(
        _fixture_response_dict(action, planned_at),
        response_pb2.PlanResourcesResponse(),
    )


# -- the golden expectations ------------------------------------------------


#: The reserved key an entry may carry alongside its expectation; never compared.
NOTE_KEY = "note"


def read_golden_expectations() -> dict[str, dict[str, Any]]:
    """The golden expectations, keyed by action, each split into ``note`` and the value.

    The ``adapter`` header is checked so another adapter's file cannot be read by mistake.
    """
    with open(GOLDEN_FILE, encoding="utf-8") as f:
        contents = json.load(f)
    if contents.get("adapter") != ADAPTER:
        raise AssertionError(
            f'{GOLDEN_FILE} declares adapter "{contents.get("adapter")}", not "{ADAPTER}"'
        )
    if contents.get("sqlalchemy") != GOLDEN_SQLALCHEMY_MAJOR:
        raise AssertionError(
            f'{GOLDEN_FILE} declares SQLAlchemy "{contents.get("sqlalchemy")}", not '
            f'"{GOLDEN_SQLALCHEMY_MAJOR}"'
        )
    recorded = {}
    for action, entry in contents["expectations"].items():
        expectation = {k: v for k, v in entry.items() if k != NOTE_KEY}
        recorded[action] = {
            "note": entry.get(NOTE_KEY),
            "expectation": expectation,
        }
    return recorded


def write_golden_expectations(expectations: dict[str, dict[str, Any]]) -> None:
    """Rewrite the golden expectations, keeping every existing ``note``.

    Runs only under ``pdm run golden:update``; CI never does. Output is sorted so the diff
    is reviewable. A missing file is allowed here only, to bootstrap one. Running under the
    other SQLAlchemy major fails: it would pass compiler changes off as translation ones.
    """
    if INSTALLED_SQLALCHEMY_MAJOR != GOLDEN_SQLALCHEMY_MAJOR:
        raise AssertionError(
            f"{GOLDEN_FILE} is generated under SQLAlchemy {GOLDEN_SQLALCHEMY_MAJOR}, and "
            f"{sqlalchemy_version('sqlalchemy')} is installed. Regenerating here would "
            "rewrite every entry the two compilers render differently."
        )
    # Skip header validation: the old file may carry an outdated header, and its notes
    # must still be kept.
    notes: dict[str, str] = {}
    if os.path.exists(GOLDEN_FILE):
        with open(GOLDEN_FILE, encoding="utf-8") as f:
            for action, entry in json.load(f).get("expectations", {}).items():
                if NOTE_KEY in entry:
                    notes[action] = entry[NOTE_KEY]
    body = {}
    for action in sorted(expectations):
        entry = dict(expectations[action])
        note = notes.get(action)
        body[action] = entry if note is None else {NOTE_KEY: note, **entry}
    os.makedirs(os.path.dirname(GOLDEN_FILE), exist_ok=True)
    with open(GOLDEN_FILE, "w", encoding="utf-8") as f:
        json.dump(
            {
                "adapter": ADAPTER,
                "sqlalchemy": GOLDEN_SQLALCHEMY_MAJOR,
                "regenerate": GOLDEN_REGENERATE_COMMAND,
                "expectations": body,
            },
            f,
            indent=2,
        )
        f.write("\n")


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
        # The column compared by `in` over a string list (w1-in-chain).
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
    # The resource may be several lambdas up (w2-outer-relation nests tags in categories).
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
# (outer-attr-depth2), so the resource correlates too.
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
    """Make ``expr`` UNKNOWN unless every intermediate to-one hop exists.

    Calls the shipped ``require_hops`` so the chained corpus actions test it.
    ``size()`` chains don't need it: a NULL declared column already yields UNKNOWN.
    """
    return require_hops(expr, rel.hop_correlation, rel.correlate_targets)


def _require_relation(op: str, coll: Any) -> _Relation:
    if not isinstance(coll, _Relation):
        raise ValueError(f"{op} over unsupported collection operand: {coll!r}")
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
            raise ValueError(
                f"hasIntersection over relation without member field: {mapped!r}"
            )
        return _require_hops(
            mapped,
            _exists_where(mapped, _scalar_membership(mapped.member_field, values)),
        )

    # map never absorbs errors, so the error check comes first.
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
    # Reuse the adapter's lowering: it drops members of the wrong type, which
    # SQLite would otherwise coerce ('5' = 5).
    return OPERATOR_FNS["in"](column, values)


def _relation_membership(relation: _Relation, value: Any):
    if isinstance(value, (list, dict)):
        raise ValueError(
            "Membership with a structured element has no scalar SQL lowering"
        )
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
# (w1-size-nonneg-chain); `tags` covers the empty-but-present case.
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


def reads_declared_collection(action: str) -> bool:
    """Whether ``action``'s plan reads a declared collection's storage.

    True for ``size()``/``index`` over one, or ``in``/``hasIntersection`` over one not in
    :data:`ATTR_MAP`. Read from the fixture so new actions join the PostgreSQL leg.
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

    fixture = _fixture_response_dict(action, PLANNED_AT)["filter"]
    return walk(fixture.get("condition", {}))


# `owner` and `coOwner` reuse columns under the other null convention: the oracle
# sends an explicit null instead of omitting the attribute (#308).
ATTRIBUTE_NULL_REPRESENTATION = {
    "tagName": "explicit",
    "request.resource.attr.owner": "explicit",
    "request.resource.attr.coOwner": "explicit",
}


def _parent_scalar(column):
    """One scalar of the to-one `parent`, as a correlated scalar subquery.

    A missing parent gives NULL, which stays excluded under negation too, so no
    ``require_hops`` guard is needed (#375).
    """
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
    # Not under `attr` (the `id-*` actions).
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
    # Not a real nested column; aliases aString for the p-struct probe.
    "request.resource.attr.obj.inner": AdvResource.a_string,
    # The real to-one chain (`rel-*` actions), as correlated scalar subqueries.
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
    # Only rel-hop2-or-exists reads the category's own name.
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

#: The dialects the golden expectations pin. SQLite is what the adversarial suite runs;
#: PostgreSQL is where most of the adapter's dialect-specific logic shows (NaN ordering,
#: ``CAST`` rounding). They differ, e.g. SQLite needs ``= 1`` on a boolean ``CASE``.
GOLDEN_DIALECTS = ("sqlite", "postgresql")


def dialect(name: str):
    # Lazy import: only the rendering helpers need these dialects.
    from sqlalchemy.dialects import postgresql, sqlite

    return {"sqlite": sqlite.dialect(), "postgresql": postgresql.dialect()}[name]


def json_parameter(label: str, value: Any) -> Any:
    """One bound parameter as the golden file records it.

    A ``datetime`` becomes ISO-8601 text. A non-finite float fails: the adapter folds those
    before binding, so one here means the translation changed.
    """
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise AssertionError(
                f"{label} binds the non-finite number {value!r}; the golden file cannot "
                "record it faithfully"
            )
        return value
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)):
        return [json_parameter(f"{label}[{i}]", item) for i, item in enumerate(value)]
    raise AssertionError(
        f"{label} binds a {type(value).__name__}, which the golden file has no encoding for"
    )


def render(query, dialect_name: str) -> tuple[str, dict[str, Any]]:
    """Compile ``query`` for one dialect, as ``(statement, parameters)``.

    Compiles the whole ``Select``: a bare WHERE clause would render correlated subqueries as
    cross joins. ``render_postcompile`` expands ``IN`` to one placeholder per value.
    Collapsing whitespace is safe because every value is a bind parameter.
    """
    compiled = query.compile(
        dialect=dialect(dialect_name), compile_kwargs={"render_postcompile": True}
    )
    return " ".join(str(compiled).split()), dict(compiled.params)


def statement_preamble() -> str:
    """The ``SELECT ... FROM`` every emitted statement starts with.

    Golden entries record only what follows ``WHERE``; the tests assert this prefix holds.
    """
    return " ".join(str(select(AdvResource).compile(dialect=dialect("sqlite"))).split())


def where_clause(statement: str) -> str | None:
    """The part of a rendered statement after ``WHERE``, or ``None`` when there is none."""
    preamble = statement_preamble()
    if statement == preamble:
        return None
    marker = preamble + " WHERE "
    if not statement.startswith(marker):
        raise AssertionError(
            f"emitted statement does not start with the expected SELECT: {statement[:200]}"
        )
    return statement[len(marker) :]


def statement_from(where: str | None) -> str:
    """The inverse of :func:`where_clause`."""
    preamble = statement_preamble()
    return preamble if where is None else f"{preamble} WHERE {where}"


def declared_columns() -> Sequence[str]:
    """Every ``table.column`` name the corpus schema declares."""
    return sorted(
        f"{table.name}.{column.name}"
        for table in AdvBase.metadata.tables.values()
        for column in table.columns
    )
