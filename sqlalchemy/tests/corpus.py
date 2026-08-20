"""The parts of the shared ``../conformance/`` corpus both of this adapter's suites read.

``test_adversarial_conformance.py`` plans against a real PDP and executes the translated
query against seeded SQLite rows; ``test_translator.py`` reads the same actions off the
golden wire fixtures and asserts nothing but the emitted SQL. They must agree on two
things or they prove less than they appear to:

- **the mapping.** The unit test pins the SQL this adapter emits for one attribute map and
  one set of operator overrides; the harness proves that same SQL returns the rows the PDP
  allows. Two copies that drifted would leave the pinned statements describing columns and
  correlated subqueries no harness ever executes, which is why the schema, ``ATTR_MAP``,
  ``OPERATOR_OVERRIDES`` and ``ATTRIBUTE_NULL_REPRESENTATION`` live here rather than in
  either suite.
- **the classification.** Which actions this adapter must refuse, and with which message, is
  a corpus decision (``adapterctl.json``), not a per-suite one.

What is deliberately NOT here: the seed rows, the derived fields and the ``check()`` oracle.
Only the harness consumes those, and its coverage guards assert set equality against the
corpus files, so they stay next to the code that reads them.

The code in this file is duplicated across adapters **on purpose** -- adapters share data,
not code, so that every adapter stays standalone. Do not extract it into ``conformance/``,
do not import another adapter's copy, and do not add a drift check between them. See
`ADR 0007 <../../docs/adr/0007-adapters-share-data-not-code.md>`_.

Test-only: it lives under ``tests/`` and never reaches the published package.
"""

import json
import math
import os
from datetime import datetime
from importlib.metadata import version as sqlalchemy_version
from typing import Any, Dict, List, Sequence, Set, Tuple, Union

from cerbos.response.v1 import response_pb2
from cerbos.sdk.model import PlanResourcesResponse
from cerbos_image import CONFORMANCE_DIR
from google.protobuf.json_format import ParseDict

from cerbos_sqlalchemy import require_hops
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
from sqlalchemy.orm import declarative_base

ADAPTER = "sqlalchemy"

WIRE_FIXTURES_DIR = os.path.join(CONFORMANCE_DIR, "wire-fixtures")

#: The golden expectations this adapter owns. Never under ``conformance/`` -- see ADR 0007.
GOLDEN_FILE = os.path.realpath(
    os.path.join(os.path.dirname(__file__), "..", "golden", "expectations.json")
)

#: The command that rewrites :data:`GOLDEN_FILE`. Documentation that travels with the data.
GOLDEN_REGENERATE_COMMAND = "pdm run golden:update"

#: The SQLAlchemy major line installed right now. ``pyproject.toml`` declares ``>=1.4`` and
#: CI runs both, so this is a real dimension rather than a formality.
INSTALLED_SQLALCHEMY_MAJOR = (
    "1.4" if sqlalchemy_version("sqlalchemy").startswith("1.4") else "2.x"
)

#: The major :data:`GOLDEN_FILE` is generated under.
#:
#: The adapter emits a SQLAlchemy expression tree; SQL text is that tree plus SQLAlchemy's
#: own compiler, and the two majors compile some trees differently — 2.x parenthesises a
#: concatenation used as a comparison operand, and adds SQLite's ``+ 0.0`` float-division
#: coercion. Neither is a translation difference, but both are byte differences, so the asset
#: has to say which compiler wrote it. The other major asserts a pinned divergence set
#: instead of the bytes (see ``test_translator.py``).
GOLDEN_SQLALCHEMY_MAJOR = "2.x"


def read_corpus_json(name: str) -> Any:
    with open(os.path.join(CONFORMANCE_DIR, name), encoding="utf-8") as f:
        return json.load(f)


# -- v1 control plane -------------------------------------------------------


class ControlPlane:
    """The catalog plus this adapter's direct outcome manifest."""

    def __init__(self, catalog: Dict[str, Any], manifest: Dict[str, Any]) -> None:
        if catalog.get("schemaVersion") != 1 or manifest.get("schemaVersion") != 1:
            raise AssertionError("control-plane files must use schemaVersion 1")
        if manifest.get("adapter") != ADAPTER:
            raise AssertionError(
                f'adapterctl.json declares {manifest.get("adapter")!r}, not {ADAPTER!r}'
            )
        actions = catalog.get("actions")
        outcomes = manifest.get("outcomes")
        if not isinstance(actions, list) or not isinstance(outcomes, dict):
            raise AssertionError("catalog actions and manifest outcomes must be objects")
        self.actions: List[Dict[str, Any]] = actions
        names = [entry.get("name") for entry in actions]
        if not all(isinstance(name, str) and name for name in names):
            raise AssertionError("catalog action names must be non-empty strings")
        if len(set(names)) != len(names):
            raise AssertionError("catalog action names must be unique")
        self.selected_action = os.environ.get("ADAPTERCTL_ACTION", "").strip()
        if self.selected_action and self.selected_action not in names:
            raise AssertionError(
                f"ADAPTERCTL_ACTION names unknown catalog action {self.selected_action!r}"
            )
        if not self.selected_action and set(names) != set(outcomes):
            raise AssertionError("adapterctl outcomes must cover the catalog exactly")
        self.outcomes = dict(outcomes)
        selected_outcome = self.outcomes.get(self.selected_action)
        if self.selected_action and (
            selected_outcome is None
            or selected_outcome.get("status") == "unassessed"
        ):
            self.outcomes[self.selected_action] = {"status": "matched"}

    def selected(self, action: str) -> bool:
        return not self.selected_action or self.selected_action == action

    def manifest_actions(self) -> Set[str]:
        return {
            entry["name"]
            for entry in self.actions
            if self.selected(entry["name"])
        }

    def upstream_blocked_actions(self, adapter: str) -> Set[str]:
        if adapter != ADAPTER:
            raise AssertionError(f"control plane loaded for {ADAPTER}, not {adapter}")
        return {
            action
            for action, outcome in self.outcomes.items()
            if self.selected(action) and outcome.get("status") == "upstream-blocked"
        }

    def oracle_expectations(self) -> Dict[str, Dict[str, Any]]:
        return {
            entry["name"]: entry["oracleExpectation"]
            for entry in self.actions
            if self.selected(entry["name"])
        }


def load_control_plane(adapter: str = ADAPTER) -> ControlPlane:
    if adapter != ADAPTER:
        raise AssertionError(f"control plane loaded for {ADAPTER}, not {adapter}")
    root = os.path.dirname(CONFORMANCE_DIR)
    with open(os.path.join(CONFORMANCE_DIR, "catalog.json"), encoding="utf-8") as f:
        catalog = json.load(f)
    with open(os.path.join(root, adapter, "adapterctl.json"), encoding="utf-8") as f:
        manifest = json.load(f)
    return ControlPlane(catalog, manifest)


def require_message(label: str, message: Any) -> str:
    """The non-empty refusal substring pinned by the adapter manifest."""
    if not isinstance(message, str) or not message:
        raise AssertionError(
            f"adapterctl.json pins no throw message for {label}: the throw suite "
            "would accept a failure for any reason"
        )
    return message


class Classification:
    def __init__(
        self,
        oracle_actions: List[str],
        throwing_actions: List[Tuple[str, str]],
    ) -> None:
        self.oracle_actions = oracle_actions
        self.throwing_actions = throwing_actions


def classify_actions_for_adapter(
    control_plane: ControlPlane, adapter: str
) -> Classification:
    if adapter != ADAPTER:
        raise AssertionError(f"control plane loaded for {ADAPTER}, not {adapter}")
    oracle_actions: List[str] = []
    throwing_actions: List[Tuple[str, str]] = []
    for entry in control_plane.actions:
        action = entry["name"]
        if not control_plane.selected(action):
            continue
        outcome = control_plane.outcomes[action]
        status = outcome.get("status")
        if status == "matched":
            oracle_actions.append(action)
        elif status == "rejected":
            reason = outcome.get("reason")
            if not isinstance(reason, str) or not reason:
                raise AssertionError(
                    f"adapterctl.json rejected outcome {action!r} has no reason"
                )
            message = require_message(
                f"outcomes.{action}", outcome.get("message")
            )
            if action != "null-eq-missing":
                throwing_actions.append((action, message))
        elif status == "upstream-blocked":
            reason = outcome.get("reason")
            if not isinstance(reason, str) or not reason:
                raise AssertionError(
                    f"adapterctl.json upstream-blocked outcome {action!r} has no reason"
                )
        elif status == "unassessed":
            raise AssertionError(f"adapterctl.json outcome {action!r} is unassessed")
        else:
            raise AssertionError(
                f"adapterctl.json outcome {action!r} has unknown status {status!r}"
            )
    return Classification(oracle_actions, sorted(throwing_actions))


def representation_dependent_rejections(
    control_plane: ControlPlane, adapter: str
) -> List[Tuple[str, str, str]]:
    if adapter != ADAPTER:
        raise AssertionError(f"control plane loaded for {ADAPTER}, not {adapter}")
    action = "null-eq-missing"
    if not control_plane.selected(action):
        return []
    outcome = control_plane.outcomes[action]
    if outcome.get("status") != "rejected":
        raise AssertionError(f"adapterctl.json must reject {action}")
    reason = outcome.get("reason")
    if not isinstance(reason, str) or not reason:
        raise AssertionError(f"adapterctl.json rejected outcome {action!r} has no reason")
    return [(action, reason, require_message(f"outcomes.{action}", outcome.get("message")))]


# -- the golden wire fixtures -----------------------------------------------

#: The instant ``regenerate-wire-fixtures.sh`` substitutes for the one operand it cannot pin.
#:
#: ``ts-window`` and ``ts-vf`` compare against ``now() - duration("24h")``, which the planner
#: folds to a literal timestamp: a different value on every capture, so the script rewrites it
#: to ``__NOW_MINUS_24H__`` to keep the drift check deterministic. Reading the fixture back
#: therefore means choosing a value, and the choice is load-bearing HERE -- the PDP emits
#: NANOSECOND precision, which is exactly why this adapter refuses both actions, and a tidy
#: millisecond substitution would translate cleanly and quietly contradict ``adapterctl.json``.
PLANNED_AT = "2026-08-11T09:13:39.123456789Z"

_NOW_MINUS_24H = "__NOW_MINUS_24H__"


def wire_fixture_actions() -> List[str]:
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


def _fixture_response_dict(action: str, planned_at: str) -> Dict[str, Any]:
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
    """The plan the pinned PDP produced for ``action``, as the HTTP SDK hands it to a caller.

    The fixture IS the PDP's HTTP response body, so the decoding here is the one
    ``cerbos.sdk.client.CerbosClient`` performs -- ``PlanResourcesResponse.from_dict``, whose
    ``decode_operand`` turns each ``{expression|variable|value}`` node into the model class
    the adapter walks. It is deliberately not a hand-built plan: a plan somebody typed is a
    belief about what the planner emits, and this repository keeps fixtures precisely because
    that belief has been wrong before. See docs/adr/0006.
    """
    return PlanResourcesResponse.from_dict(_fixture_response_dict(action, planned_at))


def grpc_plan_from_wire_fixture(
    action: str, planned_at: str = PLANNED_AT
) -> response_pb2.PlanResourcesResponse:
    """The same fixture decoded into the protobuf response the gRPC client returns.

    ``get_query`` accepts both, and reaches the condition through ``MessageToDict`` rather
    than ``to_dict()`` for this one, so the protobuf arm is a second decoding of the same
    wire bytes. See ``test_translator.py`` for what it does and does not prove -- a JSON
    fixture cannot carry everything a real gRPC frame does.
    """
    return ParseDict(
        _fixture_response_dict(action, planned_at),
        response_pb2.PlanResourcesResponse(),
    )


# -- the golden expectations ------------------------------------------------


#: The reserved key an entry may carry alongside its expectation; never compared.
NOTE_KEY = "note"


def read_golden_expectations() -> Dict[str, Dict[str, Any]]:
    """The golden expectations, keyed by action, each split into ``note`` and the value.

    ``adapter`` is checked rather than ignored: the file is a flat map of action names, so a
    copy taken from another adapter parses cleanly and would be compared against this
    adapter's output with only the diff to say something went wrong.
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


def write_golden_expectations(expectations: Dict[str, Dict[str, Any]]) -> None:
    """Rewrite the golden expectations, carrying every existing ``note`` across.

    Only ever called under ``GOLDEN_UPDATE=1`` (``pdm run golden:update``). Regeneration is
    the same deliberate act as ``conformance/scripts/regenerate-wire-fixtures.sh``, with the
    same safety: the diff is what a reviewer reads, which is why the entries are written
    sorted and one action per key. CI never sets the variable.

    A missing file is not an error here, and only here -- that is how a new adapter
    bootstraps one. Reading a missing file for an assertion stays an error, because a suite
    that quietly asserts nothing is the failure mode the completeness guard exists to
    prevent.

    Regenerating under the wrong SQLAlchemy major IS an error. The two compilers render some
    trees differently, so it would rewrite every entry that differs and present a compiler
    swap as a translation change -- a diff a reviewer would have to read line by line to
    discover said nothing.
    """
    if INSTALLED_SQLALCHEMY_MAJOR != GOLDEN_SQLALCHEMY_MAJOR:
        raise AssertionError(
            f"{GOLDEN_FILE} is generated under SQLAlchemy {GOLDEN_SQLALCHEMY_MAJOR}, and "
            f"{sqlalchemy_version('sqlalchemy')} is installed. Regenerating here would "
            "rewrite every entry the two compilers render differently."
        )
    # Notes are read WITHOUT the header validation `read_golden_expectations` applies. The
    # file about to be overwritten may legitimately carry an older header -- that is what a
    # header change looks like -- and refusing to carry the commentary across because of one
    # would make every such change silently drop it.
    notes: Dict[str, str] = {}
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
    # other harnesses use for the p-struct probe.
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


# -- rendering an emitted Select --------------------------------------------

#: The dialects the golden expectations pin, most-executed first.
#:
#: SQLite is the store ``test_adversarial_conformance.py`` actually runs the corpus against,
#: so its rendering is the one an oracle comparison stands behind. PostgreSQL is executed by
#: nothing in this repository and is the dialect the adapter's own source reasons about most
#: — NaN ordering, ``CAST`` rounding, the ``string()`` cast over a boolean — so pinning its
#: rendering is the only place that reasoning is visible at all. They are not close to
#: identical: SQLite has no boolean type, so a ``CASE`` in a WHERE clause needs ``= 1`` and a
#: negation renders as ``= 0``, and the two dialects spell float division differently.
GOLDEN_DIALECTS = ("sqlite", "postgresql")


def dialect(name: str):
    # Imported here rather than at module scope: `sqlalchemy.dialects` pulls in every
    # dialect module named, and only the rendering helpers below need them.
    from sqlalchemy.dialects import postgresql, sqlite

    return {"sqlite": sqlite.dialect(), "postgresql": postgresql.dialect()}[name]


def json_parameter(label: str, value: Any) -> Any:
    """One bound parameter, in the form the golden file records it.

    A parameter has to survive a JSON round trip or the asset records something other than
    what the driver is handed. Scalars and lists of scalars do. A ``datetime`` does not, so
    it is normalised to its ISO-8601 spelling in the data and the actions that bind one are
    pinned in code — the same answer the format prescribes for a value the encoding cannot
    hold (``conformance/README.md``, "Golden expectations").

    A non-finite float is refused outright rather than normalised: the adapter folds every
    IEEE special before a bind is built, so one reaching here is a translation change, not an
    encoding problem.
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


def render(query, dialect_name: str) -> Tuple[str, Dict[str, Any]]:
    """Compile ``query`` for one dialect, as ``(statement, parameters)``.

    The WHOLE ``Select`` is compiled, never the bare ``WHERE`` clause, because correlation is
    only observable inside the enclosing SELECT: compiled on its own, a correlated subquery
    renders the outer table into its own FROM as a cartesian product and silently compares
    against every row of it.

    ``render_postcompile`` expands an ``IN`` clause's single expanding bind into the one
    placeholder per element the DBAPI is actually handed. Without it the statement records
    ``IN (__[POSTCOMPILE_scope_1])``, which is a stage of SQLAlchemy's compiler rather than
    anything a database ever sees, and hides how many values the clause carries.

    Whitespace is collapsed because SQLAlchemy's compiler breaks clauses across lines and a
    line break is not a translation decision. It is safe to do because every value in the
    statement is a bind parameter — the ``parameters`` half of the pair is the proof — so no
    string literal can have its own whitespace flattened.
    """
    compiled = query.compile(
        dialect=dialect(dialect_name), compile_kwargs={"render_postcompile": True}
    )
    return " ".join(str(compiled).split()), dict(compiled.params)


def statement_preamble() -> str:
    """The ``SELECT ... FROM`` every emitted statement starts with.

    ``get_query`` builds ``select(table).where(condition)``, so the columns and the FROM are
    the same 180 times over and the golden entries record only what follows ``WHERE``. That
    is only lossless while this holds, which is why ``test_translator.py`` asserts it for
    every action rather than trusting it.
    """
    return " ".join(str(select(AdvResource).compile(dialect=dialect("sqlite"))).split())


def where_clause(statement: str) -> Union[str, None]:
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


def statement_from(where: Union[str, None]) -> str:
    """The inverse of :func:`where_clause`, so the recorded value is checkable both ways."""
    preamble = statement_preamble()
    return preamble if where is None else f"{preamble} WHERE {where}"


def declared_columns() -> Sequence[str]:
    """Every column name the corpus schema declares, for the "no stray identifier" rule."""
    return sorted(
        f"{table.name}.{column.name}"
        for table in AdvBase.metadata.tables.values()
        for column in table.columns
    )
