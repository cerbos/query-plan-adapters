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
degeneracy guard).

The SQLAlchemy-specific translation configuration — the schema, the attribute map,
and the operator overrides that express relation traversals as correlated subqueries
with CEL-faithful three-valued logic (an element whose column is NULL is a CEL
missing-attribute error — UNKNOWN in SQL — and must stay excluded under BOTH
polarities) — lives in ``corpus.py``, because ``test_translator.py`` pins the SQL
this adapter emits for exactly that mapping and the two must not drift. What stays
here is what only this suite consumes: the seeds, the derived fields, the oracle and
the coverage guards over all three.
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
from corpus import (
    ADAPTER,
    ATTR_MAP,
    ATTRIBUTE_NULL_REPRESENTATION,
    OPERATOR_OVERRIDES,
    AdvBase,
    AdvCategory,
    AdvInner,
    AdvLabel,
    AdvParent,
    AdvResource,
    AdvSubCategory,
    AdvTag,
    classify_actions_for_adapter,
    null_representation_throws,
    parse_actions_file,
    read_corpus_json,
    require_message,
)

from cerbos_sqlalchemy import get_query
from sqlalchemy import create_engine, event, insert, select
from sqlalchemy.dialects import postgresql

SEEDS_FILE = read_corpus_json("seeds.json")
DERIVED_FILE = read_corpus_json("derived-fields.json")
MANIFEST = parse_actions_file(read_corpus_json("actions.json"))

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

# The corpus principal is guarded the same way and for the same reason. It feeds
# the PLAN under test AND the check() oracle, so an attribute dropped on the way
# in vanishes from both sides at once: the plan folds to ALWAYS_DENIED and the
# oracle, built from the same principal, agrees. That is how langchain-chromadb's
# hardcoded attribute allowlist let `pv-exists` pass while testing nothing
# (conformance/README.md, "Adding a new hostile shape", step 7). _principal()
# passes the attributes through verbatim; the guard is what proves it still does.
#
# `id` and `roles` are deliberately IN scope, guarded by PRINCIPAL_KEYS one level
# above the attributes — the same two-level shape SEED_KEYS and TAG_KEYS use for
# a row and its `tags[]` elements. A role dropped on the way in changes every
# policy decision at once; that it is less likely to be projected away than an
# attribute is a reason to expect the assertion to stay quiet, not a reason to
# omit it.
PRINCIPAL_KEYS = {"id", "roles", "attr"}
PRINCIPAL_ATTR_KEYS = {"allowedTags", "context", "fewTeams", "manyTeams"}


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


def _assert_principal_attr_shape(label: str, value: Any) -> None:
    """One principal attribute, checked against the two JSON shapes the corpus carries.

    A key-set guard says nothing about a change inside a value and three of the
    four attributes are lists, so the element type is asserted for the same
    reason the seed guard descends into ``tags[]``.
    """
    if isinstance(value, str):
        return
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return
    raise AssertionError(
        f"{label} is neither a string nor a list of strings, the only two shapes "
        "this harness consumes: a reshaped principal attribute feeds the plan and "
        "the check() oracle at once"
    )


for _index, _seed in enumerate(SEEDS):
    _label = f"seeds.json seeds[{_index}]"
    _assert_keys(_label, set(_seed), SEED_KEYS, {SEED_NOTE_KEY})
    for _tag_index, _tag in enumerate(_seed["tags"]):
        _assert_keys(f"{_label}.tags[{_tag_index}]", set(_tag), TAG_KEYS)

# SEEDS_FILE["principal"] is the parsed JSON object, handed to the SDK untouched,
# so its keys are the corpus key set on both levels.
_PRINCIPAL: Dict[str, Any] = SEEDS_FILE["principal"]
_assert_keys("seeds.json principal", set(_PRINCIPAL), PRINCIPAL_KEYS)
_assert_keys("seeds.json principal.attr", set(_PRINCIPAL["attr"]), PRINCIPAL_ATTR_KEYS)
for _attr_key, _attr_value in _PRINCIPAL["attr"].items():
    _assert_principal_attr_shape(f"seeds.json principal.attr.{_attr_key}", _attr_value)

DERIVED: Dict[str, Dict[str, Any]] = DERIVED_FILE["derived"]
_assert_keys("derived-fields.json fields", set(DERIVED_FILE["fields"]), DERIVED_KEYS)
if set(DERIVED) != {seed["id"] for seed in SEEDS}:
    raise AssertionError(
        "derived-fields.json must carry exactly one entry per seeds.json id"
    )
for _id, _entry in DERIVED.items():
    _assert_keys(f'derived-fields.json derived["{_id}"]', set(_entry), DERIVED_KEYS)

# Capability classifications come from the shared manifest, derived at runtime
# rather than copied: unsupported conformance actions must throw, and
# globally-unsupported actions promoted for this adapter are instead checked
# against the PDP oracle. `test_translator.py` derives the same classification
# from the same expressions, which is what lets its completeness guard be total.
_CLASSIFICATION = classify_actions_for_adapter(MANIFEST, ADAPTER)
ORACLE_ACTIONS = _CLASSIFICATION.oracle_actions

# Globally expected-unsupported shapes promoted by this adapter. Regex is not
# promoted because SQL dialect regex engines do not guarantee CEL/RE2 semantics.
SQLALCHEMY_SUPPORTED_EXPECTED = _CLASSIFICATION.supported_expected

# Globally-unsupported planner shapes plus this adapter's own unsupported list:
# translation (or execution) must fail loudly, never produce a silently-wrong
# filter. Each carries the substring the raised error must contain.
THROWING_ACTIONS = _CLASSIFICATION.throwing_actions
THROWING_ACTION_NAMES = {action for action, _ in THROWING_ACTIONS}

# Actions whose `== null` probe targets an attribute the oracle OMITS for NULL
# columns. They carry no oracle comparison: under the omitted representation
# check() denies every row, so the adapter must reject the shape rather than
# emit a filter (#302).
# Every adapter must reject these, so the message map names the whole roster and
# this harness resolves its own entry exactly as it does for a throwing action.
NULL_REPRESENTATION_OMITTED = null_representation_throws(MANIFEST, ADAPTER)
# The one message every null-carrying action must be rejected with under
# ``omitted``.
NULL_OMITTED_MESSAGE = NULL_REPRESENTATION_OMITTED[0][2]

# Every classified action across all four manifest groups. `ActionsFile` reads each
# group explicitly for the same reason: a group nothing names is dropped silently,
# and a dropped group makes its actions vanish from every count at once (the
# projection trap conformance/README.md warns about).
MANIFEST_ACTIONS = MANIFEST.manifest_actions()
SQLALCHEMY_SKIPPED_DIVERGENCES = MANIFEST.skipped_divergences(ADAPTER)

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
    # Root position and bare operand forms (#388): one per hazard — the
    # negation over a bare ordering (every other negated ordering in the
    # corpus wraps a size() or a ternary), the bare boolean at the ROOT of the
    # condition, which this adapter refused outright before this change, and
    # the collection subquery disjoined with a scalar predicate rather than
    # conjoined with one.
    "not-lt",
    "root-bare-bool",
    "or-eq-exists",
    # Hazard classes the corpus missed (#387): the De Morgan branch over a
    # conjunction; the negated LIKE against a COLUMN needle, where a
    # definite-FALSE null guard would leak every NULL-needle row through the
    # NOT; the value-first hasIntersection, which used to keep its wire order
    # and hand the override a literal list where it expected a relation; and
    # the BELOW-cliff unroll of a principal collection, the shape a principal
    # with three teams produces.
    "not-and",
    "not-contains",
    "vf-hasint",
    "pv-exists-unrolled",
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
    # #387, one probe per group this adapter cannot compare: modulo (reached
    # through the int() cast that gives `%` an integer operand), the positional
    # read of a scalar list, and list equality over a map() projection, whose
    # deferred intermediate no enclosing override consumes.
    "arith-mod",
    "index-scalar-list",
    "map-eq-list",
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
                require_message("synthetic-entry", absent)

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
        assert len(MANIFEST_ACTIONS) == 199
        assert len(SEEDS) == 21
        # Each of these carries a pinned message, so a shape gained or lost has
        # to be re-triaged here rather than joining the throw suite unnoticed.
        assert len(THROWING_ACTIONS) == 19
        assert misclassified == []
        assert SQLALCHEMY_SUPPORTED_EXPECTED <= {
            entry["action"] for entry in MANIFEST.expected_unsupported
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

    # #387. `filter-as-conjunct` puts a filter() one level below the root, where
    # the guard that refuses `filter-as-condition` did not look — and this
    # adapter is one of the two where that mattered: the held tuple reached
    # `and_()` and SQLAlchemy raised its own WHERE/HAVING-role ArgumentError,
    # fail-closed but naming a coercion rather than the mechanism. Its oracle is
    # empty BY CONSTRUCTION, so it belongs to neither degeneracy-guard list and a
    # bare "it raises" would say nothing about whether refusing it is REQUIRED.
    #
    # This is that argument. The other conjunct is `R.attr.aBool`, which the
    # adapter certainly can express and which `root-bare-bool` spells on its own;
    # an adapter that dropped the conjunct it could not translate would emit
    # exactly that filter and return every row it selects, all of which the PDP
    # denies for this action.
    def test_filter_as_conjunct_must_be_refused(self, adv_cerbos_client, adv_conn):
        assert _oracle_allowed_ids(adv_cerbos_client, "filter-as-conjunct") == set()

        surviving_half = _adapter_filtered_ids(
            adv_cerbos_client, adv_conn, "root-bare-bool"
        )
        assert 0 < len(surviving_half) < len(SEEDS)

        message = next(
            m for action, m in THROWING_ACTIONS if action == "filter-as-conjunct"
        )
        with pytest.raises(ValueError, match=re.escape(message)):
            _adapter_filtered_ids(adv_cerbos_client, adv_conn, "filter-as-conjunct")

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
