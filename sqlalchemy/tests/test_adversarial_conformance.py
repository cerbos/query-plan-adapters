# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Adversarial differential conformance harness for the shared ``conformance/`` corpus.

Each action is planned against a real pinned PDP, translated with ``get_query`` and
executed against seeded SQLite. The returned ids are compared with a per-row check()
oracle. Actions reading a declared collection also run on PostgreSQL, per storage shape.
The schema, attribute map and operator overrides live in ``corpus.py``, shared with
``test_translator.py``. See ``conformance/README.md`` for the oracle recipe.
"""

import asyncio
import math
import os
import re
from datetime import datetime
from typing import Any

import pytest
from cerbos.engine.v1 import engine_pb2
from cerbos.response.v1 import response_pb2
from cerbos.sdk.client import CerbosClient
from cerbos.sdk.container import CerbosContainer
from cerbos.sdk.grpc.client import CerbosClient as GrpcCerbosClient
from cerbos.sdk.model import PlanResourcesFilterKind, Principal, Resource, ResourceDesc
from cerbos_image import CERBOS_IMAGE, CONFORMANCE_DIR
from corpus import (
    ADAPTER,
    ATTR_MAP,
    ATTRIBUTE_NULL_REPRESENTATION,
    COLLECTION_COLUMNS,
    INSTALLED_SQLALCHEMY_MAJOR,
    OPERATOR_OVERRIDES,
    PG_ARRAY_COLLECTION_COLUMNS,
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
    reads_declared_collection,
)
from google.protobuf.json_format import ParseDict
from google.protobuf.struct_pb2 import Value
from sqlalchemy import create_engine, event, insert, select, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import DeclarativeMeta
from sqlalchemy.orm.attributes import InstrumentedAttribute

from cerbos_sqlalchemy import CollectionColumn, get_query

SEEDS_FILE = read_corpus_json("seeds.json")
DERIVED_FILE = read_corpus_json("derived-fields.json")
MANIFEST = parse_actions_file(read_corpus_json("actions.json"))

SEEDS: list[dict[str, Any]] = SEEDS_FILE["seeds"]
RESOURCE_KIND: str = SEEDS_FILE["resourceKind"]

# -- corpus coverage guards -------------------------------------------------
#
# Each seed feeds both the stored row and the check() oracle, so an unconsumed
# field would be dropped from both and the comparison would still pass.
# Set equality catches keys added to the corpus and keys removed from it.
SEED_KEYS = {
    "id",
    "aBool",
    "aString",
    "aNumber",
    "aOptionalString",
    "aNumberList",
    "aBoolList",
    "tags",
    "subCategoryNames",
    "parentSeedId",
}
# Free-text prose that no harness reads.
SEED_NOTE_KEY = "note"
# Keys inside each `tags[]` element, guarded like top-level keys.
TAG_KEYS = {"id", "name"}
DERIVED_KEYS = {"createdBy", "aDouble", "createdAt", "scope", "labels", "updatedAt"}

# The principal feeds both the plan and the oracle, so a dropped attribute would
# make both deny and the action would pass vacuously. _principal() passes the
# attributes through verbatim; these guards prove it still does.
PRINCIPAL_KEYS = {"id", "roles", "attr"}
PRINCIPAL_ATTR_KEYS = {
    "allowedTags",
    "context",
    "fewTeams",
    "manyTeams",
    "zero",
    "emptyTeams",
    "manyStructs",
    "nullableStructs",
    "missingStructs",
}


def _assert_keys(
    label: str,
    got: set[str],
    want: set[str],
    optional: set[str] = frozenset(),
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
    """Assert a principal attribute has the value shape this harness expects."""
    key = label.rsplit(".", 1)[-1]
    if key == "zero" and type(value) in (int, float):
        return
    if key in {"manyStructs", "nullableStructs", "missingStructs"}:
        assert isinstance(value, list), label
        for item in value:
            assert isinstance(item, dict), label
            if key == "missingStructs":
                _assert_keys(label, set(item), set())
            else:
                _assert_keys(label, set(item), {"name"})
                assert (
                    isinstance(item["name"], str)
                    if key == "manyStructs"
                    else item["name"] is None
                ), label
        return
    if isinstance(value, str):
        return
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return
    raise AssertionError(
        f"{label} is neither a string nor a list of strings: a reshaped principal "
        "attribute feeds the plan and the check() oracle at once"
    )


for _index, _seed in enumerate(SEEDS):
    _label = f"seeds.json seeds[{_index}]"
    _assert_keys(_label, set(_seed), SEED_KEYS, {SEED_NOTE_KEY})
    for _tag_index, _tag in enumerate(_seed["tags"]):
        _assert_keys(f"{_label}.tags[{_tag_index}]", set(_tag), TAG_KEYS)

_PRINCIPAL: dict[str, Any] = SEEDS_FILE["principal"]
_assert_keys("seeds.json principal", set(_PRINCIPAL), PRINCIPAL_KEYS)
_assert_keys("seeds.json principal.attr", set(_PRINCIPAL["attr"]), PRINCIPAL_ATTR_KEYS)
for _attr_key, _attr_value in _PRINCIPAL["attr"].items():
    _assert_principal_attr_shape(f"seeds.json principal.attr.{_attr_key}", _attr_value)

DERIVED: dict[str, dict[str, Any]] = DERIVED_FILE["derived"]
_assert_keys("derived-fields.json fields", set(DERIVED_FILE["fields"]), DERIVED_KEYS)
if set(DERIVED) != {seed["id"] for seed in SEEDS}:
    raise AssertionError(
        "derived-fields.json must carry exactly one entry per seeds.json id"
    )
for _id, _entry in DERIVED.items():
    _assert_keys(f'derived-fields.json derived["{_id}"]', set(_entry), DERIVED_KEYS)

# Derived from actions.json at runtime, the same way test_translator.py derives it.
_CLASSIFICATION = classify_actions_for_adapter(MANIFEST, ADAPTER)
ORACLE_ACTIONS = _CLASSIFICATION.oracle_actions

# Globally unsupported shapes this adapter translates. Regex is not among them:
# SQL regex engines do not match CEL/RE2 semantics.
SQLALCHEMY_SUPPORTED_EXPECTED = _CLASSIFICATION.supported_expected

# Actions that must throw, each with the message substring the error must contain.
THROWING_ACTIONS = _CLASSIFICATION.throwing_actions
THROWING_ACTION_NAMES = {action for action, _ in THROWING_ACTIONS}

# Oracle actions reading a `collection_columns` collection; the PostgreSQL leg
# re-runs them under both storage shapes.
DECLARED_COLLECTION_ACTIONS = sorted(
    action for action in ORACLE_ACTIONS if reads_declared_collection(action)
)

# `== null` probes on an attribute the oracle omits when NULL. check() denies every
# row, so the adapter must reject the shape rather than emit a filter.
NULL_REPRESENTATION_OMITTED = null_representation_throws(MANIFEST, ADAPTER)
# The message every null-carrying action must be rejected with under "omitted".
NULL_OMITTED_MESSAGE = NULL_REPRESENTATION_OMITTED[0][2]

# Every classified action across all four manifest groups.
MANIFEST_ACTIONS = MANIFEST.manifest_actions()
SQLALCHEMY_SKIPPED_DIVERGENCES = MANIFEST.skipped_divergences(ADAPTER)

# -- the legs (#321) ---------------------------------------------------------
#
# Each leg varies one caller-side choice the corpus cannot vary:
# - `grpc`: a protobuf plan, decoded via `MessageToDict`. Unlike HTTP JSON, it keeps
#   the sign of a zero, so it can compare `GRPC_ONLY_ORACLE_ACTIONS`.
# - `declarative-base`: SQLAlchemy 2.0 `DeclarativeBase` models on the same tables.
#   Their metaclass is not `DeclarativeMeta`. Skipped on 1.4.
# - `async`: the `Select` executed through an `AsyncSession` over aiosqlite.
LEGS = ("http", "grpc", "declarative-base", "async")
_IS_SQLA_14 = INSTALLED_SQLALCHEMY_MAJOR == "1.4"

# Refused over HTTP only: JSON decoding drops the sign of a constant zero divisor.
# The manifest classifies HTTP, so they stay in adapterUnsupported; gRPC compares them.
GRPC_ONLY_ORACLE_ACTIONS = ("cr-div-neg-zero", "nan-ord-inf")
HTTP_ZERO_SIGN_MESSAGE = (
    "division by a constant zero whose sign is indeterminate: the HTTP transport"
)


def _require_declarative_base() -> None:
    """Skip on 1.4; on 2.0, fail if the models were not built rather than skip."""
    if _IS_SQLA_14:
        pytest.skip("DeclarativeBase requires SQLAlchemy >= 2.0")
    assert MODERN_MODELS, (
        "SQLAlchemy >= 2.0 is installed but the DeclarativeBase models failed to "
        "build — the declarative-base leg would otherwise skip silently"
    )


# Legacy model -> its DeclarativeBase twin, mapped onto the same `Table`.
MODERN_MODELS: dict[Any, Any] = {}
try:
    from sqlalchemy.orm import DeclarativeBase
except ImportError:  # SQLAlchemy 1.4
    pass
else:

    class _ModernAdvBase(DeclarativeBase):
        pass

    for _legacy in (
        AdvResource,
        AdvTag,
        AdvCategory,
        AdvSubCategory,
        AdvLabel,
        AdvParent,
        AdvInner,
    ):
        MODERN_MODELS[_legacy] = type(
            f"Modern{_legacy.__name__}",
            (_ModernAdvBase,),
            {"__table__": _legacy.__table__},
        )


def _modern_attribute(value: Any) -> Any:
    """Swap a legacy column attribute for its 2.0 twin's; return anything else as is."""
    if isinstance(value, InstrumentedAttribute) and value.class_ in MODERN_MODELS:
        return getattr(MODERN_MODELS[value.class_], value.key)
    return value


def _modern_mapping():
    """``(table, attr_map, collection_columns)`` spelled against the 2.0 twins."""
    attr_map = {name: _modern_attribute(value) for name, value in ATTR_MAP.items()}
    collection_columns = {
        name: CollectionColumn(_modern_attribute(declared.column), declared.storage)
        for name, declared in COLLECTION_COLUMNS.items()
    }
    return MODERN_MODELS[AdvResource], attr_map, collection_columns


# -- the degeneracy guard (conformance/README.md, "The degeneracy guard") ----
#
# An empty or total oracle cannot fail the comparison. Only entries in
# `degenerateOracles` may be degenerate, and they must be exactly as declared.
DEGENERATE_ORACLES: dict[str, str] = MANIFEST.degenerate_oracles

# Refused actions, which the sweep never sees. They prove the PDP and policy
# are live for groups this adapter does not compare.
DEGENERACY_LIVENESS_PROBES = (
    "regex-final-newline",
    "regex-eq-true",
    "regex-lookahead",
    "index-negative",
    "index-fractional",
    "cast-not-int",
    "cast-not-timestamp",
    "cast-not-double",
    "hier-empty-delim",
    "matches-alt",
    # HTTP decoding turns -0 into the integer 0, losing the zero's sign.
    "cr-div-neg-zero",
    "cast-int-double",
    "hier-list-id",
    "arith-mod",
    "map-eq-list",
)


# #414: the refused split as observed.
DEGENERACY_LIVENESS_PROBES += (
    "regex-digit",
    "regex-case",
    "regex-posix",
    "regex-unanchored",
    "regex-dot",
    "regex-alternation",
    "regex-grouped",
    "regex-brace",
    "regex-repetition",
    "regex-optional-operators",
    "pv-except",
    "except-size",
    "except-eq",
    "pv-structs",
    "pv-exists-one",
    "pv-filter",
    "pv-map",
    "hier-overlaps-list-prefix",
    "div-by-division",
    "temporal-raw-eq",
    "eq-list",
    "ne-list",
)


# -- deterministic derived fields (conformance/README.md) --------------------
#
# Read from derived-fields.json, never recomputed. A local copy would feed the row
# and the oracle the same mistake, and the comparison would not catch it.


def _derived_for(seed: dict[str, Any]) -> dict[str, Any]:
    entry = DERIVED.get(seed["id"])
    if entry is None:
        raise AssertionError(
            f'derived-fields.json has no entry for seed "{seed["id"]}"'
        )
    return entry


def _iso_for(seed: dict[str, Any]) -> str:
    """The seed's ISO ``createdBy`` instant for the timestamp probe."""
    return _derived_for(seed)["createdBy"]


def _double_for(seed: dict[str, Any]):
    return _derived_for(seed)["aDouble"]


def _timestamp_for(seed: dict[str, Any], field: str = "createdAt"):
    value = _derived_for(seed)[field]
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


def _scope_for(seed: dict[str, Any]):
    return _derived_for(seed)["scope"]


def _labels_for(seed: dict[str, Any]):
    return _derived_for(seed)["labels"]


# -- the real to-one relation (conformance/README.md, "The real to-one relation")
#
# `parentSeedId` names the seed whose scalars fill this row's `parent`; that seed's
# own `parentSeedId` fills `parent.inner`. Each resource owns fresh parent and inner
# rows, so a filter that returned the parent instead of the child cannot pass.

_SEEDS_BY_ID: dict[str, dict[str, Any]] = {seed["id"]: seed for seed in SEEDS}


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


def _relation_attr(seed: dict[str, Any]) -> dict[str, Any]:
    """A related row's scalars as check() attributes; a NULL column is omitted."""
    attr: dict[str, Any] = {
        "aBool": seed["aBool"],
        "aString": seed["aString"],
        "aNumber": seed["aNumber"],
    }
    if seed["aOptionalString"] is not None:
        attr["aOptionalString"] = seed["aOptionalString"]
    return attr


# -- fixtures ------------------------------------------------------------------


@pytest.fixture(scope="module")
def adv_engine():
    engine = create_engine("sqlite://")

    @event.listens_for(engine, "connect")
    def _configure(dbapi_conn, _):
        # CEL matching is case-sensitive; SQLite's LIKE is not by default.
        dbapi_conn.execute("PRAGMA case_sensitive_like = ON")

    _seed(engine)
    yield engine


def _seed(engine) -> None:
    """Create the corpus schema on ``engine`` and store every seed row in it."""
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
                "updated_at": _timestamp_for(seed, "updatedAt"),
                # The `collection_columns` copies hold exactly what _check_resource()
                # sends. No category means no `mainCategory`, stored as NULL.
                "tags_json": [_tag_attr(tag) for tag in seed["tags"]],
                "tag_names_json": [tag["name"] for tag in seed["tags"]],
                "main_sub_categories_json": [
                    {"name": name} for name in seed["subCategoryNames"]
                ]
                or None,
                # PostgreSQL arrays hold scalars, so `tags` stores ids; only size() reads it.
                "tags_array": [tag["id"] for tag in seed["tags"]],
                "tag_names_array": [tag["name"] for tag in seed["tags"]],
                "main_sub_categories_array": list(seed["subCategoryNames"]) or None,
                # Stored verbatim, null elements included.
                "a_number_list_json": seed["aNumberList"],
                "a_bool_list_json": seed["aBoolList"],
                "a_number_list_array": seed["aNumberList"],
                "a_bool_list_array": seed["aBoolList"],
            }
        )
        # A seed with no parent gets no parent row, so an absent to-one relation
        # is reachable through a scalar path.
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
        # One category per sub-name, owned by this seed, so no rows share relations.
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


@pytest.fixture
def adv_conn(adv_engine):
    with adv_engine.connect() as conn:
        yield conn


# See conformance/README.md, "Pinning service images".
with open(
    os.path.join(os.path.dirname(__file__), "..", "POSTGRES_IMAGE"), encoding="utf-8"
) as _f:
    POSTGRES_IMAGE = _f.read().strip()

# Array columns the PostgreSQL leg rebases to start at 0, with their cast-back types.
_PG_ARRAY_COLUMNS = {
    "tags_array": "TEXT[]",
    "tag_names_array": "TEXT[]",
    "main_sub_categories_array": "TEXT[]",
    "a_number_list_array": "INTEGER[]",
    "a_bool_list_array": "BOOLEAN[]",
}


@pytest.fixture(scope="module")
def pg_engine():
    """The seeds on PostgreSQL, the only store that executes the JSONB and array renderings."""
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer(POSTGRES_IMAGE) as container:
        engine = create_engine(container.get_connection_url())
        _seed(engine)
        # Arrays default to 1-based, which would hide an adapter reading `array[i + 1]`.
        # Rebasing to 0 exposes it.
        with engine.begin() as conn:
            for column, array_type in _PG_ARRAY_COLUMNS.items():
                conn.execute(
                    text(
                        f"UPDATE adversarial_resource SET {column} = CAST("
                        f"'[0:' || (cardinality({column}) - 1) || ']=' "
                        f"|| CAST({column} AS TEXT) AS {array_type}) "
                        f"WHERE cardinality({column}) > 0"
                    )
                )
        yield engine
        engine.dispose()


@pytest.fixture
def pg_conn(pg_engine):
    with pg_engine.connect() as conn:
        yield conn


@pytest.fixture(scope="module")
def adv_cerbos_container():
    strict = os.environ.get("ADAPTER_TEST_STRICT_EVALUATION", "false")
    if strict not in ("false", "true"):
        raise ValueError("ADAPTER_TEST_STRICT_EVALUATION must be false or true")
    container = CerbosContainer(image=CERBOS_IMAGE)
    container.with_volume_mapping(
        os.path.join(CONFORMANCE_DIR, "policies"), "/policies"
    )
    container.with_env("CERBOS_NO_TELEMETRY", "1")
    container.with_command(f"server --set=engine.strictEvaluation={strict}")
    container.start()
    container.wait_until_ready()
    try:
        yield container
    finally:
        container.stop()


# Plans the non-gRPC legs and answers every check(); the transport does not change a decision.
@pytest.fixture(scope="module")
def adv_cerbos_client(adv_cerbos_container):
    with CerbosClient(adv_cerbos_container.http_host(), tls_verify=False) as client:
        yield client


@pytest.fixture(scope="module")
def adv_grpc_client(adv_cerbos_container):
    with GrpcCerbosClient(adv_cerbos_container.grpc_host(), tls_verify=False) as client:
        yield client


# A file-backed copy: an in-memory SQLite database is private to its connection,
# so the async engine could not see it.
@pytest.fixture(scope="module")
def adv_async_url(tmp_path_factory):
    path = tmp_path_factory.mktemp("adversarial-async") / "seeds.sqlite"
    engine = create_engine(f"sqlite:///{path}")
    _seed(engine)
    engine.dispose()
    return f"sqlite+aiosqlite:///{path}"


def _async_filtered_ids(url: str, query) -> set[str]:
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

    async def run() -> set[str]:
        engine = create_async_engine(url)

        @event.listens_for(engine.sync_engine, "connect")
        def _configure(dbapi_conn, _):
            # Via a cursor: the async driver's adapted connection has no `execute`.
            cursor = dbapi_conn.cursor()
            cursor.execute("PRAGMA case_sensitive_like = ON")
            cursor.close()

        try:
            async with AsyncSession(engine) as session:
                result = await session.execute(query)
                return {row.id for row in result.scalars()}
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _principal() -> Principal:
    p = SEEDS_FILE["principal"]
    return Principal(id=p["id"], roles=set(p["roles"]), attr=p["attr"])


def _grpc_principal() -> engine_pb2.Principal:
    # Parse each attribute verbatim into a protobuf Value; never project it.
    p = SEEDS_FILE["principal"]
    return engine_pb2.Principal(
        id=p["id"],
        roles=p["roles"],
        attr={key: ParseDict(value, Value()) for key, value in p["attr"].items()},
    )


def _plan(client, action: str):
    """Plan ``action`` with the given SDK client, in that client's own types."""
    if isinstance(client, GrpcCerbosClient):
        plan = client.plan_resources(
            action,
            _grpc_principal(),
            engine_pb2.PlanResourcesInput.Resource(kind=RESOURCE_KIND),
        )
        # Otherwise the leg would not exercise the `MessageToDict` path.
        assert isinstance(plan, response_pb2.PlanResourcesResponse)
        return plan
    return client.plan_resources(action, _principal(), ResourceDesc(RESOURCE_KIND))


def _tag_attr(tag: dict[str, Any]) -> dict[str, Any]:
    """A tag as a check() element; a NULL name is omitted."""
    attr: dict[str, Any] = {"id": tag["id"]}
    if tag["name"] is not None:
        attr["name"] = tag["name"]
    return attr


def _label_attr(name: Any) -> dict[str, Any]:
    """A label as a check() element; a NULL name is omitted."""
    return {"name": name} if name is not None else {}


def _check_resource(seed: dict[str, Any]) -> Resource:
    """The check() resource mirroring exactly what the seeded row holds."""
    attr: dict[str, Any] = {
        "aBool": seed["aBool"],
        "aString": seed["aString"],
        "aNumber": seed["aNumber"],
        "createdBy": _iso_for(seed),
        "obj": {"inner": seed["aString"]},
        "tags": [_tag_attr(t) for t in seed["tags"]],
        # `owner` and `coOwner` send explicit nulls; CEL tells null from missing.
        "owner": seed["aOptionalString"],
        # `scope` under the explicit-null convention, for `null-value-f2f`.
        "coOwner": _scope_for(seed),
        "tagNames": [tag["name"] for tag in seed["tags"]],
        # Verbatim: a null element is a value in CEL.
        "aNumberList": seed["aNumberList"],
        "aBoolList": seed["aBoolList"],
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
    # A NULL column is omitted, so CEL errors and denies, as SQL's NULL excludes the row.
    if seed["aOptionalString"] is not None:
        attr["aOptionalString"] = seed["aOptionalString"]
    if (a_double := _double_for(seed)) is not None:
        attr["aDouble"] = a_double
    if (scope := _scope_for(seed)) is not None:
        attr["scope"] = scope
    for field in ("createdAt", "updatedAt"):
        if (raw := _derived_for(seed)[field]) is not None:
            attr[field] = raw
    # A row with no category sends no `mainCategory`, so CEL denies, as the empty join does.
    if seed["subCategoryNames"]:
        attr["mainCategory"] = {
            "name": "business",
            "subCategories": [{"name": n} for n in seed["subCategoryNames"]],
            "subNames": list(seed["subCategoryNames"]),
        }
    # No parent row means no `parent` attribute, and likewise for `parent.inner`.
    if (parent_seed := _parent_seed_of(seed)) is not None:
        parent_attr = _relation_attr(parent_seed)
        if (inner_seed := _parent_seed_of(parent_seed)) is not None:
            parent_attr["inner"] = _relation_attr(inner_seed)
        attr["parent"] = parent_attr
    return Resource(id=seed["id"], kind=RESOURCE_KIND, attr=attr)


# -- oracle: ask the PDP itself, row by row --


# One PDP and one principal per module, so decisions are stable and safe to memoize.
_ORACLE_CACHE: dict[str, set[str]] = {}


def _oracle_allowed_ids(client: CerbosClient, action: str) -> set[str]:
    if action not in _ORACLE_CACHE:
        _ORACLE_CACHE[action] = {
            seed["id"]
            for seed in SEEDS
            if client.is_allowed(action, _principal(), _check_resource(seed))
        }
    return set(_ORACLE_CACHE[action])


def _assert_oracle_shape(action: str, oracle: set[str]) -> None:
    """Assert the oracle is non-degenerate, or exactly as ``degenerateOracles`` declares."""
    all_ids = {seed["id"] for seed in SEEDS}
    declared = DEGENERATE_ORACLES.get(action)
    pointer = (
        "the differential cannot fail for a degenerate oracle — see "
        "`degenerateOracles` in conformance/actions.json"
    )
    if declared == "empty":
        assert oracle == set(), (
            f"{action} is declared empty by construction but its oracle allows "
            f"{sorted(oracle)}; {pointer}"
        )
    elif declared == "total":
        assert oracle == all_ids, (
            f"{action} is declared total by construction but its oracle denies "
            f"{sorted(all_ids - oracle)}; {pointer}"
        )
    else:
        assert 0 < len(oracle) < len(all_ids), (
            f"{action} has a degenerate oracle ({len(oracle)} of {len(all_ids)} "
            f"seeds allowed): {pointer}, and declare it there only if it is "
            "degenerate by construction"
        )


def _plan_carries_null_literal(node) -> bool:
    """Whether any plan operand is a null literal or a list containing one."""
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
    collection_columns=COLLECTION_COLUMNS,
    table=AdvResource,
    attr_map=ATTR_MAP,
) -> set[str]:
    plan = _plan(client, action)
    query = get_query(
        plan,
        table,
        attr_map,
        operator_override_fns=OPERATOR_OVERRIDES,
        null_attribute_representation=null_attribute_representation,
        attribute_null_representation=attribute_null_representation,
        collection_columns=collection_columns,
    )
    return {row.id for row in conn.execute(query).fetchall()}


# A cartesian-product warning means a subquery failed to correlate and silently
# matched every row, so warnings are errors.
@pytest.mark.filterwarnings("error::sqlalchemy.exc.SAWarning")
class TestAdversarialConformance:
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

        # Tripwires: a corpus edit must bump these deliberately.
        assert len(MANIFEST_ACTIONS) == 324
        assert len(SEEDS) == 29
        assert len(THROWING_ACTIONS) == 57
        assert misclassified == []
        assert SQLALCHEMY_SUPPORTED_EXPECTED <= {
            entry["action"] for entry in MANIFEST.expected_unsupported
        }

    @pytest.mark.parametrize(
        "leg,action",
        [(leg, action) for leg in LEGS for action in ORACLE_ACTIONS]
        + [("grpc", action) for action in GRPC_ONLY_ORACLE_ACTIONS],
    )
    def test_matches_check_oracle(self, leg, action, request, adv_cerbos_client):
        oracle = _oracle_allowed_ids(adv_cerbos_client, action)
        _assert_oracle_shape(action, oracle)
        if leg == "async":
            plan = _plan(adv_cerbos_client, action)
            query = get_query(
                plan,
                AdvResource,
                ATTR_MAP,
                operator_override_fns=OPERATOR_OVERRIDES,
                null_attribute_representation="explicit",
                attribute_null_representation=ATTRIBUTE_NULL_REPRESENTATION,
                collection_columns=COLLECTION_COLUMNS,
            )
            filtered = _async_filtered_ids(
                request.getfixturevalue("adv_async_url"), query
            )
        else:
            client = (
                request.getfixturevalue("adv_grpc_client")
                if leg == "grpc"
                else adv_cerbos_client
            )
            conn = request.getfixturevalue("adv_conn")
            if leg == "declarative-base":
                _require_declarative_base()
                table, attr_map, collection_columns = _modern_mapping()
                filtered = _adapter_filtered_ids(
                    client,
                    conn,
                    action,
                    table=table,
                    attr_map=attr_map,
                    collection_columns=collection_columns,
                )
            else:
                filtered = _adapter_filtered_ids(client, conn, action)
        assert sorted(filtered) == sorted(oracle)

    def test_the_grpc_leg_promotes_exactly_the_http_zero_sign_refusals(self):
        # Exactly the actions refused with the signed-zero message: any other one
        # should also be compared on the gRPC leg.
        refused_for_sign = sorted(
            action
            for action, message in THROWING_ACTIONS
            if message.startswith(HTTP_ZERO_SIGN_MESSAGE)
        )
        assert refused_for_sign == sorted(GRPC_ONLY_ORACLE_ACTIONS)
        adapter_unsupported = {
            entry["action"] for entry in MANIFEST.adapter_unsupported[ADAPTER]
        }
        assert set(GRPC_ONLY_ORACLE_ACTIONS) <= adapter_unsupported

    def test_the_declarative_base_leg_uses_the_2_0_models(self):
        _require_declarative_base()
        table, attr_map, collection_columns = _modern_mapping()
        assert not isinstance(table, DeclarativeMeta)
        assert isinstance(AdvResource, DeclarativeMeta)
        assert table.__table__ is AdvResource.__table__
        # Every column attribute must belong to a twin, or the leg reruns the baseline.
        remapped = [
            value
            for value in list(attr_map.values())
            + [declared.column for declared in collection_columns.values()]
            if isinstance(value, InstrumentedAttribute)
        ]
        assert remapped
        assert {value.class_ for value in remapped} <= set(MODERN_MODELS.values())

    # #227: every declared-collection action, under both storage shapes.
    @pytest.mark.parametrize(
        "storage,action",
        [
            (storage, action)
            for storage in ("json", "pgArray")
            for action in DECLARED_COLLECTION_ACTIONS
        ],
    )
    def test_declared_collection_storage_matches_check_oracle_on_postgresql(
        self, storage, action, adv_cerbos_client, pg_conn
    ):
        oracle = _oracle_allowed_ids(adv_cerbos_client, action)
        _assert_oracle_shape(action, oracle)
        filtered = _adapter_filtered_ids(
            adv_cerbos_client,
            pg_conn,
            action,
            collection_columns=(
                COLLECTION_COLUMNS if storage == "json" else PG_ARRAY_COLLECTION_COLUMNS
            ),
        )
        assert sorted(filtered) == sorted(oracle)

    def test_the_postgresql_leg_reads_what_it_claims(self, pg_conn):
        # Tripwire: any change to the derived list should be reviewed.
        assert DECLARED_COLLECTION_ACTIONS == [
            "cr-size-frac-ge",
            "hasint-bool-list-vs-string",
            "hasint-number-list-vs-string",
            "in-bool-list-vs-string",
            "in-number-list",
            "in-number-list-vs-string",
            "index-bool-list",
            "index-bool-list-not-eq",
            "index-bool-list-vs-number",
            "index-not-oob",
            "index-number-list",
            "index-number-list-not-eq",
            "index-number-list-vs-bool",
            "index-scalar-list",
            "index-scalar-list-not-eq",
            "index-scalar-list-null",
            "not-empty",
            "not-null-in-number-list",
            "null-in-number-list",
            "size-ge-one",
            "size-threshold",
            "vf-size",
            "w1-not-size-chain",
            "w1-size-chain",
            "w1-size-frac-chain",
            "w1-size-frac-le-chain",
            "w1-size-nonneg-chain",
            "w1-size-zero-chain",
        ]
        assert set(PG_ARRAY_COLLECTION_COLUMNS) == set(COLLECTION_COLUMNS)
        # The rebase must have happened, or off-by-one reads go unnoticed.
        for column in _PG_ARRAY_COLUMNS:
            lower_bounds = {
                row[0]
                for row in pg_conn.execute(
                    text(
                        f"SELECT array_lower({column}, 1) FROM adversarial_resource "
                        f"WHERE cardinality({column}) > 0"
                    )
                )
            }
            assert lower_bounds == {0}, column

    # Both transports, since each decodes the plan differently. The signed-zero
    # refusals are compared on the gRPC leg instead.
    @pytest.mark.parametrize(
        "transport,action,message",
        [
            (transport, action, message)
            for transport in ("http", "grpc")
            for action, message in THROWING_ACTIONS
            if not (transport == "grpc" and action in GRPC_ONLY_ORACLE_ACTIONS)
        ],
    )
    def test_fails_loudly(self, transport, action, message, request):
        client = request.getfixturevalue(
            "adv_grpc_client" if transport == "grpc" else "adv_cerbos_client"
        )
        # Planned outside `raises` so a PDP error fails the test. Nothing is
        # executed: the refusal must happen at translation time, not in the DB.
        plan = _plan(client, action)
        # Only translation-time refusal types count; the message pins which one fired.
        with pytest.raises((ValueError, KeyError, TypeError), match=re.escape(message)):
            get_query(
                plan,
                AdvResource,
                ATTR_MAP,
                operator_override_fns=OPERATOR_OVERRIDES,
                null_attribute_representation="explicit",
                # Needed so null-value-f2f-mixed reaches its refusal.
                attribute_null_representation=ATTRIBUTE_NULL_REPRESENTATION,
                collection_columns=COLLECTION_COLUMNS,
            )

    # #387. The oracle is empty by construction, so show why refusal is required:
    # dropping the untranslatable conjunct would leave `root-bare-bool`, which
    # returns rows the PDP denies.
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

    # #302. The over-grant under the default representation shows the rejection
    # is required, not incidental.
    @pytest.mark.parametrize("action,reason,message", NULL_REPRESENTATION_OMITTED)
    def test_null_representation_omitted_is_rejected(
        self, action, reason, message, adv_cerbos_client, adv_conn
    ):
        assert _oracle_allowed_ids(adv_cerbos_client, action) == set()

        # The default translation emits IS NULL and returns rows the PDP denies.
        over_granted = _adapter_filtered_ids(adv_cerbos_client, adv_conn, action)
        assert len(over_granted) > 0, reason

        with pytest.raises(ValueError, match=re.escape(message)):
            _adapter_filtered_ids(
                adv_cerbos_client,
                adv_conn,
                action,
                null_attribute_representation="omitted",
            )

    # #308. Same action and call-level option; only the attribute declaration
    # changes, so a declaration that did nothing would make both runs agree.
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

    # #302. Walks every corpus plan for null operands, including inside value lists,
    # so rejection cannot rely on an operator allowlist.
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

        # Otherwise the loop below would be vacuous.
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
                # Any other failure must not count as the required rejection.
                if NULL_OMITTED_MESSAGE not in str(exc):
                    not_rejected.append(
                        f"{action} (rejected for the wrong reason: {exc})"
                    )
        assert not_rejected == []

    # nan-ord-inf is absent: over HTTP it is refused, since the zero divisor's sign is lost.
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
            collection_columns=COLLECTION_COLUMNS,
        )
        compiled = query.compile(dialect=postgresql.dialect())

        assert not any(
            isinstance(value, float) and not math.isfinite(value)
            for value in compiled.params.values()
        )

    def test_upstream_has_fold_overgrant_tripwire(self, adv_cerbos_client, adv_conn):
        """Pin the planner folding ``p-has`` to ALWAYS_ALLOWED while check() denies some rows."""
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
        """Read both hops back through a join to check each row's chain holds the right values."""
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

        def a_string_of(seed) -> str | None:
            return None if seed is None else seed["aString"]

        assert stored == {
            seed["id"]: (
                a_string_of(_parent_seed_of(seed)),
                a_string_of(_parent_seed_of(_parent_seed_of(seed))),
            )
            for seed in SEEDS
        }

    def test_liveness_probes_are_refused_and_not_degenerate(self, adv_cerbos_client):
        # A probe that becomes supported must leave this list; the sweep then covers it.
        for action in DEGENERACY_LIVENESS_PROBES:
            assert action not in ORACLE_ACTIONS, f"{action} is now oracle-compared"
            ids = _oracle_allowed_ids(adv_cerbos_client, action)
            assert 0 < len(ids) < len(SEEDS), f"{action} has a degenerate oracle"

    def test_every_declared_degenerate_oracle_is_exactly_as_declared(
        self, adv_cerbos_client
    ):
        # Each entry exempts an action from the sweep, so a stale one must fail here.
        assert DEGENERATE_ORACLES
        all_ids = {seed["id"] for seed in SEEDS}
        manifest_actions = MANIFEST.manifest_actions()
        for action, declared in DEGENERATE_ORACLES.items():
            assert action in manifest_actions, f"{action} is not a corpus action"
            oracle = _oracle_allowed_ids(adv_cerbos_client, action)
            expected = set() if declared == "empty" else all_ids
            assert oracle == expected, f"{action} is not {declared} by construction"
