# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Conformance harness: replay the recorded corpus against real stores. No PDP runs here.

Each golden plan, for both recorded PDPs, is translated with ``corpus.py``'s mapping and
executed; the ids must equal the recorded ``allowed``. ``conformance-ledger.json`` lists the
exceptions. SQLite (sync and async), PostgreSQL and MySQL run every case. PostgreSQL runs
the cases that read a declared collection once more, with the collections stored as native
arrays instead of JSON. Every leg runs under both mappings in ``corpus.MAPPINGS``: relations
inferred from ``relationship()`` and relations written out by hand.
"""

import asyncio
import json
import os
import subprocess
import sys
from collections import Counter
from datetime import datetime
from typing import Any

import pytest
from corpus import (
    ATTRIBUTE_NULL_REPRESENTATION,
    COLLECTION_COLUMNS,
    FUZZ_DIR,
    MAPPINGS,
    PDP_TAGS,
    PG_ARRAY_COLLECTION_COLUMNS,
    AdvBase,
    AdvCategory,
    AdvInner,
    AdvLabel,
    AdvParent,
    AdvResource,
    AdvSubCategory,
    AdvTag,
    fuzz_thrown,
    fuzz_verdict,
    golden_cases,
    operator_mutants,
    plan_from_golden,
    read_corpus_json,
    reads_clock,
    reads_declared_collection,
)
from sqlalchemy import bindparam, create_engine, event, insert, text, update
from sqlalchemy.dialects import sqlite
from sqlalchemy.engine import make_url

from cerbos_sqlalchemy import UnsupportedPlanError, get_query

SEEDS: list[dict[str, Any]] = read_corpus_json("seeds.json")["seeds"]
DERIVED: dict[str, dict[str, Any]] = read_corpus_json("derived-fields.json")["derived"]

with open(
    os.path.join(os.path.dirname(__file__), "..", "conformance-ledger.json"),
    encoding="utf-8",
) as _f:
    _LEDGER_FILE = json.load(_f)
LEDGER: dict[str, dict[str, Any]] = _LEDGER_FILE["cases"]

GOLDEN = {tag: golden_cases(tag) for tag in PDP_TAGS}

STORES = ("sqlite", "sqlite-async", "postgresql-json", "postgresql-pgArray", "mysql")

#: The store each leg runs on, as the ledger's ``lacks`` names it.
_LEDGER_STORE = {
    "sqlite": "sqlite",
    "sqlite-async": "sqlite",
    "postgresql-json": "postgres",
    "postgresql-pgArray": "postgres",
    "mysql": "mysql",
}


def _unheld(ledger_store: str) -> set[str]:
    """The seeds ``ledger_store`` cannot hold: each requires a capability it lacks there.

    They are not loaded, and their ids are struck from every allowed set.
    """
    lacks = {
        capability
        for capability, entry in _LEDGER_FILE.get("lacks", {}).items()
        if ledger_store in entry.get("stores", [ledger_store])
    }
    return {seed["id"] for seed in SEEDS if lacks & set(seed.get("requires", []))}


def _held(store: str, allowed: list[str]) -> list[str]:
    unheld = _unheld(_LEDGER_STORE[store])
    return [seed_id for seed_id in allowed if seed_id not in unheld]


#: ``(tier, outcome)`` per store and tag, printed by ``conftest.py`` after the run.
RESULTS: dict[str, Counter] = {}

#: Fuzz verdicts per store, written by ``conftest.py`` when the session ends.
FUZZ_RESULTS: dict[str, list[dict[str, Any]]] = {}


def _params():
    for store in STORES:
        for mapping in MAPPINGS:
            for tag in PDP_TAGS:
                for case in GOLDEN[tag]:
                    # Only a declared collection's storage tells the two PostgreSQL shapes
                    # apart, so the array shape runs just the cases that read one.
                    if store == "postgresql-pgArray" and not reads_declared_collection(
                        case
                    ):
                        continue
                    yield pytest.param(
                        store,
                        mapping,
                        tag,
                        case,
                        id=f"{store}-{mapping}-{tag}-{case['id']}",
                    )


# -- the store --------------------------------------------------------------


def _seed(engine, ledger_store: str) -> None:
    """Create the corpus schema on ``engine`` and store every seed row it can hold."""
    AdvBase.metadata.create_all(engine)
    unheld = _unheld(ledger_store)
    seeds = [seed for seed in SEEDS if seed["id"] not in unheld]
    by_id = {seed["id"]: seed for seed in seeds}

    def parent_of(seed):
        return None if seed["parentSeedId"] is None else by_id[seed["parentSeedId"]]

    links: list[dict[str, Any]] = []

    def scalars(seed):
        return {
            "a_bool": seed["aBool"],
            "a_string": seed["aString"],
            "a_number": seed["aNumber"],
            "a_optional_string": seed["aOptionalString"],
        }

    def instant(value):
        return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None

    rows: dict[Any, list[dict[str, Any]]] = {
        model: []
        for model in (
            AdvResource,
            AdvParent,
            AdvInner,
            AdvTag,
            AdvCategory,
            AdvSubCategory,
            AdvLabel,
        )
    }
    for seed in seeds:
        derived = DERIVED[seed["id"]]
        tags, sub_names = seed["tags"], seed["subCategoryNames"]
        tag_names = [tag["name"] for tag in tags]
        rows[AdvResource].append(
            {
                "id": seed["id"],
                **scalars(seed),
                "a_double": derived["aDouble"],
                "created_by": derived["createdBy"],
                "scope": derived["scope"],
                "created_at": instant(derived["createdAt"]),
                "updated_at": instant(derived["updatedAt"]),
                # The ordered copies `collection_columns` declares (#227). A missing tag
                # name is omitted, not null.
                "tags_json": [
                    {k: v for k, v in tag.items() if v is not None} for tag in tags
                ],
                "tag_names_json": tag_names,
                # A seed with no category sends no `mainCategory`, so store NULL.
                "main_sub_categories_json": [{"name": n} for n in sub_names] or None,
                # PostgreSQL arrays hold scalars, so `tags` stores ids. Only size() reads it.
                "tags_array": [tag["id"] for tag in tags],
                "tag_names_array": tag_names,
                "main_sub_categories_array": list(sub_names) or None,
                # Null elements included.
                "a_number_list_json": seed["aNumberList"],
                "a_bool_list_json": seed["aBoolList"],
                "a_number_list_array": seed["aNumberList"],
                "a_bool_list_array": seed["aBoolList"],
            }
        )
        # One owned row per level. A seed with no parent gets no row, so the parent is
        # missing rather than NULL.
        parent = parent_of(seed)
        if parent is not None:
            parent_id = f"{seed['id']}-parent"
            rows[AdvParent].append(
                {"id": parent_id, **scalars(parent), "resource_id": seed["id"]}
            )
            inner = parent_of(parent)
            if inner is not None:
                rows[AdvInner].append(
                    {
                        "id": f"{parent_id}-inner",
                        **scalars(inner),
                        "parent_id": parent_id,
                    }
                )
        step = seed.get("stepParentSeedId")
        step_id = f"{seed['id']}-step" if step is not None else None
        if step is not None:
            rows[AdvParent].append(
                {"id": step_id, **scalars(by_id[step]), "resource_id": None}
            )
        if step is not None or seed.get("peerSeedId") is not None:
            links.append(
                {
                    "link_id": seed["id"],
                    "peer_id": seed.get("peerSeedId"),
                    "step_parent_id": step_id,
                }
            )
        for tag in tags:
            rows[AdvTag].append(
                {"tag_id": tag["id"], "name": tag["name"], "resource_id": seed["id"]}
            )
        # One category per seed, holding every sub-name, so no two rows share a relation.
        category_id = f"{seed['id']}-cat"
        if sub_names:
            rows[AdvCategory].append(
                {"id": category_id, "name": "business", "resource_id": seed["id"]}
            )
        for i, sub_name in enumerate(sub_names):
            sub_id = f"{seed['id']}-sub{i}"
            rows[AdvSubCategory].append(
                {"id": sub_id, "name": sub_name, "category_id": category_id}
            )
            for j, label in enumerate(derived["labels"]):
                rows[AdvLabel].append(
                    {
                        "id": f"{sub_id}-label{j}",
                        "name": label,
                        "sub_category_id": sub_id,
                    }
                )

    with engine.begin() as conn:
        for model, model_rows in rows.items():
            if model_rows:
                conn.execute(insert(model.__table__), model_rows)
        # After every row exists, so each foreign key finds its target.
        if links:
            table = AdvResource.__table__
            conn.execute(
                update(table)
                .where(table.c.id == bindparam("link_id"))
                .values(
                    peer_id=bindparam("peer_id"),
                    step_parent_id=bindparam("step_parent_id"),
                ),
                links,
            )


def _case_sensitive_like(dbapi_conn, _):
    # CEL string matching is byte-exact; SQLite's LIKE is case-insensitive by default.
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA case_sensitive_like = ON")
    cursor.close()


@pytest.fixture(scope="module")
def sqlite_url(tmp_path_factory):
    # A file, so the async engine sees the rows the sync engine seeded.
    path = tmp_path_factory.mktemp("conformance") / "seeds.sqlite"
    engine = create_engine(f"sqlite:///{path}")
    _seed(engine, "sqlite")
    engine.dispose()
    return str(path)


@pytest.fixture(scope="module")
def sqlite_engine(sqlite_url):
    engine = create_engine(f"sqlite:///{sqlite_url}")
    event.listen(engine, "connect", _case_sensitive_like)
    yield engine
    engine.dispose()


# Rebased to start at index 0, so an adapter reading `array[i + 1]` cannot pass by
# relying on PostgreSQL's default lower bound of 1.
_PG_ARRAY_COLUMNS = {
    "tags_array": "TEXT[]",
    "tag_names_array": "TEXT[]",
    "main_sub_categories_array": "TEXT[]",
    "a_number_list_array": "INTEGER[]",
    "a_bool_list_array": "BOOLEAN[]",
}


@pytest.fixture(scope="module")
def pg_engine():
    from testcontainers.postgres import PostgresContainer

    with open(
        os.path.join(os.path.dirname(__file__), "..", "POSTGRES_IMAGE"),
        encoding="utf-8",
    ) as f:
        image = f.read().strip()
    # ICU's linguistic en-US order, as a managed service's default, so the corpus proves the
    # adapter orders strings by code point itself (#489).
    initdb_args = os.environ.get(
        "ADAPTER_TEST_POSTGRES_INITDB_ARGS", "--locale-provider=icu --icu-locale=en-US"
    )
    with PostgresContainer(image).with_env(
        "POSTGRES_INITDB_ARGS", initdb_args
    ) as container:
        engine = create_engine(container.get_connection_url())
        _seed(engine, "postgres")
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
                lower = conn.execute(
                    text(
                        f"SELECT DISTINCT array_lower({column}, 1) FROM "
                        f"adversarial_resource WHERE cardinality({column}) > 0"
                    )
                ).scalars()
                assert set(lower) == {0}, column
        yield engine
        engine.dispose()


@pytest.fixture(scope="module")
def mysql_engine():
    from testcontainers.mysql import MySqlContainer

    with open(
        os.path.join(os.path.dirname(__file__), "..", "MYSQL_IMAGE"),
        encoding="utf-8",
    ) as f:
        image = f.read().strip()
    with MySqlContainer(image) as container:
        # Name the driver: testcontainers 4 leaves it out, which picks MySQLdb.
        url = make_url(container.get_connection_url()).set(drivername="mysql+pymysql")
        engine = create_engine(url)
        _seed(engine, "mysql")
        with engine.connect() as conn:
            # A case-folding collation (the server default, or a store variant's), so the corpus
            # proves the adapter compares strings byte for byte itself (#474).
            [collation] = conn.execute(
                text(
                    "SELECT DISTINCT collation_name FROM information_schema.columns "
                    "WHERE table_schema = DATABASE() AND collation_name IS NOT NULL"
                )
            ).scalars()
            assert collation.endswith("_ci"), collation
        yield engine
        engine.dispose()


_ENGINES = {
    "sqlite": "sqlite_engine",
    "postgresql-json": "pg_engine",
    "postgresql-pgArray": "pg_engine",
    "mysql": "mysql_engine",
}


# A list, not a set, so a filter that duplicates a row (a fanning-out join) is caught.
def _execute(store: str, query, request) -> list[str]:
    if store == "sqlite-async":
        return _execute_async(request.getfixturevalue("sqlite_url"), query)
    engine = request.getfixturevalue(_ENGINES[store])
    with engine.connect() as conn:
        return [row.id for row in conn.execute(query)]


def _execute_async(path: str, query) -> list[str]:
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

    async def run() -> list[str]:
        engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
        event.listen(engine.sync_engine, "connect", _case_sensitive_like)
        try:
            async with AsyncSession(engine) as session:
                return [row.id for row in (await session.execute(query)).scalars()]
        finally:
            await engine.dispose()

    return asyncio.run(run())


# -- the contract -----------------------------------------------------------


def _translate(store: str, mapping: str, case: dict[str, Any]):
    return _translate_plan(store, mapping, plan_from_golden(case))


def _translate_plan(store: str, mapping: str, plan):
    attr_map, overrides = MAPPINGS[mapping]
    return get_query(
        plan,
        AdvResource,
        attr_map,
        operator_override_fns=overrides,
        # Every column the dataset omits when NULL is declared "omitted" in
        # ATTRIBUTE_NULL_REPRESENTATION. The call-level value covers what is left, list
        # elements and principal-side literals, which the dataset sends as values; the
        # adapter's own default is pinned in test_query.py.
        null_attribute_representation="explicit",
        attribute_null_representation=ATTRIBUTE_NULL_REPRESENTATION,
        collection_columns=(
            PG_ARRAY_COLLECTION_COLUMNS
            if store == "postgresql-pgArray"
            else COLLECTION_COLUMNS
        ),
    )


# -- fitness functions for "refuse, never best-effort" ----------------------
# conformance/README.md, "The harness contract". Neither runs a query.

# A fixed instant for the relative-time placeholder, so two processes translate the same plan.
_PLANNED_AT = "2026-01-01T00:00:00.123456789Z"


def _rendered(mapping: str, case: dict[str, Any]) -> str:
    try:
        query = _translate_plan(
            "sqlite", mapping, plan_from_golden(case, planned_at=_PLANNED_AT)
        )
    except Exception as error:  # noqa: BLE001 - a throw is compared like a query
        return f"{type(error).__name__}: {error}"
    compiled = query.compile(dialect=sqlite.dialect())
    return f"{compiled}\n{compiled.params!r}"


def renderings() -> dict[str, str]:
    """Every case's translation, rendered as SQL and parameters."""
    return {
        f"{mapping} {tag} {case['id']}": _rendered(mapping, case)
        for mapping in MAPPINGS
        for tag in PDP_TAGS
        for case in GOLDEN[tag]
        if case["plannerDivergence"] is None and not reads_clock(case)
    }


# Set iteration order follows the string hash, which is fixed for a process and differs between
# processes, so the second translation runs in a child process under another hash seed.
@pytest.fixture(scope="session")
def renderings_under_another_hash_seed() -> dict[str, str]:
    seed = "1" if os.environ.get("PYTHONHASHSEED") != "1" else "2"
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            "import json, test_adversarial_conformance as t; print(json.dumps(t.renderings()))",
        ],
        cwd=os.path.dirname(__file__),
        env={**os.environ, "PYTHONHASHSEED": seed},
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(child.stdout.splitlines()[-1])


def _assert_fitness(store, mapping, tag, case, renderings_under_another_hash_seed):
    if store == "sqlite" and not reads_clock(case):
        assert (
            _rendered(mapping, case)
            == renderings_under_another_hash_seed[f"{mapping} {tag} {case['id']}"]
        ), "translating the same plan in another process gave different SQL"

    unrefused = []
    for mutant in operator_mutants(plan_from_golden(case)):
        try:
            query = _translate_plan(store, mapping, mutant)
        except UnsupportedPlanError:
            continue
        except Exception as error:  # noqa: BLE001 - reported, not raised
            unrefused.append(f"{mutant.filter.condition}\nthrew {error!r}")
        else:
            unrefused.append(f"{mutant.filter.condition}\ntranslated to {query}")
    assert unrefused == [], "an operator the adapter cannot know must be refused"


# A cartesian-product warning means a subquery failed to correlate: make it an error.
@pytest.mark.filterwarnings("error::sqlalchemy.exc.SAWarning")
@pytest.mark.parametrize("store,mapping,tag,case", _params())
def test_case(store, mapping, tag, case, request, renderings_under_another_hash_seed):
    if FUZZ_DIR:
        try:
            verdict = fuzz_verdict(
                case,
                _held(store, case["allowed"]),
                _execute(store, _translate(store, mapping, case), request),
            )
        except Exception as error:  # noqa: BLE001 - every throw is judged, not raised
            verdict = fuzz_thrown(case, error, UnsupportedPlanError)
        leg = store if mapping == "declared" else f"{store}+schema"
        FUZZ_RESULTS.setdefault(leg, []).append(verdict)
        assert verdict["verdict"] in ("match", "refused"), verdict
        return

    tally = RESULTS.setdefault(f"{store} ({mapping}) @ {tag}", Counter())
    # A golden file carries `plannerDivergence` only for the PDP tags it applies to.
    if case["plannerDivergence"] is not None:
        tally[(case["tier"], "planner divergence")] += 1
        pytest.skip(f"planner divergence: {case['plannerDivergence']['reason']}")

    _assert_fitness(store, mapping, tag, case, renderings_under_another_hash_seed)

    entry = LEDGER.get(case["id"])
    if entry is not None and tag not in entry.get("pdp", [tag]):
        entry = None
    status = entry["status"] if entry else "pass"
    allowed = _held(store, case["allowed"])

    if status == "unsupported":
        with pytest.raises(UnsupportedPlanError):
            _translate(store, mapping, case)
    else:
        returned = _execute(store, _translate(store, mapping, case), request)
        if status == "divergent":
            assert sorted(returned) != sorted(allowed), (
                f"{case['id']} now matches the PDP: remove its divergent ledger entry"
            )
        else:
            assert sorted(returned) == sorted(allowed)
    tally[(case["tier"], status)] += 1


@pytest.mark.skipif(
    FUZZ_DIR is not None, reason="a fuzz run is judged without the ledger"
)
def test_every_ledger_entry_names_a_golden_case():
    ids = {case["id"] for cases in GOLDEN.values() for case in cases}
    assert sorted(set(LEDGER) - ids) == []
    for case_id, entry in LEDGER.items():
        assert entry["status"] in ("unsupported", "divergent"), case_id
        assert entry["reason"], case_id
        assert entry["status"] != "divergent" or entry.get("issue"), case_id
        # A `pdp` scope naming an unrecorded tag matches nothing.
        assert set(entry.get("pdp", PDP_TAGS)) <= set(PDP_TAGS), case_id
