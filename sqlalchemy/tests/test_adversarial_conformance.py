# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Conformance harness: replay the recorded corpus against real stores. No PDP runs here.

Each golden plan, for both recorded PDPs, is translated with ``corpus.py``'s mapping and
executed; the ids must equal the recorded ``allowed``. ``conformance-ledger.json`` lists the
exceptions. SQLite runs every case, sync and async; PostgreSQL runs only the cases that read
a declared collection, once per storage shape.
"""

import asyncio
import json
import os
from collections import Counter
from datetime import datetime
from typing import Any

import pytest
from corpus import (
    ATTR_MAP,
    ATTRIBUTE_NULL_REPRESENTATION,
    COLLECTION_COLUMNS,
    OPERATOR_OVERRIDES,
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
    golden_cases,
    plan_from_golden,
    read_corpus_json,
    reads_declared_collection,
)
from sqlalchemy import create_engine, event, insert, text

from cerbos_sqlalchemy import UnsupportedPlanError, get_query

SEEDS: list[dict[str, Any]] = read_corpus_json("seeds.json")["seeds"]
DERIVED: dict[str, dict[str, Any]] = read_corpus_json("derived-fields.json")["derived"]

with open(
    os.path.join(os.path.dirname(__file__), "..", "conformance-ledger.json"),
    encoding="utf-8",
) as _f:
    LEDGER: dict[str, dict[str, Any]] = json.load(_f)["cases"]

GOLDEN = {tag: golden_cases(tag) for tag in PDP_TAGS}

STORES = ("sqlite", "sqlite-async", "postgresql-json", "postgresql-pgArray")

#: ``(tier, outcome)`` per store and tag, printed by ``conftest.py`` after the run.
RESULTS: dict[str, Counter] = {}


def _params():
    for store in STORES:
        for tag in PDP_TAGS:
            for case in GOLDEN[tag]:
                if store.startswith("postgresql") and not reads_declared_collection(
                    case
                ):
                    continue
                yield pytest.param(store, tag, case, id=f"{store}-{tag}-{case['id']}")


# -- the store --------------------------------------------------------------


def _seed(engine) -> None:
    """Create the corpus schema on ``engine`` and store every seed row in it."""
    AdvBase.metadata.create_all(engine)
    by_id = {seed["id"]: seed for seed in SEEDS}

    def parent_of(seed):
        return None if seed["parentSeedId"] is None else by_id[seed["parentSeedId"]]

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
    for seed in SEEDS:
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
    _seed(engine)
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
    with PostgresContainer(image) as container:
        engine = create_engine(container.get_connection_url())
        _seed(engine)
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


# A list, not a set, so a filter that duplicates a row (a fanning-out join) is caught.
def _execute(store: str, query, request) -> list[str]:
    if store == "sqlite-async":
        return _execute_async(request.getfixturevalue("sqlite_url"), query)
    engine = request.getfixturevalue(
        "pg_engine" if store.startswith("postgresql") else "sqlite_engine"
    )
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


def _translate(store: str, case: dict[str, Any]):
    return get_query(
        plan_from_golden(case),
        AdvResource,
        ATTR_MAP,
        operator_override_fns=OPERATOR_OVERRIDES,
        attribute_null_representation=ATTRIBUTE_NULL_REPRESENTATION,
        collection_columns=(
            PG_ARRAY_COLLECTION_COLUMNS
            if store == "postgresql-pgArray"
            else COLLECTION_COLUMNS
        ),
    )


# A cartesian-product warning means a subquery failed to correlate: make it an error.
@pytest.mark.filterwarnings("error::sqlalchemy.exc.SAWarning")
@pytest.mark.parametrize("store,tag,case", _params())
def test_case(store, tag, case, request):
    tally = RESULTS.setdefault(f"{store} @ {tag}", Counter())
    # A golden file carries `plannerDivergence` only for the PDP tags it applies to.
    if case["plannerDivergence"] is not None:
        tally[(case["tier"], "planner divergence")] += 1
        pytest.skip(f"planner divergence: {case['plannerDivergence']['reason']}")

    entry = LEDGER.get(case["id"])
    if entry is not None and tag not in entry.get("pdp", [tag]):
        entry = None
    status = entry["status"] if entry else "pass"

    if status == "unsupported":
        with pytest.raises(UnsupportedPlanError):
            _translate(store, case)
    else:
        returned = _execute(store, _translate(store, case), request)
        if status == "divergent":
            assert sorted(returned) != sorted(case["allowed"]), (
                f"{case['id']} now matches the PDP: remove its divergent ledger entry"
            )
        else:
            assert sorted(returned) == sorted(case["allowed"])
    tally[(case["tier"], status)] += 1


def test_every_ledger_entry_names_a_golden_case():
    ids = {case["id"] for cases in GOLDEN.values() for case in cases}
    assert sorted(set(LEDGER) - ids) == []
    for case_id, entry in LEDGER.items():
        assert entry["status"] in ("unsupported", "divergent"), case_id
        assert entry["reason"], case_id
        assert entry["status"] != "divergent" or entry.get("issue"), case_id
        # A `pdp` scope naming an unrecorded tag matches nothing.
        assert set(entry.get("pdp", PDP_TAGS)) <= set(PDP_TAGS), case_id
