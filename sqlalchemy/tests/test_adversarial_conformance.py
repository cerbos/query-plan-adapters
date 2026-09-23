"""Conformance harness: replay the recorded corpus against real stores.

For both PDPs in ``conformance/pdp-versions.json`` and every golden file under
``conformance/golden/<tag>/``, the recorded plan is translated through ``get_query`` with
the one mapping in ``corpus.py``, executed, and the returned ids are compared with the
``allowed`` ids the PDP recorded. No PDP runs here. ``conformance-ledger.json`` lists the
cases this adapter cannot pass and why; see ``conformance/README.md``, "The harness contract".

Stores:

- ``sqlite``: every case, on SQLite with ``PRAGMA case_sensitive_like = ON``.
- ``sqlite-async``: every case again, the returned ``Select`` executed through an
  ``AsyncSession`` over aiosqlite.
- ``postgresql-json`` / ``postgresql-pgArray``: the cases whose plan reads a collection
  declared in ``collection_columns``, on a pinned PostgreSQL (``POSTGRES_IMAGE``), once per
  storage shape, because that is where those renderings exist.
"""

import asyncio
import json
import os
from collections import Counter
from datetime import datetime
from typing import Any, Dict, List

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

from cerbos_sqlalchemy import UnsupportedPlanError, get_query
from sqlalchemy import create_engine, event, insert, text

SEEDS: List[Dict[str, Any]] = read_corpus_json("seeds.json")["seeds"]
DERIVED: Dict[str, Dict[str, Any]] = read_corpus_json("derived-fields.json")["derived"]

with open(
    os.path.join(os.path.dirname(__file__), "..", "conformance-ledger.json"),
    encoding="utf-8",
) as _f:
    LEDGER: Dict[str, Dict[str, Any]] = json.load(_f)["cases"]

GOLDEN = {tag: golden_cases(tag) for tag in PDP_TAGS}

STORES = ("sqlite", "sqlite-async", "postgresql-json", "postgresql-pgArray")

#: ``(tier, outcome)`` per store and tag, printed by ``conftest.py`` after the run.
RESULTS: Dict[str, Counter] = {}


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

    rows: Dict[Any, List[Dict[str, Any]]] = {
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
                # The ordered copies `collection_columns` declares (#227): exactly the lists
                # resources.json carries. A missing tag name is a missing element attribute.
                "tags_json": [
                    {k: v for k, v in tag.items() if v is not None} for tag in tags
                ],
                "tag_names_json": tag_names,
                # No category sends no `mainCategory`, and stores NULL.
                "main_sub_categories_json": [{"name": n} for n in sub_names] or None,
                # The PostgreSQL arrays hold scalars, so `tags` keeps its ids: size() counts
                # elements, and the element is never read.
                "tags_array": [tag["id"] for tag in tags],
                "tag_names_array": tag_names,
                "main_sub_categories_array": list(sub_names) or None,
                # The scalar lists as the corpus spells them, null elements included.
                "a_number_list_json": seed["aNumberList"],
                "a_bool_list_json": seed["aBoolList"],
                "a_number_list_array": seed["aNumberList"],
                "a_bool_list_array": seed["aBoolList"],
            }
        )
        # The to-one chain, one owned row per level: a seed with no parent gets no row,
        # which is what makes an absent parent a missing attribute rather than a NULL value.
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
        # One category per sub-name, per seed, so no two rows share a relation.
        for i, sub_name in enumerate(sub_names):
            category_id, sub_id = f"{seed['id']}-cat{i}", f"{seed['id']}-sub{i}"
            rows[AdvCategory].append(
                {"id": category_id, "name": "business", "resource_id": seed["id"]}
            )
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


# The PostgreSQL arrays, rebased to start at index 0: an adapter reading `array[i + 1]`
# would pass against PostgreSQL's default lower bound of 1.
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


# A list, not a set: a filter that duplicates a row (a join fanning out) returns that row
# twice to a caller, and collapsing the result would hide it.
def _execute(store: str, query, request) -> List[str]:
    if store == "sqlite-async":
        return _execute_async(request.getfixturevalue("sqlite_url"), query)
    engine = request.getfixturevalue(
        "pg_engine" if store.startswith("postgresql") else "sqlite_engine"
    )
    with engine.connect() as conn:
        return [row.id for row in conn.execute(query)]


def _execute_async(path: str, query) -> List[str]:
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

    async def run() -> List[str]:
        engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
        event.listen(engine.sync_engine, "connect", _case_sensitive_like)
        try:
            async with AsyncSession(engine) as session:
                return [row.id for row in (await session.execute(query)).scalars()]
        finally:
            await engine.dispose()

    return asyncio.run(run())


# -- the contract -----------------------------------------------------------


def _translate(store: str, case: Dict[str, Any]):
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


# A cartesian-product warning means a subquery failed to correlate and compared against
# EVERY row of a table: silently wrong, so it is an error.
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
            assert sorted(returned) != sorted(
                case["allowed"]
            ), f"{case['id']} now matches the PDP: remove its divergent ledger entry"
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
        # A `pdp` scope naming a tag no longer recorded matches nothing, so the entry is dead.
        assert set(entry.get("pdp", PDP_TAGS)) <= set(PDP_TAGS), case_id
