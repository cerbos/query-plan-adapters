# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Conformance harness: replay the recorded corpus against DuckDB. No PDP runs here.

Each golden plan, for both recorded PDPs and both SDK transports, is translated with the mapping
below and executed against an in-process DuckDB database holding the corpus dataset; the ids must
equal the recorded ``allowed``. ``conformance-ledger.json`` lists the exceptions.

Collections are DuckDB ``LIST`` columns, of ``STRUCT`` where the elements are objects. The to-one
relations are real rows in other tables. ``mainCategory`` is a to-one row holding a ``LIST``.
"""

import json
import os
import subprocess
import sys
from collections import Counter
from datetime import UTC, datetime
from typing import Any

import duckdb
import pytest
from corpus import (
    FUZZ_DIR,
    PDP_TAGS,
    fuzz_thrown,
    fuzz_verdict,
    golden_cases,
    grpc_plan_from_golden,
    operator_mutants,
    plan_from_golden,
    read_corpus_json,
    reads_clock,
)

from cerbos_duckdb import (
    Entry,
    Hop,
    Join,
    ListOf,
    NullConvention,
    NullRepresentation,
    Result,
    UnsupportedPlanError,
    ValueType,
    translate,
)

SEEDS: list[dict[str, Any]] = read_corpus_json("seeds.json")["seeds"]
DERIVED: dict[str, dict[str, Any]] = read_corpus_json("derived-fields.json")["derived"]

with open(
    os.path.join(os.path.dirname(__file__), "..", "conformance-ledger.json"),
    encoding="utf-8",
) as _f:
    _LEDGER_FILE = json.load(_f)
LEDGER: dict[str, dict[str, Any]] = _LEDGER_FILE["cases"]

GOLDEN = {tag: golden_cases(tag) for tag in PDP_TAGS}
STORE = "duckdb"
TRANSPORTS = {"http": plan_from_golden, "grpc": grpc_plan_from_golden}
TABLE = "adversarial_resource"


def _unheld() -> set[str]:
    lacks = {
        capability
        for capability, entry in _LEDGER_FILE.get("lacks", {}).items()
        if STORE in entry.get("stores", [STORE])
    }
    return {seed["id"] for seed in SEEDS if lacks & set(seed.get("requires", []))}


UNHELD = _unheld()

#: ``(tier, outcome)`` per transport and tag, printed by ``conftest.py`` after the run.
RESULTS: dict[str, Counter] = {}

#: Fuzz verdicts per leg, written by ``conftest.py`` when the session ends.
FUZZ_RESULTS: dict[str, list[dict[str, Any]]] = {}


# -- the schema and the mapping ----------------------------------------------------------------

SCHEMA = """
CREATE TABLE adversarial_resource (
    id                VARCHAR PRIMARY KEY,
    a_bool            BOOLEAN,
    a_string          VARCHAR,
    a_number          BIGINT,
    a_double          DOUBLE,
    a_optional_string VARCHAR,
    created_by        VARCHAR NOT NULL,
    scope             VARCHAR,
    created_at        TIMESTAMPTZ,
    updated_at        TIMESTAMPTZ,
    peer_id           VARCHAR,
    step_parent_id    VARCHAR,
    tags              STRUCT(id VARCHAR, name VARCHAR)[],
    a_number_list     DOUBLE[],
    a_bool_list       BOOLEAN[],
    categories        STRUCT(
                          name VARCHAR,
                          sub_categories STRUCT(name VARCHAR, labels STRUCT(name VARCHAR)[])[]
                      )[]
);

CREATE TABLE adversarial_category (
    id             VARCHAR PRIMARY KEY,
    name           VARCHAR NOT NULL,
    resource_id    VARCHAR NOT NULL UNIQUE,
    sub_categories STRUCT(name VARCHAR, labels STRUCT(name VARCHAR)[])[]
);

CREATE TABLE adversarial_parent (
    id                VARCHAR PRIMARY KEY,
    a_bool            BOOLEAN,
    a_string          VARCHAR,
    a_number          BIGINT,
    a_optional_string VARCHAR,
    resource_id       VARCHAR UNIQUE
);

CREATE TABLE adversarial_inner (
    id                VARCHAR PRIMARY KEY,
    a_bool            BOOLEAN,
    a_string          VARCHAR,
    a_number          BIGINT,
    a_optional_string VARCHAR,
    parent_id         VARCHAR NOT NULL UNIQUE
);
"""

S, B, N, T = ValueType.STRING, ValueType.BOOL, ValueType.NUMBER, ValueType.TIMESTAMP
OMITTED, EXPLICIT = NullConvention.OMITTED, NullConvention.EXPLICIT

PARENT = Join("adversarial_parent", source_column="id", target_column="resource_id")
INNER = Join(
    "adversarial_inner",
    source_column="id",
    target_column="resource_id",
    via=(Hop("adversarial_parent", child_column="parent_id", join_column="id"),),
)
PEER = Join("adversarial_resource", source_column="peer_id", target_column="id")
STEP_PARENT = Join(
    "adversarial_parent", source_column="step_parent_id", target_column="id"
)
MAIN_CATEGORY = Join(
    "adversarial_category", source_column="id", target_column="resource_id"
)

# A nameless tag or label sends no `name` key, so the field is omitted; `tagNames` sends the same
# NULL as an explicit null element.
LABELS = ListOf(fields={"name": Entry("name", S, OMITTED)})
SUB_CATEGORY_FIELDS = {"name": Entry("name", S), "labels": Entry("labels", list=LABELS)}
SUB_CATEGORIES = ListOf(element=Entry("name", S), fields=SUB_CATEGORY_FIELDS)


def _to_one(join: Join, prefix: str) -> dict[str, Entry]:
    return {
        prefix: Entry(join=join),
        f"{prefix}.aBool": Entry("a_bool", B, OMITTED, join=join),
        f"{prefix}.aString": Entry("a_string", S, OMITTED, join=join),
        f"{prefix}.aNumber": Entry("a_number", N, OMITTED, join=join),
        f"{prefix}.aOptionalString": Entry("a_optional_string", S, OMITTED, join=join),
    }


_ATTR = "request.resource.attr."

MAPPER: dict[str, Entry] = {
    "request.resource.id": Entry("id", S),
    _ATTR + "aBool": Entry("a_bool", B, OMITTED),
    _ATTR + "aString": Entry("a_string", S, OMITTED),
    _ATTR + "aNumber": Entry("a_number", N, OMITTED),
    _ATTR + "aDouble": Entry("a_double", N, OMITTED),
    _ATTR + "aOptionalString": Entry("a_optional_string", S, OMITTED),
    _ATTR + "createdBy": Entry("created_by", S),
    # `owner` and `coOwner` alias `aOptionalString` and `scope` under the other convention: the
    # dataset sends their NULL as an explicit null.
    _ATTR + "owner": Entry("a_optional_string", S, EXPLICIT),
    _ATTR + "coOwner": Entry("scope", S, EXPLICIT),
    _ATTR + "scope": Entry("scope", S, OMITTED),
    _ATTR + "createdAt": Entry("created_at", T, OMITTED),
    _ATTR + "updatedAt": Entry("updated_at", T, OMITTED),
    _ATTR + "obj.inner": Entry("a_string", S, OMITTED),
    _ATTR + "tags": Entry(
        "tags",
        list=ListOf(
            element=Entry("name", S),
            fields={"id": Entry("id", S), "name": Entry("name", S, OMITTED)},
        ),
    ),
    _ATTR + "tagNames": Entry("tags", list=ListOf(element=Entry("name", S, EXPLICIT))),
    _ATTR + "aNumberList": Entry(
        "a_number_list",
        list=ListOf(element=Entry(value_type=N, null_convention=EXPLICIT)),
    ),
    _ATTR + "aBoolList": Entry(
        "a_bool_list",
        list=ListOf(element=Entry(value_type=B, null_convention=EXPLICIT)),
    ),
    _ATTR + "categories": Entry(
        "categories",
        list=ListOf(
            fields={
                "name": Entry("name", S),
                "subCategories": Entry("sub_categories", list=SUB_CATEGORIES),
            }
        ),
    ),
    _ATTR + "mainCategory.subCategories": Entry(
        "sub_categories", join=MAIN_CATEGORY, list=SUB_CATEGORIES
    ),
    _ATTR + "mainCategory.subNames": Entry(
        "sub_categories", join=MAIN_CATEGORY, list=ListOf(element=Entry("name", S))
    ),
    **_to_one(PARENT, _ATTR + "parent"),
    **{
        k: v
        for k, v in _to_one(INNER, _ATTR + "parent.inner").items()
        if k != _ATTR + "parent.inner"
    },
    **_to_one(PEER, _ATTR + "peer"),
    **_to_one(STEP_PARENT, _ATTR + "stepParent"),
}


# -- the store ---------------------------------------------------------------------------------


def _instant(value: str | None, naive: bool) -> datetime | None:
    if value is None:
        return None
    instant = datetime.fromisoformat(value)
    return instant.astimezone(UTC).replace(tzinfo=None) if naive else instant


#: Each store leg: the session settings it runs under after seeding, and whether its timestamp
#: columns are naive ``TIMESTAMP`` holding UTC. The hostile leg is the configuration README.md says
#: the filter stays exact under: a case-folding default collation, a session time zone far from
#: UTC, and naive timestamps.
LEGS: dict[str, tuple[tuple[str, ...], bool]] = {
    "duckdb": ((), False),
    "duckdb-hostile": (
        ("SET default_collation = 'nocase'", "SET TimeZone = 'Pacific/Chatham'"),
        True,
    ),
}


def _seed(con: duckdb.DuckDBPyConnection, naive: bool) -> None:
    con.execute(SCHEMA.replace("TIMESTAMPTZ", "TIMESTAMP") if naive else SCHEMA)
    seeds = [seed for seed in SEEDS if seed["id"] not in UNHELD]
    by_id = {seed["id"]: seed for seed in SEEDS}

    def scalars(seed: dict[str, Any]) -> list[Any]:
        return [
            seed["aBool"],
            seed["aString"],
            seed["aNumber"],
            seed["aOptionalString"],
        ]

    for seed in seeds:
        derived = DERIVED[seed["id"]]
        names = seed.get("subCategoryNames") or []
        sub_categories = [
            {"name": name, "labels": [{"name": label} for label in derived["labels"]]}
            for name in names
        ]
        number_list = seed.get("aNumberList", [])
        con.execute(
            "INSERT INTO adversarial_resource VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?)",
            [
                seed["id"],
                *scalars(seed)[:3],
                derived["aDouble"],
                seed["aOptionalString"],
                derived["createdBy"],
                derived["scope"],
                _instant(derived["createdAt"], naive),
                _instant(derived["updatedAt"], naive),
                [
                    {"id": tag["id"], "name": tag["name"]}
                    for tag in seed.get("tags", [])
                ],
                None
                if number_list is None
                else [None if v is None else float(v) for v in number_list],
                seed.get("aBoolList", []),
                [{"name": "business", "sub_categories": sub_categories}]
                if names
                else [],
            ],
        )
        if names:
            con.execute(
                "INSERT INTO adversarial_category VALUES (?, ?, ?, ?)",
                [f"{seed['id']}-cat", "business", seed["id"], sub_categories],
            )
        parent = by_id.get(seed["parentSeedId"]) if seed.get("parentSeedId") else None
        if parent is not None:
            con.execute(
                "INSERT INTO adversarial_parent VALUES (?, ?, ?, ?, ?, ?)",
                [f"{seed['id']}-parent", *scalars(parent), seed["id"]],
            )
            inner = (
                by_id.get(parent["parentSeedId"])
                if parent.get("parentSeedId")
                else None
            )
            if inner is not None:
                con.execute(
                    "INSERT INTO adversarial_inner VALUES (?, ?, ?, ?, ?, ?)",
                    [
                        f"{seed['id']}-parent-inner",
                        *scalars(inner),
                        f"{seed['id']}-parent",
                    ],
                )
    for seed in seeds:
        if seed.get("peerSeedId"):
            con.execute(
                "UPDATE adversarial_resource SET peer_id = ? WHERE id = ?",
                [seed["peerSeedId"], seed["id"]],
            )
        step = (
            by_id.get(seed["stepParentSeedId"])
            if seed.get("stepParentSeedId")
            else None
        )
        if step is not None:
            step_id = f"{seed['id']}-step"
            con.execute(
                "INSERT INTO adversarial_parent (id, a_bool, a_string, a_number, a_optional_string) "
                "VALUES (?, ?, ?, ?, ?)",
                [step_id, *scalars(step)],
            )
            con.execute(
                "UPDATE adversarial_resource SET step_parent_id = ? WHERE id = ?",
                [step_id, seed["id"]],
            )


@pytest.fixture(scope="session")
def connections() -> dict[str, duckdb.DuckDBPyConnection]:
    out = {}
    for leg, (settings, naive) in LEGS.items():
        connection = duckdb.connect()
        _seed(connection, naive)
        for setting in settings:
            connection.execute(setting)
        out[leg] = connection
    return out


# -- the contract ------------------------------------------------------------------------------


def _translate(plan) -> Result:
    unmapped: list[str] = []

    def recording(reference: str) -> Entry | None:
        entry = MAPPER.get(reference)
        if entry is None and reference.startswith("request.resource."):
            unmapped.append(reference)
        return entry

    try:
        # Every column the dataset omits when NULL is declared OMITTED in the mapping. The
        # call-level value covers what is left: list elements and principal-side literals.
        return translate(
            plan, TABLE, recording, null_representation=NullRepresentation.EXPLICIT
        )
    finally:
        if unmapped:
            raise AssertionError(
                f"the harness mapping has no entry for {', '.join(unmapped)} (a harness defect)"
            )


def _select(con: duckdb.DuckDBPyConnection, where: str, params: list[Any]) -> list[str]:
    rows = con.execute(f"SELECT id FROM {TABLE} WHERE {where}", params).fetchall()
    return sorted(row[0] for row in rows)


# Splices where an unparenthesised top-level AND, OR, NOT or comparison would bind differently.
_SELF_DELIMITING = (
    "NOT {}",
    "FALSE AND {}",
    "{} AND FALSE",
    "FALSE = {}",
    "{} IS NOT TRUE",
)


def _execute(con: duckdb.DuckDBPyConnection, result: Result) -> list[str]:
    # Spliced without switching on the kind, as a caller that skips the switch would.
    ids = _select(con, result.sql, result.params)
    differs = [
        f"(({c.format(result.sql)}) IS DISTINCT FROM ({c.format('(' + result.sql + ')')}))"
        for c in _SELF_DELIMITING
    ]
    changed = _select(con, " OR ".join(differs), result.params * 2 * len(differs))
    assert changed == [], (
        f"the fragment is not self-delimiting: rows {changed} change when it is parenthesised"
    )
    composed = _select(con, f"id >= ? AND {result.sql}", ["", *result.params])
    assert composed == ids, (
        "the fragment's parameters do not compose after the caller's own"
    )
    return ids


# -- fitness functions for "refuse, never best-effort" -----------------------------------------

_PLANNED_AT = "2026-01-01T00:00:00.123456789Z"


def _rendered(case: dict[str, Any]) -> str:
    try:
        result = _translate(plan_from_golden(case, planned_at=_PLANNED_AT))
    except Exception as error:  # noqa: BLE001 - a throw is compared like a query
        return f"{type(error).__name__}: {error}"
    return f"{result.sql}\n{result.params!r}"


def renderings() -> dict[str, str]:
    return {
        f"{tag} {case['id']}": _rendered(case)
        for tag in PDP_TAGS
        for case in GOLDEN[tag]
        if case["plannerDivergence"] is None and not reads_clock(case)
    }


@pytest.fixture(scope="session")
def renderings_under_another_hash_seed() -> dict[str, str]:
    seed = "1" if os.environ.get("PYTHONHASHSEED") != "1" else "2"
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            "import json, test_conformance as t; print(json.dumps(t.renderings()))",
        ],
        cwd=os.path.dirname(__file__),
        env={**os.environ, "PYTHONHASHSEED": seed},
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(child.stdout.splitlines()[-1])


def _assert_fitness(tag: str, case: dict[str, Any], renderings: dict[str, str]) -> None:
    if not reads_clock(case):
        assert _rendered(case) == renderings[f"{tag} {case['id']}"], (
            "translating the same plan in another process gave different SQL"
        )
    unrefused = []
    for mutant in operator_mutants(plan_from_golden(case)):
        try:
            result = _translate(mutant)
        except UnsupportedPlanError:
            continue
        except Exception as error:  # noqa: BLE001 - reported, not raised
            unrefused.append(f"{mutant.filter.condition}\nthrew {error!r}")
        else:
            unrefused.append(f"{mutant.filter.condition}\ntranslated to {result.sql}")
    assert unrefused == [], "an operator the adapter cannot know must be refused"


def _params():
    for leg in LEGS:
        # The hostile leg varies the store, not the plan, so one transport covers it.
        for transport in TRANSPORTS if leg == STORE else ("http",):
            for tag in PDP_TAGS:
                for case in GOLDEN[tag]:
                    yield pytest.param(
                        leg,
                        transport,
                        tag,
                        case,
                        id=f"{leg}-{transport}-{tag}-{case['id']}",
                    )


@pytest.mark.parametrize("leg,transport,tag,case", list(_params()))
def test_case(
    connections, leg, transport, tag, case, renderings_under_another_hash_seed
):
    allowed = [seed_id for seed_id in case["allowed"] if seed_id not in UNHELD]
    fetch = TRANSPORTS[transport]
    con = connections[leg]
    if FUZZ_DIR:
        if leg != STORE:
            pytest.skip("a fuzz run replays the default leg")
        try:
            verdict = fuzz_verdict(
                case, allowed, _execute(con, _translate(fetch(case)))
            )
        except Exception as error:  # noqa: BLE001 - every throw is judged, not raised
            verdict = fuzz_thrown(case, error, UnsupportedPlanError)
        if transport == "http":
            FUZZ_RESULTS.setdefault(STORE, []).append(verdict)
        assert verdict["verdict"] in ("match", "refused"), verdict
        return

    tally = RESULTS.setdefault(f"{leg} ({transport}) @ {tag}", Counter())
    if case["plannerDivergence"] is not None:
        tally[(case["tier"], "planner divergence")] += 1
        pytest.skip(f"planner divergence: {case['plannerDivergence']['reason']}")

    if leg == STORE and transport == "http":
        _assert_fitness(tag, case, renderings_under_another_hash_seed)

    entry = LEDGER.get(case["id"])
    if entry is not None and tag not in entry.get("pdp", [tag]):
        entry = None
    status = entry["status"] if entry else "pass"
    if status == "unsupported":
        with pytest.raises(UnsupportedPlanError):
            _translate(fetch(case))
    else:
        returned = _execute(con, _translate(fetch(case)))
        if status == "divergent":
            assert returned != sorted(allowed), (
                f"{case['id']} now matches the PDP: remove its divergent ledger entry"
            )
        else:
            assert returned == sorted(allowed)
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
        assert set(entry.get("pdp", PDP_TAGS)) <= set(PDP_TAGS), case_id
