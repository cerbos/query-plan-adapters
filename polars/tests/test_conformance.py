# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Conformance harness: replay the recorded corpus against a Polars frame. No PDP runs here.

Each golden plan, for both recorded PDPs, reaches the adapter through the SDK's HTTP and
gRPC clients, is translated with ``corpus.py``'s mapping, and filters one frame built from
the dataset, eagerly and on the lazy streaming engine; the ids must equal the recorded
``allowed``. ``conformance-ledger.json`` lists the exceptions.
"""

import json
import os
import subprocess
import sys
from collections import Counter
from typing import Any

import polars as pl
import pytest
from corpus import (
    ATTR_MAP,
    ATTRIBUTE_NULL_REPRESENTATION,
    FUZZ_DIR,
    PDP_TAGS,
    build_frame,
    fuzz_thrown,
    fuzz_verdict,
    golden_cases,
    grpc_plan_from_golden,
    operator_mutants,
    plan_from_golden,
    read_corpus_json,
    reads_clock,
)

from cerbos_polars import UnsupportedPlanError, get_filter

SEEDS: list[dict[str, Any]] = read_corpus_json("seeds.json")["seeds"]
DERIVED: dict[str, dict[str, Any]] = read_corpus_json("derived-fields.json")["derived"]

with open(
    os.path.join(os.path.dirname(__file__), "..", "conformance-ledger.json"),
    encoding="utf-8",
) as _f:
    _LEDGER_FILE = json.load(_f)
LEDGER: dict[str, dict[str, Any]] = _LEDGER_FILE["cases"]

GOLDEN = {tag: golden_cases(tag) for tag in PDP_TAGS}

#: Each leg: the SDK transport the plan arrives through, and how the frame is collected.
LEGS = ("http-eager", "http-streaming", "grpc-eager")

_UNHELD = {
    seed["id"]
    for seed in SEEDS
    if set(_LEDGER_FILE.get("lacks", {})) & set(seed.get("requires", []))
}
FRAME = build_frame([seed for seed in SEEDS if seed["id"] not in _UNHELD], DERIVED)

RESULTS: dict[str, Counter] = {}
FUZZ_RESULTS: dict[str, list[dict[str, Any]]] = {}


def _held(allowed: list[str]) -> list[str]:
    return [seed_id for seed_id in allowed if seed_id not in _UNHELD]


def _params():
    for leg in LEGS:
        for tag in PDP_TAGS:
            for case in GOLDEN[tag]:
                yield pytest.param(leg, tag, case, id=f"{leg}-{tag}-{case['id']}")


def _translate_plan(plan) -> pl.Expr:
    return get_filter(
        plan,
        FRAME.schema,
        ATTR_MAP,
        attribute_null_representation=ATTRIBUTE_NULL_REPRESENTATION,
    )


def _translate(leg: str, case: dict[str, Any]) -> pl.Expr:
    plan = (
        grpc_plan_from_golden(case)
        if leg.startswith("grpc")
        else plan_from_golden(case)
    )
    return _translate_plan(plan)


def _execute(leg: str, expr: pl.Expr) -> list[str]:
    if leg.endswith("streaming"):
        frame = FRAME.lazy().filter(expr).select("id").collect(engine="streaming")
    else:
        frame = FRAME.filter(expr).select("id")
    return frame["id"].to_list()


_PLANNED_AT = "2026-01-01T00:00:00.123456789Z"


def _rendered(case: dict[str, Any]) -> str:
    try:
        expr = _translate_plan(plan_from_golden(case, planned_at=_PLANNED_AT))
    except Exception as error:  # noqa: BLE001 - a throw is compared like an expression
        return f"{type(error).__name__}: {error}"
    return expr.meta.serialize(format="json")


def renderings() -> dict[str, str]:
    """Every case's translation, serialised."""
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


def _assert_fitness(tag, case, renderings_under_another_hash_seed):
    if not reads_clock(case):
        assert (
            _rendered(case) == renderings_under_another_hash_seed[f"{tag} {case['id']}"]
        ), "translating the same plan in another process gave a different expression"

    unrefused = []
    for mutant in operator_mutants(plan_from_golden(case)):
        try:
            expr = _translate_plan(mutant)
        except UnsupportedPlanError:
            continue
        except Exception as error:  # noqa: BLE001 - reported, not raised
            unrefused.append(f"{mutant.filter.condition}\nthrew {error!r}")
        else:
            unrefused.append(f"{mutant.filter.condition}\ntranslated to {expr}")
    assert unrefused == [], "an operator the adapter cannot know must be refused"


@pytest.mark.parametrize("leg,tag,case", list(_params()))
def test_case(leg, tag, case, renderings_under_another_hash_seed):
    if FUZZ_DIR:
        try:
            verdict = fuzz_verdict(
                case, _held(case["allowed"]), _execute(leg, _translate(leg, case))
            )
        except Exception as error:  # noqa: BLE001 - every throw is judged, not raised
            verdict = fuzz_thrown(case, error, UnsupportedPlanError)
        FUZZ_RESULTS.setdefault(
            f"polars+{leg}" if leg != "http-eager" else "polars", []
        ).append(verdict)
        assert verdict["verdict"] in ("match", "refused"), verdict
        return

    tally = RESULTS.setdefault(f"{leg} @ {tag}", Counter())
    if case["plannerDivergence"] is not None:
        tally[(case["tier"], "planner divergence")] += 1
        pytest.skip(f"planner divergence: {case['plannerDivergence']['reason']}")

    if leg == "http-eager":
        _assert_fitness(tag, case, renderings_under_another_hash_seed)

    entry = LEDGER.get(case["id"])
    if entry is not None and tag not in entry.get("pdp", [tag]):
        entry = None
    status = entry["status"] if entry else "pass"
    allowed = _held(case["allowed"])

    if status == "unsupported":
        with pytest.raises(UnsupportedPlanError):
            _translate(leg, case)
    else:
        returned = _execute(leg, _translate(leg, case))
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
        assert set(entry.get("pdp", PDP_TAGS)) <= set(PDP_TAGS), case_id
