"""Domain stress harness: replay conformance/domains/<domain>/golden against Polars frames.

Runs in the adapter's own environment (`pdm install -G :all` in ../../../../polars), which
installs cerbos_polars from that folder:

  ../../../../polars/.venv/bin/python harness.py [domain...]

Each domain's tables are loaded into frames and nested into the resource's shape the way an
application would join them: a to-one relation becomes a struct column, a to-many relation a list
column. Every plan is filtered eagerly and on the streaming engine, written to
conformance/domains/results/polars/<eager|streaming>.json.
"""

import glob
import importlib
import json
import os
import sys
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path[:0] = [HERE, os.path.join(HERE, "..", "..", "..", "..", "polars", "tests")]

from cerbos.sdk.model import Principal, ResourceDesc
from cerbos_polars import UnsupportedPlanError, get_filter
from corpus import _stub_pdp

from _frames import tables

DOMAINS_DIR = os.path.realpath(os.path.join(HERE, "..", ".."))
RESULTS_DIR = os.path.join(DOMAINS_DIR, "results", "polars")
DOMAINS = [
    "healthcare",
    "fintech",
    "logistics",
    "education",
    "legal",
    "retail",
    "hr",
    "media",
    "iot",
    "government",
]
LEGS = {
    "eager": lambda frame, expr: frame.collect().filter(expr),
    "streaming": lambda frame, expr: frame.filter(expr).collect(engine="streaming"),
}
STUB = _stub_pdp()


def goldens(domain):
    paths = glob.glob(os.path.join(DOMAINS_DIR, domain, "golden", "*", "*", "*.json"))
    cases = []
    for path in sorted(paths):
        with open(path) as f:
            cases.append(json.load(f))
    return cases


def plan_for(golden):
    response = {
        "requestId": "",
        "action": golden["action"],
        "resourceKind": golden["resourceKind"],
        "policyVersion": "default",
        "filter": golden["plan"],
    }
    token = STUB.register(
        {"id": f"{golden['domain']}/{golden['principalName']}"}, response
    )
    return STUB.http_client.plan_resources(
        golden["action"],
        request_id=token,
        principal=Principal(
            id=golden["principal"]["id"], roles=set(golden["principal"]["roles"])
        ),
        resource=ResourceDesc(golden["resourceKind"]),
    )


def run_case(module, frame, leg, golden):
    allowed = sorted(golden["allowed"])
    base = {
        "domain": golden["domain"],
        "pdp": golden["pdp"],
        "principal": golden["principalName"],
        "action": golden["action"],
        "allowed": allowed,
    }
    try:
        expr = get_filter(
            plan_for(golden),
            frame,
            module.ATTR_MAP,
            null_attribute_representation=getattr(module, "NULLS", "omitted"),
        )
        returned = LEGS[leg](frame, expr).get_column(module.ID).to_list()
    except UnsupportedPlanError as error:
        return {
            **base,
            "verdict": "refused",
            "returned": [],
            "detail": str(error)[:400],
        }
    except Exception as error:  # noqa: BLE001
        if os.environ.get("DOMAIN_TRACE"):
            traceback.print_exc()
        return {
            **base,
            "verdict": "error",
            "returned": [],
            "detail": f"{type(error).__name__}: {error}"[:400],
        }
    got = sorted(returned)
    if set(got) - set(allowed):
        verdict, detail = "over-grant", f"extra: {sorted(set(got) - set(allowed))}"
    elif set(allowed) - set(got):
        verdict, detail = "under-grant", f"missing: {sorted(set(allowed) - set(got))}"
    elif len(got) != len(set(got)):
        verdict, detail = "error", "the right rows, but some of them repeated"
    else:
        verdict, detail = "pass", ""
    return {**base, "verdict": verdict, "returned": sorted(set(got)), "detail": detail}


def main(argv):
    domains = argv or DOMAINS
    results = {leg: [] for leg in LEGS}
    for domain in domains:
        module = importlib.import_module(domain)
        with open(os.path.join(DOMAINS_DIR, domain, "generated", "rows.json")) as f:
            frame = module.frame(tables(json.load(f)))
        for leg in LEGS:
            for golden in goldens(domain):
                results[leg].append(run_case(module, frame, leg, golden))
    os.makedirs(RESULTS_DIR, exist_ok=True)
    for leg, rows in results.items():
        out = {"adapter": "polars", "store": leg, "results": rows}
        if not os.environ.get("DOMAIN_NO_WRITE"):
            with open(os.path.join(RESULTS_DIR, f"{leg}.json"), "w") as f:
                json.dump(out, f, indent=2)
                f.write("\n")
        tally = {}
        for r in rows:
            tally[r["verdict"]] = tally.get(r["verdict"], 0) + 1
        print(leg, tally)
        for r in rows:
            if r["verdict"] != "pass":
                print(
                    f"  {r['verdict']:11} {r['domain']}/{r['pdp']}/{r['principal']}/{r['action']}: {r['detail'][:300]}"
                )


if __name__ == "__main__":
    main(sys.argv[1:])
