"""Domain stress harness: replay conformance/domains/<domain>/golden against DuckDB.

Runs in the adapter's own environment (`./pw install` in ../../../../duckdb), which installs
cerbos_duckdb from that folder:

  ../../../../duckdb/.venv/bin/python harness.py [domain...]

DuckDB runs in-process, so there is no store to start. Each domain loads generated/rows.json into
tables and builds views that aggregate its child tables into LIST columns, the way the adapter's
README tells an application to. Two store legs run: `duckdb` (defaults) and `duckdb-hostile`
(a case-folding default collation, a far-from-UTC session time zone and naive UTC
timestamps). Each writes
conformance/domains/results/duckdb/<store>.json.
"""

import glob
import importlib
import json
import os
import sys
import traceback
from datetime import datetime
from decimal import Decimal

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path[:0] = [HERE, os.path.join(HERE, "..", "..", "..", "..", "duckdb", "tests")]

import duckdb
from cerbos.sdk.model import Principal, ResourceDesc
from cerbos_duckdb import (
    NullConvention,
    NullRepresentation,
    UnsupportedPlanError,
    translate,
)
from corpus import _stub_pdp

DOMAINS_DIR = os.path.realpath(os.path.join(HERE, "..", ".."))
RESULTS_DIR = os.path.join(DOMAINS_DIR, "results", "duckdb")
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
STORES = {
    "duckdb": (),
    "duckdb-hostile": (
        "SET default_collation = 'nocase'",
        "SET TimeZone = 'Pacific/Chatham'",
    ),
}
TYPES = {
    "string": "VARCHAR",
    "bool": "BOOLEAN",
    "int": "INTEGER",
    "bigint": "BIGINT",
    "float": "DOUBLE",
    "decimal": "DECIMAL(18, 4)",
    "timestamp": "TIMESTAMPTZ",
    "json": "JSON",
    "string[]": "VARCHAR[]",
}


def _quote(name):
    return '"' + name.replace('"', '""') + '"'


def _convert(value, kind):
    if value is None:
        return None
    if kind == "timestamp":
        return datetime.fromisoformat(value)
    if kind == "decimal":
        return Decimal(str(value))
    if kind == "json":
        return json.dumps(value)
    return value


def _key(column, pk):
    return column["name"] in ([pk] if isinstance(pk, str) else pk) or bool(
        column.get("references")
    )


def load(con, module, domain, hostile):
    with open(os.path.join(DOMAINS_DIR, domain, "generated", "rows.json")) as f:
        spec = json.load(f)
    for table in spec["tables"]:
        columns = table["columns"]
        pk = table["primaryKey"]
        pk = [pk] if isinstance(pk, str) else pk
        ddl = ", ".join(
            f"{_quote(c['name'])} {'TIMESTAMP' if hostile and c['type'] == 'timestamp' else TYPES[c['type']]}"
            + (
                ' COLLATE "binary"'
                if hostile and c["type"] == "string" and _key(c, pk)
                else ""
            )
            + ("" if c["nullable"] else " NOT NULL")
            for c in columns
        )
        con.execute(
            f"CREATE TABLE {_quote(table['name'])} ({ddl}, PRIMARY KEY ({', '.join(map(_quote, pk))}))"
        )
        names = [c["name"] for c in columns]
        if table["rows"]:
            con.executemany(
                f"INSERT INTO {_quote(table['name'])} VALUES ({', '.join('?' for _ in names)})",
                [
                    [_convert(row.get(c["name"]), c["type"]) for c in columns]
                    for row in table["rows"]
                ],
            )
    con.execute(module.VIEWS)
    return spec["nulls"]


def goldens(domain):
    cases = []
    for path in sorted(
        glob.glob(os.path.join(DOMAINS_DIR, domain, "golden", "*", "*", "*.json"))
    ):
        with open(path) as f:
            cases.append(json.load(f))
    return cases


def plan_for(golden):
    stub = _stub_pdp()
    response = {
        "requestId": "",
        "action": golden["action"],
        "resourceKind": golden["resourceKind"],
        "policyVersion": "default",
        "filter": golden["plan"],
    }
    token = stub.register(
        {"id": f"{golden['domain']}/{golden['principalName']}"}, response
    )
    return stub.http_client.plan_resources(
        golden["action"],
        request_id=token,
        principal=Principal(
            id=golden["principal"]["id"], roles=set(golden["principal"]["roles"])
        ),
        resource=ResourceDesc(golden["resourceKind"]),
    )


def run_case(con, module, nulls, golden):
    allowed = sorted(golden["allowed"])
    base = {
        "domain": golden["domain"],
        "pdp": golden["pdp"],
        "principal": golden["principalName"],
        "action": golden["action"],
        "allowed": allowed,
    }
    convention = (
        NullConvention.EXPLICIT if nulls == "explicit" else NullConvention.OMITTED
    )
    try:
        result = translate(
            plan_for(golden),
            module.ROOT,
            module.mapping(convention),
            null_representation=NullRepresentation(nulls),
        )
        rows = con.execute(
            f"SELECT {_quote(module.ID)} FROM {module.ROOT} WHERE {result.sql}",
            result.params,
        ).fetchall()
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
    got = sorted(row[0] for row in rows)
    if set(got) - set(allowed):
        verdict, detail = "over-grant", f"extra: {sorted(set(got) - set(allowed))}"
    elif set(allowed) - set(got):
        verdict, detail = "under-grant", f"missing: {sorted(set(allowed) - set(got))}"
    elif len(got) != len(set(got)):
        verdict, detail = "error", "the right rows, but some of them repeated"
    else:
        verdict, detail = "pass", ""
    if verdict != "pass":
        detail = f"{detail} {result.sql} {result.params}"[:400]
    return {**base, "verdict": verdict, "returned": sorted(set(got)), "detail": detail}


def main(argv):
    domains = argv or DOMAINS
    os.makedirs(RESULTS_DIR, exist_ok=True)
    for store, settings in STORES.items():
        rows = []
        for domain in domains:
            module = importlib.import_module(domain)
            con = duckdb.connect()
            nulls = load(con, module, domain, bool(settings))
            for setting in settings:
                con.execute(setting)
            rows.extend(
                run_case(con, module, nulls, golden) for golden in goldens(domain)
            )
            con.close()
        out = {"adapter": "duckdb", "store": store, "results": rows}
        if not os.environ.get("DOMAIN_NO_WRITE"):
            with open(os.path.join(RESULTS_DIR, f"{store}.json"), "w") as f:
                json.dump(out, f, indent=2)
                f.write("\n")
        tally = {}
        for r in rows:
            tally[r["verdict"]] = tally.get(r["verdict"], 0) + 1
        print(store, tally)
        for r in rows:
            if r["verdict"] != "pass":
                print(
                    f"  {r['verdict']:11} {r['domain']}/{r['pdp']}/{r['principal']}/{r['action']}: {r['detail'][:300]}"
                )


if __name__ == "__main__":
    main(sys.argv[1:])
