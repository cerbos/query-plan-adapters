"""Domain stress harness: replay domains/<domain>/golden against real stores.

Usage: pdm run domains <sqlite|postgresql|mysql> [domain...]
PostgreSQL and MySQL read their URL from DOMAIN_PG_URL / DOMAIN_MYSQL_URL, e.g.

  docker run -d --name dom-py-postgres -p 25432:5432 -e POSTGRES_PASSWORD=test \
    -e POSTGRES_DB=domains -e POSTGRES_INITDB_ARGS="--locale-provider=icu --icu-locale=en-US" \
    "$(cat POSTGRES_IMAGE)"
  DOMAIN_PG_URL=postgresql+psycopg2://postgres:test@127.0.0.1:25432/domains pdm run domains postgresql

  docker run -d --name dom-py-mysql -p 23306:3306 -e MYSQL_ROOT_PASSWORD=test \
    -e MYSQL_DATABASE=domains "$(cat MYSQL_IMAGE)"
  DOMAIN_MYSQL_URL=mysql+pymysql://root:test@127.0.0.1:23306/domains pdm run domains mysql

Each store writes domains/results/sqlalchemy/<store>.json (the hand-declared mapping) and
<store>+schema.json (the mapping inferred from the models' relationships).
"""

import glob
import importlib
import json
import os
import sys
import traceback
import warnings
from datetime import datetime
from decimal import Decimal

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path[:0] = [HERE, os.path.join(HERE, "..", "tests")]

from corpus import _stub_pdp  # noqa: E402
from cerbos.sdk.model import Principal, ResourceDesc  # noqa: E402
from sqlalchemy import create_engine, event, insert  # noqa: E402
from sqlalchemy.exc import SAWarning  # noqa: E402

from cerbos_sqlalchemy import UnsupportedPlanError, get_query  # noqa: E402

DOMAINS_DIR = os.path.realpath(os.path.join(HERE, "..", "..", "domains"))
RESULTS_DIR = os.path.join(DOMAINS_DIR, "results", "sqlalchemy")
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
STYLES = {"declared": "", "inferred": "+schema"}


def _case_sensitive_like(dbapi_conn, _):
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA case_sensitive_like = ON")
    cursor.close()


def engine_for(store):
    if store == "sqlite":
        engine = create_engine("sqlite://")
        event.listen(engine, "connect", _case_sensitive_like)
        return engine
    if store == "postgresql":
        return create_engine(os.environ["DOMAIN_PG_URL"])
    if store == "mysql":
        return create_engine(os.environ["DOMAIN_MYSQL_URL"])
    raise SystemExit(f"unknown store {store}")


def _convert(value, kind):
    if value is None:
        return None
    if kind == "timestamp":
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    if kind == "decimal":
        return Decimal(str(value))
    return value


def load(engine, module, domain):
    with open(os.path.join(DOMAINS_DIR, domain, "generated", "rows.json")) as f:
        spec = json.load(f)
    metadata = module.Base.metadata
    metadata.drop_all(engine)
    metadata.create_all(engine)
    with engine.begin() as conn:
        for table in spec["tables"]:
            kinds = {c["name"]: c["type"] for c in table["columns"]}
            rows = [
                {k: _convert(v, kinds[k]) for k, v in row.items()}
                for row in table["rows"]
            ]
            if rows:
                conn.execute(insert(metadata.tables[table["name"]]), rows)


def goldens(domain):
    paths = glob.glob(os.path.join(DOMAINS_DIR, domain, "golden", "*", "*", "*.json"))
    cases = []
    for path in sorted(paths):
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
    token = stub.register({"id": f"{golden['domain']}/{golden['principalName']}"}, response)
    return stub.http_client.plan_resources(
        golden["action"],
        request_id=token,
        principal=Principal(
            id=golden["principal"]["id"], roles=set(golden["principal"]["roles"])
        ),
        resource=ResourceDesc(golden["resourceKind"]),
    )


def run_case(engine, module, style, golden, dialect):
    pk = list(module.ROOT.__table__.primary_key.columns)[0].name
    allowed = sorted(golden["allowed"])
    base = {
        "domain": golden["domain"],
        "pdp": golden["pdp"],
        "principal": golden["principalName"],
        "action": golden["action"],
        "allowed": allowed,
    }
    try:
        query = get_query(plan_for(golden), module.ROOT, **module.mapping(style, dialect))
        with warnings.catch_warnings():
            warnings.filterwarnings("error", message=".*cartesian product.*", category=SAWarning)
            with engine.connect() as conn:
                returned = [row._mapping[pk] for row in conn.execute(query)]
    except UnsupportedPlanError as error:
        return {**base, "verdict": "refused", "returned": [], "detail": str(error)[:400]}
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
    store, domains = argv[0], argv[1:] or DOMAINS
    engine = engine_for(store)
    dialect = engine.dialect.name
    results = {style: [] for style in STYLES}
    for domain in domains:
        module = importlib.import_module(domain)
        load(engine, module, domain)
        for style in STYLES:
            for golden in goldens(domain):
                results[style].append(run_case(engine, module, style, golden, dialect))
        module.Base.metadata.drop_all(engine)
    os.makedirs(RESULTS_DIR, exist_ok=True)
    for style, suffix in STYLES.items():
        name = store + suffix
        rows = results[style]
        out = {
            "adapter": "sqlalchemy",
            "store": name,
            "results": [
                {
                    k: r[k]
                    for k in (
                        "domain",
                        "pdp",
                        "principal",
                        "action",
                        "verdict",
                        "returned",
                        "allowed",
                        "detail",
                    )
                }
                for r in rows
            ],
        }
        if not os.environ.get("DOMAIN_NO_WRITE"):
            with open(os.path.join(RESULTS_DIR, f"{name}.json"), "w") as f:
                json.dump(out, f, indent=2)
                f.write("\n")
        tally = {}
        for r in rows:
            tally[r["verdict"]] = tally.get(r["verdict"], 0) + 1
        print(name, tally)
        for r in rows:
            if r["verdict"] != "pass":
                print(
                    f"  {r['verdict']:11} {r['domain']}/{r['pdp']}/{r['principal']}/{r['action']}: {r['detail'][:300]}"
                )


if __name__ == "__main__":
    main(sys.argv[1:])
