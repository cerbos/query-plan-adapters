# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""The shared ``../conformance/`` corpus: the goldens, the dataset, and a stub PDP that serves
each recorded plan through the Cerbos SDK's own clients. Each adapter keeps its own copy of this
file on purpose (ADR 0007). Test-only.
"""

import atexit
import copy
import glob
import itertools
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import grpc
from cerbos.engine.v1 import engine_pb2
from cerbos.response.v1 import response_pb2
from cerbos.sdk.client import CerbosClient
from cerbos.sdk.grpc.client import CerbosClient as GrpcCerbosClient
from cerbos.sdk.model import (
    PlanResourcesExpression,
    PlanResourcesResponse,
    Principal,
    ResourceDesc,
)
from cerbos.svc.v1 import svc_pb2_grpc
from google.protobuf.json_format import ParseDict

CONFORMANCE_DIR = os.path.realpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "conformance")
)


_DATASET_FILES = {"seeds.json", "derived-fields.json", "resources.json"}


def read_corpus_json(name: str) -> Any:
    """A fuzz run recorded with generated rows carries its own dataset, which replaces the corpus's."""
    fuzz_dir = os.environ.get("QPA_FUZZ_DIR")
    base = CONFORMANCE_DIR
    if (
        fuzz_dir
        and name in _DATASET_FILES
        and os.path.exists(os.path.join(fuzz_dir, name))
    ):
        base = fuzz_dir
    with open(os.path.join(base, name), encoding="utf-8") as f:
        return json.load(f)


# -- the golden files -------------------------------------------------------

#: Both recorded PDPs, current first.
PDP_TAGS: list[str] = [
    read_corpus_json("pdp-versions.json")[which]["tag"]
    for which in ("current", "previous")
]


#: The directory a fuzz run was recorded into (conformance/README.md, "Fuzzing"). When it is
#: set, ``golden_cases`` reads the generated cases from it instead of the corpus, and the
#: harness judges each case with ``fuzz_verdict`` instead of the ledger: a refusal is always
#: acceptable, since the ledger has no entry for a shape nobody has written down yet.
FUZZ_DIR = os.environ.get("QPA_FUZZ_DIR") or None


def golden_cases(tag: str) -> list[dict[str, Any]]:
    """Every golden file recorded for one PDP tag (in fuzz mode, the run's), sorted by case id."""
    root = (
        os.path.join(FUZZ_DIR, tag)
        if FUZZ_DIR
        else os.path.join(CONFORMANCE_DIR, "golden", tag)
    )
    paths = glob.glob(os.path.join(root, "**", "*.json"), recursive=True)
    cases = []
    for path in paths:
        with open(path, encoding="utf-8") as f:
            cases.append(json.load(f))
    return sorted(cases, key=lambda case: case["id"])


def golden_case(case_id: str, tag: str | None = None) -> dict[str, Any]:
    """One golden file, from the current PDP unless ``tag`` says otherwise."""
    path = os.path.join(
        CONFORMANCE_DIR, "golden", tag or PDP_TAGS[0], case_id + ".json"
    )
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def fuzz_verdict(
    case: dict[str, Any], expected: list[str], returned: list[str]
) -> dict[str, Any]:
    """Judges the ids a translated filter returned against the ids the PDP allowed."""
    base = {"pdp": case["pdp"], "id": case["id"]}
    if sorted(returned) == sorted(expected):
        return {**base, "verdict": "match"}
    if set(returned) - set(expected):
        verdict, detail = "over-grant", None
    elif set(expected) - set(returned):
        verdict, detail = "under-grant", None
    else:
        verdict, detail = "error", "the right rows, but some of them repeated"
    out = {
        **base,
        "verdict": verdict,
        "expected": sorted(expected),
        "returned": sorted(returned),
    }
    return {**out, "detail": detail} if detail else out


def fuzz_thrown(
    case: dict[str, Any], error: BaseException, refusal: type
) -> dict[str, Any]:
    """A translation or query that raised, judged: the refusal type is the invariant working."""
    base = {"pdp": case["pdp"], "id": case["id"]}
    if isinstance(error, refusal):
        return {**base, "verdict": "refused"}
    return {**base, "verdict": "error", "detail": f"{type(error).__name__}: {error}"}


def write_fuzz_results(adapter: str, results: dict[str, list[dict[str, Any]]]) -> None:
    """Writes ``<FUZZ_DIR>/results/<adapter>-<store>.json`` per store, for ``fuzz-report``."""
    if not FUZZ_DIR:
        return
    out = os.path.join(FUZZ_DIR, "results")
    os.makedirs(out, exist_ok=True)
    for store, verdicts in results.items():
        verdicts = sorted(verdicts, key=lambda v: (v["id"], v["pdp"]))
        with open(
            os.path.join(out, f"{adapter}-{store}.json"), "w", encoding="utf-8"
        ) as f:
            json.dump(
                {"adapter": adapter, "store": store, "results": verdicts}, f, indent=2
            )
            f.write("\n")


_NOW_MINUS_24H = "__NOW_MINUS_24H__"


def now_minus_24h() -> str:
    """``now() - duration("24h")`` as the PDP folds it: RFC 3339 in nanoseconds."""
    ns = time.time_ns() - 24 * 3600 * 10**9
    seconds, fraction = divmod(ns, 10**9)
    # Sub-microsecond precision is why this adapter refuses these plans. Force it, so a clock
    # landing on a whole microsecond cannot hide that.
    if fraction % 1000 == 0:
        fraction += 1
    stamp = datetime.fromtimestamp(seconds, UTC).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{stamp}.{fraction:09d}Z"


def _substitute(node: Any, planned_at: str) -> Any:
    if isinstance(node, dict):
        return {k: _substitute(v, planned_at) for k, v in node.items()}
    if isinstance(node, list):
        return [_substitute(v, planned_at) for v in node]
    return planned_at if node == _NOW_MINUS_24H else node


def _response_dict(case: dict[str, Any], planned_at: str | None) -> dict[str, Any]:
    return {
        "requestId": "",
        "action": case["request"]["action"],
        "resourceKind": case["request"]["resourceKind"],
        "policyVersion": "default",
        "filter": _substitute(case["plan"], planned_at or now_minus_24h()),
    }


# -- the stub PDP -----------------------------------------------------------
# A golden reaches the adapter through the real SDK clients, so it arrives in the form a
# user passes. Only the PDP is replaced: a loopback server on each transport answers a
# PlanResources request with the response registered under the request's id, a token
# unique to one call, so concurrent and repeated calls never see each other's plan. The
# stub ignores the principal; it is the corpus's own only so the request is a real one.

_PRINCIPAL = read_corpus_json("seeds.json")["principal"]


class _StubPDP:
    def __init__(self) -> None:
        self._responses: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._tokens = itertools.count()
        self._http = self._start_http()
        self._grpc, grpc_port = self._start_grpc()
        http_port = self._http.server_address[1]
        self.http_client = CerbosClient(
            f"http://127.0.0.1:{http_port}", timeout_secs=30, raise_on_error=True
        )
        self.grpc_client = GrpcCerbosClient(
            f"127.0.0.1:{grpc_port}",
            tls_verify=False,
            timeout_secs=30,
            # The stub is on loopback; never route it through an ambient proxy.
            channel_options={"grpc.enable_http_proxy": 0},
        )
        atexit.register(self.close)

    def register(self, case: dict[str, Any], response: dict[str, Any]) -> str:
        token = f"{case['id']}#{next(self._tokens)}"
        with self._lock:
            self._responses[token] = response
        return token

    def take(self, token: str) -> dict[str, Any] | None:
        with self._lock:
            return self._responses.pop(token, None)

    def _start_http(self) -> ThreadingHTTPServer:
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                body = self.rfile.read(int(self.headers["Content-Length"]))
                response = (
                    stub.take(json.loads(body)["requestId"])
                    if self.path == "/api/plan/resources"
                    else None
                )
                status = 200
                if response is None:
                    status = 404
                    response = {"code": 5, "message": f"no stub for {self.path}"}
                payload = json.dumps(response).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args: Any) -> None:
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()
        return server

    def _start_grpc(self) -> tuple[grpc.Server, int]:
        stub = self

        class Servicer(svc_pb2_grpc.CerbosServiceServicer):
            def PlanResources(self, request, context):
                response = stub.take(request.request_id)
                if response is None:
                    context.abort(
                        grpc.StatusCode.NOT_FOUND, "no stub for this request id"
                    )
                return ParseDict(response, response_pb2.PlanResourcesResponse())

        server = grpc.server(ThreadPoolExecutor(max_workers=8))
        svc_pb2_grpc.add_CerbosServiceServicer_to_server(Servicer(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        return server, port

    def close(self) -> None:
        self.http_client.close()
        self.grpc_client.close()
        self._http.shutdown()
        self._grpc.stop(None)


_stub: _StubPDP | None = None
_stub_lock = threading.Lock()


def _stub_pdp() -> _StubPDP:
    global _stub
    with _stub_lock:
        if _stub is None:
            _stub = _StubPDP()
        return _stub


def plan_from_golden(
    case: dict[str, Any], planned_at: str | None = None
) -> PlanResourcesResponse:
    """The recorded plan, as the SDK's HTTP client returns it."""
    stub = _stub_pdp()
    token = stub.register(case, _response_dict(case, planned_at))
    return stub.http_client.plan_resources(
        case["request"]["action"],
        request_id=token,
        principal=Principal(id=_PRINCIPAL["id"], roles=set(_PRINCIPAL["roles"])),
        resource=ResourceDesc(case["request"]["resourceKind"]),
    )


def grpc_plan_from_golden(
    case: dict[str, Any], planned_at: str | None = None
) -> response_pb2.PlanResourcesResponse:
    """The same plan as the SDK's gRPC client returns it: the protobuf response."""
    # A real gRPC round trip, but the stub builds the protobuf from the golden's JSON, which
    # has already lost a `-0`'s sign: the PDP itself would send a signed double.
    stub = _stub_pdp()
    token = stub.register(case, _response_dict(case, planned_at))
    return stub.grpc_client.plan_resources(
        case["request"]["action"],
        request_id=token,
        principal=engine_pb2.Principal(id=_PRINCIPAL["id"], roles=_PRINCIPAL["roles"]),
        resource=engine_pb2.PlanResourcesInput.Resource(
            kind=case["request"]["resourceKind"]
        ),
    )


# -- fitness functions for "refuse, never best-effort" ----------------------

UNKNOWN_OPERATOR = "qpa-unknown-operator"


def _expressions(operand: Any):
    if isinstance(operand, PlanResourcesExpression):
        yield operand.expression
        for child in operand.expression.operands:
            yield from _expressions(child)


def operator_mutants(plan: PlanResourcesResponse):
    """One copy of the plan per operator node, each with that node's operator renamed to one no
    PDP emits. A translator that refuses every one of them has no dispatch that falls through to
    a filter for an operator it does not know."""
    condition = plan.filter.condition
    for index in range(sum(1 for _ in _expressions(condition))):
        mutant = copy.deepcopy(plan)
        list(_expressions(mutant.filter.condition))[index].operator = UNKNOWN_OPERATOR
        yield mutant


def reads_clock(case: dict[str, Any]) -> bool:
    """Whether the golden's plan holds timeSince, which a translator resolves against the clock."""

    def reads(node: Any) -> bool:
        if isinstance(node, dict):
            return node.get("operator") == "timeSince" or any(map(reads, node.values()))
        return isinstance(node, list) and any(map(reads, node))

    return reads(case["plan"])
