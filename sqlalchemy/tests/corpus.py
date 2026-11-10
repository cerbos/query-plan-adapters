# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""The shared ``../conformance/`` corpus, schema and mapping for both corpus suites.

Both suites must use the same mapping: the harness replays recorded plans against real
stores, and the translator unit test asks what a store cannot. Each adapter keeps its own
copy of this file on purpose (ADR 0007). Test-only.
"""

import atexit
import glob
import itertools
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import grpc
from cerbos.engine.v1 import engine_pb2
from cerbos.response.v1 import response_pb2
from cerbos.sdk.client import CerbosClient
from cerbos.sdk.grpc.client import CerbosClient as GrpcCerbosClient
from cerbos.sdk.model import PlanResourcesResponse, Principal, ResourceDesc
from cerbos.svc.v1 import svc_pb2_grpc
from google.protobuf.json_format import ParseDict
from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    literal,
    select,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import declarative_base

from cerbos_sqlalchemy import CollectionColumn, Relation, relation_overrides

CONFORMANCE_DIR = os.path.realpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "conformance")
)


def read_corpus_json(name: str) -> Any:
    with open(os.path.join(CONFORMANCE_DIR, name), encoding="utf-8") as f:
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
    stamp = datetime.fromtimestamp(seconds, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
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


# -- schema -----------------------------------------------------------------
# Dedicated tables, so hostile seeds (NULL elements, duplicate names, LIKE
# metacharacters) are all representable.

AdvBase = declarative_base()

# MySQL's VARCHAR needs a length. Every corpus string fits, and the other stores ignore it.
_STRING = String(255)

# Ordered copies of the collections for `collection_columns` (#227). `none_as_null` makes an
# absent collection SQL NULL, not the JSON document `null`.
_COLLECTION_JSON = JSON(none_as_null=True).with_variant(
    JSONB(none_as_null=True), "postgresql"
)
# The same collections as PostgreSQL arrays, used only by the PostgreSQL leg. SQLite has no
# array type, so there the variant is unused JSON.
_COLLECTION_ARRAY = JSON(none_as_null=True).with_variant(ARRAY(String), "postgresql")
_NUMBER_ARRAY = JSON(none_as_null=True).with_variant(ARRAY(Integer), "postgresql")
_BOOL_ARRAY = JSON(none_as_null=True).with_variant(ARRAY(Boolean), "postgresql")


# MySQL's DATETIME defaults to whole seconds, which would store a5 on the literal it passes. A
# subclass, not with_variant: SQLAlchemy 1.4's Variant is not a DateTime to the adapter.
class _Timestamp(DateTime):
    pass


@compiles(_Timestamp, "mysql")
def _mysql_timestamp(type_, compiler, **kw):
    return "DATETIME(6)"


_TIMESTAMP = _Timestamp(timezone=True)


class AdvResource(AdvBase):
    __tablename__ = "adversarial_resource"

    id = Column(_STRING, primary_key=True)
    # Nullable: seeds j1, j2 and j3 each leave one of them NULL, a missing attribute (#488).
    a_bool = Column(Boolean, nullable=True)
    a_string = Column(_STRING, nullable=True)
    a_number = Column(Integer, nullable=True)
    a_double = Column(Float(precision=53), nullable=True)
    a_optional_string = Column(_STRING, nullable=True)
    created_by = Column(_STRING, nullable=False)
    scope = Column(_STRING, nullable=True)
    created_at = Column(_TIMESTAMP, nullable=True)
    updated_at = Column(_TIMESTAMP, nullable=True)
    tags_json = Column(_COLLECTION_JSON, nullable=True)
    tag_names_json = Column(_COLLECTION_JSON, nullable=True)
    main_sub_categories_json = Column(_COLLECTION_JSON, nullable=True)
    tags_array = Column(_COLLECTION_ARRAY, nullable=True)
    tag_names_array = Column(_COLLECTION_ARRAY, nullable=True)
    main_sub_categories_array = Column(_COLLECTION_ARRAY, nullable=True)
    # Number and boolean lists prove an element's JSON type survives comparison.
    # No relation backs them; the column is their only storage.
    a_number_list_json = Column(_COLLECTION_JSON, nullable=True)
    a_bool_list_json = Column(_COLLECTION_JSON, nullable=True)
    a_number_list_array = Column(_NUMBER_ARRAY, nullable=True)
    a_bool_list_array = Column(_BOOL_ARRAY, nullable=True)


class AdvTag(AdvBase):
    __tablename__ = "adversarial_tag"

    pk = Column(Integer, primary_key=True, autoincrement=True)
    tag_id = Column(_STRING, nullable=False)
    name = Column(_STRING, nullable=True)
    resource_id = Column(_STRING, ForeignKey("adversarial_resource.id"), nullable=False)


class AdvCategory(AdvBase):
    __tablename__ = "adversarial_category"

    id = Column(_STRING, primary_key=True)
    name = Column(_STRING, nullable=False)
    resource_id = Column(_STRING, ForeignKey("adversarial_resource.id"), nullable=False)


class AdvSubCategory(AdvBase):
    __tablename__ = "adversarial_sub_category"

    id = Column(_STRING, primary_key=True)
    name = Column(_STRING, nullable=False)
    category_id = Column(_STRING, ForeignKey("adversarial_category.id"), nullable=False)


class AdvLabel(AdvBase):
    __tablename__ = "adversarial_label"

    id = Column(_STRING, primary_key=True)
    name = Column(_STRING, nullable=True)
    sub_category_id = Column(
        _STRING, ForeignKey("adversarial_sub_category.id"), nullable=False
    )


# The corpus's one real to-one relation (ADR 0005). Unlike `obj.inner`, `parent` and
# `parent.inner` are separate rows. The unique foreign key makes it to-one.
class AdvParent(AdvBase):
    __tablename__ = "adversarial_parent"

    id = Column(_STRING, primary_key=True)
    a_bool = Column(Boolean, nullable=False)
    a_string = Column(_STRING, nullable=False)
    a_number = Column(Integer, nullable=False)
    a_optional_string = Column(_STRING, nullable=True)
    resource_id = Column(
        _STRING, ForeignKey("adversarial_resource.id"), nullable=False, unique=True
    )


class AdvInner(AdvBase):
    __tablename__ = "adversarial_inner"

    id = Column(_STRING, primary_key=True)
    a_bool = Column(Boolean, nullable=False)
    a_string = Column(_STRING, nullable=False)
    a_number = Column(Integer, nullable=False)
    a_optional_string = Column(_STRING, nullable=True)
    parent_id = Column(
        _STRING, ForeignKey("adversarial_parent.id"), nullable=False, unique=True
    )


# -- relations -----------------------------------------------------------------
# ATTR_MAP points relation attributes at the shipped `Relation`, and the shipped
# `relation_overrides()` translate macros over them, so the corpus proves the code
# the README hands consumers (#527).

TAGS = Relation(
    [AdvTag.resource_id == AdvResource.id],
    # The resource may be several lambdas up: collection/exists/outer-collection-inside-lambda
    # nests tags in categories.
    correlate=[AdvResource],
)
TAG_NAMES = Relation(
    [AdvTag.resource_id == AdvResource.id],
    correlate=[AdvResource],
    member=AdvTag.name,
)
CATEGORIES = Relation(
    [AdvCategory.resource_id == AdvResource.id],
    correlate=[AdvResource],
)
# Correlates to the enclosing category, but the body may read resource columns
# (collection/exists/nested-with-outer-attribute), so the resource correlates too.
SUB_OF_CATEGORY = Relation(
    [AdvSubCategory.category_id == AdvCategory.id],
    correlate=[AdvCategory, AdvResource],
)
LABELS_OF_SUB = Relation(
    [AdvLabel.sub_category_id == AdvSubCategory.id],
    correlate=[AdvSubCategory, AdvCategory, AdvResource],
)
# The same two-hop chain from the root. The category hop stays in the subquery's
# FROM; only the resource correlates.
MAIN_SUB = Relation(
    [
        AdvSubCategory.category_id == AdvCategory.id,
        AdvCategory.resource_id == AdvResource.id,
    ],
    correlate=[AdvResource],
    hop_correlation=[AdvCategory.resource_id == AdvResource.id],
)
MAIN_SUBNAMES = Relation(
    [
        AdvSubCategory.category_id == AdvCategory.id,
        AdvCategory.resource_id == AdvResource.id,
    ],
    correlate=[AdvResource],
    member=AdvSubCategory.name,
    hop_correlation=[AdvCategory.resource_id == AdvResource.id],
)


class _ToOneRow:
    """The to-one `parent` read whole: a map-valued attribute, stored as a row.

    A macro or `in` over it ranges over the keys of a map, and SQL cannot read which
    of a row's columns are non-NULL as those keys, so the overrides refuse it.
    """

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return "_ToOneRow(parent)"


PARENT = _ToOneRow()

OPERATOR_OVERRIDES = relation_overrides()

# Collection storage, read only by `size()` and `index` (#227). Collections with a relation
# keep their ATTR_MAP marker too, since the macros still use the relation.
# `mainCategory.subCategories` is NULL when absent, so `size() >= 0` must exclude it
# (relation/size/non-negative-to-one-chain); `tags` covers the empty-but-present case.
COLLECTION_COLUMNS = {
    "request.resource.attr.tags": CollectionColumn(AdvResource.tags_json, "json"),
    "request.resource.attr.tagNames": CollectionColumn(
        AdvResource.tag_names_json, "json"
    ),
    "request.resource.attr.mainCategory.subCategories": CollectionColumn(
        AdvResource.main_sub_categories_json, "json"
    ),
    "request.resource.attr.aNumberList": CollectionColumn(
        AdvResource.a_number_list_json, "json"
    ),
    "request.resource.attr.aBoolList": CollectionColumn(
        AdvResource.a_bool_list_json, "json"
    ),
}

#: The same five as PostgreSQL arrays. Only the harness's PostgreSQL leg uses this.
PG_ARRAY_COLLECTION_COLUMNS = {
    "request.resource.attr.tags": CollectionColumn(AdvResource.tags_array, "pgArray"),
    "request.resource.attr.tagNames": CollectionColumn(
        AdvResource.tag_names_array, "pgArray"
    ),
    "request.resource.attr.mainCategory.subCategories": CollectionColumn(
        AdvResource.main_sub_categories_array, "pgArray"
    ),
    "request.resource.attr.aNumberList": CollectionColumn(
        AdvResource.a_number_list_array, "pgArray"
    ),
    "request.resource.attr.aBoolList": CollectionColumn(
        AdvResource.a_bool_list_array, "pgArray"
    ),
}


def reads_declared_collection(case: dict[str, Any]) -> bool:
    """Whether a golden case's plan reads a declared collection's storage."""
    # `size()`/`index` over a declared collection, or `in`/`hasIntersection` over one not in
    # ATTR_MAP. Read off the plan, so a new corpus case joins the PostgreSQL leg unasked.

    def walk(node: Any) -> bool:
        if not isinstance(node, dict):
            return False
        expression = node.get("expression")
        if expression is None:
            return False
        operands = expression.get("operands", [])
        if (
            expression["operator"] in ("size", "index")
            and operands
            and operands[0].get("variable") in COLLECTION_COLUMNS
        ):
            return True
        if expression["operator"] in ("in", "hasIntersection") and any(
            operand.get("variable") in COLLECTION_COLUMNS
            and operand.get("variable") not in ATTR_MAP
            for operand in operands
        ):
            return True
        return any(walk(operand) for operand in operands)

    return walk((case["plan"] or {}).get("condition", {}))


# `owner` and `coOwner` reuse columns under the other null convention: the corpus
# sends an explicit null instead of omitting the attribute (#308). Every attribute
# resources.json omits when its column is NULL is declared omitted, so `== null`
# against one is a CEL error, not a match (#302, #488, #528); `obj.inner` aliases
# `aString`, and a `parent` hop's NULL column is omitted like an absent level.
ATTRIBUTE_NULL_REPRESENTATION = {
    "request.resource.attr.aBool": "omitted",
    "request.resource.attr.aString": "omitted",
    "request.resource.attr.aNumber": "omitted",
    "request.resource.attr.obj.inner": "omitted",
    "request.resource.attr.aOptionalString": "omitted",
    "request.resource.attr.aDouble": "omitted",
    "request.resource.attr.scope": "omitted",
    "request.resource.attr.createdAt": "omitted",
    "request.resource.attr.updatedAt": "omitted",
    "request.resource.attr.parent.aBool": "omitted",
    "request.resource.attr.parent.aString": "omitted",
    "request.resource.attr.parent.aNumber": "omitted",
    "request.resource.attr.parent.aOptionalString": "omitted",
    "request.resource.attr.parent.inner.aBool": "omitted",
    "request.resource.attr.parent.inner.aString": "omitted",
    "request.resource.attr.parent.inner.aNumber": "omitted",
    "request.resource.attr.parent.inner.aOptionalString": "omitted",
    "tagName": "explicit",
    "t": "explicit",
    # A nameless tag sends no `name` key (resources.json), so `has(t.name)` is a
    # NULL test on the element column.
    "t.name": "omitted",
    "request.resource.attr.owner": "explicit",
    "request.resource.attr.coOwner": "explicit",
}


def _parent_scalar(column):
    """One scalar of the to-one `parent`, as a correlated scalar subquery."""
    # A missing parent gives NULL, which stays excluded under negation too, so no
    # `require_hops` guard is needed (#375).
    return (
        select(column)
        .where(AdvParent.resource_id == AdvResource.id)
        .correlate(AdvResource)
        .scalar_subquery()
    )


def _inner_scalar(column):
    """The same for `parent.inner`, looked up through the parent."""
    return (
        select(column)
        .where(
            AdvInner.parent_id
            == select(AdvParent.id)
            .where(AdvParent.resource_id == AdvResource.id)
            .correlate(AdvResource)
            .scalar_subquery()
        )
        .correlate(AdvResource)
        .scalar_subquery()
    )


ATTR_MAP = {
    # Not under `attr` (the `identifier/*` cases).
    "request.resource.id": AdvResource.id,
    "request.resource.attr.aBool": AdvResource.a_bool,
    "request.resource.attr.aString": AdvResource.a_string,
    "request.resource.attr.aNumber": AdvResource.a_number,
    "request.resource.attr.aDouble": AdvResource.a_double,
    "request.resource.attr.aOptionalString": AdvResource.a_optional_string,
    "request.resource.attr.createdBy": AdvResource.created_by,
    "request.resource.attr.owner": AdvResource.a_optional_string,
    "request.resource.attr.coOwner": AdvResource.scope,
    "request.resource.attr.scope": AdvResource.scope,
    "request.resource.attr.createdAt": AdvResource.created_at,
    "request.resource.attr.updatedAt": AdvResource.updated_at,
    # Not a real nested column; aliases aString for comparison/equals/nested-map-member.
    "request.resource.attr.obj.inner": AdvResource.a_string,
    # The real to-one chain (`relation/*` cases), as correlated scalar subqueries.
    # Read whole, the parent is a map; a macro over its keys is refused.
    "request.resource.attr.parent": PARENT,
    "request.resource.attr.parent.aBool": _parent_scalar(AdvParent.a_bool),
    "request.resource.attr.parent.aString": _parent_scalar(AdvParent.a_string),
    "request.resource.attr.parent.aNumber": _parent_scalar(AdvParent.a_number),
    "request.resource.attr.parent.aOptionalString": _parent_scalar(
        AdvParent.a_optional_string
    ),
    "request.resource.attr.parent.inner.aBool": _inner_scalar(AdvInner.a_bool),
    "request.resource.attr.parent.inner.aString": _inner_scalar(AdvInner.a_string),
    "request.resource.attr.parent.inner.aNumber": _inner_scalar(AdvInner.a_number),
    "request.resource.attr.parent.inner.aOptionalString": _inner_scalar(
        AdvInner.a_optional_string
    ),
    "request.resource.attr.tags": TAGS,
    "request.resource.attr.tagNames": TAG_NAMES,
    # `t` binds a tag row over `tags`, read only as `t.id`/`t.name`, and a tag name over
    # `tagNames`, read bare (collection/all/nested-all-over-literal-list). Bare, it is the
    # name; a lambda's binding operand is never resolved to SQL.
    "t": AdvTag.name,
    "tagName": AdvTag.name,
    "t.id": AdvTag.tag_id,
    "t.name": AdvTag.name,
    "request.resource.attr.categories": CATEGORIES,
    "c": CATEGORIES,
    # Only relation/or/two-hops-or-collection-exists reads the category's own name.
    "c.name": AdvCategory.name,
    "c.subCategories": SUB_OF_CATEGORY,
    "s": SUB_OF_CATEGORY,
    "s.name": AdvSubCategory.name,
    "s.labels": LABELS_OF_SUB,
    "l": LABELS_OF_SUB,
    "l.name": AdvLabel.name,
    # A key of the parent map (collection/exists/map-keys). It has no column: the
    # body translates, and the `exists` override then refuses the map it ranges over.
    "k": literal(None, String),
    "request.resource.attr.mainCategory.subCategories": MAIN_SUB,
    "request.resource.attr.mainCategory.subNames": MAIN_SUBNAMES,
}


# -- rendering an emitted Select --------------------------------------------


def dialect(name: str):
    from sqlalchemy.dialects import postgresql, sqlite

    return {"sqlite": sqlite.dialect(), "postgresql": postgresql.dialect()}[name]


def render(query, dialect_name: str) -> tuple[str, dict[str, Any]]:
    """Compile ``query`` for one dialect, as ``(statement, parameters)``."""
    # Compile the whole Select, not the WHERE clause: correlation only shows inside it.
    compiled = query.compile(
        dialect=dialect(dialect_name), compile_kwargs={"render_postcompile": True}
    )
    return " ".join(str(compiled).split()), dict(compiled.params)
