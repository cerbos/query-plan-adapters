"""Example application for ``cerbos-sqlalchemy``, run against the shared demo domain.

This is NOT a test of what the adapter translates -- ``../tests/test_adversarial_conformance.py``
proves that against a hostile corpus with a live PDP as the oracle, and
``../tests/test_translator.py`` pins the SQL it emits. This proves the two things every suite
under ``../tests`` structurally cannot:

1. **Packaging.** The ``cerbos_sqlalchemy`` import below resolves to the wheel ``pdm build``
   produced, which ``run.sh`` installs into this directory's own environment -- so the published
   surface is executed: which modules the distribution actually carries, and the ``Requires-Dist``
   metadata resolved against this example's pinned SQLAlchemy and Cerbos SDK. Every one of those
   suites imports the adapter from ``../src`` and touches none of it. See
   ``docs/adr/0002-examples-install-the-packed-artifact.md``.
2. **Usage shape.** A harness runs one flat filtered query. Consumers also paginate, and compose
   the adapter's ``Select`` with predicates of their own. Shape 5 below is the one that earns the
   exercise.

Prints one JSON document to stdout; everything a human might want to read goes to stderr.
``demo/scripts/run-example.sh`` diffs that document against ``demo/expected.json``.
"""

import json
import os
import sys
from typing import Any, Dict, List, TypedDict, cast

from cerbos.engine.v1 import engine_pb2
from cerbos.response.v1 import response_pb2
from cerbos.sdk.grpc.client import CerbosClient

from cerbos_sqlalchemy import get_query
from sqlalchemy import Boolean, Engine, Select, String, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

ADAPTER = "sqlalchemy"
RESOURCE_KIND = "document"

DEMO_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "..", "demo"))

# A file rather than ``:memory:``, so a failing run leaves the seeded rows behind to inspect. It
# is scratch state this example owns and .gitignore excludes, so each run starts from nothing by
# deleting it -- one unlink, rather than a reset against whatever database a config happens to
# name.
DB_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "demo.db")


class Base(DeclarativeBase):
    pass


class Document(Base):
    """The demo domain's one table, as a consumer would write it.

    Flat scalar columns, no relations. The column names are deliberately NOT the Cerbos
    attribute names: ``ownerId`` is ``owner_id`` here and ``public`` is ``is_public``, which is
    ordinary Python naming and is what makes the attribute map below load-bearing rather than
    decorative.
    """

    __tablename__ = "documents"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    owner_id: Mapped[str] = mapped_column(String)
    is_public: Mapped[bool] = mapped_column(Boolean)
    region: Mapped[str] = mapped_column(String)
    archived: Mapped[bool] = mapped_column(Boolean)


class SeedPrincipal(TypedDict):
    id: str
    roles: List[str]


class SeedApplicationFilter(TypedDict):
    """The predicate the APPLICATION owns, declared in the corpus and never in policy."""

    description: str
    archived: bool
    region: str


class SeedDocument(TypedDict):
    id: str
    ownerId: str
    public: bool
    region: str
    archived: bool


class Seeds(TypedDict):
    principals: List[SeedPrincipal]
    applicationFilter: SeedApplicationFilter
    documents: List[SeedDocument]


def read_seeds() -> Seeds:
    """The shared corpus rows, read rather than restated.

    ``demo/seeds.json`` is a repository-controlled file, checked structurally by
    ``demo/scripts/validate-demo.sh`` before this ever runs -- not untrusted input, which is why
    the shape above is declared and cast rather than validated here.
    """
    with open(os.path.join(DEMO_DIR, "seeds.json"), encoding="utf-8") as f:
        return cast(Seeds, json.load(f))


SEEDS = read_seeds()

#: Cerbos attribute names are not column names, so a consumer always writes one of these. Without
#: it the adapter has nothing to resolve ``request.resource.attr.ownerId`` to and raises -- which
#: is itself worth seeing in an example.
#:
#: ``region`` and ``archived`` are deliberately absent: no rule in ``demo/policies/document.yaml``
#: names them, and they exist so the application can own a predicate the policy never sees.
ATTR_MAP = {
    "request.resource.attr.ownerId": Document.owner_id,
    "request.resource.attr.public": Document.is_public,
}


def cerbos_host() -> str:
    """The address the shared runner published the demo PDP on.

    There is deliberately no fallback. The obvious default -- Cerbos's own 3592/3593 -- is where
    every adapter's ``cerbos run`` test sidecar listens, so an unset ``CERBOS_HOST`` would not
    fail: it would quietly plan against whatever policies that sidecar serves and produce a diff
    against ``demo/expected.json`` that reads as an adapter bug. ``demo/README.md`` requires
    reaching the PDP at ``$CERBOS_HOST``, "never a hardcoded address", for exactly that reason.
    """
    host = os.environ.get("CERBOS_HOST")
    if not host:
        raise SystemExit(
            "CERBOS_HOST is not set -- run this example through "
            "demo/scripts/run-example.sh sqlalchemy"
        )
    return host


def principal(principal_id: str) -> engine_pb2.Principal:
    """Look one principal up in the corpus rather than writing an id and its roles out here.

    The roles are the half that exists nowhere else: they are what the policy's rules are keyed
    on, so a copy here would go stale silently. ``demo/scripts/validate-demo.sh`` fails the build
    on a restated one.
    """
    for candidate in SEEDS["principals"]:
        if candidate["id"] == principal_id:
            return engine_pb2.Principal(id=candidate["id"], roles=candidate["roles"])
    raise SystemExit(f"demo/seeds.json declares no principal {principal_id!r}")


def plan_kind(plan: response_pb2.PlanResourcesResponse) -> str:
    """The plan kind exactly as ``demo/expected.json`` spells it.

    Reported alongside the ids because that is what stops this program returning all eight rows
    for ``admin-view`` without ever having reached the PDP.
    """
    kind = engine_pb2.PlanResourcesFilter.Kind.Name(plan.filter.kind)
    if kind == "KIND_UNSPECIFIED":
        raise SystemExit(
            "the PDP returned a plan with no kind -- is $CERBOS_HOST a Cerbos PDP?"
        )
    return kind


class Shapes:
    """The five usage shapes of the shared demo domain, against a real SQLAlchemy session.

    None of the five branches on the plan kind, and that is the adapter's design rather than an
    omission: ``get_query`` returns a ``Select`` for all three kinds -- ``select(table)`` for an
    unconditional allow, ``select(table).where(False)`` for an unconditional denial, and the
    translated tree for a conditional plan -- so composing in shape 5 is the same line of code
    whichever came back.
    """

    def __init__(self, client: CerbosClient, engine: Engine) -> None:
        self._client = client
        self._engine = engine

    def run(self) -> Dict[str, Any]:
        """Every shape, keyed the way ``demo/expected.json`` keys them."""
        return {
            "filtered": {
                "alice/view": self.filtered("alice", "view"),
                "bob/view": self.filtered("bob", "view"),
            },
            "alwaysAllowed": {
                "admin/admin-view": self.filtered("admin", "admin-view"),
            },
            "alwaysDenied": {
                "alice/publish": self.filtered("alice", "publish"),
            },
            "paginated": {
                "alice/view": self.paginated("alice", "view", 2),
                "admin/admin-view": self.paginated("admin", "admin-view", 3),
            },
            "composed": {
                "alice/view": self.composed("alice", "view"),
                "bob/view": self.composed("bob", "view"),
                "admin/admin-view": self.composed("admin", "admin-view"),
                "alice/publish": self.composed("alice", "publish"),
            },
        }

    # -- the five usage shapes --

    def filtered(self, principal_id: str, action: str) -> Dict[str, Any]:
        """Shapes 1, 2 and 3: a plain filtered list. The adapter's ``Select`` is the whole query."""
        plan = self._plan(principal_id, action)
        return {
            "kind": plan_kind(plan),
            "ids": self._ids(get_query(plan, Document, ATTR_MAP)),
        }

    def paginated(
        self, principal_id: str, action: str, page_size: int
    ) -> Dict[str, Any]:
        """Shape 4: ``.limit()``/``.offset()`` applied on top of the adapter's ``Select``.

        Reported as page SIZES plus the sorted union of the ids, never as per-page order:
        ``demo/expected.json`` is shared by every example and several of the stores have no total
        order to paginate by. The ``ORDER BY`` is still required for the paging itself to be
        correct -- without a total order, LIMIT/OFFSET may repeat or omit rows between pages --
        which is a separate concern from how the result is asserted.
        """
        plan = self._plan(principal_id, action)
        query = get_query(plan, Document, ATTR_MAP).order_by(Document.id)

        page_sizes: List[int] = []
        ids: List[str] = []
        offset = 0
        while True:
            page = self._ids(query.limit(page_size).offset(offset))
            if not page:
                break
            page_sizes.append(len(page))
            ids.extend(page)
            if len(page) < page_size:
                break
            offset += page_size

        return {
            "kind": plan_kind(plan),
            "pageSize": page_size,
            "pageSizes": page_sizes,
            "ids": sorted(ids),
        }

    def composed(self, principal_id: str, action: str) -> Dict[str, Any]:
        """Shape 5: the adapter's ``Select`` ANDed with the application's own predicate.

        Ordinary ``.where()`` chaining -- SQLAlchemy conjoins the criteria a ``Select`` already
        carries with the ones added later, so the application never has to know which kind of
        plan produced the query it was handed.

        All three plan kinds go through here on purpose. An ``ALWAYS_ALLOWED`` plan has no
        criteria to conjoin with (``select(table)``), and an ``ALWAYS_DENIED`` one must not have
        its denial undone: ``WHERE false AND <application predicate>`` is still false, and
        executing it is what actually demonstrates that, where short-circuiting on the kind would
        only demonstrate the branch.
        """
        application_filter = SEEDS["applicationFilter"]
        plan = self._plan(principal_id, action)
        return {
            "kind": plan_kind(plan),
            "ids": self._ids(
                get_query(plan, Document, ATTR_MAP).where(
                    Document.archived == application_filter["archived"],
                    Document.region == application_filter["region"],
                )
            ),
        }

    # -- plumbing --

    def _plan(
        self, principal_id: str, action: str
    ) -> response_pb2.PlanResourcesResponse:
        return self._client.plan_resources(
            action,
            principal(principal_id),
            engine_pb2.PlanResourcesInput.Resource(kind=RESOURCE_KIND),
        )

    def _ids(self, query: Select[Any]) -> List[str]:
        """Run one query in its own session, the way a request-scoped application would.

        Sorted, because ``demo/expected.json`` is: a SELECT with no ORDER BY has no defined row
        order, and SQLite returning insertion order is an implementation detail rather than a
        promise. Shape 4 sorts each page too, which costs it nothing -- it asserts page sizes and
        the union, never per-page order.
        """
        with Session(self._engine) as session:
            return sorted(document.id for document in session.scalars(query))


def seed(engine: Engine) -> None:
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add_all(
            Document(
                id=row["id"],
                owner_id=row["ownerId"],
                is_public=row["public"],
                region=row["region"],
                archived=row["archived"],
            )
            for row in SEEDS["documents"]
        )
        session.commit()


def main() -> None:
    host = cerbos_host()

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    engine = create_engine(f"sqlite+pysqlite:///{DB_PATH}")
    seed(engine)
    print(f"seeded {len(SEEDS['documents'])} documents", file=sys.stderr)

    with CerbosClient(host, tls_verify=False) as client:
        shapes = Shapes(client, engine).run()

    json.dump({"adapter": ADAPTER, "shapes": shapes}, sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
