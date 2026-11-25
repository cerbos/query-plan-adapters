# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Example app for ``cerbos-polars`` on the shared demo domain.

Proves packaging (it imports the built wheel) and usage shapes, not semantics.
Prints one JSON document to stdout, which ``demo/scripts/run-example.sh`` diffs.
"""

import json
import os
import sys
from typing import Any, TypedDict, cast

import polars as pl
from cerbos.engine.v1 import engine_pb2
from cerbos.response.v1 import response_pb2
from cerbos.sdk.grpc.client import CerbosClient

from cerbos_polars import get_filter

ADAPTER = "polars"
RESOURCE_KIND = "document"

DEMO_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "..", "demo"))


class SeedPrincipal(TypedDict):
    id: str
    roles: list[str]


class SeedApplicationFilter(TypedDict):
    """The application's own predicate, never in policy."""

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
    principals: list[SeedPrincipal]
    applicationFilter: SeedApplicationFilter
    documents: list[SeedDocument]


def read_seeds() -> Seeds:
    """Read ``demo/seeds.json``. It is cast, not validated, since ``validate-demo.sh`` checks it."""
    with open(os.path.join(DEMO_DIR, "seeds.json"), encoding="utf-8") as f:
        return cast(Seeds, json.load(f))


SEEDS = read_seeds()

#: Maps Cerbos attributes to columns. ``region`` and ``archived`` are absent because only
#: the application's own predicate uses them.
ATTR_MAP = {
    "request.resource.attr.ownerId": "owner_id",
    "request.resource.attr.public": "is_public",
}


def documents() -> pl.LazyFrame:
    """The demo rows as a lazy frame, with column names that differ from the attribute names."""
    return pl.LazyFrame(
        {
            "id": [row["id"] for row in SEEDS["documents"]],
            "owner_id": [row["ownerId"] for row in SEEDS["documents"]],
            "is_public": [row["public"] for row in SEEDS["documents"]],
            "region": [row["region"] for row in SEEDS["documents"]],
            "archived": [row["archived"] for row in SEEDS["documents"]],
        }
    )


def cerbos_host() -> str:
    """Return the demo PDP address from ``CERBOS_HOST``.

    No default: another local PDP on 3593 would serve the wrong policies and the diff
    would look like an adapter bug.
    """
    host = os.environ.get("CERBOS_HOST")
    if not host:
        raise SystemExit(
            "CERBOS_HOST is not set -- run this example through "
            "demo/scripts/run-example.sh polars"
        )
    return host


def principal(principal_id: str) -> engine_pb2.Principal:
    """Look up a principal in the seeds, so its roles are never restated here."""
    for candidate in SEEDS["principals"]:
        if candidate["id"] == principal_id:
            return engine_pb2.Principal(id=candidate["id"], roles=candidate["roles"])
    raise SystemExit(f"demo/seeds.json declares no principal {principal_id!r}")


def plan_kind(plan: response_pb2.PlanResourcesResponse) -> str:
    """Return the plan kind as ``demo/expected.json`` spells it.

    Reporting it proves the rows came from a real PDP plan.
    """
    kind = engine_pb2.PlanResourcesFilter.Kind.Name(plan.filter.kind)
    if kind == "KIND_UNSPECIFIED":
        raise SystemExit(
            "the PDP returned a plan with no kind -- is $CERBOS_HOST a Cerbos PDP?"
        )
    return kind


class Shapes:
    """The five demo usage shapes, run against a lazy frame.

    None branches on plan kind: ``get_filter`` returns an expression for every kind.
    """

    def __init__(self, client: CerbosClient, frame: pl.LazyFrame) -> None:
        self._client = client
        self._frame = frame

    def run(self) -> dict[str, Any]:
        """Run every shape, keyed as in ``demo/expected.json``."""
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

    def filtered(self, principal_id: str, action: str) -> dict[str, Any]:
        """Shapes 1-3: the adapter's expression as the whole filter."""
        plan = self._plan(principal_id, action)
        allowed = self._frame.filter(get_filter(plan, self._frame, ATTR_MAP))
        return {"kind": plan_kind(plan), "ids": self._ids(allowed)}

    def paginated(
        self, principal_id: str, action: str, page_size: int
    ) -> dict[str, Any]:
        """Shape 4: ``.slice()`` over the filtered frame, sorted for stable paging."""
        plan = self._plan(principal_id, action)
        allowed = self._frame.filter(get_filter(plan, self._frame, ATTR_MAP)).sort("id")

        page_sizes: list[int] = []
        ids: list[str] = []
        offset = 0
        while True:
            page = self._ids(allowed.slice(offset, page_size))
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

    def composed(self, principal_id: str, action: str) -> dict[str, Any]:
        """Shape 5: the adapter's expression ANDed with the application's predicate.

        All plan kinds run through here, to show a denial stays denied after ``&``.
        """
        application_filter = SEEDS["applicationFilter"]
        plan = self._plan(principal_id, action)
        allowed = self._frame.filter(
            get_filter(plan, self._frame, ATTR_MAP)
            & (pl.col("archived") == application_filter["archived"])
            & (pl.col("region") == application_filter["region"])
        )
        return {"kind": plan_kind(plan), "ids": self._ids(allowed)}

    def _plan(
        self, principal_id: str, action: str
    ) -> response_pb2.PlanResourcesResponse:
        return self._client.plan_resources(
            action,
            principal(principal_id),
            engine_pb2.PlanResourcesInput.Resource(kind=RESOURCE_KIND),
        )

    @staticmethod
    def _ids(frame: pl.LazyFrame) -> list[str]:
        return sorted(frame.select("id").collect().get_column("id").to_list())


def main() -> None:
    host = cerbos_host()
    frame = documents()
    print(f"loaded {len(SEEDS['documents'])} documents", file=sys.stderr)

    with CerbosClient(host, tls_verify=False) as client:
        shapes = Shapes(client, frame).run()

    json.dump({"adapter": ADAPTER, "shapes": shapes}, sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
