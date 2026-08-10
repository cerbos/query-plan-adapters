/**
 * Example application for `@cerbos/orm-prisma`, run against the shared demo domain.
 *
 * This is NOT a test of what the adapter translates — `../src/adversarial.test.ts` proves that
 * against a hostile corpus and a live PDP oracle. This proves the two things that harness
 * structurally cannot:
 *
 *   1. Packaging. The import below resolves through the PUBLISHED package — `exports`, `types`,
 *      the `files` allowlist, the peer range — because run.sh installs `npm pack`'s tarball
 *      rather than linking the source directory. The harness imports from `"."` and touches
 *      none of it. See docs/adr/0002-examples-install-the-packed-artifact.md.
 *   2. Usage shape. A harness runs one flat filtered query. Consumers also paginate, and compose
 *      the adapter's filter with predicates of their own. Shape 5 below is the one that earns
 *      the exercise.
 *
 * Prints one JSON document to stdout; everything a human might want to read goes to stderr.
 * demo/scripts/run-example.sh diffs that document against demo/expected.json.
 */
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { GRPC as Cerbos } from "@cerbos/grpc";
import type { PlanResourcesResponse } from "@cerbos/core";
import { PrismaBetterSqlite3 } from "@prisma/adapter-better-sqlite3";
import {
  PlanKind,
  queryPlanToPrisma,
  type Mapper,
  type QueryPlanToPrismaResult,
} from "@cerbos/orm-prisma";

import { PrismaClient, type Prisma } from "./generated/prisma/client";

const DEMO_DIR = resolve(__dirname, "../../../demo");
const RESOURCE_KIND = "document";

interface Seeds {
  principals: { id: string; roles: string[] }[];
  applicationFilter: { description: string; archived: boolean; region: string };
  documents: {
    id: string;
    ownerId: string;
    public: boolean;
    region: string;
    archived: boolean;
  }[];
}

// demo/seeds.json is a repository-controlled corpus file, checked structurally by
// demo/scripts/validate-demo.sh before this ever runs — not untrusted input.
const seeds = JSON.parse(
  readFileSync(resolve(DEMO_DIR, "seeds.json"), "utf8")
) as Seeds;

/**
 * Cerbos attribute names are not column names, so a consumer always writes one of these. Without
 * it the adapter emits `request.resource.attr.ownerId` as a literal Prisma field and the query
 * fails — which is itself worth seeing in an example.
 */
const MAPPER: Mapper = {
  "request.resource.attr.ownerId": { field: "ownerId" },
  "request.resource.attr.public": { field: "public" },
};

/** The application's OWN predicate. Never expressed in policy; declared in demo/seeds.json. */
const APPLICATION_FILTER = {
  archived: seeds.applicationFilter.archived,
  region: seeds.applicationFilter.region,
} satisfies Prisma.DocumentWhereInput;

const prisma = new PrismaClient({
  adapter: new PrismaBetterSqlite3({ url: "file:./prisma/demo.db" }),
});
const cerbos = new Cerbos(process.env["CERBOS_HOST"] ?? "localhost:3593", {
  tls: false,
});

function principal(id: string): { id: string; roles: string[] } {
  const found = seeds.principals.find((p) => p.id === id);
  if (!found) throw new Error(`demo/seeds.json declares no principal '${id}'`);
  return found;
}

async function plan(
  principalId: string,
  action: string
): Promise<PlanResourcesResponse> {
  return cerbos.planResources({
    principal: principal(principalId),
    resource: { kind: RESOURCE_KIND },
    action,
  });
}

/**
 * What a consumer actually writes at the call site. `ALWAYS_DENIED` short-circuits rather than
 * building an impossible `where`, and `ALWAYS_ALLOWED` contributes no predicate at all — which
 * is exactly why composing it with an application filter (shape 5) is the case that breaks
 * first.
 */
type Where = Prisma.DocumentWhereInput | "denied";

function toWhere(result: QueryPlanToPrismaResult): Where {
  switch (result.kind) {
    case PlanKind.ALWAYS_DENIED:
      return "denied";
    case PlanKind.ALWAYS_ALLOWED:
      return {};
    case PlanKind.CONDITIONAL:
      return result.filters;
  }
}

async function findIds(where: Where): Promise<string[]> {
  if (where === "denied") return [];
  const rows = await prisma.document.findMany({
    where,
    select: { id: true },
  });
  return rows.map((r) => r.id).sort();
}

interface ShapeResult {
  kind: PlanKind;
  ids: string[];
}

interface PaginatedShapeResult extends ShapeResult {
  pageSize: number;
  pageSizes: number[];
}

// ---------------------------------------------------------------------------------------------
// The five usage shapes
// ---------------------------------------------------------------------------------------------

/** Shape 1: a plain filtered list. The adapter's filter is the whole query. */
async function filtered(
  principalId: string,
  action: string
): Promise<ShapeResult> {
  const queryPlan = await plan(principalId, action);
  const result = queryPlanToPrisma({ queryPlan, mapper: MAPPER });
  return { kind: result.kind, ids: await findIds(toWhere(result)) };
}

/**
 * Shape 4: pagination applied on top of the filter. Reported as page SIZES plus the sorted union
 * of the ids, never as per-page order — demo/expected.json is shared by ten stores and several
 * of them have no total order to paginate by.
 */
async function paginated(
  principalId: string,
  action: string,
  pageSize: number
): Promise<PaginatedShapeResult> {
  const queryPlan = await plan(principalId, action);
  const result = queryPlanToPrisma({ queryPlan, mapper: MAPPER });
  const where = toWhere(result);

  const pageSizes: number[] = [];
  const ids: string[] = [];
  if (where !== "denied") {
    for (let skip = 0; ; skip += pageSize) {
      const page = await prisma.document.findMany({
        where,
        select: { id: true },
        // Required for the paging itself to be correct, not for the assertion: without a total
        // order, `skip`/`take` may repeat or omit rows between pages. The assertion below stays
        // order-independent regardless — those are separate concerns.
        orderBy: { id: "asc" },
        skip,
        take: pageSize,
      });
      if (page.length === 0) break;
      pageSizes.push(page.length);
      ids.push(...page.map((r) => r.id));
      if (page.length < pageSize) break;
    }
  }

  return { kind: result.kind, pageSize, pageSizes, ids: ids.sort() };
}

/**
 * Shape 5: the adapter's filter ANDed with the application's own predicate. All three plan kinds
 * go through here on purpose — an `ALWAYS_ALLOWED` plan has no filter to AND with, and an
 * `ALWAYS_DENIED` one must not have its denial undone by the application's predicate.
 */
async function composed(
  principalId: string,
  action: string
): Promise<ShapeResult> {
  const queryPlan = await plan(principalId, action);
  const result = queryPlanToPrisma({ queryPlan, mapper: MAPPER });
  const where = toWhere(result);
  if (where === "denied") {
    return { kind: result.kind, ids: [] };
  }
  return {
    kind: result.kind,
    ids: await findIds({ AND: [where, APPLICATION_FILTER] }),
  };
}

async function seed(): Promise<void> {
  await prisma.document.deleteMany({});
  await prisma.document.createMany({ data: seeds.documents });
}

async function main(): Promise<void> {
  await seed();
  console.error(`seeded ${seeds.documents.length} documents`);

  const shapes = {
    filtered: {
      "alice/view": await filtered("alice", "view"),
      "bob/view": await filtered("bob", "view"),
    },
    alwaysAllowed: {
      "admin/admin-view": await filtered("admin", "admin-view"),
    },
    alwaysDenied: {
      "alice/publish": await filtered("alice", "publish"),
    },
    paginated: {
      "alice/view": await paginated("alice", "view", 2),
      "admin/admin-view": await paginated("admin", "admin-view", 3),
    },
    composed: {
      "alice/view": await composed("alice", "view"),
      "bob/view": await composed("bob", "view"),
      "admin/admin-view": await composed("admin", "admin-view"),
      "alice/publish": await composed("alice", "publish"),
    },
  };

  process.stdout.write(
    `${JSON.stringify({ adapter: "prisma", shapes }, null, 2)}\n`
  );
}

void (async () => {
  try {
    await main();
  } catch (error) {
    console.error(error);
    process.exitCode = 1;
  } finally {
    await prisma.$disconnect();
  }
})();
