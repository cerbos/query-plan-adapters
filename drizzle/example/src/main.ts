/**
 * Example application for `@cerbos/orm-drizzle`, run against the shared demo domain.
 *
 * This is NOT a test of what the adapter translates — `../src/adversarial.test.ts` proves that
 * against a hostile corpus and a live PDP oracle, on SQLite and on PostgreSQL. This proves the
 * two things that harness structurally cannot:
 *
 *   1. Packaging. The import below resolves through the PUBLISHED package — `exports`, `types`,
 *      the `files` allowlist, the peer range against this example's own `drizzle-orm` — because
 *      run.sh installs `npm pack`'s tarball rather than linking the source directory. The harness
 *      imports from `"."` and touches none of it. See
 *      docs/adr/0002-examples-install-the-packed-artifact.md.
 *   2. Usage shape. A harness runs one flat filtered query. Consumers also paginate, and compose
 *      the adapter's filter with predicates of their own. Shape 5 below is the one that earns
 *      the exercise.
 *
 * Prints one JSON document to stdout; everything a human might want to read goes to stderr.
 * demo/scripts/run-example.sh diffs that document against demo/expected.json.
 */
import { readFileSync, rmSync } from "node:fs";
import { resolve } from "node:path";

import { GRPC as Cerbos } from "@cerbos/grpc";
import {
  PlanKind,
  queryPlanToDrizzle,
  type Mapper,
  type QueryPlanToDrizzleResult,
} from "@cerbos/orm-drizzle";
import Database from "better-sqlite3";
import { and, asc, eq, type SQL } from "drizzle-orm";
import { drizzle } from "drizzle-orm/better-sqlite3";

import { CREATE_TABLE, documents } from "./schema";

const DEMO_DIR = resolve(__dirname, "../../../demo");
const RESOURCE_KIND = "document";

/**
 * A dedicated file rather than `:memory:`, so a failing run leaves the seeded rows behind to
 * inspect. It is scratch state this example owns and .gitignore excludes, so each run starts from
 * nothing by deleting it — one `rm`, rather than a reset against whatever database a config
 * happens to name.
 */
const DB_PATH = resolve(__dirname, "../demo.db");

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
 * it the adapter has nothing to resolve `request.resource.attr.ownerId` to and throws — which is
 * itself worth seeing in an example.
 */
const MAPPER: Mapper = {
  "request.resource.attr.ownerId": documents.ownerId,
  "request.resource.attr.public": documents.public,
};

/** The application's OWN predicate. Never expressed in policy; declared in demo/seeds.json. */
const APPLICATION_FILTER = and(
  eq(documents.archived, seeds.applicationFilter.archived),
  eq(documents.region, seeds.applicationFilter.region)
);

/**
 * The runner sets this, and there is deliberately no fallback. The obvious default — Cerbos's own
 * 3592/3593 — is the address every adapter's `cerbos run` test sidecar binds, so an unset
 * CERBOS_HOST would not fail: it would quietly plan against the conformance corpus that sidecar
 * serves, and produce a diff against demo/expected.json that reads as an adapter bug.
 * demo/README.md requires reaching the PDP at $CERBOS_HOST, "never a hardcoded address", for
 * exactly that reason.
 *
 * Checked before anything is opened or deleted, so a misinvocation costs nothing.
 */
const cerbosHost = process.env["CERBOS_HOST"];
if (!cerbosHost) {
  throw new Error(
    "CERBOS_HOST is not set — run this example through demo/scripts/run-example.sh drizzle"
  );
}

rmSync(DB_PATH, { force: true });
const sqlite = new Database(DB_PATH);
const db = drizzle(sqlite);

const cerbos = new Cerbos(cerbosHost, { tls: false });

/** Exactly `PlanResourcesResponse`, without depending on a package this example never imports. */
type QueryPlan = Awaited<ReturnType<Cerbos["planResources"]>>;

function principal(id: string): { id: string; roles: string[] } {
  const found = seeds.principals.find((p) => p.id === id);
  if (!found) throw new Error(`demo/seeds.json declares no principal '${id}'`);
  return found;
}

async function plan(principalId: string, action: string): Promise<QueryPlan> {
  return cerbos.planResources({
    principal: principal(principalId),
    resource: { kind: RESOURCE_KIND },
    action,
  });
}

/**
 * What a consumer actually writes at the call site. `ALWAYS_DENIED` short-circuits rather than
 * building an impossible predicate, and `ALWAYS_ALLOWED` becomes `undefined` — Drizzle's own word
 * for "no predicate", which `.where()` and `and()` both already understand. That second case is
 * exactly why composing with an application filter (shape 5) is the one that breaks first.
 */
type Where = SQL | undefined | "denied";

function toWhere(result: QueryPlanToDrizzleResult): Where {
  switch (result.kind) {
    case PlanKind.ALWAYS_DENIED:
      return "denied";
    case PlanKind.ALWAYS_ALLOWED:
      return undefined;
    case PlanKind.CONDITIONAL:
      return result.filter;
  }
}

function findIds(where: Where): string[] {
  if (where === "denied") return [];
  return db
    .select({ id: documents.id })
    .from(documents)
    .where(where)
    .all()
    .map((row) => row.id)
    .sort();
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
  const result = queryPlanToDrizzle({ queryPlan, mapper: MAPPER });
  return { kind: result.kind, ids: findIds(toWhere(result)) };
}

/**
 * Shape 4: pagination applied on top of the filter. Reported as page SIZES plus the sorted union
 * of the ids, never as per-page order — demo/expected.json is shared by every store and several
 * of them have no total order to paginate by.
 */
async function paginated(
  principalId: string,
  action: string,
  pageSize: number
): Promise<PaginatedShapeResult> {
  const queryPlan = await plan(principalId, action);
  const result = queryPlanToDrizzle({ queryPlan, mapper: MAPPER });
  const where = toWhere(result);

  const pageSizes: number[] = [];
  const ids: string[] = [];
  if (where !== "denied") {
    for (let offset = 0; ; offset += pageSize) {
      const page = db
        .select({ id: documents.id })
        .from(documents)
        .where(where)
        // Required for the paging itself to be correct, not for the assertion: without a total
        // order, limit/offset may repeat or omit rows between pages. The assertion below stays
        // order-independent regardless — those are separate concerns.
        .orderBy(asc(documents.id))
        .limit(pageSize)
        .offset(offset)
        .all();
      if (page.length === 0) break;
      pageSizes.push(page.length);
      ids.push(...page.map((row) => row.id));
      if (page.length < pageSize) break;
    }
  }

  return { kind: result.kind, pageSize, pageSizes, ids: ids.sort() };
}

/**
 * Shape 5: the adapter's filter ANDed with the application's own predicate. All three plan kinds
 * go through here on purpose — an `ALWAYS_ALLOWED` plan has no filter to AND with, and an
 * `ALWAYS_DENIED` one must not have its denial undone by the application's predicate.
 *
 * `and(undefined, x)` collapsing to `x` is what makes the ALWAYS_ALLOWED case a one-liner here,
 * and it is also why the sentinel above is a string rather than a second `undefined`: a denial
 * that reached `and()` would come back out as the application's predicate alone.
 */
async function composed(
  principalId: string,
  action: string
): Promise<ShapeResult> {
  const queryPlan = await plan(principalId, action);
  const result = queryPlanToDrizzle({ queryPlan, mapper: MAPPER });
  const where = toWhere(result);
  if (where === "denied") {
    return { kind: result.kind, ids: [] };
  }
  return { kind: result.kind, ids: findIds(and(where, APPLICATION_FILTER)) };
}

function seed(): void {
  sqlite.exec(CREATE_TABLE);
  db.insert(documents).values(seeds.documents).run();
}

async function main(): Promise<void> {
  seed();
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
    `${JSON.stringify({ adapter: "drizzle", shapes }, null, 2)}\n`
  );
}

void (async () => {
  try {
    await main();
  } catch (error) {
    console.error(error);
    process.exitCode = 1;
  } finally {
    sqlite.close();
  }
})();
