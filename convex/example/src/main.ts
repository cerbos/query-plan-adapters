/**
 * Example application for `@cerbos/orm-convex`, run against the shared demo domain.
 *
 * This is NOT a test of what the adapter translates — `../src/adversarial.test.ts` proves that
 * against a hostile corpus with a live PDP as the oracle, inside a real Convex backend. This
 * proves the two things that harness structurally cannot:
 *
 *   1. Packaging. The adapter is resolved through its PUBLISHED package — the `exports` map,
 *      `types`, the `files` allowlist, and the `@cerbos/core` peer range against the copy this
 *      example installs — because run.sh installs `npm pack`'s tarball rather than linking the
 *      source directory. The harness imports from `"."` and touches none of it. See
 *      docs/adr/0002-examples-install-the-packed-artifact.md. Two resolvers read it here: `tsc`
 *      under `moduleResolution: nodenext` for this file, and Convex's own bundler for
 *      ../convex/documents.ts, which is where the translation actually happens.
 *   2. Usage shape. A harness runs one flat filtered query. Consumers also paginate, and compose
 *      the adapter's filter with predicates of their own. Shape 5 below is the one that earns
 *      the exercise.
 *
 * The division of labour is Convex's, not a choice: `queryPlanToConvex` returns a function of
 * Convex's `FilterBuilder`, which exists only inside a Convex query. So this file plans, seeds and
 * reports, and ../convex/documents.ts translates and queries.
 *
 * Prints one JSON document to stdout; everything a human might want to read goes to stderr.
 * The cases and expected results are read from demo/cases.json, and the program validates the document before emitting it.
 */
import { resolve } from "node:path";

import { PlanKind } from "@cerbos/orm-convex";
import { GRPC as Cerbos } from "@cerbos/grpc";
import { ConvexHttpClient } from "convex/browser";
import { makeFunctionReference } from "convex/server";

import {
  loadConsumerCases,
  loadDemoSeeds,
  runConsumerCases,
  type DemoSeeds,
} from "./cases";

const DEMO_DIR = resolve(__dirname, "../../../demo");
const RESOURCE_KIND = "document";
const consumerCases = loadConsumerCases(DEMO_DIR);
const seeds = loadDemoSeeds(DEMO_DIR);
type SeedDocument = DemoSeeds["documents"][number];

/**
 * The application's OWN predicate, never expressed in policy. Rebuilt field by field rather than
 * forwarded whole: the corpus entry carries a `description` alongside the two fields, and the
 * Convex validator on the other side accepts exactly the fields it declares.
 */
const APPLICATION_FILTER = {
  archived: seeds.applicationFilter.archived,
  region: seeds.applicationFilter.region,
};

/**
 * The runner sets this, and there is deliberately no fallback. The obvious default — Cerbos's own
 * 3592/3593 — is the address every adapter's `cerbos run` test sidecar binds, so an unset
 * CERBOS_HOST would not fail: it would quietly plan against the conformance corpus that sidecar
 * serves, and fail a demo/cases.json case in a way that reads as an adapter bug.
 * demo/README.md requires reaching the PDP at $CERBOS_HOST, "never a hardcoded address", for
 * exactly that reason.
 */
const cerbosHost = process.env["CERBOS_HOST"];
if (!cerbosHost) {
  throw new Error(
    "CERBOS_HOST is not set — run this example through demo/scripts/run-example.sh convex",
  );
}

/**
 * The Convex backend run.sh started, on the same terms and for the same reason: 3210 is the port
 * `npm run convex:up` and the adapter's own CI bind, so a default of it would let this example
 * deploy over — and read from — the backend a conformance run is using.
 *
 * demo/scripts/run-example.sh's contract — "reads no environment except CERBOS_HOST" — is between
 * the runner and `run.sh`, and it still holds: CERBOS_HOST is the only thing the runner supplies.
 * CONVEX_URL is passed by run.sh to the process it starts, and it is an environment variable
 * rather than a literal for the same reason CERBOS_HOST is one: the port belongs to whoever
 * started the backend, and writing it here would be a second copy of a number that must not drift.
 */
const convexUrl = process.env["CONVEX_URL"];
if (!convexUrl) {
  throw new Error(
    "CONVEX_URL is not set — run this example through demo/scripts/run-example.sh convex",
  );
}

const cerbos = new Cerbos(cerbosHost, { tls: false });
const convex = new ConvexHttpClient(convexUrl);

/** Exactly `PlanResourcesResponse`, without depending on a package this example never imports. */
type QueryPlan = Awaited<ReturnType<Cerbos["planResources"]>>;

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function isQueryPlan(value: unknown): value is QueryPlan {
  if (!isRecord(value)) {
    return false;
  }
  switch (value["kind"]) {
    case PlanKind.CONDITIONAL:
      return isRecord(value["condition"]);
    case PlanKind.ALWAYS_ALLOWED:
    case PlanKind.ALWAYS_DENIED:
      return true;
    default:
      return false;
  }
}

interface ListResult {
  kind: string;
  ids: string[];
}

interface PageResult extends ListResult {
  isDone: boolean;
  cursor: string | null;
}

/**
 * Convex's documented way to name a deployed function without the generated `api` object. An
 * application that ships its own `convex/_generated/` would import `api` instead and get the same
 * references; this example does not, because that directory is deploy output rather than source —
 * gitignored, and ESM where this client compiles to CommonJS. The trade is that the argument and
 * return types are stated here rather than inferred from the handlers, which is why each one is
 * written out.
 */
const seedDocuments = makeFunctionReference<
  "mutation",
  { documents: SeedDocument[] },
  null
>("documents:seed");

const listDocuments = makeFunctionReference<
  "query",
  { queryPlan: QueryPlan; applicationFilter?: typeof APPLICATION_FILTER },
  ListResult
>("documents:list");

const pageDocuments = makeFunctionReference<
  "query",
  {
    queryPlan: QueryPlan;
    paginationOpts: { numItems: number; cursor: string | null };
  },
  PageResult
>("documents:page");

function principal(id: string): { id: string; roles: string[] } {
  const found = seeds.principals.find((p) => p.id === id);
  if (!found) throw new Error(`demo/seeds.json declares no principal '${id}'`);
  return found;
}

/**
 * Plans, and hands back something Convex will accept as an argument.
 *
 * The round trip is not decoration. `@cerbos/core` builds a plan out of `PlanExpression`,
 * `PlanExpressionValue` and `PlanExpressionVariable` CLASS instances, and Convex's argument
 * encoder rejects a class instance outright — `... is not a supported Convex type`. So a Convex
 * consumer serializes the plan on the way in, and what the backend receives is the same tree with
 * the prototypes gone.
 *
 * Which is exactly why the adapter classifies operands by their shape rather than with
 * `instanceof`: an `instanceof` check could not survive this crossing at all, on any consumer's
 * machine (cerbos/query-plan-adapters#419).
 */
async function plan(principalId: string, action: string): Promise<QueryPlan> {
  const queryPlan = await cerbos.planResources({
    principal: principal(principalId),
    resource: { kind: RESOURCE_KIND },
    action,
  });
  const serialized: unknown = JSON.parse(JSON.stringify(queryPlan));
  if (!isQueryPlan(serialized)) {
    throw new Error("Cerbos returned an invalid query plan");
  }
  return serialized;
}

const PLAN_KINDS: PlanKind[] = [
  PlanKind.CONDITIONAL,
  PlanKind.ALWAYS_ALLOWED,
  PlanKind.ALWAYS_DENIED,
];

/**
 * The plan kind comes back from the backend as a bare string: a Convex function's return value is
 * data, so the enum the adapter reported arrives with nothing left of its type. Re-narrowing it
 * against the adapter's own re-exported `PlanKind` is the same crossing the plan itself makes on
 * the way in, where the class instances @cerbos/core built arrive as plain objects — and it fails
 * the run rather than emitting a kind demo/cases.json has never heard of.
 */
function asPlanKind(reported: string): PlanKind {
  const kind = PLAN_KINDS.find((candidate) => candidate === reported);
  if (!kind) throw new Error(`unknown plan kind '${reported}'`);
  return kind;
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

/**
 * Shapes 1, 2, 3 and 5. The only difference between a plain filtered list and the composed one is
 * whether the application hands its own predicate over to be ANDed in, which is why they share a
 * call rather than being two similar-looking ones.
 */
async function listed(
  principalId: string,
  action: string,
  applicationFilter?: typeof APPLICATION_FILTER,
): Promise<ShapeResult> {
  const queryPlan = await plan(principalId, action);
  const result = await convex.query(listDocuments, {
    queryPlan,
    ...(applicationFilter ? { applicationFilter } : {}),
  });
  return { kind: asPlanKind(result.kind), ids: result.ids };
}

/**
 * Shape 4: pagination applied on top of the filter, walked with Convex's cursor. Reported as page
 * SIZES plus the sorted union of the ids, never as per-page order — demo/cases.json is shared
 * by every store and several of them have no total order to paginate by.
 *
 * Convex has no filtered count, which is why count is not one of the five shapes.
 */
async function paginated(
  principalId: string,
  action: string,
  pageSize: number,
): Promise<PaginatedShapeResult> {
  const queryPlan = await plan(principalId, action);

  const pageSizes: number[] = [];
  const ids: string[] = [];
  let cursor: string | null = null;

  // `isDone` is the ONLY end condition. A page shorter than `numItems` is not the end — a filtered
  // `.paginate()` walks the table under a read budget, so Convex may hand back fewer documents
  // than asked for and more to come — and by the same argument an empty page is not the end
  // either. Treating either as terminal would silently truncate the union.
  //
  // Which leaves the loop needing a bound rather than a second exit: at least one document per
  // request in the ordinary case means no more requests than seed rows, plus the one that reports
  // itself done. Overrunning it is a failed run rather than a hung one.
  for (let request = 0; request <= seeds.documents.length; request++) {
    // Annotated rather than inferred: `cursor` is assigned from this same value further down, so
    // inferring it here would be circular.
    const result: PageResult = await convex.query(pageDocuments, {
      queryPlan,
      paginationOpts: { numItems: pageSize, cursor },
    });
    if (result.ids.length > 0) {
      pageSizes.push(result.ids.length);
      ids.push(...result.ids);
    }
    if (result.isDone) {
      return {
        kind: asPlanKind(result.kind),
        pageSize,
        pageSizes,
        ids: ids.sort(),
      };
    }
    cursor = result.cursor;
  }

  throw new Error(
    `paginating ${principalId}/${action} did not reach the end of the table in ` +
      `${seeds.documents.length + 1} pages`,
  );
}

async function main(): Promise<void> {
  await convex.mutation(seedDocuments, { documents: seeds.documents });
  console.error(`seeded ${seeds.documents.length} documents`);

  const shapes = await runConsumerCases({
    cases: consumerCases,
    execute: (testCase) => {
      switch (testCase.operation) {
        case "paginated":
          return paginated(
            testCase.principal,
            testCase.action,
            testCase.pagination.pageSize,
          );
        case "composed":
          return listed(
            testCase.principal,
            testCase.action,
            APPLICATION_FILTER,
          );
        case "filtered":
        case "alwaysAllowed":
        case "alwaysDenied":
          return listed(testCase.principal, testCase.action);
      }
    },
  });

  process.stdout.write(
    `${JSON.stringify({ adapter: "convex", shapes }, null, 2)}\n`,
  );
}

void (async () => {
  try {
    await main();
  } catch (error) {
    console.error(error);
    process.exitCode = 1;
  }
})();
