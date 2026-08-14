/**
 * Example application for `@cerbos/orm-mongoose`, run against the shared demo domain.
 *
 * This is NOT a test of what the adapter translates — `../src/adversarial.test.ts` proves that
 * against a hostile corpus with a live PDP as the oracle, on two MongoDB server versions. This
 * proves the two things that harness structurally cannot:
 *
 *   1. Packaging. The import below resolves through the PUBLISHED package — the `exports` map,
 *      `types`, the `files` allowlist — because run.sh installs `npm pack`'s tarball rather than
 *      linking the source directory. The harness imports from `"."` and touches none of it. See
 *      docs/adr/0002-examples-install-the-packed-artifact.md. There is no peer range in that list
 *      because the adapter declares none: it never imports `mongoose`, it returns a plain filter
 *      object, and the `mongoose` in this example's node_modules is the application's own.
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
import {
  PlanKind,
  queryPlanToMongoose,
  type Mapper,
  type MongooseFilter,
  type QueryPlanToMongooseResult,
} from "@cerbos/orm-mongoose";
import mongoose from "mongoose";

import { DocumentModel } from "./schema";

const DEMO_DIR = resolve(__dirname, "../../../demo");
const RESOURCE_KIND = "document";

/**
 * The store this example owns. `run.sh` starts the server on 27117 rather than MongoDB's default
 * 27017 for the reason demo/docker-compose.yml publishes the PDP on 13592/13593: 27017 is the
 * port `npm run mongo` and the adapter's own CI both bind, and a demo server already holding it
 * would make one of the two silently talk to the other's data.
 *
 * The collection is scratch state, emptied and reseeded on every run.
 */
const MONGO_URI = "mongodb://127.0.0.1:27117/cerbos_demo";

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
 * Cerbos attribute names are not document paths, so a consumer always writes one of these. This
 * is the one piece of configuration a Mongoose consumer must not skip: an unmapped reference
 * resolves to itself, so the adapter would filter on a path literally named
 * `request.resource.attr.ownerId`, which matches no document and returns nothing.
 */
const MAPPER: Mapper = {
  "request.resource.attr.ownerId": { field: "ownerId" },
  "request.resource.attr.public": { field: "isPublic" },
};

/** The application's OWN predicate. Never expressed in policy; declared in demo/seeds.json. */
const APPLICATION_FILTER: MongooseFilter = {
  archived: seeds.applicationFilter.archived,
  region: seeds.applicationFilter.region,
};

/**
 * The runner sets this, and there is deliberately no fallback. The obvious default — Cerbos's own
 * 3592/3593 — is the address every adapter's `cerbos run` test sidecar binds, so an unset
 * CERBOS_HOST would not fail: it would quietly plan against `../policies` and produce a diff
 * against demo/expected.json that reads as an adapter bug. demo/README.md requires reaching the
 * PDP at $CERBOS_HOST, "never a hardcoded address", for exactly that reason.
 *
 * Checked before anything connects, so a misinvocation costs nothing.
 */
const cerbosHost = process.env["CERBOS_HOST"];
if (!cerbosHost) {
  throw new Error(
    "CERBOS_HOST is not set — run this example through demo/scripts/run-example.sh mongoose"
  );
}

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
 * building an impossible query, and `ALWAYS_ALLOWED` becomes `{}` — MongoDB's own word for "no
 * predicate", which both `find()` and `$and` already understand.
 *
 * A conditional plan with no filter is an error rather than `{}`: silently widening a missing
 * predicate to match-all is the one mistranslation that returns rows the PDP denied.
 */
type Where = MongooseFilter | "denied";

function toWhere(result: QueryPlanToMongooseResult): Where {
  switch (result.kind) {
    case PlanKind.ALWAYS_DENIED:
      return "denied";
    case PlanKind.ALWAYS_ALLOWED:
      return {};
    case PlanKind.CONDITIONAL: {
      if (!result.filters) {
        throw new Error("KIND_CONDITIONAL plan carried no filters");
      }
      return result.filters;
    }
  }
}

async function findIds(where: Where): Promise<string[]> {
  if (where === "denied") return [];
  const rows = await DocumentModel.find(where).select({ _id: 1 }).lean().exec();
  return rows.map((row) => row._id).sort();
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
  const result = queryPlanToMongoose({ queryPlan, mapper: MAPPER });
  return { kind: result.kind, ids: await findIds(toWhere(result)) };
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
  const result = queryPlanToMongoose({ queryPlan, mapper: MAPPER });
  const where = toWhere(result);

  const pageSizes: number[] = [];
  const ids: string[] = [];
  if (where !== "denied") {
    for (let skip = 0; ; skip += pageSize) {
      const page = await DocumentModel.find(where)
        .select({ _id: 1 })
        // Required for the paging itself to be correct, not for the assertion: without a total
        // order, skip/limit may repeat or omit documents between pages. The assertion below stays
        // order-independent regardless — those are separate concerns.
        .sort({ _id: 1 })
        .skip(skip)
        .limit(pageSize)
        .lean()
        .exec();
      if (page.length === 0) break;
      pageSizes.push(page.length);
      ids.push(...page.map((row) => row._id));
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
 * `{}` being a valid `$and` arm is what makes the ALWAYS_ALLOWED case a one-liner here, and it is
 * also why the sentinel above is a string rather than a second `{}`: a denial that reached `$and`
 * would come back out as the application's predicate alone.
 */
async function composed(
  principalId: string,
  action: string
): Promise<ShapeResult> {
  const queryPlan = await plan(principalId, action);
  const result = queryPlanToMongoose({ queryPlan, mapper: MAPPER });
  const where = toWhere(result);
  if (where === "denied") {
    return { kind: result.kind, ids: [] };
  }
  return {
    kind: result.kind,
    ids: await findIds({ $and: [where, APPLICATION_FILTER] }),
  };
}

async function seed(): Promise<void> {
  await DocumentModel.deleteMany({});
  await DocumentModel.insertMany(
    seeds.documents.map((document) => ({
      _id: document.id,
      ownerId: document.ownerId,
      isPublic: document.public,
      region: document.region,
      archived: document.archived,
    }))
  );
}

async function main(): Promise<void> {
  await mongoose.connect(MONGO_URI);
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
    `${JSON.stringify({ adapter: "mongoose", shapes }, null, 2)}\n`
  );
}

void (async () => {
  try {
    await main();
  } catch (error) {
    console.error(error);
    process.exitCode = 1;
  } finally {
    cerbos.close();
    await mongoose.disconnect();
  }
})();
