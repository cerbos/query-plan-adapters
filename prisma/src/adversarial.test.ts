import * as fs from "fs";
import * as path from "path";

import { GRPC as Cerbos } from "@cerbos/grpc";
import type { Principal, Resource } from "@cerbos/core";

import { queryPlanToPrisma, PlanKind, MapperConfig } from ".";
import { prisma } from "./test-setup.adversarial";

/**
 * Adversarial differential suite: every action in the shared `../conformance/` corpus is planned
 * against a REAL Cerbos PDP (the sidecar started by `npm run test:adversarial`, loaded with
 * `conformance/policies/adversarial.yaml`), translated by this adapter, and executed against
 * seeded rows — then the filtered id set is compared against an oracle computed by calling the
 * check API for each row with attributes mirroring that row exactly.
 *
 * No hand-computed expectations: if this adapter's filter semantics diverge from Cerbos's own
 * evaluation for any row, the mismatch surfaces mechanically. See `conformance/README.md` for the
 * oracle recipe (NULL-as-missing-attribute, the degeneracy guard) — this file only owns the
 * Prisma-specific translation (seeding, field mapping, executing the query).
 */

const cerbos = new Cerbos("127.0.0.1:3593", { tls: false });

const CONFORMANCE_DIR = path.join(__dirname, "..", "..", "conformance");

interface Tag {
  id: string;
  name: string | null;
}

interface Seed {
  id: string;
  aBool: boolean;
  aString: string;
  aNumber: number;
  aOptionalString: string | null;
  tags: Tag[];
  subCategoryNames: string[];
}

interface SeedsFile {
  principal: Principal;
  resourceKind: string;
  seeds: Seed[];
}

interface UnsupportedShape {
  action: string;
  shape: string;
  springDataMessage: string;
}

interface AdapterUnsupportedEntry {
  action: string;
  reason: string;
}

interface ActionsFile {
  conformance: string[];
  adapterUnsupported?: Record<string, AdapterUnsupportedEntry[]>;
  expectedUnsupported: UnsupportedShape[];
}

const seedsFile: SeedsFile = JSON.parse(
  fs.readFileSync(path.join(CONFORMANCE_DIR, "seeds.json"), "utf8")
);
const actionsFile: ActionsFile = JSON.parse(
  fs.readFileSync(path.join(CONFORMANCE_DIR, "actions.json"), "utf8")
);
const SEEDS = seedsFile.seeds;

// Shapes Prisma's filter language genuinely cannot express (LIKE-wildcard escaping,
// relation-count thresholds, cross-model column comparisons, ...). The adapter must throw
// for these — a loud failure, never a silently-wrong filter — so they are asserted as
// throws instead of oracle matches. Each entry carries its reason in actions.json.
const PRISMA_UNSUPPORTED = new Set(
  (actionsFile.adapterUnsupported?.prisma ?? []).map((u) => u.action)
);
const ORACLE_ACTIONS = actionsFile.conformance.filter(
  (action) => !PRISMA_UNSUPPORTED.has(action)
);
const THROWING_ACTIONS = [
  ...(actionsFile.adapterUnsupported?.prisma ?? []).map(
    (u) => [u.action, u.reason] as const
  ),
  ...actionsFile.expectedUnsupported.map((u) => [u.action, u.shape] as const),
];

/** Deterministic ISO instant per seed for the timestamp probe: split around 2025-01-01. */
function isoFor(seed: Seed): string {
  return seed.aNumber >= 2 ? "2024-06-01T00:00:00Z" : "2026-06-01T00:00:00Z";
}

const MAPPER: Record<string, MapperConfig> = {
  "request.resource.attr.aBool": { field: "aBool" },
  "request.resource.attr.aString": { field: "aString" },
  "request.resource.attr.aNumber": { field: "aNumber" },
  "request.resource.attr.aOptionalString": { field: "aOptionalString" },
  "request.resource.attr.createdBy": { field: "createdBy" },
  // obj.inner is not a real nested column — mirrors aString, same trick the spring-data
  // reference harness uses for the p-struct probe.
  "request.resource.attr.obj.inner": { field: "aString" },
  "request.resource.attr.tags": {
    relation: {
      name: "tags",
      type: "many",
      // Model name enables field-to-field comparisons between tag columns; the nullable
      // flag on `name` enables the adapter's three-valued-logic guards for collection
      // macros over elements whose name column is NULL (a missing attribute — a CEL
      // error, hence deny — on the check side).
      model: "AdversarialTag",
      fields: {
        id: { field: "tagId" },
        name: { field: "name", nullable: true },
      },
    },
  },
  "request.resource.attr.categories": {
    relation: {
      name: "categories",
      type: "many",
      fields: {
        name: { field: "name" },
        subCategories: {
          relation: {
            name: "subCategories",
            type: "many",
            fields: { name: { field: "name" } },
          },
        },
      },
    },
  },
  // Multi-hop chain probe (W1): mainCategory mirrors the SAME categories/subCategories relation
  // as a single-object dotted chain on the check side (every seed holds at most one category),
  // pinning that the adapter joins through every intermediate hop, never off the root.
  "request.resource.attr.mainCategory": {
    relation: {
      name: "categories",
      type: "many",
      fields: {
        name: { field: "name" },
        subCategories: {
          relation: {
            name: "subCategories",
            type: "many",
            fields: { name: { field: "name" } },
          },
        },
        // subNames: the same 2-hop chain but with a bare `field`, so plain `in` membership
        // compares the flattened tail's name column directly.
        subNames: {
          relation: {
            name: "subCategories",
            type: "many",
            field: "name",
          },
        },
      },
    },
  },
};

beforeAll(async () => {
  await prisma.adversarialSubCategory.deleteMany();
  await prisma.adversarialCategory.deleteMany();
  await prisma.adversarialTag.deleteMany();
  await prisma.adversarialResource.deleteMany();

  // Distinct sub-category/category graphs per seed so no rows share relations by accident.
  for (const seed of SEEDS) {
    await prisma.adversarialResource.create({
      data: {
        id: seed.id,
        aBool: seed.aBool,
        aString: seed.aString,
        aNumber: seed.aNumber,
        aOptionalString: seed.aOptionalString,
        createdBy: isoFor(seed),
        tags: {
          create: seed.tags.map((t) => ({ tagId: t.id, name: t.name })),
        },
        categories: {
          create: seed.subCategoryNames.map((subName) => ({
            name: "business",
            subCategories: { create: [{ name: subName }] },
          })),
        },
      },
    });
  }
});

afterAll(async () => {
  await prisma.$disconnect();
});

function principal(): Principal {
  return seedsFile.principal;
}

/** A NULL tag name in the DB is a missing element attribute on the check side. */
function asTagAttribute(tag: Tag): Record<string, unknown> {
  const attr: Record<string, unknown> = { id: tag.id };
  if (tag.name !== null) {
    attr.name = tag.name;
  }
  return attr;
}

/** Cerbos attributes mirroring exactly what the seeded DB row holds. */
function asCheckResource(seed: Seed): Resource {
  const attr: Record<string, unknown> = {
    aBool: seed.aBool,
    aString: seed.aString,
    aNumber: seed.aNumber,
    createdBy: isoFor(seed),
    obj: { inner: seed.aString },
    tags: seed.tags.map(asTagAttribute),
    categories: seed.subCategoryNames.map((subName) => ({
      name: "business",
      subCategories: [{ name: subName }],
    })),
  };
  // A DB NULL is a missing attribute on the check side — conditions touching it must deny
  // (CEL error), matching SQL three-valued logic excluding the row.
  if (seed.aOptionalString !== null) {
    attr.aOptionalString = seed.aOptionalString;
  }
  // mainCategory mirrors the row's single category as ONE nested object (the seeder creates
  // at most one category per seed), so direct dotted-chain CEL expressions evaluate cleanly;
  // rows without a category get NO attribute — a CEL missing-attr error (deny), matching the
  // adapter's empty join chain excluding the row.
  if (seed.subCategoryNames.length > 0) {
    attr.mainCategory = {
      name: "business",
      subCategories: seed.subCategoryNames.map((name) => ({ name })),
      subNames: seed.subCategoryNames,
    };
  }
  return { kind: seedsFile.resourceKind, id: seed.id, attr };
}

// -- oracle: ask the PDP itself, row by row --

async function oracleAllowedIds(action: string): Promise<string[]> {
  const ids: string[] = [];
  for (const seed of SEEDS) {
    const result = await cerbos.checkResource({
      principal: principal(),
      resource: asCheckResource(seed),
      actions: [action],
    });
    if (result.isAllowed(action)) {
      ids.push(seed.id);
    }
  }
  return ids.sort();
}

// -- adapter execution through the public queryPlanToPrisma path --

async function adapterFilteredIds(action: string): Promise<string[]> {
  const queryPlan = await cerbos.planResources({
    principal: principal(),
    resource: { kind: seedsFile.resourceKind },
    action,
  });
  const result = queryPlanToPrisma({
    queryPlan,
    mapper: MAPPER,
    model: "AdversarialResource",
  });
  if (result.kind === PlanKind.ALWAYS_DENIED) {
    return [];
  }
  const where = result.kind === PlanKind.CONDITIONAL ? result.filters : {};
  const rows = await prisma.adversarialResource.findMany({
    where,
    select: { id: true },
  });
  return rows.map((r) => r.id).sort();
}

describe("adversarial conformance corpus", () => {
  test.each(ORACLE_ACTIONS)(
    "%s matches the check() oracle",
    async (action) => {
      const [oracle, filtered] = await Promise.all([
        oracleAllowedIds(action),
        adapterFilteredIds(action),
      ]);
      expect(filtered).toEqual(oracle);
    }
  );

  // Shapes the adapter does not support (globally unsupported planner shapes plus Prisma's
  // declared adapterUnsupported list): translation must fail loudly, never produce a
  // silently-wrong filter.
  test.each(THROWING_ACTIONS)(
    "%s fails loudly instead of silently mistranslating (%s)",
    async (action) => {
      await expect(adapterFilteredIds(action)).rejects.toThrow();
    }
  );

  test("oracle is not degenerate", async () => {
    // Guard the guard: at least one action must produce a non-empty, non-total oracle set,
    // otherwise the differential comparison could pass vacuously (e.g. PDP denying all).
    for (const action of ["vf-le", "like-percent", "all-on-empty"]) {
      const ids = await oracleAllowedIds(action);
      expect(ids.length).toBeGreaterThan(0);
      expect(ids.length).toBeLessThan(SEEDS.length);
    }
  });
});
