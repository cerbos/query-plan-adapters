import * as fs from "fs";
import * as path from "path";

import { GRPC as Cerbos } from "@cerbos/grpc";
import type { Principal, Resource, Value } from "@cerbos/core";

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

interface KnownDivergence {
  action: string;
  adapters: string[];
}

interface ActionsFile {
  conformance: string[];
  adapterUnsupported?: Record<string, AdapterUnsupportedEntry[]>;
  adapterSupportedExpected?: Record<string, AdapterUnsupportedEntry[]>;
  expectedUnsupported: UnsupportedShape[];
  knownDivergences?: KnownDivergence[];
}

type ThrowingAction = readonly [action: string, reason: string];

interface ActionClassification {
  oracleActions: string[];
  throwingActions: ThrowingAction[];
  supportedExpected: Set<string>;
}

function classifyActionsForAdapter(
  manifest: ActionsFile,
  adapter: string
): ActionClassification {
  const unsupported = manifest.adapterUnsupported?.[adapter] ?? [];
  const unsupportedActions = new Set(unsupported.map((entry) => entry.action));
  const supportedExpected = new Set(
    (manifest.adapterSupportedExpected?.[adapter] ?? []).map(
      (entry) => entry.action
    )
  );
  const oracleActions = [
    ...manifest.conformance.filter(
      (action) => !unsupportedActions.has(action)
    ),
    ...supportedExpected,
  ];
  const throwingActions: ThrowingAction[] = [
    ...unsupported.map(
      (entry): ThrowingAction => [entry.action, entry.reason]
    ),
    ...manifest.expectedUnsupported
      .filter((entry) => !supportedExpected.has(entry.action))
      .map((entry): ThrowingAction => [entry.action, entry.shape]),
  ];

  return {
    oracleActions: [...new Set(oracleActions)].sort(),
    throwingActions: throwingActions.sort(([left], [right]) =>
      left.localeCompare(right)
    ),
    supportedExpected,
  };
}

const seedsFile: SeedsFile = JSON.parse(
  fs.readFileSync(path.join(CONFORMANCE_DIR, "seeds.json"), "utf8")
);
const actionsFile: ActionsFile = JSON.parse(
  fs.readFileSync(path.join(CONFORMANCE_DIR, "actions.json"), "utf8")
);
const SEEDS = seedsFile.seeds;

const {
  oracleActions: ORACLE_ACTIONS,
  throwingActions: THROWING_ACTIONS,
  supportedExpected: PRISMA_SUPPORTED_EXPECTED,
} = classifyActionsForAdapter(actionsFile, "prisma");
const PRISMA_KNOWN_DIVERGENCES = new Set(
  (actionsFile.knownDivergences ?? [])
    .filter((entry) => entry.adapters.includes("prisma"))
    .map((entry) => entry.action)
);
const EXPECTED_UNSUPPORTED_ACTIONS = new Set(
  actionsFile.expectedUnsupported.map((entry) => entry.action)
);
const MANIFEST_ACTIONS = new Set([
  ...actionsFile.conformance,
  ...EXPECTED_UNSUPPORTED_ACTIONS,
  ...PRISMA_KNOWN_DIVERGENCES,
]);

function doubleFor(seed: Seed): number | null {
  switch (seed.id) {
    case "a1":
      return -0.6;
    case "a2":
      return 0.25;
    case "a3":
      return null;
    default:
      return seed.aNumber + 0.3;
  }
}

function labelsFor(seed: Seed): (string | null)[] {
  switch (seed.id) {
    case "a1":
      return ["gold", "silver"];
    case "a6":
      return [null, "silver"];
    case "a8":
      return ["silver"];
    case "c1":
      return ["Gold"];
    default:
      return [];
  }
}

/** Deterministic ISO instant per seed for the timestamp probe: split around 2025-01-01. */
function isoFor(seed: Seed): string {
  return seed.aNumber >= 2 ? "2024-06-01T00:00:00Z" : "2026-06-01T00:00:00Z";
}

function timestampFor(seed: Seed): string | null {
  switch (seed.id) {
    case "a1":
      return "2020-03-15T10:30:00Z";
    case "a2":
      return "2037-01-01T00:00:00Z";
    case "a3":
      return null;
    case "a4":
      return "2024-06-01T00:00:00Z";
    case "a5":
      return "2020-03-15T10:30:00.123456Z";
    default:
      return seed.aNumber >= 2
        ? "2036-06-06T06:06:06Z"
        : "2021-05-05T05:05:05Z";
  }
}

function scopeFor(seed: Seed): string | null {
  switch (seed.id) {
    case "a1":
      return "dept";
    case "a2":
      return "dept.eng";
    case "a3":
      return "dept.eng.platform";
    case "a4":
      return "dept.eng.platform.obs";
    case "a5":
      return "dept.engineering";
    case "a6":
      return "dept.sales";
    case "a8":
      return "";
    case "a9":
      return "50%";
    case "b1":
      return "50%:a_b:x";
    case "b2":
      return "50x:a_b:y";
    case "b3":
      return "50%:aXb:y";
    case "b4":
      return "50%:a_b";
    case "b5":
      return "dept.eng.platform2";
    case "b6":
      return "50%.a_b";
    case "c1":
      return "Dept.Eng";
    case "c2":
      return "dept.eng.";
    case "d1":
      return "[env]:prod:eu";
    case "d2":
      return "e:prod:eu";
    default:
      return null;
  }
}

const MAPPER: Record<string, MapperConfig> = {
  "request.resource.attr.aBool": { field: "aBool" },
  "request.resource.attr.aString": { field: "aString" },
  "request.resource.attr.aNumber": { field: "aNumber" },
  "request.resource.attr.aDouble": { field: "aDouble" },
  "request.resource.attr.aOptionalString": { field: "aOptionalString" },
  "request.resource.attr.createdBy": { field: "createdBy" },
  "request.resource.attr.scope": { field: "scope" },
  "request.resource.attr.createdAt": {
    field: "createdAt",
    valueType: "dateTime",
  },
  "request.resource.attr.owner": {
    field: "aOptionalString",
    nullable: true,
  },
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
  "request.resource.attr.tagNames": {
    relation: {
      name: "tags",
      type: "many",
      field: "name",
      fields: { name: { field: "name", nullable: true } },
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
            fields: {
              name: { field: "name" },
              labels: {
                relation: {
                  name: "labels",
                  type: "many",
                  fields: {
                    name: { field: "name", nullable: true },
                  },
                },
              },
            },
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
  await prisma.adversarialLabel.deleteMany();
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
        aDouble: doubleFor(seed),
        aOptionalString: seed.aOptionalString,
        createdBy: isoFor(seed),
        scope: scopeFor(seed),
        createdAt: timestampFor(seed),
        tags: {
          create: seed.tags.map((t) => ({ tagId: t.id, name: t.name })),
        },
        categories: {
          create: seed.subCategoryNames.map((subName) => ({
            name: "business",
            subCategories: {
              create: [
                {
                  name: subName,
                  labels: {
                    create: labelsFor(seed).map((name) => ({ name })),
                  },
                },
              ],
            },
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
function asTagAttribute(tag: Tag): Record<string, Value> {
  const attr: Record<string, Value> = { id: tag.id };
  if (tag.name !== null) {
    attr["name"] = tag.name;
  }
  return attr;
}

/** Cerbos attributes mirroring exactly what the seeded DB row holds. */
function asCheckResource(seed: Seed): Resource {
  const attr: Record<string, Value> = {
    aBool: seed.aBool,
    aString: seed.aString,
    aNumber: seed.aNumber,
    createdBy: isoFor(seed),
    owner: seed.aOptionalString,
    tagNames: seed.tags.map((tag) => tag.name),
    obj: { inner: seed.aString },
    tags: seed.tags.map(asTagAttribute),
    categories: seed.subCategoryNames.map((subName) => ({
      name: "business",
      subCategories: [
        {
          name: subName,
          labels: labelsFor(seed).map((name): Record<string, Value> =>
            name === null ? {} : { name }
          ),
        },
      ],
    })),
  };
  // A DB NULL is a missing attribute on the check side — conditions touching it must deny
  // (CEL error), matching SQL three-valued logic excluding the row.
  if (seed.aOptionalString !== null) {
    attr["aOptionalString"] = seed.aOptionalString;
  }
  const aDouble = doubleFor(seed);
  if (aDouble !== null) {
    attr["aDouble"] = aDouble;
  }
  const scope = scopeFor(seed);
  if (scope !== null) {
    attr["scope"] = scope;
  }
  const createdAt = timestampFor(seed);
  if (createdAt !== null) {
    attr["createdAt"] = createdAt;
  }
  // mainCategory mirrors the row's single category as ONE nested object (the seeder creates
  // at most one category per seed), so direct dotted-chain CEL expressions evaluate cleanly;
  // rows without a category get NO attribute — a CEL missing-attr error (deny), matching the
  // adapter's empty join chain excluding the row.
  if (seed.subCategoryNames.length > 0) {
    attr["mainCategory"] = {
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
  test("adapter-supported expected shapes move from throwing to oracle", () => {
    const promotedAction = "promoted-shape";
    const classification = classifyActionsForAdapter(
      {
        conformance: [],
        adapterSupportedExpected: {
          prisma: [{ action: promotedAction, reason: "supported by Prisma" }],
        },
        expectedUnsupported: [
          {
            action: promotedAction,
            shape: "synthetic globally unsupported shape",
            springDataMessage: "unsupported",
          },
        ],
      },
      "prisma"
    );

    expect(classification.oracleActions).toContain(promotedAction);
    expect(
      classification.throwingActions.map(([action]) => action)
    ).not.toContain(promotedAction);
  });

  test("manifest assigns all 118 policy actions exactly one Prisma outcome", () => {
    const oracle = new Set(ORACLE_ACTIONS);
    const throwing = new Set(THROWING_ACTIONS.map(([action]) => action));
    const misclassified = [...MANIFEST_ACTIONS].filter((action) => {
      const classificationCount = [
        oracle.has(action),
        throwing.has(action),
        PRISMA_KNOWN_DIVERGENCES.has(action),
      ].filter(Boolean).length;
      return classificationCount !== 1;
    });

    expect(MANIFEST_ACTIONS.size).toBe(120);
    expect(misclassified).toEqual([]);
    expect(
      [...PRISMA_SUPPORTED_EXPECTED].filter(
        (action) => !EXPECTED_UNSUPPORTED_ACTIONS.has(action)
      )
    ).toEqual([]);
  });

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

  test("pins the upstream has() planner over-grant", async () => {
    const action = "p-has";
    expect(PRISMA_KNOWN_DIVERGENCES.has(action)).toBe(true);
    const queryPlan = await cerbos.planResources({
      principal: principal(),
      resource: { kind: seedsFile.resourceKind },
      action,
    });
    const oracle = await oracleAllowedIds(action);
    const allIds = SEEDS.map((seed) => seed.id).sort();

    expect(queryPlan.kind).toBe(PlanKind.ALWAYS_ALLOWED);
    expect(oracle.length).toBeGreaterThan(0);
    expect(oracle.length).toBeLessThan(allIds.length);
    expect(oracle).toContain("a1");
    expect(await adapterFilteredIds(action)).toEqual(allIds);
  });

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
