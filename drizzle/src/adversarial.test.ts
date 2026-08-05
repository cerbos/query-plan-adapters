import * as fs from "fs";
import * as path from "path";

import { afterAll, beforeAll, describe, expect, test } from "@jest/globals";
import { GRPC as Cerbos } from "@cerbos/grpc";
import type { Principal, Resource, Value } from "@cerbos/core";
import Database from "better-sqlite3";
import { drizzle } from "drizzle-orm/better-sqlite3";
import { integer, real, sqliteTable, text } from "drizzle-orm/sqlite-core";

import { queryPlanToDrizzle, PlanKind } from ".";
import type { MapperEntry, RelationMapping } from ".";

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
 * Drizzle-specific translation (SQLite schema, seeding, field mapping, executing the query).
 */

// Dedicated ports (gRPC 3621) so this suite can run alongside other adapters' sidecars.
const cerbos = new Cerbos("127.0.0.1:3621", { tls: false });

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
  nullRepresentationOmitted: AdapterUnsupportedEntry[];
  knownDivergences?: KnownDivergence[];
}

const seedsFile: SeedsFile = JSON.parse(
  fs.readFileSync(path.join(CONFORMANCE_DIR, "seeds.json"), "utf8")
);
const actionsFile: ActionsFile = JSON.parse(
  fs.readFileSync(path.join(CONFORMANCE_DIR, "actions.json"), "utf8")
);
const SEEDS = seedsFile.seeds;

// Reference actions this adapter cannot express without changing CEL semantics. The shared
// manifest is the source of truth so the package-local harness and README stay aligned.
const DRIZZLE_UNSUPPORTED: AdapterUnsupportedEntry[] = [
  ...(actionsFile.adapterUnsupported?.["drizzle"] ?? []),
];

const DRIZZLE_SUPPORTED_EXPECTED = new Set(
  (actionsFile.adapterSupportedExpected?.["drizzle"] ?? []).map(
    (entry) => entry.action
  )
);

const DRIZZLE_DIVERGENCES = new Set(
  (actionsFile.knownDivergences ?? [])
    .filter((entry) => entry.adapters.includes("drizzle"))
    .map((entry) => entry.action)
);

const UNSUPPORTED_ACTIONS = new Set(DRIZZLE_UNSUPPORTED.map((u) => u.action));
const EXPECTED_UNSUPPORTED_ACTIONS = new Set(
  actionsFile.expectedUnsupported.map((entry) => entry.action)
);
const ORACLE_ACTIONS = [
  ...actionsFile.conformance.filter(
    (action) => !UNSUPPORTED_ACTIONS.has(action)
  ),
  ...[...DRIZZLE_SUPPORTED_EXPECTED].sort(),
];
type ThrowingAction = readonly [action: string, reason: string];
// Globally unsupported planner shapes plus any declared Drizzle limitations: these must
// fail loudly during translation, never silently return a wrong id set.
const THROWING_ACTIONS: ThrowingAction[] = [
  ...DRIZZLE_UNSUPPORTED.map(
    (entry): ThrowingAction => [entry.action, entry.reason]
  ),
  ...actionsFile.expectedUnsupported
    .filter((entry) => !DRIZZLE_SUPPORTED_EXPECTED.has(entry.action))
    .map((entry): ThrowingAction => [entry.action, entry.shape]),
];

// Actions whose `== null` probe targets an attribute the oracle OMITS for NULL columns. They
// carry no oracle comparison: under the omitted representation check() denies every row, so the
// adapter must reject the shape rather than emit a filter (#302).
const NULL_REPRESENTATION_OMITTED = actionsFile.nullRepresentationOmitted.map(
  (entry): ThrowingAction => [entry.action, entry.reason]
);

const MANIFEST_ACTIONS = new Set([
  ...actionsFile.conformance,
  ...actionsFile.expectedUnsupported.map((entry) => entry.action),
  ...NULL_REPRESENTATION_OMITTED.map(([action]) => action),
  ...DRIZZLE_SUPPORTED_EXPECTED,
  ...DRIZZLE_DIVERGENCES,
]);

/** Deterministic ISO instant per seed for the timestamp probe: split around 2025-01-01. */
function isoFor(seed: Seed): string {
  return seed.aNumber >= 2 ? "2024-06-01T00:00:00Z" : "2026-06-01T00:00:00Z";
}

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

// -- dedicated SQLite schema (adversarial.db, gitignored) --

const DB_PATH = path.join(__dirname, "..", "adversarial.db");
fs.rmSync(DB_PATH, { force: true });
const sqlite = new Database(DB_PATH);
const db = drizzle(sqlite);

const adversarialResources = sqliteTable("adversarial_resources", {
  id: text("id").primaryKey(),
  aBool: integer("a_bool", { mode: "boolean" }).notNull(),
  aString: text("a_string").notNull(),
  aNumber: integer("a_number").notNull(),
  aDouble: real("a_double"),
  aOptionalString: text("a_optional_string"),
  createdBy: text("created_by").notNull(),
  scope: text("scope"),
  createdAt: text("created_at"),
});

const adversarialTags = sqliteTable("adversarial_tags", {
  tagId: text("tag_id").primaryKey(),
  // NULLABLE on purpose: a NULL tag name is a missing element attribute on the check
  // side (a CEL error → deny) and must stay UNKNOWN — never FALSE — in SQL.
  name: text("name"),
  resourceId: text("resource_id").notNull(),
});

const adversarialCategories = sqliteTable("adversarial_categories", {
  id: text("id").primaryKey(),
  name: text("name").notNull(),
  resourceId: text("resource_id").notNull(),
});

const adversarialSubCategories = sqliteTable("adversarial_sub_categories", {
  id: text("id").primaryKey(),
  name: text("name").notNull(),
  categoryId: text("category_id").notNull(),
});

const adversarialLabels = sqliteTable("adversarial_labels", {
  id: text("id").primaryKey(),
  name: text("name"),
  subCategoryId: text("sub_category_id").notNull(),
});

const labelsRelation: RelationMapping = {
  type: "many",
  table: adversarialLabels,
  sourceColumn: adversarialSubCategories.id,
  targetColumn: adversarialLabels.subCategoryId,
  field: adversarialLabels.name,
  fields: {
    name: adversarialLabels.name,
  },
};

const subCategoriesRelation: RelationMapping = {
  type: "many",
  table: adversarialSubCategories,
  sourceColumn: adversarialCategories.id,
  targetColumn: adversarialSubCategories.categoryId,
  field: adversarialSubCategories.name,
  fields: {
    name: adversarialSubCategories.name,
    labels: { relation: labelsRelation },
  },
};

const MAPPER: Record<string, MapperEntry> = {
  "request.resource.attr.aBool": adversarialResources.aBool,
  "request.resource.attr.aString": adversarialResources.aString,
  "request.resource.attr.aNumber": adversarialResources.aNumber,
  "request.resource.attr.aDouble": adversarialResources.aDouble,
  "request.resource.attr.aOptionalString": adversarialResources.aOptionalString,
  "request.resource.attr.createdBy": adversarialResources.createdBy,
  "request.resource.attr.scope": adversarialResources.scope,
  "request.resource.attr.createdAt": {
    column: adversarialResources.createdAt,
    valueType: "timestamp",
  },
  "request.resource.attr.owner": adversarialResources.aOptionalString,
  // obj.inner is not a real nested column — mirrors aString, same trick the spring-data
  // and prisma reference harnesses use for the p-struct probe.
  "request.resource.attr.obj.inner": adversarialResources.aString,
  "request.resource.attr.tags": {
    relation: {
      type: "many",
      table: adversarialTags,
      sourceColumn: adversarialResources.id,
      targetColumn: adversarialTags.resourceId,
      field: adversarialTags.name,
      fields: {
        id: adversarialTags.tagId,
        name: adversarialTags.name,
      },
    },
  },
  "request.resource.attr.tagNames": {
    collectionValueType: "scalar",
    relation: {
      type: "many",
      table: adversarialTags,
      sourceColumn: adversarialResources.id,
      targetColumn: adversarialTags.resourceId,
      field: adversarialTags.name,
    },
  },
  "request.resource.attr.categories": {
    relation: {
      type: "many",
      table: adversarialCategories,
      sourceColumn: adversarialResources.id,
      targetColumn: adversarialCategories.resourceId,
      fields: {
        name: adversarialCategories.name,
        subCategories: { relation: subCategoriesRelation },
      },
    },
  },
  // Multi-hop chain probe (W1): mainCategory mirrors the SAME categories/subCategories
  // relation as a single-object dotted chain on the check side (every seed holds at most
  // one category), pinning that the adapter joins through every intermediate hop, never
  // off the root. subNames flattens the tail's name column for plain `in` membership.
  "request.resource.attr.mainCategory": {
    relation: {
      type: "many",
      table: adversarialCategories,
      sourceColumn: adversarialResources.id,
      targetColumn: adversarialCategories.resourceId,
      fields: {
        name: adversarialCategories.name,
        subCategories: { relation: subCategoriesRelation },
        subNames: { relation: subCategoriesRelation },
      },
    },
  },
};

beforeAll(() => {
  sqlite.exec(`
    CREATE TABLE adversarial_resources (
      id TEXT PRIMARY KEY,
      a_bool INTEGER NOT NULL,
      a_string TEXT NOT NULL,
      a_number INTEGER NOT NULL,
      a_double REAL,
      a_optional_string TEXT,
      created_by TEXT NOT NULL,
      scope TEXT,
      created_at TEXT
    );
    CREATE TABLE adversarial_tags (
      tag_id TEXT PRIMARY KEY,
      name TEXT,
      resource_id TEXT NOT NULL
    );
    CREATE TABLE adversarial_categories (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      resource_id TEXT NOT NULL
    );
    CREATE TABLE adversarial_sub_categories (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      category_id TEXT NOT NULL
    );
    CREATE TABLE adversarial_labels (
      id TEXT PRIMARY KEY,
      name TEXT,
      sub_category_id TEXT NOT NULL
    );
  `);

  // Distinct category/sub-category graphs per seed so no rows share relations by accident.
  for (const seed of SEEDS) {
    db.insert(adversarialResources)
      .values({
        id: seed.id,
        aBool: seed.aBool,
        aString: seed.aString,
        aNumber: seed.aNumber,
        aDouble: doubleFor(seed),
        aOptionalString: seed.aOptionalString,
        createdBy: isoFor(seed),
        scope: scopeFor(seed),
        createdAt: timestampFor(seed),
      })
      .run();
    for (const tag of seed.tags) {
      db.insert(adversarialTags)
        .values({ tagId: tag.id, name: tag.name, resourceId: seed.id })
        .run();
    }
    seed.subCategoryNames.forEach((subName, index) => {
      const categoryId = `${seed.id}-cat-${index}`;
      db.insert(adversarialCategories)
        .values({ id: categoryId, name: "business", resourceId: seed.id })
        .run();
      db.insert(adversarialSubCategories)
        .values({
          id: `${categoryId}-sub`,
          name: subName,
          categoryId,
        })
        .run();
      labelsFor(seed).forEach((labelName, labelIndex) => {
        db.insert(adversarialLabels)
          .values({
            id: `${categoryId}-label-${labelIndex}`,
            name: labelName,
            subCategoryId: `${categoryId}-sub`,
          })
          .run();
      });
    });
  }
});

afterAll(() => {
  cerbos.close();
  sqlite.close();
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

function asLabelAttribute(name: string | null): Record<string, Value> {
  return name === null ? {} : { name };
}

/** Cerbos attributes mirroring exactly what the seeded DB row holds. */
function asCheckResource(seed: Seed): Resource {
  const attr: Record<string, Value> = {
    aBool: seed.aBool,
    aString: seed.aString,
    aNumber: seed.aNumber,
    createdBy: isoFor(seed),
    owner: seed.aOptionalString,
    obj: { inner: seed.aString },
    tags: seed.tags.map(asTagAttribute),
    tagNames: seed.tags.map((tag) => tag.name),
    categories: seed.subCategoryNames.map((subName) => ({
      name: "business",
      subCategories: [
        {
          name: subName,
          labels: labelsFor(seed).map(asLabelAttribute),
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

// -- adapter execution through the public queryPlanToDrizzle path --

async function adapterFilteredIds(
  action: string,
  nullAttributeRepresentation: "explicit" | "omitted" = "explicit"
): Promise<string[]> {
  const queryPlan = await cerbos.planResources({
    principal: principal(),
    resource: { kind: seedsFile.resourceKind },
    action,
  });
  const result = queryPlanToDrizzle({
    queryPlan,
    mapper: MAPPER,
    nullAttributeRepresentation,
  });
  if (result.kind === PlanKind.ALWAYS_DENIED) {
    return [];
  }
  const baseQuery = db
    .select({ id: adversarialResources.id })
    .from(adversarialResources);
  const rows =
    result.kind === PlanKind.CONDITIONAL
      ? baseQuery.where(result.filter).all()
      : baseQuery.all();
  return rows.map((row) => row.id).sort();
}

/** Whether any operand anywhere in the plan is a literal null, or a list containing one. */
function planCarriesNullLiteral(operand: unknown): boolean {
  if (typeof operand !== "object" || operand === null) return false;
  const node = operand as Record<string, unknown>;
  if ("value" in node) {
    const value = node["value"];
    return value === null || (Array.isArray(value) && value.includes(null));
  }
  const operands = node["operands"];
  return Array.isArray(operands) && operands.some(planCarriesNullLiteral);
}

describe("adversarial conformance corpus", () => {
  test("manifest assigns every action exactly one Drizzle outcome", () => {
    const oracle = new Set(ORACLE_ACTIONS);
    const throwing = new Set(THROWING_ACTIONS.map(([action]) => action));
    const nullOmitted = new Set(
      NULL_REPRESENTATION_OMITTED.map(([action]) => action)
    );
    const misclassified = [...MANIFEST_ACTIONS].filter((action) => {
      const classificationCount = [
        oracle.has(action),
        throwing.has(action),
        nullOmitted.has(action),
        DRIZZLE_DIVERGENCES.has(action),
      ].filter(Boolean).length;
      return classificationCount !== 1;
    });

    expect(MANIFEST_ACTIONS.size).toBe(127);
    expect(NULL_REPRESENTATION_OMITTED).toHaveLength(1);
    expect(misclassified).toEqual([]);
    expect(
      [...DRIZZLE_SUPPORTED_EXPECTED].filter(
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

  // Shapes the adapter does not support must fail during translation, never produce a
  // silently-wrong filter.
  test.each(THROWING_ACTIONS)(
    "%s fails loudly instead of silently mistranslating (%s)",
    async (action) => {
      await expect(adapterFilteredIds(action)).rejects.toThrow();
    }
  );

  // #302. `null-eq-missing` probes `aOptionalString == null`, and `aOptionalString` follows the
  // corpus default: a NULL column sends NO attribute. Both halves are asserted because the
  // rejection alone would pass vacuously if the adapter threw for an unrelated reason — the
  // over-grant under the default representation is what makes the rejection necessary.
  test.each(NULL_REPRESENTATION_OMITTED)(
    "%s over-grants under the explicit representation and is rejected under omitted (%s)",
    async (action) => {
      const oracle = await oracleAllowedIds(action);
      expect(oracle).toEqual([]);

      // The default translation emits IS NULL and returns exactly the rows the PDP denies.
      const overGranted = await adapterFilteredIds(action, "explicit");
      expect(overGranted.length).toBeGreaterThan(0);

      await expect(adapterFilteredIds(action, "omitted")).rejects.toThrow(
        /missing-attribute error/
      );
    }
  );

  // #302 completeness guard. The rejection must key off the null OPERAND, not off a list of
  // operators: `hasIntersection(tagNames, ["public", null])` carries one in its value list, and
  // an allowlist of eq/ne/in silently misses it. Enumerating the corpus rather than naming
  // shapes means a newly added action carrying a null constant is covered automatically.
  test("every corpus action carrying a null literal is rejected under omitted", async () => {
    const nullCarrying: string[] = [];
    for (const action of [...MANIFEST_ACTIONS].sort()) {
      const queryPlan = await cerbos.planResources({
        principal: principal(),
        resource: { kind: seedsFile.resourceKind },
        action,
      });
      if (
        queryPlan.kind === PlanKind.CONDITIONAL &&
        planCarriesNullLiteral(queryPlan.condition)
      ) {
        nullCarrying.push(action);
      }
    }

    // Guard the guard: if the walk stopped finding null operands the loop below is vacuous.
    expect(nullCarrying).toContain("null-eq-missing");
    expect(nullCarrying).toContain("in-null-elem-hasint");

    const notRejected: string[] = [];
    for (const action of nullCarrying) {
      try {
        await adapterFilteredIds(action, "omitted");
        notRejected.push(action);
      } catch {
        // expected: the shape must be rejected under this representation
      }
    }
    expect(notRejected).toEqual([]);
  });

  test("pins the upstream has() planner over-grant", async () => {
    const action = "p-has";
    expect(DRIZZLE_DIVERGENCES.has(action)).toBe(true);
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
    for (const action of ["vf-le", "like-percent", "all-on-empty", "pv-exists", "pv-all", "null-eq", "null-ne"]) {
      const ids = await oracleAllowedIds(action);
      expect(ids.length).toBeGreaterThan(0);
      expect(ids.length).toBeLessThan(SEEDS.length);
    }
  });
});
