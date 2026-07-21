import * as fs from "fs";
import * as path from "path";

import { afterAll, beforeAll, describe, expect, test } from "@jest/globals";
import { GRPC as Cerbos } from "@cerbos/grpc";
import type { Principal, Resource, Value } from "@cerbos/core";
import Database from "better-sqlite3";
import { drizzle } from "drizzle-orm/better-sqlite3";
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core";

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

// Shapes the Drizzle adapter genuinely cannot express as SQL. Drizzle builds raw SQL
// (COUNT subqueries, LENGTH(), field-to-field instr/substr matches, CASE ternaries,
// WHERE-clause arithmetic), so the list is currently EMPTY. Kept as a LOCAL constant only
// until conformance/actions.json gains an `adapterUnsupported.drizzle` key — move these
// entries there when it does.
const DRIZZLE_UNSUPPORTED: AdapterUnsupportedEntry[] = [
  ...(actionsFile.adapterUnsupported?.["drizzle"] ?? []),
];

const UNSUPPORTED_ACTIONS = new Set(DRIZZLE_UNSUPPORTED.map((u) => u.action));
const ORACLE_ACTIONS = actionsFile.conformance.filter(
  (action) => !UNSUPPORTED_ACTIONS.has(action)
);
// Globally unsupported planner shapes plus any declared Drizzle limitations: these must
// fail loudly (translation throw, or — for p-matches, which translates to a REGEXP
// predicate — a query-time error because this suite deliberately registers no REGEXP
// UDF), never silently return a wrong id set.
const THROWING_ACTIONS = [
  ...DRIZZLE_UNSUPPORTED.map((u) => [u.action, u.reason] as const),
  ...actionsFile.expectedUnsupported.map((u) => [u.action, u.shape] as const),
];

/** Deterministic ISO instant per seed for the timestamp probe: split around 2025-01-01. */
function isoFor(seed: Seed): string {
  return seed.aNumber >= 2 ? "2024-06-01T00:00:00Z" : "2026-06-01T00:00:00Z";
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
  aOptionalString: text("a_optional_string"),
  createdBy: text("created_by").notNull(),
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

const subCategoriesRelation: RelationMapping = {
  type: "many",
  table: adversarialSubCategories,
  sourceColumn: adversarialCategories.id,
  targetColumn: adversarialSubCategories.categoryId,
  field: adversarialSubCategories.name,
  fields: {
    name: adversarialSubCategories.name,
  },
};

const MAPPER: Record<string, MapperEntry> = {
  "request.resource.attr.aBool": adversarialResources.aBool,
  "request.resource.attr.aString": adversarialResources.aString,
  "request.resource.attr.aNumber": adversarialResources.aNumber,
  "request.resource.attr.aOptionalString": adversarialResources.aOptionalString,
  "request.resource.attr.createdBy": adversarialResources.createdBy,
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
      a_optional_string TEXT,
      created_by TEXT NOT NULL
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
  `);

  // Distinct category/sub-category graphs per seed so no rows share relations by accident.
  for (const seed of SEEDS) {
    db.insert(adversarialResources)
      .values({
        id: seed.id,
        aBool: seed.aBool,
        aString: seed.aString,
        aNumber: seed.aNumber,
        aOptionalString: seed.aOptionalString,
        createdBy: isoFor(seed),
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

/** Cerbos attributes mirroring exactly what the seeded DB row holds. */
function asCheckResource(seed: Seed): Resource {
  const attr: Record<string, Value> = {
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
    attr["aOptionalString"] = seed.aOptionalString;
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

async function adapterFilteredIds(action: string): Promise<string[]> {
  const queryPlan = await cerbos.planResources({
    principal: principal(),
    resource: { kind: seedsFile.resourceKind },
    action,
  });
  const result = queryPlanToDrizzle({ queryPlan, mapper: MAPPER });
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

  // Shapes the adapter does not support: translation (or execution, for p-matches) must
  // fail loudly, never produce a silently-wrong filter.
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
