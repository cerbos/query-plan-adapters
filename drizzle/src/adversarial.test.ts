import * as fs from "fs";
import * as path from "path";

import { afterAll, beforeAll, describe, expect, test } from "@jest/globals";
import { GRPC as Cerbos } from "@cerbos/grpc";
import {
  PlanExpression,
  PlanExpressionValue,
  PlanExpressionVariable,
} from "@cerbos/core";
import type {
  PlanExpressionOperand,
  Principal,
  Resource,
  Value,
} from "@cerbos/core";
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
  /** One entry per adapter that must reject the shape; the corpus asserts the key set. */
  messages: Record<string, string>;
}

interface AdapterUnsupportedEntry {
  action: string;
  reason: string;
  /** Absent on `adapterSupportedExpected` / `nullRepresentationOmitted`, required on a throw. */
  message?: string;
}

/**
 * A `nullRepresentationOmitted` entry. Every adapter must reject these — the two NULL conventions
 * are indistinguishable on the wire — so `messages` names the whole roster with no promotions to
 * subtract.
 */
interface NullRepresentationOmittedEntry {
  action: string;
  reason: string;
  messages: Record<string, string>;
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
  nullRepresentationOmitted: NullRepresentationOmittedEntry[];
  knownDivergences?: KnownDivergence[];
}

// -- corpus coverage guards ---------------------------------------------------------------------
//
// The same parsed seed feeds the stored row AND the check() oracle, so a corpus field this harness
// does not consume is dropped from both sides at once and the differential agrees for the wrong
// reason — the projection trap conformance/README.md describes for actions.json, applied to the
// seeds. Asserting set equality catches both directions: a corpus key nothing here reads, and a key
// this harness reads that the corpus no longer carries.

const SEED_KEYS = [
  "id",
  "aBool",
  "aString",
  "aNumber",
  "aOptionalString",
  "tags",
  "subCategoryNames",
] as const;

/** Corpus prose, never read by a harness: the one documented exclusion from SEED_KEYS. */
const SEED_NOTE_KEY = "note";

/** The one nested object array a seed carries. A key added inside an element is dropped from both
 * sides of the differential just as silently as a top-level one, so it is guarded the same way. */
const TAG_KEYS = ["id", "name"] as const;

const DERIVED_KEYS = [
  "createdBy",
  "aDouble",
  "createdAt",
  "scope",
  "labels",
] as const;

/** One seed's derived fields, exactly as conformance/derived-fields.json carries them. */
interface DerivedEntry {
  createdBy: string;
  aDouble: number | null;
  createdAt: string | null;
  scope: string | null;
  labels: (string | null)[];
}

interface DerivedFile {
  fields: string[];
  derived: Record<string, DerivedEntry>;
}

function assertKeys(
  label: string,
  got: string[],
  want: readonly string[],
  optional: readonly string[] = []
): void {
  const allowed = new Set<string>([...want, ...optional]);
  for (const key of got) {
    if (!allowed.has(key)) {
      throw new Error(
        `${label} carries "${key}", which this harness does not consume: an unconsumed corpus field is dropped from the stored row and the check() oracle at once`
      );
    }
  }
  const present = new Set(got);
  for (const key of want) {
    if (!present.has(key)) {
      throw new Error(
        `${label} is missing "${key}", which this harness consumes`
      );
    }
  }
}

const seedsFile: SeedsFile = JSON.parse(
  fs.readFileSync(path.join(CONFORMANCE_DIR, "seeds.json"), "utf8")
);
const actionsFile: ActionsFile = JSON.parse(
  fs.readFileSync(path.join(CONFORMANCE_DIR, "actions.json"), "utf8")
);
const derivedFile: DerivedFile = JSON.parse(
  fs.readFileSync(path.join(CONFORMANCE_DIR, "derived-fields.json"), "utf8")
);
const SEEDS = seedsFile.seeds;

// SEEDS holds the parsed JSON rows verbatim, so Object.keys reports the corpus key set. Keep it
// that way: a parser that rebuilt each row field by field could only ever report the keys this
// harness already names, and the assertion would pass vacuously.
SEEDS.forEach((seed, index) => {
  const label = `seeds.json seeds[${index}]`;
  assertKeys(label, Object.keys(seed), SEED_KEYS, [SEED_NOTE_KEY]);
  seed.tags.forEach((tag, tagIndex) => {
    assertKeys(`${label}.tags[${tagIndex}]`, Object.keys(tag), TAG_KEYS);
  });
});

assertKeys("derived-fields.json fields", derivedFile.fields, DERIVED_KEYS);
const DERIVED_IDS = Object.keys(derivedFile.derived);
if (DERIVED_IDS.length !== SEEDS.length) {
  throw new Error(
    `derived-fields.json has ${DERIVED_IDS.length} entries for ${SEEDS.length} seeds`
  );
}
for (const seed of SEEDS) {
  assertKeys(
    `derived-fields.json derived["${seed.id}"]`,
    Object.keys(derivedFor(seed)),
    DERIVED_KEYS
  );
}

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
/**
 * A shape this adapter must refuse, with the substring its error has to contain.
 *
 * The message is what turns "it threw" into "it threw for the declared reason": without it a
 * mapper typo or an unrelated validation satisfies the assertion just as well as the limitation
 * the corpus documents (cerbos/query-plan-adapters#326).
 */
type ThrowingAction = readonly [action: string, reason: string, message: string];

/** The pinned message, or a failure — a throwing action without one asserts nothing. */
function requireMessage(label: string, message: string | undefined): string {
  if (message === undefined || message === "") {
    throw new Error(
      `actions.json pins no throw message for ${label}: the throw suite would accept a failure for any reason`
    );
  }
  return message;
}

// Globally unsupported planner shapes plus any declared Drizzle limitations: these must
// fail loudly during translation, never silently return a wrong id set.
const THROWING_ACTIONS: ThrowingAction[] = [
  ...DRIZZLE_UNSUPPORTED.map(
    (entry): ThrowingAction => [
      entry.action,
      entry.reason,
      requireMessage(
        `adapterUnsupported.drizzle.${entry.action}`,
        entry.message
      ),
    ]
  ),
  ...actionsFile.expectedUnsupported
    .filter((entry) => !DRIZZLE_SUPPORTED_EXPECTED.has(entry.action))
    .map((entry): ThrowingAction => [
      entry.action,
      entry.shape,
      requireMessage(
        `expectedUnsupported.${entry.action}.messages.drizzle`,
        entry.messages?.["drizzle"]
      ),
    ]),
];

// Actions whose `== null` probe targets an attribute the oracle OMITS for NULL columns. They
// carry no oracle comparison: under the omitted representation check() denies every row, so the
// adapter must reject the shape rather than emit a filter (#302).
const NULL_REPRESENTATION_OMITTED = actionsFile.nullRepresentationOmitted.map(
  (entry): ThrowingAction => [
    entry.action,
    entry.reason,
    requireMessage(
      `nullRepresentationOmitted.${entry.action}.messages.drizzle`,
      entry.messages?.["drizzle"]
    ),
  ]
);
/** The one message every null-carrying action must be rejected with under `omitted`. */
const NULL_OMITTED_MESSAGE = NULL_REPRESENTATION_OMITTED[0]?.[2] ?? "";

const MANIFEST_ACTIONS = new Set([
  ...actionsFile.conformance,
  ...actionsFile.expectedUnsupported.map((entry) => entry.action),
  ...NULL_REPRESENTATION_OMITTED.map(([action]) => action),
  ...DRIZZLE_SUPPORTED_EXPECTED,
  // ALL divergences, not just Drizzle's: a divergence registered solely for another adapter
  // must still enter this manifest, so the size tripwire and the classified-exactly-once
  // check flag it for triage here instead of letting the action silently vanish from this
  // harness. Classification/skipping still uses the Drizzle-filtered set.
  ...(actionsFile.knownDivergences ?? []).map((entry) => entry.action),
]);

// -- the degeneracy guard (conformance/README.md, "The degeneracy guard") -----------------------
//
// A representative sample of the actions this adapter ORACLE-COMPARES, one per hostile group.
// Drizzle translates every shape in the sample, so it has no liveness-only probes: each entry is
// asserted to be in `ORACLE_ACTIONS` (cerbos/query-plan-adapters#324), which turns moving one
// into `adapterUnsupported` into a failure here rather than a silent no-op.
//
// w1-size-zero-chain and w1-not-size-chain are deliberately absent: their oracles are empty by
// CONSTRUCTION (no seed holds a to-one parent with zero children), so they cannot satisfy a
// non-empty assertion. Their siblings below carry it for that group.

const DEGENERACY_GUARD_ACTIONS = [
  "vf-le",
  "like-percent",
  "all-on-empty",
  "pv-exists",
  "pv-all",
  "null-eq",
  "null-ne",
  // The absent to-one parent (#309/#315/#316): the five discriminating chain shapes with a
  // non-empty oracle.
  "w1-all-chain",
  "w1-not-exists-chain",
  "w1-size-nonneg-chain",
  "w1-not-in-chain",
  "w1-not-hasint-chain",
  // Column arithmetic under a division (#311).
  "cr-div-neg-zero",
  "cr-div-other-column",
  "cr-div-then-add",
  "cr-div-then-add-ne",
] as const;

// -- deterministic derived fields (conformance/README.md, "Deterministic derived fields") --------
//
// Read from conformance/derived-fields.json rather than restated here. The same value feeds the
// stored row and the check() oracle, so a transcription error would be self-consistent and
// invisible to the differential; one machine-readable definition is what makes that impossible.

function derivedFor(seed: Seed): DerivedEntry {
  const entry = derivedFile.derived[seed.id];
  if (entry === undefined) {
    throw new Error(`derived-fields.json has no entry for seed "${seed.id}"`);
  }
  return entry;
}

/** Deterministic ISO instant per seed for the timestamp probe: split around 2025-01-01. */
function isoFor(seed: Seed): string {
  return derivedFor(seed).createdBy;
}

function doubleFor(seed: Seed): number | null {
  return derivedFor(seed).aDouble;
}

function scopeFor(seed: Seed): string | null {
  return derivedFor(seed).scope;
}

function timestampFor(seed: Seed): string | null {
  return derivedFor(seed).createdAt;
}

/** Third-level label names. A null element is a NULL label name — a missing element attribute. */
function labelsFor(seed: Seed): (string | null)[] {
  return derivedFor(seed).labels;
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

/** The degeneracy guard's per-action assertion, labelled so a failure names the action. */
async function expectNonDegenerateOracle(action: string): Promise<void> {
  const ids = await oracleAllowedIds(action);
  expect({
    action,
    nonEmpty: ids.length > 0,
    nonTotal: ids.length < SEEDS.length,
  }).toEqual({ action, nonEmpty: true, nonTotal: true });
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

  // Adding a throwing action without pinning its message must fail this harness rather than
  // silently degrade the throw suite to a bare "it threw" (cerbos/query-plan-adapters#326).
  test("a throwing action with no pinned message fails classification", () => {
    expect(() => requireMessage("synthetic-entry", undefined)).toThrow(
      /pins no throw message/
    );
    expect(() => requireMessage("synthetic-entry", "")).toThrow(
      /pins no throw message/
    );
  });
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

    expect(MANIFEST_ACTIONS.size).toBe(143);
    expect(NULL_REPRESENTATION_OMITTED).toHaveLength(1);
    // Deliberate tripwire: every one of these carries a pinned message, so a throwing action
    // gained or lost has to be re-triaged here rather than joining the suite unnoticed.
    expect(THROWING_ACTIONS).toHaveLength(10);
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
  // silently-wrong filter. The plan is fetched OUTSIDE the assertion so a PDP failure fails
  // the test instead of passing it, and no query executes — SQLite rejecting a wrongly
  // emitted filter afterwards must not be able to masquerade as the adapter refusing to
  // translate.
  //
  // The message is asserted, not just the throw: a bare `toThrow()` is satisfied by a mapper
  // typo or an unrelated validation, which would leave the classification resting on a failure
  // that has nothing to do with the limitation it declares (cerbos/query-plan-adapters#326).
  test.each(THROWING_ACTIONS)(
    "%s fails during translation with the declared message, before any filter exists (%s)",
    async (action, _reason, message) => {
      const queryPlan = await cerbos.planResources({
        principal: principal(),
        resource: { kind: seedsFile.resourceKind },
        action,
      });
      expect(queryPlan.kind).toBe(PlanKind.CONDITIONAL);
      expect(() =>
        queryPlanToDrizzle({
          queryPlan,
          mapper: MAPPER,
          nullAttributeRepresentation: "explicit",
        })
      ).toThrow(message);
    }
  );

  // #302. `null-eq-missing` probes `aOptionalString == null`, and `aOptionalString` follows the
  // corpus default: a NULL column sends NO attribute. Both halves are asserted because the
  // rejection alone would pass vacuously if the adapter threw for an unrelated reason — the
  // over-grant under the default representation is what makes the rejection necessary.
  test.each(NULL_REPRESENTATION_OMITTED)(
    "%s over-grants under the explicit representation and is rejected under omitted (%s)",
    async (action, _reason, message) => {
      const oracle = await oracleAllowedIds(action);
      expect(oracle).toEqual([]);

      // The default translation emits IS NULL and returns exactly the rows the PDP denies.
      const overGranted = await adapterFilteredIds(action, "explicit");
      expect(overGranted.length).toBeGreaterThan(0);

      await expect(adapterFilteredIds(action, "omitted")).rejects.toThrow(
        message
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
      } catch (error) {
        // The rejection must be the null-operand check talking, not an incidental failure — a
        // transport error or mapper typo counting as the required rejection is the silent pass
        // the corpus README warns about.
        if (!String(error).includes(NULL_OMITTED_MESSAGE)) {
          notRejected.push(`${action} (rejected for the wrong reason: ${String(error)})`);
        }
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

  // The corpus pins two count spellings over the chain — `size(...) == 0` and
  // `!(size(...) > 0)` — but the guard has to be a property of the chain rather than of the
  // two spellings that happen to be pinned. These synthesise the remaining
  // threshold/polarity combinations onto the same seeded store and assert the parentless rows
  // stay out of every one, including an arbitrary-N threshold neither corpus action reaches
  // (cerbos/query-plan-adapters#316). This adapter guards the COUNT expression itself, so it
  // was already aligned — the assertion pins that it stays that way.
  test("every count threshold over the chain inherits the absent-parent guard", async () => {
    const chain = new PlanExpressionVariable(
      "request.resource.attr.mainCategory.subCategories"
    );
    const size = new PlanExpression("size", [chain]);
    const compare = (operator: string, threshold: number) =>
      new PlanExpression(operator, [size, new PlanExpressionValue(threshold)]);
    const negate = (condition: PlanExpressionOperand) =>
      new PlanExpression("not", [condition]);

    const filteredIdsFor = (condition: PlanExpressionOperand): string[] => {
      const result = queryPlanToDrizzle({
        queryPlan: {
          kind: PlanKind.CONDITIONAL,
          condition,
          cerbosCallId: "synthetic",
          requestId: "synthetic",
          validationErrors: [],
          metadata: undefined,
        },
        mapper: MAPPER,
      });
      expect(result.kind).toBe(PlanKind.CONDITIONAL);
      const rows = db
        .select({ id: adversarialResources.id })
        .from(adversarialResources)
        .where(result.kind === PlanKind.CONDITIONAL ? result.filter : undefined)
        .all();
      return rows.map((row) => row.id).sort();
    };

    // Every seed that HAS a mainCategory holds at least one subCategory, and the 16 without
    // it are CEL missing-path errors — so each of these is empty unless the guard leaks.
    const emptyByConstruction: [string, PlanExpressionOperand][] = [
      ["size(chain) == 0", compare("eq", 0)],
      ["size(chain) <= 0", compare("le", 0)],
      ["size(chain) >= 2", compare("ge", 2)],
      ["!(size(chain) > 0)", negate(compare("gt", 0))],
      ["!(size(chain) >= 1)", negate(compare("ge", 1))],
      ["!(size(chain) < 2)", negate(compare("lt", 2))],
    ];

    for (const [shape, condition] of emptyByConstruction) {
      expect([shape, filteredIdsFor(condition)]).toEqual([shape, []]);
    }

    // The mirror image, so the loop above cannot pass by denying everything: `>= 0` and `< 2`
    // are TRUE for exactly the rows that HAVE the parent.
    const withParent = await oracleAllowedIds("w1-size-nonneg-chain");
    expect(withParent.length).toBeGreaterThan(0);
    expect(withParent.length).toBeLessThan(SEEDS.length);
    expect(filteredIdsFor(compare("ge", 0))).toEqual(withParent);
    expect(filteredIdsFor(compare("lt", 2))).toEqual(withParent);
  });

  test("oracle is not degenerate", async () => {
    // Guard the guard: each of these actions must produce a non-empty, non-total oracle set,
    // otherwise the differential comparison could pass vacuously (e.g. PDP denying all).
    for (const action of DEGENERACY_GUARD_ACTIONS) {
      expect(ORACLE_ACTIONS).toContain(action);
      await expectNonDegenerateOracle(action);
    }
  });
});
