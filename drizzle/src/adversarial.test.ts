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
import { eq, sql } from "drizzle-orm";
import type { SQL } from "drizzle-orm";
import { drizzle as drizzleSqlite } from "drizzle-orm/better-sqlite3";
import { drizzle as drizzlePostgres } from "drizzle-orm/node-postgres";
import { PostgreSqlContainer } from "@testcontainers/postgresql";
import type { StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import { Pool } from "pg";

import { queryPlanToDrizzle, PlanKind } from ".";
import type { MapperEntry } from ".";
import {
  ADAPTER,
  CONFORMANCE_DIR,
  buildMapper,
  classifyActionsForAdapter,
  postgresSchema,
  requireMessage,
  sqliteSchema,
} from "./corpus";
import type { ActionsFile, ThrowingAction } from "./corpus";

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
 * Drizzle-specific translation (schema, seeding, field mapping, executing the query).
 *
 * The whole corpus is replayed against every store this adapter claims to support, one store per
 * run, selected with `ADAPTER_TEST_DB` (`sqlite` by default, `postgres` for the container-backed
 * leg). Drizzle's dialect objects own quoting and placeholders, but the adapter still makes
 * dialect-sensitive choices of its own — `float(53)` casts, `substr`/`replace` string matching,
 * boolean-versus-integer CASE arms, timestamp binding, division by zero — and a store the harness
 * does not execute against is a store this adapter does not actually cover
 * (cerbos/query-plan-adapters#320).
 */

// Dedicated ports (gRPC 3621) so this suite can run alongside other adapters' sidecars.
const cerbos = new Cerbos("127.0.0.1:3621", { tls: false });

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
  /** The seed whose scalars this row's to-one `parent` carries; null for no parent. */
  parentSeedId: string | null;
}

interface SeedsFile {
  principal: Principal;
  resourceKind: string;
  seeds: Seed[];
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
  "parentSeedId",
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

// The corpus principal is guarded the same way and for the same reason. It feeds the PLAN under
// test AND the check() oracle, so an attribute dropped on the way in vanishes from both sides at
// once: the plan folds to ALWAYS_DENIED and the oracle, built from the same principal, agrees. That
// is how langchain-chromadb's hardcoded attribute allowlist let `pv-exists` pass while testing
// nothing (conformance/README.md, "Adding a new hostile shape", step 7). This harness passes the
// principal through verbatim, which is correct; the guard is what proves it still does.
//
// `id` and `roles` are deliberately IN scope, guarded by PRINCIPAL_KEYS one level above the
// attributes — the same two-level shape SEED_KEYS and TAG_KEYS use for a row and its `tags[]`
// elements. A role dropped on the way in changes every policy decision at once; that it is less
// likely to be projected away than an attribute is a reason to expect the assertion to stay quiet,
// not a reason to omit it.
const PRINCIPAL_KEYS = ["id", "roles", "attr"] as const;

const PRINCIPAL_ATTR_KEYS = [
  "allowedTags",
  "context",
  "fewTeams",
  "manyTeams",
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
  optional: readonly string[] = [],
): void {
  const allowed = new Set<string>([...want, ...optional]);
  for (const key of got) {
    if (!allowed.has(key)) {
      throw new Error(
        `${label} carries "${key}", which this harness does not consume: an unconsumed corpus field is dropped from the stored row and the check() oracle at once`,
      );
    }
  }
  const present = new Set(got);
  for (const key of want) {
    if (!present.has(key)) {
      throw new Error(
        `${label} is missing "${key}", which this harness consumes`,
      );
    }
  }
}

/**
 * One principal attribute, checked against the two JSON shapes the corpus carries. A key-set guard
 * says nothing about a change inside a value and three of the four attributes are lists, so the
 * element type is asserted for the same reason the seed guard descends into `tags[]`.
 */
function assertPrincipalAttrShape(label: string, value: unknown): void {
  if (typeof value === "string") return;
  if (Array.isArray(value) && value.every((el) => typeof el === "string")) {
    return;
  }
  throw new Error(
    `${label} is neither a string nor an array of strings, the only two shapes this harness consumes: a reshaped principal attribute feeds the plan and the check() oracle at once`,
  );
}

const seedsFile: SeedsFile = JSON.parse(
  fs.readFileSync(path.join(CONFORMANCE_DIR, "seeds.json"), "utf8"),
);
const actionsFile: ActionsFile = JSON.parse(
  fs.readFileSync(path.join(CONFORMANCE_DIR, "actions.json"), "utf8"),
);
const derivedFile: DerivedFile = JSON.parse(
  fs.readFileSync(path.join(CONFORMANCE_DIR, "derived-fields.json"), "utf8"),
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

// seedsFile.principal is the parsed JSON object, handed to the SDK untouched, so Object.keys
// reports the corpus key set on both levels.
assertKeys(
  "seeds.json principal",
  Object.keys(seedsFile.principal),
  PRINCIPAL_KEYS,
);
// `attr` is optional on the SDK's Principal type; the corpus always carries it, and the assertion
// above is what proves it rather than this fallback.
const PRINCIPAL_ATTR = seedsFile.principal.attr ?? {};
assertKeys(
  "seeds.json principal.attr",
  Object.keys(PRINCIPAL_ATTR),
  PRINCIPAL_ATTR_KEYS,
);
for (const [key, value] of Object.entries(PRINCIPAL_ATTR)) {
  assertPrincipalAttrShape(`seeds.json principal.attr.${key}`, value);
}

assertKeys("derived-fields.json fields", derivedFile.fields, DERIVED_KEYS);
const DERIVED_IDS = Object.keys(derivedFile.derived);
if (DERIVED_IDS.length !== SEEDS.length) {
  throw new Error(
    `derived-fields.json has ${DERIVED_IDS.length} entries for ${SEEDS.length} seeds`,
  );
}
for (const seed of SEEDS) {
  assertKeys(
    `derived-fields.json derived["${seed.id}"]`,
    Object.keys(derivedFor(seed)),
    DERIVED_KEYS,
  );
}

// Reference actions this adapter cannot express without changing CEL semantics. The shared
// manifest is the source of truth so the package-local harness and README stay aligned, and the
// classification itself is read through `./corpus`, the same reader the translator unit test
// uses — the two suites must agree on which actions this adapter refuses, and with what message.
const {
  oracleActions: ORACLE_ACTIONS,
  throwingActions: THROWING_ACTIONS,
  supportedExpected: DRIZZLE_SUPPORTED_EXPECTED,
} = classifyActionsForAdapter(actionsFile, ADAPTER);

const DRIZZLE_DIVERGENCES = new Set(
  (actionsFile.knownDivergences ?? [])
    .filter((entry) => entry.adapters.includes(ADAPTER))
    .map((entry) => entry.action),
);

const EXPECTED_UNSUPPORTED_ACTIONS = new Set(
  actionsFile.expectedUnsupported.map((entry) => entry.action),
);

// Actions whose `== null` probe targets an attribute the oracle OMITS for NULL columns. They
// carry no oracle comparison: under the omitted representation check() denies every row, so the
// adapter must reject the shape rather than emit a filter (#302).
const NULL_REPRESENTATION_OMITTED = actionsFile.nullRepresentationOmitted.map(
  (entry): ThrowingAction => [
    entry.action,
    entry.reason,
    requireMessage(
      `nullRepresentationOmitted.${entry.action}.messages.drizzle`,
      entry.messages?.["drizzle"],
    ),
  ],
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
// w1-size-zero-chain, w1-not-size-chain and w1-size-frac-chain are deliberately absent: their
// oracles are empty by CONSTRUCTION (no seed holds a to-one parent with zero children, nor one
// with two or more), so they cannot satisfy a non-empty assertion. Their siblings below carry it
// for that group.

const DEGENERACY_GUARD_ACTIONS = [
  "vf-le",
  "like-percent",
  "all-on-empty",
  "pv-exists",
  "pv-all",
  "null-eq",
  "null-ne",
  // The explicit-null convention against a non-null operand (#308). All five are compared
  // rather than thrown, because the mapper declares the convention per attribute; every one of
  // them under-granted by exactly the NULL-column rows before that declaration existed.
  "null-value-ne-const",
  "null-value-not-eq-const",
  "null-value-not-in-const",
  "null-value-f2f",
  "null-value-pv-not-exists",
  // The absent to-one parent (#309/#315/#316/#333/#334): the seven discriminating chain shapes
  // with a non-empty oracle.
  "w1-all-chain",
  "w1-not-exists-chain",
  "w1-size-nonneg-chain",
  "w1-not-in-chain",
  "w1-not-hasint-chain",
  "w1-ternary-chain-cond",
  "w1-size-frac-le-chain",
  // Column arithmetic under a division (#311).
  "cr-div-neg-zero",
  "cr-div-other-column",
  "cr-div-then-add",
  "cr-div-then-add-ne",
  // The real to-one join (#375): one per hazard — the negated hop, the null comparison, two-level
  // depth, the root conjunction, and the disjunction, whose failure direction is an under-grant.
  "rel-not-bool-hop",
  "rel-ne-null-hop",
  "rel-bool-hop2",
  "rel-hop-and-root",
  "rel-hop2-or-exists",
  // Case sensitivity in STRING MATCHING (#375 follow-up), a different mechanism from cs-eq:
  // collation governs `=`, and on SQLite nothing but `PRAGMA case_sensitive_like` governs LIKE.
  "cs-contains",
  // The primary key as a filterable attribute (#376): the key against a constant and against a
  // column under negation. Both stores must agree, so these also cover the id column's typing.
  "id-eq-const",
  "id-f2f-ne",
  // string() over a NUMERIC column, the half this adapter lowers. Its boolean sibling is refused
  // instead, because CAST(... AS TEXT) is store-dependent there — the two are deliberately
  // classified apart, so this entry proves the supported half still compares.
  "cast-string-double",
  // Root position and bare operand forms (#388): one per hazard — the negation over a bare
  // ordering (every other negated ordering in the corpus wraps a size() or a ternary), the bare
  // boolean at the ROOT of the condition, and the collection subquery disjoined with a scalar
  // predicate rather than conjoined with one.
  "not-lt",
  "root-bare-bool",
  "or-eq-exists",
  // Hazard classes the corpus missed (#387): the De Morgan branch over a conjunction; the negated
  // LIKE against a COLUMN needle, where a definite-FALSE null guard would leak every NULL-needle
  // row through the NOT; the value-first hasIntersection, which used to translate to a bare FALSE
  // here because the operands were read positionally; and the BELOW-cliff unroll of a principal
  // collection, the shape a principal with three teams produces.
  "not-and",
  "not-contains",
  "vf-hasint",
  "pv-exists-unrolled",
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

// -- the real to-one relation (conformance/README.md, "The real to-one relation") ----------------
//
// `parentSeedId` names the seed whose four scalars this row's `parent` carries, and that seed's own
// `parentSeedId` names the ones `parent.inner` carries. The chain is cut at two levels. Every
// resource owns a FRESH parent (and inner) row rather than pointing at the named seed's own row, so
// no two resources share one and a filter that returned the parent instead of the child cannot
// agree with the oracle by accident.

const SEEDS_BY_ID = new Map(SEEDS.map((seed) => [seed.id, seed]));

function parentSeedOf(seed: Seed | undefined): Seed | undefined {
  const id = seed?.parentSeedId;
  if (id === undefined || id === null) {
    return undefined;
  }
  const parent = SEEDS_BY_ID.get(id);
  if (parent === undefined) {
    throw new Error(
      `seeds.json: "${seed?.id}" names parent "${id}", which is not a seed id`,
    );
  }
  return parent;
}

/** The same four scalars as check() attributes: a NULL column is a MISSING attribute, one hop out. */
function relationAttr(seed: Seed): Record<string, Value> {
  const attr: Record<string, Value> = {
    aBool: seed.aBool,
    aString: seed.aString,
    aNumber: seed.aNumber,
  };
  if (seed.aOptionalString !== null) {
    attr["aOptionalString"] = seed.aOptionalString;
  }
  return attr;
}

// -- the seeded rows, derived once and shared by every store ------------------------------------
//
// Only the INSERT calls differ per store. Deriving the rows here rather than inside each store
// keeps a second store from quietly seeding a different graph than the one the check() oracle
// mirrors, which would make its differential agree for the wrong reason.

interface ResourceRow {
  id: string;
  aBool: boolean;
  aString: string;
  aNumber: number;
  aDouble: number | null;
  aOptionalString: string | null;
  createdBy: string;
  scope: string | null;
  createdAt: string | null;
}

interface TagRow {
  tagId: string;
  name: string | null;
  resourceId: string;
}

interface CategoryRow {
  id: string;
  name: string;
  resourceId: string;
}

interface SubCategoryRow {
  id: string;
  name: string;
  categoryId: string;
}

interface LabelRow {
  id: string;
  name: string | null;
  subCategoryId: string;
}

/** One level of the to-one chain. `resourceId`/`parentId` is unique: this is a to-ONE relation. */
interface ParentRow {
  id: string;
  aBool: boolean;
  aString: string;
  aNumber: number;
  aOptionalString: string | null;
  resourceId: string;
}

interface InnerRow {
  id: string;
  aBool: boolean;
  aString: string;
  aNumber: number;
  aOptionalString: string | null;
  parentId: string;
}

interface SeedRows {
  resources: ResourceRow[];
  parents: ParentRow[];
  inners: InnerRow[];
  tags: TagRow[];
  categories: CategoryRow[];
  subCategories: SubCategoryRow[];
  labels: LabelRow[];
}

/** Distinct category/sub-category graphs per seed so no rows share relations by accident. */
function seedRows(): SeedRows {
  const rows: SeedRows = {
    resources: [],
    parents: [],
    inners: [],
    tags: [],
    categories: [],
    subCategories: [],
    labels: [],
  };

  for (const seed of SEEDS) {
    rows.resources.push({
      id: seed.id,
      aBool: seed.aBool,
      aString: seed.aString,
      aNumber: seed.aNumber,
      aDouble: doubleFor(seed),
      aOptionalString: seed.aOptionalString,
      createdBy: isoFor(seed),
      scope: scopeFor(seed),
      createdAt: timestampFor(seed),
    });
    const parentSeed = parentSeedOf(seed);
    if (parentSeed !== undefined) {
      const parentId = `${seed.id}-parent`;
      rows.parents.push({
        id: parentId,
        aBool: parentSeed.aBool,
        aString: parentSeed.aString,
        aNumber: parentSeed.aNumber,
        aOptionalString: parentSeed.aOptionalString,
        resourceId: seed.id,
      });
      const innerSeed = parentSeedOf(parentSeed);
      if (innerSeed !== undefined) {
        rows.inners.push({
          id: `${parentId}-inner`,
          aBool: innerSeed.aBool,
          aString: innerSeed.aString,
          aNumber: innerSeed.aNumber,
          aOptionalString: innerSeed.aOptionalString,
          parentId,
        });
      }
    }
    for (const tag of seed.tags) {
      rows.tags.push({ tagId: tag.id, name: tag.name, resourceId: seed.id });
    }
    seed.subCategoryNames.forEach((subName, index) => {
      const categoryId = `${seed.id}-cat-${index}`;
      const subCategoryId = `${categoryId}-sub`;
      rows.categories.push({
        id: categoryId,
        name: "business",
        resourceId: seed.id,
      });
      rows.subCategories.push({
        id: subCategoryId,
        name: subName,
        categoryId,
      });
      labelsFor(seed).forEach((labelName, labelIndex) => {
        rows.labels.push({
          id: `${categoryId}-label-${labelIndex}`,
          name: labelName,
          subCategoryId,
        });
      });
    });
  }

  // A store inserts each list in one statement, and drizzle rejects an empty VALUES list — but
  // the reason to assert it here is that an empty relation table would leave every collection
  // macro trivially satisfied on both sides of the differential.
  for (const [label, list] of Object.entries(rows)) {
    if (list.length === 0) {
      throw new Error(`seeds.json produced no ${label} rows`);
    }
  }

  return rows;
}

// The schema and the mapper live in `./corpus`, shared with the translator unit test: that
// suite pins the SQL this adapter emits for these mappings, and this one proves that same SQL
// returns the rows the PDP allows. Two copies could drift, leaving the pinned SQL describing a
// mapping nothing executes.

// -- store targets ------------------------------------------------------------------------------

const STORE_NAMES = ["sqlite", "postgres"] as const;
type StoreName = (typeof STORE_NAMES)[number];

/**
 * One database the whole corpus is replayed against. `start` creates the schema and seeds it;
 * `selectIds` runs the translated filter and returns the ids it selects, sorted.
 */
interface AdversarialStore {
  readonly name: StoreName;
  readonly mapper: Record<string, MapperEntry>;
  start(): Promise<void>;
  stop(): Promise<void>;
  selectIds(filter: SQL | undefined): Promise<string[]>;
  /**
   * The two hops of the to-one chain read back through a real join, per resource id: the
   * `aString` of `parent` and of `parent.inner`, or null where that level does not exist. The
   * relation carries no corpus action yet, so this is what keeps the fixture from rotting.
   */
  parentChain(): Promise<Record<string, [string | null, string | null]>>;
  /** The engine's own banner, asked of the connection the suite actually queries through. */
  serverBanner(): Promise<string>;
}

function sqliteStore(): AdversarialStore {
  const schema = sqliteSchema();
  const { resources, parents, inners, tags, categories, subCategories, labels } =
    schema;

  // Dedicated file (adversarial.db, gitignored) rather than :memory:, so a failing run leaves
  // the seeded rows behind to inspect.
  const dbPath = path.join(__dirname, "..", "adversarial.db");
  fs.rmSync(dbPath, { force: true });
  const sqlite = new Database(dbPath);
  const db = drizzleSqlite(sqlite);

  return {
    name: "sqlite",
    mapper: buildMapper(schema),

    async start(): Promise<void> {
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
        CREATE TABLE adversarial_parents (
          id TEXT PRIMARY KEY,
          a_bool INTEGER NOT NULL,
          a_string TEXT NOT NULL,
          a_number INTEGER NOT NULL,
          a_optional_string TEXT,
          resource_id TEXT NOT NULL UNIQUE
        );
        CREATE TABLE adversarial_inners (
          id TEXT PRIMARY KEY,
          a_bool INTEGER NOT NULL,
          a_string TEXT NOT NULL,
          a_number INTEGER NOT NULL,
          a_optional_string TEXT,
          parent_id TEXT NOT NULL UNIQUE
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

      const rows = seedRows();
      db.insert(resources).values(rows.resources).run();
      db.insert(parents).values(rows.parents).run();
      db.insert(inners).values(rows.inners).run();
      db.insert(tags).values(rows.tags).run();
      db.insert(categories).values(rows.categories).run();
      db.insert(subCategories).values(rows.subCategories).run();
      db.insert(labels).values(rows.labels).run();
    },

    async stop(): Promise<void> {
      sqlite.close();
    },

    async selectIds(filter: SQL | undefined): Promise<string[]> {
      const selected = db
        .select({ id: resources.id })
        .from(resources)
        .where(filter)
        .all();
      return selected.map((row) => row.id).sort();
    },

    async parentChain(): Promise<
      Record<string, [string | null, string | null]>
    > {
      const rows = db
        .select({
          id: resources.id,
          parent: parents.aString,
          inner: inners.aString,
        })
        .from(resources)
        .leftJoin(parents, eq(parents.resourceId, resources.id))
        .leftJoin(inners, eq(inners.parentId, parents.id))
        .all();
      return Object.fromEntries(
        rows.map((row) => [row.id, [row.parent, row.inner]]),
      );
    },

    async serverBanner(): Promise<string> {
      const row = sqlite.prepare("select sqlite_version() as version").get();
      return `SQLite ${(row as { version: string }).version}`;
    },
  };
}

/**
 * Mirrors the ent harness's PostgreSQL target so both adapters prove the same server.
 *
 * Pinned by tag AND digest: a tag is mutable, so a tag-only pin records an intent rather than a
 * build, and this leg exists to prove typed-column behaviour a re-pushed image could change under
 * it. `conformance/scripts/validate-corpus.sh` asserts every service image reference in the
 * repository carries both halves.
 */
const POSTGRES_IMAGE =
  "postgres:17-alpine@sha256:742f40ea20b9ff2ff31db5458d127452988a2164df9e17441e191f3b72252193";

/**
 * The PostgreSQL leg (cerbos/query-plan-adapters#320).
 *
 * The column types are the point: `boolean` and `timestamptz` exercise the typed paths SQLite
 * cannot reach — on SQLite a boolean is an integer and a timestamp is text compared
 * lexicographically, so a CASE arm yielding `1` instead of `true`, or a timestamp bound in a
 * layout only string comparison tolerates, passes there and fails here. PostgreSQL also raises on
 * division by zero where SQLite returns NULL, which is what proves the adapter's IEEE CASE arms
 * guard the division rather than merely reshaping its NULL.
 *
 * The default collation the image initialises with is left alone: PostgreSQL collations are
 * deterministic, so `=` stays byte-exact and matches CEL string equality. (MySQL's default is
 * case-insensitive, which is why the ent harness has to pin a binary collation there.)
 */
function postgresStore(): AdversarialStore {
  const schema = postgresSchema();
  const { resources, parents, inners, tags, categories, subCategories, labels } =
    schema;

  let container: StartedPostgreSqlContainer | undefined;
  let pool: Pool | undefined;
  let db: ReturnType<typeof drizzlePostgres> | undefined;

  const connected = (): NonNullable<typeof db> => {
    if (!db) {
      throw new Error("PostgreSQL store used before start()");
    }
    return db;
  };

  return {
    name: "postgres",
    mapper: buildMapper(schema),

    async start(): Promise<void> {
      container = await new PostgreSqlContainer(POSTGRES_IMAGE).start();
      pool = new Pool({ connectionString: container.getConnectionUri() });
      db = drizzlePostgres(pool);

      await db.execute(sql`
        CREATE TABLE adversarial_resources (
          id                 text PRIMARY KEY,
          a_bool             boolean NOT NULL,
          a_string           text NOT NULL,
          a_number           integer NOT NULL,
          a_double           double precision,
          a_optional_string  text,
          created_by         text NOT NULL,
          scope              text,
          created_at         timestamptz
        );
        CREATE TABLE adversarial_parents (
          id                 text PRIMARY KEY,
          a_bool             boolean NOT NULL,
          a_string           text NOT NULL,
          a_number           integer NOT NULL,
          a_optional_string  text,
          resource_id        text NOT NULL UNIQUE REFERENCES adversarial_resources(id)
        );
        CREATE TABLE adversarial_inners (
          id                 text PRIMARY KEY,
          a_bool             boolean NOT NULL,
          a_string           text NOT NULL,
          a_number           integer NOT NULL,
          a_optional_string  text,
          parent_id          text NOT NULL UNIQUE REFERENCES adversarial_parents(id)
        );
        CREATE TABLE adversarial_tags (
          tag_id       text PRIMARY KEY,
          name         text,
          resource_id  text NOT NULL REFERENCES adversarial_resources(id)
        );
        CREATE TABLE adversarial_categories (
          id           text PRIMARY KEY,
          name         text NOT NULL,
          resource_id  text NOT NULL REFERENCES adversarial_resources(id)
        );
        CREATE TABLE adversarial_sub_categories (
          id           text PRIMARY KEY,
          name         text NOT NULL,
          category_id  text NOT NULL REFERENCES adversarial_categories(id)
        );
        CREATE TABLE adversarial_labels (
          id               text PRIMARY KEY,
          name             text,
          sub_category_id  text NOT NULL REFERENCES adversarial_sub_categories(id)
        );
      `);

      const rows = seedRows();
      await db.insert(resources).values(rows.resources);
      await db.insert(parents).values(rows.parents);
      await db.insert(inners).values(rows.inners);
      await db.insert(tags).values(rows.tags);
      await db.insert(categories).values(rows.categories);
      await db.insert(subCategories).values(rows.subCategories);
      await db.insert(labels).values(rows.labels);
    },

    async stop(): Promise<void> {
      await pool?.end();
      await container?.stop();
    },

    async selectIds(filter: SQL | undefined): Promise<string[]> {
      const selected = await connected()
        .select({ id: resources.id })
        .from(resources)
        .where(filter);
      return selected.map((row) => row.id).sort();
    },

    async parentChain(): Promise<
      Record<string, [string | null, string | null]>
    > {
      const rows = await connected()
        .select({
          id: resources.id,
          parent: parents.aString,
          inner: inners.aString,
        })
        .from(resources)
        .leftJoin(parents, eq(parents.resourceId, resources.id))
        .leftJoin(inners, eq(inners.parentId, parents.id));
      return Object.fromEntries(
        rows.map((row) => [row.id, [row.parent, row.inner]]),
      );
    },

    async serverBanner(): Promise<string> {
      const result = await connected().execute<{ version: string }>(
        sql`select version() as version`,
      );
      return result.rows[0]?.version ?? "";
    },
  };
}

/** Container start dominates the PostgreSQL leg's setup; the SQLite leg finishes instantly. */
const STORE_STARTUP_TIMEOUT_MS = 180_000;

function selectedStoreName(): StoreName {
  const requested = process.env["ADAPTER_TEST_DB"] ?? "sqlite";
  // A typo must fail rather than silently fall back to SQLite: a CI leg that believes it is
  // proving PostgreSQL while replaying SQLite is exactly the coverage gap this leg closes.
  if (!STORE_NAMES.includes(requested as StoreName)) {
    throw new Error(
      `Unknown ADAPTER_TEST_DB "${requested}": expected one of ${STORE_NAMES.join(", ")}`,
    );
  }
  return requested as StoreName;
}

const STORE_NAME = selectedStoreName();
const store: AdversarialStore =
  STORE_NAME === "postgres" ? postgresStore() : sqliteStore();
const MAPPER = store.mapper;

/**
 * The same mapper with every per-attribute null convention stripped, so the call-level option is
 * the only thing governing null operands.
 *
 * The #302 completeness guard is a statement about that option: every corpus action carrying a
 * null literal must be rejected under `"omitted"`. Declaring `owner`/`coOwner` as explicit-null
 * (#308) deliberately overrides the option for those two attributes — which would otherwise read
 * as the guard going quiet, when in fact it is the per-attribute declaration doing exactly its
 * job. Stripping the declarations keeps the guard testing what it was written to test.
 */
const MAPPER_WITHOUT_NULL_CONVENTIONS: Record<string, MapperEntry> =
  Object.fromEntries(
    Object.entries(MAPPER).map(([reference, entry]) => {
      if (
        typeof entry !== "object" ||
        entry === null ||
        !("nullAttributeRepresentation" in entry)
      ) {
        return [reference, entry];
      }
      const { nullAttributeRepresentation: _stripped, ...rest } = entry;
      return [reference, rest as MapperEntry];
    }),
  );

beforeAll(async () => {
  await store.start();
}, STORE_STARTUP_TIMEOUT_MS);

afterAll(async () => {
  cerbos.close();
  await store.stop();
}, STORE_STARTUP_TIMEOUT_MS);

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
    // The explicit-null alias of the `scope` column, the second half of `null-value-f2f`:
    // `scope` itself is omitted when NULL (below), so the corpus carries the same column under
    // both conventions and the field-to-field probe has two explicit nulls to compare.
    coOwner: scopeFor(seed),
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
  // The real to-one chain, mirroring the seeded rows exactly. A row with no parent sends NO
  // `parent` attribute — a CEL missing-path error (deny) — matching the adapter's join finding
  // nothing; the same holds one level down for `parent.inner`.
  const parentSeed = parentSeedOf(seed);
  if (parentSeed !== undefined) {
    const parentAttr = relationAttr(parentSeed);
    const innerSeed = parentSeedOf(parentSeed);
    if (innerSeed !== undefined) {
      parentAttr["inner"] = relationAttr(innerSeed);
    }
    attr["parent"] = parentAttr;
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
  nullAttributeRepresentation: "explicit" | "omitted" = "explicit",
  mapper: Record<string, MapperEntry> = MAPPER,
): Promise<string[]> {
  const queryPlan = await cerbos.planResources({
    principal: principal(),
    resource: { kind: seedsFile.resourceKind },
    action,
  });
  const result = queryPlanToDrizzle({
    queryPlan,
    mapper,
    nullAttributeRepresentation,
  });
  if (result.kind === PlanKind.ALWAYS_DENIED) {
    return [];
  }
  return store.selectIds(
    result.kind === PlanKind.CONDITIONAL ? result.filter : undefined,
  );
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

describe(`adversarial conformance corpus (${STORE_NAME})`, () => {
  // Anti-vacuity for the store split: every other assertion in this file is identical on both
  // legs, so a PostgreSQL leg that silently fell back to SQLite would pass the entire suite while
  // proving nothing about PostgreSQL — the exact gap #320 reports. Ask the connection the suite
  // actually queries through which engine it is.
  test("executes against the store ADAPTER_TEST_DB selects", async () => {
    const banner = await store.serverBanner();
    expect({ store: store.name, engine: banner.split(" ")[0] }).toEqual({
      store: STORE_NAME,
      engine: STORE_NAME === "postgres" ? "PostgreSQL" : "SQLite",
    });
  });

  // Adding a throwing action without pinning its message must fail this harness rather than
  // silently degrade the throw suite to a bare "it threw" (cerbos/query-plan-adapters#326).
  test("a throwing action with no pinned message fails classification", () => {
    expect(() => requireMessage("synthetic-entry", undefined)).toThrow(
      /pins no throw message/,
    );
    expect(() => requireMessage("synthetic-entry", "")).toThrow(
      /pins no throw message/,
    );
  });
  test("manifest assigns every action exactly one Drizzle outcome", () => {
    const oracle = new Set(ORACLE_ACTIONS);
    const throwing = new Set(THROWING_ACTIONS.map(([action]) => action));
    const nullOmitted = new Set(
      NULL_REPRESENTATION_OMITTED.map(([action]) => action),
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

    expect(MANIFEST_ACTIONS.size).toBe(199);
    expect(NULL_REPRESENTATION_OMITTED).toHaveLength(1);
    // Deliberate tripwire: every one of these carries a pinned message, so a throwing action
    // gained or lost has to be re-triaged here rather than joining the suite unnoticed.
    expect(THROWING_ACTIONS).toHaveLength(20);
    expect(misclassified).toEqual([]);
    expect(
      [...DRIZZLE_SUPPORTED_EXPECTED].filter(
        (action) => !EXPECTED_UNSUPPORTED_ACTIONS.has(action),
      ),
    ).toEqual([]);
  });

  test.each(ORACLE_ACTIONS)("%s matches the check() oracle", async (action) => {
    const [oracle, filtered] = await Promise.all([
      oracleAllowedIds(action),
      adapterFilteredIds(action),
    ]);
    expect(filtered).toEqual(oracle);
  });

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
        }),
      ).toThrow(message);
    },
  );

  // #387. `filter-as-conjunct` puts a filter() one level below the root, where the guard that
  // refuses `filter-as-condition` does not look. Its oracle is empty BY CONSTRUCTION — check()
  // cannot evaluate a non-boolean conjunction — so it belongs to neither degeneracy-guard list,
  // and a bare "it throws" would say nothing about whether refusing it is REQUIRED.
  //
  // This is that argument. The other conjunct is `R.attr.aBool`, which the adapter certainly can
  // express and which `root-bare-bool` spells on its own; an adapter that dropped the conjunct it
  // could not translate would emit exactly that filter and return every row it selects, all of
  // which the PDP denies for this action.
  test("filter-as-conjunct must be refused: dropping its untranslatable half over-grants", async () => {
    expect(await oracleAllowedIds("filter-as-conjunct")).toEqual([]);

    const survivingHalf = await adapterFilteredIds("root-bare-bool");
    expect(survivingHalf.length).toBeGreaterThan(0);
    expect(survivingHalf.length).toBeLessThan(SEEDS.length);

    const message = THROWING_ACTIONS.find(
      ([action]) => action === "filter-as-conjunct",
    )?.[2];
    expect(message).toBeDefined();
    await expect(adapterFilteredIds("filter-as-conjunct")).rejects.toThrow(
      message,
    );
  });

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
        message,
      );
    },
  );

  // #308. The per-attribute declaration overrides the call-level option, which is the property
  // that makes a suite mixing both conventions expressible at all. Asserted in both directions
  // against the SAME action and the SAME call-level option, varying only whether the mapper
  // declares the convention — so a declaration that did nothing would show up here as the two
  // runs agreeing.
  test("a per-attribute declaration overrides the call-level representation", async () => {
    // `owner` declares "explicit", so the call-level "omitted" does not reach it.
    await expect(adapterFilteredIds("null-eq", "omitted")).resolves.toEqual(
      await oracleAllowedIds("null-eq"),
    );

    // Strip the declaration and the same action under the same option is rejected — so the
    // stripped mapper the completeness guard below uses is not quietly equivalent to MAPPER.
    await expect(
      adapterFilteredIds("null-eq", "omitted", MAPPER_WITHOUT_NULL_CONVENTIONS),
    ).rejects.toThrow(NULL_OMITTED_MESSAGE);
  });

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
        await adapterFilteredIds(
          action,
          "omitted",
          MAPPER_WITHOUT_NULL_CONVENTIONS,
        );
        notRejected.push(action);
      } catch (error) {
        // The rejection must be the null-operand check talking, not an incidental failure — a
        // transport error or mapper typo counting as the required rejection is the silent pass
        // the corpus README warns about.
        if (!String(error).includes(NULL_OMITTED_MESSAGE)) {
          notRejected.push(
            `${action} (rejected for the wrong reason: ${String(error)})`,
          );
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
      "request.resource.attr.mainCategory.subCategories",
    );
    const size = new PlanExpression("size", [chain]);
    const compare = (operator: string, threshold: number) =>
      new PlanExpression(operator, [size, new PlanExpressionValue(threshold)]);
    const negate = (condition: PlanExpressionOperand) =>
      new PlanExpression("not", [condition]);

    const filteredIdsFor = async (
      condition: PlanExpressionOperand,
    ): Promise<string[]> => {
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
      return store.selectIds(
        result.kind === PlanKind.CONDITIONAL ? result.filter : undefined,
      );
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
      expect([shape, await filteredIdsFor(condition)]).toEqual([shape, []]);
    }

    // The mirror image, so the loop above cannot pass by denying everything: `>= 0` and `< 2`
    // are TRUE for exactly the rows that HAVE the parent.
    const withParent = await oracleAllowedIds("w1-size-nonneg-chain");
    expect(withParent.length).toBeGreaterThan(0);
    expect(withParent.length).toBeLessThan(SEEDS.length);
    expect(await filteredIdsFor(compare("ge", 0))).toEqual(withParent);
    expect(await filteredIdsFor(compare("lt", 2))).toEqual(withParent);
  });

  // The to-one relation carries no corpus action yet — this is the expand half of
  // cerbos/query-plan-adapters#372's expand–contract — so nothing else in this file would notice a
  // seeder that stored no chain at all, or one that attached every parent to the wrong resource.
  // Read the two hops back through a real join rather than counting rows: a count cannot tell an
  // inner row carrying the corpus's values from one carrying the root's own columns, which is
  // exactly the flat-column-alias failure this relation exists to make visible.
  test("the seeded to-one chain matches the corpus relation", async () => {
    const withParent = SEEDS.filter((seed) => parentSeedOf(seed) !== undefined);
    const withInner = SEEDS.filter(
      (seed) => parentSeedOf(parentSeedOf(seed)) !== undefined,
    );
    expect(withParent.length).toBeGreaterThan(0);
    expect(withInner.length).toBeGreaterThan(0);
    expect(withParent.length).toBeLessThan(SEEDS.length);

    expect(await store.parentChain()).toEqual(
      Object.fromEntries(
        SEEDS.map((seed) => [
          seed.id,
          [
            parentSeedOf(seed)?.aString ?? null,
            parentSeedOf(parentSeedOf(seed))?.aString ?? null,
          ],
        ]),
      ),
    );
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
