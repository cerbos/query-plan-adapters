import * as fs from "fs";
import * as path from "path";

import { afterAll, beforeAll, describe, expect, test } from "@jest/globals";
import { GRPC as Cerbos } from "@cerbos/grpc";
import {
  PlanExpression,
  PlanExpressionValue,
  PlanExpressionVariable,
} from "@cerbos/core";
import type { PlanExpressionOperand, Principal } from "@cerbos/core";
import Database from "better-sqlite3";
import { eq, sql } from "drizzle-orm";
import type { SQL } from "drizzle-orm";
import { drizzle as drizzleSqlite } from "drizzle-orm/better-sqlite3";
import { drizzle as drizzleMysql } from "drizzle-orm/mysql2";
import type { MySql2Database } from "drizzle-orm/mysql2";
import { drizzle as drizzlePostgres } from "drizzle-orm/node-postgres";
import { MySqlContainer } from "@testcontainers/mysql";
import type { StartedMySqlContainer } from "@testcontainers/mysql";
import { PostgreSqlContainer } from "@testcontainers/postgresql";
import type { StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import mysql from "mysql2/promise";
import { Pool } from "pg";

import { queryPlanToDrizzle, PlanKind } from ".";
import type { MapperEntry } from ".";
import {
  ADAPTER,
  CONFORMANCE_DIR,
  buildMapper,
  mysqlSchema,
  postgresSchema,
  sqliteSchema,
} from "./corpus";
import {
  loadActionControlPlane,
  loadCheckResources,
  requireOutcomeMessage,
} from "./controlPlane";

/**
 * Adversarial differential suite: every action in the shared `../conformance/` corpus is planned
 * against a REAL Cerbos PDP (the sidecar started by `npm run test:adversarial`, loaded with
 * `conformance/policies/adversarial.yaml`), translated by this adapter, and executed against
 * seeded rows — then the filtered id set is compared against an oracle computed by calling the
 * check API for each row with attributes mirroring that row exactly.
 *
 * No hand-computed expectations: if this adapter's filter semantics diverge from Cerbos's own
 * evaluation for any row, the mismatch surfaces mechanically. See `conformance/README.md` for the
 * oracle recipe (NULL-as-missing-attribute and catalog cardinality expectations) — this file only owns the
 * Drizzle-specific translation (schema, seeding, field mapping, executing the query).
 *
 * The whole corpus is replayed against every store this adapter claims to support, one store per
 * run, selected with `ADAPTER_TEST_DB` (`sqlite` by default, `postgres` and `mysql` for the
 * container-backed legs). Drizzle's dialect objects own quoting and placeholders, but the adapter
 * still makes dialect-sensitive choices of its own — `float(53)` casts, `substr`/`replace` string
 * matching, boolean-versus-integer CASE arms, timestamp binding, division by zero — and a store
 * the harness does not execute against is a store this adapter does not actually cover
 * (cerbos/query-plan-adapters#320 for PostgreSQL, #340 for MySQL).
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
// reason — the projection trap conformance/README.md describes for adapterctl.json, applied to the
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

// `./controlPlane` validates catalog expectations and adapter-local direct outcomes for both this
// harness and the translator unit test, including each refusal's pinned message substring.
const ACTION_CONTROL_PLANE = loadActionControlPlane({
  adapter: ADAPTER,
  selectedAction: process.env["ADAPTERCTL_ACTION"],
});
const FULL_MATRIX = process.env["ADAPTERCTL_ACTION"] === undefined;
const fullMatrixTest = FULL_MATRIX ? test : test.skip;
const CHECK_RESOURCES = loadCheckResources();
const ORACLE_ACTIONS = ACTION_CONTROL_PLANE.oracleActions;

// Actions whose `== null` probe targets an attribute the oracle OMITS for NULL columns. They
// carry no oracle comparison: under the omitted representation check() denies every row, so the
// adapter must reject the shape rather than emit a filter (#302).
const NULL_REPRESENTATION_OMITTED = ACTION_CONTROL_PLANE.throwingActions.filter(
  ({ action }) => action === "null-eq-missing",
);
const THROWING_ACTIONS = ACTION_CONTROL_PLANE.throwingActions.filter(
  ({ action }) => action !== "null-eq-missing",
);
/** The one message every null-carrying action must be rejected with under `omitted`. */
const NULL_OMITTED_MESSAGE = requireOutcomeMessage({
  controlPlane: ACTION_CONTROL_PLANE,
  action: "null-eq-missing",
});
const MANIFEST_ACTIONS = new Set(ACTION_CONTROL_PLANE.selectedActions);

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

const STORE_NAMES = ["sqlite", "postgres", "mysql"] as const;
type StoreName = (typeof STORE_NAMES)[number];

/**
 * The engine each leg must find itself talking to, in the spelling that engine's own banner
 * query produces. Each spelling is rejected by the other two engines, so the anti-vacuity test
 * below fails in both directions rather than only when a container is missing.
 */
const STORE_ENGINES: Record<StoreName, string> = {
  sqlite: "SQLite",
  postgres: "PostgreSQL",
  mysql: "MySQL",
};

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
  const {
    resources,
    parents,
    inners,
    tags,
    categories,
    subCategories,
    labels,
  } = schema;

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
  const {
    resources,
    parents,
    inners,
    tags,
    categories,
    subCategories,
    labels,
  } = schema;

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

/**
 * Mirrors the ent and spring-data harnesses' MySQL target so all three adapters prove the same
 * server, pinned by tag AND digest for the same reason the PostgreSQL image is.
 */
const MYSQL_IMAGE =
  "mysql:8.4@sha256:b3b90af2a6552ae30c266fdb7d5dd55f3afb72404bb78d37fe8a23eb857fd3fb";

/**
 * The collation this leg runs the whole corpus under, set on the SERVER so that the database the
 * container creates, and every table in it, inherits it.
 *
 * **This is a correctness requirement, not a preference.** CEL string equality is byte-exact.
 * MySQL's default `utf8mb4_0900_ai_ci` is case- AND accent-insensitive, and its `LIKE` follows the
 * column's collation, so under the default the following are all TRUE on MySQL 8.4 — measured,
 * not inferred:
 *
 * | probe | `utf8mb4_0900_ai_ci` (default) | `utf8mb4_0900_as_cs` |
 * |---|---|---|
 * | `'One' = 'one'` | TRUE — over-grants `cs-eq` | FALSE |
 * | `'héllo' = 'hello'` | TRUE — over-grants `unicode-eq` | FALSE |
 * | `'one' LIKE 'ON%'` | TRUE — over-grants every `hier-*` prefix probe | FALSE |
 *
 * A collation that makes `=` case-insensitive is a **store misconfiguration**, not a limitation of
 * this adapter: no filter it could emit would restore byte-exact equality, and classifying `cs-eq`
 * as a `rejected` direct outcome on that basis would blame the translator for the DDL. So the leg pins a
 * case- and accent-sensitive collation and states the requirement, exactly as `ent` pins
 * `COLLATE utf8mb4_bin` per column and `spring-data` passes `--collation-server`.
 *
 * `utf8mb4_0900_as_cs` rather than ent's `utf8mb4_bin` because the two differ on a third axis:
 * `utf8mb4_bin` is PAD SPACE, so `'a' = 'a '` is TRUE under it, while both `utf8mb4_0900_as_cs`
 * and the default are NO PAD. No corpus seed carries a trailing space today, so nothing here
 * discriminates them — this picks the one that matches CEL on all three axes rather than two, and
 * it is the collation `spring-data`'s MySQL leg already runs, so the two adapters prove one server
 * configuration between them.
 *
 * Overridable so the over-grant can be reproduced rather than taken on trust —
 * `ADAPTER_TEST_MYSQL_COLLATION=utf8mb4_0900_ai_ci npm run test:adversarial:mysql` fails on the
 * case and accent probes and nothing else. Same escape hatch as spring-data's
 * `-Dadapter.test.mysql.collation`.
 */
const MYSQL_COLLATION =
  process.env["ADAPTER_TEST_MYSQL_COLLATION"] ?? "utf8mb4_0900_as_cs";

/**
 * The MySQL leg (cerbos/query-plan-adapters#340).
 *
 * What it discriminates that neither other store can:
 *
 * - **Collation.** See `MYSQL_COLLATION` above. SQLite has `PRAGMA case_sensitive_like` and
 *   nothing else; PostgreSQL's collations are deterministic; only MySQL ships a default under
 *   which `=` itself over-grants.
 * - **Division.** MySQL returns NULL for `x / 0` where PostgreSQL raises, and `5 / 2` is `2.5`
 *   where PostgreSQL's integer division truncates to `2`. The adapter's guarded CASE arms and its
 *   `float(53)` cast on the numerator are what make the two agree; a translation that leaned on
 *   either engine's behaviour shows up here as a divergence rather than as a passing leg.
 * - **`CAST(… AS FLOAT(53))`.** Supported only from MySQL 8.0.17. Every other rendering the
 *   adapter emits is portable by construction; this one is the single version-gated construct in
 *   it, and nothing but executing it says whether the server accepts it.
 * - **`CAST(… AS TEXT)`.** Which is not a MySQL cast target at all — the divergence this leg
 *   actually found, and the reason `string()` is now refused (`UNSUPPORTED_CONVERSIONS` in
 *   `index.ts`). Both other stores accept it.
 *
 * The DDL is written here rather than derived from the drizzle schema because a store owns its own
 * schema in this harness — but it deliberately names NO collation per column, unlike `ent`'s. The
 * server is started with the collation as its default and every table inherits it, so the
 * requirement lives in one constant rather than repeated on nine columns.
 */
function mysqlStore(): AdversarialStore {
  const schema = mysqlSchema();
  const {
    resources,
    parents,
    inners,
    tags,
    categories,
    subCategories,
    labels,
  } = schema;

  let container: StartedMySqlContainer | undefined;
  let pool: mysql.Pool | undefined;
  // The promise-flavoured `mysql2/promise` pool, which is what `await`ing a query needs; the
  // driver's default generic names the callback-flavoured one, so the type is spelled out here.
  let db: MySql2Database | undefined;

  const connected = (): NonNullable<typeof db> => {
    if (!db) {
      throw new Error("MySQL store used before start()");
    }
    return db;
  };

  // MySQL rejects several statements in one `execute()`, so the schema is a list rather than a
  // script. `int` and `datetime(6)` mirror the drizzle column types in `mysqlSchema()`, which is
  // where the reasoning for each choice lives.
  const DDL = [
    `CREATE TABLE adversarial_resources (
       id                 varchar(64) PRIMARY KEY,
       a_bool             boolean NOT NULL,
       a_string           varchar(255) NOT NULL,
       a_number           int NOT NULL,
       a_double           double,
       a_optional_string  varchar(255),
       created_by         varchar(64) NOT NULL,
       scope              varchar(255),
       created_at         datetime(6)
     )`,
    `CREATE TABLE adversarial_parents (
       id                 varchar(64) PRIMARY KEY,
       a_bool             boolean NOT NULL,
       a_string           varchar(255) NOT NULL,
       a_number           int NOT NULL,
       a_optional_string  varchar(255),
       resource_id        varchar(64) NOT NULL UNIQUE REFERENCES adversarial_resources(id)
     )`,
    `CREATE TABLE adversarial_inners (
       id                 varchar(64) PRIMARY KEY,
       a_bool             boolean NOT NULL,
       a_string           varchar(255) NOT NULL,
       a_number           int NOT NULL,
       a_optional_string  varchar(255),
       parent_id          varchar(64) NOT NULL UNIQUE REFERENCES adversarial_parents(id)
     )`,
    `CREATE TABLE adversarial_tags (
       tag_id       varchar(64) PRIMARY KEY,
       name         varchar(255),
       resource_id  varchar(64) NOT NULL REFERENCES adversarial_resources(id)
     )`,
    `CREATE TABLE adversarial_categories (
       id           varchar(64) PRIMARY KEY,
       name         varchar(255) NOT NULL,
       resource_id  varchar(64) NOT NULL REFERENCES adversarial_resources(id)
     )`,
    `CREATE TABLE adversarial_sub_categories (
       id           varchar(64) PRIMARY KEY,
       name         varchar(255) NOT NULL,
       category_id  varchar(64) NOT NULL REFERENCES adversarial_categories(id)
     )`,
    `CREATE TABLE adversarial_labels (
       id               varchar(64) PRIMARY KEY,
       name             varchar(255),
       sub_category_id  varchar(64) NOT NULL REFERENCES adversarial_sub_categories(id)
     )`,
  ];

  return {
    name: "mysql",
    mapper: buildMapper(schema),

    async start(): Promise<void> {
      // The image's entrypoint prepends `mysqld` to arguments that begin with a dash, which is
      // how spring-data's MySQL leg passes the same two flags.
      container = await new MySqlContainer(MYSQL_IMAGE)
        .withCommand([
          "--character-set-server=utf8mb4",
          `--collation-server=${MYSQL_COLLATION}`,
        ])
        .start();
      pool = mysql.createPool({
        uri: container.getConnectionUri(),
        // The corpus's instants are UTC and `datetime` stores what it is handed, so nothing here
        // should be converting between zones. Saying so is what keeps a runner's local time zone
        // out of the comparison.
        timezone: "Z",
      });
      db = drizzleMysql(pool, { mode: "default" });

      for (const statement of DDL) {
        await db.execute(sql.raw(statement));
      }

      const rows = seedRows();
      await db.insert(resources).values(rows.resources.map(toMysqlResourceRow));
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

    // `@@version_comment` rather than `version()`: the latter answers `8.4.x` on MySQL and would
    // make the banner assertion a version check, while this one names the engine and is a syntax
    // error on both other stores.
    async serverBanner(): Promise<string> {
      // drizzle types every mysql2 `execute` as an INSERT-style result header; a SELECT hands
      // back `[rows, fields]`, which is what this reads.
      const result = (await connected().execute(
        sql`select @@version_comment as version`,
      )) as unknown as [{ version: string }[]];
      return result[0][0]?.version ?? "";
    },
  };
}

/**
 * The one row shape MySQL will not take as the other two stores hand it over.
 *
 * `created_at` is a real `DATETIME`, and the corpus's instants are RFC-3339 (`2024-06-01T00:00:00Z`
 * — the same strings PostgreSQL's `timestamptz` and SQLite's `text` column store verbatim).
 * MySQL's strict mode REJECTS that spelling on INSERT, so the instant is rewritten into MySQL's
 * own `YYYY-MM-DD HH:MM:SS.ffffff` here, preserving every digit including the a5 seed's
 * microseconds.
 *
 * Only the INSERT is rewritten. What the adapter BINDS in a filter is still the RFC-3339 string,
 * which MySQL parses leniently on comparison (warning 1292, and it lands on the right instant) —
 * and whether it does land there is not taken on trust: `ts-eq`, `ts-eq-offset` and `ts-ne` are
 * oracle-compared on this leg like every other action.
 */
function toMysqlResourceRow(row: ResourceRow): ResourceRow {
  if (row.createdAt === null) {
    return row;
  }
  const match = /^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2}(?:\.\d+)?)Z$/.exec(
    row.createdAt,
  );
  if (!match) {
    throw new Error(
      `derived-fields.json createdAt "${row.createdAt}" is not the RFC-3339 UTC instant this store rewrites`,
    );
  }
  return { ...row, createdAt: `${match[1]} ${match[2]}` };
}

/** Container start dominates the PostgreSQL and MySQL legs' setup; SQLite finishes instantly. */
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
const STORE_FACTORIES: Record<StoreName, () => AdversarialStore> = {
  sqlite: sqliteStore,
  postgres: postgresStore,
  mysql: mysqlStore,
};
const store: AdversarialStore = STORE_FACTORIES[STORE_NAME]();
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
  return CHECK_RESOURCES.principal;
}

// -- oracle: ask the PDP itself, row by row --

async function oracleAllowedIds(action: string): Promise<string[]> {
  const ids: string[] = [];
  for (const resource of CHECK_RESOURCES.resources) {
    const result = await cerbos.checkResource({
      principal: principal(),
      resource,
      actions: [action],
    });
    if (result.isAllowed(action)) {
      ids.push(resource.id);
    }
  }
  return ids.sort();
}

async function expectCatalogOracle(action: string): Promise<string[]> {
  const ids = await oracleAllowedIds(action);
  const expectation = ACTION_CONTROL_PLANE.oracleExpectations.get(action);
  if (expectation === undefined)
    throw new Error(`catalog has no action ${action}`);
  switch (expectation.kind) {
    case "empty":
      expect({ action, cardinality: ids.length }).toEqual({
        action,
        cardinality: 0,
      });
      break;
    case "total":
      expect({ action, cardinality: ids.length }).toEqual({
        action,
        cardinality: CHECK_RESOURCES.resources.length,
      });
      break;
    case "proper-subset":
      expect({
        action,
        nonEmpty: ids.length > 0,
        nonTotal: ids.length < CHECK_RESOURCES.resources.length,
      }).toEqual({ action, nonEmpty: true, nonTotal: true });
      break;
    default: {
      const exhaustive: never = expectation;
      throw exhaustive;
    }
  }
  return ids;
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
  // Anti-vacuity for the store split: every other assertion in this file is identical on every
  // leg, so a PostgreSQL leg that silently fell back to SQLite would pass the entire suite while
  // proving nothing about PostgreSQL — the exact gap #320 reports. Ask the connection the suite
  // actually queries through which engine it is.
  test("executes against the store ADAPTER_TEST_DB selects", async () => {
    const banner = await store.serverBanner();
    expect({ store: store.name, engine: banner.split(" ")[0] }).toEqual({
      store: STORE_NAME,
      engine: STORE_ENGINES[STORE_NAME],
    });
  });

  // The MySQL leg's other precondition, and the one no banner reports: the collation the server
  // was started with. Under MySQL's default `utf8mb4_0900_ai_ci` these three queries return the
  // case variant, the accent variant and the wrong-case prefix respectively, which is a live
  // authorization over-grant — `cs-eq`, `unicode-eq` and every `hier-*` action would then return
  // rows the PDP denies. The corpus catches that through the oracle; this catches it by NAME, so
  // a failure reads as "the store is misconfigured" rather than as a translation bug.
  //
  // Executed against the seeded rows rather than asked of `@@collation_database`, because the
  // requirement is what the comparison DOES, not what the setting is called.
  (STORE_NAME === "mysql" ? test : test.skip)(
    "the MySQL leg runs under a case- and accent-sensitive collation",
    async () => {
      expect({
        // "one" is seed a1; "One" is seed c1. Bound, not interpolated, so the comparison is the
        // one the adapter's own filters make.
        caseVariant: await store.selectIds(sql`a_string = ${"one"}`),
        // "héllo🚀" is seed a6; the accent-folded spelling matches nothing.
        accentFolded: await store.selectIds(sql`a_string = ${"hello🚀"}`),
        // `hier-*` reaches the same collation through LIKE rather than through `=`.
        wrongCasePrefix: await store.selectIds(sql`a_string like ${"ON%"}`),
      }).toEqual({
        caseVariant: ["a1"],
        accentFolded: [],
        wrongCasePrefix: [],
      });
    },
  );

  test("adapterctl selection is internally consistent", () => {
    if (FULL_MATRIX) {
      expect([...ACTION_CONTROL_PLANE.outcomes.keys()].sort()).toEqual(
        ACTION_CONTROL_PLANE.allActions,
      );
      expect(ACTION_CONTROL_PLANE.unassessedActions).toEqual([]);
    }
    expect(ACTION_CONTROL_PLANE.selectedActions).toHaveLength(
      FULL_MATRIX ? ACTION_CONTROL_PLANE.allActions.length : 1,
    );
  });

  test.each(ORACLE_ACTIONS)("%s matches the check() oracle", async (action) => {
    const [oracle, filtered] = await Promise.all([
      expectCatalogOracle(action),
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
    "$action fails during translation with the declared message, before any filter exists ($reason)",
    async ({ action, message }) => {
      await expectCatalogOracle(action);
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
  // cannot evaluate a non-boolean conjunction — so the catalog marks its oracle as empty,
  // and a bare "it throws" would say nothing about whether refusing it is REQUIRED.
  //
  // This is that argument. The other conjunct is `R.attr.aBool`, which the adapter certainly can
  // express and which `root-bare-bool` spells on its own; an adapter that dropped the conjunct it
  // could not translate would emit exactly that filter and return every row it selects, all of
  // which the PDP denies for this action.
  fullMatrixTest(
    "filter-as-conjunct must be refused: dropping its untranslatable half over-grants",
    async () => {
      await expectCatalogOracle("filter-as-conjunct");
      await expectCatalogOracle("root-bare-bool");

      const message = THROWING_ACTIONS.find(
        ({ action }) => action === "filter-as-conjunct",
      )?.message;
      expect(message).toBeDefined();
      await expect(adapterFilteredIds("filter-as-conjunct")).rejects.toThrow(
        message,
      );
    },
  );

  // #302. `null-eq-missing` probes `aOptionalString == null`, and `aOptionalString` follows the
  // corpus default: a NULL column sends NO attribute. Both halves are asserted because the
  // rejection alone would pass vacuously if the adapter threw for an unrelated reason — the
  // over-grant under the default representation is what makes the rejection necessary.

  test.each(NULL_REPRESENTATION_OMITTED)(
    "$action over-grants under the explicit representation and is rejected under omitted ($reason)",
    async ({ action, message }) => {
      await expectCatalogOracle(action);

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
  fullMatrixTest(
    "a per-attribute declaration overrides the call-level representation",
    async () => {
      // `owner` declares "explicit", so the call-level "omitted" does not reach it.
      await expect(adapterFilteredIds("null-eq", "omitted")).resolves.toEqual(
        await oracleAllowedIds("null-eq"),
      );

      // Strip the declaration and the same action under the same option is rejected — so the
      // stripped mapper the completeness guard below uses is not quietly equivalent to MAPPER.
      await expect(
        adapterFilteredIds(
          "null-eq",
          "omitted",
          MAPPER_WITHOUT_NULL_CONVENTIONS,
        ),
      ).rejects.toThrow(NULL_OMITTED_MESSAGE);
    },
  );

  // #302 completeness guard. The rejection must key off the null OPERAND, not off a list of
  // operators: `hasIntersection(tagNames, ["public", null])` carries one in its value list, and
  // an allowlist of eq/ne/in silently misses it. Enumerating the corpus rather than naming
  // shapes means a newly added action carrying a null constant is covered automatically.
  fullMatrixTest(
    "every corpus action carrying a null literal is rejected under omitted",
    async () => {
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
    },
  );

  test.each(ACTION_CONTROL_PLANE.upstreamBlockedActions)(
    "$action pins the upstream planner divergence ($reason)",
    async ({ action }) => {
      const queryPlan = await cerbos.planResources({
        principal: principal(),
        resource: { kind: seedsFile.resourceKind },
        action,
      });
      await expectCatalogOracle(action);
      const allIds = CHECK_RESOURCES.resources.map(({ id }) => id).sort();

      expect(queryPlan.kind).toBe(PlanKind.ALWAYS_ALLOWED);
      expect(await adapterFilteredIds(action)).toEqual(allIds);
    },
  );

  // The corpus pins two count spellings over the chain — `size(...) == 0` and
  // `!(size(...) > 0)` — but the guard has to be a property of the chain rather than of the
  // two spellings that happen to be pinned. These synthesise the remaining
  // threshold/polarity combinations onto the same seeded store and assert the parentless rows
  // stay out of every one, including an arbitrary-N threshold neither corpus action reaches
  // (cerbos/query-plan-adapters#316). This adapter guards the COUNT expression itself, so it
  // was already aligned — the assertion pins that it stays that way.
  fullMatrixTest(
    "every count threshold over the chain inherits the absent-parent guard",
    async () => {
      const chain = new PlanExpressionVariable(
        "request.resource.attr.mainCategory.subCategories",
      );
      const size = new PlanExpression("size", [chain]);
      const compare = (operator: string, threshold: number) =>
        new PlanExpression(operator, [
          size,
          new PlanExpressionValue(threshold),
        ]);
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
    },
  );

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
});
