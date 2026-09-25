import * as fs from "fs";
import * as path from "path";

import { afterAll, beforeAll, describe, expect, test } from "@jest/globals";
import Database from "better-sqlite3";
import { sql } from "drizzle-orm";
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

import { PlanKind, queryPlanToDrizzle, UnsupportedQueryPlanError } from ".";
import type { Mapper, MapperEntry } from ".";
import {
  buildMapper,
  mysqlSchema,
  pdpTags,
  planOf,
  postgresSchema,
  readCorpusJson,
  readGoldens,
  sqliteSchema,
} from "./corpus";
import type { Golden } from "./corpus";

/**
 * The conformance harness (conformance/README.md, "The harness contract"). It loads the corpus
 * dataset into a real store, translates every recorded golden plan for both pinned PDPs, runs the
 * query, and compares the returned ids with the `allowed` ids the PDP's check() recorded. No PDP
 * runs here. Exceptions live in `conformance-ledger.json`.
 *
 * The store is chosen with `ADAPTER_TEST_DB` (`sqlite` by default, `postgres` and `mysql` via
 * testcontainers). Drizzle owns quoting and placeholders, but the adapter still makes
 * dialect-sensitive choices — `float(53)` casts, `substr`/`replace` string matching, boolean CASE
 * arms, timestamp binding, division by zero — so a store this harness does not execute against is
 * a store the adapter does not cover (cerbos/query-plan-adapters#320, #340).
 */

// -- the dataset: conformance/seeds.json + derived-fields.json ----------------------------------

interface Tag {
  id: string;
  name: string | null;
}

interface Seed {
  id: string;
  aBool: boolean | null;
  aString: string | null;
  aNumber: number | null;
  aOptionalString: string | null;
  tags: Tag[];
  subCategoryNames: string[];
  /** The seed whose scalars this row's to-one `parent` carries; null for no parent. */
  parentSeedId: string | null;
  aNumberList: (number | null)[];
  aBoolList: (boolean | null)[];
}

interface DerivedEntry {
  createdBy: string;
  aDouble: number | null;
  createdAt: string | null;
  updatedAt: string | null;
  scope: string | null;
  labels: (string | null)[];
}

const SEEDS = (readCorpusJson("seeds.json") as { seeds: Seed[] }).seeds;
const DERIVED = (
  readCorpusJson("derived-fields.json") as { derived: Record<string, DerivedEntry> }
).derived;

function derivedFor(seed: Seed): DerivedEntry {
  const entry = DERIVED[seed.id];
  if (entry === undefined) {
    throw new Error(`derived-fields.json has no entry for seed "${seed.id}"`);
  }
  return entry;
}

// The real to-one relation (conformance/README.md, "The dataset"): every resource owns a FRESH
// parent (and inner) row carrying the named seed's scalars, so no two resources share one.
const SEEDS_BY_ID = new Map(SEEDS.map((seed) => [seed.id, seed]));

function parentSeedOf(seed: Seed | undefined): Seed | undefined {
  const id = seed?.parentSeedId;
  if (id === undefined || id === null) {
    return undefined;
  }
  const parent = SEEDS_BY_ID.get(id);
  if (parent === undefined) {
    throw new Error(`seeds.json: "${seed?.id}" names parent "${id}", which is not a seed id`);
  }
  return parent;
}

// -- the seeded rows, derived once and shared by every store ------------------------------------

interface ResourceRow {
  id: string;
  aBool: boolean | null;
  aString: string | null;
  aNumber: number | null;
  aDouble: number | null;
  aOptionalString: string | null;
  createdBy: string;
  scope: string | null;
  createdAt: string | null;
  updatedAt: string | null;
  tagNamesJson: (string | null)[];
  aNumberListJson: (number | null)[];
  aBoolListJson: (boolean | null)[];
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
  aBool: boolean | null;
  aString: string | null;
  aNumber: number | null;
  aOptionalString: string | null;
  resourceId: string;
}

interface InnerRow {
  id: string;
  aBool: boolean | null;
  aString: string | null;
  aNumber: number | null;
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
      aDouble: derivedFor(seed).aDouble,
      aOptionalString: seed.aOptionalString,
      createdBy: derivedFor(seed).createdBy,
      scope: derivedFor(seed).scope,
      createdAt: derivedFor(seed).createdAt,
      updatedAt: derivedFor(seed).updatedAt,
      tagNamesJson: seed.tags.map((tag) => tag.name),
      aNumberListJson: seed.aNumberList,
      aBoolListJson: seed.aBoolList,
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
    // One category holding every subcategory name (conformance/README.md, "The dataset").
    const categoryId = `${seed.id}-cat`;
    if (seed.subCategoryNames.length > 0) {
      rows.categories.push({
        id: categoryId,
        name: "business",
        resourceId: seed.id,
      });
    }
    seed.subCategoryNames.forEach((subName, index) => {
      const subCategoryId = `${categoryId}-sub-${index}`;
      rows.subCategories.push({
        id: subCategoryId,
        name: subName,
        categoryId,
      });
      derivedFor(seed).labels.forEach((labelName, labelIndex) => {
        rows.labels.push({
          id: `${subCategoryId}-label-${labelIndex}`,
          name: labelName,
          subCategoryId,
        });
      });
    });
  }

  // A store inserts each list in one statement, and drizzle rejects an empty VALUES list.
  for (const [label, list] of Object.entries(rows)) {
    if (list.length === 0) {
      throw new Error(`seeds.json produced no ${label} rows`);
    }
  }

  return rows;
}

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
  readonly indexMappers?: Record<string, MapperEntry>[];
  start(): Promise<void>;
  stop(): Promise<void>;
  selectIds(filter: SQL | undefined): Promise<string[]>;
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
          a_bool INTEGER,
          a_string TEXT,
          a_number INTEGER,
          a_double REAL,
          a_optional_string TEXT,
          created_by TEXT NOT NULL,
          scope TEXT,
          created_at TEXT,
          updated_at TEXT,
          tag_names_json TEXT,
          a_number_list_json TEXT,
          a_bool_list_json TEXT
        );
        CREATE TABLE adversarial_parents (
          id TEXT PRIMARY KEY,
          a_bool INTEGER,
          a_string TEXT,
          a_number INTEGER,
          a_optional_string TEXT,
          resource_id TEXT NOT NULL UNIQUE
        );
        CREATE TABLE adversarial_inners (
          id TEXT PRIMARY KEY,
          a_bool INTEGER,
          a_string TEXT,
          a_number INTEGER,
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
 * How the PostgreSQL leg's database is initialised: `--lc-collate=C`, a byte-order collation.
 *
 * **This is a correctness requirement, not a preference.** CEL orders strings by code point, and
 * `<`, `<=`, `>` and `>=` on a text column follow the column's collation. PostgreSQL collations are
 * deterministic, so `=` is byte-exact under any of them, but a linguistic one orders case,
 * accents and punctuation below the letter: under glibc's `en_US.utf8` `'One' > 'a'` is TRUE,
 * which over-grants `comparison/greater-than/string-code-point-order`
 * (cerbos/query-plan-adapters#489). `"C"` orders by byte, which for UTF-8 is code point order.
 *
 * Stated rather than inherited: the Alpine image reports `en_US.utf8` without it, and orders by
 * byte only because musl's `strcoll` does. The same database on a glibc image, or on a managed
 * service, orders linguistically.
 *
 * Overridable so the over-grant can be reproduced rather than taken on trust —
 * `ADAPTER_TEST_POSTGRES_INITDB_ARGS="--locale-provider=icu --icu-locale=en-US" npm run
 * test:adversarial:postgres` gives the pinned image ICU's linguistic order, which musl cannot, and
 * fails both `string-code-point-order` cases. A measurement escape hatch, not a CI leg.
 */
const POSTGRES_INITDB_ARGS =
  process.env["ADAPTER_TEST_POSTGRES_INITDB_ARGS"] ?? "--lc-collate=C";

/**
 * The PostgreSQL leg (cerbos/query-plan-adapters#320).
 *
 * The column types are the point: `boolean` and `timestamptz` exercise the typed paths SQLite
 * cannot reach — on SQLite a boolean is an integer and a timestamp is text the adapter rewrites into
 * its own string form, so a CASE arm yielding `1` instead of `true`, or a timestamp bound in a
 * layout PostgreSQL cannot parse, passes there and fails here. PostgreSQL also raises on
 * division by zero where SQLite returns NULL, which is what proves the adapter's IEEE CASE arms
 * guard the division rather than merely reshaping its NULL.
 *
 * The database is initialised under `POSTGRES_INITDB_ARGS`, below: a byte-order collation.
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
    // The same mapper with the three ordered lists stored two other ways: a native array rebased to
    // a zero lower bound, and plain `json`. A caller chooses the storage, so the corpus cannot vary
    // it; the harness replays every case that reads these lists under each one.
    indexMappers: [
      withListColumns(buildMapper(schema), "pgArray", {
        tagNames: resources.tagNamesArray,
        aNumberList: resources.aNumberListArray,
        aBoolList: resources.aBoolListArray,
      }),
      withListColumns(buildMapper(schema), "json", {
        tagNames: resources.tagNamesPlainJson,
        aNumberList: resources.aNumberListPlainJson,
        aBoolList: resources.aBoolListPlainJson,
      }),
    ],

    async start(): Promise<void> {
      container = await new PostgreSqlContainer(POSTGRES_IMAGE)
        .withEnvironment({ POSTGRES_INITDB_ARGS })
        .start();
      pool = new Pool({ connectionString: container.getConnectionUri() });
      db = drizzlePostgres(pool);

      await db.execute(sql`
        CREATE TABLE adversarial_resources (
          id                 text PRIMARY KEY,
          a_bool             boolean,
          a_string           text,
          a_number           integer,
          a_double           double precision,
          a_optional_string  text,
          created_by         text NOT NULL,
          scope              text,
          created_at         timestamptz,
          updated_at         timestamptz,
          tag_names_json     jsonb,
          tag_names_plain_json json,
          tag_names_array    text[],
          a_number_list_json jsonb,
          a_number_list_plain_json json,
          a_number_list_array integer[],
          a_bool_list_json   jsonb,
          a_bool_list_plain_json json,
          a_bool_list_array  boolean[]
        );
        CREATE TABLE adversarial_parents (
          id                 text PRIMARY KEY,
          a_bool             boolean,
          a_string           text,
          a_number           integer,
          a_optional_string  text,
          resource_id        text NOT NULL UNIQUE REFERENCES adversarial_resources(id)
        );
        CREATE TABLE adversarial_inners (
          id                 text PRIMARY KEY,
          a_bool             boolean,
          a_string           text,
          a_number           integer,
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
      // The same list three ways: jsonb (the shared mapper), plain json, and a native array
      // rebased to a ZERO lower bound, so a raw `[index + 1]` would read the wrong element and
      // only the positional JSON conversion the adapter emits reads the right one. A JSON null
      // element becomes a SQL NULL element, which `to_jsonb` turns back into a JSON null.
      await db.execute(sql`update adversarial_resources set
        tag_names_plain_json = tag_names_json::json,
        tag_names_array = case when jsonb_array_length(tag_names_json) > 0 then
          ('[0:' || (jsonb_array_length(tag_names_json) - 1) || ']=' ||
            array(select jsonb_array_elements_text(tag_names_json))::text)::text[]
          else array[]::text[] end,
        a_number_list_plain_json = a_number_list_json::json,
        a_number_list_array = case when jsonb_array_length(a_number_list_json) > 0 then
          ('[0:' || (jsonb_array_length(a_number_list_json) - 1) || ']=' ||
            array(select jsonb_array_elements_text(a_number_list_json))::text)::integer[]
          else array[]::integer[] end,
        a_bool_list_plain_json = a_bool_list_json::json,
        a_bool_list_array = case when jsonb_array_length(a_bool_list_json) > 0 then
          ('[0:' || (jsonb_array_length(a_bool_list_json) - 1) || ']=' ||
            array(select jsonb_array_elements_text(a_bool_list_json))::text)::boolean[]
          else array[]::boolean[] end`);
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
 * | probe | `utf8mb4_0900_ai_ci` (default) | `utf8mb4_0900_as_cs` | `utf8mb4_bin` | `utf8mb4_0900_bin` |
 * |---|---|---|---|---|
 * | `'One' = 'one'` | TRUE — over-grants `string/equals/case-sensitive` | FALSE | FALSE | FALSE |
 * | `'héllo' = 'hello'` | TRUE — over-grants `string/equals/non-ascii-literal` | FALSE | FALSE | FALSE |
 * | `'one' LIKE 'ON%'` | TRUE — over-grants every `hierarchy/*` prefix probe | FALSE | FALSE | FALSE |
 * | `'o\u00ADne' = 'one'` (soft hyphen) | TRUE | TRUE — over-grants `string/equals/case-sensitive` on seed h6 | FALSE | FALSE |
 * | `'one ' = 'one'` | FALSE | FALSE | TRUE (PAD SPACE) | FALSE |
 *
 * A collation that makes `=` match strings CEL tells apart is a **store misconfiguration**, not a
 * limitation of this adapter: no filter it could emit would restore byte-exact equality, and
 * listing `string/equals/case-sensitive` as `unsupported` in the ledger on that basis would blame the translator for the
 * DDL. So the leg pins a byte-exact collation and states the requirement, exactly as `ent` pins
 * one per column and `spring-data` passes `--collation-server`.
 *
 * Case-sensitive is not byte-exact. `utf8mb4_0900_as_cs` is a UCA collation, and UCA gives a
 * default-ignorable code point such as SOFT HYPHEN (U+00AD) no weight at all, so seed h6's
 * `"o\u00ADne"` equals `"one"` under it — an over-grant on `string/equals/case-sensitive` and every `in` over the
 * principal's teams, and an under-grant on `logic/and/three-conjuncts`'s `!=`
 * (cerbos/query-plan-adapters#474). `utf8mb4_bin` is byte-exact but PAD SPACE. Only
 * `utf8mb4_0900_bin` (MySQL 8.0.17+) is byte-exact AND NO PAD, which is why it is the collation
 * the adapter already puts on its own `string()` literals (`buildBooleanString` in `values.ts`),
 * and the one every MySQL leg in this repository runs.
 *
 * Overridable so the over-grant can be reproduced rather than taken on trust —
 * `ADAPTER_TEST_MYSQL_COLLATION=utf8mb4_0900_ai_ci npm run test:adversarial:mysql` fails on the
 * case and accent probes, and `…=utf8mb4_0900_as_cs` on the h6 soft-hyphen probes. Same escape
 * hatch as spring-data's `-Dadapter.test.mysql.collation`.
 */
const MYSQL_COLLATION =
  process.env["ADAPTER_TEST_MYSQL_COLLATION"] ?? "utf8mb4_0900_bin";

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
 *   actually found, and the reason `string()` is refused over every column but a boolean
 *   (`UNSUPPORTED_CONVERSIONS` in `values.ts`). Both other stores accept it. A boolean is lowered
 *   through a CASE instead, and its two literals are the one place the adapter names a MySQL
 *   collation: a literal compares in the CONNECTION's, which is mysql2's `utf8mb4_unicode_ci`
 *   here, not the server's `MYSQL_COLLATION` (`buildBooleanString` in `values.ts`).
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
       a_bool             boolean,
       a_string           varchar(255),
       a_number           int,
       a_double           double,
       a_optional_string  varchar(255),
       created_by         varchar(64) NOT NULL,
       scope              varchar(255),
       created_at         datetime(6),
       updated_at         datetime(6),
       tag_names_json     json,
       a_number_list_json json,
       a_bool_list_json   json
     )`,
    `CREATE TABLE adversarial_parents (
       id                 varchar(64) PRIMARY KEY,
       a_bool             boolean,
       a_string           varchar(255),
       a_number           int,
       a_optional_string  varchar(255),
       resource_id        varchar(64) NOT NULL UNIQUE REFERENCES adversarial_resources(id)
     )`,
    `CREATE TABLE adversarial_inners (
       id                 varchar(64) PRIMARY KEY,
       a_bool             boolean,
       a_string           varchar(255),
       a_number           int,
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
 * and whether it does land there is not taken on trust: the `timestamp/*` cases are replayed on
 * this leg like every other case.
 */
function toMysqlResourceRow(row: ResourceRow): ResourceRow {
  function timestamp(value: string | null): string | null {
    if (value === null) return null;
    const match = /^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2}(?:\.\d+)?)Z$/.exec(
      value,
    );
    if (!match) {
      throw new Error(
        `derived-fields.json timestamp "${value}" is not the RFC-3339 UTC instant this store rewrites`,
      );
    }
    return `${match[1]} ${match[2]}`;
  }
  return {
    ...row,
    createdAt: timestamp(row.createdAt),
    updatedAt: timestamp(row.updatedAt),
  };
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

/** `base` with the three ordered lists moved to other columns, every other declaration kept. */
function withListColumns(
  base: Record<string, MapperEntry>,
  indexable: "pgArray" | "json",
  columns: Record<"tagNames" | "aNumberList" | "aBoolList", unknown>,
): Record<string, MapperEntry> {
  const mapper = { ...base };
  for (const [name, column] of Object.entries(columns)) {
    const reference = `request.resource.attr.${name}`;
    mapper[reference] = { ...(base[reference] as object), column, indexable } as MapperEntry;
  }
  return mapper;
}

// -- the ledger -----------------------------------------------------------------------------------

interface LedgerEntry {
  status: "unsupported" | "divergent";
  reason: string;
  issue?: string;
  pdp?: string[];
}

const LEDGER = (
  JSON.parse(
    fs.readFileSync(path.join(__dirname, "..", "conformance-ledger.json"), "utf8"),
  ) as { cases: Record<string, LedgerEntry> }
).cases;

function ledgerEntry(golden: Golden, tag: string): LedgerEntry | undefined {
  const entry = LEDGER[golden.id];
  return entry && (entry.pdp === undefined || entry.pdp.includes(tag)) ? entry : undefined;
}

// -- the replay -----------------------------------------------------------------------------------

async function selectAllowed(golden: Golden, mapper: Mapper): Promise<string[]> {
  const result = queryPlanToDrizzle({ queryPlan: planOf(golden), mapper });
  if (result.kind === PlanKind.ALWAYS_DENIED) return [];
  return store.selectIds(result.kind === PlanKind.CONDITIONAL ? result.filter : undefined);
}

/** Cases whose plan reads one of the ordered lists, replayed under every storage on PostgreSQL. */
const LIST_REFERENCE = /"request\.resource\.attr\.(tagNames|aNumberList|aBoolList)"/;

const TAGS = pdpTags();
const GOLDENS = new Map(TAGS.map((tag) => [tag, readGoldens(tag)]));
const passed: Record<string, Record<string, number>> = {};
const totals: Record<string, Record<string, number>> = {};
const skipped: Record<string, number> = {};

beforeAll(() => store.start(), STORE_STARTUP_TIMEOUT_MS);

afterAll(async () => {
  await store.stop();
  const lines = TAGS.map((tag) => {
    const tiers = Object.keys(totals[tag] ?? {}).sort();
    return `${tag}: ${tiers.map((tier) => `${tier} ${passed[tag]?.[tier] ?? 0}/${totals[tag]?.[tier]}`).join(", ")} (${skipped[tag] ?? 0} planner divergence skipped)`;
  });
  console.log(`conformance (${STORE_NAME}) passed per tier\n${lines.join("\n")}`);
}, STORE_STARTUP_TIMEOUT_MS);

describe(`conformance (${STORE_NAME})`, () => {
  // Every assertion below is identical on every leg, so a PostgreSQL leg that silently fell back to
  // SQLite would pass while proving nothing about PostgreSQL. Ask the connection which engine it is.
  test("executes against the store ADAPTER_TEST_DB selects", async () => {
    const banner = await store.serverBanner();
    expect(banner.split(" ")[0]).toBe(STORE_ENGINES[STORE_NAME]);
  });

  // Store configuration is part of conformance: under MySQL's default collation these return the
  // case variant, the accent variant and the wrong-case prefix. Asserted by name so a failure reads
  // as "the store is misconfigured" rather than as a translation bug.
  (STORE_NAME === "mysql" ? test : test.skip)(
    "the MySQL leg runs under a byte-exact NO PAD collation",
    async () => {
      expect({
        caseVariant: await store.selectIds(sql`a_string = ${"one"}`),
        accentFolded: await store.selectIds(sql`a_string = ${"hello🚀"}`),
        wrongCasePrefix: await store.selectIds(sql`a_string like ${"ON%"}`),
        trailingSpace: await store.selectIds(sql`a_string = ${"one "}`),
      }).toEqual({ caseVariant: ["a1"], accentFolded: [], wrongCasePrefix: [], trailingSpace: [] });
    },
  );

  test("the ledger names no case without a golden file", () => {
    const ids = new Set(TAGS.flatMap((tag) => GOLDENS.get(tag)!.map((golden) => golden.id)));
    expect(Object.keys(LEDGER).filter((id) => !ids.has(id))).toEqual([]);
  });

  for (const tag of TAGS) {
    // A plannerDivergence is a PDP bug no adapter can pass. The generator writes it only into the
    // goldens of the tags it applies to, so a non-null value on this tag's golden is the whole test.
    const cases = GOLDENS.get(tag)!.filter((golden) => golden.plannerDivergence === null);

    test.each(cases.map((golden) => [golden.id, golden] as const))(
      `${tag} %s`,
      async (_id, golden) => {
        // A golden filed under the wrong tag directory would be replayed against the wrong ledger scope.
        expect(golden.pdp).toBe(tag);
        const entry = ledgerEntry(golden, tag);
        if (entry?.status === "unsupported") {
          expect(() => queryPlanToDrizzle({ queryPlan: planOf(golden), mapper: MAPPER })).toThrow(
            UnsupportedQueryPlanError,
          );
          return;
        }
        const ids = await selectAllowed(golden, MAPPER);
        if (entry?.status === "divergent") {
          expect(ids).not.toEqual(golden.allowed);
          return;
        }
        expect(ids).toEqual(golden.allowed);
        if (LIST_REFERENCE.test(JSON.stringify(golden.plan))) {
          for (const mapper of store.indexMappers ?? []) {
            expect(await selectAllowed(golden, mapper)).toEqual(golden.allowed);
          }
        }
        (passed[tag] ??= {})[golden.tier] = (passed[tag]?.[golden.tier] ?? 0) + 1;
      },
    );

    // The denominator is every golden in the tier, skipped planner divergences included.
    for (const golden of GOLDENS.get(tag)!) {
      (totals[tag] ??= {})[golden.tier] = (totals[tag]?.[golden.tier] ?? 0) + 1;
    }
    skipped[tag] = GOLDENS.get(tag)!.length - cases.length;
  }
});
