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
  buildMapper,
  classifyActionsForAdapter,
  degenerateOraclesOf,
  mysqlSchema,
  postgresSchema,
  readCorpusJson,
  requireMessage,
  sqliteSchema,
  assertPinnedPdp,
  pdpAddress,
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
 * run, selected with `ADAPTER_TEST_DB` (`sqlite` by default, `postgres` and `mysql` for the
 * container-backed legs). Drizzle's dialect objects own quoting and placeholders, but the adapter
 * still makes dialect-sensitive choices of its own — `float(53)` casts, `substr`/`replace` string
 * matching, boolean-versus-integer CASE arms, timestamp binding, division by zero — and a store
 * the harness does not execute against is a store this adapter does not actually cover
 * (cerbos/query-plan-adapters#320 for PostgreSQL, #340 for MySQL).
 */

const cerbos = new Cerbos(pdpAddress(), { tls: false });

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
  /** Read only by position; a null element is a VALUE, not a missing attribute. */
  aNumberList: (number | null)[];
  aBoolList: (boolean | null)[];
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
  "aNumberList",
  "aBoolList",
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
  "updatedAt",
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
  "zero",
  "emptyTeams",
  "manyStructs",
  "nullableStructs",
  "missingStructs",
] as const;

/** One seed's derived fields, exactly as conformance/derived-fields.json carries them. */
interface DerivedEntry {
  createdBy: string;
  aDouble: number | null;
  createdAt: string | null;
  updatedAt: string | null;
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

/** Principal attributes have explicit value shapes, including absent versus null struct members. */
function assertPrincipalAttrShape(label: string, value: unknown): void {
  const key = label.slice(label.lastIndexOf(".") + 1);
  if (key === "context" && typeof value === "string") return;
  if (key === "zero" && value === 0) return;
  if (
    ["allowedTags", "fewTeams", "manyTeams", "emptyTeams"].includes(key) &&
    Array.isArray(value) &&
    value.every((entry) => typeof entry === "string")
  )
    return;
  if (
    ["manyStructs", "nullableStructs", "missingStructs"].includes(key) &&
    Array.isArray(value) &&
    value.every((entry: unknown) => {
      if (typeof entry !== "object" || entry === null || Array.isArray(entry))
        return false;
      if (key === "missingStructs") return Object.keys(entry).length === 0;
      return (
        Object.keys(entry).length === 1 &&
        "name" in entry &&
        (key === "nullableStructs"
          ? entry.name === null
          : typeof entry.name === "string")
      );
    })
  )
    return;
  throw new Error(
    `${label} does not match its declared corpus principal shape`,
  );
}

const seedsFile = readCorpusJson("seeds.json") as SeedsFile;
const actionsFile = readCorpusJson("actions.json") as ActionsFile;
const derivedFile = readCorpusJson("derived-fields.json") as DerivedFile;
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
// A differential cannot fail when the oracle is empty or total: an adapter returning no rows, or
// every row, agrees with it. So every action this adapter ORACLE-COMPARES has its oracle's shape
// asserted before the comparison, reusing the oracle that comparison computes. An action listed in
// `degenerateOracles` in conformance/actions.json — the corpus-level allowlist of actions whose
// oracle is empty or total BY CONSTRUCTION, shared by every harness — must have exactly that
// oracle; every other compared action must have a non-empty, non-total one. The refused shapes
// carry the complementary liveness assertion in `DEGENERACY_LIVENESS_PROBES` below.

const DEGENERATE_ORACLES = degenerateOraclesOf(actionsFile);

const ALL_SEED_IDS = SEEDS.map((seed) => seed.id).sort();

/**
 * The degeneracy guard's per-action assertion over an oracle already computed, labelled so a
 * failure names the action and says why it matters.
 */
function expectOracleShape(action: string, ids: string[]): void {
  const declared = DEGENERATE_ORACLES.get(action);
  const shape =
    ids.length === 0
      ? "empty"
      : ids.length === ALL_SEED_IDS.length &&
          ids.every((id, index) => id === ALL_SEED_IDS[index])
        ? "total"
        : "non-degenerate";
  const expected = declared ?? "non-degenerate";
  if (shape !== expected) {
    throw new Error(
      `${action}: the check() oracle is ${shape} (${ids.length} of ${ALL_SEED_IDS.length} seeds), ` +
        `but ${declared === undefined ? "the action is not listed in" : `it is declared "${declared}" in`} ` +
        "degenerateOracles in conformance/actions.json. The differential cannot fail for a degenerate " +
        "oracle — an adapter returning no rows or every row would agree with it — so an empty or " +
        "total oracle must be declared there by construction, and a declared one must stay exactly that.",
    );
  }
}

/**
 * The planner kind each of these degenerate-by-construction actions folds to, pinned beside its
 * oracle so dropping an input cannot silently turn a conditional error probe into a folded plan.
 * The oracle itself comes from `degenerateOracles`; this map only carries what the corpus does not.
 */
const DEGENERATE_PLANNER_KINDS: Record<string, PlanKind> = {
  "except-root": PlanKind.CONDITIONAL,
  "pv-empty-exists": PlanKind.ALWAYS_DENIED,
  "pv-empty-not-exists": PlanKind.ALWAYS_ALLOWED,
  "pv-empty-all": PlanKind.ALWAYS_ALLOWED,
  "pv-empty-not-all": PlanKind.ALWAYS_DENIED,
  "pv-structs-null": PlanKind.CONDITIONAL,
  "pv-structs-missing": PlanKind.ALWAYS_DENIED,
  "type-string-number": PlanKind.CONDITIONAL,
  "type-number-string": PlanKind.CONDITIONAL,
  "type-columns": PlanKind.CONDITIONAL,
  "type-size-bool": PlanKind.CONDITIONAL,
  "type-size-number": PlanKind.CONDITIONAL,
  "type-hierarchy-number": PlanKind.CONDITIONAL,
  "type-number-contains": PlanKind.CONDITIONAL,
  "type-needle-contains": PlanKind.CONDITIONAL,
  "type-number-startswith": PlanKind.CONDITIONAL,
  "type-needle-startswith": PlanKind.CONDITIONAL,
  "type-number-endswith": PlanKind.CONDITIONAL,
  "type-needle-endswith": PlanKind.CONDITIONAL,
  "eq-map": PlanKind.CONDITIONAL,
  "ne-map": PlanKind.CONDITIONAL,
  "eq-map-null": PlanKind.CONDITIONAL,
  "in-nested-list": PlanKind.CONDITIONAL,
  "in-list-element": PlanKind.CONDITIONAL,
  "hasint-map-element": PlanKind.CONDITIONAL,
};

/**
 * Shapes this adapter refuses to translate: there is no oracle comparison behind them for the
 * sweep above to guard, so they stay here as PDP/policy liveness probes for a group the compared
 * actions cannot cover. See cerbos/query-plan-adapters#324.
 *
 * The list exists as of #340. Before the MySQL leg executed, this adapter translated every shape
 * the guard then sampled and the guard was one-sided; `cast-string-double` was compared, on the
 * belief that `CAST(... AS TEXT)` rendered a double identically on every store. It is a syntax
 * error on MySQL. Its boolean sibling `cast-string-bool` is still compared: a boolean needs no cast
 * target, only a CASE (#418).
 */
const DEGENERACY_LIVENESS_PROBES = [
  "cast-string-double",
  // An empty hierarchy delimiter is refused before the prefix LIKE is built (the LIKE would
  // match the path itself), and a regex with a top-level alternation is a matches(), which this
  // adapter never translates.
  "hier-empty-delim",
  "matches-alt",
  // #414: every newly discriminating shape guards its observed execution side.
  "div-by-division",
  "eq-list",
  "except-eq",
  "except-size",
  "hier-overlaps-list-prefix",
  "ne-list",
  "not-concat-unsolvable",
  "not-concat-unsolvable-ne",
  "pv-exists-one",
  "pv-filter",
  "pv-map",
  "pv-except",
  "pv-structs",
  "regex-alternation",
  "regex-brace",
  "regex-case",
  "regex-digit",
  "regex-dot",
  "regex-grouped",
  "regex-optional-operators",
  "regex-posix",
  "regex-repetition",
  "regex-unanchored",
  "temporal-raw-eq",
  // #396: error-bearing branches retain a non-empty oracle under their enclosing expression.
  "cast-not-double",
  "cast-not-int",
  "cast-not-string-missing",
  "cast-not-string-null",
  "cast-not-timestamp",
  "index-fractional",
  "index-negative",
  "regex-eq-true",
  "regex-final-newline",
  "regex-lookahead",
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
      derivedFor(seed).labels.forEach((labelName, labelIndex) => {
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
  readonly indexMappers?: Record<string, MapperEntry>[];
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
          created_at TEXT,
          updated_at TEXT,
          tag_names_json TEXT,
          a_number_list_json TEXT,
          a_bool_list_json TEXT
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
    indexMappers: [
      {
        ...buildMapper(schema),
        "request.resource.attr.tagNames": { column: resources.tagNamesArray, indexable: "pgArray" },
        "request.resource.attr.aNumberList": { column: resources.aNumberListArray, indexable: "pgArray" },
        "request.resource.attr.aBoolList": { column: resources.aBoolListArray, indexable: "pgArray" },
      },
      {
        ...buildMapper(schema),
        "request.resource.attr.tagNames": { column: resources.tagNamesPlainJson, indexable: "json" },
        "request.resource.attr.aNumberList": { column: resources.aNumberListPlainJson, indexable: "json" },
        "request.resource.attr.aBoolList": { column: resources.aBoolListPlainJson, indexable: "json" },
      },
    ],

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
 * | probe | `utf8mb4_0900_ai_ci` (default) | `utf8mb4_0900_as_cs` | `utf8mb4_bin` | `utf8mb4_0900_bin` |
 * |---|---|---|---|---|
 * | `'One' = 'one'` | TRUE — over-grants `cs-eq` | FALSE | FALSE | FALSE |
 * | `'héllo' = 'hello'` | TRUE — over-grants `unicode-eq` | FALSE | FALSE | FALSE |
 * | `'one' LIKE 'ON%'` | TRUE — over-grants every `hier-*` prefix probe | FALSE | FALSE | FALSE |
 * | `'o\u00ADne' = 'one'` (soft hyphen) | TRUE | TRUE — over-grants `cs-eq` on seed h6 | FALSE | FALSE |
 * | `'one ' = 'one'` | FALSE | FALSE | TRUE (PAD SPACE) | FALSE |
 *
 * A collation that makes `=` match strings CEL tells apart is a **store misconfiguration**, not a
 * limitation of this adapter: no filter it could emit would restore byte-exact equality, and
 * classifying `cs-eq` as `adapterUnsupported` on that basis would blame the translator for the
 * DDL. So the leg pins a byte-exact collation and states the requirement, exactly as `ent` pins
 * one per column and `spring-data` passes `--collation-server`.
 *
 * Case-sensitive is not byte-exact. `utf8mb4_0900_as_cs` is a UCA collation, and UCA gives a
 * default-ignorable code point such as SOFT HYPHEN (U+00AD) no weight at all, so seed h6's
 * `"o\u00ADne"` equals `"one"` under it — an over-grant on `cs-eq` and every `in` over the
 * principal's teams, and an under-grant on `nary-and`'s `!=`
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
       a_bool             boolean NOT NULL,
       a_string           varchar(255) NOT NULL,
       a_number           int NOT NULL,
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

/**
 * The same mapper with every per-attribute null convention stripped, so the call-level option is
 * the only thing governing null operands.
 *
 * The #302 completeness guard is a statement about that option: every corpus action carrying a
 * attribute-null literal must be rejected under `"omitted"`. Null list elements retain their
 * value independently of that option. Declaring `owner`/`coOwner` as explicit-null
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
  await assertPinnedPdp(cerbos);
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
    createdBy: derivedFor(seed).createdBy,
    owner: seed.aOptionalString,
    // The explicit-null alias of the `scope` column, the second half of `null-value-f2f`:
    // `scope` itself is omitted when NULL (below), so the corpus carries the same column under
    // both conventions and the field-to-field probe has two explicit nulls to compare.
    coOwner: derivedFor(seed).scope,
    obj: { inner: seed.aString },
    tags: seed.tags.map(asTagAttribute),
    tagNames: seed.tags.map((tag) => tag.name),
    // Verbatim: the stored JSON column holds the same list, null elements included.
    aNumberList: seed.aNumberList,
    aBoolList: seed.aBoolList,
    categories: seed.subCategoryNames.map((subName) => ({
      name: "business",
      subCategories: [
        {
          name: subName,
          labels: derivedFor(seed).labels.map(asLabelAttribute),
        },
      ],
    })),
  };
  // A DB NULL is a missing attribute on the check side — conditions touching it must deny
  // (CEL error), matching SQL three-valued logic excluding the row.
  if (seed.aOptionalString !== null) {
    attr["aOptionalString"] = seed.aOptionalString;
  }
  const aDouble = derivedFor(seed).aDouble;
  if (aDouble !== null) {
    attr["aDouble"] = aDouble;
  }
  const scope = derivedFor(seed).scope;
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
  const updatedAt = derivedFor(seed).updatedAt;
  if (updatedAt !== null) {
    attr["updatedAt"] = updatedAt;
  }
  const createdAt = derivedFor(seed).createdAt;
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

/** The liveness probes' assertion: a refused action's oracle is non-empty and non-total. */
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
    "the MySQL leg runs under a byte-exact collation",
    async () => {
      expect({
        // "one" is seed a1; "One" is seed c1, and "o\u00ADne" is seed h6, which a case-sensitive
        // UCA collation (utf8mb4_0900_as_cs) still returns because it weighs the soft hyphen as
        // nothing. Bound, not interpolated, so the comparison is the one the adapter's own
        // filters make.
        caseVariant: await store.selectIds(sql`a_string = ${"one"}`),
        // "héllo🚀" is seed a6; the accent-folded spelling matches nothing.
        accentFolded: await store.selectIds(sql`a_string = ${"hello🚀"}`),
        // `hier-*` reaches the same collation through LIKE rather than through `=`.
        wrongCasePrefix: await store.selectIds(sql`a_string like ${"ON%"}`),
        // NO PAD: utf8mb4_bin would return a1 for a trailing space.
        trailingSpace: await store.selectIds(sql`a_string = ${"one "}`),
      }).toEqual({
        caseVariant: ["a1"],
        accentFolded: [],
        wrongCasePrefix: [],
        trailingSpace: [],
      });
    },
  );

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

    expect(MANIFEST_ACTIONS.size).toBe(330);
    expect(NULL_REPRESENTATION_OMITTED).toHaveLength(1);
    // Deliberate tripwire: every one of these carries a pinned message, so a throwing action
    // gained or lost has to be re-triaged here rather than joining the suite unnoticed.
    expect(THROWING_ACTIONS).toHaveLength(63);
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
    // The degeneracy guard, over the oracle this comparison already computed: an empty or total
    // oracle makes the assertion below unfalsifiable unless the corpus declares it.
    expectOracleShape(action, oracle);
    expect(filtered).toEqual(oracle);
  });

  test.each([
    "index-scalar-list",
    "index-scalar-list-not-eq",
    "index-scalar-list-null",
    "index-not-oob",
    // Number and boolean elements (conformance/README.md, "Number and boolean list elements"),
    // on every representation: jsonb, plain json and a zero-based integer[] / boolean[] on
    // PostgreSQL. The two cross-type probes are the over-grant witnesses for a comparison that
    // drops an element's JSON type.
    "index-number-list",
    "index-number-list-not-eq",
    "index-bool-list",
    "index-bool-list-not-eq",
    "index-bool-list-vs-number",
    "index-number-list-vs-bool",
    // Membership in the same lists, searched element by element through the same declared
    // storage, so each representation has to keep the element's JSON type there too.
    "in-number-list",
    "in-number-list-vs-string",
    "in-bool-list-vs-string",
    "hasint-number-list-vs-string",
    "hasint-bool-list-vs-string",
    // A null ELEMENT is a value: MySQL's JSON_TABLE reads it back as SQL NULL, so the membership
    // test matches it by IS NULL, and `null in []` stays a definite false under the negation.
    "null-in-number-list",
    "not-null-in-number-list",
  ])("declared indexed storage: %s matches the oracle for every representation", async (action) => {
    const oracle = await oracleAllowedIds(action);
    expectOracleShape(action, oracle);
    for (const mapper of [MAPPER, ...(store.indexMappers ?? [])]) {
      expect(await adapterFilteredIds(action, "explicit", mapper)).toEqual(oracle);
    }
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
  // cannot evaluate a non-boolean conjunction — so it is declared in `degenerateOracles` and sits
  // outside the liveness probes, and a bare "it throws" would say nothing about whether refusing
  // it is REQUIRED.
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
  // Indexed null ELEMENTS are values, not missing attributes; their exception is oracle-proved.
  const INDEXED_NULL_ELEMENT_ACTIONS = new Set([
    "index-scalar-list-null",
    "null-in-number-list",
    "not-null-in-number-list",
  ]);
  test("null literals under omitted are rejected unless they compare an indexed element", async () => {
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
    expect(nullCarrying).toContain("index-scalar-list-null");

    // Guard the exemption too: each one must still reach the loop, or it exempts nothing.
    expect(nullCarrying).toEqual(
      expect.arrayContaining([...INDEXED_NULL_ELEMENT_ACTIONS]),
    );

    const notRejected: string[] = [];
    for (const action of nullCarrying) {
      if (INDEXED_NULL_ELEMENT_ACTIONS.has(action)) {
        expect(await adapterFilteredIds(action, "omitted", MAPPER_WITHOUT_NULL_CONVENTIONS))
          .toEqual(await oracleAllowedIds(action));
        continue;
      }
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

  // Shapes this adapter refuses, so there is no comparison behind them: these carry PDP/policy
  // liveness for their group only. Asserting the complement keeps the split honest — an action the
  // adapter gains support for leaves this list, since the sweep over every compared action already
  // guards it.
  test("liveness probes have a non-degenerate oracle", async () => {
    for (const action of DEGENERACY_LIVENESS_PROBES) {
      expect(ORACLE_ACTIONS).not.toContain(action);
      await expectNonDegenerateOracle(action);
    }
  }, 60_000);

  // Every action the corpus declares degenerate by construction, whether this adapter compares it
  // or refuses it, keeps exactly the oracle it is declared with: type errors, unequal runtime
  // types, or empty-list identities. The sweep in "matches the check() oracle" only reaches the
  // compared ones; this is what keeps a declaration honest for the rest. Where
  // DEGENERATE_PLANNER_KINDS pins one, the live planner kind is asserted too.
  test.each([...DEGENERATE_ORACLES])(
    "degenerateOracles: %s has exactly its declared %s oracle",
    async (action, declared) => {
      expect(MANIFEST_ACTIONS.has(action)).toBe(true);
      const kind = DEGENERATE_PLANNER_KINDS[action];
      const [plan, ids] = await Promise.all([
        kind === undefined
          ? undefined
          : cerbos.planResources({
              principal: seedsFile.principal,
              resource: { kind: seedsFile.resourceKind },
              action,
            }),
        oracleAllowedIds(action),
      ]);
      if (kind !== undefined) {
        expect(plan?.kind).toBe(kind);
      }
      expect(ids).toEqual(declared === "empty" ? [] : ALL_SEED_IDS);
    },
  );

  test("every pinned planner kind belongs to a degenerateOracles entry", () => {
    expect(
      Object.keys(DEGENERATE_PLANNER_KINDS).filter(
        (action) => !DEGENERATE_ORACLES.has(action),
      ),
    ).toEqual([]);
  });
});
