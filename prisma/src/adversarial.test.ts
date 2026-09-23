import * as fs from "fs";
import * as path from "path";

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

import { queryPlanToPrisma, PlanKind, MapperConfig } from ".";
import {
  CONFORMANCE_DIR,
  MAPPER,
  MODEL,
  classifyActionsForAdapter,
  degenerateOraclesOf,
  requireMessage,
  assertPinnedPdp,
  pdpAddress,
} from "./corpus";
import type {
  ActionClassification,
  ActionsFile,
  ThrowingAction,
} from "./corpus";
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
 * oracle recipe (NULL-as-missing-attribute, the degeneracy guard) — this file only owns seeding
 * and executing the query. The mapper and the per-adapter classification live in `./corpus`,
 * shared with `translator.test.ts`, which pins the filter this suite then proves returns the
 * right rows.
 *
 * The whole corpus is replayed against every store this adapter is proved on, one store per run,
 * selected with `ADAPTER_TEST_DB` (`sqlite` by default, `postgres` and `mysql` for the
 * container-backed legs — see `jest.globalSetup.adversarial.js`). The Prisma major and the store
 * are independent dimensions: v6/v7 is an ENGINE matrix, and an engine matrix says nothing about
 * how a provider coerces a fractional threshold against an `Int` column, escapes a LIKE
 * metacharacter, compares a real `timestamp` rather than milliseconds since the epoch
 * (cerbos/query-plan-adapters#320), or decides with its COLLATION whether `=` is case-sensitive at
 * all (#340).
 */

const cerbos = new Cerbos(pdpAddress(), { tls: false });

const SCHEMA_DIR = path.join(__dirname, "..", "prisma");

const STORE_NAMES = ["sqlite", "postgres", "mysql"] as const;
type StoreName = (typeof STORE_NAMES)[number];

/**
 * How each leg asks its own connection which engine answered, and the first word of the answer.
 *
 * Each spelling is a syntax error on the other two engines, so the anti-vacuity test below fails
 * in every direction rather than only when a container is missing. MySQL's `version()` answers a
 * bare `8.4.x`, which would make the assertion a version check; `@@version_comment` names the
 * engine.
 */
const STORE_BANNERS: Record<StoreName, { query: string; engine: string }> = {
  sqlite: { query: "select 'SQLite ' || sqlite_version() as banner", engine: "SQLite" },
  postgres: { query: "select version() as banner", engine: "PostgreSQL" },
  mysql: { query: "select @@version_comment as banner", engine: "MySQL" },
};

function selectedStoreName(): StoreName {
  const requested = process.env["ADAPTER_TEST_DB"] ?? "sqlite";
  // jest.adversarial.config.js rejects an unknown value before jest resolves this module; the
  // repeat here is what keeps the assertion below reading against a value this file trusts.
  if (!STORE_NAMES.includes(requested as StoreName)) {
    throw new Error(
      `Unknown ADAPTER_TEST_DB "${requested}": expected one of ${STORE_NAMES.join(", ")}`
    );
  }
  return requested as StoreName;
}

const STORE_NAME = selectedStoreName();

/**
 * The six adversarial schemas, which must hold one data model between them.
 *
 * A generated Prisma client bakes in its provider and its major, so proving the corpus on
 * (Prisma 6, Prisma 7) x (SQLite, PostgreSQL, MySQL) needs six schema files. They are only a
 * matrix over the same models — a column that drifts in one of them would seed a different row
 * shape on that leg while every assertion in this file stayed identical, which is the projection
 * trap conformance/README.md describes applied to the schema instead of the seeds.
 *
 * It is `STORE_NAMES.length * 2`, and asserted to be: adding a store means adding two schemas, and
 * a store whose schemas were never written would otherwise arrive as a resolution failure inside
 * one leg rather than as a gap in the matrix.
 */
const ADVERSARIAL_SCHEMAS = [
  "schema.adversarial.prisma",
  "schema.adversarial.v6.prisma",
  "schema.adversarial.pg.prisma",
  "schema.adversarial.pg.v6.prisma",
  "schema.adversarial.mysql.prisma",
  "schema.adversarial.mysql.v6.prisma",
] as const;

/**
 * A schema's `model` blocks, with comments, the generator/datasource blocks and all incidental
 * whitespace removed — everything that legitimately differs between the six.
 */
function modelBlocks(schema: string): string {
  const withoutComments = schema
    .split("\n")
    .filter((line) => !line.trimStart().startsWith("//"))
    .join("\n");
  const models = withoutComments.match(/^model\s[\s\S]*$/m) ?? [];
  return models
    .join("\n")
    .replace(/[ \t]+/g, " ")
    .replace(/\n\s*\n/g, "\n")
    .trim();
}

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
  /** One AdversarialNumberListElement row per element, null elements included. */
  aNumberList: (number | null)[];
  /** One AdversarialBoolListElement row per element, null elements included. */
  aBoolList: (boolean | null)[];
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
  "aNumberList",
  "aBoolList",
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

/** Principal attributes have explicit value shapes, including absent versus null struct members. */
function assertPrincipalAttrShape(label: string, value: unknown): void {
  const key = label.slice(label.lastIndexOf(".") + 1);
  if (key === "context" && typeof value === "string") return;
  if (key === "zero" && value === 0) return;
  if (["allowedTags", "fewTeams", "manyTeams", "emptyTeams"].includes(key)
      && Array.isArray(value) && value.every((entry) => typeof entry === "string")) return;
  if (["manyStructs", "nullableStructs", "missingStructs"].includes(key)
      && Array.isArray(value) && value.every((entry: unknown) => {
        if (typeof entry !== "object" || entry === null || Array.isArray(entry)) return false;
        if (key === "missingStructs") return Object.keys(entry).length === 0;
        return Object.keys(entry).length === 1 && "name" in entry
          && (key === "nullableStructs" ? entry.name === null : typeof entry.name === "string");
      })) return;
  throw new Error(`${label} does not match its declared corpus principal shape`);
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

// seedsFile.principal is the parsed JSON object, handed to the SDK untouched, so Object.keys
// reports the corpus key set on both levels.
assertKeys(
  "seeds.json principal",
  Object.keys(seedsFile.principal),
  PRINCIPAL_KEYS
);
// `attr` is optional on the SDK's Principal type; the corpus always carries it, and the assertion
// above is what proves it rather than this fallback.
const PRINCIPAL_ATTR = seedsFile.principal.attr ?? {};
assertKeys(
  "seeds.json principal.attr",
  Object.keys(PRINCIPAL_ATTR),
  PRINCIPAL_ATTR_KEYS
);
for (const [key, value] of Object.entries(PRINCIPAL_ATTR)) {
  assertPrincipalAttrShape(`seeds.json principal.attr.${key}`, value);
}

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
// Actions whose `== null` probe targets an attribute the oracle OMITS for NULL columns. They
// carry no oracle comparison: under the omitted representation check() denies every row, so the
// adapter must reject the shape rather than emit a filter (#302).
const NULL_REPRESENTATION_OMITTED = actionsFile.nullRepresentationOmitted.map(
  (entry): ThrowingAction => [
    entry.action,
    entry.reason,
    requireMessage(
      `nullRepresentationOmitted.${entry.action}.messages.prisma`,
      entry.messages?.["prisma"]
    ),
  ]
);
/** The one message every null-carrying action must be rejected with under `omitted`. */
const NULL_OMITTED_MESSAGE = NULL_REPRESENTATION_OMITTED[0]?.[2] ?? "";
const MANIFEST_ACTIONS = new Set([
  ...actionsFile.conformance,
  ...EXPECTED_UNSUPPORTED_ACTIONS,
  ...NULL_REPRESENTATION_OMITTED.map(([action]) => action),
  // ALL divergences, not just Prisma's: a divergence registered solely for another adapter
  // must still enter this manifest, so the size tripwire and the classified-exactly-once
  // check flag it for triage here instead of letting the action silently vanish from this
  // harness. Classification/skipping still uses the Prisma-filtered set.
  ...(actionsFile.knownDivergences ?? []).map((entry) => entry.action),
]);

// -- the degeneracy guard (conformance/README.md, "The degeneracy guard") -----------------------
//
// A differential cannot fail when the oracle is empty or total: an adapter returning no rows, or
// every row, agrees with it. So every action this adapter ORACLE-COMPARES has its oracle's shape
// asserted before the comparison, reusing the oracle that comparison computes. An action listed in
// `degenerateOracles` in conformance/actions.json — the corpus-level allowlist of actions whose
// oracle is empty or total BY CONSTRUCTION, shared by every harness — must have exactly that
// oracle; every other compared action must have a non-empty, non-total one.

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
        "total oracle must be declared there by construction, and a declared one must stay exactly that."
    );
  }
}

/**
 * Shapes Prisma refuses to translate: they have no oracle comparison for the sweep above to guard,
 * and stay here as PDP/policy liveness probes for a group the compared actions cannot cover. See
 * cerbos/query-plan-adapters#324.
 */
const DEGENERACY_LIVENESS_PROBES = [
  "not-nan-order-string",
  // size() is lowered only over a named relation, so the string emptiness check throws; an
  // empty hierarchy delimiter is refused before the prefix filter is built; and a regex with a
  // top-level alternation is a matches(), which this adapter never translates.
  "string-size-gt0",
  "hier-empty-delim",
  "matches-alt",
  // Prisma emits LIKE with no ESCAPE clause, so a % needle throws — and so does a backslash one,
  // which is the default escape character on PostgreSQL and MySQL and literal on SQLite
  // (cerbos/query-plan-adapters#320).
  "like-percent",
  "like-backslash",
  // every() cannot require the intermediate hop of a chain, and no relation count with an
  // arbitrary threshold — >= 0 or a rounded fractional one — has a none/some spelling.
  "w1-all-chain",
  "w1-size-nonneg-chain",
  "w1-size-frac-le-chain",
  // Prisma filters have no column arithmetic, so the whole cr-div group (#311) throws.
  "cr-div-neg-zero",
  // int() over a numeric column: truncation-versus-rounding, unsupported for every adapter but
  // convex, which promotes it in adapterSupportedExpected.
  "cast-int-double",
  // string() has no Prisma filter form (#376). cast-string-bool carries the group's probe rather
  // than cast-string-double because its oracle is 14 of 22 rather than a single row, so a PDP or
  // policy that went quiet fails the non-total half of the assertion too.
  "cast-string-bool",
  // Concatenation against the key where BOTH operands carry a column — the arithmetic solver
  // needs a value on the other side, so the id-* group's one throwing shape probes here.
  "id-concat",
  // The same missing form with BOTH operands columns, so there is no constant to invert off
  // either side (#391).
  "concat-f2f",
  // #387, one probe per group Prisma cannot compare. The negated LIKE is refused for its COLUMN
  // needle before the negation matters, so the whole not-* group throws; `mod`, a positional read
  // of a scalar list, and list equality over a projection have no Prisma filter form at all.
  "not-contains",
  "arith-mod",
  "index-scalar-list",
  "index-scalar-list-not-eq",
  "index-scalar-list-null",
  "map-eq-list",
  // Issue #414: each new non-degenerate shape guards its classified side.
  "regex-digit",
  "regex-case",
  "regex-posix",
  "regex-unanchored",
  "regex-dot",
  "regex-alternation",
  "regex-grouped",
  "regex-brace",
  "regex-repetition",
  "regex-optional-operators",
  "except-size",
  "except-eq",
  "pv-structs",
  "pv-exists-one",
  "pv-filter",
  "pv-map",
  "pv-except",
  "in-var-var-omitted",
  "in-var-var-omitted-neg",
  "div-by-division",
  "temporal-raw-eq",
  "eq-list",
  "ne-list",
  // #396: live error branches remain discriminating under negation or disjunction.
  "cast-not-double",
  "cast-not-int",
  "cast-not-string-missing",
  "cast-not-string-null",
  "cast-not-timestamp",
  "index-fractional",
  "index-negative",
  "index-not-oob",
  "regex-eq-true",
  "regex-final-newline",
  "regex-lookahead",
  // A positional read of a number or boolean list: refused at the same `index` node as
  // index-scalar-list, before the element type or the literal's type is ever looked at.
  "index-number-list",
  "index-number-list-not-eq",
  "index-bool-list",
  "index-bool-list-not-eq",
  "index-bool-list-vs-number",
  "index-number-list-vs-bool",
  // Membership in a number or boolean list with a literal of the other type. CEL's heterogeneous
  // equality answers `"2" == 2` false; the mapped valueType refuses the literal before a filter
  // exists rather than handing it to a store that might coerce it (SQLite and MySQL store a
  // boolean as the integer 1). The mixed intersection list is refused whole, although its 3 is
  // well typed.
  "in-number-list-vs-string",
  "in-bool-list-vs-string",
  "hasint-number-list-vs-string",
  "hasint-bool-list-vs-string",
  // The same refusal on a scalar column or a relation field: the literal's type is checked against
  // the mapped valueType, and a mismatch is refused before a filter exists rather than bound for
  // the store to coerce (SQLite and MySQL compare '5' = 5 as true; MySQL reads a non-numeric
  // string as 0). Each probe's `or` refuses its well-typed aNumber == 5 branch with it.
  "eq-number-vs-string",
  "ne-number-vs-string",
  "eq-bool-vs-string",
  "eq-string-vs-number",
  "in-scalar-number-vs-string",
  "exists-tag-name-vs-number",
  "hasint-map-vs-number",
  // #509: a macro nested over the same relation whose body reads the enclosing element. The
  // negated form is the one a self-comparison would over-grant, so it carries the group's probe.
  "nest-same-not-exists",
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

function doubleFor(seed: Seed): number | null {
  return derivedFor(seed).aDouble;
}

/** Third-level label names. A null element is a NULL label name — a missing element attribute. */
function labelsFor(seed: Seed): (string | null)[] {
  return derivedFor(seed).labels;
}

/** Deterministic ISO instant per seed for the timestamp probe: split around 2025-01-01. */
function isoFor(seed: Seed): string {
  return derivedFor(seed).createdBy;
}

function timestampFor(seed: Seed): string | null {
  return derivedFor(seed).createdAt;
}

function scopeFor(seed: Seed): string | null {
  return derivedFor(seed).scope;
}

// -- the real to-one relation (conformance/README.md, "The real to-one relation") ----------------
//
// `parentSeedId` names the seed whose four scalars this row's `parent` carries, and that seed's
// own `parentSeedId` names the ones `parent.inner` carries. The chain is cut at two levels. Every
// resource owns a FRESH parent (and inner) row rather than pointing at the named seed's own row,
// so no two resources share one and a filter that returned the parent instead of the child cannot
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
      `seeds.json: "${seed?.id}" names parent "${id}", which is not a seed id`
    );
  }
  return parent;
}

/** The four scalars one level of the chain stores, as columns. */
function relationColumns(seed: Seed): {
  aBool: boolean;
  aString: string;
  aNumber: number;
  aOptionalString: string | null;
} {
  return {
    aBool: seed.aBool,
    aString: seed.aString,
    aNumber: seed.aNumber,
    aOptionalString: seed.aOptionalString,
  };
}

/** The same four as check() attributes: a NULL column is a MISSING attribute, one hop out. */
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
function withoutNullConventions(
  mapper: Record<string, MapperConfig>
): Record<string, MapperConfig> {
  return Object.fromEntries(
    Object.entries(mapper).map(([reference, config]) => {
      const { nullAttributeRepresentation: _stripped, ...rest } = config;
      return [reference, rest];
    })
  );
}

const MAPPER_WITHOUT_NULL_CONVENTIONS = withoutNullConventions(MAPPER);

beforeAll(async () => {
  await assertPinnedPdp(cerbos);
  // CEL string matching is case-sensitive, and this adapter lowers contains/startsWith/endsWith
  // to LIKE. On SQLite, LIKE is case-INSENSITIVE for ASCII no matter what collation the column
  // was created with — only this pragma changes it — so without it every string predicate
  // over-grants by exactly the case-variant rows (the `cs-contains` group; proved by c1, "One").
  // The column collation the README talks about governs `=`, not LIKE, which is why cs-eq passed
  // here for a long time while the string operators did not. ent and sqlalchemy set the same
  // pragma; drizzle needs none because it lowers to REPLACE rather than LIKE.
  if (STORE_NAME === "sqlite") {
    await prisma.$executeRawUnsafe("PRAGMA case_sensitive_like = ON");
  }
  await prisma.adversarialInner.deleteMany();
  await prisma.adversarialParent.deleteMany();
  await prisma.adversarialLabel.deleteMany();
  await prisma.adversarialSubCategory.deleteMany();
  await prisma.adversarialCategory.deleteMany();
  await prisma.adversarialTag.deleteMany();
  await prisma.adversarialNumberListElement.deleteMany();
  await prisma.adversarialBoolListElement.deleteMany();
  await prisma.adversarialResource.deleteMany();

  // Distinct sub-category/category graphs per seed so no rows share relations by accident.
  for (const seed of SEEDS) {
    const parentSeed = parentSeedOf(seed);
    const innerSeed = parentSeedOf(parentSeed);
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
        updatedAt: derivedFor(seed).updatedAt,
        // One row per element, null elements included: membership is all a filter asks of
        // either list, so the element's position is not stored.
        numberList: {
          create: seed.aNumberList.map((value) => ({ value })),
        },
        boolList: {
          create: seed.aBoolList.map((value) => ({ value })),
        },
        tags: {
          create: seed.tags.map((t) => ({ tagId: t.id, name: t.name })),
        },
        // The to-one chain, one owned row per level. A seed with no parent gets no row at all,
        // which is what makes the absent-parent hazard reachable through a SCALAR rather than
        // only through mainCategory's collection.
        ...(parentSeed === undefined
          ? {}
          : {
              parent: {
                create: {
                  ...relationColumns(parentSeed),
                  ...(innerSeed === undefined
                    ? {}
                    : { inner: { create: relationColumns(innerSeed) } }),
                },
              },
            }),
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
    // The explicit-null alias of the `scope` column, the second half of `null-value-f2f`:
    // `scope` itself is omitted when NULL (below), so the corpus carries the same column under
    // both conventions and the field-to-field probe has two explicit nulls to compare.
    coOwner: scopeFor(seed),
    tagNames: seed.tags.map((tag) => tag.name),
    // Verbatim, null elements included: a null element is a VALUE in CEL, not a missing attribute,
    // which is what index-number-list-not-eq's a6 and index-bool-list-not-eq's a4 witness. The
    // store holds the same elements as child rows (numberList, boolList); only membership reads
    // them, since every positional read is refused before the list is resolved.
    aNumberList: seed.aNumberList,
    aBoolList: seed.aBoolList,
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
  const updatedAt = derivedFor(seed).updatedAt;
  if (updatedAt !== null) {
    attr["updatedAt"] = updatedAt;
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

// -- adapter execution through the public queryPlanToPrisma path --

async function adapterFilteredIds(
  action: string,
  nullAttributeRepresentation: "explicit" | "omitted" = "explicit",
  mapper: Record<string, MapperConfig> = MAPPER
): Promise<string[]> {
  const queryPlan = await cerbos.planResources({
    principal: principal(),
    resource: { kind: seedsFile.resourceKind },
    action,
  });
  const result = queryPlanToPrisma({
    queryPlan,
    mapper,
    model: MODEL,
    nullAttributeRepresentation,
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

/**
 * The ids a hand-built CONDITIONAL plan selects, for the guards that synthesise shapes the corpus
 * does not spell. Every such shape translates, so the result must be conditional.
 */
async function syntheticFilteredIds(
  condition: PlanExpressionOperand
): Promise<string[]> {
  const result = queryPlanToPrisma({
    queryPlan: {
      kind: PlanKind.CONDITIONAL,
      condition,
      cerbosCallId: "synthetic",
      requestId: "synthetic",
      validationErrors: [],
      metadata: undefined,
    },
    mapper: MAPPER,
    model: MODEL,
  });
  expect(result.kind).toBe(PlanKind.CONDITIONAL);
  const where = result.kind === PlanKind.CONDITIONAL ? result.filters : {};
  const rows = await prisma.adversarialResource.findMany({
    where,
    select: { id: true },
  });
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

describe(`adversarial conformance corpus (${STORE_NAME})`, () => {
  // Anti-vacuity for the store split: every other assertion in this file is identical on every
  // leg, so a PostgreSQL leg that silently fell back to SQLite would pass the entire suite while
  // proving nothing about PostgreSQL — the exact gap #320 reports. Ask the connection the suite
  // actually queries through which engine it is; each spelling is a syntax error on the others, so
  // this fails in every direction rather than only when a container is missing.
  test("executes against the store ADAPTER_TEST_DB selects", async () => {
    const rows = await prisma.$queryRawUnsafe<{ banner: string }[]>(
      STORE_BANNERS[STORE_NAME].query
    );
    expect(rows[0]?.banner?.split(" ")[0]).toBe(STORE_BANNERS[STORE_NAME].engine);
  });

  // The MySQL leg's other precondition, and the one no banner reports: the collation the tables
  // were converted to after `prisma db push` wrote them (jest.globalSetup.adversarial.js). Under
  // the `utf8mb4_unicode_ci` Prisma's migration engine hardcodes — or MySQL's own
  // `utf8mb4_0900_ai_ci` default — these three queries return the case variant, the accent variant
  // and the wrong-case prefix respectively, which is a live authorization over-grant: `cs-eq`,
  // `unicode-eq` and every `hier-*` action would then return rows the PDP denies. The corpus
  // catches that through the oracle; this catches it by NAME, so a failure reads as "the store is
  // misconfigured" rather than as a translation bug.
  //
  // Executed against the seeded rows rather than asked of `@@collation_database`, because the
  // requirement is what the comparison DOES, not what the setting is called.
  (STORE_NAME === "mysql" ? test : test.skip)(
    "the MySQL leg runs under a byte-exact collation",
    async () => {
      const ids = async (where: object): Promise<string[]> =>
        (
          await prisma.adversarialResource.findMany({
            where,
            select: { id: true },
          })
        )
          .map((row) => row.id)
          .sort();

      expect({
        // "one" is seed a1; "One" is seed c1, and "o\u00ADne" is seed h6, which a case-sensitive
        // UCA collation (utf8mb4_0900_as_cs) still returns because it weighs the soft hyphen as
        // nothing (#474).
        caseVariant: await ids({ aString: "one" }),
        // "héllo🚀" is seed a6; the accent-folded spelling matches nothing.
        accentFolded: await ids({ aString: "hello🚀" }),
        // `hier-*` reaches the same collation through LIKE rather than through `=`.
        wrongCasePrefix: await ids({ aString: { startsWith: "ON" } }),
        // NO PAD: utf8mb4_bin would return a1 for a trailing space.
        trailingSpace: await ids({ aString: "one " }),
      }).toEqual({
        caseVariant: ["a1"],
        accentFolded: [],
        wrongCasePrefix: [],
        trailingSpace: [],
      });
    }
  );

  // The SQLite leg's precondition, by name: `PRAGMA case_sensitive_like` (beforeAll) holds per
  // CONNECTION, so it must reach every connection a query can land on. Prisma 6 pooled SQLite
  // connections until its schema pinned `connection_limit=1`, and the pragma then governed one of
  // them; concurrent queries are what spread onto the rest, so the same case-variant LIKE is issued
  // many times at once. Under the pool most of these returned c1 ("One").
  (STORE_NAME === "sqlite" ? test : test.skip)(
    "the SQLite leg's LIKE is case-sensitive on every connection",
    async () => {
      const results = await Promise.all(
        Array.from({ length: 32 }, () =>
          prisma.adversarialResource.findMany({
            where: { aString: { contains: "one" } },
            select: { id: true },
          })
        )
      );
      // Guard the guard: the needle has to match something, or no row could be the case variant.
      expect(results[0]?.length).toBeGreaterThan(0);
      expect(
        results.filter((rows) => rows.some((row) => row.id === "c1")).length
      ).toBe(0);
    }
  );

  test("every adversarial schema declares the same data model", () => {
    const [reference, ...rest] = ADVERSARIAL_SCHEMAS.map((name) => ({
      name,
      models: modelBlocks(
        fs.readFileSync(path.join(SCHEMA_DIR, name), "utf8")
      ),
    }));
    if (!reference) {
      throw new Error("ADVERSARIAL_SCHEMAS is empty");
    }
    // Guard the guard: a regex that stopped matching would make every schema compare equal on an
    // empty string, and a store added without its two schemas would leave the matrix short.
    expect(reference.models).toContain(`model ${MODEL}`);
    expect(ADVERSARIAL_SCHEMAS).toHaveLength(STORE_NAMES.length * 2);
    for (const schema of rest) {
      expect({ name: schema.name, models: schema.models }).toEqual({
        name: schema.name,
        models: reference.models,
      });
    }
  });

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
            messages: { prisma: "unsupported" },
          },
        ],
        nullRepresentationOmitted: [],
        degenerateOracles: [],
      },
      "prisma"
    );

    expect(classification.oracleActions).toContain(promotedAction);
    expect(
      classification.throwingActions.map(([action]) => action)
    ).not.toContain(promotedAction);
  });

  // Adding a throwing action without pinning its message must fail this harness rather than
  // silently degrade the throw suite to a bare "it threw" (cerbos/query-plan-adapters#326).
  test("a throwing action with no pinned message fails classification", () => {
    const unpinnedAction = "unpinned-shape";
    const classify = (): ActionClassification =>
      classifyActionsForAdapter(
        {
          conformance: [unpinnedAction],
          adapterUnsupported: {
            prisma: [{ action: unpinnedAction, reason: "synthetic limitation" }],
          },
          expectedUnsupported: [],
          nullRepresentationOmitted: [],
          degenerateOracles: [],
        },
        "prisma"
      );

    expect(classify).toThrow(/pins no throw message/);
  });

  // Every action the corpus declares degenerate by construction, whether Prisma compares it or
  // refuses it, keeps exactly the oracle it is declared with. The sweep in "matches the check()
  // oracle" only reaches the compared ones; this is what keeps a declaration honest for the rest.
  // One case per entry, each with its own test budget, since every entry costs a check() per seed.
  test.each([...DEGENERATE_ORACLES])(
    "degenerateOracles: %s has exactly its declared %s oracle",
    async (action, declared) => {
      expect(MANIFEST_ACTIONS.has(action)).toBe(true);
      expect(await oracleAllowedIds(action)).toEqual(
        declared === "empty" ? [] : ALL_SEED_IDS
      );
    }
  );

  test("manifest assigns all 327 policy actions exactly one Prisma outcome", () => {
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
        PRISMA_KNOWN_DIVERGENCES.has(action),
      ].filter(Boolean).length;
      return classificationCount !== 1;
    });

    expect(MANIFEST_ACTIONS.size).toBe(327);
    // Deliberate tripwire: every one of these carries a pinned message, so a throwing action
    // gained or lost has to be re-triaged here rather than joining the suite unnoticed.
    expect(THROWING_ACTIONS).toHaveLength(141);
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
      // The degeneracy guard, over the oracle this comparison already computed: an empty or total
      // oracle makes the assertion below unfalsifiable unless the corpus declares it.
      expectOracleShape(action, oracle);
      expect(filtered).toEqual(oracle);
    }
  );

  // Shapes the adapter does not support (globally unsupported planner shapes plus Prisma's
  // declared adapterUnsupported list): translation must fail loudly, never produce a
  // silently-wrong filter. The plan is fetched OUTSIDE the assertion so a PDP failure fails
  // the test instead of passing it, and no query executes — the invariant is that the shape
  // throws BEFORE a filter exists, so SQLite rejecting a wrongly emitted filter afterwards
  // must not be able to masquerade as the adapter refusing to translate.
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
        queryPlanToPrisma({
          queryPlan,
          mapper: MAPPER,
          model: MODEL,
          nullAttributeRepresentation: "explicit",
        })
      ).toThrow(message);
    }
  );

  // #387. `filter-as-conjunct` puts a filter() one level below the root, where the guard that
  // refuses `filter-as-condition` does not look. Its oracle is empty BY CONSTRUCTION — check()
  // cannot evaluate a non-boolean conjunction — so it is declared in `degenerateOracles` and sits
  // outside the liveness probes, and a bare "it throws" would say nothing about whether refusing it
  // is REQUIRED.
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
      ([action]) => action === "filter-as-conjunct"
    )?.[2];
    expect(message).toBeDefined();
    await expect(adapterFilteredIds("filter-as-conjunct")).rejects.toThrow(
      message
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
        message
      );
    }
  );

  // #308. The per-attribute declaration overrides the call-level option, which is the property
  // that makes a suite mixing both conventions expressible at all. Asserted in both directions
  // against the SAME action and the SAME call-level option, varying only whether the mapper
  // declares the convention — so a declaration that did nothing would show up here as the two
  // runs agreeing.
  test("a per-attribute declaration overrides the call-level representation", async () => {
    // `owner` declares "explicit", so the call-level "omitted" does not reach it.
    await expect(adapterFilteredIds("null-eq", "omitted")).resolves.toEqual(
      await oracleAllowedIds("null-eq")
    );

    // Strip the declaration and the same action under the same option is rejected — so the
    // stripped mapper the completeness guard below uses is not quietly equivalent to MAPPER.
    await expect(
      adapterFilteredIds("null-eq", "omitted", MAPPER_WITHOUT_NULL_CONVENTIONS)
    ).rejects.toThrow(NULL_OMITTED_MESSAGE);
  });

  // #302 completeness guard. The rejection must key off the null OPERAND, not off a list of
  // operators: `hasIntersection(tagNames, ["public", null])` carries one in its value list, and
  // an allowlist of eq/ne/in silently misses it. Enumerating the corpus rather than naming
  // shapes means a newly added action carrying a null constant is covered automatically.
  test("null field comparisons are rejected under omitted; indexed elements retain their refusal", async () => {
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
      // This null compares a list element, not an optionally absent field. The index operator
      // has no Prisma filter form under either representation, so its own refusal applies.
      if (action === "index-scalar-list-null") {
        await expect(adapterFilteredIds(action, "omitted")).rejects.toThrow(
          "Unsupported operator: index",
        );
        continue;
      }
      try {
        await adapterFilteredIds(
          action,
          "omitted",
          MAPPER_WITHOUT_NULL_CONVENTIONS
        );
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

  // The corpus pins two count spellings over the chain — `size(...) == 0` and
  // `!(size(...) > 0)` — and CEL's type checker rules out a third that a real policy could
  // reach through the planner (`>= 1` and `<= 0` are the only other thresholds this adapter
  // can express, and no policy needs both spellings). The guard must nonetheless be a
  // property of the chain rather than of the two spellings that happen to be pinned, so
  // these synthesise the remaining threshold/polarity combinations directly onto the same
  // seeded store and assert the parentless rows stay out of every one
  // (cerbos/query-plan-adapters#316).
  test("every count threshold over the chain inherits the absent-parent guard", async () => {
    const chain = new PlanExpressionVariable(
      "request.resource.attr.mainCategory.subCategories"
    );
    const size = new PlanExpression("size", [chain]);
    const compare = (operator: string, threshold: number) =>
      new PlanExpression(operator, [size, new PlanExpressionValue(threshold)]);
    const negate = (condition: PlanExpressionOperand) =>
      new PlanExpression("not", [condition]);

    // Each of these is TRUE for a row with no mainCategory only if the guard leaks: an
    // absent to-one parent is a CEL missing-path error, so the PDP denies it outright.
    const emptyByConstruction: [string, PlanExpressionOperand][] = [
      ["size(chain) == 0", compare("eq", 0)],
      ["size(chain) <= 0", compare("le", 0)],
      ["size(chain) < 1", compare("lt", 1)],
      ["!(size(chain) >= 1)", negate(compare("ge", 1))],
      ["!(size(chain) > 0)", negate(compare("gt", 0))],
    ];

    for (const [shape, condition] of emptyByConstruction) {
      expect([shape, await syntheticFilteredIds(condition)]).toEqual([shape, []]);
    }

    // The mirror image, so the loop above cannot pass by denying everything: the negation of
    // an emptiness check is TRUE for exactly the rows that HAVE the parent.
    const withParent = await oracleAllowedIds("w1-size-nonneg-chain");
    expect(withParent.length).toBeGreaterThan(0);
    expect(withParent.length).toBeLessThan(SEEDS.length);
    expect(await syntheticFilteredIds(negate(compare("eq", 0)))).toEqual(withParent);
    expect(await syntheticFilteredIds(negate(compare("lt", 1)))).toEqual(withParent);
  });

  // The corpus pins ONE ternary whose condition reaches a chain — `w1-ternary-chain-cond`, whose
  // else-branch is a bare `!aBool`. The guard has to be a property of the ternary's false-branch
  // rather than of that one spelling, so these synthesise the other condition positions onto the
  // same seeded store (cerbos/query-plan-adapters#334). Each expectation is a real check() oracle,
  // never a hand-computed row list.
  test("every ternary condition over the chain inherits the absent-parent guard", async () => {
    const chainIn = new PlanExpression("in", [
      new PlanExpressionValue("finance"),
      new PlanExpressionVariable("request.resource.attr.mainCategory.subNames"),
    ]);
    const TRUE = new PlanExpressionValue(true);
    const FALSE = new PlanExpressionValue(false);
    const ternary = (
      condition: PlanExpressionOperand,
      thenBranch: PlanExpressionOperand,
      elseBranch: PlanExpressionOperand
    ) => new PlanExpression("if", [condition, thenBranch, elseBranch]);

    // The rows the chain condition is definitively TRUE for, and the ones it is definitively
    // FALSE for. Everything else — every row with no mainCategory at all — is a CEL
    // missing-path error, which selects NEITHER branch.
    const conditionTrue = await oracleAllowedIds("w1-in-chain");
    const conditionFalse = await oracleAllowedIds("w1-not-in-chain");
    expect(conditionTrue.length).toBeGreaterThan(0);
    expect(conditionFalse.length).toBeGreaterThan(0);
    expect(conditionTrue.length + conditionFalse.length).toBeLessThan(
      SEEDS.length
    );

    // The else-branch is what a bare `NOT` over the chain filter over-grants: it is TRUE for
    // every parentless row, so each of these returned the 17 missing-parent seeds on top.
    expect(await syntheticFilteredIds(ternary(chainIn, FALSE, TRUE))).toEqual(
      conditionFalse
    );
    expect(
      await syntheticFilteredIds(
        ternary(new PlanExpression("not", [chainIn]), TRUE, FALSE)
      )
    ).toEqual(conditionFalse);
    // A `not` condition in false-branch position: the double negation collapses back to the
    // positive membership, which excludes the parentless rows by itself.
    expect(
      await syntheticFilteredIds(
        ternary(new PlanExpression("not", [chainIn]), FALSE, TRUE)
      )
    ).toEqual(conditionTrue);

    // A conjunction condition needs De Morgan, not an outer NOT with the hops ANDed beside it:
    // CEL's `&&` absorbs an erroring operand when the other is FALSE, so a parentless row with
    // aBool=false makes the whole condition definitively FALSE and DOES select the else-branch.
    const aBoolFalse = SEEDS.filter((seed) => !seed.aBool).map((seed) => seed.id);
    expect(aBoolFalse.length).toBeGreaterThan(0);
    expect(
      await syntheticFilteredIds(
        ternary(
          new PlanExpression("and", [
            chainIn,
            new PlanExpressionVariable("request.resource.attr.aBool"),
          ]),
          FALSE,
          TRUE
        )
      )
    ).toEqual([...new Set([...conditionFalse, ...aBoolFalse])].sort());
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
      (seed) => parentSeedOf(parentSeedOf(seed)) !== undefined
    );
    expect(withParent.length).toBeGreaterThan(0);
    expect(withInner.length).toBeGreaterThan(0);
    expect(withParent.length).toBeLessThan(SEEDS.length);

    const joined = await prisma.adversarialResource.findMany({
      select: {
        id: true,
        parent: {
          select: { aString: true, inner: { select: { aString: true } } },
        },
      },
    });
    const stored = Object.fromEntries(
      joined.map((row) => [
        row.id,
        [row.parent?.aString ?? null, row.parent?.inner?.aString ?? null],
      ])
    );
    const expected = Object.fromEntries(
      SEEDS.map((seed) => [
        seed.id,
        [
          parentSeedOf(seed)?.aString ?? null,
          parentSeedOf(parentSeedOf(seed))?.aString ?? null,
        ],
      ])
    );
    expect(stored).toEqual(expected);
  });

  // Shapes Prisma refuses to translate carry PDP/policy liveness only. Asserting the complement
  // keeps the split honest: an action Prisma gains support for leaves this list, since the sweep
  // over every compared action already guards it.
  test.each(DEGENERACY_LIVENESS_PROBES)(
    "%s has a non-degenerate liveness oracle",
    async (action) => {
      expect(ORACLE_ACTIONS).not.toContain(action);
      await expectNonDegenerateOracle(action);
    }
  );
});
