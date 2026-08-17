import * as fs from "fs";
import * as path from "path";

import { GRPC as Cerbos } from "@cerbos/grpc";
import {
  PlanExpression,
  PlanExpressionValue,
  PlanExpressionVariable,
} from "@cerbos/core";
import type { PlanExpressionOperand, Principal } from "@cerbos/core";

import { queryPlanToPrisma, PlanKind, MapperConfig } from ".";
import { CONFORMANCE_DIR, MAPPER, MODEL } from "./corpus";
import {
  loadActionControlPlane,
  loadCheckResources,
  requireOutcomeMessage,
} from "./controlPlane";
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
 * oracle recipe (NULL-as-missing-attribute and catalog cardinality expectations). This file owns
 * seeding and query execution; `./controlPlane` validates the catalog and adapter-local direct
 * outcomes shared with `translator.test.ts`.
 *
 * The whole corpus is replayed against every store this adapter is proved on, one store per run,
 * selected with `ADAPTER_TEST_DB` (`sqlite` by default, `postgres` and `mysql` for the
 * container-backed legs — see `jest.globalSetup.adversarial.js`). The Prisma major and the store
 * are independent dimensions: v5/v6/v7 is an ENGINE matrix, and an engine matrix says nothing
 * about how a provider coerces a fractional threshold against an `Int` column, escapes a LIKE
 * metacharacter, compares a real `timestamp` rather than milliseconds since the epoch
 * (cerbos/query-plan-adapters#320), or decides with its COLLATION whether `=` is case-sensitive at
 * all (#340).
 */

const cerbos = new Cerbos("127.0.0.1:3593", { tls: false });

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
  sqlite: {
    query: "select 'SQLite ' || sqlite_version() as banner",
    engine: "SQLite",
  },
  postgres: { query: "select version() as banner", engine: "PostgreSQL" },
  mysql: { query: "select @@version_comment as banner", engine: "MySQL" },
};

function selectedStoreName(): StoreName {
  const requested = process.env["ADAPTER_TEST_DB"] ?? "sqlite";
  // jest.adversarial.config.js rejects an unknown value before jest resolves this module; the
  // repeat here is what keeps the assertion below reading against a value this file trusts.
  if (!STORE_NAMES.includes(requested as StoreName)) {
    throw new Error(
      `Unknown ADAPTER_TEST_DB "${requested}": expected one of ${STORE_NAMES.join(", ")}`,
    );
  }
  return requested as StoreName;
}

const STORE_NAME = selectedStoreName();

/**
 * The nine adversarial schemas, which must hold one data model between them.
 *
 * A generated Prisma client bakes in its provider and its major, so proving the corpus on
 * (Prisma 5, Prisma 6, Prisma 7) x (SQLite, PostgreSQL, MySQL) needs nine schema files. They form
 * only a matrix over the same models — a column that drifts in one of them would seed a different
 * row shape on that leg while every assertion in this file stayed identical, which is the
 * projection trap conformance/README.md describes applied to the schema instead of the seeds.
 *
 * It is `STORE_NAMES.length * 3`, and asserted to be: adding a store means adding three schemas,
 * and a store whose schemas were never written would otherwise arrive as a resolution failure
 * inside one leg rather than as a gap in the matrix.
 */
const ADVERSARIAL_SCHEMAS = [
  "schema.adversarial.prisma",
  "schema.adversarial.v5.prisma",
  "schema.adversarial.v6.prisma",
  "schema.adversarial.pg.prisma",
  "schema.adversarial.pg.v5.prisma",
  "schema.adversarial.pg.v6.prisma",
  "schema.adversarial.mysql.prisma",
  "schema.adversarial.mysql.v5.prisma",
  "schema.adversarial.mysql.v6.prisma",
] as const;

/**
 * A schema's `model` blocks, with comments, the generator/datasource blocks and all incidental
 * whitespace removed — everything that legitimately differs between the nine.
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

const ACTION_CONTROL_PLANE = loadActionControlPlane({
  adapter: "prisma",
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
      `seeds.json: "${seed?.id}" names parent "${id}", which is not a seed id`,
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
  mapper: Record<string, MapperConfig>,
): Record<string, MapperConfig> {
  return Object.fromEntries(
    Object.entries(mapper).map(([reference, config]) => {
      const { nullAttributeRepresentation: _stripped, ...rest } = config;
      return [reference, rest];
    }),
  );
}

const MAPPER_WITHOUT_NULL_CONVENTIONS = withoutNullConventions(MAPPER);

beforeAll(async () => {
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

/** Assert the catalog's authoritative oracle cardinality for one action. */
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

// -- adapter execution through the public queryPlanToPrisma path --

async function adapterFilteredIds(
  action: string,
  nullAttributeRepresentation: "explicit" | "omitted" = "explicit",
  mapper: Record<string, MapperConfig> = MAPPER,
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
      STORE_BANNERS[STORE_NAME].query,
    );
    expect(rows[0]?.banner?.split(" ")[0]).toBe(
      STORE_BANNERS[STORE_NAME].engine,
    );
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
    "the MySQL leg runs under a case- and accent-sensitive collation",
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
        // "one" is seed a1; "One" is seed c1.
        caseVariant: await ids({ aString: "one" }),
        // "héllo🚀" is seed a6; the accent-folded spelling matches nothing.
        accentFolded: await ids({ aString: "hello🚀" }),
        // `hier-*` reaches the same collation through LIKE rather than through `=`.
        wrongCasePrefix: await ids({ aString: { startsWith: "ON" } }),
      }).toEqual({
        caseVariant: ["a1"],
        accentFolded: [],
        wrongCasePrefix: [],
      });
    },
  );

  test("every adversarial schema declares the same data model", () => {
    const [reference, ...rest] = ADVERSARIAL_SCHEMAS.map((name) => ({
      name,
      models: modelBlocks(fs.readFileSync(path.join(SCHEMA_DIR, name), "utf8")),
    }));
    if (!reference) {
      throw new Error("ADVERSARIAL_SCHEMAS is empty");
    }
    // Guard the guard: a regex that stopped matching would make every schema compare equal on an
    // empty string, and a store added without its three schemas would leave the matrix short.
    expect(reference.models).toContain(`model ${MODEL}`);
    expect(ADVERSARIAL_SCHEMAS).toHaveLength(STORE_NAMES.length * 3);
    for (const schema of rest) {
      expect({ name: schema.name, models: schema.models }).toEqual({
        name: schema.name,
        models: reference.models,
      });
    }
  });

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

  // Shapes the adapter does not support (globally unsupported planner shapes plus Prisma's
  // manifest's `rejected` outcomes): translation must fail loudly, never produce a
  // silently-wrong filter. The plan is fetched OUTSIDE the assertion so a PDP failure fails
  // the test instead of passing it, and no query executes — the invariant is that the shape
  // throws BEFORE a filter exists, so SQLite rejecting a wrongly emitted filter afterwards
  // must not be able to masquerade as the adapter refusing to translate.
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
        queryPlanToPrisma({
          queryPlan,
          mapper: MAPPER,
          model: MODEL,
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
  // `!(size(...) > 0)` — and CEL's type checker rules out a third that a real policy could
  // reach through the planner (`>= 1` and `<= 0` are the only other thresholds this adapter
  // can express, and no policy needs both spellings). The guard must nonetheless be a
  // property of the chain rather than of the two spellings that happen to be pinned, so
  // these synthesise the remaining threshold/polarity combinations directly onto the same
  // seeded store and assert the parentless rows stay out of every one
  // (cerbos/query-plan-adapters#316).
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
        const where =
          result.kind === PlanKind.CONDITIONAL ? result.filters : {};
        const rows = await prisma.adversarialResource.findMany({
          where,
          select: { id: true },
        });
        return rows.map((row) => row.id).sort();
      };

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
        expect([shape, await filteredIdsFor(condition)]).toEqual([shape, []]);
      }

      // The mirror image, so the loop above cannot pass by denying everything: the negation of
      // an emptiness check is TRUE for exactly the rows that HAVE the parent.
      const withParent = await oracleAllowedIds("w1-size-nonneg-chain");
      expect(withParent.length).toBeGreaterThan(0);
      expect(withParent.length).toBeLessThan(SEEDS.length);
      expect(await filteredIdsFor(negate(compare("eq", 0)))).toEqual(
        withParent,
      );
      expect(await filteredIdsFor(negate(compare("lt", 1)))).toEqual(
        withParent,
      );
    },
  );

  // The corpus pins ONE ternary whose condition reaches a chain — `w1-ternary-chain-cond`, whose
  // else-branch is a bare `!aBool`. The guard has to be a property of the ternary's false-branch
  // rather than of that one spelling, so these synthesise the other condition positions onto the
  // same seeded store (cerbos/query-plan-adapters#334). Each expectation is a real check() oracle,
  // never a hand-computed row list.
  fullMatrixTest(
    "every ternary condition over the chain inherits the absent-parent guard",
    async () => {
      const chainIn = new PlanExpression("in", [
        new PlanExpressionValue("finance"),
        new PlanExpressionVariable(
          "request.resource.attr.mainCategory.subNames",
        ),
      ]);
      const TRUE = new PlanExpressionValue(true);
      const FALSE = new PlanExpressionValue(false);
      const ternary = (
        condition: PlanExpressionOperand,
        thenBranch: PlanExpressionOperand,
        elseBranch: PlanExpressionOperand,
      ) => new PlanExpression("if", [condition, thenBranch, elseBranch]);

      const filteredIdsFor = async (
        condition: PlanExpressionOperand,
      ): Promise<string[]> => {
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
        const where =
          result.kind === PlanKind.CONDITIONAL ? result.filters : {};
        const rows = await prisma.adversarialResource.findMany({
          where,
          select: { id: true },
        });
        return rows.map((row) => row.id).sort();
      };

      // The rows the chain condition is definitively TRUE for, and the ones it is definitively
      // FALSE for. Everything else — every row with no mainCategory at all — is a CEL
      // missing-path error, which selects NEITHER branch.
      const conditionTrue = await oracleAllowedIds("w1-in-chain");
      const conditionFalse = await oracleAllowedIds("w1-not-in-chain");
      expect(conditionTrue.length).toBeGreaterThan(0);
      expect(conditionFalse.length).toBeGreaterThan(0);
      expect(conditionTrue.length + conditionFalse.length).toBeLessThan(
        SEEDS.length,
      );

      // The else-branch is what a bare `NOT` over the chain filter over-grants: it is TRUE for
      // every parentless row, so each of these returned the 17 missing-parent seeds on top.
      expect(await filteredIdsFor(ternary(chainIn, FALSE, TRUE))).toEqual(
        conditionFalse,
      );
      expect(
        await filteredIdsFor(
          ternary(new PlanExpression("not", [chainIn]), TRUE, FALSE),
        ),
      ).toEqual(conditionFalse);
      // A `not` condition in false-branch position: the double negation collapses back to the
      // positive membership, which excludes the parentless rows by itself.
      expect(
        await filteredIdsFor(
          ternary(new PlanExpression("not", [chainIn]), FALSE, TRUE),
        ),
      ).toEqual(conditionTrue);

      // A conjunction condition needs De Morgan, not an outer NOT with the hops ANDed beside it:
      // CEL's `&&` absorbs an erroring operand when the other is FALSE, so a parentless row with
      // aBool=false makes the whole condition definitively FALSE and DOES select the else-branch.
      const aBoolFalse = SEEDS.filter((seed) => !seed.aBool).map(
        (seed) => seed.id,
      );
      expect(aBoolFalse.length).toBeGreaterThan(0);
      expect(
        await filteredIdsFor(
          ternary(
            new PlanExpression("and", [
              chainIn,
              new PlanExpressionVariable("request.resource.attr.aBool"),
            ]),
            FALSE,
            TRUE,
          ),
        ),
      ).toEqual([...new Set([...conditionFalse, ...aBoolFalse])].sort());
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
      ]),
    );
    const expected = Object.fromEntries(
      SEEDS.map((seed) => [
        seed.id,
        [
          parentSeedOf(seed)?.aString ?? null,
          parentSeedOf(parentSeedOf(seed))?.aString ?? null,
        ],
      ]),
    );
    expect(stored).toEqual(expected);
  });
});
