import * as fs from "fs";
import * as path from "path";

import { afterAll, beforeAll, describe, expect, test } from "@jest/globals";
import {
  PlanExpression,
  PlanExpressionValue,
  PlanExpressionVariable,
} from "@cerbos/core";
import type { PlanExpressionOperand, PlanResourcesResponse } from "@cerbos/core";

import { queryPlanToPrisma, PlanKind, UnsupportedQueryPlanError } from ".";
import {
  MAPPER,
  MODEL,
  pdpTags,
  planOf,
  readCorpusJson,
  readGolden,
  readGoldens,
} from "./corpus";
import type { Golden } from "./corpus";
import { prisma } from "./test-setup.adversarial";

/**
 * The conformance harness (conformance/README.md, "The harness contract"). It loads the corpus
 * dataset into a real store, translates every recorded golden plan for both pinned PDPs, runs the
 * query through a generated Prisma client, and compares the returned ids with the `allowed` ids the
 * PDP's check() recorded. No PDP runs here. Exceptions live in `conformance-ledger.json`.
 *
 * The store is chosen with `ADAPTER_TEST_DB` (`sqlite` by default, `postgres` and `mysql` via
 * testcontainers started in `jest.globalSetup.adversarial.js`), and the Prisma major with
 * `PRISMA_VERSION`. The two are independent dimensions: v6/v7 is an ENGINE matrix, and an engine
 * matrix says nothing about how a provider coerces a fractional threshold against an `Int` column,
 * escapes a LIKE metacharacter, compares a real `timestamp` (cerbos/query-plan-adapters#320), or
 * decides with its COLLATION whether `=` is case-sensitive at all (#340).
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
  aNumberList: (number | null)[];
  aBoolList: (boolean | null)[];
  tags: Tag[];
  subCategoryNames: string[];
  /** The seed whose scalars this row's to-one `parent` carries; null for no parent. */
  parentSeedId: string | null;
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

/** The four scalars one level of the chain stores, as columns. */
function relationColumns(seed: Seed) {
  return {
    aBool: seed.aBool,
    aString: seed.aString,
    aNumber: seed.aNumber,
    aOptionalString: seed.aOptionalString,
  };
}

async function seedStore(): Promise<void> {
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
    const derived = derivedFor(seed);
    const parentSeed = parentSeedOf(seed);
    const innerSeed = parentSeedOf(parentSeed);
    await prisma.adversarialResource.create({
      data: {
        id: seed.id,
        aBool: seed.aBool,
        aString: seed.aString,
        aNumber: seed.aNumber,
        aDouble: derived.aDouble,
        aOptionalString: seed.aOptionalString,
        createdBy: derived.createdBy,
        scope: derived.scope,
        createdAt: derived.createdAt,
        updatedAt: derived.updatedAt,
        // One row per element, null elements included: membership is all a filter asks of
        // either list, so the element's position is not stored.
        numberList: { create: seed.aNumberList.map((value) => ({ value })) },
        boolList: { create: seed.aBoolList.map((value) => ({ value })) },
        tags: { create: seed.tags.map((t) => ({ tagId: t.id, name: t.name })) },
        // The to-one chain, one owned row per level. A seed with no parent gets no row at all.
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
        // One category holding every subcategory name (conformance/README.md, "The dataset").
        categories: {
          create:
            seed.subCategoryNames.length === 0
              ? []
              : [
                  {
                    name: "business",
                    subCategories: {
                      create: seed.subCategoryNames.map((subName) => ({
                        name: subName,
                        labels: { create: derived.labels.map((name) => ({ name })) },
                      })),
                    },
                  },
                ],
        },
      },
    });
  }
}

// -- the store ------------------------------------------------------------------------------------

const STORE_NAMES = ["sqlite", "postgres", "mysql"] as const;
type StoreName = (typeof STORE_NAMES)[number];

/**
 * How each leg asks its own connection which engine answered, and the first word of the answer.
 * Each spelling is a syntax error on the other two engines, so the anti-vacuity test fails in every
 * direction. MySQL's `version()` answers a bare `8.4.x`; `@@version_comment` names the engine.
 */
const STORE_BANNERS: Record<StoreName, { query: string; engine: string }> = {
  sqlite: { query: "select 'SQLite ' || sqlite_version() as banner", engine: "SQLite" },
  postgres: { query: "select version() as banner", engine: "PostgreSQL" },
  mysql: { query: "select @@version_comment as banner", engine: "MySQL" },
};

function selectedStoreName(): StoreName {
  const requested = process.env["ADAPTER_TEST_DB"] ?? "sqlite";
  // A typo must fail rather than silently fall back to SQLite (jest.adversarial.config.js rejects
  // it first; the repeat keeps this file reading against a value it trusts).
  if (!STORE_NAMES.includes(requested as StoreName)) {
    throw new Error(
      `Unknown ADAPTER_TEST_DB "${requested}": expected one of ${STORE_NAMES.join(", ")}`
    );
  }
  return requested as StoreName;
}

const STORE_NAME = selectedStoreName();

/**
 * The six adversarial schemas — (Prisma 6, Prisma 7) x (SQLite, PostgreSQL, MySQL) — must hold one
 * data model between them: a column that drifts in one would seed a different row shape on that leg
 * while every assertion here stayed identical.
 */
const ADVERSARIAL_SCHEMAS = [
  "schema.adversarial.prisma",
  "schema.adversarial.v6.prisma",
  "schema.adversarial.pg.prisma",
  "schema.adversarial.pg.v6.prisma",
  "schema.adversarial.mysql.prisma",
  "schema.adversarial.mysql.v6.prisma",
] as const;

/** A schema's `model` blocks, without comments, generator/datasource blocks or incidental space. */
function modelBlocks(schema: string): string {
  const withoutComments = schema
    .split("\n")
    .filter((line) => !line.trimStart().startsWith("//"))
    .join("\n");
  const models = withoutComments.match(/^model\s[\s\S]*$/m) ?? [];
  return models.join("\n").replace(/[ \t]+/g, " ").replace(/\n\s*\n/g, "\n").trim();
}

async function selectIds(where: object): Promise<string[]> {
  const rows = await prisma.adversarialResource.findMany({ where, select: { id: true } });
  return rows.map((row) => row.id).sort();
}

async function selectAllowed(queryPlan: PlanResourcesResponse): Promise<string[]> {
  const result = queryPlanToPrisma({ queryPlan, mapper: MAPPER, model: MODEL });
  if (result.kind === PlanKind.ALWAYS_DENIED) return [];
  return selectIds(result.kind === PlanKind.CONDITIONAL ? result.filters : {});
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
    fs.readFileSync(path.join(__dirname, "..", "conformance-ledger.json"), "utf8")
  ) as { cases: Record<string, LedgerEntry> }
).cases;

function ledgerEntry(golden: Golden, tag: string): LedgerEntry | undefined {
  const entry = LEDGER[golden.id];
  return entry && (entry.pdp === undefined || entry.pdp.includes(tag)) ? entry : undefined;
}

// -- the replay -----------------------------------------------------------------------------------

const TAGS = pdpTags();
const GOLDENS = new Map(TAGS.map((tag) => [tag, readGoldens(tag)]));
const passed: Record<string, Record<string, number>> = {};
const totals: Record<string, Record<string, number>> = {};

beforeAll(async () => {
  // CEL string matching is case-sensitive, and this adapter lowers contains/startsWith/endsWith to
  // LIKE. On SQLite, LIKE is case-INSENSITIVE for ASCII whatever the column collation; only this
  // pragma changes it (conformance/README.md, "Store configuration is part of conformance").
  if (STORE_NAME === "sqlite") {
    await prisma.$executeRawUnsafe("PRAGMA case_sensitive_like = ON");
  }
  await seedStore();
}, 120_000);

afterAll(async () => {
  await prisma.$disconnect();
  const lines = TAGS.map((tag) => {
    const tiers = Object.keys(totals[tag] ?? {}).sort();
    const skipped = GOLDENS.get(tag)!.filter((golden) => golden.plannerDivergence !== null);
    return `${tag}: ${tiers.map((tier) => `${tier} ${passed[tag]?.[tier] ?? 0}/${totals[tag]?.[tier]}`).join(", ")} (planner divergences skipped: ${skipped.map((golden) => golden.id).join(", ") || "none"})`;
  });
  console.log(`conformance (${STORE_NAME}) passed per tier\n${lines.join("\n")}`);
});

describe(`conformance (${STORE_NAME})`, () => {
  // Every assertion below is identical on every leg, so a PostgreSQL leg that silently fell back to
  // SQLite would pass while proving nothing about PostgreSQL. Ask the connection which engine it is.
  test("executes against the store ADAPTER_TEST_DB selects", async () => {
    const rows = await prisma.$queryRawUnsafe<{ banner: string }[]>(
      STORE_BANNERS[STORE_NAME].query
    );
    expect(rows[0]?.banner?.split(" ")[0]).toBe(STORE_BANNERS[STORE_NAME].engine);
  });

  // Store configuration is part of conformance. Prisma's MySQL migration engine hardcodes
  // `utf8mb4_unicode_ci`, so the tables are converted after `db push`
  // (jest.globalSetup.adversarial.js). Under that default these return the case variant, the
  // accent variant and the wrong-case prefix; asserted by name so a failure reads as "the store is
  // misconfigured" rather than as a translation bug.
  (STORE_NAME === "mysql" ? test : test.skip)(
    "the MySQL leg runs under a byte-exact NO PAD collation",
    async () => {
      expect({
        caseVariant: await selectIds({ aString: "one" }),
        accentFolded: await selectIds({ aString: "hello🚀" }),
        wrongCasePrefix: await selectIds({ aString: { startsWith: "ON" } }),
        trailingSpace: await selectIds({ aString: "one " }),
      }).toEqual({ caseVariant: ["a1"], accentFolded: [], wrongCasePrefix: [], trailingSpace: [] });
    }
  );

  // `PRAGMA case_sensitive_like` holds per CONNECTION, so it must reach every connection a query can
  // land on. Prisma 6 pooled SQLite connections until its schema pinned `connection_limit=1`;
  // concurrent queries are what spread onto the rest.
  (STORE_NAME === "sqlite" ? test : test.skip)(
    "the SQLite leg's LIKE is case-sensitive on every connection",
    async () => {
      const results = await Promise.all(
        Array.from({ length: 32 }, () => selectIds({ aString: { contains: "one" } }))
      );
      expect(results[0]?.length).toBeGreaterThan(0);
      expect(results.filter((ids) => ids.includes("c1"))).toEqual([]);
    }
  );

  test("every adversarial schema declares the same data model", () => {
    const [reference, ...rest] = ADVERSARIAL_SCHEMAS.map((name) => ({
      name,
      models: modelBlocks(
        fs.readFileSync(path.join(__dirname, "..", "prisma", name), "utf8")
      ),
    }));
    expect(reference?.models).toContain(`model ${MODEL}`);
    for (const schema of rest) {
      expect(schema).toEqual({ name: schema.name, models: reference?.models });
    }
  });

  test("the ledger names no case without a golden file", () => {
    const ids = new Set(TAGS.flatMap((tag) => GOLDENS.get(tag)!.map((golden) => golden.id)));
    expect(Object.keys(LEDGER).filter((id) => !ids.has(id))).toEqual([]);
  });

  // A golden that does not say which PDP recorded it, or whether that PDP diverged, cannot be
  // routed: fail on it rather than let the filter below read a missing field as "no divergence".
  test("every golden file is recorded for its tag and declares plannerDivergence", () => {
    const malformed = TAGS.flatMap((tag) =>
      GOLDENS.get(tag)!
        .filter((golden) => golden.pdp !== tag || golden.plannerDivergence === undefined)
        .map((golden) => `${tag}/${golden.id}`)
    );
    expect(malformed).toEqual([]);
  });

  for (const tag of TAGS) {
    // A plannerDivergence is a PDP bug no adapter can pass. The generator writes it only into the
    // golden files of the tags it applies to, so a non-null value is the whole skip condition.
    const cases = GOLDENS.get(tag)!.filter((golden) => golden.plannerDivergence === null);

    test.each(cases.map((golden) => [golden.id, golden] as const))(
      `${tag} %s`,
      async (_id, golden) => {
        const entry = ledgerEntry(golden, tag);
        if (entry?.status === "unsupported") {
          expect(() =>
            queryPlanToPrisma({ queryPlan: planOf(golden), mapper: MAPPER, model: MODEL })
          ).toThrow(UnsupportedQueryPlanError);
          return;
        }
        // Both sides sorted the same way, so a divergent entry cannot pass on ordering alone.
        const ids = await selectAllowed(planOf(golden));
        const allowed = [...golden.allowed].sort();
        if (entry?.status === "divergent") {
          expect(ids).not.toEqual(allowed);
          return;
        }
        expect(ids).toEqual(allowed);
        (passed[tag] ??= {})[golden.tier] = (passed[tag]?.[golden.tier] ?? 0) + 1;
      }
    );

    // The denominator is every golden file in the tier, skipped planner divergences included.
    for (const golden of GOLDENS.get(tag)!) {
      (totals[tag] ??= {})[golden.tier] = (totals[tag]?.[golden.tier] ?? 0) + 1;
    }
  }
});

// -- shapes the corpus does not spell -------------------------------------------------------------
//
// Synthesised plans over the same seeded store, for guards that must be a property of the
// `mainCategory` chain rather than of the few spellings the corpus pins. Every expectation is a
// recorded check() decision from a golden file, never a hand-computed row list.

describe(`the absent-parent guard over the chain (${STORE_NAME})`, () => {
  const allowedFor = (id: string) => readGolden(TAGS[0]!, id).allowed;

  async function synthetic(condition: PlanExpressionOperand): Promise<string[]> {
    const queryPlan: PlanResourcesResponse = {
      kind: PlanKind.CONDITIONAL,
      condition,
      cerbosCallId: "synthetic",
      requestId: "synthetic",
      validationErrors: [],
      metadata: undefined,
    };
    return selectAllowed(queryPlan);
  }

  // Every count threshold, not only the two spellings the corpus pins (#316). Each is TRUE for a
  // row with no mainCategory only if the guard leaks: an absent to-one parent is a CEL
  // missing-path error, so the PDP denies it outright.
  test("every count threshold inherits the guard", async () => {
    const size = new PlanExpression("size", [
      new PlanExpressionVariable("request.resource.attr.mainCategory.subCategories"),
    ]);
    const compare = (operator: string, threshold: number) =>
      new PlanExpression(operator, [size, new PlanExpressionValue(threshold)]);
    const negate = (condition: PlanExpressionOperand) => new PlanExpression("not", [condition]);

    for (const [shape, condition] of [
      ["size(chain) == 0", compare("eq", 0)],
      ["size(chain) <= 0", compare("le", 0)],
      ["size(chain) < 1", compare("lt", 1)],
      ["!(size(chain) >= 1)", negate(compare("ge", 1))],
      ["!(size(chain) > 0)", negate(compare("gt", 0))],
    ] as const) {
      expect([shape, await synthetic(condition)]).toEqual([shape, []]);
    }

    // The mirror image, so the loop above cannot pass by denying everything.
    const withParent = allowedFor("relation/size/non-negative-to-one-chain");
    expect(withParent.length).toBeGreaterThan(0);
    expect(await synthetic(negate(compare("eq", 0)))).toEqual(withParent);
    expect(await synthetic(negate(compare("lt", 1)))).toEqual(withParent);
  });

  // Every ternary condition position over the chain, not only the one the corpus pins (#334).
  test("every ternary condition inherits the guard", async () => {
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

    // A row with no mainCategory is neither: the condition errors and selects neither branch.
    const conditionTrue = allowedFor("relation/in/to-one-chain");
    const conditionFalse = allowedFor("relation/in/negated-to-one-chain");
    expect(conditionTrue.length).toBeGreaterThan(0);
    expect(conditionFalse.length).toBeGreaterThan(0);
    expect(conditionTrue.length + conditionFalse.length).toBeLessThan(SEEDS.length);

    expect(await synthetic(ternary(chainIn, FALSE, TRUE))).toEqual(conditionFalse);
    expect(
      await synthetic(ternary(new PlanExpression("not", [chainIn]), TRUE, FALSE))
    ).toEqual(conditionFalse);
    expect(
      await synthetic(ternary(new PlanExpression("not", [chainIn]), FALSE, TRUE))
    ).toEqual(conditionTrue);

    // A conjunction condition needs De Morgan: CEL's `&&` absorbs an erroring operand when the
    // other is FALSE, so a parentless row with aBool=false DOES select the else-branch.
    const aBoolFalse = SEEDS.filter((seed) => seed.aBool === false).map((seed) => seed.id);
    expect(aBoolFalse.length).toBeGreaterThan(0);
    expect(
      await synthetic(
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
});
