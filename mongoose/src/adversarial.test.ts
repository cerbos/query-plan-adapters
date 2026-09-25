import * as fs from "node:fs";
import * as path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "@jest/globals";
import mongoose, { model, Schema } from "mongoose";

import { PlanKind, queryPlanToMongoose, UnsupportedQueryPlanError } from ".";
import { MAPPER, pdpTags, planOf, readCorpusJson, readGoldens } from "./corpus";
import type { Golden } from "./corpus";

/**
 * The conformance harness (conformance/README.md, "The harness contract"). It loads the corpus
 * dataset into a real MongoDB, translates every recorded golden plan for both pinned PDPs, runs the
 * query, and compares the returned ids with the `allowed` ids the PDP's check() recorded. No PDP
 * runs here. Exceptions live in `conformance-ledger.json`.
 *
 * The server is whatever answers on 127.0.0.1:27017 — `npm run mongo` starts the one pinned in
 * `MONGO_IMAGE`, and CI runs this suite once per server image in the workflow's matrix, because the
 * corpus discriminates server behaviour (BSON ordering, regex dialect, `$expr` casting).
 */

const MONGO_URL = "mongodb://127.0.0.1:27017/cerbos_mongoose_conformance";

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
  /** Homogeneous scalar lists. A null ELEMENT is a value, not an absent attribute. */
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
  readCorpusJson("derived-fields.json") as {
    derived: Record<string, DerivedEntry>;
  }
).derived;

function derivedFor(seed: Seed): DerivedEntry {
  const entry = DERIVED[seed.id];
  if (entry === undefined) {
    throw new Error(`derived-fields.json has no entry for seed "${seed.id}"`);
  }
  return entry;
}

// -- the store: one document per seed, relations embedded -----------------------------------------

interface AdversarialLabel {
  name: string | null;
}

interface AdversarialSubCategory {
  name: string;
  labels: AdversarialLabel[];
}

interface AdversarialCategory {
  name: string;
  subCategories: AdversarialSubCategory[];
}

/** One level of the to-one chain, stored as an embedded subdocument. */
interface AdversarialRelationLevel {
  aBool: boolean | null;
  aString: string | null;
  aNumber: number | null;
  aOptionalString: string | null;
}

interface AdversarialParent extends AdversarialRelationLevel {
  inner: AdversarialRelationLevel | null;
}

interface AdversarialResourceDocument {
  resourceId: string;
  aBool: boolean | null;
  aString: string | null;
  aNumber: number | null;
  aDouble: number | null;
  aOptionalString: string | null;
  createdBy: string;
  scope: string | null;
  createdAt: Date | null;
  updatedAt: Date | null;
  tags: Tag[];
  categories: AdversarialCategory[];
  parent: AdversarialParent | null;
  aNumberList: (number | null)[];
  aBoolList: (boolean | null)[];
}

const tagSchema = new Schema<Tag>(
  {
    id: { type: String, required: true },
    name: { type: String, default: null },
  },
  { _id: false, id: false },
);
const labelSchema = new Schema<AdversarialLabel>(
  { name: { type: String, default: null } },
  { _id: false, id: false },
);
const subCategorySchema = new Schema<AdversarialSubCategory>(
  {
    name: { type: String, required: true },
    labels: { type: [labelSchema], default: [] },
  },
  { _id: false, id: false },
);
const categorySchema = new Schema<AdversarialCategory>(
  {
    name: { type: String, required: true },
    subCategories: { type: [subCategorySchema], default: [] },
  },
  { _id: false, id: false },
);
// The corpus's one real to-one relation. A document store has no join, so both levels are
// embedded subdocuments — but the SHAPE is the same to-one chain every other store carries, and
// an absent level is a missing path here exactly as it is a missing row there.
const innerSchema = new Schema<AdversarialRelationLevel>(
  {
    // Nullable: seeds j1, j2 and j3 each store a null in one of these three (a missing attribute).
    aBool: { type: Boolean, default: null },
    aString: { type: String, default: null },
    aNumber: { type: Number, default: null },
    aOptionalString: { type: String, default: null },
  },
  { _id: false, id: false },
);
const parentSchema = new Schema<AdversarialParent>(
  {
    aBool: { type: Boolean, default: null },
    aString: { type: String, default: null },
    aNumber: { type: Number, default: null },
    aOptionalString: { type: String, default: null },
    inner: { type: innerSchema, default: null },
  },
  { _id: false, id: false },
);
const resourceSchema = new Schema<AdversarialResourceDocument>(
  {
    resourceId: { type: String, required: true, unique: true },
    // Nullable: seeds j1, j2 and j3 each store a null in one of these three (a missing attribute).
    aBool: { type: Boolean, default: null },
    aString: { type: String, default: null },
    aNumber: { type: Number, default: null },
    aDouble: { type: Number, default: null },
    aOptionalString: { type: String, default: null },
    createdBy: { type: String, required: true },
    scope: { type: String, default: null },
    createdAt: { type: Date, default: null },
    updatedAt: { type: Date, default: null },
    tags: { type: [tagSchema], default: [] },
    categories: { type: [categorySchema], default: [] },
    parent: { type: parentSchema, default: null },
    // Native TYPED arrays, which is what an application would declare, and deliberately so:
    // Mongoose casts the literal of an `$expr` comparison to the schema type of the path on the
    // other side, so `[Boolean]` is what gives it the chance to turn the cross-type probes' `1`
    // into `true`. It does not for `$arrayElemAt`, whose operand is an array rather than a path,
    // and the two probes prove that against the filter Mongoose actually sends: spelling index 0
    // as `$first`, which it does cast, over-grants b4 and c1. Both casters keep a null element
    // null.
    aNumberList: { type: [Number], default: [] },
    aBoolList: { type: [Boolean], default: [] },
  },
  { id: false },
);
const AdversarialResource = model<AdversarialResourceDocument>(
  "MongooseAdversarialResource",
  resourceSchema,
);

// -- the real to-one relation (conformance/README.md, "The dataset") ---------------------------
//
// `parentSeedId` names the seed whose four scalars this row's `parent` carries, and that seed's own
// `parentSeedId` names the ones `parent.inner` carries. The chain is cut at two levels. Every
// resource owns a FRESH parent (and inner) subdocument rather than pointing at the named seed's own
// document, so no two resources share one and a filter that returned the parent instead of the
// child cannot agree with the recorded decisions by accident.

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

/** The four scalars one level of the chain stores. */
function relationLevel(seed: Seed): AdversarialRelationLevel {
  return {
    aBool: seed.aBool,
    aString: seed.aString,
    aNumber: seed.aNumber,
    aOptionalString: seed.aOptionalString,
  };
}

/** The stored `parent` subdocument for a seed, or null when it has no parent. */
function storedParent(seed: Seed): AdversarialParent | null {
  const parentSeed = parentSeedOf(seed);
  if (parentSeed === undefined) {
    return null;
  }
  const innerSeed = parentSeedOf(parentSeed);
  return {
    ...relationLevel(parentSeed),
    inner: innerSeed === undefined ? null : relationLevel(innerSeed),
  };
}

function toDocument(seed: Seed): AdversarialResourceDocument {
  const derived = derivedFor(seed);
  return {
    resourceId: seed.id,
    aBool: seed.aBool,
    aString: seed.aString,
    aNumber: seed.aNumber,
    aDouble: derived.aDouble,
    aOptionalString: seed.aOptionalString,
    createdBy: derived.createdBy,
    scope: derived.scope,
    createdAt: derived.createdAt === null ? null : new Date(derived.createdAt),
    updatedAt: derived.updatedAt === null ? null : new Date(derived.updatedAt),
    tags: seed.tags,
    // One category holding every subcategory name (conformance/README.md, "The dataset").
    categories:
      seed.subCategoryNames.length === 0
        ? []
        : [
            {
              name: "business",
              subCategories: seed.subCategoryNames.map((name) => ({
                name,
                labels: derived.labels.map((label) => ({ name: label })),
              })),
            },
          ],
    parent: storedParent(seed),
    aNumberList: seed.aNumberList,
    aBoolList: seed.aBoolList,
  };
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
    fs.readFileSync(
      path.join(__dirname, "..", "conformance-ledger.json"),
      "utf8",
    ),
  ) as { cases: Record<string, LedgerEntry> }
).cases;

function ledgerEntry(golden: Golden, tag: string): LedgerEntry | undefined {
  const entry = LEDGER[golden.id];
  return entry && (entry.pdp === undefined || entry.pdp.includes(tag))
    ? entry
    : undefined;
}

// -- the replay -----------------------------------------------------------------------------------

async function selectAllowed(golden: Golden): Promise<string[]> {
  const result = queryPlanToMongoose({
    queryPlan: planOf(golden),
    mapper: MAPPER,
  });
  if (result.kind === PlanKind.ALWAYS_DENIED) return [];
  const rows = await AdversarialResource.find(
    result.kind === PlanKind.CONDITIONAL ? result.filters : {},
  )
    .select({ resourceId: 1, _id: 0 })
    .lean()
    .exec();
  return rows.map((row) => row.resourceId).sort();
}

const TAGS = pdpTags();
const GOLDENS = new Map(TAGS.map((tag) => [tag, readGoldens(tag)]));
const passed: Record<string, Record<string, number>> = {};
const totals: Record<string, Record<string, number>> = {};

beforeAll(async () => {
  await mongoose.connect(MONGO_URL);
  await AdversarialResource.deleteMany({});
  await AdversarialResource.create(SEEDS.map(toDocument));
}, 30_000);

afterAll(async () => {
  await mongoose.disconnect();
  const lines = TAGS.map((tag) => {
    const tiers = Object.keys(totals[tag] ?? {}).sort();
    return `${tag}: ${tiers
      .map(
        (tier) => `${tier} ${passed[tag]?.[tier] ?? 0}/${totals[tag]?.[tier]}`,
      )
      .join(", ")}`;
  });
  console.log(`conformance (mongodb) passed per tier\n${lines.join("\n")}`);
});

describe("conformance (mongodb)", () => {
  test("the ledger names no case without a golden file", () => {
    const ids = new Set(
      TAGS.flatMap((tag) => GOLDENS.get(tag)!.map((golden) => golden.id)),
    );
    expect(Object.keys(LEDGER).filter((id) => !ids.has(id))).toEqual([]);
  });

  for (const tag of TAGS) {
    // A plannerDivergence is a PDP bug no adapter can pass. The generator records it only in the
    // golden of a tag it applies to, so a non-null value is the whole test: skip exactly those.
    const cases = GOLDENS.get(tag)!.filter(
      (golden) => golden.plannerDivergence == null,
    );

    test.each(cases.map((golden) => [golden.id, golden] as const))(
      `${tag} %s`,
      async (_id, golden) => {
        const entry = ledgerEntry(golden, tag);
        if (entry?.status === "unsupported") {
          expect(() =>
            queryPlanToMongoose({ queryPlan: planOf(golden), mapper: MAPPER }),
          ).toThrow(UnsupportedQueryPlanError);
          return;
        }
        const ids = await selectAllowed(golden);
        if (entry?.status === "divergent") {
          expect(ids).not.toEqual(golden.allowed);
          return;
        }
        expect(ids).toEqual(golden.allowed);
        (passed[tag] ??= {})[golden.tier] =
          (passed[tag]?.[golden.tier] ?? 0) + 1;
      },
    );

    // The total is every golden case in the tier, skipped planner divergences included.
    for (const golden of GOLDENS.get(tag)!) {
      (totals[tag] ??= {})[golden.tier] = (totals[tag]?.[golden.tier] ?? 0) + 1;
    }
  }
});
