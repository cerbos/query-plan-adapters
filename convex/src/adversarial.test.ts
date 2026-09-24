import * as fs from "node:fs";
import * as path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "@jest/globals";
import { ConvexHttpClient } from "convex/browser";

import { api } from "../convex/_generated/api.js";
import { MAPPER } from "../convex/adversarialMapper";
import type { AdversarialDocument } from "../convex/schema";
import { queryPlanToConvex, UnsupportedQueryPlanError } from ".";
import { pdpTags, planOf, readCorpusJson, readGoldens } from "./corpus";
import type { Golden } from "./corpus";

/**
 * The conformance harness (conformance/README.md, "The harness contract"). It loads the corpus
 * dataset into a real Convex backend, translates every recorded golden plan for both pinned PDPs,
 * runs the query there, and compares the returned ids with the `allowed` ids the PDP's check()
 * recorded. No PDP runs here. Exceptions live in `conformance-ledger.json`.
 *
 * The backend is whatever answers on CONVEX_URL (default 127.0.0.1:3210) with this directory's
 * functions deployed: `npm run convex:up`, then `npx convex deploy` and `npx convex codegen`, as the
 * workflow's `integration-test` job does.
 */

const CONVEX_URL = process.env["CONVEX_URL"] ?? "http://127.0.0.1:3210";
const convex = new ConvexHttpClient(CONVEX_URL);

// -- the dataset: conformance/seeds.json + derived-fields.json ----------------------------------

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

// -- the store: one document per seed, relations nested -------------------------------------------
//
// A document store holds the attributes the way the PDP sees them, so each stored document is the
// seed's resource in conformance/resources.json plus its `id` — asserted below. A NULL column
// under the omitted convention is an ABSENT key; under the explicit convention (`owner`,
// `coOwner`, `tagNames`, the scalar lists) it is a null value.

type StoredDocument = AdversarialDocument;
type StoredRelationLevel = Omit<NonNullable<StoredDocument["parent"]>, "inner">;

// The real to-one relation (conformance/README.md, "The dataset"). `parentSeedId` names the seed
// whose four scalars this row's `parent` carries, and that seed's own `parentSeedId` names the
// ones `parent.inner` carries. The chain is cut at two levels. Every resource owns a FRESH parent
// object, so a filter that returned the parent instead of the child cannot agree by accident.

const SEEDS_BY_ID = new Map(SEEDS.map((seed) => [seed.id, seed]));

function parentSeedOf(seed: Seed | undefined): Seed | undefined {
  const id = seed?.parentSeedId;
  if (id === undefined || id === null) return undefined;
  const parent = SEEDS_BY_ID.get(id);
  if (parent === undefined) {
    throw new Error(
      `seeds.json: "${seed?.id}" names parent "${id}", which is not a seed id`,
    );
  }
  return parent;
}

/** The four scalars one level of the chain carries. A NULL column is an ABSENT key. */
function relationLevelOf(seed: Seed): StoredRelationLevel {
  const level: StoredRelationLevel = {
    aBool: seed.aBool,
    aString: seed.aString,
    aNumber: seed.aNumber,
  };
  if (seed.aOptionalString !== null)
    level.aOptionalString = seed.aOptionalString;
  return level;
}

/** The stored `parent` object for a seed, or undefined when it has no parent. */
function storedParent(seed: Seed): StoredDocument["parent"] {
  const parentSeed = parentSeedOf(seed);
  if (parentSeed === undefined) return undefined;
  const innerSeed = parentSeedOf(parentSeed);
  const parent: NonNullable<StoredDocument["parent"]> =
    relationLevelOf(parentSeed);
  if (innerSeed !== undefined) parent.inner = relationLevelOf(innerSeed);
  return parent;
}

function storedDocument(seed: Seed): StoredDocument {
  const derived = derivedFor(seed);
  const document: StoredDocument = {
    id: seed.id,
    aBool: seed.aBool,
    aString: seed.aString,
    aNumber: seed.aNumber,
    createdBy: derived.createdBy,
    // Explicit-null aliases of `aOptionalString` and `scope`.
    owner: seed.aOptionalString,
    coOwner: derived.scope,
    tagNames: seed.tags.map((tag) => tag.name),
    aNumberList: seed.aNumberList,
    aBoolList: seed.aBoolList,
    obj: { inner: seed.aString },
    tags: seed.tags.map((tag) =>
      tag.name === null ? { id: tag.id } : { id: tag.id, name: tag.name },
    ),
    categories: seed.subCategoryNames.map((name) => ({
      name: "business",
      subCategories: [
        {
          name,
          // A null element is a NULL label name — a missing element attribute.
          labels: derived.labels.map((labelName) =>
            labelName === null ? {} : { name: labelName },
          ),
        },
      ],
    })),
  };
  if (seed.aOptionalString !== null) {
    document.aOptionalString = seed.aOptionalString;
  }
  if (derived.aDouble !== null) document.aDouble = derived.aDouble;
  if (derived.createdAt !== null) document.createdAt = derived.createdAt;
  if (derived.updatedAt !== null) document.updatedAt = derived.updatedAt;
  if (derived.scope !== null) document.scope = derived.scope;
  if (seed.subCategoryNames.length > 0) {
    document.mainCategory = {
      name: "business",
      subCategories: seed.subCategoryNames.map((name) => ({ name })),
      subNames: seed.subCategoryNames,
    };
  }
  const parent = storedParent(seed);
  if (parent !== undefined) document.parent = parent;
  return document;
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

/**
 * Runs the plan in the backend. The plan crosses into the Convex function as JSON, exactly as a
 * deployed application's would, and the backend reports which half of the adapter's output — the
 * Convex filter, the in-memory post-filter, or both — answered it.
 */
async function selectAllowed(
  golden: Golden,
): Promise<{ ids: string[]; execution: string }> {
  // Every plan kind goes to the backend, ALWAYS_DENIED included, so the adapter decides each case
  // rather than the harness answering one for it.
  return convex.query(api.adversarial.executePlan, {
    queryPlan: JSON.parse(JSON.stringify(planOf(golden))),
  });
}

const TAGS = pdpTags();
const GOLDENS = new Map(TAGS.map((tag) => [tag, readGoldens(tag)]));
const passed: Record<string, Record<string, number>> = {};
const totals: Record<string, Record<string, number>> = {};
const executions: Record<string, Record<string, number>> = {};

const count = (
  table: Record<string, Record<string, number>>,
  tag: string,
  key: string,
) => {
  (table[tag] ??= {})[key] = (table[tag]?.[key] ?? 0) + 1;
};

beforeAll(async () => {
  await convex.mutation(api.adversarial.deleteAll, {});
  for (const seed of SEEDS) {
    await convex.mutation(api.adversarial.insert, storedDocument(seed));
  }
}, 60_000);

afterAll(async () => {
  await convex.mutation(api.adversarial.deleteAll, {});
  const summary = (
    table: Record<string, Record<string, number>>,
    tag: string,
  ) =>
    Object.entries(table[tag] ?? {})
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([key, n]) => `${key} ${n}`)
      .join(", ");
  const lines = TAGS.map((tag) => {
    const tiers = Object.keys(totals[tag] ?? {}).sort();
    return `${tag}: ${tiers
      .map(
        (tier) => `${tier} ${passed[tag]?.[tier] ?? 0}/${totals[tag]?.[tier]}`,
      )
      .join(", ")}; planner divergences skipped: ${
      GOLDENS.get(tag)!.filter((golden) => golden.plannerDivergence).length
    } (decided by: ${summary(executions, tag)})`;
  });
  console.log(`conformance (convex) passed per tier\n${lines.join("\n")}`);
});

describe("conformance (convex)", () => {
  test("the ledger names no case without a golden file", () => {
    const ids = new Set(
      TAGS.flatMap((tag) => GOLDENS.get(tag)!.map((golden) => golden.id)),
    );
    expect(Object.keys(LEDGER).filter((id) => !ids.has(id))).toEqual([]);
  });

  test("the ledger scopes no entry to a PDP tag that is not pinned", () => {
    // An entry scoped only to a tag that has left pdp-versions.json applies to nothing, and would
    // otherwise linger silently after a PDP bump.
    expect(
      Object.entries(LEDGER).flatMap(([id, entry]) =>
        (entry.pdp ?? [])
          .filter((tag) => !TAGS.includes(tag))
          .map((tag) => `${id}: ${tag}`),
      ),
    ).toEqual([]);
  });

  test("each stored document is its resource in resources.json", async () => {
    const { resources } = readCorpusJson("resources.json") as {
      resources: { id: string; attr: Record<string, unknown> }[];
    };
    const stored = (await convex.query(api.adversarial.listAll, {})) as {
      id: string;
    }[];
    const byId = (rows: { id: string }[]) =>
      [...rows].sort((a, b) => a.id.localeCompare(b.id));
    expect(byId(stored)).toEqual(
      byId(resources.map(({ id, attr }) => ({ id, ...attr }))),
    );
  });

  for (const tag of TAGS) {
    // A plannerDivergence is a PDP bug no adapter can pass; a golden carries it only for the tags
    // it applies to.
    const cases = GOLDENS.get(tag)!.filter(
      (golden) => !golden.plannerDivergence,
    );

    test.each(cases.map((golden) => [golden.id, golden] as const))(
      `${tag} %s`,
      async (_id, golden) => {
        const entry = ledgerEntry(golden, tag);
        if (entry?.status === "unsupported") {
          expect(() =>
            queryPlanToConvex({
              queryPlan: planOf(golden),
              mapper: MAPPER,
              allowPostFilter: true,
            }),
          ).toThrow(UnsupportedQueryPlanError);
          return;
        }
        const { ids, execution } = await selectAllowed(golden);
        if (entry?.status === "divergent") {
          expect(ids).not.toEqual(golden.allowed);
          return;
        }
        expect(ids).toEqual(golden.allowed);
        count(passed, tag, golden.tier);
        count(executions, tag, execution);
      },
    );

    // The total is every golden in the tier, skipped planner divergences included, so the printed
    // ratio is the one the README's tier table carries.
    for (const golden of GOLDENS.get(tag)!) count(totals, tag, golden.tier);
  }
});
