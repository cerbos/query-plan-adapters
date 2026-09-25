import * as fs from "node:fs";
import * as path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "@jest/globals";
import {
  ChromaClient,
  ChromaNotFoundError,
  type Collection,
  type Metadata,
} from "chromadb";

import { PlanKind, queryPlanToChromaDB, UnsupportedOperatorError } from ".";
import {
  FIELD_NAME_MAPPER,
  pdpTags,
  planOf,
  readCorpusJson,
  readGoldens,
} from "./corpus";
import type { Golden } from "./corpus";

/**
 * The conformance harness (conformance/README.md, "The harness contract"). It loads the corpus
 * dataset into a real ChromaDB collection, translates every recorded golden plan for both pinned
 * PDPs, runs the query, and compares the returned ids with the `allowed` ids the PDP's check()
 * recorded. No PDP runs here. Exceptions live in `conformance-ledger.json`.
 *
 * The server is whatever answers on CHROMA_URL (default 127.0.0.1:8234) — `npm run chroma` starts
 * the one pinned in `CHROMA_IMAGE`.
 */

const chromaUrl = new URL(process.env["CHROMA_URL"] ?? "http://127.0.0.1:8234");
const chroma = new ChromaClient({
  host: chromaUrl.hostname,
  port: Number(chromaUrl.port) || 8000,
});
const COLLECTION_NAME = "adapter-conformance";
const BASE_EMBEDDING = [0.1, 0.2, 0.3, 0.4];

// -- the dataset: conformance/seeds.json + derived-fields.json ----------------------------------

interface Seed {
  id: string;
  aBool: boolean | null;
  aString: string | null;
  aNumber: number | null;
  aOptionalString: string | null;
  /** The seed whose scalars this row's to-one `parent` carries; null for no parent. */
  parentSeedId: string | null;
}

interface DerivedEntry {
  aDouble: number | null;
  createdAt: string | null;
  updatedAt: string | null;
  createdBy: string | null;
  scope: string | null;
}

const SEEDS = (readCorpusJson("seeds.json") as { seeds: Seed[] }).seeds;
const DERIVED = (
  readCorpusJson("derived-fields.json") as {
    derived: Record<string, DerivedEntry>;
  }
).derived;
const SEEDS_BY_ID = new Map(SEEDS.map((seed) => [seed.id, seed]));

function parentSeedOf(seed: Seed | undefined): Seed | undefined {
  const id = seed?.parentSeedId;
  if (id === undefined || id === null) return undefined;
  const parent = SEEDS_BY_ID.get(id);
  if (parent === undefined) {
    throw new Error(`seeds.json: "${seed?.id}" names unknown parent "${id}"`);
  }
  return parent;
}

// -- the store: one Chroma record per seed, scalars as metadata -----------------------------------
//
// Chroma metadata holds only finite numbers, strings and booleans. A NULL column therefore writes
// no key at all, which is the missing-attribute convention `resources.json` records for it. List
// and object attributes (`tags`, `aNumberList`, `categories`, …) are not stored (#475), and neither
// are `owner` and `coOwner`, which `resources.json` sends as an explicit null on some rows — a null
// Chroma cannot hold. The mapping names none of them, so every shape that reads one is refused
// during translation: by the pushdown, and by the post-filter, which reads declared keys only.

function metadataFor(seed: Seed): Metadata {
  const derived = DERIVED[seed.id];
  if (derived === undefined) {
    throw new Error(`derived-fields.json has no entry for seed "${seed.id}"`);
  }
  const metadata: Metadata = {
    // Chroma's `where` filters metadata only, so the id is mirrored into a key for `R.id`.
    id: seed.id,
  };
  // `obj.inner` mirrors aString in the corpus resource. Every scalar can be NULL (seeds j1, j2 and
  // j3 each leave one of the first three out), and a NULL column writes no key.
  const scalars: [string, string | number | boolean | null][] = [
    ["aBool", seed.aBool],
    ["aString", seed.aString],
    ["aNumber", seed.aNumber],
    ["obj.inner", seed.aString],
    ["aOptionalString", seed.aOptionalString],
  ];
  for (const [key, value] of scalars) {
    if (value !== null) metadata[key] = value;
  }
  for (const key of [
    "aDouble",
    "createdAt",
    "updatedAt",
    "createdBy",
    "scope",
  ] as const) {
    const value = derived[key];
    if (value !== null) metadata[key] = value;
  }
  // The real to-one chain (conformance/README.md, "The dataset"), flattened onto dotted keys. A
  // level that does not exist writes no key, which is the missing `parent` / `parent.inner` path.
  const parent = parentSeedOf(seed);
  const levels: [string, Seed | undefined][] = [
    ["parent", parent],
    ["parent.inner", parentSeedOf(parent)],
  ];
  for (const [prefix, level] of levels) {
    if (level === undefined) continue;
    for (const key of [
      "aBool",
      "aString",
      "aNumber",
      "aOptionalString",
    ] as const) {
      const value = level[key];
      if (value !== null) metadata[`${prefix}.${key}`] = value;
    }
  }
  return metadata;
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

let collection: Collection | undefined;

/**
 * Every case is translated with `allowPostFilter: true`, as the convex harness does: the harness
 * uses one call for every case, and the opted-in call is the one that can answer the most of them.
 * The default-off behaviour is a caller-supplied argument the corpus cannot vary, so it is pinned
 * in `translator.test.ts` instead.
 */
function translate(golden: Golden) {
  return queryPlanToChromaDB({
    queryPlan: planOf(golden),
    fieldNameMapper: FIELD_NAME_MAPPER,
    allowPostFilter: true,
  });
}

async function selectAllowed(golden: Golden): Promise<string[]> {
  const result = translate(golden);
  if (result.kind === PlanKind.ALWAYS_DENIED) return [];
  const rows = await collection!.get({
    where: result.kind === PlanKind.CONDITIONAL ? result.filters : undefined,
    include: ["metadatas"],
  });
  const { postFilter } = result;
  // The post-filter is part of the authorization predicate: it is applied to every record the
  // `where` returned, exactly as a caller must.
  return rows.ids
    .filter((_id, index) => !postFilter || postFilter(rows.metadatas[index]))
    .sort();
}

const TAGS = pdpTags();
const GOLDENS = new Map(TAGS.map((tag) => [tag, readGoldens(tag)]));
const passed: Record<string, Record<string, number>> = {};
const totals: Record<string, Record<string, number>> = {};
const skipped: Record<string, Record<string, number>> = {};

beforeAll(async () => {
  await chroma.heartbeat();
  try {
    await chroma.deleteCollection({ name: COLLECTION_NAME });
  } catch (error: unknown) {
    if (!(error instanceof ChromaNotFoundError)) throw error;
  }
  collection = await chroma.createCollection({ name: COLLECTION_NAME });
  await collection.add({
    ids: SEEDS.map(({ id }) => id),
    embeddings: SEEDS.map(() => BASE_EMBEDDING),
    metadatas: SEEDS.map(metadataFor),
  });
}, 30_000);

afterAll(async () => {
  if (collection) await chroma.deleteCollection({ name: COLLECTION_NAME });
  const lines = TAGS.map((tag) => {
    const tiers = Object.keys(totals[tag] ?? {}).sort();
    return `${tag}: ${tiers
      .map(
        (tier) =>
          `${tier} ${passed[tag]?.[tier] ?? 0}/${totals[tag]?.[tier]}` +
          (skipped[tag]?.[tier] ? ` (${skipped[tag]?.[tier]} skipped)` : ""),
      )
      .join(", ")}`;
  });
  console.log(`conformance (chromadb) passed per tier\n${lines.join("\n")}`);
});

describe("conformance (chromadb)", () => {
  test("the ledger names no case without a golden file", () => {
    const ids = new Set(
      TAGS.flatMap((tag) => GOLDENS.get(tag)!.map((golden) => golden.id)),
    );
    expect(Object.keys(LEDGER).filter((id) => !ids.has(id))).toEqual([]);
  });

  // The ledger is read by nothing else, so a malformed entry would otherwise be silently treated
  // as "no entry" (an unknown status) or scoped to a tag that never runs (a mistyped `pdp`).
  test("every ledger entry is well-formed", () => {
    const malformed = Object.entries(LEDGER).flatMap(([id, entry]) => {
      const problems: string[] = [];
      if (entry.status !== "unsupported" && entry.status !== "divergent") {
        problems.push(`unknown status ${JSON.stringify(entry.status)}`);
      }
      if (typeof entry.reason !== "string" || entry.reason.trim() === "") {
        problems.push("no reason");
      }
      if (entry.status === "divergent" && !entry.issue) {
        problems.push("divergent without issue");
      }
      for (const pdp of entry.pdp ?? []) {
        if (!TAGS.includes(pdp)) problems.push(`unknown pdp tag ${pdp}`);
      }
      return problems.map((problem) => `${id}: ${problem}`);
    });
    expect(malformed).toEqual([]);
  });

  test("every golden file is recorded against the tag its directory names", () => {
    const misplaced = TAGS.flatMap((tag) =>
      GOLDENS.get(tag)!
        .filter((golden) => golden.pdp !== tag)
        .map((golden) => `${tag}/${golden.id}: pdp ${golden.pdp}`),
    );
    expect(misplaced).toEqual([]);
    for (const tag of TAGS) expect(GOLDENS.get(tag)!.length).toBeGreaterThan(0);
  });

  for (const tag of TAGS) {
    // A plannerDivergence is a PDP bug no adapter can pass. The generator records it only on the
    // golden files of the tags it applies to, so a non-null value is the whole skip condition.
    const cases = GOLDENS.get(tag)!.filter(
      (golden) =>
        golden.plannerDivergence === null ||
        golden.plannerDivergence === undefined,
    );

    test.each(cases.map((golden) => [golden.id, golden] as const))(
      `${tag} %s`,
      async (_id, golden) => {
        const entry = ledgerEntry(golden, tag);
        if (entry?.status === "unsupported") {
          expect(() => translate(golden)).toThrow(UnsupportedOperatorError);
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

    // The total is every golden file in the tier, skipped planner divergences included.
    for (const golden of GOLDENS.get(tag)!) {
      (totals[tag] ??= {})[golden.tier] = (totals[tag]?.[golden.tier] ?? 0) + 1;
      if (!cases.includes(golden)) {
        (skipped[tag] ??= {})[golden.tier] =
          (skipped[tag]?.[golden.tier] ?? 0) + 1;
      }
    }
  }
});
