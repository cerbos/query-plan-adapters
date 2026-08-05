import * as fs from "node:fs";
import * as path from "node:path";

import { afterAll, beforeAll, describe, expect, jest, test } from "@jest/globals";
import type { Principal, Resource, Value } from "@cerbos/core";
import { GRPC as Cerbos } from "@cerbos/grpc";
import {
  ChromaClient,
  ChromaNotFoundError,
  type Collection,
  type Metadata,
} from "chromadb";

import { PlanKind, queryPlanToChromaDB } from ".";
import type { FieldNameMapperConfig } from ".";

/**
 * Shared-corpus differential suite for the flat scalar subset Chroma metadata filters can
 * represent. Every supported action follows the complete production path:
 *
 *   pinned Cerbos PlanResources -> queryPlanToChromaDB -> real Chroma query
 *
 * The resulting IDs are compared with per-row checkResource decisions from the same PDP. Shapes
 * outside Chroma's scalar filter model must fail during translation, before a malformed or
 * silently incomplete Where filter reaches Chroma.
 */

jest.setTimeout(120_000);

const CERBOS_PORT = 3641;
const cerbos = new Cerbos(`127.0.0.1:${CERBOS_PORT}`, { tls: false });
const chromaUrl = new URL(
  process.env["CHROMA_URL"] ?? "http://127.0.0.1:8234",
);
const chroma = new ChromaClient({
  host: chromaUrl.hostname,
  port: Number(chromaUrl.port) || 8000,
});
const CONFORMANCE_DIR = path.join(__dirname, "..", "..", "conformance");
const COLLECTION_NAME = "adapter-adversarial-tests";
const BASE_EMBEDDING = [0.1, 0.2, 0.3, 0.4];

type JsonRecord = Record<string, unknown>;

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
}

interface SeedsFile {
  principal: Principal;
  resourceKind: string;
  seeds: Seed[];
}

interface UnsupportedShape {
  action: string;
  shape: string;
}

interface ActionsFile {
  conformance: string[];
  adapterUnsupported: Record<string, UnsupportedAction[]>;
  adapterSupportedExpected: Record<string, UnsupportedAction[]>;
  expectedUnsupported: UnsupportedShape[];
  knownDivergences: KnownDivergence[];
}

interface UnsupportedAction {
  action: string;
  reason: string;
}

interface KnownDivergence {
  action: string;
  adapters: string[];
}

function isRecord(value: unknown): value is JsonRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function requireRecord(value: unknown, label: string): JsonRecord {
  if (!isRecord(value)) {
    throw Error(`${label} must be an object`);
  }
  return value;
}

function requireString(value: unknown, label: string): string {
  if (typeof value !== "string") {
    throw Error(`${label} must be a string`);
  }
  return value;
}

function requireBoolean(value: unknown, label: string): boolean {
  if (typeof value !== "boolean") {
    throw Error(`${label} must be a boolean`);
  }
  return value;
}

function requireNumber(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw Error(`${label} must be a finite number`);
  }
  return value;
}

function requireArray(value: unknown, label: string): unknown[] {
  if (!Array.isArray(value)) {
    throw Error(`${label} must be an array`);
  }
  return value;
}

function parseStringArray(value: unknown, label: string): string[] {
  return requireArray(value, label).map((item, index) =>
    requireString(item, `${label}[${index}]`),
  );
}

function parseTag(value: unknown, label: string): Tag {
  const tag = requireRecord(value, label);
  const name = tag["name"];
  if (name !== null && typeof name !== "string") {
    throw Error(`${label}.name must be a string or null`);
  }
  return {
    id: requireString(tag["id"], `${label}.id`),
    name,
  };
}

function parseSeed(value: unknown, index: number): Seed {
  const label = `seeds[${index}]`;
  const seed = requireRecord(value, label);
  const optional = seed["aOptionalString"];
  if (optional !== null && typeof optional !== "string") {
    throw Error(`${label}.aOptionalString must be a string or null`);
  }
  return {
    id: requireString(seed["id"], `${label}.id`),
    aBool: requireBoolean(seed["aBool"], `${label}.aBool`),
    aString: requireString(seed["aString"], `${label}.aString`),
    aNumber: requireNumber(seed["aNumber"], `${label}.aNumber`),
    aOptionalString: optional,
    tags: requireArray(seed["tags"], `${label}.tags`).map((tag, tagIndex) =>
      parseTag(tag, `${label}.tags[${tagIndex}]`),
    ),
    subCategoryNames: parseStringArray(
      seed["subCategoryNames"],
      `${label}.subCategoryNames`,
    ),
  };
}

function parseSeedsFile(value: unknown): SeedsFile {
  const file = requireRecord(value, "seeds file");
  const principal = requireRecord(file["principal"], "principal");
  // Carry every principal attribute through verbatim. Projecting to a known
  // subset here would silently drop any attribute a newly added corpus action
  // depends on: the plan would fold to ALWAYS_DENIED and the oracle — built
  // from the same projected principal — would agree, so the action would pass
  // vacuously instead of exercising the shape it was added for.
  const principalAttr = requireRecord(principal["attr"], "principal.attr");
  return {
    principal: {
      id: requireString(principal["id"], "principal.id"),
      roles: parseStringArray(principal["roles"], "principal.roles"),
      attr: principalAttr as Record<string, Value>,
    },
    resourceKind: requireString(file["resourceKind"], "resourceKind"),
    seeds: requireArray(file["seeds"], "seeds").map(parseSeed),
  };
}

function parseUnsupportedShape(value: unknown, index: number): UnsupportedShape {
  const shape = requireRecord(value, `expectedUnsupported[${index}]`);
  return {
    action: requireString(shape["action"], `expectedUnsupported[${index}].action`),
    shape: requireString(shape["shape"], `expectedUnsupported[${index}].shape`),
  };
}

function parseUnsupportedAction(
  value: unknown,
  label: string,
): UnsupportedAction {
  const entry = requireRecord(value, label);
  return {
    action: requireString(entry["action"], `${label}.action`),
    reason: requireString(entry["reason"], `${label}.reason`),
  };
}

function parseAdapterMap(
  value: unknown,
  label: string,
): Record<string, UnsupportedAction[]> {
  const adapters = requireRecord(value, label);
  const result: Record<string, UnsupportedAction[]> = {};
  for (const [adapter, entries] of Object.entries(adapters)) {
    result[adapter] = requireArray(entries, `${label}.${adapter}`).map(
      (entry, index) =>
        parseUnsupportedAction(entry, `${label}.${adapter}[${index}]`),
    );
  }
  return result;
}

function parseKnownDivergence(
  value: unknown,
  index: number,
): KnownDivergence {
  const label = `knownDivergences[${index}]`;
  const entry = requireRecord(value, label);
  return {
    action: requireString(entry["action"], `${label}.action`),
    adapters: parseStringArray(entry["adapters"], `${label}.adapters`),
  };
}

function parseActionsFile(value: unknown): ActionsFile {
  const file = requireRecord(value, "actions file");
  return {
    conformance: parseStringArray(file["conformance"], "conformance"),
    adapterUnsupported: parseAdapterMap(
      file["adapterUnsupported"],
      "adapterUnsupported",
    ),
    adapterSupportedExpected: parseAdapterMap(
      file["adapterSupportedExpected"],
      "adapterSupportedExpected",
    ),
    expectedUnsupported: requireArray(
      file["expectedUnsupported"],
      "expectedUnsupported",
    ).map(parseUnsupportedShape),
    knownDivergences: requireArray(
      file["knownDivergences"],
      "knownDivergences",
    ).map(parseKnownDivergence),
  };
}

function readJson(fileName: string): unknown {
  return JSON.parse(fs.readFileSync(path.join(CONFORMANCE_DIR, fileName), "utf8"));
}

const seedsFile = parseSeedsFile(readJson("seeds.json"));
const actionsFile = parseActionsFile(readJson("actions.json"));
const SEEDS = seedsFile.seeds;

const CHROMA_UNSUPPORTED =
  actionsFile.adapterUnsupported["langchain-chromadb"] ?? [];
const CHROMA_UNSUPPORTED_ACTIONS = new Set(
  CHROMA_UNSUPPORTED.map(({ action }) => action),
);
const CHROMA_SUPPORTED_EXPECTED =
  actionsFile.adapterSupportedExpected["langchain-chromadb"] ?? [];
const CHROMA_SUPPORTED_EXPECTED_ACTIONS = new Set(
  CHROMA_SUPPORTED_EXPECTED.map(({ action }) => action),
);
const CHROMA_SUPPORTED_ACTIONS = [
  ...actionsFile.conformance.filter(
    (action) => !CHROMA_UNSUPPORTED_ACTIONS.has(action),
  ),
  ...CHROMA_SUPPORTED_EXPECTED_ACTIONS,
];
const CHROMA_DIVERGENCES = new Set(
  actionsFile.knownDivergences
    .filter(({ adapters }) => adapters.includes("langchain-chromadb"))
    .map(({ action }) => action),
);
const THROWING_ACTIONS: UnsupportedAction[] = [
  ...CHROMA_UNSUPPORTED,
  ...actionsFile.expectedUnsupported
    .filter(({ action }) => !CHROMA_SUPPORTED_EXPECTED_ACTIONS.has(action))
    .map(({ action, shape }) => ({
      action,
      reason: shape,
    })),
];
const MANIFEST_ACTIONS = new Set([
  ...actionsFile.conformance,
  ...actionsFile.expectedUnsupported.map(({ action }) => action),
  ...CHROMA_DIVERGENCES,
]);

// Fields are optional unless declared otherwise, so `$ne`/`$nin` are rejected by default.
// `required: true` is asserted only for the metadata keys that `metadataFor` writes for every
// seed in conformance/seeds.json. `aOptionalString` is null for a2/a4/a8/c2/e1, so it stays
// optional and its inequality shapes remain fail-closed.
const FIELD_NAME_MAPPER: Record<string, string | FieldNameMapperConfig> = {
  "request.resource.attr.aBool": { field: "aBool", required: true },
  "request.resource.attr.aString": { field: "aString", required: true },
  "request.resource.attr.aNumber": {
    field: "aNumber",
    numericType: "integer",
    required: true,
  },
  "request.resource.attr.aOptionalString": {
    field: "aOptionalString",
    required: false,
  },
  "request.resource.attr.obj.inner": { field: "obj.inner", required: true },
};

function doubleFor(seed: Seed): number | null {
  switch (seed.id) {
    case "a1":
      return -0.6;
    case "a2":
      return 0.25;
    case "a3":
      return null;
    default:
      return seed.aNumber + 0.3;
  }
}

function labelsFor(seed: Seed): (string | null)[] {
  switch (seed.id) {
    case "a1":
      return ["gold", "silver"];
    case "a6":
      return [null, "silver"];
    case "a8":
      return ["silver"];
    case "c1":
      return ["Gold"];
    default:
      return [];
  }
}

function isoFor(seed: Seed): string {
  return seed.aNumber >= 2
    ? "2024-06-01T00:00:00Z"
    : "2026-06-01T00:00:00Z";
}

function timestampFor(seed: Seed): string | null {
  switch (seed.id) {
    case "a1":
      return "2020-03-15T10:30:00Z";
    case "a2":
      return "2037-01-01T00:00:00Z";
    case "a3":
      return null;
    case "a4":
      return "2024-06-01T00:00:00Z";
    case "a5":
      return "2020-03-15T10:30:00.123456Z";
    default:
      return seed.aNumber >= 2
        ? "2036-06-06T06:06:06Z"
        : "2021-05-05T05:05:05Z";
  }
}

function scopeFor(seed: Seed): string | null {
  switch (seed.id) {
    case "a1":
      return "dept";
    case "a2":
      return "dept.eng";
    case "a3":
      return "dept.eng.platform";
    case "a4":
      return "dept.eng.platform.obs";
    case "a5":
      return "dept.engineering";
    case "a6":
      return "dept.sales";
    case "a8":
      return "";
    case "a9":
      return "50%";
    case "b1":
      return "50%:a_b:x";
    case "b2":
      return "50x:a_b:y";
    case "b3":
      return "50%:aXb:y";
    case "b4":
      return "50%:a_b";
    case "b5":
      return "dept.eng.platform2";
    case "b6":
      return "50%.a_b";
    case "c1":
      return "Dept.Eng";
    case "c2":
      return "dept.eng.";
    case "d1":
      return "[env]:prod:eu";
    case "d2":
      return "e:prod:eu";
    default:
      return null;
  }
}

function tagAttribute(tag: Tag): Record<string, Value> {
  const attr: Record<string, Value> = { id: tag.id };
  if (tag.name !== null) {
    attr["name"] = tag.name;
  }
  return attr;
}

function checkResource(seed: Seed): Resource {
  const attr: Record<string, Value> = {
    aBool: seed.aBool,
    aString: seed.aString,
    aNumber: seed.aNumber,
    createdBy: isoFor(seed),
    owner: seed.aOptionalString,
    tagNames: seed.tags.map((tag) => tag.name),
    obj: { inner: seed.aString },
    tags: seed.tags.map(tagAttribute),
    categories: seed.subCategoryNames.map((subCategoryName) => ({
      name: "business",
      subCategories: [
        {
          name: subCategoryName,
          labels: labelsFor(seed).map((name): Record<string, Value> =>
            name === null ? {} : { name },
          ),
        },
      ],
    })),
  };

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
  const createdAt = timestampFor(seed);
  if (createdAt !== null) {
    attr["createdAt"] = createdAt;
  }
  if (seed.subCategoryNames.length > 0) {
    attr["mainCategory"] = {
      name: "business",
      subCategories: seed.subCategoryNames.map((name) => ({ name })),
      subNames: seed.subCategoryNames,
    };
  }

  return { kind: seedsFile.resourceKind, id: seed.id, attr };
}

function metadataFor(seed: Seed): Metadata {
  const metadata: Metadata = {
    id: seed.id,
    aBool: seed.aBool,
    aString: seed.aString,
    aNumber: seed.aNumber,
    "obj.inner": seed.aString,
  };
  if (seed.aOptionalString !== null) {
    metadata["aOptionalString"] = seed.aOptionalString;
  }
  return metadata;
}

let collection: Collection | undefined;

function activeCollection(): Collection {
  if (!collection) {
    throw Error("Chroma collection is not initialized");
  }
  return collection;
}

beforeAll(async () => {
  await chroma.heartbeat();
  try {
    await chroma.deleteCollection({ name: COLLECTION_NAME });
  } catch (error: unknown) {
    if (!(error instanceof ChromaNotFoundError)) {
      throw error;
    }
  }

  collection = await chroma.createCollection({ name: COLLECTION_NAME });
  await collection.add({
    ids: SEEDS.map(({ id }) => id),
    embeddings: SEEDS.map(() => BASE_EMBEDDING),
    metadatas: SEEDS.map(metadataFor),
    documents: SEEDS.map(({ aString }) => aString),
  });
});

afterAll(async () => {
  if (collection) {
    await chroma.deleteCollection({ name: COLLECTION_NAME });
  }
});

async function planFor(action: string) {
  return cerbos.planResources({
    principal: seedsFile.principal,
    resource: { kind: seedsFile.resourceKind },
    action,
  });
}

async function oracleAllowedIds(action: string): Promise<string[]> {
  const ids: string[] = [];
  for (const seed of SEEDS) {
    const result = await cerbos.checkResource({
      principal: seedsFile.principal,
      resource: checkResource(seed),
      actions: [action],
    });
    if (result.isAllowed(action)) {
      ids.push(seed.id);
    }
  }
  return ids.sort();
}

async function adapterFilteredIds(action: string): Promise<string[]> {
  const translated = queryPlanToChromaDB({
    queryPlan: await planFor(action),
    fieldNameMapper: FIELD_NAME_MAPPER,
  });
  if (translated.kind === PlanKind.ALWAYS_DENIED) {
    return [];
  }

  const results = await activeCollection().query({
    queryEmbeddings: [BASE_EMBEDDING],
    where:
      translated.kind === PlanKind.CONDITIONAL
        ? translated.filters
        : undefined,
    nResults: SEEDS.length,
  });
  return [...(results.ids[0] ?? [])].sort();
}

describe("adversarial conformance corpus", () => {
  test("manifest assigns all 126 policy actions exactly one Chroma outcome", () => {
    const oracle = new Set(CHROMA_SUPPORTED_ACTIONS);
    const throwing = new Set(THROWING_ACTIONS.map(({ action }) => action));
    const misclassified = [...MANIFEST_ACTIONS].filter((action) => {
      const classificationCount = [
        oracle.has(action),
        throwing.has(action),
        CHROMA_DIVERGENCES.has(action),
      ].filter(Boolean).length;
      return classificationCount !== 1;
    });

    expect(MANIFEST_ACTIONS.size).toBe(126);
    expect(CHROMA_SUPPORTED_ACTIONS).toHaveLength(15);
    expect(oracle.size).toBe(CHROMA_SUPPORTED_ACTIONS.length);
    expect(CHROMA_UNSUPPORTED).toHaveLength(107);
    expect(CHROMA_SUPPORTED_EXPECTED).toHaveLength(0);
    expect(THROWING_ACTIONS).toHaveLength(110);
    expect(misclassified).toEqual([]);
  });

  test.each(CHROMA_SUPPORTED_ACTIONS)(
    "%s matches the check() oracle",
    async (action) => {
      const [oracle, filtered] = await Promise.all([
        oracleAllowedIds(action),
        adapterFilteredIds(action),
      ]);
      expect(filtered).toEqual(oracle);
    },
  );

  test.each(THROWING_ACTIONS)(
    "$action fails during translation ($reason)",
    async ({ action }) => {
      const queryPlan = await planFor(action);
      expect(() =>
        queryPlanToChromaDB({
          queryPlan,
          fieldNameMapper: FIELD_NAME_MAPPER,
        }),
      ).toThrow();
    },
  );

  test("pins the upstream has() planner over-grant", async () => {
    const action = "p-has";
    const queryPlan = await planFor(action);
    const oracle = await oracleAllowedIds(action);
    const allIds = SEEDS.map(({ id }) => id).sort();

    expect(CHROMA_DIVERGENCES.has(action)).toBe(true);
    expect(queryPlan.kind).toBe(PlanKind.ALWAYS_ALLOWED);
    expect(oracle.length).toBeGreaterThan(0);
    expect(oracle.length).toBeLessThan(allIds.length);
    expect(await adapterFilteredIds(action)).toEqual(allIds);
  });

  test("oracle is not degenerate", async () => {
    for (const action of ["vf-le", "nary-and", "p-in-null-multi", "pv-exists", "pv-all", "null-eq", "null-ne"]) {
      const ids = await oracleAllowedIds(action);
      expect(ids.length).toBeGreaterThan(0);
      expect(ids.length).toBeLessThan(SEEDS.length);
    }
  });
});
