import * as fs from "fs";
import * as path from "path";

import { afterAll, beforeAll, describe, expect, test } from "@jest/globals";
import type { Principal, Resource, Value } from "@cerbos/core";
import { GRPC as Cerbos } from "@cerbos/grpc";
import { ConvexHttpClient } from "convex/browser";

import { api } from "../convex/_generated/api.js";
import { PlanKind } from ".";

const CONVEX_URL = process.env["CONVEX_URL"] ?? "http://127.0.0.1:3210";
const convex = new ConvexHttpClient(CONVEX_URL);
const cerbos = new Cerbos("127.0.0.1:3593", { tls: false });
const CONFORMANCE_DIR = path.join(__dirname, "..", "..", "conformance");

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

interface ExpectedUnsupported {
  action: string;
}

interface AdapterOutcome {
  action: string;
  reason: string;
}

interface KnownDivergence {
  action: string;
  adapters: string[];
}

interface ActionsFile {
  conformance: string[];
  adapterUnsupported: Record<string, AdapterOutcome[]>;
  adapterSupportedExpected: Record<string, AdapterOutcome[]>;
  expectedUnsupported: ExpectedUnsupported[];
  knownDivergences: KnownDivergence[];
}

interface StoredDocument {
  id: string;
  aBool: boolean;
  aString: string;
  aNumber: number;
  aDouble?: number;
  aOptionalString?: string;
  createdBy: string;
  createdAt?: string;
  scope?: string;
  owner: string | null;
  tagNames: (string | null)[];
  obj: { inner: string };
  tags: { id: string; name?: string }[];
  categories: {
    name: string;
    subCategories: {
      name: string;
      labels: { name?: string }[];
    }[];
  }[];
  mainCategory?: {
    name: string;
    subCategories: { name: string }[];
    subNames: string[];
  };
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isStringArray = (value: unknown): value is string[] =>
  Array.isArray(value) && value.every((item) => typeof item === "string");

const isTag = (value: unknown): value is Tag =>
  isRecord(value) &&
  typeof value["id"] === "string" &&
  (typeof value["name"] === "string" || value["name"] === null);

const isSeed = (value: unknown): value is Seed =>
  isRecord(value) &&
  typeof value["id"] === "string" &&
  typeof value["aBool"] === "boolean" &&
  typeof value["aString"] === "string" &&
  typeof value["aNumber"] === "number" &&
  (typeof value["aOptionalString"] === "string" ||
    value["aOptionalString"] === null) &&
  Array.isArray(value["tags"]) &&
  value["tags"].every(isTag) &&
  isStringArray(value["subCategoryNames"]);

const isPrincipal = (value: unknown): value is Principal =>
  isRecord(value) &&
  typeof value["id"] === "string" &&
  isStringArray(value["roles"]) &&
  isRecord(value["attr"]);

const isSeedsFile = (value: unknown): value is SeedsFile =>
  isRecord(value) &&
  isPrincipal(value["principal"]) &&
  typeof value["resourceKind"] === "string" &&
  Array.isArray(value["seeds"]) &&
  value["seeds"].every(isSeed);

const isExpectedUnsupported = (
  value: unknown,
): value is ExpectedUnsupported =>
  isRecord(value) && typeof value["action"] === "string";

const isAdapterOutcome = (value: unknown): value is AdapterOutcome =>
  isRecord(value) &&
  typeof value["action"] === "string" &&
  typeof value["reason"] === "string";

const isAdapterMap = (
  value: unknown,
): value is Record<string, AdapterOutcome[]> =>
  isRecord(value) &&
  Object.values(value).every(
    (entries) => Array.isArray(entries) && entries.every(isAdapterOutcome),
  );

const isKnownDivergence = (value: unknown): value is KnownDivergence =>
  isRecord(value) &&
  typeof value["action"] === "string" &&
  isStringArray(value["adapters"]);

const isActionsFile = (value: unknown): value is ActionsFile =>
  isRecord(value) &&
  isStringArray(value["conformance"]) &&
  isAdapterMap(value["adapterUnsupported"]) &&
  isAdapterMap(value["adapterSupportedExpected"]) &&
  Array.isArray(value["expectedUnsupported"]) &&
  value["expectedUnsupported"].every(isExpectedUnsupported) &&
  Array.isArray(value["knownDivergences"]) &&
  value["knownDivergences"].every(isKnownDivergence);

const parsedSeeds: unknown = JSON.parse(
  fs.readFileSync(path.join(CONFORMANCE_DIR, "seeds.json"), "utf8"),
);
if (!isSeedsFile(parsedSeeds)) throw new Error("Invalid conformance seeds");
const seedsFile = parsedSeeds;

const parsedActions: unknown = JSON.parse(
  fs.readFileSync(path.join(CONFORMANCE_DIR, "actions.json"), "utf8"),
);
if (!isActionsFile(parsedActions)) throw new Error("Invalid conformance actions");
const actionsFile = parsedActions;

const CONVEX_UNSUPPORTED = actionsFile.adapterUnsupported["convex"] ?? [];
const UNSUPPORTED_ACTIONS = new Set(
  CONVEX_UNSUPPORTED.map(({ action }) => action),
);
const CONVEX_SUPPORTED_EXPECTED =
  actionsFile.adapterSupportedExpected["convex"] ?? [];
const SUPPORTED_EXPECTED_ACTIONS = new Set(
  CONVEX_SUPPORTED_EXPECTED.map(({ action }) => action),
);
const ORACLE_ACTIONS = [
  ...actionsFile.conformance.filter(
    (action) => !UNSUPPORTED_ACTIONS.has(action),
  ),
  ...SUPPORTED_EXPECTED_ACTIONS,
].sort();
const THROWING_ACTIONS = [
  ...CONVEX_UNSUPPORTED.map(({ action }) => action),
  ...actionsFile.expectedUnsupported
    .map(({ action }) => action)
    .filter((action) => !SUPPORTED_EXPECTED_ACTIONS.has(action)),
].sort();
const KNOWN_DIVERGENCES = new Set(
  actionsFile.knownDivergences
    .filter((entry) => entry.adapters.includes("convex"))
    .map((entry) => entry.action),
);

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

function createdByFor(seed: Seed): string {
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
  const scopes: Record<string, string> = {
    a1: "dept",
    a2: "dept.eng",
    a3: "dept.eng.platform",
    a4: "dept.eng.platform.obs",
    a5: "dept.engineering",
    a6: "dept.sales",
    a8: "",
    a9: "50%",
    b1: "50%:a_b:x",
    b2: "50x:a_b:y",
    b3: "50%:aXb:y",
    b4: "50%:a_b",
    b5: "dept.eng.platform2",
    b6: "50%.a_b",
    c1: "Dept.Eng",
    c2: "dept.eng.",
    d1: "[env]:prod:eu",
    d2: "e:prod:eu",
  };
  return scopes[seed.id] ?? null;
}

function storedDocument(seed: Seed): StoredDocument {
  const document: StoredDocument = {
    id: seed.id,
    aBool: seed.aBool,
    aString: seed.aString,
    aNumber: seed.aNumber,
    createdBy: createdByFor(seed),
    owner: seed.aOptionalString,
    tagNames: seed.tags.map((tag) => tag.name),
    obj: { inner: seed.aString },
    tags: seed.tags.map((tag) =>
      tag.name === null ? { id: tag.id } : { id: tag.id, name: tag.name },
    ),
    categories: seed.subCategoryNames.map((name) => ({
      name: "business",
      subCategories: [
        {
          name,
          labels: labelsFor(seed).map((labelName) =>
            labelName === null ? {} : { name: labelName },
          ),
        },
      ],
    })),
  };
  if (seed.aOptionalString !== null) {
    document.aOptionalString = seed.aOptionalString;
  }
  const double = doubleFor(seed);
  if (double !== null) document.aDouble = double;
  const timestamp = timestampFor(seed);
  if (timestamp !== null) document.createdAt = timestamp;
  const scope = scopeFor(seed);
  if (scope !== null) document.scope = scope;
  if (seed.subCategoryNames.length > 0) {
    document.mainCategory = {
      name: "business",
      subCategories: seed.subCategoryNames.map((name) => ({ name })),
      subNames: seed.subCategoryNames,
    };
  }
  return document;
}

function checkResource(seed: Seed): Resource {
  const attr: Record<string, Value> = {
    aBool: seed.aBool,
    aString: seed.aString,
    aNumber: seed.aNumber,
    createdBy: createdByFor(seed),
    owner: seed.aOptionalString,
    tagNames: seed.tags.map((tag) => tag.name),
    obj: { inner: seed.aString },
    tags: seed.tags.map((tag): Record<string, Value> =>
      tag.name === null ? { id: tag.id } : { id: tag.id, name: tag.name },
    ),
    categories: seed.subCategoryNames.map((name) => ({
      name: "business",
      subCategories: [
        {
          name,
          labels: labelsFor(seed).map((labelName): Record<string, Value> =>
            labelName === null ? {} : { name: labelName },
          ),
        },
      ],
    })),
  };
  if (seed.aOptionalString !== null) {
    attr["aOptionalString"] = seed.aOptionalString;
  }
  const double = doubleFor(seed);
  if (double !== null) attr["aDouble"] = double;
  const timestamp = timestampFor(seed);
  if (timestamp !== null) attr["createdAt"] = timestamp;
  const scope = scopeFor(seed);
  if (scope !== null) attr["scope"] = scope;
  if (seed.subCategoryNames.length > 0) {
    attr["mainCategory"] = {
      name: "business",
      subCategories: seed.subCategoryNames.map((name) => ({ name })),
      subNames: seed.subCategoryNames,
    };
  }
  return { kind: seedsFile.resourceKind, id: seed.id, attr };
}

async function oracleAllowedIds(action: string): Promise<string[]> {
  const ids: string[] = [];
  for (const seed of seedsFile.seeds) {
    const result = await cerbos.checkResource({
      principal: seedsFile.principal,
      resource: checkResource(seed),
      actions: [action],
    });
    if (result.isAllowed(action)) ids.push(seed.id);
  }
  return ids.sort();
}

async function adapterFilteredIds(action: string): Promise<string[]> {
  const queryPlan = await cerbos.planResources({
    principal: seedsFile.principal,
    resource: { kind: seedsFile.resourceKind },
    action,
  });
  if (queryPlan.kind === PlanKind.ALWAYS_DENIED) return [];
  return convex.query(api.adversarial.executePlan, {
    queryPlan: JSON.parse(JSON.stringify(queryPlan)),
  });
}

beforeAll(async () => {
  await convex.mutation(api.adversarial.deleteAll, {});
  for (const seed of seedsFile.seeds) {
    await convex.mutation(api.adversarial.insert, storedDocument(seed));
  }
});

afterAll(async () => {
  await convex.mutation(api.adversarial.deleteAll, {});
});

describe("adversarial conformance corpus", () => {
  test("assigns all policy actions exactly one Convex outcome", () => {
    const allActions = new Set([
      ...actionsFile.conformance,
      ...actionsFile.expectedUnsupported.map((entry) => entry.action),
      ...KNOWN_DIVERGENCES,
    ]);
    const oracle = new Set(ORACLE_ACTIONS);
    const throwing = new Set(THROWING_ACTIONS);
    const misclassified = [...allActions].filter(
      (action) =>
        [
          oracle.has(action),
          throwing.has(action),
          KNOWN_DIVERGENCES.has(action),
        ].filter(Boolean).length !== 1,
    );

    expect(allActions.size).toBe(120);
    expect(CONVEX_UNSUPPORTED).toHaveLength(0);
    expect(CONVEX_SUPPORTED_EXPECTED).toHaveLength(3);
    expect(ORACLE_ACTIONS).toHaveLength(119);
    expect(THROWING_ACTIONS).toHaveLength(0);
    expect(misclassified).toEqual([]);
  });

  test.each(ORACLE_ACTIONS)("%s matches the check() oracle", async (action) => {
    const [oracle, filtered] = await Promise.all([
      oracleAllowedIds(action),
      adapterFilteredIds(action),
    ]);
    expect(filtered).toEqual(oracle);
  });

  test("pins the upstream has() planner over-grant", async () => {
    const action = "p-has";
    const queryPlan = await cerbos.planResources({
      principal: seedsFile.principal,
      resource: { kind: seedsFile.resourceKind },
      action,
    });
    const oracle = await oracleAllowedIds(action);
    const allIds = seedsFile.seeds.map((seed) => seed.id).sort();

    expect(queryPlan.kind).toBe(PlanKind.ALWAYS_ALLOWED);
    expect(oracle.length).toBeGreaterThan(0);
    expect(oracle.length).toBeLessThan(allIds.length);
    expect(await adapterFilteredIds(action)).toEqual(allIds);
  });

  test("oracle is not degenerate", async () => {
    for (const action of ["vf-le", "like-percent", "all-on-empty"]) {
      const ids = await oracleAllowedIds(action);
      expect(ids.length).toBeGreaterThan(0);
      expect(ids.length).toBeLessThan(seedsFile.seeds.length);
    }
  });
});
