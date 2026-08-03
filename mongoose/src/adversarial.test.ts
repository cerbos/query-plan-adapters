import * as fs from "node:fs";
import * as path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "@jest/globals";
import type { Principal, Resource, Value } from "@cerbos/core";
import { GRPC as Cerbos } from "@cerbos/grpc";
import mongoose, { model, Schema } from "mongoose";

import { PlanKind, queryPlanToMongoose } from ".";
import type { Mapper, MapperConfig } from ".";

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

interface AdapterEntry {
  action: string;
  reason: string;
}

interface ExpectedUnsupportedEntry {
  action: string;
  shape: string;
}

interface KnownDivergence {
  action: string;
  adapters: string[];
}

interface ActionsFile {
  conformance: string[];
  adapterUnsupported: Record<string, AdapterEntry[]>;
  adapterSupportedExpected: Record<string, AdapterEntry[]>;
  expectedUnsupported: ExpectedUnsupportedEntry[];
  knownDivergences: KnownDivergence[];
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function expectRecord(value: unknown, label: string): Record<string, unknown> {
  if (!isRecord(value)) {
    throw new Error(`${label} must be an object`);
  }
  return value;
}

function expectString(value: unknown, label: string): string {
  if (typeof value !== "string") {
    throw new Error(`${label} must be a string`);
  }
  return value;
}

function expectBoolean(value: unknown, label: string): boolean {
  if (typeof value !== "boolean") {
    throw new Error(`${label} must be a boolean`);
  }
  return value;
}

function expectNumber(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`${label} must be a finite number`);
  }
  return value;
}

function expectStringArray(value: unknown, label: string): string[] {
  if (
    !Array.isArray(value) ||
    !value.every((entry): entry is string => typeof entry === "string")
  ) {
    throw new Error(`${label} must be an array of strings`);
  }
  return value;
}

function isValue(value: unknown): value is Value {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return true;
  }
  if (Array.isArray(value)) {
    return value.every(isValue);
  }
  return isRecord(value) && Object.values(value).every(isValue);
}

function parsePrincipal(value: unknown): Principal {
  const record = expectRecord(value, "principal");
  const attrRecord = expectRecord(record["attr"], "principal.attr");
  const attr: Record<string, Value> = {};
  for (const [key, entry] of Object.entries(attrRecord)) {
    if (!isValue(entry)) {
      throw new Error(`principal.attr.${key} is not a Cerbos value`);
    }
    attr[key] = entry;
  }
  return {
    id: expectString(record["id"], "principal.id"),
    roles: expectStringArray(record["roles"], "principal.roles"),
    attr,
  };
}

function parseTag(value: unknown, label: string): Tag {
  const record = expectRecord(value, label);
  const name = record["name"];
  if (name !== null && typeof name !== "string") {
    throw new Error(`${label}.name must be a string or null`);
  }
  return {
    id: expectString(record["id"], `${label}.id`),
    name,
  };
}

function parseSeed(value: unknown, index: number): Seed {
  const label = `seeds[${index}]`;
  const record = expectRecord(value, label);
  const optional = record["aOptionalString"];
  if (optional !== null && typeof optional !== "string") {
    throw new Error(`${label}.aOptionalString must be a string or null`);
  }
  const tags = record["tags"];
  if (!Array.isArray(tags)) {
    throw new Error(`${label}.tags must be an array`);
  }
  return {
    id: expectString(record["id"], `${label}.id`),
    aBool: expectBoolean(record["aBool"], `${label}.aBool`),
    aString: expectString(record["aString"], `${label}.aString`),
    aNumber: expectNumber(record["aNumber"], `${label}.aNumber`),
    aOptionalString: optional,
    tags: tags.map((tag, tagIndex) =>
      parseTag(tag, `${label}.tags[${tagIndex}]`)
    ),
    subCategoryNames: expectStringArray(
      record["subCategoryNames"],
      `${label}.subCategoryNames`
    ),
  };
}

function readJson(file: string): unknown {
  return JSON.parse(fs.readFileSync(path.join(CONFORMANCE_DIR, file), "utf8"));
}

function parseSeedsFile(value: unknown): SeedsFile {
  const record = expectRecord(value, "seeds.json");
  const seeds = record["seeds"];
  if (!Array.isArray(seeds)) {
    throw new Error("seeds.json.seeds must be an array");
  }
  return {
    principal: parsePrincipal(record["principal"]),
    resourceKind: expectString(record["resourceKind"], "resourceKind"),
    seeds: seeds.map(parseSeed),
  };
}

function parseAdapterEntry(value: unknown, label: string): AdapterEntry {
  const record = expectRecord(value, label);
  return {
    action: expectString(record["action"], `${label}.action`),
    reason: expectString(record["reason"], `${label}.reason`),
  };
}

function parseAdapterMap(
  value: unknown,
  label: string
): Record<string, AdapterEntry[]> {
  if (value === undefined) {
    return {};
  }
  const record = expectRecord(value, label);
  const result: Record<string, AdapterEntry[]> = {};
  for (const [adapter, entries] of Object.entries(record)) {
    if (!Array.isArray(entries)) {
      throw new Error(`${label}.${adapter} must be an array`);
    }
    result[adapter] = entries.map((entry, index) =>
      parseAdapterEntry(entry, `${label}.${adapter}[${index}]`)
    );
  }
  return result;
}

function parseActionsFile(value: unknown): ActionsFile {
  const record = expectRecord(value, "actions.json");
  const expected = record["expectedUnsupported"];
  const divergences = record["knownDivergences"];
  if (!Array.isArray(expected) || !Array.isArray(divergences)) {
    throw new Error("actions.json classifications must be arrays");
  }
  return {
    conformance: expectStringArray(record["conformance"], "conformance"),
    adapterUnsupported: parseAdapterMap(
      record["adapterUnsupported"],
      "adapterUnsupported"
    ),
    adapterSupportedExpected: parseAdapterMap(
      record["adapterSupportedExpected"],
      "adapterSupportedExpected"
    ),
    expectedUnsupported: expected.map((entry, index) => {
      const parsed = expectRecord(entry, `expectedUnsupported[${index}]`);
      return {
        action: expectString(
          parsed["action"],
          `expectedUnsupported[${index}].action`
        ),
        shape: expectString(
          parsed["shape"],
          `expectedUnsupported[${index}].shape`
        ),
      };
    }),
    knownDivergences: divergences.map((entry, index) => {
      const parsed = expectRecord(entry, `knownDivergences[${index}]`);
      return {
        action: expectString(
          parsed["action"],
          `knownDivergences[${index}].action`
        ),
        adapters: expectStringArray(
          parsed["adapters"],
          `knownDivergences[${index}].adapters`
        ),
      };
    }),
  };
}

const seedsFile = parseSeedsFile(readJson("seeds.json"));
const actionsFile = parseActionsFile(readJson("actions.json"));
const SEEDS = seedsFile.seeds;

const unsupportedEntries = actionsFile.adapterUnsupported["mongoose"] ?? [];
const unsupportedActions = new Set(
  unsupportedEntries.map((entry) => entry.action)
);
const supportedExpectedEntries =
  actionsFile.adapterSupportedExpected["mongoose"] ?? [];
const supportedExpectedActions = new Set(
  supportedExpectedEntries.map((entry) => entry.action)
);
const divergenceActions = new Set(
  actionsFile.knownDivergences
    .filter((entry) => entry.adapters.includes("mongoose"))
    .map((entry) => entry.action)
);
const expectedUnsupportedActions = new Set(
  actionsFile.expectedUnsupported.map((entry) => entry.action)
);
const ORACLE_ACTIONS = [
  ...actionsFile.conformance.filter(
    (action) => !unsupportedActions.has(action)
  ),
  ...supportedExpectedActions,
].sort();
const THROWING_ACTIONS = [
  ...unsupportedEntries,
  ...actionsFile.expectedUnsupported
    .filter((entry) => !supportedExpectedActions.has(entry.action))
    .map((entry) => ({ action: entry.action, reason: entry.shape })),
].sort((left, right) => left.action.localeCompare(right.action));
const MANIFEST_ACTIONS = new Set([
  ...actionsFile.conformance,
  ...actionsFile.expectedUnsupported.map((entry) => entry.action),
  ...actionsFile.knownDivergences.map((entry) => entry.action),
]);

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

interface AdversarialResourceDocument {
  resourceId: string;
  aBool: boolean;
  aString: string;
  aNumber: number;
  aDouble: number | null;
  aOptionalString: string | null;
  createdBy: string;
  scope: string | null;
  createdAt: Date | null;
  tags: Tag[];
  categories: AdversarialCategory[];
}

const tagSchema = new Schema<Tag>(
  {
    id: { type: String, required: true },
    name: { type: String, default: null },
  },
  { _id: false, id: false }
);
const labelSchema = new Schema<AdversarialLabel>(
  { name: { type: String, default: null } },
  { _id: false, id: false }
);
const subCategorySchema = new Schema<AdversarialSubCategory>(
  {
    name: { type: String, required: true },
    labels: { type: [labelSchema], default: [] },
  },
  { _id: false, id: false }
);
const categorySchema = new Schema<AdversarialCategory>(
  {
    name: { type: String, required: true },
    subCategories: { type: [subCategorySchema], default: [] },
  },
  { _id: false, id: false }
);
const resourceSchema = new Schema<AdversarialResourceDocument>(
  {
    resourceId: { type: String, required: true, unique: true },
    aBool: { type: Boolean, required: true },
    // Mongoose's string `required` validator rejects the corpus's intentional empty string.
    aString: { type: String },
    aNumber: { type: Number, required: true },
    aDouble: { type: Number, default: null },
    aOptionalString: { type: String, default: null },
    createdBy: { type: String, required: true },
    scope: { type: String, default: null },
    createdAt: { type: Date, default: null },
    tags: { type: [tagSchema], default: [] },
    categories: { type: [categorySchema], default: [] },
  },
  { id: false }
);
const AdversarialResource = model<AdversarialResourceDocument>(
  "MongooseAdversarialResource",
  resourceSchema
);

const labelsMapping: MapperConfig = {
  relation: {
    name: "labels",
    type: "many",
    fields: {
      name: { field: "name", nullable: true },
    },
  },
};
const subCategoriesMapping: MapperConfig = {
  relation: {
    name: "subCategories",
    type: "many",
    field: "name",
    fields: {
      name: { field: "name" },
      labels: labelsMapping,
    },
  },
};
const MAPPER: Mapper = {
  "request.resource.attr.aBool": { field: "aBool" },
  "request.resource.attr.aString": { field: "aString" },
  "request.resource.attr.aNumber": { field: "aNumber" },
  "request.resource.attr.aDouble": { field: "aDouble", nullable: true },
  "request.resource.attr.aOptionalString": {
    field: "aOptionalString",
    nullable: true,
  },
  "request.resource.attr.createdBy": { field: "createdBy" },
  "request.resource.attr.scope": { field: "scope", nullable: true },
  "request.resource.attr.createdAt": {
    field: "createdAt",
    nullable: true,
  },
  "request.resource.attr.owner": { field: "aOptionalString" },
  "request.resource.attr.obj.inner": { field: "aString" },
  "request.resource.attr.tags": {
    relation: {
      name: "tags",
      type: "many",
      fields: {
        id: { field: "id" },
        name: { field: "name", nullable: true },
      },
    },
  },
  "request.resource.attr.tagNames": {
    relation: {
      name: "tags",
      type: "many",
      field: "name",
      fields: { name: { field: "name" } },
    },
  },
  "request.resource.attr.categories": {
    relation: {
      name: "categories",
      type: "many",
      fields: {
        name: { field: "name" },
        subCategories: subCategoriesMapping,
      },
    },
  },
  "request.resource.attr.mainCategory": {
    relation: {
      name: "categories",
      type: "many",
      fields: {
        name: { field: "name" },
        subCategories: subCategoriesMapping,
        subNames: subCategoriesMapping,
      },
    },
  },
  "request.resource.attr.mainCategory.subCategories": {
    relation: {
      name: "categories.subCategories",
      type: "many",
      fields: { name: { field: "name" } },
    },
  },
  "request.resource.attr.mainCategory.subNames": {
    relation: {
      name: "categories.subCategories",
      type: "many",
      field: "name",
      fields: { name: { field: "name" } },
    },
  },
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

function asCheckResource(seed: Seed): Resource {
  const attr: Record<string, Value> = {
    aBool: seed.aBool,
    aString: seed.aString,
    aNumber: seed.aNumber,
    createdBy: createdByFor(seed),
    owner: seed.aOptionalString,
    tagNames: seed.tags.map((tag) => tag.name),
    obj: { inner: seed.aString },
    tags: seed.tags.map(asTagAttribute),
    categories: seed.subCategoryNames.map((name) => ({
      name: "business",
      subCategories: [
        {
          name,
          labels: labelsFor(seed).map(asLabelAttribute),
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

beforeAll(async () => {
  await mongoose.connect("mongodb://127.0.0.1:27017/cerbos_mongoose_adversarial");
  await AdversarialResource.deleteMany({});
  await AdversarialResource.create(
    SEEDS.map((seed) => {
      const createdAt = timestampFor(seed);
      return {
        resourceId: seed.id,
        aBool: seed.aBool,
        aString: seed.aString,
        aNumber: seed.aNumber,
        aDouble: doubleFor(seed),
        aOptionalString: seed.aOptionalString,
        createdBy: createdByFor(seed),
        scope: scopeFor(seed),
        createdAt: createdAt === null ? null : new Date(createdAt),
        tags: seed.tags,
        categories: seed.subCategoryNames.map((name) => ({
          name: "business",
          subCategories: [
            {
              name,
              labels: labelsFor(seed).map((label) => ({ name: label })),
            },
          ],
        })),
      };
    })
  );
}, 30_000);

afterAll(async () => {
  cerbos.close();
  await mongoose.disconnect();
});

async function oracleAllowedIds(action: string): Promise<string[]> {
  const decisions = await Promise.all(
    SEEDS.map(async (seed) => {
      const result = await cerbos.checkResource({
        principal: seedsFile.principal,
        resource: asCheckResource(seed),
        actions: [action],
      });
      return { id: seed.id, allowed: result.isAllowed(action) };
    })
  );
  return decisions
    .filter((decision) => decision.allowed)
    .map((decision) => decision.id)
    .sort();
}

async function adapterFilteredIds(action: string): Promise<string[]> {
  const queryPlan = await cerbos.planResources({
    principal: seedsFile.principal,
    resource: { kind: seedsFile.resourceKind },
    action,
  });
  const result = queryPlanToMongoose({ queryPlan, mapper: MAPPER });
  if (result.kind === PlanKind.ALWAYS_DENIED) {
    return [];
  }
  const rows = await AdversarialResource.find(
    result.kind === PlanKind.CONDITIONAL ? result.filters : {}
  )
    .select({ resourceId: 1, _id: 0 })
    .lean()
    .exec();
  return rows.map((row) => row.resourceId).sort();
}

describe("adversarial conformance corpus", () => {
  test("manifest assigns all 117 actions exactly one Mongoose outcome", () => {
    const oracle = new Set(ORACLE_ACTIONS);
    const throwing = new Set(THROWING_ACTIONS.map((entry) => entry.action));
    const misclassified = [...MANIFEST_ACTIONS].filter((action) => {
      const count = [
        oracle.has(action),
        throwing.has(action),
        divergenceActions.has(action),
      ].filter(Boolean).length;
      return count !== 1;
    });

    expect(MANIFEST_ACTIONS.size).toBe(117);
    expect(unsupportedEntries).toHaveLength(29);
    expect(supportedExpectedEntries).toHaveLength(3);
    expect(ORACLE_ACTIONS).toHaveLength(87);
    expect(THROWING_ACTIONS).toHaveLength(29);
    expect(misclassified).toEqual([]);
    expect(
      [...supportedExpectedActions].filter(
        (action) => !expectedUnsupportedActions.has(action)
      )
    ).toEqual([]);
  });

  test.each(ORACLE_ACTIONS)("%s matches the check() oracle", async (action) => {
    const [oracle, filtered] = await Promise.all([
      oracleAllowedIds(action),
      adapterFilteredIds(action),
    ]);
    expect(filtered).toEqual(oracle);
  });

  test.each(THROWING_ACTIONS)(
    "$action fails loudly instead of silently mistranslating ($reason)",
    async ({ action }) => {
      await expect(adapterFilteredIds(action)).rejects.toThrow();
    }
  );

  test("pins the upstream has() planner over-grant", async () => {
    const action = "p-has";
    const queryPlan = await cerbos.planResources({
      principal: seedsFile.principal,
      resource: { kind: seedsFile.resourceKind },
      action,
    });
    const oracle = await oracleAllowedIds(action);
    const allIds = SEEDS.map((seed) => seed.id).sort();

    expect(divergenceActions.has(action)).toBe(true);
    expect(queryPlan.kind).toBe(PlanKind.ALWAYS_ALLOWED);
    expect(oracle.length).toBeGreaterThan(0);
    expect(oracle.length).toBeLessThan(allIds.length);
    expect(await adapterFilteredIds(action)).toEqual(allIds);
  });

  test("oracle is not degenerate", async () => {
    for (const action of ["vf-le", "like-percent", "all-on-empty"]) {
      const ids = await oracleAllowedIds(action);
      expect(ids.length).toBeGreaterThan(0);
      expect(ids.length).toBeLessThan(SEEDS.length);
    }
  });
});
