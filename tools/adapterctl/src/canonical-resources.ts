import { isDeepStrictEqual } from "node:util";

import { isRecord } from "./records.ts";
import type { CheckResource, CheckResources } from "./model.ts";

type CanonicalInputs = {
  principal: Record<string, unknown>;
  resourceKind: string;
  seeds: Record<string, unknown>[];
  derived: Record<string, Record<string, unknown>>;
};

export function validateCanonicalResources(args: {
  checkResources: CheckResources;
  seedsValue: unknown;
  derivedValue: unknown;
}): string[] {
  const errors: string[] = [];
  const inputs = decodeInputs(args.seedsValue, args.derivedValue, errors);
  if (inputs === undefined) return errors;
  if (!isDeepStrictEqual(args.checkResources.principal, inputs.principal)) {
    errors.push("conformance/check-resources.json.principal: does not match seeds.json principal");
  }
  const expected = inputs.seeds.map((seed) => materializeResource(seed, inputs));
  const actualById = new Map<string, CheckResource>();
  for (const resource of args.checkResources.resources) {
    if (actualById.has(resource.id)) {
      errors.push(`conformance/check-resources.json.resources: duplicate id ${resource.id}`);
    }
    actualById.set(resource.id, resource);
  }
  const expectedIds = expected.map((resource) => resource.id).sort();
  const actualIds = [...actualById.keys()].sort();
  if (!isDeepStrictEqual(actualIds, expectedIds)) {
    errors.push("conformance/check-resources.json.resources: ids must equal seeds.json ids");
  }
  for (const resource of expected) {
    const actual = actualById.get(resource.id);
    if (actual !== undefined && !isDeepStrictEqual(actual, resource)) {
      errors.push(
        `conformance/check-resources.json.resources[${resource.id}]: does not match canonical resource`,
      );
    }
  }
  return errors;
}

function decodeInputs(
  seedsValue: unknown,
  derivedValue: unknown,
  errors: string[],
): CanonicalInputs | undefined {
  if (!isRecord(seedsValue) || !isRecord(derivedValue)) {
    errors.push("conformance seeds and derived fields must be objects");
    return undefined;
  }
  const principal = seedsValue["principal"];
  const resourceKind = seedsValue["resourceKind"];
  const rawSeeds = seedsValue["seeds"];
  const rawDerived = derivedValue["derived"];
  if (!isRecord(principal) || typeof resourceKind !== "string" ||
      !Array.isArray(rawSeeds) || !isRecord(rawDerived)) {
    errors.push("conformance seeds or derived fields have an invalid canonical-resource shape");
    return undefined;
  }
  const seeds: Record<string, unknown>[] = [];
  for (const seed of rawSeeds) {
    if (!isRecord(seed)) {
      errors.push("conformance/seeds.json.seeds: expected objects");
      return undefined;
    }
    seeds.push(seed);
  }
  const derived: Record<string, Record<string, unknown>> = {};
  for (const [id, value] of Object.entries(rawDerived)) {
    if (!isRecord(value)) {
      errors.push(`conformance/derived-fields.json.derived.${id}: expected object`);
      return undefined;
    }
    derived[id] = value;
  }
  return { principal, resourceKind, seeds, derived };
}

function materializeResource(seed: Record<string, unknown>, inputs: CanonicalInputs): CheckResource {
  const id = requiredString(seed, "id");
  const derived = inputs.derived[id];
  if (derived === undefined) throw new Error(`derived fields missing seed ${id}`);
  const tags = requiredRecords(seed, "tags");
  const names = requiredStrings(seed, "subCategoryNames");
  const labels = requiredArray(derived, "labels");
  const attr: Record<string, unknown> = {
    aBool: seed["aBool"],
    aString: seed["aString"],
    aNumber: seed["aNumber"],
    createdBy: derived["createdBy"],
    owner: seed["aOptionalString"],
    coOwner: derived["scope"],
    tagNames: tags.map((tag) => tag["name"]),
    obj: { inner: seed["aString"] },
    tags: tags.map(materializeTag),
    categories: names.map((name) => ({
      name: "business",
      subCategories: [{
        name,
        labels: labels.map((label) => label === null ? {} : { name: label }),
      }],
    })),
  };
  copyUnlessNull(attr, "aOptionalString", seed["aOptionalString"]);
  copyUnlessNull(attr, "aDouble", derived["aDouble"]);
  copyUnlessNull(attr, "scope", derived["scope"]);
  copyUnlessNull(attr, "createdAt", derived["createdAt"]);
  if (names.length > 0) {
    attr["mainCategory"] = {
      name: "business",
      subCategories: names.map((name) => ({ name })),
      subNames: names,
    };
  }
  const byId = new Map(inputs.seeds.map((item) => [requiredString(item, "id"), item]));
  const parent = relatedSeed(seed, byId);
  if (parent !== undefined) {
    const parentAttr = materializeRelation(parent);
    const inner = relatedSeed(parent, byId);
    if (inner !== undefined) parentAttr["inner"] = materializeRelation(inner);
    attr["parent"] = parentAttr;
  }
  return { kind: inputs.resourceKind, id, attr };
}

function materializeTag(tag: Record<string, unknown>): Record<string, unknown> {
  const result: Record<string, unknown> = { id: tag["id"] };
  copyUnlessNull(result, "name", tag["name"]);
  return result;
}

function materializeRelation(seed: Record<string, unknown>): Record<string, unknown> {
  const result: Record<string, unknown> = {
    aBool: seed["aBool"],
    aString: seed["aString"],
    aNumber: seed["aNumber"],
  };
  copyUnlessNull(result, "aOptionalString", seed["aOptionalString"]);
  return result;
}

function relatedSeed(
  seed: Record<string, unknown>,
  byId: Map<string, Record<string, unknown>>,
): Record<string, unknown> | undefined {
  const parentId = seed["parentSeedId"];
  if (parentId === null) return undefined;
  if (typeof parentId !== "string") throw new Error("seed parentSeedId must be string or null");
  const parent = byId.get(parentId);
  if (parent === undefined) throw new Error(`seed names unknown parent ${parentId}`);
  return parent;
}

function copyUnlessNull(target: Record<string, unknown>, key: string, value: unknown): void {
  if (value !== null) target[key] = value;
}

function requiredString(value: Record<string, unknown>, key: string): string {
  const item = value[key];
  if (typeof item !== "string") throw new Error(`expected string ${key}`);
  return item;
}

function requiredArray(value: Record<string, unknown>, key: string): unknown[] {
  const item = value[key];
  if (!Array.isArray(item)) throw new Error(`expected array ${key}`);
  return item;
}

function requiredStrings(value: Record<string, unknown>, key: string): string[] {
  const items = requiredArray(value, key);
  const strings: string[] = [];
  for (const item of items) {
    if (typeof item !== "string") throw new Error(`expected string in ${key}`);
    strings.push(item);
  }
  return strings;
}

function requiredRecords(value: Record<string, unknown>, key: string): Record<string, unknown>[] {
  const items = requiredArray(value, key);
  const records: Record<string, unknown>[] = [];
  for (const item of items) {
    if (!isRecord(item)) throw new Error(`expected object in ${key}`);
    records.push(item);
  }
  return records;
}
