import * as fs from "node:fs";
import * as path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "@jest/globals";
import {
  PlanExpression,
  PlanExpressionValue,
  PlanExpressionVariable,
} from "@cerbos/core";
import type { PlanExpressionOperand, Principal, Value } from "@cerbos/core";
import { GRPC as Cerbos } from "@cerbos/grpc";
import mongoose, { model, Schema } from "mongoose";

import { PlanKind, queryPlanToMongoose } from ".";
import {
  MAPPER,
  assertKeys,
  expectBoolean,
  expectNumber,
  expectRecord,
  expectString,
  expectStringArray,
  isValue,
  readJson,
} from "./corpus";
import {
  loadActionControlPlane,
  loadCheckResources,
  requireOutcomeMessage,
} from "./controlPlane";

const cerbos = new Cerbos("127.0.0.1:3593", { tls: false });

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

// -- corpus coverage guards ---------------------------------------------------------------------
//
// The same parsed seed feeds the stored document AND the check() oracle, so a corpus field this
// harness does not consume is dropped from both sides at once and the differential agrees for the
// wrong reason — the projection trap conformance/README.md describes for adapterctl.json, applied to
// the seeds. Asserting set equality catches both directions: a corpus key nothing here reads, and a
// key this harness reads that the corpus no longer carries.

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
// nothing (conformance/README.md, "Adding a new hostile shape", step 7). parsePrincipal carries
// every attribute through verbatim, which is correct; the guard is what proves it still does.
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

/**
 * Asserted against the RAW json rather than the parsed seeds: parseSeed rebuilds each row field by
 * field, so a parsed seed can only ever carry the keys this harness already names and the
 * assertion would pass vacuously.
 */
function assertSeedKeyCoverage(value: unknown): void {
  const seeds = expectRecord(value, "seeds.json")["seeds"];
  if (!Array.isArray(seeds)) {
    throw new Error("seeds.json.seeds must be an array");
  }
  seeds.forEach((seed, index) => {
    const label = `seeds.json seeds[${index}]`;
    const record = expectRecord(seed, label);
    assertKeys(label, Object.keys(record), SEED_KEYS, [SEED_NOTE_KEY]);
    const tags = record["tags"];
    if (!Array.isArray(tags)) {
      throw new Error(`${label}.tags must be an array`);
    }
    tags.forEach((tag, tagIndex) => {
      const tagLabel = `${label}.tags[${tagIndex}]`;
      assertKeys(tagLabel, Object.keys(expectRecord(tag, tagLabel)), TAG_KEYS);
    });
  });
}

/**
 * Asserted against the RAW json for the same reason as the seed keys: parsePrincipal rebuilds `id`
 * and `roles`, so a rebuilt principal could only ever report the keys this harness already names.
 *
 * The attribute VALUES are asserted too. parsePrincipal accepts any Cerbos `Value`, but the corpus
 * carries exactly two shapes — a string and a list of strings — and every other harness converts on
 * that basis, so a third has to fail here rather than be reshaped by one adapter and passed through
 * by another. A key-set guard says nothing about a change inside a value, and three of the four
 * attributes are lists; this is the same reason the seed guard descends into `tags[]`.
 */
function assertPrincipalKeyCoverage(value: unknown): void {
  const principal = expectRecord(
    expectRecord(value, "seeds.json")["principal"],
    "seeds.json principal",
  );
  assertKeys("seeds.json principal", Object.keys(principal), PRINCIPAL_KEYS);
  const attr = expectRecord(principal["attr"], "seeds.json principal.attr");
  assertKeys(
    "seeds.json principal.attr",
    Object.keys(attr),
    PRINCIPAL_ATTR_KEYS,
  );
  for (const [key, entry] of Object.entries(attr)) {
    if (typeof entry === "string") continue;
    if (Array.isArray(entry) && entry.every((el) => typeof el === "string")) {
      continue;
    }
    throw new Error(
      `seeds.json principal.attr.${key} is neither a string nor an array of strings, the only two shapes this harness consumes: a reshaped principal attribute feeds the plan and the check() oracle at once`,
    );
  }
}

function parseDerivedEntry(value: unknown, label: string): DerivedEntry {
  const record = expectRecord(value, label);
  assertKeys(label, Object.keys(record), DERIVED_KEYS);
  const aDouble = record["aDouble"];
  if (aDouble !== null && typeof aDouble !== "number") {
    throw new Error(`${label}.aDouble must be a number or null`);
  }
  const createdAt = record["createdAt"];
  if (createdAt !== null && typeof createdAt !== "string") {
    throw new Error(`${label}.createdAt must be a string or null`);
  }
  const scope = record["scope"];
  if (scope !== null && typeof scope !== "string") {
    throw new Error(`${label}.scope must be a string or null`);
  }
  const labels = record["labels"];
  if (
    !Array.isArray(labels) ||
    !labels.every((entry) => entry === null || typeof entry === "string")
  ) {
    throw new Error(`${label}.labels must be an array of strings or nulls`);
  }
  return {
    createdBy: expectString(record["createdBy"], `${label}.createdBy`),
    aDouble,
    createdAt,
    scope,
    labels,
  };
}

function parseDerivedFile(value: unknown): DerivedFile {
  const record = expectRecord(value, "derived-fields.json");
  const fields = expectStringArray(
    record["fields"],
    "derived-fields.json fields",
  );
  assertKeys("derived-fields.json fields", fields, DERIVED_KEYS);
  const derived: Record<string, DerivedEntry> = {};
  for (const [id, entry] of Object.entries(
    expectRecord(record["derived"], "derived-fields.json derived"),
  )) {
    derived[id] = parseDerivedEntry(
      entry,
      `derived-fields.json derived["${id}"]`,
    );
  }
  return { fields, derived };
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
  const parentSeedId = record["parentSeedId"];
  if (parentSeedId !== null && typeof parentSeedId !== "string") {
    throw new Error(`${label}.parentSeedId must be a string or null`);
  }
  return {
    id: expectString(record["id"], `${label}.id`),
    aBool: expectBoolean(record["aBool"], `${label}.aBool`),
    aString: expectString(record["aString"], `${label}.aString`),
    aNumber: expectNumber(record["aNumber"], `${label}.aNumber`),
    aOptionalString: optional,
    tags: tags.map((tag, tagIndex) =>
      parseTag(tag, `${label}.tags[${tagIndex}]`),
    ),
    subCategoryNames: expectStringArray(
      record["subCategoryNames"],
      `${label}.subCategoryNames`,
    ),
    parentSeedId,
  };
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

const rawSeedsJson = readJson("seeds.json");
assertSeedKeyCoverage(rawSeedsJson);
assertPrincipalKeyCoverage(rawSeedsJson);
const seedsFile = parseSeedsFile(rawSeedsJson);
const derivedFile = parseDerivedFile(readJson("derived-fields.json"));
const SEEDS = seedsFile.seeds;

if (Object.keys(derivedFile.derived).length !== SEEDS.length) {
  throw new Error(
    `derived-fields.json has ${
      Object.keys(derivedFile.derived).length
    } entries for ${SEEDS.length} seeds`,
  );
}
for (const seed of SEEDS) {
  // Throws when the entry is missing.
  derivedFor(seed);
}

const ACTION_CONTROL_PLANE = loadActionControlPlane({
  adapter: "mongoose",
  selectedAction: process.env["ADAPTERCTL_ACTION"],
});
const FULL_MATRIX = process.env["ADAPTERCTL_ACTION"] === undefined;
const fullMatrixTest = FULL_MATRIX ? test : test.skip;
const CHECK_RESOURCES = loadCheckResources();
const ORACLE_ACTIONS = ACTION_CONTROL_PLANE.oracleActions;
const NULL_REPRESENTATION_OMITTED = ACTION_CONTROL_PLANE.throwingActions.filter(
  ({ action }) => action === "null-eq-missing",
);
const THROWING_ACTIONS = ACTION_CONTROL_PLANE.throwingActions.filter(
  ({ action }) => action !== "null-eq-missing",
);
const MANIFEST_ACTIONS = new Set(ACTION_CONTROL_PLANE.selectedActions);

/** The one message every null-carrying action must be rejected with under `omitted`. */
const NULL_OMITTED_MESSAGE = requireOutcomeMessage({
  controlPlane: ACTION_CONTROL_PLANE,
  action: "null-eq-missing",
});

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
  aBool: boolean;
  aString: string;
  aNumber: number;
  aOptionalString: string | null;
}

interface AdversarialParent extends AdversarialRelationLevel {
  inner: AdversarialRelationLevel | null;
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
  parent: AdversarialParent | null;
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
    aBool: { type: Boolean, required: true },
    // Mongoose's string `required` validator rejects the corpus's intentional empty string.
    aString: { type: String },
    aNumber: { type: Number, required: true },
    aOptionalString: { type: String, default: null },
  },
  { _id: false, id: false },
);
const parentSchema = new Schema<AdversarialParent>(
  {
    aBool: { type: Boolean, required: true },
    aString: { type: String },
    aNumber: { type: Number, required: true },
    aOptionalString: { type: String, default: null },
    inner: { type: innerSchema, default: null },
  },
  { _id: false, id: false },
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
    parent: { type: parentSchema, default: null },
  },
  { id: false },
);
const AdversarialResource = model<AdversarialResourceDocument>(
  "MongooseAdversarialResource",
  resourceSchema,
);

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

function createdByFor(seed: Seed): string {
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
// `parentSeedId` names the seed whose four scalars this row's `parent` carries, and that seed's own
// `parentSeedId` names the ones `parent.inner` carries. The chain is cut at two levels. Every
// resource owns a FRESH parent (and inner) subdocument rather than pointing at the named seed's own
// document, so no two resources share one and a filter that returned the parent instead of the
// child cannot agree with the oracle by accident.

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

beforeAll(async () => {
  await mongoose.connect(
    "mongodb://127.0.0.1:27017/cerbos_mongoose_adversarial",
  );
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
        parent: storedParent(seed),
      };
    }),
  );
}, 30_000);

afterAll(async () => {
  cerbos.close();
  await mongoose.disconnect();
});

async function oracleAllowedIds(action: string): Promise<string[]> {
  const decisions = await Promise.all(
    CHECK_RESOURCES.resources.map(async (resource) => {
      const result = await cerbos.checkResource({
        principal: CHECK_RESOURCES.principal,
        resource,
        actions: [action],
      });
      return { id: resource.id, allowed: result.isAllowed(action) };
    }),
  );
  return decisions
    .filter((decision) => decision.allowed)
    .map((decision) => decision.id)
    .sort();
}

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

async function adapterFilteredIds(
  action: string,
  nullAttributeRepresentation: "explicit" | "omitted" = "explicit",
): Promise<string[]> {
  const queryPlan = await cerbos.planResources({
    principal: CHECK_RESOURCES.principal,
    resource: { kind: seedsFile.resourceKind },
    action,
  });
  const result = queryPlanToMongoose({
    queryPlan,
    mapper: MAPPER,
    nullAttributeRepresentation,
  });
  if (result.kind === PlanKind.ALWAYS_DENIED) {
    return [];
  }
  const rows = await AdversarialResource.find(
    result.kind === PlanKind.CONDITIONAL ? result.filters : {},
  )
    .select({ resourceId: 1, _id: 0 })
    .lean()
    .exec();
  return rows.map((row) => row.resourceId).sort();
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

describe("adversarial conformance corpus", () => {
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

  // The plan is fetched OUTSIDE the assertion so a PDP failure fails the test instead of
  // passing it, and no query executes — the invariant is that the shape throws BEFORE a
  // filter exists, so MongoDB aborting a wrongly emitted pipeline at query time must not be
  // able to masquerade as the adapter refusing to translate.
  //
  // The message is asserted, not just the throw: a bare `toThrow()` is satisfied by a mapper
  // typo or an unrelated validation, which would leave the classification resting on a failure
  // that has nothing to do with the limitation it declares (cerbos/query-plan-adapters#326).
  test.each(THROWING_ACTIONS)(
    "$action fails during translation with the declared message, before any filter exists ($reason)",
    async ({ action, message }) => {
      await expectCatalogOracle(action);
      const queryPlan = await cerbos.planResources({
        principal: CHECK_RESOURCES.principal,
        resource: { kind: seedsFile.resourceKind },
        action,
      });
      expect(queryPlan.kind).toBe(PlanKind.CONDITIONAL);
      expect(() =>
        queryPlanToMongoose({
          queryPlan,
          mapper: MAPPER,
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

      const entry = THROWING_ACTIONS.find(
        ({ action }) => action === "filter-as-conjunct",
      );
      expect(entry).toBeDefined();
      await expect(adapterFilteredIds("filter-as-conjunct")).rejects.toThrow(
        entry?.message,
      );
    },
  );

  // #302. `null-eq-missing` probes `aOptionalString == null`, and `aOptionalString` follows the
  // corpus default: a NULL column sends NO attribute, so check() denies every document.
  //
  // Mongoose is the one adapter that already expresses this PER ATTRIBUTE: `nullable: true` on a
  // mapper entry declares "a stored null is a missing Cerbos attribute", and the resulting
  // `$exists`/`$ne: null` guards make `eq(field, null)` contradictory — the empty set the oracle
  // demands. `owner` maps to the SAME column without `nullable`, so `null-eq` still returns its
  // five explicit-null documents. Both are asserted here: the pair is what proves the mapper
  // flag, not the corpus, is doing the discriminating.

  test.each(NULL_REPRESENTATION_OMITTED)(
    "$action already aligns via the nullable mapper flag and is rejected under omitted ($reason)",
    async ({ action, message }) => {
      await expectCatalogOracle(action);
      expect(await adapterFilteredIds(action, "explicit")).toEqual([]);

      // The same column WITHOUT `nullable` keeps the explicit-null translation, so the empty
      // result above is the mapper flag talking, not a filter that matches nothing everywhere.
      if (FULL_MATRIX) {
        expect(await adapterFilteredIds("null-eq", "explicit")).toEqual(
          await expectCatalogOracle("null-eq"),
        );
      }

      // The global switch is still the fail-closed backstop for callers who omit attributes
      // without declaring `nullable` on every affected mapper entry.
      await expect(adapterFilteredIds(action, "omitted")).rejects.toThrow(
        message,
      );
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
          principal: CHECK_RESOURCES.principal,
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
          await adapterFilteredIds(action, "omitted");
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
        principal: CHECK_RESOURCES.principal,
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
  // `!(size(...) > 0)` — but the guard has to be a property of the chain rather than of the
  // two spellings that happen to be pinned. These synthesise the remaining
  // threshold/polarity combinations onto the same seeded collection and assert the parentless
  // documents stay out of every one, including an arbitrary-N threshold neither corpus action
  // reaches (cerbos/query-plan-adapters#316).
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
        const result = queryPlanToMongoose({
          queryPlan: {
            kind: PlanKind.CONDITIONAL,
            condition,
            cerbosCallId: "synthetic",
            requestId: "synthetic",
            validationErrors: [],
            metadata: undefined,
          },
          mapper: MAPPER,
        });
        expect(result.kind).toBe(PlanKind.CONDITIONAL);
        const rows = await AdversarialResource.find(
          result.kind === PlanKind.CONDITIONAL ? result.filters : {},
        )
          .select({ resourceId: 1, _id: 0 })
          .lean()
          .exec();
        return rows.map((row) => row.resourceId).sort();
      };

      // Every seed that HAS a mainCategory holds at least one subCategory, and the 16 without
      // it are CEL missing-path errors — so each of these is empty unless the guard leaks.
      const emptyByConstruction: [string, PlanExpressionOperand][] = [
        ["size(chain) == 0", compare("eq", 0)],
        ["size(chain) <= 0", compare("le", 0)],
        ["size(chain) >= 2", compare("ge", 2)],
        ["!(size(chain) > 0)", negate(compare("gt", 0))],
        ["!(size(chain) >= 1)", negate(compare("ge", 1))],
        ["!(size(chain) < 2)", negate(compare("lt", 2))],
      ];

      for (const [shape, condition] of emptyByConstruction) {
        expect([shape, await filteredIdsFor(condition)]).toEqual([shape, []]);
      }

      // The mirror image, so the loop above cannot pass by denying everything: `>= 0` and `< 2`
      // are TRUE for exactly the documents that HAVE the parent.
      const withParent = await oracleAllowedIds("w1-size-nonneg-chain");
      expect(withParent.length).toBeGreaterThan(0);
      expect(withParent.length).toBeLessThan(SEEDS.length);
      expect(await filteredIdsFor(compare("ge", 0))).toEqual(withParent);
      expect(await filteredIdsFor(compare("lt", 2))).toEqual(withParent);
    },
  );

  // The mapping-hazard contract in README.md ("Mapping hazards") rests on ONE structural fact:
  // this adapter builds no subquery. A relation is a path inside the same document, so the filter
  // and the application read the same document and the five subquery hazards
  // (conformance/README.md, "Mapping hazards: the rows the subquery sees") cannot arise. The day
  // the adapter reaches a second collection — a `$lookup` stage, a `populate()` call — every one
  // of them arrives at once and the README's "not applicable" rows become over-grants, silently.
  // This is the test that stops that landing unnoticed (cerbos/query-plan-adapters#323).
  test("emits no $lookup and reaches no second collection", async () => {
    const forbidden = /\$lookup|\$graphLookup|\bpopulate\s*\(|\baggregate\s*\(/;

    // The claim is about the adapter, not about the corpus's mapper: a filter walk alone would
    // pass for a `$lookup` the corpus mapper never triggers. Reading the source is what makes the
    // guard total over mapper shapes.
    const source = fs.readFileSync(path.join(__dirname, "index.ts"), "utf8");
    // Prose about the guard is not a violation of it, so comments come off first — including
    // trailing ones, or this very file's vocabulary would trip the scan the moment someone
    // wrote `// never calls populate()` next to a line of code.
    const stripComments = (line: string): string =>
      /^(\/\/|\/\*|\*)/.test(line.trimStart())
        ? ""
        : line.replace(/\/\/.*$/, "").replace(/\/\*.*?\*\//g, "");
    const offendingLines = source
      .split("\n")
      .map((line, index) => [index + 1, stripComments(line)] as const)
      .filter(([, code]) => forbidden.test(code));
    expect(offendingLines).toEqual([]);

    // And the emitted filters, so a `$lookup` assembled from string fragments cannot slip past
    // the source scan.
    for (const action of ORACLE_ACTIONS) {
      const queryPlan = await cerbos.planResources({
        principal: CHECK_RESOURCES.principal,
        resource: { kind: seedsFile.resourceKind },
        action,
      });
      const result = queryPlanToMongoose({ queryPlan, mapper: MAPPER });
      if (result.kind !== PlanKind.CONDITIONAL) continue;
      expect([action, JSON.stringify(result.filters)]).toEqual([
        action,
        expect.not.stringMatching(forbidden),
      ]);
    }
  });

  // The to-one relation carries no corpus action yet — this is the expand half of
  // cerbos/query-plan-adapters#372's expand–contract — so nothing else in this file would notice a
  // seeder that stored no chain at all, or one that wrote the root's own columns one hop out.
  // Read the two hops back out of the stored documents rather than counting them: a count cannot
  // tell the corpus's values from the root's, which is exactly the flat-alias failure this
  // relation exists to make visible.
  test("the seeded to-one chain matches the corpus relation", async () => {
    const withParent = SEEDS.filter((seed) => parentSeedOf(seed) !== undefined);
    const withInner = SEEDS.filter(
      (seed) => parentSeedOf(parentSeedOf(seed)) !== undefined,
    );
    expect(withParent.length).toBeGreaterThan(0);
    expect(withInner.length).toBeGreaterThan(0);
    expect(withParent.length).toBeLessThan(SEEDS.length);

    const stored = await AdversarialResource.find(
      {},
      { resourceId: 1, "parent.aString": 1, "parent.inner.aString": 1, _id: 0 },
    ).lean();
    expect(
      Object.fromEntries(
        stored.map((doc) => [
          doc.resourceId,
          [doc.parent?.aString ?? null, doc.parent?.inner?.aString ?? null],
        ]),
      ),
    ).toEqual(
      Object.fromEntries(
        SEEDS.map((seed) => [
          seed.id,
          [
            parentSeedOf(seed)?.aString ?? null,
            parentSeedOf(parentSeedOf(seed))?.aString ?? null,
          ],
        ]),
      ),
    );
  });
});
