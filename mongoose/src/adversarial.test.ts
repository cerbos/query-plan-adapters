import * as fs from "node:fs";
import * as path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "@jest/globals";
import {
  PlanExpression,
  PlanExpressionValue,
  PlanExpressionVariable,
} from "@cerbos/core";
import type {
  PlanExpressionOperand,
  Principal,
  Resource,
  Value,
} from "@cerbos/core";
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

interface AdapterEntry {
  action: string;
  reason: string;
  /** Absent on `adapterSupportedExpected` / `nullRepresentationOmitted`, required on a throw. */
  message?: string;
}

interface ExpectedUnsupportedEntry {
  action: string;
  shape: string;
  /** One entry per adapter that must reject the shape; the corpus asserts the key set. */
  messages: Record<string, string>;
}

/**
 * A `nullRepresentationOmitted` entry. Every adapter must reject these — the two NULL conventions
 * are indistinguishable on the wire — so `messages` names the whole roster with no promotions to
 * subtract.
 */
interface NullRepresentationOmittedEntry {
  action: string;
  reason: string;
  messages: Record<string, string>;
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
  nullRepresentationOmitted: NullRepresentationOmittedEntry[];
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

// -- corpus coverage guards ---------------------------------------------------------------------
//
// The same parsed seed feeds the stored document AND the check() oracle, so a corpus field this
// harness does not consume is dropped from both sides at once and the differential agrees for the
// wrong reason — the projection trap conformance/README.md describes for actions.json, applied to
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

function assertKeys(
  label: string,
  got: string[],
  want: readonly string[],
  optional: readonly string[] = []
): void {
  const allowed = new Set<string>([...want, ...optional]);
  for (const key of got) {
    if (!allowed.has(key)) {
      throw new Error(
        `${label} carries "${key}", which this harness does not consume: an unconsumed corpus field is dropped from the stored document and the check() oracle at once`
      );
    }
  }
  const present = new Set(got);
  for (const key of want) {
    if (!present.has(key)) {
      throw new Error(
        `${label} is missing "${key}", which this harness consumes`
      );
    }
  }
}

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
    "derived-fields.json fields"
  );
  assertKeys("derived-fields.json fields", fields, DERIVED_KEYS);
  const derived: Record<string, DerivedEntry> = {};
  for (const [id, entry] of Object.entries(
    expectRecord(record["derived"], "derived-fields.json derived")
  )) {
    derived[id] = parseDerivedEntry(
      entry,
      `derived-fields.json derived["${id}"]`
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
      parseTag(tag, `${label}.tags[${tagIndex}]`)
    ),
    subCategoryNames: expectStringArray(
      record["subCategoryNames"],
      `${label}.subCategoryNames`
    ),
    parentSeedId,
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
  const message = record["message"];
  return {
    action: expectString(record["action"], `${label}.action`),
    reason: expectString(record["reason"], `${label}.reason`),
    // `adapterUnsupported` carries this and the classification below requires it;
    // `adapterSupportedExpected` and `nullRepresentationOmitted` do not throw, so they do not.
    ...(message === undefined
      ? {}
      : { message: expectString(message, `${label}.message`) }),
  };
}

/** The `messages` map of one `expectedUnsupported` entry: adapter key -> required substring. */
function parseMessages(
  value: unknown,
  label: string
): Record<string, string> {
  const record = expectRecord(value, label);
  const result: Record<string, string> = {};
  for (const [adapter, message] of Object.entries(record)) {
    result[adapter] = expectString(message, `${label}.${adapter}`);
  }
  return result;
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
  const nullOmitted = record["nullRepresentationOmitted"];
  if (
    !Array.isArray(expected) ||
    !Array.isArray(divergences) ||
    !Array.isArray(nullOmitted)
  ) {
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
        messages: parseMessages(
          parsed["messages"],
          `expectedUnsupported[${index}].messages`
        ),
      };
    }),
    // This parser rebuilds the manifest field by field, so a corpus group it does not name is
    // silently dropped — and a dropped group makes its actions vanish from every count and
    // every test.each, passing vacuously. Parse each group explicitly.
    nullRepresentationOmitted: nullOmitted.map((entry, index) => {
      const parsed = expectRecord(entry, `nullRepresentationOmitted[${index}]`);
      return {
        action: expectString(
          parsed["action"],
          `nullRepresentationOmitted[${index}].action`
        ),
        reason: expectString(
          parsed["reason"],
          `nullRepresentationOmitted[${index}].reason`
        ),
        messages: parseMessages(
          parsed["messages"],
          `nullRepresentationOmitted[${index}].messages`
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

const rawSeedsJson = readJson("seeds.json");
assertSeedKeyCoverage(rawSeedsJson);
const seedsFile = parseSeedsFile(rawSeedsJson);
const actionsFile = parseActionsFile(readJson("actions.json"));
const derivedFile = parseDerivedFile(readJson("derived-fields.json"));
const SEEDS = seedsFile.seeds;

if (Object.keys(derivedFile.derived).length !== SEEDS.length) {
  throw new Error(
    `derived-fields.json has ${
      Object.keys(derivedFile.derived).length
    } entries for ${SEEDS.length} seeds`
  );
}
for (const seed of SEEDS) {
  // Throws when the entry is missing.
  derivedFor(seed);
}

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
/**
 * A shape this adapter must refuse, with the substring its error has to contain.
 *
 * The message is what turns "it threw" into "it threw for the declared reason": without it a
 * mapper typo or an unrelated validation satisfies the assertion just as well as the limitation
 * the corpus documents (cerbos/query-plan-adapters#326).
 */
interface ThrowingAction {
  action: string;
  reason: string;
  message: string;
}

/** The pinned message, or a failure — a throwing action without one asserts nothing. */
function requireMessage(label: string, message: string | undefined): string {
  if (message === undefined || message === "") {
    throw new Error(
      `actions.json pins no throw message for ${label}: the throw suite would accept a failure for any reason`
    );
  }
  return message;
}

const THROWING_ACTIONS: ThrowingAction[] = [
  ...unsupportedEntries.map((entry) => ({
    action: entry.action,
    reason: entry.reason,
    message: requireMessage(
      `adapterUnsupported.mongoose.${entry.action}`,
      entry.message
    ),
  })),
  ...actionsFile.expectedUnsupported
    .filter((entry) => !supportedExpectedActions.has(entry.action))
    .map((entry) => ({
      action: entry.action,
      reason: entry.shape,
      message: requireMessage(
        `expectedUnsupported.${entry.action}.messages.mongoose`,
        entry.messages["mongoose"]
      ),
    })),
].sort((left, right) => left.action.localeCompare(right.action));
// Actions whose `== null` probe targets an attribute the oracle OMITS for NULL columns. They
// carry no oracle comparison: under the omitted representation check() denies every document, so
// the adapter must reject the shape rather than emit a filter (#302).
const NULL_REPRESENTATION_OMITTED = actionsFile.nullRepresentationOmitted.map(
  (entry): ThrowingAction => ({
    action: entry.action,
    reason: entry.reason,
    message: requireMessage(
      `nullRepresentationOmitted.${entry.action}.messages.mongoose`,
      entry.messages["mongoose"]
    ),
  })
);
/** The one message every null-carrying action must be rejected with under `omitted`. */
const NULL_OMITTED_MESSAGE = NULL_REPRESENTATION_OMITTED[0]?.message ?? "";
const MANIFEST_ACTIONS = new Set([
  ...actionsFile.conformance,
  ...actionsFile.expectedUnsupported.map((entry) => entry.action),
  ...NULL_REPRESENTATION_OMITTED.map((entry) => entry.action),
  ...actionsFile.knownDivergences.map((entry) => entry.action),
]);

// -- the degeneracy guard (conformance/README.md, "The degeneracy guard") -----------------------
//
// A representative sample of the actions this adapter ORACLE-COMPARES, one per hostile group it
// can express. The two lists are asserted to be complements of `ORACLE_ACTIONS`, so neither can
// drift into the other unnoticed.
//
// w1-size-zero-chain, w1-not-size-chain, w1-size-frac-chain and the two string-cast actions are
// deliberately absent from both lists: their oracles are empty by CONSTRUCTION (no seed holds a
// to-one parent with zero children, nor one with two or more; every seed's aString raises in
// int()/double()), so they cannot satisfy a non-empty assertion.

const DEGENERACY_GUARD_ACTIONS = [
  "vf-le",
  "like-percent",
  "all-on-empty",
  "pv-exists",
  "pv-all",
  "null-eq",
  "null-ne",
  // The explicit-null convention against a non-null operand (#308). Mongoose stores an explicit
  // null and its query semantics already treat null as a value, so these four needed no change:
  // they were aligned before the SQL adapters gained their per-attribute declaration. The fifth,
  // null-value-pv-not-exists, is a liveness probe below — the negated macro the value-list fold
  // produces is what Mongoose refuses, not the null convention underneath it.
  "null-value-ne-const",
  "null-value-not-eq-const",
  "null-value-not-in-const",
  "null-value-f2f",
  // The absent to-one parent (#309/#315/#316/#333/#334): the five discriminating chain shapes
  // Mongoose translates. Its negated-exists and ternary siblings are liveness probes below.
  "w1-all-chain",
  "w1-size-nonneg-chain",
  "w1-not-in-chain",
  "w1-not-hasint-chain",
  "w1-size-frac-le-chain",
  // Mongoose throws on the whole cr-div group (#311), so the computed-relation group is guarded
  // by the fractional-size shape it does translate.
  "cr-size-frac-ge",
  // The real to-one join (#375): one per hazard — the negated hop, the null comparison, two-level
  // depth, the root conjunction, and the disjunction, whose failure direction is an under-grant.
  "rel-not-bool-hop",
  "rel-ne-null-hop",
  "rel-bool-hop2",
  "rel-hop-and-root",
  "rel-hop2-or-exists",
  // Case sensitivity in STRING MATCHING (#375 follow-up), a different mechanism from cs-eq:
  // collation governs `=`, and on SQLite nothing but `PRAGMA case_sensitive_like` governs LIKE.
  "cs-contains",
] as const;

/**
 * Shapes Mongoose refuses to translate: they have no oracle comparison to guard, and stay here as
 * PDP/policy liveness probes for a group Mongoose's own list cannot cover. See
 * cerbos/query-plan-adapters#324.
 */
const DEGENERACY_LIVENESS_PROBES = [
  // A negated macro over a chain has no UNKNOWN to represent in a Mongo filter.
  "w1-not-exists-chain",
  // The value-list fold puts a collection macro under a negation, which Mongoose refuses before
  // it reaches the comparison leaves; its four #308 siblings above are compared instead.
  "null-value-pv-not-exists",
  // A bare ternary is planned as $expr/$cond, and $in has no aggregation-expression form here.
  "w1-ternary-chain-cond",
  // $divide aborts the query on a zero denominator, so the cr-div group throws.
  "cr-div-neg-zero",
  // int() over a numeric column: truncation-versus-rounding, unsupported for every adapter but
  // convex, which promotes it in adapterSupportedExpected.
  "cast-int-double",
] as const;

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
  { _id: false, id: false }
);
const parentSchema = new Schema<AdversarialParent>(
  {
    aBool: { type: Boolean, required: true },
    aString: { type: String },
    aNumber: { type: Number, required: true },
    aOptionalString: { type: String, default: null },
    inner: { type: innerSchema, default: null },
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
    parent: { type: parentSchema, default: null },
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
  // `coOwner` aliases the `scope` field under the explicit-null convention: the oracle sends a
  // real null attribute for it rather than omitting it. Mongoose stores an explicit null, and
  // its query semantics already treat null as a value, so no `nullable` flag applies here — the
  // flag means the opposite (a stored null IS a missing attribute).
  "request.resource.attr.coOwner": { field: "scope" },
  // obj.inner is not a real nested path — it mirrors aString. `parent.inner` below is the
  // opposite: a real two-level to-one chain. The two are kept side by side on purpose.
  "request.resource.attr.obj.inner": { field: "aString" },
  // The corpus's one REAL to-one chain (the `rel-*` actions), stored as an embedded subdocument
  // per level rather than a joined collection. `type: "one"` flattens the path to `parent.aBool`
  // AND declares the level as absent-able, which is what makes the adapter require it outside
  // any `$nor`: a document with `parent: null` has no `parent.aBool` path at all, and an
  // unguarded negation matches exactly those documents.
  "request.resource.attr.parent": {
    relation: {
      name: "parent",
      type: "one",
      fields: {
        aBool: { field: "aBool" },
        aString: { field: "aString" },
        aNumber: { field: "aNumber" },
        aOptionalString: { field: "aOptionalString", nullable: true },
      },
    },
  },
  "request.resource.attr.parent.inner": {
    relation: {
      name: "parent.inner",
      type: "one",
      fields: {
        aBool: { field: "aBool" },
        aString: { field: "aString" },
        aNumber: { field: "aNumber" },
        aOptionalString: { field: "aOptionalString", nullable: true },
      },
    },
  },
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
  // mainCategory is a to-ONE parent on the check side: a seed with no subCategoryNames
  // sends NO mainCategory attribute, so CEL raises a missing-path error and check()
  // denies. The flattened `categories.subCategories` path cannot see that on its own —
  // an absent parent and a childless parent both give an empty array — so the mapping
  // declares the parent and the adapter makes the count UNKNOWN when it is missing
  // (cerbos/query-plan-adapters#309).
  "request.resource.attr.mainCategory.subCategories": {
    relation: {
      name: "categories.subCategories",
      type: "many",
      requiresParent: "categories",
      fields: { name: { field: "name" } },
    },
  },
  "request.resource.attr.mainCategory.subNames": {
    relation: {
      name: "categories.subCategories",
      type: "many",
      field: "name",
      requiresParent: "categories",
      fields: { name: { field: "name" } },
    },
  },
};

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
      `seeds.json: "${seed?.id}" names parent "${id}", which is not a seed id`
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

/** The same four as check() attributes: a NULL field is a MISSING attribute, one hop out. */
function relationAttr(seed: Seed): Record<string, Value> {
  const attr: Record<string, Value> = {
    aBool: seed.aBool,
    aString: seed.aString,
    aNumber: seed.aNumber,
  };
  if (seed.aOptionalString !== null) {
    attr["aOptionalString"] = seed.aOptionalString;
  }
  return attr;
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
    // The explicit-null alias of the `scope` field, the second half of `null-value-f2f`:
    // `scope` itself is omitted when NULL (below), so the corpus carries the same field under
    // both conventions and the field-to-field probe has two explicit nulls to compare.
    coOwner: scopeFor(seed),
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
  // The real to-one chain, mirroring the stored subdocuments exactly. A row with no parent sends
  // NO `parent` attribute — a CEL missing-path error (deny) — matching the stored document having
  // no `parent` path; the same holds one level down for `parent.inner`.
  const parentSeed = parentSeedOf(seed);
  if (parentSeed !== undefined) {
    const parentAttr = relationAttr(parentSeed);
    const innerSeed = parentSeedOf(parentSeed);
    if (innerSeed !== undefined) {
      parentAttr["inner"] = relationAttr(innerSeed);
    }
    attr["parent"] = parentAttr;
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
        parent: storedParent(seed),
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

/** The degeneracy guard's per-action assertion, labelled so a failure names the action. */
async function expectNonDegenerateOracle(action: string): Promise<void> {
  const ids = await oracleAllowedIds(action);
  expect({
    action,
    nonEmpty: ids.length > 0,
    nonTotal: ids.length < SEEDS.length,
  }).toEqual({ action, nonEmpty: true, nonTotal: true });
}

async function adapterFilteredIds(
  action: string,
  nullAttributeRepresentation: "explicit" | "omitted" = "explicit"
): Promise<string[]> {
  const queryPlan = await cerbos.planResources({
    principal: seedsFile.principal,
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
    result.kind === PlanKind.CONDITIONAL ? result.filters : {}
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

  // Adding a throwing action without pinning its message must fail this harness rather than
  // silently degrade the throw suite to a bare "it threw" (cerbos/query-plan-adapters#326).
  test("a throwing action with no pinned message fails classification", () => {
    expect(() => requireMessage("synthetic-entry", undefined)).toThrow(
      /pins no throw message/
    );
    expect(() => requireMessage("synthetic-entry", "")).toThrow(
      /pins no throw message/
    );
  });
  test("manifest assigns all 170 actions exactly one Mongoose outcome", () => {
    const oracle = new Set(ORACLE_ACTIONS);
    const throwing = new Set(THROWING_ACTIONS.map((entry) => entry.action));
    const nullOmitted = new Set(
      NULL_REPRESENTATION_OMITTED.map((entry) => entry.action)
    );
    const misclassified = [...MANIFEST_ACTIONS].filter((action) => {
      const count = [
        oracle.has(action),
        throwing.has(action),
        nullOmitted.has(action),
        divergenceActions.has(action),
      ].filter(Boolean).length;
      return count !== 1;
    });

    expect(MANIFEST_ACTIONS.size).toBe(170);
    expect(unsupportedEntries).toHaveLength(38);
    expect(supportedExpectedEntries).toHaveLength(4);
    expect(ORACLE_ACTIONS).toHaveLength(125);
    expect(THROWING_ACTIONS).toHaveLength(43);
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
      const queryPlan = await cerbos.planResources({
        principal: seedsFile.principal,
        resource: { kind: seedsFile.resourceKind },
        action,
      });
      expect(queryPlan.kind).toBe(PlanKind.CONDITIONAL);
      expect(() =>
        queryPlanToMongoose({
          queryPlan,
          mapper: MAPPER,
          nullAttributeRepresentation: "explicit",
        })
      ).toThrow(message);
    }
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
      expect(await oracleAllowedIds(action)).toEqual([]);
      expect(await adapterFilteredIds(action, "explicit")).toEqual([]);

      // The same column WITHOUT `nullable` keeps the explicit-null translation, so the empty
      // result above is the mapper flag talking, not a filter that matches nothing everywhere.
      expect(await adapterFilteredIds("null-eq", "explicit")).toEqual(
        await oracleAllowedIds("null-eq")
      );
      expect((await oracleAllowedIds("null-eq")).length).toBeGreaterThan(0);

      // The global switch is still the fail-closed backstop for callers who omit attributes
      // without declaring `nullable` on every affected mapper entry.
      await expect(adapterFilteredIds(action, "omitted")).rejects.toThrow(
        message
      );
    }
  );

  // #302 completeness guard. The rejection must key off the null OPERAND, not off a list of
  // operators: `hasIntersection(tagNames, ["public", null])` carries one in its value list, and
  // an allowlist of eq/ne/in silently misses it. Enumerating the corpus rather than naming
  // shapes means a newly added action carrying a null constant is covered automatically.
  test("every corpus action carrying a null literal is rejected under omitted", async () => {
    const nullCarrying: string[] = [];
    for (const action of [...MANIFEST_ACTIONS].sort()) {
      const queryPlan = await cerbos.planResources({
        principal: seedsFile.principal,
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
          notRejected.push(`${action} (rejected for the wrong reason: ${String(error)})`);
        }
      }
    }
    expect(notRejected).toEqual([]);
  });

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

  // The corpus pins two count spellings over the chain — `size(...) == 0` and
  // `!(size(...) > 0)` — but the guard has to be a property of the chain rather than of the
  // two spellings that happen to be pinned. These synthesise the remaining
  // threshold/polarity combinations onto the same seeded collection and assert the parentless
  // documents stay out of every one, including an arbitrary-N threshold neither corpus action
  // reaches (cerbos/query-plan-adapters#316).
  test("every count threshold over the chain inherits the absent-parent guard", async () => {
    const chain = new PlanExpressionVariable(
      "request.resource.attr.mainCategory.subCategories"
    );
    const size = new PlanExpression("size", [chain]);
    const compare = (operator: string, threshold: number) =>
      new PlanExpression(operator, [size, new PlanExpressionValue(threshold)]);
    const negate = (condition: PlanExpressionOperand) =>
      new PlanExpression("not", [condition]);

    const filteredIdsFor = async (
      condition: PlanExpressionOperand
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
        result.kind === PlanKind.CONDITIONAL ? result.filters : {}
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
  });

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
        principal: seedsFile.principal,
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
      (seed) => parentSeedOf(parentSeedOf(seed)) !== undefined
    );
    expect(withParent.length).toBeGreaterThan(0);
    expect(withInner.length).toBeGreaterThan(0);
    expect(withParent.length).toBeLessThan(SEEDS.length);

    const stored = await AdversarialResource.find(
      {},
      { resourceId: 1, "parent.aString": 1, "parent.inner.aString": 1, _id: 0 }
    ).lean();
    expect(
      Object.fromEntries(
        stored.map((doc) => [
          doc.resourceId,
          [doc.parent?.aString ?? null, doc.parent?.inner?.aString ?? null],
        ])
      )
    ).toEqual(
      Object.fromEntries(
        SEEDS.map((seed) => [
          seed.id,
          [
            parentSeedOf(seed)?.aString ?? null,
            parentSeedOf(parentSeedOf(seed))?.aString ?? null,
          ],
        ])
      )
    );
  });

  test("oracle is not degenerate", async () => {
    // Guard the guard: each of these actions must produce a non-empty, non-total oracle set,
    // otherwise the differential comparison could pass vacuously (e.g. PDP denying all).
    //
    // Every entry is asserted to be an action Mongoose actually oracle-compares. A list copied
    // from another harness drifts into naming shapes this adapter never compares, which guard
    // nothing (cerbos/query-plan-adapters#324); the membership assertion turns moving an action
    // into Mongoose's `adapterUnsupported` set into a failure here rather than a silent no-op.
    for (const action of DEGENERACY_GUARD_ACTIONS) {
      expect(ORACLE_ACTIONS).toContain(action);
      await expectNonDegenerateOracle(action);
    }
    // Asserting the complement keeps the split honest — an action Mongoose gains support for
    // must move up into the guard proper.
    for (const action of DEGENERACY_LIVENESS_PROBES) {
      expect(ORACLE_ACTIONS).not.toContain(action);
      await expectNonDegenerateOracle(action);
    }
  });
});
