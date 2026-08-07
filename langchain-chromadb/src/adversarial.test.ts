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

interface UnsupportedShape {
  action: string;
  shape: string;
  /** One entry per adapter that must reject the shape; the corpus asserts the key set. */
  messages: Record<string, string>;
}

interface ActionsFile {
  conformance: string[];
  adapterUnsupported: Record<string, UnsupportedAction[]>;
  adapterSupportedExpected: Record<string, UnsupportedAction[]>;
  expectedUnsupported: UnsupportedShape[];
  nullRepresentationOmitted: NullRepresentationOmitted[];
  knownDivergences: KnownDivergence[];
}

interface UnsupportedAction {
  action: string;
  reason: string;
  /** Absent on `adapterSupportedExpected` / `nullRepresentationOmitted`, required on a throw. */
  message?: string;
}

/**
 * A `nullRepresentationOmitted` entry. Every adapter must reject these — the two NULL conventions
 * are indistinguishable on the wire — so `messages` names the whole roster with no promotions to
 * subtract.
 */
interface NullRepresentationOmitted {
  action: string;
  reason: string;
  messages: Record<string, string>;
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

// -- corpus coverage guards ---------------------------------------------------------------------
//
// The same parsed seed feeds the stored metadata AND the check() oracle, so a corpus field this
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
  optional: readonly string[] = [],
): void {
  const allowed = new Set<string>([...want, ...optional]);
  for (const key of got) {
    if (!allowed.has(key)) {
      throw Error(
        `${label} carries "${key}", which this harness does not consume: an unconsumed corpus field is dropped from the stored metadata and the check() oracle at once`,
      );
    }
  }
  const present = new Set(got);
  for (const key of want) {
    if (!present.has(key)) {
      throw Error(`${label} is missing "${key}", which this harness consumes`);
    }
  }
}

/**
 * Asserted against the RAW json rather than the parsed seeds: parseSeed rebuilds each row field by
 * field, so a parsed seed can only ever carry the keys this harness already names and the
 * assertion would pass vacuously.
 */
function assertSeedKeyCoverage(value: unknown): void {
  const seeds = requireArray(
    requireRecord(value, "seeds.json")["seeds"],
    "seeds.json seeds",
  );
  seeds.forEach((seed, index) => {
    const label = `seeds.json seeds[${index}]`;
    const record = requireRecord(seed, label);
    assertKeys(label, Object.keys(record), SEED_KEYS, [SEED_NOTE_KEY]);
    requireArray(record["tags"], `${label}.tags`).forEach((tag, tagIndex) => {
      const tagLabel = `${label}.tags[${tagIndex}]`;
      assertKeys(tagLabel, Object.keys(requireRecord(tag, tagLabel)), TAG_KEYS);
    });
  });
}

function parseDerivedEntry(value: unknown, label: string): DerivedEntry {
  const entry = requireRecord(value, label);
  assertKeys(label, Object.keys(entry), DERIVED_KEYS);
  const aDouble = entry["aDouble"];
  if (aDouble !== null && typeof aDouble !== "number") {
    throw Error(`${label}.aDouble must be a number or null`);
  }
  const createdAt = entry["createdAt"];
  if (createdAt !== null && typeof createdAt !== "string") {
    throw Error(`${label}.createdAt must be a string or null`);
  }
  const scope = entry["scope"];
  if (scope !== null && typeof scope !== "string") {
    throw Error(`${label}.scope must be a string or null`);
  }
  const labels = requireArray(entry["labels"], `${label}.labels`).map(
    (name, index) => {
      if (name !== null && typeof name !== "string") {
        throw Error(`${label}.labels[${index}] must be a string or null`);
      }
      return name;
    },
  );
  return {
    createdBy: requireString(entry["createdBy"], `${label}.createdBy`),
    aDouble,
    createdAt,
    scope,
    labels,
  };
}

function parseDerivedFile(value: unknown): DerivedFile {
  const file = requireRecord(value, "derived-fields.json");
  const fields = parseStringArray(
    file["fields"],
    "derived-fields.json fields",
  );
  assertKeys("derived-fields.json fields", fields, DERIVED_KEYS);
  const derived: Record<string, DerivedEntry> = {};
  for (const [id, entry] of Object.entries(
    requireRecord(file["derived"], "derived-fields.json derived"),
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

/** The `messages` map of one `expectedUnsupported` entry: adapter key -> required substring. */
function parseMessages(value: unknown, label: string): Record<string, string> {
  const messages = requireRecord(value, label);
  const result: Record<string, string> = {};
  for (const [adapter, message] of Object.entries(messages)) {
    result[adapter] = requireString(message, `${label}.${adapter}`);
  }
  return result;
}

function parseUnsupportedShape(value: unknown, index: number): UnsupportedShape {
  const shape = requireRecord(value, `expectedUnsupported[${index}]`);
  return {
    action: requireString(shape["action"], `expectedUnsupported[${index}].action`),
    shape: requireString(shape["shape"], `expectedUnsupported[${index}].shape`),
    messages: parseMessages(
      shape["messages"],
      `expectedUnsupported[${index}].messages`,
    ),
  };
}

function parseUnsupportedAction(
  value: unknown,
  label: string,
): UnsupportedAction {
  const entry = requireRecord(value, label);
  const message = entry["message"];
  return {
    action: requireString(entry["action"], `${label}.action`),
    reason: requireString(entry["reason"], `${label}.reason`),
    // `adapterUnsupported` carries this and the classification below requires it;
    // `adapterSupportedExpected` and `nullRepresentationOmitted` do not throw, so they do not.
    ...(message === undefined
      ? {}
      : { message: requireString(message, `${label}.message`) }),
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
    // This parser rebuilds the manifest field by field, so a corpus group it does not name is
    // silently dropped — and a dropped group makes its actions vanish from every count and every
    // test.each, passing vacuously. Parse each group explicitly.
    nullRepresentationOmitted: requireArray(
      file["nullRepresentationOmitted"],
      "nullRepresentationOmitted",
    ).map((entry, index) => {
      const label = `nullRepresentationOmitted[${index}]`;
      const record = requireRecord(entry, label);
      return {
        action: requireString(record["action"], `${label}.action`),
        reason: requireString(record["reason"], `${label}.reason`),
        messages: parseMessages(record["messages"], `${label}.messages`),
      };
    }),
    knownDivergences: requireArray(
      file["knownDivergences"],
      "knownDivergences",
    ).map(parseKnownDivergence),
  };
}

function readJson(fileName: string): unknown {
  return JSON.parse(fs.readFileSync(path.join(CONFORMANCE_DIR, fileName), "utf8"));
}

const rawSeedsJson = readJson("seeds.json");
assertSeedKeyCoverage(rawSeedsJson);
const seedsFile = parseSeedsFile(rawSeedsJson);
const actionsFile = parseActionsFile(readJson("actions.json"));
const derivedFile = parseDerivedFile(readJson("derived-fields.json"));
const SEEDS = seedsFile.seeds;

if (Object.keys(derivedFile.derived).length !== SEEDS.length) {
  throw Error(
    `derived-fields.json has ${Object.keys(derivedFile.derived).length} entries for ${SEEDS.length} seeds`,
  );
}
for (const seed of SEEDS) {
  // Throws when the entry is missing.
  derivedFor(seed);
}

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
/**
 * A shape this adapter must refuse, with the substring its error has to contain.
 *
 * The message is what turns "it threw" into "it threw for the declared reason": without it a
 * mapper typo or an unrelated validation satisfies the assertion just as well as the limitation
 * the corpus documents. Chroma is the largest consumer of this — most of its corpus is
 * fail-closed, so a bare throw assertion proved almost nothing (cerbos/query-plan-adapters#326).
 */
interface ThrowingAction {
  action: string;
  reason: string;
  message: string;
}

/** The pinned message, or a failure — a throwing action without one asserts nothing. */
function requireMessage(label: string, message: string | undefined): string {
  if (message === undefined || message === "") {
    throw Error(
      `actions.json pins no throw message for ${label}: the throw suite would accept a failure for any reason`,
    );
  }
  return message;
}

const THROWING_ACTIONS: ThrowingAction[] = [
  ...CHROMA_UNSUPPORTED.map(({ action, reason, message }) => ({
    action,
    reason,
    message: requireMessage(
      `adapterUnsupported.langchain-chromadb.${action}`,
      message,
    ),
  })),
  ...actionsFile.expectedUnsupported
    .filter(({ action }) => !CHROMA_SUPPORTED_EXPECTED_ACTIONS.has(action))
    .map(({ action, shape, messages }) => ({
      action,
      reason: shape,
      message: requireMessage(
        `expectedUnsupported.${action}.messages.langchain-chromadb`,
        messages["langchain-chromadb"],
      ),
    })),
];
// Actions whose `== null` probe targets an attribute the oracle OMITS for NULL columns. Chroma
// needs no representation option: it cannot index an explicit null distinguishably from a missing
// key, so every null comparison operand is already rejected outright (#302).
const NULL_REPRESENTATION_OMITTED = actionsFile.nullRepresentationOmitted.map(
  ({ action, reason, messages }) => ({
    action,
    reason,
    message: requireMessage(
      `nullRepresentationOmitted.${action}.messages.langchain-chromadb`,
      messages["langchain-chromadb"],
    ),
  }),
);
const MANIFEST_ACTIONS = new Set([
  ...actionsFile.conformance,
  ...actionsFile.expectedUnsupported.map(({ action }) => action),
  ...NULL_REPRESENTATION_OMITTED.map(({ action }) => action),
  ...CHROMA_DIVERGENCES,
]);

// -- the degeneracy guard (conformance/README.md, "The degeneracy guard") -----------------------
//
// Chroma's flat scalar metadata leaves it the narrowest oracle set of any adapter, so the guard
// is derived from that set rather than shared with the relational harnesses: every entry below is
// asserted to be in `CHROMA_SUPPORTED_ACTIONS` (cerbos/query-plan-adapters#324). This is every
// action Chroma oracle-compares except `in-empty`, whose oracle is empty by CONSTRUCTION
// (`x in []` is false for every seed) and so cannot satisfy a non-empty assertion.

const DEGENERACY_GUARD_ACTIONS = [
  "vf-le",
  "vf-ge",
  "vf-ne",
  "nary-and",
  "double-negation",
  "triple-negation",
  "cs-eq",
  "empty-string-eq",
  "unicode-eq",
  "in-single",
  "neg-number",
  "p-struct",
  "p-in-null-single",
  "p-in-null-multi",
] as const;

/**
 * Shapes Chroma refuses to translate: they have no oracle comparison to guard, and stay here as
 * PDP/policy liveness probes for the groups Chroma's own list cannot cover — the collection
 * macros, the null-selecting directions, the chained relation (#309/#315/#316), the column
 * arithmetic (#311) and the numeric cast. See cerbos/query-plan-adapters#324.
 */
const DEGENERACY_LIVENESS_PROBES = [
  "pv-exists",
  "null-eq",
  "w1-all-chain",
  // The chain reached through a ternary condition (#334) and through a fractional count
  // threshold (#333): different rejection sites, both still fail-closed here.
  "w1-ternary-chain-cond",
  "w1-size-frac-le-chain",
  "cr-div-neg-zero",
  "cast-int-double",
] as const;

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

/** Deterministic ISO instant per seed for the timestamp probe: split around 2025-01-01. */
function isoFor(seed: Seed): string {
  return derivedFor(seed).createdBy;
}

function timestampFor(seed: Seed): string | null {
  return derivedFor(seed).createdAt;
}

function scopeFor(seed: Seed): string | null {
  return derivedFor(seed).scope;
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

/** The degeneracy guard's per-action assertion, labelled so a failure names the action. */
async function expectNonDegenerateOracle(action: string): Promise<void> {
  const ids = await oracleAllowedIds(action);
  expect({
    action,
    nonEmpty: ids.length > 0,
    nonTotal: ids.length < SEEDS.length,
  }).toEqual({ action, nonEmpty: true, nonTotal: true });
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

  // Adding a throwing action without pinning its message must fail this harness rather than
  // silently degrade the throw suite to a bare "it threw" (cerbos/query-plan-adapters#326).
  test("a throwing action with no pinned message fails classification", () => {
    expect(() => requireMessage("synthetic-entry", undefined)).toThrow(
      /pins no throw message/,
    );
    expect(() => requireMessage("synthetic-entry", "")).toThrow(
      /pins no throw message/,
    );
  });
  test("manifest assigns all 146 policy actions exactly one Chroma outcome", () => {
    const oracle = new Set(CHROMA_SUPPORTED_ACTIONS);
    const throwing = new Set(THROWING_ACTIONS.map(({ action }) => action));
    const nullOmitted = new Set(
      NULL_REPRESENTATION_OMITTED.map(({ action }) => action),
    );
    const misclassified = [...MANIFEST_ACTIONS].filter((action) => {
      const classificationCount = [
        oracle.has(action),
        throwing.has(action),
        nullOmitted.has(action),
        CHROMA_DIVERGENCES.has(action),
      ].filter(Boolean).length;
      return classificationCount !== 1;
    });

    expect(MANIFEST_ACTIONS.size).toBe(146);
    expect(CHROMA_SUPPORTED_ACTIONS).toHaveLength(15);
    expect(oracle.size).toBe(CHROMA_SUPPORTED_ACTIONS.length);
    expect(CHROMA_UNSUPPORTED).toHaveLength(121);
    expect(CHROMA_SUPPORTED_EXPECTED).toHaveLength(0);
    expect(THROWING_ACTIONS).toHaveLength(129);
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

  // The message is asserted, not just the throw: a bare `toThrow()` is satisfied by a mapper
  // typo or an unrelated validation, which would leave the classification resting on a failure
  // that has nothing to do with the limitation it declares (cerbos/query-plan-adapters#326).
  test.each(THROWING_ACTIONS)(
    "$action fails during translation with the declared message ($reason)",
    async ({ action, message }) => {
      const queryPlan = await planFor(action);
      expect(() =>
        queryPlanToChromaDB({
          queryPlan,
          fieldNameMapper: FIELD_NAME_MAPPER,
        }),
      ).toThrow(message);
    },
  );

  // #302. Chroma is one of two adapters that need no `nullAttributeRepresentation` option: its
  // metadata filters accept only finite numbers, strings and booleans, so a null comparison
  // operand is rejected before the representation could matter. `null-eq` (explicit null) is
  // already in `adapterUnsupported` for the same reason, and `null-eq-missing` must fail the same
  // way. This test guards that equivalence: if Chroma ever gains a null sentinel, the shape stops
  // throwing here and the adapter acquires a representation dependency it must then declare.
  test.each(NULL_REPRESENTATION_OMITTED)(
    "$action is rejected regardless of representation ($reason)",
    async ({ action, message }) => {
      expect(await oracleAllowedIds(action)).toEqual([]);

      const queryPlan = await planFor(action);
      expect(() =>
        queryPlanToChromaDB({
          queryPlan,
          fieldNameMapper: FIELD_NAME_MAPPER,
        }),
      ).toThrow(message);
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
    // Guard the guard: each of these actions must produce a non-empty, non-total oracle set,
    // otherwise the differential comparison could pass vacuously (e.g. PDP denying all). The
    // membership assertion is what keeps the list honest — Chroma compares 15 of the corpus's
    // 133 conformance actions, so a guard list shared with a relational harness would name
    // shapes it never compares (cerbos/query-plan-adapters#324).
    for (const action of DEGENERACY_GUARD_ACTIONS) {
      expect(CHROMA_SUPPORTED_ACTIONS).toContain(action);
      await expectNonDegenerateOracle(action);
    }
    // Asserting the complement keeps the split honest — an action Chroma gains support for must
    // move up into the guard proper.
    for (const action of DEGENERACY_LIVENESS_PROBES) {
      expect(CHROMA_SUPPORTED_ACTIONS).not.toContain(action);
      await expectNonDegenerateOracle(action);
    }
  });
});
