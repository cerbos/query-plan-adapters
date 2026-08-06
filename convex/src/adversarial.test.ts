import * as fs from "fs";
import * as path from "path";

import { afterAll, beforeAll, describe, expect, test } from "@jest/globals";
import type { Principal, Resource, Value } from "@cerbos/core";
import { GRPC as Cerbos } from "@cerbos/grpc";
import { ConvexHttpClient } from "convex/browser";

import { api } from "../convex/_generated/api.js";
import { MAPPER } from "../convex/adversarial";
import { PlanKind, queryPlanToConvex } from ".";

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
  nullRepresentationOmitted: AdapterOutcome[];
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
  // Every group the interface declares must be validated: a group this predicate does not
  // name is the projection trap the corpus README warns about.
  Array.isArray(value["nullRepresentationOmitted"]) &&
  value["nullRepresentationOmitted"].every(isAdapterOutcome) &&
  Array.isArray(value["knownDivergences"]) &&
  value["knownDivergences"].every(isKnownDivergence);

const isDerivedEntry = (value: unknown): value is DerivedEntry =>
  isRecord(value) &&
  typeof value["createdBy"] === "string" &&
  (typeof value["aDouble"] === "number" || value["aDouble"] === null) &&
  (typeof value["createdAt"] === "string" || value["createdAt"] === null) &&
  (typeof value["scope"] === "string" || value["scope"] === null) &&
  Array.isArray(value["labels"]) &&
  value["labels"].every(
    (label) => label === null || typeof label === "string",
  );

const isDerivedFile = (value: unknown): value is DerivedFile =>
  isRecord(value) &&
  isStringArray(value["fields"]) &&
  isRecord(value["derived"]) &&
  Object.values(value["derived"]).every(isDerivedEntry);

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
      throw new Error(
        `${label} carries "${key}", which this harness does not consume: an unconsumed corpus field is dropped from the stored document and the check() oracle at once`,
      );
    }
  }
  const present = new Set(got);
  for (const key of want) {
    if (!present.has(key)) {
      throw new Error(
        `${label} is missing "${key}", which this harness consumes`,
      );
    }
  }
}

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

const parsedDerived: unknown = JSON.parse(
  fs.readFileSync(path.join(CONFORMANCE_DIR, "derived-fields.json"), "utf8"),
);
if (!isDerivedFile(parsedDerived)) {
  throw new Error("Invalid conformance derived fields");
}
const derivedFile = parsedDerived;

// seedsFile.seeds holds the parsed JSON rows verbatim, so Object.keys reports the corpus key set.
// Keep it that way: a parser that rebuilt each row field by field could only ever report the keys
// this harness already names, and the assertion would pass vacuously.
seedsFile.seeds.forEach((seed, index) => {
  const label = `seeds.json seeds[${index}]`;
  assertKeys(label, Object.keys(seed), SEED_KEYS, [SEED_NOTE_KEY]);
  seed.tags.forEach((tag, tagIndex) => {
    assertKeys(`${label}.tags[${tagIndex}]`, Object.keys(tag), TAG_KEYS);
  });
});

assertKeys("derived-fields.json fields", derivedFile.fields, DERIVED_KEYS);
if (Object.keys(derivedFile.derived).length !== seedsFile.seeds.length) {
  throw new Error(
    `derived-fields.json has ${Object.keys(derivedFile.derived).length} entries for ${seedsFile.seeds.length} seeds`,
  );
}
for (const seed of seedsFile.seeds) {
  assertKeys(
    `derived-fields.json derived["${seed.id}"]`,
    Object.keys(derivedFor(seed)),
    DERIVED_KEYS,
  );
}

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
// Actions whose `== null` probe targets an attribute the oracle OMITS for NULL columns. They
// carry no oracle comparison: under the omitted representation check() denies every document, so
// the adapter must reject the shape rather than emit a filter (#302).
const NULL_REPRESENTATION_OMITTED = actionsFile.nullRepresentationOmitted;
const MANIFEST_ACTIONS = new Set([
  ...actionsFile.conformance,
  ...actionsFile.expectedUnsupported.map((entry) => entry.action),
  ...NULL_REPRESENTATION_OMITTED.map((entry) => entry.action),
  // ALL divergences, not just Convex's: a divergence registered solely for another adapter
  // must still enter this manifest, so the size tripwire and the classified-exactly-once
  // check flag it for triage here instead of letting the action silently vanish from this
  // harness. Classification/skipping still uses the Convex-filtered KNOWN_DIVERGENCES.
  ...actionsFile.knownDivergences.map((entry) => entry.action),
]);

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

async function adapterFilteredIds(
  action: string,
  nullAttributeRepresentation: "explicit" | "omitted" = "explicit",
): Promise<string[]> {
  const queryPlan = await cerbos.planResources({
    principal: seedsFile.principal,
    resource: { kind: seedsFile.resourceKind },
    action,
  });
  if (queryPlan.kind === PlanKind.ALWAYS_DENIED) return [];
  return convex.query(api.adversarial.executePlan, {
    queryPlan: JSON.parse(JSON.stringify(queryPlan)),
    nullAttributeRepresentation,
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
  test("assigns all policy actions exactly one Convex outcome", () => {
    const allActions = MANIFEST_ACTIONS;
    const oracle = new Set(ORACLE_ACTIONS);
    const throwing = new Set(THROWING_ACTIONS);
    const nullOmitted = new Set(
      NULL_REPRESENTATION_OMITTED.map((entry) => entry.action),
    );
    const misclassified = [...allActions].filter(
      (action) =>
        [
          oracle.has(action),
          throwing.has(action),
          nullOmitted.has(action),
          KNOWN_DIVERGENCES.has(action),
        ].filter(Boolean).length !== 1,
    );

    expect(allActions.size).toBe(143);
    expect(CONVEX_UNSUPPORTED).toHaveLength(2);
    expect(CONVEX_SUPPORTED_EXPECTED).toHaveLength(6);
    expect(ORACLE_ACTIONS).toHaveLength(137);
    expect(THROWING_ACTIONS).toHaveLength(4);
    expect(misclassified).toEqual([]);
  });

  // The message each throwing action must fail with. Pinning the message is what proves the
  // rejection happens for the DECLARED mechanism — a bare "it threw" is satisfied just as
  // happily by a mapper typo or a transport error, and the corpus README calls that a silent
  // pass. A new throwing action must add its message here or the coverage assertion fails.
  const THROWING_MESSAGES: Record<string, RegExp> = {
    "cr-div-neg-zero": /sign is indeterminate/,
    "nan-ord-inf": /sign is indeterminate/,
    "filter-as-condition": /returns a list, not a boolean/,
    "map-as-condition": /returns a list, not a boolean/,
  };

  test("every throwing action pins the message that names its mechanism", () => {
    expect(Object.keys(THROWING_MESSAGES).sort()).toEqual(
      [...THROWING_ACTIONS].sort(),
    );
  });

  // The invariant is that an inexpressible shape must throw BEFORE its filter can be used, so
  // the assertion wraps translation only: the plan is fetched outside it (a PDP failure fails
  // the test instead of passing it), and no query executes (a store rejecting a wrongly
  // emitted filter cannot masquerade as the adapter refusing to translate).
  test.each(THROWING_ACTIONS)(
    "%s fails during translation, before any filter exists",
    async (action) => {
      const queryPlan = await cerbos.planResources({
        principal: seedsFile.principal,
        resource: { kind: seedsFile.resourceKind },
        action,
      });
      expect(queryPlan.kind).toBe(PlanKind.CONDITIONAL);
      expect(() =>
        queryPlanToConvex({
          queryPlan,
          mapper: MAPPER,
          allowPostFilter: true,
        }),
      ).toThrow(THROWING_MESSAGES[action]);
    },
  );

  test.each(ORACLE_ACTIONS)("%s matches the check() oracle", async (action) => {
    const [oracle, filtered] = await Promise.all([
      oracleAllowedIds(action),
      adapterFilteredIds(action),
    ]);
    expect(filtered).toEqual(oracle);
  });

  // #302. `null-eq-missing` probes `aOptionalString == null`, and `aOptionalString` follows the
  // corpus default: a NULL column sends NO attribute, so check() denies every document.
  //
  // Convex is a document store, so the SEEDED SHAPE can mirror that convention directly: this
  // harness omits `aOptionalString` entirely for a NULL column, and `q.eq(field, null)` does not
  // match an absent field. The default translation therefore already returns the empty set the
  // oracle demands — alignment that comes from the storage layout, not from anything the adapter
  // knows about the plan. A deployment that stored explicit nulls while omitting the attribute at
  // check time would over-grant exactly as a SQL adapter does, which is what the option guards.
  test.each(NULL_REPRESENTATION_OMITTED)(
    "$action aligns via the omitted document shape and is rejected under omitted ($reason)",
    async ({ action }) => {
      expect(await oracleAllowedIds(action)).toEqual([]);
      expect(await adapterFilteredIds(action, "explicit")).toEqual([]);

      // `owner` maps to the same seed field but IS stored as an explicit null, so the empty
      // result above is the document shape talking, not a filter that matches nothing everywhere.
      const explicitNullOracle = await oracleAllowedIds("null-eq");
      expect(explicitNullOracle.length).toBeGreaterThan(0);
      expect(await adapterFilteredIds("null-eq", "explicit")).toEqual(
        explicitNullOracle,
      );

      await expect(adapterFilteredIds(action, "omitted")).rejects.toThrow(
        /missing-attribute error/,
      );
    },
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
        // The rejection must be the null-operand check talking, not an incidental failure —
        // a transport error or mapper typo counting as the required rejection is the silent
        // pass the corpus README warns about.
        if (!/missing-attribute error/.test(String(error))) {
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
    const allIds = seedsFile.seeds.map((seed) => seed.id).sort();

    expect(queryPlan.kind).toBe(PlanKind.ALWAYS_ALLOWED);
    expect(oracle.length).toBeGreaterThan(0);
    expect(oracle.length).toBeLessThan(allIds.length);
    expect(await adapterFilteredIds(action)).toEqual(allIds);
  });

  test("oracle is not degenerate", async () => {
    // The #309/#312/#311/#315/#316 additions. w1-size-zero-chain, w1-not-size-chain and the
    // two string-cast actions are deliberately absent: their oracles are empty by
    // CONSTRUCTION (no seed holds a to-one parent with zero children; every seed's aString
    // raises in int()/double()), so they cannot satisfy this guard. cast-int-double is the
    // cast group's non-degenerate stand-in, and the w1/cr actions below carry it for their
    // groups.
    for (const action of ["vf-le", "like-percent", "all-on-empty", "pv-exists", "pv-all", "null-eq", "null-ne",
      "w1-all-chain", "w1-not-exists-chain", "w1-size-nonneg-chain",
      "w1-not-in-chain", "w1-not-hasint-chain",
      "cr-div-neg-zero", "cr-div-other-column", "cr-div-then-add", "cr-div-then-add-ne",
      "cast-int-double"]) {
      const ids = await oracleAllowedIds(action);
      expect(ids.length).toBeGreaterThan(0);
      expect(ids.length).toBeLessThan(seedsFile.seeds.length);
    }
  });
});
