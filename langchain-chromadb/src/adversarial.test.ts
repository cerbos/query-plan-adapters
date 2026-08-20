import {
  afterAll,
  beforeAll,
  describe,
  expect,
  jest,
  test,
} from "@jest/globals";
import type { Principal, Value } from "@cerbos/core";
import { GRPC as Cerbos } from "@cerbos/grpc";
import {
  ChromaClient,
  ChromaNotFoundError,
  type Collection,
  type Metadata,
} from "chromadb";

import { PlanKind, queryPlanToChromaDB } from ".";
import {
  ADAPTER,
  FIELD_NAME_MAPPER,
  parseStringArray,
  readCorpusJson,
  requireArray,
  requireBoolean,
  requireNumber,
  requireRecord,
  requireString,
} from "./corpus";
import { loadActionControlPlane, loadCheckResources } from "./controlPlane";

/**
 * Shared-corpus differential suite for the flat scalar subset Chroma metadata filters can
 * represent. Every supported action follows the complete production path:
 *
 *   pinned Cerbos PlanResources -> queryPlanToChromaDB -> real Chroma query
 *
 * The resulting IDs are compared with per-row checkResource decisions from the same PDP. Shapes
 * outside Chroma's scalar filter model must fail during translation, before a malformed or
 * silently incomplete Where filter reaches Chroma.
 *
 * The metadata mapping lives in `./corpus`; `./controlPlane` validates catalog expectations and
 * adapter-local direct outcomes for both this harness and `translator.test.ts`. The offline suite
 * pins the emitted filter, while this suite proves that filter returns the documents the PDP
 * allows.
 */

jest.setTimeout(120_000);

const CERBOS_PORT = 3641;
const cerbos = new Cerbos(`127.0.0.1:${CERBOS_PORT}`, { tls: false });
const chromaUrl = new URL(process.env["CHROMA_URL"] ?? "http://127.0.0.1:8234");
const chroma = new ChromaClient({
  host: chromaUrl.hostname,
  port: Number(chromaUrl.port) || 8000,
});
const COLLECTION_NAME = "adapter-adversarial-tests";
const BASE_EMBEDDING = [0.1, 0.2, 0.3, 0.4];

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

// The corpus principal is guarded the same way and for the same reason — and this harness is why
// the guard exists. It feeds the PLAN under test AND the check() oracle, so an attribute dropped on
// the way in vanishes from both sides at once: the plan folds to ALWAYS_DENIED and the oracle,
// built from the same principal, agrees. This harness used to rebuild the principal from a
// hardcoded attribute allowlist, and when `pv-exists` added `manyTeams` the projection dropped it
// and the action passed while testing nothing (conformance/README.md, "Adding a new hostile
// shape", step 7). The attributes are carried through verbatim now; the guard is what proves it.
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

/**
 * Asserted against the RAW json for the same reason as the seed keys: parseSeedsFile rebuilds
 * `id` and `roles`, so a rebuilt principal could only ever report the keys this harness already
 * names.
 *
 * The attribute VALUES are asserted too. A key-set guard says nothing about a change inside one and
 * three of the four attributes are lists, so the element type is asserted for the same reason the
 * seed guard descends into `tags[]`.
 */
function assertPrincipalKeyCoverage(value: unknown): void {
  const principal = requireRecord(
    requireRecord(value, "seeds.json")["principal"],
    "seeds.json principal",
  );
  assertKeys("seeds.json principal", Object.keys(principal), PRINCIPAL_KEYS);
  const attr = requireRecord(principal["attr"], "seeds.json principal.attr");
  assertKeys(
    "seeds.json principal.attr",
    Object.keys(attr),
    PRINCIPAL_ATTR_KEYS,
  );
  for (const [key, entry] of Object.entries(attr)) {
    const label = `seeds.json principal.attr.${key}`;
    if (typeof entry === "string") continue;
    if (Array.isArray(entry) && entry.every((el) => typeof el === "string")) {
      continue;
    }
    throw Error(
      `${label} is neither a string nor an array of strings, the only two shapes this harness consumes: a reshaped principal attribute feeds the plan and the check() oracle at once`,
    );
  }
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
  const fields = parseStringArray(file["fields"], "derived-fields.json fields");
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
  const parentSeedId = seed["parentSeedId"];
  if (parentSeedId !== null && typeof parentSeedId !== "string") {
    throw Error(`${label}.parentSeedId must be a string or null`);
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
    parentSeedId,
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

const rawSeedsJson = readCorpusJson("seeds.json");
assertSeedKeyCoverage(rawSeedsJson);
assertPrincipalKeyCoverage(rawSeedsJson);
const seedsFile = parseSeedsFile(rawSeedsJson);
const derivedFile = parseDerivedFile(readCorpusJson("derived-fields.json"));
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

const ACTION_CONTROL_PLANE = loadActionControlPlane({
  adapter: ADAPTER,
  selectedAction: process.env["ADAPTERCTL_ACTION"],
});
const FULL_MATRIX = process.env["ADAPTERCTL_ACTION"] === undefined;
const fullMatrixTest = FULL_MATRIX ? test : test.skip;
const CHECK_RESOURCES = loadCheckResources();
/**
 * Adapter-local direct outcomes, read through the loader `translator.test.ts` shares. Asserting
 * them identically in both suites makes the offline completeness guard total.
 */
const CHROMA_SUPPORTED_ACTIONS = ACTION_CONTROL_PLANE.oracleActions;
const THROWING_ACTIONS = ACTION_CONTROL_PLANE.throwingActions.filter(
  ({ action }) => action !== "null-eq-missing",
);
// Actions whose `== null` probe targets an attribute the oracle OMITS for NULL columns. Chroma
// needs no representation option: it cannot index an explicit null distinguishably from a missing
// key, so every null comparison operand is already rejected outright (#302).
const NULL_REPRESENTATION_OMITTED = ACTION_CONTROL_PLANE.throwingActions.filter(
  ({ action }) => action === "null-eq-missing",
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

// -- the real to-one relation (conformance/README.md, "The real to-one relation") ----------------
//
// `parentSeedId` names the seed whose four scalars this row's `parent` carries, and that seed's own
// `parentSeedId` names the ones `parent.inner` carries. The chain is cut at two levels.

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
  // The to-one chain, flattened onto dotted keys. A level that does not exist writes no key at
  // all, which is what the check side's missing `parent` / `parent.inner` path mirrors.
  const levels: [string, Seed | undefined][] = [
    ["parent", parentSeedOf(seed)],
    ["parent.inner", parentSeedOf(parentSeedOf(seed))],
  ];
  for (const [prefix, level] of levels) {
    if (level === undefined) continue;
    metadata[`${prefix}.aBool`] = level.aBool;
    metadata[`${prefix}.aString`] = level.aString;
    metadata[`${prefix}.aNumber`] = level.aNumber;
    if (level.aOptionalString !== null) {
      metadata[`${prefix}.aOptionalString`] = level.aOptionalString;
    }
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
    principal: CHECK_RESOURCES.principal,
    resource: { kind: seedsFile.resourceKind },
    action,
  });
}

async function oracleAllowedIds(action: string): Promise<string[]> {
  const ids: string[] = [];
  for (const resource of CHECK_RESOURCES.resources) {
    const result = await cerbos.checkResource({
      principal: CHECK_RESOURCES.principal,
      resource,
      actions: [action],
    });
    if (result.isAllowed(action)) {
      ids.push(resource.id);
    }
  }
  return ids.sort();
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
      translated.kind === PlanKind.CONDITIONAL ? translated.filters : undefined,
    nResults: SEEDS.length,
  });
  return [...(results.ids[0] ?? [])].sort();
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

  test.each(CHROMA_SUPPORTED_ACTIONS)(
    "%s matches the check() oracle",
    async (action) => {
      const [oracle, filtered] = await Promise.all([
        expectCatalogOracle(action),
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
      await expectCatalogOracle(action);
      const queryPlan = await planFor(action);
      expect(() =>
        queryPlanToChromaDB({
          queryPlan,
          fieldNameMapper: FIELD_NAME_MAPPER,
        }),
      ).toThrow(message);
    },
  );

  // #387. `filter-as-conjunct` puts a filter() one level below the root, where the guard that
  // refuses `filter-as-condition` does not look. Its oracle is empty BY CONSTRUCTION — check()
  // cannot evaluate a non-boolean conjunction — so the catalog marks its oracle as empty,
  // and a bare "it throws" would say nothing about whether refusing it is REQUIRED.
  //
  // This is that argument. The other conjunct is `R.attr.aBool`, which Chroma certainly can
  // express and which `root-bare-bool` spells on its own; an adapter that dropped the conjunct it
  // could not translate would emit exactly that filter and return every document it selects, all
  // of which the PDP denies for this action.
  fullMatrixTest(
    "filter-as-conjunct must be refused: dropping its untranslatable half over-grants",
    async () => {
      await expectCatalogOracle("filter-as-conjunct");
      await expectCatalogOracle("root-bare-bool");

      const entry = THROWING_ACTIONS.find(
        ({ action }) => action === "filter-as-conjunct",
      );
      expect(entry).toBeDefined();
      const queryPlan = await planFor("filter-as-conjunct");
      expect(() =>
        queryPlanToChromaDB({ queryPlan, fieldNameMapper: FIELD_NAME_MAPPER }),
      ).toThrow(entry?.message);
    },
  );

  // #302. Chroma is one of two adapters that need no `nullAttributeRepresentation` option: its
  // metadata filters accept only finite numbers, strings and booleans, so a null comparison
  // operand is rejected before the representation could matter. `null-eq` (explicit null) is
  // already has a `rejected` direct outcome for the same reason, and `null-eq-missing` must fail
  // the same
  // way. This test guards that equivalence: if Chroma ever gains a null sentinel, the shape stops
  // throwing here and the adapter acquires a representation dependency it must then declare.

  test.each(NULL_REPRESENTATION_OMITTED)(
    "$action is rejected regardless of representation ($reason)",
    async ({ action, message }) => {
      await expectCatalogOracle(action);

      const queryPlan = await planFor(action);
      expect(() =>
        queryPlanToChromaDB({
          queryPlan,
          fieldNameMapper: FIELD_NAME_MAPPER,
        }),
      ).toThrow(message);
    },
  );

  test.each(ACTION_CONTROL_PLANE.upstreamBlockedActions)(
    "$action pins the upstream planner divergence ($reason)",
    async ({ action }) => {
      const queryPlan = await planFor(action);
      await expectCatalogOracle(action);
      const allIds = CHECK_RESOURCES.resources.map(({ id }) => id).sort();

      expect(queryPlan.kind).toBe(PlanKind.ALWAYS_ALLOWED);
      expect(await adapterFilteredIds(action)).toEqual(allIds);
    },
  );

  // The to-one relation carries no corpus action yet — this is the expand half of
  // cerbos/query-plan-adapters#372's expand–contract — so nothing else in this file would notice a
  // seeder that wrote no chain keys at all, or one that wrote the root's own columns one hop out.
  // Read the two hops back out of the stored metadata rather than counting keys: a count cannot
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

    const stored = await activeCollection().get({
      ids: SEEDS.map(({ id }) => id),
      include: ["metadatas"],
    });
    expect(
      Object.fromEntries(
        stored.ids.map((id, index) => {
          const metadata = stored.metadatas[index] ?? {};
          return [
            id,
            [
              metadata["parent.aString"] ?? null,
              metadata["parent.inner.aString"] ?? null,
            ],
          ];
        }),
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
