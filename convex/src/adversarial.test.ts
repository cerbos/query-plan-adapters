import { afterAll, beforeAll, describe, expect, test } from "@jest/globals";
import { GRPC as Cerbos } from "@cerbos/grpc";
import { ConvexHttpClient } from "convex/browser";

import { api } from "../convex/_generated/api.js";
import {
  MAPPER,
  PUSHDOWN_DEMOTED_FIELDS,
  PUSHDOWN_MAPPER,
  type MapperVariant,
} from "../convex/adversarialMapper";
import type { ExecutionPath } from "../convex/planExecution";
import type { Mapper } from ".";
import { PlanKind, queryPlanToConvex } from ".";
// The corpus reader this adapter carries, shared with src/translator.test.ts. Nothing about
// seeds.json or derived-fields.json is parsed twice inside one adapter: one loader
// means one answer to "which shapes must this adapter refuse" and one declaration of the corpus
// keys it consumes. The duplication ACROSS adapters stays deliberate (ADR 0007).
import {
  isRecord,
  parseDerivedFile,
  parseSeedsFile,
  readCorpusJson,
} from "./corpus";
import type { DerivedEntry, Seed } from "./corpus";
import {
  loadActionControlPlane,
  loadCheckResources,
  requireOutcomeMessage,
} from "./controlPlane";

interface ParentChainRow {
  id: string;
  parent: string | null;
  inner: string | null;
}

const CONVEX_URL = process.env["CONVEX_URL"] ?? "http://127.0.0.1:3210";
const convex = new ConvexHttpClient(CONVEX_URL);
const cerbos = new Cerbos("127.0.0.1:3593", { tls: false });

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
  coOwner: string | null;
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
  parent?: StoredRelationLevel & { inner?: StoredRelationLevel };
}

/** One level of the to-one chain as stored: an absent `aOptionalString` is a missing attribute. */
interface StoredRelationLevel {
  aBool: boolean;
  aString: string;
  aNumber: number;
  aOptionalString?: string;
}

// -- the corpus, read once ----------------------------------------------------------------------
//
// The declared-key guards run inside these parsers, in src/corpus.ts: the seed key set, the tag
// key set, the principal and its attributes, and the derived-fields roster. They are what make a
// corpus field this harness does not consume fail loudly instead of being dropped from the stored
// document and the check() oracle at once (conformance/README.md, "Adding a new hostile shape").

const seedsFile = parseSeedsFile(readCorpusJson("seeds.json"));
const derivedFile = parseDerivedFile(
  readCorpusJson("derived-fields.json"),
  seedsFile.seeds,
);

const ACTION_CONTROL_PLANE = loadActionControlPlane({
  adapter: "convex",
  selectedAction: process.env["ADAPTERCTL_ACTION"],
});
const FULL_MATRIX = process.env["ADAPTERCTL_ACTION"] === undefined;
const fullMatrixTest = FULL_MATRIX ? test : test.skip;
const CHECK_RESOURCES = loadCheckResources();
const ORACLE_ACTIONS = ACTION_CONTROL_PLANE.oracleActions;
// Actions whose `== null` probe targets an attribute the oracle OMITS for NULL columns. They
// carry no oracle comparison: under the omitted representation check() denies every document, so
// the adapter must reject the shape rather than emit a filter (#302).
const NULL_REPRESENTATION_OMITTED = ACTION_CONTROL_PLANE.throwingActions.filter(
  ({ action }) => action === "null-eq-missing",
);
const THROWING_ACTIONS = ACTION_CONTROL_PLANE.throwingActions.filter(
  ({ action }) => action !== "null-eq-missing",
);
/** The one message every null-carrying action must be rejected with under `omitted`. */
const NULL_OMITTED_MESSAGE = requireOutcomeMessage({
  controlPlane: ACTION_CONTROL_PLANE,
  action: "null-eq-missing",
});
const MANIFEST_ACTIONS = new Set(ACTION_CONTROL_PLANE.selectedActions);

// -- pushdown coverage (cerbos/query-plan-adapters#327) ------------------------------------------
//
// Convex has no string, collection, arithmetic or cast operators in its filter API, so most of the
// corpus is decided by the adapter's in-memory post-filter after an unfiltered `.collect()` — the
// differential is then adapter-CEL against PDP-CEL, and Convex's own comparison and ordering
// semantics only get a say on the shapes that reach the engine. That split is the adapter's
// documented design, but the SIZE of it is a fact about coverage, so it is pinned here and quoted
// in the README rather than left to be re-derived by whoever next wonders.
//
// The lists below name the actions Convex's filter engine decides ON ITS OWN — a filter with no
// post-filter beside it. An action gaining or losing push-down fails this pin, which is the point:
// the README's numbers are only trustworthy while a test enforces them.

// Sorted, and it has to be: the assertion below compares this list against the classification
// with `toEqual`, which is order-sensitive, and that classification is accumulated in
// `ORACLE_ACTIONS` order — which is itself `.sort()`ed where it is built. A new entry therefore
// goes in its alphabetical place, not at the end.
//
// gt-bare, le-bare, not-gt, not-lt, root-bare-bool and root-or are the root-position and
// bare-operand forms (#388). Every one is a mapped, non-nullable field against a single literal —
// or, for root-bare-bool, a boolean field with no literal at all — which is exactly what
// `canPushToDb` accepts, so the engine decides all six with no post-filter beside them. They are
// the largest single addition this list has taken, and that is the finding: the positions the
// corpus had never planned turn out to be the ones Convex pushes down best.
const DB_DECIDED_DEFAULT = [
  "cs-eq",
  "double-negation",
  "double-threshold",
  "empty-string-eq",
  "gt-bare",
  // The primary key against a constant (#376). It reaches the engine for the same reason `cs-eq`
  // does — a mapped, non-nullable field compared with one literal — and is the only one of the
  // six id-* actions that does: the rest compare the key against another field or wrap it in a
  // concatenation, neither of which `canPushToDb` accepts.
  "id-eq-const",
  "in-single",
  "le-bare",
  "nary-and",
  "neg-number",
  "not-and",
  "not-gt",
  "not-lt",
  "p-struct",
  "root-bare-bool",
  "root-or",
  "triple-negation",
  "unicode-eq",
  "vf-ge",
  "vf-le",
  "vf-lt",
  "vf-ne",
];

/**
 * The actions `PUSHDOWN_MAPPER` moves into Convex's filter engine — the null-comparison family,
 * un-blocked by clearing `nullable` on `owner`, a field the seeded documents always carry.
 *
 * These are the ONLY actions the pushdown leg re-executes. `nullable` is read in exactly one place
 * in the adapter — `canPushToDb` — so an action whose execution path the two mappers agree on is
 * translated identically by both, and replaying it would be a second identical query. The
 * classification pin below is what makes that argument checkable rather than assumed: it asserts
 * the full split under both mappers, so an action silently changing path fails there.
 */
const PUSHDOWN_ONLY_ACTIONS = [
  "in-null-elem-mixed",
  "in-null-elem-neg",
  "in-null-elem-only",
  "in-null-elem-only-neg",
  "null-eq",
  "null-ne",
  "null-not-eq",
  // The explicit-null convention against a non-null CONSTANT (#308): a scalar comparison on a
  // mapped field, so the pushdown mapper reaches the database with it. Its field-to-field and
  // macro-fold siblings cannot push down and stay in the post-filter under both mappers.
  "null-value-ne-const",
  "null-value-not-eq-const",
  "null-value-not-in-const",
  "vf-null-ne",
];

const DB_DECIDED_PUSHDOWN = [
  ...DB_DECIDED_DEFAULT,
  ...PUSHDOWN_ONLY_ACTIONS,
].sort();

/** `in-empty` folds to ALWAYS_DENIED, so no mapper can put it in either category. */
const UNCONDITIONAL_ACTIONS = ["in-empty"];

/**
 * Actions whose root `and` splits: part pushed to Convex's filter engine, the rest post-filtered.
 *
 * `rel-hop-and-root` is the first corpus action to reach this path (#375) and the reason the split
 * branch of `buildFilters` is no longer dead code. Its root conjunct `R.attr.aBool == true` is a
 * required field against a literal, so the engine takes it; its other conjunct reads through the
 * to-one hop, which is `nullable` and therefore has to be answered by the adapter's own evaluator.
 * That is the whole point of splitting — the engine narrows, and the semantics that need CEL's
 * missing-attribute error stay with the adapter.
 */
const SPLIT_ACTIONS = ["rel-hop-and-root"];

async function executionFor(
  action: string,
  mapper: Mapper,
): Promise<ExecutionPath> {
  const queryPlan = await cerbos.planResources({
    principal: CHECK_RESOURCES.principal,
    resource: { kind: seedsFile.resourceKind },
    action,
  });
  if (queryPlan.kind !== PlanKind.CONDITIONAL) return "unconditional";
  const { filter, postFilter } = queryPlanToConvex({
    queryPlan,
    mapper,
    allowPostFilter: true,
  });
  if (filter && postFilter) return "split";
  return filter ? "db" : "post";
}

/** Whether `path` — a mapped document field, dotted for nested ones — is present on `document`. */
function documentCarries(document: unknown, path: string): boolean {
  let current: unknown = document;
  for (const part of path.split(".")) {
    if (!isRecord(current)) return false;
    if (!Object.prototype.hasOwnProperty.call(current, part)) return false;
    current = current[part];
  }
  return true;
}

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
// resource owns a FRESH parent (and inner) object rather than pointing at the named seed's own
// document, so no two resources share one and a filter that returned the parent instead of the
// child cannot agree with the oracle by accident.

const seedsById = new Map(seedsFile.seeds.map((seed) => [seed.id, seed]));

function parentSeedOf(seed: Seed | undefined): Seed | undefined {
  const id = seed?.parentSeedId;
  if (id === undefined || id === null) return undefined;
  const parent = seedsById.get(id);
  if (parent === undefined) {
    throw new Error(
      `seeds.json: "${seed?.id}" names parent "${id}", which is not a seed id`,
    );
  }
  return parent;
}

/** The four scalars one level of the chain carries. A NULL column is an ABSENT key. */
function relationLevelOf(seed: Seed): StoredRelationLevel {
  const level: StoredRelationLevel = {
    aBool: seed.aBool,
    aString: seed.aString,
    aNumber: seed.aNumber,
  };
  if (seed.aOptionalString !== null)
    level.aOptionalString = seed.aOptionalString;
  return level;
}

/** The stored `parent` object for a seed, or undefined when it has no parent. */
function storedParent(seed: Seed): StoredDocument["parent"] {
  const parentSeed = parentSeedOf(seed);
  if (parentSeed === undefined) return undefined;
  const innerSeed = parentSeedOf(parentSeed);
  const parent: NonNullable<StoredDocument["parent"]> =
    relationLevelOf(parentSeed);
  if (innerSeed !== undefined) parent.inner = relationLevelOf(innerSeed);
  return parent;
}

function storedDocument(seed: Seed): StoredDocument {
  const document: StoredDocument = {
    id: seed.id,
    aBool: seed.aBool,
    aString: seed.aString,
    aNumber: seed.aNumber,
    createdBy: createdByFor(seed),
    owner: seed.aOptionalString,
    // The explicit-null alias of the `scope` field, the second half of `null-value-f2f`:
    // `scope` itself is omitted when NULL, so the corpus carries the same field under both
    // conventions and the field-to-field probe has two explicit nulls to compare.
    coOwner: scopeFor(seed),
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
  const parent = storedParent(seed);
  if (parent !== undefined) document.parent = parent;
  return document;
}

async function oracleAllowedIds(action: string): Promise<string[]> {
  const ids: string[] = [];
  for (const resource of CHECK_RESOURCES.resources) {
    const result = await cerbos.checkResource({
      principal: CHECK_RESOURCES.principal,
      resource,
      actions: [action],
    });
    if (result.isAllowed(action)) ids.push(resource.id);
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

/**
 * Executes the action's plan in Convex and reports the ids it selected AND which half of the
 * adapter's output selected them. The path comes back from the backend rather than being
 * re-derived here: re-deriving would only re-run the translation this harness already trusts, so a
 * backend that used the wrong mapper would go unnoticed — both halves return the same ids.
 */
async function adapterRun(
  action: string,
  nullAttributeRepresentation: "explicit" | "omitted" = "explicit",
  mapper: MapperVariant = "default",
): Promise<{ ids: string[]; execution: string }> {
  const queryPlan = await cerbos.planResources({
    principal: CHECK_RESOURCES.principal,
    resource: { kind: seedsFile.resourceKind },
    action,
  });
  if (queryPlan.kind === PlanKind.ALWAYS_DENIED) {
    return { ids: [], execution: "unconditional" };
  }
  return convex.query(api.adversarial.executePlan, {
    queryPlan: JSON.parse(JSON.stringify(queryPlan)),
    nullAttributeRepresentation,
    mapper,
  });
}

async function adapterFilteredIds(
  action: string,
  nullAttributeRepresentation: "explicit" | "omitted" = "explicit",
  mapper: MapperVariant = "default",
): Promise<string[]> {
  return (await adapterRun(action, nullAttributeRepresentation, mapper)).ids;
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

  // The invariant is that an inexpressible shape must throw BEFORE its filter can be used, so
  // the assertion wraps translation only: the plan is fetched outside it (a PDP failure fails
  // the test instead of passing it), and no query executes (a store rejecting a wrongly
  // emitted filter cannot masquerade as the adapter refusing to translate).
  //
  // The message comes from this adapter's direct outcome and is asserted, not just the throw:
  // a bare `toThrow()` is
  // satisfied by a mapper typo or a transport error, which the corpus README calls a silent pass.
  // An action added without a pinned message fails classification at load
  // (cerbos/query-plan-adapters#326).
  test.each(THROWING_ACTIONS)(
    "$action fails during translation with the declared message, before any filter exists",
    async ({ action, message }) => {
      await expectCatalogOracle(action);
      const queryPlan = await cerbos.planResources({
        principal: CHECK_RESOURCES.principal,
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
      ).toThrow(message);
    },
  );

  test.each(ORACLE_ACTIONS)("%s matches the check() oracle", async (action) => {
    const [oracle, run] = await Promise.all([
      expectCatalogOracle(action),
      adapterRun(action),
    ]);
    // The path the backend reports is checked against the pinned split, so the README's coverage
    // table is a claim about what executed rather than about what this harness re-translates.
    const expected = DB_DECIDED_DEFAULT.includes(action)
      ? "db"
      : UNCONDITIONAL_ACTIONS.includes(action)
        ? "unconditional"
        : SPLIT_ACTIONS.includes(action)
          ? "split"
          : "post";
    expect({ ids: run.ids, execution: run.execution }).toEqual({
      ids: oracle,
      execution: expected,
    });
  });

  // The pushdown leg. These eight shapes are answered above by the adapter's own CEL evaluator;
  // here the SAME oracle is put to Convex's filter engine instead, so `q.eq(field, null)` and its
  // negations are proved against the PDP rather than assumed to agree with the evaluator
  // (cerbos/query-plan-adapters#327).
  //
  // The reported path is asserted alongside the ids, and that assertion is the whole leg: both
  // halves return the documents check() allows, so a backend that ignored the mapper argument and
  // post-filtered these would satisfy the id comparison and prove nothing.
  test.each(
    PUSHDOWN_ONLY_ACTIONS.filter((action) => MANIFEST_ACTIONS.has(action)),
  )(
    "%s matches the check() oracle when Convex's filter engine decides it",
    async (action) => {
      const [oracle, pushed] = await Promise.all([
        expectCatalogOracle(action),
        adapterRun(action, "explicit", "pushdown"),
      ]);
      expect({ ids: pushed.ids, execution: pushed.execution }).toEqual({
        ids: oracle,
        execution: "db",
      });
    },
  );

  // The pushdown mapper is a claim about the DOCUMENT SHAPE — "this field is never absent" — and
  // nothing in the plan can check it. If a seed ever stopped carrying the key, `canPushToDb` would
  // hand Convex a comparison whose CEL meaning is a missing-attribute error, and the engine would
  // answer it as an ordinary comparison against `undefined`.
  test("the pushdown mapper only demotes fields every seeded document carries", () => {
    const absent: string[] = [];
    for (const seed of seedsFile.seeds) {
      const document = storedDocument(seed);
      for (const field of PUSHDOWN_DEMOTED_FIELDS) {
        if (!documentCarries(document, field)) {
          absent.push(`${seed.id}.${field}`);
        }
      }
    }
    expect({ demoted: [...PUSHDOWN_DEMOTED_FIELDS], absent }).toEqual({
      demoted: ["owner"],
      absent: [],
    });
  });

  fullMatrixTest(
    "pins which corpus actions each mapper hands to Convex's filter engine",
    async () => {
      const classify = async (mapper: Mapper) => {
        const byExecution: Record<ExecutionPath, string[]> = {
          db: [],
          split: [],
          post: [],
          unconditional: [],
        };
        for (const action of ORACLE_ACTIONS) {
          byExecution[await executionFor(action, mapper)].push(action);
        }
        return byExecution;
      };

      const base = await classify(MAPPER);
      const pushdown = await classify(PUSHDOWN_MAPPER);

      expect({
        defaultDb: base.db,
        defaultSplit: base.split,
        defaultUnconditional: base.unconditional,
        pushdownDb: pushdown.db,
        pushdownSplit: pushdown.split,
        pushdownUnconditional: pushdown.unconditional,
        moved: pushdown.db.filter((action) => !base.db.includes(action)),
      }).toEqual({
        defaultDb: DB_DECIDED_DEFAULT,
        // Exactly one corpus action splits: `buildFilters` only splits a root `and`, and
        // rel-hop-and-root is the one hostile shape rooted there that mixes a pushable conjunct
        // with a non-pushable one (#375). Both mappers split it — the hop is `nullable` under each.
        defaultSplit: SPLIT_ACTIONS,
        defaultUnconditional: UNCONDITIONAL_ACTIONS,
        pushdownDb: DB_DECIDED_PUSHDOWN,
        pushdownSplit: SPLIT_ACTIONS,
        pushdownUnconditional: UNCONDITIONAL_ACTIONS,
        moved: PUSHDOWN_ONLY_ACTIONS,
      });
      expect(Object.values(base).flat().sort()).toEqual(ORACLE_ACTIONS);
      expect(Object.values(pushdown).flat().sort()).toEqual(ORACLE_ACTIONS);
    },
  );

  // #387. `filter-as-conjunct` puts a filter() one level below the root, where the guard that
  // refuses `filter-as-condition` did not look — and this adapter is where that mattered: the
  // post-filter read the held list through asBoolean(), got an evaluation error, and denied every
  // row, so the emitted filter AGREED with the empty oracle while translating a shape with no
  // boolean meaning. Its oracle is empty BY CONSTRUCTION, so it belongs to neither
  // hand-written liveness list; a bare "it throws" would say nothing about whether refusing it is
  // REQUIRED.
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
      const queryPlan = await cerbos.planResources({
        principal: CHECK_RESOURCES.principal,
        resource: { kind: seedsFile.resourceKind },
        action: "filter-as-conjunct",
      });
      expect(() =>
        queryPlanToConvex({ queryPlan, mapper: MAPPER, allowPostFilter: true }),
      ).toThrow(entry?.message);
    },
  );

  // #302. `null-eq-missing` probes `aOptionalString == null`, and `aOptionalString` follows the
  // corpus default: a NULL column sends NO attribute, so check() denies every document.
  //
  // Convex is a document store, so the SEEDED SHAPE mirrors that convention directly: this harness
  // omits `aOptionalString` entirely for a NULL column. What decides the action is the ADAPTER'S
  // POST-FILTER, not a Convex `q.eq(field, null)` — `aOptionalString` is `nullable: true` in the
  // mapper, so `canPushToDb` refuses the push-down and the whole predicate is evaluated in
  // JavaScript. There `getNestedValue` finds no such key and yields a CEL missing-attribute error,
  // which denies, so the empty set the oracle demands comes out of the same three-valued logic
  // check() applied (cerbos/query-plan-adapters#327 corrected this rationale — the `q.eq` path it
  // used to name never executes).
  //
  // The `owner` control runs through that same evaluator: it maps to the same seed field but IS
  // stored as an explicit null, so `getNestedValue` finds the key, `null == null` holds, and its
  // proper-subset documents come back. That is what makes the empty result above the document shape talking
  // rather than a filter that matches nothing everywhere.
  //
  // A deployment that stored explicit nulls while omitting the attribute at check time would
  // over-grant exactly as a SQL adapter does, which is what the option guards.

  test.each(NULL_REPRESENTATION_OMITTED)(
    "$action aligns via the omitted document shape and is rejected under omitted ($reason)",
    async ({ action, message }) => {
      await expectCatalogOracle(action);
      expect(await adapterFilteredIds(action, "explicit")).toEqual([]);

      // The rationale above names the post-filter, so pin that it IS the post-filter — as the
      // backend reports it, not as this harness would re-derive it. A mapper change that started
      // pushing either action down would make the comment false while every id comparison passed.
      if (FULL_MATRIX) {
        const explicitNullOracle = await expectCatalogOracle("null-eq");
        const [missingRun, controlRun] = await Promise.all([
          adapterRun(action, "explicit"),
          adapterRun("null-eq", "explicit"),
        ]);
        expect({
          missing: missingRun.execution,
          control: controlRun.execution,
          controlIds: controlRun.ids,
        }).toEqual({
          missing: "post",
          control: "post",
          controlIds: explicitNullOracle,
        });

        // Under the pushdown mapper the control IS answered by Convex's filter engine, so the
        // same documents coming back proves `q.eq(field, null)` agrees with the evaluator on a
        // stored explicit null — the claim the corrected rationale no longer makes about the
        // missing-field case.
        const pushedControl = await adapterRun(
          "null-eq",
          "explicit",
          "pushdown",
        );
        expect(pushedControl).toEqual({
          execution: "db",
          ids: explicitNullOracle,
        });
      }

      // The rejection is the null-operand scan over the plan, which runs before translation picks
      // a path, so it cannot depend on the mapper.
      await expect(adapterFilteredIds(action, "omitted")).rejects.toThrow(
        message,
      );
      await expect(
        adapterFilteredIds(action, "omitted", "pushdown"),
      ).rejects.toThrow(message);
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
          // The rejection must be the null-operand check talking, not an incidental failure —
          // a transport error or mapper typo counting as the required rejection is the silent
          // pass the corpus README warns about.
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

  // The to-one relation carries no corpus action yet — this is the expand half of
  // cerbos/query-plan-adapters#372's expand–contract — so nothing else in this file would notice a
  // seeder that stored no chain at all, or one that wrote the root's own columns one hop out.
  // Read the two hops back out of the stored documents rather than counting them: a count cannot
  // tell the corpus's values from the root's, which is exactly the flat-alias failure this
  // relation exists to make visible.
  test("the seeded to-one chain matches the corpus relation", async () => {
    const withParent = seedsFile.seeds.filter(
      (seed) => parentSeedOf(seed) !== undefined,
    );
    const withInner = seedsFile.seeds.filter(
      (seed) => parentSeedOf(parentSeedOf(seed)) !== undefined,
    );
    expect(withParent.length).toBeGreaterThan(0);
    expect(withInner.length).toBeGreaterThan(0);
    expect(withParent.length).toBeLessThan(seedsFile.seeds.length);

    const stored = await convex.query(api.adversarial.parentChain, {});
    expect(
      Object.fromEntries(
        stored.map((row: ParentChainRow) => [row.id, [row.parent, row.inner]]),
      ),
    ).toEqual(
      Object.fromEntries(
        seedsFile.seeds.map((seed) => [
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
