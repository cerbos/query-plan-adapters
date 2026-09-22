import { afterAll, beforeAll, describe, expect, test } from "@jest/globals";
import type { PlanResourcesResponse, Resource, Value } from "@cerbos/core";
import { GRPC as Cerbos } from "@cerbos/grpc";
import { ConvexHttpClient } from "convex/browser";

import { api } from "../convex/_generated/api.js";
import {
  MAPPER,
  PUSHDOWN_DEMOTED_FIELDS,
  PUSHDOWN_MAPPER,
  type MapperVariant,
} from "../convex/adversarialMapper";
import type { AdversarialDocument } from "../convex/schema";
import { executionPathOf } from "../convex/planExecution";
import type { ExecutionPath } from "../convex/planExecution";
import type { Mapper } from ".";
import { PlanKind, queryPlanToConvex } from ".";
// The corpus reader this adapter carries, shared with src/translator.test.ts. Nothing about
// actions.json, seeds.json or derived-fields.json is parsed twice inside one adapter: one loader
// means one answer to "which shapes must this adapter refuse" and one declaration of the corpus
// keys it consumes. The duplication ACROSS adapters stays deliberate (ADR 0007).
import {
  classifyActionsForAdapter,
  degenerateOraclesOf,
  isRecord,
  nullRepresentationOmittedFor,
  parseActionsFile,
  parseDerivedFile,
  parseSeedsFile,
  planCarriesNullLiteral,
  readCorpusJson,
  assertPinnedPdp,
  pdpAddress,
} from "./corpus";
import type { DerivedEntry, Seed } from "./corpus";

const CONVEX_URL = process.env["CONVEX_URL"] ?? "http://127.0.0.1:3210";
const convex = new ConvexHttpClient(CONVEX_URL);
const cerbos = new Cerbos(pdpAddress(), { tls: false });

type StoredDocument = AdversarialDocument;
type StoredRelationLevel = Omit<NonNullable<StoredDocument["parent"]>, "inner">;

// -- the corpus, read once ----------------------------------------------------------------------
//
// The declared-key guards run inside these parsers, in src/corpus.ts: the seed key set, the tag
// key set, the principal and its attributes, and the derived-fields roster. They are what make a
// corpus field this harness does not consume fail loudly instead of being dropped from the stored
// document and the check() oracle at once (conformance/README.md, "Adding a new hostile shape").

const seedsFile = parseSeedsFile(readCorpusJson("seeds.json"));
const actionsFile = parseActionsFile(readCorpusJson("actions.json"));
const derivedFile = parseDerivedFile(
  readCorpusJson("derived-fields.json"),
  seedsFile.seeds,
);

const CONVEX_UNSUPPORTED = actionsFile.adapterUnsupported["convex"] ?? [];
const CONVEX_SUPPORTED_EXPECTED =
  actionsFile.adapterSupportedExpected["convex"] ?? [];

// The classification is a corpus decision, so it is read rather than re-derived: which actions
// this adapter oracle-compares, which it must refuse, and with which message. src/translator.test.ts
// asserts the same throws offline against the same classifier — two suites, one answer.
const { oracleActions: ORACLE_ACTIONS, throwingActions: THROWING_ACTIONS } =
  classifyActionsForAdapter(actionsFile, "convex");

const KNOWN_DIVERGENCES = new Set(
  actionsFile.knownDivergences
    .filter((entry) => entry.adapters.includes("convex"))
    .map((entry) => entry.action),
);
// Actions whose `== null` probe targets an attribute the oracle OMITS for NULL columns. They
// carry no oracle comparison: under the omitted representation check() denies every document, so
// the adapter must reject the shape rather than emit a filter (#302).
const NULL_REPRESENTATION_OMITTED = nullRepresentationOmittedFor(
  actionsFile,
  "convex",
);
/** The one message every null-carrying action must be rejected with under `omitted`. */
const NULL_OMITTED_MESSAGE = NULL_REPRESENTATION_OMITTED[0]?.message ?? "";
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

// -- the degeneracy guard (conformance/README.md, "The degeneracy guard") -----------------------
//
// The differential cannot fail for an empty or a total oracle: an adapter that matched nothing, or
// everything, would agree with it. So EVERY action this adapter oracle-compares has its oracle's
// shape asserted inside its own comparison, before the ids are compared — reusing the oracle that
// comparison already computes, so the sweep costs no extra PDP round trip.
//
// The actions whose oracle is empty or total BY CONSTRUCTION (a type error, a conversion error, an
// empty-list identity, no seed with two children on its to-one parent, ...) are not named here.
// They are the corpus's `degenerateOracles` list in conformance/actions.json, shared by every
// harness, and each is asserted to be exactly the oracle declared there; every other compared
// action must be non-empty and non-total.

const DEGENERATE_ORACLES = degenerateOraclesOf(actionsFile);
const ALL_SEED_IDS = seedsFile.seeds.map((seed) => seed.id).sort();

/**
 * Shapes Convex refuses to translate: they have no oracle comparison for the sweep to guard, and
 * stay here as PDP/policy liveness probes for a hostile group the compared set cannot cover. See
 * cerbos/query-plan-adapters#324.
 */
const DEGENERACY_LIVENESS_PROBES = [
  // A regex with a top-level alternation falls outside the literal/anchor/trailing-.* subset
  // the post-filter accepts.
  "matches-alt",
  // JSON.stringify(-0) is "0", so the sign of a zero denominator is gone before the adapter
  // sees it and the shape is refused rather than guessed.
  "cr-div-neg-zero",
  // `list` is not in the adapter's known-operator set, so the constructed hierarchy path is
  // refused during structural validation. It is the id-* group's only throwing member here.
  "hier-list-id",
  // #414: every newly discriminating shape guards its observed execution side.
  "div-by-division",
  "except-eq",
  "except-size",
  "hier-overlaps-list-prefix",
  "pv-structs",
  "pv-except",
  "regex-alternation",
  "regex-brace",
  "regex-case",
  "regex-digit",
  "regex-dot",
  "regex-grouped",
  "regex-optional-operators",
  "regex-posix",
  "regex-repetition",
  // #396: error-bearing branches retain a non-empty oracle under their enclosing expression.
  "regex-lookahead",
] as const;

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
  // Several rules composed into one plan (#487): ALLOW with DENY, several ALLOWs, a derived role
  // and a policy variable. Every leaf is `aBool` or `aNumber` — required fields — against a
  // literal, so the whole and/or/not tree the planner builds from the rules reaches the engine.
  // Their one sibling that does not is `compose-variable`, which splits (see SPLIT_ACTIONS).
  "compose-allow-deny",
  "compose-deny-only",
  "compose-derived-deny",
  "compose-derived-role",
  "compose-multi-allow",
  "compose-multi-allow-deny",
  "compose-or-not",
  "compose-two-deny",
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
  // Membership in a map literal: the planner folds it to a key list before it reaches the wire,
  // so the engine sees the same `in` a list literal produces.
  "in-map-keys",
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
  "eq-list",
  "in-numbers",
  "ne-list",
  "root-not-bool",
  "type-number-string",
  "type-string-number",
].sort();

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
const UNCONDITIONAL_ACTIONS = [
  "in-empty",
  "pv-empty-all",
  "pv-empty-exists",
  "pv-empty-not-all",
  "pv-empty-not-exists",
  "pv-structs-missing",
].sort();

/**
 * Actions whose root `and` splits: part pushed to Convex's filter engine, the rest post-filtered.
 *
 * `rel-hop-and-root` is the first corpus action to reach this path (#375) and the reason the split
 * branch of `buildFilters` is no longer dead code. Its root conjunct `R.attr.aBool == true` is a
 * required field against a literal, so the engine takes it; its other conjunct reads through the
 * to-one hop, which is `nullable` and therefore has to be answered by the adapter's own evaluator.
 * That is the whole point of splitting — the engine narrows, and the semantics that need CEL's
 * missing-attribute error stay with the adapter.
 *
 * `compose-variable` (#487) is the second, and the first to get there by composing rules: a DENY
 * on `aNumber > 10` and an ALLOW whose policy variable tests `aOptionalString` against a list
 * become one root `and`. The negated `aNumber` comparison is a required field, so the engine takes
 * it; `aOptionalString` is `nullable`, so its membership test stays with the post-filter.
 */
const SPLIT_ACTIONS = ["compose-variable", "rel-hop-and-root"];

/** The plan the live PDP produces for `action` against the corpus principal. */
function planFor(action: string): Promise<PlanResourcesResponse> {
  return cerbos.planResources({
    principal: seedsFile.principal,
    resource: { kind: seedsFile.resourceKind },
    action,
  });
}

async function executionFor(
  action: string,
  mapper: Mapper,
): Promise<ExecutionPath> {
  return executionPathOf(
    queryPlanToConvex({
      queryPlan: await planFor(action),
      mapper,
      allowPostFilter: true,
    }),
  );
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

/**
 * The document stored for a seed. It is also, minus `id`, the resource's `check()` attributes
 * (`checkResource` below), so the stored row and the oracle cannot disagree about any value.
 */
function storedDocument(seed: Seed): StoredDocument {
  const derived = derivedFor(seed);
  const document: StoredDocument = {
    id: seed.id,
    aBool: seed.aBool,
    aString: seed.aString,
    aNumber: seed.aNumber,
    createdBy: derived.createdBy,
    owner: seed.aOptionalString,
    // The explicit-null alias of the `scope` field, the second half of `null-value-f2f`:
    // `scope` itself is omitted when NULL, so the corpus carries the same field under both
    // conventions and the field-to-field probe has two explicit nulls to compare.
    coOwner: derived.scope,
    tagNames: seed.tags.map((tag) => tag.name),
    // Verbatim, null elements included.
    aNumberList: seed.aNumberList,
    aBoolList: seed.aBoolList,
    obj: { inner: seed.aString },
    tags: seed.tags.map((tag) =>
      tag.name === null ? { id: tag.id } : { id: tag.id, name: tag.name },
    ),
    categories: seed.subCategoryNames.map((name) => ({
      name: "business",
      subCategories: [
        {
          name,
          // Third-level label names. A null element is a NULL label name — a missing element
          // attribute.
          labels: derived.labels.map((labelName) =>
            labelName === null ? {} : { name: labelName },
          ),
        },
      ],
    })),
  };
  if (seed.aOptionalString !== null) {
    document.aOptionalString = seed.aOptionalString;
  }
  if (derived.aDouble !== null) document.aDouble = derived.aDouble;
  if (derived.createdAt !== null) document.createdAt = derived.createdAt;
  if (derived.updatedAt !== null) document.updatedAt = derived.updatedAt;
  if (derived.scope !== null) document.scope = derived.scope;
  if (seed.subCategoryNames.length > 0) {
    document.mainCategory = {
      name: "business",
      subCategories: seed.subCategoryNames.map((name) => ({ name })),
      subNames: seed.subCategoryNames,
    };
  }
  // The real to-one chain. A row with no parent carries NO `parent` key — and so sends no `parent`
  // attribute, a CEL missing-path error (deny); the same holds one level down for `parent.inner`.
  const parent = storedParent(seed);
  if (parent !== undefined) document.parent = parent;
  return document;
}

function checkResource(seed: Seed): Resource {
  const { id, ...attr } = storedDocument(seed);
  return {
    kind: seedsFile.resourceKind,
    id,
    attr: attr as unknown as Record<string, Value>,
  };
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

/**
 * The degeneracy guard's per-action assertion, over an oracle the caller already computed. An
 * action `degenerateOracles` lists must have exactly the oracle declared there; any other must be
 * non-empty and non-total. Labelled so a failure names the action and says why it matters.
 */
function expectOracleShape(action: string, ids: readonly string[]): void {
  const declared = DEGENERATE_ORACLES.get(action);
  const shape =
    ids.length === 0
      ? "empty"
      : ids.length === ALL_SEED_IDS.length
        ? "total"
        : "non-degenerate";
  const why =
    declared === undefined
      ? "the differential cannot fail for a degenerate oracle; if this oracle is empty or total " +
        "by construction, declare it in `degenerateOracles` in conformance/actions.json"
      : `conformance/actions.json declares this oracle "${declared}" in \`degenerateOracles\`; ` +
        "the differential cannot fail for a degenerate oracle, so the declaration must stay exact";
  expect({ action, oracle: shape, why }).toEqual({
    action,
    oracle: declared ?? "non-degenerate",
    why,
  });
}

/** The liveness probes' assertion: a refused shape's oracle is still non-empty and non-total. */
async function expectNonDegenerateOracle(action: string): Promise<void> {
  const ids = await oracleAllowedIds(action);
  expect({
    action,
    nonEmpty: ids.length > 0,
    nonTotal: ids.length < ALL_SEED_IDS.length,
  }).toEqual({ action, nonEmpty: true, nonTotal: true });
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
  const queryPlan = await planFor(action);
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
  await assertPinnedPdp(cerbos);
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
    const allActions = MANIFEST_ACTIONS;
    const oracle = new Set(ORACLE_ACTIONS);
    const throwing = new Set(THROWING_ACTIONS.map(({ action }) => action));
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

    expect(allActions.size).toBe(310);
    expect(CONVEX_UNSUPPORTED).toHaveLength(26);
    expect(CONVEX_SUPPORTED_EXPECTED).toHaveLength(7);
    expect(ORACLE_ACTIONS).toHaveLength(278);
    expect(THROWING_ACTIONS).toHaveLength(30);
    expect(misclassified).toEqual([]);
  });

  // The invariant is that an inexpressible shape must throw BEFORE its filter can be used, so
  // the assertion wraps translation only: the plan is fetched outside it (a PDP failure fails
  // the test instead of passing it), and no query executes (a store rejecting a wrongly
  // emitted filter cannot masquerade as the adapter refusing to translate).
  //
  // The message comes from the corpus and is asserted, not just the throw: a bare `toThrow()` is
  // satisfied by a mapper typo or a transport error, which the corpus README calls a silent pass.
  // An action added without a pinned message fails classification at load
  // (cerbos/query-plan-adapters#326).
  test.each(THROWING_ACTIONS)(
    "$action fails during translation with the declared message, before any filter exists",
    async ({ action, message }) => {
      const queryPlan = await planFor(action);
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
      oracleAllowedIds(action),
      adapterRun(action),
    ]);
    // The degeneracy sweep, before the comparison it guards: see expectOracleShape.
    expectOracleShape(action, oracle);
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
  test.each(PUSHDOWN_ONLY_ACTIONS)(
    "%s matches the check() oracle when Convex's filter engine decides it",
    async (action) => {
      const [oracle, pushed] = await Promise.all([
        oracleAllowedIds(action),
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

  // The README quotes these counts; this is what keeps them true. A shape that gains or loses
  // push-down fails here rather than silently making the documented coverage a lie.
  test("pins how much of the corpus each mapper hands to Convex's filter engine", async () => {
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
      total: ORACLE_ACTIONS.length,
      defaultDb: base.db,
      defaultSplit: base.split,
      defaultUnconditional: base.unconditional,
      defaultPostCount: base.post.length,
      pushdownDb: pushdown.db,
      pushdownSplit: pushdown.split,
      pushdownPostCount: pushdown.post.length,
      // The pushdown leg only needs to re-execute actions whose routing changes.
      moved: pushdown.db.filter((action) => !base.db.includes(action)),
    }).toEqual({
      total: 278,
      defaultDb: DB_DECIDED_DEFAULT,
      // Exactly two corpus actions split: `buildFilters` only splits a root `and`, and
      // rel-hop-and-root (#375) and compose-variable (#487) are the hostile shapes rooted there
      // that mix a pushable conjunct with a non-pushable one. Both mappers split both — the hop
      // and `aOptionalString` are `nullable` under each.
      defaultSplit: SPLIT_ACTIONS,
      defaultUnconditional: UNCONDITIONAL_ACTIONS,
      defaultPostCount: 233,
      pushdownDb: DB_DECIDED_PUSHDOWN,
      pushdownSplit: SPLIT_ACTIONS,
      pushdownPostCount: 222,
      moved: PUSHDOWN_ONLY_ACTIONS,
    });
  });

  // #387. `filter-as-conjunct` puts a filter() one level below the root, where the guard that
  // refuses `filter-as-condition` did not look — and this adapter is where that mattered: the
  // post-filter read the held list through asBoolean(), got an evaluation error, and denied every
  // row, so the emitted filter AGREED with the empty oracle while translating a shape with no
  // boolean meaning. Its oracle is empty BY CONSTRUCTION — it is a corpus
  // `degenerateOracles` entry, not a liveness probe — so a bare "it throws" would say nothing
  // about whether refusing it is REQUIRED.
  //
  // This is that argument. The other conjunct is `R.attr.aBool`, which the adapter certainly can
  // express and which `root-bare-bool` spells on its own; an adapter that dropped the conjunct it
  // could not translate would emit exactly that filter and return every row it selects, all of
  // which the PDP denies for this action.
  test("filter-as-conjunct must be refused: dropping its untranslatable half over-grants", async () => {
    expect(await oracleAllowedIds("filter-as-conjunct")).toEqual([]);

    const survivingHalf = await adapterFilteredIds("root-bare-bool");
    expect(survivingHalf.length).toBeGreaterThan(0);
    expect(survivingHalf.length).toBeLessThan(seedsFile.seeds.length);

    const entry = THROWING_ACTIONS.find(
      ({ action }) => action === "filter-as-conjunct",
    );
    expect(entry).toBeDefined();
    const queryPlan = await planFor("filter-as-conjunct");
    expect(() =>
      queryPlanToConvex({ queryPlan, mapper: MAPPER, allowPostFilter: true }),
    ).toThrow(entry?.message);
  });

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
  // five documents come back. That is what makes the empty result above the document shape talking
  // rather than a filter that matches nothing everywhere.
  //
  // A deployment that stored explicit nulls while omitting the attribute at check time would
  // over-grant exactly as a SQL adapter does, which is what the option guards.

  test.each(NULL_REPRESENTATION_OMITTED)(
    "$action aligns via the omitted document shape and is rejected under omitted ($reason)",
    async ({ action, message }) => {
      expect(await oracleAllowedIds(action)).toEqual([]);
      expect(await adapterFilteredIds(action, "explicit")).toEqual([]);

      // The rationale above names the post-filter, so pin that it IS the post-filter — as the
      // backend reports it, not as this harness would re-derive it. A mapper change that started
      // pushing either action down would make the comment false while every id comparison passed.
      const explicitNullOracle = await oracleAllowedIds("null-eq");
      expect(explicitNullOracle.length).toBeGreaterThan(0);
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

      // Under the pushdown mapper the control IS answered by Convex's filter engine, so the same
      // five documents coming back proves `q.eq(field, null)` agrees with the evaluator on a
      // stored explicit null — the claim the corrected rationale no longer makes about the
      // missing-field case.
      const pushedControl = await adapterRun("null-eq", "explicit", "pushdown");
      expect(pushedControl).toEqual({
        execution: "db",
        ids: explicitNullOracle,
      });

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
  test("every corpus action carrying a null literal is rejected under omitted", async () => {
    const nullCarrying: string[] = [];
    for (const action of [...MANIFEST_ACTIONS].sort()) {
      const queryPlan = await planFor(action);
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
  });

  test("pins the upstream has() planner over-grant", async () => {
    const action = "p-has";
    const queryPlan = await planFor(action);
    const oracle = await oracleAllowedIds(action);
    const allIds = ALL_SEED_IDS;

    expect(queryPlan.kind).toBe(PlanKind.ALWAYS_ALLOWED);
    expect(oracle.length).toBeGreaterThan(0);
    expect(oracle.length).toBeLessThan(allIds.length);
    expect(await adapterFilteredIds(action)).toEqual(allIds);
  });

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
        stored.map((row) => [row.id, [row.parent, row.inner]]),
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

  // Every compared action's oracle is swept inside its own comparison above, so the only guard
  // left to state here is the complement: each liveness probe is an action Convex REFUSES (an
  // action Convex gains support for must leave this list, and the sweep then covers it), and its
  // oracle is still non-empty and non-total, so the PDP and policy are proved live for a hostile
  // group the compared set cannot reach. Each action gets its own test budget.
  test.each(DEGENERACY_LIVENESS_PROBES)(
    "%s has a non-degenerate liveness oracle",
    async (action) => {
      expect(ORACLE_ACTIONS).not.toContain(action);
      await expectNonDegenerateOracle(action);
    },
  );

  // Every `degenerateOracles` entry, compared by Convex or not, is asserted to have exactly the
  // oracle the corpus declares — the sweep only reaches the compared ones, and a declaration that
  // stopped being true for a refused shape would otherwise go unnoticed. Where a live planner KIND
  // was pinned for one of these, it still is: dropping a shape's inputs can silently turn a
  // conditional error probe into a folded plan whose oracle happens to be the same.
  const DEGENERATE_PLAN_KINDS: Record<string, PlanKind> = {
    "except-root": PlanKind.CONDITIONAL,
    "pv-empty-exists": PlanKind.ALWAYS_DENIED,
    "pv-empty-not-exists": PlanKind.ALWAYS_ALLOWED,
    "pv-empty-all": PlanKind.ALWAYS_ALLOWED,
    "pv-empty-not-all": PlanKind.ALWAYS_DENIED,
    "pv-structs-null": PlanKind.CONDITIONAL,
    "pv-structs-missing": PlanKind.ALWAYS_DENIED,
    "type-string-number": PlanKind.CONDITIONAL,
    "type-number-string": PlanKind.CONDITIONAL,
    "type-columns": PlanKind.CONDITIONAL,
    "type-size-bool": PlanKind.CONDITIONAL,
    "type-size-number": PlanKind.CONDITIONAL,
    "type-hierarchy-number": PlanKind.CONDITIONAL,
    "type-number-contains": PlanKind.CONDITIONAL,
    "type-needle-contains": PlanKind.CONDITIONAL,
    "type-number-startswith": PlanKind.CONDITIONAL,
    "type-needle-startswith": PlanKind.CONDITIONAL,
    "type-number-endswith": PlanKind.CONDITIONAL,
    "type-needle-endswith": PlanKind.CONDITIONAL,
    "eq-map": PlanKind.CONDITIONAL,
    "ne-map": PlanKind.CONDITIONAL,
    "eq-map-null": PlanKind.CONDITIONAL,
    "in-nested-list": PlanKind.CONDITIONAL,
    "in-list-element": PlanKind.CONDITIONAL,
    "hasint-map-element": PlanKind.CONDITIONAL,
  };

  test("every pinned planner kind belongs to a declared degenerate oracle", () => {
    expect(
      Object.keys(DEGENERATE_PLAN_KINDS).filter(
        (action) => !DEGENERATE_ORACLES.has(action),
      ),
    ).toEqual([]);
  });

  test.each(actionsFile.degenerateOracles)(
    "$action has exactly its declared $oracle oracle",
    async ({ action, oracle }) => {
      const kind = DEGENERATE_PLAN_KINDS[action];
      const [plan, ids] = await Promise.all([
        kind === undefined ? undefined : planFor(action),
        oracleAllowedIds(action),
      ]);
      if (kind !== undefined) expect(plan?.kind).toBe(kind);
      expect(ids).toEqual(oracle === "total" ? ALL_SEED_IDS : []);
    },
  );
});
