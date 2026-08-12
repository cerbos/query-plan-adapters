import * as fs from "fs";
import * as path from "path";

import { afterAll, beforeAll, describe, expect, test } from "@jest/globals";
import type { Principal, Resource, Value } from "@cerbos/core";
import { GRPC as Cerbos } from "@cerbos/grpc";
import { ConvexHttpClient } from "convex/browser";

import { api } from "../convex/_generated/api.js";
import {
  MAPPER,
  PUSHDOWN_DEMOTED_FIELDS,
  PUSHDOWN_MAPPER,
  type MapperVariant,
} from "../convex/adversarial";
import type { ExecutionPath } from "../convex/planExecution";
import type { Mapper } from ".";
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

interface ExpectedUnsupported {
  action: string;
  /** One entry per adapter that must reject the shape; the corpus asserts the key set. */
  messages: Record<string, string>;
}

interface AdapterOutcome {
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

interface ActionsFile {
  conformance: string[];
  adapterUnsupported: Record<string, AdapterOutcome[]>;
  adapterSupportedExpected: Record<string, AdapterOutcome[]>;
  expectedUnsupported: ExpectedUnsupported[];
  nullRepresentationOmitted: NullRepresentationOmitted[];
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
  isStringArray(value["subCategoryNames"]) &&
  (typeof value["parentSeedId"] === "string" || value["parentSeedId"] === null);

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

const isMessageMap = (value: unknown): value is Record<string, string> =>
  isRecord(value) &&
  Object.values(value).every((message) => typeof message === "string");

const isExpectedUnsupported = (value: unknown): value is ExpectedUnsupported =>
  isRecord(value) &&
  typeof value["action"] === "string" &&
  isMessageMap(value["messages"]);

const isNullRepresentationOmitted = (
  value: unknown,
): value is NullRepresentationOmitted =>
  isRecord(value) &&
  typeof value["action"] === "string" &&
  typeof value["reason"] === "string" &&
  isMessageMap(value["messages"]);

const isAdapterOutcome = (value: unknown): value is AdapterOutcome =>
  isRecord(value) &&
  typeof value["action"] === "string" &&
  typeof value["reason"] === "string" &&
  // `adapterUnsupported` carries this and the classification below requires it;
  // `adapterSupportedExpected` and `nullRepresentationOmitted` do not throw, so they do not.
  (value["message"] === undefined || typeof value["message"] === "string");

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
  value["nullRepresentationOmitted"].every(isNullRepresentationOmitted) &&
  Array.isArray(value["knownDivergences"]) &&
  value["knownDivergences"].every(isKnownDivergence);

const isDerivedEntry = (value: unknown): value is DerivedEntry =>
  isRecord(value) &&
  typeof value["createdBy"] === "string" &&
  (typeof value["aDouble"] === "number" || value["aDouble"] === null) &&
  (typeof value["createdAt"] === "string" || value["createdAt"] === null) &&
  (typeof value["scope"] === "string" || value["scope"] === null) &&
  Array.isArray(value["labels"]) &&
  value["labels"].every((label) => label === null || typeof label === "string");

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
if (!isActionsFile(parsedActions))
  throw new Error("Invalid conformance actions");
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
/**
 * A shape this adapter must refuse, with the substring its error has to contain.
 *
 * The message is what turns "it threw" into "it threw for the declared reason": without it a
 * mapper typo or an unrelated validation satisfies the assertion just as well as the limitation
 * the corpus documents. This harness used to keep the map locally; it now reads the corpus, so
 * the pin covers every adapter rather than this one (cerbos/query-plan-adapters#326).
 */
interface ThrowingAction {
  action: string;
  message: string;
}

/** The pinned message, or a failure — a throwing action without one asserts nothing. */
const requireMessage = (label: string, message: string | undefined): string => {
  if (message === undefined || message === "") {
    throw new Error(
      `actions.json pins no throw message for ${label}: the throw suite would accept a failure for any reason`,
    );
  }
  return message;
};

const THROWING_ACTIONS: ThrowingAction[] = [
  ...CONVEX_UNSUPPORTED.map(({ action, message }) => ({
    action,
    message: requireMessage(`adapterUnsupported.convex.${action}`, message),
  })),
  ...actionsFile.expectedUnsupported
    .filter(({ action }) => !SUPPORTED_EXPECTED_ACTIONS.has(action))
    .map(({ action, messages }) => ({
      action,
      message: requireMessage(
        `expectedUnsupported.${action}.messages.convex`,
        messages["convex"],
      ),
    })),
].sort((left, right) => left.action.localeCompare(right.action));
const KNOWN_DIVERGENCES = new Set(
  actionsFile.knownDivergences
    .filter((entry) => entry.adapters.includes("convex"))
    .map((entry) => entry.action),
);
// Actions whose `== null` probe targets an attribute the oracle OMITS for NULL columns. They
// carry no oracle comparison: under the omitted representation check() denies every document, so
// the adapter must reject the shape rather than emit a filter (#302).
const NULL_REPRESENTATION_OMITTED = actionsFile.nullRepresentationOmitted.map(
  ({ action, reason, messages }) => ({
    action,
    reason,
    message: requireMessage(
      `nullRepresentationOmitted.${action}.messages.convex`,
      messages["convex"],
    ),
  }),
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
  // The explicit-null convention against a non-null operand (#308). Convex stores the value the
  // caller sends, so a document field holding an explicit null compares as a null VALUE exactly
  // as CEL does: these five needed no change, and are compared rather than probed.
  "null-value-ne-const",
  "null-value-not-eq-const",
  "null-value-not-in-const",
  "null-value-f2f",
  "null-value-pv-not-exists",
  // The absent to-one parent (#309/#315/#316/#333/#334): the seven discriminating chain shapes
  // with a non-empty oracle.
  "w1-all-chain",
  "w1-not-exists-chain",
  "w1-size-nonneg-chain",
  "w1-not-in-chain",
  "w1-not-hasint-chain",
  "w1-ternary-chain-cond",
  "w1-size-frac-le-chain",
  // Column arithmetic under a division (#311); the zero-denominator arm is a liveness probe.
  "cr-div-other-column",
  "cr-div-then-add",
  "cr-div-then-add-ne",
  // Convex is the one adapter that promotes the casts in adapterSupportedExpected, so this is
  // a real comparison here rather than the liveness probe it is everywhere else.
  "cast-int-double",
  // The real to-one join (#375): one per hazard — the negated hop, the null comparison, two-level
  // depth, the root conjunction (the corpus's only SPLIT execution here), and the disjunction,
  // whose failure direction is an under-grant.
  "rel-not-bool-hop",
  "rel-ne-null-hop",
  "rel-bool-hop2",
  "rel-hop-and-root",
  "rel-hop2-or-exists",
  // Case sensitivity in STRING MATCHING (#375 follow-up), a different mechanism from cs-eq:
  // collation governs `=`, and on SQLite nothing but `PRAGMA case_sensitive_like` governs LIKE.
  "cs-contains",
  // The primary key as a filterable attribute (#376): the one id-* action the filter engine
  // decides on its own, the negated field-to-field, and the two concatenations — which the
  // post-filter answered with an evaluation error, and so no rows, before it learned CEL's
  // string overload of `+`.
  "id-eq-const",
  "id-f2f-ne",
  "id-concat",
  "id-concat-vf",
  // string() over a boolean and over a non-integer double, both of which the post-filter denied
  // outright before this change. Convex renders them in JavaScript rather than in a store, so
  // unlike the SQL adapters it can and does agree with CEL exactly.
  "cast-string-bool",
  "cast-string-double",
  // CEL's `+` between two COLUMNS (#391). The post-filter concatenates in JavaScript, which is
  // CEL's own semantics, so convex needs no operand-type declaration to resolve the overload.
  "concat-f2f",
  // Root position and bare operand forms (#388): one per hazard — the negation over a bare
  // ordering (every other negated ordering in the corpus wraps a size() or a ternary), the bare
  // boolean at the ROOT of the condition, and the collection subquery disjoined with a scalar
  // predicate rather than conjoined with one.
  "not-lt",
  "root-bare-bool",
  "or-eq-exists",
  // Hazard classes the corpus missed (#387). Convex translates all eleven of the compared ones,
  // and for three of them it is the ONLY adapter that does — the post-filter reimplements CEL
  // rather than lowering to a query language, so modulo, a positional read of a scalar list and
  // list equality all have exact meanings here. Those three carry the whole corpus's oracle
  // comparison for their groups; every other adapter probes them fail-closed.
  "not-and",
  "not-contains",
  "arith-mod",
  "index-scalar-list",
  "map-eq-list",
  "vf-hasint",
  "pv-exists-unrolled",
] as const;

/**
 * Shapes Convex refuses to translate: they have no oracle comparison to guard, and stay here as
 * PDP/policy liveness probes for a group Convex's own list cannot cover. See
 * cerbos/query-plan-adapters#324.
 */
const DEGENERACY_LIVENESS_PROBES = [
  // JSON.stringify(-0) is "0", so the sign of a zero denominator is gone before the adapter
  // sees it and the shape is refused rather than guessed.
  "cr-div-neg-zero",
  // `list` is not in the adapter's known-operator set, so the constructed hierarchy path is
  // refused during structural validation. It is the id-* group's only throwing member here.
  "hier-list-id",
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
    principal: seedsFile.principal,
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

function checkResource(seed: Seed): Resource {
  const attr: Record<string, Value> = {
    aBool: seed.aBool,
    aString: seed.aString,
    aNumber: seed.aNumber,
    createdBy: createdByFor(seed),
    owner: seed.aOptionalString,
    coOwner: scopeFor(seed),
    tagNames: seed.tags.map((tag) => tag.name),
    obj: { inner: seed.aString },
    tags: seed.tags.map(
      (tag): Record<string, Value> =>
        tag.name === null ? { id: tag.id } : { id: tag.id, name: tag.name },
    ),
    categories: seed.subCategoryNames.map((name) => ({
      name: "business",
      subCategories: [
        {
          name,
          labels: labelsFor(seed).map(
            (labelName): Record<string, Value> =>
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
  // The real to-one chain, mirroring the stored document exactly. A row with no parent sends NO
  // `parent` attribute — a CEL missing-path error (deny) — matching the stored document having no
  // `parent` key; the same holds one level down for `parent.inner`.
  const parent = storedParent(seed);
  if (parent !== undefined) attr["parent"] = parent as unknown as Value;
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

/** The degeneracy guard's per-action assertion, labelled so a failure names the action. */
async function expectNonDegenerateOracle(action: string): Promise<void> {
  const ids = await oracleAllowedIds(action);
  expect({
    action,
    nonEmpty: ids.length > 0,
    nonTotal: ids.length < seedsFile.seeds.length,
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
  const queryPlan = await cerbos.planResources({
    principal: seedsFile.principal,
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

    expect(allActions.size).toBe(199);
    expect(CONVEX_UNSUPPORTED).toHaveLength(3);
    expect(CONVEX_SUPPORTED_EXPECTED).toHaveLength(7);
    expect(ORACLE_ACTIONS).toHaveLength(191);
    expect(THROWING_ACTIONS).toHaveLength(6);
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
      ).toThrow(message);
    },
  );

  test.each(ORACLE_ACTIONS)("%s matches the check() oracle", async (action) => {
    const [oracle, run] = await Promise.all([
      oracleAllowedIds(action),
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
      // The two mappers must differ ONLY where the pushdown leg re-executes, which is what makes
      // skipping the other 156 actions there sound rather than a coverage hole.
      moved: pushdown.db.filter((action) => !base.db.includes(action)),
    }).toEqual({
      total: 191,
      defaultDb: DB_DECIDED_DEFAULT,
      // Exactly one corpus action splits: `buildFilters` only splits a root `and`, and
      // rel-hop-and-root is the one hostile shape rooted there that mixes a pushable conjunct
      // with a non-pushable one (#375). Both mappers split it — the hop is `nullable` under each.
      defaultSplit: SPLIT_ACTIONS,
      defaultUnconditional: UNCONDITIONAL_ACTIONS,
      defaultPostCount: 167,
      pushdownDb: DB_DECIDED_PUSHDOWN,
      pushdownSplit: SPLIT_ACTIONS,
      pushdownPostCount: 156,
      moved: PUSHDOWN_ONLY_ACTIONS,
    });
  });

  // #387. `filter-as-conjunct` puts a filter() one level below the root, where the guard that
  // refuses `filter-as-condition` did not look — and this adapter is where that mattered: the
  // post-filter read the held list through asBoolean(), got an evaluation error, and denied every
  // row, so the emitted filter AGREED with the empty oracle while translating a shape with no
  // boolean meaning. Its oracle is empty BY CONSTRUCTION, so it belongs to neither
  // degeneracy-guard list and a bare "it throws" would say nothing about whether refusing it is
  // REQUIRED.
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
    const queryPlan = await cerbos.planResources({
      principal: seedsFile.principal,
      resource: { kind: seedsFile.resourceKind },
      action: "filter-as-conjunct",
    });
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

  test("oracle is not degenerate", async () => {
    // Guard the guard: each of these actions must produce a non-empty, non-total oracle set,
    // otherwise the differential comparison could pass vacuously (e.g. PDP denying all).
    //
    // Every entry is asserted to be an action Convex actually oracle-compares. A list copied
    // from another harness drifts into naming shapes this adapter never compares, which guard
    // nothing (cerbos/query-plan-adapters#324); the membership assertion turns moving an action
    // into Convex's `adapterUnsupported` set into a failure here rather than a silent no-op.
    for (const action of DEGENERACY_GUARD_ACTIONS) {
      expect(ORACLE_ACTIONS).toContain(action);
      await expectNonDegenerateOracle(action);
    }
    // Asserting the complement keeps the split honest — an action Convex gains support for
    // must move up into the guard proper.
    for (const action of DEGENERACY_LIVENESS_PROBES) {
      expect(ORACLE_ACTIONS).not.toContain(action);
      await expectNonDegenerateOracle(action);
    }
  });
});
