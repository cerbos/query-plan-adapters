import * as fs from "fs";
import * as path from "path";

import {
  PlanExpression,
  PlanExpressionValue,
  PlanExpressionVariable,
} from "@cerbos/core";
import type {
  PlanExpressionOperand,
  PlanResourcesResponse,
  Principal,
  Value,
} from "@cerbos/core";

import { PlanKind } from ".";

/**
 * The parts of the shared `../conformance/` corpus that both of this adapter's suites read, plus
 * the reader for the golden expectations this adapter owns.
 *
 * `adversarial.test.ts` plans against a real PDP and executes the translated query inside a real
 * Convex backend; `translator.test.ts` reads the same actions off the golden wire fixtures and
 * asserts nothing but the filter the adapter emits. They must agree on two things or they prove
 * less than they appear to:
 *
 * - **the mapper.** The unit test pins the filter this adapter emits for a mapping; the harness
 *   proves that same filter returns the documents the PDP allows. Both read `MAPPER` from
 *   `../convex/adversarialMapper`, which is also where the Convex backend reads it — one
 *   definition, three readers, no copy to drift.
 * - **the classification.** Which actions this adapter must refuse, and with which message, is a
 *   corpus decision (`actions.json`), not a per-suite one.
 *
 * The declared-key guards live here for the same reason, even though only the harness consumes
 * seeds: they are one adapter's statement of what it reads out of the corpus, and a second copy
 * would be a second answer. They are exposed as `parse*` functions rather than run on import, so
 * the offline suite neither pays for them nor trips over a guard about data it never touches.
 *
 * The code in this file is duplicated across adapters **on purpose** — adapters share data, not
 * code, so that every adapter stays standalone. Do not extract it into `conformance/`, do not
 * import another adapter's copy, and do not add a drift check between them. See
 * [ADR 0007](../../docs/adr/0007-adapters-share-data-not-code.md).
 *
 * Test-only: excluded from `tsc --build` by `tsconfig.json`, so nothing here reaches `lib/`.
 */

export const ADAPTER = "convex";

export const CONFORMANCE_DIR = path.join(__dirname, "..", "..", "conformance");

const WIRE_FIXTURES_DIR = path.join(CONFORMANCE_DIR, "wire-fixtures");

/** The golden expectations this adapter owns. Never under `conformance/` — see ADR 0007. */
export const GOLDEN_FILE = path.join(
  __dirname,
  "..",
  "golden",
  "expectations.json",
);

export function readCorpusJson(file: string): unknown {
  return JSON.parse(fs.readFileSync(path.join(CONFORMANCE_DIR, file), "utf8"));
}

// -- corpus JSON validators ------------------------------------------------------------------------
//
// A corpus file is machine-generated and diffed by validate-corpus.sh, so these are not here to
// catch a malformed file. They are what stops a field this repository RENAMED from arriving as
// `undefined` and being read as "absent" by a suite that would then assert nothing — the projection
// trap `conformance/README.md` describes, applied to the parse rather than to a hand-written
// projection.

export const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

export const isStringArray = (value: unknown): value is string[] =>
  Array.isArray(value) && value.every((item) => typeof item === "string");

const isMessageMap = (value: unknown): value is Record<string, string> =>
  isRecord(value) &&
  Object.values(value).every((message) => typeof message === "string");

/**
 * Set equality between the keys a corpus record carries and the keys a suite declares it consumes.
 *
 * Both directions matter. A corpus key nothing reads is dropped from the stored document and the
 * `check()` oracle at once, so the differential agrees for the wrong reason; a declared key the
 * corpus no longer carries is a suite reading `undefined` and calling it data.
 */
export function assertKeys(
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

// -- actions.json --------------------------------------------------------------------------------

export interface UnsupportedShape {
  action: string;
  shape: string;
  /** One entry per adapter that must reject the shape; the corpus asserts the key set. */
  messages: Record<string, string>;
}

export interface AdapterOutcome {
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
export interface NullRepresentationOmittedEntry {
  action: string;
  reason: string;
  messages: Record<string, string>;
}

export interface KnownDivergence {
  action: string;
  adapters: string[];
}

export interface ActionsFile {
  conformance: string[];
  adapterUnsupported: Record<string, AdapterOutcome[]>;
  adapterSupportedExpected: Record<string, AdapterOutcome[]>;
  expectedUnsupported: UnsupportedShape[];
  nullRepresentationOmitted: NullRepresentationOmittedEntry[];
  knownDivergences: KnownDivergence[];
}

const isUnsupportedShape = (value: unknown): value is UnsupportedShape =>
  isRecord(value) &&
  typeof value["action"] === "string" &&
  typeof value["shape"] === "string" &&
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

const isNullRepresentationOmitted = (
  value: unknown,
): value is NullRepresentationOmittedEntry =>
  isRecord(value) &&
  typeof value["action"] === "string" &&
  typeof value["reason"] === "string" &&
  isMessageMap(value["messages"]);

const isKnownDivergence = (value: unknown): value is KnownDivergence =>
  isRecord(value) &&
  typeof value["action"] === "string" &&
  isStringArray(value["adapters"]);

/**
 * `actions.json`, validated rather than cast.
 *
 * Every group the interface declares is checked: a group this predicate does not name would be
 * read as `undefined` by whichever suite consumes it, and a suite that silently consumes nothing
 * is the failure the corpus README warns about.
 */
export function parseActionsFile(value: unknown): ActionsFile {
  if (
    !isRecord(value) ||
    !isStringArray(value["conformance"]) ||
    !isAdapterMap(value["adapterUnsupported"]) ||
    !isAdapterMap(value["adapterSupportedExpected"]) ||
    !Array.isArray(value["expectedUnsupported"]) ||
    !value["expectedUnsupported"].every(isUnsupportedShape) ||
    !Array.isArray(value["nullRepresentationOmitted"]) ||
    !value["nullRepresentationOmitted"].every(isNullRepresentationOmitted) ||
    !Array.isArray(value["knownDivergences"]) ||
    !value["knownDivergences"].every(isKnownDivergence)
  ) {
    throw new Error("Invalid conformance actions");
  }
  return value as unknown as ActionsFile;
}

/**
 * A shape this adapter must refuse, with the substring its error has to contain.
 *
 * The message is what turns "it threw" into "it threw for the declared reason": without it a
 * mapper typo or an unrelated validation satisfies the assertion just as well as the limitation
 * the corpus documents (cerbos/query-plan-adapters#326).
 */
export interface ThrowingAction {
  action: string;
  /** Why the corpus says this adapter cannot express the shape. Titles read better with it. */
  reason: string;
  message: string;
}

export interface ActionClassification {
  oracleActions: string[];
  throwingActions: ThrowingAction[];
  supportedExpected: Set<string>;
}

/** The pinned message, or a failure — a throwing action without one asserts nothing. */
export function requireMessage(
  label: string,
  message: string | undefined,
): string {
  if (message === undefined || message === "") {
    throw new Error(
      `actions.json pins no throw message for ${label}: the throw suite would accept a failure for any reason`,
    );
  }
  return message;
}

export function classifyActionsForAdapter(
  manifest: ActionsFile,
  adapter: string,
): ActionClassification {
  const unsupported = manifest.adapterUnsupported[adapter] ?? [];
  const unsupportedActions = new Set(unsupported.map((entry) => entry.action));
  const supportedExpected = new Set(
    (manifest.adapterSupportedExpected[adapter] ?? []).map(
      (entry) => entry.action,
    ),
  );
  const oracleActions = [
    ...manifest.conformance.filter((action) => !unsupportedActions.has(action)),
    ...supportedExpected,
  ];
  const throwingActions: ThrowingAction[] = [
    ...unsupported.map(
      (entry): ThrowingAction => ({
        action: entry.action,
        reason: entry.reason,
        message: requireMessage(
          `adapterUnsupported.${adapter}.${entry.action}`,
          entry.message,
        ),
      }),
    ),
    ...manifest.expectedUnsupported
      .filter((entry) => !supportedExpected.has(entry.action))
      .map(
        (entry): ThrowingAction => ({
          action: entry.action,
          reason: entry.shape,
          message: requireMessage(
            `expectedUnsupported.${entry.action}.messages.${adapter}`,
            entry.messages[adapter],
          ),
        }),
      ),
  ];

  return {
    oracleActions: [...new Set(oracleActions)].sort(),
    throwingActions: throwingActions.sort((left, right) =>
      left.action.localeCompare(right.action),
    ),
    supportedExpected,
  };
}

/**
 * The `nullRepresentationOmitted` entries, each carrying the message THIS adapter must reject with.
 *
 * Separate from the classification above because these are not a throw the adapter chose: every
 * adapter must reject them, the two NULL conventions being indistinguishable on the wire (#302).
 */
export function nullRepresentationOmittedFor(
  manifest: ActionsFile,
  adapter: string,
): (NullRepresentationOmittedEntry & { message: string })[] {
  return manifest.nullRepresentationOmitted.map((entry) => ({
    ...entry,
    message: requireMessage(
      `nullRepresentationOmitted.${entry.action}.messages.${adapter}`,
      entry.messages[adapter],
    ),
  }));
}

// -- seeds.json and derived-fields.json -----------------------------------------------------------
//
// Only the adversarial harness consumes these — the translator unit test asserts filters, not rows
// — but they live here with the rest of the corpus reader, and they are FUNCTIONS rather than
// module-level parses so that importing this file for the offline suite neither reads them nor
// fails on a guard about data it does not touch.
//
// The same parsed seed feeds the stored document AND the check() oracle, so a corpus field the
// harness does not consume is dropped from both sides at once and the differential agrees for the
// wrong reason — the projection trap conformance/README.md describes for actions.json, applied to
// the seeds.

export interface Tag {
  id: string;
  name: string | null;
}

export interface Seed {
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

export interface SeedsFile {
  principal: Principal;
  resourceKind: string;
  seeds: Seed[];
}

/** One seed's derived fields, exactly as conformance/derived-fields.json carries them. */
export interface DerivedEntry {
  createdBy: string;
  aDouble: number | null;
  createdAt: string | null;
  scope: string | null;
  labels: (string | null)[];
}

export interface DerivedFile {
  fields: string[];
  derived: Record<string, DerivedEntry>;
}

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
// nothing (conformance/README.md, "Adding a new hostile shape", step 7). The harness passes the
// principal through verbatim, which is correct; the guard is what proves it still does.
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
 * One principal attribute, checked against the two JSON shapes the corpus carries. A key-set guard
 * says nothing about a change inside a value and three of the four attributes are lists, so the
 * element type is asserted for the same reason the seed guard descends into `tags[]`.
 */
function assertPrincipalAttrShape(label: string, value: unknown): void {
  if (typeof value === "string") return;
  if (isStringArray(value)) return;
  throw new Error(
    `${label} is neither a string nor an array of strings, the only two shapes this harness consumes: a reshaped principal attribute feeds the plan and the check() oracle at once`,
  );
}

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

const isDerivedEntry = (value: unknown): value is DerivedEntry =>
  isRecord(value) &&
  typeof value["createdBy"] === "string" &&
  (typeof value["aDouble"] === "number" || value["aDouble"] === null) &&
  (typeof value["createdAt"] === "string" || value["createdAt"] === null) &&
  (typeof value["scope"] === "string" || value["scope"] === null) &&
  Array.isArray(value["labels"]) &&
  value["labels"].every((label) => label === null || typeof label === "string");

/**
 * `seeds.json`, validated and then held to the key sets above.
 *
 * The returned rows are the parsed JSON verbatim, never rebuilt field by field: `Object.keys` has
 * to report the CORPUS key set for the guard to say anything, and a parser that copied each field
 * it knew about could only ever report the keys already named here.
 */
export function parseSeedsFile(value: unknown): SeedsFile {
  if (
    !isRecord(value) ||
    !isPrincipal(value["principal"]) ||
    typeof value["resourceKind"] !== "string" ||
    !Array.isArray(value["seeds"]) ||
    !value["seeds"].every(isSeed)
  ) {
    throw new Error("Invalid conformance seeds");
  }
  const seedsFile = value as unknown as SeedsFile;

  seedsFile.seeds.forEach((seed, index) => {
    const label = `seeds.json seeds[${index}]`;
    assertKeys(label, Object.keys(seed), SEED_KEYS, [SEED_NOTE_KEY]);
    seed.tags.forEach((tag, tagIndex) => {
      assertKeys(`${label}.tags[${tagIndex}]`, Object.keys(tag), TAG_KEYS);
    });
  });

  assertKeys(
    "seeds.json principal",
    Object.keys(seedsFile.principal),
    PRINCIPAL_KEYS,
  );
  // `attr` is optional on the SDK's Principal type; the corpus always carries it, and the
  // assertion above is what proves it rather than this fallback.
  const attr = seedsFile.principal.attr ?? {};
  assertKeys("seeds.json principal.attr", Object.keys(attr), PRINCIPAL_ATTR_KEYS);
  for (const [key, attrValue] of Object.entries(attr)) {
    assertPrincipalAttrShape(`seeds.json principal.attr.${key}`, attrValue);
  }

  return seedsFile;
}

/**
 * `derived-fields.json`, validated and held to `DERIVED_KEYS` — and to the seed roster, so a seed
 * added without its derived entry fails here rather than at whichever action first reads it.
 */
export function parseDerivedFile(value: unknown, seeds: Seed[]): DerivedFile {
  if (
    !isRecord(value) ||
    !isStringArray(value["fields"]) ||
    !isRecord(value["derived"]) ||
    !Object.values(value["derived"]).every(isDerivedEntry)
  ) {
    throw new Error("Invalid conformance derived fields");
  }
  const derivedFile = value as unknown as DerivedFile;

  assertKeys("derived-fields.json fields", derivedFile.fields, DERIVED_KEYS);
  const entries = Object.keys(derivedFile.derived);
  if (entries.length !== seeds.length) {
    throw new Error(
      `derived-fields.json has ${entries.length} entries for ${seeds.length} seeds`,
    );
  }
  for (const seed of seeds) {
    const entry = derivedFile.derived[seed.id];
    if (!entry) {
      throw new Error(`derived-fields.json has no entry for seed ${seed.id}`);
    }
    assertKeys(
      `derived-fields.json derived["${seed.id}"]`,
      Object.keys(entry),
      DERIVED_KEYS,
    );
  }

  return derivedFile;
}

// -- the golden wire fixtures --------------------------------------------------------------------

/**
 * The instant `regenerate-wire-fixtures.sh` substitutes for the one operand it cannot pin.
 *
 * `ts-window` and `ts-vf` compare against `now() - duration("24h")`, which the planner folds to a
 * literal timestamp: a different value on every capture, so the script rewrites it to
 * `__NOW_MINUS_24H__` to keep the drift check deterministic. Reading the fixture back therefore
 * means choosing a value, and the choice is load-bearing rather than arbitrary — Cerbos emits the
 * PDP's clock at nanosecond precision, and this adapter compares timestamps as strings in
 * JavaScript rather than lowering them to a store's own type, so the nine digits it has to carry
 * are the nine digits a real plan carries. A tidy millisecond instant here would quietly stop
 * exercising the precision this adapter's post-filter is written to preserve, and
 * `translator.test.ts` walks both sides of that boundary through the `plannedAt` override on
 * `planFromWireFixture`.
 */
export const PLANNED_AT = "2026-08-11T09:13:39.123456789Z";

interface WireOperand {
  expression?: { operator: string; operands: WireOperand[] };
  variable?: string;
  value?: unknown;
}

interface WireFixture {
  action: string;
  resourceKind: string;
  filter: { kind: string; condition?: WireOperand };
}

function operandFromWire(
  node: WireOperand,
  plannedAt: string,
): PlanExpressionOperand {
  if (node.expression) {
    return new PlanExpression(
      node.expression.operator,
      node.expression.operands.map((child) => operandFromWire(child, plannedAt)),
    );
  }
  if (node.variable !== undefined) {
    return new PlanExpressionVariable(node.variable);
  }
  if (!("value" in node)) {
    throw new Error(
      `Wire fixture operand is neither an expression, a variable nor a value: ${JSON.stringify(node)}`,
    );
  }
  // The one cast in this file. A fixture is JSON the PDP produced, so its leaves are already
  // exactly the JSON shapes `Value` admits — but `JSON.parse` cannot say so, and re-validating a
  // file the corpus workflow regenerates and diffs would assert nothing new.
  return new PlanExpressionValue(
    (node.value === "__NOW_MINUS_24H__" ? plannedAt : node.value) as Value,
  );
}

/** Every action the corpus has a golden wire fixture for, sorted. */
export function wireFixtureActions(): string[] {
  return fs
    .readdirSync(WIRE_FIXTURES_DIR)
    .filter((name) => name.endsWith(".json"))
    .map((name) => name.slice(0, -".json".length))
    .sort();
}

/**
 * The plan the pinned PDP produced for `action`, decoded into the shape the SDK hands callers.
 *
 * The fixture is the PDP's HTTP response, so the decoding here is the one `@cerbos/http` performs
 * — `{expression|variable|value}` nodes into `PlanExpression` / `PlanExpressionVariable` /
 * `PlanExpressionValue`. It is deliberately not a hand-built plan: a plan somebody typed is a
 * belief about what the planner emits, and this repository keeps fixtures precisely because that
 * belief has been wrong before. See docs/adr/0006.
 */
export function planFromWireFixture(
  action: string,
  plannedAt: string = PLANNED_AT,
): PlanResourcesResponse {
  const fixture: WireFixture = JSON.parse(
    fs.readFileSync(path.join(WIRE_FIXTURES_DIR, `${action}.json`), "utf8"),
  );
  const base = {
    cerbosCallId: "",
    requestId: "",
    validationErrors: [],
    metadata: undefined,
  };
  switch (fixture.filter.kind) {
    case PlanKind.CONDITIONAL:
      if (!fixture.filter.condition) {
        throw new Error(
          `Wire fixture ${action} is conditional with no condition`,
        );
      }
      return {
        ...base,
        kind: PlanKind.CONDITIONAL,
        condition: operandFromWire(fixture.filter.condition, plannedAt),
      };
    case PlanKind.ALWAYS_ALLOWED:
    case PlanKind.ALWAYS_DENIED:
      return { ...base, kind: fixture.filter.kind };
    default:
      throw new Error(
        `Wire fixture ${action} has an unrecognised filter kind ${fixture.filter.kind}`,
      );
  }
}

// -- the golden expectations ---------------------------------------------------------------------

/**
 * One node of a recorded Convex filter expression.
 *
 * Unlike every SQL adapter, this one emits a **function** — `(q) => Expression<boolean>` — so
 * there is no query text to pin. What can be pinned is the call sequence that function makes
 * against the `FilterBuilder` it is handed, and that is what this records: `q.eq(q.field("aBool"),
 * true)` becomes `{op: "eq", args: [{op: "field", args: ["aBool"]}, true]}`.
 *
 * The wrapper is unconditional rather than only around builder calls, so a plan literal can never
 * be mistaken for a node: a CEL map literal is a JSON object too, and an unwrapped one carrying an
 * `op` key would read back as a call that never happened.
 */
export interface FilterNode {
  op:
    | "field"
    | "eq"
    | "neq"
    | "lt"
    | "lte"
    | "gt"
    | "gte"
    | "and"
    | "or"
    | "not";
  args: unknown[];
}

/**
 * The translator output this adapter is pinned to produce for one corpus action.
 *
 * `kind` mirrors `QueryPlanToConvexResult`. `ALWAYS_ALLOWED` / `ALWAYS_DENIED` carry no filter and
 * no path, because there is nothing to push down — those are ADR 0006's "expected plan kind"
 * bucket, kept in the same file as the filters so one lookup answers "is this action accounted
 * for?".
 *
 * `path` is which half of the adapter's output answers the query — `db`, `post`, or `split` for
 * both, the filter narrowing before the post-filter decides. It is a translator decision
 * (`canPushToDb` reading the mapper), which is why it is pinned here; the harness asserts the path
 * the BACKEND reports, a different claim — that the half the translator chose is the half that
 * actually ran.
 *
 * A `post` entry carries no `filter`: the adapter hands Convex nothing and answers the whole plan
 * in its own evaluator. What that evaluator DECIDES is not pinned here — it is a function of a
 * document, so the only assertion available is which documents it admits, and that is the
 * differential harness's job against `check()` as the oracle. What is pinned here is the routing:
 * the corpus shapes Convex's filter engine sees, and exactly which calls it is handed.
 */
export type GoldenExpectation =
  | { kind: PlanKind.ALWAYS_ALLOWED | PlanKind.ALWAYS_DENIED }
  | { kind: PlanKind.CONDITIONAL; path: "post" }
  | {
      kind: PlanKind.CONDITIONAL;
      path: "db" | "split";
      filter: FilterNode;
    };

/** The reserved key an entry may carry alongside its expectation; never compared. */
const NOTE_KEY = "note";

export interface GoldenEntry {
  /** Human commentary. Preserved verbatim when the file is regenerated. */
  note?: string;
  expectation: GoldenExpectation;
}

export interface GoldenFile {
  adapter: string;
  regenerate: string;
  expectations: Record<string, GoldenExpectation & { note?: string }>;
}

export const GOLDEN_REGENERATE_COMMAND = "npm run golden:update";

/**
 * The golden expectations, split into the commentary and the value the suite compares.
 *
 * `adapter` is checked rather than ignored: the file is a flat map of action names, so a copy
 * taken from another adapter parses cleanly and would be compared against this adapter's output
 * with only the diff to say something went wrong.
 */
export function readGoldenExpectations(): Map<string, GoldenEntry> {
  const file: GoldenFile = JSON.parse(fs.readFileSync(GOLDEN_FILE, "utf8"));
  if (file.adapter !== ADAPTER) {
    throw new Error(
      `${GOLDEN_FILE} declares adapter "${file.adapter}", not "${ADAPTER}"`,
    );
  }
  return new Map(
    Object.entries(file.expectations).map(([action, entry]) => {
      const { [NOTE_KEY]: note, ...expectation } = entry;
      return [
        action,
        {
          ...(note === undefined ? {} : { note }),
          expectation: expectation as GoldenExpectation,
        },
      ];
    }),
  );
}

/**
 * Rewrite the golden expectations, carrying every existing `note` across.
 *
 * Only ever called under `GOLDEN_UPDATE=1` (`npm run golden:update`). Regeneration is the same
 * deliberate act as `conformance/scripts/regenerate-wire-fixtures.sh`: the safety is the diff a
 * reviewer reads, which is why the entries are written sorted and one action per key.
 *
 * A missing file is not an error here, and only here — that is how a new adapter bootstraps one.
 * Reading a missing file for an assertion stays an error, because a suite that quietly asserts
 * nothing is the failure mode the completeness guard exists to prevent.
 */
export function writeGoldenExpectations(
  expectations: Map<string, GoldenExpectation>,
): void {
  const notes = new Map<string, string>();
  if (fs.existsSync(GOLDEN_FILE)) {
    for (const [action, entry] of readGoldenExpectations()) {
      if (entry.note !== undefined) {
        notes.set(action, entry.note);
      }
    }
  }
  const body: Record<string, GoldenExpectation & { note?: string }> = {};
  for (const action of [...expectations.keys()].sort()) {
    const note = notes.get(action);
    const expectation = expectations.get(action)!;
    body[action] = note === undefined ? expectation : { note, ...expectation };
  }
  const file: GoldenFile = {
    adapter: ADAPTER,
    regenerate: GOLDEN_REGENERATE_COMMAND,
    expectations: body,
  };
  fs.mkdirSync(path.dirname(GOLDEN_FILE), { recursive: true });
  fs.writeFileSync(GOLDEN_FILE, `${JSON.stringify(file, null, 2)}\n`, "utf8");
}
