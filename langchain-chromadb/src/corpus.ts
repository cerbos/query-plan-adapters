import * as fs from "node:fs";
import * as path from "node:path";

import {
  PlanExpression,
  PlanExpressionValue,
  PlanExpressionVariable,
  // The enum itself, not the adapter's `PlanKind` re-export: `index.ts` splits that into a type
  // alias and a const, so the type side has no member types for the `GoldenExpectation` union below.
  PlanKind,
} from "@cerbos/core";
import type {
  PlanExpressionOperand,
  PlanResourcesResponse,
  Value,
} from "@cerbos/core";
import type { Where } from "chromadb";

import type { FieldNameMapperConfig } from ".";

/**
 * The parts of the shared `../conformance/` corpus that both of this adapter's suites read, plus
 * the reader for the golden expectations this adapter owns.
 *
 * `adversarial.test.ts` plans against a real PDP and queries a real ChromaDB collection;
 * `translator.test.ts` reads the same actions off the golden wire fixtures and asserts nothing but
 * the emitted `Where` filter. They must agree on two things or they prove less than they appear to:
 *
 * - **the field name mapper.** The unit test pins the filter this adapter emits for a mapping; the
 *   harness proves that same filter returns the documents the PDP allows. Two copies that drifted
 *   would leave the pinned filters describing metadata keys no harness ever seeds, which is why
 *   `FIELD_NAME_MAPPER` lives here rather than in either suite.
 * - **the classification.** Which actions this adapter must refuse, and with which message, is a
 *   corpus decision (`actions.json`), not a per-suite one.
 *
 * The code in this file is duplicated across adapters **on purpose** — adapters share data, not
 * code, so that every adapter stays standalone. Do not extract it into `conformance/`, do not
 * import another adapter's copy, and do not add a drift check between them. See
 * [ADR 0007](../../docs/adr/0007-adapters-share-data-not-code.md).
 *
 * Test-only: excluded from `tsc --build` by `tsconfig.json`, so nothing here reaches `lib/`.
 */

export const ADAPTER = "langchain-chromadb";

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

// -- JSON primitives, shared by both suites' parsers ----------------------------------------------
//
// The corpus is read rather than typed: `JSON.parse` returns `any`, and a cast would let a corpus
// file that changed shape reach an assertion as `undefined` instead of failing at load.

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function requireRecord(
  value: unknown,
  label: string,
): Record<string, unknown> {
  if (!isRecord(value)) {
    throw Error(`${label} must be an object`);
  }
  return value;
}

export function requireString(value: unknown, label: string): string {
  if (typeof value !== "string") {
    throw Error(`${label} must be a string`);
  }
  return value;
}

export function requireBoolean(value: unknown, label: string): boolean {
  if (typeof value !== "boolean") {
    throw Error(`${label} must be a boolean`);
  }
  return value;
}

export function requireNumber(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw Error(`${label} must be a finite number`);
  }
  return value;
}

export function requireArray(value: unknown, label: string): unknown[] {
  if (!Array.isArray(value)) {
    throw Error(`${label} must be an array`);
  }
  return value;
}

export function parseStringArray(value: unknown, label: string): string[] {
  return requireArray(value, label).map((item, index) =>
    requireString(item, `${label}[${index}]`),
  );
}

// -- actions.json --------------------------------------------------------------------------------

export interface UnsupportedShape {
  action: string;
  shape: string;
  /** One entry per adapter that must reject the shape; the corpus asserts the key set. */
  messages: Record<string, string>;
}

export interface AdapterUnsupportedEntry {
  action: string;
  reason: string;
  /** Absent on `adapterSupportedExpected`, required on a throw. */
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
  adapterUnsupported: Record<string, AdapterUnsupportedEntry[]>;
  adapterSupportedExpected: Record<string, AdapterUnsupportedEntry[]>;
  expectedUnsupported: UnsupportedShape[];
  nullRepresentationOmitted: NullRepresentationOmittedEntry[];
  knownDivergences: KnownDivergence[];
}

/** The `messages` map of one entry: adapter key -> required substring. */
function parseMessages(value: unknown, label: string): Record<string, string> {
  const messages = requireRecord(value, label);
  const result: Record<string, string> = {};
  for (const [adapter, message] of Object.entries(messages)) {
    result[adapter] = requireString(message, `${label}.${adapter}`);
  }
  return result;
}

function parseUnsupportedShape(value: unknown, index: number): UnsupportedShape {
  const label = `expectedUnsupported[${index}]`;
  const shape = requireRecord(value, label);
  return {
    action: requireString(shape["action"], `${label}.action`),
    shape: requireString(shape["shape"], `${label}.shape`),
    messages: parseMessages(shape["messages"], `${label}.messages`),
  };
}

function parseAdapterUnsupportedEntry(
  value: unknown,
  label: string,
): AdapterUnsupportedEntry {
  const entry = requireRecord(value, label);
  const message = entry["message"];
  return {
    action: requireString(entry["action"], `${label}.action`),
    reason: requireString(entry["reason"], `${label}.reason`),
    // `adapterUnsupported` carries this and the classification below requires it;
    // `adapterSupportedExpected` does not throw, so it does not.
    ...(message === undefined
      ? {}
      : { message: requireString(message, `${label}.message`) }),
  };
}

function parseAdapterMap(
  value: unknown,
  label: string,
): Record<string, AdapterUnsupportedEntry[]> {
  const adapters = requireRecord(value, label);
  const result: Record<string, AdapterUnsupportedEntry[]> = {};
  for (const [adapter, entries] of Object.entries(adapters)) {
    result[adapter] = requireArray(entries, `${label}.${adapter}`).map(
      (entry, index) =>
        parseAdapterUnsupportedEntry(entry, `${label}.${adapter}[${index}]`),
    );
  }
  return result;
}

/**
 * `actions.json`, validated rather than cast.
 *
 * Every group is parsed **explicitly**: this rebuilds the manifest field by field, so a corpus group
 * it does not name is silently dropped — and a dropped group makes its actions vanish from every
 * count and every `test.each`, passing vacuously.
 */
export function parseActionsFile(value: unknown): ActionsFile {
  const file = requireRecord(value, "actions.json");
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
    ).map((entry, index) => {
      const label = `knownDivergences[${index}]`;
      const record = requireRecord(entry, label);
      return {
        action: requireString(record["action"], `${label}.action`),
        adapters: parseStringArray(record["adapters"], `${label}.adapters`),
      };
    }),
  };
}

/**
 * A shape this adapter must refuse, with the substring its error has to contain.
 *
 * The message is what turns "it threw" into "it threw for the declared reason": without it a
 * mapper typo or an unrelated validation satisfies the assertion just as well as the limitation
 * the corpus documents. This adapter is the largest consumer of that distinction — 164 of the
 * corpus's 199 shapes are fail-closed here, so a bare throw assertion would prove almost nothing
 * (cerbos/query-plan-adapters#326).
 */
export type ThrowingAction = readonly [
  action: string,
  reason: string,
  message: string,
];

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
    throw Error(
      `actions.json pins no throw message for ${label}: the throw suite would accept a failure for any reason`,
    );
  }
  return message;
}

/**
 * The corpus's own classification of every action, from this adapter's point of view.
 *
 * `nullRepresentationOmitted` is deliberately NOT folded in: it is its own classification, and the
 * harness's four-way "exactly one outcome per action" guard depends on the groups staying distinct.
 * Read it with `nullRepresentationThrows` below.
 */
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
      (entry): ThrowingAction => [
        entry.action,
        entry.reason,
        requireMessage(
          `adapterUnsupported.${adapter}.${entry.action}`,
          entry.message,
        ),
      ],
    ),
    ...manifest.expectedUnsupported
      .filter((entry) => !supportedExpected.has(entry.action))
      .map(
        (entry): ThrowingAction => [
          entry.action,
          entry.shape,
          requireMessage(
            `expectedUnsupported.${entry.action}.messages.${adapter}`,
            entry.messages?.[adapter],
          ),
        ],
      ),
  ];

  return {
    oracleActions: [...new Set(oracleActions)].sort(),
    throwingActions: throwingActions.sort(([left], [right]) =>
      left.localeCompare(right),
    ),
    supportedExpected,
  };
}

/**
 * The `nullRepresentationOmitted` actions, as throws.
 *
 * Every adapter must reject these — the two NULL conventions are indistinguishable on the wire — but
 * most reject them only under the `omitted` convention. This adapter rejects them unconditionally
 * and needs no `nullAttributeRepresentation` option at all: Chroma metadata holds only finite
 * numbers, strings and booleans, so it cannot store an explicit null distinguishably from an absent
 * key and the null comparison operand is refused before the convention could matter
 * (cerbos/query-plan-adapters#302). The message that says so is corpus data, pinned per adapter.
 */
export function nullRepresentationThrows(
  manifest: ActionsFile,
  adapter: string,
): ThrowingAction[] {
  return manifest.nullRepresentationOmitted.map((entry): ThrowingAction => [
    entry.action,
    entry.reason,
    requireMessage(
      `nullRepresentationOmitted.${entry.action}.messages.${adapter}`,
      entry.messages?.[adapter],
    ),
  ]);
}

// -- the golden wire fixtures --------------------------------------------------------------------

/**
 * The instant `regenerate-wire-fixtures.sh` substitutes for the one operand it cannot pin.
 *
 * `ts-window` and `ts-vf` compare against `now() - duration("24h")`, which the planner folds to a
 * literal timestamp: a different value on every capture, so the script rewrites it to
 * `__NOW_MINUS_24H__` to keep the drift check deterministic. Reading the fixture back therefore
 * means choosing a value. It is inert for this adapter — a timestamp is a string operand against a
 * `hier`/comparison shape Chroma refuses whatever the instant is — but the nanosecond precision the
 * PDP actually emits is kept anyway, so a fixture read here is the fixture the other harnesses read.
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
    throw Error(
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
        throw Error(`Wire fixture ${action} is conditional with no condition`);
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
      throw Error(
        `Wire fixture ${action} has an unrecognised filter kind ${fixture.filter.kind}`,
      );
  }
}

// -- the golden expectations ---------------------------------------------------------------------

/**
 * The translator output this adapter is pinned to produce for one corpus action: the whole
 * `{ kind, filters? }` result, which is already JSON.
 *
 * There is no rendering step and no dialect here — a Chroma `Where` clause IS a JSON document, so
 * the pinned value is the object the caller hands to `collection.query()` verbatim. That is the
 * cheapest possible instance of the golden format and the reason this adapter needed no extension
 * to it: the observable the translator produces round-trips through JSON on its own.
 */
export type GoldenExpectation =
  | { kind: PlanKind.ALWAYS_DENIED }
  | { kind: PlanKind.ALWAYS_ALLOWED; filters: Where }
  | { kind: PlanKind.CONDITIONAL; filters: Where };

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
    throw Error(
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

// -- the metadata mapping, as both suites see it --------------------------------------------------

/**
 * The corpus mapping from Cerbos attribute paths onto Chroma metadata keys.
 *
 * Fields are optional unless declared otherwise, so `$ne`/`$nin` are rejected by default.
 * `required: true` is asserted only for the metadata keys the harness's `metadataFor` writes for
 * every seed in `conformance/seeds.json`. `aOptionalString` is null for a2/a4/a8/c2/e1, so it stays
 * optional and its inequality shapes remain fail-closed.
 *
 * Shared with `adversarial.test.ts` so the filters `translator.test.ts` pins describe the metadata
 * that harness actually seeds and queries; a second mapper written for the unit test could drift
 * and pin keys nothing stores.
 */
export const FIELD_NAME_MAPPER: Record<string, string | FieldNameMapperConfig> =
  {
    // The primary key, reached as `request.resource.id` rather than through `attr` (the `id-*`
    // actions). Chroma's `where` filters metadata only — the document id is addressed by the
    // separate `ids` argument to `get()` — so `metadataFor` mirrors the id into a metadata key and
    // this maps onto that. Leaving it unmapped would make the id-* actions throw for a HARNESS
    // reason (no mapping) rather than an adapter one, which is the trap #326 documents.
    "request.resource.id": { field: "id", required: true },
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
    // The corpus's one REAL to-one chain (the `rel-*` actions), flattened onto dotted metadata keys
    // by `metadataFor`. EVERY level stays `required: false` — the whole point of the relation is
    // that a level can be absent, and 8 of the 21 seeds have no parent at all — so Chroma's
    // inequality shapes over these keys stay fail-closed. A metadata key Chroma cannot prove is
    // present cannot answer `$ne` the way CEL's missing-attribute error does
    // (cerbos/query-plan-adapters#375).
    "request.resource.attr.parent.aBool": { field: "parent.aBool" },
    "request.resource.attr.parent.aString": { field: "parent.aString" },
    "request.resource.attr.parent.aNumber": {
      field: "parent.aNumber",
      numericType: "integer",
    },
    "request.resource.attr.parent.aOptionalString": {
      field: "parent.aOptionalString",
    },
    "request.resource.attr.parent.inner.aBool": { field: "parent.inner.aBool" },
    "request.resource.attr.parent.inner.aString": {
      field: "parent.inner.aString",
    },
    "request.resource.attr.parent.inner.aNumber": {
      field: "parent.inner.aNumber",
      numericType: "integer",
    },
    "request.resource.attr.parent.inner.aOptionalString": {
      field: "parent.inner.aOptionalString",
    },
  };

/** Every metadata key the corpus mapping can produce, for the "no undeclared key" rule. */
export function mappedMetadataKeys(): string[] {
  return Object.values(FIELD_NAME_MAPPER)
    .map((entry) => (typeof entry === "string" ? entry : entry.field))
    .sort();
}

/**
 * The subset of those keys the mapping asserts is present on every document — the only keys `$ne`
 * and `$nin` are sound over, since Chroma's inequalities MATCH a document missing the key.
 *
 * A bare string carries no presence assertion, so it is optional here exactly as it is in the
 * adapter: the default lives in one place and both readings of it agree.
 */
export function requiredMetadataKeys(): string[] {
  return Object.values(FIELD_NAME_MAPPER)
    .filter(
      (entry): entry is FieldNameMapperConfig =>
        typeof entry !== "string" && entry.required === true,
    )
    .map((entry) => entry.field)
    .sort();
}
