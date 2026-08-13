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
  adapterUnsupported?: Record<string, AdapterUnsupportedEntry[]>;
  adapterSupportedExpected?: Record<string, AdapterUnsupportedEntry[]>;
  expectedUnsupported: UnsupportedShape[];
  nullRepresentationOmitted: NullRepresentationOmittedEntry[];
  knownDivergences?: KnownDivergence[];
}

/**
 * A shape this adapter must refuse, with the substring its error has to contain.
 *
 * The message is what turns "it threw" into "it threw for the declared reason": without it a
 * mapper typo or an unrelated validation satisfies the assertion just as well as the limitation
 * the corpus documents (cerbos/query-plan-adapters#326).
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
  const unsupported = manifest.adapterUnsupported?.[adapter] ?? [];
  const unsupportedActions = new Set(unsupported.map((entry) => entry.action));
  const supportedExpected = new Set(
    (manifest.adapterSupportedExpected?.[adapter] ?? []).map(
      (entry) => entry.action,
    ),
  );
  const oracleActions = [
    ...manifest.conformance.filter((action) => !unsupportedActions.has(action)),
    ...supportedExpected,
  ];
  const throwingActions: ThrowingAction[] = [
    ...unsupported.map((entry): ThrowingAction => [
      entry.action,
      entry.reason,
      requireMessage(
        `adapterUnsupported.${adapter}.${entry.action}`,
        entry.message,
      ),
    ]),
    ...manifest.expectedUnsupported
      .filter((entry) => !supportedExpected.has(entry.action))
      .map((entry): ThrowingAction => [
        entry.action,
        entry.shape,
        requireMessage(
          `expectedUnsupported.${entry.action}.messages.${adapter}`,
          entry.messages?.[adapter],
        ),
      ]),
  ];

  return {
    oracleActions: [...new Set(oracleActions)].sort(),
    throwingActions: throwingActions.sort(([left], [right]) =>
      left.localeCompare(right),
    ),
    supportedExpected,
  };
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
