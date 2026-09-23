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

import { PlanKind, MapperConfig } from ".";

/**
 * The parts of the shared `../conformance/` corpus that both of this adapter's suites read.
 *
 * `adversarial.test.ts` plans against a real PDP and executes the translated query;
 * `translator.test.ts` reads the same actions off the golden wire fixtures and asserts nothing
 * but the emitted filter. They must agree on two things or they prove less than they appear to:
 *
 * - **the mapper.** The unit test pins the filter this adapter emits for a mapping; the harness
 *   proves that same filter returns the rows the PDP allows. Two copies that drifted would leave
 *   the pinned filters describing a mapping no harness ever executes.
 * - **the classification.** Which actions this adapter must refuse, and with which message, is a
 *   corpus decision (`actions.json`), not a per-suite one.
 *
 * Test-only: excluded from `tsc --build` by `tsconfig.json`, so nothing here reaches `lib/`.
 */

export const CONFORMANCE_DIR = path.join(
  __dirname,
  "..",
  "..",
  "conformance"
);

// -- the PDP -------------------------------------------------------------------------------------

/**
 * The gRPC address of the PDP `scripts/run-adversarial.sh` started for THIS run: a Unix socket in a
 * directory that run created, exported by `cerbos run` as CERBOS_GRPC. There is deliberately no
 * default and no TCP form. A fixed port is how a suite ends up planning against another run's PDP
 * (cerbos/query-plan-adapters#476): `cerbos run` does not fail on a port that is already bound, and
 * whichever PDP answers wins — possibly on another corpus revision or evaluation mode.
 */
export function pdpAddress(): string {
  const address = process.env["CERBOS_GRPC"];
  if (address === undefined || !address.startsWith("unix:")) {
    throw new Error(
      `CERBOS_GRPC is ${JSON.stringify(address)}, expected the unix: socket scripts/run-adversarial.sh ` +
        "starts the PDP on. Run this suite through `npm run test:adversarial`, not jest directly.",
    );
  }
  return address;
}

/**
 * Fails the run unless the PDP reports the version pinned in conformance/CERBOS_VERSION. The wire
 * fixtures and every classification are recorded against that version, and locally `cerbos run`
 * is whatever `cerbos` binary is on PATH, so a stale one would otherwise pass or fail the corpus
 * for reasons the corpus does not describe.
 */
export async function assertPinnedPdp(pdp: {
  serverInfo(): Promise<{ version: string }>;
}): Promise<void> {
  const pinned = fs
    .readFileSync(path.join(CONFORMANCE_DIR, "CERBOS_VERSION"), "utf8")
    .trim();
  const { version } = await pdp.serverInfo();
  if (version !== pinned) {
    throw new Error(
      `The PDP at ${pdpAddress()} reports version ${version}, but conformance/CERBOS_VERSION pins ${pinned}.`,
    );
  }
}

const WIRE_FIXTURES_DIR = path.join(CONFORMANCE_DIR, "wire-fixtures");

/** The Prisma model the corpus's resource kind maps onto. */
export const MODEL = "AdversarialResource";

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
  degenerateOracles: DegenerateOracleEntry[];
}

/** How a `degenerateOracles` entry's check() oracle is degenerate: no seed allowed, or every seed. */
export type DegenerateOracle = "empty" | "total";

/**
 * A `degenerateOracles` entry: an action whose check() oracle is empty or total BY CONSTRUCTION.
 * The differential cannot fail for such an action — an adapter that returned nothing, or
 * everything, would agree with it — so the corpus lists them once, for every harness, and every
 * other oracle-compared action must have a non-empty, non-total oracle.
 */
export interface DegenerateOracleEntry {
  action: string;
  oracle: DegenerateOracle;
  reason: string;
}

/** `degenerateOracles` as action -> "empty" | "total", validated. */
export function degenerateOraclesOf(
  manifest: ActionsFile
): Map<string, DegenerateOracle> {
  if (!Array.isArray(manifest.degenerateOracles)) {
    throw new Error("actions.json carries no degenerateOracles array");
  }
  const oracles = new Map<string, DegenerateOracle>();
  for (const entry of manifest.degenerateOracles) {
    if (entry.oracle !== "empty" && entry.oracle !== "total") {
      throw new Error(
        `actions.json degenerateOracles.${entry.action} declares oracle ${JSON.stringify(entry.oracle)}, expected "empty" or "total"`
      );
    }
    if (oracles.has(entry.action)) {
      throw new Error(
        `actions.json degenerateOracles lists ${entry.action} more than once`
      );
    }
    oracles.set(entry.action, entry.oracle);
  }
  return oracles;
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
  message: string | undefined
): string {
  if (message === undefined || message === "") {
    throw new Error(
      `actions.json pins no throw message for ${label}: the throw suite would accept a failure for any reason`
    );
  }
  return message;
}

export function classifyActionsForAdapter(
  manifest: ActionsFile,
  adapter: string
): ActionClassification {
  const unsupported = manifest.adapterUnsupported?.[adapter] ?? [];
  const unsupportedActions = new Set(unsupported.map((entry) => entry.action));
  const supportedExpected = new Set(
    (manifest.adapterSupportedExpected?.[adapter] ?? []).map(
      (entry) => entry.action
    )
  );
  const oracleActions = [
    ...manifest.conformance.filter(
      (action) => !unsupportedActions.has(action)
    ),
    ...supportedExpected,
  ];
  const throwingActions: ThrowingAction[] = [
    ...unsupported.map(
      (entry): ThrowingAction => [
        entry.action,
        entry.reason,
        requireMessage(`adapterUnsupported.${adapter}.${entry.action}`, entry.message),
      ]
    ),
    ...manifest.expectedUnsupported
      .filter((entry) => !supportedExpected.has(entry.action))
      .map((entry): ThrowingAction => [
        entry.action,
        entry.shape,
        requireMessage(
          `expectedUnsupported.${entry.action}.messages.${adapter}`,
          entry.messages?.[adapter]
        ),
      ]),
  ];

  return {
    oracleActions: [...new Set(oracleActions)].sort(),
    throwingActions: throwingActions.sort(([left], [right]) =>
      left.localeCompare(right)
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
 * PDP's clock at nanosecond precision, which is exactly why both actions are `adapterUnsupported`
 * for this adapter (`Timestamp value exceeds millisecond precision`). A tidy millisecond instant
 * here would translate cleanly and quietly contradict `actions.json`, so the fraction carries the
 * nine digits a real plan carries. `translator.test.ts` pins both sides of that boundary, which is
 * what the `plannedAt` override on `planFromWireFixture` is for.
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
  plannedAt: string
): PlanExpressionOperand {
  if (node.expression) {
    return new PlanExpression(
      node.expression.operator,
      node.expression.operands.map((child) => operandFromWire(child, plannedAt))
    );
  }
  if (node.variable !== undefined) {
    return new PlanExpressionVariable(node.variable);
  }
  if (!("value" in node)) {
    throw new Error(
      `Wire fixture operand is neither an expression, a variable nor a value: ${JSON.stringify(node)}`
    );
  }
  // The one cast in this file. A fixture is JSON the PDP produced, so its leaves are already
  // exactly the JSON shapes `Value` admits — but `JSON.parse` cannot say so, and re-validating a
  // file the corpus workflow regenerates and diffs would assert nothing new.
  return new PlanExpressionValue(
    (node.value === "__NOW_MINUS_24H__" ? plannedAt : node.value) as Value
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
  plannedAt: string = PLANNED_AT
): PlanResourcesResponse {
  const fixture: WireFixture = JSON.parse(
    fs.readFileSync(path.join(WIRE_FIXTURES_DIR, `${action}.json`), "utf8")
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
        throw new Error(`Wire fixture ${action} is conditional with no condition`);
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
        `Wire fixture ${action} has an unrecognised filter kind ${fixture.filter.kind}`
      );
  }
}

// -- the mapper ----------------------------------------------------------------------------------

export const MAPPER: Record<string, MapperConfig> = {
  // The primary key, reached as `request.resource.id` rather than through `attr` (the `id-*`
  // actions). It is a mapping like any other here, which is the point: an adapter that resolves
  // references by stripping a `request.resource.attr.` prefix never sees this name.
  "request.resource.id": { field: "id", valueType: "string" },
  "request.resource.attr.aBool": { field: "aBool", valueType: "boolean" },
  "request.resource.attr.aString": { field: "aString", valueType: "string" },
  "request.resource.attr.aNumber": { field: "aNumber", valueType: "number" },
  "request.resource.attr.aDouble": { field: "aDouble", valueType: "number" },
  "request.resource.attr.aOptionalString": { field: "aOptionalString", valueType: "string", nullable: true },
  "request.resource.attr.createdBy": { field: "createdBy", valueType: "string" },
  "request.resource.attr.scope": { field: "scope", valueType: "string", nullable: true },
  "request.resource.attr.createdAt": {
    field: "createdAt",
    valueType: "dateTime",
  },
  "request.resource.attr.updatedAt": {
    field: "updatedAt",
    valueType: "dateTime",
  },
  // `owner` and `coOwner` alias columns that `aOptionalString` and `scope` also map, under the
  // OTHER null convention: the oracle sends a real null attribute for them rather than omitting
  // it. Declaring that here is what makes the equality family definite for these two attributes
  // and leaves it untouched for every other mapping (cerbos/query-plan-adapters#308).
  "request.resource.attr.owner": {
    field: "aOptionalString",
    nullable: true,
    nullAttributeRepresentation: "explicit",
  },
  "request.resource.attr.coOwner": {
    field: "scope",
    nullable: true,
    nullAttributeRepresentation: "explicit",
  },
  // obj.inner is not a real nested column — mirrors aString, same trick the spring-data
  // reference harness uses for the p-struct probe. `parent.inner` below is the opposite: a real
  // two-level join. The two are kept side by side on purpose.
  "request.resource.attr.obj.inner": { field: "aString", valueType: "string" },
  // The corpus's one REAL to-one chain (the `rel-*` actions). `type: "one"` is what makes the
  // adapter emit `is:` rather than `some:`, and `is:` on an optional relation is what requires
  // the hop to exist — the absent-parent guard the negated shapes discriminate. `inner` nests
  // the same declaration one level further out; two levels is where alias scoping breaks.
  "request.resource.attr.parent": {
    relation: {
      name: "parent",
      type: "one",
      model: "AdversarialParent",
      fields: {
        aBool: { field: "aBool", valueType: "boolean" },
        aString: { field: "aString", valueType: "string" },
        aNumber: { field: "aNumber", valueType: "number" },
        aOptionalString: { field: "aOptionalString", valueType: "string", nullable: true  },
        inner: {
          relation: {
            name: "inner",
            type: "one",
            model: "AdversarialInner",
            fields: {
              aBool: { field: "aBool", valueType: "boolean" },
              aString: { field: "aString", valueType: "string" },
              aNumber: { field: "aNumber", valueType: "number" },
              aOptionalString: { field: "aOptionalString", valueType: "string", nullable: true  },
            },
          },
        },
      },
    },
  },
  "request.resource.attr.tags": {
    relation: {
      name: "tags",
      type: "many",
      // Model name enables field-to-field comparisons between tag columns. `name` is NULLable
      // in the schema, so it keeps the adapter's three-valued-logic guards for collection
      // macros over elements whose name column is NULL (a missing attribute — a CEL error,
      // hence deny — on the check side). Those guards are the default; every REQUIRED element
      // column below says `nullable: false`, because Prisma rejects `{ name: null }` on one.
      model: "AdversarialTag",
      fields: {
        id: { field: "tagId", nullable: false },
        name: { field: "name", valueType: "string", nullable: true  },
      },
    },
  },
  "request.resource.attr.tagNames": {
    relation: {
      name: "tags",
      type: "many",
      field: "name",
      fields: { name: { field: "name", valueType: "string", nullable: true  } },
    },
  },
  // The two homogeneous scalar lists, one owned row per element, projected the way `tagNames`
  // is. `valueType` sits on the list mapping itself because that is where a membership test reads
  // it: a literal of another type (`"2" in aNumberList`) is refused rather than bound, since CEL's
  // heterogeneous equality answers it false and a store that coerces would not. The element column
  // is nullable because a6 and a4 hold a null element, which is a VALUE in CEL.
  "request.resource.attr.aNumberList": {
    valueType: "number",
    relation: {
      name: "numberList",
      type: "many",
      field: "value",
      fields: { value: { field: "value", valueType: "number", nullable: true } },
    },
  },
  "request.resource.attr.aBoolList": {
    valueType: "boolean",
    relation: {
      name: "boolList",
      type: "many",
      field: "value",
      fields: { value: { field: "value", valueType: "boolean", nullable: true } },
    },
  },
  "request.resource.attr.categories": {
    relation: {
      name: "categories",
      type: "many",
      fields: {
        name: { field: "name", valueType: "string", nullable: false },
        subCategories: {
          relation: {
            name: "subCategories",
            type: "many",
            fields: {
              name: { field: "name", valueType: "string", nullable: false },
              labels: {
                relation: {
                  name: "labels",
                  type: "many",
                  fields: {
                    name: { field: "name", valueType: "string", nullable: true  },
                  },
                },
              },
            },
          },
        },
      },
    },
  },
  // Multi-hop chain probe (W1): mainCategory mirrors the SAME categories/subCategories relation
  // as a single-object dotted chain on the check side (every seed holds at most one category),
  // pinning that the adapter joins through every intermediate hop, never off the root.
  "request.resource.attr.mainCategory": {
    relation: {
      name: "categories",
      type: "many",
      fields: {
        name: { field: "name", valueType: "string", nullable: false },
        subCategories: {
          relation: {
            name: "subCategories",
            type: "many",
            fields: { name: { field: "name", valueType: "string", nullable: false } },
          },
        },
        // subNames: the same 2-hop chain but with a bare `field`, so plain `in` membership
        // compares the flattened tail's name column directly.
        subNames: {
          relation: {
            name: "subCategories",
            type: "many",
            field: "name",
          },
        },
      },
    },
  },
};
