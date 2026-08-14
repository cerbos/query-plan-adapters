import * as fs from "fs";
import * as path from "path";

import { describe, expect, test } from "@jest/globals";
import type { PlanExpressionOperand, PlanResourcesResponse } from "@cerbos/core";

// The mapper the adversarial harness and the Convex backend both read, so the filters pinned here
// describe a mapping that is actually executed against seeded documents somewhere.
import {
  MAPPER,
  PUSHDOWN_DEMOTED_FIELDS,
  PUSHDOWN_MAPPER,
} from "../convex/adversarialMapper";
import { executionPathOf } from "../convex/planExecution";
import { PlanKind, queryPlanToConvex } from ".";
import type {
  Mapper,
  MapperConfig,
  NullAttributeRepresentation,
  QueryPlanToConvexResult,
} from ".";
import {
  ADAPTER,
  GOLDEN_REGENERATE_COMMAND,
  classifyActionsForAdapter,
  nullRepresentationOmittedFor,
  parseActionsFile,
  planFromWireFixture,
  readCorpusJson,
  readGoldenExpectations,
  requireMessage,
  wireFixtureActions,
  writeGoldenExpectations,
} from "./corpus";
import type { FilterNode, GoldenExpectation } from "./corpus";

/**
 * Translator unit test: for every action in the shared `../conformance/` corpus, the filter this
 * adapter emits. Offline — no Cerbos sidecar, no Convex backend, no Docker.
 *
 * A per-adapter suite used to braid four assertions into every test. Three of them are somebody
 * else's job now, and this file makes only the fourth:
 *
 * | assertion | who owns it |
 * | --- | --- |
 * | the plan the PDP produces for a policy | `conformance/wire-fixtures/`, replanned and diffed by the `Conformance Corpus` workflow |
 * | which shapes this adapter must refuse, and with what message | `conformance/actions.json` — read below, not restated |
 * | the documents a filter returns | `adversarial.test.ts`, inside a real Convex backend with `check()` as the oracle |
 * | **the filter this adapter emits for a plan** | **here** |
 *
 * **The plans are read, not written.** A hand-built plan is a *belief* about what the planner
 * emits, and this repository keeps golden fixtures because that belief has been wrong before: a
 * planner change used to fail fixture regeneration and silently leave every adapter's hand-written
 * plans describing a wire contract that no longer existed. See
 * [ADR 0006](../../docs/adr/0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md).
 *
 * **The expectations are data, not literals.** The filter this adapter is pinned to emit lives in
 * `golden/expectations.json`, a **golden expectation** file this adapter owns — never under
 * `conformance/`, where every adapter workflow triggers and one adapter re-pinning one filter would
 * re-run all the others. The file is regenerated with `npm run golden:update` and reviewed as a
 * diff, exactly like the wire fixtures it is asserted against. See
 * [ADR 0007](../../docs/adr/0007-adapters-share-data-not-code.md) and the "Golden expectations"
 * section of `conformance/README.md`.
 *
 * **What this adapter emits, and why the asset looks different from a SQL adapter's.** Every other
 * TypeScript adapter emits data — a filter object, or SQL text and its parameters. This one emits
 * a **function**, `(q) => Expression<boolean>`, plus an in-memory `postFilter` for everything
 * Convex's query engine has no operator for. There is no query text to pin, so what is recorded
 * is the call sequence that function makes against the `FilterBuilder` it is handed, together with
 * the **routing decision** — which half of the output answers the query. Convex's engine has no
 * string, collection, arithmetic or cast operators, so most of the corpus is decided by the
 * post-filter, and where the boundary falls is the single most consequential thing this translator
 * decides: an action that silently crossed it would still return the right documents (both halves
 * are supposed to) while changing what the database is asked to do.
 *
 * **What is deliberately NOT pinned here.** What the post-filter *decides*. It is a function of a
 * document, so the only assertion available is which documents it admits — and that is a row
 * comparison, which belongs in `adversarial.test.ts` where the PDP is the oracle. Pinning admitted
 * ids here would be a second, weaker copy of that suite with hand-written expectations in place of
 * an oracle.
 *
 * **Adding a corpus action fails this file.** Every wire fixture must be accounted for here
 * exactly once — a golden expectation (a recorded filter, a routing decision, or an unconditional
 * plan kind) or a throw carrying the message `actions.json` pins — and the completeness guard
 * below is what makes a new action land as a failure rather than as silence.
 */

const actionsFile = parseActionsFile(readCorpusJson("actions.json"));

/**
 * The shapes `actions.json` says this adapter must refuse, each with the message it must refuse
 * them with. Identical to the classification `adversarial.test.ts` asserts against a live PDP;
 * asserting it here as well is what lets the completeness guard below be total, and it costs a
 * millisecond rather than a container.
 *
 * A throwing action needs no golden expectation of its own: the message is already corpus data,
 * pinned once in `actions.json` and read by every adapter. Writing it into this adapter's asset
 * too would create two places to change one string with nothing to say which is authoritative.
 */
const { throwingActions: THROWING_ACTIONS } = classifyActionsForAdapter(
  actionsFile,
  ADAPTER,
);
const THROWING = new Set(THROWING_ACTIONS.map(({ action }) => action));

// -- recording what the adapter asks Convex to do -------------------------------------------------

/**
 * A `FilterBuilder` that records rather than evaluates.
 *
 * The adapter's filter is a function of the builder Convex hands it, so handing it a recorder is
 * the only way to see what it emits without a Convex deployment. Every call becomes a node; the
 * result is JSON, which is what makes it a golden asset a reviewer reads as a diff.
 *
 * Membership is tracked in a `WeakSet` rather than sniffed from the object's shape, so a plan
 * literal can never be mistaken for a recorded call — CEL map literals are JSON objects too.
 */
const RECORDED_NODES = new WeakSet<object>();

const record = (op: FilterNode["op"], args: unknown[]): FilterNode => {
  const node: FilterNode = { op, args };
  RECORDED_NODES.add(node);
  return node;
};

const isRecordedNode = (value: unknown): value is FilterNode =>
  typeof value === "object" && value !== null && RECORDED_NODES.has(value);

const RECORDER = {
  field: (name: string): unknown => record("field", [name]),
  eq: (a: unknown, b: unknown): unknown => record("eq", [a, b]),
  neq: (a: unknown, b: unknown): unknown => record("neq", [a, b]),
  lt: (a: unknown, b: unknown): unknown => record("lt", [a, b]),
  lte: (a: unknown, b: unknown): unknown => record("lte", [a, b]),
  gt: (a: unknown, b: unknown): unknown => record("gt", [a, b]),
  gte: (a: unknown, b: unknown): unknown => record("gte", [a, b]),
  and: (...args: unknown[]): unknown => record("and", args),
  or: (...args: unknown[]): unknown => record("or", args),
  not: (a: unknown): unknown => record("not", [a]),
};

type Recorder = typeof RECORDER;

/**
 * A literal the adapter binds into a filter has to survive a JSON round trip, or the golden file
 * records something other than what Convex is handed — and unlike every SQL adapter, that is not
 * merely an asset-fidelity concern here: a query plan crosses into a Convex function as `v.any()`,
 * which is JSON, so a value this rejects is a value the deployed adapter could not carry either.
 *
 * `-0` and the non-finite doubles are exactly why `cr-div-neg-zero` and `nan-ord-inf` are
 * `adapterUnsupported` for this adapter. Stating the boundary as a rule over every recorded
 * literal, rather than as those two action names, means a THIRD shape that reached it fails here
 * instead of arriving as an unexplained diff.
 */
function assertRecordable(label: string, value: unknown, at: string): void {
  if (isRecordedNode(value)) {
    value.args.forEach((arg, index) =>
      assertRecordable(label, arg, `${at}.${value.op}[${index}]`),
    );
    return;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value) || Object.is(value, -0)) {
      throw new Error(
        `${label} binds ${Object.is(value, -0) ? "-0" : String(value)} at ${at}: a query plan reaches a Convex function as JSON, which carries neither`,
      );
    }
    return;
  }
  if (value === null || ["string", "boolean"].includes(typeof value)) return;
  if (Array.isArray(value)) {
    value.forEach((element, index) =>
      assertRecordable(label, element, `${at}[${index}]`),
    );
    return;
  }
  if (typeof value === "object") {
    for (const [key, nested] of Object.entries(value)) {
      if (key === "op") {
        throw new Error(
          `${label} binds a literal carrying an "op" key at ${at}: it would read back out of the golden file as a builder call that never happened`,
        );
      }
      assertRecordable(label, nested, `${at}.${key}`);
    }
    return;
  }
  throw new Error(
    `${label} binds a ${typeof value} at ${at}, which the golden file cannot record faithfully`,
  );
}

// -- translating one corpus action ----------------------------------------------------------------

interface TranslateOptions {
  mapper?: Mapper;
  /**
   * Defaults to `true`. The corpus is mostly post-filtered, so a golden run without the opt-in
   * would throw before recording anything; the option's own gate is asserted as a rule below.
   */
  allowPostFilter?: boolean;
  nullAttributeRepresentation?: NullAttributeRepresentation;
  plannedAt?: string;
}

function translate(
  action: string,
  options: TranslateOptions = {},
): QueryPlanToConvexResult<Recorder, unknown> {
  return queryPlanToConvex<Recorder, unknown>({
    queryPlan: planFromWireFixture(action, options.plannedAt),
    mapper: options.mapper ?? MAPPER,
    allowPostFilter: options.allowPostFilter ?? true,
    ...(options.nullAttributeRepresentation
      ? { nullAttributeRepresentation: options.nullAttributeRepresentation }
      : {}),
  });
}

/** The calls the emitted filter makes against Convex's builder, validated on the way out. */
function recordFilter(
  label: string,
  filter: (q: Recorder) => unknown,
): FilterNode {
  const emitted = filter(RECORDER);
  if (!isRecordedNode(emitted)) {
    throw new Error(
      `${label} returned something other than a builder call: ${JSON.stringify(emitted)}`,
    );
  }
  assertRecordable(label, emitted, "filter");
  return emitted;
}

/** The whole translator output for one corpus action, in the shape the golden file records. */
function expectationFor(
  action: string,
  options: TranslateOptions = {},
): GoldenExpectation {
  const result = translate(action, options);
  if (result.kind !== PlanKind.CONDITIONAL) return { kind: result.kind };

  const path = executionPathOf(result);
  if (path === "unconditional") {
    throw new Error(`${action} is conditional but reports no execution path`);
  }
  if (path === "post") return { kind: PlanKind.CONDITIONAL, path };

  // Not a formality: `db` and `split` are DEFINED by a filter being present, so an output that
  // reports one without carrying it would silently record `{path: "db"}` with nothing under it.
  const { filter } = result;
  if (!filter) {
    throw new Error(`${action} reports path "${path}" but emitted no filter`);
  }
  return {
    kind: PlanKind.CONDITIONAL,
    path,
    filter: recordFilter(action, filter),
  };
}

/** Where the translator routes an action, for the rules that only care about the split. */
function pathFor(action: string, options: TranslateOptions = {}): string {
  const expectation = expectationFor(action, options);
  return expectation.kind === PlanKind.CONDITIONAL
    ? expectation.path
    : "unconditional";
}

// -- the golden expectations -----------------------------------------------------------------------
//
// `npm run golden:update` rewrites the file from what the translator emits today and preserves
// every `note`. That is the same deliberate act as regenerating the wire fixtures, and the safety
// is identical: the diff is what a reviewer reads. CI never sets the variable, so a translator
// change that moves the emitted filter fails there whatever anyone ran locally.

if (process.env["GOLDEN_UPDATE"] === "1") {
  const regenerated = new Map<string, GoldenExpectation>();
  for (const action of wireFixtureActions()) {
    // A throwing action gets no entry: its message is corpus data. Skipping it here is also what
    // keeps regeneration from papering over a misclassification — an action moved into
    // `adapterUnsupported` that this adapter still translates fails the throw suite, and one moved
    // out of it that this adapter still refuses fails regeneration itself.
    if (THROWING.has(action)) {
      continue;
    }
    regenerated.set(action, expectationFor(action));
  }
  writeGoldenExpectations(regenerated);
}

const RECORDED = readGoldenExpectations();

const RECORDED_ACTIONS = [...RECORDED.keys()];

const byPath = (want: string): string[] =>
  RECORDED_ACTIONS.filter((action) => {
    const expectation = RECORDED.get(action)!.expectation;
    return expectation.kind === PlanKind.CONDITIONAL && expectation.path === want;
  });

/** The actions Convex's own filter engine sees at all — `db` in full, `split` in part. */
const PUSHED_ACTIONS = [...byPath("db"), ...byPath("split")].sort();
const POST_ACTIONS = byPath("post");
const UNCONDITIONAL_ACTIONS = RECORDED_ACTIONS.filter(
  (action) => RECORDED.get(action)!.expectation.kind !== PlanKind.CONDITIONAL,
);

describe("corpus shapes", () => {
  test.each(RECORDED_ACTIONS)("%s emits the golden expectation", (action) => {
    expect(expectationFor(action)).toEqual(RECORDED.get(action)!.expectation);
  });

  // The message, not just the throw: a mapper typo or an unrelated validation satisfies a bare
  // `toThrow()` just as well as the limitation the corpus documents (#326). The harness makes the
  // same assertion against a live PDP; here it costs a millisecond and covers the whole roster,
  // which is what lets the completeness guard below be total.
  test.each(THROWING_ACTIONS)(
    "$action is refused with the message actions.json pins ($reason)",
    ({ action, message }) => {
      expect(() => translate(action)).toThrow(message);
    },
  );

  // Adding a throwing action without pinning its message must fail this suite rather than
  // silently degrade the throw assertions to a bare "it threw" (#326).
  test("a throwing action with no pinned message fails classification", () => {
    expect(() => requireMessage("synthetic-entry", undefined)).toThrow(
      /pins no throw message/,
    );
    expect(() => requireMessage("synthetic-entry", "")).toThrow(
      /pins no throw message/,
    );
  });

  test("every corpus action is accounted for here exactly once", () => {
    const throwing = THROWING_ACTIONS.map(({ action }) => action);
    const classified = [...RECORDED_ACTIONS, ...throwing].sort();

    // Total: a corpus action with no golden expectation and no pinned throw lands as a failure
    // rather than as silence. This is the assertion that makes the asset self-maintaining —
    // adding a hostile shape to the corpus forces someone to look at the filter this adapter
    // emits for it, and `npm run golden:update` refuses to invent one for a shape that throws.
    expect(classified).toEqual(wireFixtureActions());
    // Disjoint: an action carrying a golden expectation AND declared unsupported would satisfy
    // the union above while asserting two contradictory things.
    expect(classified).toEqual([...new Set(classified)].sort());
    // The asset is written sorted, so a translator change reads as the list of shapes it moved.
    expect(RECORDED_ACTIONS).toEqual([...RECORDED_ACTIONS].sort());

    // Tripwires. Bump them deliberately: a count that moves without anyone noticing is how a
    // shape gets dropped from an asset nobody reads end to end. `pushed` moving is the one worth
    // arguing about — it is the size of the corpus Convex's query engine decides rather than the
    // adapter's own evaluator, quoted in the README.
    expect({
      pushed: PUSHED_ACTIONS.length,
      post: POST_ACTIONS.length,
      unconditional: UNCONDITIONAL_ACTIONS.length,
      throwing: throwing.length,
    }).toEqual({ pushed: 23, post: 168, unconditional: 2, throwing: 6 });
  });
});

// -- rules over every pushed-down filter -----------------------------------------------------------
//
// Rules rather than pinned bytes on purpose: a rule holds for a corpus action nobody has added
// yet, and each of these polices a property the pinned nodes are individually consistent with but
// could drift on collectively.

describe("what the adapter asks Convex to do", () => {
  const fieldsNamedBy = (node: FilterNode): string[] =>
    node.op === "field"
      ? [String(node.args[0])]
      : node.args.flatMap((arg) =>
          isRecordedNode(arg) ? fieldsNamedBy(arg) : [],
        );

  const pushedFilter = (action: string, options: TranslateOptions = {}) => {
    const result = translate(action, options);
    if (result.kind !== PlanKind.CONDITIONAL || !result.filter) {
      throw new Error(`${action} pushes nothing down`);
    }
    return recordFilter(action, result.filter);
  };

  const declaredFields = new Set(
    Object.values(MAPPER)
      .map((config: MapperConfig) => config.field)
      .filter((field): field is string => field !== undefined),
  );

  const nullableFields = new Set(
    Object.values(MAPPER)
      .filter((config: MapperConfig) => config.nullable === true)
      .map((config) => config.field)
      .filter((field): field is string => field !== undefined),
  );

  test.each(PUSHED_ACTIONS)(
    "%s names only fields the mapper declares",
    (action) => {
      // The corpus mapper declares every reference the corpus uses, and an unresolved reference
      // does NOT fail here — it falls through to the plan path verbatim (see "mapper forms"). So
      // a `request.resource.attr.…` name in a pushed-down filter means resolution silently missed
      // a reference the caller DID map, which is a filter that matches nothing rather than one
      // that answers the policy.
      const undeclared = fieldsNamedBy(pushedFilter(action)).filter(
        (field) => !declaredFields.has(field),
      );
      expect({ action, undeclared }).toEqual({ action, undeclared: [] });
    },
  );

  test.each(PUSHED_ACTIONS)("%s pushes down no nullable field", (action) => {
    // `nullable: true` means "this path may be absent", which is CEL's missing-attribute case.
    // Convex's engine cannot tell an absent path from a false one, so a comparison over such a
    // field has to stay with the adapter's own evaluator — this is `canPushToDb`'s core
    // invariant, and violating it readmits rows the PDP denies under negation (#375).
    const pushed = fieldsNamedBy(pushedFilter(action)).filter((field) =>
      nullableFields.has(field),
    );
    expect({ action, pushed }).toEqual({ action, pushed: [] });
  });

  /**
   * The pushdown mapper's whole purpose, asserted as the set of actions it moves.
   *
   * `PUSHDOWN_MAPPER` clears `nullable` on a field the seeded documents always carry, which hands
   * the null-comparison family to Convex's engine — the one place Convex's own `q.eq(field, null)`
   * semantics get a say. The rule above is satisfied by a mapper that pushes nothing at all, so
   * this is also its anti-vacuity guard: `nullable` is read in exactly one place in the adapter,
   * and if it stopped being read these two mappers would agree everywhere.
   */
  test("clearing nullable on an always-present field moves exactly these actions", () => {
    const moved = RECORDED_ACTIONS.filter(
      (action) =>
        pathFor(action) !== pathFor(action, { mapper: PUSHDOWN_MAPPER }),
    );

    expect(moved).toEqual([
      "in-null-elem-mixed",
      "in-null-elem-neg",
      "in-null-elem-only",
      "in-null-elem-only-neg",
      "null-eq",
      "null-ne",
      "null-not-eq",
      "null-value-ne-const",
      "null-value-not-eq-const",
      "null-value-not-in-const",
      "vf-null-ne",
    ]);
    // Every one of them moves INTO the engine, never out of it: demoting a field can only widen
    // what `canPushToDb` accepts, so a move in the other direction is a bug in the mapper rather
    // than a coverage gain.
    for (const action of moved) {
      expect({ action, before: pathFor(action) }).toEqual({
        action,
        before: "post",
      });
    }
    expect(PUSHDOWN_DEMOTED_FIELDS.every((field) => nullableFields.has(field)))
      .toBe(true);
  });
});

/**
 * `allowPostFilter` is a call-level opt-in, so no policy can reach it and the corpus has no action
 * for it — but every corpus action reaches one side of it. The gate exists because a post-filter
 * is a promise the CALLER has to keep: Convex returns the documents the filter matched, and unless
 * the caller applies `postFilter` to each one before it is serialised, the untranslatable half of
 * the policy simply does not run.
 *
 * Stating it over the whole corpus rather than as two hand-built plans is what makes it a rule: an
 * action that changed sides would fail here, in both directions.
 */
describe("the allowPostFilter gate", () => {
  test.each([...POST_ACTIONS, ...byPath("split")])(
    "%s is refused without the opt-in",
    (action) => {
      expect(() => translate(action, { allowPostFilter: false })).toThrow(
        "allowPostFilter",
      );
    },
  );

  test.each(byPath("db"))("%s needs no opt-in", (action) => {
    const result = translate(action, { allowPostFilter: false });
    expect({
      filter: result.filter !== undefined,
      postFilter: result.postFilter !== undefined,
    }).toEqual({ filter: true, postFilter: false });
  });

  test.each(UNCONDITIONAL_ACTIONS)(
    "%s needs no opt-in either, having no condition to split",
    (action) => {
      expect(translate(action, { allowPostFilter: false }).kind).not.toEqual(
        PlanKind.CONDITIONAL,
      );
    },
  );
});

// -- the mapper contract, which no policy can reach ------------------------------------------------

describe("mapper forms", () => {
  // A deep relation shape, so the equivalence covers references resolved through several hops
  // rather than one lookup off the root.
  const DEEP_ACTION = "macro-depth3-exists";

  test("a function mapper resolves the same references as a record mapper", () => {
    const asFunction: Mapper = (reference) => MAPPER[reference] ?? {};

    expect(expectationFor(DEEP_ACTION, { mapper: asFunction })).toEqual(
      expectationFor(DEEP_ACTION),
    );
  });

  /**
   * The identity fallback, which the README states as a feature: "If you omit the mapper the
   * adapter will use the query plan paths verbatim." So an unmapped reference is not an error
   * here — it is a document path, and a caller whose documents are not shaped that way gets a
   * filter that matches nothing rather than one that matches too much.
   *
   * Pinned rather than assumed because it is the one place this adapter answers an unresolved
   * reference at all: the SQL adapters throw, and a reader coming from one of those would expect
   * the same. The direction of the failure is what makes it defensible — an under-grant is a bug
   * the caller sees, not rows the PDP denies.
   */
  test("an unmapped reference falls back to the plan path verbatim", () => {
    const { filter } = translate("cs-eq", { mapper: {} });
    if (!filter) throw new Error("cs-eq emitted no filter under an empty mapper");
    expect(recordFilter("cs-eq (empty mapper)", filter)).toEqual({
      op: "eq",
      args: [{ op: "field", args: ["request.resource.attr.aString"] }, "one"],
    });
  });
});

describe("nullAttributeRepresentation", () => {
  // `null-eq-missing` is the corpus's `nullRepresentationOmitted` probe: `== null` against an
  // attribute the caller OMITS when the field is NULL. The two conventions are indistinguishable
  // on the wire — the planner emits the same `eq(attr, null)` either way — so the adapter has to
  // be told, and the whole behaviour is a translator property with no store in it.
  const OMITTED_MESSAGE = requireMessage(
    "nullRepresentationOmitted.null-eq-missing.messages.convex",
    nullRepresentationOmittedFor(actionsFile, ADAPTER).find(
      (entry) => entry.action === "null-eq-missing",
    )?.message,
  );

  test("explicit is the default, and keeps the null-matching translation", () => {
    expect(
      expectationFor("null-eq-missing", {
        nullAttributeRepresentation: "explicit",
      }),
    ).toEqual(expectationFor("null-eq-missing"));
  });

  test("omitted: the same plan is refused rather than translated", () => {
    // A NULL field sends no attribute, so check() denies on a missing-attribute error while the
    // filter would return exactly those documents (#302).
    expect(() =>
      translate("null-eq-missing", {
        nullAttributeRepresentation: "omitted",
      }),
    ).toThrow(OMITTED_MESSAGE);
  });

  /**
   * The rejection is deliberately wider than the shapes that over-grant — negation is applied
   * around the built predicate, so a leaf cannot tell whether an enclosing `not` will flip it —
   * but it is not unbounded: a null-free comparison must still translate under `omitted`, or the
   * option would be a way to turn the adapter off. Asserted over the whole corpus, so the set of
   * actions the option rejects is a fact this file reports rather than one it assumes.
   */
  test("omitted rejects exactly the actions whose plan carries a null literal", () => {
    const carriesNull = (operand: unknown): boolean => {
      if (typeof operand !== "object" || operand === null) return false;
      const node = operand as Record<string, unknown>;
      if ("value" in node) {
        const value = node["value"];
        return (
          value === null || (Array.isArray(value) && value.includes(null))
        );
      }
      const operands = node["operands"];
      return Array.isArray(operands) && operands.some(carriesNull);
    };

    const rejected: string[] = [];
    const translated: string[] = [];
    for (const action of RECORDED_ACTIONS) {
      try {
        translate(action, { nullAttributeRepresentation: "omitted" });
        translated.push(action);
      } catch {
        rejected.push(action);
      }
    }

    const carrying = RECORDED_ACTIONS.filter((action) => {
      const plan = planFromWireFixture(action);
      return plan.kind === PlanKind.CONDITIONAL && carriesNull(plan.condition);
    });

    expect(rejected).toEqual(carrying);
    // Anti-vacuity in both directions: the option has to reject something and leave something.
    expect(rejected.length).toBeGreaterThan(0);
    expect(translated.length).toBeGreaterThan(0);
  });
});

// -- plans the planner cannot produce --------------------------------------------------------------

describe("plans the planner cannot produce", () => {
  // Input validation on a public function, not policy shapes. Every other assertion in this file
  // reads its plan from a fixture precisely because a typed plan is a belief about the planner —
  // but these are malformed by construction, so there is no fixture to read and nothing to
  // believe. They exist so a caller who hands the adapter a hand-rolled or half-decoded plan gets
  // an error rather than a filter.
  //
  // A shape CEL *can* express does not belong here, whatever its plan looks like: it belongs in
  // the corpus, where every adapter is asked about it.

  const plan = (condition: unknown): PlanResourcesResponse =>
    ({
      kind: PlanKind.CONDITIONAL,
      condition: condition as PlanExpressionOperand,
      cerbosCallId: "",
      requestId: "",
      validationErrors: [],
      metadata: undefined,
    }) as PlanResourcesResponse;

  test("an unrecognised plan kind", () => {
    expect(() =>
      queryPlanToConvex({
        queryPlan: { kind: "INVALID_KIND" } as unknown as PlanResourcesResponse,
        mapper: MAPPER,
      }),
    ).toThrow("Invalid query plan.");
  });

  test("an operand that is neither an expression, a variable nor a value", () => {
    expect(() =>
      queryPlanToConvex({ queryPlan: plan({}), mapper: MAPPER }),
    ).toThrow("Invalid Cerbos expression structure");
  });

  test("an operator this adapter has never heard of", () => {
    expect(() =>
      queryPlanToConvex({
        queryPlan: plan({ operator: "unsupported", operands: [] }),
        mapper: MAPPER,
      }),
    ).toThrow("Unsupported operator: unsupported");
  });

  test("isSet, which no policy can compile", () => {
    // `isSet` is not a registered CEL function, so a policy naming it fails to compile and the
    // operator never reaches the wire. The adapter carried a dedicated branch for it anyway; it
    // must fail closed like any unknown operator rather than guess at an existence filter
    // (cerbos/query-plan-adapters#261).
    expect(() =>
      queryPlanToConvex({
        queryPlan: plan({
          operator: "isSet",
          operands: [
            { name: "request.resource.attr.aOptionalString" },
            { value: true },
          ],
        }),
        mapper: MAPPER,
      }),
    ).toThrow("Unsupported operator: isSet");
  });
});

// -- shapes the corpus does not reach yet ----------------------------------------------------------

/**
 * A bridge, not a home. Everything below asserts a translator branch that no corpus action drives
 * today, which is exactly the situation `CLAUDE.md` says a per-adapter unit test must not be
 * allowed to settle into: a unit test pins the filter one adapter emits, and only a corpus action
 * asks the same question of every other adapter.
 *
 * Both gaps already have issues, and both of these blocks are to be deleted when they land:
 *
 * - `matches` against a pattern outside the RE2 subset — cerbos/query-plan-adapters#396, "the
 *   corpus never reaches a non-trivial regex".
 * - the value-list macro machinery past `exists`/`all` — cerbos/query-plan-adapters#394. The
 *   corpus drives `pv-exists`, `pv-all` and their unrolled forms; `exists_one`, the empty
 *   collection and element-field paths it does not.
 *
 * The plans here are hand-built for the same reason the sections above never are: there is no
 * fixture, because there is no action. That is the argument for the two issues rather than a
 * licence to keep writing them.
 */
describe("shapes the corpus does not reach yet", () => {
  const plan = (condition: unknown): PlanResourcesResponse =>
    ({
      kind: PlanKind.CONDITIONAL,
      condition: condition as PlanExpressionOperand,
      cerbosCallId: "",
      requestId: "",
      validationErrors: [],
      metadata: undefined,
    }) as PlanResourcesResponse;

  // #396. A pattern the adapter cannot prove it evaluates the way RE2 does is refused rather than
  // handed to JavaScript's own regex engine, whose backreferences and lazy quantifiers have no
  // RE2 equivalent — translating one means answering a question the policy never asked.
  test.each([
    ["a backreference", "(a)\\1"],
    ["a trailing wildcard under an end anchor", "^allowed.*$"],
    ["a trailing wildcard with a bare end anchor", "allowed.*$"],
  ])("matches with %s is refused", (_label, pattern) => {
    expect(() =>
      queryPlanToConvex({
        queryPlan: plan({
          operator: "matches",
          operands: [
            { name: "request.resource.attr.aString" },
            { value: pattern },
          ],
        }),
        mapper: MAPPER,
        allowPostFilter: true,
      }),
    ).toThrow("constant RE2-compatible pattern");
  });

  // #394. The planner ships a literal value-list collection above the unroll cliff, and the
  // adapter binds each element to the lambda variable rather than folding the macro.
  describe("a macro over a literal value-list collection", () => {
    const macroPostFilter = (
      macro: string,
      elements: unknown[],
      body: unknown,
    ): ((doc: Record<string, unknown>) => boolean) => {
      const result = queryPlanToConvex({
        queryPlan: plan({
          operator: macro,
          operands: [
            { value: elements },
            { operator: "lambda", operands: [body, { name: "t" }] },
          ],
        }),
        mapper: MAPPER,
        allowPostFilter: true,
      });
      if (!result.postFilter) throw new Error(`${macro} emitted no postFilter`);
      return result.postFilter;
    };

    const compare = (operator: string, right: unknown) => ({
      operator,
      operands: [{ name: "request.resource.attr.aString" }, right],
    });

    test("exists_one keeps CEL's exact cardinality", () => {
      const both = macroPostFilter(
        "exists_one",
        ["alpha", "alpha"],
        compare("eq", { name: "t" }),
      );
      expect(both({ aString: "alpha" })).toBe(false);

      const one = macroPostFilter(
        "exists_one",
        ["alpha", "beta"],
        compare("eq", { name: "t" }),
      );
      expect(one({ aString: "beta" })).toBe(true);
    });

    test("an empty collection keeps CEL's identity elements", () => {
      expect(
        macroPostFilter("exists", [], compare("eq", { name: "t" }))({
          aString: "alpha",
        }),
      ).toBe(false);
      expect(
        macroPostFilter("all", [], compare("ne", { name: "t" }))({
          aString: "alpha",
        }),
      ).toBe(true);
    });

    test("an element-field path reads the element, not the document", () => {
      const postFilter = macroPostFilter(
        "exists",
        [{ name: "alpha" }, { name: "beta" }],
        compare("eq", { name: "t.name" }),
      );
      expect(postFilter({ aString: "beta" })).toBe(true);
      expect(postFilter({ aString: "gamma" })).toBe(false);
    });
  });
});

describe("the golden asset", () => {
  // The asset carries the command that rewrites it, so a reader who opens the file after a failing
  // assertion is told how to look at the difference. That is only useful while the command exists.
  test("names a command this package actually defines", () => {
    const manifest = JSON.parse(
      fs.readFileSync(path.join(__dirname, "..", "package.json"), "utf8"),
    ) as { scripts: Record<string, string> };
    const [runner, run, script] = GOLDEN_REGENERATE_COMMAND.split(" ");

    expect({ runner, run }).toEqual({ runner: "npm", run: "run" });
    expect(Object.keys(manifest.scripts)).toContain(script);
  });
});
