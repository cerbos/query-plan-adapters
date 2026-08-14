import * as fs from "node:fs";
import * as path from "node:path";

import { describe, expect, test } from "@jest/globals";
import {
  PlanExpression,
  PlanExpressionValue,
  PlanExpressionVariable,
} from "@cerbos/core";
import type { PlanExpressionOperand, PlanResourcesResponse } from "@cerbos/core";
import type { Where } from "chromadb";

import { PlanKind, queryPlanToChromaDB } from ".";
import type { FieldMapper, FieldNameMapperConfig } from ".";
import {
  ADAPTER,
  FIELD_NAME_MAPPER,
  GOLDEN_REGENERATE_COMMAND,
  classifyActionsForAdapter,
  mappedMetadataKeys,
  nullRepresentationThrows,
  parseActionsFile,
  planFromWireFixture,
  readCorpusJson,
  readGoldenExpectations,
  requireMessage,
  wireFixtureActions,
  writeGoldenExpectations,
} from "./corpus";
import type { GoldenExpectation } from "./corpus";

/**
 * Translator unit test: for every action in the shared `../conformance/` corpus, the Chroma `Where`
 * filter this adapter emits. Offline — no Cerbos sidecar, no ChromaDB, no Docker.
 *
 * A per-adapter suite used to braid four assertions into every test. Three of them are somebody
 * else's job now, and this file makes only the fourth:
 *
 * | assertion | who owns it |
 * | --- | --- |
 * | the plan the PDP produces for a policy | `conformance/wire-fixtures/`, replanned and diffed by the `Conformance Corpus` workflow |
 * | which shapes this adapter must refuse, and with what message | `conformance/actions.json` — read below, not restated |
 * | the documents a filter returns | `adversarial.test.ts`, against a real ChromaDB collection with `check()` as the oracle |
 * | **the filter this adapter emits for a plan** | **here** |
 *
 * **The plans are read, not written.** A hand-built plan is a *belief* about what the planner
 * emits, and this repository keeps golden fixtures because that belief has been wrong before: a
 * planner change used to fail fixture regeneration and silently leave every adapter's hand-written
 * plans describing a wire contract that no longer existed. See
 * [ADR 0006](../../docs/adr/0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md).
 *
 * **The expectations are data, not literals.** The filters this adapter is pinned to emit live in
 * `golden/expectations.json`, a **golden expectation** file this adapter owns — never under
 * `conformance/`, where eleven workflows trigger and one adapter re-pinning one filter would re-run
 * the other ten. The file is regenerated with `npm run golden:update` and reviewed as a diff,
 * exactly like the wire fixtures it is asserted against. See
 * [ADR 0007](../../docs/adr/0007-adapters-share-data-not-code.md) and the "Golden expectations"
 * section of `conformance/README.md`.
 *
 * **This file reads as mostly-throws, and that is the adapter.** Chroma's `where` clause compares
 * flat scalar metadata on the document being matched: no joins, no collections, no arithmetic, no
 * pattern matching, no null. 164 of the corpus's 199 shapes are therefore fail-closed here, and
 * every one of them is asserted against the message `actions.json` pins rather than a bare "it
 * threw" — which for an adapter with this ratio is the difference between a suite and a formality
 * (cerbos/query-plan-adapters#326).
 *
 * **Adding a corpus action fails this file.** Every wire fixture must be accounted for here exactly
 * once — a golden expectation (an emitted filter or an unconditional plan kind) or a throw carrying
 * the message `actions.json` pins — and the completeness guard below is what makes a new action land
 * as a failure rather than as silence.
 */

const actionsFile = parseActionsFile(readCorpusJson("actions.json"));

/**
 * The shapes `actions.json` says this adapter must refuse, each with the message it must refuse
 * them with. Identical to the classification `adversarial.test.ts` asserts against a live PDP;
 * asserting it here as well is what lets the completeness guard below be total, and it costs a
 * millisecond rather than a PDP and a vector store.
 *
 * A throwing action needs no golden expectation of its own: the message is already corpus data,
 * pinned once in `actions.json` and read by every adapter. Writing it into this adapter's asset too
 * would create two places to change one string with nothing to say which is authoritative — and on
 * an adapter that refuses five sixths of the corpus, the asset would be almost entirely restatement
 * of shared data.
 */
const THROWING_ACTIONS = [
  ...classifyActionsForAdapter(actionsFile, ADAPTER).throwingActions,
  // The `nullRepresentationOmitted` group belongs here on this adapter and only on this adapter's
  // terms: elsewhere it is a refusal under one convention, here it is unconditional, because Chroma
  // metadata cannot hold a null distinguishably from an absent key (#302). It is a separate corpus
  // classification, so the harness keeps it separate; the throw suite does not need to.
  ...nullRepresentationThrows(actionsFile, ADAPTER),
].sort(([left], [right]) => left.localeCompare(right));
const THROWING = new Set(THROWING_ACTIONS.map(([action]) => action));

function translate(
  action: string,
  options: { fieldNameMapper?: FieldMapper; plannedAt?: string } = {},
): { kind: PlanKind; filters?: Where } {
  return queryPlanToChromaDB({
    queryPlan: planFromWireFixture(action, options.plannedAt),
    fieldNameMapper: options.fieldNameMapper ?? FIELD_NAME_MAPPER,
  });
}

/**
 * A Chroma `Where` clause is JSON already, so the golden entry is the translator's whole result —
 * no rendering step, no dialect, nothing normalised on the way in.
 *
 * The one thing worth checking on the way out is that it really is JSON: `-0` and the non-finite
 * numbers survive a `Where` object but not a `JSON.stringify`, and a plan crosses into a deployed
 * caller's Chroma query as a JSON body. A value the asset cannot record is a value the adapter
 * should not have emitted, so this fails rather than normalising.
 */
function expectationFor(action: string): GoldenExpectation {
  const result = translate(action);
  const roundTripped = JSON.parse(JSON.stringify(result)) as GoldenExpectation;
  for (const { field, operator, value } of literalsOf(result.filters)) {
    for (const literal of Array.isArray(value) ? value : [value]) {
      if (typeof literal === "number" && !Number.isFinite(literal)) {
        throw Error(
          `${action} binds a non-finite number to ${field} ${operator}; the golden file cannot record it faithfully`,
        );
      }
      if (Object.is(literal, -0)) {
        throw Error(
          `${action} binds a negative zero to ${field} ${operator}; the golden file cannot record it faithfully`,
        );
      }
    }
  }
  return roundTripped;
}

// -- reading an emitted filter back ---------------------------------------------------------------

interface Comparison {
  field: string;
  operator: string;
  value: unknown;
}

interface FilterShape {
  /** Every `$`-prefixed key used in a logical position, in encounter order. */
  logical: string[];
  comparisons: Comparison[];
}

/**
 * Decompose an emitted `Where` into the logical operators it nests and the leaf comparisons it
 * makes, so the rules below can be stated over the whole corpus rather than over hand-picked
 * shapes.
 *
 * Unknown structure is a failure rather than something skipped: a rule that silently ignores a node
 * it does not recognise is a rule a new emission shape walks straight past.
 */
function shapeOf(where: Where | undefined, path = "filters"): FilterShape {
  const shape: FilterShape = { logical: [], comparisons: [] };
  if (where === undefined) {
    return shape;
  }
  for (const [key, value] of Object.entries(where)) {
    if (key.startsWith("$")) {
      shape.logical.push(key);
      if (!Array.isArray(value)) {
        throw Error(`${path}.${key} is a logical operator over a non-array`);
      }
      for (const [index, child] of value.entries()) {
        const nested = shapeOf(child as Where, `${path}.${key}[${index}]`);
        shape.logical.push(...nested.logical);
        shape.comparisons.push(...nested.comparisons);
      }
      continue;
    }
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
      throw Error(`${path}.${key} is not a Chroma comparison object`);
    }
    for (const [operator, operand] of Object.entries(value)) {
      shape.comparisons.push({ field: key, operator, value: operand });
    }
  }
  return shape;
}

function literalsOf(where: Where | undefined): Comparison[] {
  return shapeOf(where).comparisons;
}

// -- the golden expectations ----------------------------------------------------------------------
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

const CONDITIONAL_ACTIONS = RECORDED_ACTIONS.filter(
  (action) => RECORDED.get(action)!.expectation.kind === PlanKind.CONDITIONAL,
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
    "%s is refused with the message actions.json pins (%s)",
    (action, _reason, message) => {
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
    const throwing = THROWING_ACTIONS.map(([action]) => action);
    const classified = [...RECORDED_ACTIONS, ...throwing].sort();

    // Total: a corpus action with no golden expectation and no pinned throw lands as a failure
    // rather than as silence. This is the assertion that makes the asset self-maintaining —
    // adding a hostile shape to the corpus forces someone to look at the filter this adapter emits
    // for it, and `npm run golden:update` refuses to invent one for a shape that throws.
    expect(classified).toEqual(wireFixtureActions());
    // Disjoint: an action carrying a golden expectation AND declared unsupported would satisfy
    // the union above while asserting two contradictory things.
    expect(classified).toEqual([...new Set(classified)].sort());
    // The asset is written sorted, so a translator change reads as the list of shapes it moved.
    expect(RECORDED_ACTIONS).toEqual([...RECORDED_ACTIONS].sort());

    // Tripwires. Bump them deliberately: a count that moves without anyone noticing is how a
    // shape gets dropped from an asset nobody reads end to end.
    expect({
      conditional: CONDITIONAL_ACTIONS.length,
      unconditional: RECORDED_ACTIONS.length - CONDITIONAL_ACTIONS.length,
      throwing: throwing.length,
    }).toEqual({ conditional: 33, unconditional: 2, throwing: 164 });
  });
});

/**
 * Where in the walk each rejection happens, and how many corpus shapes reach each site.
 *
 * `actions.json` pins a substring of the message per action, so the throw suite above proves every
 * refusal is the declared one. It cannot say anything about the *shape* of the refusals taken
 * together, and on an adapter that refuses 164 of 199 shapes that is the more interesting property:
 * five sixths of this corpus is rejected, and it matters whether that happens at five sites or at
 * one catch-all.
 *
 * Two things are asserted. **Total**: every refusal matches a site this adapter actually has, so a
 * shape rejected by an accident — a `TypeError`, a mapper lookup that happened to fail — cannot pass
 * as a declared limitation, which is the #326 trap at corpus scale. **Pinned counts**: a translator
 * change that moves a shape from one site to another shows up as a diff even though both sites throw
 * and `actions.json` is unchanged. The distribution below is the honest summary of this adapter:
 * `binaryOperands` rejecting a computed operand is the single mechanism behind 101 of the 164, and
 * every reason in `actions.json` for those shapes — arithmetic, casts, ternaries, projections,
 * macros above the unroll cap — reduces to the same thing at the wire level, an operand that is not
 * a bare metadata key or a literal.
 */
describe("the rejection sites the corpus reaches", () => {
  const SITES: [site: string, pattern: RegExp][] = [
    // `binaryOperands`: an operand slot holds a computed sub-expression, not a key or a literal.
    ["computed operand", /^Nested expressions are not supported/],
    // `binaryOperands`: both sides are metadata keys, and a Where clause compares one to a literal.
    ["field-to-field", /^Variable-to-variable comparisons are not supported/],
    // `mapComparison` / `mapBooleanVariable`: $ne and $nin match a document missing the key.
    ["inequality over an optional key", / is unsafe for optional Chroma metadata/],
    // `whereFor`: the operator has no Chroma equivalent at all.
    ["no such operator", /^Unsupported operator /],
    // `negateOperand`: the operator is not in NEGATED_OPERATOR, so there is nothing to invert.
    ["not negatable", /^Cannot negate operator /],
    // `normalizeOperator`: value-first `in` asks whether a literal is inside a metadata field.
    [
      "mirrored membership",
      /^ChromaDB filters cannot test whether a literal is contained/,
    ],
    // `requireLiteral` / `requireLiteralList`: the literal is null, nested, or a mixed list.
    ["non-scalar literal", / requires a (finite number|list|non-empty)/],
    // `mapComparison`: a fractional threshold against a field declared integer.
    ["fractional threshold", / cannot safely compare a fractional threshold/],
    // `binaryOperands`: the ternary arrives as a three-operand conditional.
    ["operand arity", /^Expected exactly two operands$/],
  ];

  const siteOf = (action: string): string => {
    let raised: string;
    try {
      translate(action);
      return "<did not throw>";
    } catch (error) {
      raised = error instanceof Error ? error.message : String(error);
    }
    const matched = SITES.filter(([, pattern]) => pattern.test(raised));
    if (matched.length !== 1) {
      throw Error(
        `${action} is refused with "${raised}", which matches ${matched.length} of this adapter's known rejection sites`,
      );
    }
    return matched[0]![0];
  };

  test("every refused shape lands on exactly one of them, in these numbers", () => {
    const counts: Record<string, number> = {};
    for (const [action] of THROWING_ACTIONS) {
      const site = siteOf(action);
      counts[site] = (counts[site] ?? 0) + 1;
    }

    expect(counts).toEqual({
      "computed operand": 101,
      "no such operator": 18,
      "inequality over an optional key": 12,
      "not negatable": 11,
      "field-to-field": 10,
      "mirrored membership": 5,
      "non-scalar literal": 4,
      "operand arity": 2,
      "fractional threshold": 1,
    });
    expect(Object.values(counts).reduce((sum, n) => sum + n, 0)).toEqual(
      THROWING_ACTIONS.length,
    );
  });
});

/**
 * The properties a regenerated asset must not silently accept.
 *
 * Pinned bytes do not survive `npm run golden:update` being run and committed unread; rules do. So
 * each of these is stated over every translated corpus action rather than over a chosen shape, and
 * each carries an anti-vacuity assertion, because every one of them is satisfied by an empty filter.
 */
describe("what an emitted filter may contain", () => {
  const recordedFilters = (action: string): Where | undefined =>
    (RECORDED.get(action)!.expectation as { filters?: Where }).filters;

  const ALL_COMPARISONS = CONDITIONAL_ACTIONS.flatMap((action) =>
    literalsOf(recordedFilters(action)).map((comparison) => ({
      action,
      ...comparison,
    })),
  );

  /**
   * An unmapped reference falls back to the Cerbos path verbatim (`request.resource.attr.aString`),
   * which is a metadata key no collection holds — so the filter is not an error, it is a filter
   * that matches nothing and silently denies. Chroma cannot report it either: an unknown key is
   * simply absent. This is the one rule the harness cannot make, because a filter that selects no
   * document agrees with an oracle that allows none.
   */
  test("every field a filter names is a metadata key the mapper declares", () => {
    const declared = new Set(mappedMetadataKeys());
    const undeclared = ALL_COMPARISONS.filter(
      ({ field }) => !declared.has(field),
    ).map(({ action, field }) => `${action}: ${field}`);

    expect(undeclared).toEqual([]);
    // Anti-vacuity: the rule above holds for a corpus that emits no comparison at all.
    expect(ALL_COMPARISONS.length).toBeGreaterThan(0);
  });

  /**
   * Chroma's `Where` grammar has no `$not` and no `$nor`. Every negation in a plan has to be pushed
   * down to the leaves — De Morgan over `and`/`or`, operator inversion at a comparison — and a
   * filter that carried one out to Chroma would be rejected at query time, not at translation.
   */
  test("no negation operator survives into an emitted filter", () => {
    const logical = new Set(
      CONDITIONAL_ACTIONS.flatMap(
        (action) => shapeOf(recordedFilters(action)).logical,
      ),
    );

    expect([...logical].sort()).toEqual(["$and", "$or"]);
  });

  /**
   * Anti-vacuity for the rule above: the corpus has to still drive negation through both De Morgan
   * branches and through operator inversion, or "no `$not` survived" would be a statement about a
   * corpus that never negates anything.
   */
  test("the corpus still drives the negations that rule polices", () => {
    for (const action of [
      "double-negation",
      "triple-negation",
      "not-and",
      "not-lt",
      "not-gt",
    ]) {
      expect(CONDITIONAL_ACTIONS).toContain(action);
    }
    const inverted = ALL_COMPARISONS.filter(({ operator }) =>
      ["$ne", "$nin", "$gte", "$lte"].includes(operator),
    );
    expect(inverted.length).toBeGreaterThan(0);
  });

  /**
   * The adapter's central over-grant guard, stated as a rule over the whole corpus.
   *
   * Chroma's `$ne`/`$nin` MATCH a document that is missing the metadata key, where CEL raises a
   * missing-attribute error and the PDP denies. So an inequality is only sound over a key the
   * integrator has asserted is present on every document, and the adapter refuses it otherwise.
   * A regenerated asset that quietly acquired an inequality over an optional key would be an
   * authorization bug, not a diff.
   */
  test("an inequality is emitted only over a field declared required", () => {
    const required = new Set(
      Object.values(FIELD_NAME_MAPPER)
        .filter(
          (entry): entry is FieldNameMapperConfig =>
            typeof entry !== "string" && entry.required === true,
        )
        .map((entry) => entry.field),
    );
    const unsafe = ALL_COMPARISONS.filter(
      ({ field, operator }) =>
        ["$ne", "$nin"].includes(operator) && !required.has(field),
    ).map(({ action, field, operator }) => `${action}: ${field} ${operator}`);

    expect(unsafe).toEqual([]);
  });

  /**
   * The anti-vacuity half of the rule above, and the assertion that proves `required` is read at
   * all: stripping it from the mapper must move exactly the actions whose filter uses an inequality
   * out of the translated set, and nothing else.
   *
   * A rule about a flag nothing consults passes for every corpus. This is the mutation that says
   * otherwise — it is the same argument convex's `nullable` pin makes, applied to the flag this
   * adapter gates `$ne`/`$nin` on.
   */
  test("clearing required moves exactly the actions that emit an inequality", () => {
    const optionalEverywhere: Record<string, FieldNameMapperConfig> =
      Object.fromEntries(
        Object.entries(FIELD_NAME_MAPPER).map(([reference, entry]) => [
          reference,
          typeof entry === "string"
            ? { field: entry, required: false }
            : { ...entry, required: false },
        ]),
      );

    const nowRefused = CONDITIONAL_ACTIONS.filter((action) => {
      try {
        translate(action, { fieldNameMapper: optionalEverywhere });
        return false;
      } catch {
        return true;
      }
    });
    const emitsInequality = [
      ...new Set(
        ALL_COMPARISONS.filter(({ operator }) =>
          ["$ne", "$nin"].includes(operator),
        ).map(({ action }) => action),
      ),
    ].sort();

    expect(nowRefused).toEqual(emitsInequality);
    expect(emitsInequality.length).toBeGreaterThan(0);
  });

  /**
   * Chroma stores integer and floating-point metadata distinguishably, and an ordered comparison
   * against a fractional threshold over a field declared `integer` would compare values the store
   * never holds. The adapter refuses it unless the mapping declares `numericType: "float"`.
   */
  test("an ordered comparison binds a fractional threshold only where float is declared", () => {
    const fractional = ALL_COMPARISONS.filter(
      ({ operator, value }) =>
        ["$lt", "$lte", "$gt", "$gte"].includes(operator) &&
        typeof value === "number" &&
        !Number.isInteger(value),
    ).map(({ action, field }) => `${action}: ${field}`);

    expect(fractional).toEqual([]);
    // Anti-vacuity: the rule is about ordered comparisons, so the corpus must still emit some.
    expect(
      ALL_COMPARISONS.filter(({ operator }) =>
        ["$lt", "$lte", "$gt", "$gte"].includes(operator),
      ).length,
    ).toBeGreaterThan(0);
  });

  /**
   * A `Where` clause leaves this adapter as part of a JSON request body, so a literal JSON cannot
   * carry is a literal the deployed adapter could not send. `expectationFor` refuses one outright;
   * this is the assertion that says the refusal is live rather than unreachable, by naming the
   * corpus actions whose arithmetic produces a non-finite or negatively-signed zero and confirming
   * every one of them is refused before a literal is ever built.
   */
  test("the actions that would bind a value JSON cannot hold are all refused", () => {
    for (const action of [
      "cr-div-zero",
      "cr-div-neg-zero",
      "nan-ord-inf",
      "nan-ord-le",
    ]) {
      expect(THROWING.has(action)).toBe(true);
    }
  });
});

/**
 * The mapper contract, which no policy can reach.
 *
 * These are properties of the `fieldNameMapper` argument rather than of a plan shape: the corpus
 * drives one mapping, and a second mapping is not a hostile CEL shape but a different call. This is
 * where the coverage the retired shared-policy suite had that is genuinely not a corpus action
 * lives.
 */
describe("mapper forms", () => {
  /** A translated shape whose filter names two different metadata keys. */
  const RECORD_ACTION = "nary-and";

  test("a function mapper resolves the same references as a record mapper", () => {
    const asFunction: FieldMapper = (reference) =>
      FIELD_NAME_MAPPER[reference] ?? reference;

    expect(
      translate(RECORD_ACTION, { fieldNameMapper: asFunction }),
    ).toEqual(translate(RECORD_ACTION));
  });

  /**
   * The default is optional, in both spellings a mapper has. A bare string carries no presence
   * assertion and neither does an absent entry, so `$ne` is refused for both — the adapter never
   * infers presence from the mere existence of a mapping. `vf-ne` is the discriminating action:
   * under the corpus mapper, where `aString` is declared `required: true`, it translates.
   */
  test.each([
    ["a plain-string mapping", { "request.resource.attr.aString": "aString" }],
    ["an unmapped reference", {}],
  ])("%s is optional, so an inequality over it is refused", (_label, mapper) => {
    expect(translate("vf-ne")).toEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { aString: { $ne: "one" } },
    });
    expect(() => translate("vf-ne", { fieldNameMapper: mapper })).toThrow(
      /ne is unsafe for optional Chroma metadata because missing fields match the filter/,
    );
  });

  /**
   * An unmapped reference is used verbatim as the metadata key. It is documented behaviour rather
   * than a defect, but it is also the adapter's one silent failure mode — no collection holds a key
   * called `request.resource.attr.aString`, so the filter selects nothing and the caller sees a deny
   * rather than an error. The harness cannot catch it either: a filter that returns no document
   * agrees with an oracle that allows none. Pinning it here is what makes it visible, and it is why
   * the "every field a filter names is a metadata key the mapper declares" rule above exists.
   */
  test("an unmapped reference becomes a metadata key spelled as the Cerbos path", () => {
    expect(translate("cs-eq", { fieldNameMapper: {} })).toEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { "request.resource.attr.aString": { $eq: "one" } },
    });
  });

  /**
   * The same shape the corpus refuses under the declared `integer` mapping translates once the
   * mapping says the metadata is floating point. Both directions matter: the refusal is the
   * adapter's, and it is a declaration the integrator can lift rather than a shape it cannot build.
   */
  test("numericType float admits the fractional threshold the integer declaration refuses", () => {
    const action = "double-threshold";
    const asFloat = {
      ...FIELD_NAME_MAPPER,
      "request.resource.attr.aNumber": {
        field: "aNumber",
        numericType: "float" as const,
        required: true,
      },
    };

    expect(() => translate(action)).toThrow(
      /cannot safely compare a fractional threshold/,
    );
    expect(translate(action, { fieldNameMapper: asFloat })).toEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { aNumber: { $gte: 1.5 } },
    });
  });
});

describe("plans the planner cannot produce", () => {
  // Input validation on a public function, not policy shapes. Every other assertion in this file
  // reads its plan from a fixture precisely because a typed plan is a belief about the planner —
  // but these are malformed by construction, so there is no fixture to read and nothing to
  // believe. They exist so a caller who hands the adapter a hand-rolled or half-decoded plan gets
  // an error rather than a filter.
  //
  // A shape CEL *can* express does not belong here, whatever its plan looks like: it belongs in
  // the corpus, where all ten adapters are asked about it.

  const plan = (condition: PlanExpressionOperand): PlanResourcesResponse =>
    ({
      kind: PlanKind.CONDITIONAL,
      condition,
      cerbosCallId: "",
      requestId: "",
      validationErrors: [],
      metadata: undefined,
    }) as PlanResourcesResponse;

  test("an unrecognised plan kind", () => {
    expect(() =>
      queryPlanToChromaDB({
        queryPlan: { kind: "INVALID_KIND" } as unknown as PlanResourcesResponse,
        fieldNameMapper: FIELD_NAME_MAPPER,
      }),
    ).toThrow("Invalid query plan.");
  });

  test("a comparison with the wrong number of operands", () => {
    expect(() =>
      queryPlanToChromaDB({
        queryPlan: plan(
          new PlanExpression("eq", [
            new PlanExpressionVariable("request.resource.attr.aString"),
          ]),
        ),
        fieldNameMapper: FIELD_NAME_MAPPER,
      }),
    ).toThrow("Expected exactly two operands");
  });

  test("a comparison between two literals", () => {
    expect(() =>
      queryPlanToChromaDB({
        queryPlan: plan(
          new PlanExpression("eq", [
            new PlanExpressionValue("one"),
            new PlanExpressionValue("two"),
          ]),
        ),
        fieldNameMapper: FIELD_NAME_MAPPER,
      }),
    ).toThrow(
      "Value-to-value comparisons are not supported by ChromaDB filters",
    );
  });

  test("a condition that is not an expression at all", () => {
    expect(() =>
      queryPlanToChromaDB({
        queryPlan: plan(new PlanExpressionValue(true)),
        fieldNameMapper: FIELD_NAME_MAPPER,
      }),
    ).toThrow("Query plan did not contain an expression for operand");
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
