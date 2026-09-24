import { describe, expect, test } from "@jest/globals";
import {
  PlanExpression,
  PlanExpressionValue,
  PlanExpressionVariable,
} from "@cerbos/core";
import type {
  PlanExpressionOperand,
  PlanResourcesResponse,
} from "@cerbos/core";
import type { Where } from "chromadb";

import { PlanKind, queryPlanToChromaDB, UnsupportedOperatorError } from ".";
import type { FieldMapper, FieldNameMapperConfig } from ".";
import {
  FIELD_NAME_MAPPER,
  mappedMetadataKeys,
  pdpTags,
  planOf,
  readGolden,
  readGoldens,
  requiredMetadataKeys,
} from "./corpus";

/**
 * Offline unit tests: the refusal type, the rules every emitted filter obeys, what a caller can
 * pass that the conformance corpus cannot vary (mapper forms, `required`, `numericType`), and plans
 * the planner cannot produce.
 *
 * Which documents a corpus case returns is the conformance harness's job (`adversarial.test.ts`);
 * nothing here pins the filter emitted for a corpus case. Plans come from the current PDP's golden
 * files, read by case id, rather than being typed by hand.
 */

const CURRENT = pdpTags()[0]!;

function translate(
  id: string,
  options: { fieldNameMapper?: FieldMapper; now?: string } = {},
): { kind: PlanKind; filters?: Where } {
  return queryPlanToChromaDB({
    queryPlan: planOf(readGolden(CURRENT, id), options.now),
    fieldNameMapper: options.fieldNameMapper ?? FIELD_NAME_MAPPER,
  });
}

/** Whatever `run` throws, or `undefined` if it returns. */
function thrownBy(run: () => unknown): unknown {
  try {
    run();
  } catch (error) {
    return error;
  }
  return undefined;
}

/** Every current golden this adapter translates, with the filter it emits. */
const TRANSLATED = readGoldens(CURRENT).flatMap(({ id }) => {
  try {
    return [{ id, ...translate(id) }];
  } catch {
    return [];
  }
});
const CONDITIONAL = TRANSLATED.filter(
  ({ kind }) => kind === PlanKind.CONDITIONAL,
);

describe("the refusal type", () => {
  // Every shape this adapter cannot express raises `UnsupportedOperatorError`, which the
  // conformance harness asserts for every `unsupported` ledger entry. These pin the boundary on the
  // other side: it is still an `Error`, and it names the operator a caller can branch on (#228).
  test("a refused shape raises UnsupportedOperatorError, which is an Error", () => {
    const raised = thrownBy(() =>
      translate("regex/matches/lookahead-from-principal"),
    );
    expect(raised).toBeInstanceOf(UnsupportedOperatorError);
    expect(raised).toBeInstanceOf(Error);
    expect((raised as UnsupportedOperatorError).name).toBe(
      "UnsupportedOperatorError",
    );
    expect((raised as UnsupportedOperatorError).operator).toBe("matches");
  });

  // A collection macro reports itself, never `lambda`, which names nothing a caller wrote.
  test("no refusal reports the lambda operator", () => {
    const operators = readGoldens(CURRENT).flatMap((golden) => {
      const raised = thrownBy(() => translate(golden.id));
      return raised instanceof UnsupportedOperatorError
        ? [raised.operator]
        : [];
    });
    expect(operators.length).toBeGreaterThan(0);
    expect(operators).not.toContain("lambda");
  });
});

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

/**
 * Rules every emitted filter obeys, stated over every golden plan this adapter translates rather
 * than pinned per case. Each carries an anti-vacuity assertion, because every one of them is
 * satisfied by an empty filter.
 */
describe("what an emitted filter may contain", () => {
  const ALL_COMPARISONS = CONDITIONAL.flatMap(({ id, filters }) =>
    literalsOf(filters).map((comparison) => ({ action: id, ...comparison })),
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
      CONDITIONAL.flatMap(({ filters }) => shapeOf(filters).logical),
    );

    expect([...logical].sort()).toEqual(["$and", "$or"]);
  });

  /**
   * Anti-vacuity for the rule above: the corpus has to still drive negation through both De Morgan
   * branches and through operator inversion, or "no `$not` survived" would be a statement about a
   * corpus that never negates anything.
   */
  test("the corpus still drives the negations that rule polices", () => {
    const conditional = CONDITIONAL.map(({ id }) => id);
    for (const id of [
      "logic/not/double-negation",
      "logic/not/triple-negation",
      "logic/not/over-and",
      "logic/not/less-than",
      "logic/not/greater-than",
    ]) {
      expect(conditional).toContain(id);
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
   * A translator change that quietly emitted an inequality over an optional key would be an
   * authorization bug.
   */
  test("an inequality is emitted only over a field declared required", () => {
    const required = new Set(requiredMetadataKeys());
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

    const nowRefused = CONDITIONAL.map(({ id }) => id).filter((id) =>
      thrownBy(() => translate(id, { fieldNameMapper: optionalEverywhere })),
    );
    const emitsInequality = [
      ...new Set(
        ALL_COMPARISONS.filter(({ operator }) =>
          ["$ne", "$nin"].includes(operator),
        ).map(({ action }) => action),
      ),
    ];

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
   * carry — a non-finite number, a negative zero — is a literal the deployed adapter could not
   * send faithfully.
   */
  test("every emitted literal survives a JSON round trip", () => {
    const unfaithful = ALL_COMPARISONS.filter(({ value }) =>
      (Array.isArray(value) ? value : [value]).some(
        (literal) =>
          Object.is(literal, -0) ||
          (typeof literal === "number" && !Number.isFinite(literal)),
      ),
    ).map(({ action, field }) => `${action}: ${field}`);

    expect(unfaithful).toEqual([]);
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
  const RECORD_ACTION = "logic/and/three-conjuncts";

  test("a function mapper resolves the same references as a record mapper", () => {
    const asFunction: FieldMapper = (reference) =>
      FIELD_NAME_MAPPER[reference] ?? reference;

    expect(translate(RECORD_ACTION, { fieldNameMapper: asFunction })).toEqual(
      translate(RECORD_ACTION),
    );
  });

  /**
   * `comparison/not-equals/value-first` is the discriminating case for the two tests below: under
   * the corpus mapper, where `aString` is declared `required: true`, it translates to an inequality
   * over that key. This pins that precondition, so the tests below cannot pass against some other
   * shape.
   */
  const VF_NE = "comparison/not-equals/value-first";

  test("the discriminating case is an inequality over a required key", () => {
    expect(literalsOf(translate(VF_NE).filters)).toEqual([
      { field: "aString", operator: "$ne", value: "one" },
    ]);
  });

  /**
   * The default is optional, in both spellings a mapper has. A bare string carries no presence
   * assertion and neither does an absent entry, so `$ne` is refused for both — the adapter never
   * infers presence from the mere existence of a mapping.
   */
  test.each([
    ["a plain-string mapping", { "request.resource.attr.aString": "aString" }],
    ["an unmapped reference", {}],
  ])(
    "%s is optional, so an inequality over it is refused",
    (_label, mapper) => {
      expect(() => translate(VF_NE, { fieldNameMapper: mapper })).toThrow(
        /ne is unsafe for optional Chroma metadata because missing fields match the filter/,
      );
    },
  );

  /**
   * An unmapped reference is used verbatim as the metadata key. It is documented behaviour rather
   * than a defect, but it is also the adapter's one silent failure mode — no collection holds a key
   * called `request.resource.attr.aString`, so the filter selects nothing and the caller sees a deny
   * rather than an error. The harness cannot catch it either: a filter that returns no document
   * agrees with an oracle that allows none. Pinning it here is what makes it visible, and it is why
   * the "every field a filter names is a metadata key the mapper declares" rule above exists.
   */
  test("an unmapped reference becomes a metadata key spelled as the Cerbos path", () => {
    expect(
      translate("string/equals/case-sensitive", { fieldNameMapper: {} }),
    ).toEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { "request.resource.attr.aString": { $eq: "one" } },
    });
  });

  /**
   * The one operand a golden file cannot pin, and the assertion that it does not matter here.
   *
   * The generator records the folded `now() - duration("24h")` literal as `__NOW_MINUS_24H__`,
   * because it differs on every capture — so reading the plan back means choosing an instant. On
   * the SQL adapters that choice is load-bearing: the PDP emits nanosecond precision, and a tidy
   * millisecond substitute would translate where production refuses. Here it is inert, because the
   * comparison never reaches a literal — the operand is a computed expression and `binaryOperands`
   * rejects it first.
   */
  test.each([
    "timestamp/less-than/relative-window",
    "timestamp/greater-than/relative-window-value-first",
  ])("%s is refused for the same reason at either instant precision", (id) => {
    const nanos = thrownBy(() => translate(id));
    const millis = thrownBy(() =>
      translate(id, { now: "2026-08-11T09:13:39.123Z" }),
    );

    expect(nanos).toBeInstanceOf(UnsupportedOperatorError);
    expect(millis).toEqual(nanos);
    expect((millis as Error).message).toBe((nanos as Error).message);
  });

  /**
   * The same shape the corpus refuses under the declared `integer` mapping translates once the
   * mapping says the metadata is floating point. Both directions matter: the refusal is the
   * adapter's, and it is a declaration the integrator can lift rather than a shape it cannot build.
   */
  test("numericType float admits the fractional threshold the integer declaration refuses", () => {
    const action = "comparison/greater-or-equal/fractional-threshold";
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
  // the corpus, where every adapter is asked about it.

  const plan = (condition: PlanExpressionOperand): PlanResourcesResponse =>
    ({
      kind: PlanKind.CONDITIONAL,
      condition,
      cerbosCallId: "",
      requestId: "",
      validationErrors: [],
      metadata: undefined,
    }) as PlanResourcesResponse;

  // A malformed plan is the caller's bug, not a policy shape Chroma cannot hold, so it stays a
  // plain `Error`: a caller that routes `UnsupportedOperatorError` to a fallback must not route a
  // half-decoded plan there with it (#228).
  const expectPlainError = (run: () => unknown): void => {
    const raised = thrownBy(run);
    expect(raised).toBeInstanceOf(Error);
    expect(raised).not.toBeInstanceOf(UnsupportedOperatorError);
  };

  test("an unrecognised plan kind", () => {
    const run = () =>
      queryPlanToChromaDB({
        queryPlan: { kind: "INVALID_KIND" } as unknown as PlanResourcesResponse,
        fieldNameMapper: FIELD_NAME_MAPPER,
      });
    expect(run).toThrow("Invalid query plan.");
    expectPlainError(run);
  });

  // The same message as the corpus's ternary, which is typed: there the operator (`if`) is one
  // this adapter maps to no comparison at all, here it is `eq` short of an operand.
  test("a comparison with the wrong number of operands", () => {
    const run = () =>
      queryPlanToChromaDB({
        queryPlan: plan(
          new PlanExpression("eq", [
            new PlanExpressionVariable("request.resource.attr.aString"),
          ]),
        ),
        fieldNameMapper: FIELD_NAME_MAPPER,
      });
    expect(run).toThrow("Expected exactly two operands");
    expectPlainError(run);
  });

  // Typed, unlike its neighbours: two literals is not a structural defect in the plan but a shape
  // the `Where` grammar cannot hold, since it compares a metadata key to a literal.
  test("a comparison between two literals", () => {
    const run = () =>
      queryPlanToChromaDB({
        queryPlan: plan(
          new PlanExpression("eq", [
            new PlanExpressionValue("one"),
            new PlanExpressionValue("two"),
          ]),
        ),
        fieldNameMapper: FIELD_NAME_MAPPER,
      });
    expect(run).toThrow(
      "Value-to-value comparisons are not supported by ChromaDB filters",
    );
    const raised = thrownBy(run);
    expect(raised).toBeInstanceOf(UnsupportedOperatorError);
    expect((raised as UnsupportedOperatorError).operator).toBe("eq");
  });

  test("a condition that is not an expression at all", () => {
    const run = () =>
      queryPlanToChromaDB({
        queryPlan: plan(new PlanExpressionValue(true)),
        fieldNameMapper: FIELD_NAME_MAPPER,
      });
    expect(run).toThrow("Query plan did not contain an expression for operand");
    expectPlainError(run);
  });
});
