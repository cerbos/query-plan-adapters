import { beforeAll, describe, expect, test } from "@jest/globals";
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
import type {
  FieldMapper,
  FieldNameMapperConfig,
  QueryPlanToChromaDBResult,
} from ".";
import {
  FIELD_NAME_MAPPER,
  pdpTags,
  planOf,
  readGolden,
  readGoldens,
  requiredMetadataKeys,
} from "./corpus";
import type { Golden } from "./corpus";

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

async function translate(
  id: string,
  options: { fieldNameMapper?: FieldMapper } = {},
): Promise<{ kind: PlanKind; filters?: Where }> {
  return queryPlanToChromaDB({
    queryPlan: await planOf(readGolden(CURRENT, id)),
    fieldNameMapper: options.fieldNameMapper ?? FIELD_NAME_MAPPER,
  });
}

/** Whatever `run` throws or rejects with, or `undefined` if it returns. */
async function thrownBy(run: () => unknown): Promise<unknown> {
  try {
    await run();
  } catch (error) {
    return error;
  }
  return undefined;
}

interface Translated {
  id: string;
  kind: PlanKind;
  filters?: Where;
}

/** Every current golden translated under `fieldNameMapper`, with the filter it emits. */
async function translatedUnder(
  fieldNameMapper?: FieldMapper,
): Promise<Translated[]> {
  const outcomes = await Promise.all(
    readGoldens(CURRENT).map(async ({ id }): Promise<Translated[]> => {
      try {
        const options = fieldNameMapper ? { fieldNameMapper } : {};
        return [{ id, ...(await translate(id, options)) }];
      } catch {
        return [];
      }
    }),
  );
  return outcomes.flat();
}

/** Every current golden this adapter translates, with the filter it emits. */
let TRANSLATED: Translated[] = [];
let CONDITIONAL: Translated[] = [];

/**
 * The corpus mapping with every key asserted present (`required: true`) — a caller-supplied
 * argument the corpus cannot vary. The corpus mapping itself can assert it for no scalar, since
 * every scalar attribute is missing on some seed, so this is the only mapping under which the
 * adapter's inequality path is reached at all. It is not a sound mapping for the corpus dataset
 * and is never used to select documents.
 */
const PRESENT_EVERYWHERE: Record<string, FieldNameMapperConfig> =
  Object.fromEntries(
    Object.entries(FIELD_NAME_MAPPER).map(([reference, entry]) => [
      reference,
      typeof entry === "string"
        ? { field: entry, required: true }
        : { ...entry, required: true },
    ]),
  );

/** Every current golden this adapter translates under `PRESENT_EVERYWHERE`, as a conditional. */
let CONDITIONAL_IF_PRESENT: Translated[] = [];

beforeAll(async () => {
  TRANSLATED = await translatedUnder();
  CONDITIONAL = TRANSLATED.filter(({ kind }) => kind === PlanKind.CONDITIONAL);
  CONDITIONAL_IF_PRESENT = (await translatedUnder(PRESENT_EVERYWHERE)).filter(
    ({ kind }) => kind === PlanKind.CONDITIONAL,
  );
});

describe("the refusal type", () => {
  // Every shape this adapter cannot express raises `UnsupportedOperatorError`, which the
  // conformance harness asserts for every `unsupported` ledger entry. These pin the boundary on the
  // other side: it is still an `Error`, and it names the operator a caller can branch on (#228).
  test("a refused shape raises UnsupportedOperatorError, which is an Error", async () => {
    const raised = await thrownBy(() =>
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
  test("no refusal reports the lambda operator", async () => {
    const raised = await Promise.all(
      readGoldens(CURRENT).map((golden) =>
        thrownBy(() => translate(golden.id)),
      ),
    );
    const operators = raised.flatMap((error) =>
      error instanceof UnsupportedOperatorError ? [error.operator] : [],
    );
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

/**
 * Every leaf comparison an emitted `Where` makes, so the rules below can be stated over the whole
 * corpus rather than over hand-picked shapes.
 *
 * Unknown structure is a failure rather than something skipped: a rule that silently ignores a node
 * it does not recognise is a rule a new emission shape walks straight past.
 */
function literalsOf(where: Where | undefined, path = "filters"): Comparison[] {
  if (where === undefined) {
    return [];
  }
  return Object.entries(where).flatMap(([key, value]): Comparison[] => {
    if (key.startsWith("$")) {
      if (!Array.isArray(value)) {
        throw Error(`${path}.${key} is a logical operator over a non-array`);
      }
      return value.flatMap((child, index) =>
        literalsOf(child as Where, `${path}.${key}[${index}]`),
      );
    }
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
      throw Error(`${path}.${key} is not a Chroma comparison object`);
    }
    return Object.entries(value).map(([operator, operand]) => ({
      field: key,
      operator,
      value: operand,
    }));
  });
}

/**
 * Rules every emitted filter obeys, stated over every golden plan this adapter translates rather
 * than pinned per case. Each carries an anti-vacuity assertion, because every one of them is
 * satisfied by an empty filter.
 */
describe("what an emitted filter may contain", () => {
  type ActionComparison = Comparison & { action: string };
  let ALL_COMPARISONS: ActionComparison[] = [];
  let COMPARISONS_IF_PRESENT: ActionComparison[] = [];

  beforeAll(() => {
    ALL_COMPARISONS = CONDITIONAL.flatMap(({ id, filters }) =>
      literalsOf(filters).map((comparison) => ({ action: id, ...comparison })),
    );
    COMPARISONS_IF_PRESENT = CONDITIONAL_IF_PRESENT.flatMap(({ id, filters }) =>
      literalsOf(filters).map((comparison) => ({ action: id, ...comparison })),
    );
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
   * all: starting from `PRESENT_EVERYWHERE`, stripping it from the mapper must move exactly the
   * actions whose filter uses an inequality out of the translated set, and nothing else.
   *
   * A rule about a flag nothing consults passes for every corpus. This is the mutation that says
   * otherwise — it is the same argument convex's `nullable` pin makes, applied to the flag this
   * adapter gates `$ne`/`$nin` on.
   */
  test("clearing required moves exactly the actions that emit an inequality", async () => {
    const optionalEverywhere: Record<string, FieldNameMapperConfig> =
      Object.fromEntries(
        Object.entries(FIELD_NAME_MAPPER).map(([reference, entry]) => [
          reference,
          typeof entry === "string"
            ? { field: entry, required: false }
            : { ...entry, required: false },
        ]),
      );

    const ids = CONDITIONAL_IF_PRESENT.map(({ id }) => id);
    const raised = await Promise.all(
      ids.map((id) =>
        thrownBy(() => translate(id, { fieldNameMapper: optionalEverywhere })),
      ),
    );
    const nowRefused = ids.filter((_id, index) => raised[index]);
    const emitsInequality = [
      ...new Set(
        COMPARISONS_IF_PRESENT.filter(({ operator }) =>
          ["$ne", "$nin"].includes(operator),
        ).map(({ action }) => action),
      ),
    ];

    expect(nowRefused).toEqual(emitsInequality);
    expect(emitsInequality.length).toBeGreaterThan(0);
  });

  /**
   * The mutation that proves the type declarations are read, as the test above does for
   * `required`: stripping `valueType` and `numericType` from the corpus mapping refuses only
   * actions the corpus mapping translates, each at its `ne`, and at least one of them.
   */
  test("clearing the type declarations refuses inequalities, and nothing else changes", async () => {
    const untyped: Record<string, FieldNameMapperConfig> = Object.fromEntries(
      Object.entries(FIELD_NAME_MAPPER).map(([reference, entry]) => {
        if (typeof entry === "string") return [reference, { field: entry }];
        const { valueType: _v, numericType: _n, ...rest } = entry;
        return [reference, rest];
      }),
    );
    const translated = new Set(TRANSLATED.map(({ id }) => id));

    const goldens = readGoldens(CURRENT);
    const outcomes = await Promise.all(
      goldens.map(({ id }) =>
        thrownBy(() => translate(id, { fieldNameMapper: untyped })),
      ),
    );
    const changed = goldens.flatMap(({ id }, index) => {
      const raised = outcomes[index];
      return translated.has(id) === (raised === undefined)
        ? []
        : [{ id, operator: (raised as UnsupportedOperatorError)?.operator }];
    });

    expect(changed.length).toBeGreaterThan(0);
    expect(
      changed.filter(
        ({ id, operator }) => !translated.has(id) || operator !== "ne",
      ),
    ).toEqual([]);
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
  const RECORD_ACTION = "logic/or/at-root";

  test("a function mapper resolves the same references as a record mapper", async () => {
    const asFunction: FieldMapper = (reference) =>
      FIELD_NAME_MAPPER[reference] ?? reference;

    expect(
      await translate(RECORD_ACTION, { fieldNameMapper: asFunction }),
    ).toEqual(await translate(RECORD_ACTION));
  });

  /** An inequality over `aString`, which has no spelling without `$ne`. */
  const VF_NE = "comparison/not-equals/value-first";

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
    async (_label, mapper) => {
      await expect(
        translate(VF_NE, { fieldNameMapper: mapper }),
      ).rejects.toThrow(
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
  test("an unmapped reference becomes a metadata key spelled as the Cerbos path", async () => {
    expect(
      await translate("string/equals/case-sensitive", { fieldNameMapper: {} }),
    ).toEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { "request.resource.attr.aString": { $eq: "one" } },
    });
  });

  /**
   * The same shape the corpus refuses under the declared `integer` mapping translates once the
   * mapping says the metadata is floating point. Both directions matter: the refusal is the
   * adapter's, and it is a declaration the integrator can lift rather than a shape it cannot build.
   */
  test("numericType float admits the fractional threshold the integer declaration refuses", async () => {
    const action = "comparison/greater-or-equal/fractional-threshold";
    const asFloat = {
      ...FIELD_NAME_MAPPER,
      "request.resource.attr.aNumber": {
        field: "aNumber",
        numericType: "float" as const,
        required: true,
      },
    };

    await expect(translate(action)).rejects.toThrow(
      /cannot safely compare a fractional threshold/,
    );
    expect(await translate(action, { fieldNameMapper: asFloat })).toEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { aNumber: { $gte: 1.5 } },
    });
  });
});

/**
 * `allowPostFilter` is a caller-supplied argument the corpus cannot vary: the harness translates
 * every case with it on, so what the default does, and what the option changes about a result, is
 * pinned here. Which records a post-filter admits is the harness's job, never this file's.
 */
describe("allowPostFilter", () => {
  type Attempt = { result?: QueryPlanToChromaDBResult<boolean>; error?: unknown };

  const attempt = async (
    run: () =>
      | QueryPlanToChromaDBResult<boolean>
      | Promise<QueryPlanToChromaDBResult<boolean>>,
  ): Promise<Attempt> => {
    try {
      return { result: await run() };
    } catch (error) {
      return { error };
    }
  };

  const allowing = async (
    id: string,
    fieldNameMapper: FieldMapper = FIELD_NAME_MAPPER,
  ): Promise<QueryPlanToChromaDBResult<boolean>> =>
    queryPlanToChromaDB({
      queryPlan: await planOf(readGolden(CURRENT, id)),
      fieldNameMapper,
      allowPostFilter: true,
    });

  interface Outcome {
    golden: Golden;
    omitted: Attempt;
    off: Attempt;
    on: Attempt;
  }

  let OUTCOMES: Outcome[] = [];
  let POST_FILTERED: Outcome[] = [];

  beforeAll(async () => {
    OUTCOMES = await Promise.all(
      readGoldens(CURRENT).map(async (golden) => {
        const queryPlan = await planOf(golden);
        return {
          golden,
          omitted: await attempt(() =>
            queryPlanToChromaDB({
              queryPlan,
              fieldNameMapper: FIELD_NAME_MAPPER,
            }),
          ),
          off: await attempt(() =>
            queryPlanToChromaDB({
              queryPlan,
              fieldNameMapper: FIELD_NAME_MAPPER,
              allowPostFilter: false,
            }),
          ),
          on: await attempt(() => allowing(golden.id)),
        };
      }),
    );
    POST_FILTERED = OUTCOMES.filter(({ on }) => on.result?.postFilter);
  });

  const describeError = (error: unknown): string =>
    error instanceof UnsupportedOperatorError
      ? `UnsupportedOperatorError(${error.operator}): ${error.message}`
      : String(error);

  test("left off, every plan that needs a post-filter still throws UnsupportedOperatorError", () => {
    expect(POST_FILTERED.length).toBeGreaterThan(0);
    for (const { omitted, off } of POST_FILTERED) {
      expect(omitted.error).toBeInstanceOf(UnsupportedOperatorError);
      expect(off.error).toBeInstanceOf(UnsupportedOperatorError);
    }
  });

  test("left off, no result carries a postFilter, and false is the same as omitting it", () => {
    for (const { omitted, off } of OUTCOMES) {
      expect(omitted.result?.postFilter).toBeUndefined();
      expect(off.result).toEqual(omitted.result);
      expect(describeError(off.error)).toBe(describeError(omitted.error));
    }
  });

  test("turned on, a plan Chroma can express whole is answered by filters alone, as without it", () => {
    const translated = OUTCOMES.filter(({ omitted }) => omitted.result);
    expect(translated.length).toBeGreaterThan(0);
    for (const { omitted, on } of translated) {
      expect(on.result).toEqual(omitted.result);
    }
  });

  // The predicate is compiled before it is returned, so every refusal happens at translation and
  // the predicate itself only ever answers. A caller applies it inside a `.filter()`; a throw there
  // would fail the whole search on one odd record.
  test("the predicate answers a boolean for any metadata a record can carry, and never throws", () => {
    const keys = [
      ...new Set(
        Object.values(FIELD_NAME_MAPPER).map((entry) =>
          typeof entry === "string" ? entry : entry.field,
        ),
      ),
    ];
    const shapes: (Record<string, unknown> | null | undefined)[] = [
      null,
      undefined,
      {},
      Object.fromEntries(keys.map((key) => [key, ["a", "list"]])),
      Object.fromEntries(keys.map((key) => [key, 0])),
      Object.fromEntries(keys.map((key) => [key, ""])),
      Object.fromEntries(keys.map((key) => [key, true])),
      Object.fromEntries(keys.map((key) => [key, { sparse: [1] }])),
    ];
    for (const { on } of POST_FILTERED) {
      for (const metadata of shapes) {
        expect(typeof on.result!.postFilter!(metadata)).toBe("boolean");
      }
    }
  });

  // The harness stores no list (#475), so what a stored list does is caller data no case can vary.
  // It reads as a missing attribute, which CEL denies under a negation as well: a list is never
  // compared as though it were whatever the PDP was sent.
  test("a key holding a list reads as missing, which denies under negation too", async () => {
    const { postFilter } = await allowing(
      "string/starts-with/negated-field-to-field",
    );
    expect(postFilter?.({ aString: "one", aOptionalString: "x" })).toBe(true);
    expect(postFilter?.({ aString: ["one"], aOptionalString: "x" })).toBe(false);
    expect(postFilter?.({ aOptionalString: "x" })).toBe(false);
  });

  // The pushdown's fallback — an unmapped path used verbatim as a key — is not the post-filter's:
  // a key no record carries is a missing attribute, which would deny records the PDP allows rather
  // than refuse the shape.
  test.each([
    [
      "a record mapper without the entry",
      Object.fromEntries(
        Object.entries(FIELD_NAME_MAPPER).filter(
          ([reference]) => reference !== "request.resource.attr.parent.aString",
        ),
      ),
    ],
    [
      "a function mapper that returns nothing for it",
      ((reference: string) =>
        reference === "request.resource.attr.parent.aString"
          ? undefined
          : FIELD_NAME_MAPPER[reference]) as FieldMapper,
    ],
  ])("the post-filter refuses a reference the mapping does not declare: %s", async (_label, mapper) => {
    const id = "relation/contains/one-hop";
    expect((await allowing(id)).postFilter).toBeDefined();
    const raised = await thrownBy(() => allowing(id, mapper));
    expect(raised).toBeInstanceOf(UnsupportedOperatorError);
    expect((raised as UnsupportedOperatorError).operator).toBe("contains");
  });

  // A stored -0.0 reads back as 0 (the client writes metadata as JSON), so `string()` over a stored
  // number is refused. A key declared boolean never holds a number, which is what lets the corpus
  // mapping's `valueType: "boolean"` admit it.
  test("valueType boolean is what lets string() read a boolean key", async () => {
    const id = "cast/string/from-boolean";
    expect((await allowing(id)).postFilter).toBeDefined();
    const undeclared = {
      ...FIELD_NAME_MAPPER,
      "request.resource.attr.aBool": "aBool",
    };
    const raised = await thrownBy(() => allowing(id, undeclared));
    expect(raised).toBeInstanceOf(UnsupportedOperatorError);
    expect((raised as UnsupportedOperatorError).operator).toBe("string");
  });
});

describe("type declarations a mapper cannot combine", () => {
  // A mapper misconfiguration, not a shape Chroma cannot hold, so a plain `Error` (#228).
  test.each([
    [
      "both valueType and numericType",
      { field: "aBool", valueType: "boolean", numericType: "integer" },
    ],
    ["an unknown valueType", { field: "aBool", valueType: "string" }],
  ])("%s is a plain Error", async (_label, entry) => {
    const raised = await thrownBy(() =>
      translate("logic/not/bare-boolean-attribute", {
        fieldNameMapper: {
          ...FIELD_NAME_MAPPER,
          "request.resource.attr.aBool": entry as FieldNameMapperConfig,
        },
      }),
    );
    expect(raised).toBeInstanceOf(Error);
    expect(raised).not.toBeInstanceOf(UnsupportedOperatorError);
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
  const expectPlainError = async (run: () => unknown): Promise<void> => {
    const raised = await thrownBy(run);
    expect(raised).toBeInstanceOf(Error);
    expect(raised).not.toBeInstanceOf(UnsupportedOperatorError);
  };

  test("an unrecognised plan kind", async () => {
    const run = () =>
      queryPlanToChromaDB({
        queryPlan: { kind: "INVALID_KIND" } as unknown as PlanResourcesResponse,
        fieldNameMapper: FIELD_NAME_MAPPER,
      });
    expect(run).toThrow("Invalid query plan.");
    await expectPlainError(run);
  });

  // The same message as the corpus's ternary, which is typed: there the operator (`if`) is one
  // this adapter maps to no comparison at all, here it is `eq` short of an operand.
  test("a comparison with the wrong number of operands", async () => {
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
    await expectPlainError(run);
  });

  // Typed, unlike its neighbours: two literals is not a structural defect in the plan but a shape
  // the `Where` grammar cannot hold, since it compares a metadata key to a literal.
  test("a comparison between two literals", async () => {
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
    const raised = await thrownBy(run);
    expect(raised).toBeInstanceOf(UnsupportedOperatorError);
    expect((raised as UnsupportedOperatorError).operator).toBe("eq");
  });

  test("a condition that is not an expression at all", async () => {
    const run = () =>
      queryPlanToChromaDB({
        queryPlan: plan(new PlanExpressionValue(true)),
        fieldNameMapper: FIELD_NAME_MAPPER,
      });
    expect(run).toThrow("Query plan did not contain an expression for operand");
    await expectPlainError(run);
  });
});
