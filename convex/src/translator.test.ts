import { describe, expect, test } from "@jest/globals";
import type {
  PlanExpressionOperand,
  PlanResourcesResponse,
} from "@cerbos/core";

// The mapper the Convex backend translates every conformance case with.
import { MAPPER } from "../convex/adversarialMapper";
import { PlanKind, queryPlanToConvex, UnsupportedQueryPlanError } from ".";
import type {
  Mapper,
  MapperConfig,
  NullAttributeRepresentation,
  QueryPlanToConvexResult,
} from ".";
import {
  pdpTags,
  planOf,
  readCorpusJson,
  readGolden,
  readGoldens,
} from "./corpus";

/**
 * Offline unit tests: what a caller can pass that the conformance corpus cannot vary (mapper forms,
 * `allowPostFilter`, `nullAttributeRepresentation`), the refusal type, invariants of the filter
 * handed to Convex's engine, and plans the planner cannot produce.
 *
 * Which documents a corpus case returns is the conformance harness's job (`adversarial.test.ts`);
 * nothing here pins the filter emitted for a corpus case. Plans come from the current PDP's golden
 * files, read by case id, rather than being typed by hand.
 */

const CURRENT = pdpTags()[0]!;
const GOLDENS = readGoldens(CURRENT).filter(
  (golden) => !golden.plannerDivergence,
);
const golden = (id: string) => readGolden(CURRENT, id);

// -- recording what the adapter asks Convex to do -------------------------------------------------

/**
 * A `FilterBuilder` that records rather than evaluates: the adapter's filter is a function of the
 * builder Convex hands it, so a recorder is the only way to see what it emits without a Convex
 * deployment. Membership is tracked in a `WeakSet`, so a plan literal can never be mistaken for a
 * recorded call.
 */
interface FilterNode {
  op: string;
  args: unknown[];
}

const RECORDED_NODES = new WeakSet<object>();

const record = (op: string, args: unknown[]): FilterNode => {
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

interface TranslateOptions {
  mapper?: Mapper;
  /** Defaults to `true`: most of the corpus is post-filtered. */
  allowPostFilter?: boolean;
  nullAttributeRepresentation?: NullAttributeRepresentation;
}

function translate(
  id: string,
  options: TranslateOptions = {},
): QueryPlanToConvexResult<Recorder, unknown> {
  return queryPlanToConvex<Recorder, unknown>({
    queryPlan: planOf(golden(id)),
    mapper: options.mapper ?? MAPPER,
    allowPostFilter: options.allowPostFilter ?? true,
    ...(options.nullAttributeRepresentation
      ? { nullAttributeRepresentation: options.nullAttributeRepresentation }
      : {}),
  });
}

/** Translates a case, or reports that the adapter refused it. */
function translated(
  id: string,
  options: TranslateOptions = {},
): QueryPlanToConvexResult<Recorder, unknown> | "refused" {
  try {
    return translate(id, options);
  } catch (error) {
    if (error instanceof UnsupportedQueryPlanError) return "refused";
    throw error;
  }
}

/** The calls the emitted filter makes against Convex's builder. */
function recordFilter(label: string, filter: (q: Recorder) => unknown) {
  const emitted = filter(RECORDER);
  if (!isRecordedNode(emitted)) {
    throw new Error(
      `${label} returned something other than a builder call: ${JSON.stringify(emitted)}`,
    );
  }
  return emitted;
}

/** Each translatable golden, with the part Convex's own engine is handed, if any. */
const TRANSLATED = GOLDENS.flatMap((g) => {
  const result = translated(g.id);
  return result === "refused" ? [] : [{ id: g.id, result }];
});
const PUSHED = TRANSLATED.filter(({ result }) => result.filter !== undefined);

// The documents as the PDP saw them (conformance/resources.json), which is also exactly what the
// conformance harness stores — so a post-filter can be run against them offline.
const DOCUMENTS = (
  readCorpusJson("resources.json") as {
    resources: { id: string; attr: Record<string, unknown> }[];
  }
).resources.map(({ id, attr }) => ({ id, ...attr }));

describe("the refusal type", () => {
  // Every shape this adapter cannot express raises `UnsupportedQueryPlanError`, which the
  // conformance harness asserts for every `unsupported` ledger entry. These pin the boundary on the
  // other side: it is still an `Error`, and a mapper mistake or a missing opt-in is NOT a refusal.
  test("a refused shape raises UnsupportedQueryPlanError, which is an Error", () => {
    let thrown: unknown;
    try {
      translate("regex/matches/lookahead-from-principal");
    } catch (error) {
      thrown = error;
    }
    expect(thrown).toBeInstanceOf(UnsupportedQueryPlanError);
    expect(thrown).toBeInstanceOf(Error);
    expect((thrown as Error).name).toBe("UnsupportedQueryPlanError");
  });

  test("an unmapped reference is a plain Error, not a refusal", () => {
    let thrown: unknown;
    try {
      queryPlanToConvex({
        queryPlan: planOf(golden("string/equals/case-sensitive")),
      });
    } catch (error) {
      thrown = error;
    }
    expect(thrown).toBeInstanceOf(Error);
    expect(thrown).not.toBeInstanceOf(UnsupportedQueryPlanError);
  });

  test("a missing allowPostFilter opt-in is a plain Error, not a refusal", () => {
    let thrown: unknown;
    try {
      translate("regex/matches/anchored-prefix", { allowPostFilter: false });
    } catch (error) {
      thrown = error;
    }
    expect(thrown).toBeInstanceOf(Error);
    expect(thrown).not.toBeInstanceOf(UnsupportedQueryPlanError);
  });
});

// -- rules over every filter handed to Convex's engine ---------------------------------------------
//
// Rules rather than pinned filters: each holds for a corpus case nobody has added yet.

describe("what the adapter asks Convex to do", () => {
  const fieldsNamedBy = (node: FilterNode): string[] =>
    node.op === "field"
      ? [String(node.args[0])]
      : node.args.flatMap((arg) =>
          isRecordedNode(arg) ? fieldsNamedBy(arg) : [],
        );

  const configs = Object.values(MAPPER);
  const fieldsWhere = (keep: (config: MapperConfig) => boolean) =>
    new Set(
      configs
        .filter(keep)
        .map((config) => config.field)
        .filter((field): field is string => field !== undefined),
    );
  const declaredFields = fieldsWhere(() => true);
  const nullableFields = fieldsWhere((config) => config.nullable === true);

  // Anti-vacuity: the rules below say nothing if no case reaches the engine.
  test("some cases reach Convex's filter engine", () => {
    expect(PUSHED.length).toBeGreaterThan(0);
  });

  test.each(PUSHED.map(({ id, result }) => [id, result] as const))(
    "%s names only mapped, non-nullable fields",
    (id, result) => {
      // A `request.resource.attr.…` name would mean resolution missed a reference the caller DID
      // map — a path no document stores, which a negation reads as a match on every document.
      // A nullable field is CEL's missing-attribute case, which Convex's engine cannot tell from
      // a false one, so it has to stay with the post-filter (#375).
      const fields = fieldsNamedBy(recordFilter(id, result.filter!));
      expect({
        undeclared: fields.filter((field) => !declaredFields.has(field)),
        nullable: fields.filter((field) => nullableFields.has(field)),
      }).toEqual({ undeclared: [], nullable: [] });
    },
  );
});

/**
 * `allowPostFilter` is a call-level opt-in, so no policy can reach it — but every corpus case
 * reaches one side of it. A post-filter is a promise the CALLER has to keep: unless it is applied
 * to every candidate before it is serialised, the untranslatable half of the policy does not run.
 */
describe("the allowPostFilter gate", () => {
  test.each(TRANSLATED.map(({ id, result }) => [id, result] as const))(
    "%s needs the opt-in exactly when it carries a post-filter",
    (id, result) => {
      if (result.postFilter !== undefined) {
        expect(() => translate(id, { allowPostFilter: false })).toThrow(
          "allowPostFilter",
        );
      } else {
        expect(translate(id, { allowPostFilter: false }).kind).toBe(
          result.kind,
        );
      }
    },
  );
});

// -- the mapper contract, which no policy can reach ------------------------------------------------

describe("mapper forms", () => {
  // A deep relation shape, so the equivalence covers references resolved through several hops.
  const DEEP_CASE = "collection/exists/nested-three-levels";

  test("a function mapper resolves the same references as a record mapper", () => {
    const asFunction: Mapper = (reference) => MAPPER[reference] ?? {};
    const decisions = (mapper: Mapper) => {
      const { postFilter } = translate(DEEP_CASE, { mapper });
      if (!postFilter) throw new Error(`${DEEP_CASE} emitted no postFilter`);
      return DOCUMENTS.map((doc) => postFilter(doc));
    };
    const byRecord = decisions(MAPPER);
    expect(byRecord).toContain(true);
    expect(decisions(asFunction)).toEqual(byRecord);
  });

  /**
   * A reference with no entry is refused rather than read verbatim as a document path. Read
   * verbatim, the path is absent from every document, Convex reads an absent field as `undefined`,
   * and `undefined != "x"` is true, so `R.attr.status != "x"` matched every document
   * (cerbos/query-plan-adapters#492).
   */
  const without = (reference: string): Mapper =>
    Object.fromEntries(
      Object.entries(MAPPER).filter(([key]) => key !== reference),
    );

  test.each([
    // pushed down, pushed down under `not`, and answered by the post-filter: the refusal is made
    // over the plan, before either half of the output exists.
    ["string/equals/case-sensitive", "request.resource.attr.aString"],
    ["logic/not/greater-than", "request.resource.attr.aNumber"],
    [
      "null/not-equals/missing-attribute-against-literal",
      "request.resource.attr.aOptionalString",
    ],
  ])("%s is refused when %s has no entry", (id, reference) => {
    expect(() => translate(id, { mapper: without(reference) })).toThrow(
      `No mapper entry for ${reference}: an unmapped reference is not used verbatim`,
    );
  });

  test("an unmapped reference is refused when a function mapper returns no entry", () => {
    const mapper = ((reference: string) =>
      reference === "request.resource.attr.aNumber"
        ? undefined
        : MAPPER[reference]) as Mapper;
    expect(() => translate("logic/not/greater-than", { mapper })).toThrow(
      "No mapper entry for request.resource.attr.aNumber",
    );
  });

  // A lambda's own variable is bound by the macro, not by the mapper, and must not be refused.
  test("a lambda variable needs no entry", () => {
    expect(() => translate(DEEP_CASE)).not.toThrow();
  });

  // The opt-in: an entry that names no `field` keeps the plan path.
  test("an empty entry keeps the plan path verbatim", () => {
    const { filter } = translate("string/equals/case-sensitive", {
      mapper: { "request.resource.attr.aString": {} },
    });
    if (!filter)
      throw new Error("emitted no filter under an empty-entry mapper");
    expect(recordFilter("empty entry", filter)).toEqual({
      op: "eq",
      args: [{ op: "field", args: ["request.resource.attr.aString"] }, "one"],
    });
  });
});

describe("nullAttributeRepresentation", () => {
  // The corpus expresses the omitted convention through the mapping (`nullable: true`) and the
  // document shape, and the harness runs every case under the default. The option is for a caller
  // whose documents store a NULL field as an explicit null while omitting the attribute from
  // check(): the plan cannot reveal that, so the adapter has to be told.
  const MISSING = "null/equals/null-literal-on-missing-attribute";

  test("explicit is the default", () => {
    const explicit = translate(MISSING, {
      nullAttributeRepresentation: "explicit",
    });
    const byDefault = translate(MISSING);
    expect(DOCUMENTS.map((doc) => explicit.postFilter!(doc))).toEqual(
      DOCUMENTS.map((doc) => byDefault.postFilter!(doc)),
    );
  });

  test("omitted: the same plan is refused rather than translated", () => {
    expect(() =>
      translate(MISSING, { nullAttributeRepresentation: "omitted" }),
    ).toThrow(UnsupportedQueryPlanError);
  });

  const carriesNullLiteral = (node: unknown): boolean => {
    if (Array.isArray(node)) return node.some(carriesNullLiteral);
    if (typeof node !== "object" || node === null) return false;
    if ("value" in node) {
      const { value } = node as { value: unknown };
      return value === null || (Array.isArray(value) && value.includes(null));
    }
    return Object.values(node).some(carriesNullLiteral);
  };

  /**
   * The rejection is deliberately wider than the shapes that over-grant — negation is applied
   * around the built predicate, so a leaf cannot tell whether an enclosing `not` will flip it —
   * but it keys off the null OPERAND, never off an operator list, and a null-free plan must still
   * translate, or the option would be a way to turn the adapter off.
   */
  test("omitted rejects exactly the plans that carry a null literal", () => {
    const rejected = TRANSLATED.filter(
      ({ id }) =>
        translated(id, { nullAttributeRepresentation: "omitted" }) ===
        "refused",
    ).map(({ id }) => id);
    const carrying = TRANSLATED.filter(({ id }) =>
      carriesNullLiteral(golden(id).plan.condition ?? null),
    ).map(({ id }) => id);

    expect(rejected).toEqual(carrying);
    expect(rejected).toContain(MISSING);
    expect(rejected).toContain(
      "null/has-intersection/literal-list-with-null-element",
    );
    expect(rejected.length).toBeLessThan(TRANSLATED.length);
  });
});

/**
 * Under "omitted" a NULL field sends no attribute, so CEL denies every comparison against it, while
 * the pushed-down `q.neq(...)` and a negated comparison match a document the path is absent from.
 * An entry that declares no `nullable` therefore takes the call-level convention as its default,
 * exactly as if it declared `nullable: true`, and the post-filter reads a stored null as missing
 * (#493). A caller-supplied argument no corpus case can vary: the harness runs one mapping, under
 * "explicit".
 */
describe("omitted: an undeclared entry is nullable", () => {
  const omitted = (id: string, options: TranslateOptions = {}) =>
    translate(id, { ...options, nullAttributeRepresentation: "omitted" });

  // The corpus mapping declares `aString` and `aNumber` nullable, so each case strips the
  // declaration from the field it reads, leaving the entry for the call-level default to decide.
  const undeclared = (field: string): Mapper =>
    Object.fromEntries(
      Object.entries(MAPPER).map(([key, config]) => {
        if (key !== `request.resource.attr.${field}`) return [key, config];
        const { nullable: _nullable, ...rest } = config;
        return [key, rest];
      }),
    );

  // `ne`, a negated ordering, and a negated `in` over an undeclared entry, each of which Convex's
  // engine answers as a match on an absent path.
  const CASES = [
    ["string/equals/case-sensitive", "aString", "one", "two"],
    ["comparison/not-equals/value-first", "aString", "two", "one"],
    ["logic/not/greater-than", "aNumber", 0, 2],
    ["null/in/negated-explicit-null-in-literal-list", "owner", "y", "x"],
  ] as const;

  test.each(CASES)(
    "%s stays off Convex's engine, and needs the post-filter opt-in",
    (id, field, _allowed, _denied) => {
      const mapper = undeclared(field);
      expect(translate(id, { mapper }).path).toBe("db");
      expect(omitted(id, { mapper }).path).toBe("post");
      expect(() => omitted(id, { mapper, allowPostFilter: false })).toThrow(
        "allowPostFilter",
      );
    },
  );

  test.each(CASES)(
    "%s denies a document %s is missing or null in",
    (id, field, allowed, denied) => {
      const { postFilter } = omitted(id, { mapper: undeclared(field) });
      expect(postFilter!({ [field]: allowed })).toBe(true);
      expect(postFilter!({ [field]: denied })).toBe(false);
      expect(postFilter!({})).toBe(false);
      expect(postFilter!({ [field]: null })).toBe(false);
    },
  );

  test("a function mapper takes the default too", () => {
    const mapper = undeclared("aString") as Record<string, MapperConfig>;
    const asFunction: Mapper = (reference) => mapper[reference]!;
    expect(
      omitted("string/equals/case-sensitive", { mapper: asFunction }).path,
    ).toBe("post");
  });

  // `nullable: false` is the per-entry opt-out: it asserts the field is always stored and never
  // null. With every entry declaring its own `nullable`, nothing is left for the default to decide,
  // so every null-free plan asks Convex's engine for exactly what it does under "explicit".
  test("declaring nullable: false keeps the explicit translation", () => {
    const declared: Mapper = Object.fromEntries(
      Object.entries(MAPPER).map(([key, config]) => [
        key,
        { ...config, nullable: config.nullable ?? false },
      ]),
    );
    const pushedDown = (result: QueryPlanToConvexResult<Recorder, unknown>) =>
      result.filter ? recordFilter("filter", result.filter) : undefined;
    const differing = TRANSLATED.filter(
      ({ id }) => !/\bnull\b/.test(JSON.stringify(golden(id).plan)),
    )
      .filter(({ id, result }) => {
        const declaredResult = omitted(id, { mapper: declared });
        return (
          declaredResult.path !== result.path ||
          JSON.stringify(pushedDown(declaredResult)) !==
            JSON.stringify(pushedDown(result))
        );
      })
      .map(({ id }) => id);
    expect(differing).toEqual([]);
  });
});

// -- hand-built plans ------------------------------------------------------------------------------

/** A conditional plan around a hand-built condition, for the two sections below that need one. */
const plan = (condition: unknown): PlanResourcesResponse =>
  ({
    kind: PlanKind.CONDITIONAL,
    condition: condition as PlanExpressionOperand,
    cerbosCallId: "",
    requestId: "",
    validationErrors: [],
    metadata: undefined,
  }) as PlanResourcesResponse;

// -- plans the planner cannot produce --------------------------------------------------------------

describe("plans the planner cannot produce", () => {
  // Input validation on a public function, not policy shapes. Every other assertion in this file
  // reads its plan from a golden file precisely because a typed plan is a belief about the planner —
  // but these are malformed by construction, so there is no golden file to read and nothing to
  // believe. They exist so a caller who hands the adapter a hand-rolled or half-decoded plan gets
  // an error rather than a filter.
  //
  // A shape CEL *can* express does not belong here, whatever its plan looks like: it belongs in
  // the corpus, where every adapter is asked about it.

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
 * A bridge, not a home. Everything below asserts a translator branch that no corpus case drives
 * today, which is exactly the situation `CLAUDE.md` says a per-adapter unit test must not be
 * allowed to settle into: a unit test pins the filter one adapter emits, and only a corpus action
 * asks the same question of every other adapter.
 *
 * The remaining gaps are tracked below; delete these tests when their corpus coverage lands:
 *
 * - backreferences and trailing-wildcard/end-anchor combinations — #396.
 * - the value-list macro machinery past what `principal/*` drives — cerbos/query-plan-adapters#394.
 *   The corpus drives `exists`, `all` and `exists_one` over a list of distinct strings; a
 *   duplicate element under `exists_one`, an empty collection that is not folded away, and
 *   element-field paths it does not.
 *
 * The plans here are hand-built for the same reason the sections above never are: there is no
 * golden file, because there is no case. That is the argument for the issue rather than a
 * licence to keep writing them.
 */
describe("shapes the corpus does not reach yet", () => {
  // Corpus gap (#396): `regex/matches/lookahead-from-principal` covers lookahead rejection, but
  // these backreference and trailing-wildcard/end-anchor combinations still have no corpus case.
  // Keep their refusal contract until those exact shapes are planned and replayed.
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
        macroPostFilter(
          "exists",
          [],
          compare("eq", { name: "t" }),
        )({
          aString: "alpha",
        }),
      ).toBe(false);
      expect(
        macroPostFilter(
          "all",
          [],
          compare("ne", { name: "t" }),
        )({
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

// Type-only API contracts: these assignments stop compiling if an impossible result returns.
test("result types require the payload declared by each execution path", () => {
  type Result = QueryPlanToConvexResult<Recorder, unknown>;
  type Rejects<T> = T extends Result ? false : true;
  const bareConditional: Rejects<{ kind: PlanKind.CONDITIONAL }> = true;
  const missingDbFilter: Rejects<{ kind: PlanKind.CONDITIONAL; path: "db" }> =
    true;
  const missingPostFilter: Rejects<{
    kind: PlanKind.CONDITIONAL;
    path: "post";
  }> = true;
  const missingSplitPostFilter: Rejects<{
    kind: PlanKind.CONDITIONAL;
    path: "split";
    filter: (q: Recorder) => unknown;
  }> = true;
  const unconditionalFilter: Rejects<{
    kind: PlanKind.ALWAYS_ALLOWED;
    filter: (q: Recorder) => unknown;
  }> = true;
  expect([
    bareConditional,
    missingDbFilter,
    missingPostFilter,
    missingSplitPostFilter,
    unconditionalFilter,
  ]).toEqual([true, true, true, true, true]);
});
