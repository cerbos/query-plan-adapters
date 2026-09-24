import { describe, expect, test } from "@jest/globals";
import type {
  PlanExpressionOperand,
  PlanResourcesResponse,
} from "@cerbos/core";

import { queryPlanToPrisma, PlanKind, UnsupportedQueryPlanError } from ".";
import type {
  Mapper,
  MapperConfig,
  NullAttributeRepresentation,
  PrismaFilter,
  QueryPlanToPrismaResult,
} from ".";
import { MAPPER, MODEL, pdpTags, planOf, readGolden, readGoldens } from "./corpus";

/**
 * Offline unit tests: what a caller can pass that the conformance corpus cannot vary (mapper forms,
 * `subqueryFilter`, element nullability, `nullAttributeRepresentation`), the refusal type, the
 * timestamp literal contract, and plans the planner cannot produce.
 *
 * Which rows a corpus case returns is the conformance harness's job (`adversarial.test.ts`); nothing
 * here pins the filter emitted for a corpus case. Plans come from the current PDP's golden files,
 * read by case id, rather than being typed by hand.
 */

const CURRENT = pdpTags()[0]!;

/** The current PDP's recorded plan for a corpus case, `__NOW_MINUS_24H__` filled as `now`. */
function planFor(id: string, now?: string): PlanResourcesResponse {
  return planOf(readGolden(CURRENT, id), now);
}

function translate(
  id: string,
  options: {
    mapper?: Mapper;
    nullAttributeRepresentation?: NullAttributeRepresentation;
  } = {}
): QueryPlanToPrismaResult {
  return queryPlanToPrisma({
    queryPlan: planFor(id),
    mapper: options.mapper ?? MAPPER,
    model: MODEL,
    ...(options.nullAttributeRepresentation
      ? { nullAttributeRepresentation: options.nullAttributeRepresentation }
      : {}),
  });
}

describe("the refusal type", () => {
  test("an untranslatable shape raises UnsupportedQueryPlanError, which is an Error", () => {
    let raised: unknown;
    try {
      translate("collection/index/first-element-of-string-list");
    } catch (error) {
      raised = error;
    }
    expect(raised).toBeInstanceOf(UnsupportedQueryPlanError);
    expect(raised).toBeInstanceOf(Error);
    expect((raised as Error).name).toBe("UnsupportedQueryPlanError");
  });

  test("mapper misconfiguration is a plain Error, not a refusal", () => {
    let raised: unknown;
    try {
      queryPlanToPrisma({ queryPlan: planFor("comparison/equals/field-to-field"), mapper: MAPPER });
    } catch (error) {
      raised = error;
    }
    expect(raised).toBeInstanceOf(Error);
    expect(raised).not.toBeInstanceOf(UnsupportedQueryPlanError);
    expect((raised as Error).message).toContain("requires the `model` option");
  });
});

describe("declared scalar types", () => {
  test("an undeclared type preserves the legacy mapper contract", () => {
    expect(
      translate("type-mismatch/equals/string-field-against-number-principal", {
        mapper: { "request.resource.attr.aString": { field: "aString" } },
      }),
    ).toEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { aString: { equals: 0 } },
    });
    // Declared, the type settles the comparison: CEL's heterogeneous equality answers a string
    // column against a number false for every row, so nothing is bound for a store to coerce.
    expect(translate("type-mismatch/equals/string-field-against-number-principal")).toEqual({
      kind: PlanKind.ALWAYS_DENIED,
    });
  });
});

/** The shared mapper with every per-attribute null declaration stripped. */
const UNDECLARED: Record<string, MapperConfig> = Object.fromEntries(
  Object.entries(MAPPER).map(([reference, config]) => {
    const { nullAttributeRepresentation: _stripped, ...rest } = config;
    return [reference, rest];
  })
);

/** Whether any operand anywhere in the wire plan is a literal null, or a list containing one. */
function carriesNullLiteral(node: unknown): boolean {
  if (typeof node !== "object" || node === null) return false;
  const record = node as Record<string, unknown>;
  if ("value" in record) {
    const value = record["value"];
    return value === null || (Array.isArray(value) && value.includes(null));
  }
  return Object.values(record).some((child) =>
    Array.isArray(child) ? child.some(carriesNullLiteral) : carriesNullLiteral(child)
  );
}

describe("nullAttributeRepresentation", () => {
  // `== null` against an attribute the caller OMITS when the column is NULL. The two conventions
  // are indistinguishable on the wire — the planner emits the same `eq(attr, null)` either way — so
  // the adapter has to be told. The corpus mapping declares it per attribute; these vary the
  // call-level option and the declaration, which the corpus cannot.
  const MISSING = "null/equals/null-literal-on-missing-attribute";

  test("explicit (the default): == null is an IS NULL filter", () => {
    expect(translate(MISSING, { mapper: UNDECLARED })).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { aOptionalString: { equals: null } },
    });
  });

  test("omitted: the same plan denies every row rather than selecting the NULL ones", () => {
    // A NULL column sends no attribute, so check() denies on a missing-attribute error while the
    // IS NULL filter would return exactly those rows (#302); a present value is never null.
    expect(
      translate(MISSING, { mapper: UNDECLARED, nullAttributeRepresentation: "omitted" })
    ).toEqual({ kind: PlanKind.ALWAYS_DENIED });
  });

  test("a per-attribute declaration overrides the call-level option, in both directions", () => {
    // Declared omitted, called explicit: no IS NULL filter (#308).
    expect(translate(MISSING)).toEqual({ kind: PlanKind.ALWAYS_DENIED });
    // Declared explicit (`owner`), called omitted: translated.
    expect(
      translate("null/equals/null-literal", { nullAttributeRepresentation: "omitted" }).kind
    ).toBe(PlanKind.CONDITIONAL);
    // Strip the declaration and the same call is denied outright.
    expect(
      translate("null/equals/null-literal", {
        mapper: UNDECLARED,
        nullAttributeRepresentation: "omitted",
      })
    ).toEqual({ kind: PlanKind.ALWAYS_DENIED });
  });

  // The rejection must key off the null OPERAND, not a list of operators: `hasIntersection(tagNames,
  // ["public", null])` carries one in its value list. Enumerating the goldens rather than naming
  // shapes covers a newly added case carrying a null constant automatically.
  // A plan whose null comparison settles (`x == null` is false or an error, so every row is
  // denied) passes too: ALWAYS_DENIED selects no NULL row.
  test("no plan carrying a null literal selects NULL rows under call-level omitted", () => {
    const carrying = readGoldens(CURRENT).filter((golden) => carriesNullLiteral(golden.plan));
    const ids = carrying.map((golden) => golden.id);
    expect(ids).toContain(MISSING);
    expect(ids).toContain("null/has-intersection/literal-list-with-null-element");

    const notRejected = carrying.flatMap((golden) => {
      try {
        const result = queryPlanToPrisma({
          queryPlan: planOf(golden),
          mapper: UNDECLARED,
          model: MODEL,
          nullAttributeRepresentation: "omitted",
        });
        return result.kind === PlanKind.ALWAYS_DENIED ? [] : [golden.id];
      } catch (error) {
        // A positional read of a list compares an element, not an optionally absent field; the
        // index operator has no Prisma filter form under either representation.
        const message = String(error);
        return message.includes("missing-attribute error") ||
          message.includes("Unsupported operator: index")
          ? []
          : [`${golden.id} (${message})`];
      }
    });
    expect(notRejected).toEqual([]);
  });
});

describe("reentrant function mappers", () => {
  // Function mappers are caller-supplied, so the corpus cannot exercise nested adapter calls. The
  // nested call's "omitted" must not leak into the outer "explicit" translation of `== null`.
  test.each([
    "null/equals/null-literal-on-missing-attribute",
    "comparison/equals/field-to-field",
    "collection/all/empty-collection",
  ])("keeps %s isolated from a nested translation", (action) => {
    const expected = translate(action, { mapper: UNDECLARED });
    let calls = 0;
    const mapper: Mapper = (key) => {
      calls++;
      queryPlanToPrisma({
        queryPlan: planFor("arithmetic/add/field-plus-constant"),
        mapper: MAPPER,
        model: "NestedModel",
        nullAttributeRepresentation: "omitted",
      });
      return UNDECLARED[key] ?? { field: key };
    };
    expect(translate(action, { mapper })).toStrictEqual(expected);
    expect(calls).toBeGreaterThan(0);
  });
});

test("an unplannable nested map does not register nullable fields on the outer lambda", () => {
  // CEL cannot reach this branch: Cerbos 0.54.0 rejects
  // R.attr.tags.all(t, R.attr.tags.map(x, x.name)) with
  // "expected type 'bool' but found 'list(dyn)'". This is a hand-crafted plan contract. A list
  // where a boolean is required is a CEL error, so the body settles to false (all() holds only
  // over no tags) before any lambda scope is entered.
  const condition: PlanExpressionOperand = {
    operator: "all",
    operands: [
      { name: "request.resource.attr.tags" },
      {
        operator: "lambda",
        operands: [
          {
            operator: "map",
            operands: [
              { name: "request.resource.attr.tags" },
              {
                operator: "lambda",
                operands: [{ name: "x.name" }, { name: "x" }],
              },
            ],
          },
          { name: "t" },
        ],
      },
    ],
  };
  const result = queryPlanToPrisma({
    queryPlan: { ...planFor("collection/all/empty-collection"), kind: PlanKind.CONDITIONAL, condition },
    mapper: MAPPER,
    model: MODEL,
  });
  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: { tags: { none: {} } },
  });
});

describe("timestamp literals", () => {
  // The golden records the folded `now() - duration("24h")` literal as `__NOW_MINUS_24H__`, because
  // it differs on every capture. That makes this the one plan whose value the reader chooses — so
  // it is also the one place the whole timestamp boundary can be walked, by substituting the
  // instant and asking what the adapter does with it.
  const at = (plannedAt: string) =>
    queryPlanToPrisma({
      queryPlan: planFor("timestamp/less-than/relative-window", plannedAt),
      mapper: MAPPER,
      model: MODEL,
    });

  test("a nanosecond instant — what the PDP actually folds — compares against the next millisecond", () => {
    // A DateTime attribute is a whole millisecond, so `a < T` for a T between two milliseconds is
    // `a < ceil(T)`. The corpus loader substitutes a nanosecond instant for the same reason.
    expect(at("2026-08-11T09:13:39.123456789Z")).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { createdAt: { lt: "2026-08-11T09:13:39.124Z" } },
    });
  });

  test("the same plan at millisecond precision translates", () => {
    expect(at("2026-08-11T09:13:39.123Z")).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { createdAt: { lt: "2026-08-11T09:13:39.123Z" } },
    });
  });

  test("excess fractional digits are accepted only when they are zero", () => {
    expect(at("2026-08-11T09:13:39.123000Z")).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { createdAt: { lt: "2026-08-11T09:13:39.123Z" } },
    });
  });

  // Each of these is refused rather than coerced: a `Date` parsed from a lenient string would
  // compare against the column as some other instant, which is a filter that returns rows the PDP
  // denies rather than an error the caller can see.
  test.each([
    ["a date with no time part", "2024-01-01"],
    ["a year outside CEL's instant range", "0000-01-01T00:00:00Z"],
    ["a day that does not exist", "2024-02-30T00:00:00Z"],
    ["an offset that pushes past the maximum instant", "9999-12-31T23:00:00-02:00"],
  ])("%s fails closed", (_label, value) => {
    expect(() => at(value)).toThrow(/RFC 3339|millisecond|instant range/);
  });
});

describe("relation subqueryFilter", () => {
  /**
   * The one mapping hazard the corpus cannot express with a policy action, because the policy is
   * irrelevant to it: the rows the adapter's subquery sees must equal the rows the application
   * put into the resource attributes. Prisma has no schema-level filtered relation — a `where`
   * injected by a client extension rewrites the top-level query, never the nested
   * `some`/`every`/`none` this adapter generates — so a narrowing the application applies to its
   * own reads does not reach the subquery unless the caller declares it. See "Mapping hazards" in
   * conformance/README.md and cerbos/query-plan-adapters#314.
   */
  const VISIBLE_ONLY: PrismaFilter = { name: { not: "hidden" } };

  const mapperFor = (subqueryFilter?: PrismaFilter): Mapper => ({
    "request.resource.attr.tags": {
      relation: {
        name: "tags",
        type: "many",
        // `nullable: false` keeps the element NULL guard (see "relation element nullability"
        // below) out of these filters, so they isolate what the declaration changes.
        fields: { name: { field: "name", nullable: false } },
        ...(subqueryFilter ? { subqueryFilter } : {}),
      },
    },
  });

  const filtersFor = (action: string, subqueryFilter?: PrismaFilter) => {
    const result = translate(action, { mapper: mapperFor(subqueryFilter) });
    if (result.kind !== PlanKind.CONDITIONAL) {
      throw new Error(`Expected CONDITIONAL result for ${action}`);
    }
    return result.filters;
  };

  test("declared: exists() examines only the records the application serialised", () => {
    expect(filtersFor("collection/exists/empty-collection", VISIBLE_ONLY)).toStrictEqual({
      tags: {
        some: { AND: [{ name: { not: "hidden" } }, { name: { equals: "public" } }] },
      },
    });
  });

  test("declared: all() narrows the records examined, not the records required", () => {
    // `every: AND(visible, P)` would REQUIRE every record to be visible, dropping any row that
    // holds a hidden tag. The rewrite to `none: AND(visible, NOT P)` is what makes the
    // declaration mean "ignore what the application hides" — including the empty-collection case,
    // where CEL's all() is vacuously true and check() agrees because the application sent an
    // empty list for the same reason.
    expect(filtersFor("collection/all/empty-collection", VISIBLE_ONLY)).toStrictEqual({
      tags: {
        none: {
          AND: [
            { name: { not: "hidden" } },
            { NOT: { name: { equals: "public" } } },
          ],
        },
      },
    });
  });

  test("declared: an emptiness check counts only the visible records", () => {
    expect(filtersFor("size/equals/negated-collection-zero", VISIBLE_ONLY)).toStrictEqual({
      NOT: { tags: { none: { name: { not: "hidden" } } } },
    });
  });

  test("undeclared: the emitted filter is what it was before the field existed", () => {
    // The non-breaking guarantee. Silence must not add a clause, and must not warn.
    expect(filtersFor("collection/exists/empty-collection")).toStrictEqual({
      tags: { some: { name: { equals: "public" } } },
    });
    expect(filtersFor("collection/all/empty-collection")).toStrictEqual({
      tags: { every: { name: { equals: "public" } } },
    });
    expect(filtersFor("size/equals/negated-collection-zero")).toStrictEqual({
      NOT: { tags: { none: {} } },
    });
  });
});

describe("the mapper contract", () => {
  // A mapper is caller-supplied, so these are caller errors rather than policy shapes: no corpus
  // action can produce them, because the corpus fixes one mapper per adapter.

  test.each([undefined, true, false])(
    "hierarchy segment nullability %s is respected",
    (nullable) => {
      const result = translate("hierarchy/overlaps/path-built-from-list-value-first", {
        mapper: { "request.resource.attr.scope": { field: "scope", nullable } },
      });
      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters:
          nullable === false
            ? {}
            : {
                AND: [
                  { OR: [{ scope: { equals: "" } }, { scope: { not: "" } }] },
                ],
              },
      });
    },
  );

  test("a function mapper resolves a scalar reference", () => {
    expect(
      translate("string/equals/case-sensitive", {
        mapper: (key: string) => ({
          field: key.replace("request.resource.attr.", ""),
        }),
      })
    ).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { aString: { equals: "one" } },
    });
  });

  test("a function mapper resolves a relation", () => {
    expect(
      translate("relation/bare-attribute/one-hop-boolean", {
        mapper: () => ({
          relation: {
            name: "parent",
            type: "one",
            fields: { aBool: { field: "aBool" } },
          },
        }),
      })
    ).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { parent: { is: { aBool: { equals: true } } } },
    });
  });

  test.each(["collection/exists/scalar-list-equals", "collection/exists/scalar-list-negated-body"])(
    "%s resolves a projection supplied through a prefix mapper",
    (action) => {
      const direct = translate(action);
      if (direct.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected a conditional projection plan");
      }
      const mapper: Mapper = {
        "request.resource.attr": {
          relation: {
            name: "categories",
            type: "many",
            fields: {
              tagNames: {
                relation: { name: "tags", type: "many", field: "name" },
              },
            },
          },
        },
      };
      expect(translate(action, { mapper })).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { categories: { some: direct.filters } },
      });
    },
  );

  test("negating an overlaps that folds to an unconditional filter is refused", () => {
    // `nullable: false` on every segment lets `overlaps(hierarchy("dept"), hierarchy(["dept",
    // scope]))` fold to `{}` (asserted above). Prisma evaluates `{ NOT: {} }` as true, so the
    // negation would match every row where CEL's `!true` matches none; the adapter has no
    // field-free false condition to emit instead (cerbos/query-plan-adapters#495).
    const fixture = planFor("hierarchy/overlaps/path-built-from-list-value-first");
    if (fixture.kind !== PlanKind.CONDITIONAL) {
      throw new Error("Expected a conditional hierarchy plan");
    }
    expect(() =>
      queryPlanToPrisma({
        queryPlan: {
          ...fixture,
          condition: { operator: "not", operands: [fixture.condition] },
        },
        mapper: { "request.resource.attr.scope": { field: "scope", nullable: false } },
      })
    ).toThrow("Cannot negate an unconditional filter");
  });

  describe("relation element nullability", () => {
    // Omitting the element NULL guard is what over-grants — a negated exists(), an all() or a
    // hasIntersection() over map() would admit rows holding a NULL element that check() denies
    // — so silence means nullable, and only `nullable: false` drops the guard
    // (cerbos/query-plan-adapters#495). The corpus maps one element column per nullability, so
    // it cannot vary the declaration itself.
    const guarded = {
      AND: [
        { tags: { every: { name: { equals: "public" } } } },
        { tags: { none: { name: null } } },
      ],
    };
    const unguarded = { tags: { every: { name: { equals: "public" } } } };
    const allFor = (fields?: Record<string, MapperConfig>) =>
      translate("collection/all/empty-collection", {
        mapper: {
          "request.resource.attr.tags": {
            relation: { name: "tags", type: "many", ...(fields ? { fields } : {}) },
          },
        },
      });

    test.each([
      ["undeclared", { name: { field: "name" } }, guarded],
      ["nullable: true", { name: { field: "name", nullable: true } }, guarded],
      ["nullable: false", { name: { field: "name", nullable: false } }, unguarded],
      ["an element with no field mapping at all", undefined, guarded],
    ] as const)("%s", (_label, fields, filters) => {
      expect(allFor(fields as Record<string, MapperConfig> | undefined)).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters,
      });
    });
  });

  test("size() against a scalar mapping is refused rather than guessed", () => {
    expect(() =>
      translate("size/equals/negated-collection-zero", {
        mapper: { "request.resource.attr.tags": { field: "tags" } },
      })
    ).toThrow("size operator requires a relation mapping");
  });
});

describe("plans the planner cannot produce", () => {
  // Input validation on a public function, not policy shapes. Every other assertion in this file
  // reads its plan from a golden file precisely because a typed plan is a belief about the planner —
  // but these are malformed by construction, so there is no golden to read and nothing to
  // believe. They exist so a caller who hands the adapter a hand-rolled or half-decoded plan gets
  // an error rather than a filter.
  //
  // The test for a shape CEL *can* express does not belong here, whatever its plan looks like:
  // it belongs in the corpus, where every adapter is asked about it. Two such shapes were
  // pinned here by hand-built plan before #377 and are now
  // [#394](https://github.com/cerbos/query-plan-adapters/issues/394).

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
      queryPlanToPrisma({
        queryPlan: { kind: "INVALID_KIND" } as unknown as PlanResourcesResponse,
        mapper: {},
      })
    ).toThrow("Invalid query plan.");
  });

  test("a condition with neither operator nor operands", () => {
    expect(() => queryPlanToPrisma({ queryPlan: plan({}), mapper: {} })).toThrow(
      "Invalid Cerbos expression structure"
    );
  });

  test("an operator this adapter has never heard of", () => {
    expect(() =>
      queryPlanToPrisma({
        queryPlan: plan({ operator: "unsupported", operands: [] }),
        mapper: {},
      })
    ).toThrow("Unsupported operator: unsupported");
  });

  test("an operand that is neither a variable nor a value", () => {
    expect(() =>
      queryPlanToPrisma({
        queryPlan: plan({ operator: "eq", operands: [{}, { value: "test" }] }),
        mapper: {},
      })
    ).toThrow("No valid left operand found");
  });

  test("a ternary with the wrong arity", () => {
    expect(() =>
      queryPlanToPrisma({
        queryPlan: plan({
          operator: "if",
          operands: [{ name: "request.resource.attr.aBool" }, { value: true }],
        }),
        mapper: MAPPER,
      })
    ).toThrow(
      "if (ternary) requires exactly 3 operands (condition, then, else), got 2"
    );
  });

  test("a ternary whose condition is not a boolean expression", () => {
    expect(() =>
      queryPlanToPrisma({
        queryPlan: plan({
          operator: "if",
          operands: [{ value: null }, { value: true }, { value: false }],
        }),
        mapper: MAPPER,
      })
    ).toThrow("if (ternary) condition must be a boolean expression");
  });

  test("a comparison against a ternary with three operands", () => {
    expect(() =>
      queryPlanToPrisma({
        queryPlan: plan({
          operator: "eq",
          operands: [
            {
              operator: "if",
              operands: [
                { name: "request.resource.attr.aBool" },
                { value: true },
                { value: false },
              ],
            },
            { value: true },
            { value: false },
          ],
        }),
        mapper: MAPPER,
      })
    ).toThrow("eq with a ternary requires exactly 2 operands, got 3");
  });

  test("a macro over a collection value that is not a list", () => {
    expect(() =>
      queryPlanToPrisma({
        queryPlan: plan({
          operator: "exists",
          operands: [
            { value: "not-a-list" },
            {
              operator: "lambda",
              operands: [
                {
                  operator: "eq",
                  operands: [
                    { name: "request.resource.attr.aString" },
                    { name: "t" },
                  ],
                },
                { name: "t" },
              ],
            },
          ],
        }),
        mapper: MAPPER,
      })
    ).toThrow("exists over a literal collection requires a list value");
  });

  // An empty conjunction is `{ AND: [] }` in Prisma, which matches every row, and the constant
  // folder would otherwise reduce it to `true` before the translator saw it
  // (cerbos/query-plan-adapters#495). The empty disjunction is refused alongside it: neither is
  // something the planner emits.
  test.each([
    ["an empty and", { operator: "and", operands: [] }, "and requires at least one operand"],
    ["an empty or", { operator: "or", operands: [] }, "or requires at least one operand"],
    [
      "an empty and under an or",
      {
        operator: "or",
        operands: [
          { name: "request.resource.attr.aBool" },
          { operator: "and", operands: [] },
        ],
      },
      "and requires at least one operand",
    ],
    [
      "an empty and under a not",
      { operator: "not", operands: [{ operator: "and", operands: [] }] },
      "and requires at least one operand",
    ],
  ])("%s", (_label, condition, message) => {
    expect(() =>
      queryPlanToPrisma({ queryPlan: plan(condition), mapper: MAPPER })
    ).toThrow(message);
  });

  test("a condition that folds to constant false is always denied", () => {
    expect(
      queryPlanToPrisma({
        queryPlan: plan({
          operator: "if",
          operands: [{ value: true }, { value: false }, { value: true }],
        }),
        mapper: MAPPER,
      })
    ).toEqual({ kind: PlanKind.ALWAYS_DENIED });
  });
});

// -- KIND 3: corpus gaps --------------------------------------------------------------------------
//
// Policy-reachable shapes the corpus does not discriminate yet. Each is a bridge until a case with
// a discriminating seed lands (https://github.com/cerbos/query-plan-adapters/issues/509), and is
// deleted then.

describe("corpus gaps", () => {
  const chainAll = {
    operator: "all",
    operands: [
      { name: "request.resource.attr.mainCategory.subCategories" },
      {
        operator: "lambda",
        operands: [
          { operator: "eq", operands: [{ name: "s.name" }, { value: "finance" }] },
          { name: "s" },
        ],
      },
    ],
  };
  const translateCondition = (condition: unknown) =>
    queryPlanToPrisma({
      queryPlan: {
        kind: PlanKind.CONDITIONAL,
        condition,
        cerbosCallId: "",
        requestId: "",
        validationErrors: [],
        metadata: undefined,
      } as PlanResourcesResponse,
      mapper: MAPPER,
      model: MODEL,
    });

  test("Corpus gap. A negated all() over a chain needs its false witness at the end of the chain", () => {
    // Every seeded category holds exactly one subcategory, so the corpus cannot tell this from
    // `categories: { some: { NOT: { subCategories: { some: P } } } }` — which admits a category
    // with NO subcategories, where CEL's all() is vacuously true and its negation false.
    expect(translateCondition({ operator: "not", operands: [chainAll] })).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        categories: {
          some: { subCategories: { some: { NOT: { name: { equals: "finance" } } } } },
        },
      },
    });
  });
});
