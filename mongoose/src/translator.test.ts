import * as fs from "node:fs";
import * as path from "node:path";

import { describe, expect, test } from "@jest/globals";
import type {
  PlanExpressionOperand,
  PlanResourcesResponse,
} from "@cerbos/core";
import { Types } from "mongoose";

import { PlanKind, queryPlanToMongoose, UnsupportedQueryPlanError } from ".";
import type {
  Mapper,
  MapperConfig,
  NullAttributeRepresentation,
  QueryPlanToMongooseResult,
} from ".";
import { MAPPER, pdpTags, planOf, readGolden, readGoldens } from "./corpus";

/**
 * Offline unit tests: what a caller can pass that the conformance corpus cannot vary (mapper forms,
 * `valueParser`, `nullAttributeRepresentation`), the refusal type, the timestamp literal contract,
 * the no-second-collection property, and plans the planner cannot produce.
 *
 * Which documents a corpus case returns is the conformance harness's job (`adversarial.test.ts`);
 * nothing here pins the filter emitted for a corpus case. Plans come from the current PDP's golden
 * files, read by case id, rather than being typed by hand.
 */

const CURRENT = pdpTags()[0]!;
const golden = (id: string) => readGolden(CURRENT, id);

/** The current PDP's recorded plan for a corpus case, as `@cerbos/http` returns it. */
function planFor(id: string, now?: string): Promise<PlanResourcesResponse> {
  return planOf(golden(id), now);
}

interface TranslateOptions {
  mapper?: Mapper;
  nullAttributeRepresentation?: NullAttributeRepresentation;
}

function translatePlan(
  queryPlan: PlanResourcesResponse,
  options: TranslateOptions = {},
): QueryPlanToMongooseResult {
  return queryPlanToMongoose({
    queryPlan,
    mapper: options.mapper ?? MAPPER,
    ...(options.nullAttributeRepresentation
      ? { nullAttributeRepresentation: options.nullAttributeRepresentation }
      : {}),
  });
}

async function translate(
  id: string,
  options: TranslateOptions = {},
): Promise<QueryPlanToMongooseResult> {
  return translatePlan(await planFor(id), options);
}

describe("the refusal type", () => {
  // Every shape this adapter cannot express raises `UnsupportedQueryPlanError`, which the
  // conformance harness asserts for every `unsupported` ledger entry. These pin the boundary on the
  // other side: it is still an `Error`, and a mapper mistake is NOT a refusal.
  test("a refused shape raises UnsupportedQueryPlanError, which is an Error", async () => {
    let thrown: unknown;
    try {
      await translate("regex/matches/lookahead-from-principal");
    } catch (error) {
      thrown = error;
    }
    expect(thrown).toBeInstanceOf(UnsupportedQueryPlanError);
    expect(thrown).toBeInstanceOf(Error);
    expect((thrown as Error).name).toBe("UnsupportedQueryPlanError");
  });

  test("an unmapped reference is a plain Error, not a refusal", async () => {
    const queryPlan = await planFor("string/equals/case-sensitive");
    let thrown: unknown;
    try {
      queryPlanToMongoose({ queryPlan });
    } catch (error) {
      thrown = error;
    }
    expect(thrown).toBeInstanceOf(Error);
    expect(thrown).not.toBeInstanceOf(UnsupportedQueryPlanError);
  });
});

// The mapping-hazard contract in README.md ("Mapping hazards") rests on ONE structural fact: this
// adapter builds no subquery. A relation is a path inside the same document, so the filter and the
// application read the same document and the subquery hazards cannot arise. The day the adapter
// reaches a second collection — a `$lookup` stage, a `populate()` call — every one of them arrives
// at once. No corpus case can state that (a case asks which rows come back), so it is pinned here
// against the source, which is total over mapper shapes where a walk of emitted filters is not
// (cerbos/query-plan-adapters#323).
test("the adapter source emits no $lookup and reaches no second collection", () => {
  const forbidden = /\$lookup|\$graphLookup|\bpopulate\s*\(|\baggregate\s*\(/;
  const sourceFiles = fs
    .readdirSync(__dirname)
    .filter(
      (file) =>
        file.endsWith(".ts") &&
        !file.endsWith(".test.ts") &&
        file !== "corpus.ts",
    )
    .sort();
  // Guard the guard: a scan that found no files, or lost the entry point, would pass vacuously.
  expect(sourceFiles).toContain("index.ts");
  expect(sourceFiles).toContain("filter.ts");
  // Prose about the guard is not a violation of it, so comments come off first.
  const stripComments = (line: string): string =>
    /^(\/\/|\/\*|\*)/.test(line.trimStart())
      ? ""
      : line.replace(/\/\/.*$/, "").replace(/\/\*.*?\*\//g, "");
  const offendingLines = sourceFiles.flatMap((file) =>
    fs
      .readFileSync(path.join(__dirname, file), "utf8")
      .split("\n")
      .map(
        (line, index) => [`${file}:${index + 1}`, stripComments(line)] as const,
      )
      .filter(([, code]) => forbidden.test(code)),
  );
  expect(offendingLines).toEqual([]);
});

describe("nullAttributeRepresentation", () => {
  // `null/equals/null-literal-on-missing-attribute` is `== null` against an attribute the caller
  // OMITS when the field is NULL. The two conventions are indistinguishable
  // on the wire — the planner emits the same `eq(attr, null)` either way — so the adapter has to
  // be told, and the whole behaviour is a translator property with no store in it.
  //
  // Mongoose expresses it twice over: per attribute with the `nullable` mapper flag (which the
  // conformance mapping declares, so the harness replays that case under it), and globally with
  // this switch, which is the default for every entry that declares no `nullable` (#493).
  test("explicit: the null operand is translated", async () => {
    expect(
      await translate("null/equals/null-literal-on-missing-attribute", {
        nullAttributeRepresentation: "explicit",
      }),
    ).toStrictEqual(
      await translate("null/equals/null-literal-on-missing-attribute"),
    );
  });

  test("omitted: the same plan is refused rather than translated", async () => {
    // A NULL field sends no attribute, so check() denies on a missing-attribute error while a
    // null-selecting filter would return exactly those documents (#302).
    await expect(
      translate("null/equals/null-literal-on-missing-attribute", {
        nullAttributeRepresentation: "omitted",
      }),
    ).rejects.toThrow("missing-attribute error");
  });

  test.each(["explicit", "omitted"] as const)(
    "%s: a reentrant function mapper cannot replace the caller's null representation",
    async (nullAttributeRepresentation) => {
      const nestedRepresentation =
        nullAttributeRepresentation === "explicit" ? "omitted" : "explicit";
      const nested = await planFor("string/equals/case-sensitive");
      const outerPlan = await planFor(
        "null/equals/null-literal-on-missing-attribute",
      );
      let nestedCalls = 0;
      const mapper: Mapper = (key) => {
        translatePlan(nested, {
          nullAttributeRepresentation: nestedRepresentation,
        });
        nestedCalls += 1;
        return typeof MAPPER === "function"
          ? MAPPER(key)
          : (MAPPER[key] ?? { field: key });
      };
      const outer = () =>
        translatePlan(outerPlan, {
          mapper,
          nullAttributeRepresentation,
        });
      if (nullAttributeRepresentation === "explicit") {
        expect(outer()).toStrictEqual(
          await translate("null/equals/null-literal-on-missing-attribute"),
        );
      } else {
        expect(outer).toThrow("missing-attribute error");
      }
      expect(nestedCalls).toBeGreaterThan(0);
    },
  );

  // The rejection keys off the null OPERAND, not off a list of operators, so a value list carrying
  // one is refused as well.
  test("omitted: a null element inside a value list is refused too", async () => {
    await expect(
      translate("null/in/literal-list-of-only-null", {
        nullAttributeRepresentation: "omitted",
      }),
    ).rejects.toThrow("missing-attribute error");
  });

  // The same claim over every recorded plan, so a new case carrying a null constant is covered
  // without anyone naming it here. An INDEXED null element is the one exception: the option
  // describes absent fields, not null list elements, which keep their explicit null value. A
  // negated shape may be refused for the negation over a field the option makes nullable before
  // its null operand is reached, so the refusal type is what counts.
  test("omitted: every golden plan carrying a null literal is refused", async () => {
    const carriesNull = (node: unknown): boolean => {
      if (typeof node !== "object" || node === null) return false;
      const record = node as Record<string, unknown>;
      if ("value" in record) {
        const value = record["value"];
        return value === null || (Array.isArray(value) && value.includes(null));
      }
      const expression = record["expression"] as
        | { operands?: unknown[] }
        | undefined;
      return (expression?.operands ?? []).some(carriesNull);
    };
    const indexedNull =
      "collection/index/first-element-of-string-list-equals-null";
    const nullCarrying = readGoldens(CURRENT)
      .filter(
        (g) =>
          g.plan.kind === PlanKind.CONDITIONAL && carriesNull(g.plan.condition),
      )
      .map((g) => g.id);
    // Guard the guard: if the walk stopped finding null operands the loop below is vacuous.
    expect(nullCarrying).toContain(
      "null/equals/null-literal-on-missing-attribute",
    );
    expect(nullCarrying).toContain(indexedNull);
    const plans = new Map(
      await Promise.all(
        nullCarrying.map(async (id) => [id, await planFor(id)] as const),
      ),
    );

    // A plan the adapter refuses under "explicit" too is refused for its shape, not its null
    // (a whole-list comparison with `[null]` in the list), so the option has nothing to add.
    const refusedRegardless = (id: string): boolean => {
      try {
        translatePlan(plans.get(id)!);
        return false;
      } catch (error) {
        return error instanceof UnsupportedQueryPlanError;
      }
    };
    const notRejected = nullCarrying.filter((id) => {
      if (id === indexedNull || refusedRegardless(id)) return false;
      try {
        translatePlan(plans.get(id)!, { nullAttributeRepresentation: "omitted" });
        return true;
      } catch (error) {
        return !(error instanceof UnsupportedQueryPlanError);
      }
    });
    expect(notRejected).toEqual([]);
    expect(() =>
      translatePlan(plans.get(indexedNull)!, {
        nullAttributeRepresentation: "omitted",
      }),
    ).not.toThrow();
  });

  // The negative control, and the reason the two assertions above are not vacuous: a guard that
  // rejected EVERY plan under `omitted` would satisfy both while breaking every caller who set
  // the option. The switch narrows what translates; it does not turn the adapter off. With every
  // entry declaring its own `nullable`, nothing is left for the default to decide, so every
  // null-free plan translates exactly as it does under "explicit".
  test("omitted: with every entry declared, a null-free plan is untouched", async () => {
    const outcome = (build: () => QueryPlanToMongooseResult): unknown => {
      try {
        return build();
      } catch (error) {
        return error instanceof UnsupportedQueryPlanError
          ? "refused"
          : String(error);
      }
    };
    const nullFree = readGoldens(CURRENT).filter(
      (g) => !/\bnull\b/.test(JSON.stringify(g.plan)),
    );
    // Guard the guard: the loop below must cover real plans.
    expect(nullFree.map((g) => g.id)).toContain("string/equals/case-sensitive");
    const plans = await Promise.all(nullFree.map((g) => planOf(g)));
    const differing = nullFree
      .filter(
        (_g, index) =>
          JSON.stringify(outcome(() => translatePlan(plans[index]!))) !==
          JSON.stringify(
            outcome(() =>
              translatePlan(plans[index]!, {
                mapper: declaringNullable(MAPPER, false),
                nullAttributeRepresentation: "omitted",
              }),
            ),
          ),
      )
      .map((g) => g.id);
    expect(differing).toEqual([]);
  });
});

/** `mapper` with `nullable` set to `value` on every entry and relation field that declares none. */
function declaringNullable(mapper: Mapper, value: boolean): Mapper {
  const declare = (config: MapperConfig): MapperConfig => ({
    ...config,
    nullable: config.nullable ?? value,
    ...(config.relation
      ? {
          relation: {
            ...config.relation,
            ...(config.relation.fields
              ? {
                  fields: Object.fromEntries(
                    Object.entries(config.relation.fields).map(([key, field]) => [
                      key,
                      declare(field),
                    ]),
                  ),
                }
              : {}),
          },
        }
      : {}),
  });
  if (typeof mapper === "function") return (key) => declare(mapper(key));
  return Object.fromEntries(
    Object.entries(mapper).map(([key, config]) => [key, declare(config)]),
  );
}

// Under "omitted" a NULL field sends no attribute, so CEL denies every comparison against it, while
// MongoDB's `$ne` and `$nor` match a document the path is absent from or null in. An entry that
// declares no `nullable` therefore takes the call-level convention as its default, exactly as if it
// declared `nullable: true` (#493). A caller-supplied argument no corpus case can vary: the harness
// runs one mapping, under "explicit".
describe("omitted: an undeclared entry is nullable", () => {
  const omitted = (id: string, mapper: Mapper = MAPPER) =>
    translate(id, { mapper, nullAttributeRepresentation: "omitted" });
  const topLevelConjuncts = (filter: unknown): unknown[] => {
    const record = filter as Record<string, unknown>;
    return Array.isArray(record["$and"])
      ? record["$and"].flatMap(topLevelConjuncts)
      : [filter];
  };

  // The corpus mapping declares the scalars it seeds as missing (`aString`, `aNumber`, `aBool`)
  // nullable, so each test strips the declaration from the field it reads, leaving the entry for
  // the call-level default to decide. `owner` maps the nullable `aOptionalString` column without
  // declaring it, as a caller on the explicit convention would.
  const undeclared = (field: string): Mapper =>
    Object.fromEntries(
      Object.entries(MAPPER as Record<string, MapperConfig>).map(
        ([key, config]) => {
          if (config.field !== field) return [key, config];
          const { nullable: _nullable, ...rest } = config;
          return [key, rest];
        },
      ),
    );
  const guard = (field: string) => JSON.stringify({ [field]: { $ne: null } });

  test.each([
    ["string/equals/case-sensitive", "aString"],
    ["comparison/not-equals/value-first", "aString"],
    ["null/not-equals/explicit-null-against-literal", "aOptionalString"],
  ])(
    "%s requires the field to be present and non-null, outside any negation",
    async (id, field) => {
      const mapper = undeclared(field);
      expect(
        topLevelConjuncts((await translate(id, { mapper })).filters),
      ).not.toContainEqual({
        [field]: { $ne: null },
      });
      expect(
        topLevelConjuncts((await omitted(id, mapper)).filters),
      ).toContainEqual({
        [field]: { $ne: null },
      });
    },
  );

  // A `not` over an undeclared entry is translated as one over a declared-nullable entry: the
  // guard sits outside the `$nor`, which would otherwise readmit the missing documents. A negated
  // `&&` is pushed down to its leaves first, so each leaf carries its own guard.
  test.each([
    ["null/equals/negated-explicit-null-against-literal", "aOptionalString"],
    ["null/in/negated-explicit-null-in-literal-list", "aOptionalString"],
    ["logic/not/over-and", "aString"],
  ])("%s guards %s outside the negation", async (id, field) => {
    const mapper = undeclared(field);
    expect(
      JSON.stringify((await translate(id, { mapper })).filters),
    ).not.toContain(guard(field));
    expect(JSON.stringify((await omitted(id, mapper)).filters)).toContain(
      guard(field),
    );
  });

  test("a function mapper takes the default too", async () => {
    const mapper = undeclared("aString") as Record<string, MapperConfig>;
    const asFunction: Mapper = (key) => mapper[key]!;
    for (const id of ["string/equals/case-sensitive", "logic/not/over-and"]) {
      expect(await omitted(id, asFunction)).toStrictEqual(
        await omitted(id, mapper),
      );
    }
  });

  // `tagNames` projects the `name` field of each `tags` element, which declares no `nullable`.
  test("a relation's element field takes the default too", async () => {
    const id = "collection/exists/scalar-list-equals";
    const guard = JSON.stringify({ name: { $ne: null } });
    expect(JSON.stringify((await translate(id)).filters)).not.toContain(guard);
    expect(JSON.stringify((await omitted(id)).filters)).toContain(guard);
  });

  // `nullable: false` is the per-entry opt-out: it asserts the field is always stored.
  test("declaring nullable: false keeps the explicit translation", async () => {
    for (const id of [
      "string/equals/case-sensitive",
      "comparison/not-equals/value-first",
      "logic/not/over-and",
    ]) {
      expect(await omitted(id, declaringNullable(MAPPER, false))).toStrictEqual(
        await translate(id),
      );
    }
  });
});

describe("timestamp literals", () => {
  // The generator records the folded `now() - duration("24h")` literal in
  // `timestamp/less-than/relative-window` as a placeholder, because it differs on every capture.
  // That makes this the one golden plan whose value the reader chooses — so it is also the one place the whole timestamp
  // boundary can be walked, by substituting the instant and asking what the adapter does with it.
  const at = async (plannedAt: string) =>
    queryPlanToMongoose({
      queryPlan: await planFor("timestamp/less-than/relative-window", plannedAt),
      mapper: MAPPER,
    });

  // The nanosecond instant the PDP actually folds is refused — that, and nothing else, is why the
  // two `timestamp/*/relative-window*` cases are `unsupported` in conformance-ledger.json.
  test("the nanosecond instant the PDP folds is refused", async () => {
    await expect(at("2026-08-11T09:13:39.123456789Z")).rejects.toThrow(
      UnsupportedQueryPlanError,
    );
  });

  // Its counterpart: only the precision differs.
  test("the same plan at millisecond precision translates", async () => {
    const result = await at("2026-08-11T09:13:39.123Z");
    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    // The comparison operand is the instant, guarded so that a field that is not a date and not an RFC 3339 string converts to
    // null, and null loses every comparison rather than matching one.
    expect(JSON.stringify(result.filters)).toContain(
      '"2026-08-11T09:13:39.123Z"',
    );
  });

  // Each of these is refused rather than coerced: a BSON Date built from a lenient string would
  // compare against the field as some other instant, which is a filter that returns documents the
  // PDP denies rather than an error the caller can see. A BSON Date holds milliseconds, so the
  // sub-millisecond cases are refused even when the extra digits are zeros — the value would be
  // silently truncated, and truncation is what makes the relative-window cases unsupported in the first place.
  test.each([
    ["a date with no time part", "2024-01-01"],
    ["a year outside CEL's instant range", "0000-01-01T00:00:00Z"],
    ["a day that does not exist", "2024-02-30T00:00:00Z"],
    ["sub-millisecond precision", "2024-01-01T00:00:00.1234Z"],
    ["excess fractional digits, even when zero", "2026-08-11T09:13:39.123000Z"],
    [
      "an offset that pushes past the maximum instant",
      "9999-12-31T23:00:00-02:00",
    ],
  ])("%s fails closed", async (_label, value) => {
    await expect(at(value)).rejects.toThrow(
      "timestamp value must be a millisecond-exact RFC 3339 instant in the CEL range",
    );
  });
});

describe("the mapper contract", () => {
  // A mapper is caller-supplied, so these are caller shapes rather than policy shapes: no corpus
  // case can produce them, because the corpus fixes one mapper per adapter.

  test("a function mapper resolves a scalar reference", async () => {
    expect(
      await translate("string/equals/case-sensitive", {
        mapper: (key: string) => ({
          field: key.replace("request.resource.attr.", ""),
        }),
      }),
    ).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { aString: { $eq: "one" } },
    });
  });

  test("a function mapper resolves a relation", async () => {
    expect(
      await translate("relation/bare-attribute/one-hop-boolean", {
        mapper: (key: string) =>
          key === "request.resource.attr.parent"
            ? {
                relation: {
                  name: "parent",
                  type: "one",
                  fields: { aBool: { field: "aBool" } },
                },
              }
            : { field: key.replace("request.resource.attr.", "") },
      }),
    ).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { "parent.aBool": { $eq: true } },
    });
  });

  // `valueParser` is how a caller reconciles the type Cerbos sends with the type the collection
  // stores. The corpus cannot exercise it: its mapper deliberately carries none, because the
  // harness compares document ids against `check()` and a parser that changed a value would change
  // both sides at once.
  test("a valueParser rewrites the constant of an equality", async () => {
    expect(
      await translate("string/equals/case-sensitive", {
        mapper: {
          "request.resource.attr.aString": {
            field: "aString",
            valueParser: (value: unknown) => String(value).toUpperCase(),
          },
        },
      }),
    ).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { aString: { $eq: "ONE" } },
    });
  });

  test("a valueParser rewrites every element of a membership list", async () => {
    expect(
      await translate("null/in/missing-attribute-in-multi-element-list", {
        mapper: {
          "request.resource.attr.aOptionalString": {
            field: "aOptionalString",
            valueParser: (value: unknown) => String(value).toUpperCase(),
          },
        },
      }),
    ).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { aOptionalString: { $in: ["X", "ONE_TWO"] } },
    });
  });

  // The reason the corpus mapper maps the primary key to a string field rather than to `_id`: an
  // ObjectId collection key is the common real deployment, three of the `identifier/*` cases
  // compare the key against a string field, and one mapping cannot be both. So the coercion is pinned
  // here, against the same golden plan, rather than left to a README example nothing runs.
  test("a valueParser coerces the primary key to an ObjectId", async () => {
    expect(
      await translate("identifier/equals/literal", {
        mapper: {
          "request.resource.id": {
            field: "_id",
            valueParser: (value: unknown) =>
              new Types.ObjectId(String(value).padEnd(24, "0")),
          },
        },
      }),
    ).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { _id: { $eq: new Types.ObjectId("f10000000000000000000000") } },
    });
  });

  test("a valueParser declared on a relation field applies through the hop", async () => {
    const relation = (valueParser?: (value: unknown) => unknown): Mapper => ({
      "request.resource.attr.parent": {
        relation: {
          name: "parent",
          type: "one",
          fields: {
            aString: {
              field: "aString",
              ...(valueParser ? { valueParser } : {}),
            },
          },
        },
      },
    });

    expect(
      await translate("relation/equals/one-hop-string", {
        mapper: relation((value) => String(value).toUpperCase()),
      }),
    ).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { "parent.aString": { $eq: "ONE" } },
    });
    // And the same mapping without one, so the assertion above is the parser talking rather than
    // some other normalisation of the constant.
    expect(
      await translate("relation/equals/one-hop-string", { mapper: relation() }),
    ).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { "parent.aString": { $eq: "One" } },
    });
  });
});

describe("an unmapped reference", () => {
  // A caller shape, like the rest of the mapper contract: the corpus fixes one mapper that maps
  // every reference its policies reach, so no corpus case can ask what happens to a name the
  // mapper does not declare. Before cerbos/query-plan-adapters#492 it was used verbatim as a
  // document path, and `$ne`/`$nor` over a path no document stores matched every document.
  const without = (reference: string): Mapper =>
    Object.fromEntries(
      Object.entries(MAPPER as Record<string, MapperConfig>).filter(
        ([key]) => key !== reference,
      ),
    );

  test.each([
    [
      "null/not-equals/missing-attribute-against-literal",
      "request.resource.attr.aOptionalString",
    ],
    ["logic/not/greater-than", "request.resource.attr.aNumber"],
  ])("%s is refused when %s has no entry", async (id, reference) => {
    await expect(translate(id, { mapper: without(reference) })).rejects.toThrow(
      `No mapper entry for ${reference}: an unmapped reference is not used verbatim`,
    );
  });

  test("is refused when a function mapper returns no entry for it", async () => {
    const mapper = ((key: string) =>
      key === "request.resource.attr.aOptionalString"
        ? undefined
        : (MAPPER as Record<string, MapperConfig>)[key]) as Mapper;
    await expect(
      translate("null/not-equals/missing-attribute-against-literal", {
        mapper,
      }),
    ).rejects.toThrow("No mapper entry for request.resource.attr.aOptionalString");
  });

  test("is refused under the default mapper", async () => {
    const queryPlan = await planFor(
      "null/not-equals/missing-attribute-against-literal",
    );
    expect(() => queryPlanToMongoose({ queryPlan })).toThrow("No mapper entry for request.resource.attr.aOptionalString");
  });

  // The opt-in: an entry that names neither a field nor a relation keeps the plan path, for a
  // caller whose documents really are shaped like it.
  test("keeps the plan path when an empty entry declares it", async () => {
    expect(
      await translate("null/not-equals/missing-attribute-against-literal", {
        mapper: { "request.resource.attr.aOptionalString": {} },
      }),
    ).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { "request.resource.attr.aOptionalString": { $ne: "x" } },
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
  // A test for a shape CEL *can* express does not belong here, whatever its plan looks like: it
  // belongs in the corpus, where every adapter is asked about it. The shapes the retired suite
  // pinned that way are
  // [#394](https://github.com/cerbos/query-plan-adapters/issues/394) and
  // [#396](https://github.com/cerbos/query-plan-adapters/issues/396).

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
      queryPlanToMongoose({
        queryPlan: { kind: "INVALID_KIND" } as unknown as PlanResourcesResponse,
        mapper: {},
      }),
    ).toThrow("Invalid query plan.");
  });

  test("a condition with neither operator nor operands", () => {
    expect(() =>
      queryPlanToMongoose({ queryPlan: plan({}), mapper: {} }),
    ).toThrow("Invalid Cerbos expression structure");
  });

  test("an operator this adapter has never heard of", () => {
    expect(() =>
      queryPlanToMongoose({
        queryPlan: plan({ operator: "unsupported", operands: [] }),
        mapper: {},
      }),
    ).toThrow("Unsupported operator: unsupported");
  });

  test("a macro over a collection value that is not a list", () => {
    expect(() =>
      queryPlanToMongoose({
        queryPlan: plan({
          operator: "exists",
          operands: [
            { value: { not: "a list" } },
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
      }),
    ).toThrow("exists over a literal collection requires a list value");
  });
});

// KIND 3 — corpus gap (cerbos/query-plan-adapters#509). No seed stores a list as null or omits it,
// so no case can witness a negated membership over one: whether the corpus should carry such a
// row is #548. CEL denies `!(2 in null)`, where a bare `$nor` over the membership matches a null
// or absent array (#534). Delete this when a case carrying a null-list row lands.
describe("a negated membership over a native array field", () => {
  test("Corpus gap. requires the list to be stored as an array, outside the $nor", async () => {
    const { filters } = await translate("null/in/negated-null-literal-in-number-list");
    expect(filters).toMatchObject({
      $and: expect.arrayContaining([{ aNumberList: { $type: "array" } }]),
    });
  });
});
