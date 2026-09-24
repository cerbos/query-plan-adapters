import { describe, expect, test } from "@jest/globals";
import type {
  PlanExpressionOperand,
  PlanResourcesResponse,
} from "@cerbos/core";
import { eq, ne, sql } from "drizzle-orm";
import type { SQL } from "drizzle-orm";
import { MySqlDialect } from "drizzle-orm/mysql-core/dialect";
import { PgDialect } from "drizzle-orm/pg-core/dialect";
import { bigint, doublePrecision, numeric, pgTable, real } from "drizzle-orm/pg-core";
import { SQLiteSyncDialect } from "drizzle-orm/sqlite-core/dialect";

import { PlanKind, queryPlanToDrizzle, UnsupportedQueryPlanError } from ".";
import { formatCelDouble, parseCelDoubleString } from "./conversion";
import { compileRegex } from "./regex";
import type {
  Mapper,
  MapperEntry,
  NullAttributeRepresentation,
  QueryPlanToDrizzleResult,
} from ".";
import {
  buildMapper,
  mysqlSchema,
  pdpTags,
  planOf,
  postgresSchema,
  readGolden,
  readGoldens,
  sqliteSchema,
} from "./corpus";

/**
 * Offline unit tests: what a caller can pass that the conformance corpus cannot vary (mapper forms,
 * `transform`, declared index storage, `subqueryFilter`, `nullAttributeRepresentation`), the
 * refusal type, the timestamp literal contract, and plans the planner cannot produce.
 *
 * Which rows a corpus case returns is the conformance harness's job (`adversarial.test.ts`); nothing
 * here pins the filter emitted for a corpus case. Plans come from the current PDP's golden files,
 * read by case id, rather than being typed by hand.
 */

const CURRENT = pdpTags()[0]!;
const golden = (id: string) => readGolden(CURRENT, id);

type Store = "sqlite" | "postgresql" | "mysql";
const STORES: Store[] = ["sqlite", "postgresql", "mysql"];

const MAPPERS: Record<Store, Record<string, MapperEntry>> = {
  sqlite: buildMapper(sqliteSchema()),
  postgresql: buildMapper(postgresSchema()),
  mysql: buildMapper(mysqlSchema()),
};

const DIALECTS = {
  postgresql: new PgDialect(),
  sqlite: new SQLiteSyncDialect(),
  mysql: new MySqlDialect(),
};

/** The shared mapper with every per-attribute null declaration stripped. */
function withoutNullDeclarations(
  mapper: Record<string, MapperEntry>,
): Record<string, MapperEntry> {
  return Object.fromEntries(
    Object.entries(mapper).map(([reference, entry]) => {
      if (typeof entry !== "object" || entry === null || !("nullAttributeRepresentation" in entry)) {
        return [reference, entry];
      }
      const { nullAttributeRepresentation: _stripped, ...rest } = entry;
      return [reference, rest as MapperEntry];
    }),
  );
}

const UNDECLARED = withoutNullDeclarations(MAPPERS.postgresql);

function translate(
  store: Store,
  id: string,
  options: {
    mapper?: Mapper;
    nullAttributeRepresentation?: NullAttributeRepresentation;
    now?: string;
  } = {},
): QueryPlanToDrizzleResult {
  return queryPlanToDrizzle({
    queryPlan: planOf(golden(id), options.now),
    mapper: options.mapper ?? MAPPERS[store],
    ...(options.nullAttributeRepresentation
      ? { nullAttributeRepresentation: options.nullAttributeRepresentation }
      : {}),
  });
}

function filterFor(store: Store, id: string, options: Parameters<typeof translate>[2] = {}): SQL {
  const result = translate(store, id, options);
  if (result.kind !== PlanKind.CONDITIONAL) {
    throw new Error(`${id} translated to ${result.kind}, not a filter`);
  }
  return result.filter;
}

function render(store: Store, filter: SQL): { sql: string; params: unknown[] } {
  const query = DIALECTS[store].sqlToQuery(filter);
  return { sql: query.sql, params: query.params };
}

describe("the refusal type", () => {
  test("is an exported Error subclass, so existing catch blocks keep working", () => {
    const error = new UnsupportedQueryPlanError("x");
    expect(error).toBeInstanceOf(Error);
    expect(error.name).toBe("UnsupportedQueryPlanError");
  });

  test("a shape the adapter cannot express raises it", () => {
    expect(() => translate("postgresql", "collection/map/equals-list-literal")).toThrow(
      UnsupportedQueryPlanError,
    );
  });

  test("a mapper misconfiguration is a plain Error, not a refusal", () => {
    // An unmapped reference is a bug in the caller's mapping; reporting it as "unsupported" would
    // let a harness count a typo as a declared limitation.
    let caught: unknown;
    try {
      translate("postgresql", "string/equals/case-sensitive", { mapper: {} });
    } catch (error) {
      caught = error;
    }
    expect(caught).toBeInstanceOf(Error);
    expect(caught).not.toBeInstanceOf(UnsupportedQueryPlanError);
    expect(String(caught)).toMatch(/No mapping/);
  });
});

describe("mapper forms", () => {
  // A deep relation shape, so the equivalence covers references resolved through several hops.
  const DEEP = "collection/exists/nested-three-levels";

  test("a function mapper resolves the same references as a record mapper", () => {
    const record = MAPPERS.postgresql;
    const asFunction: Mapper = (reference) => record[reference];
    expect(render("postgresql", filterFor("postgresql", DEEP, { mapper: asFunction }))).toEqual(
      render("postgresql", filterFor("postgresql", DEEP)),
    );
  });

  test("a transform replaces the comparison the adapter would have built", () => {
    const schema = postgresSchema();
    const lowered: Mapper = {
      ...MAPPERS.postgresql,
      "request.resource.attr.aString": {
        column: schema.resources.aString,
        transform: ({ value }) =>
          eq(sql`lower(${schema.resources.aString})`, String(value).toLowerCase()),
      },
    };
    const id = "string/equals/case-sensitive";
    const plain = render("postgresql", filterFor("postgresql", id));
    const rendered = render("postgresql", filterFor("postgresql", id, { mapper: lowered }));

    // The transform owns the whole comparison: its SQL, and the value it binds.
    expect(rendered.sql).toContain("lower(");
    expect(rendered.params).toEqual(plain.params.map((param) => String(param).toLowerCase()));
  });
});

describe("declared index storage", () => {
  const reference = "request.resource.attr.tagNames";
  const INDEX = "collection/index/first-element-of-string-list";

  test.each(STORES)("%s refuses an undeclared storage shape", (store) => {
    const entry = MAPPERS[store][reference];
    if (!entry || typeof entry !== "object" || !("indexable" in entry)) {
      throw new Error("The shared mapper must declare index storage");
    }
    const { indexable: _indexable, ...undeclared } = entry;
    expect(() =>
      translate(store, INDEX, { mapper: { ...MAPPERS[store], [reference]: undeclared } }),
    ).toThrow("Index storage shape is undeclared");
  });

  test("a function mapper preserves the declared representation", () => {
    const mapper: Mapper = (ref) => MAPPERS.postgresql[ref];
    expect(render("postgresql", filterFor("postgresql", INDEX, { mapper }))).toEqual(
      render("postgresql", filterFor("postgresql", INDEX)),
    );
  });

  test("pgArray cannot be declared for a SQLite column", () => {
    expect(() =>
      translate("sqlite", INDEX, {
        mapper: { [reference]: { column: sqliteSchema().resources.tagNamesJson, indexable: "pgArray" } },
      }),
    ).toThrow("requires a PostgreSQL array column");
  });

  test("a column transform cannot silently disappear from indexed access", () => {
    expect(() =>
      translate("postgresql", INDEX, {
        mapper: {
          [reference]: {
            column: postgresSchema().resources.tagNamesJson,
            indexable: "json",
            transform: () => sql`false`,
          },
        },
      }),
    ).toThrow("without a transform");
  });

  const converted = pgTable("converted_arrays", {
    numericString: numeric().array(),
    numericNumber: numeric({ mode: "number" }).array(),
    bigintNumber: bigint({ mode: "number" }).array(),
    realNumber: real().array(),
    doubleNumber: doublePrecision().array(),
  });
  test.each([
    converted.numericString,
    converted.numericNumber,
    converted.bigintNumber,
    converted.realNumber,
    converted.doubleNumber,
  ])("refuses array representations that change scalar types or null elements", (column) => {
    expect(() =>
      translate("postgresql", INDEX, { mapper: { [reference]: { column, indexable: "pgArray" } } }),
    ).toThrow("without custom decoding");
  });
});

/**
 * The rows the adapter's subquery sees must equal the rows the application put into the resource
 * attributes. The adapter reads the relation table directly, so a soft-delete flag or tenant column
 * the application applies to its own reads does NOT reach the generated EXISTS unless the caller
 * declares it (README, "Mapping hazards"; cerbos/query-plan-adapters#314).
 */
describe("relation subqueryFilter", () => {
  const schema = postgresSchema();
  const VISIBLE_ONLY = ne(schema.tags.tagId, "hidden");
  const DECLARATION = '"tag_id" <> ';

  const mapperFor = (subqueryFilter?: SQL): Mapper => ({
    "request.resource.attr.tags": {
      relation: {
        type: "many",
        table: schema.tags,
        sourceColumn: schema.resources.id,
        targetColumn: schema.tags.resourceId,
        field: schema.tags.name,
        fields: { name: schema.tags.name, id: schema.tags.tagId },
        ...(subqueryFilter ? { subqueryFilter } : {}),
      },
    },
  });

  const sqlFor = (id: string, subqueryFilter?: SQL): string =>
    render("postgresql", filterFor("postgresql", id, { mapper: mapperFor(subqueryFilter) })).sql;

  const occurrences = (haystack: string, needle: string): number =>
    haystack.split(needle).length - 1;

  test("declared: exists() examines only the records the application serialised", () => {
    // Two correlated subqueries — the witness and the UNKNOWN probe — and both must be narrowed.
    const declared = sqlFor("collection/exists/empty-collection", VISIBLE_ONLY);
    expect(occurrences(declared, "exists (select 1")).toEqual(2);
    expect(occurrences(declared, DECLARATION)).toEqual(2);
  });

  test("declared: all() narrows the records examined, not the records required", () => {
    const declared = sqlFor("collection/all/empty-collection", VISIBLE_ONLY);
    expect(occurrences(declared, "exists (select 1")).toEqual(2);
    expect(occurrences(declared, DECLARATION)).toEqual(2);
  });

  test("declared: a count sees only the records the application serialised", () => {
    expect(sqlFor("size/greater-than/collection-above-one", VISIBLE_ONLY)).toContain(DECLARATION);
  });

  test("undeclared: silence adds no clause", () => {
    expect(sqlFor("collection/exists/empty-collection")).not.toContain(DECLARATION);
  });
});

describe("nullAttributeRepresentation", () => {
  // `aOptionalString == null`: the planner emits the same `eq(attr, null)` whichever convention
  // the caller uses, so the adapter has to be told.
  const NULL_EQ_MISSING = "null/equals/null-literal-on-missing-attribute";
  const OMITTED = /under nullAttributeRepresentation "omitted"/;

  test("explicit: a null operand becomes an IS NULL filter", () => {
    expect(
      render(
        "postgresql",
        filterFor("postgresql", NULL_EQ_MISSING, {
          mapper: UNDECLARED,
          nullAttributeRepresentation: "explicit",
        }),
      ).sql,
    ).toContain('"a_optional_string" is null');
  });

  test("omitted: the same plan is refused rather than translated", () => {
    // A NULL column sends no attribute, so check() denies on a missing-attribute error while the
    // filter above would return exactly those rows (#302).
    expect(() =>
      translate("postgresql", NULL_EQ_MISSING, {
        mapper: UNDECLARED,
        nullAttributeRepresentation: "omitted",
      }),
    ).toThrow(UnsupportedQueryPlanError);
    expect(() =>
      translate("postgresql", NULL_EQ_MISSING, {
        mapper: UNDECLARED,
        nullAttributeRepresentation: "omitted",
      }),
    ).toThrow(OMITTED);
  });

  // #308. A per-attribute declaration overrides the call-level option in both directions.
  test("a per-attribute declaration overrides the call-level option", () => {
    // `owner` declares "explicit", so a call-level "omitted" does not reach it.
    const nullEq = "null/equals/null-literal";
    expect(
      render("postgresql", filterFor("postgresql", nullEq, { nullAttributeRepresentation: "omitted" })),
    ).toEqual(render("postgresql", filterFor("postgresql", nullEq)));
    expect(() =>
      translate("postgresql", nullEq, { mapper: UNDECLARED, nullAttributeRepresentation: "omitted" }),
    ).toThrow(OMITTED);

    // `aOptionalString` declares "omitted", so a call-level "explicit" does not reach it either.
    expect(() =>
      translate("postgresql", NULL_EQ_MISSING, { nullAttributeRepresentation: "explicit" }),
    ).toThrow(OMITTED);
  });

  test.each(["eq", "ne"])("mixed scalar types preserve explicit-null %s", (operator) => {
    const resources = postgresSchema().resources;
    const mapper: Mapper = {
      ...MAPPERS.postgresql,
      "request.resource.attr.aString": {
        column: resources.aOptionalString,
        nullAttributeRepresentation: "explicit",
      },
      "request.resource.id": { column: resources.aNumber, nullAttributeRepresentation: "explicit" },
    };
    const id =
      operator === "eq" ? "identifier/equals/field-to-field" : "identifier/not-equals/field-to-field";
    const rendered = render("postgresql", filterFor("postgresql", id, { mapper }));
    expect(rendered.sql).toContain(
      '"adversarial_resources"."a_optional_string" is null and "adversarial_resources"."a_number" is null',
    );
    expect(rendered.sql.includes("not ")).toBe(operator === "ne");
  });

  test.each(["explicit", "omitted"] as const)(
    "a reentrant mapper preserves the outer %s representation",
    (outer) => {
      const inner = outer === "explicit" ? "omitted" : "explicit";
      const mapper: Mapper = (reference) => {
        translate("postgresql", "comparison/less-or-equal/value-first", {
          nullAttributeRepresentation: inner,
        });
        return UNDECLARED[reference];
      };
      const run = () =>
        translate("postgresql", NULL_EQ_MISSING, { mapper, nullAttributeRepresentation: outer });
      if (outer === "omitted") {
        expect(run).toThrow(OMITTED);
      } else {
        expect(run()).toEqual(translate("postgresql", NULL_EQ_MISSING, { mapper: UNDECLARED }));
      }
    },
  );

  // #302 completeness: under a call-level "omitted" with no per-attribute declarations, every plan
  // carrying a null literal is refused — keyed off the null OPERAND, not a list of operators — except
  // a null compared with an indexed list ELEMENT, which is a value, not a missing attribute.
  test("under omitted, every null literal is refused unless it compares an indexed element", () => {
    const carriesNull = (node: unknown): boolean => {
      if (typeof node !== "object" || node === null) return false;
      const record = node as Record<string, unknown>;
      if ("value" in record) {
        const value = record["value"];
        return value === null || (Array.isArray(value) && value.includes(null));
      }
      return Object.values(record).some(carriesNull);
    };
    const INDEXED_ELEMENT = [
      "collection/index/first-element-of-string-list-equals-null",
      "null/in/null-literal-in-number-list",
      "null/in/negated-null-literal-in-number-list",
    ];

    const nullCarrying = readGoldens(CURRENT)
      .filter((g) => carriesNull(g.plan))
      .map((g) => g.id);
    expect(nullCarrying).toEqual(expect.arrayContaining([NULL_EQ_MISSING, ...INDEXED_ELEMENT]));

    const notRejected = nullCarrying.filter((id) => {
      const run = () =>
        translate("postgresql", id, { mapper: UNDECLARED, nullAttributeRepresentation: "omitted" });
      if (INDEXED_ELEMENT.includes(id)) {
        run();
        return false;
      }
      try {
        run();
        return true;
      } catch (error) {
        return !OMITTED.test(String(error));
      }
    });
    expect(notRejected).toEqual([]);
  });
});

const timestampPlan = (operator: string, instant: string): PlanResourcesResponse =>
  ({
    kind: PlanKind.CONDITIONAL,
    condition: {
      operator,
      operands: [
        { operator: "timestamp", operands: [{ name: "request.resource.attr.createdAt" }] },
        { operator: "timestamp", operands: [{ value: instant }] },
      ],
    } as unknown as PlanExpressionOperand,
    cerbosCallId: "",
    requestId: "",
    validationErrors: [],
    metadata: undefined,
  }) as PlanResourcesResponse;

describe("timestamp literals", () => {
  // `timestamp/less-than/relative-window` compares against the literal the planner folds
  // `now() - duration("24h")` into, so the instant is the one value a reader chooses: this walks
  // the timestamp literal boundary by substituting it.
  const WINDOW = "timestamp/less-than/relative-window";
  const at = (now: string) => filterFor("postgresql", WINDOW, { now });

  // No seed sits within a grid step of `now()`, so which side of the literal the bound grid point
  // falls on is invisible to the harness: a floor where a ceiling belongs returns the same rows.
  test.each([
    ["postgresql", "2026-08-11T09:13:39.123457Z"],
    ["mysql", "2026-08-11T09:13:39.123457Z"],
    ["sqlite", "2026-08-11T09:13:39.124Z"],
  ] as const)(
    "a nanosecond instant — what the PDP actually folds — bounds `<` by the next grid point (%s)",
    (store, bound) => {
      const filter = filterFor(store, WINDOW, { now: "2026-08-11T09:13:39.123456789Z" });
      expect(render(store, filter).params).toEqual([bound]);
    },
  );

  test.each([
    ["lt", "<", "2026-08-11T09:13:39.123457Z"],
    ["le", "<=", "2026-08-11T09:13:39.123456Z"],
    ["gt", ">", "2026-08-11T09:13:39.123456Z"],
    ["ge", ">=", "2026-08-11T09:13:39.123457Z"],
  ])("`%s` against an off-grid instant compares with the grid point on its side", (operator, symbol, bound) => {
    const filter = queryPlanToDrizzle({
      queryPlan: timestampPlan(operator, "2026-08-11T09:13:39.123456789Z"),
      mapper: MAPPERS.postgresql,
    });
    if (filter.kind !== PlanKind.CONDITIONAL) throw new Error("expected a filter");
    const rendered = render("postgresql", filter.filter);
    expect(rendered.sql).toContain(` ${symbol} $1`);
    expect(rendered.params).toEqual([bound]);
  });

  test.each(["eq", "ne"])("`%s` against an off-grid instant binds no instant at all", (operator) => {
    const filter = queryPlanToDrizzle({
      queryPlan: timestampPlan(operator, "2026-08-11T09:13:39.123456789Z"),
      mapper: MAPPERS.postgresql,
    });
    if (filter.kind !== PlanKind.CONDITIONAL) throw new Error("expected a filter");
    expect(render("postgresql", filter.filter).params).toEqual([]);
  });

  test("the same plan at millisecond precision translates", () => {
    expect(render("postgresql", at("2026-08-11T09:13:39.123Z")).params).toEqual([
      "2026-08-11T09:13:39.123Z",
    ]);
  });

  test("excess fractional digits are accepted only when they are zero", () => {
    expect(render("postgresql", at("2026-08-11T09:13:39.123000Z")).params).toEqual([
      "2026-08-11T09:13:39.123Z",
    ]);
  });

  // Refused rather than coerced: a Date parsed from a lenient string would compare against the
  // column as some other instant.
  test.each([
    ["a date with no time part", "2024-01-01"],
    ["a year outside CEL's instant range", "0000-01-01T00:00:00Z"],
    ["a day that does not exist", "2024-02-30T00:00:00Z"],
    ["an offset that pushes past the maximum instant", "9999-12-31T23:00:00-02:00"],
  ])("%s fails closed", (_label, value) => {
    expect(() => at(value)).toThrow(/RFC-3339|millisecond|instant range/);
  });
});

describe("what the stores cannot show", () => {
  // Properties of the rendering that returned rows cannot reveal, checked over every current golden
  // plan this adapter translates.
  const translated = readGoldens(CURRENT).flatMap((g) => {
    try {
      const result = queryPlanToDrizzle({ queryPlan: planOf(g), mapper: MAPPERS.mysql });
      return result.kind === PlanKind.CONDITIONAL ? [[g.id, result.filter] as const] : [];
    } catch (error) {
      if (error instanceof UnsupportedQueryPlanError) return [];
      throw error;
    }
  });

  // `real` is single precision on PostgreSQL: it would round a CEL double on the way through a
  // division, and whether a seed's value survives that is an accident of the seeds.
  test("casts to 53-bit floating point, never to single precision", () => {
    const rendered = translated.map(([id, filter]) => [id, render("mysql", filter).sql] as const);
    expect(rendered.filter(([, text]) => text.includes(" as real")).map(([id]) => id)).toEqual([]);
    expect(rendered.some(([, text]) => text.includes("as float(53)"))).toBe(true);
  });

  // PostgreSQL parses 'NaN' and 'Infinity' as double precision inputs and every comparison against
  // them is false — the same rows a folded translation returns, so only the parameters tell.
  test.each(STORES)("binds no non-finite number (%s)", (store) => {
    const offenders = readGoldens(CURRENT).filter((g) => {
      try {
        const result = queryPlanToDrizzle({ queryPlan: planOf(g), mapper: MAPPERS[store] });
        return (
          result.kind === PlanKind.CONDITIONAL &&
          render(store, result.filter).params.some(
            (param) => typeof param === "number" && !Number.isFinite(param),
          )
        );
      } catch (error) {
        if (error instanceof UnsupportedQueryPlanError) return false;
        throw error;
      }
    });
    expect(offenders.map((g) => g.id)).toEqual([]);
  });
});

// KIND 3 — corpus gap (cerbos/query-plan-adapters#509). `string(R.attr.aDouble) == "1e+06"` is
// policy-reachable, but the corpus's one `string()`-of-a-double case has a single witness, -0.6,
// so no case proves the exponent layout or the refusal of a spelling CEL never produces. Delete
// this block when cases carrying those spellings land.
describe("CEL's string() of a double", () => {
  // Every expected string is Go's own `fmt.Sprintf("%g", d)`, cel-go's spelling of the conversion.
  test.each([
    [-0.6, "-0.6"],
    [2, "2"],
    [123456, "123456"],
    [1e6, "1e+06"],
    [1234567, "1.234567e+06"],
    [1e21, "1e+21"],
    [0.0001, "0.0001"],
    [0.00001, "1e-05"],
    [1.5e-5, "1.5e-05"],
    [0.1 + 0.2, "0.30000000000000004"],
    [100000.5, "100000.5"],
    [Number.MAX_VALUE, "1.7976931348623157e+308"],
    [5e-324, "5e-324"],
  ])("Corpus gap. %p is spelled %p", (value, spelling) => {
    expect(formatCelDouble(value)).toBe(spelling);
    expect(parseCelDoubleString(spelling)).toBe(value);
  });

  test.each(["2.0", "1e6", "1E+06", " 2", "+2", "0x10", "", ".5", "Infinity"])(
    "Corpus gap. no double is spelled %p, so equality with it matches no row",
    (spelling) => {
      expect(parseCelDoubleString(spelling)).toBeUndefined();
    },
  );
});

// KIND 3 — corpus gap (cerbos/query-plan-adapters#509). `R.attr.aString in {"one": 1}` is
// policy-reachable, and no case asks it: CEL's `in` over a map tests its keys. Delete this block
// when a case carrying a map literal as the collection of `in` lands.
describe("membership in a map literal", () => {
  test("Corpus gap. tests the map's keys, not its values", () => {
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "in",
        operands: [
          { name: "request.resource.attr.aString" },
          {
            operator: "struct",
            operands: [
              { operator: "set-field", operands: [{ value: "one" }, { value: 1 }] },
              { operator: "set-field", operands: [{ value: "two" }, { value: "three" }] },
            ],
          },
        ],
      } as unknown as PlanExpressionOperand,
      cerbosCallId: "",
      requestId: "",
      validationErrors: [],
      metadata: undefined,
    } as PlanResourcesResponse;
    const result = queryPlanToDrizzle({ queryPlan, mapper: MAPPERS.postgresql });
    if (result.kind !== PlanKind.CONDITIONAL) throw new Error("expected a filter");
    expect(render("postgresql", result.filter).params).toEqual(["one", "two"]);
  });
});

// KIND 3 — corpus gap (cerbos/query-plan-adapters#509). Every pattern here is policy-reachable, and
// the corpus's regex cases witness only one pattern per rule. These pin the RE2 rules the
// lowering relies on that no case exercises yet. Delete each when a case carrying it lands.
describe("RE2 patterns lowered without a regex engine", () => {
  test.each([
    ["a+b", [{ kind: "contains", literals: ["ab"] }]],
    ["x*ab*", [{ kind: "contains", literals: ["a"] }]],
    ["^a.*", [{ kind: "startsWith", literals: ["a"] }]],
    ["^(?:a|b)c?$", [{ kind: "equals", literals: ["a", "ac", "b", "bc"] }]],
    ["^\\.$", [{ kind: "equals", literals: ["."] }]],
    ["^[[:digit:]x-z]{2,}$", [{ kind: "allCharactersIn", characters: [..."0123456789xyz"], min: 2 }]],
    ["^.+$", [{ kind: "noNewline", min: 1 }]],
    ["a|", [{ kind: "contains", literals: ["a"] }, { kind: "contains", literals: [""] }]],
  ])("Corpus gap. %p lowers to %p", (pattern, plans) => {
    expect(compileRegex(pattern)).toEqual(plans);
  });

  test("Corpus gap. (?i) folds k and s the way RE2 does, through KELVIN SIGN and LONG S", () => {
    expect(compileRegex("(?i)^ks$")).toEqual([
      {
        kind: "equals",
        literals: ["ks", "kS", "k\u017F", "Ks", "KS", "K\u017F", "\u212As", "\u212AS", "\u212A\u017F"],
      },
    ]);
  });

  // Each is rejected by Go's regexp.Compile, so CEL raises when it evaluates matches().
  test.each(["a(?=b)", "a(?!b)", "(?<=a)b", "a**", "{2}a", "*a", "a{2,1}", "a{1001}", "(a", "[b-a]", "a\\"])(
    "Corpus gap. %p is an RE2 error, so the condition is UNKNOWN",
    (pattern) => {
      expect(compileRegex(pattern)).toBe("error");
    },
  );

  test("Corpus gap. a brace that opens no count is a literal", () => {
    expect(compileRegex("^a{,2}$")).toEqual([{ kind: "equals", literals: ["a{,2}"] }]);
  });

  test.each(["[^a]", "\\D", "a.b", "^a.*b.*c$", "(?i)^é$", "(?s)a", "\\bword", "^a$b"])(
    "Corpus gap. %p is refused",
    (pattern) => {
      expect(() => compileRegex(pattern)).toThrow(UnsupportedQueryPlanError);
    },
  );
});

// KIND 3 — corpus gap (cerbos/query-plan-adapters#509). Only descendentOf is carried by a case for
// a hierarchy split on an empty delimiter. Delete each test when a case carrying it lands.
describe("a hierarchy split per character", () => {
  const characterHierarchy = (operator: string): PlanResourcesResponse =>
    ({
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator,
        operands: [
          {
            operator: "hierarchy",
            operands: [{ name: "request.resource.attr.scope" }, { value: "" }],
          },
          { operator: "hierarchy", operands: [{ value: "ab" }, { value: "" }] },
        ],
      } as unknown as PlanExpressionOperand,
      cerbosCallId: "",
      requestId: "",
      validationErrors: [],
      metadata: undefined,
    }) as PlanResourcesResponse;
  const paramsOf = (operator: string): unknown[] => {
    const result = queryPlanToDrizzle({
      queryPlan: characterHierarchy(operator),
      mapper: MAPPERS.postgresql,
    });
    if (result.kind !== PlanKind.CONDITIONAL) throw new Error("expected a filter");
    return render("postgresql", result.filter).params;
  };

  // Go's strings.Split("", "") is zero segments, which is every longer path's ancestor.
  test("Corpus gap. ancestorOf is one of the strict prefixes, the empty string included", () => {
    expect(paramsOf("ancestorOf")).toEqual(["", "a"]);
  });

  test("Corpus gap. overlaps is any prefix, or an extension", () => {
    expect(paramsOf("overlaps")).toEqual(["", "a", "ab", "ab", "ab"]);
  });
});

// KIND 3 — corpus gap (cerbos/query-plan-adapters#509). int() over a string or double column is
// carried by cases only against small constants. Delete each test when a case carrying it lands.
describe("int() over a string or double column", () => {
  const intPlan = (condition: unknown): PlanResourcesResponse =>
    ({
      kind: PlanKind.CONDITIONAL,
      condition: condition as PlanExpressionOperand,
      cerbosCallId: "",
      requestId: "",
      validationErrors: [],
      metadata: undefined,
    }) as PlanResourcesResponse;
  const intOf = { operator: "int", operands: [{ name: "request.resource.attr.aString" }] };

  // PostgreSQL and MySQL compare a bigint with a double by converting the bigint, which is inexact
  // at 2^53 and beyond: measured, int("9223372036854775807") == 2^63 is true on both.
  test("Corpus gap. is refused against a constant at or beyond 2^53", () => {
    const queryPlan = intPlan({ operator: "eq", operands: [intOf, { value: 2 ** 53 }] });
    expect(() => queryPlanToDrizzle({ queryPlan, mapper: MAPPERS.postgresql })).toThrow(
      UnsupportedQueryPlanError,
    );
  });

  // A result up to 2^63 - 1 would overflow a bigint and fail the whole query.
  test("Corpus gap. is refused inside arithmetic", () => {
    const queryPlan = intPlan({
      operator: "gt",
      operands: [{ operator: "add", operands: [intOf, { value: 1 }] }, { value: 50 }],
    });
    expect(() => queryPlanToDrizzle({ queryPlan, mapper: MAPPERS.postgresql })).toThrow(
      UnsupportedQueryPlanError,
    );
  });
});

describe("plans the planner cannot produce", () => {
  // Input validation on a public function: malformed by construction, so there is no golden to
  // read. A shape CEL *can* express belongs in the corpus instead.
  const plan = (condition: unknown): PlanResourcesResponse =>
    ({
      kind: PlanKind.CONDITIONAL,
      condition: condition as PlanExpressionOperand,
      cerbosCallId: "",
      requestId: "",
      validationErrors: [],
      metadata: undefined,
    }) as PlanResourcesResponse;

  const lambdaOver = (collection: unknown, field: string) =>
    plan({
      operator: "exists",
      operands: [
        collection,
        {
          operator: "lambda",
          operands: [
            { operator: "eq", operands: [{ name: "request.resource.attr.aString" }, { name: field }] },
            { name: "t" },
          ],
        },
      ],
    });

  test.each([
    ["an unrecognised plan kind", { kind: "INVALID_KIND" } as unknown as PlanResourcesResponse, /Invalid plan kind/],
    ["an unknown operator", plan({ operator: "unsupported", operands: [] }), /Unsupported operator: unsupported/],
    [
      "a collection macro over a value that is not a list",
      lambdaOver({ value: "not-a-list" }, "t"),
      "'exists' over a literal collection requires a list value",
    ],
    [
      "a lambda reading a path no element carries",
      lambdaOver({ value: [{ name: "alpha" }] }, "t.missing"),
      'Cannot resolve "t.missing"',
    ],
  ] as const)("%s is refused", (_label, queryPlan, message) => {
    const run = () => queryPlanToDrizzle({ queryPlan, mapper: MAPPERS.postgresql });
    expect(run).toThrow(UnsupportedQueryPlanError);
    expect(run).toThrow(message);
  });
});
