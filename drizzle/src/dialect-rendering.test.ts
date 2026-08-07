import { describe, expect, test } from "@jest/globals";
import type {
  PlanExpressionOperand,
  PlanResourcesResponse,
  ValidationError,
} from "@cerbos/core";
import { MySqlDialect } from "drizzle-orm/mysql-core/dialect";
import { PgDialect } from "drizzle-orm/pg-core/dialect";
import { boolean, doublePrecision, pgTable, text } from "drizzle-orm/pg-core";
import { SQLiteSyncDialect } from "drizzle-orm/sqlite-core/dialect";

import { PlanKind, queryPlanToDrizzle } from ".";
import type { Mapper, RelationMapping } from ".";

/**
 * What the rendered SQL has to look like, for the properties EXECUTION cannot reach.
 *
 * Most of what this file used to assert is now executed: `adversarial.test.ts` replays the whole
 * conformance corpus against a real PostgreSQL server under `ADAPTER_TEST_DB=postgres`
 * (`npm run test:adversarial:postgres`), so a fragment that renders plausibly but evaluates
 * differently there fails on the rows it returns rather than on a substring. Before that leg
 * existed, "PostgreSQL support" was pinned at the rendered-string level and nothing else
 * (cerbos/query-plan-adapters#320).
 *
 * Three properties survive here, because no executed leg can reach them:
 *
 * 1. **MySQL.** The README and the `drizzle-orm` peer range claim it, but no MySQL server runs
 *    anywhere in this repository. Rendering the same filter through `MySqlDialect` is the only
 *    coverage it has, so every case below renders through all three claimed dialects rather than
 *    PostgreSQL alone. It proves the adapter emits no dialect-exclusive function — `instr()` is
 *    SQLite-only — not that MySQL evaluates the result the way CEL does.
 * 2. **The precision of a cast.** `cast(x as real)` is single precision on PostgreSQL, which
 *    rounds the IEEE doubles CEL arithmetic is defined over. Whether the corpus holds a value
 *    that survives `float(53)` and not `real` is an accident of the seeds; the spelling is not.
 * 3. **What the driver is asked to bind.** PostgreSQL accepts `NaN` as a `double precision`
 *    input, and every comparison against it is false — exactly what the folded translation
 *    produces. An executed leg therefore cannot tell a folded NaN from a bound one; the bound
 *    parameter list can.
 */

const resources = pgTable("resources", {
  id: text("id").primaryKey(),
  title: text("title"),
  needle: text("needle"),
  score: doublePrecision("score"),
  enabled: boolean("enabled"),
});

const tags = pgTable("tags", {
  resourceId: text("resource_id").notNull(),
  name: text("name"),
});

const tagsRelation: RelationMapping = {
  type: "many",
  table: tags,
  sourceColumn: resources.id,
  targetColumn: tags.resourceId,
  field: tags.name,
  fields: { name: tags.name },
};

const mapper: Mapper = {
  "request.resource.attr.title": resources.title,
  "request.resource.attr.needle": resources.needle,
  "request.resource.attr.score": resources.score,
  "request.resource.attr.aBool": resources.enabled,
  "request.resource.attr.tags": { relation: tagsRelation },
};

const buildPlan = (
  condition: PlanExpressionOperand
): PlanResourcesResponse => ({
  cerbosCallId: "call-id",
  requestId: "request-id",
  validationErrors: [] satisfies ValidationError[],
  metadata: undefined,
  kind: PlanKind.CONDITIONAL,
  condition,
});

const translate = (condition: PlanExpressionOperand) => {
  const result = queryPlanToDrizzle({
    queryPlan: buildPlan(condition),
    mapper,
  });
  if (result.kind !== PlanKind.CONDITIONAL) {
    throw new Error(`Expected conditional plan, received ${result.kind}`);
  }
  return result.filter;
};

/**
 * The same filter rendered through every dialect the adapter claims. PostgreSQL and SQLite are
 * also executed end to end by the conformance harness; MySQL is not, which is why the assertions
 * below are written against the whole list rather than one member of it.
 */
const renderClaimedDialects = (
  condition: PlanExpressionOperand
): { dialect: string; sql: string }[] => {
  const filter = translate(condition);
  return [
    { dialect: "postgresql", sql: new PgDialect().sqlToQuery(filter).sql },
    { dialect: "mysql", sql: new MySqlDialect().sqlToQuery(filter).sql },
    { dialect: "sqlite", sql: new SQLiteSyncDialect().sqlToQuery(filter).sql },
  ];
};

const boundParameters = (condition: PlanExpressionOperand): unknown[] =>
  new PgDialect().sqlToQuery(translate(condition)).params;

describe("rendering across the claimed dialects", () => {
  test.each(["contains", "startsWith", "endsWith"])(
    "%s uses no SQLite-only string function",
    (operator) => {
      const condition = {
        operator,
        operands: [
          { name: "request.resource.attr.title" },
          { name: "request.resource.attr.needle" },
        ],
      } satisfies PlanExpressionOperand;

      for (const { dialect, sql } of renderClaimedDialects(condition)) {
        // instr() exists on SQLite and MySQL but not PostgreSQL; replace/substr/length are
        // common to all three. PostgreSQL's evaluation of these operators is proved by the
        // corpus's cr-contains, f2f-* and p-startswith-concat actions.
        expect({ dialect, usesInstr: sql.includes("instr(") }).toEqual({
          dialect,
          usesInstr: false,
        });
        expect(sql).toMatch(/\b(?:replace|substr|length)\(/);
      }
    }
  );

  test("hierarchy prefix matching uses no SQLite-only string function", () => {
    const condition = {
      operator: "descendentOf",
      operands: [
        {
          operator: "hierarchy",
          operands: [{ name: "request.resource.attr.title" }, { value: ":" }],
        },
        {
          operator: "hierarchy",
          operands: [{ value: "[env]:prod" }, { value: ":" }],
        },
      ],
    } satisfies PlanExpressionOperand;

    for (const { dialect, sql } of renderClaimedDialects(condition)) {
      expect({ dialect, usesInstr: sql.includes("instr(") }).toEqual({
        dialect,
        usesInstr: false,
      });
      expect(sql).toContain("substr(");
    }
  });

  test("collection tri-state expressions yield booleans, not integers", () => {
    const exists = {
      operator: "exists",
      operands: [
        { name: "request.resource.attr.tags" },
        {
          operator: "lambda",
          operands: [
            {
              operator: "eq",
              operands: [{ name: "t.name" }, { value: "public" }],
            },
            { name: "t" },
          ],
        },
      ],
    } satisfies PlanExpressionOperand;
    const condition = {
      operator: "not",
      operands: [exists],
    } satisfies PlanExpressionOperand;

    for (const { dialect, sql } of renderClaimedDialects(condition)) {
      // PostgreSQL rejects `case ... then 1 end` where a boolean is required, so its half of
      // this is executed by every corpus action over a collection macro. MySQL accepts the
      // integer form silently, which is what this case is here for.
      expect(sql).toContain("then true");
      expect(sql).toContain("else false");
      expect({
        dialect,
        usesIntegerArms: /\bthen [01]\b|\belse [01]\b/.test(sql),
      }).toEqual({ dialect, usesIntegerArms: false });
    }
  });

  test("CEL arithmetic casts to 53-bit floating point, never to single precision", () => {
    const condition = {
      operator: "gt",
      operands: [
        {
          operator: "div",
          operands: [{ name: "request.resource.attr.score" }, { value: 2 }],
        },
        { value: 0.1 },
      ],
    } satisfies PlanExpressionOperand;

    for (const { dialect, sql } of renderClaimedDialects(condition)) {
      // `real` is 4 bytes on PostgreSQL: it would round a CEL double on the way through the
      // division. Executing the corpus cannot pin this — whether a seed's value survives single
      // precision is an accident of the seeds, not of the translation.
      expect(sql).toContain("as float(53)");
      expect({ dialect, usesSinglePrecision: sql.includes(" as real") }).toEqual(
        { dialect, usesSinglePrecision: false }
      );
    }
  });
});

describe("what the driver is asked to bind", () => {
  // PostgreSQL parses 'NaN' and 'Infinity' as double precision inputs and every comparison
  // against them is false — the same rows a folded translation returns. So an executed leg
  // agrees either way, and only the parameter list distinguishes them.
  test.each([
    {
      name: "NaN in a ternary result",
      condition: {
        operator: "gt",
        operands: [
          {
            operator: "if",
            operands: [
              { name: "request.resource.attr.aBool" },
              { value: 1 },
              { operator: "div", operands: [{ value: 0 }, { value: 0 }] },
            ],
          },
          { value: 0.5 },
        ],
      } satisfies PlanExpressionOperand,
    },
    {
      name: "infinities in ternary branches",
      condition: {
        operator: "gt",
        operands: [
          {
            operator: "if",
            operands: [
              { name: "request.resource.attr.aBool" },
              { operator: "div", operands: [{ value: 1 }, { value: 0 }] },
              { operator: "div", operands: [{ value: -1 }, { value: 0 }] },
            ],
          },
          { value: 0.5 },
        ],
      } satisfies PlanExpressionOperand,
    },
  ])("folds $name using CEL/IEEE ordering", ({ condition }) => {
    const nonFinite = boundParameters(condition).filter(
      (parameter): parameter is number =>
        typeof parameter === "number" && !Number.isFinite(parameter)
    );

    expect(nonFinite).toEqual([]);
  });

  test.each([
    {
      name: "field compared with NaN",
      condition: {
        operator: "lt",
        operands: [
          { name: "request.resource.attr.score" },
          { operator: "div", operands: [{ value: 0 }, { value: 0 }] },
        ],
      } satisfies PlanExpressionOperand,
    },
    {
      name: "NaN compared with field",
      condition: {
        operator: "gt",
        operands: [
          { operator: "div", operands: [{ value: 0 }, { value: 0 }] },
          { name: "request.resource.attr.score" },
        ],
      } satisfies PlanExpressionOperand,
    },
  ])("folds one-sided $name without losing NULL semantics", ({ condition }) => {
    const rendered = new PgDialect().sqlToQuery(translate(condition));
    const nonFinite = rendered.params.filter(
      (parameter): parameter is number =>
        typeof parameter === "number" && !Number.isFinite(parameter)
    );

    expect(nonFinite).toEqual([]);
    // The fold must keep a NULL score UNKNOWN rather than collapsing it to false, which is what
    // makes the row stay excluded under negation too.
    expect(rendered.sql).toContain("is null then null else false");
  });
});
