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

const renderPostgreSQLQuery = (condition: PlanExpressionOperand) =>
  new PgDialect().sqlToQuery(translate(condition));

const renderPostgreSQL = (condition: PlanExpressionOperand): string =>
  renderPostgreSQLQuery(condition).sql;

const renderClaimedDialects = (condition: PlanExpressionOperand): string[] => {
  const filter = translate(condition);
  return [
    new PgDialect().sqlToQuery(filter).sql,
    new MySqlDialect().sqlToQuery(filter).sql,
    new SQLiteSyncDialect().sqlToQuery(filter).sql,
  ];
};

describe("PostgreSQL rendering", () => {
  test.each(["contains", "startsWith", "endsWith"])(
    "%s uses PostgreSQL-supported literal string operations",
    (operator) => {
      const condition = {
        operator,
        operands: [
          { name: "request.resource.attr.title" },
          { name: "request.resource.attr.needle" },
        ],
      } satisfies PlanExpressionOperand;
      const rendered = renderPostgreSQL(condition);

      expect(rendered).not.toContain("instr(");
      expect(rendered).toMatch(/\b(?:replace|substr|length)\(/);
      for (const dialectSQL of renderClaimedDialects(condition)) {
        expect(dialectSQL).not.toContain("instr(");
        expect(dialectSQL).toMatch(/\b(?:replace|substr|length)\(/);
      }
    }
  );

  test("hierarchy prefix matching uses PostgreSQL-supported functions", () => {
    const rendered = renderPostgreSQL({
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
    });

    expect(rendered).not.toContain("instr(");
    expect(rendered).toContain("substr(");
  });

  test("collection tri-state expressions remain boolean-valued", () => {
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
    const rendered = renderPostgreSQL(condition);

    expect(rendered).toContain("then true");
    expect(rendered).toContain("else false");
    expect(rendered).not.toMatch(/\bthen [01]\b|\belse [01]\b/);
    for (const dialectSQL of renderClaimedDialects(condition)) {
      expect(dialectSQL).toContain("then true");
      expect(dialectSQL).toContain("else false");
      expect(dialectSQL).not.toMatch(/\bthen [01]\b|\belse [01]\b/);
    }
  });

  test("CEL arithmetic casts fields to portable 53-bit floating point", () => {
    const condition = {
      operator: "gt",
      operands: [
        {
          operator: "div",
          operands: [
            { name: "request.resource.attr.score" },
            { value: 2 },
          ],
        },
        { value: 0.1 },
      ],
    } satisfies PlanExpressionOperand;
    const rendered = renderPostgreSQL(condition);

    expect(rendered).toContain("as float(53)");
    expect(rendered).not.toContain(" as real");
    for (const dialectSQL of renderClaimedDialects(condition)) {
      expect(dialectSQL).toContain("as float(53)");
      expect(dialectSQL).not.toContain(" as real");
    }
  });

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
              {
                operator: "div",
                operands: [{ value: 0 }, { value: 0 }],
              },
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
              {
                operator: "div",
                operands: [{ value: 1 }, { value: 0 }],
              },
              {
                operator: "div",
                operands: [{ value: -1 }, { value: 0 }],
              },
            ],
          },
          { value: 0.5 },
        ],
      } satisfies PlanExpressionOperand,
    },
  ])("folds $name using CEL/IEEE ordering", ({ condition }) => {
    const rendered = renderPostgreSQLQuery(condition);
    const nonFiniteNumbers = rendered.params.filter(
      (parameter): parameter is number =>
        typeof parameter === "number" && !Number.isFinite(parameter)
    );

    expect(nonFiniteNumbers).toEqual([]);
  });

  test.each([
    {
      name: "field compared with NaN",
      condition: {
        operator: "lt",
        operands: [
          { name: "request.resource.attr.score" },
          {
            operator: "div",
            operands: [{ value: 0 }, { value: 0 }],
          },
        ],
      } satisfies PlanExpressionOperand,
    },
    {
      name: "NaN compared with field",
      condition: {
        operator: "gt",
        operands: [
          {
            operator: "div",
            operands: [{ value: 0 }, { value: 0 }],
          },
          { name: "request.resource.attr.score" },
        ],
      } satisfies PlanExpressionOperand,
    },
  ])("folds one-sided $name without losing NULL semantics", ({ condition }) => {
    const rendered = renderPostgreSQLQuery(condition);
    const nonFiniteNumbers = rendered.params.filter(
      (parameter): parameter is number =>
        typeof parameter === "number" && !Number.isFinite(parameter)
    );

    expect(nonFiniteNumbers).toEqual([]);
    expect(rendered.sql).toContain("is null then null else false");
  });
});
