import {
  PlanExpressionOperand,
  PlanKind,
  PlanResourcesResponse,
} from "@cerbos/core";
import type { ValidationError } from "@cerbos/core";
import { eq, sql } from "drizzle-orm";
import Database from "better-sqlite3";
import { drizzle } from "drizzle-orm/better-sqlite3";
import {
  integer,
  sqliteTable,
  text,
} from "drizzle-orm/sqlite-core";
import { queryPlanToDrizzle } from ".";
import type { MapperEntry, QueryPlanToDrizzleResult } from ".";

type Resource = {
  id: string;
  status: string;
  title: string;
  ownerId: string;
  optional: string | null;
  amount: number;
};

const resources = sqliteTable("resources", {
  id: text("id").primaryKey(),
  status: text("status").notNull(),
  title: text("title").notNull(),
  ownerId: text("owner_id").notNull(),
  optional: text("optional"),
  amount: integer("amount").notNull(),
});

const sqlite = new Database(":memory:");
const db = drizzle(sqlite);

const mapper: Record<string, MapperEntry> = {
  "request.resource.attr.id": resources.id,
  "request.resource.attr.status": resources.status,
  "request.resource.attr.title": resources.title,
  "request.resource.attr.ownerId": resources.ownerId,
  "request.resource.attr.optional": resources.optional,
  "request.resource.attr.amount": resources.amount,
};

const basePlanFields = {
  cerbosCallId: "call-id",
  requestId: "req-id",
  validationErrors: [] as ValidationError[],
  metadata: undefined as PlanResourcesResponse["metadata"],
};

const buildPlan = (
  condition: PlanExpressionOperand
): PlanResourcesResponse => ({
  ...basePlanFields,
  kind: PlanKind.CONDITIONAL,
  condition,
});

const seedData: Resource[] = [
  {
    id: "res-1",
    status: "active",
    title: "Alpha",
    ownerId: "user-1",
    optional: "note",
    amount: 10,
  },
  {
    id: "res-2",
    status: "draft",
    title: "Beta",
    ownerId: "user-1",
    optional: null,
    amount: 20,
  },
  {
    id: "res-3",
    status: "archived",
    title: "Gamma",
    ownerId: "user-2",
    optional: "info",
    amount: 5,
  },
  {
    id: "res-4",
    status: "active",
    title: "Delta",
    ownerId: "user-3",
    optional: null,
    amount: 15,
  },
];

beforeAll(() => {
  sqlite.exec(`
    CREATE TABLE resources (
      id TEXT PRIMARY KEY,
      status TEXT NOT NULL,
      title TEXT NOT NULL,
      owner_id TEXT NOT NULL,
      optional TEXT,
      amount INTEGER NOT NULL
    );
  `);
});

beforeEach(() => {
  sqlite.exec("DELETE FROM resources");
  const insert = sqlite.prepare(
    `INSERT INTO resources (id, status, title, owner_id, optional, amount)
     VALUES (@id, @status, @title, @ownerId, @optional, @amount)`
  );
  const transaction = sqlite.transaction((rows: Resource[]) => {
    for (const row of rows) {
      insert.run(row);
    }
  });
  transaction(seedData);
});

describe("queryPlanToDrizzle", () => {
  const ensureFilter = (result: QueryPlanToDrizzleResult) => {
    if (result.kind !== PlanKind.CONDITIONAL) {
      throw new Error(`Expected conditional plan, received ${result.kind}`);
    }
    return result.filter;
  };

  const selectIds = (where?: any) =>
    db
      .select({ id: resources.id })
      .from(resources)
      .where(where ?? sql`1 = 1`)
      .all()
      .map((row) => row.id);

  test("returns all records for ALWAYS_ALLOWED", () => {
    const plan: PlanResourcesResponse = {
      ...basePlanFields,
      kind: PlanKind.ALWAYS_ALLOWED,
    };
    const result = queryPlanToDrizzle({ queryPlan: plan, mapper });
    expect(result.kind).toBe(PlanKind.ALWAYS_ALLOWED);
  });

  test("returns no filter for ALWAYS_DENIED", () => {
    const plan: PlanResourcesResponse = {
      ...basePlanFields,
      kind: PlanKind.ALWAYS_DENIED,
    };
    const result = queryPlanToDrizzle({ queryPlan: plan, mapper });
    expect(result.kind).toBe(PlanKind.ALWAYS_DENIED);
  });

  test("handles equality comparison", () => {
    const plan = buildPlan({
      operator: "eq",
      operands: [
        { name: "request.resource.attr.status" },
        { value: "active" },
      ],
    });

    const result = queryPlanToDrizzle({ queryPlan: plan, mapper });
    const ids = selectIds(ensureFilter(result));
    expect(ids.sort()).toEqual(["res-1", "res-4"]);
  });

  test("supports logical and nested comparisons", () => {
    const plan = buildPlan({
      operator: "and",
      operands: [
        {
          operator: "eq",
          operands: [
            { name: "request.resource.attr.status" },
            { value: "active" },
          ],
        },
        {
          operator: "gt",
          operands: [
            { name: "request.resource.attr.amount" },
            { value: 12 },
          ],
        },
      ],
    });

    const result = queryPlanToDrizzle({ queryPlan: plan, mapper });
    const ids = selectIds(ensureFilter(result));
    expect(ids).toEqual(["res-4"]);
  });

  test("supports logical OR", () => {
    const plan = buildPlan({
      operator: "or",
      operands: [
        {
          operator: "eq",
          operands: [
            { name: "request.resource.attr.status" },
            { value: "draft" },
          ],
        },
        {
          operator: "eq",
          operands: [
            { name: "request.resource.attr.ownerId" },
            { value: "user-2" },
          ],
        },
      ],
    });

    const result = queryPlanToDrizzle({ queryPlan: plan, mapper });
    const ids = selectIds(ensureFilter(result)).sort();
    expect(ids).toEqual(["res-2", "res-3"]);
  });

  test("supports NOT", () => {
    const plan = buildPlan({
      operator: "not",
      operands: [
        {
          operator: "eq",
          operands: [
            { name: "request.resource.attr.status" },
            { value: "active" },
          ],
        },
      ],
    });

    const result = queryPlanToDrizzle({ queryPlan: plan, mapper });
    const ids = selectIds(ensureFilter(result)).sort();
    expect(ids).toEqual(["res-2", "res-3"]);
  });

  test("supports IN operator", () => {
    const plan = buildPlan({
      operator: "in",
      operands: [
        { name: "request.resource.attr.id" },
        { value: ["res-1", "res-3"] },
      ],
    });

    const result = queryPlanToDrizzle({ queryPlan: plan, mapper });
    const ids = selectIds(ensureFilter(result)).sort();
    expect(ids).toEqual(["res-1", "res-3"]);
  });

  test("supports string operators", () => {
    const containsPlan = buildPlan({
      operator: "contains",
      operands: [
        { name: "request.resource.attr.title" },
        { value: "ta" },
      ],
    });

    const startsPlan = buildPlan({
      operator: "startsWith",
      operands: [
        { name: "request.resource.attr.title" },
        { value: "Al" },
      ],
    });

    const endsPlan = buildPlan({
      operator: "endsWith",
      operands: [
        { name: "request.resource.attr.title" },
        { value: "ma" },
      ],
    });

    expect(
      selectIds(
        ensureFilter(queryPlanToDrizzle({ queryPlan: containsPlan, mapper }))
      ).sort()
    ).toEqual(["res-2", "res-4"]);
    expect(
      selectIds(
        ensureFilter(queryPlanToDrizzle({ queryPlan: startsPlan, mapper }))
      )
    ).toEqual(["res-1"]);
    expect(
      selectIds(
        ensureFilter(queryPlanToDrizzle({ queryPlan: endsPlan, mapper }))
      )
    ).toEqual(["res-3"]);
  });

  test("supports isSet", () => {
    const plan = buildPlan({
      operator: "isSet",
      operands: [
        { name: "request.resource.attr.optional" },
        { value: true },
      ],
    });

    const unsetPlan = buildPlan({
      operator: "isSet",
      operands: [
        { name: "request.resource.attr.optional" },
        { value: false },
      ],
    });

    const setIds = selectIds(
      ensureFilter(queryPlanToDrizzle({ queryPlan: plan, mapper }))
    ).sort();
    const unsetIds = selectIds(
      ensureFilter(queryPlanToDrizzle({ queryPlan: unsetPlan, mapper }))
    ).sort();

    expect(setIds).toEqual(["res-1", "res-3"]);
    expect(unsetIds).toEqual(["res-2", "res-4"]);
  });

  test("allows mapper functions", () => {
    const plan = buildPlan({
      operator: "eq",
      operands: [
        { name: "request.resource.attr.status" },
        { value: "active" },
      ],
    });

    const mapperFn = (reference: string) => mapper[reference];
    const result = queryPlanToDrizzle({ queryPlan: plan, mapper: mapperFn });
    expect(selectIds(ensureFilter(result)).sort()).toEqual(["res-1", "res-4"]);
  });

  test("supports custom transform", () => {
    const plan = buildPlan({
      operator: "eq",
      operands: [
        { name: "request.resource.attr.title" },
        { value: "alpha" },
      ],
    });

    const result = queryPlanToDrizzle({
      queryPlan: plan,
      mapper: {
        ...mapper,
        "request.resource.attr.title": {
          column: resources.title,
          transform: ({ value }) => eq(sql`lower(${resources.title})`, (value as string).toLowerCase()),
        },
      },
    });

    expect(selectIds(ensureFilter(result))).toEqual(["res-1"]);
  });

  test("throws when mapping is missing", () => {
    const plan = buildPlan({
      operator: "eq",
      operands: [
        { name: "request.resource.attr.unknown" },
        { value: "value" },
      ],
    });

    expect(() =>
      queryPlanToDrizzle({
        queryPlan: plan,
        mapper,
      })
    ).toThrow(/No mapping/);
  });
});
