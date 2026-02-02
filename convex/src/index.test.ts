import { test, expect, describe } from "@jest/globals";
import { queryPlanToConvex, PlanKind, Mapper } from ".";
import {
  PlanExpression,
  PlanResourcesConditionalResponse,
  PlanResourcesResponse,
} from "@cerbos/core";
import { GRPC as Cerbos } from "@cerbos/grpc";

const cerbos = new Cerbos("127.0.0.1:3593", { tls: false });

interface MockField {
  __fieldPath: string;
}

const isMockField = (v: unknown): v is MockField =>
  typeof v === "object" && v !== null && "__fieldPath" in v;

const getNestedValue = (obj: Record<string, unknown>, path: string): unknown => {
  const parts = path.split(".");
  let current: unknown = obj;
  for (const part of parts) {
    if (current === null || current === undefined) return undefined;
    current = (current as Record<string, unknown>)[part];
  }
  return current;
};

const createMockFilterBuilder = (doc: Record<string, unknown>) => ({
  field: (name: string): MockField => ({ __fieldPath: name }),
  eq: (a: unknown, b: unknown): boolean => {
    const resolvedA = isMockField(a) ? getNestedValue(doc, a.__fieldPath) : a;
    const resolvedB = isMockField(b) ? getNestedValue(doc, b.__fieldPath) : b;
    return resolvedA === resolvedB;
  },
  neq: (a: unknown, b: unknown): boolean => {
    const resolvedA = isMockField(a) ? getNestedValue(doc, a.__fieldPath) : a;
    const resolvedB = isMockField(b) ? getNestedValue(doc, b.__fieldPath) : b;
    return resolvedA !== resolvedB;
  },
  lt: (a: unknown, b: unknown): boolean => {
    const resolvedA = isMockField(a) ? getNestedValue(doc, a.__fieldPath) : a;
    const resolvedB = isMockField(b) ? getNestedValue(doc, b.__fieldPath) : b;
    return (resolvedA as number) < (resolvedB as number);
  },
  lte: (a: unknown, b: unknown): boolean => {
    const resolvedA = isMockField(a) ? getNestedValue(doc, a.__fieldPath) : a;
    const resolvedB = isMockField(b) ? getNestedValue(doc, b.__fieldPath) : b;
    return (resolvedA as number) <= (resolvedB as number);
  },
  gt: (a: unknown, b: unknown): boolean => {
    const resolvedA = isMockField(a) ? getNestedValue(doc, a.__fieldPath) : a;
    const resolvedB = isMockField(b) ? getNestedValue(doc, b.__fieldPath) : b;
    return (resolvedA as number) > (resolvedB as number);
  },
  gte: (a: unknown, b: unknown): boolean => {
    const resolvedA = isMockField(a) ? getNestedValue(doc, a.__fieldPath) : a;
    const resolvedB = isMockField(b) ? getNestedValue(doc, b.__fieldPath) : b;
    return (resolvedA as number) >= (resolvedB as number);
  },
  and: (...args: unknown[]): boolean => args.every(Boolean),
  or: (...args: unknown[]): boolean => args.some(Boolean),
  not: (a: unknown): boolean => !a,
});

interface Resource {
  key: string;
  aBool: boolean;
  aNumber: number;
  aString: string;
  aOptionalString?: string;
  nested: {
    aBool: boolean;
    aNumber: number;
    aString: string;
  };
}

const fixtureResources: Resource[] = [
  {
    key: "a",
    aBool: true,
    aNumber: 1,
    aString: "string",
    aOptionalString: "string",
    nested: { aBool: true, aNumber: 1, aString: "string" },
  },
  {
    key: "b",
    aBool: false,
    aNumber: 2,
    aString: "string2",
    nested: { aBool: true, aNumber: 1, aString: "string" },
  },
  {
    key: "c",
    aBool: false,
    aNumber: 3,
    aString: "string3",
    nested: { aBool: true, aNumber: 1, aString: "string" },
  },
];

const applyFilter = (
  resources: Resource[],
  filter: (q: ReturnType<typeof createMockFilterBuilder>) => unknown,
): Resource[] =>
  resources.filter((r) => {
    const q = createMockFilterBuilder(r as unknown as Record<string, unknown>);
    return filter(q);
  });

const defaultMapper: Mapper = {
  "request.resource.attr.aBool": { field: "aBool" },
  "request.resource.attr.aNumber": { field: "aNumber" },
  "request.resource.attr.aString": { field: "aString" },
  "request.resource.attr.aOptionalString": { field: "aOptionalString" },
  "request.resource.attr.nested.aBool": { field: "nested.aBool" },
  "request.resource.attr.nested.aNumber": { field: "nested.aNumber" },
  "request.resource.attr.nested.aString": { field: "nested.aString" },
};

describe("Core Functionality", () => {
  test("always allowed", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "always-allow",
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    expect(result).toStrictEqual({ kind: PlanKind.ALWAYS_ALLOWED });
  });

  test("always denied", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "always-deny",
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    expect(result).toStrictEqual({ kind: PlanKind.ALWAYS_DENIED });
  });
});

describe("Field Operations", () => {
  test("conditional - eq", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "equal",
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    expect(result.filter).toBeDefined();

    const filtered = applyFilter(fixtureResources, result.filter!);
    expect(filtered.map((r) => r.key)).toEqual(
      fixtureResources.filter((a) => a.aBool).map((r) => r.key),
    );
  });

  test("conditional - eq - inverted order", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "equal",
    });
    const typeQp = queryPlan as PlanResourcesConditionalResponse;
    const condition = typeQp.condition as PlanExpression;
    const [firstOperand, secondOperand] = condition.operands;
    if (!firstOperand || !secondOperand) {
      throw new Error("Expected two operands");
    }

    const invertedQueryPlan: PlanResourcesConditionalResponse = {
      ...typeQp,
      condition: { ...condition, operands: [secondOperand, firstOperand] },
    };

    const result = queryPlanToConvex({
      queryPlan: invertedQueryPlan,
      mapper: defaultMapper,
    });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    const filtered = applyFilter(fixtureResources, result.filter!);
    expect(filtered.map((r) => r.key)).toEqual(
      fixtureResources.filter((a) => a.aBool).map((r) => r.key),
    );
  });

  test("conditional - ne", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "ne",
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    const filtered = applyFilter(fixtureResources, result.filter!);
    expect(filtered.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((a) => a.aString !== "string")
        .map((r) => r.key),
    );
  });

  test("conditional - explicit-deny (not)", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "explicit-deny",
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    const filtered = applyFilter(fixtureResources, result.filter!);
    expect(filtered.map((r) => r.key)).toEqual(
      fixtureResources.filter((a) => !a.aBool).map((r) => r.key),
    );
  });

  test("conditional - gt", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "gt",
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    const filtered = applyFilter(fixtureResources, result.filter!);
    expect(filtered.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => r.aNumber > 1)
        .map((r) => r.key),
    );
  });

  test("conditional - lt", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "lt",
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    const filtered = applyFilter(fixtureResources, result.filter!);
    expect(filtered.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => r.aNumber < 2)
        .map((r) => r.key),
    );
  });

  test("conditional - gte", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "gte",
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    const filtered = applyFilter(fixtureResources, result.filter!);
    expect(filtered.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => r.aNumber >= 1)
        .map((r) => r.key),
    );
  });

  test("conditional - lte", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "lte",
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    const filtered = applyFilter(fixtureResources, result.filter!);
    expect(filtered.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => r.aNumber <= 2)
        .map((r) => r.key),
    );
  });
});

describe("Logical Operations", () => {
  test("conditional - and", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "and",
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    const filtered = applyFilter(fixtureResources, result.filter!);
    expect(filtered.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => r.aBool && r.aString !== "string")
        .map((r) => r.key),
    );
  });

  test("conditional - or", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "or",
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    const filtered = applyFilter(fixtureResources, result.filter!);
    expect(filtered.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => r.aBool || r.aString !== "string")
        .map((r) => r.key),
    );
  });

  test("nand - always denied (no allow rule)", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "nand",
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    expect(result.kind).toBe(PlanKind.ALWAYS_DENIED);
  });

  test("nor - always denied (no allow rule)", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "nor",
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    expect(result.kind).toBe(PlanKind.ALWAYS_DENIED);
  });
});

describe("Collection Operations", () => {
  test("conditional - in", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "in",
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    const filtered = applyFilter(fixtureResources, result.filter!);
    const allowedValues = new Set(["string", "anotherString"]);
    expect(filtered.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => allowedValues.has(r.aString))
        .map((r) => r.key),
    );
  });
});

describe("Nested Fields", () => {
  test("conditional - eq nested", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "equal-nested",
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    const filtered = applyFilter(fixtureResources, result.filter!);
    expect(filtered.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((a) => a.nested.aBool)
        .map((r) => r.key),
    );
  });
});

describe("Bare Boolean Operands", () => {
  test("conditional - bare-bool", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "bare-bool",
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    const filtered = applyFilter(fixtureResources, result.filter!);
    expect(filtered.map((r) => r.key)).toEqual(
      fixtureResources.filter((a) => a.aBool).map((r) => r.key),
    );
  });

  test("conditional - bare-bool-negated", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "bare-bool-negated",
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    const filtered = applyFilter(fixtureResources, result.filter!);
    expect(filtered.map((r) => r.key)).toEqual(
      fixtureResources.filter((a) => !a.aBool).map((r) => r.key),
    );
  });
});

describe("isSet", () => {
  test("conditional - is-set", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "is-set",
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    const filtered = applyFilter(fixtureResources, result.filter!);
    expect(filtered.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((a) => a.aOptionalString !== undefined)
        .map((r) => r.key),
    );
  });
});

describe("Unsupported Operators", () => {
  test("throws for contains", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "contains",
    });

    expect(() =>
      queryPlanToConvex({ queryPlan, mapper: defaultMapper }),
    ).toThrow("Unsupported operator for Convex: contains");
  });

  test("throws for startsWith", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "starts-with",
    });

    expect(() =>
      queryPlanToConvex({ queryPlan, mapper: defaultMapper }),
    ).toThrow("Unsupported operator for Convex: startsWith");
  });

  test("throws for endsWith", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "ends-with",
    });

    expect(() =>
      queryPlanToConvex({ queryPlan, mapper: defaultMapper }),
    ).toThrow("Unsupported operator for Convex: endsWith");
  });

  test("throws for exists (collection)", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "exists",
    });

    expect(() =>
      queryPlanToConvex({
        queryPlan,
        mapper: defaultMapper,
      }),
    ).toThrow(/Unsupported operator for Convex/);
  });
});

describe("Mapper Functions", () => {
  test("function mapper for field names", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "equal",
    });

    const result = queryPlanToConvex({
      queryPlan,
      mapper: (key: string) => ({
        field: key.replace("request.resource.attr.", ""),
      }),
    });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    const filtered = applyFilter(fixtureResources, result.filter!);
    expect(filtered.map((r) => r.key)).toEqual(
      fixtureResources.filter((a) => a.aBool).map((r) => r.key),
    );
  });
});

describe("Error Handling", () => {
  test("throws error for invalid query plan", () => {
    const invalidQueryPlan = { kind: "INVALID_KIND" as PlanKind };

    expect(() =>
      queryPlanToConvex({
        queryPlan: invalidQueryPlan as unknown as PlanResourcesResponse,
        mapper: {},
      }),
    ).toThrow("Invalid query plan.");
  });

  test("throws error for invalid expression structure", () => {
    const invalidQueryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {},
    };

    expect(() =>
      queryPlanToConvex({
        queryPlan: invalidQueryPlan as unknown as PlanResourcesResponse,
        mapper: {},
      }),
    ).toThrow("Invalid Cerbos expression structure");
  });

  test("throws error for unsupported operator", () => {
    const invalidQueryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: { operator: "unsupported", operands: [] },
    };

    expect(() =>
      queryPlanToConvex({
        queryPlan: invalidQueryPlan as unknown as PlanResourcesResponse,
        mapper: {},
      }),
    ).toThrow("Unsupported operator: unsupported");
  });
});
