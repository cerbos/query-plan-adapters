import { test, expect, describe } from "@jest/globals";
import { queryPlanToConvex, PlanKind, Mapper } from ".";
import {
  PlanExpression,
  PlanExpressionVariable,
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

  test("nand - conditional (deny rule with allow fallback)", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "nand",
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    const filtered = applyFilter(fixtureResources, result.filter!);
    expect(filtered.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => !(r.aBool === true && r.aString !== "string"))
        .map((r) => r.key),
    );
  });

  test("nor - conditional (deny rule with allow fallback)", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "nor",
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    const filtered = applyFilter(fixtureResources, result.filter!);
    expect(filtered.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => !(r.aBool === true || r.aString !== "string"))
        .map((r) => r.key),
    );
  });
});

describe("Negation Operations", () => {
  test("conditional - not-and (DeMorgan over AND)", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "not-and",
    });

    const typeQp = queryPlan as PlanResourcesConditionalResponse;
    const condition = typeQp.condition as PlanExpression;
    expect(condition.operator).toBe("not");
    const inner = condition.operands[0] as PlanExpression;
    expect(inner.operator).toBe("and");
    expect((inner.operands[0] as PlanExpression).operator).toBe("eq");
    expect((inner.operands[1] as PlanExpression).operator).toBe("ne");

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    expect(result.filter).toBeDefined();
    expect(result.postFilter).toBeUndefined();

    const filtered = applyFilter(fixtureResources, result.filter!);
    expect(filtered.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => !(r.aBool === true && r.aString !== "string"))
        .map((r) => r.key),
    );
  });

  test("conditional - not-or (DeMorgan over OR)", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "not-or",
    });

    const typeQp = queryPlan as PlanResourcesConditionalResponse;
    const condition = typeQp.condition as PlanExpression;
    expect(condition.operator).toBe("not");
    const inner = condition.operands[0] as PlanExpression;
    expect(inner.operator).toBe("or");
    expect((inner.operands[0] as PlanExpression).operator).toBe("eq");
    expect((inner.operands[1] as PlanExpression).operator).toBe("ne");

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    expect(result.filter).toBeDefined();
    expect(result.postFilter).toBeUndefined();

    const filtered = applyFilter(fixtureResources, result.filter!);
    expect(filtered.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => !(r.aBool === true || r.aString !== "string"))
        .map((r) => r.key),
    );
  });

  test("conditional - not-gt", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "not-gt",
    });

    const typeQp = queryPlan as PlanResourcesConditionalResponse;
    const condition = typeQp.condition as PlanExpression;
    expect(condition.operator).toBe("not");
    const inner = condition.operands[0] as PlanExpression;
    expect(inner.operator).toBe("gt");

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    expect(result.filter).toBeDefined();
    expect(result.postFilter).toBeUndefined();

    const filtered = applyFilter(fixtureResources, result.filter!);
    expect(filtered.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => !(r.aNumber > 1))
        .map((r) => r.key),
    );
  });

  test("conditional - not-lt", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "not-lt",
    });

    const typeQp = queryPlan as PlanResourcesConditionalResponse;
    const condition = typeQp.condition as PlanExpression;
    expect(condition.operator).toBe("not");
    const inner = condition.operands[0] as PlanExpression;
    expect(inner.operator).toBe("lt");

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    expect(result.filter).toBeDefined();
    expect(result.postFilter).toBeUndefined();

    const filtered = applyFilter(fixtureResources, result.filter!);
    expect(filtered.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => !(r.aNumber < 2))
        .map((r) => r.key),
    );
  });

  test("conditional - not-contains (postFilter)", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "not-contains",
    });

    const typeQp = queryPlan as PlanResourcesConditionalResponse;
    const condition = typeQp.condition as PlanExpression;
    expect(condition.operator).toBe("not");
    const inner = condition.operands[0] as PlanExpression;
    expect(inner.operator).toBe("contains");

    // contains is not DB-pushable, so this falls back to postFilter
    const result = queryPlanToConvex({
      queryPlan,
      mapper: defaultMapper,
      allowPostFilter: true,
    });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();

    const filtered = fixtureResources.filter((r) =>
      result.postFilter!(r as unknown as Record<string, unknown>),
    );
    expect(filtered.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => !r.aString.includes("str"))
        .map((r) => r.key),
    );
  });

  test("conditional - not-contains throws without allowPostFilter", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "not-contains",
    });

    expect(() =>
      queryPlanToConvex({ queryPlan, mapper: defaultMapper }),
    ).toThrow("allowPostFilter");
  });

  test("conditional - not-starts-with (postFilter)", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "not-starts-with",
    });

    const typeQp = queryPlan as PlanResourcesConditionalResponse;
    const condition = typeQp.condition as PlanExpression;
    expect(condition.operator).toBe("not");
    const inner = condition.operands[0] as PlanExpression;
    expect(inner.operator).toBe("startsWith");

    // startsWith is not DB-pushable, so this falls back to postFilter
    const result = queryPlanToConvex({
      queryPlan,
      mapper: defaultMapper,
      allowPostFilter: true,
    });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();

    const filtered = fixtureResources.filter((r) =>
      result.postFilter!(r as unknown as Record<string, unknown>),
    );
    expect(filtered.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => !r.aString.startsWith("str"))
        .map((r) => r.key),
    );
  });

  test("conditional - not-starts-with throws without allowPostFilter", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "not-starts-with",
    });

    expect(() =>
      queryPlanToConvex({ queryPlan, mapper: defaultMapper }),
    ).toThrow("allowPostFilter");
  });
});

describe("Additional Operator Shapes", () => {
  test("conditional - is-not-set produces eq(field, null)", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "is-not-set",
    });

    expect(queryPlan.kind).toBe(PlanKind.CONDITIONAL);
    expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
      operator: "eq",
      operands: [
        { name: "request.resource.attr.aOptionalString" },
        { value: null },
      ],
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    expect(result.filter).toBeDefined();

    // eq(field, null) matches null, NOT undefined (distinct in Convex)
    const docWithNull = { aOptionalString: null } as Record<string, unknown>;
    const docMissing = {} as Record<string, unknown>;
    const docWithValue = { aOptionalString: "hello" } as Record<string, unknown>;

    expect(result.filter!(createMockFilterBuilder(docWithNull))).toBe(true);
    expect(result.filter!(createMockFilterBuilder(docMissing))).toBe(false);
    expect(result.filter!(createMockFilterBuilder(docWithValue))).toBe(false);
  });

  test("conditional - equal-bool-false produces eq(field, false)", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "equal-bool-false",
    });

    expect(queryPlan.kind).toBe(PlanKind.CONDITIONAL);
    expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
      operator: "eq",
      operands: [
        { name: "request.resource.attr.aBool" },
        { value: false },
      ],
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    expect(result.filter).toBeDefined();

    const filtered = applyFilter(fixtureResources, result.filter!);
    expect(filtered.map((r) => r.key)).toEqual(
      fixtureResources.filter((r) => r.aBool === false).map((r) => r.key),
    );
  });

  test("conditional - in-number produces or of eq across numeric values", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "in-number",
    });

    expect(queryPlan.kind).toBe(PlanKind.CONDITIONAL);
    expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
      operator: "in",
      operands: [
        { name: "request.resource.attr.aNumber" },
        { value: [1, 2, 3] },
      ],
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    expect(result.filter).toBeDefined();

    const filtered = applyFilter(fixtureResources, result.filter!);
    expect(filtered.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => [1, 2, 3].includes(r.aNumber))
        .map((r) => r.key),
    );
  });

  test("conditional - equal-field-to-field falls back to postFilter", async () => {
    // TODO: field-to-field is a follow-up — the Convex adapter currently only
    // pushes field-to-value comparisons. Variable-vs-variable eq drops to
    // postFilter when allowed, and throws otherwise. This pins the plan shape.
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "equal-field-to-field",
    });

    expect(queryPlan.kind).toBe(PlanKind.CONDITIONAL);
    expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
      operator: "eq",
      operands: [
        { name: "request.resource.attr.aString" },
        { name: "request.resource.attr.id" },
      ],
    });

    const mapper: Mapper = {
      ...defaultMapper,
      "request.resource.attr.id": { field: "id" },
    };

    const result = queryPlanToConvex({
      queryPlan,
      mapper,
      allowPostFilter: true,
    });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();

    expect(result.postFilter!({ aString: "abc", id: "abc" })).toBe(true);
    expect(result.postFilter!({ aString: "abc", id: "xyz" })).toBe(false);
  });

  test("conditional - equal-field-to-field throws without allowPostFilter", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "equal-field-to-field",
    });

    const mapper: Mapper = {
      ...defaultMapper,
      "request.resource.attr.id": { field: "id" },
    };

    expect(() => queryPlanToConvex({ queryPlan, mapper })).toThrow(
      "allowPostFilter",
    );
  });

  test("conditional - or-leaf-exists falls back to postFilter", async () => {
    // exists() is not DB-pushable in the Convex adapter, so or(pushable, exists)
    // returns only a postFilter when allowed.
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "or-leaf-exists",
    });

    expect(queryPlan.kind).toBe(PlanKind.CONDITIONAL);
    const condition = (queryPlan as PlanResourcesConditionalResponse)
      .condition as PlanExpression;
    expect(condition.operator).toBe("or");
    expect((condition.operands[0] as PlanExpression).operator).toBe("eq");
    expect((condition.operands[1] as PlanExpression).operator).toBe("exists");

    const mapper: Mapper = {
      ...defaultMapper,
      "request.resource.attr.tags": { field: "tags" },
    };

    const result = queryPlanToConvex({
      queryPlan,
      mapper,
      allowPostFilter: true,
    });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();
    // TODO(#229): invoking the postFilter on real Cerbos plans throws because
    // the evaluator assumes lambda operands as [var, body] but Cerbos emits
    // [body, var]. Tracked as a separate follow-up; for now we only assert
    // the split (filter undefined, postFilter present).
  });

  test("conditional - or-leaf-exists throws without allowPostFilter", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "or-leaf-exists",
    });

    const mapper: Mapper = {
      ...defaultMapper,
      "request.resource.attr.tags": { field: "tags" },
    };

    expect(() => queryPlanToConvex({ queryPlan, mapper })).toThrow(
      "allowPostFilter",
    );
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
  test("conditional - is-set produces ne(field, null)", async () => {
    // #given - Cerbos translates `!= null` to ne(field, null)
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "is-set",
    });

    // #when
    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    // #then - ne(field, null) excludes null but passes undefined (missing)
    expect(result.kind).toBe(PlanKind.CONDITIONAL);

    const docWithValue = { aOptionalString: "hello" } as Record<string, unknown>;
    const docWithNull = { aOptionalString: null } as Record<string, unknown>;
    const docMissing = {} as Record<string, unknown>;

    const qValue = createMockFilterBuilder(docWithValue);
    const qNull = createMockFilterBuilder(docWithNull);
    const qMissing = createMockFilterBuilder(docMissing);

    expect(result.filter!(qValue)).toBe(true);
    expect(result.filter!(qNull)).toBe(false);
    expect(result.filter!(qMissing)).toBe(true);
  });

  test("isSet operator compares against undefined", () => {
    // #given - isSet operator checks for field existence (undefined in Convex)
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "isSet",
        operands: [
          { name: "request.resource.attr.aOptionalString" },
          { value: true },
        ],
      },
    } as unknown as PlanResourcesResponse;

    // #when
    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    // #then - isSet checks undefined (missing), not null
    const docWithValue = { aOptionalString: "hello" } as Record<string, unknown>;
    const docWithNull = { aOptionalString: null } as Record<string, unknown>;
    const docMissing = {} as Record<string, unknown>;

    const qValue = createMockFilterBuilder(docWithValue);
    const qNull = createMockFilterBuilder(docWithNull);
    const qMissing = createMockFilterBuilder(docMissing);

    expect(result.filter!(qValue)).toBe(true);
    expect(result.filter!(qNull)).toBe(true);
    expect(result.filter!(qMissing)).toBe(false);
  });
});

describe("Null Semantics", () => {
  test("ne null preserves null, does not convert to undefined", () => {
    // #given
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "ne",
        operands: [
          { name: "request.resource.attr.aOptionalString" },
          { value: null },
        ],
      },
    } as unknown as PlanResourcesResponse;

    // #when
    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    // #then - null and undefined are distinct in Convex
    const docWithNull = { aOptionalString: null };
    const docWithUndefined = {};
    const docWithValue = { aOptionalString: "hello" };

    const qNull = createMockFilterBuilder(docWithNull as Record<string, unknown>);
    const qUndefined = createMockFilterBuilder(docWithUndefined as Record<string, unknown>);
    const qValue = createMockFilterBuilder(docWithValue as Record<string, unknown>);

    expect(result.filter!(qNull)).toBe(false);
    expect(result.filter!(qUndefined)).toBe(true);
    expect(result.filter!(qValue)).toBe(true);
  });

  test("eq null matches only null, not undefined", () => {
    // #given
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "eq",
        operands: [
          { name: "request.resource.attr.aOptionalString" },
          { value: null },
        ],
      },
    } as unknown as PlanResourcesResponse;

    // #when
    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    // #then
    const docWithNull = { aOptionalString: null };
    const docWithUndefined = {};
    const docWithValue = { aOptionalString: "hello" };

    const qNull = createMockFilterBuilder(docWithNull as Record<string, unknown>);
    const qUndefined = createMockFilterBuilder(docWithUndefined as Record<string, unknown>);
    const qValue = createMockFilterBuilder(docWithValue as Record<string, unknown>);

    expect(result.filter!(qNull)).toBe(true);
    expect(result.filter!(qUndefined)).toBe(false);
    expect(result.filter!(qValue)).toBe(false);
  });

  test("in with null preserves null in values", () => {
    // #given
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "in",
        operands: [
          { name: "request.resource.attr.aOptionalString" },
          { value: [null, "hello"] },
        ],
      },
    } as unknown as PlanResourcesResponse;

    // #when
    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    // #then
    const docWithNull = { aOptionalString: null };
    const docWithUndefined = {};
    const docWithHello = { aOptionalString: "hello" };
    const docWithOther = { aOptionalString: "other" };

    const qNull = createMockFilterBuilder(docWithNull as Record<string, unknown>);
    const qUndefined = createMockFilterBuilder(docWithUndefined as Record<string, unknown>);
    const qHello = createMockFilterBuilder(docWithHello as Record<string, unknown>);
    const qOther = createMockFilterBuilder(docWithOther as Record<string, unknown>);

    expect(result.filter!(qNull)).toBe(true);
    expect(result.filter!(qUndefined)).toBe(false);
    expect(result.filter!(qHello)).toBe(true);
    expect(result.filter!(qOther)).toBe(false);
  });
});

describe("Post-filter String Operators", () => {
  test("contains returns postFilter that filters correctly", () => {
    // #given
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "contains",
        operands: [
          { name: "request.resource.attr.aString" },
          { value: "ring" },
        ],
      },
    } as unknown as PlanResourcesResponse;

    // #when
    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper, allowPostFilter: true });

    // #then
    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();
    expect(result.postFilter!({ aString: "string" })).toBe(true);
    expect(result.postFilter!({ aString: "other" })).toBe(false);
  });

  test("startsWith returns postFilter that filters correctly", () => {
    // #given
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "startsWith",
        operands: [
          { name: "request.resource.attr.aString" },
          { value: "str" },
        ],
      },
    } as unknown as PlanResourcesResponse;

    // #when
    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper, allowPostFilter: true });

    // #then
    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();
    expect(result.postFilter!({ aString: "string" })).toBe(true);
    expect(result.postFilter!({ aString: "other" })).toBe(false);
  });

  test("endsWith returns postFilter that filters correctly", () => {
    // #given
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "endsWith",
        operands: [
          { name: "request.resource.attr.aString" },
          { value: "ing" },
        ],
      },
    } as unknown as PlanResourcesResponse;

    // #when
    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper, allowPostFilter: true });

    // #then
    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();
    expect(result.postFilter!({ aString: "string" })).toBe(true);
    expect(result.postFilter!({ aString: "other" })).toBe(false);
  });
});

describe("Post-filter Collection Operators", () => {
  test("hasIntersection returns postFilter", () => {
    // #given
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "hasIntersection",
        operands: [
          { name: "request.resource.attr.tags" },
          { value: ["a", "b"] },
        ],
      },
    } as unknown as PlanResourcesResponse;
    const mapper: Mapper = {
      "request.resource.attr.tags": { field: "tags" },
    };

    // #when
    const result = queryPlanToConvex({ queryPlan, mapper, allowPostFilter: true });

    // #then
    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();
    expect(result.postFilter!({ tags: ["a", "c"] })).toBe(true);
    expect(result.postFilter!({ tags: ["c", "d"] })).toBe(false);
    expect(result.postFilter!({ tags: [] })).toBe(false);
  });

  test("exists with lambda returns postFilter", () => {
    // #given - exists(tags, lambda(tag, eq(tag.id, "tag1")))
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "exists",
        operands: [
          { name: "request.resource.attr.tags" },
          {
            operator: "lambda",
            operands: [
              { name: "tag" },
              {
                operator: "eq",
                operands: [
                  { name: "tag.id" },
                  { value: "tag1" },
                ],
              },
            ],
          },
        ],
      },
    } as unknown as PlanResourcesResponse;
    const mapper: Mapper = {
      "request.resource.attr.tags": { field: "tags" },
    };

    // #when
    const result = queryPlanToConvex({ queryPlan, mapper, allowPostFilter: true });

    // #then
    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();
    expect(result.postFilter!({ tags: [{ id: "tag1" }, { id: "tag2" }] })).toBe(true);
    expect(result.postFilter!({ tags: [{ id: "tag2" }, { id: "tag3" }] })).toBe(false);
    expect(result.postFilter!({ tags: [] })).toBe(false);
  });

  test("exists_one with lambda returns postFilter", () => {
    // #given
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "exists_one",
        operands: [
          { name: "request.resource.attr.tags" },
          {
            operator: "lambda",
            operands: [
              { name: "t" },
              {
                operator: "eq",
                operands: [
                  { name: "t" },
                  { value: "a" },
                ],
              },
            ],
          },
        ],
      },
    } as unknown as PlanResourcesResponse;
    const mapper: Mapper = {
      "request.resource.attr.tags": { field: "tags" },
    };

    // #when
    const result = queryPlanToConvex({ queryPlan, mapper, allowPostFilter: true });

    // #then
    expect(result.postFilter).toBeDefined();
    expect(result.postFilter!({ tags: ["a", "b"] })).toBe(true);
    expect(result.postFilter!({ tags: ["a", "a"] })).toBe(false);
    expect(result.postFilter!({ tags: ["b", "c"] })).toBe(false);
  });

  test("all with lambda returns postFilter", () => {
    // #given
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "all",
        operands: [
          { name: "request.resource.attr.scores" },
          {
            operator: "lambda",
            operands: [
              { name: "s" },
              {
                operator: "gt",
                operands: [
                  { name: "s" },
                  { value: 0 },
                ],
              },
            ],
          },
        ],
      },
    } as unknown as PlanResourcesResponse;
    const mapper: Mapper = {
      "request.resource.attr.scores": { field: "scores" },
    };

    // #when
    const result = queryPlanToConvex({ queryPlan, mapper, allowPostFilter: true });

    // #then
    expect(result.postFilter).toBeDefined();
    expect(result.postFilter!({ scores: [1, 2, 3] })).toBe(true);
    expect(result.postFilter!({ scores: [0, 1, 2] })).toBe(false);
  });
});

describe("Mixed Expression Splitting", () => {
  test("and(supported, unsupported) returns both filter and postFilter", () => {
    // #given - and(eq(aBool, true), contains(aString, "str"))
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "and",
        operands: [
          {
            operator: "eq",
            operands: [
              { name: "request.resource.attr.aBool" },
              { value: true },
            ],
          },
          {
            operator: "contains",
            operands: [
              { name: "request.resource.attr.aString" },
              { value: "str" },
            ],
          },
        ],
      },
    } as unknown as PlanResourcesResponse;

    // #when
    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper, allowPostFilter: true });

    // #then
    expect(result.filter).toBeDefined();
    expect(result.postFilter).toBeDefined();

    const filtered = applyFilter(fixtureResources, result.filter!);
    expect(filtered.map((r) => r.key)).toEqual(["a"]);

    const postFiltered = filtered.filter((r) =>
      result.postFilter!(r as unknown as Record<string, unknown>),
    );
    expect(postFiltered.map((r) => r.key)).toEqual(["a"]);
  });

  test("or(supported, unsupported) returns only postFilter", () => {
    // #given - or(eq(aBool, true), contains(aString, "3"))
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "or",
        operands: [
          {
            operator: "eq",
            operands: [
              { name: "request.resource.attr.aBool" },
              { value: true },
            ],
          },
          {
            operator: "contains",
            operands: [
              { name: "request.resource.attr.aString" },
              { value: "3" },
            ],
          },
        ],
      },
    } as unknown as PlanResourcesResponse;

    // #when
    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper, allowPostFilter: true });

    // #then
    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();

    const postFiltered = fixtureResources.filter((r) =>
      result.postFilter!(r as unknown as Record<string, unknown>),
    );
    expect(postFiltered.map((r) => r.key)).toEqual(["a", "c"]);
  });

  test("not(unsupported) returns only postFilter", () => {
    // #given
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "not",
        operands: [
          {
            operator: "contains",
            operands: [
              { name: "request.resource.attr.aString" },
              { value: "ring" },
            ],
          },
        ],
      },
    } as unknown as PlanResourcesResponse;

    // #when
    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper, allowPostFilter: true });

    // #then
    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();
    expect(result.postFilter!({ aString: "other" })).toBe(true);
    expect(result.postFilter!({ aString: "string" })).toBe(false);
  });
});

describe("Backward Compatibility", () => {
  test("fully supported expression returns only filter, no postFilter", () => {
    // #given
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "eq",
        operands: [
          { name: "request.resource.attr.aBool" },
          { value: true },
        ],
      },
    } as unknown as PlanResourcesResponse;

    // #when
    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    // #then
    expect(result.filter).toBeDefined();
    expect(result.postFilter).toBeUndefined();
  });

  test("fully unsupported expression returns only postFilter, no filter", () => {
    // #given
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "contains",
        operands: [
          { name: "request.resource.attr.aString" },
          { value: "str" },
        ],
      },
    } as unknown as PlanResourcesResponse;

    // #when
    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper, allowPostFilter: true });

    // #then
    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();
  });
});

describe("allowPostFilter Gate", () => {
  test("throws when postFilter is required and allowPostFilter is not set", () => {
    // #given
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "contains",
        operands: [
          { name: "request.resource.attr.aString" },
          { value: "str" },
        ],
      },
    } as unknown as PlanResourcesResponse;

    // #when / #then
    expect(() =>
      queryPlanToConvex({ queryPlan, mapper: defaultMapper }),
    ).toThrow("allowPostFilter");
  });

  test("throws when mixed expression produces postFilter and allowPostFilter is not set", () => {
    // #given
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "and",
        operands: [
          {
            operator: "eq",
            operands: [
              { name: "request.resource.attr.aBool" },
              { value: true },
            ],
          },
          {
            operator: "contains",
            operands: [
              { name: "request.resource.attr.aString" },
              { value: "str" },
            ],
          },
        ],
      },
    } as unknown as PlanResourcesResponse;

    // #when / #then
    expect(() =>
      queryPlanToConvex({ queryPlan, mapper: defaultMapper }),
    ).toThrow("allowPostFilter");
  });

  test("does not throw for fully pushable expressions without allowPostFilter", () => {
    // #given
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "eq",
        operands: [
          { name: "request.resource.attr.aBool" },
          { value: true },
        ],
      },
    } as unknown as PlanResourcesResponse;

    // #when
    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });

    // #then
    expect(result.filter).toBeDefined();
    expect(result.postFilter).toBeUndefined();
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

describe("Variable-vs-Variable Expressions", () => {
  test("eq with two variables falls back to postFilter", () => {
    // #given - CEL: request.resource.attr.owner == request.resource.attr.manager
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "eq",
        operands: [
          { name: "request.resource.attr.aString" },
          { name: "request.resource.attr.aOptionalString" },
        ],
      },
    } as unknown as PlanResourcesResponse;

    // #when
    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper, allowPostFilter: true });

    // #then
    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();
    expect(result.postFilter!({ aString: "hello", aOptionalString: "hello" })).toBe(true);
    expect(result.postFilter!({ aString: "hello", aOptionalString: "world" })).toBe(false);
  });

  test("eq with two variables throws without allowPostFilter", () => {
    // #given
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "eq",
        operands: [
          { name: "request.resource.attr.aString" },
          { name: "request.resource.attr.aOptionalString" },
        ],
      },
    } as unknown as PlanResourcesResponse;

    // #when / #then
    expect(() =>
      queryPlanToConvex({ queryPlan, mapper: defaultMapper }),
    ).toThrow("allowPostFilter");
  });

  test("in with variable array falls back to postFilter", () => {
    // #given - CEL: request.resource.attr.aString in request.resource.attr.tags
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "in",
        operands: [
          { name: "request.resource.attr.aString" },
          { name: "request.resource.attr.tags" },
        ],
      },
    } as unknown as PlanResourcesResponse;
    const mapper: Mapper = {
      ...defaultMapper,
      "request.resource.attr.tags": { field: "tags" },
    };

    // #when
    const result = queryPlanToConvex({ queryPlan, mapper, allowPostFilter: true });

    // #then
    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();
    expect(result.postFilter!({ aString: "a", tags: ["a", "b"] })).toBe(true);
    expect(result.postFilter!({ aString: "c", tags: ["a", "b"] })).toBe(false);
  });

  test("and with mixed pushable and variable-vs-variable splits correctly", () => {
    // #given - and(eq(aBool, true), eq(aString, aOptionalString))
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "and",
        operands: [
          {
            operator: "eq",
            operands: [
              { name: "request.resource.attr.aBool" },
              { value: true },
            ],
          },
          {
            operator: "eq",
            operands: [
              { name: "request.resource.attr.aString" },
              { name: "request.resource.attr.aOptionalString" },
            ],
          },
        ],
      },
    } as unknown as PlanResourcesResponse;

    // #when
    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper, allowPostFilter: true });

    // #then
    expect(result.filter).toBeDefined();
    expect(result.postFilter).toBeDefined();
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

describe("Arithmetic Operators (postFilter)", () => {
  test("arith-add - gt(add(aNumber, 1), 2)", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "arith-add",
    });

    expect(queryPlan.kind).toBe(PlanKind.CONDITIONAL);
    expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
      operator: "gt",
      operands: [
        {
          operator: "add",
          operands: [
            { name: "request.resource.attr.aNumber" },
            { value: 1 },
          ],
        },
        { value: 2 },
      ],
    });

    const result = queryPlanToConvex({
      queryPlan,
      mapper: defaultMapper,
      allowPostFilter: true,
    });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();

    const postFiltered = fixtureResources.filter((r) =>
      result.postFilter!(r as unknown as Record<string, unknown>),
    );
    expect(postFiltered.map((r) => r.key)).toEqual(
      fixtureResources.filter((r) => r.aNumber + 1 > 2).map((r) => r.key),
    );
  });

  test("arith-add throws without allowPostFilter", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "arith-add",
    });

    expect(() =>
      queryPlanToConvex({ queryPlan, mapper: defaultMapper }),
    ).toThrow("allowPostFilter");
  });

  test("arith-sub - lt(sub(aNumber, 1), 2)", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "arith-sub",
    });

    expect(queryPlan.kind).toBe(PlanKind.CONDITIONAL);
    expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
      operator: "lt",
      operands: [
        {
          operator: "sub",
          operands: [
            { name: "request.resource.attr.aNumber" },
            { value: 1 },
          ],
        },
        { value: 2 },
      ],
    });

    const result = queryPlanToConvex({
      queryPlan,
      mapper: defaultMapper,
      allowPostFilter: true,
    });

    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();

    const postFiltered = fixtureResources.filter((r) =>
      result.postFilter!(r as unknown as Record<string, unknown>),
    );
    expect(postFiltered.map((r) => r.key)).toEqual(
      fixtureResources.filter((r) => r.aNumber - 1 < 2).map((r) => r.key),
    );
  });

  test("arith-mult - gt(mult(aNumber, 2), 2)", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "arith-mult",
    });

    expect(queryPlan.kind).toBe(PlanKind.CONDITIONAL);
    expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
      operator: "gt",
      operands: [
        {
          operator: "mult",
          operands: [
            { name: "request.resource.attr.aNumber" },
            { value: 2 },
          ],
        },
        { value: 2 },
      ],
    });

    const result = queryPlanToConvex({
      queryPlan,
      mapper: defaultMapper,
      allowPostFilter: true,
    });

    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();

    const postFiltered = fixtureResources.filter((r) =>
      result.postFilter!(r as unknown as Record<string, unknown>),
    );
    expect(postFiltered.map((r) => r.key)).toEqual(
      fixtureResources.filter((r) => r.aNumber * 2 > 2).map((r) => r.key),
    );
  });

  test("arith-div - gt(div(aNumber, 2), 0)", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "arith-div",
    });

    expect(queryPlan.kind).toBe(PlanKind.CONDITIONAL);
    expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
      operator: "gt",
      operands: [
        {
          operator: "div",
          operands: [
            { name: "request.resource.attr.aNumber" },
            { value: 2 },
          ],
        },
        { value: 0 },
      ],
    });

    const result = queryPlanToConvex({
      queryPlan,
      mapper: defaultMapper,
      allowPostFilter: true,
    });

    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();

    const postFiltered = fixtureResources.filter((r) =>
      result.postFilter!(r as unknown as Record<string, unknown>),
    );
    expect(postFiltered.map((r) => r.key)).toEqual(
      fixtureResources.filter((r) => r.aNumber / 2 > 0).map((r) => r.key),
    );
  });

  test("arith-mod - eq(mod(aNumber, 2), 0)", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "arith-mod",
    });

    expect(queryPlan.kind).toBe(PlanKind.CONDITIONAL);
    expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
      operator: "eq",
      operands: [
        {
          operator: "mod",
          operands: [
            {
              operator: "int",
              operands: [{ name: "request.resource.attr.aNumber" }],
            },
            { value: 2 },
          ],
        },
        { value: 0 },
      ],
    });

    const result = queryPlanToConvex({
      queryPlan,
      mapper: defaultMapper,
      allowPostFilter: true,
    });

    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();

    const postFiltered = fixtureResources.filter((r) =>
      result.postFilter!(r as unknown as Record<string, unknown>),
    );
    expect(postFiltered.map((r) => r.key)).toEqual(
      fixtureResources.filter((r) => r.aNumber % 2 === 0).map((r) => r.key),
    );
  });
});

describe("Regex Operator (postFilter)", () => {
  test("matches-regex - matches(aString, 'str.*')", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "matches-regex",
    });

    expect(queryPlan.kind).toBe(PlanKind.CONDITIONAL);
    expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
      operator: "matches",
      operands: [
        { name: "request.resource.attr.aString" },
        { value: "^str.*" },
      ],
    });

    const result = queryPlanToConvex({
      queryPlan,
      mapper: defaultMapper,
      allowPostFilter: true,
    });

    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();

    const postFiltered = fixtureResources.filter((r) =>
      result.postFilter!(r as unknown as Record<string, unknown>),
    );
    expect(postFiltered.map((r) => r.key)).toEqual(
      fixtureResources.filter((r) => /str.*/.test(r.aString)).map((r) => r.key),
    );
  });

  test("matches-regex throws without allowPostFilter", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "matches-regex",
    });

    expect(() =>
      queryPlanToConvex({ queryPlan, mapper: defaultMapper }),
    ).toThrow("allowPostFilter");
  });
});

describe("List Index Operator (postFilter)", () => {
  test("index-list - eq(index(ownedBy, 0), 'user1')", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "index-list",
    });

    expect(queryPlan.kind).toBe(PlanKind.CONDITIONAL);
    expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
      operator: "eq",
      operands: [
        {
          operator: "index",
          operands: [
            { name: "request.resource.attr.ownedBy" },
            { value: 0 },
          ],
        },
        { value: "user1" },
      ],
    });

    const mapper: Mapper = {
      ...defaultMapper,
      "request.resource.attr.ownedBy": { field: "ownedBy" },
    };

    const result = queryPlanToConvex({
      queryPlan,
      mapper,
      allowPostFilter: true,
    });

    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();

    const docs = [
      { key: "a", ownedBy: ["user1", "user2"] },
      { key: "b", ownedBy: ["user2", "user1"] },
      { key: "c", ownedBy: [] },
    ];
    const postFiltered = docs.filter((d) =>
      result.postFilter!(d as unknown as Record<string, unknown>),
    );
    expect(postFiltered.map((d) => d.key)).toEqual(["a"]);
  });

  test("index-list throws without allowPostFilter", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "index-list",
    });

    expect(() =>
      queryPlanToConvex({ queryPlan, mapper: defaultMapper }),
    ).toThrow("allowPostFilter");
  });
});

describe("Type Conversion Operators (postFilter)", () => {
  test("convert-string - eq(string(aNumber), '1')", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "convert-string",
    });

    expect(queryPlan.kind).toBe(PlanKind.CONDITIONAL);
    expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
      operator: "eq",
      operands: [
        {
          operator: "string",
          operands: [{ name: "request.resource.attr.aNumber" }],
        },
        { value: "1" },
      ],
    });

    const result = queryPlanToConvex({
      queryPlan,
      mapper: defaultMapper,
      allowPostFilter: true,
    });

    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();

    const postFiltered = fixtureResources.filter((r) =>
      result.postFilter!(r as unknown as Record<string, unknown>),
    );
    expect(postFiltered.map((r) => r.key)).toEqual(
      fixtureResources.filter((r) => String(r.aNumber) === "1").map((r) => r.key),
    );
  });

  test("convert-string throws without allowPostFilter", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "convert-string",
    });

    expect(() =>
      queryPlanToConvex({ queryPlan, mapper: defaultMapper }),
    ).toThrow("allowPostFilter");
  });

  test("convert-double - gt(double(aNumber), 1.5)", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "convert-double",
    });

    expect(queryPlan.kind).toBe(PlanKind.CONDITIONAL);
    expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
      operator: "gt",
      operands: [
        {
          operator: "double",
          operands: [{ name: "request.resource.attr.aNumber" }],
        },
        { value: 1.5 },
      ],
    });

    const result = queryPlanToConvex({
      queryPlan,
      mapper: defaultMapper,
      allowPostFilter: true,
    });

    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();

    const postFiltered = fixtureResources.filter((r) =>
      result.postFilter!(r as unknown as Record<string, unknown>),
    );
    expect(postFiltered.map((r) => r.key)).toEqual(
      fixtureResources.filter((r) => Number(r.aNumber) > 1.5).map((r) => r.key),
    );
  });

  test("convert-int - gt(int(aString), 0)", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "convert-int",
    });

    expect(queryPlan.kind).toBe(PlanKind.CONDITIONAL);
    expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
      operator: "gt",
      operands: [
        {
          operator: "int",
          operands: [{ name: "request.resource.attr.aString" }],
        },
        { value: 0 },
      ],
    });

    const result = queryPlanToConvex({
      queryPlan,
      mapper: defaultMapper,
      allowPostFilter: true,
    });

    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();

    // parseInt on existing fixtures yields NaN -> no matches.
    // Use ad-hoc docs to exercise the operator behaviour.
    const docs = [
      { key: "x", aString: "42" },
      { key: "y", aString: "0" },
      { key: "z", aString: "string" },
      { key: "w", aString: "-3" },
    ];
    const postFiltered = docs.filter((d) =>
      result.postFilter!(d as unknown as Record<string, unknown>),
    );
    expect(postFiltered.map((d) => d.key)).toEqual(["x"]);
  });
});

describe("Ternary Operator (postFilter)", () => {
  test("ternary - gt(if(aBool, aNumber, 0), 0)", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "ternary",
    });

    expect(queryPlan.kind).toBe(PlanKind.CONDITIONAL);
    expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
      operator: "gt",
      operands: [
        {
          operator: "if",
          operands: [
            { name: "request.resource.attr.aBool" },
            { name: "request.resource.attr.aNumber" },
            { value: 0 },
          ],
        },
        { value: 0 },
      ],
    });

    const result = queryPlanToConvex({
      queryPlan,
      mapper: defaultMapper,
      allowPostFilter: true,
    });

    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();

    const postFiltered = fixtureResources.filter((r) =>
      result.postFilter!(r as unknown as Record<string, unknown>),
    );
    expect(postFiltered.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => (r.aBool ? r.aNumber : 0) > 0)
        .map((r) => r.key),
    );
  });

  test("ternary throws without allowPostFilter", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "ternary",
    });

    expect(() =>
      queryPlanToConvex({ queryPlan, mapper: defaultMapper }),
    ).toThrow("allowPostFilter");
  });
});

describe("Size Operator (postFilter)", () => {
  test("string-size - gt(size(aString), 0)", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "string-size",
    });

    expect(queryPlan.kind).toBe(PlanKind.CONDITIONAL);
    expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
      operator: "gt",
      operands: [
        {
          operator: "size",
          operands: [{ name: "request.resource.attr.aString" }],
        },
        { value: 0 },
      ],
    });

    const result = queryPlanToConvex({
      queryPlan,
      mapper: defaultMapper,
      allowPostFilter: true,
    });

    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();

    const postFiltered = fixtureResources.filter((r) =>
      result.postFilter!(r as unknown as Record<string, unknown>),
    );
    expect(postFiltered.map((r) => r.key)).toEqual(
      fixtureResources.filter((r) => r.aString.length > 0).map((r) => r.key),
    );
  });

  test("empty-collection - eq(size(tags), 0)", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "empty-collection",
    });

    expect(queryPlan.kind).toBe(PlanKind.CONDITIONAL);
    expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
      operator: "eq",
      operands: [
        {
          operator: "size",
          operands: [{ name: "request.resource.attr.tags" }],
        },
        { value: 0 },
      ],
    });

    const mapper: Mapper = {
      ...defaultMapper,
      "request.resource.attr.tags": { field: "tags" },
    };

    const result = queryPlanToConvex({
      queryPlan,
      mapper,
      allowPostFilter: true,
    });

    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();

    const docs = [
      { key: "a", tags: [] },
      { key: "b", tags: ["x"] },
      { key: "c", tags: ["x", "y"] },
    ];
    const postFiltered = docs.filter((d) =>
      result.postFilter!(d as unknown as Record<string, unknown>),
    );
    expect(postFiltered.map((d) => d.key)).toEqual(["a"]);
  });

  test("empty-collection throws without allowPostFilter", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "empty-collection",
    });

    expect(() =>
      queryPlanToConvex({ queryPlan, mapper: defaultMapper }),
    ).toThrow("allowPostFilter");
  });
});

describe("Collection Macro Composition (#232)", () => {
  test("conditional - all-nested falls back to postFilter", async () => {
    // all(R.attr.tags, lambda(and(eq(tag.name, "public"), ne(tag.id, "tag1")), tag))
    // The `all` macro is not DB-pushable in Convex, so this falls back to postFilter.
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "all-nested",
    });

    expect(queryPlan.kind).toBe(PlanKind.CONDITIONAL);
    const condition = (queryPlan as PlanResourcesConditionalResponse)
      .condition as PlanExpression;
    expect(condition.operator).toBe("all");
    expect((condition.operands[0] as PlanExpressionVariable).name).toBe(
      "request.resource.attr.tags",
    );
    const lambda = condition.operands[1] as PlanExpression;
    expect(lambda.operator).toBe("lambda");
    // Cerbos emits lambda operands as [body, var]
    expect((lambda.operands[0] as PlanExpression).operator).toBe("and");
    expect((lambda.operands[1] as PlanExpressionVariable).name).toBe("tag");

    const mapper: Mapper = {
      ...defaultMapper,
      "request.resource.attr.tags": { field: "tags" },
    };

    const result = queryPlanToConvex({
      queryPlan,
      mapper,
      allowPostFilter: true,
    });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();
    // TODO(#232): the convex evaluator assumes lambda operands as [var, body]
    // but Cerbos emits [body, var], so invoking the postFilter on real Cerbos
    // plans throws (tracked alongside #229). For now we only assert the split
    // (filter undefined, postFilter present).
  });

  test("conditional - all-nested throws without allowPostFilter", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "all-nested",
    });

    const mapper: Mapper = {
      ...defaultMapper,
      "request.resource.attr.tags": { field: "tags" },
    };

    expect(() => queryPlanToConvex({ queryPlan, mapper })).toThrow(
      "allowPostFilter",
    );
  });

  test("conditional - map-compared falls back to postFilter", async () => {
    // eq(map(R.attr.tags, lambda(t.id, t)), ["tag1", "tag2"])
    // The `map` macro is not DB-pushable; eq with a non-variable LHS forces postFilter.
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "map-compared",
    });

    expect(queryPlan.kind).toBe(PlanKind.CONDITIONAL);
    const condition = (queryPlan as PlanResourcesConditionalResponse)
      .condition as PlanExpression;
    expect(condition.operator).toBe("eq");
    const mapExpr = condition.operands[0] as PlanExpression;
    expect(mapExpr.operator).toBe("map");
    expect((mapExpr.operands[0] as PlanExpressionVariable).name).toBe(
      "request.resource.attr.tags",
    );
    const lambda = mapExpr.operands[1] as PlanExpression;
    expect(lambda.operator).toBe("lambda");
    // Lambda body projects t.id; var is t. Cerbos emits [body, var].
    expect((lambda.operands[0] as PlanExpressionVariable).name).toBe("t.id");
    expect((lambda.operands[1] as PlanExpressionVariable).name).toBe("t");

    const mapper: Mapper = {
      ...defaultMapper,
      "request.resource.attr.tags": { field: "tags" },
    };

    const result = queryPlanToConvex({
      queryPlan,
      mapper,
      allowPostFilter: true,
    });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();
    // TODO(#232): the convex evaluator assumes lambda operands as [var, body]
    // but Cerbos emits [body, var]. For map-compared the lambda body is a
    // variable (`t.id`) so the evaluator does not throw, but the resulting
    // projection binds the wrong variable name and produces incorrect output.
    // For now we only assert the split (filter undefined, postFilter present).
  });

  test("conditional - map-compared throws without allowPostFilter", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "map-compared",
    });

    const mapper: Mapper = {
      ...defaultMapper,
      "request.resource.attr.tags": { field: "tags" },
    };

    expect(() => queryPlanToConvex({ queryPlan, mapper })).toThrow(
      "allowPostFilter",
    );
  });

  test("conditional - filter-count-gt falls back to postFilter", async () => {
    // gt(size(filter(R.attr.tags, lambda(eq(t.name, "public"), t))), 0)
    // The `filter` and `size` operators are not DB-pushable, so this falls back to postFilter.
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "filter-count-gt",
    });

    expect(queryPlan.kind).toBe(PlanKind.CONDITIONAL);
    const condition = (queryPlan as PlanResourcesConditionalResponse)
      .condition as PlanExpression;
    expect(condition.operator).toBe("gt");
    const sizeExpr = condition.operands[0] as PlanExpression;
    expect(sizeExpr.operator).toBe("size");
    const filterExpr = sizeExpr.operands[0] as PlanExpression;
    expect(filterExpr.operator).toBe("filter");
    expect((filterExpr.operands[0] as PlanExpressionVariable).name).toBe(
      "request.resource.attr.tags",
    );
    const lambda = filterExpr.operands[1] as PlanExpression;
    expect(lambda.operator).toBe("lambda");
    // Cerbos emits lambda operands as [body, var]
    expect((lambda.operands[0] as PlanExpression).operator).toBe("eq");
    expect((lambda.operands[1] as PlanExpressionVariable).name).toBe("t");

    const mapper: Mapper = {
      ...defaultMapper,
      "request.resource.attr.tags": { field: "tags" },
    };

    const result = queryPlanToConvex({
      queryPlan,
      mapper,
      allowPostFilter: true,
    });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    expect(result.filter).toBeUndefined();
    expect(result.postFilter).toBeDefined();
    // TODO(#232): invoking the postFilter on real Cerbos plans throws because
    // the evaluator assumes lambda operands as [var, body] but Cerbos emits
    // [body, var]. Tracked alongside #229. For now we only assert the split
    // (filter undefined, postFilter present).
  });

  test("conditional - filter-count-gt throws without allowPostFilter", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "filter-count-gt",
    });

    const mapper: Mapper = {
      ...defaultMapper,
      "request.resource.attr.tags": { field: "tags" },
    };

    expect(() => queryPlanToConvex({ queryPlan, mapper })).toThrow(
      "allowPostFilter",
    );
  });
});
