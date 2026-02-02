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
