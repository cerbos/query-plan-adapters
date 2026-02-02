import { queryPlanToPrisma, PlanKind, QueryPlanToPrismaResult } from ".";
import {
  beforeAll,
  beforeEach,
  afterEach,
  describe,
  test,
  expect,
} from "@jest/globals";
import {
  PlanExpression,
  PlanExpressionOperand,
  PlanResourcesConditionalResponse,
  PlanResourcesResponse,
} from "@cerbos/core";
import { GRPC as Cerbos } from "@cerbos/grpc";
import { prisma, Prisma } from "./test-setup";

const cerbos = new Cerbos("127.0.0.1:3593", { tls: false });

function createConditionalPlan(
  condition: PlanExpressionOperand
): PlanResourcesConditionalResponse {
  return {
    kind: PlanKind.CONDITIONAL,
    condition,
    cerbosCallId: "",
    requestId: "",
    validationErrors: [],
    metadata: undefined,
  };
}

function getExpressionOperand(
  expression: PlanExpression,
  index: number
): PlanExpressionOperand {
  const operand = expression.operands[index];
  if (!operand) {
    throw new Error(`Missing operand at index ${index}`);
  }
  return operand;
}

const fixtureUsers: Prisma.UserCreateInput[] = [
  {
    id: "user1",
    aBool: true,
    aNumber: 1,
    aString: "string",
  },
  {
    id: "user2",
    aBool: true,
    aNumber: 2,
    aString: "string",
  },
];

const fixtureNextLevelResources: Prisma.NextLevelNestedResourceCreateInput[] = [
  {
    id: "nextLevel1",
    aBool: true,
    aNumber: 1,
    aString: "string",
  },
  {
    id: "nextLevel2",
    aBool: false,
    aNumber: 1,
    aString: "string",
  },
  {
    id: "nextLevel3",
    aBool: true,
    aNumber: 1,
    aString: "string",
  },
];

const fixtureTags: Prisma.TagCreateInput[] = [
  {
    id: "tag1",
    name: "public",
  },
  {
    id: "tag2",
    name: "private",
  },
  {
    id: "tag3",
    name: "draft",
  },
];

const fixtureNestedResources: Prisma.NestedResourceCreateInput[] = [
  {
    id: "nested1",
    aBool: true,
    aNumber: 1,
    aString: "string",
    nextlevel: {
      connect: {
        id: "nextLevel1",
      },
    },
  },
  {
    id: "nested2",
    aBool: false,
    aNumber: 1,
    aString: "string",
    nextlevel: {
      connect: {
        id: "nextLevel2",
      },
    },
  },
  {
    id: "nested3",
    aBool: true,
    aNumber: 1,
    aString: "string",
    nextlevel: {
      connect: {
        id: "nextLevel3",
      },
    },
  },
];

const fixtureLabels: Prisma.LabelCreateInput[] = [
  { id: "label1", name: "important" },
  { id: "label2", name: "archived" },
  { id: "label3", name: "flagged" },
];

const fixtureSubCategories: Prisma.SubCategoryCreateInput[] = [
  {
    id: "sub1",
    name: "finance",
    labels: {
      connect: [{ id: "label1" }, { id: "label2" }],
    },
  },
  {
    id: "sub2",
    name: "tech",
    labels: {
      connect: [{ id: "label2" }, { id: "label3" }],
    },
  },
];

const fixtureCategories: Prisma.CategoryCreateInput[] = [
  {
    id: "cat1",
    name: "business",
    subCategories: {
      connect: [{ id: "sub1" }],
    },
  },
  {
    id: "cat2",
    name: "development",
    subCategories: {
      connect: [{ id: "sub2" }],
    },
  },
];

const fixtureResources: Prisma.ResourceCreateInput[] = [
  {
    id: "resource1",
    aBool: true,
    aNumber: 1,
    aString: "string",
    aOptionalString: "optionalString",
    createdBy: {
      connect: {
        id: "user1",
      },
    },
    ownedBy: {
      connect: [
        {
          id: "user1",
        },
      ],
    },
    nested: {
      connect: {
        id: "nested1",
      },
    },
    tags: {
      connect: [
        { id: "tag1" }, // public
      ],
    },
    categories: {
      connect: [{ id: "cat1" }],
    },
  },
  {
    id: "resource2",
    aBool: false,
    aNumber: 2,
    aString: "string2",
    createdBy: {
      connect: {
        id: "user2",
      },
    },
    ownedBy: {
      connect: [
        {
          id: "user2",
        },
      ],
    },
    nested: {
      connect: {
        id: "nested3",
      },
    },
    tags: {
      connect: [
        { id: "tag2" }, // private
      ],
    },
    categories: {
      connect: [{ id: "cat2" }],
    },
  },
  {
    id: "resource3",
    aBool: false,
    aNumber: 3,
    aString: "string3",
    createdBy: {
      connect: {
        id: "user1",
      },
    },
    ownedBy: {
      connect: [
        {
          id: "user1",
        },
        {
          id: "user2",
        },
      ],
    },
    nested: {
      connect: {
        id: "nested3",
      },
    },
    tags: {
      connect: [
        { id: "tag1" }, // public
        { id: "tag3" }, // draft
      ],
    },
    categories: {
      connect: [{ id: "cat1" }, { id: "cat2" }],
    },
  },
];

beforeAll(async () => {
  await prisma.resource.deleteMany();
  await prisma.nextLevelNestedResource.deleteMany();
  await prisma.nestedResource.deleteMany();
  await prisma.tag.deleteMany();
  await prisma.user.deleteMany();
});

beforeEach(async () => {
  for (const tag of fixtureTags) {
    await prisma.tag.create({ data: tag });
  }
  for (const user of fixtureUsers) {
    await prisma.user.create({ data: user });
  }
  for (const resource of fixtureNextLevelResources) {
    await prisma.nextLevelNestedResource.create({ data: resource });
  }
  for (const resource of fixtureNestedResources) {
    await prisma.nestedResource.create({ data: resource });
  }
  for (const label of fixtureLabels) {
    await prisma.label.create({ data: label });
  }
  for (const subCategory of fixtureSubCategories) {
    await prisma.subCategory.create({ data: subCategory });
  }
  for (const category of fixtureCategories) {
    await prisma.category.create({ data: category });
  }
  for (const resource of fixtureResources) {
    await prisma.resource.create({ data: resource });
  }
});

afterEach(async () => {
  await prisma.resource.deleteMany();
  await prisma.nestedResource.deleteMany();
  await prisma.nextLevelNestedResource.deleteMany();
  await prisma.tag.deleteMany();
  await prisma.user.deleteMany();
  await prisma.category.deleteMany();
  await prisma.subCategory.deleteMany();
  await prisma.label.deleteMany();
});

// Core Functionality
describe("Basic Plan Types", () => {
  test("always allowed", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "always-allow",
    });

    expect(queryPlan.kind).toEqual(PlanKind.ALWAYS_ALLOWED);

    const result = queryPlanToPrisma({
      queryPlan,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.ALWAYS_ALLOWED,
    });

    const query = await prisma.resource.findMany({});
    expect(query.length).toEqual(fixtureResources.length);
  });

  test("always denied", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "always-deny",
    });

    expect(queryPlan.kind).toEqual(PlanKind.ALWAYS_DENIED);

    const result = queryPlanToPrisma({
      queryPlan,
      mapper: {},
    });

    expect(result).toEqual({
      kind: PlanKind.ALWAYS_DENIED,
    });
  });
});

// Field Operations
describe("Field Operations", () => {
  describe("Basic Field Tests", () => {
    test("conditional - eq", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "equal",
      });

      expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
      expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual(
        {
          operator: "eq",
          operands: [{ name: "request.resource.attr.aBool" }, { value: true }],
        }
      );

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.aBool": { field: "aBool" },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { aBool: { equals: true } },
      });
      const query = await prisma.resource.findMany({
        where:
          result.kind === PlanKind.CONDITIONAL ? { ...result.filters } : {},
      });
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources.filter((a) => a.aBool).map((r) => r.id)
      );
    });

    test("conditional - eq - inverted order", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "equal",
      });

      expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);

      expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual(
        {
          operator: "eq",
          operands: [{ name: "request.resource.attr.aBool" }, { value: true }],
        }
      );

      const typeQp = queryPlan as PlanResourcesConditionalResponse;

      const invertedQueryPlan: PlanResourcesConditionalResponse = {
        ...typeQp,
        condition: {
          ...typeQp.condition,
          operands: [
            getExpressionOperand(typeQp.condition as PlanExpression, 1),
            getExpressionOperand(typeQp.condition as PlanExpression, 0),
          ],
        },
      };

      const result = queryPlanToPrisma({
        queryPlan: invertedQueryPlan,
        mapper: {
          "request.resource.attr.aBool": { field: "aBool" },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { aBool: { equals: true } },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources.filter((a) => a.aBool).map((r) => r.id)
      );
    });

    test("conditional - ne", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "ne",
      });

      expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
      expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual(
        {
          operator: "ne",
          operands: [
            { name: "request.resource.attr.aString" },
            { value: "string" },
          ],
        }
      );

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.aString": { field: "aString" },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { aString: { not: "string" } },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources.filter((a) => a.aString != "string").map((r) => r.id)
      );
    });

    test("conditional - explicit-deny", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "explicit-deny",
      });

      expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
      expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual(
        {
          operator: "not",
          operands: [
            {
              operator: "eq",
              operands: [
                { name: "request.resource.attr.aBool" },
                { value: true },
              ],
            },
          ],
        }
      );

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.aBool": { field: "aBool" },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { NOT: { aBool: { equals: true } } },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources.filter((a) => !a.aBool).map((r) => r.id)
      );
    });
  });

  describe("Bare Boolean Tests", () => {
    test("conditional - bare boolean true (no value operand)", () => {
      // #given
      const queryPlan = createConditionalPlan({
        name: "request.resource.attr.aBool",
      });

      // #when
      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.aBool": { field: "aBool" },
        },
      });

      // #then
      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { aBool: { equals: true } },
      });
    });

    test("conditional - bare boolean negated (not with single named operand)", () => {
      // #given
      const queryPlan = createConditionalPlan({
        operator: "not",
        operands: [{ name: "request.resource.attr.aBool" }],
      });

      // #when
      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.aBool": { field: "aBool" },
        },
      });

      // #then
      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { aBool: { equals: false } },
      });
    });

    test("conditional - bare boolean without mapper", () => {
      // #given
      const queryPlan = createConditionalPlan({
        name: "request.resource.attr.aBool",
      });

      // #when
      const result = queryPlanToPrisma({ queryPlan });

      // #then
      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { "request.resource.attr.aBool": { equals: true } },
      });
    });

    test("conditional - negated bare boolean without mapper", () => {
      // #given
      const queryPlan = createConditionalPlan({
        operator: "not",
        operands: [{ name: "request.resource.attr.aBool" }],
      });

      // #when
      const result = queryPlanToPrisma({ queryPlan });

      // #then
      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { "request.resource.attr.aBool": { equals: false } },
      });
    });

    test("conditional - bare boolean inside AND", () => {
      // #given
      const queryPlan = createConditionalPlan({
        operator: "and",
        operands: [
          { name: "request.resource.attr.aBool" },
          {
            operator: "eq",
            operands: [
              { name: "request.resource.attr.aString" },
              { value: "string" },
            ],
          },
        ],
      });

      // #when
      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.aBool": { field: "aBool" },
          "request.resource.attr.aString": { field: "aString" },
        },
      });

      // #then
      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          AND: [
            { aBool: { equals: true } },
            { aString: { equals: "string" } },
          ],
        },
      });
    });

    test("conditional - bare boolean inside OR", () => {
      // #given
      const queryPlan = createConditionalPlan({
        operator: "or",
        operands: [
          { name: "request.resource.attr.aBool" },
          {
            operator: "eq",
            operands: [
              { name: "request.resource.attr.aNumber" },
              { value: 1 },
            ],
          },
        ],
      });

      // #when
      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.aBool": { field: "aBool" },
          "request.resource.attr.aNumber": { field: "aNumber" },
        },
      });

      // #then
      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          OR: [
            { aBool: { equals: true } },
            { aNumber: { equals: 1 } },
          ],
        },
      });
    });

    test("conditional - double negation bare boolean", () => {
      // #given
      const queryPlan = createConditionalPlan({
        operator: "not",
        operands: [
          {
            operator: "not",
            operands: [{ name: "request.resource.attr.aBool" }],
          },
        ],
      });

      // #when
      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.aBool": { field: "aBool" },
        },
      });

      // #then
      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { NOT: { aBool: { equals: false } } },
      });
    });

    test("conditional - bare boolean with relation", () => {
      // #given
      const queryPlan = createConditionalPlan({
        name: "request.resource.attr.nested.aBool",
      });

      // #when
      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.nested": {
            relation: {
              name: "nestedResource",
              type: "one",
            },
          },
          "request.resource.attr.nested.aBool": {
            relation: {
              name: "nestedResource",
              type: "one",
            },
            field: "aBool",
          },
        },
      });

      // #then
      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          nestedResource: { is: { aBool: { equals: true } } },
        },
      });
    });

    test("conditional - negated bare boolean with relation", () => {
      // #given
      const queryPlan = createConditionalPlan({
        operator: "not",
        operands: [{ name: "request.resource.attr.nested.aBool" }],
      });

      // #when
      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.nested": {
            relation: {
              name: "nestedResource",
              type: "one",
            },
          },
          "request.resource.attr.nested.aBool": {
            relation: {
              name: "nestedResource",
              type: "one",
            },
            field: "aBool",
          },
        },
      });

      // #then
      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          NOT: { nestedResource: { is: { aBool: { equals: true } } } },
        },
      });
    });

    test("conditional - negated bare boolean with many relation", () => {
      // #given
      const queryPlan = createConditionalPlan({
        operator: "not",
        operands: [{ name: "request.resource.attr.tags.active" }],
      });

      // #when
      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.tags.active": {
            relation: {
              name: "tags",
              type: "many",
            },
            field: "active",
          },
        },
      });

      // #then — must be NOT { some { equals: true } }, not some { equals: false }
      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          NOT: { tags: { some: { active: { equals: true } } } },
        },
      });
    });

    test("conditional - bare boolean via cerbos policy", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "bare-bool",
      });

      expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
      expect(
        (queryPlan as PlanResourcesConditionalResponse).condition
      ).toEqual({ name: "request.resource.attr.aBool" });

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.aBool": { field: "aBool" },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { aBool: { equals: true } },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources.filter((a) => a.aBool).map((r) => r.id)
      );
    });

    test("conditional - negated bare boolean via cerbos policy", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "bare-bool-negated",
      });

      expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
      expect(
        (queryPlan as PlanResourcesConditionalResponse).condition
      ).toEqual({
        operator: "not",
        operands: [{ name: "request.resource.attr.aBool" }],
      });

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.aBool": { field: "aBool" },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { aBool: { equals: false } },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources.filter((a) => !a.aBool).map((r) => r.id)
      );
    });

    test("conditional - bare boolean nested via cerbos policy", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "bare-bool-nested",
      });

      expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
      expect(
        (queryPlan as PlanResourcesConditionalResponse).condition
      ).toEqual({ name: "request.resource.attr.nested.aBool" });

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.nested": {
            relation: {
              name: "nested",
              type: "one",
              fields: {
                aBool: { field: "aBool" },
              },
            },
          },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { nested: { is: { aBool: { equals: true } } } },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
        include: { nested: true },
      });
      expect(query.length).toBeGreaterThan(0);
      for (const resource of query) {
        expect(resource.nested.aBool).toBe(true);
      }
    });

    test("conditional - negated bare boolean nested via cerbos policy", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "bare-bool-nested-negated",
      });

      expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
      expect(
        (queryPlan as PlanResourcesConditionalResponse).condition
      ).toEqual({
        operator: "not",
        operands: [{ name: "request.resource.attr.nested.aBool" }],
      });

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.nested": {
            relation: {
              name: "nested",
              type: "one",
              fields: {
                aBool: { field: "aBool" },
              },
            },
          },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          NOT: { nested: { is: { aBool: { equals: true } } } },
        },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
        include: { nested: true },
      });
      for (const resource of query) {
        expect(resource.nested.aBool).not.toBe(true);
      }
    });
  });

  describe("Comparison Tests", () => {
    test("conditional - gt", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "gt",
      });

      expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
      expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual(
        {
          operator: "gt",
          operands: [{ name: "request.resource.attr.aNumber" }, { value: 1 }],
        }
      );

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.aNumber": { field: "aNumber" },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          aNumber: { gt: 1 },
        },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources
          .filter((r) => {
            return r.aNumber > 1;
          })
          .map((r) => r.id)
      );
    });

    test("conditional - lt", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "lt",
      });

      expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
      expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual(
        {
          operator: "lt",
          operands: [{ name: "request.resource.attr.aNumber" }, { value: 2 }],
        }
      );

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.aNumber": { field: "aNumber" },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          aNumber: { lt: 2 },
        },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources
          .filter((r) => {
            return r.aNumber < 2;
          })
          .map((r) => r.id)
      );
    });

    test("conditional - gte", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "gte",
      });

      expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
      expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual(
        {
          operator: "ge",
          operands: [{ name: "request.resource.attr.aNumber" }, { value: 1 }],
        }
      );

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.aNumber": { field: "aNumber" },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          aNumber: { gte: 1 },
        },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources
          .filter((r) => {
            return r.aNumber >= 1;
          })
          .map((r) => r.id)
      );
    });

    test("conditional - lte", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "lte",
      });

      expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
      expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual(
        {
          operator: "le",
          operands: [{ name: "request.resource.attr.aNumber" }, { value: 2 }],
        }
      );

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.aNumber": { field: "aNumber" },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          aNumber: { lte: 2 },
        },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources
          .filter((r) => {
            return r.aNumber <= 2;
          })
          .map((r) => r.id)
      );
    });
  });

  describe("String Tests", () => {
    test("conditional - contains", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "contains",
      });

      expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
      expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual(
        {
          operands: [
            {
              name: "request.resource.attr.aString",
            },
            {
              value: "str",
            },
          ],
          operator: "contains",
        }
      );

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.aString": { field: "aString" },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { aString: { contains: "str" } },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources
          .filter((a) => a.aString.includes("str"))
          .map((r) => r.id)
      );
    });

    test("conditional - startsWith", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "starts-with",
      });

      expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
      expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual(
        {
          operands: [
            {
              name: "request.resource.attr.aString",
            },
            {
              value: "str",
            },
          ],
          operator: "startsWith",
        }
      );

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.aString": { field: "aString" },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { aString: { startsWith: "str" } },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources
          .filter((a) => a.aString.startsWith("str"))
          .map((r) => r.id)
      );
    });

    test("conditional - endsWith", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "ends-with",
      });

      expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
      expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual(
        {
          operands: [
            {
              name: "request.resource.attr.aString",
            },
            {
              value: "ing",
            },
          ],
          operator: "endsWith",
        }
      );

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.aString": { field: "aString" },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { aString: { endsWith: "ing" } },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources
          .filter((a) => a.aString.endsWith("ing"))
          .map((r) => r.id)
      );
    });

    test("conditional - isSet", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "is-set",
      });

      expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
      expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual(
        {
          operands: [
            {
              name: "request.resource.attr.aOptionalString",
            },
            {
              value: null,
            },
          ],
          operator: "ne",
        }
      );

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.aOptionalString": { field: "aOptionalString" },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { aOptionalString: { not: null } },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources.filter((a) => a.aOptionalString).map((r) => r.id)
      );
    });
  });
});

// Collection Operations
describe("Collection Operations", () => {
  describe("Basic Collections", () => {
    test("conditional - in", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "in",
      });

      expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
      expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual(
        {
          operator: "in",
          operands: [
            { name: "request.resource.attr.aString" },
            { value: ["string", "anotherString"] },
          ],
        }
      );

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.aString": { field: "aString" },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          aString: { in: ["string", "anotherString"] },
        },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources
          .filter((r) => {
            return ["string", "anotherString"].includes(r.aString);
          })
          .map((r) => r.id)
      );
    });

    test("conditional - in - scalar value", async () => {
      const queryPlan = createConditionalPlan({
        operator: "in",
        operands: [
          { name: "request.resource.attr.aString" },
          { value: "string" },
        ],
      });

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.aString": { field: "aString" },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { aString: "string" },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });

      expect(query.map((r) => r.id).sort()).toEqual(
        fixtureResources
          .filter((r) => r.aString === "string")
          .map((r) => r.id)
          .sort()
      );
    });

    test("conditional - relation in", async () => {
      const queryPlan = createConditionalPlan({
        operator: "in",
        operands: [
          { name: "request.resource.attr.categories.name" },
          { value: ["business"] },
        ],
      });

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.categories": {
            relation: {
              name: "categories",
              type: "many",
              fields: {
                name: { field: "name" },
              },
            },
          },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          categories: {
            some: {
              name: "business",
            },
          },
        },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });

      expect(query.map((r) => r.id).sort()).toEqual(
        fixtureResources
          .filter((resource) => {
            const categoryRefs =
              (resource.categories?.connect as Prisma.CategoryWhereUniqueInput[]) ??
              [];
            return categoryRefs.some((categoryRef) => {
              const category = fixtureCategories.find(
                (fc) => fc.id === categoryRef.id
              );
              return category?.name === "business";
            });
          })
          .map((r) => r.id)
          .sort()
      );
    });

    test("conditional - relation in multiple values", async () => {
      const queryPlan = createConditionalPlan({
        operator: "in",
        operands: [
          { name: "request.resource.attr.categories.name" },
          { value: ["business", "development"] },
        ],
      });

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.categories": {
            relation: {
              name: "categories",
              type: "many",
              fields: {
                name: { field: "name" },
              },
            },
          },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          categories: {
            some: {
              name: { in: ["business", "development"] },
            },
          },
        },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });

      expect(query.map((r) => r.id).sort()).toEqual(
        fixtureResources
          .filter((resource) => {
            const categoryRefs =
              (resource.categories?.connect as Prisma.CategoryWhereUniqueInput[]) ??
              [];
            return categoryRefs.some((categoryRef) => {
              const category = fixtureCategories.find(
                (fc) => fc.id === categoryRef.id
              );
              return ["business", "development"].includes(category?.name ?? "");
            });
          })
          .map((r) => r.id)
          .sort()
      );
    });

    test("conditional - except relation subset", async () => {
      const queryPlan = createConditionalPlan({
        operator: "except",
        operands: [
          { name: "request.resource.attr.categories" },
          {
            operator: "lambda",
            operands: [
              {
                operator: "eq",
                operands: [
                  { name: "cat.name" },
                  { value: "business" },
                ],
              },
              { name: "cat" },
            ],
          },
        ],
      });

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.categories": {
            relation: {
              name: "categories",
              type: "many",
              fields: {
                name: { field: "name" },
              },
            },
          },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          categories: {
            some: {
              NOT: {
                name: { equals: "business" },
              },
            },
          },
        },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });

      expect(query.map((r) => r.id).sort()).toEqual(
        fixtureResources
          .filter((resource) => {
            const categoryRefs =
              (resource.categories?.connect as Prisma.CategoryWhereUniqueInput[]) ??
              [];
            return categoryRefs.some((categoryRef) => {
              const category = fixtureCategories.find(
                (fc) => fc.id === categoryRef.id
              );
              return category?.name !== "business";
            });
          })
          .map((r) => r.id)
          .sort()
      );
    });

    test("conditional - except nested relation subset", async () => {
      const queryPlan = createConditionalPlan({
        operator: "except",
        operands: [
          { name: "request.resource.attr.categories" },
          {
            operator: "lambda",
            operands: [
              {
                operator: "exists",
                operands: [
                  { name: "cat.subCategories" },
                  {
                    operator: "lambda",
                    operands: [
                      {
                        operator: "eq",
                        operands: [
                          { name: "sub.name" },
                          { value: "finance" },
                        ],
                      },
                      { name: "sub" },
                    ],
                  },
                ],
              },
              { name: "cat" },
            ],
          },
        ],
      });

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.categories": {
            relation: {
              name: "categories",
              type: "many",
              fields: {
                subCategories: {
                  relation: {
                    name: "subCategories",
                    type: "many",
                    fields: {
                      name: { field: "name" },
                    },
                  },
                },
              },
            },
          },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          categories: {
            some: {
              NOT: {
                subCategories: {
                  some: {
                    name: { equals: "finance" },
                  },
                },
              },
            },
          },
        },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });

      expect(query.map((r) => r.id).sort()).toEqual(
        fixtureResources
          .filter((resource) => {
            const categoryRefs =
              (resource.categories?.connect as Prisma.CategoryWhereUniqueInput[]) ??
              [];
            return categoryRefs.some((categoryRef) => {
              const category = fixtureCategories.find(
                (fc) => fc.id === categoryRef.id
              );
              if (!category) {
                return false;
              }
              const subCategoryRefs =
                (category.subCategories?.connect as Prisma.SubCategoryWhereUniqueInput[]) ??
                [];
              return !subCategoryRefs.some((subCategoryRef) => {
                const subCategory = fixtureSubCategories.find(
                  (fsc) => fsc.id === subCategoryRef.id
                );
                return subCategory?.name === "finance";
              });
            });
          })
          .map((r) => r.id)
          .sort()
      );
    });

    test("conditional - exists single", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "exists-single",
      });

      expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);

      expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual(
        {
          operator: "exists",
          operands: [
            {
              name: "request.resource.attr.tags",
            },
            {
              operator: "lambda",
              operands: [
                {
                  operator: "eq",
                  operands: [
                    {
                      name: "tag.id",
                    },
                    {
                      value: "tag1",
                    },
                  ],
                },
                {
                  name: "tag",
                },
              ],
            },
          ],
        }
      );

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.tags": {
            relation: {
              name: "tags",
              type: "many",
            },
          },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          tags: {
            some: {
              id: {
                equals: "tag1",
              },
            },
          },
        },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });

      expect(query.map((r) => r.id)).toEqual(
        fixtureResources
          .filter(
            (a) =>
              Array.isArray(a.tags?.connect) &&
              a.tags?.connect
                .map((t) => {
                  return fixtureTags.find((f) => f.id === t.id);
                })
                .filter((t) => t?.id == "tag1").length > 0
          )
          .map((r) => r.id)
      );
    });

    test("conditional - exists multiple", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "exists-multiple",
      });

      expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);

      expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual(
        {
          operator: "exists",
          operands: [
            {
              name: "request.resource.attr.tags",
            },
            {
              operator: "lambda",
              operands: [
                {
                  operator: "and",
                  operands: [
                    {
                      operator: "eq",
                      operands: [
                        {
                          name: "tag.id",
                        },
                        {
                          value: "tag1",
                        },
                      ],
                    },
                    {
                      operator: "eq",
                      operands: [
                        {
                          name: "tag.name",
                        },
                        {
                          value: "public",
                        },
                      ],
                    },
                  ],
                },
                {
                  name: "tag",
                },
              ],
            },
          ],
        }
      );

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.tags": {
            relation: {
              name: "tags",
              type: "many",
            },
          },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          tags: {
            some: {
              AND: [
                {
                  id: {
                    equals: "tag1",
                  },
                },
                {
                  name: {
                    equals: "public",
                  },
                },
              ],
            },
          },
        },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });

      expect(query.map((r) => r.id)).toEqual(
        fixtureResources
          .filter(
            (a) =>
              Array.isArray(a.tags?.connect) &&
              a.tags?.connect
                .map((t) => {
                  return fixtureTags.find((f) => f.id === t.id);
                })
                .filter((t) => t?.id === "tag1" && t?.name === "public")
                .length > 0
          )
          .map((r) => r.id)
      );
    });

    test("conditional - exists_one", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "exists-one",
      });

      expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
      expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual(
        {
          operator: "exists_one",
          operands: [
            {
              name: "request.resource.attr.tags",
            },
            {
              operator: "lambda",
              operands: [
                {
                  operator: "eq",
                  operands: [
                    {
                      name: "tag.name",
                    },
                    {
                      value: "public",
                    },
                  ],
                },
                {
                  name: "tag",
                },
              ],
            },
          ],
        }
      );

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.tags": {
            relation: {
              name: "tags",
              type: "many",
            },
          },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          tags: {
            some: {
              name: { equals: "public" },
            },
          },
          AND: [
            {
              tags: {
                every: {
                  OR: [
                    { name: { equals: "public" } },
                    { NOT: { name: { equals: "public" } } },
                  ],
                },
              },
            },
          ],
        },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });

      expect(query.map((r) => r.id)).toEqual(
        fixtureResources
          .filter(
            (a) =>
              Array.isArray(a.tags?.connect) &&
              a.tags?.connect
                .map((t) => {
                  return fixtureTags.find((f) => f.id === t.id);
                })
                .filter((t) => t?.id === "tag1" && t?.name === "public")
                .length > 0
          )
          .map((r) => r.id)
      );
    });

    test("conditional - all", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "all",
      });

      expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
      expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual(
        {
          operator: "all",
          operands: [
            {
              name: "request.resource.attr.tags",
            },
            {
              operator: "lambda",
              operands: [
                {
                  operator: "eq",
                  operands: [
                    {
                      name: "tag.name",
                    },
                    {
                      value: "public",
                    },
                  ],
                },
                {
                  name: "tag",
                },
              ],
            },
          ],
        }
      );

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.tags": {
            relation: {
              name: "tags",
              type: "many",
            },
          },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          tags: {
            every: {
              name: { equals: "public" },
            },
          },
        },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });

      expect(query.map((r) => r.id)).toEqual(
        fixtureResources
          .filter(
            (a) =>
              Array.isArray(a.tags?.connect) &&
              a.tags?.connect
                .map((t) => {
                  return fixtureTags.find((f) => f.id === t.id);
                })
                .every((t) => t?.name === "public")
          )
          .map((r) => r.id)
      );
    });

    test("conditional - filter", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "filter",
      });

      expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
      expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual(
        {
          operator: "filter",
          operands: [
            {
              name: "request.resource.attr.tags",
            },
            {
              operator: "lambda",
              operands: [
                {
                  operator: "eq",
                  operands: [
                    {
                      name: "tag.name",
                    },
                    {
                      value: "public",
                    },
                  ],
                },
                {
                  name: "tag",
                },
              ],
            },
          ],
        }
      );

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.tags": {
            relation: {
              name: "tags",
              type: "many",
            },
          },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          tags: {
            some: {
              name: { equals: "public" },
            },
          },
        },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });

      expect(query.map((r) => r.id)).toEqual(
        fixtureResources
          .filter(
            (a) =>
              Array.isArray(a.tags?.connect) &&
              a.tags?.connect
                .map((t) => {
                  return fixtureTags.find((f) => f.id === t.id);
                })
                .filter((t) => t?.name === "public").length > 0
          )
          .map((r) => r.id)
      );
    });

    test("conditional - map collection", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "map-collection",
      });

      expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
      expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual(
        {
          operator: "hasIntersection",
          operands: [
            {
              operator: "map",
              operands: [
                { name: "request.resource.attr.tags" },
                {
                  operator: "lambda",
                  operands: [{ name: "tag.name" }, { name: "tag" }],
                },
              ],
            },
            { value: ["public", "private"] },
          ],
        }
      );

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.tags": {
            relation: {
              name: "tags",
              type: "many",
              fields: {
                name: { field: "name" },
              },
            },
          },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          tags: {
            some: {
              name: { in: ["public", "private"] },
            },
          },
        },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });

      // Should return resources that have either "public" or "private" tags
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources
          .filter((r) => {
            const tagNames = Array.isArray(r.tags?.connect)
              ? r.tags.connect.map(
                  (t) => fixtureTags.find((ft) => ft.id === t.id)?.name
                ) || []
              : [];
            return tagNames.some((name) =>
              ["public", "private"].includes(name || "")
            );
          })
          .map((r) => r.id)
      );
    });
  });
});

// Relations
describe("Relations", () => {
  describe("Simple Relations", () => {
    describe("One-to-One", () => {
      test("conditional - relation is", async () => {
        const queryPlan = await cerbos.planResources({
          principal: { id: "user1", roles: ["USER"] },
          resource: { kind: "resource" },
          action: "relation-is",
        });

        expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);

        const conditions = (queryPlan as PlanResourcesConditionalResponse)
          .condition;

        expect(conditions).toEqual({
          operator: "eq",
          operands: [
            { name: "request.resource.attr.createdBy" },
            { value: "user1" },
          ],
        });

        const result = queryPlanToPrisma({
          queryPlan,
          mapper: {
            "request.resource.attr.createdBy": {
              relation: {
                name: "createdBy",
                type: "one",
                field: "id",
              },
            },
          },
        });

        expect(result).toStrictEqual({
          kind: PlanKind.CONDITIONAL,
          filters: {
            createdBy: {
              is: {
                id: {
                  equals: "user1",
                },
              },
            },
          },
        });

        if (result.kind !== PlanKind.CONDITIONAL) {
          throw new Error("Expected CONDITIONAL result");
        }

        const query = await prisma.resource.findMany({
          where: { ...result.filters },
        });

        expect(query.map((r) => r.id)).toEqual(
          fixtureResources
            .filter((r) => {
              if (!r.createdBy?.connect) return false;
              return (r.createdBy.connect as { id: string }).id == "user1";
            })
            .map((r) => r.id)
        );
      });

      test("conditional - relation is not", async () => {
        const queryPlan = await cerbos.planResources({
          principal: { id: "user1", roles: ["USER"] },
          resource: { kind: "resource" },
          action: "relation-is-not",
        });

        expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);

        const conditions = (queryPlan as PlanResourcesConditionalResponse)
          .condition;

        expect(conditions).toEqual({
          operator: "not",
          operands: [
            {
              operator: "eq",
              operands: [
                { name: "request.resource.attr.createdBy" },
                { value: "user1" },
              ],
            },
          ],
        });

        const result = queryPlanToPrisma({
          queryPlan,
          mapper: {
            "request.resource.attr.createdBy": {
              relation: {
                name: "createdBy",
                type: "one",
                field: "id",
              },
            },
          },
        });

        expect(result).toStrictEqual({
          kind: PlanKind.CONDITIONAL,
          filters: {
            NOT: {
              createdBy: {
                is: {
                  id: {
                    equals: "user1",
                  },
                },
              },
            },
          },
        });

        if (result.kind !== PlanKind.CONDITIONAL) {
          throw new Error("Expected CONDITIONAL result");
        }

        const query = await prisma.resource.findMany({
          where: { ...result.filters },
        });

        expect(query.map((r) => r.id)).toEqual(
          fixtureResources
            .filter((r) => {
              if (!r.createdBy?.connect) return false;
              return (r.createdBy.connect as { id: string }).id != "user1";
            })
            .map((r) => r.id)
        );
      });

      test("conditional - relation is without field", async () => {
        const queryPlan = await cerbos.planResources({
          principal: { id: "user1", roles: ["USER"] },
          resource: { kind: "resource" },
          action: "relation-is",
        });

        expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);

        const conditions = (queryPlan as PlanResourcesConditionalResponse)
          .condition;

        expect(conditions).toEqual({
          operator: "eq",
          operands: [
            { name: "request.resource.attr.createdBy" },
            { value: "user1" },
          ],
        });

        const result = queryPlanToPrisma({
          queryPlan,
          mapper: {
            "request.resource.attr.createdBy": {
              relation: {
                name: "createdBy",
                type: "one",
                field: "id",
              },
            },
          },
        });

        expect(result).toStrictEqual({
          kind: PlanKind.CONDITIONAL,
          filters: {
            createdBy: {
              is: {
                id: { equals: "user1" },
              },
            },
          },
        });

        if (result.kind !== PlanKind.CONDITIONAL) {
          throw new Error("Expected CONDITIONAL result");
        }

        const query = await prisma.resource.findMany({
          where: { ...result.filters },
        });

        expect(query.map((r) => r.id)).toEqual(
          fixtureResources
            .filter((r) => {
              if (!r.createdBy?.connect) return false;
              return (r.createdBy.connect as { id: string }).id == "user1";
            })
            .map((r) => r.id)
        );
      });
    });

    describe("One-to-Many/Many-to-Many", () => {
      test("conditional - relation some", async () => {
        const queryPlan = await cerbos.planResources({
          principal: { id: "user1", roles: ["USER"] },
          resource: { kind: "resource" },
          action: "relation-some",
        });

        expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);

        const conditions = (queryPlan as PlanResourcesConditionalResponse)
          .condition;

        expect(conditions).toEqual({
          operator: "in",
          operands: [
            { value: "user1" },
            { name: "request.resource.attr.ownedBy" },
          ],
        });

        const result = queryPlanToPrisma({
          queryPlan,
          mapper: {
            "request.resource.attr.ownedBy": {
              relation: {
                name: "ownedBy",
                type: "many",
                field: "id",
              },
            },
          },
        });

        expect(result).toStrictEqual({
          kind: PlanKind.CONDITIONAL,
          filters: {
            ownedBy: {
              some: {
                id: "user1",
              },
            },
          },
        });

        if (result.kind !== PlanKind.CONDITIONAL) {
          throw new Error("Expected CONDITIONAL result");
        }

        const query = await prisma.resource.findMany({
          where: { ...result.filters },
        });

        expect(query.map((r) => r.id)).toEqual(
          fixtureResources
            .filter((r) => {
              if (!r.ownedBy?.connect) return false;
              return (
                (r.ownedBy.connect as { id: string }[]).filter(
                  (o) => o.id == "user1"
                ).length > 0
              );
            })
            .map((r) => r.id)
        );
      });

      test("conditional - relation none", async () => {
        const queryPlan = await cerbos.planResources({
          principal: { id: "user1", roles: ["USER"] },
          resource: { kind: "resource" },
          action: "relation-none",
        });

        expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);

        const conditions = (queryPlan as PlanResourcesConditionalResponse)
          .condition;

        expect(conditions).toEqual({
          operator: "not",
          operands: [
            {
              operator: "in",
              operands: [
                { value: "user1" },
                { name: "request.resource.attr.ownedBy" },
              ],
            },
          ],
        });

        const result = queryPlanToPrisma({
          queryPlan,
          mapper: {
            "request.resource.attr.ownedBy": {
              relation: {
                name: "ownedBy",
                type: "many",
                field: "id",
              },
            },
          },
        });

        expect(result).toStrictEqual({
          kind: PlanKind.CONDITIONAL,
          filters: {
            NOT: {
              ownedBy: {
                some: {
                  id: "user1",
                },
              },
            },
          },
        });

        if (result.kind !== PlanKind.CONDITIONAL) {
          throw new Error("Expected CONDITIONAL result");
        }

        const query = await prisma.resource.findMany({
          where: { ...result.filters },
        });

        expect(query.map((r) => r.id)).toEqual(
          fixtureResources
            .filter((r) => {
              if (!r.ownedBy?.connect) return false;
              return (
                (r.ownedBy.connect as { id: string }[]).filter(
                  (o) => o.id == "user1"
                ).length == 0
              );
            })
            .map((r) => r.id)
        );
      });

      test("conditional - has-tag", async () => {
        const queryPlan = await cerbos.planResources({
          principal: { id: "user1", roles: ["USER"] },
          resource: { kind: "resource" },
          action: "has-tag",
        });

        expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
        expect(
          (queryPlan as PlanResourcesConditionalResponse).condition
        ).toEqual({
          operator: "in",
          operands: [
            { value: "public" },
            { name: "request.resource.attr.tags" },
          ],
        });

        const result = queryPlanToPrisma({
          queryPlan,
          mapper: {
            "request.resource.attr.tags": {
              relation: {
                name: "tags",
                type: "many", // This indicates a many-to-many relationship
                field: "name",
              },
            },
          },
        });

        expect(result).toStrictEqual({
          kind: PlanKind.CONDITIONAL,
          filters: {
            tags: {
              some: {
                name: "public",
              },
            },
          },
        });

        if (result.kind !== PlanKind.CONDITIONAL) {
          throw new Error("Expected CONDITIONAL result");
        }

        const query = await prisma.resource.findMany({
          where: { ...result.filters },
          include: { tags: true },
        });

        const expectedIds = fixtureResources
          .filter((resource) => {
            const tagRefs =
              (resource.tags?.connect as Prisma.TagWhereUniqueInput[]) ?? [];
            return tagRefs.some((tagRef) => {
              const tag = fixtureTags.find((ft) => ft.id === tagRef.id);
              return tag?.name === "public";
            });
          })
          .map((r) => r.id)
          .sort();

        expect(query.map((r) => r.id).sort()).toEqual(expectedIds);
      });

      test("conditional - has-no-tag", async () => {
        const queryPlan = await cerbos.planResources({
          principal: { id: "user1", roles: ["USER"] },
          resource: { kind: "resource" },
          action: "has-no-tag",
        });

        expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
        expect(
          (queryPlan as PlanResourcesConditionalResponse).condition
        ).toEqual({
          operator: "not",
          operands: [
            {
              operator: "in",
              operands: [
                { value: "private" },
                { name: "request.resource.attr.tags" },
              ],
            },
          ],
        });

        const result = queryPlanToPrisma({
          queryPlan,
          mapper: {
            "request.resource.attr.tags": {
              relation: {
                name: "tags",
                type: "many",
                field: "name",
              },
            },
          },
        });

        expect(result).toStrictEqual({
          kind: PlanKind.CONDITIONAL,
          filters: {
            NOT: {
              tags: {
                some: {
                  name: "private",
                },
              },
            },
          },
        });

        if (result.kind !== PlanKind.CONDITIONAL) {
          throw new Error("Expected CONDITIONAL result");
        }

        const query = await prisma.resource.findMany({
          where: { ...result.filters },
          include: { tags: true },
        });

        const expectedIds = fixtureResources
          .filter((resource) => {
            const tagRefs =
              (resource.tags?.connect as Prisma.TagWhereUniqueInput[]) ?? [];
            return tagRefs.every((tagRef) => {
              const tag = fixtureTags.find((ft) => ft.id === tagRef.id);
              return tag?.name !== "private";
            });
          })
          .map((r) => r.id)
          .sort();

        expect(query.map((r) => r.id).sort()).toEqual(expectedIds);
      });

      test("conditional - has-intersection", async () => {
        const queryPlan = await cerbos.planResources({
          principal: {
            id: "user1",
            roles: ["USER"],
            attr: { tags: ["public", "draft"] },
          },
          resource: { kind: "resource" },
          action: "has-intersection",
        });

        expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
        // expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
        //   operator: "hasIntersection",
        //   operands: [
        //     { name: "request.resource.attr.tags" },
        //     { value: ["public", "draft"] },
        //   ],
        // });

        const result = queryPlanToPrisma({
          queryPlan,
          mapper: {
            "request.resource.attr.tags": {
              relation: {
                name: "tags",
                type: "many",
              },
            },
          },
        });

        expect(result).toStrictEqual({
          kind: PlanKind.CONDITIONAL,
          filters: {
            tags: {
              some: {
                name: { in: ["public", "draft"] },
              },
            },
          },
        });

        if (result.kind !== PlanKind.CONDITIONAL) {
          throw new Error("Expected CONDITIONAL result");
        }

        const query = await prisma.resource.findMany({
          where: { ...result.filters },
        });

        const expectedIds = fixtureResources
          .filter((resource) => {
            const tagRefs =
              (resource.tags?.connect as Prisma.TagWhereUniqueInput[]) ?? [];
            return tagRefs.some((tagRef) => {
              const tag = fixtureTags.find((ft) => ft.id === tagRef.id);
              return ["public", "draft"].includes(tag?.name ?? "");
            });
          })
          .map((r) => r.id)
          .sort();

        expect(query.map((r) => r.id).sort()).toEqual(expectedIds);
      });

      test("conditional - hasIntersection with direct value", async () => {
        const queryPlan = await cerbos.planResources({
          principal: { id: "user1", roles: ["USER"] },
          resource: { kind: "resource" },
          action: "has-intersection-direct",
        });

        expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
        expect(
          (queryPlan as PlanResourcesConditionalResponse).condition
        ).toEqual({
          operator: "hasIntersection",
          operands: [
            { name: "request.resource.attr.tags" },
            { value: ["public", "draft"] },
          ],
        });

        const result = queryPlanToPrisma({
          queryPlan,
          mapper: {
            "request.resource.attr.tags": {
              relation: {
                name: "tags",
                type: "many",
                field: "name",
              },
            },
          },
        });

        expect(result).toStrictEqual({
          kind: PlanKind.CONDITIONAL,
          filters: {
            tags: {
              some: {
                name: { in: ["public", "draft"] },
              },
            },
          },
        });

        if (result.kind !== PlanKind.CONDITIONAL) {
          throw new Error("Expected CONDITIONAL result");
        }

        const query = await prisma.resource.findMany({
          where: { ...result.filters },
        });
        expect(query.map((r) => r.id)).toEqual(
          fixtureResources
            .filter(
              (r) =>
                Array.isArray(r.tags?.connect) &&
                r.tags?.connect
                  .map((t) => fixtureTags.find((ft) => ft.id === t.id)?.name)
                  .some((name) => ["public", "draft"].includes(name ?? ""))
            )
            .map((r) => r.id)
        );
      });

      test("conditional - size(field) > 0 produces some", () => {
        const result = queryPlanToPrisma({
          queryPlan: createConditionalPlan({
            operator: "gt",
            operands: [
              {
                operator: "size",
                operands: [{ name: "request.resource.attr.ownedBy" }],
              },
              { value: 0 },
            ],
          }),
          mapper: {
            "request.resource.attr.ownedBy": {
              relation: { name: "ownedBy", type: "many", field: "id" },
            },
          },
        });

        expect(result).toStrictEqual({
          kind: PlanKind.CONDITIONAL,
          filters: { ownedBy: { some: {} } },
        });
      });

      test("conditional - size(field) == 0 produces none", () => {
        const result = queryPlanToPrisma({
          queryPlan: createConditionalPlan({
            operator: "eq",
            operands: [
              {
                operator: "size",
                operands: [{ name: "request.resource.attr.ownedBy" }],
              },
              { value: 0 },
            ],
          }),
          mapper: {
            "request.resource.attr.ownedBy": {
              relation: { name: "ownedBy", type: "many", field: "id" },
            },
          },
        });

        expect(result).toStrictEqual({
          kind: PlanKind.CONDITIONAL,
          filters: { ownedBy: { none: {} } },
        });
      });

      test("conditional - not(size(field) > 0) produces NOT some", () => {
        const result = queryPlanToPrisma({
          queryPlan: createConditionalPlan({
            operator: "not",
            operands: [
              {
                operator: "gt",
                operands: [
                  {
                    operator: "size",
                    operands: [{ name: "request.resource.attr.ownedBy" }],
                  },
                  { value: 0 },
                ],
              },
            ],
          }),
          mapper: {
            "request.resource.attr.ownedBy": {
              relation: { name: "ownedBy", type: "many", field: "id" },
            },
          },
        });

        expect(result).toStrictEqual({
          kind: PlanKind.CONDITIONAL,
          filters: { NOT: { ownedBy: { some: {} } } },
        });
      });

      test("conditional - size(field) >= 1 produces some", () => {
        const result = queryPlanToPrisma({
          queryPlan: createConditionalPlan({
            operator: "ge",
            operands: [
              {
                operator: "size",
                operands: [{ name: "request.resource.attr.ownedBy" }],
              },
              { value: 1 },
            ],
          }),
          mapper: {
            "request.resource.attr.ownedBy": {
              relation: { name: "ownedBy", type: "many", field: "id" },
            },
          },
        });

        expect(result).toStrictEqual({
          kind: PlanKind.CONDITIONAL,
          filters: { ownedBy: { some: {} } },
        });
      });

      test("conditional - size on nested relation walks full chain", () => {
        const result = queryPlanToPrisma({
          queryPlan: createConditionalPlan({
            operator: "gt",
            operands: [
              {
                operator: "size",
                operands: [
                  { name: "request.resource.attr.team.members" },
                ],
              },
              { value: 0 },
            ],
          }),
          mapper: {
            "request.resource.attr.team": {
              relation: {
                name: "team",
                type: "one",
                fields: {
                  members: {
                    relation: { name: "members", type: "many" },
                  },
                },
              },
            },
          },
        });

        expect(result).toStrictEqual({
          kind: PlanKind.CONDITIONAL,
          filters: {
            team: {
              is: {
                members: { some: {} },
              },
            },
          },
        });
      });

      test("conditional - unsupported size comparison throws", () => {
        expect(() =>
          queryPlanToPrisma({
            queryPlan: createConditionalPlan({
              operator: "gt",
              operands: [
                {
                  operator: "size",
                  operands: [{ name: "request.resource.attr.ownedBy" }],
                },
                { value: 5 },
              ],
            }),
            mapper: {
              "request.resource.attr.ownedBy": {
                relation: { name: "ownedBy", type: "many", field: "id" },
              },
            },
          })
        ).toThrow("Unsupported size comparison: size(...) gt 5");
      });
    });
  });

  describe("Nested Relations", () => {
    describe("Single Level", () => {
      test("conditional - nested eq", async () => {
        const queryPlan = await cerbos.planResources({
          principal: { id: "user1", roles: ["USER"] },
          resource: { kind: "resource" },
          action: "equal-nested",
        });

        expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);

        const conditions = (queryPlan as PlanResourcesConditionalResponse)
          .condition;

        expect(conditions).toEqual({
          operator: "eq",
          operands: [
            { name: "request.resource.attr.nested.aBool" },
            { value: true },
          ],
        });

        const result = queryPlanToPrisma({
          queryPlan,
          mapper: {
            "request.resource.attr.nested": {
              relation: {
                name: "nested",
                type: "one",
                fields: {
                  aBool: { field: "aBool" },
                },
              },
            },
          },
        });

        expect(result).toStrictEqual({
          kind: PlanKind.CONDITIONAL,
          filters: { nested: { is: { aBool: { equals: true } } } },
        });

        if (result.kind !== PlanKind.CONDITIONAL) {
          throw new Error("Expected CONDITIONAL result");
        }

        const query = await prisma.resource.findMany({
          where: { ...result.filters },
        });

        expect(query.map((r) => r.id)).toEqual(
          fixtureResources
            .filter(
              (a) =>
                fixtureNestedResources.find(
                  (f) => f.id === a.nested.connect?.id
                )?.aBool
            )
            .map((r) => r.id)
        );
      });

      test("conditional - relation eq with number", async () => {
        const queryPlan = await cerbos.planResources({
          principal: { id: "user1", roles: ["USER"] },
          resource: { kind: "resource" },
          action: "relation-eq-number",
        });

        expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);

        const conditions = (queryPlan as PlanResourcesConditionalResponse)
          .condition;

        expect(conditions).toEqual({
          operator: "eq",
          operands: [
            { name: "request.resource.attr.nested.aNumber" },
            { value: 1 },
          ],
        });

        const result = queryPlanToPrisma({
          queryPlan,
          mapper: {
            "request.resource.attr.nested": {
              relation: {
                name: "nested",
                type: "one",
              },
            },
          },
        });

        expect(result).toStrictEqual({
          kind: PlanKind.CONDITIONAL,
          filters: {
            nested: {
              is: {
                aNumber: { equals: 1 },
              },
            },
          },
        });

        if (result.kind !== PlanKind.CONDITIONAL) {
          throw new Error("Expected CONDITIONAL result");
        }

        const query = await prisma.resource.findMany({
          where: { ...result.filters },
        });

        expect(query.map((r) => r.id)).toEqual(
          fixtureResources
            .filter((r) => {
              const nestedResource = fixtureNestedResources.find(
                (n) => n.id === r.nested.connect?.id
              );
              return (nestedResource?.aNumber ?? 0) == 1;
            })
            .map((r) => r.id)
        );
      });

      test("conditional - relation eq with number without field", async () => {
        const queryPlan = await cerbos.planResources({
          principal: { id: "user1", roles: ["USER"] },
          resource: { kind: "resource" },
          action: "relation-eq-number",
        });

        expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);

        const conditions = (queryPlan as PlanResourcesConditionalResponse)
          .condition;

        expect(conditions).toEqual({
          operator: "eq",
          operands: [
            { name: "request.resource.attr.nested.aNumber" },
            { value: 1 },
          ],
        });

        const result = queryPlanToPrisma({
          queryPlan,
          mapper: {
            "request.resource.attr.nested": {
              relation: {
                name: "nested",
                type: "one",
              },
            },
          },
        });

        expect(result).toStrictEqual({
          kind: PlanKind.CONDITIONAL,
          filters: {
            nested: {
              is: {
                aNumber: {
                  equals: 1,
                },
              },
            },
          },
        });

        if (result.kind !== PlanKind.CONDITIONAL) {
          throw new Error("Expected CONDITIONAL result");
        }

        const query = await prisma.resource.findMany({
          where: { ...result.filters },
        });

        expect(query.map((r) => r.id)).toEqual(
          fixtureResources
            .filter((r) => {
              const nestedResource = fixtureNestedResources.find(
                (n) => n.id === r.nested.connect?.id
              );
              return nestedResource && nestedResource.aNumber === 1;
            })
            .map((r) => r.id)
        );
      });

      test("conditional - relation lt with number", async () => {
        const queryPlan = await cerbos.planResources({
          principal: { id: "user1", roles: ["USER"] },
          resource: { kind: "resource" },
          action: "relation-lt-number",
        });

        expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
        const conditions = (queryPlan as PlanResourcesConditionalResponse)
          .condition;
        expect(conditions).toEqual({
          operator: "lt",
          operands: [
            { name: "request.resource.attr.nested.aNumber" },
            { value: 2 },
          ],
        });

        const result = queryPlanToPrisma({
          queryPlan,
          mapper: {
            "request.resource.attr.nested": {
              relation: {
                name: "nested",
                type: "one",
              },
            },
          },
        });

        expect(result).toStrictEqual({
          kind: PlanKind.CONDITIONAL,
          filters: {
            nested: {
              is: {
                aNumber: { lt: 2 },
              },
            },
          },
        });

        if (result.kind !== PlanKind.CONDITIONAL) {
          throw new Error("Expected CONDITIONAL result");
        }

        const query = await prisma.resource.findMany({
          where: { ...result.filters },
        });

        expect(query.map((r) => r.id)).toEqual(
          fixtureResources
            .filter((r) => {
              const nestedResource = fixtureNestedResources.find(
                (n) => n.id === r.nested.connect?.id
              );
              return (nestedResource?.aNumber ?? 0) < 2;
            })
            .map((r) => r.id)
        );
      });

      test("conditional - relation lte with number", async () => {
        const queryPlan = await cerbos.planResources({
          principal: { id: "user1", roles: ["USER"] },
          resource: { kind: "resource" },
          action: "relation-lte-number",
        });

        expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
        const conditions = (queryPlan as PlanResourcesConditionalResponse)
          .condition;
        expect(conditions).toEqual({
          operator: "le",
          operands: [
            { name: "request.resource.attr.nested.aNumber" },
            { value: 2 },
          ],
        });

        const result = queryPlanToPrisma({
          queryPlan,
          mapper: {
            "request.resource.attr.nested": {
              relation: {
                name: "nested",
                type: "one",
              },
            },
          },
        });

        expect(result).toStrictEqual({
          kind: PlanKind.CONDITIONAL,
          filters: {
            nested: {
              is: {
                aNumber: { lte: 2 },
              },
            },
          },
        });

        if (result.kind !== PlanKind.CONDITIONAL) {
          throw new Error("Expected CONDITIONAL result");
        }

        const query = await prisma.resource.findMany({
          where: { ...result.filters },
        });

        expect(query.map((r) => r.id)).toEqual(
          fixtureResources
            .filter((r) => {
              const nestedResource = fixtureNestedResources.find(
                (n) => n.id === r.nested.connect?.id
              );
              return (nestedResource?.aNumber ?? 0) <= 2;
            })
            .map((r) => r.id)
        );
      });

      test("conditional - relation gte with number", async () => {
        const queryPlan = await cerbos.planResources({
          principal: { id: "user1", roles: ["USER"] },
          resource: { kind: "resource" },
          action: "relation-gte-number",
        });

        expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
        const conditions = (queryPlan as PlanResourcesConditionalResponse)
          .condition;
        expect(conditions).toEqual({
          operator: "ge",
          operands: [
            { name: "request.resource.attr.nested.aNumber" },
            { value: 1 },
          ],
        });

        const result = queryPlanToPrisma({
          queryPlan,
          mapper: {
            "request.resource.attr.nested": {
              relation: {
                name: "nested",
                type: "one",
              },
            },
          },
        });

        expect(result).toStrictEqual({
          kind: PlanKind.CONDITIONAL,
          filters: {
            nested: {
              is: {
                aNumber: { gte: 1 },
              },
            },
          },
        });

        if (result.kind !== PlanKind.CONDITIONAL) {
          throw new Error("Expected CONDITIONAL result");
        }

        const query = await prisma.resource.findMany({
          where: { ...result.filters },
        });

        expect(query.map((r) => r.id)).toEqual(
          fixtureResources
            .filter((r) => {
              const nestedResource = fixtureNestedResources.find(
                (n) => n.id === r.nested.connect?.id
              );
              return (nestedResource?.aNumber ?? 0) >= 1;
            })
            .map((r) => r.id)
        );
      });

      test("conditional - relation-gt-number", async () => {
        const queryPlan = await cerbos.planResources({
          principal: { id: "user1", roles: ["USER"] },
          resource: { kind: "resource" },
          action: "relation-gt-number",
        });

        expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
        const conditions = (queryPlan as PlanResourcesConditionalResponse)
          .condition;
        expect(conditions).toEqual({
          operator: "gt",
          operands: [
            { name: "request.resource.attr.nested.aNumber" },
            { value: 1 },
          ],
        });

        const result = queryPlanToPrisma({
          queryPlan,
          mapper: {
            "request.resource.attr.nested": {
              relation: {
                name: "nested",
                type: "one",
              },
            },
          },
        });

        expect(result).toStrictEqual({
          kind: PlanKind.CONDITIONAL,
          filters: {
            nested: {
              is: {
                aNumber: { gt: 1 },
              },
            },
          },
        });

        if (result.kind !== PlanKind.CONDITIONAL) {
          throw new Error("Expected CONDITIONAL result");
        }

        const query = await prisma.resource.findMany({
          where: { ...result.filters },
        });

        expect(query.map((r) => r.id)).toEqual(
          fixtureResources
            .filter((r) => {
              const nestedResource = fixtureNestedResources.find(
                (n) => n.id === r.nested.connect?.id
              );
              return (nestedResource?.aNumber ?? 0) > 1;
            })
            .map((r) => r.id)
        );
      });
    });

    describe("Deep Nesting", () => {
      test("conditional - deeply nested eq", async () => {
        const queryPlan = await cerbos.planResources({
          principal: { id: "user1", roles: ["USER"] },
          resource: { kind: "resource" },
          action: "equal-deeply-nested",
        });

        expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);

        const conditions = (queryPlan as PlanResourcesConditionalResponse)
          .condition;

        expect(conditions).toEqual({
          operator: "eq",
          operands: [
            { name: "request.resource.attr.nested.nextlevel.aBool" },
            { value: true },
          ],
        });

        const result = queryPlanToPrisma({
          queryPlan,
          mapper: {
            "request.resource.attr.nested": {
              relation: {
                name: "nested",
                type: "one",
                fields: {
                  nextlevel: {
                    relation: {
                      name: "nextlevel",
                      type: "one",
                      fields: {
                        aBool: { field: "aBool" },
                      },
                    },
                  },
                },
              },
            },
          },
        });

        expect(result).toStrictEqual({
          kind: PlanKind.CONDITIONAL,
          filters: {
            nested: {
              is: {
                nextlevel: {
                  is: {
                    aBool: {
                      equals: true,
                    },
                  },
                },
              },
            },
          },
        });

        if (result.kind !== PlanKind.CONDITIONAL) {
          throw new Error("Expected CONDITIONAL result");
        }

        const query = await prisma.resource.findMany({
          where: { ...result.filters },
        });

        expect(query.map((r) => r.id)).toEqual(
          fixtureResources
            .filter((a) => {
              const nestedResource = fixtureNestedResources.find(
                (f) => f.id === a.nested.connect?.id
              );
              const nextLevelResource = fixtureNextLevelResources.find(
                (f) => f.id === nestedResource?.nextlevel.connect?.id
              );
              return nextLevelResource?.aBool === true;
            })
            .map((r) => r.id)
        );
      });

      test("conditional - deeply nested many to many category label", async () => {
        const queryPlan = await cerbos.planResources({
          principal: { id: "user1", roles: ["USER"] },
          resource: { kind: "resource" },
          action: "deep-nested-category-label",
        });

        expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);

        expect(
          (queryPlan as PlanResourcesConditionalResponse).condition
        ).toEqual({
          operator: "exists",
          operands: [
            {
              name: "request.resource.attr.categories",
            },
            {
              operator: "lambda",
              operands: [
                {
                  operator: "exists",
                  operands: [
                    {
                      name: "cat.subCategories",
                    },
                    {
                      operator: "lambda",
                      operands: [
                        {
                          operator: "exists",
                          operands: [
                            {
                              name: "sub.labels",
                            },
                            {
                              operator: "lambda",
                              operands: [
                                {
                                  operator: "eq",
                                  operands: [
                                    {
                                      name: "label.name",
                                    },
                                    {
                                      value: "important",
                                    },
                                  ],
                                },
                                {
                                  name: "label",
                                },
                              ],
                            },
                          ],
                        },
                        {
                          name: "sub",
                        },
                      ],
                    },
                  ],
                },
                {
                  name: "cat",
                },
              ],
            },
          ],
        });

        const result = queryPlanToPrisma({
          queryPlan,
          mapper: {
            "request.resource.attr.categories": {
              relation: {
                name: "categories",
                type: "many",
                fields: {
                  subCategories: {
                    relation: {
                      name: "subCategories",
                      type: "many",
                      fields: {
                        labels: {
                          relation: {
                            name: "labels",
                            type: "many",
                            fields: {
                              name: { field: "name" },
                            },
                          },
                        },
                      },
                    },
                  },
                },
              },
            },
          },
        });

        expect(result).toStrictEqual({
          kind: PlanKind.CONDITIONAL,
          filters: {
            categories: {
              some: {
                subCategories: {
                  some: {
                    labels: {
                      some: {
                        name: { equals: "important" },
                      },
                    },
                  },
                },
              },
            },
          },
        });

        if (result.kind !== PlanKind.CONDITIONAL) {
          throw new Error("Expected CONDITIONAL result");
        }

        const query = await prisma.resource.findMany({
          where: { ...result.filters },
          include: {
            categories: {
              include: {
                subCategories: {
                  include: {
                    labels: true,
                  },
                },
              },
            },
          },
        });

        expect(query.map((r) => r.id)).toEqual(
          fixtureResources
            .filter((r) => {
              return (
                r.categories?.connect as Prisma.CategoryWhereUniqueInput[]
              )
                .map((c) => {
                  return fixtureCategories.find((fc) => fc.id === c.id);
                })
                .some((c) => {
                  return (
                    c?.subCategories
                      ?.connect as Prisma.SubCategoryWhereUniqueInput[]
                  )
                    .map((sc) => {
                      return fixtureSubCategories.find(
                        (fsc) => fsc.id === sc.id
                      );
                    })
                    .some((sc) => {
                      return (
                        sc?.labels?.connect as Prisma.LabelWhereUniqueInput[]
                      )
                        .map((l) => {
                          return fixtureLabels.find((fl) => fl.id === l.id);
                        })
                        .some((l) => l?.name === "important");
                    });
                });
            })
            .map((r) => r.id)
        );
      });

      test("conditional - deep nested exists with multiple conditions", async () => {
        const queryPlan = await cerbos.planResources({
          principal: { id: "user1", roles: ["USER"] },
          resource: { kind: "resource" },
          action: "deep-nested-exists",
        });

        const result = queryPlanToPrisma({
          queryPlan,
          mapper: {
            "request.resource.attr.categories": {
              relation: {
                name: "categories",
                type: "many",
                fields: {
                  subCategories: {
                    relation: {
                      name: "subCategories",
                      type: "many",
                      field: "name",
                    },
                  },
                },
              },
            },
          },
        });

        expect(result).toStrictEqual({
          kind: PlanKind.CONDITIONAL,
          filters: {
            categories: {
              some: {
                AND: [
                  { name: { equals: "business" } },
                  {
                    subCategories: {
                      some: {
                        name: { equals: "finance" },
                      },
                    },
                  },
                ],
              },
            },
          },
        });

        if (result.kind !== PlanKind.CONDITIONAL) {
          throw new Error("Expected CONDITIONAL result");
        }

        const query = await prisma.resource.findMany({
          where: { ...result.filters },
        });

        expect(query.map((r) => r.id)).toEqual(
          fixtureResources
            .filter((r) => {
              const categoryConnections = r.categories
                ?.connect as Prisma.CategoryWhereUniqueInput[];
              return (
                categoryConnections?.some((categoryConnection) => {
                  const category = fixtureCategories.find(
                    (fc) => fc.id === categoryConnection.id
                  );
                  return (
                    category?.name === "business" &&
                    (
                      category?.subCategories
                        ?.connect as Prisma.SubCategoryWhereUniqueInput[]
                    ).some(
                      (sc) =>
                        fixtureSubCategories.find((fsc) => fsc.id === sc.id)
                          ?.name === "finance"
                    )
                  );
                }) ?? false
              );
            })
            .map((r) => r.id)
        );
      });
    });

    describe("Nested String Operations", () => {
      test("conditional - nested contains", async () => {
        const queryPlan = await cerbos.planResources({
          principal: { id: "user1", roles: ["USER"] },
          resource: { kind: "resource" },
          action: "nested-contains",
        });

        const result = queryPlanToPrisma({
          queryPlan,
          mapper: {
            "request.resource.attr.nested": {
              relation: {
                name: "nested",
                type: "one",
                fields: {
                  aString: { field: "aString" },
                },
              },
            },
          },
        });

        expect(result).toStrictEqual({
          kind: PlanKind.CONDITIONAL,
          filters: {
            nested: {
              is: {
                aString: { contains: "str" },
              },
            },
          },
        });

        if (result.kind !== PlanKind.CONDITIONAL) {
          throw new Error("Expected CONDITIONAL result");
        }

        const query = await prisma.resource.findMany({
          where: { ...result.filters },
        });

        expect(query.map((r) => r.id)).toEqual(
          fixtureResources
            .filter((r) => {
              const nested = fixtureNestedResources.find(
                (n) => n.id === r.nested.connect?.id
              );
              return nested?.aString.includes("str");
            })
            .map((r) => r.id)
        );
      });

      test("conditional - deeply nested startsWith", async () => {
        const queryPlan = await cerbos.planResources({
          principal: { id: "user1", roles: ["USER"] },
          resource: { kind: "resource" },
          action: "deeply-nested-starts-with",
        });

        const result = queryPlanToPrisma({
          queryPlan,
          mapper: {
            "request.resource.attr.nested": {
              relation: {
                name: "nested",
                type: "one",
                fields: {
                  nextlevel: {
                    relation: {
                      name: "nextlevel",
                      type: "one",
                      fields: {
                        aString: { field: "aString" },
                      },
                    },
                  },
                },
              },
            },
          },
        });

        expect(result).toStrictEqual({
          kind: PlanKind.CONDITIONAL,
          filters: {
            nested: {
              is: {
                nextlevel: {
                  is: {
                    aString: { startsWith: "str" },
                  },
                },
              },
            },
          },
        });

        if (result.kind !== PlanKind.CONDITIONAL) {
          throw new Error("Expected CONDITIONAL result");
        }

        const query = await prisma.resource.findMany({
          where: { ...result.filters },
        });

        expect(query.map((r) => r.id)).toEqual(
          fixtureResources
            .filter((r) => {
              const nested = fixtureNestedResources.find(
                (n) => n.id === r.nested.connect?.id
              );
              const nextLevel = fixtureNextLevelResources.find(
                (nl) => nl.id === nested?.nextlevel.connect?.id
              );
              return nextLevel?.aString.startsWith("str");
            })
            .map((r) => r.id)
        );
      });
    });
  });
});

// Complex Operations
describe("Complex Operations", () => {
  describe("Logical Operations", () => {
    test("conditional - and", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "and",
      });

      expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
      expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual(
        {
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
              operator: "ne",
              operands: [
                { name: "request.resource.attr.aString" },
                { value: "string" },
              ],
            },
          ],
        }
      );

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.aBool": { field: "aBool" },
          "request.resource.attr.aString": { field: "aString" },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          AND: [
            {
              aBool: { equals: true },
            },
            {
              aString: { not: "string" },
            },
          ],
        },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });
      expect(query).toEqual(
        fixtureResources
          .filter((r) => {
            return r.aBool && r.aString != "string";
          })
          .map((f) => ({ ...f, owners: undefined }))
      );
    });

    test("conditional - or", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "or",
      });

      expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
      expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual(
        {
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
              operator: "ne",
              operands: [
                { name: "request.resource.attr.aString" },
                { value: "string" },
              ],
            },
          ],
        }
      );

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.aBool": { field: "aBool" },
          "request.resource.attr.aString": { field: "aString" },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          OR: [
            {
              aBool: { equals: true },
            },
            {
              aString: { not: "string" },
            },
          ],
        },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources
          .filter((r) => {
            return r.aBool || r.aString != "string";
          })
          .map((r) => r.id)
      );
    });

    test("conditional - relation multiple all", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "relation-multiple-all",
      });

      expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
      expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual(
        {
          operator: "and",
          operands: [
            {
              operator: "gt",
              operands: [
                { name: "request.resource.attr.nested.aNumber" },
                { value: 1 },
              ],
            },
            {
              operator: "lt",
              operands: [
                { name: "request.resource.attr.nested.aNumber" },
                { value: 3 },
              ],
            },
          ],
        }
      );

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.nested": {
            relation: {
              name: "nested",
              type: "one",
            },
          },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          AND: [
            {
              nested: {
                is: {
                  aNumber: { gt: 1 },
                },
              },
            },
            {
              nested: {
                is: {
                  aNumber: { lt: 3 },
                },
              },
            },
          ],
        },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });

      expect(query.map((r) => r.id)).toEqual(
        fixtureResources
          .filter((r) => {
            const nestedResource = fixtureNestedResources.find(
              (n) => n.id === r.nested.connect?.id
            );
            return (
              (nestedResource?.aNumber ?? 0) > 1 &&
              (nestedResource?.aNumber ?? 0) < 3
            );
          })
          .map((r) => r.id)
      );
    });

    test("conditional - relation multiple or", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "relation-multiple-or",
      });

      expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
      expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual(
        {
          operator: "or",
          operands: [
            {
              operator: "eq",
              operands: [
                { name: "request.resource.attr.createdBy" },
                { value: "user1" },
              ],
            },
            {
              operator: "in",
              operands: [
                { value: "user1" },
                { name: "request.resource.attr.ownedBy" },
              ],
            },
          ],
        }
      );

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.createdBy": {
            relation: {
              name: "createdBy",
              type: "one",
              field: "id",
            },
          },
          "request.resource.attr.ownedBy": {
            relation: {
              name: "ownedBy",
              type: "many",
              field: "id",
            },
          },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          OR: [
            {
              createdBy: {
                is: {
                  id: {
                    equals: "user1",
                  },
                },
              },
            },
            {
              ownedBy: {
                some: {
                  id: "user1",
                },
              },
            },
          ],
        },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });

      // Should return resources where user1 is either creator or owner
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources
          .filter((r) => {
            const createdBy = r.createdBy?.connect?.id;
            const ownedBy = Array.isArray(r.ownedBy?.connect)
              ? r.ownedBy?.connect.find((o) => o.id == "user1")
              : undefined;
            return createdBy == "user1" || ownedBy;
          })
          .map((r) => r.id)
      );
    });

    test("conditional - relation multiple none", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "relation-multiple-none",
      });

      expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
      expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual(
        {
          operator: "and",
          operands: [
            {
              operator: "not",
              operands: [
                {
                  operator: "eq",
                  operands: [
                    { name: "request.resource.attr.createdBy" },
                    { value: "user1" },
                  ],
                },
              ],
            },
            {
              operator: "not",
              operands: [
                {
                  operator: "in",
                  operands: [
                    { value: "public" },
                    { name: "request.resource.attr.tags" },
                  ],
                },
              ],
            },
          ],
        }
      );

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.createdBy": {
            relation: {
              name: "createdBy",
              type: "one",
              field: "id",
            },
          },
          "request.resource.attr.tags": {
            relation: {
              name: "tags",
              type: "many",
              field: "name",
            },
          },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          AND: [
            {
              NOT: {
                createdBy: {
                  is: {
                    id: {
                      equals: "user1",
                    },
                  },
                },
              },
            },
            {
              NOT: {
                tags: {
                  some: {
                    name: "public",
                  },
                },
              },
            },
          ],
        },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });

      // Should return resources where user1 is creator AND has public tag
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources
          .filter((r) => {
            const createdBy = r.createdBy?.connect?.id;
            const hasPublicTag =
              Array.isArray(r.tags?.connect) &&
              r.tags?.connect.find((t) => t.id === "tag1"); // tag1 is public
            return !(createdBy === "user1" && hasPublicTag);
          })
          .map((r) => r.id)
      );
    });
  });

  describe("Collection Operations", () => {
    test("conditional - exists on nested collection", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "exists-nested-collection",
      });

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.categories": {
            relation: {
              name: "categories",
              type: "many",
              fields: {
                name: { field: "name" },
                subCategories: {
                  relation: {
                    name: "subCategories",
                    type: "many",
                    fields: {
                      name: { field: "name" },
                    },
                  },
                },
              },
            },
          },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          categories: {
            some: {
              AND: [
                { name: { equals: "business" } },
                {
                  subCategories: {
                    some: {
                      name: { equals: "finance" },
                    },
                  },
                },
              ],
            },
          },
        },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await prisma.resource.findMany({
        where: { ...result.filters },
      });

      expect(query.map((r) => r.id)).toEqual(
        fixtureResources
          .filter((r) => {
            const categoryConnections = r.categories
              ?.connect as Prisma.CategoryWhereUniqueInput[];
            return categoryConnections?.some((categoryConnection) => {
              const category = fixtureCategories.find(
                (fc) => fc.id === categoryConnection.id
              );
              return (
                category?.name === "business" &&
                (
                  category?.subCategories
                    ?.connect as Prisma.SubCategoryWhereUniqueInput[]
                )?.some(
                  (sc) =>
                    fixtureSubCategories.find((fsc) => fsc.id === sc.id)
                      ?.name === "finance"
                )
              );
            });
          })
          .map((r) => r.id)
      );
    });

    test("isSet with nested relation", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "is-set-nested",
      });

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.nested.aOptionalString": {
            relation: {
              name: "nested",
              type: "one",
              fields: {
                aOptionalString: { field: "aOptionalString" },
              },
            },
          },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          nested: {
            is: {
              aOptionalString: { not: null },
            },
          },
        },
      });
    });

    test("hasIntersection with nested collection", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "has-intersection-nested",
      });

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.categories": {
            relation: {
              name: "categories",
              type: "many",
              fields: {
                subCategories: {
                  relation: {
                    name: "subCategories",
                    type: "many",
                    fields: {
                      name: { field: "name" },
                    },
                  },
                },
              },
            },
          },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          categories: {
            some: {
              subCategories: {
                some: {
                  name: { in: ["finance", "tech"] },
                },
              },
            },
          },
        },
      });
    });
  });

  describe("Deep Nesting", () => {
    test("filter on deeply nested relation", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "filter-deeply-nested",
      });

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.categories": {
            relation: {
              name: "categories",
              type: "many",
              fields: {
                subCategories: {
                  relation: {
                    name: "subCategories",
                    type: "many",
                    fields: {
                      labels: {
                        relation: {
                          name: "labels",
                          type: "many",
                          fields: {
                            name: { field: "name" },
                          },
                        },
                      },
                    },
                  },
                },
              },
            },
          },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          categories: {
            some: {
              subCategories: {
                some: {
                  labels: {
                    some: {
                      name: { equals: "important" },
                    },
                  },
                },
              },
            },
          },
        },
      });
    });

    test("map on deeply nested relation", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "map-deeply-nested",
      });

      const result = queryPlanToPrisma({
        queryPlan,
        mapper: {
          "request.resource.attr.categories": {
            relation: {
              name: "categories",
              type: "many",
              fields: {
                subCategories: {
                  relation: {
                    name: "subCategories",
                    type: "many",
                    fields: {
                      name: { field: "name" },
                    },
                  },
                },
              },
            },
          },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          categories: {
            some: {
              subCategories: {
                some: {
                  name: { in: ["finance", "tech"] },
                },
              },
            },
          },
        },
      });
    });
  });
});

// Error Handling
describe("Error Cases", () => {
  test("throws error for invalid query plan", () => {
    const invalidQueryPlan = {
      kind: "INVALID_KIND" as PlanKind,
    };

    expect(() =>
      queryPlanToPrisma({
        queryPlan: invalidQueryPlan as unknown as PlanResourcesResponse,
        mapper: {},
      })
    ).toThrow("Invalid query plan.");
  });

  test("throws error for invalid expression structure", () => {
    const invalidQueryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        // Missing operator and operands
      },
    };

    expect(() =>
      queryPlanToPrisma({
        queryPlan: invalidQueryPlan as unknown as PlanResourcesResponse,
        mapper: {},
      })
    ).toThrow("Invalid Cerbos expression structure");
  });

  test("throws error for unsupported operator", () => {
    const invalidQueryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "unsupported",
        operands: [],
      },
    };

    expect(() =>
      queryPlanToPrisma({
        queryPlan: invalidQueryPlan as unknown as PlanResourcesResponse,
        mapper: {},
      })
    ).toThrow("Unsupported operator: unsupported");
  });

  test("throws error for invalid operand structure", () => {
    const invalidQueryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "eq",
        operands: [
          {}, // Invalid operand without name or value
          { value: "test" },
        ],
      },
    };

    expect(() =>
      queryPlanToPrisma({
        queryPlan: invalidQueryPlan as unknown as PlanResourcesResponse,
        mapper: {},
      })
    ).toThrow("No valid left operand found");
  });
});

// Utility Tests
describe("Mapper Functions", () => {
  test("function mapper for field names", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "equal",
    });

    const result = queryPlanToPrisma({
      queryPlan,
      mapper: (key: string) => ({
        field: key.replace("request.resource.attr.", ""),
      }),
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { aBool: { equals: true } },
    });
  });

  test("function mapper for relations", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "relation-is",
    });

    const result = queryPlanToPrisma({
      queryPlan,
      mapper: () => ({
        relation: {
          name: "createdBy",
          type: "one",
          field: "id",
        },
      }),
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        createdBy: {
          is: {
            id: {
              equals: "user1",
            },
          },
        },
      },
    });
  });
});

// Integration
describe("Integration", () => {
  test("conditional - kitchen sink", async () => {
    const queryPlan = await cerbos.planResources({
      principal: {
        id: "user1",
        roles: ["USER"],
        attr: { tags: ["public", "draft"] },
      },
      resource: { kind: "resource" },
      action: "kitchensink",
    });

    expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);

    // expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
    //   operator: "filter",
    //   operands: [
    //     {
    //       name: "request.resource.attr.tags",
    //     },
    //     {
    //       operator: "lambda",
    //       operands: [
    //         {
    //           operator: "eq",
    //           operands: [
    //             {
    //               name: "tag.name",
    //             },
    //             {
    //               value: "public",
    //             },
    //           ],
    //         },
    //         {
    //           name: "tag",
    //         },
    //       ],
    //     },
    //   ],
    // });

    const result = queryPlanToPrisma({
      queryPlan,
      mapper: {
        "request.resource.attr.aOptionalString": { field: "aOptionalString" },
        "request.resource.attr.aBool": { field: "aBool" },
        "request.resource.attr.aString": { field: "aString" },
        "request.resource.attr.tags": {
          relation: {
            name: "tags",
            type: "many",
          },
        },
        "request.resource.attr.nested": {
          relation: {
            name: "nested",
            type: "one",
            fields: {
              nextlevel: {
                relation: {
                  name: "nextlevel",
                  type: "one",
                  fields: {
                    aBool: { field: "aBool" },
                  },
                },
              },
            },
          },
        },
      },
    });

    // expect(result).toStrictEqual({
    //   kind: PlanKind.CONDITIONAL,
    //   filters: {
    //     tags: {
    //       some: {
    //         name: { equals: "public" },
    //       },
    //     },
    //   },
    // });

    if (result.kind !== PlanKind.CONDITIONAL) {
      throw new Error("Expected CONDITIONAL result");
    }

    // console.log(JSON.stringify(result, null, 2));
    await prisma.resource.findMany({
      where: { ...result.filters },
    });

    // expect(query.map((r) => r.id)).toEqual(
    //   fixtureResources
    //     .filter(
    //       (a) =>
    //         Array.isArray(a.tags?.connect) &&
    //         a.tags?.connect
    //           .map((t) => {
    //             return fixtureTags.find((f) => f.id === t.id);
    //           })
    //           .filter((t) => t?.name === "public").length > 0
    //     )
    //     .map((r) => r.id)
    // );
  });

  test("conditional - relation-has-members (size > 0)", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "relation-has-members",
    });

    expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);

    const result = queryPlanToPrisma({
      queryPlan,
      mapper: {
        "request.resource.attr.ownedBy": {
          relation: { name: "ownedBy", type: "many", field: "id" },
        },
      },
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { ownedBy: { some: {} } },
    });

    if (result.kind !== PlanKind.CONDITIONAL) {
      throw new Error("Expected CONDITIONAL result");
    }

    const query = await prisma.resource.findMany({
      where: { ...result.filters },
    });

    expect(query.map((r) => r.id)).toEqual(
      fixtureResources
        .filter((r) => {
          if (!r.ownedBy?.connect) return false;
          return (r.ownedBy.connect as { id: string }[]).length > 0;
        })
        .map((r) => r.id)
    );
  });

  test("conditional - relation-has-no-members (negated size > 0)", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "relation-has-no-members",
    });

    expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);

    const result = queryPlanToPrisma({
      queryPlan,
      mapper: {
        "request.resource.attr.ownedBy": {
          relation: { name: "ownedBy", type: "many", field: "id" },
        },
      },
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { NOT: { ownedBy: { some: {} } } },
    });

    if (result.kind !== PlanKind.CONDITIONAL) {
      throw new Error("Expected CONDITIONAL result");
    }

    const query = await prisma.resource.findMany({
      where: { ...result.filters },
    });

    expect(query.map((r) => r.id)).toEqual(
      fixtureResources
        .filter((r) => {
          if (!r.ownedBy?.connect) return true;
          return (r.ownedBy.connect as { id: string }[]).length === 0;
        })
        .map((r) => r.id)
    );
  });
});

describe("Deep Nested Relations", () => {
  test("conditional - deeply nested many to many category label", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "deep-nested-category-label",
    });

    expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);

    expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
      operator: "exists",
      operands: [
        {
          name: "request.resource.attr.categories",
        },
        {
          operator: "lambda",
          operands: [
            {
              operator: "exists",
              operands: [
                {
                  name: "cat.subCategories",
                },
                {
                  operator: "lambda",
                  operands: [
                    {
                      operator: "exists",
                      operands: [
                        {
                          name: "sub.labels",
                        },
                        {
                          operator: "lambda",
                          operands: [
                            {
                              operator: "eq",
                              operands: [
                                {
                                  name: "label.name",
                                },
                                {
                                  value: "important",
                                },
                              ],
                            },
                            {
                              name: "label",
                            },
                          ],
                        },
                      ],
                    },
                    {
                      name: "sub",
                    },
                  ],
                },
              ],
            },
            {
              name: "cat",
            },
          ],
        },
      ],
    });

    const result = queryPlanToPrisma({
      queryPlan,
      mapper: {
        "request.resource.attr.categories": {
          relation: {
            name: "categories",
            type: "many",
            fields: {
              subCategories: {
                relation: {
                  name: "subCategories",
                  type: "many",
                  fields: {
                    labels: {
                      relation: {
                        name: "labels",
                        type: "many",
                        fields: {
                          name: { field: "name" },
                        },
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        categories: {
          some: {
            subCategories: {
              some: {
                labels: {
                  some: {
                    name: { equals: "important" },
                  },
                },
              },
            },
          },
        },
      },
    });

    if (result.kind !== PlanKind.CONDITIONAL) {
      throw new Error("Expected CONDITIONAL result");
    }

    const query = await prisma.resource.findMany({
      where: { ...result.filters },
      include: {
        categories: {
          include: {
            subCategories: {
              include: {
                labels: true,
              },
            },
          },
        },
      },
    });

    expect(query.map((r) => r.id)).toEqual(
      fixtureResources
        .filter((r) => {
          return (r.categories?.connect as Prisma.CategoryWhereUniqueInput[])
            .map((c) => {
              return fixtureCategories.find((fc) => fc.id === c.id);
            })
            .some((c) => {
              return (
                c?.subCategories
                  ?.connect as Prisma.SubCategoryWhereUniqueInput[]
              )
                .map((sc) => {
                  return fixtureSubCategories.find((fsc) => fsc.id === sc.id);
                })
                .some((sc) => {
                  return (sc?.labels?.connect as Prisma.LabelWhereUniqueInput[])
                    .map((l) => {
                      return fixtureLabels.find((fl) => fl.id === l.id);
                    })
                    .some((l) => l?.name === "important");
                });
            });
        })
        .map((r) => r.id)
    );
  });

  test("conditional - deep nested exists with multiple conditions", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "deep-nested-exists",
    });

    const result = queryPlanToPrisma({
      queryPlan,
      mapper: {
        "request.resource.attr.categories": {
          relation: {
            name: "categories",
            type: "many",
            fields: {
              subCategories: {
                relation: {
                  name: "subCategories",
                  type: "many",
                  field: "name",
                },
              },
            },
          },
        },
      },
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        categories: {
          some: {
            AND: [
              { name: { equals: "business" } },
              {
                subCategories: {
                  some: {
                    name: { equals: "finance" },
                  },
                },
              },
            ],
          },
        },
      },
    });

    if (result.kind !== PlanKind.CONDITIONAL) {
      throw new Error("Expected CONDITIONAL result");
    }

    const query = await prisma.resource.findMany({
      where: { ...result.filters },
    });

    expect(query.map((r) => r.id)).toEqual(
      fixtureResources
        .filter((r) => {
          const categoryConnections = r.categories
            ?.connect as Prisma.CategoryWhereUniqueInput[];
          return (
            categoryConnections?.some((categoryConnection) => {
              const category = fixtureCategories.find(
                (fc) => fc.id === categoryConnection.id
              );
              return (
                category?.name === "business" &&
                (
                  category?.subCategories
                    ?.connect as Prisma.SubCategoryWhereUniqueInput[]
                )?.some(
                  (sc) =>
                    fixtureSubCategories.find((fsc) => fsc.id === sc.id)
                      ?.name === "finance"
                )
              );
            }) ?? false
          );
        })
        .map((r) => r.id)
    );
  });
});

describe("Nested Relations String Operations", () => {
  test("conditional - nested contains", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "nested-contains",
    });

    const result = queryPlanToPrisma({
      queryPlan,
      mapper: {
        "request.resource.attr.nested": {
          relation: {
            name: "nested",
            type: "one",
            fields: {
              aString: { field: "aString" },
            },
          },
        },
      },
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        nested: {
          is: {
            aString: { contains: "str" },
          },
        },
      },
    });

    if (result.kind !== PlanKind.CONDITIONAL) {
      throw new Error("Expected CONDITIONAL result");
    }

    const query = await prisma.resource.findMany({
      where: { ...result.filters },
    });

    expect(query.map((r) => r.id)).toEqual(
      fixtureResources
        .filter((r) => {
          const nested = fixtureNestedResources.find(
            (n) => n.id === r.nested.connect?.id
          );
          return nested?.aString.includes("str");
        })
        .map((r) => r.id)
    );
  });

  test("conditional - deeply nested startsWith", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "deeply-nested-starts-with",
    });

    const result = queryPlanToPrisma({
      queryPlan,
      mapper: {
        "request.resource.attr.nested": {
          relation: {
            name: "nested",
            type: "one",
            fields: {
              nextlevel: {
                relation: {
                  name: "nextlevel",
                  type: "one",
                  fields: {
                    aString: { field: "aString" },
                  },
                },
              },
            },
          },
        },
      },
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        nested: {
          is: {
            nextlevel: {
              is: {
                aString: { startsWith: "str" },
              },
            },
          },
        },
      },
    });

    if (result.kind !== PlanKind.CONDITIONAL) {
      throw new Error("Expected CONDITIONAL result");
    }

    const query = await prisma.resource.findMany({
      where: { ...result.filters },
    });

    expect(query.map((r) => r.id)).toEqual(
      fixtureResources
        .filter((r) => {
          const nested = fixtureNestedResources.find(
            (n) => n.id === r.nested.connect?.id
          );
          const nextLevel = fixtureNextLevelResources.find(
            (nl) => nl.id === nested?.nextlevel.connect?.id
          );
          return nextLevel?.aString.startsWith("str");
        })
        .map((r) => r.id)
    );
  });
});

describe("Collection Operations with Nested Relations", () => {
  test("conditional - exists on nested collection", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "exists-nested-collection",
    });

    const result = queryPlanToPrisma({
      queryPlan,
      mapper: {
        "request.resource.attr.categories": {
          relation: {
            name: "categories",
            type: "many",
            fields: {
              name: { field: "name" },
              subCategories: {
                relation: {
                  name: "subCategories",
                  type: "many",
                  fields: {
                    name: { field: "name" },
                  },
                },
              },
            },
          },
        },
      },
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        categories: {
          some: {
            AND: [
              { name: { equals: "business" } },
              {
                subCategories: {
                  some: {
                    name: { equals: "finance" },
                  },
                },
              },
            ],
          },
        },
      },
    });

    if (result.kind !== PlanKind.CONDITIONAL) {
      throw new Error("Expected CONDITIONAL result");
    }

    const query = await prisma.resource.findMany({
      where: { ...result.filters },
    });

    expect(query.map((r) => r.id)).toEqual(
      fixtureResources
        .filter((r) => {
          const categoryConnections = r.categories
            ?.connect as Prisma.CategoryWhereUniqueInput[];
          return categoryConnections?.some((categoryConnection) => {
            const category = fixtureCategories.find(
              (fc) => fc.id === categoryConnection.id
            );
            return (
              category?.name === "business" &&
              (
                category?.subCategories
                  ?.connect as Prisma.SubCategoryWhereUniqueInput[]
              )?.some(
                (sc) =>
                  fixtureSubCategories.find((fsc) => fsc.id === sc.id)?.name ===
                  "finance"
              )
            );
          });
        })
        .map((r) => r.id)
    );
  });
});

// Types
describe("Return Types", () => {
  test("returns ALWAYS_ALLOWED type", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "always-allow",
    });

    const result = queryPlanToPrisma({
      queryPlan,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.ALWAYS_ALLOWED,
    });

    // Type assertion check
    if (result.kind === PlanKind.ALWAYS_ALLOWED) {
      expect(Object.keys(result)).toEqual(["kind"]);
    } else {
      throw new Error("Expected ALWAYS_ALLOWED result");
    }
  });

  test("returns ALWAYS_DENIED type", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "always-deny",
    });

    const result = queryPlanToPrisma({
      queryPlan,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.ALWAYS_DENIED,
    });

    // Type assertion check
    if (result.kind === PlanKind.ALWAYS_DENIED) {
      expect(Object.keys(result)).toEqual(["kind"]);
    } else {
      throw new Error("Expected ALWAYS_DENIED result");
    }
  });

  test("returns CONDITIONAL type with filters", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "equal",
    });

    const result = queryPlanToPrisma({
      queryPlan,
      mapper: {
        "request.resource.attr.aBool": { field: "aBool" },
      },
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { aBool: { equals: true } },
    });

    // Type assertion check
    if (result.kind === PlanKind.CONDITIONAL) {
      expect(Object.keys(result).sort()).toEqual(["filters", "kind"].sort());
      expect(typeof result.filters).toBe("object");
    } else {
      throw new Error("Expected CONDITIONAL result");
    }
  });

  test("validates result type structure at compile time", () => {
    // Type-level test
    type ResultType = QueryPlanToPrismaResult;

    // These should compile
    const allowed: ResultType = { kind: PlanKind.ALWAYS_ALLOWED };
    const denied: ResultType = { kind: PlanKind.ALWAYS_DENIED };
    const conditional: ResultType = {
      kind: PlanKind.CONDITIONAL,
      filters: { someField: { equals: true } },
    };

    // Verify the objects exist to prevent unused variable warnings
    expect(allowed.kind).toBe(PlanKind.ALWAYS_ALLOWED);
    expect(denied.kind).toBe(PlanKind.ALWAYS_DENIED);
    expect(conditional.kind).toBe(PlanKind.CONDITIONAL);
  });
});
