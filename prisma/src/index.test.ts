import { queryPlanToPrisma, PlanKind } from ".";
import { beforeAll, beforeEach, afterEach, test, expect } from "@jest/globals";
import {
  PlanExpression,
  PlanResourcesConditionalResponse,
  PlanResourcesResponse,
} from "@cerbos/core";
import { Prisma, PrismaClient } from "@prisma/client";
import { GRPC as Cerbos } from "@cerbos/grpc";

const prisma = new PrismaClient();
const cerbos = new Cerbos("127.0.0.1:3593", { tls: false });

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

const fixtureNestedResources: Prisma.NestedResourceCreateInput[] = [
  {
    id: "nested1",
    aBool: true,
    aNumber: 1,
    aString: "string",
  },
  {
    id: "nested2",
    aBool: false,
    aNumber: 1,
    aString: "string",
  },
  {
    id: "nested3",
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
  },
];

beforeAll(async () => {
  await prisma.resource.deleteMany();
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
  for (const resource of fixtureNestedResources) {
    await prisma.nestedResource.create({ data: resource });
  }
  for (const resource of fixtureResources) {
    await prisma.resource.create({ data: resource });
  }
});

afterEach(async () => {
  await prisma.resource.deleteMany();
  await prisma.nestedResource.deleteMany();
  await prisma.tag.deleteMany();
  await prisma.user.deleteMany();
});

test("always allowed", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "always-allow",
  });

  expect(queryPlan.kind).toEqual(PlanKind.ALWAYS_ALLOWED);

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {},
  });

  expect(result).toStrictEqual({
    kind: PlanKind.ALWAYS_ALLOWED,
    filters: {},
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
    fieldNameMapper: {},
  });

  expect(result).toEqual({
    kind: PlanKind.ALWAYS_DENIED,
  });
});

test("conditional - eq", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "equal",
  });

  expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
  expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
    operator: "eq",
    operands: [{ name: "request.resource.attr.aBool" }, { value: true }],
  });

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.aBool": "aBool",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: { aBool: { equals: true } },
  });
  const query = await prisma.resource.findMany({
    where: { ...result.filters },
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

  expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
    operator: "eq",
    operands: [{ name: "request.resource.attr.aBool" }, { value: true }],
  });

  const typeQp = queryPlan as PlanResourcesConditionalResponse;

  const invertedQueryPlan: PlanResourcesConditionalResponse = {
    ...typeQp,
    condition: {
      ...typeQp.condition,
      operands: [
        (typeQp.condition as PlanExpression).operands[1],
        (typeQp.condition as PlanExpression).operands[0],
      ],
    },
  };

  const result = queryPlanToPrisma({
    queryPlan: invertedQueryPlan,
    fieldNameMapper: {
      "request.resource.attr.aBool": "aBool",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: { aBool: { equals: true } },
  });

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
  expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
    operator: "ne",
    operands: [{ name: "request.resource.attr.aString" }, { value: "string" }],
  });

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.aString": "aString",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: { aString: { not: "string" } },
  });
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
  expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
    operator: "not",
    operands: [
      {
        operator: "eq",
        operands: [{ name: "request.resource.attr.aBool" }, { value: true }],
      },
    ],
  });

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.aBool": "aBool",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: { NOT: { aBool: { equals: true } } },
  });
  const query = await prisma.resource.findMany({
    where: { ...result.filters },
  });
  expect(query.map((r) => r.id)).toEqual(
    fixtureResources.filter((a) => !a.aBool).map((r) => r.id)
  );
});

test("conditional - and", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "and",
  });

  expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
  expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
    operator: "and",
    operands: [
      {
        operator: "eq",
        operands: [{ name: "request.resource.attr.aBool" }, { value: true }],
      },
      {
        operator: "ne",
        operands: [
          { name: "request.resource.attr.aString" },
          { value: "string" },
        ],
      },
    ],
  });

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.aBool": "aBool",
      "request.resource.attr.aString": "aString",
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
  expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
    operator: "or",
    operands: [
      {
        operator: "eq",
        operands: [{ name: "request.resource.attr.aBool" }, { value: true }],
      },
      {
        operator: "ne",
        operands: [
          { name: "request.resource.attr.aString" },
          { value: "string" },
        ],
      },
    ],
  });

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.aBool": "aBool",
      "request.resource.attr.aString": "aString",
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

test("conditional - in", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "in",
  });

  expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
  expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
    operator: "in",
    operands: [
      { name: "request.resource.attr.aString" },
      { value: ["string", "anotherString"] },
    ],
  });

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.aString": "aString",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: {
      aString: { in: ["string", "anotherString"] },
    },
  });

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

test("conditional - gt", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "gt",
  });

  expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
  expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
    operator: "gt",
    operands: [{ name: "request.resource.attr.aNumber" }, { value: 1 }],
  });

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.aNumber": "aNumber",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: {
      aNumber: { gt: 1 },
    },
  });

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
  expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
    operator: "lt",
    operands: [{ name: "request.resource.attr.aNumber" }, { value: 2 }],
  });

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.aNumber": "aNumber",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: {
      aNumber: { lt: 2 },
    },
  });

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
  expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
    operator: "ge",
    operands: [{ name: "request.resource.attr.aNumber" }, { value: 1 }],
  });

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.aNumber": "aNumber",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: {
      aNumber: { gte: 1 },
    },
  });

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
  expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
    operator: "le",
    operands: [{ name: "request.resource.attr.aNumber" }, { value: 2 }],
  });

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.aNumber": "aNumber",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: {
      aNumber: { lte: 2 },
    },
  });

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

test("conditional - relation some", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "relation-some",
  });

  expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);

  const conditions = (queryPlan as PlanResourcesConditionalResponse).condition;

  expect(conditions).toEqual({
    operator: "in",
    operands: [{ value: "user1" }, { name: "request.resource.attr.ownedBy" }],
  });

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {},
    relationMapper: {
      "request.resource.attr.ownedBy": {
        relation: "ownedBy",
        field: "id",
        type: "many",
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

  const query = await prisma.resource.findMany({
    where: { ...result.filters },
  });

  expect(query.map((r) => r.id)).toEqual(
    fixtureResources
      .filter((r) => {
        if (!r.ownedBy?.connect) return false;
        return (
          (r.ownedBy.connect as { id: string }[]).filter((o) => o.id == "user1")
            .length > 0
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

  const conditions = (queryPlan as PlanResourcesConditionalResponse).condition;

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
    fieldNameMapper: {},
    relationMapper: {
      "request.resource.attr.ownedBy": {
        relation: "ownedBy",
        field: "id",
        type: "many",
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

  const query = await prisma.resource.findMany({
    where: { ...result.filters },
  });

  expect(query.map((r) => r.id)).toEqual(
    fixtureResources
      .filter((r) => {
        if (!r.ownedBy?.connect) return false;
        return (
          (r.ownedBy.connect as { id: string }[]).filter((o) => o.id == "user1")
            .length == 0
        );
      })
      .map((r) => r.id)
  );
});

test("conditional - relation is", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "relation-is",
  });

  expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);

  const conditions = (queryPlan as PlanResourcesConditionalResponse).condition;

  expect(conditions).toEqual({
    operator: "eq",
    operands: [{ name: "request.resource.attr.createdBy" }, { value: "user1" }],
  });

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {},
    relationMapper: {
      "request.resource.attr.createdBy": {
        relation: "createdBy",
        field: "id",
        type: "one",
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

  const conditions = (queryPlan as PlanResourcesConditionalResponse).condition;

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
    fieldNameMapper: {},
    relationMapper: {
      "request.resource.attr.createdBy": {
        relation: "createdBy",
        field: "id",
        type: "one",
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

test("conditional - nested eq", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "equal-nested",
  });

  expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);

  const conditions = (queryPlan as PlanResourcesConditionalResponse).condition;

  expect(conditions).toEqual({
    operator: "eq",
    operands: [{ name: "request.resource.attr.nested.aBool" }, { value: true }],
  });

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.nested.aBool": "nested.aBool",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: { nested: { aBool: { equals: true } } },
  });
  const query = await prisma.resource.findMany({
    where: { ...result.filters },
  });

  expect(query.map((r) => r.id)).toEqual(
    fixtureResources
      .filter(
        (a) =>
          fixtureNestedResources.find((f) => f.id === a.nested.connect?.id)
            ?.aBool
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

  const conditions = (queryPlan as PlanResourcesConditionalResponse).condition;

  expect(conditions).toEqual({
    operator: "eq",
    operands: [{ name: "request.resource.attr.nested.aNumber" }, { value: 1 }],
  });

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {},
    relationMapper: {
      "request.resource.attr.nested.aNumber": {
        relation: "nested",
        field: "aNumber",
        type: "one",
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

test("conditional - relation lt with number", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "relation-lt-number",
  });

  expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
  const conditions = (queryPlan as PlanResourcesConditionalResponse).condition;
  expect(conditions).toEqual({
    operator: "lt",
    operands: [{ name: "request.resource.attr.nested.aNumber" }, { value: 2 }],
  });

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {},
    relationMapper: {
      "request.resource.attr.nested.aNumber": {
        relation: "nested",
        field: "aNumber",
        type: "one",
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
  const conditions = (queryPlan as PlanResourcesConditionalResponse).condition;
  expect(conditions).toEqual({
    operator: "le",
    operands: [{ name: "request.resource.attr.nested.aNumber" }, { value: 2 }],
  });

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {},
    relationMapper: {
      "request.resource.attr.nested.aNumber": {
        relation: "nested",
        field: "aNumber",
        type: "one",
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
  const conditions = (queryPlan as PlanResourcesConditionalResponse).condition;
  expect(conditions).toEqual({
    operator: "ge",
    operands: [{ name: "request.resource.attr.nested.aNumber" }, { value: 1 }],
  });

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {},
    relationMapper: {
      "request.resource.attr.nested.aNumber": {
        relation: "nested",
        field: "aNumber",
        type: "one",
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

test("conditional - has-tag", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "has-tag",
  });

  expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
  expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
    operator: "in",
    operands: [{ value: "public" }, { name: "request.resource.attr.tags" }],
  });

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {},
    relationMapper: {
      "request.resource.attr.tags": {
        relation: "tags",
        field: "name",
        type: "many",
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

  const query = await prisma.resource.findMany({
    where: { ...result.filters },
    include: { tags: true },
  });

  expect(query.map((r) => r.id)).toEqual(["resource1", "resource3"]);
});

test("conditional - has-no-tag", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "has-no-tag",
  });

  expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
  expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
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
    fieldNameMapper: {},
    relationMapper: {
      "request.resource.attr.tags": {
        relation: "tags",
        field: "name",
        type: "many",
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

  const query = await prisma.resource.findMany({
    where: { ...result.filters },
    include: { tags: true },
  });

  expect(query.map((r) => r.id)).toEqual(["resource1", "resource3"]);
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
  expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
    operator: "hasIntersection",
    operands: [
      { name: "request.resource.attr.tags" },
      { value: ["public", "draft"] },
    ],
  });

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {},
    relationMapper: {
      "request.resource.attr.tags": {
        relation: "tags",
        field: "name",
        type: "many",
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

  const query = await prisma.resource.findMany({
    where: { ...result.filters },
    include: { tags: true },
  });

  // Should return resources that have either "public" or "draft" tags
  expect(query.map((r) => r.id)).toEqual(["resource1", "resource3"]);
});

test("conditional - relation-gt-number", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "relation-gt-number",
  });

  expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
  const conditions = (queryPlan as PlanResourcesConditionalResponse).condition;
  expect(conditions).toEqual({
    operator: "gt",
    operands: [{ name: "request.resource.attr.nested.aNumber" }, { value: 1 }],
  });

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {},
    relationMapper: {
      "request.resource.attr.nested.aNumber": {
        relation: "nested",
        field: "aNumber",
        type: "one",
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

test("conditional - relation multiple all", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "relation-multiple-all",
  });

  expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
  expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
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
  });

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {},
    relationMapper: {
      "request.resource.attr.nested.aNumber": {
        relation: "nested",
        field: "aNumber",
        type: "one",
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
  expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
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
  });

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {},
    relationMapper: {
      "request.resource.attr.createdBy": {
        relation: "createdBy",
        field: "id",
        type: "one",
      },
      "request.resource.attr.ownedBy": {
        relation: "ownedBy",
        field: "id",
        type: "many",
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
  expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
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
  });

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {},
    relationMapper: {
      "request.resource.attr.createdBy": {
        relation: "createdBy",
        field: "id",
        type: "one",
      },
      "request.resource.attr.tags": {
        relation: "tags",
        field: "name",
        type: "many",
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

test("throws error for invalid query plan", () => {
  const invalidQueryPlan = {
    kind: "INVALID_KIND" as PlanKind,
  };

  expect(() =>
    queryPlanToPrisma({
      queryPlan: invalidQueryPlan as unknown as PlanResourcesResponse,
      fieldNameMapper: {},
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
      fieldNameMapper: {},
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
      fieldNameMapper: {},
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
      fieldNameMapper: {},
    })
  ).toThrow("No operand with 'name' found");
});

test("function mapper for field names", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "equal",
  });

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: (key: string) => key.replace("request.resource.attr.", ""),
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
    fieldNameMapper: {},
    relationMapper: (key: string) => ({
      relation: "createdBy",
      field: "id",
      type: "one",
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

test("conditional - contains", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "contains",
  });

  expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
  expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
    operands: [
      {
        name: "request.resource.attr.aString",
      },
      {
        value: "str",
      },
    ],
    operator: "contains",
  });

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.aString": "aString",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: { aString: { contains: "str" } },
  });

  const query = await prisma.resource.findMany({
    where: { ...result.filters },
  });
  expect(query.map((r) => r.id)).toEqual(
    fixtureResources.filter((a) => a.aString.includes("str")).map((r) => r.id)
  );
});

test("conditional - startsWith", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "starts-with",
  });

  expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
  expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
    operands: [
      {
        name: "request.resource.attr.aString",
      },
      {
        value: "str",
      },
    ],
    operator: "startsWith",
  });

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.aString": "aString",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: { aString: { startsWith: "str" } },
  });

  const query = await prisma.resource.findMany({
    where: { ...result.filters },
  });
  expect(query.map((r) => r.id)).toEqual(
    fixtureResources.filter((a) => a.aString.startsWith("str")).map((r) => r.id)
  );
});

test("conditional - isSet", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "is-set",
  });

  expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
  expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
    operands: [
      {
        name: "request.resource.attr.aOptionalString",
      },
      {
        value: null,
      },
    ],
    operator: "ne",
  });

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.aOptionalString": "aOptionalString",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: { aOptionalString: { not: null } },
  });

  const query = await prisma.resource.findMany({
    where: { ...result.filters },
  });
  expect(query.map((r) => r.id)).toEqual(
    fixtureResources.filter((a) => a.aOptionalString).map((r) => r.id)
  );
});
