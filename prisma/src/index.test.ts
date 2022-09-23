import queryPlanToPrisma from ".";
import { PlanKind, PlanResourcesResponse } from "@cerbos/core";
import { PrismaClient } from '@prisma/client'
import { GRPC as Cerbos } from "@cerbos/grpc";

const prisma = new PrismaClient()
const cerbos = new Cerbos("127.0.0.1:3593", { tls: false })

test("always allowed", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "sally", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "always-allow"
  })

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {},
  });

  expect(result).toStrictEqual({});

  const query = await prisma.resource.findMany({ where: { ...result } })
  expect(query.length).toEqual(0)

});

test("always denied", async () => {

  const queryPlan = await cerbos.planResources({
    principal: { id: "sally", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "always-deny"
  })

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {},
  });

  expect(result).toStrictEqual({ "1": { "equals": 0 } });

  const query = await prisma.resource.findMany({ where: { ...result } })
  expect(query.length).toEqual(0)

});


test("conditional - eq", () => {
  const queryPlan: PlanResourcesResponse = {
    requestId: "",
    metadata: undefined,
    kind: PlanKind.CONDITIONAL,
    condition: {
      operator: "eq",
      operands: [{ name: "request.resource.attr.ownerId" }, { value: "sally" }],
    },
  };

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.ownerId": "ownerId",
    },
  });

  expect(result).toStrictEqual({ ownerId: { equals: "sally" } });
});

test("conditional - eq - inverted order", () => {
  const queryPlan: PlanResourcesResponse = {
    requestId: "",
    metadata: undefined,
    kind: PlanKind.CONDITIONAL,
    condition: {
      operator: "eq",
      operands: [
        { value: "sally" },
        { name: "request.resource.attr.ownerId" }
      ],
    },
  };

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.ownerId": "ownerId",
    },
  });

  expect(result).toStrictEqual({ ownerId: { equals: "sally" } });
});


test("conditional - and", () => {
  const queryPlan: PlanResourcesResponse = {
    requestId: "",
    metadata: undefined,
    kind: PlanKind.CONDITIONAL,
    condition: {
      "operator": "and",
      "operands": [
        {
          "operator": "eq",
          "operands": [
            {
              "name": "request.resource.attr.department"
            },
            {
              "value": "marketing"
            }
          ]

        },
        {
          "operator": "ne",
          "operands": [
            {
              "name": "request.resource.attr.team"
            },
            {
              "value": "design"
            }
          ]
        }

      ]
    },
  };

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.department": "department",
      "request.resource.attr.team": "team",
    },
  });

  expect(result).toStrictEqual({
    AND: [
      {
        department: { equals: "marketing" }
      },
      {
        team: { not: "design" }
      }
    ]
  });
});


test("conditional - or", () => {
  const queryPlan: PlanResourcesResponse = {
    requestId: "",
    metadata: undefined,
    kind: PlanKind.CONDITIONAL,
    condition: {
      "operator": "or",
      "operands": [
        {
          "operator": "eq",
          "operands": [
            {
              "name": "request.resource.attr.department"
            },
            {
              "value": "marketing"
            }
          ]

        },
        {
          "operator": "ne",
          "operands": [
            {
              "name": "request.resource.attr.team"
            },
            {
              "value": "design"
            }
          ]
        }

      ]
    },
  };

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.department": "department",
      "request.resource.attr.team": "team",
    },
  });

  expect(result).toStrictEqual({
    OR: [
      {
        department: { equals: "marketing" }
      },
      {
        team: { not: "design" }
      }
    ]
  });
});

test("conditional - in", () => {
  const queryPlan: PlanResourcesResponse = {
    requestId: "",
    metadata: undefined,
    kind: PlanKind.CONDITIONAL,
    condition: {

      "operator": "in",
      "operands": [
        {
          "name": "request.resource.attr.department"
        },
        {
          "value": ["marketing", "design"]
        }
      ]
    },
  };

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.department": "department",
    },
  });

  expect(result).toStrictEqual({
    department: { in: ["marketing", "design"] }
  });
});


test("conditional - gt", () => {
  const queryPlan: PlanResourcesResponse = {
    requestId: "",
    metadata: undefined,
    kind: PlanKind.CONDITIONAL,
    condition: {

      "operator": "gt",
      "operands": [
        {
          "name": "request.resource.attr.amount"
        },
        {
          "value": 1000
        }
      ]
    },
  };

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.amount": "amount",
    },
  });

  expect(result).toStrictEqual({
    amount: { gt: 1000 }
  });
});

test("conditional - lt", () => {
  const queryPlan: PlanResourcesResponse = {
    requestId: "",
    metadata: undefined,
    kind: PlanKind.CONDITIONAL,
    condition: {

      "operator": "lt",
      "operands": [
        {
          "name": "request.resource.attr.amount"
        },
        {
          "value": 1000
        }
      ]
    },
  };

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.amount": "amount",
    },
  });

  expect(result).toStrictEqual({
    amount: { lt: 1000 }
  });
});


test("conditional - gte", () => {
  const queryPlan: PlanResourcesResponse = {
    requestId: "",
    metadata: undefined,
    kind: PlanKind.CONDITIONAL,
    condition: {

      "operator": "gte",
      "operands": [
        {
          "name": "request.resource.attr.amount"
        },
        {
          "value": 1000
        }
      ]
    },
  };

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.amount": "amount",
    },
  });

  expect(result).toStrictEqual({
    amount: { gte: 1000 }
  });
});

test("conditional - lte", () => {
  const queryPlan: PlanResourcesResponse = {
    requestId: "",
    metadata: undefined,
    kind: PlanKind.CONDITIONAL,
    condition: {

      "operator": "lte",
      "operands": [
        {
          "name": "request.resource.attr.amount"
        },
        {
          "value": 1000
        }
      ]
    },
  };

  const result = queryPlanToPrisma({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.amount": "amount",
    },
  });

  expect(result).toStrictEqual({
    amount: { lte: 1000 }
  });
});



