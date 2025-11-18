import { beforeAll, test, expect, afterAll, describe } from "@jest/globals";
import { queryPlanToMongoose, PlanKind, Mapper } from ".";
import {
  PlanExpression,
  PlanExpressionValue,
  PlanExpressionVariable,
  PlanResourcesConditionalResponse,
  PlanResourcesResponse,
} from "@cerbos/core";
import { GRPC as Cerbos } from "@cerbos/grpc";
import mongoose, { Schema, model } from "mongoose";

const cerbos = new Cerbos("127.0.0.1:3593", { tls: false });

beforeAll(async () => {
  await mongoose.connect("mongodb://127.0.0.1:27017/test");
  await Resource.deleteMany({});
  for (const resource of fixtureResources) {
    await Resource.create(resource);
  }
});

afterAll(async () => {
  await mongoose.disconnect();
});

interface IResource {
  key: string;
  id: string;
  aBool: Boolean;
  aNumber: Number;
  aString: String;
  aOptionalString?: string;
  nested: {
    id: string;
    aBool: Boolean;
    aNumber: Number;
    aString: String;
  };
  tags: {
    id: string;
    name: string;
  }[];
  createdBy: {
    id: string;
    aBool: Boolean;
    aNumber: Number;
    aString: String;
  };
  ownedBy: {
    id: string;
    aBool: Boolean;
    aNumber: Number;
    aString: String;
  }[];
}

const resourceSchema = new Schema<IResource>({
  key: String,
  aBool: { type: Boolean },
  aNumber: { type: Number, required: true },
  aString: String,
  aOptionalString: { type: String, required: false },
  nested: {
    id: String,
    aBool: { type: Boolean },
    aNumber: { type: Number, required: true },
    aString: String,
  },
  tags: [
    {
      id: String,
      name: String,
    },
  ],
  createdBy: {
    id: String,
    aBool: { type: Boolean },
    aNumber: { type: Number, required: true },
    aString: String,
  },
  ownedBy: [
    {
      id: String,
      aBool: { type: Boolean },
      aNumber: { type: Number, required: true },
      aString: String,
    },
  ],
});

const Resource = model<IResource>("Resource", resourceSchema);

const fixtureResources: IResource[] = [
  {
    key: "a",
    id: "resource1",
    aBool: true,
    aNumber: 1,
    aString: "string",
    aOptionalString: "string",
    nested: {
      id: "nested1",
      aBool: true,
      aNumber: 1,
      aString: "string",
    },
    tags: [
      {
        id: "tag1",
        name: "public",
      },
    ],
    createdBy: {
      id: "user1",
      aBool: true,
      aNumber: 1,
      aString: "string",
    },
    ownedBy: [
      {
        id: "user1",
        aBool: true,
        aNumber: 1,
        aString: "string",
      },
    ],
  },
  {
    key: "b",
    id: "resource2",
    aBool: false,
    aNumber: 2,
    aString: "string2",
    nested: {
      id: "nested2",
      aBool: true,
      aNumber: 1,
      aString: "string",
    },
    tags: [
      {
        id: "tag2",
        name: "private",
      },
    ],
    createdBy: {
      id: "user2",
      aBool: true,
      aNumber: 1,
      aString: "string",
    },
    ownedBy: [
      {
        id: "user2",
        aBool: true,
        aNumber: 1,
        aString: "string",
      },
    ],
  },
  {
    key: "c",
    id: "resource3",
    aBool: false,
    aNumber: 3,
    aString: "string3",
    nested: {
      id: "nested3",
      aBool: true,
      aNumber: 1,
      aString: "string",
    },
    tags: [
      {
        id: "tag1",
        name: "public",
      },
      {
        id: "tag3",
        name: "draft",
      },
    ],
    createdBy: {
      id: "user2",
      aBool: true,
      aNumber: 1,
      aString: "string",
    },
    ownedBy: [
      {
        id: "user1",
        aBool: true,
        aNumber: 1,
        aString: "string",
      },
      {
        id: "user2",
        aBool: true,
        aNumber: 1,
        aString: "string",
      },
    ],
  },
];

const allowedTagNames = new Set<string>(["public", "draft"]);
const allowedStringValues = new Set<string>(["string", "anotherString"]);

const defaultMapper: Mapper = {
  "request.resource.attr.aBool": { field: "aBool" },
  "request.resource.attr.aNumber": { field: "aNumber" },
  "request.resource.attr.aString": { field: "aString" },
  "request.resource.attr.aOptionalString": { field: "aOptionalString" },
  "request.resource.attr.nested.aBool": { field: "nested.aBool" },
  "request.resource.attr.nested.aNumber": { field: "nested.aNumber" },
  "request.resource.attr.nested.aString": { field: "nested.aString" },
};

describe("Adapter Unit Behavior", () => {
  test("maps single-object relations without elemMatch", async () => {
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: new PlanExpression("eq", [
        new PlanExpressionVariable("request.resource.attr.createdBy.id"),
        new PlanExpressionValue("user1"),
      ]),
    } as PlanResourcesResponse;

    const mapper: Mapper = {
      "request.resource.attr.createdBy": {
        relation: {
          name: "createdBy",
          type: "one",
          field: "id",
        },
      },
    };

    const result = queryPlanToMongoose({
      queryPlan,
      mapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        "createdBy.id": { $eq: "user1" },
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((resource) => resource.createdBy.id === "user1")
        .map((resource) => resource.key)
    );
  });

  test("handles hasIntersection map projection", async () => {
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: new PlanExpression("hasIntersection", [
        new PlanExpression("map", [
          new PlanExpressionVariable("request.resource.attr.tags"),
          new PlanExpression("lambda", [
            new PlanExpressionVariable("tag.name"),
            new PlanExpressionVariable("tag"),
          ]),
        ]),
        new PlanExpressionValue(["public", "draft"]),
      ]),
    } as PlanResourcesResponse;

    const mapper: Mapper = {
      "request.resource.attr.tags": {
        relation: {
          name: "tags",
          type: "many",
        },
      },
    };

    const result = queryPlanToMongoose({
      queryPlan,
      mapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        tags: {
          $elemMatch: {
            name: { $in: ["public", "draft"] },
          },
        },
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((resource) =>
          resource.tags.some((tag) => allowedTagNames.has(tag.name))
        )
        .map((resource) => resource.key)
    );
  });
});

describe("Core Functionality", () => {
  test("always allowed", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "always-allow",
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: defaultMapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.ALWAYS_ALLOWED,
    });

    const query = await Resource.find({});
    expect(query.length).toEqual(fixtureResources.length);
  });

  test("always denied", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "always-deny",
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: defaultMapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.ALWAYS_DENIED,
    });
  });
});

describe("Field Operations", () => {
  describe("Basic Field Tests", () => {
    test("conditional - eq", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "equal",
      });

      const result = queryPlanToMongoose({
        queryPlan,
        mapper: defaultMapper,
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          aBool: {
            $eq: true,
          },
        },
      });

      const query = await Resource.find(result.filters || {});
      expect(query.map((r) => r.key)).toEqual(
        fixtureResources.filter((a) => a.aBool).map((r) => r.key)
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
        throw new Error("Expected two operands in the conditional query plan");
      }

      const invertedQueryPlan: PlanResourcesConditionalResponse = {
        ...typeQp,
        condition: {
          ...condition,
          operands: [secondOperand, firstOperand],
        },
      };

      const result = queryPlanToMongoose({
        queryPlan: invertedQueryPlan,
        mapper: defaultMapper,
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          aBool: {
            $eq: true,
          },
        },
      });

      const query = await Resource.find(result.filters || {});
      expect(query.map((r) => r.key)).toEqual(
        fixtureResources.filter((a) => a.aBool).map((r) => r.key)
      );
    });

    test("conditional - ne", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "ne",
      });

      const result = queryPlanToMongoose({
        queryPlan,
        mapper: defaultMapper,
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { aString: { $ne: "string" } },
      });
      const query = await Resource.find(result.filters || {});
      expect(query.map((r) => r.key)).toEqual(
        fixtureResources.filter((a) => a.aString != "string").map((r) => r.key)
      );
    });

    test("conditional - explicit-deny", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "explicit-deny",
      });

      const result = queryPlanToMongoose({
        queryPlan,
        mapper: defaultMapper,
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { $nor: [{ aBool: { $eq: true } }] },
      });
      const query = await Resource.find(result.filters || {});
      expect(query.map((r) => r.key)).toEqual(
        fixtureResources.filter((a) => !a.aBool).map((r) => r.key)
      );
    });
  });

  describe("Comparison Tests", () => {
    test("conditional - gt", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "gt",
      });

      const result = queryPlanToMongoose({
        queryPlan,
        mapper: defaultMapper,
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          aNumber: { $gt: 1 },
        },
      });

      const query = await Resource.find(result.filters || {});
      expect(query.map((r) => r.key)).toEqual(
        fixtureResources
          .filter((r) => {
            return (r.aNumber as number) > 1;
          })
          .map((r) => r.key)
      );
    });

    test("conditional - lt", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "lt",
      });

      const result = queryPlanToMongoose({
        queryPlan,
        mapper: defaultMapper,
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          aNumber: { $lt: 2 },
        },
      });

      const query = await Resource.find(result.filters || {});
      expect(query.map((r) => r.key)).toEqual(
        fixtureResources
          .filter((r) => {
            return (r.aNumber as number) < 2;
          })
          .map((r) => r.key)
      );
    });

    test("conditional - gte", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "gte",
      });

      const result = queryPlanToMongoose({
        queryPlan,
        mapper: defaultMapper,
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          aNumber: { $gte: 1 },
        },
      });

      const query = await Resource.find(result.filters || {});
      expect(query.map((r) => r.key)).toEqual(
        fixtureResources
          .filter((r) => {
            return (r.aNumber as number) >= 1;
          })
          .map((r) => r.key)
      );
    });

    test("conditional - lte", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "lte",
      });

      const result = queryPlanToMongoose({
        queryPlan,
        mapper: defaultMapper,
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          aNumber: { $lte: 2 },
        },
      });

      const query = await Resource.find(result.filters || {});
      expect(query.map((r) => r.key)).toEqual(
        fixtureResources
          .filter((r) => {
            return (r.aNumber as number) <= 2;
          })
          .map((r) => r.key)
      );
    });
  });

  describe("String Operations", () => {
    test("conditional - contains", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "contains",
      });

      const result = queryPlanToMongoose({
        queryPlan,
        mapper: defaultMapper,
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          aString: { $regex: "str" },
        },
      });

      const query = await Resource.find(result.filters || {});
      expect(query.map((r) => r.key)).toEqual(
        fixtureResources
          .filter((r) => r.aString.includes("str"))
          .map((r) => r.key)
      );
    });

    test("conditional - startsWith", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "starts-with",
      });

      const result = queryPlanToMongoose({
        queryPlan,
        mapper: defaultMapper,
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          aString: { $regex: "^str" },
        },
      });

      const query = await Resource.find(result.filters || {});
      expect(query.map((r) => r.key)).toEqual(
        fixtureResources
          .filter((r) => r.aString.startsWith("str"))
          .map((r) => r.key)
      );
    });

    test("conditional - endsWith", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "ends-with",
      });

      const result = queryPlanToMongoose({
        queryPlan,
        mapper: defaultMapper,
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          aString: { $regex: "ing$" },
        },
      });

      const query = await Resource.find(result.filters || {});
      expect(query.map((r) => r.key)).toEqual(
        fixtureResources
          .filter((r) => r.aString.endsWith("ing"))
          .map((r) => r.key)
      );
    });
  });
});

describe("Collection Operations", () => {
  test("conditional - in", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "in",
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: defaultMapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        aString: { $in: ["string", "anotherString"] },
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => {
          return allowedStringValues.has(r.aString as string);
        })
        .map((r) => r.key)
    );
  });

  test("conditional - exists", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "exists",
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: {
        ...defaultMapper,
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
          $elemMatch: {
            name: { $eq: "public" },
          },
        },
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => r.tags.some((t) => t.name === "public"))
        .map((r) => r.key)
    );
  });

  test("conditional - exists_one", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "exists-one",
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: {
        ...defaultMapper,
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
          $elemMatch: {
            name: { $eq: "public" },
          },
        },
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => r.tags.some((t) => t.name === "public"))
        .map((r) => r.key)
    );
  });

  test("conditional - all", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "all",
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: {
        ...defaultMapper,
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
          $not: {
            $elemMatch: {
              $nor: [{ name: { $eq: "public" } }],
            },
          },
        },
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => r.tags.every((t) => t.name === "public"))
        .map((r) => r.key)
    );
  });

  test("conditional - hasIntersection", async () => {
    const queryPlan = await cerbos.planResources({
      principal: {
        id: "user1",
        roles: ["USER"],
        attr: { tags: ["public", "draft"] },
      },
      resource: { kind: "resource" },
      action: "has-intersection",
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: {
        ...defaultMapper,
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
          $elemMatch: {
            name: {
              $in: ["public", "draft"],
            },
          },
        },
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => r.tags.some((t) => allowedTagNames.has(t.name)))
        .map((r) => r.key)
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

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: defaultMapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        $and: [{ aBool: { $eq: true } }, { aString: { $ne: "string" } }],
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => r.aBool && r.aString !== "string")
        .map((r) => r.key)
    );
  });

  test("conditional - or", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "or",
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: defaultMapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        $or: [
          {
            aBool: {
              $eq: true,
            },
          },
          {
            aString: { $ne: "string" },
          },
        ],
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => {
          return r.aBool || r.aString != "string";
        })
        .map((r) => r.key)
    );
  });
});

describe("Relations", () => {
  describe("Nested Relations", () => {
    test("conditional - eq nested", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "equal-nested",
      });

      const result = queryPlanToMongoose({
        queryPlan,
        mapper: defaultMapper,
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          "nested.aBool": {
            $eq: true,
          },
        },
      });
      const query = await Resource.find(result.filters || {});
      expect(query.map((r) => r.key)).toEqual(
        fixtureResources.filter((a) => a.nested.aBool).map((r) => r.key)
      );
    });
  });
});

describe("Mapper Functions", () => {
  test("function mapper for field names", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "equal",
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: (key: string) => ({
        field: key.replace("request.resource.attr.", ""),
      }),
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { aBool: { $eq: true } },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((resource) => resource.aBool === true)
        .map((resource) => resource.key)
    );
  });

  test("function mapper for relations", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "relation-is",
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: (_key: string) => ({
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
        "createdBy.id": { $eq: "user1" },
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((resource) => resource.createdBy.id === "user1")
        .map((resource) => resource.key)
    );
  });
});

describe("Error Handling", () => {
  test("throws error for invalid query plan", () => {
    const invalidQueryPlan = {
      kind: "INVALID_KIND" as PlanKind,
    };

    expect(() =>
      queryPlanToMongoose({
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
      queryPlanToMongoose({
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
      queryPlanToMongoose({
        queryPlan: invalidQueryPlan as unknown as PlanResourcesResponse,
        mapper: {},
      })
    ).toThrow("Unsupported operator: unsupported");
  });
});
