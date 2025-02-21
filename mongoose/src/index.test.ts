import { beforeAll, test, expect, afterAll } from "@jest/globals";
import { queryPlanToMongoose, PlanKind, Mapper } from ".";
import { PlanExpression, PlanResourcesConditionalResponse } from "@cerbos/core";
import { GRPC as Cerbos } from "@cerbos/grpc";
import mongoose, { Schema, model } from "mongoose";

const cerbos = new Cerbos("127.0.0.1:3593", { tls: false });

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

const defaultMapper: Mapper = {
  "request.resource.attr.aBool": { field: "aBool" },
  "request.resource.attr.aNumber": { field: "aNumber" },
  "request.resource.attr.aString": { field: "aString" },
  "request.resource.attr.aOptionalString": { field: "aOptionalString" },
  "request.resource.attr.nested.aBool": { field: "nested.aBool" },
  "request.resource.attr.nested.aNumber": { field: "nested.aNumber" },
  "request.resource.attr.nested.aString": { field: "nested.aString" },
};

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
        return ["string", "anotherString"].includes(r.aString as string);
      })
      .map((r) => r.key)
  );
});

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
    fixtureResources.filter((r) => r.aString.includes("str")).map((r) => r.key)
  );
});

test("conditional - isSet", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "is-set",
  });

  const result = queryPlanToMongoose({
    queryPlan,
    mapper: defaultMapper,
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: {
      aOptionalString: { $ne: null },
    },
  });

  const query = await Resource.find(result.filters || {});
  expect(query.map((r) => r.key)).toEqual(
    fixtureResources.filter((r) => r.aOptionalString != null).map((r) => r.key)
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
      .filter((r) => r.tags.some((t) => ["public", "draft"].includes(t.name)))
      .map((r) => r.key)
  );
});
