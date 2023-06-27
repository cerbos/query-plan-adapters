import { queryPlanToMongoose, PlanKind } from ".";
import { PlanExpression, PlanResourcesConditionalResponse } from "@cerbos/core";
import { GRPC as Cerbos } from "@cerbos/grpc";
import mongoose, { Schema, model } from "mongoose";

const cerbos = new Cerbos("127.0.0.1:3593", { tls: false });

interface IUser {
  aBool: Boolean;
  aNumber: Number;
  aString: String;
}

interface IResource {
  key: string;
  aBool: Boolean;
  aNumber: Number;
  aString: String;
}

const userSchema = new Schema<IUser>({
  aBool: { type: Boolean },
  aNumber: { type: Number, required: true },
  aString: String,
});

const resourceSchema = new Schema<IResource>({
  key: String,
  aBool: { type: Boolean },
  aNumber: { type: Number, required: true },
  aString: String,
});

const User = model<IUser>("User", userSchema);
const Resource = model<IResource>("Resource", resourceSchema);

const fixtureUsers: IUser[] = [
  {
    aBool: true,
    aNumber: 1,
    aString: "string",
  },
  {
    aBool: true,
    aNumber: 2,
    aString: "string",
  },
];

const fixtureResources: IResource[] = [
  {
    key: "a",
    aBool: true,
    aNumber: 1,
    aString: "string",
  },
  {
    key: "b",
    aBool: false,
    aNumber: 2,
    aString: "string2",
  },
  {
    key: "c",
    aBool: false,
    aNumber: 3,
    aString: "string3",
  },
];

beforeAll(async () => {
  await mongoose.connect("mongodb://127.0.0.1:27017/test");
  mongoose.set("debug", true);
  console.log("Clearing data");
  await User.deleteMany({});
  await Resource.deleteMany({});
  console.log("Creating data");
  for (const user of fixtureUsers) {
    await User.create(user);
  }
  for (const resource of fixtureResources) {
    await Resource.create(resource);
  }
});

afterAll(async () => {
  await mongoose.disconnect();
});

test("always allowed", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "always-allow",
  });

  const result = queryPlanToMongoose({
    queryPlan,
    fieldNameMapper: {},
  });

  expect(result).toStrictEqual({
    kind: PlanKind.ALWAYS_ALLOWED,
    filters: {},
  });

  const query = await Resource.find({
    ...result.filters,
  });
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
    fieldNameMapper: {},
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
    fieldNameMapper: {
      "request.resource.attr.aBool": "aBool",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: { aBool: true },
  });
  const query = await Resource.find({
    ...result.filters,
  });
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
    fieldNameMapper: {
      "request.resource.attr.aBool": "aBool",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: { aBool: true },
  });

  const query = await Resource.find({
    ...result.filters,
  });
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
    fieldNameMapper: {
      "request.resource.attr.aString": "aString",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: { aString: { $ne: "string" } },
  });
  const query = await Resource.find({
    ...result.filters,
  });
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
    fieldNameMapper: {
      "request.resource.attr.aBool": "aBool",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: { $nor: [{ aBool: true }] },
  });
  const query = await Resource.find({
    ...result.filters,
  });
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
    fieldNameMapper: {
      "request.resource.attr.aBool": "aBool",
      "request.resource.attr.aString": "aString",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: {
      $and: [
        {
          aBool: true,
        },
        {
          aString: { $ne: "string" },
        },
      ],
    },
  });

  const query = await Resource.find({
    ...result.filters,
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

  const result = queryPlanToMongoose({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.aBool": "aBool",
      "request.resource.attr.aString": "aString",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: {
      $or: [
        {
          aBool: true,
        },
        {
          aString: { $ne: "string" },
        },
      ],
    },
  });

  const query = await Resource.find({
    ...result.filters,
  });
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
    fieldNameMapper: {
      "request.resource.attr.aString": "aString",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: {
      aString: { $in: ["string", "anotherString"] },
    },
  });

  const query = await Resource.find({
    ...result.filters,
  });
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
    fieldNameMapper: {
      "request.resource.attr.aNumber": "aNumber",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: {
      aNumber: { $gt: 1 },
    },
  });

  const query = await Resource.find({
    ...result.filters,
  });
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
    fieldNameMapper: {
      "request.resource.attr.aNumber": "aNumber",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: {
      aNumber: { $lt: 2 },
    },
  });

  const query = await Resource.find({
    ...result.filters,
  });
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
    fieldNameMapper: {
      "request.resource.attr.aNumber": "aNumber",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: {
      aNumber: { $gte: 1 },
    },
  });

  const query = await Resource.find({
    ...result.filters,
  });
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
    fieldNameMapper: {
      "request.resource.attr.aNumber": "aNumber",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: {
      aNumber: { $lte: 2 },
    },
  });

  const query = await Resource.find({
    ...result.filters,
  });
  expect(query.map((r) => r.key)).toEqual(
    fixtureResources
      .filter((r) => {
        return (r.aNumber as number) <= 2;
      })
      .map((r) => r.key)
  );
});
