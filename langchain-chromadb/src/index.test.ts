import { beforeAll, afterAll, test, expect, jest } from "@jest/globals";
import { GRPC as Cerbos } from "@cerbos/grpc";
import { ChromaClient, ChromaNotFoundError } from "chromadb";
import type { Collection, Metadata, Where } from "chromadb";
import { queryPlanToChromaDB, PlanKind } from ".";

const cerbos = new Cerbos("127.0.0.1:3593", { tls: false });
const chromaUrl = new URL(
  process.env["CHROMA_URL"] ?? "http://127.0.0.1:8000",
);
const chroma = new ChromaClient({
  host: chromaUrl.hostname,
  port: Number(chromaUrl.port) || 8000,
});

const collectionName = "adapter-tests";
const baseEmbedding = [0.1, 0.2, 0.3, 0.4];

jest.setTimeout(30_000);

interface ResourceMetadata {
  key: string;
  aBool: boolean;
  aNumber: number;
  aString: string;
  "nested.aBool": boolean;
  "nested.aNumber": number;
  "nested.aString": string;
}

const fixtureResources: ResourceMetadata[] = [
  {
    key: "a",
    aBool: true,
    aNumber: 1,
    aString: "string",
    "nested.aBool": true,
    "nested.aNumber": 1,
    "nested.aString": "string",
  },
  {
    key: "b",
    aBool: false,
    aNumber: 2,
    aString: "string2",
    "nested.aBool": true,
    "nested.aNumber": 1,
    "nested.aString": "string",
  },
  {
    key: "c",
    aBool: false,
    aNumber: 3,
    aString: "string3",
    "nested.aBool": true,
    "nested.aNumber": 1,
    "nested.aString": "string",
  },
];

const fixtureMetadatas = fixtureResources as unknown as Metadata[];

let collection: Collection;

beforeAll(async () => {
  await chroma.heartbeat();

  try {
    await chroma.deleteCollection({ name: collectionName });
  } catch (err: unknown) {
    if (!(err instanceof ChromaNotFoundError)) {
      throw err;
    }
  }

  collection = await chroma.createCollection({ name: collectionName });
  await collection.add({
    ids: fixtureResources.map((r) => r.key),
    embeddings: fixtureResources.map(() => baseEmbedding),
    metadatas: fixtureMetadatas,
    documents: fixtureResources.map((r) => r.aString),
  });
});

afterAll(async () => {
  await chroma.deleteCollection({ name: collectionName });
});

async function queryResourceIds(
  where?: Record<string, unknown>,
): Promise<string[]> {
  const results = await collection.query({
    queryEmbeddings: [baseEmbedding],
    where: where as Where | undefined,
    nResults: fixtureResources.length,
  });

  return (results.ids?.[0] ?? []) as string[];
}

test("always allowed", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "always-allow",
  });

  const result = queryPlanToChromaDB({
    queryPlan,
    fieldNameMapper: {},
  });

  expect(result).toStrictEqual({
    kind: PlanKind.ALWAYS_ALLOWED,
    filters: {},
  });

  const matches = await queryResourceIds();
  expect(matches.sort()).toEqual(fixtureResources.map((r) => r.key).sort());
});

test("always denied", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "always-deny",
  });

  const result = queryPlanToChromaDB({
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

  const result = queryPlanToChromaDB({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.aBool": "aBool",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: { aBool: { $eq: true } },
  });

  const matches = await queryResourceIds(result.filters);
  expect(matches).toEqual(
    fixtureResources.filter((r) => r.aBool).map((r) => r.key),
  );
});

test("conditional - ne", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "ne",
  });

  const result = queryPlanToChromaDB({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.aString": "aString",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: { aString: { $ne: "string" } },
  });

  const matches = await queryResourceIds(result.filters);
  expect(matches.sort()).toEqual(
    fixtureResources
      .filter((r) => r.aString !== "string")
      .map((r) => r.key)
      .sort(),
  );
});

test("conditional - and", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "and",
  });

  const result = queryPlanToChromaDB({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.aBool": "aBool",
      "request.resource.attr.aString": "aString",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: {
      $and: [{ aBool: { $eq: true } }, { aString: { $ne: "string" } }],
    },
  });

  const matches = await queryResourceIds(result.filters);
  expect(matches).toEqual([]);
});

test("conditional - or", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "or",
  });

  const result = queryPlanToChromaDB({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.aBool": "aBool",
      "request.resource.attr.aString": "aString",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: {
      $or: [{ aBool: { $eq: true } }, { aString: { $ne: "string" } }],
    },
  });

  const matches = await queryResourceIds(result.filters);
  expect(matches.sort()).toEqual(fixtureResources.map((r) => r.key).sort());
});

test("conditional - in", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "in",
  });

  const result = queryPlanToChromaDB({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.aString": "aString",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: { aString: { $in: ["string", "anotherString"] } },
  });

  const matches = await queryResourceIds(result.filters);
  expect(matches.sort()).toEqual(
    fixtureResources
      .filter((r) => ["string", "anotherString"].includes(r.aString))
      .map((r) => r.key)
      .sort(),
  );
});

test("conditional - gt", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "gt",
  });

  const result = queryPlanToChromaDB({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.aNumber": "aNumber",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: { aNumber: { $gt: 1 } },
  });

  const matches = await queryResourceIds(result.filters);
  expect(matches.sort()).toEqual(
    fixtureResources
      .filter((r) => r.aNumber > 1)
      .map((r) => r.key)
      .sort(),
  );
});

test("conditional - lt", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "lt",
  });

  const result = queryPlanToChromaDB({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.aNumber": "aNumber",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: { aNumber: { $lt: 2 } },
  });

  const matches = await queryResourceIds(result.filters);
  expect(matches).toEqual(
    fixtureResources.filter((r) => r.aNumber < 2).map((r) => r.key),
  );
});

test("conditional - gte", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "gte",
  });

  const result = queryPlanToChromaDB({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.aNumber": "aNumber",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: { aNumber: { $gte: 1 } },
  });

  const matches = await queryResourceIds(result.filters);
  expect(matches.sort()).toEqual(fixtureResources.map((r) => r.key).sort());
});

test("conditional - lte", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "lte",
  });

  const result = queryPlanToChromaDB({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.aNumber": "aNumber",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: { aNumber: { $lte: 2 } },
  });

  const matches = await queryResourceIds(result.filters);
  expect(matches.sort()).toEqual(
    fixtureResources
      .filter((r) => r.aNumber <= 2)
      .map((r) => r.key)
      .sort(),
  );
});

test("conditional - explicit-deny (not eq)", async () => {
  // #given - policy: DENY when aBool==true, ALLOW otherwise
  // Cerbos produces not(eq(aBool, true))
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "explicit-deny",
  });

  // #when
  const result = queryPlanToChromaDB({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.aBool": "aBool",
    },
  });

  // #then - not(eq) negates to $ne
  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: { aBool: { $ne: true } },
  });

  const matches = await queryResourceIds(result.filters);
  expect(matches.sort()).toEqual(
    fixtureResources
      .filter((r) => !r.aBool)
      .map((r) => r.key)
      .sort(),
  );
});

test("conditional - nand (not and)", async () => {
  // #given - policy: DENY when (aBool==true AND aString!="string"), ALLOW otherwise
  // Cerbos produces not(and(eq(aBool, true), ne(aString, "string")))
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "nand",
  });

  // #when
  const result = queryPlanToChromaDB({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.aBool": "aBool",
      "request.resource.attr.aString": "aString",
    },
  });

  // #then - De Morgan's: not(and(A, B)) → or(not(A), not(B))
  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: {
      $or: [{ aBool: { $ne: true } }, { aString: { $eq: "string" } }],
    },
  });

  const matches = await queryResourceIds(result.filters);
  expect(matches.sort()).toEqual(
    fixtureResources
      .filter((r) => !(r.aBool && r.aString !== "string"))
      .map((r) => r.key)
      .sort(),
  );
});

test("conditional - nor (not or)", async () => {
  // #given - policy: DENY when (aBool==true OR aString!="string"), ALLOW otherwise
  // Cerbos produces not(or(eq(aBool, true), ne(aString, "string")))
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "nor",
  });

  // #when
  const result = queryPlanToChromaDB({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.aBool": "aBool",
      "request.resource.attr.aString": "aString",
    },
  });

  // #then - De Morgan's: not(or(A, B)) → and(not(A), not(B))
  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: {
      $and: [{ aBool: { $ne: true } }, { aString: { $eq: "string" } }],
    },
  });

  const matches = await queryResourceIds(result.filters);
  expect(matches.sort()).toEqual(
    fixtureResources
      .filter((r) => !(r.aBool || r.aString !== "string"))
      .map((r) => r.key)
      .sort(),
  );
});

test("conditional - eq with dot-notation field", async () => {
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: "equal-nested",
  });

  const result = queryPlanToChromaDB({
    queryPlan,
    fieldNameMapper: {
      "request.resource.attr.nested.aBool": "nested.aBool",
    },
  });

  expect(result).toStrictEqual({
    kind: PlanKind.CONDITIONAL,
    filters: { "nested.aBool": { $eq: true } },
  });

  const matches = await queryResourceIds(result.filters);
  expect(matches).toEqual(
    fixtureResources
      .filter((r) => r["nested.aBool"])
      .map((r) => r.key),
  );
});
