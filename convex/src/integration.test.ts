import { beforeAll, afterAll, test, expect, describe } from "@jest/globals";
import { ConvexHttpClient } from "convex/browser";
import { api } from "../convex/_generated/api.js";
import { queryPlanToConvex, PlanKind, Mapper } from ".";
import { GRPC as Cerbos } from "@cerbos/grpc";

const CONVEX_URL = process.env["CONVEX_URL"] || "http://127.0.0.1:3210";
const convex = new ConvexHttpClient(CONVEX_URL);
const cerbos = new Cerbos("127.0.0.1:3593", { tls: false });

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

beforeAll(async () => {
  await convex.mutation(api.resources.deleteAll, {});
  for (const resource of fixtureResources) {
    await convex.mutation(api.resources.insert, resource);
  }
});

afterAll(async () => {
  await convex.mutation(api.resources.deleteAll, {});
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

describe("Integration: Convex + Cerbos", () => {
  test("eq filter against real Convex DB", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "equal",
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });
    expect(result.kind).toBe(PlanKind.CONDITIONAL);

    const docs = await convex.query(api.resources.filteredQuery, {
      filterType: "eq",
      filterField: "aBool",
      filterValue: true,
    });

    expect(docs.map((d: Resource) => d.key).sort()).toEqual(
      fixtureResources.filter((r) => r.aBool).map((r) => r.key).sort(),
    );
  });

  test("ne filter against real Convex DB", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "ne",
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });
    expect(result.kind).toBe(PlanKind.CONDITIONAL);

    const docs = await convex.query(api.resources.filteredQuery, {
      filterType: "neq",
      filterField: "aString",
      filterValue: "string",
    });

    expect(docs.map((d: Resource) => d.key).sort()).toEqual(
      fixtureResources.filter((r) => r.aString !== "string").map((r) => r.key).sort(),
    );
  });

  test("gt filter against real Convex DB", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "gt",
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });
    expect(result.kind).toBe(PlanKind.CONDITIONAL);

    const docs = await convex.query(api.resources.filteredQuery, {
      filterType: "gt",
      filterField: "aNumber",
      filterValue: 1,
    });

    expect(docs.map((d: Resource) => d.key).sort()).toEqual(
      fixtureResources.filter((r) => r.aNumber > 1).map((r) => r.key).sort(),
    );
  });

  test("in filter (composed as OR of eq) against real Convex DB", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "in",
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });
    expect(result.kind).toBe(PlanKind.CONDITIONAL);

    const docs = await convex.query(api.resources.filteredQuery, {
      filterType: "in",
      filterField: "aString",
      filterValues: ["string", "anotherString"],
    });

    const allowed = new Set(["string", "anotherString"]);
    expect(docs.map((d: Resource) => d.key).sort()).toEqual(
      fixtureResources.filter((r) => allowed.has(r.aString)).map((r) => r.key).sort(),
    );
  });

  test("and filter against real Convex DB", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "and",
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });
    expect(result.kind).toBe(PlanKind.CONDITIONAL);

    const docs = await convex.query(api.resources.filteredQuery, {
      filterType: "and",
      filterField: "aBool",
      filterValue: true,
      filterField2: "aString",
      filterValue2: "string",
    });

    expect(docs.map((d: Resource) => d.key).sort()).toEqual(
      fixtureResources
        .filter((r) => r.aBool && r.aString !== "string")
        .map((r) => r.key)
        .sort(),
    );
  });

  test("or filter against real Convex DB", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "or",
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });
    expect(result.kind).toBe(PlanKind.CONDITIONAL);

    const docs = await convex.query(api.resources.filteredQuery, {
      filterType: "or",
      filterField: "aBool",
      filterValue: true,
      filterField2: "aString",
      filterValue2: "string",
    });

    expect(docs.map((d: Resource) => d.key).sort()).toEqual(
      fixtureResources
        .filter((r) => r.aBool || r.aString !== "string")
        .map((r) => r.key)
        .sort(),
    );
  });

  test("not filter against real Convex DB", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "explicit-deny",
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });
    expect(result.kind).toBe(PlanKind.CONDITIONAL);

    const docs = await convex.query(api.resources.filteredQuery, {
      filterType: "not",
      filterField: "aBool",
      filterValue: true,
    });

    expect(docs.map((d: Resource) => d.key).sort()).toEqual(
      fixtureResources.filter((r) => !r.aBool).map((r) => r.key).sort(),
    );
  });

  test("isSet filter against real Convex DB", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "is-set",
    });

    const result = queryPlanToConvex({ queryPlan, mapper: defaultMapper });
    expect(result.kind).toBe(PlanKind.CONDITIONAL);

    const docs = await convex.query(api.resources.filteredQuery, {
      filterType: "isSet",
      filterField: "aOptionalString",
    });

    expect(docs.map((d: Resource) => d.key).sort()).toEqual(
      fixtureResources
        .filter((r) => r.aOptionalString !== undefined)
        .map((r) => r.key)
        .sort(),
    );
  });
});
