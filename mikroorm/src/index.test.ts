import { MikroORM, EntityManager } from "@mikro-orm/core";
import { SqliteDriver } from "@mikro-orm/sqlite";
import {
  beforeAll,
  beforeEach,
  afterEach,
  afterAll,
  describe,
  test,
  expect,
} from "@jest/globals";
import { GRPC as Cerbos } from "@cerbos/grpc";
import { PlanKind, PlanResourcesConditionalResponse } from "@cerbos/core";
import { queryPlanToMikroORM } from "./index";

// Entity definitions
import {
  Resource,
  User,
  NestedResource,
  NextLevelNestedResource,
  Tag,
  Label,
  SubCategory,
  Category,
} from "./entities";

let orm: MikroORM;
let em: EntityManager;
const cerbos = new Cerbos("127.0.0.1:3593", { tls: false });

// Test fixtures
const fixtureUsers = [
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

const fixtureNextLevelResources = [
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

const fixtureTags = [
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

const fixtureNestedResources = [
  {
    id: "nested1",
    aBool: true,
    aNumber: 1,
    aString: "string",
    nextlevel: "nextLevel1",
  },
  {
    id: "nested2",
    aBool: false,
    aNumber: 1,
    aString: "string",
    nextlevel: "nextLevel2",
  },
  {
    id: "nested3",
    aBool: true,
    aNumber: 1,
    aString: "string",
    nextlevel: "nextLevel3",
  },
];

const fixtureLabels = [
  { id: "label1", name: "important" },
  { id: "label2", name: "archived" },
  { id: "label3", name: "flagged" },
];

const fixtureSubCategories = [
  {
    id: "sub1",
    name: "finance",
    labels: ["label1", "label2"],
  },
  {
    id: "sub2",
    name: "tech",
    labels: ["label2", "label3"],
  },
];

const fixtureCategories = [
  {
    id: "cat1",
    name: "business",
    subCategories: ["sub1"],
  },
  {
    id: "cat2",
    name: "development",
    subCategories: ["sub2"],
  },
];

const fixtureResources = [
  {
    id: "resource1",
    aBool: true,
    aNumber: 1,
    aString: "string",
    aOptionalString: "optionalString",
    createdBy: "user1",
    ownedBy: ["user1"],
    nested: "nested1",
    tags: ["tag1"],
    categories: ["cat1"],
  },
  {
    id: "resource2",
    aBool: false,
    aNumber: 2,
    aString: "string2",
    createdBy: "user2",
    ownedBy: ["user2"],
    nested: "nested3",
    tags: ["tag2"],
    categories: ["cat2"],
  },
  {
    id: "resource3",
    aBool: false,
    aNumber: 3,
    aString: "string3",
    createdBy: "user1",
    ownedBy: ["user1", "user2"],
    nested: "nested3",
    tags: ["tag1", "tag3"],
    categories: ["cat1", "cat2"],
  },
];

beforeAll(async () => {
  orm = await MikroORM.init<SqliteDriver>({
    entities: [
      Resource,
      User,
      NestedResource,
      NextLevelNestedResource,
      Tag,
      Label,
      SubCategory,
      Category,
    ],
    dbName: ":memory:",
    driver: SqliteDriver,
  });

  await orm.getSchemaGenerator().createSchema();
  em = orm.em.fork();
});

beforeEach(async () => {
  // Clear database and load fixtures
  await em.execute("DELETE FROM resource");
  await em.execute("DELETE FROM user");
  await em.execute("DELETE FROM nested_resource");
  await em.execute("DELETE FROM next_level_nested_resource");
  await em.execute("DELETE FROM tag");
  await em.execute("DELETE FROM label");
  await em.execute("DELETE FROM sub_category");
  await em.execute("DELETE FROM category");

  // Create entities in correct order
  for (const user of fixtureUsers) {
    const userEntity = em.create(User, user);
    await em.persistAndFlush(userEntity);
  }

  for (const nextLevel of fixtureNextLevelResources) {
    const nextLevelEntity = em.create(NextLevelNestedResource, nextLevel);
    await em.persistAndFlush(nextLevelEntity);
  }

  for (const tag of fixtureTags) {
    const tagEntity = em.create(Tag, tag);
    await em.persistAndFlush(tagEntity);
  }

  for (const label of fixtureLabels) {
    const labelEntity = em.create(Label, label);
    await em.persistAndFlush(labelEntity);
  }

  for (const nested of fixtureNestedResources) {
    const nextLevel = await em.findOne(NextLevelNestedResource, {
      id: nested.nextlevel,
    });
    const nestedEntity = em.create(NestedResource, {
      ...nested,
      nextlevel: nextLevel,
    });
    await em.persistAndFlush(nestedEntity);
  }

  for (const subCategory of fixtureSubCategories) {
    const labels = await em.find(Label, { id: { $in: subCategory.labels } });
    const subCategoryEntity = em.create(SubCategory, {
      ...subCategory,
      labels,
    });
    await em.persistAndFlush(subCategoryEntity);
  }

  for (const category of fixtureCategories) {
    const subCategories = await em.find(SubCategory, {
      id: { $in: category.subCategories },
    });
    const categoryEntity = em.create(Category, { ...category, subCategories });
    await em.persistAndFlush(categoryEntity);
  }

  for (const resource of fixtureResources) {
    const createdBy = await em.findOne(User, { id: resource.createdBy });
    const ownedBy = await em.find(User, { id: { $in: resource.ownedBy } });
    const nested = await em.findOne(NestedResource, { id: resource.nested });
    const tags = await em.find(Tag, { id: { $in: resource.tags } });
    const categories = await em.find(Category, {
      id: { $in: resource.categories },
    });

    const resourceEntity = em.create(Resource, {
      ...resource,
      createdBy,
      ownedBy,
      nested,
      tags,
      categories,
    });
    await em.persistAndFlush(resourceEntity);
  }
});

afterEach(async () => {
  em.clear();
});

afterAll(async () => {
  await orm.close(true);
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

    const result = queryPlanToMikroORM({
      queryPlan,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.ALWAYS_ALLOWED,
    });

    const query = await em.find(Resource, {});
    expect(query.length).toEqual(fixtureResources.length);
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

    const result = queryPlanToMikroORM({
      queryPlan,
      mapper: {
        "request.resource.attr.aBool": { field: "aBool" },
      },
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { aBool: true },
    });

    if (result.kind !== PlanKind.CONDITIONAL) {
      throw new Error("Expected CONDITIONAL result");
    }

    const query = await em.find(Resource, result.filters);
    expect(query.map((r) => r.id)).toEqual(
      fixtureResources.filter((r) => r.aBool).map((r) => r.id)
    );
  });

  // Add more basic tests...
});

// Field Operations
describe("Field Operations", () => {
  describe("Basic Field Tests", () => {
    // ...existing eq test...

    test("conditional - ne", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "ne",
      });

      const result = queryPlanToMikroORM({
        queryPlan,
        mapper: {
          "request.resource.attr.aString": { field: "aString" },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { aString: { $ne: "string" } },
      });

      if (result.kind !== PlanKind.CONDITIONAL) {
        throw new Error("Expected CONDITIONAL result");
      }

      const query = await em.find(Resource, result.filters);
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources.filter((r) => r.aString !== "string").map((r) => r.id)
      );
    });

    test("conditional - gt", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "gt",
      });

      const result = queryPlanToMikroORM({
        queryPlan,
        mapper: {
          "request.resource.attr.aNumber": { field: "aNumber" },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { aNumber: { $gt: 1 } },
      });

      const query = await em.find(Resource, result.filters);
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources.filter((r) => r.aNumber > 1).map((r) => r.id)
      );
    });

    test("conditional - lt", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "lt",
      });

      const result = queryPlanToMikroORM({
        queryPlan,
        mapper: {
          "request.resource.attr.aNumber": { field: "aNumber" },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { aNumber: { $lt: 2 } },
      });

      const query = await em.find(Resource, result.filters);
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources.filter((r) => r.aNumber < 2).map((r) => r.id)
      );
    });
  });

  describe("Comparison Tests", () => {
    // ...existing gt, lt tests...

    test("conditional - gte", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "gte",
      });

      const result = queryPlanToMikroORM({
        queryPlan,
        mapper: {
          "request.resource.attr.aNumber": { field: "aNumber" },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { aNumber: { $gte: 1 } },
      });

      const query = await em.find(Resource, result.filters);
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources.filter((r) => r.aNumber >= 1).map((r) => r.id)
      );
    });

    test("conditional - lte", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "lte",
      });

      const result = queryPlanToMikroORM({
        queryPlan,
        mapper: {
          "request.resource.attr.aNumber": { field: "aNumber" },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { aNumber: { $lte: 2 } },
      });

      const query = await em.find(Resource, result.filters);
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources.filter((r) => r.aNumber <= 2).map((r) => r.id)
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

      const result = queryPlanToMikroORM({
        queryPlan,
        mapper: {
          "request.resource.attr.aString": { field: "aString" },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { aString: { $like: "%str%" } },
      });

      const query = await em.find(Resource, result.filters);
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources
          .filter((r) => r.aString.includes("str"))
          .map((r) => r.id)
      );
    });

    test("conditional - startsWith", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "starts-with",
      });

      const result = queryPlanToMikroORM({
        queryPlan,
        mapper: {
          "request.resource.attr.aString": { field: "aString" },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { aString: { $like: "str%" } },
      });

      const query = await em.find(Resource, result.filters);
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources
          .filter((r) => r.aString.startsWith("str"))
          .map((r) => r.id)
      );
    });

    test("conditional - endsWith", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "ends-with",
      });

      const result = queryPlanToMikroORM({
        queryPlan,
        mapper: {
          "request.resource.attr.aString": { field: "aString" },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { aString: { $like: "%ing" } },
      });

      const query = await em.find(Resource, result.filters);
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources
          .filter((r) => r.aString.endsWith("ing"))
          .map((r) => r.id)
      );
    });

    test("conditional - isSet", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "is-set",
      });

      const result = queryPlanToMikroORM({
        queryPlan,
        mapper: {
          "request.resource.attr.aOptionalString": { field: "aOptionalString" },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { aOptionalString: { $ne: null } },
      });

      const query = await em.find(Resource, result.filters);
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources.filter((r) => r.aOptionalString).map((r) => r.id)
      );
    });
  });
});

// Collection Operations
describe("Collection Operations", () => {
  test("conditional - in", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "in",
    });

    const result = queryPlanToMikroORM({
      queryPlan,
      mapper: {
        "request.resource.attr.aString": { field: "aString" },
      },
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { aString: { $in: ["string", "anotherString"] } },
    });

    const query = await em.find(Resource, result.filters);
    expect(query.map((r) => r.id)).toEqual(
      fixtureResources
        .filter((r) => ["string", "anotherString"].includes(r.aString))
        .map((r) => r.id)
    );
  });

  test("conditional - exists", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "exists",
    });

    const result = queryPlanToMikroORM({
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
          $exists: true,
          name: "public",
        },
      },
    });

    const query = await em.find(Resource, result.filters);
    expect(query.map((r) => r.id)).toEqual(
      fixtureResources.filter((r) => r.tags?.includes("tag1")).map((r) => r.id)
    );
  });

  test("conditional - exists_one", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "exists-one",
    });

    const result = queryPlanToMikroORM({
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
        $and: [
          { tags: { $exists: true, name: "public" } },
          {
            $expr: {
              $eq: [
                {
                  $size: {
                    $filter: { input: "$tags", cond: { name: "public" } },
                  },
                },
                1,
              ],
            },
          },
        ],
      },
    });

    const query = await em.find(Resource, result.filters);
    expect(query.map((r) => r.id)).toEqual(
      fixtureResources
        .filter((r) => r.tags?.filter((t) => t === "tag1").length === 1)
        .map((r) => r.id)
    );
  });

  test("conditional - all", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "all",
    });

    const result = queryPlanToMikroORM({
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
        tags: { $all: { name: "public" } },
      },
    });

    const query = await em.find(Resource, result.filters);
    expect(query.map((r) => r.id)).toEqual(
      fixtureResources
        .filter((r) =>
          r.tags?.every(
            (t) => fixtureTags.find((ft) => ft.id === t)?.name === "public"
          )
        )
        .map((r) => r.id)
    );
  });
});

// Relations
describe("Relations", () => {
  describe("One-to-One", () => {
    test("conditional - relation eq", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "relation-eq",
      });

      const result = queryPlanToMikroORM({
        queryPlan,
        mapper: {
          "request.resource.attr.nested": {
            relation: {
              name: "nested",
              type: "one",
              field: "id",
            },
          },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { "nested.id": "nested1" },
      });

      const query = await em.find(Resource, result.filters);
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources.filter((r) => r.nested === "nested1").map((r) => r.id)
      );
    });
  });

  describe("One-to-Many", () => {
    test("conditional - relation some", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "relation-some",
      });

      const result = queryPlanToMikroORM({
        queryPlan,
        mapper: {
          "request.resource.attr.tags": {
            relation: {
              name: "tags",
              type: "many",
              field: "id",
            },
          },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { "tags.id": { $in: ["tag1"] } },
      });

      const query = await em.find(Resource, result.filters);
      expect(query.map((r) => r.id)).toEqual(
        fixtureResources
          .filter((r) => r.tags?.includes("tag1"))
          .map((r) => r.id)
      );
    });
  });

  describe("Nested Relations", () => {
    test("conditional - deeply nested category label", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "deep-nested-category-label",
      });

      const result = queryPlanToMikroORM({
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
          "categories.subCategories.labels": {
            $exists: true,
            name: "important",
          },
        },
      });

      const query = await em.find(Resource, result.filters);
      expect(query.map((r) => r.id)).toEqual(["resource1"]);
    });
  });
});

// Complex Operations
describe("Complex Operations", () => {
  test("conditional - and", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "and",
    });

    const result = queryPlanToMikroORM({
      queryPlan,
      mapper: {
        "request.resource.attr.aBool": { field: "aBool" },
        "request.resource.attr.aNumber": { field: "aNumber" },
      },
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        $and: [{ aBool: true }, { aNumber: { $gt: 1 } }],
      },
    });

    const query = await em.find(Resource, result.filters);
    expect(query.map((r) => r.id)).toEqual(
      fixtureResources.filter((r) => r.aBool && r.aNumber > 1).map((r) => r.id)
    );
  });

  test("conditional - or", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "or",
    });

    const result = queryPlanToMikroORM({
      queryPlan,
      mapper: {
        "request.resource.attr.aBool": { field: "aBool" },
        "request.resource.attr.aNumber": { field: "aNumber" },
      },
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        $or: [{ aBool: true }, { aNumber: { $gt: 2 } }],
      },
    });

    const query = await em.find(Resource, result.filters);
    expect(query.map((r) => r.id)).toEqual(
      fixtureResources.filter((r) => r.aBool || r.aNumber > 2).map((r) => r.id)
    );
  });

  test("conditional - hasIntersection with map", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "has-intersection-map",
    });

    const result = queryPlanToMikroORM({
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
        "categories.subCategories.name": { $in: ["finance", "tech"] },
      },
    });

    const query = await em.find(Resource, result.filters);
    expect(query.map((r) => r.id)).toEqual([
      "resource1",
      "resource2",
      "resource3",
    ]);
  });
});

// Deep Nesting
describe("Deep Nesting", () => {
  test("conditional - deeply nested eq", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "deeply-nested-eq",
    });

    const result = queryPlanToMikroORM({
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
      filters: { "nested.nextlevel.aBool": true },
    });

    const query = await em.find(Resource, result.filters);
    expect(query.map((r) => r.id)).toEqual(
      fixtureResources
        .filter((r) => {
          const nested = fixtureNestedResources.find((n) => n.id === r.nested);
          const nextLevel = fixtureNextLevelResources.find(
            (nl) => nl.id === nested?.nextlevel
          );
          return nextLevel?.aBool === true;
        })
        .map((r) => r.id)
    );
  });
});

// Error Cases
describe("Error Cases", () => {
  test("throws error for invalid query plan", () => {
    const invalidQueryPlan = {
      kind: "INVALID_KIND" as PlanKind,
    };

    expect(() =>
      queryPlanToMikroORM({
        queryPlan: invalidQueryPlan as any,
      })
    ).toThrow("Invalid query plan.");
  });

  test("throws error for invalid expression structure", () => {
    const invalidQueryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {},
    };

    expect(() =>
      queryPlanToMikroORM({
        queryPlan: invalidQueryPlan as any,
      })
    ).toThrow("Invalid Cerbos expression structure");
  });
});

describe("Return Types", () => {
  test("returns correct type for ALWAYS_ALLOWED", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "always-allow",
    });

    const result = queryPlanToMikroORM({ queryPlan });
    expect(result.kind).toBe(PlanKind.ALWAYS_ALLOWED);

    if (result.kind === PlanKind.ALWAYS_ALLOWED) {
      expect(Object.keys(result)).toEqual(["kind"]);
    }
  });

  test("returns correct type for CONDITIONAL", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "equal",
    });

    const result = queryPlanToMikroORM({
      queryPlan,
      mapper: {
        "request.resource.attr.aBool": { field: "aBool" },
      },
    });

    expect(result.kind).toBe(PlanKind.CONDITIONAL);

    if (result.kind === PlanKind.CONDITIONAL) {
      expect(Object.keys(result).sort()).toEqual(["filters", "kind"].sort());
      expect(typeof result.filters).toBe("object");
    }
  });
});

// ... rest of the existing code ...
