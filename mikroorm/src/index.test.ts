import {
  MikroORM,
  EntityManager,
  Connection,
  IDatabaseDriver,
} from "@mikro-orm/core";
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
import { PlanKind } from "@cerbos/core";
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

// Type definitions for fixtures
type TestFixtures = {
  users: User[];
  nextLevelResources: NextLevelNestedResource[];
  tags: Tag[];
  labels: Label[];
  nestedResources: NestedResource[];
  subCategories: SubCategory[];
  categories: Category[];
  resources: Resource[];
};

// Define interface for testing utilities
interface TestContext {
  orm: MikroORM;
  em: EntityManager;
  cerbos: Cerbos;
  fixtures: TestFixtures;
  loadFixtures: () => Promise<void>;
}

// Create test context
const testContext: TestContext = {
  orm: null as unknown as MikroORM,
  em: null as unknown as EntityManager<IDatabaseDriver<Connection>>,
  cerbos: new Cerbos("127.0.0.1:3593", { tls: false }),
  fixtures: {
    users: [
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
    ],
    nextLevelResources: [
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
    ],
    tags: [
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
    ],
    nestedResources: [
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
    ],
    labels: [
      { id: "label1", name: "important" },
      { id: "label2", name: "archived" },
      { id: "label3", name: "flagged" },
    ],
    subCategories: [
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
    ],
    categories: [
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
    ],
    resources: [
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
    ],
  },
  loadFixtures: async function () {
    const { em, fixtures } = this as TestContext;

    // Clear database
    await Promise.all([
      em.nativeDelete(Resource, {}),
      em.nativeDelete(User, {}),
      em.nativeDelete(NestedResource, {}),
      em.nativeDelete(NextLevelNestedResource, {}),
      em.nativeDelete(Tag, {}),
      em.nativeDelete(Label, {}),
      em.nativeDelete(SubCategory, {}),
      em.nativeDelete(Category, {}),
    ]);

    // Create entities in dependency order
    // 1. Create users
    for (const user of fixtures.users) {
      const userEntity = em.create(User, user);
      await em.persistAndFlush(userEntity);
    }

    // 2. Create next level resources
    for (const nextLevel of fixtures.nextLevelResources) {
      const nextLevelEntity = em.create(NextLevelNestedResource, nextLevel);
      await em.persistAndFlush(nextLevelEntity);
    }

    // 3. Create tags
    for (const tag of fixtures.tags) {
      const tagEntity = em.create(Tag, tag);
      await em.persistAndFlush(tagEntity);
    }

    // 4. Create labels
    for (const label of fixtures.labels) {
      const labelEntity = em.create(Label, label);
      await em.persistAndFlush(labelEntity);
    }

    // 5. Create nested resources
    for (const nested of fixtures.nestedResources) {
      const nextLevel = await em.findOne(NextLevelNestedResource, {
        id: nested.nextlevel,
      });
      if (!nextLevel) {
        throw new Error(
          `NextLevelNestedResource not found: ${nested.nextlevel}`
        );
      }
      const nestedEntity = em.create(NestedResource, {
        ...nested,
        nextlevel: nextLevel,
      });
      await em.persistAndFlush(nestedEntity);
    }

    // 6. Create sub categories
    for (const subCategory of fixtures.subCategories) {
      const labels = await em.find(Label, { id: { $in: subCategory.labels } });
      const subCategoryEntity = em.create(SubCategory, {
        ...subCategory,
        labels,
      });
      await em.persistAndFlush(subCategoryEntity);
    }

    // 7. Create categories
    for (const category of fixtures.categories) {
      const subCategories = await em.find(SubCategory, {
        id: { $in: category.subCategories },
      });
      const categoryEntity = em.create(Category, {
        ...category,
        subCategories,
      });
      await em.persistAndFlush(categoryEntity);
    }

    // 8. Create resources
    for (const resource of fixtures.resources) {
      const createdBy = await em.findOne(User, { id: resource.createdBy });
      if (!createdBy) {
        throw new Error(`User not found: ${resource.createdBy}`);
      }

      const ownedBy = await em.find(User, { id: { $in: resource.ownedBy } });
      const nested = await em.findOne(NestedResource, { id: resource.nested });
      if (!nested) {
        throw new Error(`NestedResource not found: ${resource.nested}`);
      }

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
  },
};

// Helper function for running a test case
async function runQueryPlanTest(
  actionName: string,
  mapper: Record<string, any> = {},
  expectedFilters: any,
  expectedResults?: string[]
) {
  const { cerbos, em, fixtures } = testContext;

  // Get query plan from Cerbos
  const queryPlan = await cerbos.planResources({
    principal: { id: "user1", roles: ["USER"] },
    resource: { kind: "resource" },
    action: actionName,
  });

  // Transform to MikroORM query
  const result = queryPlanToMikroORM({
    queryPlan,
    mapper,
  });

  // Verify the structure of the result
  if (expectedFilters) {
    if (result.kind === PlanKind.CONDITIONAL) {
      expect(result.filters).toStrictEqual(expectedFilters);
    } else {
      expect(result).toStrictEqual(expectedFilters);
    }
  }

  // If expected results are provided, execute the query and check
  if (expectedResults !== undefined && result.kind === PlanKind.CONDITIONAL) {
    const query = await em.find(Resource, result.filters);
    const resultIds = query.map((r) => r.id);
    expect(resultIds.sort()).toEqual(expectedResults.sort());
  }

  return result;
}

// Setup/Teardown
beforeAll(async () => {
  testContext.orm = await MikroORM.init<SqliteDriver>({
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

  await testContext.orm.getSchemaGenerator().createSchema();
  testContext.em = testContext.orm.em.fork();
});

beforeEach(async () => {
  await testContext.loadFixtures();
});

afterEach(() => {
  testContext.em.clear();
});

afterAll(async () => {
  await testContext.orm.close(true);
});

// Core Functionality
describe("Basic Plan Types", () => {
  test("always allowed", async () => {
    const result = await runQueryPlanTest(
      "always-allow",
      {},
      { kind: PlanKind.ALWAYS_ALLOWED }
    );

    expect(result.kind).toEqual(PlanKind.ALWAYS_ALLOWED);

    // Confirm we can access all records
    const query = await testContext.em.find(Resource, {});
    expect(query.length).toEqual(testContext.fixtures.resources.length);
  });

  test("conditional - eq", async () => {
    const expectedIds = testContext.fixtures.resources
      .filter((r) => r.aBool)
      .map((r) => r.id);

    await runQueryPlanTest(
      "equal",
      { "request.resource.attr.aBool": { field: "aBool" } },
      { aBool: true },
      expectedIds
    );
  });
});

// Field Operations
describe("Field Operations", () => {
  describe("Basic Field Tests", () => {
    test("conditional - ne", async () => {
      const expectedIds = testContext.fixtures.resources
        .filter((r) => r.aString !== "string")
        .map((r) => r.id);

      await runQueryPlanTest(
        "ne",
        { "request.resource.attr.aString": { field: "aString" } },
        { aString: { $ne: "string" } },
        expectedIds
      );
    });

    test("conditional - gt", async () => {
      const expectedIds = testContext.fixtures.resources
        .filter((r) => r.aNumber > 1)
        .map((r) => r.id);

      await runQueryPlanTest(
        "gt",
        { "request.resource.attr.aNumber": { field: "aNumber" } },
        { aNumber: { $gt: 1 } },
        expectedIds
      );
    });

    test("conditional - lt", async () => {
      const expectedIds = testContext.fixtures.resources
        .filter((r) => r.aNumber < 2)
        .map((r) => r.id);

      await runQueryPlanTest(
        "lt",
        { "request.resource.attr.aNumber": { field: "aNumber" } },
        { aNumber: { $lt: 2 } },
        expectedIds
      );
    });
  });

  describe("Comparison Tests", () => {
    test("conditional - gte", async () => {
      const expectedIds = testContext.fixtures.resources
        .filter((r) => r.aNumber >= 1)
        .map((r) => r.id);

      await runQueryPlanTest(
        "gte",
        { "request.resource.attr.aNumber": { field: "aNumber" } },
        { aNumber: { $gte: 1 } },
        expectedIds
      );
    });

    test("conditional - lte", async () => {
      const expectedIds = testContext.fixtures.resources
        .filter((r) => r.aNumber <= 2)
        .map((r) => r.id);

      await runQueryPlanTest(
        "lte",
        { "request.resource.attr.aNumber": { field: "aNumber" } },
        { aNumber: { $lte: 2 } },
        expectedIds
      );
    });
  });

  describe("String Operations", () => {
    test("conditional - contains", async () => {
      const expectedIds = testContext.fixtures.resources
        .filter((r) => r.aString.includes("str"))
        .map((r) => r.id);

      await runQueryPlanTest(
        "contains",
        { "request.resource.attr.aString": { field: "aString" } },
        { aString: { $like: "%str%" } },
        expectedIds
      );
    });

    test("conditional - startsWith", async () => {
      const expectedIds = testContext.fixtures.resources
        .filter((r) => r.aString.startsWith("str"))
        .map((r) => r.id);

      await runQueryPlanTest(
        "starts-with",
        { "request.resource.attr.aString": { field: "aString" } },
        { aString: { $like: "str%" } },
        expectedIds
      );
    });

    test("conditional - endsWith", async () => {
      const expectedIds = testContext.fixtures.resources
        .filter((r) => r.aString.endsWith("ing"))
        .map((r) => r.id);

      await runQueryPlanTest(
        "ends-with",
        { "request.resource.attr.aString": { field: "aString" } },
        { aString: { $like: "%ing" } },
        expectedIds
      );
    });

    test("conditional - isSet", async () => {
      const expectedIds = testContext.fixtures.resources
        .filter((r) => r.aOptionalString)
        .map((r) => r.id);

      await runQueryPlanTest(
        "is-set",
        {
          "request.resource.attr.aOptionalString": { field: "aOptionalString" },
        },
        { aOptionalString: { $ne: null } },
        expectedIds
      );
    });
  });
});

// Collection Operations
describe("Collection Operations", () => {
  test("conditional - in", async () => {
    const expectedIds = testContext.fixtures.resources
      .filter((r) => ["string", "anotherString"].includes(r.aString))
      .map((r) => r.id);

    await runQueryPlanTest(
      "in",
      { "request.resource.attr.aString": { field: "aString" } },
      { aString: { $in: ["string", "anotherString"] } },
      expectedIds
    );
  });

  test("conditional - exists", async () => {
    const expectedIds = testContext.fixtures.resources
      .filter((r) => r.tags?.getItems().some((tag) => tag.id === "tag1"))
      .map((r) => r.id);

    await runQueryPlanTest(
      "exists",
      {
        "request.resource.attr.tags": {
          relation: {
            name: "tags",
            type: "many",
          },
        },
      },
      {
        tags: {
          $exists: true,
          name: "public",
        },
      },
      expectedIds
    );
  });

  test("conditional - exists_one", async () => {
    const expectedIds = testContext.fixtures.resources
      .filter((r) => r.tags?.filter((t) => t.id === "tag1").length === 1)
      .map((r) => r.id);

    await runQueryPlanTest(
      "exists-one",
      {
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
      {
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
      expectedIds
    );
  });

  test("conditional - all", async () => {
    const { fixtures } = testContext;

    const expectedIds = fixtures.resources
      .filter((r) =>
        r.tags
          ?.getItems()
          .every(
            (t) => fixtures.tags.find((ft) => ft.id === t.id)?.name === "public"
          )
      )
      .map((r) => r.id);

    await runQueryPlanTest(
      "all",
      {
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
      {
        tags: { $all: { name: "public" } },
      },
      expectedIds
    );
  });
});

// Relations
describe("Relations", () => {
  describe("One-to-One", () => {
    test("conditional - relation eq", async () => {
      const expectedIds = testContext.fixtures.resources
        .filter((r) => r.nested?.id === "nested1")
        .map((r) => r.id);

      await runQueryPlanTest(
        "relation-eq",
        {
          "request.resource.attr.nested": {
            relation: {
              name: "nested",
              type: "one",
              field: "id",
            },
          },
        },
        { "nested.id": "nested1" },
        expectedIds
      );
    });
  });

  describe("One-to-Many", () => {
    test("conditional - relation some", async () => {
      const expectedIds = testContext.fixtures.resources
        .filter((r) => r.tags?.contains({ id: "tag1" } as Tag))
        .map((r) => r.id);

      await runQueryPlanTest(
        "relation-some",
        {
          "request.resource.attr.tags": {
            relation: {
              name: "tags",
              type: "many",
              field: "id",
            },
          },
        },
        { "tags.id": { $in: ["tag1"] } },
        expectedIds
      );
    });
  });

  describe("Nested Relations", () => {
    test("conditional - deeply nested category label", async () => {
      await runQueryPlanTest(
        "deep-nested-category-label",
        {
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
        {
          "categories.subCategories.labels": {
            $exists: true,
            name: "important",
          },
        },
        ["resource1"] // Expected IDs
      );
    });
  });
});

// Complex Operations
describe("Complex Operations", () => {
  test("conditional - and", async () => {
    const expectedIds = testContext.fixtures.resources
      .filter((r) => r.aBool && r.aNumber > 1)
      .map((r) => r.id);

    await runQueryPlanTest(
      "and",
      {
        "request.resource.attr.aBool": { field: "aBool" },
        "request.resource.attr.aNumber": { field: "aNumber" },
      },
      {
        $and: [{ aBool: true }, { aNumber: { $gt: 1 } }],
      },
      expectedIds
    );
  });

  test("conditional - or", async () => {
    const expectedIds = testContext.fixtures.resources
      .filter((r) => r.aBool || r.aNumber > 2)
      .map((r) => r.id);

    await runQueryPlanTest(
      "or",
      {
        "request.resource.attr.aBool": { field: "aBool" },
        "request.resource.attr.aNumber": { field: "aNumber" },
      },
      {
        $or: [{ aBool: true }, { aNumber: { $gt: 2 } }],
      },
      expectedIds
    );
  });

  test("conditional - hasIntersection with map", async () => {
    await runQueryPlanTest(
      "has-intersection-map",
      {
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
      {
        "categories.subCategories.name": { $in: ["finance", "tech"] },
      },
      ["resource1", "resource2", "resource3"]
    );
  });
});

// Deep Nesting
describe("Deep Nesting", () => {
  test("conditional - deeply nested eq", async () => {
    const { fixtures } = testContext;

    const expectedIds = fixtures.resources
      .filter((r) => {
        const nested = fixtures.nestedResources.find(
          (n) => n.id === (r.nested as unknown as string)
        );
        const nextLevel = fixtures.nextLevelResources.find(
          (nl) => nl.id === nested?.nextlevel?.id
        );
        return nextLevel?.aBool === true;
      })
      .map((r) => r.id);

    await runQueryPlanTest(
      "deeply-nested-eq",
      {
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
      { "nested.nextlevel.aBool": true },
      expectedIds
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

// Return Types
describe("Return Types", () => {
  test("returns correct type for ALWAYS_ALLOWED", async () => {
    const result = await runQueryPlanTest(
      "always-allow",
      {},
      { kind: PlanKind.ALWAYS_ALLOWED }
    );

    expect(result.kind).toBe(PlanKind.ALWAYS_ALLOWED);

    if (result.kind === PlanKind.ALWAYS_ALLOWED) {
      expect(Object.keys(result)).toEqual(["kind"]);
    }
  });

  test("returns correct type for CONDITIONAL", async () => {
    const result = await runQueryPlanTest(
      "equal",
      {
        "request.resource.attr.aBool": { field: "aBool" },
      },
      { aBool: true }
    );

    expect(result.kind).toBe(PlanKind.CONDITIONAL);

    if (result.kind === PlanKind.CONDITIONAL) {
      expect(Object.keys(result).sort()).toEqual(["filters", "kind"].sort());
      expect(typeof result.filters).toBe("object");
    }
  });
});
