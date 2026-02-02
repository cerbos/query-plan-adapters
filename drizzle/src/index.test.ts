import {
  PlanExpressionOperand,
  PlanKind,
  PlanResourcesResponse,
} from "@cerbos/core";
import { GRPC as Cerbos } from "@cerbos/grpc";
import type { ValidationError } from "@cerbos/core";
import Database from "better-sqlite3";
import { eq, sql } from "drizzle-orm";
import { drizzle } from "drizzle-orm/better-sqlite3";
import {
  integer,
  sqliteTable,
  text,
} from "drizzle-orm/sqlite-core";
import { queryPlanToDrizzle } from ".";
import type { MapperEntry, QueryPlanToDrizzleResult } from ".";

const cerbos = new Cerbos("127.0.0.1:3593", { tls: false });
const sqlite = new Database(":memory:");
const db = drizzle(sqlite);

const users = sqliteTable("users", {
  id: text("id").primaryKey(),
  aString: text("a_string").notNull(),
  aNumber: integer("a_number").notNull(),
  aBool: integer("a_bool", { mode: "boolean" }).notNull(),
});

const resources = sqliteTable("resources", {
  id: text("id").primaryKey(),
  aString: text("a_string").notNull(),
  aNumber: integer("a_number").notNull(),
  aBool: integer("a_bool", { mode: "boolean" }).notNull(),
  aOptionalString: text("a_optional_string"),
  creatorId: text("creator_id").notNull(),
  nestedResourceId: text("nested_resource_id").notNull(),
});

const nextLevelNestedResources = sqliteTable("next_level_nested_resources", {
  id: text("id").primaryKey(),
  aString: text("a_string").notNull(),
  aNumber: integer("a_number").notNull(),
  aBool: integer("a_bool", { mode: "boolean" }).notNull(),
});

const nestedResources = sqliteTable("nested_resources", {
  id: text("id").primaryKey(),
  aString: text("a_string").notNull(),
  aNumber: integer("a_number").notNull(),
  aBool: integer("a_bool", { mode: "boolean" }).notNull(),
  aOptionalString: text("a_optional_string"),
  nextLevelId: text("next_level_id").notNull(),
});

const tags = sqliteTable("tags", {
  id: text("id").primaryKey(),
  name: text("name").notNull(),
});

const resourceTags = sqliteTable("resource_tags", {
  resourceId: text("resource_id").notNull(),
  tagId: text("tag_id").notNull(),
});

const resourceOwners = sqliteTable("resource_owners", {
  resourceId: text("resource_id").notNull(),
  ownerId: text("owner_id").notNull(),
});

const categories = sqliteTable("categories", {
  id: text("id").primaryKey(),
  name: text("name").notNull(),
});

const resourceCategories = sqliteTable("resource_categories", {
  resourceId: text("resource_id").notNull(),
  categoryId: text("category_id").notNull(),
});

const subCategories = sqliteTable("sub_categories", {
  id: text("id").primaryKey(),
  name: text("name").notNull(),
});

const categorySubCategories = sqliteTable("category_sub_categories", {
  categoryId: text("category_id").notNull(),
  subCategoryId: text("sub_category_id").notNull(),
});

const labels = sqliteTable("labels", {
  id: text("id").primaryKey(),
  name: text("name").notNull(),
});

const subCategoryLabels = sqliteTable("sub_category_labels", {
  subCategoryId: text("sub_category_id").notNull(),
  labelId: text("label_id").notNull(),
});

type User = {
  id: string;
  aBool: boolean;
  aNumber: number;
  aString: string;
};

type NextLevel = {
  id: string;
  aBool: boolean;
  aNumber: number;
  aString: string;
};

type NestedResource = {
  id: string;
  aBool: boolean;
  aNumber: number;
  aString: string;
  aOptionalString: string | null;
  nextLevelId: string;
};

type Tag = {
  id: string;
  name: string;
};

type Label = {
  id: string;
  name: string;
};

type SubCategory = {
  id: string;
  name: string;
  labelIds: string[];
};

type Category = {
  id: string;
  name: string;
  subCategoryIds: string[];
};

type ResourceFixture = {
  id: string;
  aBool: boolean;
  aNumber: number;
  aString: string;
  aOptionalString: string | null;
  createdById: string;
  ownedByIds: string[];
  nestedId: string;
  tagIds: string[];
  categoryIds: string[];
};

const userFixtures: User[] = [
  { id: "user1", aBool: true, aNumber: 1, aString: "string" },
  { id: "user2", aBool: true, aNumber: 2, aString: "string" },
];

const nextLevelFixtures: NextLevel[] = [
  { id: "nextLevel1", aBool: true, aNumber: 1, aString: "string" },
  { id: "nextLevel2", aBool: false, aNumber: 1, aString: "string" },
  { id: "nextLevel3", aBool: true, aNumber: 1, aString: "string" },
];

const nestedFixtures: NestedResource[] = [
  {
    id: "nested1",
    aBool: true,
    aNumber: 1,
    aString: "string",
    aOptionalString: null,
    nextLevelId: "nextLevel1",
  },
  {
    id: "nested2",
    aBool: false,
    aNumber: 1,
    aString: "string",
    aOptionalString: null,
    nextLevelId: "nextLevel2",
  },
  {
    id: "nested3",
    aBool: true,
    aNumber: 1,
    aString: "string",
    aOptionalString: null,
    nextLevelId: "nextLevel3",
  },
];

const tagFixtures: Tag[] = [
  { id: "tag1", name: "public" },
  { id: "tag2", name: "private" },
  { id: "tag3", name: "draft" },
];

const labelFixtures: Label[] = [
  { id: "label1", name: "important" },
  { id: "label2", name: "archived" },
  { id: "label3", name: "flagged" },
];

const subCategoryFixtures: SubCategory[] = [
  { id: "sub1", name: "finance", labelIds: ["label1", "label2"] },
  { id: "sub2", name: "tech", labelIds: ["label2", "label3"] },
];

const categoryFixtures: Category[] = [
  { id: "cat1", name: "business", subCategoryIds: ["sub1"] },
  { id: "cat2", name: "development", subCategoryIds: ["sub2"] },
];

const resourceFixtures: ResourceFixture[] = [
  {
    id: "resource1",
    aBool: true,
    aNumber: 1,
    aString: "string",
    aOptionalString: "optionalString",
    createdById: "user1",
    ownedByIds: ["user1"],
    nestedId: "nested1",
    tagIds: ["tag1"],
    categoryIds: ["cat1"],
  },
  {
    id: "resource2",
    aBool: false,
    aNumber: 2,
    aString: "string2",
    aOptionalString: null,
    createdById: "user2",
    ownedByIds: ["user2"],
    nestedId: "nested3",
    tagIds: ["tag2"],
    categoryIds: ["cat2"],
  },
  {
    id: "resource3",
    aBool: false,
    aNumber: 3,
    aString: "string3",
    aOptionalString: null,
    createdById: "user1",
    ownedByIds: ["user1", "user2"],
    nestedId: "nested3",
    tagIds: ["tag1", "tag3"],
    categoryIds: ["cat1", "cat2"],
  },
];

const userMap = new Map(userFixtures.map((user) => [user.id, user]));
const nextLevelMap = new Map(nextLevelFixtures.map((nl) => [nl.id, nl]));
const nestedMap = new Map(
  nestedFixtures.map((nested) => [nested.id, nested])
);
const tagMap = new Map(tagFixtures.map((tag) => [tag.id, tag]));
const labelMap = new Map(labelFixtures.map((label) => [label.id, label]));
const subCategoryMap = new Map(
  subCategoryFixtures.map((sub) => [sub.id, sub])
);
const categoryMap = new Map(categoryFixtures.map((cat) => [cat.id, cat]));

const resourceAttributes = resourceFixtures.map((resource) => ({
  id: resource.id,
  aBool: resource.aBool,
  aNumber: resource.aNumber,
  aString: resource.aString,
  aOptionalString: resource.aOptionalString,
  createdBy: { ...userMap.get(resource.createdById)! },
  ownedBy: resource.ownedByIds.map((ownerId) => ({
    ...userMap.get(ownerId)!,
  })),
  nested: {
    ...nestedMap.get(resource.nestedId)!,
    nextlevel: { ...nextLevelMap.get(nestedMap.get(resource.nestedId)!.nextLevelId)! },
  },
  tags: resource.tagIds.map((tagId) => ({ ...tagMap.get(tagId)! })),
  categories: resource.categoryIds.map((categoryId) => {
    const category = categoryMap.get(categoryId)!;
    return {
      id: category.id,
      name: category.name,
      subCategories: category.subCategoryIds.map((subId) => {
        const sub = subCategoryMap.get(subId)!;
        return {
          id: sub.id,
          name: sub.name,
          labels: sub.labelIds.map((labelId) => ({
            ...labelMap.get(labelId)!,
          })),
        };
      }),
    };
  }),
}));

const allResourceIds = resourceAttributes.map((resource) => resource.id).sort();

const expectedCache = new Map<string, Promise<string[]>>();

const allowedResourceIds = (action: string): Promise<string[]> => {
  let cached = expectedCache.get(action);
  if (!cached) {
    cached = (async () => {
      const response = await cerbos.checkResources({
        principal: { id: "user1", roles: ["USER"] },
        resources: resourceAttributes.map((resource) => ({
          resource: {
            kind: "resource",
            id: resource.id,
            attr: resource,
          },
          actions: [action],
        })),
      });

      return response.results
        .filter((result) => result.isAllowed(action) === true)
        .map((result) => result.resource.id)
        .sort();
    })();
    expectedCache.set(action, cached);
  }
  return cached;
};

const mapper: Record<string, MapperEntry> = {
  "request.resource.attr.id": resources.id,
  "request.resource.attr.aString": resources.aString,
  "request.resource.attr.aNumber": resources.aNumber,
  "request.resource.attr.aBool": resources.aBool,
  "request.resource.attr.aOptionalString": resources.aOptionalString,
  "request.resource.attr.createdBy": {
    relation: {
      type: "one",
      table: users,
      sourceColumn: resources.creatorId,
      targetColumn: users.id,
      field: users.id,
      fields: {
        id: users.id,
        aString: users.aString,
        aNumber: users.aNumber,
        aBool: users.aBool,
      },
    },
  },
  "request.resource.attr.ownedBy": {
    relation: {
      type: "many",
      table: resourceOwners,
      sourceColumn: resources.id,
      targetColumn: resourceOwners.resourceId,
      field: resourceOwners.ownerId,
      fields: {
        id: {
          relation: {
            type: "one",
            table: users,
            sourceColumn: resourceOwners.ownerId,
            targetColumn: users.id,
            field: users.id,
            fields: {
              aString: users.aString,
              aNumber: users.aNumber,
              aBool: users.aBool,
            },
          },
        },
        aString: {
          relation: {
            type: "one",
            table: users,
            sourceColumn: resourceOwners.ownerId,
            targetColumn: users.id,
            field: users.aString,
          },
        },
        aNumber: {
          relation: {
            type: "one",
            table: users,
            sourceColumn: resourceOwners.ownerId,
            targetColumn: users.id,
            field: users.aNumber,
          },
        },
        aBool: {
          relation: {
            type: "one",
            table: users,
            sourceColumn: resourceOwners.ownerId,
            targetColumn: users.id,
            field: users.aBool,
          },
        },
      },
    },
  },
  "request.resource.attr.nested": {
    relation: {
      type: "one",
      table: nestedResources,
      sourceColumn: resources.nestedResourceId,
      targetColumn: nestedResources.id,
      field: nestedResources.id,
      fields: {
        id: nestedResources.id,
        aString: nestedResources.aString,
        aNumber: nestedResources.aNumber,
        aBool: nestedResources.aBool,
        aOptionalString: nestedResources.aOptionalString,
        nextlevel: {
          relation: {
            type: "one",
            table: nextLevelNestedResources,
            sourceColumn: nestedResources.nextLevelId,
            targetColumn: nextLevelNestedResources.id,
            field: nextLevelNestedResources.id,
            fields: {
              id: nextLevelNestedResources.id,
              aString: nextLevelNestedResources.aString,
              aNumber: nextLevelNestedResources.aNumber,
              aBool: nextLevelNestedResources.aBool,
            },
          },
        },
      },
    },
  },
  "request.resource.attr.tags": {
    relation: {
      type: "many",
      table: resourceTags,
      sourceColumn: resources.id,
      targetColumn: resourceTags.resourceId,
      field: resourceTags.tagId,
      fields: {
        id: {
          relation: {
            type: "one",
            table: tags,
            sourceColumn: resourceTags.tagId,
            targetColumn: tags.id,
            field: tags.id,
            fields: {
              name: tags.name,
            },
          },
        },
        name: {
          relation: {
            type: "one",
            table: tags,
            sourceColumn: resourceTags.tagId,
            targetColumn: tags.id,
            field: tags.name,
          },
        },
      },
    },
  },
  "request.resource.attr.categories": {
    relation: {
      type: "many",
      table: resourceCategories,
      sourceColumn: resources.id,
      targetColumn: resourceCategories.resourceId,
      field: resourceCategories.categoryId,
      fields: {
        id: {
          relation: {
            type: "one",
            table: categories,
            sourceColumn: resourceCategories.categoryId,
            targetColumn: categories.id,
            field: categories.id,
            fields: {
              name: categories.name,
            },
          },
        },
        name: {
          relation: {
            type: "one",
            table: categories,
            sourceColumn: resourceCategories.categoryId,
            targetColumn: categories.id,
            field: categories.name,
            fields: {
              subCategories: {
                relation: {
                  type: "many",
                  table: categorySubCategories,
                  sourceColumn: categories.id,
                  targetColumn: categorySubCategories.categoryId,
                  field: categorySubCategories.subCategoryId,
                  fields: {
                    id: {
                      relation: {
                        type: "one",
                        table: subCategories,
                        sourceColumn: categorySubCategories.subCategoryId,
                        targetColumn: subCategories.id,
                        field: subCategories.id,
                        fields: {
                          name: subCategories.name,
                        },
                      },
                    },
                    name: {
                      relation: {
                        type: "one",
                        table: subCategories,
                        sourceColumn: categorySubCategories.subCategoryId,
                        targetColumn: subCategories.id,
                        field: subCategories.name,
                      },
                    },
                    labels: {
                      relation: {
                        type: "many",
                        table: subCategoryLabels,
                        sourceColumn: categorySubCategories.subCategoryId,
                        targetColumn: subCategoryLabels.subCategoryId,
                        field: subCategoryLabels.labelId,
                        fields: {
                          id: {
                            relation: {
                              type: "one",
                              table: labels,
                              sourceColumn: subCategoryLabels.labelId,
                              targetColumn: labels.id,
                              field: labels.id,
                              fields: {
                                name: labels.name,
                              },
                            },
                          },
                          name: {
                            relation: {
                              type: "one",
                              table: labels,
                              sourceColumn: subCategoryLabels.labelId,
                              targetColumn: labels.id,
                              field: labels.name,
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
        },
        subCategories: {
          relation: {
            type: "many",
            table: categorySubCategories,
            sourceColumn: resourceCategories.categoryId,
            targetColumn: categorySubCategories.categoryId,
            field: categorySubCategories.subCategoryId,
            fields: {
              id: {
                relation: {
                  type: "one",
                  table: subCategories,
                  sourceColumn: categorySubCategories.subCategoryId,
                  targetColumn: subCategories.id,
                  field: subCategories.id,
                  fields: {
                    name: subCategories.name,
                  },
                },
              },
              name: {
                relation: {
                  type: "one",
                  table: subCategories,
                  sourceColumn: categorySubCategories.subCategoryId,
                  targetColumn: subCategories.id,
                  field: subCategories.name,
                },
              },
              labels: {
                relation: {
                  type: "many",
                  table: subCategoryLabels,
                  sourceColumn: categorySubCategories.subCategoryId,
                  targetColumn: subCategoryLabels.subCategoryId,
                  field: subCategoryLabels.labelId,
                  fields: {
                    id: {
                      relation: {
                        type: "one",
                        table: labels,
                        sourceColumn: subCategoryLabels.labelId,
                        targetColumn: labels.id,
                        field: labels.id,
                        fields: {
                          name: labels.name,
                        },
                      },
                    },
                    name: {
                      relation: {
                        type: "one",
                        table: labels,
                        sourceColumn: subCategoryLabels.labelId,
                        targetColumn: labels.id,
                        field: labels.name,
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
  },
};

const basePlanFields = {
  cerbosCallId: "call-id",
  requestId: "req-id",
  validationErrors: [] as ValidationError[],
  metadata: undefined as PlanResourcesResponse["metadata"],
};

const buildPlan = (
  condition: PlanExpressionOperand
): PlanResourcesResponse => ({
  ...basePlanFields,
  kind: PlanKind.CONDITIONAL,
  condition,
});

const ensureFilter = (result: QueryPlanToDrizzleResult) => {
  if (result.kind !== PlanKind.CONDITIONAL) {
    throw new Error(`Expected conditional plan, received ${result.kind}`);
  }
  return result.filter;
};

const selectIds = (filter?: ReturnType<typeof ensureFilter>) => {
  const baseQuery = db.select({ id: resources.id }).from(resources);
  const queryWithFilter = filter ? baseQuery.where(filter) : baseQuery;
  return queryWithFilter
    .all()
    .map((row) => row.id)
    .sort();
};

const conditionalActions = [
  "all",
  "and",
  "combined-and",
  "combined-not",
  "combined-or",
  "contains",
  "deep-nested-category-label",
  "deep-nested-exists",
  "deeply-nested-starts-with",
  "ends-with",
  "equal",
  "equal-deeply-nested",
  "equal-nested",
  "exists-multiple",
  "exists-nested-collection",
  "exists-one",
  "exists-single",
  "explicit-deny",
  "filter",
  "filter-deeply-nested",
  "gt",
  "gte",
  "has-intersection",
  "has-intersection-direct",
  "has-intersection-nested",
  "has-no-tag",
  "has-tag",
  "in",
  "is-set",
  "is-set-nested",
  "kitchensink",
  "lt",
  "lte",
  "map-collection",
  "map-deeply-nested",
  "nand",
  "ne",
  "nested-contains",
  "nor",
  "or",
  "relation-eq-number",
  "relation-gt-number",
  "relation-gte-number",
  "relation-is",
  "relation-is-not",
  "relation-lt-number",
  "relation-lte-number",
  "relation-multiple-all",
  "relation-multiple-none",
  "relation-multiple-or",
  "relation-none",
  "relation-some",
  "starts-with",
];

beforeAll(() => {
  sqlite.exec(`
    CREATE TABLE users (
      id TEXT PRIMARY KEY,
      a_string TEXT NOT NULL,
      a_number INTEGER NOT NULL,
      a_bool INTEGER NOT NULL
    );
    CREATE TABLE next_level_nested_resources (
      id TEXT PRIMARY KEY,
      a_string TEXT NOT NULL,
      a_number INTEGER NOT NULL,
      a_bool INTEGER NOT NULL
    );
    CREATE TABLE nested_resources (
      id TEXT PRIMARY KEY,
      a_string TEXT NOT NULL,
      a_number INTEGER NOT NULL,
      a_bool INTEGER NOT NULL,
      a_optional_string TEXT,
      next_level_id TEXT NOT NULL
    );
    CREATE TABLE resources (
      id TEXT PRIMARY KEY,
      a_string TEXT NOT NULL,
      a_number INTEGER NOT NULL,
      a_bool INTEGER NOT NULL,
      a_optional_string TEXT,
      creator_id TEXT NOT NULL,
      nested_resource_id TEXT NOT NULL
    );
    CREATE TABLE tags (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL
    );
    CREATE TABLE resource_tags (
      resource_id TEXT NOT NULL,
      tag_id TEXT NOT NULL
    );
    CREATE TABLE resource_owners (
      resource_id TEXT NOT NULL,
      owner_id TEXT NOT NULL
    );
    CREATE TABLE categories (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL
    );
    CREATE TABLE resource_categories (
      resource_id TEXT NOT NULL,
      category_id TEXT NOT NULL
    );
    CREATE TABLE sub_categories (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL
    );
    CREATE TABLE category_sub_categories (
      category_id TEXT NOT NULL,
      sub_category_id TEXT NOT NULL
    );
    CREATE TABLE labels (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL
    );
    CREATE TABLE sub_category_labels (
      sub_category_id TEXT NOT NULL,
      label_id TEXT NOT NULL
    );
  `);
});

beforeEach(() => {
  const tables = [
    "resource_categories",
    "resource_tags",
    "resource_owners",
    "resources",
    "nested_resources",
    "next_level_nested_resources",
    "tags",
    "categories",
    "category_sub_categories",
    "sub_category_labels",
    "sub_categories",
    "labels",
    "users",
  ];

  for (const table of tables) {
    sqlite.prepare(`DELETE FROM ${table};`).run();
  }

  db.insert(users)
    .values(userFixtures.map(({ id, aBool, aNumber, aString }) => ({
      id,
      aBool,
      aNumber,
      aString,
    })))
    .run();

  db.insert(nextLevelNestedResources)
    .values(
      nextLevelFixtures.map(({ id, aBool, aNumber, aString }) => ({
        id,
        aBool,
        aNumber,
        aString,
      }))
    )
    .run();

  db.insert(nestedResources)
    .values(
      nestedFixtures.map((nested) => ({
        id: nested.id,
        aBool: nested.aBool,
        aNumber: nested.aNumber,
        aString: nested.aString,
        aOptionalString: nested.aOptionalString,
        nextLevelId: nested.nextLevelId,
      }))
    )
    .run();

  db.insert(tags)
    .values(tagFixtures.map(({ id, name }) => ({ id, name })))
    .run();

  db.insert(labels)
    .values(labelFixtures.map(({ id, name }) => ({ id, name })))
    .run();

  db.insert(subCategories)
    .values(subCategoryFixtures.map(({ id, name }) => ({ id, name })))
    .run();

  db.insert(categories)
    .values(categoryFixtures.map(({ id, name }) => ({ id, name })))
    .run();

  db.insert(resources)
    .values(
      resourceFixtures.map((resource) => ({
        id: resource.id,
        aBool: resource.aBool,
        aNumber: resource.aNumber,
        aString: resource.aString,
        aOptionalString: resource.aOptionalString,
        creatorId: resource.createdById,
        nestedResourceId: resource.nestedId,
      }))
    )
    .run();

  db.insert(resourceOwners)
    .values(
      resourceFixtures.flatMap((resource) =>
        resource.ownedByIds.map((ownerId) => ({
          resourceId: resource.id,
          ownerId,
        }))
      )
    )
    .run();

  db.insert(resourceTags)
    .values(
      resourceFixtures.flatMap((resource) =>
        resource.tagIds.map((tagId) => ({
          resourceId: resource.id,
          tagId,
        }))
      )
    )
    .run();

  db.insert(resourceCategories)
    .values(
      resourceFixtures.flatMap((resource) =>
        resource.categoryIds.map((categoryId) => ({
          resourceId: resource.id,
          categoryId,
        }))
      )
    )
    .run();

  db.insert(categorySubCategories)
    .values(
      categoryFixtures.flatMap((category) =>
        category.subCategoryIds.map((subCategoryId) => ({
          categoryId: category.id,
          subCategoryId,
        }))
      )
    )
    .run();

  db.insert(subCategoryLabels)
    .values(
      subCategoryFixtures.flatMap((sub) =>
        sub.labelIds.map((labelId) => ({
          subCategoryId: sub.id,
          labelId,
        }))
      )
    )
    .run();
});

afterAll(() => {
  cerbos.close();
  sqlite.close();
});

describe("queryPlanToDrizzle", () => {
  test("returns all records for ALWAYS_ALLOWED", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "always-allow",
    });

    const expected = await allowedResourceIds("always-allow");
    const result = queryPlanToDrizzle({ queryPlan, mapper });

    expect(result).toEqual({ kind: PlanKind.ALWAYS_ALLOWED });
    expect(expected).toEqual(allResourceIds);
  });

  test("returns no records for ALWAYS_DENIED", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "always-deny",
    });

    const expected = await allowedResourceIds("always-deny");
    const result = queryPlanToDrizzle({ queryPlan, mapper });

    expect(result).toEqual({ kind: PlanKind.ALWAYS_DENIED });
    expect(expected).toEqual([]);
  });

  test.each(conditionalActions)(
    "produces matching results for %s",
    async (action) => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action,
      });

      const expected = await allowedResourceIds(action);
      const result = queryPlanToDrizzle({ queryPlan, mapper });

      if (result.kind === PlanKind.ALWAYS_ALLOWED) {
        expect(expected).toEqual(allResourceIds);
        return;
      }

      if (result.kind === PlanKind.ALWAYS_DENIED) {
        expect(expected).toEqual([]);
        return;
      }

      const ids = selectIds(ensureFilter(result));
      expect(ids).toEqual(expected);
    }
  );

  test("supports function mappers", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "equal",
    });

    const mapperFn = (reference: string): MapperEntry | undefined =>
      mapper[reference];
    const result = queryPlanToDrizzle({ queryPlan, mapper: mapperFn });
    const expected = await allowedResourceIds("equal");
    const ids = selectIds(ensureFilter(result));
    expect(ids).toEqual(expected);
  });

  test("supports custom transforms", () => {
    const queryPlan = buildPlan({
      operator: "eq",
      operands: [
        { name: "request.resource.attr.aString" },
        { value: "STRING2" },
      ],
    });

    const result = queryPlanToDrizzle({
      queryPlan,
      mapper: {
        ...mapper,
        "request.resource.attr.aString": {
          column: resources.aString,
          transform: ({ value }) =>
            eq(
              sql`lower(${resources.aString})`,
              (value as string).toLowerCase()
            ),
        },
      },
    });

    const ids = selectIds(ensureFilter(result));
    expect(ids).toEqual(["resource2"]);
  });

  test("produces matching results for except", () => {
    // #given
    const queryPlan = buildPlan({
      operator: "except",
      operands: [
        { name: "request.resource.attr.tags" },
        {
          operator: "lambda",
          operands: [
            {
              operator: "eq",
              operands: [
                { name: "tag.name" },
                { value: "public" },
              ],
            },
            { name: "tag" },
          ],
        },
      ],
    });

    // #when
    const result = queryPlanToDrizzle({ queryPlan, mapper });

    // #then — resources with any tag whose name != "public"
    const ids = selectIds(ensureFilter(result));
    const expected = resourceFixtures
      .filter((r) =>
        r.tagIds.some((tagId) => {
          const tag = tagFixtures.find((t) => t.id === tagId);
          return tag?.name !== "public";
        })
      )
      .map((r) => r.id)
      .sort();
    expect(ids).toEqual(expected);
  });

  test("produces matching results for exists_one on nested relation", () => {
    // #given — exists_one on ownedBy with nested user field
    const queryPlan = buildPlan({
      operator: "exists_one",
      operands: [
        { name: "request.resource.attr.ownedBy" },
        {
          operator: "lambda",
          operands: [
            {
              operator: "eq",
              operands: [
                { name: "owner.aBool" },
                { value: true },
              ],
            },
            { name: "owner" },
          ],
        },
      ],
    });

    // #when
    const result = queryPlanToDrizzle({ queryPlan, mapper });

    // #then — resources where exactly one owner has aBool=true
    const ids = selectIds(ensureFilter(result));
    const expected = resourceFixtures
      .filter((r) => {
        const owners = r.ownedByIds.map((id) => userFixtures.find((u) => u.id === id)!);
        return owners.filter((o) => o.aBool).length === 1;
      })
      .map((r) => r.id)
      .sort();
    expect(ids).toEqual(expected);
  });

  test("throws when mapping is missing", () => {
    const queryPlan = buildPlan({
      operator: "eq",
      operands: [
        { name: "request.resource.attr.unknown" },
        { value: "value" },
      ],
    });

    expect(() =>
      queryPlanToDrizzle({
        queryPlan,
        mapper,
      })
    ).toThrow(/No mapping/);
  });
});
