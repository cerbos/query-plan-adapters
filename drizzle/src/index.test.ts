import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "@jest/globals";
import {
  PlanExpressionOperand,
  PlanKind,
  PlanResourcesResponse,
} from "@cerbos/core";
import { GRPC as Cerbos } from "@cerbos/grpc";
import type { ValidationError, Value } from "@cerbos/core";
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

/**
 * The principal each action is planned and checked with.
 *
 * `has-intersection` compares a resource projection against `P.attr.tags`. With no such
 * attribute the planner cannot fold it to a value list and ships `get-field(struct(), tags)`
 * instead — an operand this adapter has no `IN` list to build from. It used to translate that
 * to a bare FALSE and agree with an equally empty expectation, so the action proved nothing;
 * it now throws, which is correct but leaves the intersection untested unless the principal
 * actually carries the attribute (cerbos/query-plan-adapters#387).
 *
 * "public" is held by resource1 and resource3, so the expectation is 2 of 3 rather than all or
 * nothing. Both sides of the differential call this, so the expectation follows automatically.
 */
const principalFor = (action: string) => ({
  id: "user1",
  roles: ["USER"],
  ...(action === "has-intersection" ? { attr: { tags: ["public"] } } : {}),
});

const allowedResourceIds = (action: string): Promise<string[]> => {
  let cached = expectedCache.get(action);
  if (!cached) {
    cached = (async () => {
      const response = await cerbos.checkResources({
        principal: principalFor(action),
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
  "not-and",
  "not-contains",
  "not-gt",
  "not-lt",
  "not-or",
  "not-starts-with",
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
  // New scenarios — arithmetic on a column inside a comparison.
  "arith-add",
  "arith-sub",
  "arith-mult",
  "arith-div",
  // CEL type conversions. Only string() survives as a CAST: int()/double() are rejected —
  // see the throwing assertions below and cerbos/query-plan-adapters#311.
  "convert-string",
  // Ternary expression — compiled to CASE WHEN.
  "ternary",
  // size() on scalar (LENGTH) and on relation (correlated COUNT subquery).
  "string-size",
  "empty-collection",
  // Issue #229: lock in additional operator/comparison shapes.
  "is-not-set",
  "equal-bool-false",
  "in-number",
  "or-leaf-exists",
  // Issue #232: collection macro composition.
  "all-nested",
  // #263: field-to-field equality and size(filter(...)) now translate (previously threw);
  // verified here against the check() oracle like every other supported shape.
  "equal-field-to-field",
  "filter-count-gt",
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
        principal: principalFor(action),
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

  // TODO(#229): Drizzle adapter does not support field-to-field equality
  // (both operands are name operands). Cerbos emits this as eq(name, name)
  // and the dispatch in buildFilterFromExpression requires one value operand.
  // If/when the adapter learns to compare two columns, replace this with a
  // data-driven assertion against the conditionalActions loop.
  // #311: int()/double() cannot be lowered to SQL CAST. CEL reads a WHOLE string or raises
  // (and an error denies), while CAST reads a numeric prefix — SQLite turns "100%_done"
  // into 100 — so the old lowering returned rows the PDP denies. The numeric direction is
  // no safer: CEL truncates toward zero where PostgreSQL and MySQL round. `arith-mod` is
  // here because its policy wraps the column in int() before the modulus.
  test.each([
    ["convert-int", /int\(\)/],
    ["convert-double", /double\(\)/],
    ["arith-mod", /int\(\)/],
  ])("throws for %s (SQL CAST is not a CEL conversion)", async (action, message) => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action,
    });

    expect(() => queryPlanToDrizzle({ queryPlan, mapper })).toThrow(message);
  });

  // #313: filter() returns a list, not a boolean. Used as a whole condition there is no
  // meaning to pick — it is not `size(filter(...)) > 0` — so the adapter fails closed
  // rather than guessing at "non-empty". The size(filter(...)) form still translates; see
  // the `filter-count-gt` case in the conditional actions above.
  test("throws for filter used as a condition", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "filter",
    });

    expect(() => queryPlanToDrizzle({ queryPlan, mapper })).toThrow(
      /returns a list, not a boolean/
    );
  });

  test("throws for index-list (array indexing on a relation)", async () => {
    // ownedBy is modelled as a join table — there is no scalar index column,
    // so R.attr.ownedBy[0] cannot be translated into a deterministic SQL fragment.
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "index-list",
    });

    expect(() =>
      queryPlanToDrizzle({ queryPlan, mapper })
    ).toThrow(/index/i);
  });

  // TODO(#232): drizzle adapter does not support map(...) == [...] — the
  // map() expression resolves to a column projection, not a list comparable
  // to a literal array. Lock in current behaviour.
  test("throws for map-compared (map(t, t.id) == [..])", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "map-compared",
    });

    expect(() => queryPlanToDrizzle({ queryPlan, mapper })).toThrow();
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

  test("fails closed for regex matches regardless of SQL dialect support", () => {
    const queryPlan = buildPlan({
      operator: "matches",
      operands: [
        { name: "request.resource.attr.aString" },
        { value: "^foo$" },
      ],
    });

    expect(() => queryPlanToDrizzle({ queryPlan, mapper })).toThrow(
      /do not guarantee CEL\/RE2 semantics/
    );
  });

  test.each([
    "2024-01-01",
    "0000-01-01T00:00:00Z",
    "2024-02-30T00:00:00Z",
    "2024-01-01T00:00:00.1234Z",
    "9999-12-31T23:00:00-02:00",
  ])("fails closed for inexact or invalid timestamp literal %s", (value) => {
    const queryPlan = buildPlan({
      operator: "eq",
      operands: [
        {
          operator: "timestamp",
          operands: [{ name: "request.resource.attr.createdAt" }],
        },
        { operator: "timestamp", operands: [{ value }] },
      ],
    });

    expect(() =>
      queryPlanToDrizzle({
        queryPlan,
        mapper: {
          "request.resource.attr.createdAt": {
            column: resources.aString,
            valueType: "timestamp",
          },
        },
      })
    ).toThrow(/RFC-3339|millisecond|instant range/);
  });

  test("accepts a timestamp literal whose excess fractional digits are zero", () => {
    const queryPlan = buildPlan({
      operator: "eq",
      operands: [
        {
          operator: "timestamp",
          operands: [{ name: "request.resource.attr.createdAt" }],
        },
        {
          operator: "timestamp",
          operands: [{ value: "2024-01-01T00:00:00.123000Z" }],
        },
      ],
    });

    expect(() =>
      queryPlanToDrizzle({
        queryPlan,
        mapper: {
          "request.resource.attr.createdAt": {
            column: resources.aString,
            valueType: "timestamp",
          },
        },
      })
    ).not.toThrow();
  });
});

describe("known-value collections (planner unroll cliff)", () => {
  // The planner unrolls exists/all over a known collection (e.g. a folded
  // principal attribute) into an or/and chain at <= 10 elements
  // (cerbos/cerbos#2570, #2817) and emits the lambda with a literal value-list
  // collection above that. These tests straddle the 10-item cliff so both wire
  // shapes stay exercised regardless of the PDP version behind the sidecar.
  describe("live plans across the 10-item threshold", () => {
    const buildTeams = (size: number): string[] => {
      const teams = ["string", "string3"];
      while (teams.length < size) {
        teams.push(`filler-${teams.length}`);
      }
      return teams;
    };

    const allowedIdsForPrincipal = async (
      action: string,
      principal: { id: string; roles: string[]; attr: Record<string, Value> }
    ): Promise<string[]> => {
      const response = await cerbos.checkResources({
        principal,
        resources: resourceAttributes.map((resource) => ({
          resource: { kind: "resource", id: resource.id, attr: resource },
          actions: [action],
        })),
      });
      return response.results
        .filter((result) => result.isAllowed(action) === true)
        .map((result) => result.resource.id)
        .sort();
    };

    // Supported PDPs are >= 0.54, where both macros unroll at <= 10 elements and
    // ship the value-list lambda above that. Pin the wire shape so each leg
    // provably exercises its side of the cliff — if a future planner moves the
    // threshold, this fails loudly instead of silently testing one shape only.
    const expectedShape: Record<string, { unrolled: string; macro: string }> = {
      "principal-exists": { unrolled: "or", macro: "exists" },
      "principal-all": { unrolled: "and", macro: "all" },
    };

    describe.each([9, 10, 11])("with %i-element principal collection", (size) => {
      test.each(["principal-exists", "principal-all"])(
        "produces results matching checkResources for %s",
        async (action) => {
          const principal = {
            id: "user1",
            roles: ["USER"],
            attr: { teams: buildTeams(size) },
          };
          const queryPlan = await cerbos.planResources({
            principal,
            resource: { kind: "resource" },
            action,
          });

          expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
          const condition = (
            queryPlan as { condition: PlanExpressionOperand }
          ).condition;
          const shape = expectedShape[action];
          expect(
            "operator" in condition ? condition.operator : undefined
          ).toEqual(size <= 10 ? shape?.unrolled : shape?.macro);

          const result = queryPlanToDrizzle({ queryPlan, mapper });
          const ids = selectIds(ensureFilter(result));
          expect(ids).toEqual(await allowedIdsForPrincipal(action, principal));
          // Sanity-check the oracle itself matched the intended rows.
          const teams = buildTeams(size);
          const expected = resourceAttributes
            .filter((r) =>
              action === "principal-exists"
                ? teams.includes(r.aString)
                : !teams.includes(r.aString)
            )
            .map((r) => r.id)
            .sort();
          expect(ids).toEqual(expected);
        }
      );
    });
  });

  describe("value-list lambda fold", () => {
    const valueListPlan = (
      operator: string,
      elements: Value[],
      body: PlanExpressionOperand,
      variable = "t",
      negated = false
    ): PlanResourcesResponse => {
      const macro = {
        operator,
        operands: [
          { value: elements },
          { operator: "lambda", operands: [body, { name: variable }] },
        ],
      } satisfies PlanExpressionOperand;
      const condition = negated
        ? ({ operator: "not", operands: [macro] } satisfies PlanExpressionOperand)
        : macro;
      return buildPlan(condition);
    };

    test("exists over a value list matches any element", () => {
      const result = queryPlanToDrizzle({
        queryPlan: valueListPlan("exists", ["string", "string3"], {
          operator: "eq",
          operands: [{ name: "request.resource.attr.aString" }, { name: "t" }],
        }),
        mapper,
      });
      expect(selectIds(ensureFilter(result))).toEqual([
        "resource1",
        "resource3",
      ]);
    });

    test("all over a value list requires every element", () => {
      const result = queryPlanToDrizzle({
        queryPlan: valueListPlan("all", ["string2"], {
          operator: "ne",
          operands: [{ name: "request.resource.attr.aString" }, { name: "t" }],
        }),
        mapper,
      });
      expect(selectIds(ensureFilter(result))).toEqual([
        "resource1",
        "resource3",
      ]);
    });

    test("negated exists over a value list preserves UNKNOWN as deny", () => {
      const result = queryPlanToDrizzle({
        queryPlan: valueListPlan(
          "exists",
          ["does-not-match"],
          {
            operator: "eq",
            operands: [
              { name: "request.resource.attr.aOptionalString" },
              { name: "t" },
            ],
          },
          "t",
          true
        ),
        mapper,
      });
      expect(selectIds(ensureFilter(result))).toEqual(["resource1"]);
    });

    test("negated all over a value list preserves UNKNOWN as deny", () => {
      const result = queryPlanToDrizzle({
        queryPlan: valueListPlan(
          "all",
          ["optionalString", "does-not-match"],
          {
            operator: "eq",
            operands: [
              { name: "request.resource.attr.aOptionalString" },
              { name: "t" },
            ],
          },
          "t",
          true
        ),
        mapper,
      });
      expect(selectIds(ensureFilter(result))).toEqual(["resource1"]);
    });

    test("empty value list keeps CEL identity semantics", () => {
      const body: PlanExpressionOperand = {
        operator: "eq",
        operands: [{ name: "request.resource.attr.aString" }, { name: "t" }],
      };

      // exists over [] is false; all over [] is true.
      expect(
        selectIds(
          ensureFilter(
            queryPlanToDrizzle({ queryPlan: valueListPlan("exists", [], body), mapper })
          )
        )
      ).toEqual([]);
      expect(
        selectIds(
          ensureFilter(
            queryPlanToDrizzle({ queryPlan: valueListPlan("all", [], body), mapper })
          )
        )
      ).toEqual(allResourceIds);
    });

    test("substitutes variable path references into element fields", () => {
      const result = queryPlanToDrizzle({
        queryPlan: valueListPlan(
          "exists",
          [{ name: "string" }, { name: "string3" }],
          {
            operator: "eq",
            operands: [
              { name: "request.resource.attr.aString" },
              { name: "t.name" },
            ],
          }
        ),
        mapper,
      });
      expect(selectIds(ensureFilter(result))).toEqual([
        "resource1",
        "resource3",
      ]);
    });

    test("throws when a variable path is missing on an element", () => {
      expect(() =>
        queryPlanToDrizzle({
          queryPlan: valueListPlan("exists", [{ name: "alpha" }], {
            operator: "eq",
            operands: [
              { name: "request.resource.attr.aString" },
              { name: "t.missing" },
            ],
          }),
          mapper,
        })
      ).toThrow('Cannot resolve "t.missing"');
    });

    test("throws for exists_one over a value list", () => {
      expect(() =>
        queryPlanToDrizzle({
          queryPlan: valueListPlan("exists_one", ["a"], {
            operator: "eq",
            operands: [{ name: "request.resource.attr.aString" }, { name: "t" }],
          }),
          mapper,
        })
      ).toThrow("'exists_one' over a literal collection value is not supported");
    });

    test("throws for a non-list collection value", () => {
      const plan = buildPlan({
        operator: "exists",
        operands: [
          { value: "not-a-list" },
          {
            operator: "lambda",
            operands: [
              {
                operator: "eq",
                operands: [
                  { name: "request.resource.attr.aString" },
                  { name: "t" },
                ],
              },
              { name: "t" },
            ],
          },
        ],
      } as PlanExpressionOperand);
      expect(() => queryPlanToDrizzle({ queryPlan: plan, mapper })).toThrow(
        "'exists' over a literal collection requires a list value"
      );
    });
  });
});

// cerbos/query-plan-adapters#302. Both NULL-column conventions produce the identical
// `eq(attr, null)` wire node, so the adapter cannot infer which one the caller uses. Under
// "omitted" a NULL column carries no attribute at all, CEL raises a missing-attribute error,
// and check() denies every row — an IS NULL filter would return precisely the rows the PDP
// refuses.
describe("nullAttributeRepresentation", () => {
  const nullEqPlan = buildPlan({
    operator: "eq",
    operands: [
      { name: "request.resource.attr.aOptionalString" },
      { value: null },
    ],
  } as PlanExpressionOperand);

  const nullNePlan = buildPlan({
    operator: "ne",
    operands: [
      { name: "request.resource.attr.aOptionalString" },
      { value: null },
    ],
  } as PlanExpressionOperand);

  test('"explicit" is the default and keeps the IS NULL translation', () => {
    const withDefault = ensureFilter(
      queryPlanToDrizzle({ queryPlan: nullEqPlan, mapper })
    );
    const withExplicit = ensureFilter(
      queryPlanToDrizzle({
        queryPlan: nullEqPlan,
        mapper,
        nullAttributeRepresentation: "explicit",
      })
    );

    expect(db.select().from(resources).where(withDefault).toSQL().sql).toEqual(
      db.select().from(resources).where(withExplicit).toSQL().sql
    );
    expect(db.select().from(resources).where(withDefault).toSQL().sql).toContain(
      "is null"
    );
  });

  test('"omitted" rejects eq against a null operand', () => {
    expect(() =>
      queryPlanToDrizzle({
        queryPlan: nullEqPlan,
        mapper,
        nullAttributeRepresentation: "omitted",
      })
    ).toThrow(/missing-attribute error/);
  });

  // Conservatively rejected too: `ne` alone is aligned under "omitted", but negation is applied
  // by wrapping the built condition, so a leaf cannot see whether an enclosing `not` will flip
  // IS NOT NULL back into a NULL-selecting predicate.
  test('"omitted" rejects ne against a null operand', () => {
    expect(() =>
      queryPlanToDrizzle({
        queryPlan: nullNePlan,
        mapper,
        nullAttributeRepresentation: "omitted",
      })
    ).toThrow(/missing-attribute error/);
  });

  test('"omitted" rejects a null element in an in-list', () => {
    const plan = buildPlan({
      operator: "in",
      operands: [
        { name: "request.resource.attr.aOptionalString" },
        { value: ["x", null] },
      ],
    } as PlanExpressionOperand);

    expect(() =>
      queryPlanToDrizzle({
        queryPlan: plan,
        mapper,
        nullAttributeRepresentation: "omitted",
      })
    ).toThrow(/null element in an `in` list/);
  });

  test('"omitted" leaves null-free comparisons untouched', () => {
    const plan = buildPlan({
      operator: "eq",
      operands: [
        { name: "request.resource.attr.aOptionalString" },
        { value: "x" },
      ],
    } as PlanExpressionOperand);

    const filter = ensureFilter(
      queryPlanToDrizzle({
        queryPlan: plan,
        mapper,
        nullAttributeRepresentation: "omitted",
      })
    );

    expect(db.select().from(resources).where(filter).toSQL().sql).toContain(
      "="
    );
  });
});

// cerbos/query-plan-adapters#308. The per-attribute half of the same option. A call-level flag
// cannot express a policy suite that mixes the two conventions — the same column mapped twice,
// sent explicitly under one attribute name and omitted under another — so the declaration lives
// on the mapper entry and the call-level option is only its default.
describe("per-attribute nullAttributeRepresentation", () => {
  const explicitMapper: Record<string, MapperEntry> = {
    ...mapper,
    "request.resource.attr.owner": {
      column: resources.aOptionalString,
      nullAttributeRepresentation: "explicit",
    },
    "request.resource.attr.coOwner": {
      column: resources.aString,
      nullAttributeRepresentation: "explicit",
    },
  };

  const sqlFor = (
    expression: PlanExpressionOperand,
    withMapper: Record<string, MapperEntry> = explicitMapper
  ) =>
    db
      .select()
      .from(resources)
      .where(
        ensureFilter(
          queryPlanToDrizzle({
            queryPlan: buildPlan(expression),
            mapper: withMapper,
          })
        )
      )
      .toSQL().sql;

  const comparison = (operator: string, name: string, value: unknown) =>
    ({
      operator,
      operands: [{ name }, { value }],
    }) as PlanExpressionOperand;

  // A null VALUE is not equal to "x", so CEL returns a definite FALSE and its negation a
  // definite TRUE. `col <> 'x'` is UNKNOWN instead, which excludes the row under BOTH
  // polarities — the row the PDP allows never comes back.
  test("renders ne against a constant so a NULL row is included", () => {
    // `not (col is not null and col = ?)` — definite FALSE for a NULL column inside the NOT,
    // hence definite TRUE outside it, where a bare `col <> ?` would have been UNKNOWN.
    expect(
      sqlFor(comparison("ne", "request.resource.attr.owner", "x"))
    ).toContain('not ("resources"."a_optional_string" is not null and');
  });

  test("renders eq against a constant so it is definite under a negation", () => {
    expect(
      sqlFor(comparison("eq", "request.resource.attr.owner", "x"))
    ).toContain('"a_optional_string" is not null');
  });

  // The equality family only. An ordering comparison against a null receiver is a no-overload
  // error in CEL, which denies under both polarities — exactly what UNKNOWN already does — so
  // `gt` must keep propagating it rather than being made definite.
  test("leaves ordering comparisons propagating UNKNOWN", () => {
    expect(
      sqlFor(comparison("gt", "request.resource.attr.owner", "x"))
    ).not.toContain("is null");
  });

  test("makes membership without a null element definite", () => {
    expect(
      sqlFor(comparison("in", "request.resource.attr.owner", ["x", "y"]))
    ).toContain('"a_optional_string" is not null');
  });

  test("matches two explicit nulls in a field-to-field equality", () => {
    const sql = sqlFor({
      operator: "eq",
      operands: [
        { name: "request.resource.attr.owner" },
        { name: "request.resource.attr.coOwner" },
      ],
    } as PlanExpressionOperand);

    expect(sql).toContain('"a_optional_string" is null');
    expect(sql).toContain('"a_string" is null');
  });

  // An entry that declares nothing keeps the historical rendering, so declaring the convention
  // on one attribute cannot change the SQL emitted for any other mapping.
  test("leaves an undeclared entry untouched", () => {
    expect(
      sqlFor(comparison("ne", "request.resource.attr.aOptionalString", "x"))
    ).not.toContain("is null");
  });

  // The entry-level declaration overrides the call-level default in both directions, which is
  // the whole point: one call, two conventions.
  test('an entry declaring "omitted" rejects a null operand under the "explicit" default', () => {
    expect(() =>
      queryPlanToDrizzle({
        queryPlan: buildPlan(
          comparison("eq", "request.resource.attr.omitted", null)
        ),
        mapper: {
          ...mapper,
          "request.resource.attr.omitted": {
            column: resources.aOptionalString,
            nullAttributeRepresentation: "omitted",
          },
        },
        nullAttributeRepresentation: "explicit",
      })
    ).toThrow(/missing-attribute error/);
  });

  test('an entry declaring "explicit" still translates a null operand under the "omitted" default', () => {
    const filter = ensureFilter(
      queryPlanToDrizzle({
        queryPlan: buildPlan(
          comparison("eq", "request.resource.attr.owner", null)
        ),
        mapper: explicitMapper,
        nullAttributeRepresentation: "omitted",
      })
    );

    expect(db.select().from(resources).where(filter).toSQL().sql).toContain(
      '"a_optional_string" is null'
    );
  });
});

// The class 1 mapping-hazard contract (README, "Mapping hazards"): the adapter reads the relation
// table directly, so a soft-delete flag, tenant column or subtype discriminator the application
// applies to its own reads does NOT reach the generated EXISTS unless the caller declares it.
// `subqueryFilter` is that declaration (cerbos/query-plan-adapters#323).
describe("relation subqueryFilter", () => {
  const hazardResources = sqliteTable("hazard_resources", {
    id: text("id").primaryKey(),
  });
  const hazardTags = sqliteTable("hazard_tags", {
    id: text("id").primaryKey(),
    resourceId: text("resource_id").notNull(),
    name: text("name").notNull(),
    deleted: integer("deleted", { mode: "boolean" }).notNull(),
  });

  const relation = (subqueryFilter?: ReturnType<typeof eq>) => ({
    relation: {
      type: "many" as const,
      table: hazardTags,
      sourceColumn: hazardResources.id,
      targetColumn: hazardTags.resourceId,
      field: hazardTags.name,
      fields: { name: hazardTags.name },
      ...(subqueryFilter ? { subqueryFilter } : {}),
    },
  });

  const mapperFor = (
    subqueryFilter?: ReturnType<typeof eq>
  ): Record<string, MapperEntry> => ({
    "request.resource.attr.tags": relation(subqueryFilter),
  });

  const VISIBLE_ONLY = eq(hazardTags.deleted, false);

  const existsPlan = buildPlan({
    operator: "exists",
    operands: [
      { name: "request.resource.attr.tags" },
      {
        operator: "lambda",
        operands: [
          {
            operator: "eq",
            operands: [{ name: "t.name" }, { value: "secret" }],
          },
          { name: "t" },
        ],
      },
    ],
  } as PlanExpressionOperand);

  const allPlan = buildPlan({
    operator: "all",
    operands: [
      { name: "request.resource.attr.tags" },
      {
        operator: "lambda",
        operands: [
          {
            operator: "eq",
            operands: [{ name: "t.name" }, { value: "public" }],
          },
          { name: "t" },
        ],
      },
    ],
  } as PlanExpressionOperand);

  const sizePlan = buildPlan({
    operator: "eq",
    operands: [
      {
        operator: "size",
        operands: [{ name: "request.resource.attr.tags" }],
      },
      { value: 1 },
    ],
  } as PlanExpressionOperand);

  const idsFor = (
    plan: PlanResourcesResponse,
    subqueryFilter?: ReturnType<typeof eq>
  ): string[] =>
    db
      .select({ id: hazardResources.id })
      .from(hazardResources)
      .where(ensureFilter(queryPlanToDrizzle({ queryPlan: plan, mapper: mapperFor(subqueryFilter) })))
      .all()
      .map((row) => row.id)
      .sort();

  beforeAll(() => {
    sqlite.exec(`
      CREATE TABLE hazard_resources (id TEXT PRIMARY KEY);
      CREATE TABLE hazard_tags (
        id TEXT PRIMARY KEY,
        resource_id TEXT NOT NULL,
        name TEXT NOT NULL,
        deleted INTEGER NOT NULL
      );
      INSERT INTO hazard_resources (id) VALUES ('r1'), ('r2');
      -- r1's application view is ['public']; the 'secret' row is soft-deleted, so the
      -- application never serialised it into the resource attributes.
      INSERT INTO hazard_tags (id, resource_id, name, deleted) VALUES
        ('t1', 'r1', 'public', 0),
        ('t2', 'r1', 'secret', 1),
        ('t3', 'r2', 'secret', 0);
    `);
  });

  test("declared: exists() sees only the rows the application serialised", () => {
    // Undeclared, r1 matches on a row its own reads hide — the over-grant #314 catalogues.
    expect(idsFor(existsPlan)).toEqual(["r1", "r2"]);
    expect(idsFor(existsPlan, VISIBLE_ONLY)).toEqual(["r2"]);
  });

  test("declared: all() narrows the scan, not the result", () => {
    // all() compiles to a NOT EXISTS over a false witness, so a declaration applied around the
    // subquery instead of inside it would leave the hidden 'secret' row denying r1.
    expect(idsFor(allPlan)).toEqual([]);
    expect(idsFor(allPlan, VISIBLE_ONLY)).toEqual(["r1"]);
  });

  test("declared: size() counts only the rows the application serialised", () => {
    expect(idsFor(sizePlan)).toEqual(["r2"]);
    expect(idsFor(sizePlan, VISIBLE_ONLY)).toEqual(["r1", "r2"]);
  });

  test("undeclared: the emitted SQL is byte-identical to before the field existed", () => {
    // The non-breaking guarantee. Silence must not add a clause, and must not warn.
    const withoutField = queryPlanToDrizzle({
      queryPlan: existsPlan,
      mapper: {
        "request.resource.attr.tags": {
          relation: {
            type: "many",
            table: hazardTags,
            sourceColumn: hazardResources.id,
            targetColumn: hazardTags.resourceId,
            field: hazardTags.name,
            fields: { name: hazardTags.name },
          },
        },
      },
    });
    const withUndefined = queryPlanToDrizzle({
      queryPlan: existsPlan,
      mapper: mapperFor(undefined),
    });

    const render = (result: QueryPlanToDrizzleResult) =>
      db
        .select()
        .from(hazardResources)
        .where(ensureFilter(result))
        .toSQL();

    expect(render(withUndefined)).toEqual(render(withoutField));
    expect(render(withoutField).sql).not.toContain("deleted");
  });
});
