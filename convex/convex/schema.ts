import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

export default defineSchema({
  resources: defineTable({
    key: v.string(),
    aBool: v.boolean(),
    aNumber: v.number(),
    aString: v.string(),
    aOptionalString: v.optional(v.string()),
    nested: v.object({
      aBool: v.boolean(),
      aNumber: v.number(),
      aString: v.string(),
    }),
  }),
  adversarial: defineTable({
    id: v.string(),
    aBool: v.boolean(),
    aString: v.string(),
    aNumber: v.number(),
    aDouble: v.optional(v.number()),
    aOptionalString: v.optional(v.string()),
    createdBy: v.string(),
    createdAt: v.optional(v.string()),
    scope: v.optional(v.string()),
    owner: v.union(v.string(), v.null()),
    coOwner: v.union(v.string(), v.null()),
    tagNames: v.array(v.union(v.string(), v.null())),
    obj: v.object({ inner: v.string() }),
    tags: v.array(
      v.object({
        id: v.string(),
        name: v.optional(v.string()),
      }),
    ),
    categories: v.array(
      v.object({
        name: v.string(),
        subCategories: v.array(
          v.object({
            name: v.string(),
            labels: v.array(v.object({ name: v.optional(v.string()) })),
          }),
        ),
      }),
    ),
    mainCategory: v.optional(
      v.object({
        name: v.string(),
        subCategories: v.array(v.object({ name: v.string() })),
        subNames: v.array(v.string()),
      }),
    ),
  }),
});
