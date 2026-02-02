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
});
