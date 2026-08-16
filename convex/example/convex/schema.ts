/**
 * The demo domain's single resource kind, as a Convex consumer would model it: one table of flat
 * scalar fields and no relations. The richer schema the adapter is PROVED against lives in
 * ../../convex/schema.ts.
 *
 * `id` is the demo domain's own identifier and is not Convex's `_id`. Every example reports the
 * ids in demo/seeds.json, and a Convex document id is minted by the backend on insert — so the
 * two cannot be the same field, and the filter shapes below read the one the corpus names.
 */
import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

/**
 * Exported because `documents.ts` validates the seeding mutation's argument against the same
 * fields. One declaration: a table and a mutation that disagreed about a field would fail at the
 * schema check on insert, which is late and reads as a seeding bug.
 */
export const documentFields = {
  id: v.string(),
  ownerId: v.string(),
  public: v.boolean(),
  // Never referenced by policy. `region` and `archived` are the application's own fields, and
  // ANDing them with the adapter's filter is usage shape 5.
  region: v.string(),
  archived: v.boolean(),
};

export default defineSchema({
  documents: defineTable(documentFields),
});
