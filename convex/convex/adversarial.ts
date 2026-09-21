import type { Expression, FilterBuilder } from "convex/server";
import { v } from "convex/values";

import { PlanKind, queryPlanToConvex } from "../src/index";
// The mapper lives in its own module because `src/translator.test.ts` reads it too, and that
// suite must run without the `_generated` API this file imports. See adversarialMapper.ts.
import { MAPPERS } from "./adversarialMapper";
import { mutation, query } from "./_generated/server";
import type { DataModel } from "./_generated/dataModel";
import { executionPathOf, isPlanResourcesResponse } from "./planExecution";
import { adversarialDocument } from "./schema";

export const insert = mutation({
  args: adversarialDocument,
  handler: async (ctx, args) => ctx.db.insert("adversarial", args),
});

/**
 * The two hops of the to-one chain, per resource id, read back out of the stored documents. The
 * relation carries no corpus action yet, so this is what keeps the fixture from rotting.
 */
export const parentChain = query({
  args: {},
  handler: async (ctx) => {
    const docs = await ctx.db.query("adversarial").collect();
    return docs.map((doc) => ({
      id: doc.id,
      parent: doc.parent?.aString ?? null,
      inner: doc.parent?.inner?.aString ?? null,
    }));
  },
});

export const deleteAll = mutation({
  args: {},
  handler: async (ctx) => {
    const docs = await ctx.db.query("adversarial").collect();
    for (const doc of docs) {
      await ctx.db.delete(doc._id);
    }
  },
});

export const executePlan = query({
  args: {
    queryPlan: v.any(),
    // #302: the conformance harness runs the `nullRepresentationOmitted` actions through the
    // same entry point with the option flipped, so it has to cross the Convex boundary.
    nullAttributeRepresentation: v.optional(
      v.union(v.literal("explicit"), v.literal("omitted")),
    ),
    // #327: the pushdown leg replays the whole corpus with `nullable` demoted where the document
    // shape allows it, so the mapper choice has to cross the Convex boundary too.
    mapper: v.optional(v.union(v.literal("default"), v.literal("pushdown"))),
  },
  handler: async (ctx, args) => {
    const queryPlan: unknown = args.queryPlan;
    if (!isPlanResourcesResponse(queryPlan)) {
      throw new Error("Invalid Cerbos query plan");
    }

    const translated = queryPlanToConvex<
      FilterBuilder<DataModel["adversarial"]>,
      Expression<boolean>
    >({
      queryPlan,
      mapper: MAPPERS[args.mapper ?? "default"],
      allowPostFilter: true,
      nullAttributeRepresentation:
        args.nullAttributeRepresentation ?? "explicit",
    });

    // Reported alongside the ids so the harness can assert WHICH half answered: a pushdown leg
    // that quietly fell back to the post-filter would return the same ids (#327).
    const execution = executionPathOf(translated);
    if (translated.kind === PlanKind.ALWAYS_DENIED)
      return { ids: [], execution };

    let queryBuilder = ctx.db.query("adversarial");
    if (translated.kind === PlanKind.CONDITIONAL && translated.path !== "post") {
      queryBuilder = queryBuilder.filter(translated.filter);
    }
    const docs = await queryBuilder.collect();
    const ids = docs
      .filter((doc) =>
        translated.kind === PlanKind.CONDITIONAL && translated.path !== "db"
          ? translated.postFilter({ ...doc })
          : true,
      )
      .map((doc) => doc.id)
      .sort();
    return { ids, execution };
  },
});
