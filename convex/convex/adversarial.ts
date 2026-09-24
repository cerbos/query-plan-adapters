import type { Expression, FilterBuilder } from "convex/server";
import { v } from "convex/values";

import { PlanKind, queryPlanToConvex } from "../src/index";
// The mapper lives in its own module because `src/translator.test.ts` reads it too, and that
// suite must run without the `_generated` API this file imports. See adversarialMapper.ts.
import { MAPPER } from "./adversarialMapper";
import { mutation, query } from "./_generated/server";
import type { DataModel } from "./_generated/dataModel";
import { executionPathOf, isPlanResourcesResponse } from "./planExecution";
import { adversarialDocument } from "./schema";

export const insert = mutation({
  args: adversarialDocument,
  handler: async (ctx, args) => ctx.db.insert("adversarial", args),
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

/** Every stored document, `_id` and `_creationTime` stripped, for the dataset check. */
export const listAll = query({
  args: {},
  handler: async (ctx) => {
    const docs = await ctx.db.query("adversarial").collect();
    return docs.map(({ _id, _creationTime, ...doc }) => doc);
  },
});

export const executePlan = query({
  args: { queryPlan: v.any() },
  handler: async (ctx, args) => {
    const queryPlan: unknown = args.queryPlan;
    if (!isPlanResourcesResponse(queryPlan)) {
      throw new Error("Invalid Cerbos query plan");
    }

    const translated = queryPlanToConvex<
      FilterBuilder<DataModel["adversarial"]>,
      Expression<boolean>
    >({ queryPlan, mapper: MAPPER, allowPostFilter: true });

    // Reported alongside the ids so the harness can say how much of the corpus Convex's own
    // filter engine decided, rather than the adapter's in-memory post-filter.
    const execution = executionPathOf(translated);
    if (translated.kind === PlanKind.ALWAYS_DENIED)
      return { ids: [], execution };

    let queryBuilder = ctx.db.query("adversarial");
    if (
      translated.kind === PlanKind.CONDITIONAL &&
      translated.path !== "post"
    ) {
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
