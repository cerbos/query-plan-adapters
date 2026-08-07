import type { Expression, FilterBuilder } from "convex/server";
import { v } from "convex/values";

import { PlanKind, queryPlanToConvex } from "../src/index";
import type { Mapper } from "../src/index";
import { mutation, query } from "./_generated/server";
import type { DataModel } from "./_generated/dataModel";
import { executionPathOf, isPlanResourcesResponse } from "./planExecution";

export const insert = mutation({
  args: {
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
  },
  handler: async (ctx, args) => {
    return await ctx.db.insert("resources", args);
  },
});

export const deleteAll = mutation({
  args: {},
  handler: async (ctx) => {
    const docs = await ctx.db.query("resources").collect();
    for (const doc of docs) {
      await ctx.db.delete(doc._id);
    }
  },
});

// Exported so the integration suite translates with the exact mapper this backend executes —
// a duplicated copy in the test would be a projection that can drift.
export const MAPPER: Mapper = {
  "request.resource.attr.aBool": { field: "aBool" },
  "request.resource.attr.aNumber": { field: "aNumber" },
  "request.resource.attr.aString": { field: "aString" },
  "request.resource.attr.aOptionalString": {
    field: "aOptionalString",
    nullable: true,
  },
  "request.resource.attr.nested.aBool": { field: "nested.aBool" },
  "request.resource.attr.nested.aNumber": { field: "nested.aNumber" },
  "request.resource.attr.nested.aString": { field: "nested.aString" },
};

/**
 * Runs a Cerbos query plan against the `resources` table THROUGH THE ADAPTER.
 *
 * This used to be a `filterType`/`filterField`/`filterValue` switch that rebuilt each filter by
 * hand, which meant the integration suite proved Convex's filter API worked and never executed a
 * single translated filter (cerbos/query-plan-adapters#327). The plan goes in, the adapter's
 * `filter`/`postFilter` pair comes out, and the keys it selects are what the suite compares
 * against `check()`.
 */
export const executePlan = query({
  args: { queryPlan: v.any() },
  handler: async (ctx, args) => {
    const queryPlan: unknown = args.queryPlan;
    if (!isPlanResourcesResponse(queryPlan)) {
      throw new Error("Invalid Cerbos query plan");
    }

    const translated = queryPlanToConvex<
      FilterBuilder<DataModel["resources"]>,
      Expression<boolean>
    >({ queryPlan, mapper: MAPPER, allowPostFilter: true });

    const execution = executionPathOf(translated);
    if (translated.kind === PlanKind.ALWAYS_DENIED) return { keys: [], execution };

    let queryBuilder = ctx.db.query("resources");
    if (translated.kind === PlanKind.CONDITIONAL && translated.filter) {
      queryBuilder = queryBuilder.filter(translated.filter);
    }
    const docs = await queryBuilder.collect();
    const keys = docs
      .filter((doc) =>
        translated.postFilter ? translated.postFilter({ ...doc }) : true,
      )
      .map((doc) => doc.key)
      .sort();
    return { keys, execution };
  },
});
