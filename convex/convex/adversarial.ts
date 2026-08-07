import type { Expression, FilterBuilder } from "convex/server";
import { v } from "convex/values";

import { PlanKind, queryPlanToConvex } from "../src/index";
import type { Mapper, MapperConfig } from "../src/index";
import { mutation, query } from "./_generated/server";
import type { DataModel } from "./_generated/dataModel";
import { executionPathOf, isPlanResourcesResponse } from "./planExecution";

const tag = v.object({ id: v.string(), name: v.optional(v.string()) });
const label = v.object({ name: v.optional(v.string()) });
const subCategory = v.object({
  name: v.string(),
  labels: v.array(label),
});
const category = v.object({
  name: v.string(),
  subCategories: v.array(subCategory),
});
const mainCategory = v.object({
  name: v.string(),
  subCategories: v.array(v.object({ name: v.string() })),
  subNames: v.array(v.string()),
});

const document = {
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
  tagNames: v.array(v.union(v.string(), v.null())),
  obj: v.object({ inner: v.string() }),
  tags: v.array(tag),
  categories: v.array(category),
  mainCategory: v.optional(mainCategory),
};

// Exported so the adversarial throw suite can invoke translation with the exact mapper the
// backend uses — a duplicated copy would be a projection that can drift.
//
// Typed as the record arm of `Mapper` rather than as `Mapper` itself, so `PUSHDOWN_MAPPER` below
// can derive from it without asserting away the function arm.
export const MAPPER: Record<string, MapperConfig> = {
  "request.resource.attr.aBool": { field: "aBool" },
  "request.resource.attr.aString": { field: "aString" },
  "request.resource.attr.aNumber": { field: "aNumber" },
  "request.resource.attr.aDouble": { field: "aDouble", nullable: true },
  "request.resource.attr.aOptionalString": {
    field: "aOptionalString",
    nullable: true,
  },
  "request.resource.attr.createdBy": { field: "createdBy" },
  "request.resource.attr.createdAt": { field: "createdAt", nullable: true },
  "request.resource.attr.scope": { field: "scope", nullable: true },
  "request.resource.attr.owner": { field: "owner", nullable: true },
  "request.resource.attr.tagNames": { field: "tagNames" },
  "request.resource.attr.obj.inner": { field: "obj.inner" },
  "request.resource.attr.tags": { field: "tags" },
  "request.resource.attr.categories": { field: "categories" },
  "request.resource.attr.mainCategory": {
    field: "mainCategory",
    nullable: true,
  },
  "request.resource.attr.mainCategory.subCategories": {
    field: "mainCategory.subCategories",
    nullable: true,
  },
  "request.resource.attr.mainCategory.subNames": {
    field: "mainCategory.subNames",
    nullable: true,
  },
};

/**
 * Fields `MAPPER` declares `nullable` that the seeded documents nonetheless ALWAYS carry.
 *
 * `nullable: true` means "this path may be absent from the document", and `canPushToDb` refuses to
 * push any comparison touching such a field: an absent path has CEL missing-attribute semantics
 * that a Convex comparison cannot reproduce. `owner` is the one nullable field whose value can be
 * NULL while the key is still present — the table declares it `v.union(v.string(), v.null())`, not
 * `v.optional(...)` — so the flag is buying nothing there and costs the whole null-comparison
 * family its push-down.
 *
 * Demoting it is a statement about the DOCUMENT SHAPE, not about the plan, so the harness asserts
 * the key is present on every seeded document before trusting this list
 * (cerbos/query-plan-adapters#327).
 */
export const PUSHDOWN_DEMOTED_FIELDS = ["owner"] as const;

/**
 * The same mapper with `nullable` cleared on {@link PUSHDOWN_DEMOTED_FIELDS}, so the comparison
 * shapes over those fields are decided by Convex's filter engine instead of the adapter's
 * in-memory post-filter. That moves the null-comparison family across the boundary, which is
 * where Convex's own `q.eq(field, null)` semantics live.
 *
 * How much of the corpus each mapper actually hands to the engine is pinned by the harness and
 * quoted in the adapter README; it is deliberately not restated here.
 */
export const PUSHDOWN_MAPPER: Record<string, MapperConfig> = Object.fromEntries(
  Object.entries(MAPPER).map(([path, config]) =>
    PUSHDOWN_DEMOTED_FIELDS.some((field) => field === config.field)
      ? [path, { ...config, nullable: false }]
      : [path, config],
  ),
);

/** Which mapper `executePlan` should translate with. */
export type MapperVariant = "default" | "pushdown";

const MAPPERS: Record<MapperVariant, Mapper> = {
  default: MAPPER,
  pushdown: PUSHDOWN_MAPPER,
};

export const insert = mutation({
  args: document,
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
      nullAttributeRepresentation: args.nullAttributeRepresentation ?? "explicit",
    });

    // Reported alongside the ids so the harness can assert WHICH half answered: a pushdown leg
    // that quietly fell back to the post-filter would return the same ids (#327).
    const execution = executionPathOf(translated);
    if (translated.kind === PlanKind.ALWAYS_DENIED) return { ids: [], execution };

    let queryBuilder = ctx.db.query("adversarial");
    if (translated.kind === PlanKind.CONDITIONAL && translated.filter) {
      queryBuilder = queryBuilder.filter(translated.filter);
    }
    const docs = await queryBuilder.collect();
    const ids = docs
      .filter((doc) =>
        translated.postFilter ? translated.postFilter({ ...doc }) : true,
      )
      .map((doc) => doc.id)
      .sort();
    return { ids, execution };
  },
});
