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

// The corpus's one real to-one relation. A document store has no join, so both levels are nested
// objects — but the SHAPE is the same to-one chain every other store carries, and an absent level
// is a missing path here exactly as it is a missing row there.
const relationLevel = {
  aBool: v.boolean(),
  aString: v.string(),
  aNumber: v.number(),
  aOptionalString: v.optional(v.string()),
};
const parent = v.object({
  ...relationLevel,
  inner: v.optional(v.object(relationLevel)),
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
  coOwner: v.union(v.string(), v.null()),
  tagNames: v.array(v.union(v.string(), v.null())),
  obj: v.object({ inner: v.string() }),
  tags: v.array(tag),
  categories: v.array(category),
  mainCategory: v.optional(mainCategory),
  parent: v.optional(parent),
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
  // `coOwner` is the explicit-null alias of the `scope` field, the second half of
  // `null-value-f2f`: `scope` itself is omitted when NULL, so the corpus carries the same
  // value under both conventions (cerbos/query-plan-adapters#308).
  "request.resource.attr.coOwner": { field: "coOwner", nullable: true },
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
  // The corpus's real to-one chain is seeded in the `parent` object above and mirrored on the
  // check side, but carries no mapping yet: nothing references it until the join shapes land
  // (#375), and an unexercised mapping is a declaration no assertion holds to anything. This is
  // the expand half of cerbos/query-plan-adapters#372's expand-contract.
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
