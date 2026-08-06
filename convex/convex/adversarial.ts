import type {
  PlanExpressionOperand,
  PlanResourcesResponse,
} from "@cerbos/core";
import type { Expression, FilterBuilder } from "convex/server";
import { v } from "convex/values";

import { PlanKind, queryPlanToConvex } from "../src/index";
import type { Mapper } from "../src/index";
import { mutation, query } from "./_generated/server";
import type { DataModel } from "./_generated/dataModel";

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
export const MAPPER: Mapper = {
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

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isPlanOperand = (value: unknown): value is PlanExpressionOperand => {
  if (!isRecord(value)) return false;
  if (typeof value["operator"] === "string") {
    const operands = value["operands"];
    return Array.isArray(operands) && operands.every(isPlanOperand);
  }
  if (typeof value["name"] === "string") return true;
  return Object.prototype.hasOwnProperty.call(value, "value");
};

const isPlanResourcesResponse = (
  value: unknown,
): value is PlanResourcesResponse => {
  if (!isRecord(value)) return false;
  const kind = value["kind"];
  if (kind === PlanKind.ALWAYS_ALLOWED || kind === PlanKind.ALWAYS_DENIED) {
    return true;
  }
  return kind === PlanKind.CONDITIONAL && isPlanOperand(value["condition"]);
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
      mapper: MAPPER,
      allowPostFilter: true,
      nullAttributeRepresentation: args.nullAttributeRepresentation ?? "explicit",
    });

    if (translated.kind === PlanKind.ALWAYS_DENIED) return [];

    let queryBuilder = ctx.db.query("adversarial");
    if (translated.kind === PlanKind.CONDITIONAL && translated.filter) {
      queryBuilder = queryBuilder.filter(translated.filter);
    }
    const docs = await queryBuilder.collect();
    return docs
      .filter((doc) =>
        translated.postFilter ? translated.postFilter({ ...doc }) : true,
      )
      .map((doc) => doc.id)
      .sort();
  },
});
