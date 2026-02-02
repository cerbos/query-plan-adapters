import { mutation, query } from "./_generated/server";
import { v } from "convex/values";

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

const filterValue = v.union(v.string(), v.number(), v.boolean());

export const filteredQuery = query({
  args: {
    filterType: v.string(),
    filterField: v.optional(v.string()),
    filterValue: v.optional(filterValue),
    filterValues: v.optional(v.array(filterValue)),
    filterField2: v.optional(v.string()),
    filterValue2: v.optional(filterValue),
  },
  handler: async (ctx, args) => {
    const { filterType, filterField, filterValue, filterValues, filterField2, filterValue2 } = args;

    let q = ctx.db.query("resources");

    switch (filterType) {
      case "eq":
        return await q
          .filter((f) => f.eq(f.field(filterField!), filterValue))
          .collect();
      case "neq":
        return await q
          .filter((f) => f.neq(f.field(filterField!), filterValue))
          .collect();
      case "gt":
        return await q
          .filter((f) => f.gt(f.field(filterField!), filterValue))
          .collect();
      case "gte":
        return await q
          .filter((f) => f.gte(f.field(filterField!), filterValue))
          .collect();
      case "lt":
        return await q
          .filter((f) => f.lt(f.field(filterField!), filterValue))
          .collect();
      case "lte":
        return await q
          .filter((f) => f.lte(f.field(filterField!), filterValue))
          .collect();
      case "not":
        return await q
          .filter((f) => f.not(f.eq(f.field(filterField!), filterValue)))
          .collect();
      case "and":
        return await q
          .filter((f) =>
            f.and(
              f.eq(f.field(filterField!), filterValue),
              f.neq(f.field(filterField2!), filterValue2),
            ),
          )
          .collect();
      case "or":
        return await q
          .filter((f) =>
            f.or(
              f.eq(f.field(filterField!), filterValue),
              f.neq(f.field(filterField2!), filterValue2),
            ),
          )
          .collect();
      case "in":
        if (!filterValues || filterValues.length === 0) {
          return [];
        }
        return await q
          .filter((f) =>
            f.or(
              ...filterValues.map((v) => f.eq(f.field(filterField!), v)),
            ),
          )
          .collect();
      case "isSet":
        return await q
          .filter((f) => f.neq(f.field(filterField!), undefined))
          .collect();
      default:
        return await q.collect();
    }
  },
});
