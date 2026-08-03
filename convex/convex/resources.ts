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
const resourceField = v.union(
  v.literal("key"),
  v.literal("aBool"),
  v.literal("aNumber"),
  v.literal("aString"),
  v.literal("aOptionalString"),
  v.literal("nested.aBool"),
  v.literal("nested.aNumber"),
  v.literal("nested.aString"),
);

const requireArgument = <T>(value: T | undefined, name: string): T => {
  if (value === undefined) {
    throw new Error(`${name} is required`);
  }
  return value;
};

export const filteredQuery = query({
  args: {
    filterType: v.string(),
    filterField: v.optional(resourceField),
    filterValue: v.optional(filterValue),
    filterValues: v.optional(v.array(filterValue)),
    filterField2: v.optional(resourceField),
    filterValue2: v.optional(filterValue),
  },
  handler: async (ctx, args) => {
    const { filterType, filterField, filterValue, filterValues, filterField2, filterValue2 } = args;

    let q = ctx.db.query("resources");

    switch (filterType) {
      case "eq":
        return await q
          .filter((f) =>
            f.eq(
              f.field(requireArgument(filterField, "filterField")),
              requireArgument(filterValue, "filterValue"),
            ),
          )
          .collect();
      case "neq":
        return await q
          .filter((f) =>
            f.neq(
              f.field(requireArgument(filterField, "filterField")),
              requireArgument(filterValue, "filterValue"),
            ),
          )
          .collect();
      case "gt":
        return await q
          .filter((f) =>
            f.gt(
              f.field(requireArgument(filterField, "filterField")),
              requireArgument(filterValue, "filterValue"),
            ),
          )
          .collect();
      case "gte":
        return await q
          .filter((f) =>
            f.gte(
              f.field(requireArgument(filterField, "filterField")),
              requireArgument(filterValue, "filterValue"),
            ),
          )
          .collect();
      case "lt":
        return await q
          .filter((f) =>
            f.lt(
              f.field(requireArgument(filterField, "filterField")),
              requireArgument(filterValue, "filterValue"),
            ),
          )
          .collect();
      case "lte":
        return await q
          .filter((f) =>
            f.lte(
              f.field(requireArgument(filterField, "filterField")),
              requireArgument(filterValue, "filterValue"),
            ),
          )
          .collect();
      case "not":
        return await q
          .filter((f) =>
            f.not(
              f.eq(
                f.field(requireArgument(filterField, "filterField")),
                requireArgument(filterValue, "filterValue"),
              ),
            ),
          )
          .collect();
      case "and":
        return await q
          .filter((f) =>
            f.and(
              f.eq(
                f.field(requireArgument(filterField, "filterField")),
                requireArgument(filterValue, "filterValue"),
              ),
              f.neq(
                f.field(requireArgument(filterField2, "filterField2")),
                requireArgument(filterValue2, "filterValue2"),
              ),
            ),
          )
          .collect();
      case "or":
        return await q
          .filter((f) =>
            f.or(
              f.eq(
                f.field(requireArgument(filterField, "filterField")),
                requireArgument(filterValue, "filterValue"),
              ),
              f.neq(
                f.field(requireArgument(filterField2, "filterField2")),
                requireArgument(filterValue2, "filterValue2"),
              ),
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
              ...filterValues.map((value) =>
                f.eq(
                  f.field(requireArgument(filterField, "filterField")),
                  value,
                ),
              ),
            ),
          )
          .collect();
      case "isSet":
        return await q
          .filter((f) =>
            f.neq(
              f.field(requireArgument(filterField, "filterField")),
              undefined,
            ),
          )
          .collect();
      default:
        return await q.collect();
    }
  },
});
