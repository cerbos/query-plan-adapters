import type { PlanResourcesResponse } from "@cerbos/core";
import { mutationGeneric, queryGeneric } from "convex/server";
import type { GenericDataModel, GenericDatabaseReader } from "convex/server";
import { v } from "convex/values";

import { PlanKind, queryPlanToConvex } from "../../src/index";
import { MAPPINGS, type DomainName } from "../mappings";

const domain = v.union(...Object.keys(MAPPINGS).map((name) => v.literal(name)));

const table = (db: GenericDatabaseReader<GenericDataModel>, name: string) => db.query(name);

export const reset = mutationGeneric({
  args: { domain, docs: v.array(v.any()) },
  handler: async (ctx, args) => {
    for (const doc of await ctx.db.query(args.domain).collect()) await ctx.db.delete(doc._id);
    for (const doc of args.docs) await ctx.db.insert(args.domain, doc);
  },
});

export const executePlan = queryGeneric({
  args: { domain, queryPlan: v.any() },
  handler: async (ctx, args) => {
    const mapping = MAPPINGS[args.domain as DomainName];
    const translated = queryPlanToConvex({
      queryPlan: args.queryPlan as PlanResourcesResponse,
      mapper: mapping.mapper,
      allowPostFilter: true,
      nullAttributeRepresentation: mapping.nulls,
    });
    const idsOf = (docs: Record<string, unknown>[]) => docs.map((doc) => doc["id"] as string).sort();
    const all = () => table(ctx.db, args.domain);

    if (translated.kind !== PlanKind.CONDITIONAL) {
      return {
        ids: translated.kind === PlanKind.ALWAYS_DENIED ? [] : idsOf(await all().collect()),
        path: "unconditional",
        detail: "",
      };
    }

    if (translated.path === "db") {
      return { ids: idsOf(await all().filter(translated.filter).collect()), path: "db", detail: "" };
    }

    const { postFilter } = translated;
    const candidates = await (translated.path === "split" ? all().filter(translated.prefilter) : all()).collect();
    const ids = idsOf(candidates.filter((doc) => postFilter({ ...doc })));
    let detail = "";

    if (translated.path === "split") {
      const alone = idsOf((await all().collect()).filter((doc) => postFilter({ ...doc })));
      if (JSON.stringify(alone) !== JSON.stringify(ids)) detail = `postFilter alone selected ${JSON.stringify(alone)}`;
    }

    return { ids, path: translated.path, detail };
  },
});
