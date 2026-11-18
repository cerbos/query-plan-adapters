import { afterAll, describe, expect, test } from "@jest/globals";
import { ConvexHttpClient } from "convex/browser";
import { anyApi } from "convex/server";

import { queryPlanToConvex, UnsupportedQueryPlanError } from "../src";
import { planOf } from "../src/corpus";
import {
  asCorpusGolden,
  DOMAINS,
  judge,
  readDomainGoldens,
  readResources,
  summary,
  thrown,
  writeResults,
} from "./common";
import type { Result } from "./common";
import { MAPPINGS } from "./mappings";

const convex = new ConvexHttpClient(process.env["CONVEX_URL"] ?? "http://127.0.0.1:3210");
const STORE = process.env["DOMAIN_STORE"] ?? "convex";
const ONLY = process.env["DOMAIN_ONLY"];
const results: Result[] = [];
const paths: Record<string, number> = {};

afterAll(() => {
  console.log(
    `${STORE}\n${summary(results)}\nexecution paths: ${JSON.stringify(paths)}\n${writeResults("convex", STORE, results)}`,
  );
});

describe.each(DOMAINS.filter((d) => !ONLY || ONLY.split(",").includes(d)).map((d) => [d]))("%s", (domain) => {
  test("replay", async () => {
    const mapping = MAPPINGS[domain];
    const docs = readResources(domain).resources.map((r) =>
      mapping.toDocument ? mapping.toDocument(r) : { id: r.id, ...r.attr },
    );
    await convex.mutation(anyApi["domains"]!["reset"]!, { domain, docs });

    for (const golden of readDomainGoldens(domain)) {
      try {
        const queryPlan = await planOf(asCorpusGolden(golden));
        queryPlanToConvex({
          queryPlan,
          mapper: mapping.mapper,
          allowPostFilter: true,
          nullAttributeRepresentation: mapping.nulls,
        });
        const out = (await convex.query(anyApi["domains"]!["executePlan"]!, {
          domain,
          queryPlan: JSON.parse(JSON.stringify(queryPlan)),
        })) as { ids: string[]; path: string; detail: string };
        paths[out.path] = (paths[out.path] ?? 0) + 1;
        const verdict = judge(golden, out.ids, out.detail);
        verdict.detail = `path=${out.path}${out.detail ? `; ${out.detail}` : ""}`;
        results.push(verdict);
      } catch (error) {
        results.push(thrown(golden, error, UnsupportedQueryPlanError));
      }
    }

    expect(results.length).toBeGreaterThan(0);
  }, 300_000);
});
