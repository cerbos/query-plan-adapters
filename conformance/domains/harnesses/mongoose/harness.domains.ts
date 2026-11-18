import * as os from "node:os";

import { afterAll, beforeAll, describe, expect, test } from "@jest/globals";
import mongoose from "mongoose";

import { PlanKind, queryPlanToMongoose, UnsupportedQueryPlanError } from "@cerbos/orm-mongoose";
import { planOf } from "@cerbos/orm-mongoose/corpus";
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
import type { DomainMapping } from "./mapping";

const MONGO_URL = process.env["MONGO_URL"] ?? "mongodb://127.0.0.1:17017/cerbos_domains";
const STORE = process.env["DOMAIN_STORE"] ?? "mongo-7";
const VARIANT = process.env["DOMAIN_VARIANT"] ?? "";
const ONLY = process.env["DOMAIN_ONLY"];
const results: Result[] = [];

beforeAll(async () => {
  await mongoose.connect(MONGO_URL, { runtimeAdapters: { os } });
}, 30_000);

afterAll(async () => {
  await mongoose.disconnect();
  console.log(`${STORE}\n${summary(results)}\n${writeResults("mongoose", STORE, results)}`);
});

describe.each(DOMAINS.filter((d) => !ONLY || ONLY.split(",").includes(d)).map((d) => [d]))("%s", (domain) => {
  test("replay", async () => {
    const file = VARIANT && domain === VARIANT.split(":")[0] ? VARIANT.split(":")[1] : domain;
    const mapping: DomainMapping = require(`./${file}`).default;
    const Model = mongoose.model(`dom_${domain}`, mapping.schema, `dom_${domain}`);
    await Model.deleteMany({});

    const docs = readResources(domain).resources.map((r) =>
      mapping.toDocument ? mapping.toDocument(r) : { _id: r.id, ...r.attr },
    );

    await Model.insertMany(docs);

    if (mapping.load) await mapping.load(docs);

    for (const golden of readDomainGoldens(domain)) {
      try {
        const result = queryPlanToMongoose({
          queryPlan: await planOf(asCorpusGolden(golden)),
          schema: Model,
          mapper: mapping.mapper,
          nullAttributeRepresentation: mapping.nulls,
        });

        let ids: string[];

        if (result.kind === PlanKind.ALWAYS_DENIED) ids = [];
        else if (mapping.query) {
          ids = await mapping.query(Model, result.kind === PlanKind.CONDITIONAL ? result.filters : {});
        } else {
          const rows = await Model.find(result.kind === PlanKind.CONDITIONAL ? result.filters : {})
            .select({ _id: 1 })
            .lean()
            .exec();

          ids = rows.map((row) => String(row._id));
        }

        const verdict = judge(golden, ids);

        if (verdict.verdict !== "pass" && result.kind === PlanKind.CONDITIONAL) {
          verdict.detail = `filter: ${JSON.stringify(result.filters).slice(0, 1500)}`;
        }

        results.push(verdict);
      } catch (error) {
        results.push(thrown(golden, error, UnsupportedQueryPlanError));
      }
    }

    expect(results.length).toBeGreaterThan(0);
  }, 120_000);
});
