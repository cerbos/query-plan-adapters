import { afterAll, beforeAll, describe, expect, test } from "@jest/globals";
import { ChromaClient, ChromaNotFoundError, type Collection } from "chromadb";

import { PlanKind, queryPlanToChromaDB, UnsupportedOperatorError } from "../../../../langchain-chromadb/src";
import { planOf } from "../../../../langchain-chromadb/test/golden-plans";
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

const chromaUrl = new URL(process.env["CHROMA_URL"] ?? "http://127.0.0.1:18000");
const chroma = new ChromaClient({ host: chromaUrl.hostname, port: Number(chromaUrl.port) || 8000 });
const POST_FILTER = process.env["DOMAIN_POST_FILTER"] !== "0";
const STORE = process.env["DOMAIN_STORE"] ?? (POST_FILTER ? "chroma" : "chroma-where-only");
const ONLY = process.env["DOMAIN_ONLY"];
const results: Result[] = [];

beforeAll(async () => {
  await chroma.heartbeat();
}, 30_000);

afterAll(() => {
  console.log(`${STORE}\n${summary(results)}\n${writeResults("langchain-chromadb", STORE, results)}`);
});

async function selectAllowed(collection: Collection, mapping: DomainMapping, queryPlan: Awaited<ReturnType<typeof planOf>>) {
  const result = queryPlanToChromaDB({
    queryPlan,
    fieldNameMapper: mapping.fieldNameMapper,
    allowPostFilter: POST_FILTER,
  });

  if (result.kind !== PlanKind.CONDITIONAL) {
    return {
      ids: result.kind === PlanKind.ALWAYS_DENIED ? [] : (await collection.get()).ids,
      detail: "",
    };
  }

  if (!("postFilter" in result)) {
    return {
      ids: (await collection.get({ where: result.filters })).ids,
      detail: `where: ${JSON.stringify(result.filters)}`,
    };
  }

  const { postFilter, prefilter } = result;
  const narrowed = await collection.get({ where: prefilter, include: ["metadatas"] });
  const ids = narrowed.ids.filter((_id, i) => postFilter(narrowed.metadatas[i]));
  let detail = `prefilter: ${JSON.stringify(prefilter)}`;

  if (prefilter !== undefined) {
    const every = await collection.get({ include: ["metadatas"] });
    const alone = every.ids.filter((_id, i) => postFilter(every.metadatas[i]));

    if (JSON.stringify([...alone].sort()) !== JSON.stringify([...ids].sort())) {
      detail += `; postFilter alone selected ${JSON.stringify(alone.sort())}`;
    }
  }

  return { ids, detail };
}

describe.each(DOMAINS.filter((d) => !ONLY || ONLY.split(",").includes(d)).map((d) => [d]))("%s", (domain) => {
  test("replay", async () => {
    const mapping: DomainMapping = require(`./${domain}`).default;
    const name = `dom-${domain}`;

    try {
      await chroma.deleteCollection({ name });
    } catch (error: unknown) {
      if (!(error instanceof ChromaNotFoundError)) throw error;
    }

    const collection = await chroma.createCollection({ name, embeddingFunction: null });
    const resources = readResources(domain).resources;
    await collection.add({
      ids: resources.map((r) => r.id),
      embeddings: resources.map(() => [0.1, 0.2, 0.3, 0.4]),
      metadatas: resources.map(mapping.toMetadata),
    });

    for (const golden of readDomainGoldens(domain)) {
      try {
        const { ids, detail } = await selectAllowed(collection, mapping, await planOf(asCorpusGolden(golden)));
        const verdict = judge(golden, ids);

        if (verdict.verdict !== "pass" || detail.includes("alone")) verdict.detail = detail.slice(0, 1500);
        results.push(verdict);
      } catch (error) {
        results.push(thrown(golden, error, UnsupportedOperatorError));
      }
    }

    await chroma.deleteCollection({ name });
    expect(results.length).toBeGreaterThan(0);
  }, 120_000);
});
