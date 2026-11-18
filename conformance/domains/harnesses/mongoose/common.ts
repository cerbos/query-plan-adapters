import * as fs from "node:fs";
import * as path from "node:path";

import type { JsonObject, JsonValue } from "@bufbuild/protobuf";

export const DOMAINS_DIR = path.join(__dirname, "..", "..");

export const DOMAINS = [
  "healthcare",
  "fintech",
  "logistics",
  "education",
  "legal",
  "retail",
  "hr",
  "media",
  "iot",
  "government",
] as const;

export type Domain = (typeof DOMAINS)[number];

export interface DomainGolden {
  domain: string;
  pdp: string;
  principalName: string;
  action: string;
  plan: JsonObject;
  allowed: string[];
}

export interface Resource {
  id: string;
  attr: Record<string, JsonValue>;
}

export interface Resources {
  kind: string;
  nulls: "omitted" | "explicit";
  resources: Resource[];
}

export type Verdict = "pass" | "refused" | "over-grant" | "under-grant" | "error";

export interface Result {
  domain: string;
  pdp: string;
  principal: string;
  action: string;
  verdict: Verdict;
  returned: string[];
  allowed: string[];
  detail: string;
}

export function readResources(domain: string): Resources {
  return JSON.parse(
    fs.readFileSync(path.join(DOMAINS_DIR, domain, "generated", "resources.json"), "utf8"),
  );
}

export function readDomainGoldens(domain: string): DomainGolden[] {
  const root = path.join(DOMAINS_DIR, domain, "golden");
  const out: DomainGolden[] = [];

  for (const pdp of fs.readdirSync(root).sort()) {
    for (const principal of fs.readdirSync(path.join(root, pdp)).sort()) {
      for (const file of fs.readdirSync(path.join(root, pdp, principal)).sort()) {
        if (!file.endsWith(".json")) continue;
        out.push(JSON.parse(fs.readFileSync(path.join(root, pdp, principal, file), "utf8")));
      }
    }
  }

  return out.sort(
    (a, b) =>
      b.pdp.localeCompare(a.pdp) ||
      a.principalName.localeCompare(b.principalName) ||
      a.action.localeCompare(b.action),
  );
}

/** The conformance helper's `Golden` shape, so `planOf` serves the plan through both SDK stubs. */
export function asCorpusGolden(golden: DomainGolden) {
  return {
    id: `${golden.domain}/${golden.principalName}/${golden.action}`,
    pdp: golden.pdp,
    tier: "core" as const,
    plan: golden.plan,
    allowed: golden.allowed,
    plannerDivergence: null,
  };
}

export function judge(golden: DomainGolden, returned: string[], detail = ""): Result {
  const allowed = [...golden.allowed].sort();
  const got = [...new Set(returned)].sort();
  const allowedSet = new Set(allowed);
  const gotSet = new Set(got);
  let verdict: Verdict;

  if (got.some((id) => !allowedSet.has(id))) verdict = "over-grant";
  else if (allowed.some((id) => !gotSet.has(id))) verdict = "under-grant";
  else if (got.length !== returned.length) verdict = "error";
  else verdict = "pass";

  return {
    domain: golden.domain,
    pdp: golden.pdp,
    principal: golden.principalName,
    action: golden.action,
    verdict,
    returned: got,
    allowed,
    detail: verdict === "error" && !detail ? "duplicate ids returned" : detail,
  };
}

export function thrown(
  golden: DomainGolden,
  cause: unknown,
  refusal: abstract new (...args: never[]) => Error,
): Result {
  const message = cause instanceof Error ? `${cause.name}: ${cause.message}` : String(cause);

  return {
    domain: golden.domain,
    pdp: golden.pdp,
    principal: golden.principalName,
    action: golden.action,
    verdict: cause instanceof refusal ? "refused" : "cause",
    returned: [],
    allowed: [...golden.allowed].sort(),
    detail: message.split("\n")[0]!.slice(0, 400),
  };
}

export function writeResults(adapter: string, store: string, results: Result[]): string {
  const dir = path.join(DOMAINS_DIR, "results", adapter);
  fs.mkdirSync(dir, { recursive: true });
  const file = path.join(dir, `${store}.json`);
  const lines = results.map((result) => `    ${JSON.stringify(result)}`);
  fs.writeFileSync(
    file,
    `{"adapter": ${JSON.stringify(adapter)}, "store": ${JSON.stringify(store)}, "results": [\n${lines.join(",\n")}\n]}\n`,
  );

  return file;
}

export function summary(results: Result[]): string {
  const counts: Record<string, Record<string, number>> = {};

  for (const r of results) {
    const row = (counts[r.domain] ??= {});
    row[r.verdict] = (row[r.verdict] ?? 0) + 1;
  }

  return Object.entries(counts)
    .map(([domain, row]) => `${domain}: ${JSON.stringify(row)}`)
    .join("\n");
}
