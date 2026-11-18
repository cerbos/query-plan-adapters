import * as fs from "node:fs";
import * as path from "node:path";

import { readGoldens as readCorpusGoldens } from "../../../langchain-chromadb/test/golden-plans";
import type { Golden } from "../../../langchain-chromadb/test/golden-plans";

/**
 * Fuzz mode (conformance/README.md, "Fuzzing"): the goldens a fuzz run recorded, and the verdict
 * each case gets in place of the ledger.
 */

// -- fuzz mode ------------------------------------------------------------------------------------

/**
 * The directory a fuzz run was recorded into (conformance/README.md, "Fuzzing"). When it is set,
 * `readGoldens` reads the generated cases from it instead of the corpus, and the harness judges
 * each case by `fuzzVerdict` instead of the ledger: a refusal is always acceptable, since the
 * ledger has no entry for a shape nobody has written down yet.
 */
export const FUZZ_DIR = process.env["QPA_FUZZ_DIR"] || undefined;

/** Every case recorded against `tag`: the fuzz run's when `FUZZ_DIR` is set, else the corpus's. */
export function readGoldens(tag: string): Golden[] {
  return FUZZ_DIR ? readCorpusGoldens(tag, FUZZ_DIR) : readCorpusGoldens(tag);
}

export interface FuzzVerdict {
  pdp: string;
  id: string;
  verdict: "match" | "refused" | "over-grant" | "under-grant" | "error";
  expected?: string[];
  returned?: string[];
  detail?: string;
}

/** Judges the ids a translated filter returned against the ids the PDP allowed. */
export function fuzzVerdict(golden: Golden, expected: string[], returned: string[]): FuzzVerdict {
  const base = { pdp: golden.pdp, id: golden.id };

  // Compared element by element: under jest the arrays come from different realms, which
  // isDeepStrictEqual tells apart.
  if (returned.length === expected.length && returned.every((id, i) => id === expected[i])) {
    return { ...base, verdict: "match" };
  }

  const allowed = new Set(expected);
  const got = new Set(returned);

  const verdict = returned.some((id) => !allowed.has(id))
    ? "over-grant"
    : expected.some((id) => !got.has(id))
      ? "under-grant"
      : "error";

  if (verdict === "error") {
    return { ...base, verdict, expected, returned, detail: "the right rows, but repeated or out of order" };
  }

  return { ...base, verdict, expected, returned };
}

/** A translation or query that threw, judged: the adapter's refusal type is the invariant working. */
export function fuzzThrown(golden: Golden, cause: unknown, refusal: abstract new (...args: never[]) => Error): FuzzVerdict {
  const base = { pdp: golden.pdp, id: golden.id };

  if (cause instanceof refusal) return { ...base, verdict: "refused" };

  return { ...base, verdict: "error", detail: cause instanceof Error ? `${cause.name}: ${cause.message}` : String(cause) };
}

/** Fails the case unless its verdict is a match or a refusal. */
export function assertFuzzVerdict(verdict: FuzzVerdict): void {
  if (verdict.verdict === "match" || verdict.verdict === "refused") return;
  throw new Error(
    `${verdict.verdict}: expected [${verdict.expected?.join(", ") ?? ""}], returned [${verdict.returned?.join(", ") ?? ""}]${verdict.detail ? `: ${verdict.detail}` : ""}`,
  );
}

/** Writes `<FUZZ_DIR>/results/<adapter>-<store>.json`, which `fuzz-report` reads. */
export function writeFuzzResults(adapter: string, store: string, results: FuzzVerdict[]): void {
  if (!FUZZ_DIR) return;
  const dir = path.join(FUZZ_DIR, "results");
  fs.mkdirSync(dir, { recursive: true });
  const sorted = [...results].sort((a, b) => a.id.localeCompare(b.id) || a.pdp.localeCompare(b.pdp));
  fs.writeFileSync(
    path.join(dir, `${adapter}-${store}.json`),
    `${JSON.stringify({ adapter, store, results: sorted }, null, 2)}\n`,
  );
}
