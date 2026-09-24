import * as fs from "node:fs";
import * as path from "node:path";

import {
  PlanExpression,
  PlanExpressionValue,
  PlanExpressionVariable,
} from "@cerbos/core";
import type {
  PlanExpressionOperand,
  PlanResourcesResponse,
  Value,
} from "@cerbos/core";

import { PlanKind } from ".";

/**
 * What both of this adapter's suites read from the shared `../conformance/` corpus: the recorded
 * golden plans. `adversarial.test.ts` replays them against a real Convex backend;
 * `translator.test.ts` reads a few by case id for the caller-option tests the corpus cannot vary.
 * The one mapper both use lives in `../convex/adversarialMapper.ts`, where the backend reads it.
 *
 * Duplicated across adapters on purpose — adapters share data, not code (ADR 0007).
 * Test-only: excluded from `tsc --build` by `tsconfig.json`, so nothing here reaches `lib/`.
 */

export const CONFORMANCE_DIR = path.join(__dirname, "..", "..", "conformance");

export function readCorpusJson(file: string): unknown {
  return JSON.parse(fs.readFileSync(path.join(CONFORMANCE_DIR, file), "utf8"));
}

interface WireOperand {
  expression?: { operator: string; operands: WireOperand[] };
  variable?: string;
  value?: unknown;
}

export interface Golden {
  id: string;
  pdp: string;
  tier: "core" | "extended" | "adversarial";
  plan: { kind: string; condition?: WireOperand };
  allowed: string[];
  /** A planner bug no adapter can pass. Recorded only in the goldens of the tags it applies to. */
  plannerDivergence: { issue: string; pdp?: string[] } | null;
}

/** The PDP tags the goldens were recorded against: current first, then previous. */
export function pdpTags(): string[] {
  const versions = readCorpusJson("pdp-versions.json") as Record<
    "current" | "previous",
    { tag: string }
  >;
  return [versions.current.tag, versions.previous.tag];
}

/** Every golden file recorded against `tag`, sorted by case id. */
export function readGoldens(tag: string): Golden[] {
  const root = path.join(CONFORMANCE_DIR, "golden", tag);
  return (fs.readdirSync(root, { recursive: true }) as string[])
    .filter((file) => file.endsWith(".json"))
    .map(
      (file) =>
        JSON.parse(fs.readFileSync(path.join(root, file), "utf8")) as Golden,
    )
    .sort((a, b) => a.id.localeCompare(b.id));
}

export function readGolden(tag: string, id: string): Golden {
  return JSON.parse(
    fs.readFileSync(
      path.join(CONFORMANCE_DIR, "golden", tag, `${id}.json`),
      "utf8",
    ),
  ) as Golden;
}

/**
 * The instant substituted for `__NOW_MINUS_24H__`, the literal the planner folds
 * `now() - duration("24h")` into. The PDP folds its clock at nanosecond precision, so the
 * substitute carries sub-millisecond digits too, and the adapter sees the literal it would see in
 * production.
 */
export function nowMinus24h(): string {
  const ms = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
  return `${ms.slice(0, -1)}456789Z`;
}

function operandFromWire(
  node: WireOperand,
  now: string,
): PlanExpressionOperand {
  if (node.expression) {
    return new PlanExpression(
      node.expression.operator,
      node.expression.operands.map((child) => operandFromWire(child, now)),
    );
  }
  if (node.variable !== undefined) {
    return new PlanExpressionVariable(node.variable);
  }
  // The golden is JSON the PDP produced, so its leaves are already the shapes `Value` admits.
  return new PlanExpressionValue(
    (node.value === "__NOW_MINUS_24H__" ? now : node.value) as Value,
  );
}

/** A golden's plan decoded the way `@cerbos/http` decodes a PlanResources response. */
export function planOf(
  golden: Golden,
  now: string = nowMinus24h(),
): PlanResourcesResponse {
  const base = {
    cerbosCallId: "",
    requestId: "",
    validationErrors: [],
    metadata: undefined,
  };
  const { kind, condition } = golden.plan;
  if (kind === PlanKind.CONDITIONAL && condition) {
    return {
      ...base,
      kind: PlanKind.CONDITIONAL,
      condition: operandFromWire(condition, now),
    };
  }
  if (kind === PlanKind.ALWAYS_ALLOWED || kind === PlanKind.ALWAYS_DENIED) {
    return { ...base, kind };
  }
  throw new Error(
    `${golden.id}: unrecognised plan ${JSON.stringify(golden.plan)}`,
  );
}
