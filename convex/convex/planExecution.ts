import type {
  PlanExpressionOperand,
  PlanResourcesResponse,
} from "@cerbos/core";

import { PlanKind } from "../src/index";
import type { QueryPlanToConvexResult } from "../src/index";

// Shared by the two test backends. Both re-establish a plan's shape after it crosses the Convex
// boundary as `v.any()`, and both report which half of the adapter's output actually answered the
// query. One copy each: the guard is what stands between an arbitrary JSON payload and
// `queryPlanToConvex`, and the path is a claim a harness asserts, so two drifting copies would
// mean the two backends disagreed about what they had proved.

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

export const isPlanResourcesResponse = (
  value: unknown,
): value is PlanResourcesResponse => {
  if (!isRecord(value)) return false;
  const kind = value["kind"];
  if (kind === PlanKind.ALWAYS_ALLOWED || kind === PlanKind.ALWAYS_DENIED) {
    return true;
  }
  return kind === PlanKind.CONDITIONAL && isPlanOperand(value["condition"]);
};

/**
 * Which half of the adapter's output decided the query.
 *
 * A harness cannot tell `db` from `post` by looking at the ids: both are supposed to return the
 * documents `check()` allows, so a leg that meant to exercise Convex's filter engine and silently
 * fell back to the in-memory evaluator passes its oracle comparison unchanged. Reporting the path
 * FROM THE BACKEND is what makes that assertable — deriving it in the harness instead would only
 * re-run the same translation the harness already trusts, not observe the one that ran.
 */
export type ExecutionPath = "db" | "split" | "post" | "unconditional";

// Generic over the filter's own types: `ConvexFilter` is a function type, so it is contravariant
// in `Q` and a result built for a concrete `FilterBuilder` is not assignable to an `unknown` one.
export const executionPathOf = <Q, R>(
  translated: QueryPlanToConvexResult<Q, R>,
): ExecutionPath => {
  if (translated.kind !== PlanKind.CONDITIONAL) return "unconditional";
  if (translated.filter && translated.postFilter) return "split";
  return translated.filter ? "db" : "post";
};
