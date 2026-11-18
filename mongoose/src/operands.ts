import type {
  PlanExpression,
  PlanExpressionOperand,
  PlanExpressionValue,
  PlanExpressionVariable,
} from "@cerbos/core";

import { UnsupportedQueryPlanError } from "./errors";
import type { MongoValue } from "./expression";

export const isExpression = (e: PlanExpressionOperand): e is PlanExpression =>
  "operator" in e;

export const isValue = (e: PlanExpressionOperand): e is PlanExpressionValue =>
  "value" in e;

export const isVariable = (
  e: PlanExpressionOperand,
): e is PlanExpressionVariable => "name" in e;

export const getOperandAt = (
  operands: PlanExpressionOperand[],
  index: number,
  errorMessage: string,
): PlanExpressionOperand => {
  const operand = operands[index];

  if (!operand) {
    throw new UnsupportedQueryPlanError(errorMessage);
  }

  return operand;
};

/** Every variable name referenced anywhere below `operand`, in plan order. */
export const collectVariableNames = (
  operand: PlanExpressionOperand,
): string[] => {
  if (isVariable(operand)) {
    return [operand.name];
  }

  if (isExpression(operand)) {
    return operand.operands.flatMap(collectVariableNames);
  }

  return [];
};

/**
 * Whether a comparison operand carries a plan-level `null` — directly, or as an element of an
 * `in` list. This is the null-representation guard's own predicate rather than a reuse of
 * `requireExists`: the two happen to coincide today, but `requireExists` means "the field must be
 * present", and a future caller setting it for that reason alone must not trip the null rejection.
 */
export const carriesNullOperand = (value: MongoValue): boolean =>
  value === null || (Array.isArray(value) && value.includes(null));

// Every operator a Cerbos plan holds. Some translations decide a subtree from its type alone, so an
// unknown operator is refused before translating.
const PLANNER_OPERATORS = new Set([
  "add", "all", "ancestorOf", "and", "contains", "descendentOf", "div", "double", "duration", "dyn",
  "endsWith", "eq", "except", "exists", "exists_one", "filter", "ge", "get-field", "gt", "has",
  "hasIntersection", "has_intersection", "hierarchy", "if", "in", "index", "int", "intersect",
  "isSubset", "lambda", "le", "list", "lt", "map", "matches", "mod", "mult", "ne", "not", "or",
  "overlaps", "set-field", "size", "startsWith", "string", "struct", "sub", "timeSince",
  "timestamp", "upperAscii",
]);

export const assertPlannerOperators = (operand: PlanExpressionOperand): void => {
  if (!isExpression(operand)) return;

  if (!PLANNER_OPERATORS.has(operand.operator)) {
    throw new UnsupportedQueryPlanError(`Unsupported operator: ${operand.operator}`);
  }

  operand.operands.forEach(assertPlannerOperators);
};
