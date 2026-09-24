// Reading the planner's expression tree: operand shapes, and the comparison-operator tables every
// handler shares.

import type { PlanExpressionOperand, Value } from "@cerbos/core";
import { UnsupportedQueryPlanError } from "./errors";

export interface NamedOperand {
  name: string;
}

export interface ValueOperand {
  value: Value;
}

export interface OperatorOperand {
  operator: string;
  operands: PlanExpressionOperand[];
}

export function isNamedOperand(
  operand: PlanExpressionOperand
): operand is NamedOperand {
  return "name" in operand && typeof operand.name === "string";
}

export function isValueOperand(
  operand: PlanExpressionOperand
): operand is ValueOperand {
  return "value" in operand && operand.value !== undefined;
}

export function isOperatorOperand(
  operand: PlanExpressionOperand
): operand is OperatorOperand {
  return (
    "operator" in operand &&
    typeof operand.operator === "string" &&
    "operands" in operand &&
    Array.isArray(operand.operands)
  );
}

export function assertDefined<T>(value: T | undefined, message: string): T {
  if (value === undefined) {
    throw new UnsupportedQueryPlanError(message);
  }
  return value;
}

/**
 * Refuses an `and`/`or` with no operands. The planner never emits one, and neither empty form has
 * a safe rendering: `{ AND: [] }` is true in Prisma and matches every row, and the constant folder
 * would reduce an empty `and` to `true` (an unconditional filter) before the translator saw it. A
 * malformed plan is a bug report, never a filter.
 */
export function assertLogicalOperands(
  operator: string,
  operands: PlanExpressionOperand[]
): void {
  if ((operator === "and" || operator === "or") && operands.length === 0) {
    throw new UnsupportedQueryPlanError(
      `${operator} requires at least one operand: an empty ${operator} is not something the ` +
        "planner emits, and translating it would produce an unconditional filter " +
        "({ AND: [] } matches every row)"
    );
  }
}

/** The Prisma scalar filter each Cerbos comparison operator lowers to. */
export const CERBOS_TO_PRISMA_OPERATOR: Record<string, string> = {
  eq: "equals",
  ne: "not",
  lt: "lt",
  le: "lte",
  gt: "gt",
  ge: "gte",
};

/** The Cerbos comparison operators: exactly the keys of CERBOS_TO_PRISMA_OPERATOR. */
export const COMPARISON_OPERATORS: ReadonlySet<string> = new Set(
  Object.keys(CERBOS_TO_PRISMA_OPERATOR)
);

// Directional operators mirror when their operands swap sides; symmetric operators are unchanged.
const MIRRORED_OPERATOR: Record<string, string> = {
  lt: "gt",
  gt: "lt",
  le: "ge",
  ge: "le",
};

/** The operator that holds when the two operands of `operator` swap sides. */
export function mirrorOperator(operator: string): string {
  return MIRRORED_OPERATOR[operator] ?? operator;
}

/**
 * Normalize a binary comparison to field-side-first. The planner preserves policy source
 * order, so a constant may precede the field it constrains (`1 < R.attr.x` arrives as
 * `lt(value, variable)`) — swap the operands and mirror directional operators so downstream
 * handlers can assume the field/expression side is first. Without this, value-first
 * comparisons are silently inverted (#256).
 */
export function normalizeBinaryOperands(
  operator: string,
  operands: PlanExpressionOperand[]
): { operator: string; operands: PlanExpressionOperand[] } {
  const [first, second] = operands;
  if (
    operands.length === 2 &&
    first !== undefined &&
    second !== undefined &&
    isValueOperand(first) &&
    !isValueOperand(second)
  ) {
    return { operator: mirrorOperator(operator), operands: [second, first] };
  }
  return { operator, operands };
}
