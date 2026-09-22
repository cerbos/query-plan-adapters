import type { PlanExpressionOperand, Value } from "@cerbos/core";

import { isExpressionOperand, isValueOperand } from "./operands";
import type { ExpressionOperand } from "./operands";

/**
 * Constant folding in JavaScript's IEEE double space — CEL's own number space — for the
 * arithmetic and comparisons SQL cannot be trusted with: division by zero, NaN and infinities.
 */

export const ARITHMETIC_OPERATORS: Record<string, string> = {
  add: "+",
  sub: "-",
  mult: "*",
  div: "/",
  mod: "%",
};

export type LeafComparisonOperator = "eq" | "ne" | "lt" | "le" | "gt" | "ge";

/**
 * The arithmetic that folds exactly in IEEE doubles. CEL's `%` is integer-only while Cerbos
 * attribute values are always doubles, so a modulus is a no-overload error that denies every row
 * at check time; folding it with JavaScript's `%` would answer a question CEL refused.
 */
type Fold = (left: number, right: number) => number;

const FOLDABLE_ARITHMETIC = new Map<string, Fold>([
  ["add", (left, right) => left + right],
  ["sub", (left, right) => left - right],
  ["mult", (left, right) => left * right],
  ["div", (left, right) => left / right],
]);

/** The foldable binary arithmetic node `operand` is, if it is one. */
const foldableArithmetic = (
  operand: PlanExpressionOperand,
):
  | { apply: Fold; left: PlanExpressionOperand; right: PlanExpressionOperand }
  | undefined => {
  if (!isExpressionOperand(operand) || operand.operands.length !== 2) {
    return undefined;
  }
  const apply = FOLDABLE_ARITHMETIC.get(operand.operator);
  const [left, right] = operand.operands;
  return apply && left && right ? { apply, left, right } : undefined;
};

/**
 * The value of an operand that is a number literal, or `+ - * /` over number literals. Anything
 * else — a column, a string, a modulus — is `undefined`.
 */
export const resolveConstantNumber = (
  operand: PlanExpressionOperand,
): number | undefined => {
  if (isValueOperand(operand)) {
    return typeof operand.value === "number" ? operand.value : undefined;
  }
  const arithmetic = foldableArithmetic(operand);
  if (!arithmetic) {
    return undefined;
  }
  const left = resolveConstantNumber(arithmetic.left);
  const right = resolveConstantNumber(arithmetic.right);
  return left === undefined || right === undefined
    ? undefined
    : arithmetic.apply(left, right);
};

/**
 * A division whose denominator is not a known non-zero constant, so the row-level result
 * may be a CEL NaN or signed infinity that SQL cannot represent.
 */
const isZeroCapableDivision = (
  operand: PlanExpressionOperand,
): operand is ExpressionOperand => {
  if (!isExpressionOperand(operand) || operand.operator !== "div") {
    return false;
  }
  // A division of two constants already folds to an exact IEEE value, and the
  // constant/NaN paths render it more tightly than a CASE can.
  if (resolveConstantNumber(operand) !== undefined) {
    return false;
  }
  const denominatorOperand = operand.operands[1];
  if (!denominatorOperand) {
    return false;
  }
  const denominator = resolveConstantNumber(denominatorOperand);
  return denominator === undefined || denominator === 0;
};

/**
 * The first zero-capable division anywhere in an arithmetic tree. The comparison need not
 * sit directly on the division: `div(a, a) + 1.0 != 2.0` composes addition on top of a
 * result that can be NaN, and lowering that to `NULL + 1` excludes the one row CEL allows.
 */
export const findZeroCapableDivision = (
  operand: PlanExpressionOperand,
): ExpressionOperand | undefined => {
  if (isZeroCapableDivision(operand)) {
    return operand;
  }
  if (
    !isExpressionOperand(operand) ||
    !(operand.operator in ARITHMETIC_OPERATORS)
  ) {
    return undefined;
  }
  for (const child of operand.operands) {
    const found = findZeroCapableDivision(child);
    if (found) {
      return found;
    }
  }
  return undefined;
};

/**
 * Evaluate an arithmetic tree in JavaScript's IEEE double space with `target` replaced by
 * `substitute`, so a NaN or infinity produced by a division propagates through the
 * surrounding arithmetic exactly as CEL propagates it. Returns undefined when any other
 * leaf is not a constant — SQL cannot carry a non-finite value alongside a column, and
 * guessing would return rows the PDP denies.
 */
export const foldWithSubstitution = (
  operand: PlanExpressionOperand,
  target: PlanExpressionOperand,
  substitute: number,
): number | undefined => {
  if (operand === target) {
    return substitute;
  }
  if (isValueOperand(operand)) {
    return typeof operand.value === "number" ? operand.value : undefined;
  }
  // A modulus is not foldable, so it fails closed here rather than being answered.
  const arithmetic = foldableArithmetic(operand);
  if (!arithmetic) {
    return undefined;
  }
  const left = foldWithSubstitution(arithmetic.left, target, substitute);
  const right = foldWithSubstitution(arithmetic.right, target, substitute);
  return left === undefined || right === undefined
    ? undefined
    : arithmetic.apply(left, right);
};

export const evaluateConstantNumberComparison = (
  operator: LeafComparisonOperator,
  left: number,
  right: number,
): boolean => {
  if (
    operator !== "eq" &&
    operator !== "ne" &&
    (Number.isNaN(left) || Number.isNaN(right))
  )
    return false;
  switch (operator) {
    case "eq":
      return left === right;
    case "ne":
      return left !== right;
    case "lt":
      return left < right;
    case "le":
      return left <= right;
    case "gt":
      return left > right;
    case "ge":
      return left >= right;
  }
};

/** A comparison between two literals of the same CEL type; `undefined` when CEL has no overload. */
export const evaluateScalarValueComparison = (
  operator: LeafComparisonOperator,
  left: Value,
  right: Value,
): boolean | undefined => {
  const bothNumbers = typeof left === "number" && typeof right === "number";
  const bothStrings = typeof left === "string" && typeof right === "string";
  const bothBooleans = typeof left === "boolean" && typeof right === "boolean";
  const bothNull = left === null && right === null;
  if (!bothNumbers && !bothStrings && !bothBooleans && !bothNull) {
    return undefined;
  }
  switch (operator) {
    case "eq":
      return left === right;
    case "ne":
      return left !== right;
    case "lt":
      return bothNumbers || bothStrings ? left < right : undefined;
    case "le":
      return bothNumbers || bothStrings ? left <= right : undefined;
    case "gt":
      return bothNumbers || bothStrings ? left > right : undefined;
    case "ge":
      return bothNumbers || bothStrings ? left >= right : undefined;
  }
};
