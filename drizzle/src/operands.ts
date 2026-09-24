import type { PlanExpressionOperand, Value } from "@cerbos/core";
import { UnsupportedQueryPlanError } from "./errors";

/** Shape tests over the planner's operand union, and the lambda-unpacking every macro shares. */

export type NamedOperand = { name: string };

export type ExpressionOperand = {
  operator: string;
  operands: PlanExpressionOperand[];
};

export const isNameOperand = (
  operand: PlanExpressionOperand,
): operand is NamedOperand =>
  "name" in operand && typeof operand.name === "string";

export const isValueOperand = (
  operand: PlanExpressionOperand,
): operand is { value: Value } => "value" in operand;

export const isExpressionOperand = (
  operand: PlanExpressionOperand,
): operand is ExpressionOperand =>
  "operator" in operand &&
  "operands" in operand &&
  Array.isArray(operand.operands);

/** Whether `operand` is a call to `operator`. */
export const isOperatorCall = (
  operand: PlanExpressionOperand,
  operator: string,
): operand is ExpressionOperand =>
  isExpressionOperand(operand) && operand.operator === operator;

/** Whether an operand is a CEL `string()` conversion, whose result is a string whatever it reads. */
export const isStringConversion = (operand: PlanExpressionOperand): boolean =>
  isOperatorCall(operand, "string");

/** The literal list an operand carries, if it carries one. */
export const extractArrayValue = (
  operand: PlanExpressionOperand,
): Value[] | undefined =>
  "value" in operand && Array.isArray(operand.value)
    ? operand.value
    : undefined;

const looksLikeLambdaVariable = (operand: NamedOperand): boolean =>
  !operand.name.includes(".");

/**
 * Split a `lambda` operand into its iteration variable and its body.
 *
 * The planner does not promise an operand order, so the variable is whichever operand is a bare
 * name. When both are names, the undotted one is the variable; when that does not settle it, the
 * second operand is, which is where the planner puts it.
 */
export const extractLambdaComponents = (
  lambdaOperand: PlanExpressionOperand,
  context: string,
): { variable: NamedOperand; expression: PlanExpressionOperand } => {
  if (!isOperatorCall(lambdaOperand, "lambda")) {
    throw new UnsupportedQueryPlanError(`${context} must be a lambda expression`);
  }
  if (lambdaOperand.operands.length !== 2) {
    throw new UnsupportedQueryPlanError("Lambda operand requires exactly two operands");
  }
  const [first, second] = lambdaOperand.operands;
  if (!first || !second) {
    throw new UnsupportedQueryPlanError("Lambda operand is missing operands");
  }

  if (!isNameOperand(second)) {
    if (!isNameOperand(first)) {
      throw new UnsupportedQueryPlanError("Lambda operand requires a variable operand");
    }
    return { variable: first, expression: second };
  }
  if (
    isNameOperand(first) &&
    looksLikeLambdaVariable(first) &&
    !looksLikeLambdaVariable(second)
  ) {
    return { variable: first, expression: second };
  }
  return { variable: second, expression: first };
};

/**
 * The plan with every `list(...)` and `struct(set-field(...)...)` constructor whose leaves are all
 * constants replaced by the constant it builds — `["a"]`, `{"a": 1}` — so a constructor reaches the
 * translators exactly as the planner's own folded literals do. A struct with a non-string or
 * repeated key is left as it is: CEL rejects a repeated key, and a JSON-sourced attribute never
 * holds a non-string one.
 */
export const foldConstantConstructors = (
  operand: PlanExpressionOperand,
): PlanExpressionOperand => {
  if (!isExpressionOperand(operand)) return operand;
  const operands = operand.operands.map(foldConstantConstructors);
  if (operand.operator === "list" && operands.every(isValueOperand)) {
    return { value: operands.map((element) => element.value) };
  }
  if (operand.operator === "struct") {
    const entries: [string, Value][] = [];
    for (const field of operands) {
      if (!isExpressionOperand(field) || field.operator !== "set-field") break;
      const [key, value] = field.operands;
      if (
        field.operands.length !== 2 || key === undefined || value === undefined ||
        !isValueOperand(key) || typeof key.value !== "string" || !isValueOperand(value) ||
        entries.some(([existing]) => existing === key.value)
      ) {
        break;
      }
      entries.push([key.value, value.value]);
    }
    if (entries.length === operands.length) {
      return { value: Object.fromEntries(entries) };
    }
  }
  return { operator: operand.operator, operands };
};
