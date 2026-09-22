import type {
  PlanExpressionOperand,
  PlanExpressionValue,
  Value,
} from "@cerbos/core";

import type { MongooseFilter } from "./index";
import { getOperandAt, isExpression, isVariable } from "./operands";

// Operators whose second operand is a lambda that binds an iteration variable.
export const LAMBDA_BINDING_OPERATORS = new Set([
  "exists",
  "exists_one",
  "all",
  "filter",
  "map",
  "except",
]);

/**
 * Substitute a lambda iteration variable with a concrete collection element
 * inside a lambda body. A bare reference to the variable becomes the element
 * itself; a `variable.path.to.field` reference drills into the element. A
 * nested collection macro whose lambda rebinds the same variable name shadows
 * the outer variable, so substitution only descends into its collection
 * operand.
 */
export const substituteLambdaVariable = (
  operand: PlanExpressionOperand,
  variableName: string,
  element: Value,
): PlanExpressionOperand => {
  if (isVariable(operand)) {
    if (operand.name === variableName) {
      return { value: element };
    }
    if (operand.name.startsWith(`${variableName}.`)) {
      let current: Value = element;
      for (const segment of operand.name
        .slice(variableName.length + 1)
        .split(".")) {
        if (
          current === null ||
          typeof current !== "object" ||
          Array.isArray(current) ||
          !(segment in current)
        ) {
          throw new Error(
            `Cannot resolve "${operand.name}": collection element has no field "${segment}"`,
          );
        }
        current = current[segment] as Value;
      }
      return { value: current };
    }
    return operand;
  }

  if (isExpression(operand)) {
    if (
      LAMBDA_BINDING_OPERATORS.has(operand.operator) &&
      operand.operands.length === 2
    ) {
      const [nestedCollection, nestedLambda] = operand.operands;
      if (
        nestedCollection !== undefined &&
        nestedLambda !== undefined &&
        isExpression(nestedLambda) &&
        nestedLambda.operator === "lambda"
      ) {
        const nestedVariable = nestedLambda.operands[1];
        if (
          nestedVariable !== undefined &&
          isVariable(nestedVariable) &&
          nestedVariable.name === variableName
        ) {
          // The nested lambda shadows our variable: substitute only in the
          // collection operand.
          return {
            operator: operand.operator,
            operands: [
              substituteLambdaVariable(nestedCollection, variableName, element),
              nestedLambda,
            ],
          };
        }
      }
    }
    return {
      operator: operand.operator,
      operands: operand.operands.map((o) =>
        substituteLambdaVariable(o, variableName, element),
      ),
    };
  }

  return operand;
};
/**
 * Fold a collection macro whose collection operand is a literal value list.
 *
 * The planner emits this shape when a known-value collection (typically a
 * folded principal attribute) has more than 10 elements — at 10 or fewer it
 * unrolls `exists`/`all` into an or/and chain itself (cerbos/cerbos#2570,
 * cerbos/cerbos#2817; `maxItems = 10` in the planner's struct matcher). Apply
 * the same fold here so the translated filter does not depend on which side of
 * that threshold the collection lands: substitute each element into the lambda
 * body and combine the per-element filters with `$or` (`exists`) or `$and`
 * (`all`).
 *
 * A literal collection has no relation to `$elemMatch` against, so this is the
 * only translation available — and the only one needed, since every element is
 * already known at plan time.
 */
export const foldLiteralCollection = (
  operator: string,
  collection: PlanExpressionValue,
  lambda: PlanExpressionOperand,
  translate: (body: PlanExpressionOperand) => MongooseFilter,
): MongooseFilter => {
  if (operator !== "exists" && operator !== "all") {
    throw new Error(
      `${operator} over a literal collection value is not supported. ` +
        "Only exists() and all() can be folded into a flat filter.",
    );
  }

  const elements = collection.value;
  if (!Array.isArray(elements)) {
    throw new Error(
      `${operator} over a literal collection requires a list value`,
    );
  }

  if (!isExpression(lambda) || lambda.operator !== "lambda") {
    throw new Error(
      `Second operand of ${operator} must be a lambda expression`,
    );
  }
  if (lambda.operands.length !== 2) {
    throw new Error(
      `${operator} over a literal collection supports single-variable lambdas only`,
    );
  }

  const body = getOperandAt(
    lambda.operands,
    0,
    "Lambda expression must provide a condition",
  );
  const variable = getOperandAt(
    lambda.operands,
    1,
    "Lambda variable must have a name",
  );
  if (!isVariable(variable)) {
    throw new Error("Lambda variable must have a name");
  }

  if (elements.length === 0) {
    // CEL identity semantics over an empty collection: exists() matches
    // nothing, all() matches everything. MongoDB rejects an empty `$or`/`$and`,
    // so state the constant directly.
    return { $expr: operator === "all" };
  }

  // `translate` carries the enclosing collection scope through unchanged: a fold
  // nested inside another macro's lambda must keep rejecting outer-document
  // references exactly as the equivalent or/and chain would.
  const filters = elements.map((element) =>
    translate(substituteLambdaVariable(body, variable.name, element)),
  );

  return operator === "exists" ? { $or: filters } : { $and: filters };
};
