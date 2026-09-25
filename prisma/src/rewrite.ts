// Plan-to-plan rewrites that run before translation. None of them knows about Prisma: each takes
// an expression tree and returns an equivalent one that the translator can lower more directly.

import type { PlanExpressionOperand, Value } from "@cerbos/core";

import {
  evaluateConstantComparison,
  foldArithmetic,
} from "./evaluate";
import {
  COMPARISON_OPERATORS,
  assertDefined,
  assertLogicalOperands,
  isNamedOperand,
  isOperatorOperand,
  isValueOperand,
} from "./plan";
import type { NamedOperand, OperatorOperand } from "./plan";
import { UnsupportedQueryPlanError } from "./errors";

/** The boolean collection macros: each takes a collection and a lambda. */
export const COLLECTION_OPERATORS = new Set([
  "exists",
  "exists_one",
  "all",
  "except",
  "filter",
]);

// Operators whose second operand is a lambda that binds an iteration variable.
const LAMBDA_BINDING_OPERATORS = new Set([...COLLECTION_OPERATORS, "map"]);

export function containsCollectionOperator(expr: PlanExpressionOperand): boolean {
  if (!isOperatorOperand(expr)) {
    return false;
  }
  if (COLLECTION_OPERATORS.has(expr.operator)) {
    return true;
  }
  return expr.operands.some(containsCollectionOperator);
}

/**
 * Substitute a lambda iteration variable with a concrete collection element
 * inside a lambda body. A bare reference to the variable becomes the element
 * itself; a `variable.path.to.field` reference drills into the element. A
 * nested collection macro whose lambda rebinds the same variable name shadows
 * the outer variable, so substitution only descends into its collection
 * operand.
 */
export function substituteLambdaVariable(
  operand: PlanExpressionOperand,
  variableName: string,
  element: Value
): PlanExpressionOperand {
  if (isNamedOperand(operand)) {
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
          !(segment in current) ||
          current[segment] === undefined
        ) {
          throw new UnsupportedQueryPlanError(
            `Cannot resolve "${operand.name}": collection element has no field "${segment}"`
          );
        }
        current = current[segment];
      }
      return { value: current };
    }
    return operand;
  }

  if (!isOperatorOperand(operand)) {
    return operand;
  }

  const [nestedCollection, nestedLambda] = operand.operands;
  if (
    LAMBDA_BINDING_OPERATORS.has(operand.operator) &&
    operand.operands.length === 2 &&
    nestedCollection !== undefined &&
    nestedLambda !== undefined &&
    isOperatorOperand(nestedLambda) &&
    nestedLambda.operator === "lambda"
  ) {
    const nestedVariable = nestedLambda.operands[1];
    if (
      nestedVariable !== undefined &&
      isNamedOperand(nestedVariable) &&
      nestedVariable.name === variableName
    ) {
      // The nested lambda shadows our variable: substitute only in the collection operand.
      return {
        operator: operand.operator,
        operands: [
          substituteLambdaVariable(nestedCollection, variableName, element),
          nestedLambda,
        ],
      };
    }
  }
  return {
    operator: operand.operator,
    operands: operand.operands.map((o) =>
      substituteLambdaVariable(o, variableName, element)
    ),
  };
}

function mentionsVariable(
  expr: PlanExpressionOperand,
  variableName: string
): boolean {
  if (isNamedOperand(expr)) {
    return (
      expr.name === variableName || expr.name.startsWith(variableName + ".")
    );
  }
  if (isOperatorOperand(expr)) {
    return expr.operands.some((operand) =>
      mentionsVariable(operand, variableName)
    );
  }
  return false;
}

function isOuterScopeName(name: string, enclosingVariables: string[]): boolean {
  return !enclosingVariables.some(
    (variable) => name === variable || name.startsWith(variable + ".")
  );
}

/** Replaces every occurrence of the bare named operand `name` with a boolean constant. */
function substituteNamedOperand(
  expr: PlanExpressionOperand,
  name: string,
  value: boolean
): PlanExpressionOperand {
  if (isNamedOperand(expr)) {
    return expr.name === name ? { value } : expr;
  }
  if (isOperatorOperand(expr)) {
    return {
      operator: expr.operator,
      operands: expr.operands.map((operand) =>
        substituteNamedOperand(operand, name, value)
      ),
    };
  }
  return expr;
}

const BOOLEAN_POSITION_OPERATORS = new Set(["and", "or", "not", "if"]);

/**
 * Finds a bare boolean reference to an OUTER column (root or enclosing lambda scope) in a
 * boolean position of a lambda body: a direct operand of and/or/not, a ternary condition, or
 * the body itself.
 */
function findOuterBooleanReference(
  expr: PlanExpressionOperand,
  enclosingVariables: string[]
): string | undefined {
  if (isNamedOperand(expr)) {
    return isOuterScopeName(expr.name, enclosingVariables)
      ? expr.name
      : undefined;
  }
  if (!isOperatorOperand(expr)) {
    return undefined;
  }
  const booleanPositions = BOOLEAN_POSITION_OPERATORS.has(expr.operator)
    ? expr.operands
    : // Comparisons wrapping a ternary keep the outer reference in the ternary condition.
      expr.operands.filter(
        (operand) =>
          isOperatorOperand(operand) &&
          BOOLEAN_POSITION_OPERATORS.has(operand.operator)
      );
  for (const operand of booleanPositions) {
    const found = findOuterBooleanReference(operand, enclosingVariables);
    if (found !== undefined) {
      return found;
    }
  }
  return undefined;
}

/**
 * The parts of `macro(collection, lambda(body, variable))` when `expr` is a boolean collection
 * macro over a single-variable lambda, or undefined for any other shape.
 */
function parseLambdaMacro(expr: OperatorOperand):
  | {
      collection: PlanExpressionOperand;
      lambda: OperatorOperand;
      variable: NamedOperand;
    }
  | undefined {
  const [collection, lambda] = expr.operands;
  if (
    !COLLECTION_OPERATORS.has(expr.operator) ||
    expr.operands.length !== 2 ||
    collection === undefined ||
    lambda === undefined ||
    !isOperatorOperand(lambda) ||
    lambda.operator !== "lambda" ||
    lambda.operands.length !== 2
  ) {
    return undefined;
  }
  const variable = lambda.operands[1];
  if (variable === undefined || !isNamedOperand(variable)) {
    return undefined;
  }
  return { collection, lambda, variable };
}

/**
 * Rewrites lambda bodies that reference columns of an ENCLOSING scope (the root row, or an
 * outer lambda's element) so that every filter lands on the model it belongs to. Without this,
 * an outer column referenced inside `tags.exists(t, ...)` would be emitted as a field of the
 * tag model. Two sound transforms, applied bottom-up:
 *
 * - Conjunct hoisting (exists only): `exists(c, P(c) && Q)` with row-constant Q becomes
 *   `exists(c, P(c)) && Q`. Valid in three-valued logic (AND distributes over the per-element
 *   OR), including the empty-collection case (both sides are FALSE).
 * - Case split (any collection op): a bare outer boolean reference Q in a boolean position
 *   becomes `(Q && op[Q:=true]) || (!Q && op[Q:=false]) || (Q && !Q)`. The contradiction arm
 *   keeps the whole expression UNKNOWN (not FALSE) when Q is NULL-derived, mirroring the
 *   guarded-ternary encoding.
 */
export function hoistOuterScopeReferences(
  expr: PlanExpressionOperand,
  enclosingVariables: string[]
): PlanExpressionOperand {
  if (!isOperatorOperand(expr)) {
    return expr;
  }

  const macro = parseLambdaMacro(expr);
  if (macro === undefined) {
    return {
      operator: expr.operator,
      operands: expr.operands.map((operand) =>
        hoistOuterScopeReferences(operand, enclosingVariables)
      ),
    };
  }
  const { collection, lambda, variable } = macro;

  const scopeVariables = [...enclosingVariables, variable.name];
  const body = hoistOuterScopeReferences(
    assertDefined(lambda.operands[0], "Lambda requires a condition"),
    scopeVariables
  );

  const rebuild = (newBody: PlanExpressionOperand): OperatorOperand => ({
    operator: expr.operator,
    operands: [collection, { operator: "lambda", operands: [newBody, variable] }],
  });

  // Conjunct hoisting out of exists().
  if (
    expr.operator === "exists" &&
    isOperatorOperand(body) &&
    body.operator === "and"
  ) {
    const elementConjuncts = body.operands.filter((operand) =>
      mentionsVariable(operand, variable.name)
    );
    const outerConjuncts = body.operands.filter(
      (operand) => !mentionsVariable(operand, variable.name)
    );
    if (outerConjuncts.length > 0 && elementConjuncts.length > 0) {
      const innerBody =
        elementConjuncts.length === 1
          ? elementConjuncts[0]!
          : { operator: "and", operands: elementConjuncts };
      return {
        operator: "and",
        operands: [rebuild(innerBody), ...outerConjuncts],
      };
    }
  }

  // Case split on a bare outer boolean reference.
  const outerRef = findOuterBooleanReference(body, scopeVariables);
  if (outerRef !== undefined) {
    const q: PlanExpressionOperand = { name: outerRef };
    const notQ: PlanExpressionOperand = { operator: "not", operands: [q] };
    const arms: PlanExpressionOperand[] = [
      {
        operator: "and",
        operands: [q, rebuild(substituteNamedOperand(body, outerRef, true))],
      },
      {
        operator: "and",
        operands: [notQ, rebuild(substituteNamedOperand(body, outerRef, false))],
      },
      // Contradiction arm: UNKNOWN when the reference is NULL-derived, never TRUE.
      { operator: "and", operands: [q, notQ] },
    ];
    // all() over an empty collection never evaluates its body, so CEL holds it TRUE even when
    // the reference is missing; the arms above are all UNKNOWN on that row. `all(c, false)` is
    // TRUE exactly when the collection is empty, and definite, so it restores that row without
    // touching any other (#488).
    if (expr.operator === "all") {
      arms.push(rebuild({ value: false }));
    }
    return { operator: "or", operands: arms };
  }

  return rebuild(body);
}

/**
 * `and`/`or` over already-folded operands: a dominating constant decides the whole expression,
 * and the other constants drop out.
 */
function foldLogical(
  operator: "and" | "or",
  operands: PlanExpressionOperand[]
): PlanExpressionOperand {
  const dominant = operator === "or";
  if (operands.some((o) => isValueOperand(o) && o.value === dominant)) {
    return { value: dominant };
  }
  const remaining = operands.filter((o) => !isValueOperand(o));
  if (remaining.length === 0) return { value: !dominant };
  if (remaining.length === 1) return remaining[0]!;
  return { operator, operands: remaining };
}

/**
 * Bottom-up constant folding over the plan AST. Only shapes the case-split transform can
 * produce need folding (constant ternary conditions, comparisons between two constants,
 * logical operators with constant operands, collection macros with constant bodies); real
 * planner output arrives pre-folded.
 */
export function constantFoldExpression(
  expr: PlanExpressionOperand
): PlanExpressionOperand {
  if (!isOperatorOperand(expr)) {
    return expr;
  }
  assertLogicalOperands(expr.operator, expr.operands);
  const operands = expr.operands.map(constantFoldExpression);
  const folded: OperatorOperand = { operator: expr.operator, operands };
  const [first, second] = operands;
  const bothConstant =
    first !== undefined &&
    second !== undefined &&
    isValueOperand(first) &&
    isValueOperand(second);

  switch (expr.operator) {
    case "if": {
      const [condition, thenBranch, elseBranch] = operands;
      if (condition !== undefined && isValueOperand(condition)) {
        if (typeof condition.value !== "boolean") {
          throw new UnsupportedQueryPlanError("if (ternary) condition must be a boolean expression");
        }
        return assertDefined(
          condition.value ? thenBranch : elseBranch,
          "if (ternary) requires branch operands"
        );
      }
      return folded;
    }
    case "and":
    case "or":
      return foldLogical(expr.operator, operands);
    case "not":
      if (first !== undefined && isValueOperand(first)) {
        return { value: first.value !== true };
      }
      return folded;
    case "add":
    case "sub":
    case "mult":
    case "div":
      return bothConstant
        ? { value: foldArithmetic(expr.operator, first.value, second.value) }
        : folded;
    case "contains":
    case "startsWith":
    case "endsWith": {
      if (
        bothConstant &&
        typeof first.value === "string" &&
        typeof second.value === "string"
      ) {
        const receiver = first.value;
        const needle = second.value;
        return {
          value:
            expr.operator === "contains"
              ? receiver.includes(needle)
              : expr.operator === "startsWith"
                ? receiver.startsWith(needle)
                : receiver.endsWith(needle),
        };
      }
      return folded;
    }
    case "exists":
    case "all": {
      const lambda = second;
      if (
        first !== undefined &&
        lambda !== undefined &&
        isOperatorOperand(lambda) &&
        lambda.operator === "lambda" &&
        lambda.operands[0] !== undefined &&
        isValueOperand(lambda.operands[0])
      ) {
        const bodyValue = lambda.operands[0].value === true;
        if (expr.operator === "exists" && !bodyValue) return { value: false };
        if (expr.operator === "all" && bodyValue) return { value: true };
        const sizeExpr: PlanExpressionOperand = {
          operator: "size",
          operands: [first],
        };
        // exists(c, true) is "collection is non-empty"; all(c, false) is "collection is empty".
        return expr.operator === "exists"
          ? { operator: "gt", operands: [sizeExpr, { value: 0 }] }
          : { operator: "eq", operands: [sizeExpr, { value: 0 }] };
      }
      return folded;
    }
    default:
      if (
        COMPARISON_OPERATORS.has(expr.operator) &&
        bothConstant
      ) {
        return {
          value: evaluateConstantComparison(
            expr.operator,
            first.value,
            second.value
          ),
        };
      }
      return folded;
  }
}
