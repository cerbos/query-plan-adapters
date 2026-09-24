// The translator's core: one dispatch from a plan operator to its handler, the negation that
// every handler shares, and the resolution of an operand into a column or a constant.

import type { PlanExpressionOperand, Value } from "@cerbos/core";

import {
  buildNegatedCollectionFilter,
  handleCollectionOperator,
  handleHasIntersectionOperator,
  handleLambdaOperator,
  handleMapOperator,
} from "./collections";
import { handleInOperator, handleRelationalOperator } from "./comparison";
import { ARITHMETIC_OPERATORS, foldArithmetic } from "./evaluate";
import { buildFieldFilter } from "./fields";
import {
  handleAncestorDescendantOperator,
  handleOverlapsOperator,
} from "./hierarchy";
import type { PrismaFilter } from "./index";
import { isResolvedValue, resolveFieldReference } from "./mapping";
import type { ResolvedOperand, TranslationContext } from "./mapping";
import {
  assertDefined,
  assertLogicalOperands,
  isNamedOperand,
  isOperatorOperand,
  isValueOperand,
} from "./plan";
import type { OperatorOperand } from "./plan";
import { negateRequiringHops, referencesChainedRelation } from "./relations";
import { containsCollectionOperator } from "./rewrite";
import { handleStringOperator } from "./strings";
import { handleBooleanTernaryOperator, tryHandleTernaryComparison } from "./ternary";
import { normalizeRfc3339Milliseconds } from "./timestamp";
import { UnsupportedQueryPlanError } from "./errors";

/**
 * Builds a Prisma filter from a Cerbos expression.
 */
export function buildPrismaFilterFromCerbosExpression(
  expression: PlanExpressionOperand,
  context: TranslationContext
): PrismaFilter {
  // A bare named operand represents a boolean field reference (e.g. `R.attr.booleanAttr`)
  if (isNamedOperand(expression)) {
    return buildFieldFilter(
      resolveFieldReference(expression.name, context),
      "equals",
      true
    );
  }

  if (!isOperatorOperand(expression)) {
    throw new UnsupportedQueryPlanError("Invalid Cerbos expression structure");
  }

  // Every plan operator this adapter translates. Adding an operator is adding a case here;
  // anything else is refused as unsupported.
  const { operator, operands } = expression;
  assertLogicalOperands(operator, operands);
  switch (operator) {
    case "and":
      return {
        AND: operands.map((operand) =>
          buildPrismaFilterFromCerbosExpression(operand, context)
        ),
      };
    case "or":
      return {
        OR: operands.map((operand) =>
          buildPrismaFilterFromCerbosExpression(operand, context)
        ),
      };
    case "not": {
      const operand = operands[0];
      if (!operand) {
        throw new UnsupportedQueryPlanError("not operator requires an operand");
      }
      return buildNegatedFilter(operand, context);
    }
    case "if":
      return handleBooleanTernaryOperator(operands, context);
    case "eq":
    case "ne":
    case "lt":
    case "le":
    case "gt":
    case "ge":
      return handleRelationalOperator(operator, operands, context);
    case "in":
      return handleInOperator(operands, context);
    case "contains":
    case "startsWith":
    case "endsWith":
      return handleStringOperator(operator, operands, context);
    case "hasIntersection":
      return handleHasIntersectionOperator(operands, context);
    case "lambda":
      return handleLambdaOperator(operands, context);
    case "exists":
    case "exists_one":
    case "all":
    case "except":
    case "filter":
      return handleCollectionOperator(operator, operands, context);
    case "map":
      return handleMapOperator(operands, context);
    case "overlaps":
      return handleOverlapsOperator(operands, context);
    case "ancestorOf":
      return handleAncestorDescendantOperator(operands, context, "ancestor");
    case "descendentOf":
      return handleAncestorDescendantOperator(operands, context, "descendant");
    default:
      throw new UnsupportedQueryPlanError(`Unsupported operator: ${operator}`);
  }
}

/**
 * Builds the filter for `!expr`. Plain field predicates negate correctly with Prisma's NOT
 * (SQL three-valued logic keeps NULL rows excluded under both polarities), but relation
 * subqueries collapse UNKNOWN to false at the EXISTS boundary, so `NOT(some(P))` would
 * wrongly include rows whose elements make P UNKNOWN (a NULL element column is a missing
 * attribute — a CEL error — on the check side, which Cerbos treats as deny). Negation is
 * therefore pushed down to the collection operators, which encode CEL's exact error
 * semantics:
 *
 * - exists(c,P) is TRUE with a true witness, FALSE if every element is definitively false,
 *   and an ERROR otherwise — so `!exists` admits only rows with no P-match AND no
 *   UNKNOWN-element (the nullable-field guard).
 * - all(c,P) is FALSE only with a definitive false witness (a false witness absorbs error
 *   elements) — so `!all` is `some(NOT P)`, which SQL already evaluates definitively.
 */
export function buildNegatedFilter(
  operand: PlanExpressionOperand,
  context: TranslationContext
): PrismaFilter {
  if (isOperatorOperand(operand)) {
    if (operand.operator === "not") {
      return buildPrismaFilterFromCerbosExpression(
        assertDefined(operand.operands[0], "not requires an operand"),
        context
      );
    }
    const ternary = tryHandleTernaryComparison(
      operand.operator,
      operand.operands,
      context,
      true
    );
    if (ternary !== null) return ternary;
  }
  if (isNamedOperand(operand)) {
    const { relations, ...fieldRef } = resolveFieldReference(
      operand.name,
      context
    );
    if (!relations || relations.length === 0) {
      return buildFieldFilter(fieldRef, "equals", false);
    }
    return negateRequiringHops(
      operand,
      buildPrismaFilterFromCerbosExpression(operand, context),
      context
    );
  }

  // `and`/`or` must be pushed through with De Morgan whenever a branch can be UNKNOWN, so
  // CEL's error absorption survives: `!(A && B)` with A erroring and B false is TRUE in CEL,
  // and `OR[!A, !B]` reproduces that where a single outer NOT over the conjunction — with the
  // hop requirement ANDed outside it — would deny. A chained relation is the second source of
  // UNKNOWN besides the collection macros, so it opens the same push-down. (A nested `not` was
  // already unwrapped above.)
  if (
    isOperatorOperand(operand) &&
    (containsCollectionOperator(operand) ||
      referencesChainedRelation(operand, context))
  ) {
    switch (operand.operator) {
      case "and":
        return {
          OR: operand.operands.map((o) => buildNegatedFilter(o, context)),
        };
      case "or":
        return {
          AND: operand.operands.map((o) => buildNegatedFilter(o, context)),
        };
      case "exists":
      case "all":
      case "except":
      case "exists_one":
      case "filter":
        return buildNegatedCollectionFilter(
          operand.operator,
          operand.operands,
          context
        );
    }
  }

  return negateRequiringHops(
    operand,
    buildPrismaFilterFromCerbosExpression(operand, context),
    context
  );
}

/**
 * Resolves a plan operand into a column reference or a constant. A nested expression that is
 * neither a timestamp nor foldable arithmetic resolves to the filter it translates to.
 */
export function resolveOperand(
  operand: PlanExpressionOperand,
  context: TranslationContext
): ResolvedOperand {
  if (isNamedOperand(operand)) {
    return resolveFieldReference(operand.name, context);
  }
  if (isValueOperand(operand)) {
    return { value: operand.value };
  }
  if (isOperatorOperand(operand)) {
    if (operand.operator === "timestamp") {
      return resolveTimestampOperand(operand, context);
    }
    const folded = tryFoldValueExpression(operand, context);
    if (folded !== null) return { value: folded };
    return { value: buildPrismaFilterFromCerbosExpression(operand, context) };
  }
  throw new UnsupportedQueryPlanError("Operand must have name, value, or be an expression");
}

function resolveTimestampOperand(
  expression: OperatorOperand,
  context: TranslationContext
): ResolvedOperand {
  if (expression.operands.length !== 1) {
    throw new UnsupportedQueryPlanError("timestamp() requires exactly one operand");
  }

  const operand = assertDefined(
    expression.operands[0],
    "timestamp() requires an operand"
  );
  if (isNamedOperand(operand)) {
    const fieldRef = resolveFieldReference(operand.name, context);
    if (fieldRef.valueType !== "dateTime") {
      throw new UnsupportedQueryPlanError(
        `timestamp() field ${operand.name} must be mapped with valueType: \"dateTime\"`
      );
    }
    return { ...fieldRef, timestampWrapped: true };
  }

  if (!isValueOperand(operand) || typeof operand.value !== "string") {
    throw new UnsupportedQueryPlanError("timestamp() requires a field reference or RFC 3339 string");
  }
  return { value: normalizeRfc3339Milliseconds(operand.value) };
}

/** The value of arithmetic over two constant operands, or null when it cannot be folded. */
export function tryFoldValueExpression(
  expr: OperatorOperand,
  context: TranslationContext
): Value | null {
  if (!ARITHMETIC_OPERATORS.has(expr.operator)) return null;
  const leftOp = expr.operands[0];
  const rightOp = expr.operands[1];
  if (!leftOp || !rightOp) return null;

  const left = resolveOperand(leftOp, context);
  if (!isResolvedValue(left)) return null;
  const right = resolveOperand(rightOp, context);
  if (!isResolvedValue(right)) return null;

  try {
    return foldArithmetic(expr.operator, left.value, right.value);
  } catch {
    return null;
  }
}
