// `cond ? a : b`, in boolean position and as an operand of a comparison. Both lower to the same
// guarded disjunction, which keeps CEL's error semantics when the condition is UNKNOWN.

import type { PlanExpressionOperand } from "@cerbos/core";

import { handleRelationalOperator } from "./comparison";
import { evaluateConstantComparison } from "./evaluate";
import { rejectConstantFalse } from "./fields";
import type { PrismaFilter } from "./index";
import type { TranslationContext } from "./mapping";
import {
  CERBOS_TO_PRISMA_OPERATOR,
  assertDefined,
  isOperatorOperand,
  isValueOperand,
  normalizeBinaryOperands,
} from "./plan";
import {
  buildNegatedFilter,
  buildPrismaFilterFromCerbosExpression,
} from "./translate";

/** What one ternary branch contributes: a known constant, or a filter to be guarded. */
type TernaryBranchPredicate =
  | { kind: "constant"; value: boolean }
  | { kind: "filter"; filter: PrismaFilter };

// The comparison that holds exactly when `operator` does not, over two definite operands.
const COMPLEMENT_OPERATOR: Record<string, string> = {
  eq: "ne",
  ne: "eq",
  lt: "ge",
  le: "gt",
  gt: "le",
  ge: "lt",
};

function getTernaryOperands(operands: PlanExpressionOperand[]): {
  condition: PlanExpressionOperand;
  thenBranch: PlanExpressionOperand;
  elseBranch: PlanExpressionOperand;
} {
  if (operands.length !== 3) {
    throw new Error(
      `if (ternary) requires exactly 3 operands (condition, then, else), got ${operands.length}`
    );
  }

  return {
    condition: assertDefined(
      operands[0],
      "if (ternary) requires a condition operand"
    ),
    thenBranch: assertDefined(
      operands[1],
      "if (ternary) requires a then operand"
    ),
    elseBranch: assertDefined(
      operands[2],
      "if (ternary) requires an else operand"
    ),
  };
}

function getConstantBooleanCondition(
  condition: PlanExpressionOperand
): boolean | undefined {
  if (!isValueOperand(condition)) {
    return undefined;
  }
  if (typeof condition.value !== "boolean") {
    throw new Error("if (ternary) condition must be a boolean expression");
  }
  return condition.value;
}

function buildBooleanBranchFilter(
  branch: PlanExpressionOperand,
  context: TranslationContext
): TernaryBranchPredicate {
  if (!isValueOperand(branch)) {
    return {
      kind: "filter",
      filter: buildPrismaFilterFromCerbosExpression(branch, context),
    };
  }
  if (typeof branch.value !== "boolean") {
    throw new Error(
      "if (ternary) branch in boolean position must be a boolean"
    );
  }
  return { kind: "constant", value: branch.value };
}

/**
 * `(cond AND then) OR (NOT cond AND else) OR (cond AND NOT cond)`, dropping any arm a constant
 * branch makes redundant.
 *
 * "The condition is definitively FALSE" is buildNegatedFilter's contract — three-valued, so a
 * condition that is UNKNOWN (a NULL column, an erroring lambda body, an absent to-one parent)
 * selects NEITHER branch. It delegates rather than spelling a second negation. A private copy is
 * how the ternary missed the hop requirement #315/#316 added: a bare `NOT` over a chain filter is
 * TRUE when the to-one parent is absent, so the else-branch was selected for rows CEL denies
 * outright — the missing path is an evaluation error, not a false condition
 * (cerbos/query-plan-adapters#334). Delegating also gives the ternary the De Morgan push-down,
 * which a chained `and`/`or` condition needs for CEL's error absorption to survive.
 */
function buildGuardedTernaryFilter(
  condition: PlanExpressionOperand,
  thenFilter: TernaryBranchPredicate,
  elseFilter: TernaryBranchPredicate,
  context: TranslationContext
): PrismaFilter {
  const conditionTrue = buildPrismaFilterFromCerbosExpression(
    condition,
    context
  );
  const conditionFalse = buildNegatedFilter(condition, context);
  const guardedBranches: PrismaFilter[] = [];

  if (thenFilter.kind === "filter") {
    guardedBranches.push({ AND: [conditionTrue, thenFilter.filter] });
  } else if (thenFilter.value) {
    guardedBranches.push(conditionTrue);
  }

  if (elseFilter.kind === "filter") {
    guardedBranches.push({ AND: [conditionFalse, elseFilter.filter] });
  } else if (elseFilter.value) {
    guardedBranches.push(conditionFalse);
  }

  // For a known condition this contradiction is false. If the condition is
  // NULL/error-derived, both sides are SQL UNKNOWN, which keeps the whole
  // ternary UNKNOWN under an outer NOT instead of authorizing the row.
  guardedBranches.push({ AND: [conditionTrue, conditionFalse] });

  return { OR: guardedBranches };
}

function finalizeTernaryBranchPredicate(
  predicate: TernaryBranchPredicate
): PrismaFilter {
  if (predicate.kind === "filter") {
    return predicate.filter;
  }
  return predicate.value ? {} : rejectConstantFalse();
}

export function handleBooleanTernaryOperator(
  operands: PlanExpressionOperand[],
  context: TranslationContext
): PrismaFilter {
  const { condition, thenBranch, elseBranch } = getTernaryOperands(operands);
  const constantCondition = getConstantBooleanCondition(condition);
  if (constantCondition !== undefined) {
    return finalizeTernaryBranchPredicate(
      buildBooleanBranchFilter(
        constantCondition ? thenBranch : elseBranch,
        context
      )
    );
  }

  return buildGuardedTernaryFilter(
    condition,
    buildBooleanBranchFilter(thenBranch, context),
    buildBooleanBranchFilter(elseBranch, context),
    context
  );
}

/** The comparison with its ternary operand replaced by `branch`, as a branch predicate. */
function buildTernaryComparisonBranch(
  operator: string,
  operands: PlanExpressionOperand[],
  ternaryIndex: number,
  branch: PlanExpressionOperand,
  context: TranslationContext,
  negated: boolean
): TernaryBranchPredicate {
  const substitutedOperands = operands.map((operand, index) =>
    index === ternaryIndex ? branch : operand
  );
  assertDefined(substitutedOperands[0], `${operator} requires a left operand`);
  assertDefined(substitutedOperands[1], `${operator} requires a right operand`);

  const normalized = normalizeBinaryOperands(operator, substitutedOperands);

  const [normalizedFirst, normalizedSecond] = normalized.operands;
  if (
    normalized.operands.length === 2 &&
    normalizedFirst !== undefined &&
    normalizedSecond !== undefined &&
    isValueOperand(normalizedFirst) &&
    isValueOperand(normalizedSecond)
  ) {
    return {
      kind: "constant",
      value:
        evaluateConstantComparison(
          normalized.operator,
          normalizedFirst.value,
          normalizedSecond.value
        ) !== negated,
    };
  }

  if (!negated) {
    return {
      kind: "filter",
      filter: buildPrismaFilterFromCerbosExpression(normalized, context),
    };
  }

  const nested = tryHandleTernaryComparison(
    normalized.operator,
    normalized.operands,
    context,
    true
  );
  if (nested !== null) return { kind: "filter", filter: nested };
  // Keep missing-value and relation guards inside the selected branch. Constants
  // are negated above, since !(NaN <= n) is true but NaN > n is false in CEL 0.30.
  return {
    kind: "filter",
    filter: handleRelationalOperator(
      assertDefined(
        COMPLEMENT_OPERATOR[normalized.operator],
        "Unsupported negated comparison"
      ),
      normalized.operands,
      context
    ),
  };
}

/**
 * A comparison with a ternary operand (`(c ? a : b) == x`), translated by distributing the
 * comparison into both branches; or null when `operator` is not a comparison or no operand is a
 * ternary. `negated` builds the filter for the comparison's negation instead.
 */
export function tryHandleTernaryComparison(
  operator: string,
  operands: PlanExpressionOperand[],
  context: TranslationContext,
  negated = false
): PrismaFilter | null {
  if (CERBOS_TO_PRISMA_OPERATOR[operator] === undefined) {
    return null;
  }

  const ternaryIndex = operands.findIndex(
    (operand) => isOperatorOperand(operand) && operand.operator === "if"
  );
  if (ternaryIndex === -1) {
    return null;
  }
  if (operands.length !== 2) {
    throw new Error(
      `${operator} with a ternary requires exactly 2 operands, got ${operands.length}`
    );
  }

  const ternary = assertDefined(
    operands[ternaryIndex],
    "Ternary comparison operand is missing"
  );
  if (!isOperatorOperand(ternary)) {
    throw new Error("Ternary comparison operand must be an expression");
  }

  const { condition, thenBranch, elseBranch } = getTernaryOperands(
    ternary.operands
  );
  const branchFor = (branch: PlanExpressionOperand) =>
    buildTernaryComparisonBranch(
      operator,
      operands,
      ternaryIndex,
      branch,
      context,
      negated
    );

  const constantCondition = getConstantBooleanCondition(condition);
  if (constantCondition !== undefined) {
    return finalizeTernaryBranchPredicate(
      branchFor(constantCondition ? thenBranch : elseBranch)
    );
  }

  return buildGuardedTernaryFilter(
    condition,
    branchFor(thenBranch),
    branchFor(elseBranch),
    context
  );
}
