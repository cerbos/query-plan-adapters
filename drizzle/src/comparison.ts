import type { PlanExpressionOperand, Value } from "@cerbos/core";
import { and, not, or, sql } from "drizzle-orm";
import type { SQL } from "drizzle-orm";

import {
  evaluateConstantNumberComparison,
  evaluateScalarValueComparison,
  findZeroCapableDivision,
  foldWithSubstitution,
  resolveConstantNumber,
} from "./arithmetic";
import type { LeafComparisonOperator } from "./arithmetic";
import { buildFilterFromExpression } from "./filter";
import { buildIndexedComparison } from "./indexed";
import {
  buildColumnExpression,
  columnForOperand,
  isMappingConfig,
  mappingNullRepresentation,
  resolveFieldReference,
} from "./mapper";
import {
  isExpressionOperand,
  isNameOperand,
  isOperatorCall,
  isStringConversion,
  isValueOperand,
} from "./operands";
import type { ExpressionOperand } from "./operands";
import {
  applyComparison,
  applyComparisonWithExpression,
  buildNullGuard,
  constantCondition,
  operandExpression,
  withPolarity,
} from "./predicates";
import { wrapCombinedRelations, wrapRelationChain } from "./relations";
import { buildValueExpression, resolveScalarOperand } from "./values";
import type { BuildFilterOptions, Mapper } from "./types";

/**
 * `eq`, `ne`, `lt`, `le`, `gt`, `ge` between two operands, each of which may be a field, a
 * constant, arithmetic, a ternary or an index access. The shapes are tried from the most
 * specific to the plain column-against-constant case, which is the last branch.
 */

/** One comparison being translated: its operator and the context every branch needs. */
interface ComparisonContext {
  operator: LeafComparisonOperator;
  mapper: Mapper;
  options: BuildFilterOptions;
  /** The polarity a `not` pushed down to this comparison. */
  negated: boolean;
}

// Mirror map for value-first comparisons: the planner preserves source order, so
// `3 <= R.attr.aNumber` arrives as le(value, variable) and must become `aNumber >= 3`,
// never `aNumber <= 3` (see cerbos/query-plan-adapters#258/#259 for the same bug class
// in other adapters).
const MIRRORED_OPERATORS: Record<LeafComparisonOperator, LeafComparisonOperator> = {
  eq: "eq",
  ne: "ne",
  lt: "gt",
  le: "ge",
  gt: "lt",
  ge: "le",
};

/** `[side, other]` in wire order, given which side of the comparison `side` was on. */
const inWireOrder = <T>(sideIsLeft: boolean, side: T, other: T): [T, T] =>
  sideIsLeft ? [side, other] : [other, side];

/**
 * A comparison against a ternary, distributed over its branches: `(c ? a : b) op x` becomes
 * `(c AND a op x) OR (NOT c AND b op x)`. An UNKNOWN condition leaves both arms UNKNOWN.
 */
const buildTernaryComparison = (
  context: ComparisonContext,
  ternary: ExpressionOperand,
  other: PlanExpressionOperand,
  ternaryIsLeft: boolean,
): SQL => {
  if (ternary.operands.length !== 3) {
    throw new Error("'if' operator requires exactly three operands");
  }
  const [conditionOperand, thenOperand, elseOperand] = ternary.operands;
  if (!conditionOperand || !thenOperand || !elseOperand) {
    throw new Error("'if' operator is missing operands");
  }
  const { operator, mapper, options, negated } = context;
  const condition = buildFilterFromExpression(conditionOperand, mapper, options);
  const compareBranch = (branch: PlanExpressionOperand): SQL =>
    buildComparisonFilter(
      operator,
      ...inWireOrder(ternaryIsLeft, branch, other),
      mapper,
      options,
      negated,
    );
  const thenFilter = compareBranch(thenOperand);
  const elseFilter = compareBranch(elseOperand);
  const combined = or(
    and(condition, thenFilter),
    and(not(condition), elseFilter),
  );
  if (!combined) {
    throw new Error("Unable to combine ternary comparison conditions");
  }
  return combined;
};

/**
 * Fold a division whose denominator may be zero, at the comparison site.
 *
 * CEL attribute arithmetic is double-typed, so `0/0` is NaN and `x/0` is a
 * signed infinity — neither of which SQL can represent. Lowering the division
 * to NULL (SQLite's division-by-zero result) makes every comparison UNKNOWN.
 * That agrees with CEL for ORDERED comparisons, which is why `cr-div-zero`
 * passed, but it silently denies rows an INEQUALITY allows: `NaN != 1.0` is
 * TRUE in CEL while `NULL != 1.0` is UNKNOWN.
 *
 * Keep the three IEEE cases as CASE arms and fold each against the other
 * operand in JavaScript's own IEEE space, so no NaN or Infinity is ever bound
 * as a driver parameter. Comparing a non-finite against the finite sentinel 0
 * gives the same answer as against any finite comparand, which is the same
 * trick `buildDynamicNaNComparison` uses.
 *
 * A NULL numerator, denominator or comparand leaves the result NULL, so the
 * row stays excluded under BOTH polarities — CEL's missing-attribute deny.
 */
const buildDivisionComparison = (
  context: ComparisonContext,
  enclosing: PlanExpressionOperand,
  division: ExpressionOperand,
  other: PlanExpressionOperand,
  divisionIsLeft: boolean,
): SQL => {
  const { operator, mapper, options, negated } = context;
  const [numeratorOperand, denominatorOperand] = division.operands;
  if (!numeratorOperand || !denominatorOperand) {
    throw new Error("'div' operator is missing operands");
  }
  if (
    findZeroCapableDivision(numeratorOperand) ||
    findZeroCapableDivision(denominatorOperand)
  ) {
    throw new Error(
      "Nested division cannot be lowered safely: SQL may evaluate an inner zero divisor before the outer non-finite comparison guards",
    );
  }
  const numerator = buildValueExpression(numeratorOperand, mapper, options);
  const denominator = buildValueExpression(denominatorOperand, mapper, options);
  const otherExpr = buildValueExpression(other, mapper, options);

  // IEEE-754 keeps the sign of a zero, so `n / -0.0` is the OPPOSITE infinity from
  // `n / 0.0`. The planner ships the denominator verbatim (the wire operand is `-0`),
  // so a CONSTANT denominator's sign is knowable and must be applied. A COLUMN
  // denominator is not: SQL cannot tell -0.0 from 0.0 and no portable function reads
  // the sign bit, so the positive-zero reading is assumed and documented.
  const denominatorIsNegativeZero = Object.is(
    resolveConstantNumber(denominatorOperand),
    -0,
  );
  const signed = (infinity: number): number =>
    denominatorIsNegativeZero ? -infinity : infinity;

  // The comparison may sit above the division rather than on it — `div(a, b) + 1 != 2`.
  // Substitute each IEEE outcome for the division and fold the rest of the enclosing
  // expression in JavaScript's own IEEE space, so `NaN + 1.0` stays NaN instead of
  // becoming SQL NULL (which would exclude a row `NaN != 2.0` allows).
  const arm = (nonFinite: number): SQL => {
    const folded = foldWithSubstitution(enclosing, division, nonFinite);
    if (folded === undefined) {
      throw new Error(
        "Cannot translate arithmetic over a division whose denominator may be zero: " +
          "the surrounding expression mixes the non-finite result with a column, and " +
          "SQL has no NaN or Infinity to carry it through",
      );
    }
    const result = evaluateConstantNumberComparison(
      operator,
      ...inWireOrder(divisionIsLeft, folded, 0),
    );
    return result !== negated ? sql`true` : sql`false`;
  };

  const enclosingExpr = buildValueExpression(enclosing, mapper, options);
  const finite = applyComparisonWithExpression(
    operator,
    ...inWireOrder(divisionIsLeft, enclosingExpr, otherExpr),
  );

  // Only the operands that reach a column can be NULL; a constant one is settled here, and
  // asking PostgreSQL `$1 IS NULL` about a bare parameter is an error, not a redundancy.
  const nullGuard = buildNullGuard(
    operandExpression(numerator, numeratorOperand),
    operandExpression(denominator, denominatorOperand),
    operandExpression(otherExpr, other),
  );
  const ieeeArms = sql`
      when ${denominator} = 0 and ${numerator} = 0 then ${arm(Number.NaN)}
      when ${denominator} = 0 and ${numerator} > 0 then ${arm(signed(Number.POSITIVE_INFINITY))}
      when ${denominator} = 0 then ${arm(signed(Number.NEGATIVE_INFINITY))}
      else ${withPolarity(finite, negated)}
    end)`;

  return nullGuard
    ? sql`(case
      when ${nullGuard} then null ${ieeeArms}`
    : sql`(case ${ieeeArms}`;
};

/**
 * A comparison against a NaN constant. Every IEEE comparison with NaN has the same result for
 * every present value: only != is true. Preserve SQL NULL as UNKNOWN so a missing CEL attribute
 * still denies under either polarity, without ever sending a NaN parameter to a driver.
 */
const buildDynamicNaNComparison = (
  context: ComparisonContext,
  dynamicOperand: PlanExpressionOperand,
  nanIsLeft: boolean,
): SQL => {
  const { operator, mapper, options, negated } = context;
  const dynamic = resolveScalarOperand(dynamicOperand, mapper, options);
  const presentResult = evaluateConstantNumberComparison(
    operator,
    ...inWireOrder(nanIsLeft, Number.NaN, 0),
  );
  const presentCondition = presentResult ? sql`true` : sql`false`;
  const filter = sql`(case when ${dynamic.expr} is null then null else ${presentCondition} end)`;
  const reference = isNameOperand(dynamicOperand)
    ? dynamicOperand.name
    : `'${operator}' operand`;
  return withPolarity(
    wrapRelationChain(dynamic.relations, filter, reference, options),
    negated,
  );
};

/**
 * Operands of two different CEL types. CEL's heterogeneous equality makes `eq` false and `ne`
 * true for present values, and an ordering between them is a no-overload error (UNKNOWN). SQL
 * would instead coerce one side — MySQL reads `'true'` as 0 — so the answer is spelled out.
 */
const buildMixedTypeComparison = (
  context: ComparisonContext,
  left: PlanExpressionOperand,
  right: PlanExpressionOperand,
): SQL => {
  const { operator, mapper, options, negated } = context;
  const declaresExplicitNull = (operand: PlanExpressionOperand): boolean =>
    isNameOperand(operand) &&
    mappingNullRepresentation(
      resolveFieldReference(operand.name, mapper).mapping,
    ) === "explicit";
  // A `string()` over a NULL boolean is NULL (`buildBooleanString`): CEL raises there.
  const canBeNull = (operand: PlanExpressionOperand): boolean =>
    isStringConversion(operand) ||
    (isNameOperand(operand) && !declaresExplicitNull(operand));

  const leftResolved = resolveScalarOperand(left, mapper, options);
  const rightResolved = resolveScalarOperand(right, mapper, options);
  const nullGuard = buildNullGuard(
    { expr: leftResolved.expr, canBeNull: canBeNull(left) },
    { expr: rightResolved.expr, canBeNull: canBeNull(right) },
  );
  const bothExplicitNull = [left, right].every(declaresExplicitNull);
  const equality = bothExplicitNull
    ? sql`(${leftResolved.expr} is null and ${rightResolved.expr} is null)`
    : sql`false`;
  const present =
    operator === "eq"
      ? equality
      : operator === "ne"
        ? bothExplicitNull
          ? not(equality)
          : sql`true`
        : // A boolean UNKNOWN on every dialect; bare NULL is inferred as text by
          // PostgreSQL when both CASE arms are NULL, and NOT then rejects it.
          sql`(null = true)`;
  const comparison = nullGuard
    ? sql`(case when ${nullGuard} then null else ${present} end)`
    : present;
  const filter = wrapCombinedRelations(
    comparison,
    leftResolved.relations,
    rightResolved.relations,
    "mixed-type comparison",
    options,
  );
  return withPolarity(filter, negated);
};

/**
 * The CEL type of an operand where the plan or the mapping settles it, or `undefined`. A
 * transform owns its own comparison, so its column's type is not the operand's.
 */
const scalarType = (
  operand: PlanExpressionOperand,
  mapper: Mapper,
): string | undefined => {
  if (isValueOperand(operand)) {
    return operand.value === null ? undefined : typeof operand.value;
  }
  // CEL's `string()` returns a string whatever it converts. Typing it here routes
  // `string(flag) == R.attr.aNumber` to the heterogeneous-equality arm, where MySQL would
  // otherwise coerce `'true'` to 0 and match every row whose number is 0.
  if (isStringConversion(operand)) return "string";
  if (isNameOperand(operand)) {
    const mapping = resolveFieldReference(operand.name, mapper).mapping;
    if (isMappingConfig(mapping) && mapping.transform) return undefined;
  }
  return columnForOperand(operand, mapper)?.dataType;
};

/** Two fields, each possibly behind a relation. */
const buildFieldToFieldComparison = (
  context: ComparisonContext,
  left: { name: string },
  right: { name: string },
): SQL => {
  const { operator, mapper, options } = context;
  const leftResolved = resolveFieldReference(left.name, mapper);
  const rightResolved = resolveFieldReference(right.name, mapper);
  if (
    [leftResolved.mapping, rightResolved.mapping].some(
      (mapping) => isMappingConfig(mapping) && mapping.valueType === "timestamp",
    )
  ) {
    throw new Error(
      "Bare temporal field comparison cannot preserve CEL string equality: SQL timestamp columns discard the original lexical spelling; compare timestamp(...) values instead",
    );
  }
  const comparison = applyComparisonWithExpression(
    operator,
    buildColumnExpression(leftResolved.mapping, left.name),
    buildColumnExpression(rightResolved.mapping, right.name),
    mappingNullRepresentation(leftResolved.mapping) === "explicit",
    mappingNullRepresentation(rightResolved.mapping) === "explicit",
  );
  return wrapCombinedRelations(
    comparison,
    leftResolved.relations,
    rightResolved.relations,
    left.name,
    options,
  );
};

/** A field against a constant, the field on the left (a value-first comparison is mirrored). */
const buildFieldToConstantComparison = (
  context: ComparisonContext,
  operator: LeafComparisonOperator,
  field: { name: string },
  value: { value: Value },
): SQL => {
  const resolved = resolveFieldReference(field.name, context.mapper);
  const comparison = applyComparison(
    resolved.mapping,
    operator,
    value.value,
    context.options,
  );
  return wrapRelationChain(
    resolved.relations,
    comparison,
    field.name,
    context.options,
  );
};

export const buildComparisonFilter = (
  operator: LeafComparisonOperator,
  left: PlanExpressionOperand,
  right: PlanExpressionOperand,
  mapper: Mapper,
  options: BuildFilterOptions,
  negated: boolean,
): SQL => {
  const context: ComparisonContext = { operator, mapper, options, negated };

  const indexed = [left, right].find((operand) =>
    isOperatorCall(operand, "index"),
  );
  if (indexed && isExpressionOperand(indexed)) {
    return buildIndexedComparison(
      operator,
      indexed,
      indexed === left ? right : left,
      mapper,
      options,
      negated,
    );
  }

  if (isOperatorCall(left, "if")) {
    return buildTernaryComparison(context, left, right, true);
  }
  if (isOperatorCall(right, "if")) {
    return buildTernaryComparison(context, right, left, false);
  }

  // A zero denominator is only reachable when it is not a known non-zero constant;
  // otherwise fall through to the plain arithmetic path. The division may be nested inside
  // further arithmetic, so search the whole tree rather than only the comparison operand.
  const leftDivision = findZeroCapableDivision(left);
  const rightDivision = findZeroCapableDivision(right);
  if (leftDivision && rightDivision) {
    // Both sides can go non-finite, and each CASE rewrite only folds its own side — the other
    // would still lower to NULL, turning `NaN != NaN` (TRUE in CEL) into UNKNOWN. Fail closed
    // rather than emit the under-granting filter.
    throw new Error(
      "Cannot translate a comparison with a zero-capable division on BOTH sides: only one " +
        "side can be folded into IEEE arms, and the other would lower to SQL NULL",
    );
  }
  if (leftDivision) {
    return buildDivisionComparison(context, left, leftDivision, right, true);
  }
  if (rightDivision) {
    return buildDivisionComparison(context, right, rightDivision, left, false);
  }

  const leftConstant = resolveConstantNumber(left);
  const rightConstant = resolveConstantNumber(right);
  if (leftConstant !== undefined && rightConstant !== undefined) {
    return constantCondition(
      evaluateConstantNumberComparison(operator, leftConstant, rightConstant) !==
        negated,
    );
  }
  if (
    leftConstant !== undefined &&
    Number.isNaN(leftConstant) &&
    !isValueOperand(right)
  ) {
    return buildDynamicNaNComparison(context, right, true);
  }
  if (
    rightConstant !== undefined &&
    Number.isNaN(rightConstant) &&
    !isValueOperand(left)
  ) {
    return buildDynamicNaNComparison(context, left, false);
  }

  if (isValueOperand(left) && isValueOperand(right)) {
    const result = evaluateScalarValueComparison(
      operator,
      left.value,
      right.value,
    );
    if (result === undefined) {
      throw new Error(
        `'${operator}' cannot compare the provided constant value types`,
      );
    }
    return constantCondition(result !== negated);
  }

  if (
    (isNameOperand(left) || isNameOperand(right)) &&
    [left, right].some(
      (operand) => isValueOperand(operand) && Array.isArray(operand.value),
    )
  ) {
    throw new Error(
      "Whole-list comparison is not supported: a relation mapping exposes element rows, not an ordered list value",
    );
  }

  const leftType = scalarType(left, mapper);
  const rightType = scalarType(right, mapper);
  if (leftType && rightType && leftType !== rightType) {
    return buildMixedTypeComparison(context, left, right);
  }

  let filter: SQL;
  if (isExpressionOperand(left) || isExpressionOperand(right)) {
    filter = applyComparisonWithExpression(
      operator,
      buildValueExpression(left, mapper, options),
      buildValueExpression(right, mapper, options),
    );
  } else if (isNameOperand(left) && isNameOperand(right)) {
    filter = buildFieldToFieldComparison(context, left, right);
  } else if (isNameOperand(left) && isValueOperand(right)) {
    filter = buildFieldToConstantComparison(context, operator, left, right);
  } else if (isValueOperand(left) && isNameOperand(right)) {
    filter = buildFieldToConstantComparison(
      context,
      MIRRORED_OPERATORS[operator],
      right,
      left,
    );
  } else {
    throw new Error(`'${operator}' operator requires field or value operands`);
  }
  return withPolarity(filter, negated);
};
