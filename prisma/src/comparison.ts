// Comparisons (`eq`, `ne`, `lt`, `le`, `gt`, `ge`) and membership (`in`): against a constant,
// between two columns, over size(), and over linear arithmetic solved for the column.

import type { PlanExpressionOperand, Value } from "@cerbos/core";

import { ARITHMETIC_OPERATORS, evaluateConstantComparison, foldArithmetic } from "./evaluate";
import {
  buildComparisonFilter,
  buildFieldFilter,
  buildMembershipFilter,
  rejectConstantFalse,
} from "./fields";
import type { PrismaFilter } from "./index";
import {
  currentScope,
  getLeafField,
  isExplicitNullReference,
  isResolvedFieldReference,
  isResolvedValue,
  resolveFieldReference,
} from "./mapping";
import type { ResolvedFieldReference, TranslationContext } from "./mapping";
import {
  CERBOS_TO_PRISMA_OPERATOR,
  assertDefined,
  isNamedOperand,
  isOperatorOperand,
  isValueOperand,
  mirrorOperator,
  normalizeBinaryOperands,
} from "./plan";
import type { OperatorOperand } from "./plan";
import { relationFilter, wrapInRelations } from "./relations";
import { tryHandleTernaryComparison } from "./ternary";
import { resolveOperand, tryFoldValueExpression } from "./translate";
import { UnsupportedQueryPlanError } from "./errors";

/**
 * Helper function to process relational operators (eq, ne, lt, etc.)
 */
export function handleRelationalOperator(
  operator: string,
  operands: PlanExpressionOperand[],
  context: TranslationContext
): PrismaFilter {
  const ternaryFilter = tryHandleTernaryComparison(operator, operands, context);
  if (ternaryFilter) {
    return ternaryFilter;
  }

  ({ operator, operands } = normalizeBinaryOperands(operator, operands));
  if (!CERBOS_TO_PRISMA_OPERATOR[operator]) {
    throw new UnsupportedQueryPlanError(`Unsupported operator: ${operator}`);
  }

  const leftOperand = operands.find(
    (o) => isNamedOperand(o) || isOperatorOperand(o)
  );
  if (!leftOperand) throw new UnsupportedQueryPlanError("No valid left operand found");

  const rightOperand = operands.find((o) => o !== leftOperand);
  if (!rightOperand) throw new UnsupportedQueryPlanError("No valid right operand found");

  if (isOperatorOperand(leftOperand) && leftOperand.operator === "size") {
    return handleSizeComparison(operator, leftOperand, rightOperand, context);
  }

  const arithOperand = [leftOperand, rightOperand].find(
    (o): o is OperatorOperand =>
      isOperatorOperand(o) && ARITHMETIC_OPERATORS.has(o.operator)
  );
  if (arithOperand) {
    const arithIsLeft = arithOperand === leftOperand;
    return handleArithmeticComparison(
      operator,
      arithOperand,
      arithIsLeft ? rightOperand : leftOperand,
      arithIsLeft,
      context
    );
  }

  if (
    [leftOperand, rightOperand].some(
      (o) => isOperatorOperand(o) && o.operator === "map"
    )
  ) {
    throw new UnsupportedQueryPlanError(
      `Direct comparison of map(...) to a value is not supported (operator: ${operator}). ` +
        `Wrap the map() expression in hasIntersection(map(...), [...]) instead.`
    );
  }

  const left = resolveOperand(leftOperand, context);
  const right = resolveOperand(rightOperand, context);

  if (isResolvedValue(left)) {
    if (isResolvedFieldReference(right)) {
      return buildComparisonFilter(
        context,
        right,
        mirrorOperator(operator),
        left.value
      );
    }
    return evaluateConstantComparison(operator, left.value, right.value)
      ? {}
      : rejectConstantFalse();
  }

  if (isResolvedFieldReference(right)) {
    if (
      left.valueType === "dateTime" &&
      right.valueType === "dateTime" &&
      (!left.timestampWrapped || !right.timestampWrapped)
    ) {
      throw new UnsupportedQueryPlanError(
        "Raw temporal column comparison loses RFC-3339 string spelling; wrap both operands in timestamp()"
      );
    }
    if (
      left.valueType !== undefined &&
      right.valueType !== undefined &&
      left.valueType !== right.valueType
    ) {
      throw new UnsupportedQueryPlanError("Cannot compare fields with different mapped value types");
    }
    return buildFieldToFieldFilter(
      context,
      operator,
      leftOperand,
      left,
      rightOperand,
      right
    );
  }

  return buildComparisonFilter(context, left, operator, right.value);
}

/**
 * `size(collection) CMP n`, for the thresholds that mean "is empty" or "is non-empty" — the only
 * counts a relation filter can express.
 */
function handleSizeComparison(
  operator: string,
  sizeOperand: OperatorOperand,
  valueOperand: PlanExpressionOperand,
  context: TranslationContext
): PrismaFilter {
  const collectionOperand = sizeOperand.operands[0];
  if (!collectionOperand || !isNamedOperand(collectionOperand)) {
    throw new UnsupportedQueryPlanError("size operator requires a named collection operand");
  }

  if (!isValueOperand(valueOperand)) {
    throw new UnsupportedQueryPlanError("size comparison requires a numeric value operand");
  }

  const count = valueOperand.value;
  if (typeof count !== "number") {
    throw new UnsupportedQueryPlanError("size comparison requires a numeric value");
  }

  const isNonEmpty =
    (operator === "gt" && count === 0) || (operator === "ge" && count === 1);

  const isEmpty =
    (operator === "eq" && count === 0) ||
    (operator === "lt" && count === 1) ||
    (operator === "le" && count === 0);

  if (!isNonEmpty && !isEmpty) {
    throw new UnsupportedQueryPlanError(
      `Unsupported size comparison: size(...) ${operator} ${count}`
    );
  }

  const { relations } = resolveFieldReference(collectionOperand.name, context);
  if (!relations || relations.length === 0) {
    throw new UnsupportedQueryPlanError("size operator requires a relation mapping");
  }

  // The count applies to the deepest relation; every relation before it is a hop to reach it.
  const deepest = relations[relations.length - 1]!;
  return wrapInRelations(
    relations.slice(0, -1),
    relationFilter(deepest, isNonEmpty ? "some" : "none", {})
  );
}

/**
 * Builds a column-vs-column comparison as a Prisma field reference. Prisma only supports
 * references between fields of the SAME model, so the container is the root model for
 * root-level comparisons and the relation's model inside a lambda; mixed-scope comparisons
 * (an element column against an outer column) are cross-model and must fail loudly.
 */
function buildFieldToFieldFilter(
  context: TranslationContext,
  operator: string,
  leftOperand: PlanExpressionOperand,
  left: ResolvedFieldReference,
  rightOperand: PlanExpressionOperand,
  right: ResolvedFieldReference
): PrismaFilter {
  const prismaOperator = assertDefined(
    CERBOS_TO_PRISMA_OPERATOR[operator],
    `Unsupported operator: ${operator}`
  );

  if (!isNamedOperand(leftOperand) || !isNamedOperand(rightOperand)) {
    throw new UnsupportedQueryPlanError("Field-to-field comparison requires two named operands");
  }

  const container = fieldReferenceContainer(
    context,
    leftOperand.name,
    left,
    rightOperand.name,
    right
  );
  const leftField = getLeafField(left.path);
  const rightField = getLeafField(right.path);
  const fieldReference = { _ref: rightField, _container: container };

  // Two explicit nulls are EQUAL in CEL, and a null against a value is definitely NOT equal.
  // A bare field reference gives UNKNOWN for both, which drops the row under either polarity.
  const leftExplicit = isExplicitNullReference(left);
  const rightExplicit = isExplicitNullReference(right);
  if (operator === "eq" || operator === "ne") {
    // Mixing the two conventions across one comparison has no faithful rendering. The declared
    // side needs a definite answer for its NULL (CEL holds a null VALUE); the undeclared side
    // needs UNKNOWN for its NULL (a missing attribute, which CEL denies under both polarities). A
    // definite predicate returns rows the PDP refuses; a plain one drops rows the PDP allows.
    // Refuse it rather than pick a direction — declare both attributes, or neither.
    if (leftExplicit !== rightExplicit) {
      throw new UnsupportedQueryPlanError(
        `Cannot translate \`${operator}\` between two columns under mixed null conventions: ` +
          "cannot compare an attribute declared explicit-null with one on the omitted convention: the omitted side is UNKNOWN for a NULL column while the declared side is definite, and no single predicate is both. Declare nullAttributeRepresentation on both mapper entries, or on neither."
      );
    }
    if (leftExplicit) {
      const equality: PrismaFilter = {
        OR: [
          { AND: [{ [leftField]: null }, { [rightField]: null }] },
          {
            AND: [
              { [leftField]: { not: null } },
              { [rightField]: { not: null } },
              { [leftField]: { equals: fieldReference } },
            ],
          },
        ],
      };
      return operator === "eq" ? equality : { NOT: equality };
    }
  }

  // `ne` is the one operator whose Prisma spelling cannot carry a field reference: `not` nests a
  // NestedStringFilter, which has no `_ref` form, so `{ col: { not: { _ref } } }` is rejected by
  // the client rather than executed. Hoisting the negation to the top-level `NOT` combinator says
  // the same thing with a shape that does accept one — the same rewrite the explicit-null branch
  // above already performs. Three-valued logic is unchanged by the hoist: a NULL on either side
  // makes the inner equality UNKNOWN, and `NOT UNKNOWN` is still UNKNOWN, so the row stays out
  // under both polarities, which is what a missing attribute does on the check side.
  if (operator === "ne") {
    return { NOT: { [leftField]: { equals: fieldReference } } };
  }

  return { [leftField]: { [prismaOperator]: fieldReference } };
}

/**
 * The Prisma model both columns of a field-to-field comparison belong to: the relation model
 * inside a lambda when both are element columns, otherwise the root model.
 */
function fieldReferenceContainer(
  context: TranslationContext,
  leftName: string,
  left: ResolvedFieldReference,
  rightName: string,
  right: ResolvedFieldReference
): string {
  const scope = currentScope(context);
  if (scope) {
    const prefix = scope.variableName + ".";
    const leftInScope = leftName.startsWith(prefix);
    const rightInScope = rightName.startsWith(prefix);
    if (leftInScope !== rightInScope) {
      throw new UnsupportedQueryPlanError(
        `Cannot compare a collection element column with an outer column (${leftName} vs ${rightName}): ` +
          "Prisma field references only work between fields of the same model"
      );
    }
    if (leftInScope) {
      if (!scope.relationModel) {
        throw new Error(
          "Field-to-field comparison inside a collection requires `relation.model` in the mapper"
        );
      }
      return scope.relationModel;
    }
  }

  if (
    (left.relations && left.relations.length > 0) ||
    (right.relations && right.relations.length > 0)
  ) {
    throw new UnsupportedQueryPlanError(
      "Cannot compare columns across relations: Prisma field references only work between fields of the same model"
    );
  }
  if (!context.rootModel) {
    throw new Error(
      "Field-to-field comparison requires the `model` option (the Prisma model name) to build a field reference"
    );
  }
  return context.rootModel;
}

/**
 * Translates `column ⊕ constant  CMP  value` (and its mirrored forms) by solving for the
 * column: Prisma where-filters cannot express column arithmetic, but linear arithmetic with
 * one constant side always rewrites to a plain comparison. Multiplying/dividing by a
 * negative constant mirrors directional operators. The rewrite happens in IEEE double space,
 * matching CEL: Cerbos attribute numbers are doubles, so this preserves check-time semantics
 * (e.g. `n * 0.1 == 0.3` solves to an unrepresentable fraction and matches no integer row,
 * exactly as CEL evaluates it).
 */
function handleArithmeticComparison(
  operator: string,
  arithExpr: OperatorOperand,
  otherOperand: PlanExpressionOperand,
  arithIsLeft: boolean,
  context: TranslationContext
): PrismaFilter {
  const arithOp = arithExpr.operator;
  const arithLeftOp = assertDefined(
    arithExpr.operands[0],
    `${arithOp} operator requires a left operand`
  );
  const arithRightOp = assertDefined(
    arithExpr.operands[1],
    `${arithOp} operator requires a right operand`
  );

  if (
    isOperatorOperand(otherOperand) &&
    ARITHMETIC_OPERATORS.has(otherOperand.operator) &&
    tryFoldValueExpression(otherOperand, context) === null
  ) {
    throw new UnsupportedQueryPlanError(
      "Arithmetic on both sides of a comparison is not supported: the expression cannot be solved to a plain column filter"
    );
  }

  const arithLeft = resolveOperand(arithLeftOp, context);
  const arithRight = resolveOperand(arithRightOp, context);
  const other = resolveOperand(otherOperand, context);

  // Fully constant arithmetic: fold and compare the other side against the result. The
  // arithmetic side keeps its source position, so when the folded constant was written
  // FIRST (`1 + 2 < f`), directional operators mirror to keep the field on the left.
  if (isResolvedValue(arithLeft) && isResolvedValue(arithRight)) {
    const folded = foldArithmetic(arithOp, arithLeft.value, arithRight.value);
    if (!isResolvedFieldReference(other)) {
      throw new UnsupportedQueryPlanError(
        `${arithOp} with two values requires a field reference on the other side`
      );
    }
    return buildComparisonFilter(
      context,
      other,
      arithIsLeft ? mirrorOperator(operator) : operator,
      folded
    );
  }

  if (!isResolvedValue(other)) {
    throw new UnsupportedQueryPlanError(
      `${arithOp} operator with field references requires a value on the other side of the comparison`
    );
  }

  let fieldRef: ResolvedFieldReference;
  let constant: Value;
  let fieldIsLeft: boolean;

  if (isResolvedFieldReference(arithLeft) && isResolvedValue(arithRight)) {
    fieldRef = arithLeft;
    constant = arithRight.value;
    fieldIsLeft = true;
  } else if (
    isResolvedValue(arithLeft) &&
    isResolvedFieldReference(arithRight)
  ) {
    fieldRef = arithRight;
    constant = arithLeft.value;
    fieldIsLeft = false;
  } else {
    throw new UnsupportedQueryPlanError(
      `${arithOp} operator requires exactly one field reference and one value, or two values`
    );
  }

  // The comparison operator was normalized arithmetic-side-first by the caller; if the
  // arithmetic was written on the RIGHT of the comparison, direction was already mirrored.
  let effectiveOperator = arithIsLeft ? operator : mirrorOperator(operator);

  // String concatenation solving (eq/ne only).
  if (arithOp === "add" && typeof constant === "string") {
    if (effectiveOperator !== "eq" && effectiveOperator !== "ne") {
      throw new UnsupportedQueryPlanError(
        `Operator ${effectiveOperator} is not supported with string concatenation`
      );
    }
    const solvedValue = solveAdd(other.value, constant, fieldIsLeft);
    if (solvedValue === null) {
      // A contradictory / exhaustive pair remains SQL UNKNOWN for a missing field.
      // An empty IN or an empty filter loses that error when an outer NOT is applied.
      const equality = buildFieldFilter(fieldRef, "equals", other.value);
      const inequality = buildFieldFilter(fieldRef, "not", other.value);
      return effectiveOperator === "eq"
        ? { AND: [equality, inequality] }
        : { OR: [equality, inequality] };
    }
    return buildComparisonFilter(context, fieldRef, effectiveOperator, solvedValue);
  }

  if (typeof constant !== "number" || typeof other.value !== "number") {
    throw new UnsupportedQueryPlanError(`${arithOp} comparison requires numeric operands`);
  }

  if (
    arithOp === "add" &&
    (effectiveOperator === "eq" || effectiveOperator === "ne") &&
    (!Number.isInteger(constant) || !Number.isInteger(other.value))
  ) {
    throw new UnsupportedQueryPlanError(
      "Fractional addition equality cannot be translated safely: solving IEEE-754 addition into a plain Prisma column comparison is not reversible"
    );
  }

  let solved: number;
  switch (arithOp) {
    case "add":
      solved = other.value - constant;
      break;
    case "sub":
      if (fieldIsLeft) {
        // f - c CMP v  ⇔  f CMP v + c
        solved = other.value + constant;
      } else {
        // c - f CMP v  ⇔  -f CMP v - c  ⇔  f mirror(CMP) c - v
        solved = constant - other.value;
        effectiveOperator = mirrorOperator(effectiveOperator);
      }
      break;
    case "mult":
      if (constant === 0) {
        throw new UnsupportedQueryPlanError(
          "Multiplication by a constant zero must be folded by the Cerbos planner"
        );
      }
      solved = other.value / constant;
      if (constant < 0) {
        effectiveOperator = mirrorOperator(effectiveOperator);
      }
      break;
    case "div":
      if (!fieldIsLeft) {
        throw new UnsupportedQueryPlanError(
          "Division by a column is not supported: the comparison cannot be solved to a plain column filter"
        );
      }
      if (constant === 0) {
        throw new UnsupportedQueryPlanError("Division by a constant zero is not supported");
      }
      solved = other.value * constant;
      if (constant < 0) {
        effectiveOperator = mirrorOperator(effectiveOperator);
      }
      break;
    default:
      throw new UnsupportedQueryPlanError(`Unsupported operator: ${arithOp}`);
  }

  return buildComparisonFilter(context, fieldRef, effectiveOperator, solved);
}

/**
 * The column value that makes `column + addConstant` (or `addConstant + column`) equal
 * `comparisonValue`, or null when no string does.
 */
function solveAdd(
  comparisonValue: Value,
  addConstant: Value,
  fieldIsLeft: boolean
): Value | null {
  if (typeof comparisonValue === "string" && typeof addConstant === "string") {
    if (fieldIsLeft) {
      if (!comparisonValue.endsWith(addConstant)) return null;
      return comparisonValue.slice(
        0,
        comparisonValue.length - addConstant.length
      );
    }
    if (!comparisonValue.startsWith(addConstant)) return null;
    return comparisonValue.slice(addConstant.length);
  }
  if (typeof comparisonValue === "number" && typeof addConstant === "number") {
    return comparisonValue - addConstant;
  }
  throw new UnsupportedQueryPlanError("Type mismatch in add comparison");
}

/**
 * Helper function to handle "in" operator
 */
export function handleInOperator(
  operands: PlanExpressionOperand[],
  context: TranslationContext
): PrismaFilter {
  if (operands.length !== 2) {
    throw new UnsupportedQueryPlanError("in requires exactly two operands");
  }
  const member = assertDefined(operands[0], "in requires a member operand");
  const collection = assertDefined(
    operands[1],
    "in requires a collection operand"
  );

  if (isNamedOperand(member) && isValueOperand(collection)) {
    const fieldRef = resolveFieldReference(member.name, context);
    const values = Array.isArray(collection.value)
      ? collection.value
      : [collection.value];
    return buildMembershipFilter(context, fieldRef, values);
  }

  if (isValueOperand(member) && isNamedOperand(collection)) {
    return buildMembershipFilter(
      context,
      resolveFieldReference(collection.name, context),
      [member.value]
    );
  }

  if (isNamedOperand(member) && isNamedOperand(collection)) {
    throw new UnsupportedQueryPlanError(
      "Membership between two database attributes is not supported: Prisma cannot compare a related element column with an outer scalar column"
    );
  }

  throw new UnsupportedQueryPlanError(
    "in requires a field member and literal list, or a literal member and field collection"
  );
}
