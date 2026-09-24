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
import type {
  ResolvedFieldReference,
  ResolvedValue,
  TranslationContext,
} from "./mapping";
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
import { roundSubMillisecond } from "./timestamp";
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

  // Only a comparison between a column and one constant can absorb a sub-millisecond instant.
  const oneColumn = isColumnOperand(leftOperand) !== isColumnOperand(rightOperand);
  const left = resolveOperand(leftOperand, context, oneColumn);
  const right = resolveOperand(rightOperand, context, oneColumn);

  if (isResolvedValue(left)) {
    if (isResolvedFieldReference(right)) {
      return buildValueComparisonFilter(
        context,
        right,
        mirrorOperator(operator),
        left
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

  return buildValueComparisonFilter(context, left, operator, right);
}

/** A column reference, bare or wrapped in timestamp(). */
function isColumnOperand(operand: PlanExpressionOperand): boolean {
  if (isNamedOperand(operand)) return true;
  return (
    isOperatorOperand(operand) &&
    operand.operator === "timestamp" &&
    operand.operands.length === 1 &&
    isNamedOperand(operand.operands[0]!)
  );
}

/** buildComparisonFilter, for a resolved constant that may be a sub-millisecond timestamp. */
function buildValueComparisonFilter(
  context: TranslationContext,
  fieldRef: ResolvedFieldReference,
  operator: string,
  constant: ResolvedValue
): PrismaFilter {
  if (!constant.subMillisecond) {
    return buildComparisonFilter(context, fieldRef, operator, constant.value);
  }
  const rounded = roundSubMillisecond(operator);
  if (rounded !== null) {
    return buildComparisonFilter(context, fieldRef, rounded, constant.value);
  }
  // No whole millisecond equals the instant. Spelled as a contradiction on the column, so a NULL
  // stays UNKNOWN under both polarities — timestamp() of a missing attribute is an error.
  const never: PrismaFilter = {
    AND: [
      buildComparisonFilter(context, fieldRef, "lt", constant.value),
      buildComparisonFilter(context, fieldRef, "ge", constant.value),
    ],
  };
  return operator === "eq" ? never : { NOT: never };
}

/**
 * A size is a non-negative integer, so every `size(x) CMP n` — fractional, negative or huge `n`
 * included — is one of: at least `k`, exactly `k`, or the negation of either. `at least 0` is
 * true whenever `x` can be evaluated at all.
 */
type CountPredicate = {
  kind: "atLeast" | "exactly";
  count: number;
  negated: boolean;
};

function countPredicate(operator: string, n: number): CountPredicate {
  const atLeast = (count: number, negated = false): CountPredicate => ({
    kind: "atLeast",
    count: Math.max(count, 0),
    negated,
  });
  switch (operator) {
    case "gt":
      return atLeast(Math.floor(n) + 1);
    case "ge":
      return atLeast(Math.ceil(n));
    case "lt":
      return atLeast(Math.ceil(n), true);
    case "le":
      return atLeast(Math.floor(n) + 1, true);
    case "eq":
    case "ne": {
      const negated = operator === "ne";
      // A fractional size never equals anything; a negative one never exists.
      return Number.isInteger(n) && n >= 0
        ? { kind: "exactly", count: n, negated }
        : atLeast(0, !negated);
    }
    default:
      throw new UnsupportedQueryPlanError(`Unsupported operator: ${operator}`);
  }
}

/**
 * No store holds a string of 2^32 characters: PostgreSQL caps a value at 1 GB, SQLite at
 * 2^31 - 1 bytes and MySQL's LONGTEXT at 2^32 - 1 bytes. A length threshold at or past it is
 * decided without a pattern.
 */
const STRING_LENGTH_CEILING = 2 ** 32;

/** Past this, a length threshold short of the ceiling is refused rather than spelled out. */
const MAX_LENGTH_PATTERN = 1024;

/**
 * `size(column) CMP n` over a string column, as `LIKE` patterns of `_`: Prisma does not escape
 * the wildcards in a `startsWith` needle, so `startsWith: "_____"` is `LIKE '_____%'`, true exactly
 * when the value holds at least five characters. Each store's `_` matches one character — a code
 * point in a UTF-8 database — which is what CEL's size() counts. `startsWith: ""` is `LIKE '%'`,
 * true for every non-NULL value. Every filter built here is therefore UNKNOWN on a NULL column
 * under both polarities, as size() of a missing attribute is an error in CEL.
 */
function buildStringLengthFilter(
  fieldRef: ResolvedFieldReference,
  predicate: CountPredicate
): PrismaFilter {
  const atLeast = (count: number): PrismaFilter => {
    if (count >= STRING_LENGTH_CEILING) {
      return { NOT: buildFieldFilter(fieldRef, "startsWith", "") };
    }
    if (count > MAX_LENGTH_PATTERN) {
      throw new UnsupportedQueryPlanError(
        `Cannot translate a string length threshold of ${count}: the LIKE pattern it needs ` +
          `exceeds the ${MAX_LENGTH_PATTERN}-character limit`
      );
    }
    return buildFieldFilter(fieldRef, "startsWith", "_".repeat(count));
  };
  const filter =
    predicate.kind === "atLeast"
      ? atLeast(predicate.count)
      : { AND: [atLeast(predicate.count), { NOT: atLeast(predicate.count + 1) }] };
  return predicate.negated ? { NOT: filter } : filter;
}

/**
 * `size(collection) CMP n`. Over a relation, only the thresholds that mean "is empty", "is
 * non-empty" or "the chain reaching it exists" — the only counts a relation filter can express.
 * Over a string column, any threshold (see buildStringLengthFilter).
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
  if (typeof count !== "number" || !Number.isFinite(count)) {
    throw new UnsupportedQueryPlanError("size comparison requires a numeric value");
  }
  const predicate = countPredicate(operator, count);

  const fieldRef = resolveFieldReference(collectionOperand.name, context);
  const { relations } = fieldRef;
  if (!relations || relations.length === 0) {
    if (fieldRef.valueType === "string") {
      return buildStringLengthFilter(fieldRef, predicate);
    }
    throw new UnsupportedQueryPlanError("size operator requires a relation mapping");
  }

  // The count applies to the deepest relation; every relation before it is a hop to reach it.
  const hops = relations.slice(0, -1);
  const deepest = relations[relations.length - 1]!;
  const { kind, count: k, negated } = predicate;
  // `size >= 0` holds whenever the collection can be reached. Only a chain can fail to reach it;
  // a direct relation would be an unconditional `{}`, which no negation could then express.
  if (kind === "atLeast" && k === 0 && !negated && hops.length > 0) {
    return wrapInRelations(hops, {});
  }
  const isNonEmpty =
    (kind === "atLeast" && k === 1 && !negated) ||
    (kind === "exactly" && k === 0 && negated);
  const isEmpty =
    (kind === "atLeast" && k === 1 && negated) ||
    (kind === "exactly" && k === 0 && !negated);

  if (!isNonEmpty && !isEmpty) {
    throw new UnsupportedQueryPlanError(
      `Unsupported size comparison: size(...) ${operator} ${count}`
    );
  }

  return wrapInRelations(
    hops,
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
