import type { PlanExpression, PlanExpressionOperand } from "@cerbos/core";

import { UnsupportedQueryPlanError } from "./errors";
import {
  COMPARISON_OPERATORS,
  buildAggregationExpression,
  buildAggregationExpressionFromExpression,
} from "./aggregation";
import type { ComparisonOperator } from "./aggregation";
import {
  assertCollectionScopedReference,
  assertNullOperandTranslatable,
} from "./context";
import type { TranslateContext } from "./context";
import {
  buildFieldFilter,
  buildGuardedFieldFilter,
  withEvaluationGuards,
  withNullableGuards,
} from "./guards";
import { buildHierarchyFilter } from "./hierarchy";
import type { HierarchyOperator } from "./hierarchy";
import type { Mapper, MongooseFilter } from "./index";
import { LAMBDA_BINDING_OPERATORS, foldLiteralCollection } from "./lambda";
import {
  applyValueParser,
  createScopedMapper,
  isNullableReference,
  resolveFieldReference,
  resolveMapperConfig,
  withOmittedNullDefault,
} from "./mapper";
import {
  carriesNullOperand,
  collectVariableNames,
  getOperandAt,
  isExpression,
  isValue,
  isVariable,
} from "./operands";
import { escapeRegexValue, normalizeRe2PatternForMongo } from "./regex";

/** Translates a CONDITIONAL plan's condition into a Mongoose filter. */
export const translateCondition = (
  condition: PlanExpressionOperand,
  ctx: TranslateContext,
): MongooseFilter => {
  rejectNullConstructor(condition, ctx);
  return buildFilter(condition, ctx);
};

/** A null inside a list/struct literal is a NULL value only under the explicit representation. */
function rejectNullConstructor(
  operand: PlanExpressionOperand,
  ctx: TranslateContext,
  inConstructor = false,
): void {
  if (isValue(operand)) {
    if (inConstructor && carriesNullOperand(operand.value)) {
      assertNullOperandTranslatable(
        ctx,
        "a null literal in a collection or struct constructor",
      );
    }
  } else if (isExpression(operand)) {
    const nested =
      inConstructor ||
      ["list", "struct", "set-field"].includes(operand.operator);
    operand.operands.forEach((child) =>
      rejectNullConstructor(child, ctx, nested),
    );
  }
}

type FilterOperator = (
  expression: PlanExpression,
  ctx: TranslateContext,
) => MongooseFilter;

/**
 * Every operator that can appear in boolean position. Adding one is adding an entry here; an
 * operator that can also appear inside `$expr` needs an entry in aggregation.ts as well.
 */
const FILTER_OPERATORS: Record<string, FilterOperator> = {
  and: ({ operands }, ctx) => ({
    $and: operands.map((op) => buildFilter(op, ctx)),
  }),
  or: ({ operands }, ctx) => ({
    $or: operands.map((op) => buildFilter(op, ctx)),
  }),
  not: ({ operands }, ctx) => translateNot(operands, ctx),
  eq: comparison("eq"),
  ne: comparison("ne"),
  lt: comparison("lt"),
  le: comparison("le"),
  gt: comparison("gt"),
  ge: comparison("ge"),
  in: ({ operands }, ctx) => translateIn(operands, ctx),
  matches: ({ operands }, ctx) => translateMatches(operands, ctx),
  contains: translateStringPredicate,
  startsWith: translateStringPredicate,
  endsWith: translateStringPredicate,
  hasIntersection: ({ operands }, ctx) =>
    translateHasIntersection(operands, ctx),
  exists: quantifier("exists"),
  all: quantifier("all"),
  exists_one: () => {
    throw new UnsupportedQueryPlanError(
      "exists_one requires exact match cardinality and is unsupported",
    );
  },
  // filter() yields a list, not a boolean. Reaching it in a boolean position means the
  // plan used it as a predicate, and there is no meaning to pick: `filter(...)` is not
  // `size(filter(...)) > 0`. Fail closed (cerbos/query-plan-adapters#313); the legitimate
  // `size(filter(...))` form is handled by the size operator before this.
  filter: () => {
    throw new UnsupportedQueryPlanError(
      "filter() returns a list, not a boolean, so it cannot be a condition on its own; " +
        "only size(filter(...)) has a boolean meaning",
    );
  },
  // map() in a BOOLEAN position: the projection is a list, not a predicate. Rendering it
  // as `$elemMatch: { $exists: true }` silently answers "the projection is non-empty",
  // which is not what the policy said (cerbos/query-plan-adapters#313). The legitimate
  // consumer, hasIntersection(map(...), [...]), destructures the map operand itself
  // before reaching this table.
  map: () => {
    throw new UnsupportedQueryPlanError(
      "map() returns a list, not a boolean, so it cannot be a condition on its own; " +
        "only hasIntersection(map(...), [...]) gives the projection a boolean meaning",
    );
  },
  lambda: ({ operands }, ctx) => translateLambda(operands, ctx),
  if: (expression, ctx) => {
    if (ctx.scope.kind === "collection") {
      throw new UnsupportedQueryPlanError(
        "if aggregation expressions inside collection predicates are unsupported",
      );
    }
    return withEvaluationGuards(
      {
        $expr: buildAggregationExpressionFromExpression(expression, ctx.mapper),
      },
      expression.operands,
      ctx.mapper,
    );
  },
  ancestorOf: hierarchy("ancestorOf"),
  descendentOf: hierarchy("descendentOf"),
  overlaps: hierarchy("overlaps"),
};

/** Builds Mongoose conditions from a Cerbos expression in boolean position. */
const buildFilter = (
  expression: PlanExpressionOperand,
  ctx: TranslateContext,
): MongooseFilter => {
  if (isVariable(expression)) {
    return translateBareVariable(expression.name, ctx);
  }
  if (!isExpression(expression)) {
    throw new UnsupportedQueryPlanError("Invalid Cerbos expression structure");
  }

  const { operator, operands } = expression;

  // A literal value list arrives as a macro's collection operand when the
  // planner could not unroll it over a known collection (more than 10
  // elements). Dispatch before the operator table: every collection macro
  // needs the same treatment, and none of their relation-mapping requirements
  // can be satisfied by a literal.
  const [collectionOperand, lambdaOperand] = operands;
  if (
    LAMBDA_BINDING_OPERATORS.has(operator) &&
    operands.length === 2 &&
    collectionOperand !== undefined &&
    lambdaOperand !== undefined &&
    isValue(collectionOperand)
  ) {
    return foldLiteralCollection(
      operator,
      collectionOperand,
      lambdaOperand,
      (body) => buildFilter(body, ctx),
    );
  }

  if (!Object.hasOwn(FILTER_OPERATORS, operator)) {
    throw new UnsupportedQueryPlanError(`Unsupported operator: ${operator}`);
  }
  return FILTER_OPERATORS[operator]!(expression, ctx);
};

/** Every leaf resolves its field in the active collection scope before emission. */
const emitLeafComparison = (
  ctx: TranslateContext,
  fieldName: string,
  comparison: unknown,
  guards: { nullable: boolean; requireExists: boolean },
): MongooseFilter => {
  assertCollectionScopedReference(fieldName, ctx);
  const { path, relation } = resolveFieldReference(fieldName, ctx.mapper);
  const filter = buildGuardedFieldFilter(
    relation?.type === "many" ? path.slice(1) : path,
    comparison,
    guards.nullable,
    guards.requireExists,
  );
  return relation?.type === "many"
    ? { [relation.name]: { $elemMatch: filter } }
    : filter;
};

/**
 * A field compared with a constant: the field is nullable-guarded, and a null constant (or a null
 * inside a constant list) requires the field to exist and is refused under `"omitted"`.
 */
const emitValueComparison = (
  ctx: TranslateContext,
  fieldName: string,
  comparison: unknown,
  constant: unknown,
  nullOperandContext: string,
): MongooseFilter => {
  const nullable = isNullableReference(fieldName, ctx.mapper);
  const requireExists = carriesNullOperand(constant);
  if (requireExists) {
    assertNullOperandTranslatable(ctx, nullOperandContext);
  }
  return emitLeafComparison(ctx, fieldName, comparison, {
    nullable,
    requireExists,
  });
};

const translateBareVariable = (
  name: string,
  ctx: TranslateContext,
): MongooseFilter => {
  assertCollectionScopedReference(name, ctx);
  const { path, relation } = resolveFieldReference(name, ctx.mapper);
  // A to-MANY relation in boolean position is a collection, and a collection has no truth
  // value. A to-ONE relation is a different thing wearing the same field: it flattens to a
  // single dotted scalar path (`parent.aBool`), so it reads exactly like a plain column here.
  // Its absent-hop requirement is applied by whichever operator encloses it — `not` ANDs it
  // OUTSIDE the `$nor` — rather than by this leaf (cerbos/query-plan-adapters#375).
  if (relation && relation.type !== "one") {
    throw new UnsupportedQueryPlanError("Bare collection variables are unsupported");
  }
  return buildGuardedFieldFilter(
    path,
    { $eq: true },
    isNullableReference(name, ctx.mapper),
  );
};

/**
 * Whether evaluating `operand` always reads the variable `name`, so that a missing attribute
 * there is an error CEL cannot avoid. `&&` and `||` absorb an erroring operand when another
 * decides the result, a ternary reads only the branch its condition selects, and a lambda body
 * is never evaluated over an empty collection: each of those reads `name` unconditionally only
 * if every path through it does. Every other operator is strict in its operands.
 */
const alwaysReads = (operand: PlanExpressionOperand, name: string): boolean => {
  if (isVariable(operand)) {
    return operand.name === name;
  }
  if (!isExpression(operand)) {
    return false;
  }
  const [first, ...rest] = operand.operands;
  switch (operand.operator) {
    case "and":
    case "or":
      return operand.operands.every((child) => alwaysReads(child, name));
    case "if":
      return (
        (first !== undefined && alwaysReads(first, name)) ||
        (rest.length > 0 && rest.every((child) => alwaysReads(child, name)))
      );
    default:
      if (LAMBDA_BINDING_OPERATORS.has(operand.operator)) {
        return first !== undefined && alwaysReads(first, name);
      }
      return operand.operands.some((child) => alwaysReads(child, name));
  }
};

const nullableGuardIsExact = (
  operand: PlanExpressionOperand,
  ctx: TranslateContext,
): boolean => {
  const nullable = collectVariableNames(operand).filter((name) =>
    isNullableReference(name, ctx.mapper),
  );
  if (nullable.length === 0) {
    return true;
  }
  return (
    ctx.scope.kind === "root" &&
    nullable.every((name) => alwaysReads(operand, name)) &&
    nullable.every(
      (name) => resolveFieldReference(name, ctx.mapper).relation?.type !== "many",
    )
  );
};

const translateNot = (
  operands: PlanExpressionOperand[],
  ctx: TranslateContext,
): MongooseFilter => {
  const operand = getOperandAt(
    operands,
    0,
    "not operator requires at least one operand",
  );
  // De Morgan, pushed down to the leaves. CEL's `&&`/`||` let a decided operand absorb an
  // error in the other, so `!(!aBool && parent.x == "one")` is TRUE on a parentless document
  // whose aBool is true. Guarding the whole negation for every parent it dots through denies
  // that document; negating each operand instead gives every leaf its own guard. The rewrite
  // is exact under CEL's error semantics: `!(a && b)` and `!a || !b` agree on every
  // combination of true, false and error.
  if (isExpression(operand) && (operand.operator === "and" || operand.operator === "or")) {
    const dual = operand.operator === "and" ? "$or" : "$and";
    return {
      [dual]: operand.operands.map((child) => translateNot([child], ctx)),
    };
  }
  if (
    (isExpression(operand) &&
      ["exists", "exists_one", "all"].includes(operand.operator)) ||
    !nullableGuardIsExact(operand, ctx)
  ) {
    throw new UnsupportedQueryPlanError(
      "not over nullable fields or collection macros cannot preserve Cerbos error semantics",
    );
  }
  // withEvaluationGuards ANDs its conjuncts OUTSIDE this $nor, which is where the
  // absent-parent requirement has to sit: inside, the negation would flip it along with
  // the predicate and readmit every parentless document (#315, #316).
  return withEvaluationGuards(
    { $nor: [buildFilter(operand, ctx)] },
    [operand],
    ctx.mapper,
  );
};

/** `value OP field` is `field MIRROR(OP) value`. */
const MIRRORED_COMPARISON: Record<ComparisonOperator, ComparisonOperator> = {
  eq: "eq",
  ne: "ne",
  lt: "gt",
  le: "ge",
  gt: "lt",
  ge: "le",
};

function comparison(operator: ComparisonOperator): FilterOperator {
  return ({ operands }, ctx) => translateComparison(operator, operands, ctx);
}

const translateComparison = (
  operator: ComparisonOperator,
  operands: PlanExpressionOperand[],
  ctx: TranslateContext,
): MongooseFilter => {
  const { mapper } = ctx;
  const leftOperand = getOperandAt(
    operands,
    0,
    `${operator} operator requires a left operand`,
  );
  const rightOperand = getOperandAt(
    operands,
    1,
    `${operator} operator requires a right operand`,
  );
  const bothOperands = [leftOperand, rightOperand];

  if (
    isVariable(leftOperand) &&
    isVariable(rightOperand) &&
    [leftOperand, rightOperand].some(
      (operand) =>
        resolveMapperConfig(operand.name, mapper)?.valueType === "dateTime",
    )
  ) {
    throw new UnsupportedQueryPlanError(
      "Bare temporal field comparison cannot preserve CEL string equality: stored Dates discard the original lexical spelling; compare timestamp(...) values instead",
    );
  }
  if (
    (isVariable(leftOperand) || isVariable(rightOperand)) &&
    bothOperands.some(
      (operand) => isValue(operand) && Array.isArray(operand.value),
    )
  ) {
    throw new UnsupportedQueryPlanError(
      "Whole-list comparison is not supported: a relation mapping exposes scalar element fields, not an ordered list value",
    );
  }
  // If either operand is a (non-relational) expression, emit a `$expr`
  // with aggregation-pipeline operators. This covers arithmetic, type
  // conversion, ternary, `index`, `size`, `matches` etc. on either side
  // of the comparison (e.g. `aNumber == int(aString)` or `(a + 1) > b`).
  if (
    isExpression(leftOperand) ||
    isExpression(rightOperand) ||
    (isVariable(leftOperand) && isVariable(rightOperand))
  ) {
    if (
      isExpression(leftOperand) &&
      leftOperand.operator === "if" &&
      isExpression(rightOperand) &&
      rightOperand.operator === "if"
    ) {
      throw new UnsupportedQueryPlanError(
        "Mongoose cannot cast comparisons between two conditional expressions",
      );
    }
    if (ctx.scope.kind === "collection") {
      throw new UnsupportedQueryPlanError(
        `${operator} aggregation expressions inside collection predicates are unsupported`,
      );
    }
    const leftAgg = buildAggregationExpression(leftOperand, mapper);
    const rightAgg = buildAggregationExpression(rightOperand, mapper);
    return withEvaluationGuards(
      { $expr: { [COMPARISON_OPERATORS[operator]]: [leftAgg, rightAgg] } },
      bothOperands,
      mapper,
    );
  }

  const variableOperand = bothOperands.find(isVariable);
  const valueOperand = bothOperands.find(isValue);
  if (!variableOperand || !valueOperand) {
    throw new UnsupportedQueryPlanError(
      `${operator} requires a field/value pair or aggregation operands`,
    );
  }
  assertCollectionScopedReference(variableOperand.name, ctx);

  const effectiveOperator =
    variableOperand === leftOperand ? operator : MIRRORED_COMPARISON[operator];
  // A constant of a different scalar type than the declared field never equals it.
  if (
    (effectiveOperator === "eq" || effectiveOperator === "ne") &&
    !canEqualDeclaredType(variableOperand.name, valueOperand.value, mapper)
  ) {
    if (ctx.scope.kind === "root") {
      return withNullableGuards(
        { $expr: { $eq: [effectiveOperator === "ne", true] } },
        [variableOperand],
        mapper,
      );
    }
    // MongoDB refuses `$expr` inside `$elemMatch` ("$expr can only be applied to the top-level
    // document"), so an element predicate states the answer as a leaf on the element's own
    // field: nothing is in an empty list, and `!=` holds wherever the field is present (and,
    // when nullable, non-null).
    return emitLeafComparison(
      ctx,
      variableOperand.name,
      effectiveOperator === "eq" ? { $in: [] } : { $exists: true },
      {
        nullable:
          effectiveOperator === "ne" &&
          isNullableReference(variableOperand.name, mapper),
        requireExists: false,
      },
    );
  }
  return emitValueComparison(
    ctx,
    variableOperand.name,
    {
      [COMPARISON_OPERATORS[effectiveOperator]]: applyValueParser(
        variableOperand.name,
        valueOperand.value,
        mapper,
      ),
    },
    valueOperand.value,
    `\`${effectiveOperator}\` against a null operand`,
  );
};

const translateIn = (
  operands: PlanExpressionOperand[],
  ctx: TranslateContext,
): MongooseFilter => {
  const leftOperand = getOperandAt(operands, 0, "in requires a left operand");
  const rightOperand = getOperandAt(operands, 1, "in requires a right operand");

  if (isVariable(leftOperand) && isValue(rightOperand)) {
    if (!Array.isArray(rightOperand.value)) {
      throw new UnsupportedQueryPlanError("in with a field on the left requires an array value");
    }
    return emitValueComparison(
      ctx,
      leftOperand.name,
      {
        $in: withoutUnequalConstants(
          leftOperand.name,
          rightOperand.value,
          ctx.mapper,
        ).map((value) => applyValueParser(leftOperand.name, value, ctx.mapper)),
      },
      rightOperand.value,
      "a null element in an `in` list",
    );
  }

  if (isValue(leftOperand) && Array.isArray(leftOperand.value)) {
    throw new UnsupportedQueryPlanError(
      "List-element membership is not supported: a scalar relation mapping cannot compare a list value with one element",
    );
  }
  if (isValue(leftOperand) && isVariable(rightOperand)) {
    if (
      !canEqualDeclaredType(rightOperand.name, leftOperand.value, ctx.mapper)
    ) {
      // No element of the declared type equals this needle. An empty `$in` answers false in
      // either scope, where `{ $eq: needle }` would be cast by Mongoose into a match.
      return emitLeafComparison(
        ctx,
        rightOperand.name,
        { $in: [] },
        { nullable: false, requireExists: false },
      );
    }
    const needle = applyValueParser(
      rightOperand.name,
      leftOperand.value,
      ctx.mapper,
    );
    const uncast = emitUncastListMembership(
      ctx,
      rightOperand.name,
      [needle],
      carriesNullOperand(leftOperand.value),
      "a null needle in a mapped-collection `in`",
    );
    if (uncast) {
      return uncast;
    }
    return emitValueComparison(
      ctx,
      rightOperand.name,
      { $eq: needle },
      leftOperand.value,
      "a null needle in a mapped-collection `in`",
    );
  }

  throw new UnsupportedQueryPlanError(
    "in supports only field-in-value-list or value-in-mapped-collection shapes",
  );
};

/**
 * `needle in field` / `hasIntersection(field, needles)` over a list stored as a native array on the
 * document itself — not a relation — answered by the server's own element equality, out of
 * Mongoose's reach.
 *
 * A query-level `{ field: needle }` or `{ field: { $in: needles } }` is cast by Mongoose to the
 * schema type of the array's elements before it is sent: over a `[Number]` array `"2"` becomes
 * `2`, over a `[Boolean]` array `"true"` becomes `true`, over a `[String]` array `2` becomes `"2"`.
 * CEL's equality is heterogeneous — `"2" in [2]` is false — so the cast filter returns rows the PDP
 * denies. The plan carries no element type, so the literal cannot be checked here instead.
 *
 * Mongoose casts an `$expr` `$in` only when its array operand is a bare field path, so the array
 * is read through `$cond`/`$isArray` — which also makes a missing or non-array field an empty
 * list (no match) rather than a server error — and each needle is wrapped in `$literal`, so a
 * string spelled like a field path stays a string. Aggregation equality matches numbers across
 * BSON numeric types and never across types, which is CEL's equality. The
 * `type-mismatch/in/string-literal-in-resource-*-list` cases run against typed arrays and
 * over-grant without this.
 *
 * Returns undefined for a relation or inside a collection predicate, where the element is a
 * subdocument field matched through `$elemMatch`, and for an empty needle list, which has
 * nothing to cast.
 */
const emitUncastListMembership = (
  ctx: TranslateContext,
  fieldName: string,
  needles: unknown[],
  carriesNull: boolean,
  nullOperandContext: string,
): MongooseFilter | undefined => {
  if (ctx.scope.kind !== "root" || needles.length === 0) {
    return undefined;
  }
  const { path, relation } = resolveFieldReference(fieldName, ctx.mapper);
  if (relation) {
    return undefined;
  }
  if (carriesNull) {
    assertNullOperandTranslatable(ctx, nullOperandContext);
  }
  const field = `$${path.join(".")}`;
  // A fresh array operand per needle: Mongoose's caster rewrites `$cond` in place.
  const memberships = needles.map((needle) => ({
    $in: [{ $literal: needle }, { $cond: [{ $isArray: field }, field, []] }],
  }));
  return withNullableGuards(
    {
      $expr: memberships.length === 1 ? memberships[0] : { $or: memberships },
    },
    [{ name: fieldName }],
    ctx.mapper,
  );
};

const translateMatches = (
  operands: PlanExpressionOperand[],
  ctx: TranslateContext,
): MongooseFilter => {
  const fieldOperand = getOperandAt(
    operands,
    0,
    "matches operator requires a field operand",
  );
  const patternOperand = getOperandAt(
    operands,
    1,
    "matches operator requires a regex pattern value",
  );
  if (
    !isVariable(fieldOperand) ||
    !isValue(patternOperand) ||
    typeof patternOperand.value !== "string"
  ) {
    throw new UnsupportedQueryPlanError("matches operator requires a string regex pattern");
  }

  return emitLeafComparison(
    ctx,
    fieldOperand.name,
    { $regex: normalizeRe2PatternForMongo(patternOperand.value) },
    { nullable: false, requireExists: false },
  );
};

/** `contains`/`startsWith`/`endsWith`: a `$regex` on a string field, `$expr` otherwise. */
function translateStringPredicate(
  expression: PlanExpression,
  ctx: TranslateContext,
): MongooseFilter {
  const { operator, operands } = expression;
  const { mapper } = ctx;
  const leftOperand = getOperandAt(
    operands,
    0,
    `${operator} operator requires a receiver`,
  );
  const rightOperand = getOperandAt(
    operands,
    1,
    `${operator} operator requires a needle`,
  );
  if (
    !isVariable(leftOperand) ||
    !isValue(rightOperand) ||
    typeof rightOperand.value !== "string" ||
    !isStringOrUntyped(resolveMapperConfig(leftOperand.name, mapper)?.valueType)
  ) {
    if (ctx.scope.kind === "collection") {
      throw new UnsupportedQueryPlanError(
        `${operator} aggregation expressions inside collection predicates are unsupported`,
      );
    }
    return withEvaluationGuards(
      { $expr: buildAggregationExpressionFromExpression(expression, mapper) },
      [expression],
      mapper,
    );
  }

  const escapedValue = escapeRegexValue(rightOperand.value);
  // Mongo matches $regex with PCRE2, where `$` also matches immediately
  // before a final newline, so "tail\n" would satisfy endsWith("tail")
  // while CEL says false. `\z` is the absolute end of subject — the same
  // rewrite normalizeRe2PatternForMongo applies to a trailing `$`.
  // `^` needs no counterpart: without PCRE2_MULTILINE it already matches
  // only at the start of the subject.
  const regexStr =
    operator === "contains"
      ? escapedValue
      : operator === "startsWith"
        ? `^${escapedValue}`
        : `${escapedValue}\\z`;

  return emitLeafComparison(
    ctx,
    leftOperand.name,
    { $regex: regexStr },
    {
      nullable: isNullableReference(leftOperand.name, mapper),
      requireExists: false,
    },
  );
}

const isStringOrUntyped = (valueType: string | undefined): boolean =>
  valueType === undefined || valueType === "string";

const translateHasIntersection = (
  operands: PlanExpressionOperand[],
  ctx: TranslateContext,
): MongooseFilter => {
  // A null element in the intersection list lowers to a null-matching disjunct exactly as
  // it does for `in`, so it is subject to the same representation guard.
  if (
    operands.some(
      (operand) => isValue(operand) && carriesNullOperand(operand.value),
    )
  ) {
    assertNullOperandTranslatable(ctx, "a null element in hasIntersection");
  }
  if (operands.length !== 2) {
    throw new UnsupportedQueryPlanError("hasIntersection requires exactly two operands");
  }

  const firstOperand = getOperandAt(
    operands,
    0,
    "hasIntersection requires a field operand",
  );
  const secondOperand = getOperandAt(
    operands,
    1,
    "hasIntersection requires a value operand",
  );
  // hasIntersection is commutative and the planner preserves source order, so the constant
  // list arrives FIRST when the policy spells it first. The operands are not
  // interchangeable in the emitted query — one becomes the field path, the other the `$in`
  // list — so normalize to collection-first instead of reading them positionally.
  const valueFirst = isValue(firstOperand) && Array.isArray(firstOperand.value);
  const leftOperand = valueFirst ? secondOperand : firstOperand;
  const rightOperand = valueFirst ? firstOperand : secondOperand;

  if (isExpression(leftOperand) && leftOperand.operator === "map") {
    return translateMapIntersection(leftOperand, rightOperand, ctx);
  }

  if (!isVariable(leftOperand) || !isValue(rightOperand)) {
    throw new UnsupportedQueryPlanError("Invalid operands for hasIntersection");
  }
  if (!Array.isArray(rightOperand.value)) {
    throw new UnsupportedQueryPlanError("hasIntersection requires an array value");
  }
  const values = withoutUnequalConstants(
    leftOperand.name,
    rightOperand.value,
    ctx.mapper,
  );
  const uncast = emitUncastListMembership(
    ctx,
    leftOperand.name,
    values,
    carriesNullOperand(values),
    "a null element in hasIntersection",
  );
  if (uncast) {
    return uncast;
  }
  return emitLeafComparison(
    ctx,
    leftOperand.name,
    { $in: values },
    {
      nullable: false,
      requireExists: values.includes(null),
    },
  );
};

/**
 * The scalar type a reference's stored value is declared with — for a relation mapped to one
 * element field (`relation.field`), that element field's — or undefined when a constant cannot be
 * checked against it: no `valueType`, a `dateTime` (compared through its own path), or a
 * `valueParser`, which is the caller's explicit override of the constant.
 */
const declaredScalarType = (
  reference: string,
  mapper: Mapper,
): "number" | "string" | "boolean" | undefined => {
  const config = resolveMapperConfig(reference, mapper);
  const relation = config?.relation;
  const typed = relation
    ? relation.field
      ? relation.fields?.[relation.field]
      : undefined
    : config;
  if (!typed || typed.valueParser || config?.valueParser) {
    return undefined;
  }
  return typed.valueType === "dateTime" ? undefined : typed.valueType;
};

/**
 * False when `constant` is a non-null scalar of another type than the one `reference` declares.
 *
 * Mongoose casts a query-level literal to the schema type before it is sent — `"5"` to `5` over a
 * Number path, `"true"` to `true` over a Boolean, `0` to `"0"` over a String, and the same inside
 * `$elemMatch` over a typed subdocument field — while CEL's equality is heterogeneous: `5 == "5"`
 * is false. Answering such a constant here keeps it away from the caster, and leaves a constant of
 * the declared type in a plain query an index can answer. Without a `valueType` there is nothing
 * to check against, and Mongoose's cast applies (README, "Mapping hazards").
 */
const canEqualDeclaredType = (
  reference: string,
  constant: unknown,
  mapper: Mapper,
): boolean => {
  const declared = declaredScalarType(reference, mapper);
  return (
    declared === undefined || constant === null || typeof constant === declared
  );
};

/** A constant list without the elements `reference`'s declared type can never equal. */
const withoutUnequalConstants = (
  reference: string,
  constants: unknown[],
  mapper: Mapper,
): unknown[] =>
  constants.filter((constant) =>
    canEqualDeclaredType(reference, constant, mapper),
  );

/** `hasIntersection(collection.map(e, e.field), [values])`: some element's field is in the list. */
/**
 * The mapper a lambda body over `collection` is translated with. Under `"omitted"` the element
 * fields the relation does not declare are nullable by default too, like every other field.
 */
const scopedMapperFor = (
  collection: string,
  variable: string,
  ctx: TranslateContext,
): Mapper => {
  const scoped = createScopedMapper(collection, variable, ctx.mapper);
  return ctx.nullRepresentation === "omitted"
    ? withOmittedNullDefault(scoped)
    : scoped;
};

const translateMapIntersection = (
  map: PlanExpression,
  valuesOperand: PlanExpressionOperand,
  ctx: TranslateContext,
): MongooseFilter => {
  const { mapper } = ctx;
  const collectionOperand = getOperandAt(
    map.operands,
    0,
    "Expected a variable in map expression",
  );
  const lambdaOperand = getOperandAt(
    map.operands,
    1,
    "Expected a lambda in map expression",
  );
  if (!isVariable(collectionOperand)) {
    throw new UnsupportedQueryPlanError("Expected a variable in map expression");
  }
  if (!isExpression(lambdaOperand)) {
    throw new UnsupportedQueryPlanError("Expected a lambda in map expression");
  }
  if (lambdaOperand.operator !== "lambda") {
    throw new UnsupportedQueryPlanError("Second operand of map must be a lambda expression");
  }
  const projectionOperand = getOperandAt(
    lambdaOperand.operands,
    0,
    "Map lambda requires a projection operand",
  );
  const variableOperand = getOperandAt(
    lambdaOperand.operands,
    1,
    "Map lambda requires a variable operand",
  );
  if (!isVariable(variableOperand)) {
    throw new UnsupportedQueryPlanError("Invalid map expression structure");
  }
  if (!isValue(valuesOperand) || !Array.isArray(valuesOperand.value)) {
    throw new UnsupportedQueryPlanError("hasIntersection requires an array value");
  }

  const { relation } = resolveFieldReference(collectionOperand.name, mapper);
  if (!relation) {
    throw new Error("map operator requires a relation mapping");
  }
  if (relation.type !== "many") {
    throw new Error("map operator requires a collection relation");
  }
  if (!isVariable(projectionOperand)) {
    throw new UnsupportedQueryPlanError("Map projection must be a variable reference");
  }

  const scopedMapper = scopedMapperFor(
    collectionOperand.name,
    variableOperand.name,
    ctx,
  );
  const elementPath = resolveFieldReference(
    projectionOperand.name,
    scopedMapper,
  ).path;
  // The `$in` below sits inside `$elemMatch`, where Mongoose casts to the element field's type.
  const values = withoutUnequalConstants(
    projectionOperand.name,
    valuesOperand.value,
    scopedMapper,
  );
  const matchingElement = {
    [relation.name]: {
      $elemMatch: buildGuardedFieldFilter(
        elementPath,
        { $in: values },
        false,
        values.includes(null),
      ),
    },
  };
  if (!isNullableReference(projectionOperand.name, scopedMapper)) {
    return matchingElement;
  }
  // A nullable projection is a missing attribute on any element that stores null, which makes
  // the whole CEL `map` raise: no element may be null.
  return {
    $and: [
      {
        [relation.name]: {
          $not: { $elemMatch: buildFieldFilter(elementPath, { $eq: null }) },
        },
      },
      matchingElement,
    ],
  };
};

/**
 * `exists` and `all` over a mapped to-many relation, as `$elemMatch` over its elements. The
 * lambda body is translated with the element as its scope.
 */
function quantifier(operator: "exists" | "all"): FilterOperator {
  return ({ operands }, ctx) => {
    if (operands.length !== 2) {
      throw new UnsupportedQueryPlanError(`${operator} requires exactly two operands`);
    }
    const collectionOperand = getOperandAt(
      operands,
      0,
      `${operator} operator requires a collection operand`,
    );
    const lambdaOperand = getOperandAt(
      operands,
      1,
      `${operator} operator requires a lambda operand`,
    );
    if (!isVariable(collectionOperand) || !isExpression(lambdaOperand)) {
      throw new UnsupportedQueryPlanError("Invalid operands for collection operation");
    }
    if (lambdaOperand.operator !== "lambda") {
      throw new UnsupportedQueryPlanError("Second operand must be a lambda expression");
    }
    const conditionOperand = getOperandAt(
      lambdaOperand.operands,
      0,
      "Lambda operand requires a condition",
    );
    const variableOperand = getOperandAt(
      lambdaOperand.operands,
      1,
      "Lambda operand requires a variable",
    );
    if (!isVariable(variableOperand)) {
      throw new UnsupportedQueryPlanError("Lambda variable must have a name");
    }

    const { relation } = resolveFieldReference(
      collectionOperand.name,
      ctx.mapper,
    );
    if (!relation) {
      throw new Error(`${operator} operator requires a relation mapping`);
    }
    if (relation.type !== "many") {
      throw new Error(`${operator} operator requires a collection relation`);
    }

    const elementCondition = buildFilter(conditionOperand, {
      ...ctx,
      mapper: scopedMapperFor(
        collectionOperand.name,
        variableOperand.name,
        ctx,
      ),
      scope: { kind: "collection", variable: variableOperand.name },
    });

    if (operator === "exists") {
      return { [relation.name]: { $elemMatch: elementCondition } };
    }
    // "No element fails the condition", on a field that must be an array.
    return {
      [relation.name]: {
        $type: "array",
        $not: { $elemMatch: { $nor: [elementCondition] } },
      },
    };
  };
}

const translateLambda = (
  operands: PlanExpressionOperand[],
  ctx: TranslateContext,
): MongooseFilter => {
  const conditionOperand = getOperandAt(
    operands,
    0,
    "lambda operator requires a condition operand",
  );
  const variableOperand = getOperandAt(
    operands,
    1,
    "lambda operator requires a variable operand",
  );
  if (!isVariable(variableOperand)) {
    throw new UnsupportedQueryPlanError("Lambda variable must have a name");
  }

  return buildFilter(conditionOperand, {
    ...ctx,
    // Strip the variable prefix from field references.
    mapper: (key: string) => ({
      field: key.replace(`${variableOperand.name}.`, ""),
    }),
    // A standalone lambda preserves whether its caller entered a collection.
    scope:
      ctx.scope.kind === "root"
        ? ctx.scope
        : { kind: "collection", variable: variableOperand.name },
  });
};

function hierarchy(operator: HierarchyOperator): FilterOperator {
  return ({ operands }, ctx) =>
    buildHierarchyFilter(operator, operands, ctx.mapper);
}
