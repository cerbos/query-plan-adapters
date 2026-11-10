import type { PlanExpression, PlanExpressionOperand, PlanExpressionValue, Value } from "@cerbos/core";

import { FinalUnsupportedQueryPlanError, UnsupportedQueryPlanError } from "./errors";
import {
  COMPARISON_OPERATORS,
  ORDERINGS,
  buildAggregationExpression,
  buildAggregationExpressionFromExpression,
  buildEvaluationGuards,
  celScalarType,
  compare,
  isComparison,
  isListTyped,
  temporalMismatch,
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
  buildListTypeGuard,
  withEvaluationGuards,
  withNullableGuards,
} from "./guards";
import { truth } from "./logic";
import { translateHas } from "./presence";
import { buildHierarchyFilter } from "./hierarchy";
import type { HierarchyOperator } from "./hierarchy";
import type { MapperConfig, MongooseFilter } from "./index";
import type { ScopedMapper } from "./mapper";
import { LAMBDA_BINDING_OPERATORS, foldLiteralCollection } from "./lambda";
import {
  applyValueParser,
  createScopedMapper,
  declaredScalarType,
  isNullableReference,
  relationOfReference,
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

/**
 * A macro over a reference mapped as a to-one relation. To CEL that attribute is a map, and a macro
 * over a map ranges over its KEYS; the adapter translates a macro only as an element match over a
 * collection relation's subdocuments, and has no filter that iterates a subdocument's field names.
 * A refusal rather than a mapper error: the mapping is right, the shape is what cannot be spelled.
 */
const toOneMacroRefusal = (operator: string): UnsupportedQueryPlanError =>
  new UnsupportedQueryPlanError(
    `${operator}() over a to-one relation ranges over the related object's keys in CEL, and ` +
      "a MongoDB filter has no form that iterates a subdocument's field names",
  );

import { escapeRegexValue, lowerRe2PatternForMongo } from "./regex";
import { subMillisecondTimestampBounds } from "./timestamp";
import { isBoolean, isNumber, isObject, isString, lookup, pair, typeofValue } from "./narrowing";
import type { MongoValue } from "./expression";

/** Translates a CONDITIONAL plan's condition into a Mongoose filter. */
export const translateCondition = (
  condition: PlanExpressionOperand,
  ctx: TranslateContext,
): MongooseFilter => {
  rejectNullConstructor(condition, ctx);

  return buildFilter(selectMapMembers(condition, ctx.mapper), ctx);
};

/** A map key that `obj.key` selects exactly as `obj["key"]` does, and a document path can carry. */
const SELECTABLE_KEY = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * `obj["key"]` as the attribute `obj.key`, wherever the caller maps that name. To CEL the two are
 * the same lookup — a missing key is the same error either way — and the mapped name is the
 * field the application stores that member in. A key the mapper does not know is left as an
 * `index`, which is refused there.
 */
const selectMapMembers = (
  operand: PlanExpressionOperand,
  mapper: ScopedMapper,
): PlanExpressionOperand => {
  if (!isExpression(operand)) {
    return operand;
  }

  const operands = operand.operands.map((child) => selectMapMembers(child, mapper));

  const folded =
    foldConstructor(operand.operator, operands) ??
    foldSubMillisecondComparison(operand.operator, operands, mapper);

  if (folded) return folded;
  const [collection, key] = operands;

  if (
    operand.operator === "index" &&
    operands.length === 2 &&
    collection !== undefined &&
    key !== undefined &&
    isVariable(collection) &&
    isValue(key) &&
    isString(key.value) &&
    SELECTABLE_KEY.test(key.value) &&
    // A to-many relation is a list to CEL, where a string index is an error, not a member.
    relationOfReference(collection.name, mapper)?.type !== "many" &&
    resolveMapperConfig(`${collection.name}.${key.value}`, mapper) !== undefined
  ) {
    return { name: `${collection.name}.${key.value}` };
  }

  return { operator: operand.operator, operands };
};

/**
 * A list or map constructor whose every element is a constant, as the constant it builds. A map
 * with a non-string or repeated key stays a constructor: CEL raises on a repeated key, and a
 * document has string keys only.
 */
const foldConstructor = (
  operator: string,
  operands: PlanExpressionOperand[],
): PlanExpressionOperand | undefined => {
  if (operator === "list") {
    return operands.every(isValue) ? { value: operands.map((child) => child.value) } : undefined;
  }

  if (operator !== "struct") return undefined;
  const entries: [string, Value][] = [];

  for (const field of operands) {
    if (!isExpression(field) || field.operator !== "set-field" || field.operands.length !== 2) {
      return undefined;
    }

    const [key, value] = pair(field.operands);

    if (!isValue(key) || !isString(key.value) || !isValue(value)) return undefined;
    entries.push([key.value, value.value]);
  }

  if (new Set(entries.map(([key]) => key)).size !== entries.length) return undefined;

  return { value: Object.fromEntries(entries) };
};

/**
 * A comparison between a timestamp and `timestamp("…")` finer than a millisecond, which the
 * planner folds `now() - duration(…)` into, as the same comparison against the millisecond below
 * it: a stored date holds whole milliseconds, so it is never between the two.
 */
const foldSubMillisecondComparison = (
  operator: string,
  operands: PlanExpressionOperand[],
  mapper: ScopedMapper,
): PlanExpressionOperand | undefined => {
  if (!Object.hasOwn(MIRRORED_COMPARISON, operator) || operands.length !== 2) return undefined;

  const floorOf = (operand: PlanExpressionOperand | undefined) => {
    const literal = operand !== undefined && isExpression(operand) && operand.operator === "timestamp" ? operand.operands[0] : undefined;

    return literal !== undefined && isValue(literal) && isString(literal.value)
      ? subMillisecondTimestampBounds(literal.value)?.floor
      : undefined;
  };

  const literalAt = floorOf(operands[0]) ? 0 : floorOf(operands[1]) ? 1 : undefined;

  if (literalAt === undefined) return undefined;
  const other = operands[1 - literalAt]!;

  if (celScalarType(other, mapper) !== "timestamp" || floorOf(other)) return undefined;

  if (isExpression(other) && other.operator === "timestamp" && isValue(other.operands[0]!)) return undefined;

  const floor: PlanExpressionOperand = {
    operator: "timestamp",
    operands: [{ value: floorOf(operands[literalAt])!.toISOString() }],
  };

  const atMost: PlanExpressionOperand = { operator: "le", operands: [other, floor] };
  const above: PlanExpressionOperand = { operator: "gt", operands: [other, floor] };

  if (!isComparison(operator)) return undefined;

  switch (literalAt === 1 ? operator : MIRRORED_COMPARISON[operator]) {
    case "lt":
    case "le":
      return atMost;
    case "gt":
    case "ge":
      return above;
    case "eq":
      return { operator: "and", operands: [atMost, above] };
    default:
      return { operator: "or", operands: [atMost, above] };
  }
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
const FILTER_OPERATORS = {
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
  has: ({ operands }, ctx) => translateHas(operands, ctx, false),
  hasIntersection: ({ operands }, ctx) =>
    translateHasIntersection(operands, ctx),
  exists: quantifier("exists"),
  all: quantifier("all"),
  // `a.isSubset(b)` is "every element of a is in b": CEL's `in` on each element, duplicates and
  // all, which is the element-wise all() the adapter already translates.
  isSubset: ({ operands }, ctx) => {
    const [subset, superset] = operands;

    if (operands.length !== 2 || subset === undefined || superset === undefined) {
      throw new UnsupportedQueryPlanError("isSubset requires exactly two operands");
    }

    return buildFilter(everyElementIn("all", subset, superset, false), ctx);
  },
  exists_one: () => {
    throw new UnsupportedQueryPlanError(
      "exists_one requires exact match cardinality and is unsupported",
    );
  },
  // filter(), map(), except() and intersect() yield a list, not a boolean: in boolean position
  // that is a runtime type error, which denies under either polarity. `filter(...)` is not
  // `size(filter(...)) > 0` (cerbos/query-plan-adapters#313).
  filter: listInBooleanPosition,
  map: listInBooleanPosition,
  except: listInBooleanPosition,
  intersect: listInBooleanPosition,
  lambda: ({ operands }, ctx) => translateLambda(operands, ctx),
  // A ternary evaluates only the branch its condition selects, and CEL's three-valued logic says
  // exactly which document that leaves an error.
  if: (expression, ctx) => {
    if (ctx.scope.kind === "collection") {
      throw new UnsupportedQueryPlanError(
        "if aggregation expressions inside collection predicates are unsupported",
      );
    }

    return { $expr: { $eq: [truth(expression, ctx.mapper), true] } };
  },
  ancestorOf: hierarchy("ancestorOf"),
  descendentOf: hierarchy("descendentOf"),
  overlaps: hierarchy("overlaps"),
} satisfies Record<string, FilterOperator>;

/** Builds Mongoose conditions from a Cerbos expression in boolean position. */
const buildFilter = (
  expression: PlanExpressionOperand,
  ctx: TranslateContext,
): MongooseFilter =>
  isExpression(expression) && LOGIC_FALLBACK_OPERATORS.has(expression.operator)
    ? withLogicFallback(expression, ctx, true, () => buildFilterDirect(expression, ctx))
    : buildFilterDirect(expression, ctx);

const buildFilterDirect = (
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

  if (
    (operator === "exists" || operator === "all") &&
    operands.length === 2 &&
    collectionOperand !== undefined &&
    lambdaOperand !== undefined &&
    isExpression(collectionOperand) &&
    collectionOperand.operator === "list"
  ) {
    return foldConstructedList(operator, collectionOperand, lambdaOperand, ctx);
  }

  if (!Object.hasOwn(FILTER_OPERATORS, operator)) {
    throw new UnsupportedQueryPlanError(`Unsupported operator: ${operator}`);
  }

  return lookup(FILTER_OPERATORS, operator)!(expression, ctx);
};

/** The operators whose refusal at the root the three-valued evaluation may answer instead. */
const LOGIC_FALLBACK_OPERATORS = new Set([
  "eq", "ne", "lt", "le", "gt", "ge", "in", "hasIntersection", "exists", "all", "exists_one",
  "isSubset", "ancestorOf", "descendentOf", "overlaps",
]);

/**
 * `build`'s filter; where it refuses at the top level, the condition evaluated in CEL's
 * three-valued logic instead, kept where it is `polarity`. The refusal stands where the
 * three-valued evaluation has no form for the condition either.
 */
const withLogicFallback = (
  condition: PlanExpressionOperand,
  ctx: TranslateContext,
  polarity: boolean,
  build: () => MongooseFilter,
): MongooseFilter => {
  try {
    return build();
  } catch (refusal) {
    if (
      !(refusal instanceof UnsupportedQueryPlanError) ||
      refusal instanceof FinalUnsupportedQueryPlanError ||
      ctx.scope.kind === "collection" ||
      ctx.withinNegation ||
      (ctx.nullRepresentation === "omitted" && holdsNullLiteral(condition))
    ) {
      throw refusal;
    }

    try {
      return { $expr: { $eq: [truth(condition, ctx.mapper), polarity] } };
    } catch (error) {
      if (error instanceof UnsupportedQueryPlanError) throw refusal;
      throw error;
    }
  }
};

const holdsNullLiteral = (operand: PlanExpressionOperand): boolean =>
  isValue(operand)
    ? carriesNullOperand(operand.value)
    : isExpression(operand) && operand.operands.some(holdsNullLiteral);

/** A list where CEL needs a boolean: a runtime type error at the root, refused inside a macro. */
function listInBooleanPosition(expression: PlanExpression, ctx: TranslateContext): MongooseFilter {
  if (ctx.scope.kind === "collection") {
    throw new UnsupportedQueryPlanError(`${expression.operator}() returns a list, not a boolean`);
  }

  return { $expr: { $eq: [truth(expression, ctx.mapper), true] } };
}

/** Every leaf resolves its field in the active collection scope before emission. */
const emitLeafComparison = (
  ctx: TranslateContext,
  fieldName: string,
  comparison: MongoValue,
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
  comparison: MongoValue,
  constant: MongoValue,
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

  // Mongoose casts true onto a String path as "true", which CEL would not accept as a condition.
  return buildGuardedFieldFilter(
    path,
    { $type: "bool", $eq: true },
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

  // `!!x` is `x`, errors included, and eliminating the pair keeps a leaf that answers an error
  // as `false` from being flipped twice by nested `$nor`s.
  if (isExpression(operand) && operand.operator === "has") {
    return translateHas(operand.operands, ctx, true);
  }

  if (isExpression(operand) && operand.operator === "not") {
    return buildFilter(
      getOperandAt(operand.operands, 0, "not operator requires at least one operand"),
      ctx,
    );
  }

  if (isExpression(operand)) {
    const negated = negatedRewrite(operand, ctx);

    if (negated !== undefined) {
      return negated;
    }
  }

  // An ordering between a field and a constant negates to its complement, so it keeps the
  // positive leaf's semantics: MongoDB's `$lt` compares only values of the constant's own BSON
  // type, and a constant CEL cannot order against the field answers `false` either way. A `$nor`
  // over the ordering would instead be TRUE for a null, or a value of another type, where CEL
  // raises an error and denies (cerbos/query-plan-adapters#516).
  const complement = complementedOrdering(operand);

  if (complement) {
    return buildFilter(complement, ctx);
  }

  if (
    (isExpression(operand) &&
      ["exists", "exists_one", "all"].includes(operand.operator)) ||
    !nullableGuardIsExact(operand, ctx)
  ) {
    return withLogicFallback(operand, ctx, false, () => {
      throw new UnsupportedQueryPlanError(
        "not over nullable fields or collection macros cannot preserve Cerbos error semantics",
      );
    });
  }

  return withLogicFallback(operand, ctx, false, () => negateGenerally(operand, ctx));
};

/** `!operand` as a $nor, with every guard the operand needs ANDed outside it. */
const negateGenerally = (operand: PlanExpressionOperand, ctx: TranslateContext): MongooseFilter => {
  // withEvaluationGuards ANDs its conjuncts OUTSIDE this $nor, which is where the
  // absent-parent requirement has to sit: inside, the negation would flip it along with
  // the predicate and readmit every parentless document (#315, #316). A null or absent list
  // is the same kind of error CEL denies, so its array requirement sits there too (#534).
  // MongoDB accepts $expr only at the top level, and inside a collection predicate the guards a
  // negation needs outside its $nor would be element-scoped $expr.
  if (ctx.scope.kind === "collection" && buildEvaluationGuards(operand, ctx.mapper).length > 0) {
    throw new UnsupportedQueryPlanError(
      "A negated expression inside a collection predicate needs $expr guards, which MongoDB accepts only at the top level",
    );
  }

  let negated: MongooseFilter = { $nor: [buildFilter(operand, { ...ctx, withinNegation: true })] };

  // A bare field in boolean position raises in CEL unless it holds a boolean, so its type is
  // required outside the $nor, which would otherwise match every other value.
  if (isVariable(operand)) {
    const { path } = resolveFieldReference(operand.name, ctx.mapper);
    negated = { $and: [buildFieldFilter(path, { $type: "bool" }), negated] };
  }

  const guarded = withEvaluationGuards(negated, [operand], ctx.mapper);
  const listGuard = buildListTypeGuard(operand, ctx.mapper, ctx.scope.kind === "root");

  return listGuard ? { $and: [listGuard, guarded] } : guarded;
};

/**
 * The negation of a shape this adapter rewrites into another, stated as the rewrite of its
 * complement so that the errors CEL raises stay errors under both polarities; undefined for any
 * other shape, which the generic `$nor` path takes.
 */
const negatedRewrite = (
  operand: PlanExpression,
  ctx: TranslateContext,
): MongooseFilter | undefined => {
  const [left, right] = operand.operands;

  if (operand.operands.length !== 2 || left === undefined || right === undefined) {
    return undefined;
  }

  if (
    operand.operator === "in" &&
    isValue(left) &&
    isExpression(right) &&
    right.operator === "map"
  ) {
    return translateMapIntersection(right, { value: [left.value] }, ctx, true);
  }

  // `!hasIntersection(coll.map(e, e.field), [..])`, either operand order: a `$nor` over the
  // positive filter would flip its no-null-projection guard too, and match the documents whose
  // map() raises (an element without the field), which CEL denies under either polarity.
  if (operand.operator === "hasIntersection") {
    const [map, values] =
      isExpression(right) && right.operator === "map" ? [right, left] : [left, right];

    if (isExpression(map) && map.operator === "map") {
      return translateMapIntersection(map, values, ctx, true);
    }

    return undefined;
  }

  if (operand.operator === "isSubset") {
    return buildFilter(everyElementIn("exists", left, right, true), ctx);
  }

  if (matchesInvalidPattern(operand)) {
    return buildFilter(operand, ctx);
  }

  if (operand.operator === "eq" || operand.operator === "ne") {
    const flipped = operand.operator === "eq" ? "ne" : "eq";

    const rewritten =
      emptyListComparison(flipped, left, right) ??
      mapLiteralLookupMembership(flipped, left, right, ctx.mapper);

    return rewritten === undefined ? undefined : buildFilter(rewritten, ctx);
  }

  return undefined;
};

/** The iteration variable of the macros this module builds itself. */
const ELEMENT_VARIABLE = "__cerbos_element";

/**
 * `collection.all(e, e in other)` (or `exists`, and with the membership negated): the element-wise
 * form CEL's list functions reduce to. A literal collection is folded, a mapped one becomes an
 * `$elemMatch`, exactly as a policy that spelled the macro out.
 */
const everyElementIn = (
  quantifierOperator: "all" | "exists",
  collection: PlanExpressionOperand,
  other: PlanExpressionOperand,
  negateMembership: boolean,
): PlanExpression => {
  const membership: PlanExpression = {
    operator: "in",
    operands: [{ name: ELEMENT_VARIABLE }, other],
  };

  return {
    operator: quantifierOperator,
    operands: [
      collection,
      {
        operator: "lambda",
        operands: [
          negateMembership ? { operator: "not", operands: [membership] } : membership,
          { name: ELEMENT_VARIABLE },
        ],
      },
    ],
  };
};

/**
 * `a.except(b) == []` and `intersect(a, b) == []` (or `!=`) as element-wise macros. Neither list
 * is materialised: `a - b` is empty exactly when every element of `a` is in `b`, and the
 * intersection exactly when no element of either is in the other, whichever list Cerbos ranges
 * over. Only the emptiness of the result is translated; its size or contents are not.
 */
const emptyListComparison = (
  operator: string,
  left: PlanExpressionOperand,
  right: PlanExpressionOperand,
): PlanExpression | undefined => {
  if (operator !== "eq" && operator !== "ne") {
    return undefined;
  }

  const isEmptyList = (operand: PlanExpressionOperand): boolean =>
    isValue(operand) && Array.isArray(operand.value) && operand.value.length === 0;

  const setOperation = isEmptyList(right) ? left : isEmptyList(left) ? right : undefined;

  if (
    setOperation === undefined ||
    !isExpression(setOperation) ||
    setOperation.operands.length !== 2
  ) {
    return undefined;
  }

  const [first, second] = pair(setOperation.operands);

  const empty = operator === "eq";

  if (setOperation.operator === "except") {
    return empty
      ? everyElementIn("all", first, second, false)
      : everyElementIn("exists", first, second, true);
  }

  if (setOperation.operator === "intersect") {
    // Range over the list the document holds; membership in the constant one is a plain `$in`.
    const [collection, other] =
      isVariable(second) && !isVariable(first) ? [second, first] : [first, second];

    return empty
      ? everyElementIn("all", collection, other, true)
      : everyElementIn("exists", collection, other, false);
  }

  return undefined;
};

/**
 * `{"k1": v1, ...}[field] == c` (or `!=`, either side) as `field in [the keys whose value is (or
 * is not) c]`. A key the map lacks is an error to CEL under both operators, and a field outside
 * the key list matches neither membership. Only a map of scalar constants is decided here.
 */
const mapLiteralLookupMembership = (
  operator: string,
  left: PlanExpressionOperand,
  right: PlanExpressionOperand,
  mapper: ScopedMapper,
): PlanExpression | undefined => {
  if (operator !== "eq" && operator !== "ne") {
    return undefined;
  }

  const [lookup, constant] = isValue(right) ? [left, right] : [right, left];

  if (
    !isValue(constant) ||
    !isScalarConstant(constant.value) ||
    !isExpression(lookup) ||
    lookup.operator !== "index" ||
    lookup.operands.length !== 2
  ) {
    return undefined;
  }

  const [map, key] = pair(lookup.operands);

  if (!isExpression(map) || map.operator !== "struct" || !isVariable(key)) {
    return undefined;
  }

  if (relationOfReference(key.name, mapper)?.type === "many") {
    throw new UnsupportedQueryPlanError(
      "A list cannot be a map key in CEL, so indexing a map constant by a collection is an " +
        "error, where membership would match its elements",
    );
  }

  const entries = map.operands.map((entry) => {
    const [entryKey, entryValue] = isExpression(entry) ? entry.operands : [];

    if (
      !isExpression(entry) ||
      entry.operator !== "set-field" ||
      entry.operands.length !== 2 ||
      entryKey === undefined ||
      entryValue === undefined ||
      !isValue(entryKey) ||
      !isValue(entryValue) ||
      !isScalarConstant(entryKey.value) ||
      !isScalarConstant(entryValue.value)
    ) {
      return undefined;
    }

    return [entryKey.value, entryValue.value] as const;
  });

  if (entries.some((entry) => entry === undefined)) {
    return undefined;
  }

  const keys = entries
    .filter((entry) => (entry![1] === constant.value) === (operator === "eq"))
    .map((entry) => entry![0]);

  return { operator: "in", operands: [key, { value: keys }] };
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

/** `!(a < b)` is `a >= b` whenever CEL can order `a` and `b` at all. */
const COMPLEMENTED_ORDERING = {
  lt: "ge",
  le: "gt",
  gt: "le",
  ge: "lt",
} satisfies Partial<Record<string, ComparisonOperator>>;

/** The complement of an ordering between one field and one constant; undefined for anything else. */
const complementedOrdering = (
  operand: PlanExpressionOperand,
): PlanExpression | undefined => {
  if (!isExpression(operand)) return undefined;
  const complement = lookup(COMPLEMENTED_ORDERING, operand.operator);
  const [left, right] = operand.operands;

  if (
    complement === undefined ||
    operand.operands.length !== 2 ||
    !left ||
    !right ||
    !((isVariable(left) && isValue(right)) || (isValue(left) && isVariable(right)))
  ) {
    return undefined;
  }

  return { operator: complement, operands: operand.operands };
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

  const rewritten =
    emptyListComparison(operator, leftOperand, rightOperand) ??
    mapLiteralLookupMembership(operator, leftOperand, rightOperand, mapper);

  if (rewritten !== undefined) {
    return buildFilter(rewritten, ctx);
  }

  const comparisonExpression: PlanExpression = { operator, operands: bothOperands };

  // A timestamp or duration against a value of another type: no ordering overload, so an error on
  // every document; `==` is false. The guard the aggregation lowering declares for an ordering
  // keeps every document out, outside any $nor a negation wraps this in.
  if (temporalMismatch(leftOperand, rightOperand, mapper)) {
    if (ctx.scope.kind === "collection") {
      throw new UnsupportedQueryPlanError(
        "A comparison between CEL types inside a collection predicate needs $expr",
      );
    }

    const decided = !ORDERINGS.has(operator) && operator === "ne";

    return withEvaluationGuards({ $expr: decided }, [comparisonExpression], mapper);
  }

  // A null constant asks only whether the field is stored, which a Date still says.
  if (
    bothOperands.some(
      (operand) =>
        isVariable(operand) &&
        resolveMapperConfig(operand.name, mapper)?.valueType === "dateTime",
    ) &&
    !bothOperands.some((operand) => isValue(operand) && operand.value === null)
  ) {
    throw new FinalUnsupportedQueryPlanError(
      "Bare temporal field comparison cannot preserve CEL string equality: stored Dates discard the original lexical spelling; compare timestamp(...) values instead",
    );
  }

  const composite = bothOperands.find(
    (operand): operand is PlanExpressionValue => isValue(operand) && isObject(operand.value),
  );

  const other = composite && bothOperands.find((operand) => operand !== composite);

  if (composite && other && (operator === "eq" || operator === "ne") && !isValue(other)) {
    return translateCompositeEquality(operator, other, composite.value, ctx);
  }

  if (composite && other && isExpression(other)) {
    throw new UnsupportedQueryPlanError(
      "An ordering against a list or map constant inside an expression is unsupported",
    );
  }

  const roundedLiteral = roundSubMillisecondTimestampLiteral(
    operator,
    leftOperand,
    rightOperand,
    ctx,
  );

  if (roundedLiteral !== undefined) {
    return roundedLiteral;
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
    if (ctx.scope.kind === "collection") {
      throw new UnsupportedQueryPlanError(
        `${operator} aggregation expressions inside collection predicates are unsupported`,
      );
    }

    // The comparison itself is guarded too: an ordering between two types CEL cannot order is an
    // error, which the guard keeps out under either polarity.
    const expr = compare(
      operator,
      buildAggregationExpression(leftOperand, mapper),
      buildAggregationExpression(rightOperand, mapper),
    );

    return withEvaluationGuards({ $expr: expr }, [comparisonExpression], mapper);
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

  // CEL has no ordering between types, so `aNumber < "5"` is an error that denies, where
  // Mongoose would cast `"5"` to the declared Number and compare. A negation reaches here as the
  // complemented ordering (`translateNot`), so this `false` holds under both polarities.
  if (
    effectiveOperator !== "eq" &&
    effectiveOperator !== "ne" &&
    !canOrderAgainstDeclaredType(variableOperand.name, valueOperand.value, mapper)
  ) {
    return emitLeafComparison(
      ctx,
      variableOperand.name,
      { $in: [] },
      { nullable: false, requireExists: false },
    );
  }

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

/**
 * A comparison against `timestamp("…")` finer than a millisecond, which the planner folds
 * `now() - duration("24h")` into: a BSON Date is never between the two milliseconds around the
 * literal, so an ordering is the same ordering against the one of them that keeps the answer,
 * and an equality is false on every document CEL can evaluate. Only a timestamp on the other
 * side is rewritten; a string or number there has no ordering in CEL, which the comparison
 * itself answers. Undefined when the comparison is not that shape.
 */
const roundSubMillisecondTimestampLiteral = (
  operator: ComparisonOperator,
  leftOperand: PlanExpressionOperand,
  rightOperand: PlanExpressionOperand,
  ctx: TranslateContext,
): MongooseFilter | undefined => {
  const bounds = (operand: PlanExpressionOperand) => {
    const literal = isExpression(operand) && operand.operator === "timestamp" ? operand.operands[0] : undefined;

    return literal !== undefined && isValue(literal) && isString(literal.value)
      ? subMillisecondTimestampBounds(literal.value)
      : undefined;
  };

  const [literalOperand, other, literalOnLeft] = bounds(leftOperand)
    ? [leftOperand, rightOperand, true]
    : [rightOperand, leftOperand, false];

  const literal = bounds(literalOperand);

  if (literal === undefined || bounds(other) !== undefined) {
    return undefined;
  }

  const otherType = celScalarType(other, ctx.mapper);

  if (otherType === undefined || otherType === "mixed") {
    return undefined;
  }

  // As `other OP literal`.
  const effective = literalOnLeft ? MIRRORED_COMPARISON[operator] : operator;

  if (effective === "eq" || effective === "ne") {
    if (otherType !== "timestamp") {
      return undefined;
    }

    return effective === "eq"
      ? { $expr: false }
      : withEvaluationGuards({ $expr: true }, [other], ctx.mapper);
  }

  // An ordering against another type is an error CEL raises whichever bound stands in.
  const bound = effective === "lt" || effective === "ge" ? literal.ceil : literal.floor;

  const rounded: PlanExpressionOperand = {
    operator: "timestamp",
    operands: [{ value: bound.toISOString() }],
  };

  return buildFilter(
    { operator, operands: literalOnLeft ? [rounded, other] : [other, rounded] },
    ctx,
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

  // `needle in coll.map(e, e.field)` is `hasIntersection(coll.map(e, e.field), [needle])`.
  if (
    isValue(leftOperand) &&
    isExpression(rightOperand) &&
    rightOperand.operator === "map"
  ) {
    return translateMapIntersection(
      rightOperand,
      { value: [leftOperand.value] },
      ctx,
      false,
    );
  }

  if (
    isValue(leftOperand) &&
    isExpression(rightOperand) &&
    rightOperand.operator === "if"
  ) {
    return buildFilter(
      membershipInConditionalList(leftOperand.value, rightOperand),
      ctx,
    );
  }

  if (isValue(leftOperand) && isVariable(rightOperand)) {
    if (relationOfReference(rightOperand.name, ctx.mapper)?.type === "one") {
      throw new UnsupportedQueryPlanError(
        "`in` over a to-one relation tests the related object's keys in CEL, which are the " +
          "fields its subdocument carries, and the adapter has no filter for a subdocument's " +
          "field names",
      );
    }

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

const isScalarConstant = (value: MongoValue): boolean =>
  value === null || isString(value) || isNumber(value) || isBoolean(value);

/**
 * `needle in (cond ? [..] : [..])` with constant branches: the membership is decided per branch
 * at translation time, which leaves the condition — still evaluated, and still able to raise.
 * This is how the planner spells `"role" in runtime.effectiveDerivedRoles`.
 */
const membershipInConditionalList = (
  needle: MongoValue,
  conditional: PlanExpression,
): PlanExpressionOperand => {
  const [condition, thenOperand, elseOperand] = conditional.operands;

  if (
    conditional.operands.length !== 3 ||
    condition === undefined ||
    thenOperand === undefined ||
    elseOperand === undefined
  ) {
    throw new UnsupportedQueryPlanError("if operator requires three operands");
  }

  const contains = (branch: PlanExpressionOperand): boolean => {
    if (
      !isValue(branch) ||
      !Array.isArray(branch.value) ||
      !isScalarConstant(needle) ||
      !branch.value.every(isScalarConstant)
    ) {
      throw new UnsupportedQueryPlanError(
        "Membership in a conditional list is translated only when both branches are lists of " +
          "scalar constants and the needle is a scalar constant",
      );
    }

    return branch.value.some((element) => element === needle);
  };

  const inThen = contains(thenOperand);
  const inElse = contains(elseOperand);
  const negatedCondition = { operator: "not", operands: [condition] };

  if (inThen === inElse) {
    // Either branch gives the same answer, but CEL still evaluates the condition, whose error
    // denies: `c || !c` holds exactly where `c` evaluates, `c && !c` nowhere.
    return {
      operator: inThen ? "or" : "and",
      operands: [condition, negatedCondition],
    };
  }

  return inThen ? condition : negatedCondition;
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
  needles: MongoValue[],
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
    !isString(patternOperand.value)
  ) {
    throw new UnsupportedQueryPlanError("matches operator requires a string regex pattern");
  }

  const lowered = lowerRe2PatternForMongo(patternOperand.value);

  return emitLeafComparison(
    ctx,
    fieldOperand.name,
    // A pattern CEL cannot compile is an evaluation error, which denies the document under
    // either polarity; nothing is in an empty list, in either scope.
    "invalid" in lowered ? { $in: [] } : { $regex: lowered.pattern },
    { nullable: false, requireExists: false },
  );
};

/** `matches()` over a constant pattern Go's regexp rejects: an error to CEL, false either way. */
const matchesInvalidPattern = (operand: PlanExpression): boolean => {
  const pattern = operand.operands[1];

  return (
    operand.operator === "matches" &&
    pattern !== undefined &&
    isValue(pattern) &&
    isString(pattern.value) &&
    "invalid" in lowerRe2PatternForMongo(pattern.value)
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
    !isString(rightOperand.value) ||
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
  // rewrite lowerRe2PatternForMongo applies to a trailing `$`.
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
    return translateMapIntersection(leftOperand, rightOperand, ctx, false);
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
  constant: MongoValue,
  mapper: ScopedMapper,
): boolean => {
  const declared = declaredScalarType(reference, mapper);

  return (
    declared === undefined || constant === null || typeofValue(constant) === declared
  );
};

/**
 * False when CEL cannot order `reference` against `constant`: the constant is not a number, a
 * string or a boolean (ordering against null, a list or a map is always an error), or it is a
 * scalar of another type than the one `reference` declares.
 */
const canOrderAgainstDeclaredType = (
  reference: string,
  constant: MongoValue,
  mapper: ScopedMapper,
): boolean => {
  if (
    !isNumber(constant) &&
    !isString(constant) &&
    !isBoolean(constant)
  ) {
    return false;
  }

  const declared = declaredScalarType(reference, mapper);

  return declared === undefined || typeofValue(constant) === declared;
};

/** A constant list without the elements `reference`'s declared type can never equal. */
const withoutUnequalConstants = (
  reference: string,
  constants: MongoValue[],
  mapper: ScopedMapper,
): MongoValue[] =>
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
): ScopedMapper => {
  const scoped = createScopedMapper(collection, variable, ctx.mapper);

  return ctx.nullRepresentation === "omitted"
    ? { lookup: withOmittedNullDefault(scoped.lookup), nullableDefault: false, bound: [] }
    : scoped;
};

const translateMapIntersection = (
  map: PlanExpression,
  valuesOperand: PlanExpressionOperand,
  ctx: TranslateContext,
  negated: boolean,
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
    throw toOneMacroRefusal("map");
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

  const elementMatch = {
    $elemMatch: buildGuardedFieldFilter(
      elementPath,
      { $in: values },
      false,
      values.includes(null),
    ),
  };

  // A nullable projection is a missing attribute on any element that stores null, which makes
  // the whole CEL `map` raise under either polarity: no element may be null.
  const noMissingProjection = isNullableReference(
    projectionOperand.name,
    scopedMapper,
  )
    ? [
        {
          [relation.name]: {
            $not: { $elemMatch: buildFieldFilter(elementPath, { $eq: null }) },
          },
        },
      ]
    : [];

  if (negated) {
    // No element matches, over a collection that is there: an absent or null list is an error
    // to CEL, which a bare `$not` would match. A relation reached through a parent that can be
    // absent has no single array to require.
    if (relation.requiresParent !== undefined) {
      throw new UnsupportedQueryPlanError(
        "A negated membership in a projection reached through an absent-able parent cannot " +
          "require the collection to exist",
      );
    }

    return {
      $and: [
        { [relation.name]: { $type: "array" } },
        ...noMissingProjection,
        { [relation.name]: { $not: elementMatch } },
      ],
    };
  }

  const matchingElement = { [relation.name]: elementMatch };

  return noMissingProjection.length === 0
    ? matchingElement
    : { $and: [...noMissingProjection, matchingElement] };
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

    if (lambdaOperand.operands.length !== 2) {
      throw new UnsupportedQueryPlanError(
        `two-variable ${operator}() binds each element's list index or map key, and ` +
          "$elemMatch exposes neither: it matches an array element by its own fields, and " +
          "has no form that iterates a subdocument's field names",
      );
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
      throw toOneMacroRefusal(operator);
    }

    // The planner folds a body that reads no element to a constant (`t.name in []`). A missing
    // or null list is an error CEL denies under both macros, so each needs an actual array.
    if (isValue(conditionOperand) && conditionOperand.value === false) {
      return operator === "exists"
        ? { [relation.name]: { $in: [] } }
        : { [relation.name]: { $type: "array", $size: 0 } };
    }

    if (isValue(conditionOperand) && conditionOperand.value === true && operator === "all") {
      return { [relation.name]: { $type: "array" } };
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

/**
 * `[R.attr.a, R.attr.b].exists(s, body)`: a macro over a list the policy builds from attributes,
 * unrolled into the body once per element, as a literal list is. CEL builds the whole list before
 * it ranges over it, so one element it cannot evaluate — a missing attribute — is an error that
 * denies the document whatever the bodies say: every element's guard is ANDed outside them.
 */
const foldConstructedList = (
  operator: "exists" | "all",
  list: PlanExpression,
  lambda: PlanExpressionOperand,
  ctx: TranslateContext,
): MongooseFilter => {
  if (ctx.scope.kind !== "root") {
    throw new UnsupportedQueryPlanError(
      `${operator}() over a constructed list inside a collection predicate is unsupported`,
    );
  }

  if (
    !isExpression(lambda) ||
    lambda.operator !== "lambda" ||
    lambda.operands.length !== 2
  ) {
    throw new UnsupportedQueryPlanError(
      `${operator}() over a constructed list requires a single-variable lambda`,
    );
  }

  const [body, variable] = pair(lambda.operands);

  if (!isVariable(variable)) {
    throw new UnsupportedQueryPlanError("Lambda variable must have a name");
  }

  const elements = list.operands;

  if (!elements.every((element) => isVariable(element) || isValue(element))) {
    throw new UnsupportedQueryPlanError(
      `${operator}() over a constructed list is translated only when every element is an ` +
        "attribute or a constant",
    );
  }

  if (elements.length === 0) {
    return { $expr: operator === "all" };
  }

  const bodies = elements.map((element) =>
    buildFilter(substituteElement(body, variable.name, element), ctx),
  );

  return withEvaluationGuards(
    operator === "exists" ? { $or: bodies } : { $and: bodies },
    elements,
    ctx.mapper,
  );
};

/**
 * `body` with every reference to the iteration variable replaced by `element`. A nested macro
 * that rebinds the name shadows it; a field read through the variable (`s.name`) is refused,
 * since the element is a scalar attribute or constant here.
 */
const substituteElement = (
  operand: PlanExpressionOperand,
  name: string,
  element: PlanExpressionOperand,
): PlanExpressionOperand => {
  if (isVariable(operand)) {
    if (operand.name === name) {
      return element;
    }

    if (operand.name.startsWith(`${name}.`)) {
      throw new UnsupportedQueryPlanError(
        `Cannot read ${operand.name}: the elements of a constructed list are translated as scalars`,
      );
    }

    return operand;
  }

  if (!isExpression(operand)) {
    return operand;
  }

  if (operand.operator === "lambda") {
    const bound = operand.operands.slice(1);

    if (bound.some((child) => isVariable(child) && child.name === name)) {
      return operand;
    }
  }

  return {
    operator: operand.operator,
    operands: operand.operands.map((child) => substituteElement(child, name, element)),
  };
};

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
    mapper: {
      lookup: (key) => ({ field: key.replace(`${variableOperand.name}.`, "") }),
      nullableDefault: false,
      bound: [],
    },
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

/**
 * `==`/`!=` between a field or a map() projection and a list or map constant.
 *
 * A field that declares a scalar `valueType` never equals a list or a map, so the answer is
 * settled without a query. Otherwise the list is compared whole inside `$expr`, where `$eq`
 * compares two arrays element by element and in order, as CEL's list equality does. Only a list of
 * scalars is compared this way: MongoDB's NaN equals NaN where CEL's does not, and it compares
 * embedded documents field by field in stored order where CEL's map equality ignores order.
 */
const translateCompositeEquality = (
  operator: "eq" | "ne",
  other: PlanExpressionOperand,
  constant: MongoValue,
  ctx: TranslateContext,
): MongooseFilter => {
  const { mapper } = ctx;

  if (ctx.scope.kind === "collection") {
    throw new UnsupportedQueryPlanError(
      "Whole-value comparison inside a collection predicate needs $expr, which MongoDB accepts only at the top level",
    );
  }

  if (isVariable(other)) {
    const config = resolveMapperConfig(other.name, mapper);

    if (config?.valueType && !config.valueParser) {
      return withNullableGuards({ $expr: { $eq: [operator === "ne", true] } }, [other], mapper);
    }
  }

  if (!Array.isArray(constant) || !constant.every(isComparableScalar)) {
    throw new UnsupportedQueryPlanError(
      "Whole-value comparison with a map, or with a list holding a list, a map or NaN, is " +
        "unsupported: MongoDB compares embedded documents in stored field order and NaN equal to NaN",
    );
  }

  const [list, guard] = listExpression(other, mapper);
  const compared = { $expr: { [COMPARISON_OPERATORS[operator]]: [list, { $literal: constant }] } };
  const filter = withEvaluationGuards(compared, [other], mapper);

  return guard ? { $and: [guard, filter] } : filter;
};

/** A scalar CEL and MongoDB's `$eq` agree on: a string, a boolean, null, or a number other than NaN. */
const isComparableScalar = (value: MongoValue): boolean =>
  value === null ||
  isString(value) ||
  isBoolean(value) ||
  (isNumber(value) && !Number.isNaN(value));

/**
 * The list `operand` stands for, as an aggregation expression, and the filter requiring it to be
 * stored as an array (undefined for a plain field, whose non-array value simply compares unequal):
 * a plain field as itself, and a to-many relation's projection or a map() over one as a `$map`
 * over its elements. `$map` reads a missing element field as null.
 */
const listExpression = (
  operand: PlanExpressionOperand,
  mapper: ScopedMapper,
): [MongoValue, MongooseFilter | undefined] => {
  if (isVariable(operand)) {
    const resolved = resolveFieldReference(operand.name, mapper);

    if (!resolved.relation) return [`$${resolved.path.join(".")}`, undefined];
    const relation = resolveMapperConfig(operand.name, mapper)?.relation;

    if (!(relation?.type === "many" && relation.field && relation.requiresParent === undefined)) {
      throw new UnsupportedQueryPlanError(
        "Whole-list comparison needs a plain field or a to-many relation's projected field",
      );
    }

    return projectedList(relation, relation.field, mapper);
  }

  // A list the plan computes (a concatenation, intersect(), except(), a ternary between lists):
  // its value, null where CEL raises, which the caller's evaluation guards keep out.
  if (
    isExpression(operand) &&
    isListTyped(operand) &&
    ["add", "intersect", "except", "if", "list"].includes(operand.operator)
  ) {
    return [buildAggregationExpression(operand, mapper), undefined];
  }

  if (!(isExpression(operand) && operand.operator === "map" && operand.operands.length === 2)) {
    throw new UnsupportedQueryPlanError(
      "Whole-list comparison needs a field, a relation projection or a map() over a relation",
    );
  }

  const [collection, lambda] = pair(operand.operands);
  const projectionOperand = isExpression(lambda) ? lambda.operands[0] : undefined;
  const elementOperand = isExpression(lambda) ? lambda.operands[1] : undefined;

  if (
    !isVariable(collection) ||
    !isExpression(lambda) ||
    lambda.operator !== "lambda" ||
    !projectionOperand ||
    !isVariable(projectionOperand) ||
    !elementOperand ||
    !isVariable(elementOperand)
  ) {
    throw new UnsupportedQueryPlanError(
      "map() in a whole-list comparison must project one field of each element",
    );
  }

  const relation = resolveMapperConfig(collection.name, mapper)?.relation;

  if (!(relation?.type === "many" && relation.requiresParent === undefined)) {
    throw new UnsupportedQueryPlanError("map() in a whole-list comparison needs a to-many relation");
  }

  const projection = projectionOperand.name;
  const element = elementOperand.name;

  const field =
    projection === element
      ? relation.field
      : projection.startsWith(`${element}.`)
        ? projection.slice(element.length + 1)
        : undefined;

  if (field === undefined) {
    throw new UnsupportedQueryPlanError(
      "map() in a whole-list comparison must project one field of each element",
    );
  }

  return projectedList(relation, field, mapper);
};

const projectedList = (
  relation: NonNullable<MapperConfig["relation"]>,
  field: string,
  mapper: ScopedMapper,
): [MongoValue, MongooseFilter] => {
  const config = relation.fields?.[field];
  const nullable = config?.nullable ?? mapper.nullableDefault;

  if (nullable) {
    throw new UnsupportedQueryPlanError(
      "Whole-list comparison over a nullable element field: a null element is a missing attribute, which CEL raises on",
    );
  }

  const path = config?.field ?? field;

  return [
    { $map: { input: `$${relation.name}`, in: `$$this.${path}` } },
    { [relation.name]: { $type: "array" } },
  ];
};
