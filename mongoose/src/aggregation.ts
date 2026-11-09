import type { PlanExpression, PlanExpressionOperand } from "@cerbos/core";

import { UnsupportedQueryPlanError } from "./errors";
import type { Mapper, MongooseFilter } from "./index";
import { filterValue, mapValue } from "./logic";
import {
  declaredScalarType,
  isNullableReference,
  relationOfReference,
  resolveFieldReference,
  valueTypeOf,
} from "./mapper";
import { getOperandAt, isExpression, isValue, isVariable } from "./operands";
import { lowerRe2PatternForMongo } from "./regex";
import {
  MAX_CEL_TIMESTAMP,
  MIN_CEL_TIMESTAMP,
  RFC3339_TIMESTAMP_MONGO_PATTERN,
  isRfc3339Timestamp,
} from "./timestamp";

/**
 * Aggregation-expression lowering, for everything that has to be evaluated inside `$expr`:
 * arithmetic, conversions, ternaries, `size`, positional reads, and comparisons between two
 * fields.
 *
 * Each operator may also declare a GUARD: the filter that keeps out every document on which the
 * expression cannot be evaluated — where CEL raises and `check()` denies, while the pipeline
 * would carry on with a null or a wrongly typed value. The enclosing filter walks its operands
 * for guards and ANDs each one alongside itself (see `withEvaluationGuards`).
 */

/**
 * The aggregation-pipeline spelling of the comparisons, shared by `$expr` and by the query
 * operators a plain field/value comparison emits.
 */
export const COMPARISON_OPERATORS = {
  eq: "$eq",
  ne: "$ne",
  lt: "$lt",
  le: "$lte",
  gt: "$gt",
  ge: "$gte",
} as const;

export type ComparisonOperator = keyof typeof COMPARISON_OPERATORS;

const isComparison = (operator: string): operator is ComparisonOperator =>
  Object.hasOwn(COMPARISON_OPERATORS, operator);

/** The comparisons that are orderings: between two types an ordering is an error to CEL, `==` false. */
export const ORDERINGS = new Set(["lt", "le", "gt", "ge"]);

const ARITHMETIC: Record<string, string> = {
  add: "$add",
  sub: "$subtract",
  mult: "$multiply",
  div: "$divide",
};

/** Operators whose second operand is a lambda, which the three-valued evaluation evaluates. */
export const LAMBDA_OPERATORS = new Set(["exists", "exists_one", "all", "filter", "map", "lambda"]);

/** Guarded by "the expression is not null": each evaluates to null exactly where CEL raises. */
const NOT_NULL_GUARDED = new Set([
  "string", "double", "int", "size", "contains", "startsWith", "endsWith", "add", "sub", "mult",
  "div", "mod", "in", "except", "hierarchy", "ancestorOf", "descendentOf", "overlaps",
  "hasIntersection", "map", "intersect", "isSubset", "upperAscii", "timeSince",
]);

const isNumber = (value: unknown): value is number => typeof value === "number";
const isString = (value: unknown): value is string => typeof value === "string";
const isComposite = (value: unknown): boolean =>
  Array.isArray(value) || (typeof value === "object" && value !== null && !(value instanceof Date));
const isMap = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value) && !(value instanceof Date);

/**
 * A plan operand as an aggregation expression: a variable becomes a `$field.path`, a value a
 * constant (see `buildConstant`), an expression recurses.
 */
export const buildAggregationExpression = (
  operand: PlanExpressionOperand,
  mapper: Mapper,
): unknown => {
  if (isVariable(operand)) return fieldPath(operand.name, mapper);
  if (isValue(operand)) return buildConstant(operand.value);
  if (isExpression(operand)) return buildAggregationExpressionFromExpression(operand, mapper);
  throw new UnsupportedQueryPlanError("Invalid operand structure");
};

const build = buildAggregationExpression;

/**
 * A variable's value. A to-many relation's projected field is a `$map` over its elements, where
 * the dotted path would skip an element that lacks the field instead of reading it as null, and
 * so shorten the list.
 */
const fieldPath = (name: string, mapper: Mapper): unknown => {
  const { path, relation } = resolveFieldReference(name, mapper);
  if (relation?.type !== "many") return `$${path.join(".")}`;
  const field = path[1];
  if (relation.requiresParent !== undefined) return relationElements(relation, field);
  if (field === undefined) return `$${relation.name}`;
  return { $map: { input: `$${relation.name}`, as: "cerbos_element", in: `$$cerbos_element.${field}` } };
};

/**
 * The elements of a to-many relation reached through a to-one parent stored as an array
 * (`requiresParent`): the parent's list, or null (a CEL error) where no parent is stored.
 */
export const relationElements = (
  relation: { name: string; requiresParent?: string },
  field: string | undefined,
): unknown => {
  const parent = relation.requiresParent!;
  if (!relation.name.startsWith(`${parent}.`)) {
    throw new UnsupportedQueryPlanError("A relation's requiresParent must be a prefix of its path");
  }
  const child = relation.name.slice(parent.length + 1);
  const element = field ? `$$cerbos_element.${field}` : "$$cerbos_element";
  const elements = {
    $reduce: {
      input: `$${parent}`,
      initialValue: [],
      in: {
        $concatArrays: [
          "$$value",
          {
            $cond: [
              { $isArray: `$$this.${child}` },
              { $map: { input: `$$this.${child}`, as: "cerbos_element", in: element } },
              [],
            ],
          },
        ],
      },
    },
  };
  const parents = { $cond: [{ $isArray: `$${parent}` }, `$${parent}`, []] };
  // The parent is stored, and holds the list: a parent without it is a missing attribute.
  const stored = {
    $and: [
      { $gt: [{ $size: parents }, 0] },
      { $allElementsTrue: [{ $map: { input: parents, in: { $isArray: `$$this.${child}` } } }] },
    ],
  };
  return { $cond: [stored, elements, null] };
};

/**
 * A plan constant as an aggregation expression. Inside `$expr` a string that starts with `$` is a
 * field path (and `$$` a variable), and an array or a document is evaluated element by element,
 * so `R.attr.a + "q" == "$b"` would compare with the document's own `b` field and return
 * documents the PDP denies (cerbos/query-plan-adapters#575). `$literal` keeps each such constant
 * the value it is.
 */
export const buildConstant = (value: unknown): unknown => {
  if (isMap(value) || (Array.isArray(value) && value.some((element) => !isComparableElement(element)))) {
    throw new UnsupportedQueryPlanError(
      "A map constant inside an expression is unsupported: MongoDB compares embedded documents in stored field order, CEL's maps ignore it",
    );
  }
  if (Array.isArray(value) || (isString(value) && value.startsWith("$"))) return { $literal: value };
  return value;
};

/** An element a constant list may hold inside `$expr`: a scalar other than NaN, or a map of one such scalar. */
const isComparableElement = (element: unknown): boolean => {
  if (isMap(element)) {
    const values = Object.values(element);
    return values.length === 1 && !isMap(values[0]) && isComparableElement(values[0]);
  }
  return !Array.isArray(element) && !(isNumber(element) && Number.isNaN(element));
};

export const buildAggregationExpressionFromExpression = (
  expression: PlanExpression,
  mapper: Mapper,
): unknown => {
  const { operator, operands } = expression;
  if (operator === "and" || operator === "or") {
    return { [`$${operator}`]: operands.map((op) => build(op, mapper)) };
  }
  if (isComparison(operator)) {
    // A timestamp or duration against a value of another CEL type: an ordering is an error
    // (null), and `==` is decided without reading either side.
    if (temporalMismatch(operands[0], operands[1], mapper)) {
      return ORDERINGS.has(operator) ? null : operator === "ne";
    }
    return compare(operator, build(operands[0]!, mapper), build(operands[1]!, mapper));
  }
  switch (operator) {
    case "add":
      return buildAdd(operands, mapper);
    case "sub":
    case "mult":
    case "div":
      return buildArithmetic(operator, operands, mapper);
    case "mod":
      return operands.some(isIntTyped) ? buildIntArithmetic("mod", operands, mapper) : null;
    case "not":
      return { $not: [build(getOperandAt(operands, 0, "not operator requires an operand"), mapper)] };
    case "string":
      return buildString(operands, mapper);
    case "int":
      return buildInt(operands, mapper);
    case "in":
      return buildIn(operands, mapper);
    case "filter":
      return filterValue(expression, mapper);
    case "map":
      return mapValue(expression, mapper);
    case "except":
      return scalarLists(operands, mapper, () => ({
        $filter: {
          input: "$$cerbos_left",
          as: "cerbos_item",
          cond: {
            $or: [
              { $eq: ["$$cerbos_item", Number.NaN] },
              { $not: [{ $in: ["$$cerbos_item", "$$cerbos_right"] }] },
            ],
          },
        },
      }));
    // Cerbos's `intersect(a, b)` walks the shorter list (the first on a tie) and keeps, in order
    // and with its repeats, each element the other list contains.
    case "intersect":
      return scalarLists(operands, mapper, () => ({
        $let: {
          vars: { cerbos_swap: { $gt: [{ $size: "$$cerbos_left" }, { $size: "$$cerbos_right" }] } },
          in: {
            $filter: {
              input: { $cond: ["$$cerbos_swap", "$$cerbos_right", "$$cerbos_left"] },
              as: "cerbos_item",
              cond: {
                $and: [
                  { $ne: ["$$cerbos_item", Number.NaN] },
                  { $in: ["$$cerbos_item", { $cond: ["$$cerbos_swap", "$$cerbos_left", "$$cerbos_right"] }] },
                ],
              },
            },
          },
        },
      }));
    case "isSubset":
      return scalarLists(operands, mapper, () => ({
        $allElementsTrue: [
          {
            $map: {
              input: "$$cerbos_left",
              as: "cerbos_item",
              in: { $and: [{ $ne: ["$$cerbos_item", Number.NaN] }, { $in: ["$$cerbos_item", "$$cerbos_right"] }] },
            },
          },
        ],
      }));
    case "hasIntersection":
      return scalarLists(operands, mapper, () => ({
        $anyElementTrue: [
          {
            $map: {
              input: "$$cerbos_left",
              as: "cerbos_item",
              in: { $and: [{ $ne: ["$$cerbos_item", Number.NaN] }, { $in: ["$$cerbos_item", "$$cerbos_right"] }] },
            },
          },
        ],
      }));
    // CEL's upperAscii() folds a-z only, which is what $toUpper does to a string; anything but a
    // string is null here.
    case "upperAscii":
      return {
        $let: {
          vars: { cerbos_input: build(getOperandAt(operands, 0, "upperAscii requires an operand"), mapper) },
          in: { $cond: [{ $eq: [{ $type: "$$cerbos_input" }, "string"] }, { $toUpper: "$$cerbos_input" }, null] },
        },
      };
    case "duration": {
      const operand = getOperandAt(operands, 0, "duration requires an operand");
      if (!isValue(operand) || !isString(operand.value)) {
        throw new UnsupportedQueryPlanError("duration() is translated only over a constant string");
      }
      return parseDurationMilliseconds(operand.value);
    }
    // timeSince() is relative to the clock of whoever evaluates it, so it reads the server's $$NOW.
    case "timeSince": {
      const operand = getOperandAt(operands, 0, "timeSince requires an operand");
      if (celKind(operand, mapper) !== "timestamp") {
        throw new UnsupportedQueryPlanError("timeSince() is translated only over a timestamp");
      }
      return {
        $let: {
          vars: { cerbos_instant: build(operand, mapper) },
          in: { $cond: [{ $eq: [{ $type: "$$cerbos_instant" }, "date"] }, { $subtract: ["$$NOW", "$$cerbos_instant"] }, null] },
        },
      };
    }
    case "hierarchy":
      return buildHierarchy(operands, mapper);
    case "ancestorOf":
      return hierarchyPrefix(operands.map((op) => build(op, mapper)), "ancestor");
    case "descendentOf":
      return hierarchyPrefix(operands.map((op) => build(op, mapper)).reverse(), "ancestor");
    case "overlaps":
      return hierarchyPrefix(operands.map((op) => build(op, mapper)), "overlaps");
    case "list":
      return operands.map((op) => build(op, mapper));
    case "double":
      return buildDouble(operands, mapper);
    case "if": {
      const [condition, whenTrue, whenFalse] = operands;
      if (!condition || !whenTrue || !whenFalse) {
        throw new UnsupportedQueryPlanError("if operator requires three operands");
      }
      return { $cond: { if: build(condition, mapper), then: build(whenTrue, mapper), else: build(whenFalse, mapper) } };
    }
    case "index": {
      if (isMapLiteralIndex(operands)) return buildMapLookup(operands, mapper);
      const [collection, index] = constantIndex(operands);
      // A negative or fractional position is an error on every document; its guard keeps every
      // document out. $arrayElemAt keeps each element's BSON type, so `true` is never `1` here.
      if (index === undefined) return null;
      return { $arrayElemAt: [build(collection, mapper), index] };
    }
    case "get-field": {
      const [input, field] = operands;
      if (!input || !field || !isVariable(field)) {
        throw new UnsupportedQueryPlanError("get-field requires an input and a field name");
      }
      return { $getField: { field: field.name, input: build(input, mapper) } };
    }
    // $size for an array, $strLenCP for a string, and null — an error to CEL — otherwise. A
    // relation reached through a parent array is already the one parent's list, or null where
    // no parent is stored: an absent to-one parent is UNKNOWN, not 0 (#309).
    case "size":
      return {
        $let: {
          vars: { cerbos_sized: build(getOperandAt(operands, 0, "size operator requires an operand"), mapper) },
          in: {
            $cond: [
              { $isArray: "$$cerbos_sized" },
              { $size: "$$cerbos_sized" },
              { $cond: [{ $eq: [{ $type: "$$cerbos_sized" }, "string"] }, { $strLenCP: "$$cerbos_sized" }, null] },
            ],
          },
        },
      };
    case "matches": {
      const [input, pattern] = operands;
      if (!input || !pattern || !isValue(pattern) || !isString(pattern.value)) {
        throw new UnsupportedQueryPlanError("matches operator requires two operands");
      }
      const lowered = lowerRe2PatternForMongo(pattern.value);
      // An invalid RE2 pattern raises in CEL; its guard keeps every document out.
      if ("invalid" in lowered) return null;
      return { $regexMatch: { input: build(input, mapper), regex: buildConstant(lowered.pattern) } };
    }
    case "contains":
      return buildStringPredicate(expression, mapper, (index) => ({ $gte: [index, 0] }));
    case "startsWith":
      return buildStringPredicate(expression, mapper, (index) => ({ $eq: [index, 0] }));
    case "endsWith":
      return buildStringPredicate(expression, mapper, (_index, receiver, needle) => {
        const receiverLength = { $strLenCP: receiver };
        const needleLength = { $strLenCP: needle };
        return {
          $cond: {
            if: { $gte: [receiverLength, needleLength] },
            then: { $eq: [{ $substrCP: [receiver, { $subtract: [receiverLength, needleLength] }, needleLength] }, needle] },
            else: false,
          },
        };
      });
    case "timestamp": {
      const operand = getOperandAt(operands, 0, "timestamp operator requires an operand");
      if (isValue(operand)) {
        if (!isString(operand.value) || !isRfc3339Timestamp(operand.value)) {
          throw new UnsupportedQueryPlanError(
            "timestamp value must be a millisecond-exact RFC 3339 instant in the CEL range",
          );
        }
        return new Date(operand.value);
      }
      return buildTimestampConversion(build(operand, mapper));
    }
    default:
      throw new UnsupportedQueryPlanError(`Unsupported operator inside aggregation expression: ${operator}`);
  }
};

/** The evaluation guards for every guarded expression below `operand`, outermost first. */
export const buildEvaluationGuards = (
  operand: PlanExpressionOperand,
  mapper: Mapper,
): MongooseFilter[] => {
  if (!isExpression(operand)) return [];
  const guard = guardFor(operand, mapper);
  // A lambda's body reads its own element, and its value is already null where the body
  // raises; the collection is guarded where the value is.
  if (LAMBDA_OPERATORS.has(operand.operator)) return guard ? [guard] : [];
  const nested =
    operand.operator === "if" && operand.operands.length === 3
      ? branchGuards(operand, mapper)
      : operand.operands.flatMap((child) => buildEvaluationGuards(child, mapper));
  return guard ? [guard, ...nested] : nested;
};

const isExprGuard = (guard: MongooseFilter): boolean =>
  Object.keys(guard).length === 1 && "$expr" in guard;

/**
 * A ternary evaluates only the branch its condition selects, so a branch's guards apply only
 * where it is selected: `{$cond: [condition, then-guards, else-guards]}` inside `$expr`. A guard
 * that is not an `$expr` cannot sit in a `$cond`, so then every guard applies unconditionally,
 * which denies more than CEL does but never less.
 */
const branchGuards = (expression: PlanExpression, mapper: Mapper): MongooseFilter[] => {
  const [condition, whenTrue, whenFalse] = expression.operands as [
    PlanExpressionOperand,
    PlanExpressionOperand,
    PlanExpressionOperand,
  ];
  const conditionGuards = buildEvaluationGuards(condition, mapper);
  const thenGuards = buildEvaluationGuards(whenTrue, mapper);
  const elseGuards = buildEvaluationGuards(whenFalse, mapper);
  if (thenGuards.length === 0 && elseGuards.length === 0) return conditionGuards;
  if (![...thenGuards, ...elseGuards].every(isExprGuard)) {
    return [...conditionGuards, ...thenGuards, ...elseGuards];
  }
  const selected = [thenGuards, elseGuards].map((guards) =>
    guards.length === 0 ? true : { $and: guards.map((guard) => guard["$expr"]) },
  );
  return [...conditionGuards, { $expr: { $cond: [build(condition, mapper), ...selected] } }];
};

export const guardFor = (
  expression: PlanExpression,
  mapper: Mapper,
): MongooseFilter | undefined => {
  const { operator, operands } = expression;
  if (isComparison(operator) && temporalMismatch(operands[0], operands[1], mapper)) {
    // No overload: an ordering raises on every document. `==` is false and needs no guard.
    return ORDERINGS.has(operator) ? { $expr: false } : undefined;
  }
  if (NOT_NULL_GUARDED.has(operator)) return notNullGuard(expression, mapper);
  switch (operator) {
    case "lt":
    case "le":
    case "gt":
    case "ge":
      return { $expr: orderable(build(operands[0]!, mapper), build(operands[1]!, mapper)) };
    case "eq":
    case "ne": {
      const [left, right] = operands;
      if (!left || !right || [left, right].some((op) => isValue(op) && !isComposite(op.value))) {
        return undefined;
      }
      return { $expr: equalityDecidable(build(left, mapper), build(right, mapper)) };
    }
    // MongoDB reads any value as truthy or falsy, where CEL's logic has no overload for anything
    // but a boolean.
    case "and":
    case "or":
    case "not":
      return { $expr: { $and: operands.map((op) => ({ $eq: [{ $type: build(op, mapper) }, "bool"] })) } };
    // A ternary over anything but a boolean condition raises in CEL; $cond reads any value as
    // truthy or falsy instead.
    case "if":
      return {
        $expr: { $eq: [{ $type: build(getOperandAt(operands, 0, "if requires a condition"), mapper) }, "bool"] },
      };
    // A literal is validated at translation time; only a field can fail per document.
    case "timestamp":
      return operands[0] && !isValue(operands[0]) ? notNullGuard(expression, mapper) : undefined;
    case "index": {
      // A key the map literal does not hold is an error to CEL, and null here.
      if (isMapLiteralIndex(operands)) return notNullGuard(expression, mapper);
      const [collectionOperand, index] = constantIndex(operands);
      // CEL raises for a negative or fractional list position whatever the document holds.
      if (index === undefined) return { $expr: false };
      // An out-of-range index is an error to CEL, not a missing value.
      const collection = build(collectionOperand, mapper);
      return {
        $expr: { $cond: { if: { $isArray: [collection] }, then: { $gt: [{ $size: [collection] }, index] }, else: false } },
      };
    }
    // A field the element lacks is a missing-key error to CEL, and so is a stored null on a field
    // the relation declares nullable; $getField yields missing or null, which a negated
    // comparison would read as unequal.
    case "get-field": {
      const [input, field] = operands;
      if (!input || !field || !isVariable(field)) return undefined;
      const value = buildAggregationExpressionFromExpression(expression, mapper);
      const collection = isExpression(input) && input.operator === "index" ? input.operands[0] : undefined;
      const nullable =
        collection !== undefined && isVariable(collection) && isNullableReference(`${collection.name}.${field.name}`, mapper);
      return {
        $expr: nullable ? { $ne: [{ $ifNull: [value, null] }, null] } : { $ne: [{ $type: value }, "missing"] },
      };
    }
    case "matches": {
      const input = getOperandAt(operands, 0, "matches operator requires an input operand");
      const pattern = operands[1];
      // An invalid RE2 pattern raises whatever the document holds.
      if (pattern && isValue(pattern) && isString(pattern.value) && "invalid" in lowerRe2PatternForMongo(pattern.value)) {
        return { $expr: false };
      }
      return { $expr: { $eq: [{ $type: build(input, mapper) }, "string"] } };
    }
    default:
      return undefined;
  }
};

const notNullGuard = (expression: PlanExpression, mapper: Mapper): MongooseFilter => ({
  $expr: { $ne: [buildAggregationExpressionFromExpression(expression, mapper), null] },
});

/** Binds each operand to `$$operand<i>` so an expression can read it more than once. */
const withOperands = (
  operands: PlanExpressionOperand[],
  mapper: Mapper,
  body: (values: string[]) => unknown,
): unknown => {
  const names = operands.map((_, index) => `operand${index}`);
  return {
    $let: {
      vars: Object.fromEntries(names.map((name, index) => [name, build(operands[index]!, mapper)])),
      in: body(names.map((name) => `$$${name}`)),
    },
  };
};

/**
 * CEL overloads `+` on strings, lists, timestamps and durations, and MongoDB's `$add` takes
 * numbers and dates only, aborting the whole query otherwise. A constant settles the overload,
 * since CEL has no mixed-type `+`; with none, the operands' runtime types do, per document: two
 * strings concatenate, two numbers add as doubles, two lists concatenate, and anything else has
 * no overload (null).
 */
const buildAdd = (operands: PlanExpressionOperand[], mapper: Mapper): unknown => {
  if (operands.some((op) => isValue(op) && isString(op.value))) {
    // $concat raises on anything but a string, where CEL has no overload: null instead.
    return withOperands(operands, mapper, (values) => ({
      $cond: [{ $and: values.map((value) => ({ $eq: [{ $type: value }, "string"] })) }, { $concat: values }, null],
    }));
  }
  if (operands.some((op) => isTemporal(op, mapper))) return buildTemporalArithmetic("add", operands, mapper);
  if (operands.some(isListTyped)) {
    return withOperands(operands, mapper, (values) => ({
      $cond: [{ $and: values.map((value) => ({ $isArray: value })) }, { $concatArrays: values }, null],
    }));
  }
  if (operands.some((op) => isIntTyped(op) || isValue(op))) return buildArithmetic("add", operands, mapper);
  return withOperands(operands, mapper, (values) => {
    const all = (check: (value: string) => unknown) => ({ $and: values.map(check) });
    return {
      $switch: {
        branches: [
          { case: all((value) => ({ $eq: [{ $type: value }, "string"] })), then: { $concat: values } },
          { case: all((value) => ({ $isNumber: value })), then: { $add: values.map((value) => ({ $toDouble: value })) } },
          { case: all((value) => ({ $isArray: value })), then: { $concatArrays: values } },
        ],
        default: null,
      },
    };
  });
};

/**
 * `needle in list`: null (a CEL error) where the list is not an array. `$in` compares as `$eq`
 * does, which agrees with CEL's equality for scalars but NaN, which CEL never finds; a map or
 * list needle is null too, since MongoDB compares embedded documents in stored field order.
 */
const buildIn = (operands: PlanExpressionOperand[], mapper: Mapper): unknown => {
  const [needle, list] = operands;
  if (!needle || !list) throw new UnsupportedQueryPlanError("in requires two operands");
  if (isVariable(list) && relationOfReference(list.name, mapper)?.type === "one") {
    throw new UnsupportedQueryPlanError(
      "`in` over a to-one relation tests the related object's keys in CEL, and the adapter has no expression for a subdocument's field names",
    );
  }
  return {
    $let: {
      vars: { cerbos_needle: build(needle, mapper), cerbos_list: build(list, mapper) },
      in: {
        $switch: {
          branches: [
            { case: { $not: [{ $isArray: "$$cerbos_list" }] }, then: null },
            { case: { $in: [{ $type: "$$cerbos_needle" }, ["object", "array"]] }, then: null },
            { case: { $eq: ["$$cerbos_needle", Number.NaN] }, then: false },
          ],
          default: { $in: ["$$cerbos_needle", "$$cerbos_list"] },
        },
      },
    },
  };
};

/**
 * Binds the two operands to `$$cerbos_left` and `$$cerbos_right`, and evaluates `body` where both
 * are lists of scalars; null (a CEL error) where either is not a list. A list or map element
 * makes the whole value null too, since MongoDB compares embedded documents in stored field order
 * where CEL's maps ignore it (which denies where CEL might allow).
 */
const scalarLists = (
  operands: PlanExpressionOperand[],
  mapper: Mapper,
  body: () => unknown,
): unknown => {
  const [left, right] = operands;
  if (!left || !right) throw new UnsupportedQueryPlanError("A list function requires two operands");
  const scalars = (list: string) => ({
    $allElementsTrue: [{ $map: { input: list, in: { $not: [{ $in: [{ $type: "$$this" }, ["object", "array"]] }] } } }],
  });
  return {
    $let: {
      vars: { cerbos_left: build(left, mapper), cerbos_right: build(right, mapper) },
      in: {
        $cond: [
          { $and: [{ $isArray: "$$cerbos_left" }, { $isArray: "$$cerbos_right" }] },
          { $cond: [{ $and: [scalars("$$cerbos_left"), scalars("$$cerbos_right")] }, body(), null] },
          null,
        ],
      },
    },
  };
};

type CelKind = "string" | "number" | "bool" | "timestamp" | "duration";

const TEMPORAL_RESULTS: Record<string, CelKind> = {
  "add,timestamp,duration": "timestamp",
  "add,duration,timestamp": "timestamp",
  "sub,timestamp,duration": "timestamp",
  "sub,timestamp,timestamp": "duration",
  "add,duration,duration": "duration",
  "sub,duration,duration": "duration",
};

/** The CEL type of a temporal `+` or `-`, or undefined where CEL has no such overload. */
const temporalResult = (operator: string, kinds: (CelKind | undefined)[]): CelKind | undefined =>
  TEMPORAL_RESULTS[[operator, ...kinds].join(",")];

/**
 * The CEL type `operand` evaluates to, as far as the plan and the mapper settle it: a constant's
 * own, a field's declared `valueType` (a `dateTime` field is the string the application sends),
 * and the result type of an operator that has one.
 */
const celKind = (operand: PlanExpressionOperand, mapper: Mapper): CelKind | undefined => {
  if (isValue(operand)) {
    const { value } = operand;
    if (isString(value)) return "string";
    if (typeof value === "boolean") return "bool";
    return isNumber(value) ? "number" : undefined;
  }
  if (isVariable(operand)) {
    const type = valueTypeOf(operand.name, mapper);
    return type === "string" || type === "dateTime"
      ? "string"
      : type === "number"
        ? "number"
        : type === "boolean"
          ? "bool"
          : undefined;
  }
  if (!isExpression(operand)) return undefined;
  switch (operand.operator) {
    case "timestamp":
      return "timestamp";
    case "duration":
    case "timeSince":
      return "duration";
    case "add":
    case "sub":
      return temporalResult(operand.operator, operand.operands.map((op) => celKind(op, mapper)));
    case "upperAscii":
    case "string":
      return "string";
    case "int":
    case "double":
    case "size":
      return "number";
    default:
      return undefined;
  }
};

const isTemporal = (operand: PlanExpressionOperand, mapper: Mapper): boolean => {
  const kind = celKind(operand, mapper);
  return kind === "timestamp" || kind === "duration";
};

/**
 * Whether a comparison sets a timestamp or a duration against a value of another known CEL type.
 * An ordering between them has no overload, which raises; `==` between them is false. MongoDB
 * would order a date against a string by BSON type, and a duration against any number.
 */
export const temporalMismatch = (
  left: PlanExpressionOperand | undefined,
  right: PlanExpressionOperand | undefined,
  mapper: Mapper,
): boolean => {
  if (!left || !right) return false;
  const kinds = [celKind(left, mapper), celKind(right, mapper)];
  if (kinds.includes(undefined) || kinds[0] === kinds[1]) return false;
  return kinds.some((kind) => kind === "timestamp" || kind === "duration");
};

/**
 * timestamp ± duration, timestamp - timestamp and duration ± duration, with a timestamp as a BSON
 * date and a duration as its milliseconds. A timestamp computed outside CEL's range is an error.
 */
const buildTemporalArithmetic = (
  operator: string,
  operands: PlanExpressionOperand[],
  mapper: Mapper,
): unknown => {
  const kinds = operands.map((op) => celKind(op, mapper));
  const result = temporalResult(operator, kinds);
  if (result === undefined) {
    throw new UnsupportedQueryPlanError(
      `${operator} over ${kinds.map((kind) => kind ?? "an untyped operand").join(" and ")}: CEL's ` +
        "only temporal overloads are timestamp ± duration, timestamp - timestamp and duration ± duration",
    );
  }
  return withOperands(operands, mapper, (values) => {
    const typed = values.map((value, index) =>
      kinds[index] === "timestamp" ? { $eq: [{ $type: value }, "date"] } : { $isNumber: value },
    );
    let computed: unknown = { [ARITHMETIC[operator]!]: values };
    if (result === "timestamp") {
      computed = {
        $let: {
          vars: { cerbos_instant: computed },
          in: {
            $cond: [
              { $and: [{ $gte: ["$$cerbos_instant", MIN_CEL_TIMESTAMP] }, { $lte: ["$$cerbos_instant", MAX_CEL_TIMESTAMP] }] },
              "$$cerbos_instant",
              null,
            ],
          },
        },
      };
    }
    return { $cond: [{ $and: typed }, computed, null] };
  });
};

/**
 * Whether `operand` is a list the plan settles as one: a list constant or constructor, a ternary
 * whose branches are both lists, a `+` with a list operand, or a list function.
 */
export const isListTyped = (operand: PlanExpressionOperand): boolean => {
  if (isValue(operand)) return Array.isArray(operand.value);
  if (!isExpression(operand)) return false;
  switch (operand.operator) {
    case "list":
    case "intersect":
    case "except":
    case "filter":
    case "map":
      return true;
    case "add":
      return operand.operands.some(isListTyped);
    case "if":
      return operand.operands.length === 3 && operand.operands.slice(1).every(isListTyped);
    default:
      return false;
  }
};

const isMapLiteralIndex = (operands: PlanExpressionOperand[]): boolean => {
  const [collection, key] = operands;
  return collection !== undefined && isValue(collection) && isMap(collection.value) && key !== undefined;
};

/**
 * A map literal indexed by a key the document supplies (`{"a": "X"}[R.attr.k]`): the value under
 * that key, or null, CEL's no-such-key error, where the map holds none. Only a map of scalars
 * other than null and NaN is read this way: a null value could not be told from the error.
 */
const buildMapLookup = (operands: PlanExpressionOperand[], mapper: Mapper): unknown => {
  const [map, key] = operands as [PlanExpressionOperand & { value: Record<string, unknown> }, PlanExpressionOperand];
  const entries = Object.entries(map.value);
  if (!entries.every(([, value]) => value !== null && !isMap(value) && isComparableElement(value))) {
    throw new UnsupportedQueryPlanError(
      "A map literal is indexed only when every value is a scalar other than null and NaN: a null value reads as the missing key's error",
    );
  }
  if (entries.length === 0) return null;
  return {
    $let: {
      vars: { cerbos_key: build(key, mapper) },
      in: {
        $switch: {
          branches: entries.map(([name, value]) => ({
            case: { $eq: ["$$cerbos_key", buildConstant(name)] },
            then: buildConstant(value),
          })),
          default: null,
        },
      },
    },
  };
};

/**
 * Cerbos's hierarchy(): the segments of a string split on the separator ("." when none is given;
 * an empty one splits into code points, as Go's strings.Split does), or a list of strings as it
 * is. Anything else is null (a CEL error).
 */
const buildHierarchy = (operands: PlanExpressionOperand[], mapper: Mapper): unknown => {
  const [path, separator] = operands;
  if (!path) throw new UnsupportedQueryPlanError("hierarchy requires a path");
  if (separator && !(isValue(separator) && isString(separator.value))) {
    throw new UnsupportedQueryPlanError("hierarchy separator must be a string constant");
  }
  const delimiter = separator && isValue(separator) ? (separator.value as string) : ".";
  const split =
    delimiter === ""
      ? { $map: { input: { $range: [0, { $strLenCP: "$$cerbos_path" }] }, in: { $substrCP: ["$$cerbos_path", "$$this", 1] } } }
      : { $split: ["$$cerbos_path", buildConstant(delimiter)] };
  const branches: unknown[] = [{ case: { $eq: [{ $type: "$$cerbos_path" }, "string"] }, then: split }];
  if (!separator) {
    const everyString = {
      $allElementsTrue: [{ $map: { input: "$$cerbos_path", in: { $eq: [{ $type: "$$this" }, "string"] } } }],
    };
    branches.push({ case: { $and: [{ $isArray: "$$cerbos_path" }, everyString] }, then: "$$cerbos_path" });
  }
  return { $let: { vars: { cerbos_path: build(path, mapper) }, in: { $switch: { branches, default: null } } } };
};

/**
 * ancestorOf (the second strictly longer, starting with the first) and overlaps (the shorter
 * starting the longer) between two hierarchies; null where either is not one.
 */
const hierarchyPrefix = (hierarchies: unknown[], relation: "ancestor" | "overlaps"): unknown => {
  const [first, second] = hierarchies;
  if (first === undefined || second === undefined) {
    throw new UnsupportedQueryPlanError("a hierarchy comparison requires two operands");
  }
  const starts = (short: string, long: string) => ({
    $cond: [{ $eq: [{ $size: short }, 0] }, true, { $eq: [{ $slice: [long, { $size: short }] }, short] }],
  });
  const compared =
    relation === "ancestor"
      ? {
          $and: [
            { $gt: [{ $size: "$$cerbos_second" }, { $size: "$$cerbos_first" }] },
            starts("$$cerbos_first", "$$cerbos_second"),
          ],
        }
      : {
          $cond: [
            { $lte: [{ $size: "$$cerbos_first" }, { $size: "$$cerbos_second" }] },
            starts("$$cerbos_first", "$$cerbos_second"),
            starts("$$cerbos_second", "$$cerbos_first"),
          ],
        };
  return {
    $let: {
      vars: { cerbos_first: first, cerbos_second: second },
      in: { $cond: [{ $and: [{ $isArray: "$$cerbos_first" }, { $isArray: "$$cerbos_second" }] }, compared, null] },
    },
  };
};

/**
 * A CEL comparison inside `$expr`. MongoDB orders NaN below every number and equal to itself,
 * where every CEL comparison with NaN is false but `!=`, which is true. Two lists are equal
 * element by element, so NaN inside them is compared the same way (see `equalityDecidable` for
 * the lists and maps that are not compared at all).
 */
export const compare = (operator: ComparisonOperator, left: unknown, right: unknown): unknown => {
  let scalar: unknown = { [COMPARISON_OPERATORS[operator]]: ["$$left", "$$right"] };
  if (operator === "eq" || operator === "ne") {
    // Every array operator reads an operand that is an array on any document: MongoDB 9 folds a
    // constant operand before $cond would skip the branch.
    const lists = (variable: string) => ({ $cond: [{ $isArray: variable }, variable, []] });
    const equal = {
      $cond: [
        { $and: [{ $isArray: "$$left" }, { $isArray: "$$right" }] },
        {
          $and: [
            { $eq: [{ $size: lists("$$left") }, { $size: lists("$$right") }] },
            {
              $allElementsTrue: [
                {
                  $map: {
                    input: { $zip: { inputs: [lists("$$left"), lists("$$right")] } },
                    as: "cerbos_pair",
                    in: {
                      $and: [
                        { $ne: [{ $arrayElemAt: ["$$cerbos_pair", 0] }, Number.NaN] },
                        { $eq: [{ $arrayElemAt: ["$$cerbos_pair", 0] }, { $arrayElemAt: ["$$cerbos_pair", 1] }] },
                      ],
                    },
                  },
                },
              ],
            },
          ],
        },
        { $eq: ["$$left", "$$right"] },
      ],
    };
    scalar = operator === "eq" ? equal : { $not: [equal] };
  }
  return {
    $let: {
      vars: { left, right },
      in: {
        $cond: [{ $or: [{ $eq: ["$$left", Number.NaN] }, { $eq: ["$$right", Number.NaN] }] }, operator === "ne", scalar],
      },
    },
  };
};

/**
 * Whether MongoDB's equality can decide CEL's between `left` and `right`: not between two maps
 * (MongoDB compares embedded documents in stored field order, CEL ignores it), nor between two
 * lists where either holds a list or a map. Those are kept out under either polarity.
 */
const equalityDecidable = (left: unknown, right: unknown): unknown => {
  const composite = (list: string) => ({
    $anyElementTrue: [
      { $map: { input: { $cond: [{ $isArray: list }, list, []] }, in: { $in: [{ $type: "$$this" }, ["object", "array"]] } } },
    ],
  });
  return {
    $let: {
      vars: { left, right },
      in: {
        $not: [
          {
            $or: [
              { $and: [{ $eq: [{ $type: "$$left" }, "object"] }, { $eq: [{ $type: "$$right" }, "object"] }] },
              {
                $and: [
                  { $isArray: "$$left" },
                  { $isArray: "$$right" },
                  { $or: [composite("$$left"), composite("$$right")] },
                ],
              },
            ],
          },
        ],
      },
    },
  };
};

/**
 * Whether CEL can order `left` against `right` at all: two numbers, two strings, two booleans or
 * two timestamps. Anything else is a no-such-overload error, which denies under either polarity,
 * where MongoDB orders every pair of BSON types. cel-go answers an ordering whose left side is
 * NaN false whatever the right side is, and one whose right side is NaN false when the left side
 * is a number, a string, a boolean or a timestamp; it raises for null, a list or a map on the left.
 */
export const orderable = (left: unknown, right: unknown): unknown => ({
  $let: {
    vars: { left, right },
    in: {
      $or: [
        { $eq: ["$$left", Number.NaN] },
        {
          $and: [
            { $eq: ["$$right", Number.NaN] },
            { $in: [{ $type: "$$left" }, ["double", "int", "long", "decimal", "string", "bool", "date"]] },
          ],
        },
        { $and: [{ $isNumber: "$$left" }, { $isNumber: "$$right" }] },
        {
          $and: [
            { $eq: [{ $type: "$$left" }, { $type: "$$right" }] },
            { $in: [{ $type: "$$left" }, ["string", "bool", "date"]] },
          ],
        },
      ],
    },
  },
});

/**
 * CEL arithmetic over doubles: every attribute number reaches CEL as a double. MongoDB keeps an
 * int an int, so int 0 times -1 is 0 where CEL's double is -0.0, and 1 over that is +Infinity
 * where CEL's is -Infinity; so every operand is converted with `$toDouble` first. A non-number
 * operand is an error to CEL and null here (MongoDB would abort the query instead).
 *
 * Division is IEEE 754's, which `$divide` is except by zero, where it aborts the query: x / 0 is
 * NaN when x is NaN or zero, and otherwise an infinity whose sign is x's, flipped by a negative
 * zero divisor (`$toString` spells -0.0 "-0").
 *
 * Arithmetic over a CEL int is int arithmetic instead (`buildIntArithmetic`).
 */
const buildArithmetic = (operator: string, operands: PlanExpressionOperand[], mapper: Mapper): unknown => {
  if (operands.some((op) => isTemporal(op, mapper))) return buildTemporalArithmetic(operator, operands, mapper);
  if (operands.some(isIntTyped)) return buildIntArithmetic(operator, operands, mapper);
  return withOperands(operands, mapper, (values) => {
    const doubles = values.map((value) => ({ $toDouble: value }));
    let result: unknown;
    if (operator === "div") {
      const [numerator, denominator] = doubles;
      const divisor = operands[1]!;
      if (isValue(divisor) && isNumber(divisor.value)) {
        result = divisor.value === 0 ? divideByZero(numerator, denominator, divisor.value) : { $divide: doubles };
      } else {
        result = { $cond: [{ $eq: [denominator, 0] }, divideByZero(numerator, denominator), { $divide: doubles }] };
      }
    } else {
      result = { [ARITHMETIC[operator]!]: doubles };
    }
    return { $cond: [{ $and: values.map((value) => ({ $isNumber: value })) }, result, null] };
  });
};

/** A constant denominator's sign is settled here rather than read back from the server. */
const divideByZero = (numerator: unknown, denominator: unknown, constant?: number): unknown => {
  const negativeZero = constant === undefined ? { $eq: [{ $toString: denominator }, "-0"] } : Object.is(constant, -0);
  return {
    $cond: [
      { $or: [{ $eq: [numerator, Number.NaN] }, { $eq: [numerator, 0] }] },
      Number.NaN,
      { $cond: [{ $eq: [{ $lt: [numerator, 0] }, negativeZero] }, Infinity, -Infinity] },
    ],
  };
};

const buildString = (operands: PlanExpressionOperand[], mapper: Mapper): unknown => {
  const operand = getOperandAt(operands, 0, "string conversion requires an operand");
  if (rendersUntypedIntegralConstant(operand)) {
    throw new UnsupportedQueryPlanError(
      "string() over an integral constant whose int or double type the plan does not carry: " +
        'CEL renders the int 1000000 as "1000000" and the double as "1e+06", and the plan ' +
        "ships both as the same bare number",
    );
  }
  const input = build(operand, mapper);
  // A CEL int renders in plain decimal, which $toString of a long is.
  if (isIntTyped(operand)) return { $cond: [{ $isNumber: input }, { $toString: input }, null] };
  return {
    $switch: {
      branches: [
        { case: { $in: [{ $type: input }, ["int", "long", "double", "decimal"]] }, then: celDoubleToString({ $toDouble: input }) },
        { case: { $in: [{ $type: input }, ["string", "bool"]] }, then: { $toString: input } },
      ],
      default: null,
    },
  };
};

/**
 * Whether string() over the operand could render a numeric constant, bare or as a ternary
 * branch, whose int or double type the plan dropped: the plan ships 1000000 and 1000000.0 alike.
 */
const rendersUntypedIntegralConstant = (operand: PlanExpressionOperand): boolean => {
  if (isValue(operand)) {
    return isNumber(operand.value) && Number.isInteger(operand.value) && Math.abs(operand.value) >= 1e6;
  }
  if (!isExpression(operand) || operand.operator !== "if") return false;
  // A branch CEL types int fixes the ternary's type, and so every literal branch's.
  if (isIntTyped(operand)) return false;
  return operand.operands.slice(1).some(rendersUntypedIntegralConstant);
};

/**
 * int() as cel-go converts: a number truncates toward zero, and raises outside (-2^63, 2^63),
 * NaN and the infinities included; a string must be a whole base-10 int64, an optional sign then
 * ASCII digits. Anything else raises, and is null here. $convert on its own would parse a
 * numeric prefix and round.
 */
const buildInt = (operands: PlanExpressionOperand[], mapper: Mapper): unknown => {
  const input = build(getOperandAt(operands, 0, "int conversion requires an operand"), mapper);
  return {
    $let: {
      vars: { input },
      in: {
        $switch: {
          branches: [
            {
              case: { $isNumber: "$$input" },
              then: {
                $let: {
                  vars: { double: { $toDouble: "$$input" } },
                  in: {
                    $cond: [
                      { $and: [{ $gt: ["$$double", MIN_CEL_INT_DOUBLE] }, { $lt: ["$$double", MAX_CEL_INT_DOUBLE] }] },
                      { $toLong: { $trunc: ["$$double"] } },
                      null,
                    ],
                  },
                },
              },
            },
            {
              case: { $eq: [{ $type: "$$input" }, "string"] },
              then: {
                $cond: [
                  { $regexMatch: { input: "$$input", regex: CEL_INT_STRING_PATTERN } },
                  { $convert: { input: { $ltrim: { input: "$$input", chars: "+" } }, to: "long", onError: null, onNull: null } },
                  null,
                ],
              },
            },
          ],
          default: null,
        },
      },
    },
  };
};

/**
 * double() as cel-go converts: a number as itself, and a string that is a decimal floating-point
 * literal. Go's ParseFloat also reads "Inf", "NaN" and hexadecimal forms, which this leaves to
 * null, and denied, where CEL might allow.
 */
const buildDouble = (operands: PlanExpressionOperand[], mapper: Mapper): unknown => {
  const input = build(getOperandAt(operands, 0, "double conversion requires an operand"), mapper);
  return {
    $let: {
      vars: { input },
      in: {
        $switch: {
          branches: [
            { case: { $isNumber: "$$input" }, then: { $toDouble: "$$input" } },
            {
              case: { $eq: [{ $type: "$$input" }, "string"] },
              then: {
                $cond: [
                  { $regexMatch: { input: "$$input", regex: CEL_DOUBLE_STRING_PATTERN } },
                  {
                    $convert: {
                      input: { $replaceAll: { input: "$$input", find: "_", replacement: "" } },
                      to: "double",
                      onError: null,
                      onNull: null,
                    },
                  },
                  null,
                ],
              },
            },
          ],
          default: null,
        },
      },
    },
  };
};

/**
 * Whether CEL types `operand` int: int(), size(), `%`, and arithmetic or a ternary over one. CEL's
 * checker then types every literal beside it int too.
 */
export const isIntTyped = (operand: PlanExpressionOperand): boolean => {
  if (!isExpression(operand)) return false;
  switch (operand.operator) {
    case "int":
    case "size":
    case "mod":
      return true;
    case "add":
    case "sub":
    case "mult":
    case "div":
      return operand.operands.some(isIntTyped);
    case "if":
      return operand.operands.slice(1).some(isIntTyped);
    default:
      return false;
  }
};

/**
 * Whether `operand` may stand beside a CEL int: an int-typed expression, a numeric literal, or a
 * ternary whose branches all are, which CEL's checker types int beside an int.
 */
const isIntOperand = (operand: PlanExpressionOperand): boolean =>
  isIntTyped(operand) ||
  (isValue(operand) && isNumber(operand.value)) ||
  (isExpression(operand) && operand.operator === "if" && operand.operands.slice(1).every(isIntOperand));

/**
 * Whether `operand` evaluates to a CEL int (or is a numeric literal beside one), as an aggregation
 * boolean: int(), size() and `%` are ints, arithmetic is an int when every operand is, a ternary
 * when the branch it selects is, and an attribute never is.
 */
const intSelected = (operand: PlanExpressionOperand, mapper: Mapper): unknown => {
  if (isValue(operand)) return isNumber(operand.value);
  if (!isExpression(operand)) return false;
  switch (operand.operator) {
    case "int":
    case "size":
    case "mod":
      return true;
    case "add":
    case "sub":
    case "mult":
    case "div":
    {
      const parts = operand.operands.map((op) => intSelected(op, mapper));
      if (parts.includes(false)) return false;
      const open = parts.filter((part) => part !== true);
      return open.length === 0 ? true : { $and: open };
    }
    case "if": {
      const [condition, whenTrue, whenFalse] = operand.operands;
      if (!condition || !whenTrue || !whenFalse) return false;
      const branches = [intSelected(whenTrue, mapper), intSelected(whenFalse, mapper)];
      if (branches[0] === branches[1] && typeof branches[0] === "boolean") return branches[0];
      return { $cond: [build(condition, mapper), ...branches] };
    }
    default:
      return false;
  }
};

const INT64_MIN = -9223372036854775808;
const SAFE_INTEGER = 9007199254740992;

/**
 * CEL int arithmetic over int64. A literal beside an int is an int; an attribute beside one is a
 * double, which has no overload with an int, so the whole expression raises and is null here.
 * Overflow raises in CEL, where MongoDB widens a long to a double; division and `%` by zero
 * raise; division truncates toward zero, spelled exactly as (n - n % d) / d while both are within
 * 2^53 and null (denied) beyond.
 */
const buildIntArithmetic = (operator: string, operands: PlanExpressionOperand[], mapper: Mapper): unknown => {
  if (!operands.every(isIntOperand)) return null;
  for (const op of operands) {
    if (isValue(op) && !Number.isInteger(op.value)) {
      throw new UnsupportedQueryPlanError("A fractional literal beside a CEL int does not type-check");
    }
  }
  return withOperands(operands, mapper, (values) => {
    const longs = values.map((value) => ({ $toLong: value }));
    let result: unknown;
    if (operator === "div" || operator === "mod") {
      const [numerator, denominator] = longs;
      const invalid =
        operator === "div"
          ? {
              $or: [
                { $eq: [denominator, 0] },
                { $gt: [{ $abs: numerator }, SAFE_INTEGER] },
                { $gt: [{ $abs: denominator }, SAFE_INTEGER] },
              ],
            }
          : { $or: [{ $eq: [denominator, 0] }, { $and: [{ $eq: [numerator, INT64_MIN] }, { $eq: [denominator, -1] }] }] };
      const quotient =
        operator === "div"
          ? { $toLong: { $divide: [{ $subtract: [numerator, { $mod: [numerator, denominator] }] }, denominator] } }
          : { $mod: [numerator, denominator] };
      result = { $cond: [invalid, null, quotient] };
    } else {
      result = {
        $let: {
          vars: { long: { [ARITHMETIC[operator]!]: longs } },
          in: { $cond: [{ $eq: [{ $type: "$$long" }, "long"] }, "$$long", null] },
        },
      };
    }
    // A ternary typed int by one branch can select an attribute in the other, which reaches CEL
    // as a double and has no overload with an int. Which branch is an int is settled by the plan,
    // not by the stored BSON type: a driver stores an integral double as an int.
    const typed = values.map((value, index) => ({
      $and: [{ $isNumber: value }, intSelected(operands[index]!, mapper)],
    }));
    return { $cond: [{ $and: typed }, result, null] };
  });
};

/** contains/startsWith/endsWith over two strings; null (an error to CEL) otherwise. */
const buildStringPredicate = (
  { operator, operands }: PlanExpression,
  mapper: Mapper,
  predicate: (index: unknown, receiver: unknown, needle: unknown) => unknown,
): unknown => {
  const [receiverOperand, needleOperand] = operands;
  if (!receiverOperand || !needleOperand) throw new UnsupportedQueryPlanError(`${operator} requires two operands`);
  const receiver = build(receiverOperand, mapper);
  const needle = build(needleOperand, mapper);
  return {
    $cond: [
      { $and: [{ $eq: [{ $type: receiver }, "string"] }, { $eq: [{ $type: needle }, "string"] }] },
      predicate({ $indexOfCP: [receiver, needle] }, receiver, needle),
      null,
    ],
  };
};

/**
 * The position of an index expression: a non-negative integer, or undefined for a numeric
 * constant CEL can never index with (negative, or fractional), which raises on every document.
 */
const constantIndex = (
  operands: PlanExpressionOperand[],
): readonly [PlanExpressionOperand, number | undefined] => {
  const [collection, index] = operands;
  if (!collection || !index) throw new UnsupportedQueryPlanError("index operator requires two operands");
  if (!isValue(index) || !isNumber(index.value)) {
    throw new UnsupportedQueryPlanError("index operator requires a numeric constant position");
  }
  const position = index.value;
  if (!Number.isInteger(position) || position < 0) return [collection, undefined];
  return [collection, position];
};


/** `number` is a double, or a numeric constant whose int or double type the plan does not carry;
 * `int` is the result of `int()`. */
type CelScalarType = "number" | "int" | "string" | "boolean" | "timestamp" | "duration";

/**
 * The CEL type `operand` evaluates to, as far as the plan settles it: a constant's own, a field's
 * declared `valueType`, and the result type of an operator that has one. `mixed` is a ternary
 * whose branches have different types; undefined is a type the plan does not carry.
 */
export const celScalarType = (
  operand: PlanExpressionOperand,
  mapper: Mapper,
): CelScalarType | "mixed" | undefined => {
  if (isValue(operand)) {
    const type = typeof operand.value;
    return type === "number" || type === "string" || type === "boolean"
      ? type
      : undefined;
  }
  if (isVariable(operand)) {
    return declaredScalarType(operand.name, mapper);
  }
  if (!isExpression(operand)) {
    return undefined;
  }
  switch (operand.operator) {
    // CEL has no mixed-type `+`, so one operand of a known type types the sum. The one
    // heterogeneous overload is timestamp + duration, which is a timestamp.
    case "add": {
      const types = operand.operands.map((op) => celScalarType(op, mapper));
      if (types.includes("mixed")) return "mixed";
      if (types.includes("timestamp")) return "timestamp";
      if (types.length > 0 && types.every((type) => type === "duration")) {
        return "duration";
      }
      return types.find((type) => type === "string" || type === "number");
    }
    // timestamp - timestamp is a duration, timestamp - duration a timestamp.
    case "sub": {
      const [left, right] = operand.operands.map((op) => celScalarType(op, mapper));
      if (left === "timestamp") {
        return right === "timestamp"
          ? "duration"
          : right === "duration"
            ? "timestamp"
            : undefined;
      }
      return left === "duration" ? "duration" : "number";
    }
    case "timestamp":
      return "timestamp";
    case "duration":
    case "timeSince":
      return "duration";
    case "upperAscii":
      return "string";
    case "mult":
    case "div":
    case "mod":
    case "size":
      return "number";
    case "string":
      return "string";
    case "if": {
      const [, thenOperand, elseOperand] = operand.operands;
      const [, thenType, elseType] = operand.operands.map((op) =>
        celScalarType(op, mapper),
      );
      if (thenType === "mixed" || elseType === "mixed") return "mixed";
      if (thenType === undefined || elseType === undefined) return undefined;
      // CEL gives both branches one type, so a numeric constant beside an int branch is an int.
      if (thenType === "int" && elseType === "number" && elseOperand && isValue(elseOperand)) {
        return "int";
      }
      if (elseType === "int" && thenType === "number" && thenOperand && isValue(thenOperand)) {
        return "int";
      }
      return thenType === elseType ? thenType : "mixed";
    }
    case "int":
      return "int";
    case "double":
      return "number";
    case "eq":
    case "ne":
    case "lt":
    case "le":
    case "gt":
    case "ge":
    case "and":
    case "or":
    case "not":
    case "matches":
    case "contains":
    case "startsWith":
    case "endsWith":
      return "boolean";
    default:
      return undefined;
  }
};


/**
 * CEL's `string()` of a double: every attribute number reaches CEL as a double, which cel-go
 * prints as Go's shortest `%g` (`strconv.FormatFloat(d, 'g', -1, 64)`). `$toString` agrees with
 * it (shortest digits, `e+XX`/`e-XX` exponents, `-0`, `NaN`) except in two places: it keeps fixed
 * notation up to an exponent of 15 where Go switches at 6 ("1000000" for "1e+06"), and it spells
 * the infinities "Infinity" where Go spells them "+Inf" and "-Inf". Both are rewritten here; for
 * 1e6 <= |d| < 1e16 the fixed form holds exactly the shortest digits, so moving the point is
 * enough.
 */
const celDoubleToString = (double: unknown): unknown => ({
  $let: {
    vars: { d: double },
    in: {
      $switch: {
        branches: [
          { case: { $eq: ["$$d", Infinity] }, then: "+Inf" },
          { case: { $eq: ["$$d", -Infinity] }, then: "-Inf" },
          {
            case: {
              $and: [
                { $gte: [{ $abs: "$$d" }, 1e6] },
                { $lt: [{ $abs: "$$d" }, 1e16] },
              ],
            },
            then: {
              $let: {
                vars: { fixed: { $toString: { $abs: "$$d" } } },
                in: {
                  $let: {
                    vars: {
                      point: { $indexOfCP: ["$$fixed", "."] },
                      digits: {
                        $rtrim: {
                          input: {
                            $replaceAll: {
                              input: "$$fixed",
                              find: ".",
                              replacement: "",
                            },
                          },
                          chars: "0",
                        },
                      },
                    },
                    in: {
                      $let: {
                        vars: {
                          exponent: {
                            $subtract: [
                              {
                                $cond: [
                                  { $eq: ["$$point", -1] },
                                  { $strLenCP: "$$fixed" },
                                  "$$point",
                                ],
                              },
                              1,
                            ],
                          },
                        },
                        in: {
                          $concat: [
                            { $cond: [{ $lt: ["$$d", 0] }, "-", ""] },
                            { $substrCP: ["$$digits", 0, 1] },
                            {
                              $cond: [
                                { $gt: [{ $strLenCP: "$$digits" }, 1] },
                                {
                                  $concat: [
                                    ".",
                                    {
                                      $substrCP: [
                                        "$$digits",
                                        1,
                                        { $strLenCP: "$$digits" },
                                      ],
                                    },
                                  ],
                                },
                                "",
                              ],
                            },
                            "e+",
                            { $cond: [{ $lt: ["$$exponent", 10] }, "0", ""] },
                            { $toString: "$$exponent" },
                          ],
                        },
                      },
                    },
                  },
                },
              },
            },
          },
        ],
        default: { $toString: "$$d" },
      },
    },
  },
});


const DURATION_UNIT_NANOSECONDS: Record<string, bigint> = {
  ns: 1n,
  us: 1_000n,
  "µs": 1_000n,
  "μs": 1_000n,
  ms: 1_000_000n,
  s: 1_000_000_000n,
  m: 60_000_000_000n,
  h: 3_600_000_000_000n,
};

/**
 * A Go duration string ("86400s", "1h30m", "-1.5s", "250ms") as a whole number of milliseconds.
 * Refused when it is not millisecond-exact, or not a duration at all.
 */
function parseDurationMilliseconds(literal: string): number {
  const refuse = (): never => {
    throw new UnsupportedQueryPlanError(
      `duration ${JSON.stringify(literal)} is not a millisecond-exact duration: a BSON date ` +
        "carries milliseconds, so a finer duration has no exact spelling",
    );
  };
  const match = /^([+-]?)((?:\d+(?:\.\d*)?|\.\d+)(?:ns|us|µs|μs|ms|s|m|h))+$/.exec(literal);
  if (!match) {
    return refuse();
  }
  const sign = match[1] === "-" ? -1n : 1n;
  let nanoseconds = 0n;
  for (const [, digits, unit] of literal
    .slice(match[1]!.length)
    .matchAll(/(\d+(?:\.\d*)?|\.\d+)(ns|us|µs|μs|ms|s|m|h)/g)) {
    const scale = DURATION_UNIT_NANOSECONDS[unit!]!;
    const [whole = "", fraction = ""] = digits!.split(".");
    const fractionScale = 10n ** BigInt(fraction.length);
    const fractional = BigInt(fraction || "0") * scale;
    if (fractional % fractionScale !== 0n) {
      return refuse();
    }
    nanoseconds += BigInt(whole || "0") * scale + fractional / fractionScale;
  }
  if (nanoseconds % 1_000_000n !== 0n) {
    return refuse();
  }
  const milliseconds = sign * (nanoseconds / 1_000_000n);
  if (milliseconds > BigInt(Number.MAX_SAFE_INTEGER)) {
    return refuse();
  }
  return Number(milliseconds);
}


/**
 * The strings CEL's `int()` reads, Go's `strconv.ParseInt` in base 10: an optional sign and
 * decimal digits, the whole string. The `+` is trimmed before `$convert`, and a value past
 * int64 is `$convert`'s error, null.
 */
const CEL_INT_STRING_PATTERN = "^[+-]?[0-9]+\\z";

/**
 * The strings CEL's `double()` reads, Go's `strconv.ParseFloat`: a decimal with an optional
 * fraction and exponent, underscores allowed between digits (removed before `$convert`). Its
 * hexadecimal form and the words "inf", "infinity" and "nan" are left to null, which denies.
 */
const CEL_DOUBLE_STRING_PATTERN =
  "^[+-]?(?:[0-9](?:_?[0-9])*(?:\\.(?:[0-9](?:_?[0-9])*)?)?|\\.[0-9](?:_?[0-9])*)(?:[eE][+-]?[0-9](?:_?[0-9])*)?\\z";

/** CEL converts a double to an int strictly inside int64's range (`doubleToInt64Checked`). */
const MIN_CEL_INT_DOUBLE = -9223372036854775808;
const MAX_CEL_INT_DOUBLE = 9223372036854775808;


/**
 * A field as a CEL timestamp: a stored date as-is, an RFC 3339 string converted, anything else —
 * including an instant outside CEL's range — null.
 */
function buildTimestampConversion(input: unknown): MongooseFilter {
  const converted = {
    $cond: {
      if: { $eq: [{ $type: input }, "date"] },
      then: input,
      else: {
        $cond: {
          if: {
            $cond: {
              if: { $eq: [{ $type: input }, "string"] },
              then: {
                $regexMatch: { input, regex: RFC3339_TIMESTAMP_MONGO_PATTERN },
              },
              else: false,
            },
          },
          then: {
            $convert: { input, to: "date", onError: null, onNull: null },
          },
          else: null,
        },
      },
    },
  };
  return {
    $let: {
      vars: { converted },
      in: {
        $cond: {
          if: {
            $and: [
              { $ne: ["$$converted", null] },
              { $gte: ["$$converted", MIN_CEL_TIMESTAMP] },
              { $lte: ["$$converted", MAX_CEL_TIMESTAMP] },
            ],
          },
          then: "$$converted",
          else: null,
        },
      },
    },
  };
}
