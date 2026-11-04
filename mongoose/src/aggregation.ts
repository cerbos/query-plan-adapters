import type { PlanExpression, PlanExpressionOperand } from "@cerbos/core";

import { UnsupportedQueryPlanError } from "./errors";
import type { Mapper, MongooseFilter } from "./index";
import {
  declaredScalarType,
  isNullableReference,
  relationOfReference,
  resolveFieldReference,
} from "./mapper";
import { isExpression, isValue, isVariable } from "./operands";
import { normalizeRe2PatternForMongo } from "./regex";
import {
  MAX_CEL_TIMESTAMP,
  MIN_CEL_TIMESTAMP,
  RFC3339_TIMESTAMP_MONGO_PATTERN,
  isRfc3339Timestamp,
} from "./timestamp";

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

/**
 * Builds an aggregation-pipeline expression value for use inside `$expr`.
 * - Variables become field paths prefixed with `$` (e.g. `"$aNumber"`).
 * - Values become themselves, wrapped in `$literal` where the pipeline would evaluate them.
 * - Nested expressions recurse.
 */
export const buildAggregationExpression = (
  operand: PlanExpressionOperand,
  mapper: Mapper,
): unknown => {
  if (isVariable(operand)) {
    const { path } = resolveFieldReference(operand.name, mapper);
    return "$" + path.join(".");
  }
  if (isValue(operand)) {
    return buildConstant(operand.value);
  }
  if (isExpression(operand)) {
    return buildAggregationExpressionFromExpression(operand, mapper);
  }
  throw new UnsupportedQueryPlanError("Invalid operand structure");
};

/**
 * A plan constant as an aggregation expression. The pipeline reads a string starting with `$` as
 * a field path (`"$$…"` as a variable), and evaluates a list or map element by element, so such a
 * constant — `R.attr.aString + "g" == "$aOptionalString"` — would compare against the document's
 * own field (cerbos/query-plan-adapters#575). `$literal` makes it a constant. Every other constant
 * stays bare: Mongoose's `$expr` caster sends each literal operand of `$add`, `$subtract` and the
 * like through its Number cast, which rejects a `{ $literal: 1 }`.
 */
const buildConstant = (value: unknown): unknown =>
  isEvaluatedByPipeline(value) ? { $literal: value } : value;

const isEvaluatedByPipeline = (value: unknown): boolean => {
  if (typeof value === "string") return value.startsWith("$");
  if (Array.isArray(value)) return value.some(isEvaluatedByPipeline);
  if (typeof value === "object" && value !== null) {
    return Object.entries(value).some(
      ([key, element]) => key.startsWith("$") || isEvaluatedByPipeline(element),
    );
  }
  return false;
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
 * Whether CEL can never order `left` against `right`: both types are known and differ. CEL has no
 * ordering between types, so the comparison is an error that denies under either polarity, where
 * `$lt` falls back to BSON's cross-type order (every number sorts below every string) and answers
 * true or false for every document. A type the plan does not carry is not checked, as on the
 * field-to-constant path. A ternary whose branches differ orders against one branch and raises on
 * the other, per document, which one `$expr` cannot say, so it is refused.
 */
export const isUnorderable = (
  left: PlanExpressionOperand,
  right: PlanExpressionOperand,
  mapper: Mapper,
): boolean => {
  const leftType = celScalarType(left, mapper);
  const rightType = celScalarType(right, mapper);
  if (leftType === "mixed" || rightType === "mixed") {
    throw new UnsupportedQueryPlanError(
      "Cannot order against a conditional whose branches have different types: CEL raises an " +
        "error on the documents that take the branch of the other type, and $expr would order " +
        "them by BSON type instead",
    );
  }
  const numeric = (type: CelScalarType | undefined) => type === "number" || type === "int";
  return (
    leftType !== undefined &&
    rightType !== undefined &&
    leftType !== rightType &&
    !(numeric(leftType) && numeric(rightType))
  );
};

/**
 * Whether an operand is certainly a double to CEL: an attribute number, or arithmetic over one.
 * A numeric constant is not, since the plan drops whether it was written as an int or a double.
 */
const isDoubleTyped = (operand: PlanExpressionOperand, mapper: Mapper): boolean => {
  if (isVariable(operand)) {
    return celScalarType(operand, mapper) === "number";
  }
  if (!isExpression(operand)) {
    return false;
  }
  switch (operand.operator) {
    case "double":
      return true;
    case "add":
    case "sub":
    case "mult":
    case "div":
      return operand.operands.some((op) => isDoubleTyped(op, mapper));
    case "if":
      return operand.operands.slice(1).some((op) => isDoubleTyped(op, mapper));
    default:
      return false;
  }
};

/**
 * Arithmetic over an `int()`: CEL has no overload between an int and a double, so beside an
 * attribute number the operation is an error on every document, which `null` under the
 * not-null guard denies. Beside an int or a numeric constant it is refused: the constant may
 * have been written as a double, which the plan does not say, and int overflow and division
 * are not lowered.
 */
const intArithmetic = (
  operator: string,
  operands: PlanExpressionOperand[],
  mapper: Mapper,
): unknown | undefined => {
  if (!operands.some((op) => celScalarType(op, mapper) === "int")) {
    return undefined;
  }
  if (operands.some((op) => isDoubleTyped(op, mapper))) {
    return { $literal: null };
  }
  throw new UnsupportedQueryPlanError(
    `${operator} over an int() is translated only beside an attribute number, where CEL has no ` +
      "int and double overload: a numeric constant may have been written as a double, which " +
      "the plan does not say",
  );
};

const intArithmeticGuard = (
  expression: PlanExpression,
  mapper: Mapper,
): MongooseFilter | undefined =>
  expression.operands.some((op) => celScalarType(op, mapper) === "int")
    ? notNullGuard(expression, mapper)
    : undefined;

type AggregationOperator = {
  build: (expression: PlanExpression, mapper: Mapper) => unknown;
  /**
   * The filter that keeps out every document on which this expression cannot be evaluated —
   * where CEL raises and `check()` denies, while the pipeline would carry on with a null or a
   * wrong-typed value. Every operand the enclosing filter translated is walked for these and each
   * one found is ANDed alongside it (see `withEvaluationGuards`).
   */
  guard?: (
    expression: PlanExpression,
    mapper: Mapper,
  ) => MongooseFilter | undefined;
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

/** Every operator that can appear inside `$expr`. Adding one is adding an entry here. */
const AGGREGATION_OPERATORS: Record<string, AggregationOperator> = {
  eq: variadic(COMPARISON_OPERATORS.eq),
  ne: variadic(COMPARISON_OPERATORS.ne),
  lt: ordering(COMPARISON_OPERATORS.lt),
  le: ordering(COMPARISON_OPERATORS.le),
  gt: ordering(COMPARISON_OPERATORS.gt),
  ge: ordering(COMPARISON_OPERATORS.ge),
  and: variadic("$and"),
  or: variadic("$or"),
  sub: {
    build: (expression, mapper) =>
      intArithmetic("sub", expression.operands, mapper) ??
      variadic("$subtract").build(expression, mapper),
    guard: (expression, mapper) =>
      intArithmeticGuard(expression, mapper) ?? timestampRangeGuard(expression, mapper),
  },
  mult: {
    build: (expression, mapper) =>
      intArithmetic("mult", expression.operands, mapper) ??
      variadic("$multiply").build(expression, mapper),
    guard: intArithmeticGuard,
  },
  // CEL's `%` is integer-only: it has no double overload, and every number a resource attribute
  // carries reaches CEL as a double, so `R.attr.x % 2` is a no-such-overload error that denies
  // the row under either polarity, where `$mod` computes a floating remainder. The one int this
  // pipeline produces is `size()` (`int()` is refused), and a zero divisor is an error that
  // `$mod` turns into an aborted query, so the divisor must be a non-zero integer constant.
  mod: {
    build: ({ operands }, mapper) => {
      const int = intArithmetic("mod", operands, mapper);
      if (int !== undefined) {
        return int;
      }
      const [dividend, divisor] = operands;
      if (
        !dividend ||
        !(isExpression(dividend) && dividend.operator === "size")
      ) {
        throw new UnsupportedQueryPlanError(
          "mod operator requires an integer dividend: CEL's % has no double overload and every " +
            "attribute number reaches CEL as a double, so the modulo is an error on every " +
            "document where $mod would compute a floating remainder",
        );
      }
      if (
        !divisor ||
        !isValue(divisor) ||
        !Number.isInteger(divisor.value) ||
        divisor.value === 0
      ) {
        throw new UnsupportedQueryPlanError(
          "mod operator requires a non-zero integer constant divisor: $mod aborts the whole " +
            "query on a zero divisor instead of denying that document",
        );
      }
      return {
        $mod: operands.map((op) => buildAggregationExpression(op, mapper)),
      };
    },
    guard: intArithmeticGuard,
  },
  // CEL overloads `+` on strings, and MongoDB does not: `$add` accepts numeric and date types
  // only and the server rejects the whole query at execution time rather than returning no
  // rows ("$add only supports numeric or date types"). `$concat` is the string spelling.
  //
  // A CONSTANT settles which overload it is — CEL has no mixed-type `+`, so one string operand
  // means every operand is a string (cerbos/query-plan-adapters#376). Between two field paths
  // there is no constant, and the plan carries no field types, so neither spelling can be
  // chosen: the shape is refused at translation instead of being sent to the server as a guess
  // that aborts the whole query (cerbos/query-plan-adapters#391).
  add: {
    build: ({ operands }, mapper) => {
      const built = () =>
        operands.map((op) => buildAggregationExpression(op, mapper));
      const int = intArithmetic("add", operands, mapper);
      if (int !== undefined) {
        return int;
      }
      if (operands.some((op) => isValue(op) && typeof op.value === "string")) {
        return { $concat: built() };
      }
      // CEL also overloads `+` on lists, which `$add` rejects as loudly as strings. A list
      // operand is spelled out in the plan — a list constant, a ternary between two, or a
      // nested concatenation — and `$concatArrays` keeps CEL's order and duplicates. A list
      // read from a field is not: a relation projects through its elements, so it is refused.
      if (operands.some(isListOperand)) {
        if (!operands.every(isListOperand)) {
          throw new UnsupportedQueryPlanError(
            "List concatenation is translated only between list constants and ternaries over " +
              "them: a list read from the document is a projection over a relation's " +
              "elements, not an ordered list value $concatArrays can extend",
          );
        }
        return { $concatArrays: built() };
      }
      // Between bare field paths the mapper's declared types decide, and where they do not,
      // the stored values do, per document: two strings concatenate, two numbers add, and any
      // other pair is CEL's no-such-overload error, a null here that the guard denies. A nested
      // expression keeps the numeric reading it has always had — the divisions and ternaries
      // that reach here are numeric by construction.
      if (operands.every(isVariable)) {
        const types = operands.map((op) => celScalarType(op, mapper));
        if (types.every((type) => type === "string")) {
          return { $concat: built() };
        }
        if (types.every((type) => type === "number")) {
          return { $add: built() };
        }
        if (types.some((type) => type !== undefined && type !== "string" && type !== "number")) {
          throw new UnsupportedQueryPlanError(
            "'+' between two fields is translated only over strings and numbers",
          );
        }
        const paths = built();
        return {
          $switch: {
            branches: [
              {
                case: { $and: paths.map((path) => ({ $eq: [{ $type: path }, "string"] })) },
                then: { $concat: paths },
              },
              {
                case: { $and: paths.map((path) => ({ $isNumber: path })) },
                then: { $add: paths },
              },
            ],
            default: null,
          },
        };
      }
      // A nested concatenation (`a + "_" + b` is `(a + "_") + b`) carries its string type up
      // even though no operand of THIS `+` is a constant.
      if (
        operands.some((op) => isExpression(op) && celScalarType(op, mapper) === "string")
      ) {
        return { $concat: built() };
      }
      return { $add: built() };
    },
    // timestamp + duration leaves CEL's timestamp range as an error, where `$add` carries on.
    guard: (expression, mapper) =>
      intArithmeticGuard(expression, mapper) ??
      (expression.operands.every(isVariable) &&
      !expression.operands.every((op) => celScalarType(op, mapper) !== undefined)
        ? notNullGuard(expression, mapper)
        : timestampRangeGuard(expression, mapper)),
  },
  div: {
    build: ({ operands }, mapper) => {
      const int = intArithmetic("div", operands, mapper);
      if (int !== undefined) {
        return int;
      }
      const denominator = operands[1];
      if (
        !denominator ||
        !isValue(denominator) ||
        typeof denominator.value !== "number" ||
        denominator.value === 0
      ) {
        throw new UnsupportedQueryPlanError(
          "div operator requires a non-zero constant denominator",
        );
      }
      return {
        $divide: operands.map((op) => buildAggregationExpression(op, mapper)),
      };
    },
    guard: intArithmeticGuard,
  },
  not: {
    build: ({ operands }, mapper) => {
      const operand = operands[0];
      if (!operand) {
        throw new UnsupportedQueryPlanError("not operator requires an operand");
      }
      return { $not: [buildAggregationExpression(operand, mapper)] };
    },
  },
  string: {
    build: ({ operands }, mapper) => {
      const operand = operands[0];
      if (!operand) {
        throw new UnsupportedQueryPlanError("string conversion requires an operand");
      }
      const input = buildAggregationExpression(operand, mapper);
      // An int renders in plain decimal, as `$toString` spells a long; a ternary's int branch
      // types the constant in the other.
      if (celScalarType(operand, mapper) === "int") {
        return { $toString: input };
      }
      if (rendersUntypedIntegralConstant(operand)) {
        throw new UnsupportedQueryPlanError(
          "string() over an integral constant whose int or double type the plan does not carry: " +
            'CEL renders the int 1000000 as "1000000" and the double as "1e+06", and the plan ' +
            "ships both as the same bare number",
        );
      }
      return {
        $switch: {
          branches: [
            {
              case: {
                $in: [{ $type: input }, ["int", "long", "double", "decimal"]],
              },
              then: celDoubleToString({ $toDouble: input }),
            },
            {
              case: { $in: [{ $type: input }, ["string", "bool"]] },
              then: { $toString: input },
            },
          ],
          default: null,
        },
      };
    },
    guard: notNullGuard,
  },
  // CEL's int()/double() are not $convert. CEL reads a WHOLE string or raises, and an
  // error DENIES the row; $convert parses a leading numeric prefix, so "100%_done"
  // becomes 100 and the filter returns records the PDP denies. The numeric direction is
  // no safer: CEL truncates toward zero while $convert to "long" ROUNDS, so int(-0.6) is
  // 0 to CEL and -1 here. Nothing in the plan says what type the field holds, so no
  // conversion is faithful for every document (cerbos/query-plan-adapters#311).
  double: {
    build: ({ operands }, mapper) => {
      const operand = operands[0];
      if (!operand) {
        throw new UnsupportedQueryPlanError("double conversion requires an operand");
      }
      const input = buildAggregationExpression(operand, mapper);
      return {
        $switch: {
          branches: [
            {
              case: { $isNumber: input },
              then: { $toDouble: input },
            },
            {
              case: {
                $and: [
                  { $eq: [{ $type: input }, "string"] },
                  { $regexMatch: { input, regex: CEL_DOUBLE_STRING_PATTERN } },
                ],
              },
              then: {
                $convert: {
                  input: { $replaceAll: { input, find: "_", replacement: "" } },
                  to: "double",
                  onError: null,
                  onNull: null,
                },
              },
            },
          ],
          default: null,
        },
      };
    },
    guard: notNullGuard,
  },
  int: {
    build: ({ operands }, mapper) => {
      const operand = operands[0];
      if (!operand) {
        throw new UnsupportedQueryPlanError("int conversion requires an operand");
      }
      const type = celScalarType(operand, mapper);
      if (type === "int") {
        return buildAggregationExpression(operand, mapper);
      }
      if (type !== undefined && type !== "number" && type !== "string") {
        throw new UnsupportedQueryPlanError(`int() over a ${type} is not translated`);
      }
      const input = buildAggregationExpression(operand, mapper);
      return {
        $switch: {
          branches: [
            {
              // Truncation toward zero, within the range CEL converts; NaN orders below every
              // number here and fails the lower bound as CEL's error does.
              case: {
                $and: [
                  { $isNumber: input },
                  { $gt: [input, MIN_CEL_INT_DOUBLE] },
                  { $lt: [input, MAX_CEL_INT_DOUBLE] },
                ],
              },
              then: {
                $convert: { input: { $trunc: input }, to: "long", onError: null, onNull: null },
              },
            },
            {
              case: {
                $and: [
                  { $eq: [{ $type: input }, "string"] },
                  { $regexMatch: { input, regex: CEL_INT_STRING_PATTERN } },
                ],
              },
              then: {
                $convert: {
                  input: { $ltrim: { input, chars: "+" } },
                  to: "long",
                  onError: null,
                  onNull: null,
                },
              },
            },
          ],
          default: null,
        },
      };
    },
    guard: notNullGuard,
  },
  if: {
    build: ({ operands }, mapper) => {
      const [ifOp, thenOp, elseOp] = operands;
      if (!ifOp || !thenOp || !elseOp) {
        throw new UnsupportedQueryPlanError("if operator requires three operands");
      }
      return {
        $cond: {
          if: buildAggregationExpression(ifOp, mapper),
          then: buildAggregationExpression(thenOp, mapper),
          else: buildAggregationExpression(elseOp, mapper),
        },
      };
    },
  },
  index: {
    build: ({ operands }, mapper) => {
      const [collection, index] = parseConstantIndexOperands(operands);
      // Keep `$arrayElemAt` even for index 0. Mongoose's `$expr` caster casts a comparison's
      // literal to the schema type of a PATH on the other side, and it treats `$first`/`$last`
      // as one: over a `[Boolean]` array, `$first == 1` is cast to `== true`, which CEL denies.
      // `$arrayElemAt` takes an array operand, so the literal reaches the server uncast
      // (`type-mismatch/equals/boolean-list-element-against-number-literal` and
      // `type-mismatch/equals/number-list-element-against-boolean-literal` fail otherwise).
      return {
        $arrayElemAt: [buildAggregationExpression(collection, mapper), index],
      };
    },
    // An out-of-range index is an error to CEL, not a missing value.
    guard: ({ operands }, mapper) => {
      const [collectionOperand, index] = parseConstantIndexOperands(operands);
      const collection = buildAggregationExpression(collectionOperand, mapper);
      return {
        $expr: {
          $cond: {
            if: { $isArray: collection },
            then: { $gt: [{ $size: collection }, index] },
            else: false,
          },
        },
      };
    },
  },
  "get-field": {
    build: ({ operands }, mapper) => {
      const [inputOperand, fieldOperand] = operands;
      if (!inputOperand || !fieldOperand || !isVariable(fieldOperand)) {
        throw new UnsupportedQueryPlanError("get-field requires an input and a field name");
      }
      return {
        $getField: {
          field: fieldOperand.name,
          input: buildAggregationExpression(inputOperand, mapper),
        },
      };
    },
    // A field the element lacks is a missing-key error to CEL, and so is a stored null on a field
    // the relation declares nullable. `$getField` yields missing or null there instead, which a
    // negated comparison would read as unequal: `!(tags[0].name == "public")` selected a first
    // tag with no name.
    guard: (expression, mapper) => {
      const [inputOperand, fieldOperand] = expression.operands;
      if (!inputOperand || !fieldOperand || !isVariable(fieldOperand)) {
        return undefined;
      }
      const value = buildAggregationExpressionFromExpression(expression, mapper);
      const collection =
        isExpression(inputOperand) && inputOperand.operator === "index"
          ? inputOperand.operands[0]
          : undefined;
      const nullable =
        collection !== undefined &&
        isVariable(collection) &&
        isNullableReference(`${collection.name}.${fieldOperand.name}`, mapper);
      return {
        $expr: nullable
          ? { $ne: [{ $ifNull: [value, null] }, null] }
          : { $ne: [{ $type: value }, "missing"] },
      };
    },
  },
  size: {
    build: ({ operands }, mapper) => {
      const operand = operands[0];
      if (!operand) {
        throw new UnsupportedQueryPlanError("size operator requires an operand");
      }
      const inner = buildAggregationExpression(operand, mapper);
      // Works for both arrays and strings: $size for arrays, $strLenCP otherwise.
      const size = {
        $cond: [
          { $isArray: inner },
          { $size: inner },
          {
            $cond: [
              { $eq: [{ $type: inner }, "string"] },
              { $strLenCP: inner },
              null,
            ],
          },
        ],
      };
      const parentPath = isVariable(operand)
        ? relationOfReference(operand.name, mapper)?.requiresParent
        : undefined;
      if (parentPath === undefined) {
        return size;
      }
      // Reached through the stored parent array, `$parent.children` is one array PER parent
      // element, so `$size` of it counts parents, not children. The chain's list is every
      // child of the (one) parent: flatten before counting.
      const children = {
        $size: {
          $reduce: {
            input: { $ifNull: [inner, []] },
            initialValue: [],
            in: {
              $concatArrays: [
                "$$value",
                { $cond: [{ $isArray: "$$this" }, "$$this", []] },
              ],
            },
          },
        },
      };
      // An absent to-one parent counts as UNKNOWN, not 0. null loses against every number
      // in BSON order, so both `== 0` and `>= 0` exclude the document (#309).
      return {
        $cond: [
          { $gt: [{ $size: { $ifNull: [`$${parentPath}`, []] } }, 0] },
          children,
          null,
        ],
      };
    },
    guard: notNullGuard,
  },
  matches: {
    build: ({ operands }, mapper) => {
      const [valueOp, patternOp] = operands;
      if (
        !valueOp ||
        !patternOp ||
        !isValue(patternOp) ||
        typeof patternOp.value !== "string"
      ) {
        throw new UnsupportedQueryPlanError("matches operator requires two operands");
      }
      return {
        $regexMatch: {
          input: buildAggregationExpression(valueOp, mapper),
          regex: normalizeRe2PatternForMongo(patternOp.value),
        },
      };
    },
    guard: ({ operands }, mapper) => {
      const inputOperand = operands[0];
      if (!inputOperand) {
        throw new UnsupportedQueryPlanError("matches operator requires an input operand");
      }
      return {
        $expr: {
          $eq: [
            { $type: buildAggregationExpression(inputOperand, mapper) },
            "string",
          ],
        },
      };
    },
  },
  contains: {
    build: (expression, mapper) =>
      buildStringPredicate(expression, mapper, (index) => ({
        $gte: [index, 0],
      })),
    guard: notNullGuard,
  },
  startsWith: {
    build: (expression, mapper) =>
      buildStringPredicate(expression, mapper, (index) => ({
        $eq: [index, 0],
      })),
    guard: notNullGuard,
  },
  endsWith: {
    build: (expression, mapper) =>
      buildStringPredicate(expression, mapper, (_index, receiver, needle) => {
        const receiverLength = { $strLenCP: receiver };
        const needleLength = { $strLenCP: needle };
        return {
          $cond: {
            if: { $gte: [receiverLength, needleLength] },
            then: {
              $eq: [
                {
                  $substrCP: [
                    receiver,
                    { $subtract: [receiverLength, needleLength] },
                    needleLength,
                  ],
                },
                needle,
              ],
            },
            else: false,
          },
        };
      }),
    guard: notNullGuard,
  },
  timestamp: {
    build: ({ operands }, mapper) => {
      const operand = operands[0];
      if (!operand) {
        throw new UnsupportedQueryPlanError("timestamp operator requires an operand");
      }
      if (isValue(operand)) {
        if (
          typeof operand.value !== "string" ||
          !isRfc3339Timestamp(operand.value)
        ) {
          throw new UnsupportedQueryPlanError(
            "timestamp value must be a millisecond-exact RFC 3339 instant in the CEL range",
          );
        }
        return new Date(operand.value);
      }
      return buildTimestampConversion(
        buildAggregationExpression(operand, mapper),
      );
    },
    // A literal is validated at translation time; only a field can fail to convert per document.
    guard: (expression, mapper) =>
      expression.operands[0] && !isValue(expression.operands[0])
        ? notNullGuard(expression, mapper)
        : undefined,
  },
  // A duration is carried as its length in milliseconds, the unit `$add` and `$subtract` apply to
  // a date and the one the difference of two dates is in. The planner normalises the literal to
  // seconds ("86400s"); one finer than a millisecond has no exact spelling and is refused, as a
  // sub-millisecond timestamp literal is.
  duration: {
    build: ({ operands }) => {
      const operand = operands[0];
      if (!operand || !isValue(operand) || typeof operand.value !== "string") {
        throw new UnsupportedQueryPlanError(
          "duration() is translated only over a constant string",
        );
      }
      return parseDurationMilliseconds(operand.value);
    },
  },
  // `timeSince` is relative to the clock of whoever evaluates it, so it is read from the server's
  // own `$$NOW` at query time, in milliseconds like every duration here.
  timeSince: {
    build: ({ operands }, mapper) => {
      const operand = operands[0];
      if (!operand) {
        throw new UnsupportedQueryPlanError("timeSince requires an operand");
      }
      if (celScalarType(operand, mapper) !== "timestamp") {
        throw new UnsupportedQueryPlanError(
          "timeSince() is translated only over a timestamp() conversion",
        );
      }
      return {
        $subtract: ["$$NOW", buildAggregationExpression(operand, mapper)],
      };
    },
  },
  // CEL's upperAscii folds a-z only and leaves every other character as it is, which is what
  // MongoDB's `$toUpper` does too. `$toUpper` turns a null into "" and a number into its digits,
  // where CEL has no overload, so anything but a string is null here and guarded out.
  upperAscii: {
    build: ({ operands }, mapper) => {
      const operand = operands[0];
      if (!operand) {
        throw new UnsupportedQueryPlanError("upperAscii requires an operand");
      }
      const input = buildAggregationExpression(operand, mapper);
      return {
        $cond: [{ $eq: [{ $type: input }, "string"] }, { $toUpper: input }, null],
      };
    },
    guard: notNullGuard,
  },
};

/**
 * Whether `operand` is a list the plan spells out: a list constant, a ternary whose branches are
 * both such lists, or a concatenation of them.
 */
const isListOperand = (operand: PlanExpressionOperand): boolean => {
  if (isValue(operand)) {
    return Array.isArray(operand.value);
  }
  if (!isExpression(operand)) {
    return false;
  }
  if (operand.operator === "if") {
    const [, thenOperand, elseOperand] = operand.operands;
    return (
      thenOperand !== undefined &&
      elseOperand !== undefined &&
      isListOperand(thenOperand) &&
      isListOperand(elseOperand)
    );
  }
  return (
    operand.operator === "add" &&
    operand.operands.length > 0 &&
    operand.operands.every(isListOperand)
  );
};

/** A timestamp computed by `+` or `-` outside CEL's range is an error to CEL, not a date. */
function timestampRangeGuard(
  expression: PlanExpression,
  mapper: Mapper,
): MongooseFilter | undefined {
  if (celScalarType(expression, mapper) !== "timestamp") {
    return undefined;
  }
  const computed = buildAggregationExpressionFromExpression(expression, mapper);
  return {
    $expr: {
      $and: [
        { $gte: [computed, MIN_CEL_TIMESTAMP] },
        { $lte: [computed, MAX_CEL_TIMESTAMP] },
      ],
    },
  };
}

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

const aggregationOperator = (
  operator: string,
): AggregationOperator | undefined =>
  Object.hasOwn(AGGREGATION_OPERATORS, operator)
    ? AGGREGATION_OPERATORS[operator]
    : undefined;

export const buildAggregationExpressionFromExpression = (
  expression: PlanExpression,
  mapper: Mapper,
): unknown => {
  const definition = aggregationOperator(expression.operator);
  if (!definition) {
    throw new UnsupportedQueryPlanError(
      `Unsupported operator inside aggregation expression: ${expression.operator}`,
    );
  }
  return definition.build(expression, mapper);
};

/**
 * The evaluation guards for every guarded expression below `operand`, outermost first.
 */
export const buildEvaluationGuards = (
  operand: PlanExpressionOperand,
  mapper: Mapper,
): MongooseFilter[] => {
  if (!isExpression(operand)) {
    return [];
  }
  const guard = aggregationOperator(operand.operator)?.guard?.(operand, mapper);
  const nested =
    scopedBranchGuards(operand, mapper) ??
    operand.operands.flatMap((child) => buildEvaluationGuards(child, mapper));
  return guard ? [guard, ...nested] : nested;
};

/**
 * The guards of a ternary: CEL reads only the branch the condition selects, so each branch's
 * guards hold under that condition alone, as one `$cond`. Undefined for any other expression,
 * or a branch guard that is not an aggregation expression.
 */
const scopedBranchGuards = (
  operand: PlanExpression,
  mapper: Mapper,
): MongooseFilter[] | undefined => {
  const [condition, whenTrue, whenFalse] = operand.operands;
  if (operand.operator !== "if" || !condition || !whenTrue || !whenFalse) {
    return undefined;
  }
  const asExpression = (guards: MongooseFilter[]): unknown[] | undefined => {
    const expressions = guards.map((guard) =>
      Object.keys(guard).length === 1 ? guard["$expr"] : undefined,
    );
    return expressions.every((expression) => expression !== undefined)
      ? expressions
      : undefined;
  };
  const thenGuards = asExpression(buildEvaluationGuards(whenTrue, mapper));
  const elseGuards = asExpression(buildEvaluationGuards(whenFalse, mapper));
  if (thenGuards === undefined || elseGuards === undefined) {
    return undefined;
  }
  const conditionGuards = buildEvaluationGuards(condition, mapper);
  if (thenGuards.length === 0 && elseGuards.length === 0) {
    return conditionGuards;
  }
  const all = (expressions: unknown[]): unknown =>
    expressions.length === 0 ? true : { $and: expressions };
  return [
    ...conditionGuards,
    {
      $expr: {
        $cond: [
          buildAggregationExpression(condition, mapper),
          all(thenGuards),
          all(elseGuards),
        ],
      },
    },
  ];
};

/** A plan operator that maps one-to-one onto a variadic aggregation operator. */
function variadic(mongoOperator: string): AggregationOperator {
  return {
    build: ({ operands }, mapper) => ({
      [mongoOperator]: operands.map((op) =>
        buildAggregationExpression(op, mapper),
      ),
    }),
  };
}

/**
 * An ordering nested inside `$expr`, where it cannot answer CEL's error: an operand of each type
 * would leave a ternary's condition, or a `$not`, to BSON's cross-type order.
 */
function ordering(mongoOperator: string): AggregationOperator {
  const { build } = variadic(mongoOperator);
  return {
    build: (expression, mapper) => {
      const built = build(expression, mapper);
      const [left, right] = expression.operands;
      if (left && right && isUnorderable(left, right, mapper)) {
        throw new UnsupportedQueryPlanError(
          "Cannot order operands of different types inside an aggregation expression: CEL " +
            "raises an error there, and $expr would order them by BSON type instead",
        );
      }
      return built;
    },
  };
}

/** The expression evaluates to null exactly where CEL would raise. */
function notNullGuard(
  expression: PlanExpression,
  mapper: Mapper,
): MongooseFilter {
  return {
    $expr: {
      $ne: [buildAggregationExpressionFromExpression(expression, mapper), null],
    },
  };
}

/**
 * Whether `string()` over the operand could render a numeric constant, bare or as a ternary
 * branch, whose int or double type the plan dropped: the plan ships `1000000` and `1000000.0`
 * alike. Only an integral magnitude of 1e6 or more renders differently, since Go's shortest `%g`
 * switches a double to an exponent there ("1e+06") while an int stays plain decimal. `int()` and
 * `double()`, which would fix a branch's type, are refused on their own.
 */
function rendersUntypedIntegralConstant(operand: PlanExpressionOperand): boolean {
  if (isValue(operand)) {
    return (
      typeof operand.value === "number" &&
      Number.isInteger(operand.value) &&
      Math.abs(operand.value) >= 1e6
    );
  }
  if (!isExpression(operand) || operand.operator !== "if") return false;
  return operand.operands.slice(1).some(rendersUntypedIntegralConstant);
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

/** `contains`/`startsWith`/`endsWith` over two strings; null (an error to CEL) otherwise. */
function buildStringPredicate(
  { operator, operands }: PlanExpression,
  mapper: Mapper,
  predicate: (index: unknown, receiver: unknown, needle: unknown) => unknown,
): MongooseFilter {
  const [receiverOperand, needleOperand] = operands;
  if (!receiverOperand || !needleOperand) {
    throw new UnsupportedQueryPlanError(`${operator} requires two operands`);
  }
  const receiver = buildAggregationExpression(receiverOperand, mapper);
  const needle = buildAggregationExpression(needleOperand, mapper);
  return {
    $cond: [
      {
        $and: [
          { $eq: [{ $type: receiver }, "string"] },
          { $eq: [{ $type: needle }, "string"] },
        ],
      },
      predicate({ $indexOfCP: [receiver, needle] }, receiver, needle),
      null,
    ],
  };
}

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

export function parseConstantIndexOperands(
  operands: PlanExpressionOperand[],
): readonly [collection: PlanExpressionOperand, index: number] {
  const [collectionOperand, indexOperand] = operands;
  if (!collectionOperand || !indexOperand) {
    throw new UnsupportedQueryPlanError("index operator requires two operands");
  }
  if (
    !isValue(indexOperand) ||
    typeof indexOperand.value !== "number" ||
    !Number.isInteger(indexOperand.value) ||
    indexOperand.value < 0
  ) {
    throw new UnsupportedQueryPlanError("index operator requires a non-negative integer constant");
  }
  return [collectionOperand, indexOperand.value];
}
