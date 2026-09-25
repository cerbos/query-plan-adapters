import type { PlanExpression, PlanExpressionOperand } from "@cerbos/core";

import { UnsupportedQueryPlanError } from "./errors";
import type { Mapper, MongooseFilter } from "./index";
import { relationOfReference, resolveFieldReference } from "./mapper";
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
 * - Values become themselves.
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
    return operand.value;
  }
  if (isExpression(operand)) {
    return buildAggregationExpressionFromExpression(operand, mapper);
  }
  throw new UnsupportedQueryPlanError("Invalid operand structure");
};

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
  lt: variadic(COMPARISON_OPERATORS.lt),
  le: variadic(COMPARISON_OPERATORS.le),
  gt: variadic(COMPARISON_OPERATORS.gt),
  ge: variadic(COMPARISON_OPERATORS.ge),
  and: variadic("$and"),
  or: variadic("$or"),
  sub: variadic("$subtract"),
  mult: variadic("$multiply"),
  // CEL's `%` is integer-only: it has no double overload, and every number a resource attribute
  // carries reaches CEL as a double, so `R.attr.x % 2` is a no-such-overload error that denies
  // the row under either polarity, where `$mod` computes a floating remainder. The one int this
  // pipeline produces is `size()` (`int()` is refused), and a zero divisor is an error that
  // `$mod` turns into an aborted query, so the divisor must be a non-zero integer constant.
  mod: {
    build: ({ operands }, mapper) => {
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
      if (operands.some((op) => isValue(op) && typeof op.value === "string")) {
        return { $concat: built() };
      }
      // Only two bare field paths reveal nothing. A nested expression keeps the numeric reading
      // it has always had — the divisions and ternaries that reach here are numeric by
      // construction, and narrowing them would refuse shapes this adapter already answers.
      if (operands.every(isVariable)) {
        throw new UnsupportedQueryPlanError(
          "Cannot tell numeric addition from string concatenation in '+' between two fields: " +
            "CEL overloads '+' on strings and the query plan carries no field types, so neither " +
            "$add nor $concat can be chosen",
        );
      }
      return { $add: built() };
    },
  },
  div: {
    build: ({ operands }, mapper) => {
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
      if (rendersUntypedIntegralConstant(operand)) {
        throw new UnsupportedQueryPlanError(
          "string() over an integral constant whose int or double type the plan does not carry: " +
            'CEL renders the int 1000000 as "1000000" and the double as "1e+06", and the plan ' +
            "ships both as the same bare number",
        );
      }
      const input = buildAggregationExpression(operand, mapper);
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
  double: { build: refuseNumericConversion, guard: notNullGuard },
  int: { build: refuseNumericConversion, guard: notNullGuard },
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
};

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
  const nested = operand.operands.flatMap((child) =>
    buildEvaluationGuards(child, mapper),
  );
  return guard ? [guard, ...nested] : nested;
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

function refuseNumericConversion({ operator }: PlanExpression): never {
  throw new UnsupportedQueryPlanError(
    `'${operator}()' cannot be translated: $convert parses a numeric prefix where CEL ` +
      "requires the whole string and raises otherwise, and rounds where CEL truncates " +
      "toward zero",
  );
}

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
