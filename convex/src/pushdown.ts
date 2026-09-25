import type {
  PlanExpressionOperand,
  PlanExpressionValue,
  PlanExpressionVariable,
} from "@cerbos/core";

import { UnsupportedQueryPlanError } from "./errors";
import type { Mapper } from "./index";
import {
  isExpression,
  isNullableField,
  isValue,
  isVariable,
  operandAt,
  resolveField,
} from "./operands";
import type { ComparisonOperator } from "./operands";

/**
 * What Convex's filter engine can answer on its own, and the builder calls that answer it.
 *
 * `PUSHDOWN_RULES` is the whole list: an operator with no rule is always left to the post-filter.
 * Each rule pairs the test (`accepts`) with the emission (`emit`), and `emit` is only ever called
 * on a node its `accepts` passed, so adding a pushable operator is adding one entry.
 */

/** The subset of Convex's `FilterBuilder` the emitted filter calls. */
export interface FilterQ {
  eq: (a: unknown, b: unknown) => unknown;
  neq: (a: unknown, b: unknown) => unknown;
  lt: (a: unknown, b: unknown) => unknown;
  lte: (a: unknown, b: unknown) => unknown;
  gt: (a: unknown, b: unknown) => unknown;
  gte: (a: unknown, b: unknown) => unknown;
  and: (...args: unknown[]) => unknown;
  or: (...args: unknown[]) => unknown;
  not: (a: unknown) => unknown;
  field: (name: string) => unknown;
}

interface PushdownRule {
  accepts: (operands: PlanExpressionOperand[], mapper: Mapper) => boolean;
  /**
   * `negated` is whether an odd number of `not`s encloses the node. The emitted filter matches
   * exactly the documents on which the node, under that polarity, is CEL `true`. A CEL error is
   * false under BOTH polarities, so a rule whose operator can error cannot leave its negation to an
   * enclosing `q.not`, which would turn the error's `false` into `true`.
   */
  emit: (
    operands: PlanExpressionOperand[],
    q: FilterQ,
    mapper: Mapper,
    operator: string,
    negated: boolean,
  ) => unknown;
}

const CONVEX_COMPARISON = {
  eq: "eq",
  ne: "neq",
  lt: "lt",
  le: "lte",
  gt: "gt",
  ge: "gte",
} as const satisfies Record<ComparisonOperator, keyof FilterQ>;

/** `5 < x` is `x > 5`: the comparison that holds once the operands are swapped. */
const MIRRORED_COMPARISON: Record<ComparisonOperator, ComparisonOperator> = {
  eq: "eq",
  ne: "ne",
  lt: "gt",
  le: "ge",
  gt: "lt",
  ge: "le",
};

/** A comparison's field and literal, in either order; undefined for any other operand pair. */
const fieldAndLiteral = (
  operands: PlanExpressionOperand[],
):
  | {
      field: PlanExpressionVariable;
      literal: PlanExpressionValue;
      swapped: boolean;
    }
  | undefined => {
  const [left, right] = operands;
  if (!left || !right) return undefined;
  if (isVariable(left) && isValue(right)) {
    return { field: left, literal: right, swapped: false };
  }
  if (isValue(left) && isVariable(right)) {
    return { field: right, literal: left, swapped: true };
  }
  return undefined;
};

/** `in` with the field first and a literal list second. */
const fieldInList = (
  operands: PlanExpressionOperand[],
): { field: PlanExpressionVariable; values: unknown[] } | undefined => {
  const [needle, haystack] = operands;
  if (!needle || !haystack) return undefined;
  if (
    isVariable(needle) &&
    isValue(haystack) &&
    Array.isArray(haystack.value)
  ) {
    return { field: needle, values: haystack.value };
  }
  return undefined;
};

const allPushable = (operands: PlanExpressionOperand[], mapper: Mapper) =>
  operands.every((operand) => canPushToDb(operand, mapper));

/** The largest int64. Convex orders every int64 below every float64. */
const INT64_MAX = 2n ** 63n - 1n;

/**
 * A filter holding for exactly the documents whose `field` has the Convex type a JSON `literal` is
 * stored as, or undefined for a literal CEL has no ordering for at all (null, a list, a map).
 *
 * Convex orders values ACROSS types — undefined < null < int64 < float64 < boolean < string <
 * bytes < array < object — so `q.lt(field, "5")` is true for every number, and `q.gt(field, 0)`
 * for every boolean and every string. CEL has no ordering between types: the same comparison is an
 * error, which denies the document under either polarity. Each guard is bounded by the values
 * just outside the literal's own type in that order.
 */
const sameTypeAs = (literal: unknown, q: FilterQ, field: unknown): unknown => {
  switch (typeof literal) {
    case "number":
      return q.and(q.gt(field, INT64_MAX), q.lt(field, false));
    case "boolean":
      return q.and(q.gte(field, false), q.lte(field, true));
    case "string":
      return q.and(q.gte(field, ""), q.lt(field, new ArrayBuffer(0)));
    default:
      return undefined;
  }
};

/** The field and the Convex method that compares it with the literal, operand order resolved. */
const comparisonParts = (
  operands: PlanExpressionOperand[],
  q: FilterQ,
  mapper: Mapper,
  operator: string,
) => {
  const pair = fieldAndLiteral(operands);
  if (!pair) {
    throw new UnsupportedQueryPlanError(
      `${operator} operator requires one field and one value operand`,
    );
  }
  const cel = operator as ComparisonOperator;
  const method =
    CONVEX_COMPARISON[pair.swapped ? MIRRORED_COMPARISON[cel] : cel];
  const field = q.field(resolveField(pair.field.name, mapper));
  return { field, literal: pair.literal.value, method };
};

/** `==` and `!=`: CEL equality across types is false, never an error, exactly as in Convex. */
const equalityRule: PushdownRule = {
  accepts: (operands, mapper) => {
    const pair = fieldAndLiteral(operands);
    return pair !== undefined && !isNullableField(pair.field.name, mapper);
  },
  emit: (operands, q, mapper, operator, negated) => {
    const { field, literal, method } = comparisonParts(
      operands,
      q,
      mapper,
      operator,
    );
    const compared = q[method](field, literal);
    return negated ? q.not(compared) : compared;
  },
};

/**
 * `<`, `<=`, `>`, `>=`: pushed only behind a guard confining the field to the literal's type, and
 * the guard stays OUTSIDE any negation. `!(x < "5")` over a number is still a CEL error, so it
 * must not become Convex's `!(true)` for the same document — or, for `!(x >= "5")`, `!(false)`,
 * which returns every number (cerbos/query-plan-adapters#516).
 */
const orderingRule: PushdownRule = {
  accepts: (operands, mapper) => {
    const pair = fieldAndLiteral(operands);
    return pair !== undefined && !isNullableField(pair.field.name, mapper);
  },
  emit: (operands, q, mapper, operator, negated) => {
    const { field, literal, method } = comparisonParts(
      operands,
      q,
      mapper,
      operator,
    );
    const guard = sameTypeAs(literal, q, field);
    // Ordering against null, a list or a map is an error for every document: false either way.
    if (guard === undefined) return q.eq(true, false);
    const compared = q[method](field, literal);
    return q.and(guard, negated ? q.not(compared) : compared);
  },
};

const PUSHDOWN_RULES: Record<string, PushdownRule> = {
  // Negation is pushed inward (De Morgan) rather than wrapped around the built predicate, so each
  // leaf sees its own polarity. That is sound under CEL's three-valued logic: `!(a && b)` is true
  // exactly when `!a` or `!b` is true, with an error in either counting as not true.
  and: {
    accepts: allPushable,
    emit: (operands, q, mapper, _operator, negated) =>
      junction(operands, q, mapper, negated, negated ? "or" : "and"),
  },
  or: {
    accepts: allPushable,
    emit: (operands, q, mapper, _operator, negated) =>
      junction(operands, q, mapper, negated, negated ? "and" : "or"),
  },
  not: {
    accepts: allPushable,
    emit: (operands, q, mapper, _operator, negated) =>
      emitExpression(
        operandAt(operands, 0, "not operator requires at least one operand"),
        q,
        mapper,
        !negated,
      ),
  },
  eq: equalityRule,
  ne: equalityRule,
  lt: orderingRule,
  le: orderingRule,
  gt: orderingRule,
  ge: orderingRule,
  in: {
    accepts: (operands, mapper) => {
      const membership = fieldInList(operands);
      return (
        membership !== undefined &&
        !isNullableField(membership.field.name, mapper)
      );
    },
    emit: (operands, q, mapper, _operator, negated) => {
      const membership = fieldInList(operands);
      if (!membership) {
        throw new UnsupportedQueryPlanError(
          "in operator requires one field and one array value",
        );
      }
      const field = resolveField(membership.field.name, mapper);
      const { values } = membership;
      const built =
        values.length === 0
          ? q.eq(true, false)
          : values.length === 1
            ? q.eq(q.field(field), values[0])
            : q.or(...values.map((v: unknown) => q.eq(q.field(field), v)));
      return negated ? q.not(built) : built;
    },
  },
};

const ruleFor = (operator: string): PushdownRule | undefined =>
  Object.prototype.hasOwnProperty.call(PUSHDOWN_RULES, operator)
    ? PUSHDOWN_RULES[operator]
    : undefined;

/** Whether Convex's filter engine answers `expression` exactly as CEL does. */
export const canPushToDb = (
  expression: PlanExpressionOperand,
  mapper: Mapper,
): boolean => {
  if (isValue(expression)) return true;
  // A BARE variable in boolean position is subject to the same nullability rule as one under a
  // comparison. It was exempt, which is only invisible while every bare boolean is a required
  // field: `R.attr.parent.aBool` is absent for a row with no parent, and Convex's engine cannot
  // tell an absent path from a false one, so the negation readmits every parentless row
  // (cerbos/query-plan-adapters#375). Keeping it off the engine hands it to the adapter's own CEL
  // evaluator, which has the missing-attribute error the semantics need.
  if (isVariable(expression)) return !isNullableField(expression.name, mapper);
  if (!isExpression(expression)) return false;
  const rule = ruleFor(expression.operator);
  return rule !== undefined && rule.accepts(expression.operands, mapper);
};

/** The builder calls for an expression `canPushToDb` accepted. */
export const translateExpression = (
  expression: PlanExpressionOperand,
  q: FilterQ,
  mapper: Mapper,
): unknown => emitExpression(expression, q, mapper, false);

const emitExpression = (
  expression: PlanExpressionOperand,
  q: FilterQ,
  mapper: Mapper,
  negated: boolean,
): unknown => {
  const polarised = (built: unknown) => (negated ? q.not(built) : built);
  if (isValue(expression)) {
    if (typeof expression.value === "boolean") {
      return polarised(q.eq(true, expression.value));
    }
    throw new UnsupportedQueryPlanError("Unexpected bare value in expression");
  }
  if (isVariable(expression)) {
    return polarised(
      q.eq(q.field(resolveField(expression.name, mapper)), true),
    );
  }
  if (!isExpression(expression)) {
    throw new UnsupportedQueryPlanError("Invalid Cerbos expression structure");
  }
  const rule = ruleFor(expression.operator);
  if (!rule) {
    throw new UnsupportedQueryPlanError(
      `Unsupported operator: ${expression.operator}`,
    );
  }
  return rule.emit(
    expression.operands,
    q,
    mapper,
    expression.operator,
    negated,
  );
};

/** `and`/`or` over operands that each carry the enclosing polarity. */
const junction = (
  operands: PlanExpressionOperand[],
  q: FilterQ,
  mapper: Mapper,
  negated: boolean,
  combine: "and" | "or",
): unknown => {
  if (operands.length === 0) return q.eq(true, combine === "and");
  const emitted = operands.map((op) => emitExpression(op, q, mapper, negated));
  if (emitted.length === 1) return emitted[0];
  return q[combine](...emitted);
};
