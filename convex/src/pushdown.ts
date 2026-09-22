import type {
  PlanExpressionOperand,
  PlanExpressionValue,
  PlanExpressionVariable,
} from "@cerbos/core";

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
  emit: (
    operands: PlanExpressionOperand[],
    q: FilterQ,
    mapper: Mapper,
    operator: string,
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

const comparisonRule: PushdownRule = {
  accepts: (operands, mapper) => {
    const pair = fieldAndLiteral(operands);
    return pair !== undefined && !isNullableField(pair.field.name, mapper);
  },
  emit: (operands, q, mapper, operator) => {
    const pair = fieldAndLiteral(operands);
    if (!pair) {
      throw new Error(
        `${operator} operator requires one field and one value operand`,
      );
    }
    const cel = operator as ComparisonOperator;
    const method =
      CONVEX_COMPARISON[pair.swapped ? MIRRORED_COMPARISON[cel] : cel];
    return q[method](
      q.field(resolveField(pair.field.name, mapper)),
      pair.literal.value,
    );
  },
};

const PUSHDOWN_RULES: Record<string, PushdownRule> = {
  and: {
    accepts: allPushable,
    emit: (operands, q, mapper) => {
      if (operands.length === 0) return q.eq(true, true);
      if (operands.length === 1)
        return translateExpression(operands[0]!, q, mapper);
      return q.and(...operands.map((op) => translateExpression(op, q, mapper)));
    },
  },
  or: {
    accepts: allPushable,
    emit: (operands, q, mapper) => {
      if (operands.length === 0) return q.eq(true, false);
      if (operands.length === 1)
        return translateExpression(operands[0]!, q, mapper);
      return q.or(...operands.map((op) => translateExpression(op, q, mapper)));
    },
  },
  not: {
    accepts: allPushable,
    emit: (operands, q, mapper) =>
      q.not(
        translateExpression(
          operandAt(operands, 0, "not operator requires at least one operand"),
          q,
          mapper,
        ),
      ),
  },
  eq: comparisonRule,
  ne: comparisonRule,
  lt: comparisonRule,
  le: comparisonRule,
  gt: comparisonRule,
  ge: comparisonRule,
  in: {
    accepts: (operands, mapper) => {
      const membership = fieldInList(operands);
      return (
        membership !== undefined &&
        !isNullableField(membership.field.name, mapper)
      );
    },
    emit: (operands, q, mapper) => {
      const membership = fieldInList(operands);
      if (!membership) {
        throw new Error("in operator requires one field and one array value");
      }
      const field = resolveField(membership.field.name, mapper);
      const { values } = membership;
      if (values.length === 0) return q.eq(true, false);
      if (values.length === 1) return q.eq(q.field(field), values[0]);
      return q.or(...values.map((v: unknown) => q.eq(q.field(field), v)));
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
): unknown => {
  if (isValue(expression)) {
    if (typeof expression.value === "boolean") {
      return q.eq(true, expression.value);
    }
    throw new Error("Unexpected bare value in expression");
  }
  if (isVariable(expression)) {
    return q.eq(q.field(resolveField(expression.name, mapper)), true);
  }
  if (!isExpression(expression)) {
    throw new Error("Invalid Cerbos expression structure");
  }
  const rule = ruleFor(expression.operator);
  if (!rule) {
    throw new Error(`Unsupported operator: ${expression.operator}`);
  }
  return rule.emit(expression.operands, q, mapper, expression.operator);
};
