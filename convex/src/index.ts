import {
  PlanResourcesResponse,
  PlanExpressionOperand,
  PlanExpression,
  PlanExpressionValue,
  PlanExpressionVariable,
  PlanKind,
} from "@cerbos/core";

export { PlanKind };

export type ConvexFilter<Q> = (q: Q) => unknown;

export type MapperConfig = {
  field?: string;
};

export type Mapper =
  | Record<string, MapperConfig>
  | ((key: string) => MapperConfig);

export interface QueryPlanToConvexArgs {
  queryPlan: PlanResourcesResponse;
  mapper?: Mapper;
}

export interface QueryPlanToConvexResult<Q = unknown> {
  kind: PlanKind;
  filter?: ConvexFilter<Q>;
}

const UNSUPPORTED_OPERATORS = new Set([
  "contains",
  "startsWith",
  "endsWith",
  "hasIntersection",
  "exists",
  "exists_one",
  "all",
  "filter",
  "map",
  "lambda",
]);

const isExpression = (e: PlanExpressionOperand): e is PlanExpression =>
  "operator" in e;
const isValue = (e: PlanExpressionOperand): e is PlanExpressionValue =>
  "value" in e;
const isVariable = (e: PlanExpressionOperand): e is PlanExpressionVariable =>
  "name" in e;

const resolveField = (reference: string, mapper: Mapper): string => {
  const config =
    typeof mapper === "function" ? mapper(reference) : mapper[reference];
  return config?.field ?? reference;
};

const getOperandAt = (
  operands: PlanExpressionOperand[],
  index: number,
  errorMessage: string,
): PlanExpressionOperand => {
  const operand = operands[index];
  if (!operand) {
    throw new Error(errorMessage);
  }
  return operand;
};

const findOperand = (
  operands: PlanExpressionOperand[],
  predicate: (operand: PlanExpressionOperand) => boolean,
  errorMessage: string,
): PlanExpressionOperand => {
  const operand = operands.find(predicate);
  if (!operand) {
    throw new Error(errorMessage);
  }
  return operand;
};

interface FilterQ {
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

const convexValue = (v: unknown): unknown => (v === null ? undefined : v);

const validateExpression = (
  expression: PlanExpressionOperand,
  mapper: Mapper,
): void => {
  if (isValue(expression)) return;
  if (isVariable(expression)) return;

  if (!isExpression(expression)) {
    throw new Error("Invalid Cerbos expression structure");
  }

  const { operator, operands } = expression;

  if (UNSUPPORTED_OPERATORS.has(operator)) {
    throw new Error(`Unsupported operator for Convex: ${operator}`);
  }

  const supported = new Set([
    "and", "or", "not", "eq", "ne", "lt", "le", "gt", "ge", "in", "isSet",
  ]);

  if (!supported.has(operator)) {
    throw new Error(`Unsupported operator: ${operator}`);
  }

  for (const op of operands) {
    validateExpression(op, mapper);
  }
};

const buildConvexFilter = (
  expression: PlanExpressionOperand,
  mapper: Mapper,
): ConvexFilter<FilterQ> => {
  validateExpression(expression, mapper);
  return (q: FilterQ) => translateExpression(expression, q, mapper);
};

const translateExpression = (
  expression: PlanExpressionOperand,
  q: FilterQ,
  mapper: Mapper,
): unknown => {
  if (isValue(expression)) {
    if (typeof expression.value === "boolean") {
      return expression.value ? q.eq(true, true) : q.eq(true, false);
    }
    throw new Error("Unexpected bare value in expression");
  }

  if (isVariable(expression)) {
    const field = resolveField(expression.name, mapper);
    return q.eq(q.field(field), true);
  }

  if (!isExpression(expression)) {
    throw new Error("Invalid Cerbos expression structure");
  }

  const { operator, operands } = expression;

  const requireOperandAt = (index: number, message: string) =>
    getOperandAt(operands, index, message);

  const requireOperandMatching = (
    predicate: (operand: PlanExpressionOperand) => boolean,
    message: string,
  ) => findOperand(operands, predicate, message);

  switch (operator) {
    case "and": {
      if (operands.length === 0) return q.eq(true, true);
      if (operands.length === 1)
        return translateExpression(operands[0]!, q, mapper);
      return q.and(
        ...operands.map((op) => translateExpression(op, q, mapper)),
      );
    }

    case "or": {
      if (operands.length === 0) return q.eq(true, false);
      if (operands.length === 1)
        return translateExpression(operands[0]!, q, mapper);
      return q.or(
        ...operands.map((op) => translateExpression(op, q, mapper)),
      );
    }

    case "not": {
      const operand = requireOperandAt(
        0,
        "not operator requires at least one operand",
      );
      return q.not(translateExpression(operand, q, mapper));
    }

    case "eq":
    case "ne":
    case "lt":
    case "le":
    case "gt":
    case "ge": {
      const convexOp = {
        eq: "eq",
        ne: "neq",
        lt: "lt",
        le: "lte",
        gt: "gt",
        ge: "gte",
      }[operator] as keyof Pick<FilterQ, "eq" | "neq" | "lt" | "lte" | "gt" | "gte">;

      const leftOperand = requireOperandMatching(
        (o) => isVariable(o) || isExpression(o),
        `${operator} operator requires a field operand`,
      );
      const rightOperand = requireOperandMatching(
        (o) => o !== leftOperand,
        `${operator} operator requires a value operand`,
      );

      if (isVariable(leftOperand) && isValue(rightOperand)) {
        const field = resolveField(leftOperand.name, mapper);
        return q[convexOp](q.field(field), convexValue(rightOperand.value));
      }

      if (isValue(leftOperand) && isVariable(rightOperand)) {
        const field = resolveField(rightOperand.name, mapper);
        return q[convexOp](q.field(field), convexValue(leftOperand.value));
      }

      throw new Error(
        `${operator} operator requires one field and one value operand`,
      );
    }

    case "in": {
      const fieldOperand = requireOperandMatching(
        (o) => isVariable(o),
        "in operator requires a field operand",
      );
      const valueOperand = requireOperandMatching(
        (o) => isValue(o),
        "in operator requires a value operand",
      );

      if (!isVariable(fieldOperand) || !isValue(valueOperand)) {
        throw new Error("in operator requires one field and one array value");
      }

      const field = resolveField(fieldOperand.name, mapper);
      const values = valueOperand.value;

      if (!Array.isArray(values)) {
        throw new Error("in operator requires an array value");
      }

      if (values.length === 0) {
        return q.eq(true, false);
      }

      if (values.length === 1) {
        return q.eq(q.field(field), convexValue(values[0]));
      }

      return q.or(
        ...values.map((v: unknown) => q.eq(q.field(field), convexValue(v))),
      );
    }

    case "isSet": {
      const fieldOperand = requireOperandMatching(
        (o) => isVariable(o),
        "isSet operator requires a field operand",
      );
      const valueOperand = requireOperandMatching(
        (o) => isValue(o),
        "isSet operator requires a boolean operand",
      );

      if (!isVariable(fieldOperand) || !isValue(valueOperand)) {
        throw new Error("isSet operator requires one field and one boolean");
      }

      const field = resolveField(fieldOperand.name, mapper);

      if (valueOperand.value) {
        return q.neq(q.field(field), undefined);
      }
      return q.eq(q.field(field), undefined);
    }

    default:
      throw new Error(`Unsupported operator: ${operator}`);
  }
};

export function queryPlanToConvex<Q = unknown>({
  queryPlan,
  mapper = {},
}: QueryPlanToConvexArgs): QueryPlanToConvexResult<Q> {
  switch (queryPlan.kind) {
    case PlanKind.ALWAYS_ALLOWED:
      return { kind: PlanKind.ALWAYS_ALLOWED };
    case PlanKind.ALWAYS_DENIED:
      return { kind: PlanKind.ALWAYS_DENIED };
    case PlanKind.CONDITIONAL:
      return {
        kind: PlanKind.CONDITIONAL,
        filter: buildConvexFilter(
          queryPlan.condition,
          mapper,
        ) as ConvexFilter<Q>,
      };
    default:
      throw Error("Invalid query plan.");
  }
}
