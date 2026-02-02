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
  allowPostFilter?: boolean;
}

export interface QueryPlanToConvexResult<Q = unknown> {
  kind: PlanKind;
  filter?: ConvexFilter<Q>;
  postFilter?: (doc: Record<string, unknown>) => boolean;
}

const DB_PUSHABLE_OPERATORS = new Set([
  "and", "or", "not", "eq", "ne", "lt", "le", "gt", "ge", "in", "isSet",
]);

const ALL_KNOWN_OPERATORS = new Set([
  ...DB_PUSHABLE_OPERATORS,
  "contains", "startsWith", "endsWith",
  "hasIntersection", "exists", "exists_one", "all",
  "filter", "map", "lambda",
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

const canPushToDb = (expression: PlanExpressionOperand): boolean => {
  if (isValue(expression) || isVariable(expression)) return true;
  if (!isExpression(expression)) return false;
  if (!DB_PUSHABLE_OPERATORS.has(expression.operator)) return false;
  return expression.operands.every(canPushToDb);
};

const validateStructure = (expression: PlanExpressionOperand): void => {
  if (isValue(expression) || isVariable(expression)) return;
  if (!isExpression(expression)) {
    throw new Error("Invalid Cerbos expression structure");
  }
  if (!ALL_KNOWN_OPERATORS.has(expression.operator)) {
    throw new Error(`Unsupported operator: ${expression.operator}`);
  }
  for (const op of expression.operands) {
    validateStructure(op);
  }
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
        return q[convexOp](q.field(field), rightOperand.value);
      }

      if (isValue(leftOperand) && isVariable(rightOperand)) {
        const field = resolveField(rightOperand.name, mapper);
        return q[convexOp](q.field(field), leftOperand.value);
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
        return q.eq(q.field(field), values[0]);
      }

      return q.or(
        ...values.map((v: unknown) => q.eq(q.field(field), v)),
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

type Bindings = Record<string, unknown>;

const getNestedValue = (obj: unknown, path: string): unknown => {
  const parts = path.split(".");
  let current: unknown = obj;
  for (const part of parts) {
    if (current === null || current === undefined) return undefined;
    current = (current as Record<string, unknown>)[part];
  }
  return current;
};

const resolveOperandValue = (
  operand: PlanExpressionOperand,
  doc: Record<string, unknown>,
  mapper: Mapper,
  bindings: Bindings,
): unknown => {
  if (isValue(operand)) return operand.value;

  if (isVariable(operand)) {
    const name = operand.name;
    const dotIdx = name.indexOf(".");
    if (dotIdx !== -1) {
      const root = name.substring(0, dotIdx);
      if (root in bindings) {
        const rest = name.substring(dotIdx + 1);
        return getNestedValue(bindings[root], rest);
      }
    }
    if (name in bindings) return bindings[name];
    const field = resolveField(name, mapper);
    return getNestedValue(doc, field);
  }

  return evaluateExpression(operand, doc, mapper, bindings);
};

const evaluateExpression = (
  expression: PlanExpressionOperand,
  doc: Record<string, unknown>,
  mapper: Mapper,
  bindings: Bindings,
): unknown => {
  if (isValue(expression)) return expression.value;

  if (isVariable(expression)) {
    return resolveOperandValue(expression, doc, mapper, bindings);
  }

  if (!isExpression(expression)) {
    throw new Error("Invalid Cerbos expression structure");
  }

  const { operator, operands } = expression;

  const resolve = (op: PlanExpressionOperand) =>
    resolveOperandValue(op, doc, mapper, bindings);

  switch (operator) {
    case "and":
      return operands.every((op) => evaluateExpression(op, doc, mapper, bindings));

    case "or":
      return operands.some((op) => evaluateExpression(op, doc, mapper, bindings));

    case "not":
      return !evaluateExpression(operands[0]!, doc, mapper, bindings);

    case "eq":
      return resolve(operands[0]!) === resolve(operands[1]!);

    case "ne":
      return resolve(operands[0]!) !== resolve(operands[1]!);

    case "lt":
      return (resolve(operands[0]!) as number) < (resolve(operands[1]!) as number);

    case "le":
      return (resolve(operands[0]!) as number) <= (resolve(operands[1]!) as number);

    case "gt":
      return (resolve(operands[0]!) as number) > (resolve(operands[1]!) as number);

    case "ge":
      return (resolve(operands[0]!) as number) >= (resolve(operands[1]!) as number);

    case "in": {
      const needle = resolve(operands[0]!);
      const haystack = resolve(operands[1]!) as unknown[];
      return Array.isArray(haystack) && haystack.includes(needle);
    }

    case "isSet": {
      const fieldVal = resolve(operands[0]!);
      const expected = resolve(operands[1]!);
      return expected ? fieldVal !== undefined : fieldVal === undefined;
    }

    case "contains": {
      const str = resolve(operands[0]!) as string;
      const substr = resolve(operands[1]!) as string;
      return typeof str === "string" && str.includes(substr);
    }

    case "startsWith": {
      const str = resolve(operands[0]!) as string;
      const prefix = resolve(operands[1]!) as string;
      return typeof str === "string" && str.startsWith(prefix);
    }

    case "endsWith": {
      const str = resolve(operands[0]!) as string;
      const suffix = resolve(operands[1]!) as string;
      return typeof str === "string" && str.endsWith(suffix);
    }

    case "hasIntersection": {
      const a = resolve(operands[0]!) as unknown[];
      const b = resolve(operands[1]!) as unknown[];
      if (!Array.isArray(a) || !Array.isArray(b)) return false;
      return a.some((v) => b.includes(v));
    }

    case "exists":
    case "exists_one":
    case "all": {
      const collection = resolve(operands[0]!) as unknown[];
      if (!Array.isArray(collection)) return false;
      const lambdaExpr = operands[1]!;
      if (!isExpression(lambdaExpr) || lambdaExpr.operator !== "lambda") {
        throw new Error(`${operator} requires a lambda operand`);
      }
      const lambdaVar = lambdaExpr.operands[0]!;
      const lambdaBody = lambdaExpr.operands[1]!;
      if (!isVariable(lambdaVar)) {
        throw new Error("lambda first operand must be a variable");
      }
      const varName = lambdaVar.name;

      if (operator === "exists") {
        return collection.some((item) =>
          evaluateExpression(lambdaBody, doc, mapper, { ...bindings, [varName]: item }),
        );
      }
      if (operator === "exists_one") {
        return collection.filter((item) =>
          evaluateExpression(lambdaBody, doc, mapper, { ...bindings, [varName]: item }),
        ).length === 1;
      }
      return collection.every((item) =>
        evaluateExpression(lambdaBody, doc, mapper, { ...bindings, [varName]: item }),
      );
    }

    case "filter": {
      const collection = resolve(operands[0]!) as unknown[];
      if (!Array.isArray(collection)) return [];
      const lambdaExpr = operands[1]!;
      if (!isExpression(lambdaExpr) || lambdaExpr.operator !== "lambda") {
        throw new Error("filter requires a lambda operand");
      }
      const lambdaVar = lambdaExpr.operands[0]!;
      const lambdaBody = lambdaExpr.operands[1]!;
      if (!isVariable(lambdaVar)) {
        throw new Error("lambda first operand must be a variable");
      }
      const varName = lambdaVar.name;
      return collection.filter((item) =>
        evaluateExpression(lambdaBody, doc, mapper, { ...bindings, [varName]: item }),
      );
    }

    case "map": {
      const collection = resolve(operands[0]!) as unknown[];
      if (!Array.isArray(collection)) return [];
      const lambdaExpr = operands[1]!;
      if (!isExpression(lambdaExpr) || lambdaExpr.operator !== "lambda") {
        throw new Error("map requires a lambda operand");
      }
      const lambdaVar = lambdaExpr.operands[0]!;
      const lambdaBody = lambdaExpr.operands[1]!;
      if (!isVariable(lambdaVar)) {
        throw new Error("lambda first operand must be a variable");
      }
      const varName = lambdaVar.name;
      return collection.map((item) =>
        evaluateExpression(lambdaBody, doc, mapper, { ...bindings, [varName]: item }),
      );
    }

    case "lambda":
      throw new Error("lambda should not be evaluated directly");

    default:
      throw new Error(`Unsupported operator: ${operator}`);
  }
};

interface SplitResult {
  filter?: ConvexFilter<FilterQ>;
  postFilter?: (doc: Record<string, unknown>) => boolean;
}

const buildFilters = (
  expression: PlanExpressionOperand,
  mapper: Mapper,
): SplitResult => {
  validateStructure(expression);

  if (canPushToDb(expression)) {
    return {
      filter: (q: FilterQ) => translateExpression(expression, q, mapper),
    };
  }

  if (isExpression(expression) && expression.operator === "and" && expression.operands.length > 1) {
    const pushable: PlanExpressionOperand[] = [];
    const nonPushable: PlanExpressionOperand[] = [];

    for (const op of expression.operands) {
      if (canPushToDb(op)) {
        pushable.push(op);
      } else {
        nonPushable.push(op);
      }
    }

    if (pushable.length > 0 && nonPushable.length > 0) {
      const dbExpr: PlanExpressionOperand = pushable.length === 1
        ? pushable[0]!
        : { operator: "and", operands: pushable } as PlanExpression;

      const jsExpr: PlanExpressionOperand = nonPushable.length === 1
        ? nonPushable[0]!
        : { operator: "and", operands: nonPushable } as PlanExpression;

      return {
        filter: (q: FilterQ) => translateExpression(dbExpr, q, mapper),
        postFilter: (doc: Record<string, unknown>) =>
          Boolean(evaluateExpression(jsExpr, doc, mapper, {})),
      };
    }
  }

  return {
    postFilter: (doc: Record<string, unknown>) =>
      Boolean(evaluateExpression(expression, doc, mapper, {})),
  };
};

export function queryPlanToConvex<Q = unknown>({
  queryPlan,
  mapper = {},
  allowPostFilter = false,
}: QueryPlanToConvexArgs): QueryPlanToConvexResult<Q> {
  switch (queryPlan.kind) {
    case PlanKind.ALWAYS_ALLOWED:
      return { kind: PlanKind.ALWAYS_ALLOWED };
    case PlanKind.ALWAYS_DENIED:
      return { kind: PlanKind.ALWAYS_DENIED };
    case PlanKind.CONDITIONAL: {
      const { filter, postFilter } = buildFilters(queryPlan.condition, mapper);

      if (postFilter && !allowPostFilter) {
        throw new Error(
          "The query plan contains conditions that cannot be evaluated by Convex's " +
          "query engine and require client-side filtering (postFilter). This means " +
          "data will be fetched from the database before authorization filtering is " +
          "applied. Set { allowPostFilter: true } to opt in to this behavior.",
        );
      }

      const result: QueryPlanToConvexResult<Q> = { kind: PlanKind.CONDITIONAL };
      if (filter) result.filter = filter as ConvexFilter<Q>;
      if (postFilter) result.postFilter = postFilter;
      return result;
    }
    default:
      throw Error("Invalid query plan.");
  }
}
