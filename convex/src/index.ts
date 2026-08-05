import {
  PlanResourcesResponse,
  PlanExpressionOperand,
  PlanExpression,
  PlanExpressionValue,
  PlanExpressionVariable,
  PlanKind,
} from "@cerbos/core";

export { PlanKind };

export type ConvexFilter<Q, R = unknown> = (q: Q) => R;

export type MapperConfig = {
  field?: string;
  nullable?: boolean;
};

export type Mapper =
  | Record<string, MapperConfig>
  | ((key: string) => MapperConfig);

/**
 * How the caller represents a NULL field when building the attributes it sends to `check()`.
 *
 * The planner emits the same `eq(attr, null)` node either way, so the plan cannot reveal which
 * convention is in use and the adapter has to be told.
 *
 * - `"explicit"` (default) — a NULL field is sent as an explicit `null` attribute. CEL compares
 *   `null == null`, so matching null selects exactly the documents `check()` allows.
 * - `"omitted"` — a NULL field sends no attribute at all. CEL then raises a missing-attribute
 *   error, which Cerbos treats as a deny, so a filter that *selects* null documents returns
 *   documents the PDP denies. Null comparison operands are rejected instead of translated.
 *
 * See https://github.com/cerbos/query-plan-adapters/issues/302.
 */
export type NullAttributeRepresentation = "explicit" | "omitted";

export interface QueryPlanToConvexArgs {
  queryPlan: PlanResourcesResponse;
  mapper?: Mapper;
  allowPostFilter?: boolean;
  /**
   * Which NULL-field representation the caller uses when building `check()` attributes.
   * Defaults to `"explicit"`, preserving the historical null-matching translation.
   */
  nullAttributeRepresentation?: NullAttributeRepresentation;
}

export interface QueryPlanToConvexResult<Q = unknown, R = unknown> {
  kind: PlanKind;
  filter?: ConvexFilter<Q, R>;
  postFilter?: (doc: Record<string, unknown>) => boolean;
}

const DB_PUSHABLE_OPERATORS = new Set([
  "and", "or", "not", "eq", "ne", "lt", "le", "gt", "ge", "in",
]);

const ALL_KNOWN_OPERATORS = new Set([
  ...DB_PUSHABLE_OPERATORS,
  "contains", "startsWith", "endsWith",
  "hasIntersection", "exists", "exists_one", "all",
  "filter", "map", "lambda",
  "add", "sub", "mult", "div", "mod",
  "matches", "index", "size",
  "string", "double", "int",
  "if", "get-field", "timestamp",
  "hierarchy", "ancestorOf", "descendentOf", "overlaps",
]);

const isExpression = (e: PlanExpressionOperand): e is PlanExpression =>
  "operator" in e;
const isValue = (e: PlanExpressionOperand): e is PlanExpressionValue =>
  "value" in e;
const isVariable = (e: PlanExpressionOperand): e is PlanExpressionVariable =>
  "name" in e;

const looksLikeLambdaVariable = (
  operand: PlanExpressionOperand,
): operand is PlanExpressionVariable =>
  isVariable(operand) && !operand.name.includes(".");

const extractLambdaComponents = (
  operand: PlanExpressionOperand,
): { body: PlanExpressionOperand; variable: PlanExpressionVariable } => {
  if (!isExpression(operand) || operand.operator !== "lambda") {
    throw new Error("Expected a lambda operand");
  }
  if (operand.operands.length !== 2) {
    throw new Error("Lambda requires exactly two operands");
  }

  const first = getOperandAt(operand.operands, 0, "Lambda body is required");
  const second = getOperandAt(
    operand.operands,
    1,
    "Lambda variable is required",
  );

  if (looksLikeLambdaVariable(second)) {
    return { body: first, variable: second };
  }
  if (looksLikeLambdaVariable(first)) {
    return { body: second, variable: first };
  }
  throw new Error("Lambda requires a variable operand");
};

const resolveField = (reference: string, mapper: Mapper): string => {
  const config =
    typeof mapper === "function" ? mapper(reference) : mapper[reference];
  return config?.field ?? reference;
};

const isNullableField = (reference: string, mapper: Mapper): boolean => {
  const config =
    typeof mapper === "function" ? mapper(reference) : mapper[reference];
  return config?.nullable ?? false;
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

type ComparisonOperator = "eq" | "ne" | "lt" | "le" | "gt" | "ge";

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

const mirrorComparison = (operator: ComparisonOperator): ComparisonOperator => {
  switch (operator) {
    case "lt":
      return "gt";
    case "le":
      return "ge";
    case "gt":
      return "lt";
    case "ge":
      return "le";
    case "eq":
    case "ne":
      return operator;
  }
};

const applyComparison = (
  q: FilterQ,
  operator: ComparisonOperator,
  left: unknown,
  right: unknown,
): unknown => {
  switch (operator) {
    case "eq":
      return q.eq(left, right);
    case "ne":
      return q.neq(left, right);
    case "lt":
      return q.lt(left, right);
    case "le":
      return q.lte(left, right);
    case "gt":
      return q.gt(left, right);
    case "ge":
      return q.gte(left, right);
  }
};

const canPushToDb = (
  expression: PlanExpressionOperand,
  mapper: Mapper,
): boolean => {
  if (isValue(expression) || isVariable(expression)) return true;
  if (!isExpression(expression)) return false;
  if (!DB_PUSHABLE_OPERATORS.has(expression.operator)) return false;

  switch (expression.operator) {
    case "eq":
    case "ne":
    case "lt":
    case "le":
    case "gt":
    case "ge": {
      const [left, right] = expression.operands;
      if (!left || !right) return false;
      const variable = isVariable(left)
        ? left
        : isVariable(right)
          ? right
          : undefined;
      const hasOneLiteral =
        (isVariable(left) && isValue(right)) ||
        (isValue(left) && isVariable(right));
      return Boolean(
        hasOneLiteral &&
          variable &&
          !isNullableField(variable.name, mapper),
      );
    }
    case "in": {
      const [needle, haystack] = expression.operands;
      if (!needle || !haystack) return false;
      return Boolean(
        isVariable(needle) &&
          isValue(haystack) &&
          Array.isArray(haystack.value) &&
          !isNullableField(needle.name, mapper),
      );
    }
    default:
      return expression.operands.every((operand) =>
        canPushToDb(operand, mapper),
      );
  }
};

const validateStructure = (expression: PlanExpressionOperand): void => {
  if (isValue(expression) || isVariable(expression)) return;
  if (!isExpression(expression)) {
    throw new Error("Invalid Cerbos expression structure");
  }
  if (!ALL_KNOWN_OPERATORS.has(expression.operator)) {
    throw new Error(`Unsupported operator: ${expression.operator}`);
  }
  if (expression.operator === "matches") {
    const pattern = expression.operands[1];
    if (
      !pattern ||
      !isValue(pattern) ||
      typeof pattern.value !== "string" ||
      !parseSafeRegexPattern(pattern.value)
    ) {
      throw new Error(
        "matches requires a constant RE2-compatible pattern in the supported " +
        "literal, anchor, and trailing .* subset",
      );
    }
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
      const leftOperand = requireOperandAt(
        0,
        `${operator} operator requires a left operand`,
      );
      const rightOperand = requireOperandAt(
        1,
        `${operator} operator requires a right operand`,
      );

      if (isVariable(leftOperand) && isValue(rightOperand)) {
        const field = resolveField(leftOperand.name, mapper);
        return applyComparison(q, operator, q.field(field), rightOperand.value);
      }

      if (isValue(leftOperand) && isVariable(rightOperand)) {
        const field = resolveField(rightOperand.name, mapper);
        return applyComparison(
          q,
          mirrorComparison(operator),
          q.field(field),
          leftOperand.value,
        );
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

    default:
      throw new Error(`Unsupported operator: ${operator}`);
  }
};

type Bindings = Record<string, unknown>;

const EVALUATION_ERROR = Symbol("Cerbos evaluation error");

interface SafeRegexPattern {
  literal: string;
  anchoredStart: boolean;
  anchoredEnd: boolean;
  trailingWildcard: boolean;
}

const SAFE_REGEX_PATTERN = /^(\^)?([A-Za-z0-9 _:/-]+?)(\.\*)?(\$)?$/;

const parseSafeRegexPattern = (pattern: string): SafeRegexPattern | undefined => {
  const match = SAFE_REGEX_PATTERN.exec(pattern);
  const literal = match?.[2];
  if (!literal) return undefined;
  if (match[3] === ".*" && match[4] === "$") return undefined;
  return {
    literal,
    anchoredStart: match[1] === "^",
    trailingWildcard: match[3] === ".*",
    anchoredEnd: match[4] === "$",
  };
};

const matchesSafeRegexPattern = (
  receiver: string,
  pattern: SafeRegexPattern,
): boolean => {
  if (pattern.anchoredStart && pattern.anchoredEnd) {
    return receiver === pattern.literal;
  }
  if (pattern.anchoredStart) return receiver.startsWith(pattern.literal);
  if (pattern.anchoredEnd) return receiver.endsWith(pattern.literal);
  return receiver.includes(pattern.literal);
};

const isEvaluationError = (
  value: unknown,
): value is typeof EVALUATION_ERROR => value === EVALUATION_ERROR;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const getNestedValue = (obj: unknown, path: string): unknown => {
  const parts = path.split(".");
  let current: unknown = obj;
  for (const part of parts) {
    if (
      !isRecord(current) ||
      !Object.prototype.hasOwnProperty.call(current, part)
    ) {
      return EVALUATION_ERROR;
    }
    current = current[part];
  }
  return current;
};

const valuesEqual = (left: unknown, right: unknown): boolean => {
  if (typeof left === "number" && typeof right === "number") {
    return !Number.isNaN(left) && !Number.isNaN(right) && left === right;
  }
  if (Object.is(left, right)) return true;
  if (Array.isArray(left) && Array.isArray(right)) {
    return (
      left.length === right.length &&
      left.every((value, index) => valuesEqual(value, right[index]))
    );
  }
  if (isRecord(left) && isRecord(right)) {
    const leftKeys = Object.keys(left).sort();
    const rightKeys = Object.keys(right).sort();
    return (
      leftKeys.length === rightKeys.length &&
      leftKeys.every(
        (key, index) =>
          key === rightKeys[index] && valuesEqual(left[key], right[key]),
      )
    );
  }
  return false;
};

interface HierarchyValue {
  value: string;
  delimiter: string;
}

const isHierarchyValue = (value: unknown): value is HierarchyValue =>
  isRecord(value) &&
  typeof value["value"] === "string" &&
  typeof value["delimiter"] === "string";

const isStrictAncestor = (
  ancestor: HierarchyValue,
  descendent: HierarchyValue,
): boolean =>
  ancestor.delimiter === descendent.delimiter &&
  ancestor.value !== descendent.value &&
  descendent.value.startsWith(ancestor.value + ancestor.delimiter);

const RFC3339_TIMESTAMP =
  /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|([+-])(\d{2}):(\d{2}))$/;

const MIN_TIMESTAMP_NANOS = -62135596800000000000n;
const MAX_TIMESTAMP_NANOS = 253402300799999999999n;

const isLeapYear = (year: number): boolean =>
  year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);

const daysInMonth = (year: number, month: number): number => {
  const days = [31, isLeapYear(year) ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  return days[month - 1] ?? 0;
};

const parseRfc3339Timestamp = (
  value: string,
): bigint | typeof EVALUATION_ERROR => {
  const match = RFC3339_TIMESTAMP.exec(value);
  if (!match) return EVALUATION_ERROR;

  const [, yearPart, monthPart, dayPart, hourPart, minutePart, secondPart,
    fractionPart, zonePart, offsetSign, offsetHourPart, offsetMinutePart] = match;
  if (
    !yearPart ||
    !monthPart ||
    !dayPart ||
    !hourPart ||
    !minutePart ||
    !secondPart ||
    !zonePart
  ) {
    return EVALUATION_ERROR;
  }

  const year = Number(yearPart);
  const month = Number(monthPart);
  const day = Number(dayPart);
  const hour = Number(hourPart);
  const minute = Number(minutePart);
  const second = Number(secondPart);
  if (
    year < 1 ||
    year > 9999 ||
    month < 1 ||
    month > 12 ||
    day < 1 ||
    day > daysInMonth(year, month) ||
    hour > 23 ||
    minute > 59 ||
    second > 59
  ) {
    return EVALUATION_ERROR;
  }

  let offsetMinutes = 0;
  if (zonePart !== "Z") {
    if (!offsetSign || !offsetHourPart || !offsetMinutePart) {
      return EVALUATION_ERROR;
    }
    const offsetHour = Number(offsetHourPart);
    const offsetMinute = Number(offsetMinutePart);
    if (offsetHour > 23 || offsetMinute > 59) return EVALUATION_ERROR;
    offsetMinutes = (offsetHour * 60 + offsetMinute) *
      (offsetSign === "+" ? 1 : -1);
  }

  const instant = new Date(0);
  instant.setUTCFullYear(year, month - 1, day);
  instant.setUTCHours(hour, minute, second, 0);
  const epochMillis = instant.getTime() - offsetMinutes * 60_000;
  if (!Number.isFinite(epochMillis)) return EVALUATION_ERROR;

  const fractionNanos = BigInt((fractionPart ?? "").padEnd(9, "0") || "0");
  const timestampNanos = BigInt(epochMillis) * 1_000_000n + fractionNanos;
  return timestampNanos < MIN_TIMESTAMP_NANOS ||
    timestampNanos > MAX_TIMESTAMP_NANOS
    ? EVALUATION_ERROR
    : timestampNanos;
};

const CEL_DOUBLE_STRING =
  /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/;
const CEL_INT_STRING = /^-?\d+$/;

const convertToString = (
  value: unknown,
): string | typeof EVALUATION_ERROR => {
  if (typeof value === "string") return value;
  if (
    typeof value === "number" &&
    Number.isSafeInteger(value) &&
    !Object.is(value, -0)
  ) {
    return String(value);
  }
  return EVALUATION_ERROR;
};

const convertToDouble = (
  value: unknown,
): number | typeof EVALUATION_ERROR => {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : EVALUATION_ERROR;
  }
  if (typeof value !== "string" || !CEL_DOUBLE_STRING.test(value)) {
    return EVALUATION_ERROR;
  }
  const converted = Number(value);
  return Number.isFinite(converted) ? converted : EVALUATION_ERROR;
};

const convertToInt = (
  value: unknown,
): number | typeof EVALUATION_ERROR => {
  if (typeof value === "number") {
    if (!Number.isFinite(value)) return EVALUATION_ERROR;
    const converted = Math.trunc(value);
    return Number.isSafeInteger(converted) ? converted : EVALUATION_ERROR;
  }
  if (typeof value !== "string" || !CEL_INT_STRING.test(value)) {
    return EVALUATION_ERROR;
  }
  const converted = Number(value);
  return Number.isSafeInteger(converted) ? converted : EVALUATION_ERROR;
};

const asBoolean = (value: unknown): boolean | typeof EVALUATION_ERROR =>
  typeof value === "boolean" ? value : EVALUATION_ERROR;

const compareValues = (
  operator: ComparisonOperator,
  left: unknown,
  right: unknown,
): boolean | typeof EVALUATION_ERROR => {
  if (isEvaluationError(left) || isEvaluationError(right)) {
    return EVALUATION_ERROR;
  }
  if (operator === "eq" || operator === "ne") {
    const equal = valuesEqual(left, right);
    return operator === "eq" ? equal : !equal;
  }
  if (
    (typeof left !== "number" || typeof right !== "number") &&
    (typeof left !== "string" || typeof right !== "string") &&
    (typeof left !== "bigint" || typeof right !== "bigint")
  ) {
    return EVALUATION_ERROR;
  }
  switch (operator) {
    case "lt":
      return left < right;
    case "le":
      return left <= right;
    case "gt":
      return left > right;
    case "ge":
      return left >= right;
  }
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
    case "and": {
      let sawError = false;
      for (const operand of operands) {
        const value = asBoolean(
          evaluateExpression(operand, doc, mapper, bindings),
        );
        if (value === false) return false;
        if (isEvaluationError(value)) sawError = true;
      }
      return sawError ? EVALUATION_ERROR : true;
    }

    case "or": {
      let sawError = false;
      for (const operand of operands) {
        const value = asBoolean(
          evaluateExpression(operand, doc, mapper, bindings),
        );
        if (value === true) return true;
        if (isEvaluationError(value)) sawError = true;
      }
      return sawError ? EVALUATION_ERROR : false;
    }

    case "not": {
      const value = asBoolean(resolve(getOperandAt(operands, 0, "not operand")));
      return isEvaluationError(value) ? value : !value;
    }

    case "eq":
    case "ne":
    case "lt":
    case "le":
    case "gt":
    case "ge":
      return compareValues(
        operator,
        resolve(getOperandAt(operands, 0, `${operator} left operand`)),
        resolve(getOperandAt(operands, 1, `${operator} right operand`)),
      );

    case "in": {
      const needle = resolve(getOperandAt(operands, 0, "in needle"));
      const haystack = resolve(getOperandAt(operands, 1, "in haystack"));
      if (isEvaluationError(needle) || isEvaluationError(haystack)) {
        return EVALUATION_ERROR;
      }
      if (!Array.isArray(haystack)) return EVALUATION_ERROR;
      return haystack.some((value) => valuesEqual(value, needle));
    }

    case "contains": {
      const receiver = resolve(getOperandAt(operands, 0, "contains receiver"));
      const needle = resolve(getOperandAt(operands, 1, "contains needle"));
      return typeof receiver === "string" && typeof needle === "string"
        ? receiver.includes(needle)
        : EVALUATION_ERROR;
    }

    case "startsWith": {
      const receiver = resolve(getOperandAt(operands, 0, "startsWith receiver"));
      const prefix = resolve(getOperandAt(operands, 1, "startsWith prefix"));
      return typeof receiver === "string" && typeof prefix === "string"
        ? receiver.startsWith(prefix)
        : EVALUATION_ERROR;
    }

    case "endsWith": {
      const receiver = resolve(getOperandAt(operands, 0, "endsWith receiver"));
      const suffix = resolve(getOperandAt(operands, 1, "endsWith suffix"));
      return typeof receiver === "string" && typeof suffix === "string"
        ? receiver.endsWith(suffix)
        : EVALUATION_ERROR;
    }

    case "hasIntersection": {
      const left = resolve(getOperandAt(operands, 0, "hasIntersection left"));
      const right = resolve(getOperandAt(operands, 1, "hasIntersection right"));
      if (!Array.isArray(left) || !Array.isArray(right)) {
        return EVALUATION_ERROR;
      }
      return left.some((leftValue) =>
        right.some((rightValue) => valuesEqual(leftValue, rightValue)),
      );
    }

    case "exists":
    case "exists_one":
    case "all": {
      const collection = resolve(getOperandAt(operands, 0, `${operator} collection`));
      if (!Array.isArray(collection)) return EVALUATION_ERROR;
      const lambda = extractLambdaComponents(
        getOperandAt(operands, 1, `${operator} lambda`),
      );
      const varName = lambda.variable.name;
      let trueCount = 0;
      let sawError = false;
      for (const item of collection) {
        const value = asBoolean(
          evaluateExpression(lambda.body, doc, mapper, {
            ...bindings,
            [varName]: item,
          }),
        );
        if (value === true) {
          trueCount += 1;
          if (operator === "exists") return true;
        } else if (value === false && operator === "all") {
          return false;
        } else if (isEvaluationError(value)) {
          sawError = true;
        }
      }
      if (sawError) return EVALUATION_ERROR;
      if (operator === "exists") return false;
      if (operator === "exists_one") return trueCount === 1;
      return true;
    }

    case "filter": {
      const collection = resolve(getOperandAt(operands, 0, "filter collection"));
      if (!Array.isArray(collection)) return EVALUATION_ERROR;
      const lambda = extractLambdaComponents(
        getOperandAt(operands, 1, "filter lambda"),
      );
      const varName = lambda.variable.name;
      const filtered: unknown[] = [];
      for (const item of collection) {
        const value = asBoolean(
          evaluateExpression(lambda.body, doc, mapper, {
            ...bindings,
            [varName]: item,
          }),
        );
        if (isEvaluationError(value)) return EVALUATION_ERROR;
        if (value) filtered.push(item);
      }
      return filtered;
    }

    case "map": {
      const collection = resolve(getOperandAt(operands, 0, "map collection"));
      if (!Array.isArray(collection)) return EVALUATION_ERROR;
      const lambda = extractLambdaComponents(
        getOperandAt(operands, 1, "map lambda"),
      );
      const varName = lambda.variable.name;
      const mapped: unknown[] = [];
      for (const item of collection) {
        const value = evaluateExpression(lambda.body, doc, mapper, {
          ...bindings,
          [varName]: item,
        });
        if (isEvaluationError(value)) return EVALUATION_ERROR;
        mapped.push(value);
      }
      return mapped;
    }

    case "lambda":
      throw new Error("lambda should not be evaluated directly");

    case "add":
    case "sub":
    case "mult":
    case "div":
    case "mod": {
      const left = resolve(getOperandAt(operands, 0, `${operator} left`));
      const right = resolve(getOperandAt(operands, 1, `${operator} right`));
      if (typeof left !== "number" || typeof right !== "number") {
        return EVALUATION_ERROR;
      }
      switch (operator) {
        case "add":
          return left + right;
        case "sub":
          return left - right;
        case "mult":
          return left * right;
        case "div":
          return left / right;
        case "mod":
          return left % right;
      }
    }

    case "matches": {
      const receiver = resolve(getOperandAt(operands, 0, "matches receiver"));
      const pattern = resolve(getOperandAt(operands, 1, "matches pattern"));
      if (typeof receiver !== "string" || typeof pattern !== "string") {
        return EVALUATION_ERROR;
      }
      const safePattern = parseSafeRegexPattern(pattern);
      return safePattern
        ? matchesSafeRegexPattern(receiver, safePattern)
        : EVALUATION_ERROR;
    }

    case "index": {
      const collection = resolve(getOperandAt(operands, 0, "index collection"));
      const index = resolve(getOperandAt(operands, 1, "index value"));
      if (
        !Array.isArray(collection) ||
        typeof index !== "number" ||
        !Number.isInteger(index) ||
        index < 0 ||
        index >= collection.length
      ) {
        return EVALUATION_ERROR;
      }
      return collection[index];
    }

    case "get-field": {
      const target = resolve(getOperandAt(operands, 0, "get-field target"));
      const fieldOperand = getOperandAt(operands, 1, "get-field name");
      const field = isVariable(fieldOperand)
        ? fieldOperand.name
        : isValue(fieldOperand) && typeof fieldOperand.value === "string"
          ? fieldOperand.value
          : undefined;
      return field ? getNestedValue(target, field) : EVALUATION_ERROR;
    }

    case "size": {
      const value = resolve(getOperandAt(operands, 0, "size operand"));
      if (typeof value === "string") return Array.from(value).length;
      if (Array.isArray(value)) return value.length;
      if (isRecord(value)) return Object.keys(value).length;
      return EVALUATION_ERROR;
    }

    case "string": {
      const value = resolve(getOperandAt(operands, 0, "string operand"));
      return isEvaluationError(value) ? value : convertToString(value);
    }

    case "double": {
      const value = resolve(getOperandAt(operands, 0, "double operand"));
      return isEvaluationError(value) ? value : convertToDouble(value);
    }

    case "int": {
      const value = resolve(getOperandAt(operands, 0, "int operand"));
      return isEvaluationError(value) ? value : convertToInt(value);
    }

    case "if": {
      const condition = asBoolean(resolve(getOperandAt(operands, 0, "if condition")));
      if (isEvaluationError(condition)) return condition;
      return resolve(
        getOperandAt(operands, condition ? 1 : 2, "if selected branch"),
      );
    }

    case "timestamp": {
      const value = resolve(getOperandAt(operands, 0, "timestamp operand"));
      if (typeof value !== "string") return EVALUATION_ERROR;
      return parseRfc3339Timestamp(value);
    }

    case "hierarchy": {
      const value = resolve(getOperandAt(operands, 0, "hierarchy value"));
      const delimiterOperand = operands[1];
      const delimiter = delimiterOperand ? resolve(delimiterOperand) : ".";
      return typeof value === "string" && typeof delimiter === "string"
        ? { value, delimiter }
        : EVALUATION_ERROR;
    }

    case "ancestorOf":
    case "descendentOf":
    case "overlaps": {
      const left = resolve(getOperandAt(operands, 0, `${operator} left`));
      const right = resolve(getOperandAt(operands, 1, `${operator} right`));
      if (!isHierarchyValue(left) || !isHierarchyValue(right)) {
        return EVALUATION_ERROR;
      }
      if (operator === "ancestorOf") return isStrictAncestor(left, right);
      if (operator === "descendentOf") return isStrictAncestor(right, left);
      return (
        valuesEqual(left, right) ||
        isStrictAncestor(left, right) ||
        isStrictAncestor(right, left)
      );
    }

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

  if (canPushToDb(expression, mapper)) {
    return {
      filter: (q: FilterQ) => translateExpression(expression, q, mapper),
    };
  }

  if (isExpression(expression) && expression.operator === "and" && expression.operands.length > 1) {
    const pushable: PlanExpressionOperand[] = [];
    const nonPushable: PlanExpressionOperand[] = [];

    for (const op of expression.operands) {
      if (canPushToDb(op, mapper)) {
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
          evaluateExpression(jsExpr, doc, mapper, {}) === true,
      };
    }
  }

  return {
    postFilter: (doc: Record<string, unknown>) =>
      evaluateExpression(expression, doc, mapper, {}) === true,
  };
};

/**
 * Rejects every null literal operand in the plan when the caller omits attributes for NULL
 * fields.
 *
 * Under the `"omitted"` representation a NULL field carries no attribute, so CEL raises a
 * missing-attribute error and `check()` denies the document; matching null would return exactly
 * the documents the PDP refuses. Convex translates a plan down two paths — a pushed-down
 * `q.eq(...)` filter and an in-memory `postFilter` — so the check runs once over the plan tree
 * rather than at each emission site.
 *
 * The scan matches on the OPERAND, never on an allowlist of operators. A null constant reaches a
 * null-selecting predicate through more shapes than the obvious `eq`/`ne`/`in` — `hasIntersection`
 * carries one in its value list too — and any operator added later would silently escape a list
 * that has to be maintained by hand.
 *
 * The rejection is also deliberately wider than the over-granting shapes: `ne(x, null)` on its own
 * is aligned, but negation is applied around the built predicate, so a leaf cannot tell whether an
 * enclosing `not` will flip a not-null predicate back into a null-selecting one. Rejecting every
 * null operand is correct under any nesting; narrowing it requires negation-parity tracking.
 */
const carriesNullLiteral = (operand: PlanExpressionOperand): boolean =>
  isValue(operand) &&
  (operand.value === null ||
    (Array.isArray(operand.value) && operand.value.includes(null)));

const assertNoNullComparisonOperands = (
  expression: PlanExpressionOperand,
): void => {
  if (!isExpression(expression)) return;

  if (expression.operands.some(carriesNullLiteral)) {
    throw new Error(
      `Cannot translate \`${expression.operator}\` against a null operand under ` +
      'nullAttributeRepresentation "omitted": a NULL field sends no attribute, so Cerbos ' +
      "evaluates the comparison as a missing-attribute error (deny) while a null-selecting " +
      'filter would return those documents. Send NULL fields as explicit nulls and use ' +
      '"explicit", or keep this shape out of the policy.',
    );
  }

  for (const operand of expression.operands) {
    assertNoNullComparisonOperands(operand);
  }
};

export function queryPlanToConvex<Q = unknown, R = unknown>({
  queryPlan,
  mapper = {},
  allowPostFilter = false,
  nullAttributeRepresentation = "explicit",
}: QueryPlanToConvexArgs): QueryPlanToConvexResult<Q, R> {
  switch (queryPlan.kind) {
    case PlanKind.ALWAYS_ALLOWED:
      return { kind: PlanKind.ALWAYS_ALLOWED };
    case PlanKind.ALWAYS_DENIED:
      return { kind: PlanKind.ALWAYS_DENIED };
    case PlanKind.CONDITIONAL: {
      if (nullAttributeRepresentation === "omitted") {
        assertNoNullComparisonOperands(queryPlan.condition);
      }
      const { filter, postFilter } = buildFilters(queryPlan.condition, mapper);

      if (postFilter && !allowPostFilter) {
        throw new Error(
          "The query plan contains conditions that cannot be evaluated by Convex's " +
          "query engine and require trusted-backend filtering (postFilter). Apply " +
          "postFilter to every candidate before it is serialized or returned. Set " +
          "{ allowPostFilter: true } to opt in to this behavior.",
        );
      }

      const result: QueryPlanToConvexResult<Q, R> = { kind: PlanKind.CONDITIONAL };
      if (filter) result.filter = filter as ConvexFilter<Q, R>;
      if (postFilter) result.postFilter = postFilter;
      return result;
    }
    default:
      throw Error("Invalid query plan.");
  }
}
