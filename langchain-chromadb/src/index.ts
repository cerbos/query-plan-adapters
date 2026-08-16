import {
  PlanExpression,
  PlanExpressionOperand,
  PlanExpressionValue,
  PlanExpressionVariable,
  PlanKind as PK,
  PlanResourcesResponse,
} from "@cerbos/core";
import type { Where } from "chromadb";

export type PlanKind = PK;
export const PlanKind = PK;

export interface FieldNameMapperConfig {
  field: string;
  required?: boolean;
  numericType?: "integer" | "float";
}

type FieldNameMapperValue = string | FieldNameMapperConfig;

export type FieldMapper =
  | Record<string, FieldNameMapperValue>
  | ((key: string) => FieldNameMapperValue);

export interface QueryPlanToChromaDBArgs {
  queryPlan: PlanResourcesResponse;
  fieldNameMapper: FieldMapper;
}

// Exported so a consumer can name what it is handed, as prisma and drizzle already do for theirs.
// A caller that passes the result to a function of its own — which is what composing the clause
// with an application-owned one looks like — otherwise has to write it out or reach for
// `ReturnType<typeof queryPlanToChromaDB>`. Found by `example/`, which is the only thing here that
// resolves this package through its published surface
// (docs/adr/0002-examples-install-the-packed-artifact.md).
export interface QueryPlanToChromaDBResult {
  kind: PlanKind;
  filters?: Where;
}

type ChromaLiteral = string | number | boolean;

type BinaryOperands = {
  variable: PlanExpressionVariable;
  variableIndex: number;
  value: PlanExpressionValue;
};

type ResolvedField = {
  name: string;
  numericType?: "integer" | "float";
  required: boolean;
};

type FieldResolver = (key: string) => ResolvedField;

// Operands are classified by shape, never with `instanceof`. `instanceof` is nominal, so it
// answers "was this built by MY copy of @cerbos/core?" rather than "what kind of operand is
// this?" — and a consumer whose Cerbos client resolves a different copy of core than this
// adapter does is an ordinary npm outcome, not a misconfiguration. No dependency declaration
// prevents it: npm resolves a peer to the highest version satisfying it, not the one that
// dedupes with the rest of the tree, so every range leaves some consumer with two copies
// (cerbos/query-plan-adapters#419). The three operand types have disjoint shapes, so matching
// on them is exact and survives however many copies exist. Same trio as every other adapter.
const isExpression = (e: PlanExpressionOperand): e is PlanExpression =>
  "operator" in e;
const isValue = (e: PlanExpressionOperand): e is PlanExpressionValue =>
  "value" in e;
const isVariable = (e: PlanExpressionOperand): e is PlanExpressionVariable =>
  "name" in e;

const NEGATED_OPERATOR: Readonly<Record<string, string>> = {
  eq: "ne",
  ne: "eq",
  lt: "ge",
  gt: "le",
  le: "gt",
  ge: "lt",
  in: "nin",
};

const MIRRORED_OPERATOR: Readonly<Record<string, string>> = {
  eq: "eq",
  ne: "ne",
  lt: "gt",
  le: "ge",
  gt: "lt",
  ge: "le",
};

export function queryPlanToChromaDB({
  queryPlan,
  fieldNameMapper,
}: QueryPlanToChromaDBArgs): QueryPlanToChromaDBResult {
  // Fields default to optional: Chroma's $ne/$nin match records where the metadata key is
  // absent, while Cerbos denies on a missing attribute. Without an explicit
  // `required: true` assertion from the integrator the adapter cannot know the key is always
  // present, so those operators are rejected rather than allowed to over-grant.
  const toField = (key: string): ResolvedField => {
    const mapped =
      typeof fieldNameMapper === "function"
        ? fieldNameMapper(key)
        : fieldNameMapper[key];
    if (typeof mapped === "string") {
      return { name: mapped, required: false };
    }
    if (mapped) {
      return {
        name: mapped.field,
        numericType: mapped.numericType,
        required: mapped.required ?? false,
      };
    }
    return { name: key, required: false };
  };

  switch (queryPlan.kind) {
    case PlanKind.ALWAYS_ALLOWED:
      return {
        kind: PlanKind.ALWAYS_ALLOWED,
        filters: {},
      };
    case PlanKind.ALWAYS_DENIED:
      return {
        kind: PlanKind.ALWAYS_DENIED,
      };
    case PlanKind.CONDITIONAL:
      return {
        kind: PlanKind.CONDITIONAL,
        filters: mapOperand(queryPlan.condition, toField),
      };
    default:
      throw Error("Invalid query plan.");
  }
}

function binaryOperands(operands: PlanExpressionOperand[]): BinaryOperands {
  if (operands.length !== 2) {
    throw Error("Expected exactly two operands");
  }

  let variable: PlanExpressionVariable | undefined;
  let variableIndex = -1;
  let value: PlanExpressionValue | undefined;

  for (const [index, operand] of operands.entries()) {
    if (isVariable(operand)) {
      if (variable) {
        throw Error(
          "Variable-to-variable comparisons are not supported by ChromaDB filters",
        );
      }
      variable = operand;
      variableIndex = index;
    } else if (isValue(operand)) {
      if (value) {
        throw Error("Value-to-value comparisons are not supported by ChromaDB filters");
      }
      value = operand;
    } else {
      throw Error("Nested expressions are not supported by ChromaDB filters");
    }
  }

  if (!variable) {
    throw Error(`Unexpected variable ${String(operands)}`);
  }
  if (!value) {
    throw Error(
      "Variable-to-variable comparisons are not supported by ChromaDB filters",
    );
  }

  return { variable, variableIndex, value };
}

function isChromaLiteral(value: unknown): value is ChromaLiteral {
  return (
    typeof value === "string" ||
    typeof value === "boolean" ||
    (typeof value === "number" && Number.isFinite(value))
  );
}

function requireLiteral(value: unknown, operator: string): ChromaLiteral {
  if (!isChromaLiteral(value)) {
    throw Error(
      `${operator} requires a finite number, string, or boolean literal`,
    );
  }
  return value;
}

function requireNumber(value: unknown, operator: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw Error(`${operator} requires a finite number literal`);
  }
  return value;
}

function requireLiteralList(value: unknown, operator: string): ChromaLiteral[] {
  if (!Array.isArray(value) || value.length === 0) {
    throw Error(`${operator} requires a non-empty literal list`);
  }
  if (!value.every(isChromaLiteral)) {
    throw Error(
      `${operator} requires a list containing only finite numbers, strings, or booleans`,
    );
  }
  const firstType = typeof value[0];
  if (!value.every((item) => typeof item === firstType)) {
    throw Error(`${operator} requires a list whose values have one scalar type`);
  }
  return value;
}

function whereFor(
  fieldName: string,
  operator: string,
  value: unknown,
): Where {
  switch (operator) {
    case "eq":
      return { [fieldName]: { $eq: requireLiteral(value, operator) } };
    case "ne":
      return { [fieldName]: { $ne: requireLiteral(value, operator) } };
    case "lt":
      return { [fieldName]: { $lt: requireNumber(value, operator) } };
    case "le":
      return { [fieldName]: { $lte: requireNumber(value, operator) } };
    case "gt":
      return { [fieldName]: { $gt: requireNumber(value, operator) } };
    case "ge":
      return { [fieldName]: { $gte: requireNumber(value, operator) } };
    case "in":
      return { [fieldName]: { $in: requireLiteralList(value, operator) } };
    case "nin":
      return { [fieldName]: { $nin: requireLiteralList(value, operator) } };
    default:
      throw Error(`Unsupported operator ${operator}`);
  }
}

function normalizeOperator(operator: string, variableIndex: number): string {
  if (variableIndex === 0) {
    return operator;
  }
  if (operator === "in") {
    throw Error(
      "ChromaDB filters cannot test whether a literal is contained in a metadata field",
    );
  }

  const mirrored = MIRRORED_OPERATOR[operator];
  if (!mirrored) {
    throw Error(`Unsupported operator ${operator}`);
  }
  return mirrored;
}

function mapComparison(
  operator: string,
  operands: PlanExpressionOperand[],
  resolveField: FieldResolver,
  negate: boolean,
): Where {
  const { variable, variableIndex, value } = binaryOperands(operands);
  const normalized = normalizeOperator(operator, variableIndex);
  const mappedOperator = negate ? NEGATED_OPERATOR[normalized] : normalized;
  if (!mappedOperator) {
    throw Error(`Cannot negate operator ${normalized}`);
  }

  const field = resolveField(variable.name);
  if (!field.name) {
    throw Error("Field name is required");
  }
  if (!field.required && (mappedOperator === "ne" || mappedOperator === "nin")) {
    throw Error(
      `${mappedOperator} is unsafe for optional Chroma metadata because missing fields match the filter`,
    );
  }
  if (
    ["lt", "le", "gt", "ge"].includes(mappedOperator) &&
    typeof value.value === "number" &&
    !Number.isInteger(value.value) &&
    field.numericType !== "float"
  ) {
    throw Error(
      `${mappedOperator} cannot safely compare a fractional threshold unless the mapped Chroma metadata field declares numericType: "float"`,
    );
  }
  return whereFor(field.name, mappedOperator, value.value);
}

function mapBooleanVariable(
  variable: PlanExpressionVariable,
  resolveField: FieldResolver,
  negate: boolean,
): Where {
  const field = resolveField(variable.name);
  if (!field.name) {
    throw Error("Field name is required");
  }
  if (negate && !field.required) {
    throw Error(
      "ne is unsafe for optional Chroma metadata because missing fields match the filter",
    );
  }
  return whereFor(field.name, negate ? "ne" : "eq", true);
}

function negateOperand(
  operand: PlanExpressionOperand,
  resolveField: FieldResolver,
): Where {
  if (isVariable(operand)) {
    return mapBooleanVariable(operand, resolveField, true);
  }
  if (!isExpression(operand)) {
    throw Error(
      `Query plan did not contain an expression for operand ${String(operand)}`,
    );
  }

  const { operator, operands } = operand;

  if (operator === "and") {
    if (operands.length < 2) throw Error("Expected at least 2 operands");
    return {
      $or: operands.map((child) => negateOperand(child, resolveField)),
    };
  }

  if (operator === "or") {
    if (operands.length < 2) throw Error("Expected at least 2 operands");
    return {
      $and: operands.map((child) => negateOperand(child, resolveField)),
    };
  }

  if (operator === "not") {
    if (operands.length !== 1 || !operands[0])
      throw Error("Expected exactly one operand");
    return mapOperand(operands[0], resolveField);
  }

  if (!NEGATED_OPERATOR[operator]) {
    throw Error(`Cannot negate operator ${operator}`);
  }
  return mapComparison(operator, operands, resolveField, true);
}

function mapOperand(
  operand: PlanExpressionOperand,
  resolveField: FieldResolver,
): Where {
  if (isVariable(operand)) {
    return mapBooleanVariable(operand, resolveField, false);
  }
  if (!isExpression(operand)) {
    throw Error(
      `Query plan did not contain an expression for operand ${String(operand)}`,
    );
  }

  const { operator, operands } = operand;

  if (operator === "and") {
    if (operands.length < 2) throw Error("Expected at least 2 operands");
    return {
      $and: operands.map((child) => mapOperand(child, resolveField)),
    };
  }

  if (operator === "or") {
    if (operands.length < 2) throw Error("Expected at least 2 operands");
    return {
      $or: operands.map((child) => mapOperand(child, resolveField)),
    };
  }

  if (operator === "not") {
    if (operands.length !== 1 || !operands[0])
      throw Error("Expected exactly one operand");
    return negateOperand(operands[0], resolveField);
  }

  return mapComparison(operator, operands, resolveField, false);
}
