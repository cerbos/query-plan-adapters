import {
  PlanExpression,
  PlanExpressionOperand,
  PlanExpressionValue,
  PlanExpressionVariable,
  PlanKind as PK,
  PlanResourcesResponse,
} from "@cerbos/core";

export type PlanKind = PK;
export const PlanKind = PK;

type FieldMapper =
  | {
      [key: string]: string;
    }
  | ((key: string) => string);

interface QueryPlanToChromaDBArgs {
  queryPlan: PlanResourcesResponse;
  fieldNameMapper: FieldMapper;
}

interface QueryPlanToChromaDBResult {
  kind: PlanKind;
  filters?: Record<string, unknown>;
}

export function queryPlanToChromaDB({
  queryPlan,
  fieldNameMapper,
}: QueryPlanToChromaDBArgs): QueryPlanToChromaDBResult {
  const toFieldName = (key: string) => {
    if (typeof fieldNameMapper === "function") {
      return fieldNameMapper(key);
    }

    if (fieldNameMapper[key]) {
      return fieldNameMapper[key];
    }

    return key;
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
        filters: mapOperand(queryPlan.condition, toFieldName),
      };
    default:
      throw Error(`Invalid query plan.`);
  }
}

function getOperandVariable(
  operands: PlanExpressionOperand[],
): string | undefined {
  const op = operands.find((o) => o instanceof PlanExpressionVariable);
  if (!op) return;
  return (op as PlanExpressionVariable).name;
}

function getOperandValue(
  operands: PlanExpressionOperand[],
): unknown | undefined {
  const op = operands.find((o) => o instanceof PlanExpressionValue);
  if (!op) return;
  return (op as PlanExpressionValue).value;
}

const NEGATED_OPERATOR: Record<string, string> = {
  eq: "ne",
  ne: "eq",
  lt: "ge",
  gt: "le",
  le: "gt",
  ge: "lt",
  in: "nin",
};

const OPERATORS: Record<string, string> = {
  eq: "$eq",
  ne: "$ne",
  in: "$in",
  nin: "$nin",
  lt: "$lt",
  gt: "$gt",
  le: "$lte",
  ge: "$gte",
};

function negateOperand(
  operand: PlanExpressionOperand,
  getFieldName: (key: string) => string,
): Record<string, unknown> {
  if (!(operand instanceof PlanExpression))
    throw Error(
      `Query plan did not contain an expression for operand ${String(operand)}`,
    );

  const { operator, operands } = operand;

  if (operator === "and") {
    if (operands.length < 2) throw Error("Expected at least 2 operands");
    return {
      $or: operands.map((o) => negateOperand(o, getFieldName)),
    };
  }

  if (operator === "or") {
    if (operands.length < 2) throw Error("Expected at least 2 operands");
    return {
      $and: operands.map((o) => negateOperand(o, getFieldName)),
    };
  }

  if (operator === "not") {
    if (operands.length !== 1 || !operands[0])
      throw Error("Expected exactly one operand");
    return mapOperand(operands[0], getFieldName);
  }

  const negated = NEGATED_OPERATOR[operator];
  if (!negated) throw Error(`Cannot negate operator ${operator}`);

  const chromaOp = OPERATORS[negated];
  if (!chromaOp) throw Error(`Unsupported negated operator ${negated}`);

  const opVariable = getOperandVariable(operands);
  if (!opVariable) throw Error(`Unexpected variable ${String(operands)}`);

  const opValue = getOperandValue(operands);
  const fieldName = getFieldName(opVariable);
  if (!fieldName) throw Error("Field name is required");

  return { [fieldName]: { [chromaOp]: opValue } };
}

function mapOperand(
  operand: PlanExpressionOperand,
  getFieldName: (key: string) => string,
): Record<string, unknown> {
  if (!(operand instanceof PlanExpression))
    throw Error(
      `Query plan did not contain an expression for operand ${String(operand)}`,
    );

  const { operator, operands } = operand;

  if (operator === "and") {
    if (operands.length < 2) throw Error("Expected at least 2 operands");
    return {
      $and: operands.map((o) => mapOperand(o, getFieldName)),
    };
  }

  if (operator === "or") {
    if (operands.length < 2) throw Error("Expected at least 2 operands");
    return {
      $or: operands.map((o) => mapOperand(o, getFieldName)),
    };
  }

  if (operator === "not") {
    if (operands.length !== 1 || !operands[0])
      throw Error("Expected exactly one operand");
    return negateOperand(operands[0], getFieldName);
  }

  const chromaOp = OPERATORS[operator];
  if (!chromaOp) throw Error(`Unsupported operator ${operator}`);

  const opVariable = getOperandVariable(operands);
  if (!opVariable) throw Error(`Unexpected variable ${String(operands)}`);

  const opValue = getOperandValue(operands);
  const fieldName = getFieldName(opVariable);
  if (!fieldName) throw Error("Field name is required");

  return { [fieldName]: { [chromaOp]: opValue } };
}
