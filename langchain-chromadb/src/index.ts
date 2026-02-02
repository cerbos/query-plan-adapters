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

const OPERATORS: {
  [key: string]: {
    fieldCondition: string;
  };
} = {
  eq: {
    fieldCondition: "$eq",
  },
  ne: {
    fieldCondition: "$ne",
  },
  in: {
    fieldCondition: "$in",
  },
  lt: {
    fieldCondition: "$lt",
  },
  gt: {
    fieldCondition: "$gt",
  },
  le: {
    fieldCondition: "$lte",
  },
  ge: {
    fieldCondition: "$gte",
  },
};

function mapOperand(
  operand: PlanExpressionOperand,
  getFieldName: (key: string) => string,
  output: Record<string, unknown> = {},
): Record<string, unknown> {
  if (!(operand instanceof PlanExpression))
    throw Error(
      `Query plan did not contain an expression for operand ${String(operand)}`,
    );

  const { operator, operands } = operand;

  if (operator == "and") {
    if (operands.length < 2) throw Error("Expected atleast 2 operands");
    output["$and"] = operands.map((o) => mapOperand(o, getFieldName, {}));
    return output;
  }

  if (operator == "or") {
    if (operands.length < 2) throw Error("Expected atleast 2 operands");
    output["$or"] = operands.map((o) => mapOperand(o, getFieldName, {}));
    return output;
  }

  if (operator == "not") {
    if (operands.length > 1) throw Error("Expected only one operand");
    if (Object.keys(output).length === 0) {
      output["$nor"] = [
        operands.map((o) => mapOperand(o, getFieldName, {}))[0],
      ];
    } else {
      output["$not"] = operands.map((o) =>
        mapOperand(o, getFieldName, {}),
      )[0];
    }
    return output;
  }

  const operation = OPERATORS[operator];
  if (!operation) throw Error(`Unsupported operator ${operator}`);

  const opVariable = getOperandVariable(operands);
  if (!opVariable) throw Error(`Unexpected variable ${String(operands)}`);

  const opValue = getOperandValue(operands);
  const fieldName = getFieldName(opVariable);
  if (!fieldName) throw Error("Field name is required");

  const [firstSegment, ...rest] = fieldName.split(".");
  if (!firstSegment) throw Error("Invalid field name");

  if (rest.length > 0) {
    output[firstSegment] = convertPathToJSON(rest, {
      [operation.fieldCondition]: opValue,
    });
  } else {
    output[firstSegment] = {
      [operation.fieldCondition]: opValue,
    };
  }
  return output;
}

function convertPathToJSON(
  segments: string[],
  value: Record<string, unknown>,
): Record<string, unknown> {
  const result: Record<string, unknown> = {};

  let current: Record<string, unknown> = result;
  for (let i = 0; i < segments.length; i++) {
    const segment = segments[i];
    if (!segment) throw Error("Invalid field path segment");
    if (i === segments.length - 1) {
      current[segment] = value;
    } else {
      const next: Record<string, unknown> = {};
      current[segment] = next;
      current = next;
    }
  }

  return result;
}
