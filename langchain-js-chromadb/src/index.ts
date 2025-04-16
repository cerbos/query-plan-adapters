import {
  PlanResourcesResponse,
  PlanExpressionOperand,
  PlanExpression,
  PlanExpressionValue,
  PlanExpressionVariable,
  PlanKind,
} from "@cerbos/core";

function isExpression(e: PlanExpressionOperand): e is PlanExpression {
  return (e as any).operator !== undefined;
}

function isValue(e: PlanExpressionOperand): e is PlanExpressionValue {
  return (e as any).value !== undefined;
}

function isVariable(e: PlanExpressionOperand): e is PlanExpressionVariable {
  return (e as any).variable !== undefined;
}

function getOperandVariable(operands: PlanExpressionOperand[]) {
  const op = operands.find((o) => o.hasOwnProperty("name"));
  if (!op) return;
  return (op as PlanExpressionVariable).name;
}

function getOperandValue(operands: PlanExpressionOperand[]) {
  const op = operands.find((o) => isValue(o));
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

export function mapOperand(
  operand: PlanExpressionOperand,
  getFieldName: (key: string) => string,
  output: any = {}
): any {
  if (!isExpression(operand))
    throw Error(
      `Query plan did not contain an expression for operand ${operand}`
    );

  const { operator, operands } = operand;

  // HANDLE NESTING OPERATIONS: AND/OR/NOT
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

  // get the operation parameters
  const operation = OPERATORS[operator];
  if (!operation) throw Error(`Unsupported operator ${operator}`);

  const opVariable = getOperandVariable(operands);
  if (!opVariable) throw Error(`Unexpected variable ${operands}`);

  const opValue = getOperandValue(operands);
  const fieldName = getFieldName(opVariable);

  const fieldPath = fieldName.split(".");
  if (fieldPath.length > 1) {
    output[fieldPath[0]] = convertPathToJSON(fieldPath.splice(1), {
      [operation.fieldCondition]: opValue,
    });
  } else {
    output[fieldPath[0]] = {
      [operation.fieldCondition]: opValue,
    };
  }
  return output;
}

function convertPathToJSON(segments: string[], value: any): Object {
  const result = {};

  let current: Record<string, any> = result;
  for (let i = 0; i < segments.length; i++) {
    const segment = segments[i];
    if (i === segments.length - 1) {
      current[segment] = value;
    } else {
      current[segment] = {};
      current = current[segment];
    }
  }

  return result;
}
