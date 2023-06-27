import {
  PlanResourcesResponse,
  PlanExpressionOperand,
  PlanExpression,
  PlanExpressionValue,
  PlanExpressionVariable,
  PlanKind as PK,
} from "@cerbos/core";

export type PlanKind = PK;
export const PlanKind = PK;

type FieldMapper =
  | {
      [key: string]: string;
    }
  | ((key: string) => string);

type Relation = {
  relation: string;
  field: string;
};

type RelationMapper =
  | {
      [key: string]: Relation;
    }
  | ((key: string) => Relation);

interface QueryPlanToPrismaArgs {
  queryPlan: PlanResourcesResponse;
  fieldNameMapper: FieldMapper;
}

interface QueryPlanToPrismaResult {
  kind: PlanKind;
  filters?: any;
}

export function queryPlanToMongoose({
  queryPlan,
  fieldNameMapper,
}: QueryPlanToPrismaArgs): QueryPlanToPrismaResult {
  const toFieldName = (key: string) => {
    if (typeof fieldNameMapper === "function") {
      return fieldNameMapper(key);
    } else if (fieldNameMapper[key]) {
      return fieldNameMapper[key];
    } else {
      return key;
    }
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
    relationalCondition?: string;
    fieldCondition: string;
  };
} = {
  eq: {
    relationalCondition: "is",
    fieldCondition: "equals",
  },
  ne: {
    relationalCondition: "isNot",
    fieldCondition: "$ne",
  },
  in: {
    relationalCondition: "some",
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

  if (operator == "not") {
    if (operands.length > 1) throw Error("Expected only one operand");
    if (Object.keys(output).length === 0) {
      output["$nor"] = [
        operands.map((o) => mapOperand(o, getFieldName, {}))[0],
      ];
    } else {
      output["$not"] = operands.map((o) => mapOperand(o, getFieldName, {}))[0];
    }
    return output;
  }

  // get the operation parameters
  const operation = OPERATORS[operator];
  if (!operation) throw Error(`Unsupported operator ${operator}`);

  const opVariable = getOperandVariable(operands);
  if (!opVariable) throw Error(`Unexpected variable ${operands}`);

  const opValue = getOperandValue(operands);
  const fieldName = getFieldName(opVariable);

  // There is a relational mapper for this variable
  if (fieldName && operation.fieldCondition) {
    // There is a field mapper for this variable

    if (operation.fieldCondition === "equals") {
      output[fieldName] = opValue;
    } else {
      output[fieldName] = {
        [operation.fieldCondition]: opValue,
      };
    }
    return output;
  } else {
    throw Error("Failed to map");
  }
}
