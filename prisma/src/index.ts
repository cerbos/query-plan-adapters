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
  relationMapper?: RelationMapper;
}

interface QueryPlanToPrismaResult {
  kind: PlanKind;
  filters?: any;
}

export function queryPlanToPrisma({
  queryPlan,
  fieldNameMapper,
  relationMapper = {},
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

  const toRelationName = (key: string) => {
    if (typeof relationMapper === "function") {
      return relationMapper(key);
    } else if (relationMapper[key]) {
      return relationMapper[key];
    }
    return null;
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
        filters: mapOperand(queryPlan.condition, toFieldName, toRelationName),
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
    fieldCondition: "not",
  },
  in: {
    relationalCondition: "some",
    fieldCondition: "in",
  },
  lt: {
    fieldCondition: "lt",
  },
  gt: {
    fieldCondition: "gt",
  },
  le: {
    fieldCondition: "lte",
  },
  ge: {
    fieldCondition: "gte",
  },
};

function mapOperand(
  operand: PlanExpressionOperand,
  getFieldName: (key: string) => string,
  getRelationName: (key: string) => Relation | null,
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
    output.AND = operands.map((o) =>
      mapOperand(o, getFieldName, getRelationName, {})
    );
    return output;
  }

  if (operator == "or") {
    if (operands.length < 2) throw Error("Expected atleast 2 operands");
    output.OR = operands.map((o) =>
      mapOperand(o, getFieldName, getRelationName, {})
    );
    return output;
  }

  if (operator == "not") {
    if (operands.length > 1) throw Error("Expected only one operand");
    output.NOT = operands.map((o) =>
      mapOperand(o, getFieldName, getRelationName, {})
    )[0];
    return output;
  }

  // get the operation parameters
  const operation = OPERATORS[operator];
  if (!operation) throw Error(`Unsupported operator ${operator}`);

  const opVariable = getOperandVariable(operands);
  if (!opVariable) throw Error(`Unexpected variable ${operands}`);

  const opValue = getOperandValue(operands);
  const relation = getRelationName(opVariable);
  const fieldName = getFieldName(opVariable);

  // There is a relational mapper for this variable
  if (relation && operation.relationalCondition) {
    output[relation.relation] = {
      [operation.relationalCondition]: {
        [relation.field]: opValue,
      },
    };
    return output;
  } else if (fieldName && operation.fieldCondition) {
    // There is a field mapper for this variable

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
  } else {
    throw Error("Failed to map");
  }
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
