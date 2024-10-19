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

interface QueryPlanToPostgresArgs {
  queryPlan: PlanResourcesResponse;
  fieldNameMapper: FieldMapper;
  relationMapper?: RelationMapper;
}

interface QueryPlanToPostgresResult {
  kind: PlanKind;
  whereClause?: string;
}

export function queryPlanToPostgres({
  queryPlan,
  fieldNameMapper,
  relationMapper = {},
}: QueryPlanToPostgresArgs): QueryPlanToPostgresResult {
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
      };
    case PlanKind.ALWAYS_DENIED:
      return {
        kind: PlanKind.ALWAYS_DENIED,
      };
    case PlanKind.CONDITIONAL:
      return {
        kind: PlanKind.CONDITIONAL,
        whereClause: mapOperandToSQL(
          queryPlan.condition,
          toFieldName,
          toRelationName
        ),
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
    fieldCondition: "=",
  },
  ne: {
    relationalCondition: "isNot",
    fieldCondition: "!=",
  },
  in: {
    relationalCondition: "some",
    fieldCondition: "IN",
  },
  lt: {
    fieldCondition: "<",
  },
  gt: {
    fieldCondition: ">",
  },
  le: {
    fieldCondition: "<=",
  },
  ge: {
    fieldCondition: ">=",
  },
};

function mapOperandToSQL(
  operand: PlanExpressionOperand,
  getFieldName: (key: string) => string,
  getRelationName: (key: string) => Relation | null
): string {
  if (!isExpression(operand)) {
    throw new Error(
      `Query plan did not contain an expression for operand ${operand}`
    );
  }

  const { operator, operands } = operand;

  // Handle logical operations AND, OR, NOT
  if (operator === "and") {
    const clauses = operands.map((o) =>
      mapOperandToSQL(o, getFieldName, getRelationName)
    );
    return `(${clauses.join(" AND ")})`;
  }

  if (operator === "or") {
    const clauses = operands.map((o) =>
      mapOperandToSQL(o, getFieldName, getRelationName)
    );
    return `(${clauses.join(" OR ")})`;
  }

  if (operator === "not") {
    const clause = mapOperandToSQL(operands[0], getFieldName, getRelationName);
    return `NOT (${clause})`;
  }

  // Handle comparison operations
  const operation = OPERATORS[operator];
  if (!operation) throw new Error(`Unsupported operator ${operator}`);

  const opVariable = getOperandVariable(operands);
  if (!opVariable) throw new Error(`Unexpected variable ${operands}`);

  const opValue = getOperandValue(operands);
  const relation = getRelationName(opVariable);
  const fieldName = getFieldName(opVariable);

  // Build the SQL where clause for relational or direct field condition
  if (relation && operation.relationalCondition) {
    return `${relation.relation} ${
      operation.relationalCondition
    } ${formatSQLValue(opValue)}`;
  } else if (fieldName && operation.fieldCondition) {
    return `${fieldName} ${operation.fieldCondition} ${formatSQLValue(
      opValue
    )}`;
  } else {
    throw new Error("Failed to map");
  }
}

function formatSQLValue(value: any): string {
  if (typeof value === "boolean") {
    // Handle boolean values
    return value ? "TRUE" : "FALSE";
  } else if (Array.isArray(value)) {
    // Handle arrays (for `IN` operations)
    return `(${value.map((v) => formatSQLValue(v)).join(", ")})`;
  } else if (typeof value === "number") {
    // Handle numeric values without quotes
    return `${value}`;
  } else {
    // Handle strings or other types
    return `'${value}'`;
  }
}
