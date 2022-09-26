import {
  PlanResourcesResponse,
  PlanExpressionOperand,
  PlanExpression,
  PlanExpressionValue,
  PlanExpressionVariable,
  PlanResourcesConditionalResponse,
  PlanKind as PK
} from "@cerbos/core";

export type PlanKind = PK;
export const PlanKind = PK;

type FieldMapper = {
  [key: string]: string;
} | ((key: string) => string)

type Relation = {
  relation: string,
  field: string
}

type RelationMapper = {
  [key: string]: Relation;
} | ((key: string) => Relation)

interface QueryPlanToPrismaArgs {
  queryPlan: PlanResourcesResponse;
  fieldNameMapper: FieldMapper,
  relationMapper?: RelationMapper
}

interface QueryPlanToPrismaResult {
  kind: PlanKind,
  filters?: any
}

export function queryPlanToPrisma({
  queryPlan,
  fieldNameMapper,
  relationMapper = {}
}: QueryPlanToPrismaArgs): QueryPlanToPrismaResult {
  if (queryPlan.kind === PlanKind.ALWAYS_ALLOWED) return {
    kind: PlanKind.ALWAYS_ALLOWED,
    filters: {}
  };
  if (queryPlan.kind === PlanKind.ALWAYS_DENIED) return {
    kind: PlanKind.ALWAYS_DENIED
  };
  return {
    kind: PlanKind.CONDITIONAL,
    filters: mapOperand(
      (queryPlan as PlanResourcesConditionalResponse).condition,
      (key: string) => {
        if (typeof fieldNameMapper === "function") {
          return fieldNameMapper(key);
        } else {
          return (fieldNameMapper[key] = fieldNameMapper[key] || key);
        }
      },
      (key: string) => {
        if (typeof relationMapper === "function") {
          return relationMapper(key);
        } else if (relationMapper[key]) {
          return relationMapper[key];
        }
        return null;
      },
      {}
    )
  };
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

function getOperandName(operands: PlanExpressionOperand[]) {
  const op = operands.find(o => o.hasOwnProperty("name"));
  if (!op) return;
  return (op as PlanExpressionVariable).name
}

function getOperandValue(operands: PlanExpressionOperand[]) {
  const op = operands.find(o => o.hasOwnProperty("value"));
  if (!op) return;
  return (op as PlanExpressionValue).value
}


function mapOperand(
  operand: PlanExpressionOperand,
  getFieldName: (key: string) => string,
  getRelationName: (key: string) => Relation | null,
  output: any = {}
): any {
  if (isExpression(operand)) {
    const { operator, operands } = operand;
    const opName = getOperandName(operands);
    const opValue = getOperandValue(operands);
    const relation = opName && getRelationName(opName);
    const fieldName = opName && getFieldName(opName)

    if (operator == "and") {
      if (operands.length < 2) throw Error("Expected atleast 2 operands")
      output.AND = operands.map((o) =>
        mapOperand(o, getFieldName, getRelationName, {})
      );
    }

    if (operator == "or") {
      if (operands.length < 2) throw Error("Expected atleast 2 operands")
      output.OR = operands.map((o) =>
        mapOperand(o, getFieldName, getRelationName, {})
      );
    }

    if (operator == "not") {
      if (operands.length > 1) throw Error("Expected only one operand")
      output.NOT = operands.map((o) =>
        mapOperand(o, getFieldName, getRelationName, {})
      )[0];
    }

    if (operator == "eq") {
      if (relation) {
        output[relation.relation] = {
          is: {
            [relation.field]: opValue
          }
        }
      } else if (fieldName) {
        output[fieldName] = {
          equals: opValue,
        };
      }
    }

    if (operator == "ne") {
      if (relation) {
        output[relation.relation] = {
          isNot: {
            [relation.field]: opValue
          }
        }
      } else if (fieldName) {
        output[fieldName] = {
          not: opValue,
        };
      }
    }

    if (operator == "lt" && fieldName) {
      output[fieldName] = {
        lt: opValue,
      };
    }

    if (operator == "gt" && fieldName) {
      output[fieldName] = {
        gt: opValue,
      };
    }

    if (operator == "le" && fieldName) {
      output[fieldName] = {
        lte: opValue,
      };
    }

    if (operator == "ge" && fieldName) {
      output[fieldName] = {
        gte: opValue,
      };
    }

    if (operator == "in") {
      if (relation) {
        output[relation.relation] = {
          some: {
            [relation.field]: opValue
          }
        }
      } else if (fieldName) {
        output[fieldName] = {
          in: opValue,
        };
      }
    }
  }

  return output;
}
