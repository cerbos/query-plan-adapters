import {
  PlanResourcesResponse,
  PlanExpressionOperand,
  PlanExpression,
  PlanExpressionValue,
  PlanExpressionVariable,
  PlanResourcesConditionalResponse,
  PlanKind
} from "@cerbos/core";

type Mapper = {
  [key: string]: string;
}
  | ((key: string) => string)

interface QueryPlanToPrismaArgs {
  queryPlan: PlanResourcesResponse;
  fieldNameMapper: Mapper,
  relationMapper?: Mapper
}

interface QueryPlanToPrismaResult {
  kind: PlanKind,
  filters?: any
}

export default function queryPlanToPrisma({
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
  return ((operands.find(o => o.hasOwnProperty("name"))) as PlanExpressionVariable).name
}

function getOperandValue(operands: PlanExpressionOperand[]) {
  return ((operands.find(o => o.hasOwnProperty("value"))) as PlanExpressionValue).value
}


function mapOperand(
  operand: PlanExpressionOperand,
  getFieldName: (key: string) => string,
  output: any = {}
): any {
  if (isExpression(operand)) {
    const { operator, operands } = operand;
    switch (operator) {
      case "and":
        output.AND = operands.map((o) =>
          mapOperand(o, getFieldName, {})
        );
        break;
      case "or":
        output.OR = operands.map((o) =>
          mapOperand(o, getFieldName, {})
        );
        break;
      case "eq":
        output[
          getFieldName(getOperandName(operands))
        ] = {
          equals: getOperandValue(operands),
        };
        break;
      case "ne":
        output[
          getFieldName(getOperandName(operands))
        ] = {
          not: getOperandValue(operands),
        };
        break;
      case "lt":
        output[
          getFieldName(getOperandName(operands))
        ] = {
          lt: getOperandValue(operands),
        };
        break;
      case "gt":
        output[
          getFieldName(getOperandName(operands))
        ] = {
          gt: getOperandValue(operands),
        };
        break;
      case "le":
        output[
          getFieldName(getOperandName(operands))
        ] = {
          lte: getOperandValue(operands),
        };
        break;
      case "ge":
        output[
          getFieldName(getOperandName(operands))
        ] = {
          gte: getOperandValue(operands),
        };
        break;
      case "in":
        output[
          getFieldName(getOperandName(operands))
        ] = {
          in: getOperandValue(operands),
        };
        break;
    }
  }

  return output;
}
