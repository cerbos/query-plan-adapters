import {
  PlanResourcesResponse,
  PlanExpressionOperand,
  PlanExpression,
  PlanExpressionValue,
  PlanExpressionVariable,
  PlanResourcesConditionalResponse,
  PlanKind
} from "@cerbos/core";

interface QueryPlanToPrismaArgs {
  queryPlan: PlanResourcesResponse;
  fieldNameMapper:
  | {
    [key: string]: string;
  }
  | ((key: string) => string);
}

export default function queryPlanToPrisma({
  queryPlan,
  fieldNameMapper,
}: QueryPlanToPrismaArgs): any {
  if (queryPlan.kind === PlanKind.ALWAYS_ALLOWED) return {};
  if (queryPlan.kind === PlanKind.ALWAYS_DENIED) return { "1": { "equals": 0 } };
  return mapOperand(
    (queryPlan as PlanResourcesConditionalResponse).condition,
    (key: string) => {
      if (typeof fieldNameMapper === "function") {
        return fieldNameMapper(key);
      } else {
        return (fieldNameMapper[key] = fieldNameMapper[key] || key);
      }
    },
    {}
  );
}

function isExpression(e: PlanExpressionOperand): e is PlanExpression {
  return (e as any).expression !== undefined;
}

function isValue(e: PlanExpressionOperand): e is PlanExpressionValue {
  return (e as any).value !== undefined;
}

function isVariable(e: PlanExpressionOperand): e is PlanExpressionVariable {
  return (e as any).variable !== undefined;
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
          getFieldName((operands[0] as PlanExpressionVariable).name)
        ] = {
          equals: (operands[1] as PlanExpressionValue).value,
        };
        break;
      case "ne":
        output[
          getFieldName((operands[0] as PlanExpressionVariable).name)
        ] = {
          not: (operands[1] as PlanExpressionValue).value,
        };
        break;
      case "lt":
        output[
          getFieldName((operands[0] as PlanExpressionVariable).name)
        ] = {
          lt: (operands[1] as PlanExpressionValue).value,
        };
        break;
      case "gt":
        output[
          getFieldName((operands[0] as PlanExpressionVariable).name)
        ] = {
          gt: (operands[1] as PlanExpressionValue).value,
        };
        break;
      case "lte":
        output[
          getFieldName((operands[0] as PlanExpressionVariable).name)
        ] = {
          lte: (operands[1] as PlanExpressionValue).value,
        };
        break;
      case "gte":
        output[
          getFieldName((operands[0] as PlanExpressionVariable).name)
        ] = {
          gte: (operands[1] as PlanExpressionValue).value,
        };
        break;
      case "in":
        output[
          getFieldName((operands[0] as PlanExpressionVariable).name)
        ] = {
          in: (operands[1] as PlanExpressionValue).value,
        };
        break;
    }
  }

  return output;
}
