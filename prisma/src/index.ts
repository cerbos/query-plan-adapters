import {
  ExpressionOperand,
  IQueryPlanResponse,
  IQueryPlanExpression,
  IQueryPlanValue,
  IQueryPlanVariable,
  QueryPlanKind,
} from "cerbos";

interface QueryPlanToPrismaArgs {
  queryPlan: IQueryPlanResponse;
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
  if (queryPlan.filter.kind === QueryPlanKind.KIND_ALWAYS_ALLOWED) return {};
  if (queryPlan.filter.kind === QueryPlanKind.KIND_ALWAYS_DENIED) return { "1": { "equals": 0 } };
  return mapOperand(
    queryPlan.filter.condition,
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

function isExpression(e: ExpressionOperand): e is IQueryPlanExpression {
  return (e as any).expression !== undefined;
}

function isValue(e: ExpressionOperand): e is IQueryPlanValue {
  return (e as any).value !== undefined;
}

function isVariable(e: ExpressionOperand): e is IQueryPlanVariable {
  return (e as any).variable !== undefined;
}

function mapOperand(
  operand: ExpressionOperand,
  getFieldName: (key: string) => string,
  output: any = {}
): any {
  if (isExpression(operand)) {
    const { expression } = operand;
    switch (expression.operator) {
      case "and":
        output.AND = expression.operands.map((o) =>
          mapOperand(o, getFieldName, {})
        );
        break;
      case "or":
        output.OR = expression.operands.map((o) =>
          mapOperand(o, getFieldName, {})
        );
        break;
      case "eq":
        output[
          getFieldName((expression.operands[0] as IQueryPlanVariable).variable)
        ] = {
          equals: (expression.operands[1] as IQueryPlanValue).value,
        };
        break;
      case "ne":
        output[
          getFieldName((expression.operands[0] as IQueryPlanVariable).variable)
        ] = {
          not: (expression.operands[1] as IQueryPlanValue).value,
        };
        break;
      case "lt":
        output[
          getFieldName((expression.operands[0] as IQueryPlanVariable).variable)
        ] = {
          lt: (expression.operands[1] as IQueryPlanValue).value,
        };
        break;
      case "gt":
        output[
          getFieldName((expression.operands[0] as IQueryPlanVariable).variable)
        ] = {
          gt: (expression.operands[1] as IQueryPlanValue).value,
        };
        break;
      case "lte":
        output[
          getFieldName((expression.operands[0] as IQueryPlanVariable).variable)
        ] = {
          lte: (expression.operands[1] as IQueryPlanValue).value,
        };
        break;
      case "gte":
        output[
          getFieldName((expression.operands[0] as IQueryPlanVariable).variable)
        ] = {
          gte: (expression.operands[1] as IQueryPlanValue).value,
        };
        break;
      case "in":
        output[
          getFieldName((expression.operands[0] as IQueryPlanVariable).variable)
        ] = {
          in: (expression.operands[1] as IQueryPlanValue).value,
        };
        break;
    }
  }

  return output;
}
