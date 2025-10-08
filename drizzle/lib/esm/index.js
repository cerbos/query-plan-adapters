import { PlanKind, } from "@cerbos/core";
import { and, or, not, eq, ne, lt, lte, gt, gte, inArray, isNull, sql, } from "drizzle-orm";
export { PlanKind };
const isMappingConfig = (entry) => typeof entry === "object" && entry !== null && "column" in entry;
const isNameOperand = (operand) => "name" in operand && typeof operand.name === "string";
const isValueOperand = (operand) => "value" in operand;
const isExpressionOperand = (operand) => "operator" in operand && Array.isArray(operand.operands);
const resolveMapping = (reference, mapper) => {
    const mapping = typeof mapper === "function" ? mapper(reference) : mapper[reference];
    if (!mapping) {
        throw new Error(`No mapping found for reference: ${reference}`);
    }
    return mapping;
};
const applyComparison = (mapping, operator, value) => {
    if (typeof mapping === "function") {
        return mapping({ operator, value });
    }
    if (isMappingConfig(mapping)) {
        if (mapping.transform) {
            return mapping.transform({ operator, value });
        }
        return applyComparison(mapping.column, operator, value);
    }
    const column = mapping;
    switch (operator) {
        case "eq":
            return value === null ? isNull(column) : eq(column, value);
        case "ne":
            return value === null
                ? not(isNull(column))
                : ne(column, value);
        case "lt":
            return lt(column, value);
        case "le":
            return lte(column, value);
        case "gt":
            return gt(column, value);
        case "ge":
            return gte(column, value);
        case "in":
            if (!Array.isArray(value)) {
                throw new Error("The 'in' operator requires an array value");
            }
            return inArray(column, value);
        case "contains":
            if (typeof value !== "string") {
                throw new Error("The 'contains' operator requires a string value");
            }
            return sql `${column} LIKE ${`%${value}%`}`;
        case "startsWith":
            if (typeof value !== "string") {
                throw new Error("The 'startsWith' operator requires a string value");
            }
            return sql `${column} LIKE ${`${value}%`}`;
        case "endsWith":
            if (typeof value !== "string") {
                throw new Error("The 'endsWith' operator requires a string value");
            }
            return sql `${column} LIKE ${`%${value}`}`;
        case "isSet":
            if (typeof value !== "boolean") {
                throw new Error("The 'isSet' operator requires a boolean value");
            }
            return value ? not(isNull(column)) : isNull(column);
        default:
            throw new Error(`Unsupported operator: ${operator}`);
    }
};
const buildFilterFromExpression = (expression, mapper) => {
    if (!isExpressionOperand(expression)) {
        throw new Error("Invalid expression operand");
    }
    const { operator, operands } = expression;
    switch (operator) {
        case "and": {
            if (operands.length === 0) {
                throw new Error("'and' operator requires at least one operand");
            }
            const filters = operands.map((operand) => buildFilterFromExpression(operand, mapper));
            return and(...filters);
        }
        case "or": {
            if (operands.length === 0) {
                throw new Error("'or' operator requires at least one operand");
            }
            const filters = operands.map((operand) => buildFilterFromExpression(operand, mapper));
            return or(...filters);
        }
        case "not": {
            if (operands.length !== 1) {
                throw new Error("'not' operator requires exactly one operand");
            }
            return not(buildFilterFromExpression(operands[0], mapper));
        }
        case "eq":
        case "ne":
        case "lt":
        case "le":
        case "gt":
        case "ge":
        case "in":
        case "contains":
        case "startsWith":
        case "endsWith":
        case "isSet": {
            const fieldOperand = operands.find(isNameOperand);
            if (!fieldOperand) {
                throw new Error("Comparison operator missing field operand");
            }
            const valueOperand = operands.find(isValueOperand);
            if (!valueOperand) {
                throw new Error("Comparison operator missing value operand");
            }
            const mapping = resolveMapping(fieldOperand.name, mapper);
            return applyComparison(mapping, operator, valueOperand.value);
        }
        default:
            throw new Error(`Unsupported operator: ${operator}`);
    }
};
export function queryPlanToDrizzle({ queryPlan, mapper, }) {
    switch (queryPlan.kind) {
        case PlanKind.ALWAYS_ALLOWED:
            return { kind: PlanKind.ALWAYS_ALLOWED };
        case PlanKind.ALWAYS_DENIED:
            return { kind: PlanKind.ALWAYS_DENIED };
        case PlanKind.CONDITIONAL:
            return {
                kind: PlanKind.CONDITIONAL,
                filter: buildFilterFromExpression(queryPlan.condition, mapper),
            };
        default:
            throw new Error("Invalid plan kind");
    }
}
//# sourceMappingURL=index.js.map