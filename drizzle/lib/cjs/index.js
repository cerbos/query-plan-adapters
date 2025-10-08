"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.PlanKind = void 0;
exports.queryPlanToDrizzle = queryPlanToDrizzle;
const core_1 = require("@cerbos/core");
Object.defineProperty(exports, "PlanKind", { enumerable: true, get: function () { return core_1.PlanKind; } });
const drizzle_orm_1 = require("drizzle-orm");
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
            return value === null ? (0, drizzle_orm_1.isNull)(column) : (0, drizzle_orm_1.eq)(column, value);
        case "ne":
            return value === null
                ? (0, drizzle_orm_1.not)((0, drizzle_orm_1.isNull)(column))
                : (0, drizzle_orm_1.ne)(column, value);
        case "lt":
            return (0, drizzle_orm_1.lt)(column, value);
        case "le":
            return (0, drizzle_orm_1.lte)(column, value);
        case "gt":
            return (0, drizzle_orm_1.gt)(column, value);
        case "ge":
            return (0, drizzle_orm_1.gte)(column, value);
        case "in":
            if (!Array.isArray(value)) {
                throw new Error("The 'in' operator requires an array value");
            }
            return (0, drizzle_orm_1.inArray)(column, value);
        case "contains":
            if (typeof value !== "string") {
                throw new Error("The 'contains' operator requires a string value");
            }
            return (0, drizzle_orm_1.sql) `${column} LIKE ${`%${value}%`}`;
        case "startsWith":
            if (typeof value !== "string") {
                throw new Error("The 'startsWith' operator requires a string value");
            }
            return (0, drizzle_orm_1.sql) `${column} LIKE ${`${value}%`}`;
        case "endsWith":
            if (typeof value !== "string") {
                throw new Error("The 'endsWith' operator requires a string value");
            }
            return (0, drizzle_orm_1.sql) `${column} LIKE ${`%${value}`}`;
        case "isSet":
            if (typeof value !== "boolean") {
                throw new Error("The 'isSet' operator requires a boolean value");
            }
            return value ? (0, drizzle_orm_1.not)((0, drizzle_orm_1.isNull)(column)) : (0, drizzle_orm_1.isNull)(column);
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
            return (0, drizzle_orm_1.and)(...filters);
        }
        case "or": {
            if (operands.length === 0) {
                throw new Error("'or' operator requires at least one operand");
            }
            const filters = operands.map((operand) => buildFilterFromExpression(operand, mapper));
            return (0, drizzle_orm_1.or)(...filters);
        }
        case "not": {
            if (operands.length !== 1) {
                throw new Error("'not' operator requires exactly one operand");
            }
            return (0, drizzle_orm_1.not)(buildFilterFromExpression(operands[0], mapper));
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
function queryPlanToDrizzle({ queryPlan, mapper, }) {
    switch (queryPlan.kind) {
        case core_1.PlanKind.ALWAYS_ALLOWED:
            return { kind: core_1.PlanKind.ALWAYS_ALLOWED };
        case core_1.PlanKind.ALWAYS_DENIED:
            return { kind: core_1.PlanKind.ALWAYS_DENIED };
        case core_1.PlanKind.CONDITIONAL:
            return {
                kind: core_1.PlanKind.CONDITIONAL,
                filter: buildFilterFromExpression(queryPlan.condition, mapper),
            };
        default:
            throw new Error("Invalid plan kind");
    }
}
//# sourceMappingURL=index.js.map