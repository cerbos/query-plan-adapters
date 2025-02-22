import {
  PlanResourcesResponse,
  PlanExpressionOperand,
  PlanKind,
} from "@cerbos/core";

export { PlanKind };

export type PrismaFilter = Record<string, any>;

export type MapperConfig = {
  field?: string;
  relation?: {
    name: string;
    type: "one" | "many";
    field?: string;
    fields?: {
      [key: string]: MapperConfig;
    };
  };
};

export type Mapper =
  | {
      [key: string]: MapperConfig;
    }
  | ((key: string) => MapperConfig);

export interface QueryPlanToPrismaArgs {
  queryPlan: PlanResourcesResponse;
  mapper?: Mapper;
}

export interface QueryPlanToPrismaResult {
  kind: PlanKind;
  filters?: Record<string, any>;
}

/**
 * Converts a Cerbos query plan to a Prisma filter.
 * @param {QueryPlanToPrismaArgs} args - The arguments containing the query plan and mapper.
 * @returns {QueryPlanToPrismaResult} The result containing the kind and filters.
 */
export function queryPlanToPrisma({
  queryPlan,
  mapper = {},
}: QueryPlanToPrismaArgs): QueryPlanToPrismaResult {
  // Choose filter conversion based on plan kind
  switch (queryPlan.kind) {
    case PlanKind.ALWAYS_ALLOWED:
      return {
        kind: PlanKind.ALWAYS_ALLOWED,
      };
    case PlanKind.ALWAYS_DENIED:
      return { kind: PlanKind.ALWAYS_DENIED };
    case PlanKind.CONDITIONAL:
      return {
        kind: PlanKind.CONDITIONAL,
        filters: buildPrismaFilterFromCerbosExpression(
          queryPlan.condition,
          mapper
        ),
      };
    default:
      throw Error(`Invalid query plan.`);
  }
}

/**
 * Resolves a field reference considering relations.
 * @param {string} reference - The field reference.
 * @param {Mapper} mapper - The mapper configuration.
 * @returns {object} The resolved field reference with path and relation.
 */
const resolveFieldReference = (
  reference: string,
  mapper: Mapper
): {
  path: string[];
  relations?: Array<{
    name: string;
    type: "one" | "many";
    field?: string;
    nestedMapper?: { [key: string]: MapperConfig };
  }>;
} => {
  const parts = reference.split(".");

  // Try exact match first
  const config =
    typeof mapper === "function" ? mapper(reference) : mapper[reference];

  // Find the longest matching prefix in the mapper
  let matchedPrefix = "";
  let matchedConfig: MapperConfig | undefined;

  if (!config) {
    for (let i = parts.length - 1; i >= 0; i--) {
      const prefix = parts.slice(0, i + 1).join(".");
      const prefixConfig =
        typeof mapper === "function" ? mapper(prefix) : mapper[prefix];
      if (prefixConfig) {
        matchedPrefix = prefix;
        matchedConfig = prefixConfig;
        break;
      }
    }
  }

  if (config || matchedConfig) {
    const activeConfig = config || matchedConfig!;
    if (activeConfig.relation) {
      const { name, type, fields } = activeConfig.relation;
      const matchedParts = matchedPrefix ? matchedPrefix.split(".") : [];
      const remainingParts = matchedPrefix
        ? parts.slice(matchedParts.length)
        : parts.slice(1);

      // Get the field name to use in the filter
      // If a field is defined in the relation config, use that
      // Otherwise check if there's a mapping in the fields object
      // Finally fall back to the last part of the reference
      let field: string | undefined;
      const relations: Array<{
        name: string;
        type: "one" | "many";
        field?: string;
        nestedMapper?: { [key: string]: MapperConfig };
      }> = [];

      // Add the current relation
      relations.push({
        name,
        type,
        field: activeConfig.relation.field,
        nestedMapper: fields,
      });

      // Check for nested relations in the remaining parts
      if (fields && remainingParts.length > 0) {
        let currentMapper = fields;
        let currentParts = remainingParts;

        while (currentParts.length > 0) {
          const nextConfig = currentMapper[currentParts[0]];
          if (nextConfig?.relation) {
            relations.push({
              name: nextConfig.relation.name,
              type: nextConfig.relation.type,
              field: nextConfig.relation.field,
              nestedMapper: nextConfig.relation.fields,
            });
            currentMapper = nextConfig.relation.fields || {};
            currentParts = currentParts.slice(1);
          } else {
            // Found a field config or no config
            if (nextConfig?.field) {
              field = nextConfig.field;
            } else {
              field = currentParts[currentParts.length - 1];
            }
            break;
          }
        }
      }

      return {
        path: field ? [field] : remainingParts,
        relations,
      };
    }
    return { path: [activeConfig.field || reference] };
  }

  // Fallback to raw reference
  return { path: [reference] };
};

/**
 * Determines the appropriate Prisma operator based on relation type.
 * @param {object} relation - The relation configuration.
 * @returns {string} The Prisma relation operator.
 */
const getPrismaRelationOperator = (relation: {
  name: string;
  type: "one" | "many";
  field?: string;
}) => (relation.type === "one" ? "is" : "some");

/**
 * Builds Prisma WHERE conditions from a Cerbos expression.
 * @param {PlanExpressionOperand} expression - The Cerbos expression.
 * @param {Mapper} mapper - The mapper configuration.
 * @returns {PrismaFilter} The Prisma filter.
 */
const buildNestedRelationFilter = (
  relations: Array<{
    name: string;
    type: "one" | "many";
    field?: string;
    nestedMapper?: { [key: string]: MapperConfig };
  }>,
  fieldFilter: any
): any => {
  if (relations.length === 0) {
    return fieldFilter;
  }

  // Start with the innermost relation and work outward
  let currentFilter = fieldFilter;
  for (let i = relations.length - 1; i >= 0; i--) {
    const relation = relations[i];
    const relationOperator = getPrismaRelationOperator(relation);

    // When using a field in a relation, we want to apply it directly to the relation's filter
    if (relation.field && i === relations.length - 1) {
      // Only apply field name change at leaf level
      const [, filterValue] = Object.entries(currentFilter)[0];
      currentFilter = {
        [relation.field]: filterValue,
      };
    }

    // Build the filter for this relation level
    currentFilter = {
      [relation.name]: {
        [relationOperator]: currentFilter,
      },
    };
  }

  return currentFilter;
};

const buildPrismaFilterFromCerbosExpression = (
  expression: PlanExpressionOperand,
  mapper: Mapper
): PrismaFilter => {
  if (!("operator" in expression) || !("operands" in expression)) {
    throw new Error("Invalid Cerbos expression structure");
  }
  const { operator, operands } = expression;

  /**
   * Helper function to resolve an operand.
   * @param {PlanExpressionOperand} operand - The operand from expression.
   * @returns {any} An object with a field reference or value.
   */
  const resolveOperand = (operand: PlanExpressionOperand): any => {
    if ("name" in operand && operand.name) {
      return resolveFieldReference(operand.name, mapper);
    } else if ("value" in operand && operand.value !== undefined) {
      return { value: operand.value };
    } else if ("operator" in operand) {
      // Nested expression resolution
      const nestedResult = buildPrismaFilterFromCerbosExpression(
        operand,
        mapper
      );
      return { value: nestedResult };
    }
    throw new Error("Operand must have name, value, or be an expression");
  };

  // Process the operator type
  switch (operator) {
    case "and": {
      // Combine operands with logical AND
      return {
        AND: operands.map((operand) =>
          buildPrismaFilterFromCerbosExpression(operand, mapper)
        ),
      };
    }
    case "or": {
      // Combine operands with logical OR
      return {
        OR: operands.map((operand) =>
          buildPrismaFilterFromCerbosExpression(operand, mapper)
        ),
      };
    }
    case "not": {
      // Negate the operand filter
      return {
        NOT: buildPrismaFilterFromCerbosExpression(operands[0], mapper),
      };
    }
    case "eq":
    case "ne":
    case "lt":
    case "le":
    case "gt":
    case "ge": {
      // Relational operators: find left and right operands
      const leftOperand = operands.find((o) => "name" in o || "operator" in o);
      if (!leftOperand) throw new Error("No valid left operand found");
      const rightOperand = operands.find((o) => o !== leftOperand);
      if (!rightOperand) throw new Error("No valid right operand found");
      const left = resolveOperand(leftOperand);
      const right = resolveOperand(rightOperand);
      const prismaOperator = {
        eq: "equals",
        ne: "not",
        lt: "lt",
        le: "lte",
        gt: "gt",
        ge: "gte",
      }[operator];
      if ("path" in left) {
        const { path, relations } = left;
        const filterValue = { [prismaOperator]: right.value };
        const fieldFilter = {
          [path[path.length - 1]]: filterValue,
        };

        if (relations && relations.length > 0) {
          return buildNestedRelationFilter(relations, fieldFilter);
        }

        return fieldFilter;
      }
      return { [prismaOperator]: right.value };
    }
    case "in": {
      // For relations, the operands might be in reverse order (value first, then name)
      const nameOperand = operands.find((o) => "name" in o)!;
      const valueOperand = operands.find((o) => "value" in o)!;
      const { path, relations } = resolveOperand(nameOperand);
      const { value } = resolveOperand(valueOperand);

      // If we have relations, handle differently than direct field
      if (relations && relations.length > 0) {
        const fieldFilter = {
          [path[path.length - 1]]: value,
        };
        return buildNestedRelationFilter(relations, fieldFilter);
      }

      // Direct field case
      return {
        [path[path.length - 1]]: { in: value },
      };
    }
    case "contains":
    case "startsWith":
    case "endsWith": {
      const { path, relations } = resolveOperand(
        operands.find((o) => "name" in o)!
      );
      const { value } = resolveOperand(operands.find((o) => "value" in o)!);
      if (typeof value !== "string") {
        throw new Error(`${operator} operator requires string value`);
      }
      const fieldFilter = { [path[path.length - 1]]: { [operator]: value } };

      if (relations && relations.length > 0) {
        return buildNestedRelationFilter(relations, fieldFilter);
      }

      return fieldFilter;
    }
    case "isSet": {
      const { path, relations } = resolveOperand(
        operands.find((o) => "name" in o)!
      );
      const { value } = resolveOperand(operands.find((o) => "value" in o)!);
      const fieldFilter = {
        [path[path.length - 1]]: value ? { not: null } : { equals: null },
      };

      if (relations && relations.length > 0) {
        return buildNestedRelationFilter(relations, fieldFilter);
      }

      return fieldFilter;
    }
    case "hasIntersection": {
      if (operands.length !== 2)
        throw new Error("hasIntersection requires exactly two operands");
      const [leftOperand, rightOperand] = operands;

      // Handle map expressions as first operand
      if ("operator" in leftOperand && leftOperand.operator === "map") {
        if (!("value" in rightOperand))
          throw new Error("Second operand of hasIntersection must be a value");

        const [collection, lambda] = leftOperand.operands;
        if (!("name" in collection))
          throw new Error(
            "First operand of map must be a collection reference"
          );

        const { relations } = resolveFieldReference(collection.name, mapper);
        if (!relations || relations.length === 0)
          throw new Error("Map operation requires relations");

        // Extract the field name from the lambda expression
        if (!("operands" in lambda)) {
          throw new Error("Invalid lambda expression structure");
        }
        const [projection] = lambda.operands;
        if (!("name" in projection))
          throw new Error("Invalid map lambda expression structure");

        const fieldName = projection.name.split(".").pop()!;

        return buildNestedRelationFilter(relations, {
          [fieldName]: { in: rightOperand.value },
        });
      }

      // Handle direct field reference
      if (!("name" in leftOperand))
        throw new Error(
          "First operand of hasIntersection must be a field reference or map expression"
        );
      if (!("value" in rightOperand))
        throw new Error("Second operand of hasIntersection must be a value");

      const { path, relations } = resolveFieldReference(
        leftOperand.name,
        mapper
      );
      if (!Array.isArray(rightOperand.value))
        throw new Error("hasIntersection requires an array value");

      // For relation fields
      if (relations && relations.length > 0) {
        const fieldFilter = {
          [path[path.length - 1]]: { in: rightOperand.value },
        };
        // Use a single some operator through buildNestedRelationFilter
        return buildNestedRelationFilter(relations, fieldFilter);
      }

      // For direct array fields
      return {
        [path[path.length - 1]]: { some: rightOperand.value },
      };
    }
    case "lambda": {
      // Handle lambda expressions by replacing variable prefix on keys
      const [condition, variable] = operands;
      if (!("name" in variable))
        throw new Error("Lambda variable must have a name");
      return buildPrismaFilterFromCerbosExpression(
        condition,
        (key: string) => ({ field: key.replace(`${variable.name}.`, "") })
      );
    }
    case "exists":
    case "exists_one":
    case "all":
    case "except":
    case "filter": {
      if (operands.length !== 2)
        throw new Error(`${operator} requires exactly two operands`);
      const [collection, lambda] = operands;
      if (!("name" in collection))
        throw new Error(
          "First operand of exists/all/except must be a collection reference"
        );
      if (!("operator" in lambda))
        throw new Error(
          "Second operand of exists/all/except must be a lambda expression"
        );
      const { relations } = resolveFieldReference(collection.name, mapper);
      if (!relations)
        throw new Error(`${operator} operator requires a relation mapping`);

      // Process the lambda expression to create base filter
      const lambdaCondition = buildPrismaFilterFromCerbosExpression(
        lambda,
        mapper
      );

      const relation = relations[0];
      const filterField = relation.field || Object.keys(lambdaCondition)[0];
      const filterValue = lambdaCondition[Object.keys(lambdaCondition)[0]];

      switch (operator) {
        case "exists":
        case "filter":
          return {
            [relation.name]: {
              some: { [filterField]: filterValue },
            },
          };
        case "exists_one":
          return {
            [relation.name]: {
              some: { [filterField]: filterValue },
            },
            AND: [
              {
                [relation.name]: {
                  every: {
                    OR: [
                      { [filterField]: filterValue },
                      { NOT: { [filterField]: filterValue } },
                    ],
                  },
                },
              },
            ],
          };
        case "all":
          return {
            [relation.name]: {
              every: { [filterField]: filterValue },
            },
          };
        default:
          throw new Error(`Unexpected operator: ${operator}`);
      }
    }
    case "map": {
      // Process map operator to project field values
      if (operands.length !== 2)
        throw new Error("map requires exactly two operands");
      const [collection, lambda] = operands;
      if (!("name" in collection))
        throw new Error("First operand of map must be a collection reference");
      if (!("operator" in lambda) || lambda.operator !== "lambda")
        throw new Error("Second operand of map must be a lambda expression");
      const { relations } = resolveFieldReference(collection.name, mapper);
      if (!relations)
        throw new Error("map operator requires a relation mapping");
      const [projection, variable] = lambda.operands;
      if (!("name" in projection) || !("name" in variable))
        throw new Error("Invalid map lambda expression structure");
      return buildNestedRelationFilter(relations, {
        [getPrismaRelationOperator(relations[relations.length - 1])]: {
          select: {
            [projection.name.replace(`${variable.name}.`, "")]: true,
          },
        },
      });
    }
    default:
      throw new Error(`Unsupported operator: ${operator}`);
  }
};
