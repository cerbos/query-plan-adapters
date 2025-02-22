import {
  PlanResourcesResponse,
  PlanExpressionOperand,
  PlanKind,
  Value,
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
  switch (queryPlan.kind) {
    case PlanKind.ALWAYS_ALLOWED:
      return { kind: PlanKind.ALWAYS_ALLOWED };
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
 * Resolves a field reference considering relations and nested fields.
 * Handles both direct field mappings and relation mappings with support for nested structures.
 *
 * @param {string} reference - The field reference from Cerbos
 * @param {Mapper} mapper - Configuration for field/relation mapping
 * @returns {Object} Resolved reference with path and optional relations
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
  const config =
    typeof mapper === "function" ? mapper(reference) : mapper[reference];
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

  const activeConfig = config || matchedConfig!;
  if (activeConfig?.relation) {
    const { name, type, fields } = activeConfig.relation;
    const matchedParts = matchedPrefix ? matchedPrefix.split(".") : [];
    const remainingParts = matchedPrefix
      ? parts.slice(matchedParts.length)
      : parts.slice(1);

    let field: string | undefined;
    const relations: Array<{
      name: string;
      type: "one" | "many";
      field?: string;
      nestedMapper?: { [key: string]: MapperConfig };
    }> = [
      { name, type, field: activeConfig.relation.field, nestedMapper: fields },
    ];

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
          field = nextConfig?.field || currentParts[currentParts.length - 1];
          break;
        }
      }
    }

    return { path: field ? [field] : remainingParts, relations };
  }

  return { path: [activeConfig?.field || reference] };
};

/**
 * Determines the appropriate Prisma operator based on relation type.
 * Used for building relation filters in Prisma queries.
 *
 * @param {Object} relation - Relation configuration
 * @param {string} relation.name - Name of the relation
 * @param {('one'|'many')} relation.type - Type of relation
 * @returns {string} Prisma relation operator ('is' for one-to-one, 'some' for one-to-many)
 */
const getPrismaRelationOperator = (relation: {
  name: string;
  type: "one" | "many";
  field?: string;
}) => (relation.type === "one" ? "is" : "some");

/**
 * Builds a nested relation filter for Prisma queries.
 * Handles multiple levels of relations and applies appropriate operators.
 *
 * @param {Array} relations - Array of relation configurations
 * @param {Object} fieldFilter - Base filter to apply at the deepest level
 * @returns {Object} Nested Prisma filter structure
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
  if (relations.length === 0) return fieldFilter;

  let currentFilter = fieldFilter;
  for (let i = relations.length - 1; i >= 0; i--) {
    const relation = relations[i];
    const relationOperator = getPrismaRelationOperator(relation);

    if (relation.field && i === relations.length - 1) {
      const [, filterValue] = Object.entries(currentFilter)[0];
      currentFilter = { [relation.field]: filterValue };
    }

    currentFilter = { [relation.name]: { [relationOperator]: currentFilter } };
  }

  return currentFilter;
};

type ResolvedFieldReference = {
  path: string[];
  relations?: Array<{
    name: string;
    type: "one" | "many";
    field?: string;
    nestedMapper?: { [key: string]: MapperConfig };
  }>;
};

type ResolvedValue = {
  value: any;
};

type ResolvedOperand = ResolvedFieldReference | ResolvedValue;

/**
 * Type guard to check if an operand is a resolved field reference.
 * @param {ResolvedOperand} operand - Operand to check
 * @returns {boolean} True if operand is a field reference
 */
function isResolvedFieldReference(
  operand: ResolvedOperand
): operand is ResolvedFieldReference {
  return "path" in operand;
}

/**
 * Type guard to check if an operand is a resolved value.
 * @param {ResolvedOperand} operand - Operand to check
 * @returns {boolean} True if operand is a value
 */
function isResolvedValue(operand: ResolvedOperand): operand is ResolvedValue {
  return "value" in operand;
}

/**
 * Type guard for PlanExpressionOperand with name property.
 * @param {PlanExpressionOperand} operand - Operand to check
 * @returns {boolean} True if operand has name property
 */
function isPlanExpressionWithName(
  operand: PlanExpressionOperand
): operand is { name: string } {
  return "name" in operand && typeof operand.name === "string";
}

/**
 * Type guard for PlanExpressionOperand with value property.
 * @param {PlanExpressionOperand} operand - Operand to check
 * @returns {boolean} True if operand has value property
 */
function isPlanExpressionWithValue(
  operand: PlanExpressionOperand
): operand is { value: Value } {
  return "value" in operand && operand.value !== undefined;
}

/**
 * Type guard for PlanExpressionOperand with operator property.
 * @param {PlanExpressionOperand} operand - Operand to check
 * @returns {boolean} True if operand has operator and operands properties
 */
function isPlanExpressionWithOperator(
  operand: PlanExpressionOperand
): operand is { operator: string; operands: PlanExpressionOperand[] } {
  return (
    "operator" in operand &&
    typeof operand.operator === "string" &&
    "operands" in operand &&
    Array.isArray(operand.operands)
  );
}

/**
 * Resolves a PlanExpressionOperand into a ResolvedOperand.
 * Handles field references, values, and nested expressions.
 *
 * @param {PlanExpressionOperand} operand - Operand to resolve
 * @param {Mapper} mapper - Field mapping configuration
 * @returns {ResolvedOperand} Resolved operand
 * @throws {Error} If operand is invalid
 */
const resolveOperand = (
  operand: PlanExpressionOperand,
  mapper: Mapper
): ResolvedOperand => {
  if (isPlanExpressionWithName(operand)) {
    return resolveFieldReference(operand.name, mapper);
  } else if (isPlanExpressionWithValue(operand)) {
    return { value: operand.value };
  } else if (isPlanExpressionWithOperator(operand)) {
    const nestedResult = buildPrismaFilterFromCerbosExpression(operand, mapper);
    return { value: nestedResult };
  }
  throw new Error("Operand must have name, value, or be an expression");
};

/**
 * Builds a Prisma filter from a Cerbos expression.
 * Main function for converting Cerbos query conditions to Prisma filters.
 *
 * @param {PlanExpressionOperand} expression - Cerbos expression to convert
 * @param {Mapper} mapper - Field mapping configuration
 * @returns {PrismaFilter} Prisma compatible filter
 * @throws {Error} If expression is invalid or operator is unsupported
 */
const buildPrismaFilterFromCerbosExpression = (
  expression: PlanExpressionOperand,
  mapper: Mapper
): PrismaFilter => {
  // Validate expression structure
  if (!("operator" in expression) || !("operands" in expression)) {
    throw new Error("Invalid Cerbos expression structure");
  }
  const { operator, operands } = expression;

  /**
   * Helper function to process relational operators (eq, ne, lt, etc.)
   * @param {string} operator - Relational operator
   * @param {ResolvedOperand} left - Left operand
   * @param {ResolvedOperand} right - Right operand
   * @returns {PrismaFilter} Prisma filter for the relation
   */
  const processRelationalOperator = (
    operator: string,
    left: ResolvedOperand,
    right: ResolvedOperand
  ) => {
    const prismaOperator = {
      eq: "equals",
      ne: "not",
      lt: "lt",
      le: "lte",
      gt: "gt",
      ge: "gte",
    }[operator];
    if (isResolvedFieldReference(left)) {
      const { path, relations } = left;
      if (!isResolvedValue(right)) {
        throw new Error("Right operand must be a value");
      }
      if (!prismaOperator) {
        throw new Error(`Unsupported operator: ${operator}`);
      }
      const filterValue = { [prismaOperator]: right.value };
      const fieldFilter = { [path[path.length - 1]]: filterValue };

      if (relations && relations.length > 0) {
        return buildNestedRelationFilter(relations, fieldFilter);
      }

      return fieldFilter;
    }
    if (!isResolvedValue(right)) {
      throw new Error("Right operand must be a value");
    }
    if (!prismaOperator) {
      throw new Error(`Unsupported operator: ${operator}`);
    }
    return { [prismaOperator]: right.value };
  };

  // Process different operator types
  switch (operator) {
    case "and":
      return {
        AND: operands.map((operand) =>
          buildPrismaFilterFromCerbosExpression(operand, mapper)
        ),
      };
    case "or":
      return {
        OR: operands.map((operand) =>
          buildPrismaFilterFromCerbosExpression(operand, mapper)
        ),
      };
    case "not":
      return {
        NOT: buildPrismaFilterFromCerbosExpression(operands[0], mapper),
      };
    case "eq":
    case "ne":
    case "lt":
    case "le":
    case "gt":
    case "ge": {
      const leftOperand = operands.find(
        (o) => isPlanExpressionWithName(o) || isPlanExpressionWithOperator(o)
      );
      if (!leftOperand) throw new Error("No valid left operand found");
      const rightOperand = operands.find((o) => o !== leftOperand);
      if (!rightOperand) throw new Error("No valid right operand found");
      const left = resolveOperand(leftOperand, mapper);
      const right = resolveOperand(rightOperand, mapper);
      return processRelationalOperator(operator, left, right);
    }
    case "in": {
      const nameOperand = operands.find(isPlanExpressionWithName);
      if (!nameOperand) throw new Error("Name operand is undefined");
      const valueOperand = operands.find(isPlanExpressionWithValue);
      if (!valueOperand) throw new Error("Value operand is undefined");

      const resolved = resolveOperand(nameOperand, mapper);
      if (!isResolvedFieldReference(resolved))
        throw new Error("Name operand must resolve to a field reference");
      const { path, relations } = resolved;
      const resolvedValue = resolveOperand(valueOperand, mapper);
      if (!isResolvedValue(resolvedValue))
        throw new Error("Value operand must resolve to a value");
      const { value } = resolvedValue;

      if (relations && relations.length > 0) {
        const fieldFilter = { [path[path.length - 1]]: value };
        return buildNestedRelationFilter(relations, fieldFilter);
      }

      return { [path[path.length - 1]]: { in: value } };
    }
    case "contains":
    case "startsWith":
    case "endsWith": {
      const nameOperand = operands.find(isPlanExpressionWithName);
      if (!nameOperand) throw new Error("Name operand is undefined");
      const resolved = resolveOperand(nameOperand, mapper);
      if (!isResolvedFieldReference(resolved))
        throw new Error("Name operand must resolve to a field reference");
      const { path, relations } = resolved;

      const valueOperand = operands.find(isPlanExpressionWithValue);
      if (!valueOperand) throw new Error("Value operand is undefined");
      const resolvedValue = resolveOperand(valueOperand, mapper);
      if (!isResolvedValue(resolvedValue))
        throw new Error("Value operand must resolve to a value");
      const { value } = resolvedValue;
      if (typeof value !== "string")
        throw new Error(`${operator} operator requires string value`);
      const fieldFilter = { [path[path.length - 1]]: { [operator]: value } };

      if (relations && relations.length > 0) {
        return buildNestedRelationFilter(relations, fieldFilter);
      }

      return fieldFilter;
    }
    case "isSet": {
      const nameOperand = operands.find(isPlanExpressionWithName);
      if (!nameOperand) throw new Error("Name operand is undefined");
      const resolved = resolveOperand(nameOperand, mapper);
      if (!isResolvedFieldReference(resolved))
        throw new Error("Name operand must resolve to a field reference");
      const { path, relations } = resolved;

      const valueOperand = operands.find(isPlanExpressionWithValue);
      if (!valueOperand) throw new Error("Value operand is undefined");
      const resolvedValue = resolveOperand(valueOperand, mapper);
      const fieldFilter = {
        [path[path.length - 1]]: resolvedValue
          ? { not: null }
          : { equals: null },
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

      if ("operator" in leftOperand && leftOperand.operator === "map") {
        if (!("value" in rightOperand))
          throw new Error("Second operand of hasIntersection must be a value");

        const [collection, lambda] = leftOperand.operands;
        if (!("name" in collection))
          throw new Error(
            "First operand of map must be a collection reference"
          );

        // Get variable name from lambda
        if (!isPlanExpressionWithOperator(lambda)) {
          throw new Error("Lambda expression must have operands");
        }
        const [, variable] = lambda.operands;
        if (!("name" in variable))
          throw new Error("Lambda variable must have a name");

        // Create scoped mapper for the collection
        const scopedMapper = createScopedMapper(
          collection.name,
          variable.name,
          mapper
        );

        const { relations } = resolveFieldReference(collection.name, mapper);
        if (!relations || relations.length === 0)
          throw new Error("Map operation requires relations");

        if (!("operands" in lambda))
          throw new Error("Invalid lambda expression structure");
        const [projection] = lambda.operands;
        if (!("name" in projection))
          throw new Error("Invalid map lambda expression structure");

        // Use scoped mapper for resolving the projection
        const resolved = resolveFieldReference(projection.name, scopedMapper);
        const fieldName = resolved.path[resolved.path.length - 1];

        return buildNestedRelationFilter(relations, {
          [fieldName]: { in: rightOperand.value },
        });
      }

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

      if (relations && relations.length > 0) {
        const fieldFilter = {
          [path[path.length - 1]]: { in: rightOperand.value },
        };
        return buildNestedRelationFilter(relations, fieldFilter);
      }

      return { [path[path.length - 1]]: { some: rightOperand.value } };
    }
    case "lambda": {
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

      // Get variable name from lambda
      const [, variable] = lambda.operands;
      if (!("name" in variable))
        throw new Error("Lambda variable must have a name");

      // Create scoped mapper for the collection
      const scopedMapper = createScopedMapper(
        collection.name,
        variable.name,
        mapper
      );

      const { relations } = resolveFieldReference(collection.name, mapper);
      if (!relations)
        throw new Error(`${operator} operator requires a relation mapping`);

      const lambdaCondition = buildPrismaFilterFromCerbosExpression(
        lambda.operands[0], // Use the condition part of the lambda
        scopedMapper
      );

      const relation = relations[0];
      let filterValue = lambdaCondition;

      // If the lambda condition already has a relation structure, merge it
      if (lambdaCondition.AND || lambdaCondition.OR) {
        filterValue = lambdaCondition;
      } else {
        const filterField = relation.field || Object.keys(lambdaCondition)[0];
        filterValue = {
          [filterField]: lambdaCondition[Object.keys(lambdaCondition)[0]],
        };
      }

      switch (operator) {
        case "exists":
        case "filter":
          return { [relation.name]: { some: filterValue } };
        case "exists_one":
          return {
            [relation.name]: {
              some: filterValue,
            },
            AND: [
              {
                [relation.name]: {
                  every: {
                    OR: [filterValue, { NOT: filterValue }],
                  },
                },
              },
            ],
          };
        case "all":
          return { [relation.name]: { every: filterValue } };
        default:
          throw new Error(`Unexpected operator: ${operator}`);
      }
    }
    case "map": {
      if (operands.length !== 2)
        throw new Error("map requires exactly two operands");
      const [collection, lambda] = operands;
      if (!("name" in collection))
        throw new Error("First operand of map must be a collection reference");
      if (!("operator" in lambda) || lambda.operator !== "lambda")
        throw new Error("Second operand of map must be a lambda expression");

      // Get variable name from lambda
      const [projection, variable] = lambda.operands;
      if (!("name" in projection) || !("name" in variable))
        throw new Error("Invalid map lambda expression structure");

      // Create scoped mapper for the collection
      const scopedMapper = createScopedMapper(
        collection.name,
        variable.name,
        mapper
      );

      const { relations } = resolveFieldReference(collection.name, mapper);
      if (!relations)
        throw new Error("map operator requires a relation mapping");

      // Use scoped mapper for resolving the projection
      const resolved = resolveFieldReference(projection.name, scopedMapper);
      const fieldName = resolved.path[resolved.path.length - 1];

      return buildNestedRelationFilter(relations, {
        [getPrismaRelationOperator(relations[relations.length - 1])]: {
          select: { [fieldName]: true },
        },
      });
    }
    default:
      throw new Error(`Unsupported operator: ${operator}`);
  }
};

/**
 * Creates a scoped mapper for collection operations that preserves access to parent scope
 * and handles nested collections
 */
const createScopedMapper =
  (collectionPath: string, variableName: string, fullMapper: Mapper): Mapper =>
  (key: string) => {
    // If the key starts with the variable name, it's accessing the collection item
    if (key.startsWith(variableName + ".")) {
      const strippedKey = key.replace(variableName + ".", "");
      const parts = strippedKey.split(".");

      // Get the collection's relation config
      const collectionConfig =
        typeof fullMapper === "function"
          ? fullMapper(collectionPath)
          : fullMapper[collectionPath];

      if (collectionConfig?.relation?.fields) {
        // For nested paths, traverse the fields configuration
        let currentConfig = collectionConfig.relation.fields;
        let field = parts[0];

        for (let i = 0; i < parts.length - 1; i++) {
          const part = parts[i];
          if (currentConfig[part]?.relation?.fields) {
            currentConfig = currentConfig[part].relation.fields;
            field = parts[i + 1];
          } else {
            break;
          }
        }

        // Return the field config if it exists, otherwise create a default one
        return currentConfig[field] || { field };
      }
      return { field: strippedKey };
    }

    // For keys not referencing the collection item, use the full mapper
    return typeof fullMapper === "function" ? fullMapper(key) : fullMapper[key];
  };
