import {
  PlanResourcesResponse,
  PlanExpressionOperand,
  PlanKind,
  Value,
} from "@cerbos/core";

export { PlanKind };

// Type Definitions
export type PrismaFilter = Record<string, any>;

export type MapperConfig = {
  field?: string;
  relation?: {
    name: string;
    type: "one" | "many";
    field?: string;
    fields?: Record<string, MapperConfig>;
  };
};

export type Mapper =
  | Record<string, MapperConfig>
  | ((key: string) => MapperConfig);

export interface QueryPlanToPrismaArgs {
  queryPlan: PlanResourcesResponse;
  mapper?: Mapper;
}

export type QueryPlanToPrismaResult =
  | {
      kind: PlanKind.ALWAYS_ALLOWED | PlanKind.ALWAYS_DENIED;
    }
  | {
      kind: PlanKind.CONDITIONAL;
      filters: PrismaFilter;
    };

// Type guards for operands
interface NamedOperand {
  name: string;
}

interface ValueOperand {
  value: Value;
}

interface OperatorOperand {
  operator: string;
  operands: PlanExpressionOperand[];
}

function isNamedOperand(
  operand: PlanExpressionOperand
): operand is NamedOperand {
  return "name" in operand && typeof operand.name === "string";
}

function isValueOperand(
  operand: PlanExpressionOperand
): operand is ValueOperand {
  return "value" in operand && operand.value !== undefined;
}

function isOperatorOperand(
  operand: PlanExpressionOperand
): operand is OperatorOperand {
  return (
    "operator" in operand &&
    typeof operand.operator === "string" &&
    "operands" in operand &&
    Array.isArray(operand.operands)
  );
}

// Field reference resolution types
type RelationConfig = {
  name: string;
  type: "one" | "many";
  field?: string;
  nestedMapper?: Record<string, MapperConfig>;
};

type ResolvedFieldReference = {
  path: string[];
  relations?: RelationConfig[];
};

type ResolvedValue = {
  value: any;
};

type ResolvedOperand = ResolvedFieldReference | ResolvedValue;

function isResolvedFieldReference(
  operand: ResolvedOperand
): operand is ResolvedFieldReference {
  return "path" in operand;
}

function isResolvedValue(operand: ResolvedOperand): operand is ResolvedValue {
  return "value" in operand;
}

/**
 * Converts a Cerbos query plan to a Prisma filter.
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
 */
function resolveFieldReference(
  reference: string,
  mapper: Mapper
): ResolvedFieldReference {
  const parts = reference.split(".");
  const config =
    typeof mapper === "function" ? mapper(reference) : mapper[reference];

  let matchedPrefix = "";
  let matchedConfig: MapperConfig | undefined;

  // If no direct match, look for partial matches
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

  // Handle relation mapping
  if (activeConfig?.relation) {
    const { name, type, fields } = activeConfig.relation;
    const matchedParts = matchedPrefix ? matchedPrefix.split(".") : [];
    const remainingParts = matchedPrefix
      ? parts.slice(matchedParts.length)
      : parts.slice(1);

    let field: string | undefined;
    const relations: RelationConfig[] = [
      {
        name,
        type,
        field: activeConfig.relation.field,
        nestedMapper: fields,
      },
    ];

    // Process nested relations
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

  // Simple field mapping
  return { path: [activeConfig?.field || reference] };
}

/**
 * Determines the appropriate Prisma operator based on relation type.
 */
function getPrismaRelationOperator(relation: {
  name: string;
  type: "one" | "many";
  field?: string;
}): string {
  return relation.type === "one" ? "is" : "some";
}

/**
 * Builds a nested relation filter for Prisma queries.
 */
function buildNestedRelationFilter(
  relations: RelationConfig[],
  fieldFilter: any
): any {
  if (relations.length === 0) return fieldFilter;

  let currentFilter = fieldFilter;

  // Build nested structure from inside out
  for (let i = relations.length - 1; i >= 0; i--) {
    const relation = relations[i];
    const relationOperator = getPrismaRelationOperator(relation);

    // Handle special case for the deepest relation
    if (relation.field && i === relations.length - 1) {
      const [, filterValue] = Object.entries(currentFilter)[0];
      currentFilter = { [relation.field]: filterValue };
    }

    currentFilter = { [relation.name]: { [relationOperator]: currentFilter } };
  }

  return currentFilter;
}

/**
 * Resolves a PlanExpressionOperand into a ResolvedOperand.
 */
function resolveOperand(
  operand: PlanExpressionOperand,
  mapper: Mapper
): ResolvedOperand {
  if (isNamedOperand(operand)) {
    return resolveFieldReference(operand.name, mapper);
  } else if (isValueOperand(operand)) {
    return { value: operand.value };
  } else if (isOperatorOperand(operand)) {
    const nestedResult = buildPrismaFilterFromCerbosExpression(operand, mapper);
    return { value: nestedResult };
  }
  throw new Error("Operand must have name, value, or be an expression");
}

/**
 * Creates a scoped mapper for collection operations
 */
function createScopedMapper(
  collectionPath: string,
  variableName: string,
  fullMapper: Mapper
): Mapper {
  return (key: string) => {
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
}

/**
 * Builds a Prisma filter from a Cerbos expression.
 */
function buildPrismaFilterFromCerbosExpression(
  expression: PlanExpressionOperand,
  mapper: Mapper
): PrismaFilter {
  // Validate expression structure
  if (!isOperatorOperand(expression)) {
    throw new Error("Invalid Cerbos expression structure");
  }

  const { operator, operands } = expression;

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
      return handleRelationalOperator(operator, operands, mapper);
    }

    case "in": {
      return handleInOperator(operands, mapper);
    }

    case "contains":
    case "startsWith":
    case "endsWith": {
      return handleStringOperator(operator, operands, mapper);
    }

    case "isSet": {
      return handleIsSetOperator(operands, mapper);
    }

    case "hasIntersection": {
      return handleHasIntersectionOperator(operands, mapper);
    }

    case "lambda": {
      return handleLambdaOperator(operands);
    }

    case "exists":
    case "exists_one":
    case "all":
    case "except":
    case "filter": {
      return handleCollectionOperator(operator, operands, mapper);
    }

    case "map": {
      return handleMapOperator(operands, mapper);
    }

    default:
      throw new Error(`Unsupported operator: ${operator}`);
  }
}

/**
 * Helper function to process relational operators (eq, ne, lt, etc.)
 */
function handleRelationalOperator(
  operator: string,
  operands: PlanExpressionOperand[],
  mapper: Mapper
): PrismaFilter {
  const prismaOperator = {
    eq: "equals",
    ne: "not",
    lt: "lt",
    le: "lte",
    gt: "gt",
    ge: "gte",
  }[operator];

  if (!prismaOperator) {
    throw new Error(`Unsupported operator: ${operator}`);
  }

  const leftOperand = operands.find(
    (o) => isNamedOperand(o) || isOperatorOperand(o)
  );
  if (!leftOperand) throw new Error("No valid left operand found");

  const rightOperand = operands.find((o) => o !== leftOperand);
  if (!rightOperand) throw new Error("No valid right operand found");

  const left = resolveOperand(leftOperand, mapper);
  const right = resolveOperand(rightOperand, mapper);

  if (isResolvedFieldReference(left)) {
    const { path, relations } = left;

    if (!isResolvedValue(right)) {
      throw new Error("Right operand must be a value");
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

  return { [prismaOperator]: right.value };
}

/**
 * Helper function to handle "in" operator
 */
function handleInOperator(
  operands: PlanExpressionOperand[],
  mapper: Mapper
): PrismaFilter {
  const nameOperand = operands.find(isNamedOperand);
  if (!nameOperand) throw new Error("Name operand is undefined");

  const valueOperand = operands.find(isValueOperand);
  if (!valueOperand) throw new Error("Value operand is undefined");

  const resolved = resolveOperand(nameOperand, mapper);
  if (!isResolvedFieldReference(resolved)) {
    throw new Error("Name operand must resolve to a field reference");
  }

  const { path, relations } = resolved;
  const resolvedValue = resolveOperand(valueOperand, mapper);

  if (!isResolvedValue(resolvedValue)) {
    throw new Error("Value operand must resolve to a value");
  }

  const { value } = resolvedValue;

  if (relations && relations.length > 0) {
    const fieldFilter = { [path[path.length - 1]]: value };
    return buildNestedRelationFilter(relations, fieldFilter);
  }

  return { [path[path.length - 1]]: { in: value } };
}

/**
 * Helper function to handle string operators (contains, startsWith, endsWith)
 */
function handleStringOperator(
  operator: string,
  operands: PlanExpressionOperand[],
  mapper: Mapper
): PrismaFilter {
  const nameOperand = operands.find(isNamedOperand);
  if (!nameOperand) throw new Error("Name operand is undefined");

  const resolved = resolveOperand(nameOperand, mapper);
  if (!isResolvedFieldReference(resolved)) {
    throw new Error("Name operand must resolve to a field reference");
  }

  const { path, relations } = resolved;

  const valueOperand = operands.find(isValueOperand);
  if (!valueOperand) throw new Error("Value operand is undefined");

  const resolvedValue = resolveOperand(valueOperand, mapper);
  if (!isResolvedValue(resolvedValue)) {
    throw new Error("Value operand must resolve to a value");
  }

  const { value } = resolvedValue;
  if (typeof value !== "string") {
    throw new Error(`${operator} operator requires string value`);
  }

  const fieldFilter = { [path[path.length - 1]]: { [operator]: value } };

  if (relations && relations.length > 0) {
    return buildNestedRelationFilter(relations, fieldFilter);
  }

  return fieldFilter;
}

/**
 * Helper function to handle "isSet" operator
 */
function handleIsSetOperator(
  operands: PlanExpressionOperand[],
  mapper: Mapper
): PrismaFilter {
  const nameOperand = operands.find(isNamedOperand);
  if (!nameOperand) throw new Error("Name operand is undefined");

  const resolved = resolveOperand(nameOperand, mapper);
  if (!isResolvedFieldReference(resolved)) {
    throw new Error("Name operand must resolve to a field reference");
  }

  const { path, relations } = resolved;

  const valueOperand = operands.find(isValueOperand);
  if (!valueOperand) throw new Error("Value operand is undefined");

  const resolvedValue = resolveOperand(valueOperand, mapper);
  if (!isResolvedValue(resolvedValue)) {
    throw new Error("Value operand must resolve to a value");
  }

  const fieldFilter = {
    [path[path.length - 1]]: resolvedValue.value
      ? { not: null }
      : { equals: null },
  };

  if (relations && relations.length > 0) {
    return buildNestedRelationFilter(relations, fieldFilter);
  }

  return fieldFilter;
}

/**
 * Helper function to handle "hasIntersection" operator
 */
function handleHasIntersectionOperator(
  operands: PlanExpressionOperand[],
  mapper: Mapper
): PrismaFilter {
  if (operands.length !== 2) {
    throw new Error("hasIntersection requires exactly two operands");
  }

  const [leftOperand, rightOperand] = operands;

  // Check if left operand is a map operation
  if (isOperatorOperand(leftOperand) && leftOperand.operator === "map") {
    if (!isValueOperand(rightOperand)) {
      throw new Error("Second operand of hasIntersection must be a value");
    }

    const [collection, lambda] = leftOperand.operands;
    if (!isNamedOperand(collection)) {
      throw new Error("First operand of map must be a collection reference");
    }

    // Get variable name from lambda
    if (!isOperatorOperand(lambda)) {
      throw new Error("Lambda expression must have operands");
    }

    const [, variable] = lambda.operands;
    if (!isNamedOperand(variable)) {
      throw new Error("Lambda variable must have a name");
    }

    // Create scoped mapper for the collection
    const scopedMapper = createScopedMapper(
      collection.name,
      variable.name,
      mapper
    );

    const { relations } = resolveFieldReference(collection.name, mapper);
    if (!relations || relations.length === 0) {
      throw new Error("Map operation requires relations");
    }

    if (!isOperatorOperand(lambda)) {
      throw new Error("Invalid lambda expression structure");
    }

    const [projection] = lambda.operands;
    if (!isNamedOperand(projection)) {
      throw new Error("Invalid map lambda expression structure");
    }

    // Use scoped mapper for resolving the projection
    const resolved = resolveFieldReference(projection.name, scopedMapper);
    const fieldName = resolved.path[resolved.path.length - 1];

    return buildNestedRelationFilter(relations, {
      [fieldName]: { in: rightOperand.value },
    });
  }

  // Handle regular field reference
  if (!isNamedOperand(leftOperand)) {
    throw new Error(
      "First operand of hasIntersection must be a field reference or map expression"
    );
  }

  if (!isValueOperand(rightOperand)) {
    throw new Error("Second operand of hasIntersection must be a value");
  }

  const { path, relations } = resolveFieldReference(leftOperand.name, mapper);

  if (!Array.isArray(rightOperand.value)) {
    throw new Error("hasIntersection requires an array value");
  }

  if (relations && relations.length > 0) {
    const fieldFilter = {
      [path[path.length - 1]]: { in: rightOperand.value },
    };
    return buildNestedRelationFilter(relations, fieldFilter);
  }

  return { [path[path.length - 1]]: { some: rightOperand.value } };
}

/**
 * Helper function to handle "lambda" operator
 */
function handleLambdaOperator(operands: PlanExpressionOperand[]): PrismaFilter {
  const [condition, variable] = operands;

  if (!isNamedOperand(variable)) {
    throw new Error("Lambda variable must have a name");
  }

  return buildPrismaFilterFromCerbosExpression(condition, (key: string) => ({
    field: key.replace(`${variable.name}.`, ""),
  }));
}

/**
 * Helper function to handle collection operators (exists, all, except, filter)
 */
function handleCollectionOperator(
  operator: string,
  operands: PlanExpressionOperand[],
  mapper: Mapper
): PrismaFilter {
  if (operands.length !== 2) {
    throw new Error(`${operator} requires exactly two operands`);
  }

  const [collection, lambda] = operands;

  if (!isNamedOperand(collection)) {
    throw new Error(
      `First operand of ${operator} must be a collection reference`
    );
  }

  if (!isOperatorOperand(lambda)) {
    throw new Error(
      `Second operand of ${operator} must be a lambda expression`
    );
  }

  // Get variable name from lambda
  const [, variable] = lambda.operands;
  if (!isNamedOperand(variable)) {
    throw new Error("Lambda variable must have a name");
  }

  // Create scoped mapper for the collection
  const scopedMapper = createScopedMapper(
    collection.name,
    variable.name,
    mapper
  );

  const { relations } = resolveFieldReference(collection.name, mapper);
  if (!relations) {
    throw new Error(`${operator} operator requires a relation mapping`);
  }

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

/**
 * Helper function to handle "map" operator
 */
function handleMapOperator(
  operands: PlanExpressionOperand[],
  mapper: Mapper
): PrismaFilter {
  if (operands.length !== 2) {
    throw new Error("map requires exactly two operands");
  }

  const [collection, lambda] = operands;

  if (!isNamedOperand(collection)) {
    throw new Error("First operand of map must be a collection reference");
  }

  if (!isOperatorOperand(lambda) || lambda.operator !== "lambda") {
    throw new Error("Second operand of map must be a lambda expression");
  }

  // Get variable name from lambda
  const [projection, variable] = lambda.operands;
  if (!isNamedOperand(projection) || !isNamedOperand(variable)) {
    throw new Error("Invalid map lambda expression structure");
  }

  // Create scoped mapper for the collection
  const scopedMapper = createScopedMapper(
    collection.name,
    variable.name,
    mapper
  );

  const { relations } = resolveFieldReference(collection.name, mapper);
  if (!relations) {
    throw new Error("map operator requires a relation mapping");
  }

  // Use scoped mapper for resolving the projection
  const resolved = resolveFieldReference(projection.name, scopedMapper);
  const fieldName = resolved.path[resolved.path.length - 1];

  return buildNestedRelationFilter(relations, {
    [getPrismaRelationOperator(relations[relations.length - 1])]: {
      select: { [fieldName]: true },
    },
  });
}
