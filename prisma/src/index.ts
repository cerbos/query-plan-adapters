import {
  PlanResourcesResponse,
  PlanExpressionOperand,
  PlanKind,
  Value,
} from "@cerbos/core";

export { PlanKind };

const CERBOS_TO_PRISMA_OPERATOR: Record<string, string> = {
  eq: "equals",
  ne: "not",
  lt: "lt",
  le: "lte",
  gt: "gt",
  ge: "gte",
};

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

function assertDefined<T>(value: T | undefined, message: string): T {
  if (value === undefined) {
    throw new Error(message);
  }
  return value;
}

function getLeafField(path: string[]): string {
  const fieldName = path[path.length - 1];
  if (!fieldName) {
    throw new Error("Field path cannot be empty");
  }
  return fieldName;
}

function getFilterEntry(filter: Record<string, unknown>): [string, unknown] {
  const entry = Object.entries(filter)[0];
  if (!entry) {
    throw new Error("Filter must contain at least one entry");
  }
  return entry;
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

  const activeConfig = config ?? matchedConfig;

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
      let currentMapper: Record<string, MapperConfig> | undefined = fields;
      let currentParts = remainingParts;

      while (currentParts.length > 0) {
        if (!currentMapper) {
          break;
        }

        const currentPart = currentParts[0];
        if (!currentPart) {
          break;
        }

        const nextConfig: MapperConfig | undefined = currentMapper[currentPart];
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
          const lastPart = currentParts[currentParts.length - 1];
          if (!lastPart) {
            break;
          }
          field = nextConfig?.field || lastPart;
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
    if (!relation) {
      throw new Error("Relation mapping is missing");
    }
    const relationOperator = getPrismaRelationOperator(relation);

    // Handle special case for the deepest relation
    if (relation.field && i === relations.length - 1) {
      const [key, filterValue] = getFilterEntry(currentFilter);
      if (key === "NOT") {
        currentFilter = { NOT: { [relation.field]: filterValue } };
      } else {
        currentFilter = { [relation.field]: filterValue };
      }
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
    const folded = tryFoldValueExpression(operand, mapper);
    if (folded !== null) return { value: folded };
    const nestedResult = buildPrismaFilterFromCerbosExpression(operand, mapper);
    return { value: nestedResult };
  }
  throw new Error("Operand must have name, value, or be an expression");
}

function tryFoldValueExpression(
  expr: OperatorOperand,
  mapper: Mapper
): Value | null {
  if (expr.operator !== "add") return null;
  const leftOp = expr.operands[0];
  const rightOp = expr.operands[1];
  if (!leftOp || !rightOp) return null;

  const left = resolveOperand(leftOp, mapper);
  if (!isResolvedValue(left)) return null;
  const right = resolveOperand(rightOp, mapper);
  if (!isResolvedValue(right)) return null;

  try {
    return foldAdd(left.value, right.value);
  } catch {
    return null;
  }
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
        const baseConfig = collectionConfig.relation.fields;
        if (!baseConfig) {
          return { field: strippedKey };
        }

        let currentConfig = baseConfig;
        let field = parts[0] || strippedKey;

        for (let i = 0; i < parts.length - 1; i++) {
          const part = parts[i];
          const nextPart = parts[i + 1];
          if (!part || !nextPart) {
            break;
          }

          const nextConfig = currentConfig[part];
          if (nextConfig?.relation?.fields) {
            currentConfig = nextConfig.relation.fields;
            field = nextPart;
          } else {
            break;
          }
        }

        if (!field) {
          field = strippedKey;
        }

        // Return the field config if it exists, otherwise create a default one
        return currentConfig[field] || { field };
      }
      return { field: strippedKey };
    }

    // For keys not referencing the collection item, use the full mapper
    if (typeof fullMapper === "function") {
      return fullMapper(key);
    }
    return fullMapper[key] || { field: key };
  };
}

/**
 * Builds a Prisma filter from a Cerbos expression.
 */
function buildPrismaFilterFromCerbosExpression(
  expression: PlanExpressionOperand,
  mapper: Mapper
): PrismaFilter {
  // A bare named operand represents a boolean field reference (e.g. `R.attr.booleanAttr`)
  if (isNamedOperand(expression)) {
    const { path, relations } = resolveFieldReference(expression.name, mapper);
    const fieldName = getLeafField(path);
    const fieldFilter = { [fieldName]: { equals: true } };
    if (relations && relations.length > 0) {
      return buildNestedRelationFilter(relations, fieldFilter);
    }
    return fieldFilter;
  }

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

    case "not": {
      const operand = operands[0];
      if (!operand) {
        throw new Error("not operator requires an operand");
      }
      if (isNamedOperand(operand)) {
        const { path, relations } = resolveFieldReference(
          operand.name,
          mapper
        );
        if (!relations || relations.length === 0) {
          const fieldName = getLeafField(path);
          return { [fieldName]: { equals: false } };
        }
      }
      return {
        NOT: buildPrismaFilterFromCerbosExpression(operand, mapper),
      };
    }

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

    case "overlaps": {
      return handleOverlapsOperator(operands, mapper);
    }

    case "ancestorOf": {
      return handleAncestorDescendantOperator(operands, mapper, "ancestor");
    }

    case "descendentOf": {
      return handleAncestorDescendantOperator(operands, mapper, "descendant");
    }

    default:
      throw new Error(`Unsupported operator: ${operator}`);
  }
}

function handleSizeComparison(
  operator: string,
  sizeOperand: OperatorOperand,
  valueOperand: PlanExpressionOperand,
  mapper: Mapper
): PrismaFilter {
  const collectionOperand = sizeOperand.operands[0];
  if (!collectionOperand || !isNamedOperand(collectionOperand)) {
    throw new Error("size operator requires a named collection operand");
  }

  if (!isValueOperand(valueOperand)) {
    throw new Error("size comparison requires a numeric value operand");
  }

  const count = valueOperand.value;
  if (typeof count !== "number") {
    throw new Error("size comparison requires a numeric value");
  }

  const isNonEmpty =
    (operator === "gt" && count === 0) || (operator === "ge" && count === 1);

  const isEmpty =
    (operator === "eq" && count === 0) ||
    (operator === "lt" && count === 1) ||
    (operator === "le" && count === 0);

  if (!isNonEmpty && !isEmpty) {
    throw new Error(
      `Unsupported size comparison: size(...) ${operator} ${count}`
    );
  }

  const { relations } = resolveFieldReference(collectionOperand.name, mapper);
  if (!relations || relations.length === 0) {
    throw new Error("size operator requires a relation mapping");
  }

  const deepest = relations[relations.length - 1];
  if (!deepest) {
    throw new Error("size operator requires a relation mapping");
  }

  const prismaOp = isNonEmpty ? "some" : "none";
  const leafFilter = { [deepest.name]: { [prismaOp]: {} } };

  if (relations.length === 1) {
    return leafFilter;
  }

  return buildNestedRelationFilter(relations.slice(0, -1), leafFilter);
}

/**
 * Helper function to process relational operators (eq, ne, lt, etc.)
 */
function handleRelationalOperator(
  operator: string,
  operands: PlanExpressionOperand[],
  mapper: Mapper
): PrismaFilter {
  const prismaOperator = CERBOS_TO_PRISMA_OPERATOR[operator];

  if (!prismaOperator) {
    throw new Error(`Unsupported operator: ${operator}`);
  }

  const leftOperand = operands.find(
    (o) => isNamedOperand(o) || isOperatorOperand(o)
  );
  if (!leftOperand) throw new Error("No valid left operand found");

  const rightOperand = operands.find((o) => o !== leftOperand);
  if (!rightOperand) throw new Error("No valid right operand found");

  if (isOperatorOperand(leftOperand) && leftOperand.operator === "size") {
    return handleSizeComparison(operator, leftOperand, rightOperand, mapper);
  }

  const addOperand = [leftOperand, rightOperand].find(
    (o): o is OperatorOperand =>
      isOperatorOperand(o) && o.operator === "add"
  );
  if (addOperand) {
    const otherOperand =
      addOperand === leftOperand ? rightOperand : leftOperand;
    return handleAddComparison(operator, addOperand, otherOperand, mapper);
  }

  const left = resolveOperand(leftOperand, mapper);
  const right = resolveOperand(rightOperand, mapper);

  if (isResolvedFieldReference(left)) {
    const { path, relations } = left;

    if (!isResolvedValue(right)) {
      throw new Error("Right operand must be a value");
    }

    const filterValue = { [prismaOperator]: right.value };
    const fieldName = getLeafField(path);
    const fieldFilter = { [fieldName]: filterValue };

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
  const values = Array.isArray(value) ? value : [value];
  const fieldName = getLeafField(path);

  if (relations && relations.length > 0) {
    const fieldFilter =
      values.length === 1
        ? { [fieldName]: values[0] }
        : { [fieldName]: { in: values } };
    return buildNestedRelationFilter(relations, fieldFilter);
  }

  return values.length === 1
    ? { [fieldName]: values[0] }
    : { [fieldName]: { in: values } };
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

  const fieldName = getLeafField(path);
  const fieldFilter = { [fieldName]: { [operator]: value } };

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

  const fieldName = getLeafField(path);
  const fieldFilter = {
    [fieldName]: resolvedValue.value ? { not: null } : { equals: null },
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

  const leftOperand = assertDefined(
    operands[0],
    "hasIntersection requires a left operand"
  );
  const rightOperand = assertDefined(
    operands[1],
    "hasIntersection requires a right operand"
  );

  // Check if left operand is a map operation
  if (isOperatorOperand(leftOperand) && leftOperand.operator === "map") {
    if (!isValueOperand(rightOperand)) {
      throw new Error("Second operand of hasIntersection must be a value");
    }

    const collection = assertDefined(
      leftOperand.operands[0],
      "Map expression must include a collection reference"
    );
    const lambda = assertDefined(
      leftOperand.operands[1],
      "Map expression must include a lambda expression"
    );

    if (!isNamedOperand(collection)) {
      throw new Error("First operand of map must be a collection reference");
    }

    // Get variable name from lambda
    if (!isOperatorOperand(lambda)) {
      throw new Error("Lambda expression must have operands");
    }

    const variable = assertDefined(
      lambda.operands[1],
      "Lambda variable must have a name"
    );
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

    const projection = assertDefined(
      lambda.operands[0],
      "Invalid map lambda expression structure"
    );
    if (!isNamedOperand(projection)) {
      throw new Error("Invalid map lambda expression structure");
    }

    // Use scoped mapper for resolving the projection
    const resolved = resolveFieldReference(projection.name, scopedMapper);
    const fieldName = getLeafField(resolved.path);

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
    const fieldName = getLeafField(path);
    const fieldFilter = {
      [fieldName]: { in: rightOperand.value },
    };
    return buildNestedRelationFilter(relations, fieldFilter);
  }

  const fieldName = getLeafField(path);
  return { [fieldName]: { some: rightOperand.value } };
}

/**
 * Helper function to handle "lambda" operator
 */
function handleLambdaOperator(operands: PlanExpressionOperand[]): PrismaFilter {
  const condition = assertDefined(
    operands[0],
    "Lambda requires a condition operand"
  );
  const variable = assertDefined(
    operands[1],
    "Lambda requires a variable operand"
  );

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

  const collection = assertDefined(
    operands[0],
    `${operator} requires a collection operand`
  );
  const lambda = assertDefined(
    operands[1],
    `${operator} requires a lambda operand`
  );

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
  const variable = assertDefined(
    lambda.operands[1],
    "Lambda variable must have a name"
  );
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
    throw new Error(`${operator} operator requires a relation mapping`);
  }

  const lambdaConditionOperand = assertDefined(
    lambda.operands[0],
    "Lambda expression must provide a condition"
  );
  const lambdaCondition = buildPrismaFilterFromCerbosExpression(
    lambdaConditionOperand, // Use the condition part of the lambda
    scopedMapper
  );

  const relation = assertDefined(
    relations[0],
    `${operator} operator requires a relation mapping`
  );
  let filterValue = lambdaCondition;

  // If the lambda condition already has a relation structure, merge it
  if (lambdaCondition["AND"] || lambdaCondition["OR"]) {
    filterValue = lambdaCondition;
  } else {
    const lambdaKeys = Object.keys(lambdaCondition);
    const defaultKey = lambdaKeys[0];
    if (!defaultKey) {
      throw new Error("Lambda condition must have at least one field");
    }
    const lambdaFieldValue = lambdaCondition[defaultKey];
    if (lambdaFieldValue === undefined) {
      throw new Error("Lambda condition field value cannot be undefined");
    }
    const filterField = relation.field || defaultKey;
    filterValue = {
      [filterField]: lambdaFieldValue,
    };
  }

  switch (operator) {
    case "exists":
      return { [relation.name]: { some: filterValue } };
    case "filter":
      throw new Error(
        "The filter() collection operator returns a list, not a boolean. " +
          "It cannot be used as a standalone condition. " +
          "Use exists() or combine filter() with size() instead."
      );
    case "except":
      return { [relation.name]: { some: { NOT: filterValue } } };
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

  const collection = assertDefined(
    operands[0],
    "map requires a collection operand"
  );
  const lambda = assertDefined(
    operands[1],
    "map requires a lambda operand"
  );

  if (!isNamedOperand(collection)) {
    throw new Error("First operand of map must be a collection reference");
  }

  if (!isOperatorOperand(lambda) || lambda.operator !== "lambda") {
    throw new Error("Second operand of map must be a lambda expression");
  }

  // Get variable name from lambda
  const projection = assertDefined(
    lambda.operands[0],
    "Map lambda expression must provide a projection"
  );
  const variable = assertDefined(
    lambda.operands[1],
    "Map lambda expression must provide a variable"
  );
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
  if (!relations || relations.length === 0) {
    throw new Error("map operator requires a relation mapping");
  }

  // Use scoped mapper for resolving the projection
  const resolved = resolveFieldReference(projection.name, scopedMapper);
  const fieldName = getLeafField(resolved.path);
  const lastRelation = assertDefined(
    relations[relations.length - 1],
    "Relation mapping must contain at least one relation"
  );

  return buildNestedRelationFilter(relations, {
    [getPrismaRelationOperator(lastRelation)]: {
      select: { [fieldName]: true },
    },
  });
}

function buildImpossibleFilter(fieldRef: ResolvedFieldReference): PrismaFilter {
  return buildFieldFilter(fieldRef, "in", []);
}

function handleAddComparison(
  operator: string,
  addExpr: OperatorOperand,
  otherOperand: PlanExpressionOperand,
  mapper: Mapper
): PrismaFilter {
  const addLeftOp = assertDefined(
    addExpr.operands[0],
    "add operator requires a left operand"
  );
  const addRightOp = assertDefined(
    addExpr.operands[1],
    "add operator requires a right operand"
  );

  const addLeft = resolveOperand(addLeftOp, mapper);
  const addRight = resolveOperand(addRightOp, mapper);
  const other = resolveOperand(otherOperand, mapper);

  if (isResolvedValue(addLeft) && isResolvedValue(addRight)) {
    const folded = foldAdd(addLeft.value, addRight.value);
    const prismaOp = CERBOS_TO_PRISMA_OPERATOR[operator];
    if (!prismaOp) throw new Error(`Unsupported operator: ${operator}`);

    if (isResolvedFieldReference(other)) {
      return buildFieldFilter(other, prismaOp, folded);
    }
    throw new Error("add with two values requires a field reference on the other side");
  }

  if (!isResolvedValue(other)) {
    throw new Error(
      "add operator with field references requires a value on the other side of the comparison"
    );
  }

  let fieldRef: ResolvedFieldReference;
  let addValue: Value;
  let fieldIsLeft: boolean;

  if (isResolvedFieldReference(addLeft) && isResolvedValue(addRight)) {
    fieldRef = addLeft;
    addValue = addRight.value;
    fieldIsLeft = true;
  } else if (isResolvedValue(addLeft) && isResolvedFieldReference(addRight)) {
    fieldRef = addRight;
    addValue = addLeft.value;
    fieldIsLeft = false;
  } else {
    throw new Error(
      "add operator requires exactly one field reference and one value, or two values"
    );
  }

  if (operator !== "eq" && operator !== "ne") {
    throw new Error(
      `Operator ${operator} is not supported with add and field references`
    );
  }

  const solvedValue = solveAdd(other.value, addValue, fieldIsLeft);
  if (solvedValue === null) {
    if (operator === "eq") return buildImpossibleFilter(fieldRef);
    return {};
  }

  return buildFieldFilter(fieldRef, CERBOS_TO_PRISMA_OPERATOR[operator]!, solvedValue);
}

function foldAdd(left: Value, right: Value): Value {
  if (typeof left === "string" || typeof right === "string") {
    return String(left) + String(right);
  }
  if (typeof left === "number" && typeof right === "number") {
    return left + right;
  }
  throw new Error("add operator requires string or number operands");
}

function solveAdd(
  comparisonValue: Value,
  addConstant: Value,
  fieldIsLeft: boolean
): Value | null {
  if (typeof comparisonValue === "string" && typeof addConstant === "string") {
    if (fieldIsLeft) {
      if (!comparisonValue.endsWith(addConstant)) return null;
      return comparisonValue.slice(
        0,
        comparisonValue.length - addConstant.length
      );
    }
    if (!comparisonValue.startsWith(addConstant)) return null;
    return comparisonValue.slice(addConstant.length);
  }
  if (typeof comparisonValue === "number" && typeof addConstant === "number") {
    return comparisonValue - addConstant;
  }
  throw new Error("Type mismatch in add comparison");
}

type ConstantSegment = { type: "constant"; value: string };
type FieldSegment = { type: "field"; fieldRef: ResolvedFieldReference };
type HierarchySegment = ConstantSegment | FieldSegment;

type ConstantHierarchy = {
  type: "constant";
  segments: string[];
  raw: string;
  delimiter: string;
};

type FieldHierarchy = {
  type: "field";
  fieldRef: ResolvedFieldReference;
  delimiter: string;
};

type SegmentedHierarchy = {
  type: "segmented";
  segments: HierarchySegment[];
};

type ResolvedHierarchy = ConstantHierarchy | FieldHierarchy | SegmentedHierarchy;

function resolveHierarchy(
  expr: OperatorOperand,
  mapper: Mapper
): ResolvedHierarchy {
  const operands = expr.operands;

  if (operands.length === 2) {
    const strOperand = assertDefined(operands[0], "hierarchy requires operands");
    const delimOperand = assertDefined(
      operands[1],
      "hierarchy requires a delimiter"
    );
    if (!isValueOperand(delimOperand)) {
      throw new Error("hierarchy delimiter must be a value");
    }
    const delimiter = String(delimOperand.value);

    if (isValueOperand(strOperand)) {
      const raw = String(strOperand.value);
      return { type: "constant", segments: raw.split(delimiter), raw, delimiter };
    }
    if (isNamedOperand(strOperand)) {
      return {
        type: "field",
        fieldRef: resolveFieldReference(strOperand.name, mapper),
        delimiter,
      };
    }
    throw new Error("hierarchy(string, delimiter) requires a value or field operand");
  }

  if (operands.length === 1) {
    const inner = assertDefined(operands[0], "hierarchy requires an operand");

    if (isValueOperand(inner)) {
      const raw = String(inner.value);
      return { type: "constant", segments: raw.split("."), raw, delimiter: "." };
    }

    if (isNamedOperand(inner)) {
      return {
        type: "field",
        fieldRef: resolveFieldReference(inner.name, mapper),
        delimiter: ".",
      };
    }

    if (isOperatorOperand(inner) && inner.operator === "list") {
      const segments = inner.operands.map((op): HierarchySegment => {
        const resolved = resolveOperand(op, mapper);
        if (isResolvedValue(resolved)) {
          return { type: "constant", value: String(resolved.value) };
        }
        return { type: "field", fieldRef: resolved };
      });
      return { type: "segmented", segments };
    }

    throw new Error("hierarchy requires a value, field, or list operand");
  }

  throw new Error("hierarchy requires 1 or 2 operands");
}

function toSegments(resolved: ResolvedHierarchy): HierarchySegment[] {
  switch (resolved.type) {
    case "constant":
      return resolved.segments.map((s) => ({ type: "constant" as const, value: s }));
    case "segmented":
      return resolved.segments;
    case "field":
      throw new Error(
        "Cannot get segments from a field-reference hierarchy"
      );
  }
}

function normalizeHierarchy(
  h: ResolvedHierarchy,
  defaultDelimiter = "."
): ResolvedHierarchy {
  if (h.type !== "segmented") return h;
  const allConstant = h.segments.every(
    (s): s is ConstantSegment => s.type === "constant"
  );
  if (!allConstant) return h;
  const segments = (h.segments as ConstantSegment[]).map((s) => s.value);
  return {
    type: "constant",
    segments,
    raw: segments.join(defaultDelimiter),
    delimiter: defaultDelimiter,
  };
}

function checkPrefixConditions(
  shorter: HierarchySegment[],
  longer: HierarchySegment[]
): PrismaFilter | null {
  if (shorter.length > longer.length) return null;

  const conditions: PrismaFilter[] = [];

  for (let i = 0; i < shorter.length; i++) {
    const s = shorter[i]!;
    const l = longer[i]!;

    if (s.type === "constant" && l.type === "constant") {
      if (s.value !== l.value) return null;
    } else if (s.type === "field" && l.type === "constant") {
      conditions.push(buildFieldFilter(s.fieldRef, "equals", l.value));
    } else if (s.type === "constant" && l.type === "field") {
      conditions.push(buildFieldFilter(l.fieldRef, "equals", s.value));
    } else {
      throw new Error(
        "Cannot compare two field references in hierarchy overlap"
      );
    }
  }

  if (conditions.length === 0) return {};
  if (conditions.length === 1) return conditions[0]!;
  return { AND: conditions };
}

function handleOverlapsOperator(
  operands: PlanExpressionOperand[],
  mapper: Mapper
): PrismaFilter {
  const [left, right] = extractHierarchyOperands("overlaps", operands, mapper);

  if (left.type === "field" || right.type === "field") {
    return handleFieldOverlaps(left, right);
  }

  const leftSegs = toSegments(left);
  const rightSegs = toSegments(right);

  const leftPrefixOfRight = checkPrefixConditions(leftSegs, rightSegs);
  const rightPrefixOfLeft = checkPrefixConditions(rightSegs, leftSegs);

  const validConditions = [leftPrefixOfRight, rightPrefixOfLeft].filter(
    (c): c is PrismaFilter => c !== null
  );

  if (validConditions.length === 0) {
    const allSegs = [...leftSegs, ...rightSegs];
    const fieldSeg = allSegs.find(
      (s): s is FieldSegment => s.type === "field"
    );
    if (fieldSeg) return buildImpossibleFilter(fieldSeg.fieldRef);
    throw new Error("Cannot determine overlap: no field references found");
  }

  if (validConditions.some((c) => Object.keys(c).length === 0)) return {};

  // When both directions are valid (equal-length hierarchies), they produce
  // identical conditions since the same segment pairs are compared in both.
  return validConditions[0]!;
}

function handleFieldOverlaps(
  left: ResolvedHierarchy,
  right: ResolvedHierarchy
): PrismaFilter {
  if (left.type === "field" && right.type === "field") {
    throw new Error("overlaps: cannot compare two field-reference hierarchies");
  }

  const field = (left.type === "field" ? left : right) as FieldHierarchy;
  const other = left.type === "field" ? right : left;

  if (other.type !== "constant") {
    throw new Error("overlaps: segmented hierarchies with field hierarchies are not supported");
  }

  const delimiter = field.delimiter;
  const otherRaw = other.segments.join(delimiter);
  const strictPrefixes = getStrictPrefixes(other.segments, delimiter);

  const conditions: PrismaFilter[] = [];
  if (strictPrefixes.length > 0) {
    conditions.push(buildFieldFilter(field.fieldRef, "in", strictPrefixes));
  }
  conditions.push(buildFieldFilter(field.fieldRef, "equals", otherRaw));
  conditions.push(buildFieldFilter(field.fieldRef, "startsWith", otherRaw + delimiter));

  if (conditions.length === 1) return conditions[0]!;
  return { OR: conditions };
}

function extractHierarchyOperands(
  operatorName: string,
  operands: PlanExpressionOperand[],
  mapper: Mapper
): [ResolvedHierarchy, ResolvedHierarchy] {
  if (operands.length !== 2) {
    throw new Error(`${operatorName} requires exactly two operands`);
  }
  const leftOp = assertDefined(operands[0], `${operatorName} requires a left operand`);
  const rightOp = assertDefined(operands[1], `${operatorName} requires a right operand`);

  if (
    !isOperatorOperand(leftOp) || leftOp.operator !== "hierarchy" ||
    !isOperatorOperand(rightOp) || rightOp.operator !== "hierarchy"
  ) {
    throw new Error(`${operatorName} requires two hierarchy operands`);
  }

  return [
    normalizeHierarchy(resolveHierarchy(leftOp, mapper)),
    normalizeHierarchy(resolveHierarchy(rightOp, mapper)),
  ];
}

function getStrictPrefixes(segments: string[], delimiter: string): string[] {
  if (segments.length <= 1) return [];
  const prefixes: string[] = [];
  let current = segments[0]!;
  prefixes.push(current);
  for (let i = 1; i < segments.length - 1; i++) {
    current = current + delimiter + segments[i]!;
    prefixes.push(current);
  }
  return prefixes;
}

function handleAncestorDescendantOperator(
  operands: PlanExpressionOperand[],
  mapper: Mapper,
  direction: "ancestor" | "descendant"
): PrismaFilter {
  const operatorName = direction === "ancestor" ? "ancestorOf" : "descendentOf";
  const [left, right] = extractHierarchyOperands(operatorName, operands, mapper);

  // ancestorOf(A, B) = A is strict prefix of B
  // descendentOf(A, B) = B is strict prefix of A
  const ancestor = direction === "ancestor" ? left : right;
  const descendant = direction === "ancestor" ? right : left;

  if (ancestor.type === "constant" && descendant.type === "field") {
    const prefix = ancestor.segments.join(descendant.delimiter) + descendant.delimiter;
    return buildFieldFilter(descendant.fieldRef, "startsWith", prefix);
  }

  if (ancestor.type === "field" && descendant.type === "constant") {
    const delimiter = ancestor.delimiter;
    const prefixes = getStrictPrefixes(descendant.segments, delimiter);
    if (prefixes.length === 0) {
      return buildImpossibleFilter(ancestor.fieldRef);
    }
    if (prefixes.length === 1) {
      return buildFieldFilter(ancestor.fieldRef, "equals", prefixes[0]!);
    }
    return buildFieldFilter(ancestor.fieldRef, "in", prefixes);
  }

  if (ancestor.type === "constant" && descendant.type === "constant") {
    const ancestorSegs = ancestor.segments;
    const descendantSegs = descendant.segments;
    if (
      descendantSegs.length > ancestorSegs.length &&
      ancestorSegs.every((seg, i) => seg === descendantSegs[i])
    ) {
      return {};
    }
    throw new Error(`${operatorName}: constants do not satisfy ${direction} relationship`);
  }

  throw new Error(`${operatorName}: unsupported hierarchy type combination`);
}

function buildFieldFilter(
  fieldRef: ResolvedFieldReference,
  prismaOp: string,
  value: Value
): PrismaFilter {
  const fieldName = getLeafField(fieldRef.path);
  const fieldFilter = { [fieldName]: { [prismaOp]: value } };
  if (fieldRef.relations && fieldRef.relations.length > 0) {
    return buildNestedRelationFilter(fieldRef.relations, fieldFilter);
  }
  return fieldFilter;
}
