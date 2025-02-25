import {
  PlanResourcesResponse,
  PlanExpressionOperand,
  PlanKind,
  Value,
} from "@cerbos/core";

export { PlanKind };

export type MikroORMFilter = Record<string, any>;

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

export interface QueryPlanToMikroORMArgs {
  queryPlan: PlanResourcesResponse;
  mapper?: Mapper;
}

export type QueryPlanToMikroORMResult =
  | {
      kind: PlanKind.ALWAYS_ALLOWED | PlanKind.ALWAYS_DENIED;
    }
  | {
      kind: PlanKind.CONDITIONAL;
      filters: Record<string, any>;
    };

/**
 * Converts a Cerbos query plan to a MikroORM filter.
 */
export function queryPlanToMikroORM({
  queryPlan,
  mapper = {},
}: QueryPlanToMikroORMArgs): QueryPlanToMikroORMResult {
  switch (queryPlan.kind) {
    case PlanKind.ALWAYS_ALLOWED:
      return { kind: PlanKind.ALWAYS_ALLOWED };
    case PlanKind.ALWAYS_DENIED:
      return { kind: PlanKind.ALWAYS_DENIED };
    case PlanKind.CONDITIONAL:
      return {
        kind: PlanKind.CONDITIONAL,
        filters: buildMikroORMFilterFromCerbosExpression(
          queryPlan.condition,
          mapper
        ),
      };
    default:
      throw Error(`Invalid query plan.`);
  }
}

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

  let path = "";
  const lastRelation = relations[relations.length - 1];

  // Build the path for MikroORM's dot notation
  relations.forEach((relation) => {
    path += path ? "." + relation.name : relation.name;
  });

  if (lastRelation.field) {
    const [, filterValue] = Object.entries(fieldFilter)[0];
    return { [`${path}.${lastRelation.field}`]: filterValue };
  }

  return { [path]: fieldFilter };
};

const buildMikroORMFilterFromCerbosExpression = (
  expression: PlanExpressionOperand,
  mapper: Mapper
): MikroORMFilter => {
  if (!("operator" in expression) || !("operands" in expression)) {
    throw new Error("Invalid Cerbos expression structure");
  }

  const { operator, operands } = expression;

  const processRelationalOperator = (
    operator: string,
    left: ResolvedOperand,
    right: ResolvedOperand
  ): MikroORMFilter => {
    const mikroORMOperator = {
      eq: "$eq",
      ne: "$ne",
      lt: "$lt",
      le: "$lte",
      gt: "$gt",
      ge: "$gte",
    }[operator];

    if (isResolvedFieldReference(left)) {
      const { path, relations } = left;
      if (!isResolvedValue(right)) {
        throw new Error("Right operand must be a value");
      }
      if (!mikroORMOperator) {
        throw new Error(`Unsupported operator: ${operator}`);
      }

      const filterValue =
        operator === "eq" ? right.value : { [mikroORMOperator]: right.value };
      const fieldFilter = { [path[path.length - 1]]: filterValue };

      if (relations && relations.length > 0) {
        return buildNestedRelationFilter(relations, fieldFilter);
      }

      return fieldFilter;
    }
    throw new Error("Left operand must be a field reference");
  };

  switch (operator) {
    case "and":
      return {
        $and: operands.map((op) =>
          buildMikroORMFilterFromCerbosExpression(op, mapper)
        ),
      };
    case "or":
      return {
        $or: operands.map((op) =>
          buildMikroORMFilterFromCerbosExpression(op, mapper)
        ),
      };
    case "not":
      return {
        $not: buildMikroORMFilterFromCerbosExpression(operands[0], mapper),
      };
    case "eq":
    case "ne":
    case "lt":
    case "le":
    case "gt":
    case "ge": {
      const leftOperand = operands.find(isPlanExpressionWithName);
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

      const fieldFilter = {
        [path[path.length - 1]]: { $in: valueOperand.value },
      };

      if (relations && relations.length > 0) {
        return buildNestedRelationFilter(relations, fieldFilter);
      }

      return fieldFilter;
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

      const mikroORMOperator = {
        contains: "$like",
        startsWith: "$like",
        endsWith: "$like",
      }[operator];

      const likePattern = {
        contains: `%${value}%`,
        startsWith: `${value}%`,
        endsWith: `%${value}`,
      }[operator];

      const fieldFilter = {
        [path[path.length - 1]]: { [mikroORMOperator]: likePattern },
      };

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
        [path[path.length - 1]]: resolvedValue ? { $ne: null } : { $eq: null },
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

        if (!isPlanExpressionWithOperator(lambda)) {
          throw new Error("Lambda expression must have operands");
        }
        const [, variable] = lambda.operands;
        if (!("name" in variable))
          throw new Error("Lambda variable must have a name");

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

        const resolved = resolveFieldReference(projection.name, scopedMapper);
        const fieldName = resolved.path[resolved.path.length - 1];
        const relationPath = relations.map((r) => r.name).join(".");

        return {
          [`${relationPath}.${fieldName}`]: { $in: rightOperand.value },
        };
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
          [path[path.length - 1]]: { $in: rightOperand.value },
        };
        return buildNestedRelationFilter(relations, fieldFilter);
      }

      return { [path[path.length - 1]]: { $in: rightOperand.value } };
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

      const [, variable] = lambda.operands;
      if (!("name" in variable))
        throw new Error("Lambda variable must have a name");

      const scopedMapper = createScopedMapper(
        collection.name,
        variable.name,
        mapper
      );

      const { relations } = resolveFieldReference(collection.name, mapper);
      if (!relations)
        throw new Error(`${operator} operator requires a relation mapping`);

      const lambdaCondition = buildMikroORMFilterFromCerbosExpression(
        lambda.operands[0],
        scopedMapper
      );

      const relationPath = relations.map((r) => r.name).join(".");
      const relation = relations[0];
      let filterValue = lambdaCondition;

      if (relation.field) {
        filterValue = {
          [relation.field]: lambdaCondition[Object.keys(lambdaCondition)[0]],
        };
      }

      switch (operator) {
        case "exists":
        case "filter":
          return { [`${relationPath}`]: { $exists: true, ...filterValue } };
        case "exists_one":
          return {
            $and: [
              { [`${relationPath}`]: { $exists: true, ...filterValue } },
              {
                $expr: {
                  $eq: [
                    {
                      $size: {
                        $filter: {
                          input: `$${relationPath}`,
                          cond: filterValue,
                        },
                      },
                    },
                    1,
                  ],
                },
              },
            ],
          };
        case "all":
          return { [`${relationPath}`]: { $all: filterValue } };
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

      const [projection, variable] = lambda.operands;
      if (!("name" in projection) || !("name" in variable))
        throw new Error("Invalid map lambda expression structure");

      const scopedMapper = createScopedMapper(
        collection.name,
        variable.name,
        mapper
      );

      const { relations } = resolveFieldReference(collection.name, mapper);
      if (!relations)
        throw new Error("map operator requires a relation mapping");

      const resolved = resolveFieldReference(projection.name, scopedMapper);
      const fieldName = resolved.path[resolved.path.length - 1];
      const relationPath = relations.map((r) => r.name).join(".");

      return {
        [`${relationPath}`]: {
          $select: fieldName,
        },
      };
    }

    default:
      throw new Error(`Unsupported operator: ${operator}`);
  }
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
 */
function isResolvedFieldReference(
  operand: ResolvedOperand
): operand is ResolvedFieldReference {
  return "path" in operand;
}

/**
 * Type guard to check if an operand is a resolved value.
 */
function isResolvedValue(operand: ResolvedOperand): operand is ResolvedValue {
  return "value" in operand;
}

/**
 * Type guard for PlanExpressionOperand with name property.
 */
function isPlanExpressionWithName(
  operand: PlanExpressionOperand
): operand is { name: string } {
  return "name" in operand && typeof operand.name === "string";
}

/**
 * Type guard for PlanExpressionOperand with value property.
 */
function isPlanExpressionWithValue(
  operand: PlanExpressionOperand
): operand is { value: Value } {
  return "value" in operand && operand.value !== undefined;
}

/**
 * Type guard for PlanExpressionOperand with operator property.
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
    const nestedResult = buildMikroORMFilterFromCerbosExpression(
      operand,
      mapper
    );
    return { value: nestedResult };
  }
  throw new Error("Operand must have name, value, or be an expression");
};

/**
 * Creates a scoped mapper for collection operations that preserves access to parent scope
 * and handles nested collections.
 */
const createScopedMapper =
  (collectionPath: string, variableName: string, fullMapper: Mapper): Mapper =>
  (key: string) => {
    if (key.startsWith(variableName + ".")) {
      const strippedKey = key.replace(variableName + ".", "");
      const parts = strippedKey.split(".");

      const collectionConfig =
        typeof fullMapper === "function"
          ? fullMapper(collectionPath)
          : fullMapper[collectionPath];

      if (collectionConfig?.relation?.fields) {
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

        return currentConfig[field] || { field };
      }
      return { field: strippedKey };
    }

    return typeof fullMapper === "function" ? fullMapper(key) : fullMapper[key];
  };
