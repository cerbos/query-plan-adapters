import {
  PlanResourcesResponse,
  PlanExpressionOperand,
  PlanExpression,
  PlanExpressionValue,
  PlanExpressionVariable,
  PlanKind,
} from "@cerbos/core";

export { PlanKind };

export type MongooseFilter = Record<string, any>;

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

export interface QueryPlanToMongooseArgs {
  queryPlan: PlanResourcesResponse;
  mapper?: Mapper;
}

export interface QueryPlanToMongooseResult {
  kind: PlanKind;
  filters?: MongooseFilter;
}

// Helper functions for type checking
const isExpression = (e: PlanExpressionOperand): e is PlanExpression =>
  "operator" in e;
const isValue = (e: PlanExpressionOperand): e is PlanExpressionValue =>
  "value" in e;
const isVariable = (e: PlanExpressionOperand): e is PlanExpressionVariable =>
  "name" in e;

/**
 * Converts a Cerbos query plan to a Mongoose filter
 */
export function queryPlanToMongoose({
  queryPlan,
  mapper = {},
}: QueryPlanToMongooseArgs): QueryPlanToMongooseResult {
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
        filters: buildMongooseFilterFromCerbosExpression(
          queryPlan.condition,
          mapper
        ),
      };
    default:
      throw Error(`Invalid query plan.`);
  }
}

/**
 * Resolves a field reference considering relations
 */
type ResolvedFieldReference = {
  path: string[];
  relation?: {
    name: string;
    type: "one" | "many";
    field?: string;
    nestedMapper?: {
      [key: string]: MapperConfig;
    };
  };
};

const resolveFieldReference = (
  reference: string,
  mapper: Mapper
): ResolvedFieldReference => {
  const parts = reference.split(".");
  const lastPart = parts[parts.length - 1];

  // Try exact match first
  const config =
    typeof mapper === "function" ? mapper(reference) : mapper[reference];

  if (config?.relation) {
    const { name, field, fields, type } = config.relation;
    const path = field ? [name, field] : [name];
    return {
      path,
      relation: {
        name,
        type,
        field,
        nestedMapper: fields,
      },
    };
  }

  if (config?.field) {
    return { path: [config.field] };
  }

  // Try parent relation for nested fields
  if (parts.length > 1) {
    const parentPath = parts.slice(0, -1).join(".");
    const parentConfig =
      typeof mapper === "function" ? mapper(parentPath) : mapper[parentPath];

    if (parentConfig?.relation) {
      const { name, fields, type } = parentConfig.relation;
      const fieldConfig = fields?.[lastPart];
      const fieldName = fieldConfig?.field || lastPart;
      return {
        path: fieldName ? [name, fieldName] : [name],
        relation: {
          name,
          type,
          field: fieldName,
          nestedMapper: fields,
        },
      };
    }
  }

  return { path: [reference] };
};

const buildNestedObject = (path: string[], value: any) =>
  path.reduceRight(
    (acc: any, key: string, index: number) =>
      index === path.length - 1 ? { [key]: value } : { [key]: acc },
    value
  );

const buildFieldFilter = (path: string[], value: any) =>
  path.length === 0 ? value : buildNestedObject(path, value);

/**
 * Creates a scoped mapper for collection operations
 */
const createScopedMapper =
  (collectionPath: string, variableName: string, fullMapper: Mapper): Mapper =>
  (key: string) => {
    if (key.startsWith(variableName + ".")) {
      const strippedKey = key.replace(variableName + ".", "");

      // Get the collection's relation config
      const collectionConfig =
        typeof fullMapper === "function"
          ? fullMapper(collectionPath)
          : fullMapper[collectionPath];

      if (collectionConfig?.relation?.fields) {
        const fieldConfig = collectionConfig.relation.fields[strippedKey];
        if (fieldConfig) {
          return fieldConfig;
        }
      }

      // If no specific field mapping found, return default mapping
      return { field: strippedKey };
    }

    // For non-variable keys, use the full mapper
    return typeof fullMapper === "function"
      ? fullMapper(key)
      : fullMapper[key] || { field: key };
  };

/**
 * Builds Mongoose conditions from a Cerbos expression
 */
const buildMongooseFilterFromCerbosExpression = (
  expression: PlanExpressionOperand,
  mapper: Mapper
): MongooseFilter => {
  if (!isExpression(expression)) {
    throw new Error("Invalid Cerbos expression structure");
  }

  const { operator, operands } = expression;

  const resolveOperand = (operand: PlanExpressionOperand): any => {
    if (isVariable(operand)) {
      return resolveFieldReference(operand.name, mapper);
    } else if (isValue(operand)) {
      return { value: operand.value };
    } else if (isExpression(operand)) {
      const nestedResult = buildMongooseFilterFromCerbosExpression(
        operand,
        mapper
      );
      return { value: nestedResult };
    }
    throw new Error("Invalid operand structure");
  };

  switch (operator) {
    case "and":
      return {
        $and: operands.map((op) =>
          buildMongooseFilterFromCerbosExpression(op, mapper)
        ),
      };

    case "or":
      return {
        $or: operands.map((op) =>
          buildMongooseFilterFromCerbosExpression(op, mapper)
        ),
      };

    case "not":
      return {
        $nor: [buildMongooseFilterFromCerbosExpression(operands[0], mapper)],
      };

    case "eq":
    case "ne":
    case "lt":
    case "le":
    case "gt":
    case "ge": {
      const mongoOperator = {
        eq: "$eq",
        ne: "$ne",
        lt: "$lt",
        le: "$lte",
        gt: "$gt",
        ge: "$gte",
      }[operator];

      const leftOperand = operands.find(
        (o) => isVariable(o) || isExpression(o)
      );
      const rightOperand = operands.find((o) => o !== leftOperand);

      if (!leftOperand || !rightOperand) {
        throw new Error("Missing operands for comparison");
      }

      const left = resolveOperand(leftOperand);
      const right = resolveOperand(rightOperand);

      if ("path" in left) {
        const { path, relation } = left;
        const comparison = { [mongoOperator]: right.value };

        if (relation) {
          if (relation.type === "many") {
            const elementPath = path.slice(1);
            return {
              [relation.name]: {
                $elemMatch: buildFieldFilter(elementPath, comparison),
              },
            };
          }
          return buildFieldFilter(path, comparison);
        }

        return buildFieldFilter(path, comparison);
      }
      return { [mongoOperator]: right.value };
    }

    case "in": {
      const resolvedField = resolveOperand(
        operands.find((o) => isVariable(o))!
      );
      const { path, relation } = resolvedField;
      const { value } = resolveOperand(operands.find((o) => isValue(o))!);
      const comparison = { $in: value };

      if (relation) {
        if (relation.type === "many") {
          return {
            [relation.name]: {
              $elemMatch: buildFieldFilter(path.slice(1), comparison),
            },
          };
        }
        return buildFieldFilter(path, comparison);
      }

      return buildFieldFilter(path, comparison);
    }

    case "contains":
    case "startsWith":
    case "endsWith": {
      const left = resolveOperand(operands.find((o) => isVariable(o))!);
      const right = resolveOperand(operands.find((o) => isValue(o))!);

      if (typeof right.value !== "string") {
        throw new Error(`${operator} operator requires string value`);
      }

      const regexStr =
        operator === "contains"
          ? right.value
          : operator === "startsWith"
          ? `^${right.value}`
          : `${right.value}$`;

      const { path, relation } = left;
      if (relation) {
        const elementPath = path.slice(1);
        if (relation.type === "many") {
          return {
            [relation.name]: {
              $elemMatch: buildFieldFilter(elementPath, { $regex: regexStr }),
            },
          };
        }
        return {
          ...buildFieldFilter(path, { $regex: regexStr }),
        };
      }
      return buildFieldFilter(path, { $regex: regexStr });
    }

    case "isSet": {
      const { path, relation } = resolveOperand(
        operands.find((o) => isVariable(o))!
      );
      const { value } = resolveOperand(operands.find((o) => isValue(o))!);

      const existsFilter = value
        ? { $exists: true, $ne: null }
        : { $exists: false };

      if (relation) {
        if (relation.type === "many") {
          return {
            [relation.name]: {
              $elemMatch: buildFieldFilter(path.slice(1), existsFilter),
            },
          };
        }
        return buildFieldFilter(path, existsFilter);
      }

      return buildFieldFilter(path, existsFilter);
    }

    case "hasIntersection": {
      if (operands.length !== 2) {
        throw new Error("hasIntersection requires exactly two operands");
      }

      const [leftOperand, rightOperand] = operands;

      // Handle map expressions specially for hasIntersection
      if (isExpression(leftOperand) && leftOperand.operator === "map") {
        if (!isVariable(leftOperand.operands[0])) {
          throw new Error("Expected a variable in map expression");
        }
        if (!isExpression(leftOperand.operands[1])) {
          throw new Error("Expected a lambda in map expression");
        }
        const [collection, lambda] = leftOperand.operands;
        const lambdaExpression = lambda as PlanExpression;

        if (lambdaExpression.operator !== "lambda") {
          throw new Error("Second operand of map must be a lambda expression");
        }

        const [projection, variable] = lambdaExpression.operands;
        if (!isVariable(collection) || !isVariable(variable)) {
          throw new Error("Invalid map expression structure");
        }

        if (!isValue(rightOperand) || !Array.isArray(rightOperand.value)) {
          throw new Error("hasIntersection requires an array value");
        }

        const scopedMapper = createScopedMapper(
          collection.name,
          variable.name,
          mapper
        );

        const collectionResolved = resolveFieldReference(
          collection.name,
          mapper
        );
        if (!collectionResolved.relation) {
          throw new Error("map operator requires a relation mapping");
        }
        if (collectionResolved.relation.type !== "many") {
          throw new Error("map operator requires a collection relation");
        }

        if (!isVariable(projection)) {
          throw new Error("Map projection must be a variable reference");
        }

        const projectionResolved = resolveFieldReference(
          projection.name,
          scopedMapper
        );
        const elementPath = projectionResolved.path;

        return {
          [collectionResolved.relation.name]: {
            $elemMatch: buildFieldFilter(elementPath, {
              $in: rightOperand.value,
            }),
          },
        };
      }

      if (!isVariable(leftOperand) || !isValue(rightOperand)) {
        throw new Error("Invalid operands for hasIntersection");
      }

      const { path, relation } = resolveFieldReference(
        leftOperand.name,
        mapper
      );

      if (!Array.isArray(rightOperand.value)) {
        throw new Error("hasIntersection requires an array value");
      }

      if (relation) {
        if (relation.type === "many") {
          return {
            [relation.name]: {
              $elemMatch: buildFieldFilter(
                path.slice(1),
                { $in: rightOperand.value }
              ),
            },
          };
        }
        return buildFieldFilter(path, { $in: rightOperand.value });
      }

      return buildFieldFilter(path, { $in: rightOperand.value });
    }

    // Collection operations
    case "exists":
    case "exists_one":
    case "filter": {
      if (operands.length !== 2) {
        throw new Error(`${operator} requires exactly two operands`);
      }

      const [collection, lambda] = operands;
      if (!isVariable(collection) || !isExpression(lambda)) {
        throw new Error("Invalid operands for collection operation");
      }

      if (lambda.operator !== "lambda") {
        throw new Error("Second operand must be a lambda expression");
      }

      const [condition, variable] = lambda.operands;
      if (!isVariable(variable)) {
        throw new Error("Lambda variable must have a name");
      }

      // Create scoped mapper for the collection
      const scopedMapper = createScopedMapper(
        collection.name,
        variable.name,
        mapper
      );

      const { relation } = resolveFieldReference(collection.name, mapper);
      if (!relation) {
        throw new Error(`${operator} operator requires a relation mapping`);
      }
      if (relation.type !== "many") {
        throw new Error(`${operator} operator requires a collection relation`);
      }
      if (relation.type !== "many") {
        throw new Error(`${operator} operator requires a collection relation`);
      }

      const lambdaCondition = buildMongooseFilterFromCerbosExpression(
        condition,
        scopedMapper
      );

      // Note: exists_one should ideally mean "exactly one element matches the condition"
      // but MongoDB doesn't have a simple query operator for this. For now, exists_one
      // behaves like exists (at least one match). To implement true exists_one semantics,
      // we would need aggregation pipelines.
      return {
        [relation.name]: {
          $elemMatch: lambdaCondition,
        },
      };
    }

    case "lambda": {
      const [condition, variable] = operands;
      if (!isVariable(variable)) {
        throw new Error("Lambda variable must have a name");
      }

      // Create a mapper that strips the variable prefix from field references
      return buildMongooseFilterFromCerbosExpression(
        condition,
        (key: string) => ({
          field: key.replace(`${variable.name}.`, ""),
        })
      );
    }

    case "map": {
      if (operands.length !== 2) {
        throw new Error("map requires exactly two operands");
      }

      const [collection, lambda] = operands;
      if (
        !isVariable(collection) ||
        !isExpression(lambda) ||
        lambda.operator !== "lambda"
      ) {
        throw new Error("Invalid map expression structure");
      }

      const { relation } = resolveFieldReference(collection.name, mapper);
      if (!relation) {
        throw new Error("map operator requires a relation mapping");
      }
      if (relation.type !== "many") {
        throw new Error("map operator requires a collection relation");
      }

      const [projection, variable] = lambda.operands;
      if (!isVariable(projection) || !isVariable(variable)) {
        throw new Error("Invalid map lambda expression structure");
      }

      const scopedMapper = createScopedMapper(
        collection.name,
        variable.name,
        mapper
      );

      const projectionResolved = resolveFieldReference(
        projection.name,
        scopedMapper
      );

      // Return the field name directly for MongoDB to handle projection
      return {
        [relation.name]: {
          $elemMatch: {
            ...buildFieldFilter(projectionResolved.path, { $exists: true }),
          },
        },
      };
    }

    case "all": {
      if (operands.length !== 2) {
        throw new Error(`${operator} requires exactly two operands`);
      }

      const [collection, lambda] = operands;
      if (!isVariable(collection) || !isExpression(lambda)) {
        throw new Error("Invalid operands for collection operation");
      }

      if (lambda.operator !== "lambda") {
        throw new Error("Second operand must be a lambda expression");
      }

      const [condition, variable] = lambda.operands;
      if (!isVariable(variable)) {
        throw new Error("Lambda variable must have a name");
      }

      // Create scoped mapper for the collection
      const scopedMapper = createScopedMapper(
        collection.name,
        variable.name,
        mapper
      );

      const { relation } = resolveFieldReference(collection.name, mapper);
      if (!relation) {
        throw new Error(`${operator} operator requires a relation mapping`);
      }
      if (relation.type !== "many") {
        throw new Error(`${operator} operator requires a collection relation`);
      }

      const lambdaCondition = buildMongooseFilterFromCerbosExpression(
        condition,
        scopedMapper
      );

      return {
        [relation.name]: {
          $not: {
            $elemMatch: {
              $nor: [lambdaCondition],
            },
          },
        },
      };
    }

    default:
      throw new Error(`Unsupported operator: ${operator}`);
  }
};
