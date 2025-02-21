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
const resolveFieldReference = (reference: string, mapper: Mapper) => {
  const parts = reference.split(".");
  const lastPart = parts[parts.length - 1];

  // Try exact match first
  const config =
    typeof mapper === "function" ? mapper(reference) : mapper[reference];

  if (config) {
    if (config.relation) {
      const { name, field, fields } = config.relation;
      return {
        path: [name, field].filter(Boolean),
        relation: {
          name,
          field,
          nestedMapper: fields,
        },
      };
    }
    return { path: [config.field || reference] };
  }

  // Try parent relation for nested fields
  if (parts.length > 1) {
    const parentPath = parts.slice(0, -1).join(".");
    const parentConfig =
      typeof mapper === "function" ? mapper(parentPath) : mapper[parentPath];

    if (parentConfig?.relation) {
      const { name, fields } = parentConfig.relation;
      const fieldConfig = fields?.[lastPart];
      const fieldName = fieldConfig?.field || lastPart;
      return {
        path: [name],
        relation: {
          name,
          field: fieldName,
          nestedMapper: fields,
        },
      };
    }
  }

  return { path: [reference] };
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
        if (relation) {
          return {
            [relation.name]: {
              $elemMatch: {
                [relation.field!]: { [mongoOperator]: right.value },
              },
            },
          };
        }
        return path.reduceRight(
          (acc: any, key: any, index: number) =>
            index === path.length - 1
              ? { [key]: { [mongoOperator]: right.value } }
              : { [key]: acc },
          {}
        );
      }
      return { [mongoOperator]: right.value };
    }

    case "in":
      const { path, relation } = resolveOperand(
        operands.find((o) => isVariable(o))!
      );
      const { value } = resolveOperand(operands.find((o) => isValue(o))!);

      if (relation) {
        return {
          [relation.name]: {
            $elemMatch: {
              [relation.field!]: { $in: value },
            },
          },
        };
      }
      return path.reduceRight(
        (acc: any, key: any, index: number) =>
          index === path.length - 1
            ? { [key]: { $in: value } }
            : { [key]: acc },
        {}
      );

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
        return {
          [relation.name]: {
            $elemMatch: {
              [relation.field!]: { $regex: regexStr },
            },
          },
        };
      }
      return path.reduceRight(
        (acc: any, key: any, index: number) =>
          index === path.length - 1
            ? { [key]: { $regex: regexStr } }
            : { [key]: acc },
        {}
      );
    }

    case "isSet": {
      const { path, relation } = resolveOperand(
        operands.find((o) => isVariable(o))!
      );
      const { value } = resolveOperand(operands.find((o) => isValue(o))!);

      if (relation) {
        return {
          [relation.name]: {
            $elemMatch: {
              [relation.field!]: value
                ? { $exists: true, $ne: null }
                : { $exists: false },
            },
          },
        };
      }
      return path.reduceRight(
        (acc: any, key: any, index: number) =>
          index === path.length - 1
            ? {
                [key]: value
                  ? { $exists: true, $ne: null }
                  : { $exists: false },
              }
            : { [key]: acc },
        {}
      );
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
        const { relation } = resolveFieldReference(
          leftOperand.operands[0].name,
          mapper
        );
        if (!relation) {
          throw new Error("map operator requires a relation mapping");
        }

        // For array fields, we want to check if any element matches
        return {
          [relation.name]: {
            $elemMatch: {
              name: { $in: (rightOperand as PlanExpressionValue).value },
            },
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
        return {
          [relation.name]: {
            $elemMatch: {
              [relation.field!]: { $in: rightOperand.value },
            },
          },
        };
      }

      return path.reduceRight(
        (acc, key, index) =>
          index === path.length - 1
            ? { [key]: { $in: rightOperand.value } }
            : { [key]: acc },
        {}
      );
    }

    // Collection operations
    case "exists":
    case "exists_one":
    case "all":
    case "filter": {
      if (operands.length !== 2) {
        throw new Error(`${operator} requires exactly two operands`);
      }

      const [collection, lambda] = operands;
      if (!isVariable(collection) || !isExpression(lambda)) {
        throw new Error("Invalid operands for collection operation");
      }

      const { relation } = resolveFieldReference(collection.name, mapper);
      if (!relation) {
        throw new Error(`${operator} operator requires a relation mapping`);
      }

      const condition = buildMongooseFilterFromCerbosExpression(lambda, mapper);

      switch (operator) {
        case "exists":
        case "filter":
          return {
            [relation.name]: {
              $elemMatch: condition,
            },
          };
        case "exists_one":
          return {
            $and: [
              { [relation.name]: { $elemMatch: condition } },
              { [relation.name]: { $size: 1 } },
            ],
          };
        case "all":
          return {
            [relation.name]: {
              $not: {
                $elemMatch: {
                  $nor: [condition],
                },
              },
            },
          };
        default:
          throw new Error(`Unexpected operator: ${operator}`);
      }
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

      const [projection, variable] = lambda.operands;
      if (!isVariable(projection) || !isVariable(variable)) {
        throw new Error("Invalid map lambda expression structure");
      }

      // Get the field name we're projecting
      const projectionField = projection.name.split(".").pop();

      // Return the field name directly for MongoDB to handle projection
      return {
        [relation.name]: {
          $elemMatch: {
            [projectionField!]: { $exists: true },
          },
        },
      };
    }

    default:
      throw new Error(`Unsupported operator: ${operator}`);
  }
};
