import {
  PlanResourcesResponse,
  PlanExpressionOperand,
  PlanKind as PK,
} from "@cerbos/core";

export type PlanKind = PK;
export const PlanKind = PK;

type PrismaFilter = Record<string, any>;

type MapperConfig = {
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

type Mapper =
  | {
      [key: string]: MapperConfig;
    }
  | ((key: string) => MapperConfig);

interface QueryPlanToPrismaArgs {
  queryPlan: PlanResourcesResponse;
  mapper: Mapper;
}

interface QueryPlanToPrismaResult {
  kind: PlanKind;
  filters?: Record<string, any>;
}

export function queryPlanToPrisma({
  queryPlan,
  mapper,
}: QueryPlanToPrismaArgs): QueryPlanToPrismaResult {
  switch (queryPlan.kind) {
    case PlanKind.ALWAYS_ALLOWED:
      return {
        kind: PlanKind.ALWAYS_ALLOWED,
        filters: {},
      };
    case PlanKind.ALWAYS_DENIED:
      return {
        kind: PlanKind.ALWAYS_DENIED,
      };
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
 * Resolves a field reference considering relations
 */
const resolveFieldReference = (
  reference: string,
  mapper: Mapper
): {
  path: string[];
  relation?: {
    name: string;
    type: "one" | "many";
    field?: string;
    nestedMapper?: { [key: string]: MapperConfig };
  };
} => {
  const parts = reference.split(".");
  const lastPart = parts[parts.length - 1];

  // First try exact match
  const config =
    typeof mapper === "function" ? mapper(reference) : mapper[reference];

  if (config) {
    if (config.relation) {
      const { name, type, field, fields } = config.relation;
      return {
        path: [name, field].filter(Boolean) as string[],
        relation: {
          name,
          type,
          field,
          nestedMapper: fields,
        },
      };
    }
    return { path: [config.field || reference] };
  }

  // If no exact match and we have multiple parts, check for parent relation
  if (parts.length > 1) {
    const parentPath = parts.slice(0, -1).join(".");
    const parentConfig =
      typeof mapper === "function" ? mapper(parentPath) : mapper[parentPath];

    if (parentConfig?.relation) {
      const { name, type, fields } = parentConfig.relation;

      // Check if there's an explicit field mapping
      const fieldConfig = fields?.[lastPart];
      const fieldName = fieldConfig?.field || lastPart;

      return {
        path: [name],
        relation: {
          name,
          type,
          field: fieldName,
          nestedMapper: fields,
        },
      };
    }
  }

  // Default to using the raw reference if no mapping found
  return { path: [reference] };
};

/**
 * Determines the appropriate Prisma operator based on relation type and operation
 */
const getPrismaRelationOperator = (relation: {
  name: string;
  type: "one" | "many";
  field?: string;
}) => {
  // Use 'is' for one-to-one relations, 'some' for one-to-many/many-to-many
  const relationOperator = relation.type === "one" ? "is" : "some";
  return relationOperator;
};

/**
 * Builds Prisma WHERE conditions from a Cerbos expression
 */
const buildPrismaFilterFromCerbosExpression = (
  expression: PlanExpressionOperand,
  mapper: Mapper
): PrismaFilter => {
  if (!("operator" in expression) || !("operands" in expression)) {
    throw new Error("Invalid Cerbos expression structure");
  }

  const { operator, operands } = expression;

  const resolveOperand = (operand: PlanExpressionOperand): any => {
    if ("name" in operand && operand.name) {
      return resolveFieldReference(operand.name, mapper);
    } else if ("value" in operand && operand.value !== undefined) {
      return { value: operand.value };
    } else if ("operator" in operand) {
      // Handle nested expressions
      const nestedResult = buildPrismaFilterFromCerbosExpression(
        operand,
        mapper
      );
      return { value: nestedResult };
    }
    throw new Error("Operand must have name, value, or be an expression");
  };

  switch (operator) {
    case "and": {
      return {
        AND: operands.map((operand: PlanExpressionOperand) =>
          buildPrismaFilterFromCerbosExpression(operand, mapper)
        ),
      };
    }
    case "or": {
      return {
        OR: operands.map((operand: PlanExpressionOperand) =>
          buildPrismaFilterFromCerbosExpression(operand, mapper)
        ),
      };
    }
    case "not": {
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
      const leftOperand = operands.find(
        (o: PlanExpressionOperand) => "name" in o || "operator" in o
      );
      if (!leftOperand) throw new Error("No valid left operand found");

      const rightOperand = operands.find(
        (o: PlanExpressionOperand) => o !== leftOperand
      );
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
        const { path, relation } = left;
        if (relation) {
          const relationOperator = getPrismaRelationOperator(relation);
          if (relation.field) {
            return {
              [relation.name]: {
                [relationOperator]: {
                  [relation.field]: { [prismaOperator]: right.value },
                },
              },
            };
          }
          // If no field specified, apply directly to the relation
          return {
            [relation.name]: {
              [relationOperator]: { [prismaOperator]: right.value },
            },
          };
        }

        return path.reduceRight(
          (acc: PrismaFilter, key: string, index: number) => {
            return index === path.length - 1
              ? { [key]: { [prismaOperator]: right.value } }
              : { [key]: acc };
          },
          {}
        );
      }

      return { [prismaOperator]: right.value };
    }

    case "in": {
      const { path, relation } = resolveOperand(
        operands.find((o: PlanExpressionOperand) => "name" in o)!
      );
      const { value } = resolveOperand(
        operands.find((o: PlanExpressionOperand) => "value" in o)!
      );

      if (relation) {
        const relationOperator = getPrismaRelationOperator(relation);
        return {
          [relation.name]: {
            [relationOperator]: {
              [relation.field!]: value,
            },
          },
        };
      }

      return path.reduceRight(
        (acc: PrismaFilter, key: string, index: number) => {
          return index === path.length - 1
            ? { [key]: { in: value } }
            : { [key]: acc };
        },
        {}
      );
    }

    case "contains":
    case "startsWith":
    case "endsWith": {
      const leftOperand = operands.find(
        (o: PlanExpressionOperand) => "name" in o
      );
      if (!leftOperand) throw new Error("No operand with 'name' found");

      const rightOperand = operands.find(
        (o: PlanExpressionOperand) => "value" in o
      );
      if (!rightOperand) throw new Error("No operand with 'value' found");

      const { path, relation } = resolveOperand(leftOperand);
      const { value } = resolveOperand(rightOperand);

      if (typeof value !== "string") {
        throw new Error(`${operator} operator requires string value`);
      }

      if (relation) {
        const relationOperator = getPrismaRelationOperator(relation);
        return {
          [relation.name]: {
            [relationOperator]: {
              [relation.field!]: { [operator]: value },
            },
          },
        };
      }

      return path.reduceRight(
        (acc: PrismaFilter, key: string, index: number) => {
          return index === path.length - 1
            ? { [key]: { [operator]: value } }
            : { [key]: acc };
        },
        {}
      );
    }

    case "isSet": {
      const leftOperand = operands.find(
        (o: PlanExpressionOperand) => "name" in o
      );
      if (!leftOperand) throw new Error("No operand with 'name' found");

      const rightOperand = operands.find(
        (o: PlanExpressionOperand) => "value" in o
      );
      if (!rightOperand) throw new Error("No operand with 'value' found");

      const { path, relation } = resolveOperand(leftOperand);
      const { value } = resolveOperand(rightOperand);

      if (relation) {
        const relationOperator = getPrismaRelationOperator(relation);
        return {
          [relation.name]: {
            [relationOperator]: {
              [relation.field!]: value ? { not: null } : { equals: null },
            },
          },
        };
      }

      return path.reduceRight(
        (acc: PrismaFilter, key: string, index: number) => {
          return index === path.length - 1
            ? { [key]: value ? { not: null } : { equals: null } }
            : { [key]: acc };
        },
        {}
      );
    }

    case "hasIntersection": {
      if (operands.length !== 2) {
        throw new Error("hasIntersection requires exactly two operands");
      }

      const [leftOperand, rightOperand] = operands;

      // Handle case where first operand is a map expression
      if ("operator" in leftOperand && leftOperand.operator === "map") {
        const mapResult = buildPrismaFilterFromCerbosExpression(
          leftOperand,
          mapper
        );

        // Get the relation and field from the map result
        const relationKey = Object.keys(mapResult)[0];
        const relation = mapResult[relationKey];
        const selectKey = Object.keys(
          relation[Object.keys(relation)[0]].select
        )[0];

        if (!("value" in rightOperand)) {
          throw new Error("Second operand of hasIntersection must be a value");
        }

        return {
          [relationKey]: {
            some: {
              [selectKey]: { in: rightOperand.value },
            },
          },
        };
      }

      // Original logic for direct field reference
      if (!("name" in leftOperand)) {
        throw new Error(
          "First operand of hasIntersection must be a field reference or map expression"
        );
      }

      if (!("value" in rightOperand)) {
        throw new Error("Second operand of hasIntersection must be a value");
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
            some: {
              [relation.field!]: { in: rightOperand.value },
            },
          },
        };
      }

      return path.reduceRight(
        (acc: PrismaFilter, key: string, index: number) => {
          return index === path.length - 1
            ? { [key]: { hasSome: rightOperand.value } }
            : { [key]: acc };
        },
        {}
      );
    }

    case "lambda": {
      // Lambda expressions are typically used with exists/forAll operators
      // The first operand is the condition, second is the variable definition
      const [condition, variable] = operands;
      if (!("name" in variable)) {
        throw new Error("Lambda variable must have a name");
      }

      return buildPrismaFilterFromCerbosExpression(
        condition,
        (key: string) => ({
          field: key.replace(`${variable.name}.`, ""),
        })
      );
    }

    case "exists":
    case "exists_one":
    case "all":
    case "except":
    case "filter": {
      if (operands.length !== 2) {
        throw new Error(`${operator} requires exactly two operands`);
      }

      const [collection, lambda] = operands;
      if (!("name" in collection)) {
        throw new Error(
          "First operand of exists/all/except must be a collection reference"
        );
      }

      if (!("operator" in lambda)) {
        throw new Error(
          "Second operand of exists/all/except must be a lambda expression"
        );
      }

      const { relation } = resolveFieldReference(collection.name, mapper);

      if (!relation) {
        throw new Error(`${operator} operator requires a relation mapping`);
      }

      const lambdaCondition = buildPrismaFilterFromCerbosExpression(
        lambda,
        mapper
      );

      switch (operator) {
        case "exists":
          return {
            [relation.name]: {
              some: lambdaCondition,
            },
          };
        case "exists_one":
          return {
            [relation.name]: {
              some: lambdaCondition,
            },
            AND: [
              {
                [relation.name]: {
                  every: {
                    OR: [lambdaCondition, { NOT: lambdaCondition }],
                  },
                },
              },
            ],
          };
        case "all":
          return {
            [relation.name]: {
              every: lambdaCondition,
            },
          };
        case "filter":
          return {
            [relation.name]: {
              some: lambdaCondition,
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
      if (!("name" in collection)) {
        throw new Error("First operand of map must be a collection reference");
      }

      if (!("operator" in lambda) || lambda.operator !== "lambda") {
        throw new Error("Second operand of map must be a lambda expression");
      }

      const { relation } = resolveFieldReference(collection.name, mapper);

      if (!relation) {
        throw new Error("map operator requires a relation mapping");
      }

      const [projection, variable] = lambda.operands;
      if (!("name" in projection) || !("name" in variable)) {
        throw new Error("Invalid map lambda expression structure");
      }

      // For map operations, we project the field specified in the lambda
      return {
        [relation.name]: {
          [getPrismaRelationOperator(relation)]: {
            select: {
              [projection.name.replace(`${variable.name}.`, "")]: true,
            },
          },
        },
      };
    }

    default: {
      throw new Error(`Unsupported operator: ${operator}`);
    }
  }
};
