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
        relation: { name, type, field, nestedMapper: fields },
      };
    }
    return { path: [config.field || reference] };
  }

  // Check for parent relation with multiple parts
  if (parts.length > 1) {
    const parentPath = parts.slice(0, -1).join(".");
    const parentConfig =
      typeof mapper === "function" ? mapper(parentPath) : mapper[parentPath];

    if (parentConfig?.relation) {
      const { name, type, fields } = parentConfig.relation;
      const fieldConfig = fields?.[lastPart];
      const fieldName = fieldConfig?.field || lastPart;
      return {
        path: [name],
        relation: { name, type, field: fieldName, nestedMapper: fields },
      };
    }
  }

  // Fallback to raw reference if no mapper fits
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
          return {
            [relation.name]: {
              [relationOperator]: { [prismaOperator]: right.value },
            },
          };
        }
        // Create nested filter for field path
        return path.reduceRight(
          (acc: any, key: string, index: number) =>
            index === path.length - 1
              ? { [key]: { [prismaOperator]: right.value } }
              : { [key]: acc },
          {}
        );
      }
      return { [prismaOperator]: right.value };
    }
    case "in": {
      // Inclusion operator for lists
      const { path, relation } = resolveOperand(
        operands.find((o) => "name" in o)!
      );
      const { value } = resolveOperand(operands.find((o) => "value" in o)!);
      if (relation) {
        const relationOperator = getPrismaRelationOperator(relation);
        return {
          [relation.name]: { [relationOperator]: { [relation.field!]: value } },
        };
      }
      return path.reduceRight(
        (acc: any, key: string, index: number) =>
          index === path.length - 1 ? { [key]: { in: value } } : { [key]: acc },
        {}
      );
    }
    case "contains":
    case "startsWith":
    case "endsWith": {
      // String operators; ensure value is a string
      const leftOperand = operands.find((o) => "name" in o);
      if (!leftOperand) throw new Error("No operand with 'name' found");
      const rightOperand = operands.find((o) => "value" in o);
      if (!rightOperand) throw new Error("No operand with 'value' found");
      const { path, relation } = resolveOperand(leftOperand);
      const { value } = resolveOperand(rightOperand);
      if (typeof value !== "string")
        throw new Error(`${operator} operator requires string value`);
      if (relation) {
        const relationOperator = getPrismaRelationOperator(relation);
        return {
          [relation.name]: {
            [relationOperator]: { [relation.field!]: { [operator]: value } },
          },
        };
      }
      return path.reduceRight(
        (acc: any, key: string, index: number) =>
          index === path.length - 1
            ? { [key]: { [operator]: value } }
            : { [key]: acc },
        {}
      );
    }
    case "isSet": {
      // Check if a field is set (not null) or unset (null)
      const leftOperand = operands.find((o) => "name" in o);
      if (!leftOperand) throw new Error("No operand with 'name' found");
      const rightOperand = operands.find((o) => "value" in o);
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
        (acc: any, key: string, index: number) =>
          index === path.length - 1
            ? { [key]: value ? { not: null } : { equals: null } }
            : { [key]: acc },
        {}
      );
    }
    case "hasIntersection": {
      // Ensure exactly two operands for intersection check
      if (operands.length !== 2)
        throw new Error("hasIntersection requires exactly two operands");
      const [leftOperand, rightOperand] = operands;
      // Handle map expressions as first operand
      if ("operator" in leftOperand && leftOperand.operator === "map") {
        const mapResult = buildPrismaFilterFromCerbosExpression(
          leftOperand,
          mapper
        );
        const relationKey = Object.keys(mapResult)[0];
        const relation = mapResult[relationKey];
        const selectKey = Object.keys(
          relation[Object.keys(relation)[0]].select
        )[0];
        if (!("value" in rightOperand))
          throw new Error("Second operand of hasIntersection must be a value");
        return {
          [relationKey]: { some: { [selectKey]: { in: rightOperand.value } } },
        };
      }
      // Handle direct field reference
      if (!("name" in leftOperand))
        throw new Error(
          "First operand of hasIntersection must be a field reference or map expression"
        );
      if (!("value" in rightOperand))
        throw new Error("Second operand of hasIntersection must be a value");
      const { path, relation } = resolveFieldReference(
        leftOperand.name,
        mapper
      );
      if (!Array.isArray(rightOperand.value))
        throw new Error("hasIntersection requires an array value");
      if (relation) {
        return {
          [relation.name]: {
            some: { [relation.field!]: { in: rightOperand.value } },
          },
        };
      }
      return path.reduceRight(
        (acc, key, index) =>
          index === path.length - 1
            ? { [key]: { hasSome: rightOperand.value } }
            : { [key]: acc },
        {}
      );
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
      // Operators applying conditions on collections via lambda expressions
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
      const { relation } = resolveFieldReference(collection.name, mapper);
      if (!relation)
        throw new Error(`${operator} operator requires a relation mapping`);
      const lambdaCondition = buildPrismaFilterFromCerbosExpression(
        lambda,
        mapper
      );
      switch (operator) {
        case "exists":
          return { [relation.name]: { some: lambdaCondition } };
        case "exists_one":
          return {
            [relation.name]: { some: lambdaCondition },
            AND: [
              {
                [relation.name]: {
                  every: { OR: [lambdaCondition, { NOT: lambdaCondition }] },
                },
              },
            ],
          };
        case "all":
          return { [relation.name]: { every: lambdaCondition } };
        case "filter":
          return { [relation.name]: { some: lambdaCondition } };
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
      const { relation } = resolveFieldReference(collection.name, mapper);
      if (!relation)
        throw new Error("map operator requires a relation mapping");
      const [projection, variable] = lambda.operands;
      if (!("name" in projection) || !("name" in variable))
        throw new Error("Invalid map lambda expression structure");
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
    default:
      throw new Error(`Unsupported operator: ${operator}`);
  }
};
