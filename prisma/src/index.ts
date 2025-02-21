import {
  PlanResourcesResponse,
  PlanExpressionOperand,
  PlanKind as PK,
} from "@cerbos/core";

export type PlanKind = PK;
export const PlanKind = PK;

type PrismaFilter = Record<string, any>;
type FieldMapper =
  | {
      [key: string]: string;
    }
  | ((key: string) => string);

type Relation = {
  relation: string;
  field?: string; // Make field optional
  type: "one" | "many"; // Add type field to Relation interface
};

type RelationMapper =
  | {
      [key: string]: Relation;
    }
  | ((key: string) => Relation);

interface QueryPlanToPrismaArgs {
  queryPlan: PlanResourcesResponse;
  fieldNameMapper: FieldMapper;
  relationMapper?: RelationMapper;
}

interface QueryPlanToPrismaResult {
  kind: PlanKind;
  filters?: Record<string, any>;
}

export function queryPlanToPrisma({
  queryPlan,
  fieldNameMapper,
  relationMapper = {},
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
          fieldNameMapper,
          relationMapper
        ),
      };
    default:
      throw Error(`Invalid query plan.`);
  }
}

/**
 * Maps a field using the field mapper
 */
const mapField = (field: string, fieldMapper?: FieldMapper): string => {
  if (!fieldMapper) return field;

  return typeof fieldMapper === "function"
    ? fieldMapper(field)
    : fieldMapper[field] || field;
};

/**
 * Resolves a field reference considering relations
 */
const resolveFieldReference = (
  reference: string,
  fieldMapper?: FieldMapper,
  relationMapper?: RelationMapper
): { path: string[]; relation?: Relation } => {
  const mappedReference = mapField(reference, fieldMapper);

  if (!relationMapper) {
    return { path: mappedReference.split(".") };
  }

  // Check for relation using the full path first
  const relation =
    typeof relationMapper === "function"
      ? relationMapper(mappedReference)
      : relationMapper[mappedReference];

  if (relation) {
    return {
      path: [relation.relation, relation.field].filter(Boolean) as string[],
      relation,
    };
  }

  // If no direct match, check for relation prefix
  const parts = mappedReference.split(".");
  for (let i = parts.length - 1; i > 0; i--) {
    const prefix = parts.slice(0, i).join(".");
    const relationForPrefix =
      typeof relationMapper === "function"
        ? relationMapper(prefix)
        : relationMapper[prefix];

    if (relationForPrefix) {
      return {
        path: [relationForPrefix.relation, ...parts.slice(i)],
        relation: {
          ...relationForPrefix,
          field: parts.slice(i).join("."),
        },
      };
    }
  }

  return { path: mappedReference.split(".") };
};

/**
 * Determines the appropriate Prisma operator based on relation type and operation
 */
const getPrismaRelationOperator = (relation: Relation, operator: string) => {
  // Use 'is' for one-to-one relations, 'some' for one-to-many/many-to-many
  const relationOperator = relation.type === "one" ? "is" : "some";

  return relationOperator;
};

/**
 * Builds Prisma WHERE conditions from a Cerbos expression
 */
const buildPrismaFilterFromCerbosExpression = (
  expression: PlanExpressionOperand,
  fieldMapper?: FieldMapper,
  relationMapper?: RelationMapper
): PrismaFilter => {
  if (!("operator" in expression) || !("operands" in expression)) {
    throw new Error("Invalid Cerbos expression structure");
  }

  const { operator, operands } = expression;

  const resolveOperand = (operand: PlanExpressionOperand): any => {
    if ("name" in operand && operand.name) {
      return resolveFieldReference(operand.name, fieldMapper, relationMapper);
    } else if ("value" in operand && operand.value !== undefined) {
      return { value: operand.value };
    } else if ("operator" in operand) {
      // Handle nested expressions
      const nestedResult = buildPrismaFilterFromCerbosExpression(
        operand,
        fieldMapper,
        relationMapper
      );
      return { value: nestedResult };
    }
    throw new Error("Operand must have name, value, or be an expression");
  };

  switch (operator) {
    case "and": {
      return {
        AND: operands.map((operand: PlanExpressionOperand) =>
          buildPrismaFilterFromCerbosExpression(
            operand,
            fieldMapper,
            relationMapper
          )
        ),
      };
    }
    case "or": {
      return {
        OR: operands.map((operand: PlanExpressionOperand) =>
          buildPrismaFilterFromCerbosExpression(
            operand,
            fieldMapper,
            relationMapper
          )
        ),
      };
    }
    case "not": {
      return {
        NOT: buildPrismaFilterFromCerbosExpression(
          operands[0],
          fieldMapper,
          relationMapper
        ),
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
        // Field reference case
        const { path, relation } = left;
        if (relation) {
          const relationOperator = getPrismaRelationOperator(
            relation,
            operator
          );
          return {
            [relation.relation]: {
              [relationOperator]: {
                [relation.field!]: { [prismaOperator]: right.value },
              },
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

      // Handle nested expression case
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
        const relationOperator = getPrismaRelationOperator(relation, "in");
        return {
          [relation.relation]: {
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
        const relationOperator = getPrismaRelationOperator(relation, operator);
        return {
          [relation.relation]: {
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
        const relationOperator = getPrismaRelationOperator(relation, "isSet");
        return {
          [relation.relation]: {
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
          fieldMapper,
          relationMapper
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
        fieldMapper,
        relationMapper
      );

      if (!Array.isArray(rightOperand.value)) {
        throw new Error("hasIntersection requires an array value");
      }

      if (relation) {
        return {
          [relation.relation]: {
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
        (key: string) => key.replace(`${variable.name}.`, ""),
        relationMapper
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

      const { relation } = resolveFieldReference(
        collection.name,
        fieldMapper,
        relationMapper
      );

      if (!relation) {
        throw new Error(`${operator} operator requires a relation mapping`);
      }

      const lambdaCondition = buildPrismaFilterFromCerbosExpression(
        lambda,
        fieldMapper,
        relationMapper
      );

      switch (operator) {
        case "exists":
          return {
            [relation.relation]: {
              some: lambdaCondition,
            },
          };
        case "exists_one":
          return {
            [relation.relation]: {
              some: lambdaCondition,
            },
            AND: [
              {
                [relation.relation]: {
                  every: {
                    OR: [lambdaCondition, { NOT: lambdaCondition }],
                  },
                },
              },
            ],
          };
        case "all":
          return {
            [relation.relation]: {
              every: lambdaCondition,
            },
          };
        case "filter":
          return {
            [relation.relation]: {
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

      const { relation } = resolveFieldReference(
        collection.name,
        fieldMapper,
        relationMapper
      );

      if (!relation) {
        throw new Error("map operator requires a relation mapping");
      }

      const [projection, variable] = lambda.operands;
      if (!("name" in projection) || !("name" in variable)) {
        throw new Error("Invalid map lambda expression structure");
      }

      // For map operations, we project the field specified in the lambda
      return {
        [relation.relation]: {
          [getPrismaRelationOperator(relation, "map")]: {
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
