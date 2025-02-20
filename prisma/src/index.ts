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
  field: string;
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

  const relation =
    typeof relationMapper === "function"
      ? relationMapper(mappedReference)
      : relationMapper[mappedReference];

  if (relation) {
    return {
      path: [relation.relation, relation.field],
      relation,
    };
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
    }
    throw new Error("Operand must have name or value");
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
        (o: PlanExpressionOperand) => "name" in o
      );
      if (!leftOperand) throw new Error("No operand with 'name' found");

      const rightOperand = operands.find(
        (o: PlanExpressionOperand) => "value" in o
      );
      if (!rightOperand) throw new Error("No operand with 'value' found");

      const { path, relation } = resolveOperand(leftOperand);
      const { value } = resolveOperand(rightOperand);

      const prismaOperator = {
        eq: "equals",
        ne: "not",
        lt: "lt",
        le: "lte",
        gt: "gt",
        ge: "gte",
      }[operator];

      if (relation) {
        const relationOperator = getPrismaRelationOperator(relation, operator);
        return {
          [relation.relation]: {
            [relationOperator]: {
              [relation.field]: { [prismaOperator]: value },
            },
          },
        };
      }

      return path.reduceRight(
        (acc: PrismaFilter, key: string, index: number) => {
          return index === path.length - 1
            ? { [key]: { [prismaOperator]: value } }
            : { [key]: acc };
        },
        {}
      );
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
              [relation.field]: value,
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
              [relation.field]: { [operator]: value },
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
              [relation.field]: value ? { not: null } : { equals: null },
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

    default: {
      throw new Error(`Unsupported operator: ${operator}`);
    }
  }
};
