import {
  PlanExpressionOperand,
  PlanKind,
  PlanResourcesResponse,
  Value,
} from "@cerbos/core";
import {
  and,
  or,
  not,
  eq,
  ne,
  lt,
  lte,
  gt,
  gte,
  inArray,
  isNull,
  sql,
  exists,
} from "drizzle-orm";
import type { AnyColumn, SQL } from "drizzle-orm";

const TABLE_NAME = Symbol.for("drizzle:Name");
const FALSE_CONDITION = sql`0 = 1`;
const TRUE_CONDITION = sql`1 = 1`;

type RelationTable = unknown;

export { PlanKind };

export type DrizzleFilter = SQL;

const SCOPED_RELATION = Symbol("ScopedRelationEntry");

type ComparisonOperator =
  | "eq"
  | "ne"
  | "lt"
  | "le"
  | "gt"
  | "ge"
  | "in"
  | "contains"
  | "startsWith"
  | "endsWith"
  | "isSet";

type MapperTransform = (args: {
  operator: ComparisonOperator;
  value: Value;
}) => SQL;

type MappingConfig = {
  column?: AnyColumn;
  transform?: MapperTransform;
  relation?: RelationMapping;
};

interface RelationValue {
  kind: "relation";
  relation: RelationMapping;
}

type BaseMapperEntry =
  | AnyColumn
  | MappingConfig
  | MapperTransform
  | RelationValue;

interface ResolvedMapping {
  relations: RelationMapping[];
  mapping: BaseMapperEntry;
}

interface ScopedRelationEntry {
  [SCOPED_RELATION]: true;
  resolve: () => {
    relations: RelationMapping[];
    mapping: BaseMapperEntry;
  };
}

export type MapperEntry = BaseMapperEntry | ScopedRelationEntry;

export type Mapper =
  | {
      [key: string]: MapperEntry | undefined;
    }
  | ((reference: string) => MapperEntry | undefined);

export interface QueryPlanToDrizzleArgs {
  queryPlan: PlanResourcesResponse;
  mapper: Mapper;
}

export type QueryPlanToDrizzleResult =
  | {
      kind: PlanKind.ALWAYS_ALLOWED | PlanKind.ALWAYS_DENIED;
    }
  | {
      kind: PlanKind.CONDITIONAL;
      filter: DrizzleFilter;
    };

export interface RelationMapping {
  type: "one" | "many";
  table: RelationTable;
  sourceColumn: AnyColumn;
  targetColumn: AnyColumn;
  field?: MapperEntry;
  fields?: { [key: string]: MapperEntry };
}

const isScopedRelationEntry = (
  entry: MapperEntry
): entry is ScopedRelationEntry =>
  typeof entry === "object" &&
  entry !== null &&
  (entry as ScopedRelationEntry)[SCOPED_RELATION] === true;

const isMappingConfig = (entry: MapperEntry): entry is MappingConfig =>
  typeof entry === "object" &&
  entry !== null &&
  !isScopedRelationEntry(entry) &&
  ("column" in entry || "transform" in entry || "relation" in entry);

const isRelationValue = (entry: BaseMapperEntry): entry is RelationValue =>
  typeof entry === "object" &&
  entry !== null &&
  (entry as RelationValue).kind === "relation";

const toBaseMapperEntry = (entry: MapperEntry): BaseMapperEntry =>
  isScopedRelationEntry(entry) ? entry.resolve().mapping : entry;

const makeScopedRelationEntry = (resolution: ResolvedMapping): ScopedRelationEntry => ({
  [SCOPED_RELATION]: true,
  resolve: () => resolution,
});

type ScopedMapperMetadata = {
  leadingRelations: RelationMapping[];
  primaryRelation: RelationMapping;
};

const SCOPED_METADATA = Symbol("ScopedMapperMetadata");

const resolveRelationChain = (
  reference: string,
  mapper: Mapper
): RelationMapping[] => {
  const direct = getMappingEntry(reference, mapper);
  if (direct !== undefined) {
    if (isScopedRelationEntry(direct)) {
      return direct.resolve().relations;
    }
    if (isMappingConfig(direct) && direct.relation) {
      return [direct.relation];
    }
  }

  const parts = reference.split(".");
  for (let i = parts.length - 1; i > 0; i--) {
    const prefix = parts.slice(0, i).join(".");
    const suffix = parts.slice(i);
    const entry = getMappingEntry(prefix, mapper);
    if (!entry) {
      continue;
    }
    if (isScopedRelationEntry(entry)) {
      const resolved = entry.resolve();
      if (suffix.length === 0) {
        return resolved.relations;
      }
      if (isMappingConfig(resolved.mapping) && resolved.mapping.relation) {
        const nested = resolveRelationField(
          resolved.mapping.relation,
          suffix,
          reference,
          resolved.relations
        );
        return nested.relations;
      }
      continue;
    }
    if (isMappingConfig(entry) && entry.relation) {
      const resolved = resolveRelationField(
        entry.relation,
        suffix,
        reference,
        []
      );
      return resolved.relations;
    }
  }

  throw new Error(`No relation mapping found for reference: ${reference}`);
};

const createScopedMapper = (
  collectionReference: string,
  variableName: string,
  mapper: Mapper
): Mapper => {
  const relationChain = resolveRelationChain(collectionReference, mapper);
  if (relationChain.length === 0) {
    throw new Error(
      `No relation mapping found for reference: ${collectionReference}`
    );
  }
  const primaryRelation = relationChain[relationChain.length - 1];
  const leadingRelations = relationChain.slice(0, -1);

  const scopedMapper: Mapper = (reference: string) => {
    if (reference === variableName) {
      const resolved = resolveRelationField(
        primaryRelation,
        [],
        collectionReference,
        leadingRelations
      );
      return makeScopedRelationEntry(resolved);
    }

    if (reference.startsWith(`${variableName}.`)) {
      const remainder = reference.slice(variableName.length + 1);
      const parts = remainder.split(".");
      const resolved = resolveRelationField(
        primaryRelation,
        parts,
        `${collectionReference}.${remainder}`,
        leadingRelations
      );
      return makeScopedRelationEntry(resolved);
    }

    return getMappingEntry(reference, mapper);
  };

  if (typeof scopedMapper === "function") {
    (scopedMapper as Mapper & { [SCOPED_METADATA]?: ScopedMapperMetadata })[
      SCOPED_METADATA
    ] = {
      leadingRelations,
      primaryRelation,
    };
  }

  return scopedMapper;
};

const resolveRelationDefaultField = (
  resolved: { relations: RelationMapping[]; mapping: BaseMapperEntry },
  reference: string
): { relations: RelationMapping[]; mapping: BaseMapperEntry } => {
  if (!isRelationValue(resolved.mapping)) {
    return resolved;
  }
  const defaultField = resolved.mapping.relation.field;
  if (!defaultField) {
    throw new Error(
      `Relation mapping for '${reference}' does not define a default field`
    );
  }
  return {
    relations: resolved.relations,
    mapping: toBaseMapperEntry(defaultField),
  };
};

const isNameOperand = (
  operand: PlanExpressionOperand
): operand is { name: string } =>
  "name" in operand && typeof operand.name === "string";

const isValueOperand = (
  operand: PlanExpressionOperand
): operand is { value: Value } => "value" in operand;

const isExpressionOperand = (
  operand: PlanExpressionOperand
): operand is { operator: string; operands: PlanExpressionOperand[] } =>
  "operator" in operand && Array.isArray((operand as any).operands);

const getMappingEntry = (reference: string, mapper: Mapper): MapperEntry | undefined =>
  typeof mapper === "function" ? mapper(reference) : mapper[reference];

const getTableName = (table: RelationTable, reference: string): string => {
  const name = (table as { [key: symbol]: string | undefined })[TABLE_NAME];
  if (!name) {
    throw new Error(`Unable to resolve table name for relation: ${reference}`);
  }
  return name;
};

const wrapWithRelations = (
  relations: RelationMapping[],
  filter: SQL,
  reference: string,
  options?: { skipRelations?: Set<RelationMapping> }
): SQL => {
  return relations
    .slice()
    .reverse()
    .reduce((currentFilter, relation) => {
      if (options?.skipRelations?.has(relation)) {
        return currentFilter;
      }
      const joinCondition = eq(relation.targetColumn, relation.sourceColumn);
      const condition = and(joinCondition, currentFilter);
      const tableName = getTableName(relation.table, reference);
      return exists(
        sql`(select 1 from ${sql.raw(tableName)} where ${condition})`
      );
    }, filter);
};

const resolveRelationField = (
  relation: RelationMapping,
  path: string[],
  reference: string,
  accumulated: RelationMapping[],
  allowDefaultField = true
): { relations: RelationMapping[]; mapping: BaseMapperEntry } => {
  const relations = [...accumulated, relation];

  if (path.length === 0) {
    if (!allowDefaultField) {
      return { relations, mapping: { kind: "relation", relation } };
    }
    if (!relation.field) {
      throw new Error(
        `Relation mapping for '${reference}' does not define a default field`
      );
    }
    return { relations, mapping: toBaseMapperEntry(relation.field) };
  }

  const [segment, ...rest] = path;
  const fields = relation.fields ?? {};
  const fieldEntry = fields[segment];

  if (fieldEntry !== undefined) {
    if (isMappingConfig(fieldEntry) && fieldEntry.relation) {
      return resolveRelationField(
        fieldEntry.relation,
        rest,
        reference,
        relations
      );
    }
    if (rest.length > 0) {
      throw new Error(
        `Mapping for '${segment}' does not support further nesting in '${reference}'`
      );
    }
    return { relations, mapping: toBaseMapperEntry(fieldEntry) };
  }

  const inferredColumn = (relation.table as Record<string, MapperEntry>)[
    segment
  ];

  if (inferredColumn !== undefined) {
    if (rest.length > 0) {
      throw new Error(
        `Unable to resolve nested path '${segment}.${rest.join(".")}' for relation '${reference}'`
      );
    }
    return { relations, mapping: toBaseMapperEntry(inferredColumn) };
  }

  throw new Error(
    `No mapping found for relation segment '${segment}' in reference '${reference}'`
  );
};

const resolveFieldReference = (
  reference: string,
  mapper: Mapper
): { relations: RelationMapping[]; mapping: BaseMapperEntry } => {
  const direct = getMappingEntry(reference, mapper);
  if (direct !== undefined) {
    if (isScopedRelationEntry(direct)) {
      return direct.resolve();
    }
    if (isMappingConfig(direct) && direct.relation) {
      return resolveRelationField(direct.relation, [], reference, [], false);
    }
    return { relations: [], mapping: direct };
  }

  const parts = reference.split(".");
  for (let i = parts.length - 1; i > 0; i--) {
    const prefix = parts.slice(0, i).join(".");
    const suffix = parts.slice(i);
    const entry = getMappingEntry(prefix, mapper);
    if (!entry || !isMappingConfig(entry) || !entry.relation) {
      continue;
    }
    return resolveRelationField(entry.relation, suffix, reference, []);
  }

  throw new Error(`No mapping found for reference: ${reference}`);
};

const applyComparison = (
  mapping: BaseMapperEntry,
  operator: ComparisonOperator,
  value: Value
): SQL => {
  if (isRelationValue(mapping)) {
    return applyRelationComparison(mapping, operator);
  }
  if (typeof mapping === "function") {
    return mapping({ operator, value });
  }

  if (isMappingConfig(mapping)) {
    if (mapping.relation) {
      throw new Error("Relation mappings must be resolved before comparison");
    }
    if (mapping.transform) {
      return mapping.transform({ operator, value });
    }
    if (!mapping.column) {
      throw new Error("Mapping configuration requires a column or transform");
    }
    return applyComparison(mapping.column, operator, value);
  }

  const column = mapping as AnyColumn;

  switch (operator) {
    case "eq":
      return value === null ? isNull(column) : eq(column, value as any);
    case "ne":
      return value === null
        ? not(isNull(column))
        : ne(column, value as any);
    case "lt":
      return lt(column, value as any);
    case "le":
      return lte(column, value as any);
    case "gt":
      return gt(column, value as any);
    case "ge":
      return gte(column, value as any);
    case "in":
      if (!Array.isArray(value)) {
        return inArray(column, [value as any]);
      }
      return inArray(column, value as any[]);
    case "contains":
      if (typeof value !== "string") {
        throw new Error("The 'contains' operator requires a string value");
      }
      return sql`${column} LIKE ${`%${value}%`}`;
    case "startsWith":
      if (typeof value !== "string") {
        throw new Error("The 'startsWith' operator requires a string value");
      }
      return sql`${column} LIKE ${`${value}%`}`;
    case "endsWith":
      if (typeof value !== "string") {
        throw new Error("The 'endsWith' operator requires a string value");
      }
      return sql`${column} LIKE ${`%${value}`}`;
    case "isSet":
      if (typeof value !== "boolean") {
        throw new Error("The 'isSet' operator requires a boolean value");
      }
      return value ? not(isNull(column)) : isNull(column);
    default:
      throw new Error(`Unsupported operator: ${operator}`);
  }
};

function applyRelationComparison(
  relationValue: RelationValue,
  operator: ComparisonOperator
): SQL {
  switch (operator) {
    case "eq":
      return FALSE_CONDITION;
    case "ne":
      return TRUE_CONDITION;
    case "in":
      return FALSE_CONDITION;
    default:
      throw new Error(
        `Unsupported operator '${operator}' for relation comparison`
      );
  }
}

const extractArrayValue = (
  operand: PlanExpressionOperand
): Value[] | undefined => {
  if ("value" in operand && Array.isArray(operand.value)) {
    return operand.value as Value[];
  }
  return undefined;
};

const buildHasIntersectionFilter = (
  operands: PlanExpressionOperand[],
  mapper: Mapper
): SQL => {
  if (operands.length !== 2) {
    throw new Error("'hasIntersection' operator requires exactly two operands");
  }

  const [leftOperand, rightOperand] = operands;
  const rightValues = extractArrayValue(rightOperand) ?? [];

  if (rightValues.length === 0) {
    return FALSE_CONDITION;
  }

  const buildResolvedFilter = (
    resolved: { relations: RelationMapping[]; mapping: BaseMapperEntry },
    reference: string,
    wrapOptions?: BuildFilterOptions
  ) => {
    const normalized = resolveRelationDefaultField(resolved, reference);
    const filter = applyComparison(normalized.mapping, "in", rightValues);
    return normalized.relations.length
      ? wrapWithRelations(normalized.relations, filter, reference, wrapOptions)
      : filter;
  };

  if (isExpressionOperand(leftOperand) && leftOperand.operator === "map") {
    if (leftOperand.operands.length !== 2) {
      throw new Error("'map' operator within hasIntersection requires two operands");
    }
    const [collectionOperand, lambdaOperand] = leftOperand.operands;
    if (!isNameOperand(collectionOperand)) {
      throw new Error("Map collection operand must be a field reference");
    }
    if (!isExpressionOperand(lambdaOperand) || lambdaOperand.operator !== "lambda") {
      throw new Error("Map lambda operand must be a lambda expression");
    }
    const [projectionOperand, variableOperand] = lambdaOperand.operands;
    if (!isNameOperand(variableOperand) || !isNameOperand(projectionOperand)) {
      throw new Error("Invalid map lambda structure");
    }

    const scopedMapper = createScopedMapper(
      collectionOperand.name,
      variableOperand.name,
      mapper
    );
    const metadata = (scopedMapper as Mapper & {
      [SCOPED_METADATA]?: ScopedMapperMetadata;
    })[SCOPED_METADATA];
    if (metadata && metadata.leadingRelations.length > 0) {
      return FALSE_CONDITION;
    }
    const skipRelations =
      metadata !== undefined
        ? new Set<RelationMapping>([
            metadata.primaryRelation,
            ...metadata.leadingRelations,
          ])
        : undefined;
    const resolved = resolveFieldReference(
      projectionOperand.name,
      scopedMapper
    );
    const projectedFilter = buildResolvedFilter(
      resolved,
      projectionOperand.name,
      skipRelations ? { skipRelations } : undefined
    );
    if (!metadata) {
      return projectedFilter;
    }
    const withPrimary = wrapWithRelations(
      [metadata.primaryRelation],
      projectedFilter,
      projectionOperand.name
    );
    return metadata.leadingRelations.length
      ? wrapWithRelations(
          metadata.leadingRelations,
          withPrimary,
          projectionOperand.name
        )
      : withPrimary;
  }

  if (!isNameOperand(leftOperand)) {
    throw new Error(
      "'hasIntersection' requires a field reference or map expression as the first operand"
    );
  }

  const resolved = resolveFieldReference(leftOperand.name, mapper);
  return buildResolvedFilter(resolved, leftOperand.name);
};

const buildCollectionOperatorFilter = (
  operator: "exists" | "exists_one" | "filter" | "all",
  operands: PlanExpressionOperand[],
  mapper: Mapper
): SQL => {
  if (operands.length !== 2) {
    throw new Error(`'${operator}' operator requires exactly two operands`);
  }

  const [collectionOperand, lambdaOperand] = operands;
  if (!isNameOperand(collectionOperand)) {
    throw new Error("Collection operand must be a field reference");
  }
  if (!isExpressionOperand(lambdaOperand) || lambdaOperand.operator !== "lambda") {
    throw new Error("Lambda operand must be a lambda expression");
  }

  const [conditionOperand, variableOperand] = lambdaOperand.operands;
  if (!isNameOperand(variableOperand)) {
    throw new Error("Lambda variable must have a name operand");
  }

  const scopedMapper = createScopedMapper(
    collectionOperand.name,
    variableOperand.name,
    mapper
  );

  const relationChain = resolveRelationChain(collectionOperand.name, mapper);
  if (relationChain.length === 0) {
    throw new Error(`'${operator}' operator requires a relation mapping`);
  }

  const metadata = (scopedMapper as Mapper & {
    [SCOPED_METADATA]?: ScopedMapperMetadata;
  })[SCOPED_METADATA];
  const primaryRelation =
    metadata?.primaryRelation ?? relationChain[relationChain.length - 1];
  const leadingRelations =
    metadata?.leadingRelations ?? relationChain.slice(0, -1);
  const skipRelations = new Set<RelationMapping>([
    primaryRelation,
    ...leadingRelations,
  ]);

  const rowCondition = buildFilterFromExpression(
    conditionOperand,
    scopedMapper,
    { skipRelations }
  );

  const primaryFilter = wrapWithRelations(
    [primaryRelation],
    rowCondition,
    collectionOperand.name
  );

  const correlatedFilter = leadingRelations.length
    ? wrapWithRelations(
        leadingRelations,
        primaryFilter,
        collectionOperand.name
      )
    : primaryFilter;

  switch (operator) {
    case "filter":
      return correlatedFilter;
    case "exists":
      return correlatedFilter;
    case "all": {
      const failingFilter = wrapWithRelations(
        leadingRelations,
        wrapWithRelations(
          [primaryRelation],
          not(rowCondition),
          collectionOperand.name
        ),
        collectionOperand.name
      );
      return not(failingFilter);
    }
    case "exists_one": {
      const tableName = getTableName(
        primaryRelation.table,
        collectionOperand.name
      );
      const joinCondition = eq(
        primaryRelation.targetColumn,
        primaryRelation.sourceColumn
      );
      const matchCondition = and(joinCondition, rowCondition);
      if (!matchCondition) {
        return FALSE_CONDITION;
      }

      const countCheck = sql`(select count(*) from ${sql.raw(tableName)} where ${matchCondition}) = 1`;
      const combinedFilter = and(correlatedFilter, countCheck);
      if (!combinedFilter) {
        return FALSE_CONDITION;
      }
      return combinedFilter;
    }
  }

  throw new Error(`Unsupported collection operator: ${operator}`);
};

type BuildFilterOptions = {
  skipRelations?: Set<RelationMapping>;
};

const buildFilterFromExpression = (
  expression: PlanExpressionOperand,
  mapper: Mapper,
  options?: BuildFilterOptions
): SQL => {
  if (!isExpressionOperand(expression)) {
    throw new Error("Invalid expression operand");
  }

  const { operator, operands } = expression;

  switch (operator) {
    case "and": {
      if (operands.length === 0) {
        throw new Error("'and' operator requires at least one operand");
      }
      const filters = operands.map((operand) =>
        buildFilterFromExpression(operand, mapper, options)
      );
      const combined = and(...filters);
      if (!combined) {
        throw new Error("'and' operator produced an empty filter");
      }
      return combined;
    }
    case "or": {
      if (operands.length === 0) {
        throw new Error("'or' operator requires at least one operand");
      }
      const filters = operands.map((operand) =>
        buildFilterFromExpression(operand, mapper, options)
      );
      const combined = or(...filters);
      if (!combined) {
        throw new Error("'or' operator produced an empty filter");
      }
      return combined;
    }
    case "not": {
      if (operands.length !== 1) {
        throw new Error("'not' operator requires exactly one operand");
      }
      return not(buildFilterFromExpression(operands[0], mapper, options));
    }
    case "eq":
    case "ne":
    case "lt":
    case "le":
    case "gt":
    case "ge":
    case "in":
    case "contains":
    case "startsWith":
    case "endsWith":
    case "isSet": {
      const fieldOperand = operands.find(isNameOperand);
      if (!fieldOperand) {
        throw new Error("Comparison operator missing field operand");
      }
      const valueOperand = operands.find(isValueOperand);
      if (!valueOperand) {
        throw new Error("Comparison operator missing value operand");
      }
      const resolved = resolveFieldReference(fieldOperand.name, mapper);
      const filter = applyComparison(resolved.mapping, operator, valueOperand.value);
      return resolved.relations.length
        ? wrapWithRelations(
            resolved.relations,
            filter,
            fieldOperand.name,
            options
          )
        : filter;
    }
    case "hasIntersection":
      return buildHasIntersectionFilter(operands, mapper);
    case "exists":
    case "filter":
    case "all":
    case "exists_one":
      return buildCollectionOperatorFilter(
        operator as "exists" | "filter" | "all" | "exists_one",
        operands,
        mapper
      );
    default:
      throw new Error(`Unsupported operator: ${operator}`);
  }
};

export function queryPlanToDrizzle({
  queryPlan,
  mapper,
}: QueryPlanToDrizzleArgs): QueryPlanToDrizzleResult {
  switch (queryPlan.kind) {
    case PlanKind.ALWAYS_ALLOWED:
      return { kind: PlanKind.ALWAYS_ALLOWED };
    case PlanKind.ALWAYS_DENIED:
      return { kind: PlanKind.ALWAYS_DENIED };
    case PlanKind.CONDITIONAL:
      return {
        kind: PlanKind.CONDITIONAL,
        filter: buildFilterFromExpression(queryPlan.condition, mapper),
      };
    default:
      throw new Error("Invalid plan kind");
  }
}
