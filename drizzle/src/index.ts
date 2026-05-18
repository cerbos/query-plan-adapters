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
  like,
  isNull,
  sql,
  exists,
  getTableName,
} from "drizzle-orm";
import type { AnyColumn, SQL, Table } from "drizzle-orm";
import { Param } from "drizzle-orm/sql";

const FALSE_CONDITION = sql`0 = 1`;
const TRUE_CONDITION = sql`1 = 1`;

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
  table: Table;
  sourceColumn: AnyColumn;
  targetColumn: AnyColumn;
  field?: MapperEntry;
  fields?: { [key: string]: MapperEntry };
}

type ScopedMapperMetadata = {
  leadingRelations: RelationMapping[];
  primaryRelation: RelationMapping;
};

const scopedMapperMetadata = new WeakMap<
  (reference: string) => MapperEntry | undefined,
  ScopedMapperMetadata
>();

const isScopedRelationEntry = (
  entry: MapperEntry
): entry is ScopedRelationEntry =>
  typeof entry === "object" &&
  entry !== null &&
  SCOPED_RELATION in entry;

const isMappingConfig = (entry: MapperEntry): entry is MappingConfig =>
  typeof entry === "object" &&
  entry !== null &&
  !isScopedRelationEntry(entry) &&
  ("column" in entry || "transform" in entry || "relation" in entry);

const isRelationValue = (entry: BaseMapperEntry): entry is RelationValue =>
  typeof entry === "object" &&
  entry !== null &&
  "kind" in entry &&
  entry.kind === "relation";

const isColumn = (entry: BaseMapperEntry): entry is AnyColumn =>
  typeof entry === "object" &&
  entry !== null &&
  !isRelationValue(entry) &&
  !isMappingConfig(entry) &&
  typeof entry !== "function";

const toBaseMapperEntry = (entry: MapperEntry): BaseMapperEntry =>
  isScopedRelationEntry(entry) ? entry.resolve().mapping : entry;

const makeScopedRelationEntry = (resolution: ResolvedMapping): ScopedRelationEntry => ({
  [SCOPED_RELATION]: true,
  resolve: () => resolution,
});

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
  if (!primaryRelation) {
    throw new Error(
      `Unable to resolve primary relation for reference: ${collectionReference}`
    );
  }
  const leadingRelations = relationChain.slice(0, -1);

  const scopedFn = (reference: string): MapperEntry | undefined => {
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

  scopedMapperMetadata.set(scopedFn, {
    leadingRelations,
    primaryRelation,
  });

  return scopedFn;
};

const getScopedMetadata = (mapper: Mapper): ScopedMapperMetadata | undefined => {
  if (typeof mapper !== "function") {
    return undefined;
  }
  return scopedMapperMetadata.get(mapper);
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
  "operator" in operand && "operands" in operand && Array.isArray(operand.operands);

type NamedOperand = { name: string };

const looksLikeLambdaVariable = (operand: NamedOperand): boolean =>
  !operand.name.includes(".");

const extractLambdaComponents = (
  lambdaOperand: PlanExpressionOperand,
  context: string
): { variable: NamedOperand; expression: PlanExpressionOperand } => {
  if (!isExpressionOperand(lambdaOperand) || lambdaOperand.operator !== "lambda") {
    throw new Error(`${context} must be a lambda expression`);
  }
  if (lambdaOperand.operands.length !== 2) {
    throw new Error("Lambda operand requires exactly two operands");
  }
  const [first, second] = lambdaOperand.operands;
  if (!first || !second) {
    throw new Error("Lambda operand is missing operands");
  }

  const firstIsName = isNameOperand(first);
  const secondIsName = isNameOperand(second);
  const firstNameOperand = firstIsName ? first : undefined;
  const secondNameOperand = secondIsName ? second : undefined;

  if (!firstNameOperand && !secondNameOperand) {
    throw new Error("Lambda operand requires a variable operand");
  }

  if (firstNameOperand && !secondNameOperand) {
    return { variable: firstNameOperand, expression: second };
  }
  if (!firstNameOperand && secondNameOperand) {
    return { variable: secondNameOperand, expression: first };
  }

  const firstLooksLikeVariable = looksLikeLambdaVariable(firstNameOperand!);
  const secondLooksLikeVariable = looksLikeLambdaVariable(secondNameOperand!);

  if (firstLooksLikeVariable && !secondLooksLikeVariable) {
    return { variable: firstNameOperand!, expression: second };
  }
  if (!firstLooksLikeVariable && secondLooksLikeVariable) {
    return { variable: secondNameOperand!, expression: first };
  }

  return { variable: secondNameOperand!, expression: first };
};

const getMappingEntry = (reference: string, mapper: Mapper): MapperEntry | undefined =>
  typeof mapper === "function" ? mapper(reference) : mapper[reference];

const resolveTableName = (table: Table, reference: string): string => {
  try {
    return getTableName(table);
  } catch {
    throw new Error(`Unable to resolve table name for relation: ${reference}`);
  }
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
      const tableName = resolveTableName(relation.table, reference);
      return exists(
        sql`(select 1 from ${sql.identifier(tableName)} where ${condition})`
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
  if (segment === undefined) {
    throw new Error(
      `Invalid relation path for reference '${reference}': missing segment`
    );
  }
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

  const inferredColumn = (segment in relation.table)
    ? (relation.table as never)[segment]
    : undefined;

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

const ARITHMETIC_OPERATORS: Record<string, string> = {
  add: "+",
  sub: "-",
  mult: "*",
  div: "/",
  mod: "%",
};

const CONVERSION_TARGETS: Record<string, string> = {
  string: "TEXT",
  double: "REAL",
  int: "INTEGER",
};

const buildValueExpressionFromValue = (value: Value): SQL => sql`${value}`;

const buildColumnExpression = (
  mapping: BaseMapperEntry,
  reference: string
): SQL => {
  if (isRelationValue(mapping)) {
    throw new Error(
      `Cannot use relation '${reference}' as a scalar value expression`
    );
  }
  if (typeof mapping === "function") {
    throw new Error(
      `Cannot use transform mapping for '${reference}' as a value expression`
    );
  }
  if (isMappingConfig(mapping)) {
    if (mapping.relation) {
      throw new Error(
        `Cannot use relation mapping for '${reference}' as a scalar value expression`
      );
    }
    if (!mapping.column) {
      throw new Error(
        `Mapping for '${reference}' requires a column to be used as a value expression`
      );
    }
    return sql`${mapping.column}`;
  }
  if (!isColumn(mapping)) {
    throw new Error(`Expected column mapping for '${reference}'`);
  }
  return sql`${mapping}`;
};

const buildSizeExpression = (
  operand: PlanExpressionOperand,
  mapper: Mapper
): SQL => {
  if (!isNameOperand(operand)) {
    throw new Error("'size' operator requires a name operand");
  }
  // Determine whether the operand is a relation or a scalar column.
  let resolved: { relations: RelationMapping[]; mapping: BaseMapperEntry };
  try {
    resolved = resolveFieldReference(operand.name, mapper);
  } catch (err) {
    throw err;
  }
  // Relation: produce a correlated COUNT subquery walking the relation chain.
  if (resolved.relations.length > 0) {
    const relations = resolved.relations;
    const primary = relations[relations.length - 1]!;
    const tableName = resolveTableName(primary.table, operand.name);
    const joinCondition = eq(primary.targetColumn, primary.sourceColumn);
    const inner = sql`(select count(*) from ${sql.identifier(tableName)} where ${joinCondition})`;
    if (relations.length === 1) {
      return inner;
    }
    // Wrap with leading relation EXISTS contexts — uncommon, but support it.
    const leading = relations.slice(0, -1);
    return wrapWithRelations(leading, inner, operand.name);
  }
  // Scalar column: LENGTH(col).
  const colExpr = buildColumnExpression(resolved.mapping, operand.name);
  return sql`length(${colExpr})`;
};

const buildValueExpression = (
  operand: PlanExpressionOperand,
  mapper: Mapper
): SQL => {
  if (isValueOperand(operand)) {
    return buildValueExpressionFromValue(operand.value);
  }
  if (isNameOperand(operand)) {
    const resolved = resolveFieldReference(operand.name, mapper);
    if (resolved.relations.length > 0) {
      throw new Error(
        `Cannot use relation '${operand.name}' as a scalar value expression`
      );
    }
    return buildColumnExpression(resolved.mapping, operand.name);
  }
  if (!isExpressionOperand(operand)) {
    throw new Error("Invalid value-expression operand");
  }

  const { operator, operands } = operand;

  if (operator in ARITHMETIC_OPERATORS) {
    if (operands.length !== 2) {
      throw new Error(`Arithmetic operator '${operator}' requires two operands`);
    }
    const left = buildValueExpression(operands[0]!, mapper);
    const right = buildValueExpression(operands[1]!, mapper);
    const op = ARITHMETIC_OPERATORS[operator]!;
    return sql`(${left} ${sql.raw(op)} ${right})`;
  }

  if (operator in CONVERSION_TARGETS) {
    if (operands.length !== 1) {
      throw new Error(
        `Conversion operator '${operator}' requires exactly one operand`
      );
    }
    const inner = buildValueExpression(operands[0]!, mapper);
    const target = CONVERSION_TARGETS[operator]!;
    return sql`cast(${inner} as ${sql.raw(target)})`;
  }

  if (operator === "if") {
    if (operands.length !== 3) {
      throw new Error("'if' operator requires exactly three operands");
    }
    const cond = buildConditionFromOperand(operands[0]!, mapper);
    const thenExpr = buildValueExpression(operands[1]!, mapper);
    const elseExpr = buildValueExpression(operands[2]!, mapper);
    return sql`(case when ${cond} then ${thenExpr} else ${elseExpr} end)`;
  }

  if (operator === "size") {
    if (operands.length !== 1) {
      throw new Error("'size' operator requires exactly one operand");
    }
    return buildSizeExpression(operands[0]!, mapper);
  }

  if (operator === "index") {
    throw new Error(
      "'index' operator (array indexing) is not supported by the Drizzle adapter"
    );
  }

  throw new Error(`Unsupported value-expression operator: ${operator}`);
};

// Build a boolean SQL condition from an operand. Used for ternary `if` test branch.
// Falls back to coercing scalar/name operands to a truthiness check.
const buildConditionFromOperand = (
  operand: PlanExpressionOperand,
  mapper: Mapper
): SQL => {
  if (isExpressionOperand(operand)) {
    return buildFilterFromExpression(operand, mapper);
  }
  if (isNameOperand(operand)) {
    const resolved = resolveFieldReference(operand.name, mapper);
    const colExpr = buildColumnExpression(resolved.mapping, operand.name);
    return sql`${colExpr} = 1`;
  }
  if (isValueOperand(operand)) {
    return operand.value
      ? TRUE_CONDITION
      : FALSE_CONDITION;
  }
  throw new Error("Invalid condition operand");
};

const applyComparisonWithExpression = (
  operator: ComparisonOperator,
  fieldExpr: SQL,
  valueExpr: SQL
): SQL => {
  switch (operator) {
    case "eq":
      return sql`${fieldExpr} = ${valueExpr}`;
    case "ne":
      return sql`${fieldExpr} <> ${valueExpr}`;
    case "lt":
      return sql`${fieldExpr} < ${valueExpr}`;
    case "le":
      return sql`${fieldExpr} <= ${valueExpr}`;
    case "gt":
      return sql`${fieldExpr} > ${valueExpr}`;
    case "ge":
      return sql`${fieldExpr} >= ${valueExpr}`;
    default:
      throw new Error(
        `Operator '${operator}' is not supported for expression-valued operands`
      );
  }
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

  if (!isColumn(mapping)) {
    throw new Error("Expected a column mapping");
  }
  const column: AnyColumn = mapping;
  const bound = new Param(value, column);

  switch (operator) {
    case "eq":
      return value === null ? isNull(column) : sql`${column} = ${bound}`;
    case "ne":
      return value === null
        ? not(isNull(column))
        : sql`${column} <> ${bound}`;
    case "lt":
      return sql`${column} < ${bound}`;
    case "le":
      return sql`${column} <= ${bound}`;
    case "gt":
      return sql`${column} > ${bound}`;
    case "ge":
      return sql`${column} >= ${bound}`;
    case "in": {
      const values = Array.isArray(value) ? value : [value];
      return sql`${column} in ${values.map(v => new Param(v, column))}`;
    }
    case "contains":
      if (typeof value !== "string") {
        throw new Error("The 'contains' operator requires a string value");
      }
      return like(column, `%${value}%`);
    case "startsWith":
      if (typeof value !== "string") {
        throw new Error("The 'startsWith' operator requires a string value");
      }
      return like(column, `${value}%`);
    case "endsWith":
      if (typeof value !== "string") {
        throw new Error("The 'endsWith' operator requires a string value");
      }
      return like(column, `%${value}`);
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
  _relationValue: RelationValue,
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
    return operand.value;
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

  const leftOperand = operands[0];
  const rightOperand = operands[1];
  if (!leftOperand || !rightOperand) {
    throw new Error("'hasIntersection' requires exactly two operands");
  }
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
    const collectionOperand = leftOperand.operands[0];
    const lambdaOperand = leftOperand.operands[1];
    if (!collectionOperand || !lambdaOperand) {
      throw new Error("Map expression is missing operands");
    }
    if (!isNameOperand(collectionOperand)) {
      throw new Error("Map collection operand must be a field reference");
    }
    const { variable: variableOperand, expression: projectionOperand } =
      extractLambdaComponents(lambdaOperand, "Map lambda operand");
    if (!isNameOperand(variableOperand) || !isNameOperand(projectionOperand)) {
      throw new Error("Invalid map lambda structure");
    }

    const scopedMapper = createScopedMapper(
      collectionOperand.name,
      variableOperand.name,
      mapper
    );
    const metadata = getScopedMetadata(scopedMapper);
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

type CollectionOperator = "exists" | "exists_one" | "filter" | "all" | "except";

const buildCollectionOperatorFilter = (
  operator: CollectionOperator,
  operands: PlanExpressionOperand[],
  mapper: Mapper
): SQL => {
  if (operands.length !== 2) {
    throw new Error(`'${operator}' operator requires exactly two operands`);
  }

  const collectionOperand = operands[0];
  const lambdaOperand = operands[1];
  if (!collectionOperand || !lambdaOperand) {
    throw new Error(`'${operator}' operator requires collection and lambda operands`);
  }
  if (!isNameOperand(collectionOperand)) {
    throw new Error("Collection operand must be a field reference");
  }
  const { variable: variableOperand, expression: conditionOperand } =
    extractLambdaComponents(lambdaOperand, `'${operator}' lambda operand`);
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
  const fallbackPrimaryRelation = relationChain[relationChain.length - 1];
  if (!fallbackPrimaryRelation) {
    throw new Error(`Unable to resolve primary relation for '${collectionOperand.name}'`);
  }

  const metadata = getScopedMetadata(scopedMapper);
  const primaryRelation: RelationMapping =
    metadata?.primaryRelation ?? fallbackPrimaryRelation;
  const leadingRelations: RelationMapping[] =
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
      return FALSE_CONDITION;
    case "exists":
      return correlatedFilter;
    case "except": {
      const exceptFilter = wrapWithRelations(
        [primaryRelation],
        not(rowCondition),
        collectionOperand.name
      );
      return leadingRelations.length
        ? wrapWithRelations(
            leadingRelations,
            exceptFilter,
            collectionOperand.name
          )
        : exceptFilter;
    }
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
      const tableName = resolveTableName(
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

      const countCheck = sql`(select count(*) from ${sql.identifier(tableName)} where ${matchCondition}) = 1`;
      const wrappedCountCheck = leadingRelations.length
        ? wrapWithRelations(
            leadingRelations,
            countCheck,
            collectionOperand.name
          )
        : countCheck;
      const combinedFilter = and(correlatedFilter, wrappedCountCheck);
      if (!combinedFilter) {
        return FALSE_CONDITION;
      }
      return combinedFilter;
    }
    default:
      throw new Error(`Unsupported collection operator: ${operator}`);
  }
};

type BuildFilterOptions = {
  skipRelations?: Set<RelationMapping>;
};

const COLLECTION_OPERATORS: Record<string, CollectionOperator> = {
  exists: "exists",
  filter: "filter",
  all: "all",
  except: "except",
  exists_one: "exists_one",
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
      const operand = operands[0];
      if (!operand) {
        throw new Error("'not' operator is missing operand");
      }
      return not(buildFilterFromExpression(operand, mapper, options));
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
      // Detect an expression-valued operand on either side (e.g. arithmetic,
      // type-conversion, `if`, or `size`). When present, evaluate both sides
      // as SQL value expressions and emit a raw comparison.
      if (
        operator !== "in" &&
        operator !== "contains" &&
        operator !== "startsWith" &&
        operator !== "endsWith" &&
        operator !== "isSet" &&
        operands.length === 2 &&
        operands.some(isExpressionOperand)
      ) {
        const [leftOperand, rightOperand] = operands;
        if (!leftOperand || !rightOperand) {
          throw new Error("Comparison operator requires two operands");
        }
        const leftExpr = buildValueExpression(leftOperand, mapper);
        const rightExpr = buildValueExpression(rightOperand, mapper);
        return applyComparisonWithExpression(operator, leftExpr, rightExpr);
      }
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
    case "matches": {
      if (operands.length !== 2) {
        throw new Error("'matches' operator requires exactly two operands");
      }
      const fieldOperand = operands[0];
      const patternOperand = operands[1];
      if (!fieldOperand || !isNameOperand(fieldOperand)) {
        throw new Error("'matches' first operand must be a field reference");
      }
      if (!patternOperand || !isValueOperand(patternOperand)) {
        throw new Error("'matches' second operand must be a regex value");
      }
      if (typeof patternOperand.value !== "string") {
        throw new Error("'matches' regex pattern must be a string");
      }
      const resolved = resolveFieldReference(fieldOperand.name, mapper);
      if (resolved.relations.length > 0) {
        throw new Error(
          "'matches' on a relation-valued field is not supported"
        );
      }
      const colExpr = buildColumnExpression(resolved.mapping, fieldOperand.name);
      const filter = sql`${colExpr} regexp ${patternOperand.value}`;
      return filter;
    }
    case "hasIntersection":
      return buildHasIntersectionFilter(operands, mapper);
    default: {
      const collectionOp = COLLECTION_OPERATORS[operator];
      if (collectionOp) {
        return buildCollectionOperatorFilter(collectionOp, operands, mapper);
      }
      throw new Error(`Unsupported operator: ${operator}`);
    }
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
