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

// Mirror map for value-first comparisons: the planner preserves source order, so
// `3 <= R.attr.aNumber` arrives as le(value, variable) and must become `aNumber >= 3`,
// never `aNumber <= 3` (see cerbos/query-plan-adapters#258/#259 for the same bug class
// in other adapters).
const MIRRORED_OPERATORS: Record<string, ComparisonOperator> = {
  eq: "eq",
  ne: "ne",
  lt: "gt",
  le: "ge",
  gt: "lt",
  ge: "le",
};

type StringMatchOperator = "contains" | "startsWith" | "endsWith";

// CEL-exact string matching: instr/substr are case-sensitive, interpret no LIKE
// metacharacters (% _ \ in the needle match literally), and propagate NULL as SQL
// UNKNOWN — which excludes the row under both polarities, mirroring the CEL
// missing-attribute error (deny). The receiver is ALWAYS the haystack and the needle
// ALWAYS the pattern; operands are never swapped.
const buildStringMatchCondition = (
  operator: StringMatchOperator,
  receiver: SQL,
  needle: SQL
): SQL => {
  switch (operator) {
    case "contains":
      return sql`instr(${receiver}, ${needle}) > 0`;
    case "startsWith":
      return sql`substr(${receiver}, 1, length(${needle})) = ${needle}`;
    case "endsWith":
      return sql`substr(${receiver}, length(${receiver}) - length(${needle}) + 1) = ${needle}`;
  }
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

/**
 * Resolve a collection-macro scope: the scoped mapper for the lambda variable plus the
 * relation chain split into the primary (innermost) relation and any leading hops.
 */
const resolveCollectionScope = (
  collectionOperand: PlanExpressionOperand,
  lambdaOperand: PlanExpressionOperand,
  context: string,
  mapper: Mapper
): {
  collectionName: string;
  scopedMapper: Mapper;
  primaryRelation: RelationMapping;
  leadingRelations: RelationMapping[];
  skipRelations: Set<RelationMapping>;
  conditionOperand: PlanExpressionOperand;
} => {
  if (!isNameOperand(collectionOperand)) {
    throw new Error("Collection operand must be a field reference");
  }
  const { variable: variableOperand, expression: conditionOperand } =
    extractLambdaComponents(lambdaOperand, context);
  if (!isNameOperand(variableOperand)) {
    throw new Error("Lambda variable must have a name operand");
  }

  const scopedMapper = createScopedMapper(
    collectionOperand.name,
    variableOperand.name,
    mapper
  );

  const relationChain = resolveRelationChain(collectionOperand.name, mapper);
  const fallbackPrimaryRelation = relationChain[relationChain.length - 1];
  if (!fallbackPrimaryRelation) {
    throw new Error(
      `Unable to resolve primary relation for '${collectionOperand.name}'`
    );
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

  return {
    collectionName: collectionOperand.name,
    scopedMapper,
    primaryRelation,
    leadingRelations,
    skipRelations,
    conditionOperand,
  };
};

const buildSizeExpression = (
  operand: PlanExpressionOperand,
  mapper: Mapper,
  options?: BuildFilterOptions
): SQL => {
  // size(filter(coll, lambda)): COUNT with the lambda condition as the predicate. An
  // element whose condition is UNKNOWN (NULL column) poisons the whole count to NULL —
  // mirroring CEL, where filter() surfaces the missing-attribute error instead of
  // skipping the element — so the enclosing comparison is UNKNOWN and the row is
  // excluded under both polarities.
  if (isExpressionOperand(operand) && operand.operator === "filter") {
    if (operand.operands.length !== 2) {
      throw new Error("'filter' operator requires exactly two operands");
    }
    const [collectionOperand, lambdaOperand] = operand.operands;
    if (!collectionOperand || !lambdaOperand) {
      throw new Error("'filter' operator requires collection and lambda operands");
    }
    const scope = resolveCollectionScope(
      collectionOperand,
      lambdaOperand,
      "'filter' lambda operand",
      mapper
    );
    const rowCondition = buildFilterFromExpression(
      scope.conditionOperand,
      scope.scopedMapper,
      { skipRelations: scope.skipRelations }
    );
    const tableName = resolveTableName(
      scope.primaryRelation.table,
      scope.collectionName
    );
    const joinCondition = eq(
      scope.primaryRelation.targetColumn,
      scope.primaryRelation.sourceColumn
    );
    const chainWhere = scope.leadingRelations.length
      ? wrapWithRelations(
          scope.leadingRelations,
          joinCondition,
          scope.collectionName,
          options
        )
      : joinCondition;
    return sql`(select case when coalesce(sum(case when (${rowCondition}) is null then 1 else 0 end), 0) > 0 then null else coalesce(sum(case when ${rowCondition} then 1 else 0 end), 0) end from ${sql.identifier(tableName)} where ${chainWhere})`;
  }

  if (!isNameOperand(operand)) {
    throw new Error(
      "'size' operator requires a field reference or filter expression"
    );
  }
  // Determine whether the operand is a relation or a scalar column.
  const resolved = resolveFieldReference(operand.name, mapper);
  // Relation: produce a correlated COUNT subquery over the tail of the relation chain,
  // joining THROUGH every intermediate hop (never straight off the root).
  if (resolved.relations.length > 0) {
    const relations = resolved.relations;
    const primary = relations[relations.length - 1]!;
    const leading = relations.slice(0, -1);
    const tableName = resolveTableName(primary.table, operand.name);
    const joinCondition = eq(primary.targetColumn, primary.sourceColumn);
    const chainWhere = leading.length
      ? wrapWithRelations(leading, joinCondition, operand.name, options)
      : joinCondition;
    return sql`(select count(*) from ${sql.identifier(tableName)} where ${chainWhere})`;
  }
  // Scalar column: LENGTH(col).
  const colExpr = buildColumnExpression(resolved.mapping, operand.name);
  return sql`length(${colExpr})`;
};

const buildValueExpression = (
  operand: PlanExpressionOperand,
  mapper: Mapper,
  options?: BuildFilterOptions
): SQL => {
  if (isValueOperand(operand)) {
    return buildValueExpressionFromValue(operand.value);
  }
  if (isNameOperand(operand)) {
    const resolved = resolveFieldReference(operand.name, mapper);
    // Relations already established by an enclosing lambda subquery (skipRelations) leave
    // the element column directly addressable; anything else cannot be a scalar.
    const unskipped = resolved.relations.filter(
      (relation) => !options?.skipRelations?.has(relation)
    );
    if (unskipped.length > 0) {
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
    const left = buildValueExpression(operands[0]!, mapper, options);
    const right = buildValueExpression(operands[1]!, mapper, options);
    if (operator === "div") {
      // CEL attribute arithmetic is double-typed: force REAL division so an
      // INTEGER/INTEGER pair does not silently truncate (3 / 2 must be 1.5, not 1).
      // SQLite yields NULL for division by zero — UNKNOWN, excluded under both
      // polarities — matching CEL's NaN comparisons (always false → deny).
      return sql`(cast(${left} as real) / ${right})`;
    }
    const op = ARITHMETIC_OPERATORS[operator]!;
    return sql`(${left} ${sql.raw(op)} ${right})`;
  }

  if (operator in CONVERSION_TARGETS) {
    if (operands.length !== 1) {
      throw new Error(
        `Conversion operator '${operator}' requires exactly one operand`
      );
    }
    const inner = buildValueExpression(operands[0]!, mapper, options);
    const target = CONVERSION_TARGETS[operator]!;
    return sql`cast(${inner} as ${sql.raw(target)})`;
  }

  if (operator === "if") {
    if (operands.length !== 3) {
      throw new Error("'if' operator requires exactly three operands");
    }
    const cond = buildConditionFromOperand(operands[0]!, mapper, options);
    const thenExpr = buildValueExpression(operands[1]!, mapper, options);
    const elseExpr = buildValueExpression(operands[2]!, mapper, options);
    // Two guarded WHEN arms (no bare ELSE): an UNKNOWN condition matches neither arm and
    // the CASE yields NULL, so the enclosing comparison stays UNKNOWN — excluded under
    // both polarities, mirroring the CEL missing-attribute error (deny). A bare ELSE
    // would silently route UNKNOWN rows into the else branch.
    return sql`(case when ${cond} then ${thenExpr} when not (${cond}) then ${elseExpr} end)`;
  }

  if (operator === "size") {
    if (operands.length !== 1) {
      throw new Error("'size' operator requires exactly one operand");
    }
    return buildSizeExpression(operands[0]!, mapper, options);
  }

  if (operator === "index") {
    throw new Error(
      "'index' operator (array indexing) is not supported by the Drizzle adapter"
    );
  }

  throw new Error(`Unsupported value-expression operator: ${operator}`);
};

// Build a boolean SQL condition from an operand. Used for ternary `if` test branches;
// delegates to the filter builder, which handles expression, name, and value operands.
const buildConditionFromOperand = (
  operand: PlanExpressionOperand,
  mapper: Mapper,
  options?: BuildFilterOptions
): SQL => buildFilterFromExpression(operand, mapper, options);

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
    case "startsWith":
    case "endsWith":
      if (typeof value !== "string") {
        throw new Error(`The '${operator}' operator requires a string value`);
      }
      return buildStringMatchCondition(operator, sql`${column}`, sql`${value}`);
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

interface ResolvedScalarOperand {
  expr: SQL;
  relations: RelationMapping[];
}

// Resolve an operand into a scalar SQL expression plus any relation chain its column
// lives behind (empty for values and computed expressions).
const resolveScalarOperand = (
  operand: PlanExpressionOperand,
  mapper: Mapper,
  options?: BuildFilterOptions
): ResolvedScalarOperand => {
  if (isValueOperand(operand)) {
    return { expr: buildValueExpressionFromValue(operand.value), relations: [] };
  }
  if (isNameOperand(operand)) {
    const resolved = resolveFieldReference(operand.name, mapper);
    return {
      expr: buildColumnExpression(resolved.mapping, operand.name),
      relations: resolved.relations,
    };
  }
  return { expr: buildValueExpression(operand, mapper, options), relations: [] };
};

// Wrap a filter with two operands' relation chains (deduplicated by identity) so both
// sides' columns are in scope: the primary chain innermost, any extra relations from the
// secondary chain around it. skipRelations (enclosing lambda scopes) are honoured.
const wrapCombinedRelations = (
  filter: SQL,
  primary: RelationMapping[],
  secondary: RelationMapping[],
  reference: string,
  options?: BuildFilterOptions
): SQL => {
  let wrapped = filter;
  if (primary.length) {
    wrapped = wrapWithRelations(primary, wrapped, reference, options);
  }
  const seen = new Set(primary);
  const extra = secondary.filter((relation) => !seen.has(relation));
  if (extra.length) {
    wrapped = wrapWithRelations(extra, wrapped, reference, options);
  }
  return wrapped;
};

// Field-or-constant string matching (contains/startsWith/endsWith) with the receiver as
// the haystack and the needle as the pattern, in wire order — the planner preserves
// source order, so `"const".contains(R.attr.col)` arrives as contains(value, variable)
// and must NOT be operand-order normalized (a swap silently inverts haystack and needle).
const buildStringMatchFilter = (
  operator: StringMatchOperator,
  operands: PlanExpressionOperand[],
  mapper: Mapper,
  options?: BuildFilterOptions
): SQL => {
  if (operands.length !== 2) {
    throw new Error(`'${operator}' operator requires exactly two operands`);
  }
  const [receiverOperand, needleOperand] = operands;
  if (!receiverOperand || !needleOperand) {
    throw new Error(
      `'${operator}' operator requires receiver and needle operands`
    );
  }

  // Column receiver with a constant needle: transform/function mappings own their own
  // match semantics, so keep routing those through applyComparison.
  if (isNameOperand(receiverOperand) && isValueOperand(needleOperand)) {
    const resolved = resolveFieldReference(receiverOperand.name, mapper);
    const mapping = resolved.mapping;
    if (
      typeof mapping === "function" ||
      isRelationValue(mapping) ||
      (isMappingConfig(mapping) && mapping.transform !== undefined)
    ) {
      const filter = applyComparison(mapping, operator, needleOperand.value);
      return resolved.relations.length
        ? wrapWithRelations(
            resolved.relations,
            filter,
            receiverOperand.name,
            options
          )
        : filter;
    }
  }

  if (isValueOperand(needleOperand) && typeof needleOperand.value !== "string") {
    throw new Error(`The '${operator}' operator requires a string value`);
  }
  const receiver = resolveScalarOperand(receiverOperand, mapper, options);
  const needle = resolveScalarOperand(needleOperand, mapper, options);
  const filter = buildStringMatchCondition(operator, receiver.expr, needle.expr);
  const reference = isNameOperand(receiverOperand)
    ? receiverOperand.name
    : isNameOperand(needleOperand)
      ? needleOperand.name
      : `'${operator}' operand`;
  return wrapCombinedRelations(
    filter,
    receiver.relations,
    needle.relations,
    reference,
    options
  );
};

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

  // CEL projects EVERY element before intersecting, so an element whose projected
  // attribute is missing (a NULL column) is an evaluation error — deny — even when
  // another element intersects. Guard with NOT EXISTS(element with NULL projection);
  // a no-op for NOT NULL columns.
  const nullProjectionGuard = (
    relations: RelationMapping[],
    mapping: BaseMapperEntry,
    reference: string,
    wrapOptions?: BuildFilterOptions
  ): SQL | undefined => {
    const effective = relations.filter(
      (relation) => !wrapOptions?.skipRelations?.has(relation)
    );
    if (!effective.length) {
      return undefined;
    }
    const isPlainColumn =
      isColumn(mapping) ||
      (isMappingConfig(mapping) &&
        mapping.column !== undefined &&
        !mapping.relation &&
        !mapping.transform);
    if (!isPlainColumn) {
      return undefined;
    }
    const colExpr = buildColumnExpression(mapping, reference);
    return not(
      wrapWithRelations(
        relations,
        sql`${colExpr} is null`,
        reference,
        wrapOptions
      )
    );
  };

  const buildResolvedFilter = (
    resolved: { relations: RelationMapping[]; mapping: BaseMapperEntry },
    reference: string,
    wrapOptions?: BuildFilterOptions
  ) => {
    const normalized = resolveRelationDefaultField(resolved, reference);
    const filter = applyComparison(normalized.mapping, "in", rightValues);
    if (!normalized.relations.length) {
      return filter;
    }
    const wrapped = wrapWithRelations(
      normalized.relations,
      filter,
      reference,
      wrapOptions
    );
    const guard = nullProjectionGuard(
      normalized.relations,
      normalized.mapping,
      reference,
      wrapOptions
    );
    if (!guard) {
      return wrapped;
    }
    return and(wrapped, guard) ?? wrapped;
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
    const normalizedProjection = resolveRelationDefaultField(
      resolved,
      projectionOperand.name
    );
    // Exclude rows whose projected element column is NULL (CEL map() errors on a missing
    // element attribute → deny). Only emit this guard when the projection is a direct column
    // of the primary relation's table (no relations beyond the primary/leading ones already
    // skipped): when it lives behind further nested relations, buildResolvedFilter already
    // wrapped an equivalent NULL guard through that chain inside projectedFilter, and
    // re-guarding here with only the primary relation would reference the nested table's
    // column without joining it.
    const projectionBeyondPrimary = normalizedProjection.relations.filter(
      (relation) => !skipRelations?.has(relation)
    );
    const guard =
      projectionBeyondPrimary.length === 0
        ? nullProjectionGuard(
            [metadata.primaryRelation],
            normalizedProjection.mapping,
            projectionOperand.name
          )
        : undefined;
    const guarded = guard
      ? and(withPrimary, guard) ?? withPrimary
      : withPrimary;
    return metadata.leadingRelations.length
      ? wrapWithRelations(
          metadata.leadingRelations,
          guarded,
          projectionOperand.name
        )
      : guarded;
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

/**
 * Collection macros with CEL's three-valued semantics. An element whose lambda condition
 * is UNKNOWN (a NULL element column) is a CEL missing-attribute evaluation error:
 *
 * - exists  = TRUE on a true witness (absorbs errors), else error if any element errors;
 * - all     = FALSE on a false witness (absorbs errors), else error if any element errors;
 * - exists_one errors on ANY erroring element, never absorbed.
 *
 * A CEL error is a deny, and stays a deny under negation (!error is still an error) —
 * so the negated forms are NOT the plain SQL NOT of the positive forms. The `negated`
 * flag selects the correct polarity-specific translation using SQLite's IS [NOT]
 * TRUE/FALSE, which distinguish UNKNOWN from FALSE.
 */
const buildCollectionOperatorFilter = (
  operator: CollectionOperator,
  operands: PlanExpressionOperand[],
  mapper: Mapper,
  negated: boolean,
  options?: BuildFilterOptions
): SQL => {
  if (operands.length !== 2) {
    throw new Error(`'${operator}' operator requires exactly two operands`);
  }

  const collectionOperand = operands[0];
  const lambdaOperand = operands[1];
  if (!collectionOperand || !lambdaOperand) {
    throw new Error(`'${operator}' operator requires collection and lambda operands`);
  }

  const scope = resolveCollectionScope(
    collectionOperand,
    lambdaOperand,
    `'${operator}' lambda operand`,
    mapper
  );
  const { primaryRelation, leadingRelations, collectionName } = scope;

  const rowCondition = buildFilterFromExpression(
    scope.conditionOperand,
    scope.scopedMapper,
    { skipRelations: scope.skipRelations }
  );

  // Leading hops already established by an enclosing lambda scope (options.skipRelations)
  // must not be re-joined off the root — the subquery correlates against the enclosing
  // scope's table instead.
  const wrapLeading = (inner: SQL): SQL =>
    leadingRelations.length
      ? wrapWithRelations(leadingRelations, inner, collectionName, options)
      : inner;
  const wrapAll = (inner: SQL): SQL =>
    wrapLeading(
      wrapWithRelations([primaryRelation], inner, collectionName)
    );

  switch (operator) {
    case "filter": {
      const filter = FALSE_CONDITION;
      return negated ? not(filter) : filter;
    }
    case "exists":
      // Positive: a true witness. Negated (exists = FALSE): EVERY element is determined
      // false — an UNKNOWN element would be an unabsorbed CEL error (deny).
      return negated
        ? not(wrapAll(sql`(${rowCondition}) is not false`))
        : wrapAll(rowCondition);
    case "except": {
      const exceptFilter = wrapAll(not(rowCondition));
      return negated ? not(exceptFilter) : exceptFilter;
    }
    case "all":
      // Positive: no element is false OR unknown. Negated (all = FALSE): a determined
      // false witness exists (which absorbs sibling errors in CEL).
      return negated
        ? wrapAll(sql`(${rowCondition}) is false`)
        : not(wrapAll(sql`(${rowCondition}) is not true`));
    case "exists_one": {
      const tableName = resolveTableName(primaryRelation.table, collectionName);
      const joinCondition = eq(
        primaryRelation.targetColumn,
        primaryRelation.sourceColumn
      );
      const matchCondition = and(joinCondition, rowCondition);
      if (!matchCondition) {
        return FALSE_CONDITION;
      }

      const countExpr = sql`(select count(*) from ${sql.identifier(tableName)} where ${matchCondition})`;
      const countCheck = negated
        ? sql`${countExpr} <> 1`
        : sql`${countExpr} = 1`;
      // exists_one never absorbs an erroring element: ANY unknown-condition element is a
      // CEL error (deny) regardless of polarity.
      const unknownGuard = not(
        wrapWithRelations(
          [primaryRelation],
          sql`(${rowCondition}) is null`,
          collectionName
        )
      );
      const combinedFilter = and(countCheck, unknownGuard);
      if (!combinedFilter) {
        return FALSE_CONDITION;
      }
      return wrapLeading(combinedFilter);
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

/**
 * Build a boolean filter from a plan operand, tracking negation polarity instead of
 * emitting a plain SQL NOT at each `not` node. Plain NOT is correct for leaf
 * comparisons (an UNKNOWN comparison stays UNKNOWN — excluded — under NOT, matching
 * the CEL error → deny), but NOT over a collection macro is not its complement in
 * CEL's error semantics, so negation is pushed inward (De Morgan through and/or,
 * polarity-specific collection translations) until it lands on a leaf.
 */
const buildFilterFromExpression = (
  expression: PlanExpressionOperand,
  mapper: Mapper,
  options?: BuildFilterOptions,
  negated = false
): SQL => {
  // Bare variable in boolean position (e.g. `R.attr.aBool` as an and/or/not operand).
  if (isNameOperand(expression)) {
    const resolved = resolveFieldReference(expression.name, mapper);
    const filter = applyComparison(resolved.mapping, "eq", true);
    const wrapped = resolved.relations.length
      ? wrapWithRelations(resolved.relations, filter, expression.name, options)
      : filter;
    return negated ? not(wrapped) : wrapped;
  }
  if (isValueOperand(expression)) {
    return Boolean(expression.value) !== negated
      ? TRUE_CONDITION
      : FALSE_CONDITION;
  }
  if (!isExpressionOperand(expression)) {
    throw new Error("Invalid expression operand");
  }

  const { operator, operands } = expression;

  switch (operator) {
    case "and":
    case "or": {
      if (operands.length === 0) {
        throw new Error(`'${operator}' operator requires at least one operand`);
      }
      const filters = operands.map((operand) =>
        buildFilterFromExpression(operand, mapper, options, negated)
      );
      // De Morgan under negation: !(a AND b) = !a OR !b (and vice versa).
      const combineWithAnd = (operator === "and") !== negated;
      const combined = combineWithAnd ? and(...filters) : or(...filters);
      if (!combined) {
        throw new Error(`'${operator}' operator produced an empty filter`);
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
      return buildFilterFromExpression(operand, mapper, options, !negated);
    }
    case "if": {
      // Bare boolean-result ternary: guard each branch with the (un)satisfied
      // condition. An UNKNOWN condition leaves BOTH arms UNKNOWN — excluded under
      // either polarity — matching the CEL error → deny.
      if (operands.length !== 3) {
        throw new Error("'if' operator requires exactly three operands");
      }
      const [condOperand, thenOperand, elseOperand] = operands;
      if (!condOperand || !thenOperand || !elseOperand) {
        throw new Error("'if' operator is missing operands");
      }
      const cond = buildFilterFromExpression(condOperand, mapper, options);
      const thenFilter = buildFilterFromExpression(
        thenOperand,
        mapper,
        options,
        negated
      );
      const elseFilter = buildFilterFromExpression(
        elseOperand,
        mapper,
        options,
        negated
      );
      const combined = or(and(cond, thenFilter), and(not(cond), elseFilter));
      if (!combined) {
        throw new Error("'if' operator produced an empty filter");
      }
      return combined;
    }
    case "eq":
    case "ne":
    case "lt":
    case "le":
    case "gt":
    case "ge": {
      if (operands.length !== 2) {
        throw new Error(
          `'${operator}' operator requires exactly two operands`
        );
      }
      const [left, right] = operands;
      if (!left || !right) {
        throw new Error("Comparison operator requires two operands");
      }
      let filter: SQL;
      if (isExpressionOperand(left) || isExpressionOperand(right)) {
        // Expression-valued side (arithmetic, conversion, `if`, `size`): evaluate both
        // sides as SQL value expressions in wire order and emit a raw comparison.
        const leftExpr = buildValueExpression(left, mapper, options);
        const rightExpr = buildValueExpression(right, mapper, options);
        filter = applyComparisonWithExpression(operator, leftExpr, rightExpr);
      } else if (isNameOperand(left) && isNameOperand(right)) {
        // Field-to-field: compare the two columns directly, wrapping whatever relation
        // chains the columns live behind so both are in scope.
        const leftResolved = resolveFieldReference(left.name, mapper);
        const rightResolved = resolveFieldReference(right.name, mapper);
        const comparison = applyComparisonWithExpression(
          operator,
          buildColumnExpression(leftResolved.mapping, left.name),
          buildColumnExpression(rightResolved.mapping, right.name)
        );
        filter = wrapCombinedRelations(
          comparison,
          leftResolved.relations,
          rightResolved.relations,
          left.name,
          options
        );
      } else if (isNameOperand(left) && isValueOperand(right)) {
        const resolved = resolveFieldReference(left.name, mapper);
        const comparison = applyComparison(
          resolved.mapping,
          operator,
          right.value
        );
        filter = resolved.relations.length
          ? wrapWithRelations(resolved.relations, comparison, left.name, options)
          : comparison;
      } else if (isValueOperand(left) && isNameOperand(right)) {
        // Value-first comparison: the planner preserves source order, so MIRROR the
        // operator instead of silently swapping operands (3 <= col ⇔ col >= 3).
        const mirrored = MIRRORED_OPERATORS[operator]!;
        const resolved = resolveFieldReference(right.name, mapper);
        const comparison = applyComparison(
          resolved.mapping,
          mirrored,
          left.value
        );
        filter = resolved.relations.length
          ? wrapWithRelations(resolved.relations, comparison, right.name, options)
          : comparison;
      } else {
        throw new Error(
          `'${operator}' operator requires field or value operands`
        );
      }
      return negated ? not(filter) : filter;
    }
    case "contains":
    case "startsWith":
    case "endsWith": {
      const filter = buildStringMatchFilter(operator, operands, mapper, options);
      return negated ? not(filter) : filter;
    }
    case "in":
    case "isSet": {
      // Membership/isSet: whichever side is the name operand is the column — the planner
      // emits both `R.attr.x in [..]` (name, values) and `"v" in R.attr.list`
      // (value, name), and both mean membership against the column.
      const fieldOperand = operands.find(isNameOperand);
      if (!fieldOperand) {
        throw new Error("Comparison operator missing field operand");
      }
      const valueOperand = operands.find(isValueOperand);
      if (!valueOperand) {
        throw new Error("Comparison operator missing value operand");
      }
      const resolved = resolveFieldReference(fieldOperand.name, mapper);
      const comparison = applyComparison(
        resolved.mapping,
        operator,
        valueOperand.value
      );
      const filter = resolved.relations.length
        ? wrapWithRelations(
            resolved.relations,
            comparison,
            fieldOperand.name,
            options
          )
        : comparison;
      return negated ? not(filter) : filter;
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
      return negated ? not(filter) : filter;
    }
    case "hasIntersection": {
      const filter = buildHasIntersectionFilter(operands, mapper);
      return negated ? not(filter) : filter;
    }
    default: {
      const collectionOp = COLLECTION_OPERATORS[operator];
      if (collectionOp) {
        return buildCollectionOperatorFilter(
          collectionOp,
          operands,
          mapper,
          negated,
          options
        );
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
