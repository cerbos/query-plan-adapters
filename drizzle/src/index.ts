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

const RFC3339_MILLISECOND_TIMESTAMP =
  /^((?!0000)\d{4})-(\d{2})-(\d{2})[Tt](?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.(\d{1,9}))?(?:[Zz]|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/;
const MIN_RFC3339_TIMESTAMP_MILLISECONDS = Date.parse(
  "0001-01-01T00:00:00.000Z"
);
const MAX_RFC3339_TIMESTAMP_MILLISECONDS = Date.parse(
  "9999-12-31T23:59:59.999Z"
);

const normalizeRfc3339Milliseconds = (value: string): string => {
  const match = RFC3339_MILLISECOND_TIMESTAMP.exec(value);
  const yearText = match?.[1];
  const monthText = match?.[2];
  const dayText = match?.[3];
  const fraction = match?.[4] ?? "";
  if (!yearText || !monthText || !dayText) {
    throw new Error(`Invalid RFC-3339 timestamp value: ${value}`);
  }
  if ([...fraction.slice(3)].some((digit) => digit !== "0")) {
    throw new Error(
      `Timestamp value exceeds millisecond precision: ${value}`
    );
  }

  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  const calendarDate = new Date(0);
  calendarDate.setUTCHours(0, 0, 0, 0);
  calendarDate.setUTCFullYear(year, month - 1, day);
  if (
    calendarDate.getUTCFullYear() !== year ||
    calendarDate.getUTCMonth() !== month - 1 ||
    calendarDate.getUTCDate() !== day
  ) {
    throw new Error(`Invalid RFC-3339 timestamp value: ${value}`);
  }

  const milliseconds = Date.parse(value);
  if (Number.isNaN(milliseconds)) {
    throw new Error(`Invalid RFC-3339 timestamp value: ${value}`);
  }
  if (
    milliseconds < MIN_RFC3339_TIMESTAMP_MILLISECONDS ||
    milliseconds > MAX_RFC3339_TIMESTAMP_MILLISECONDS
  ) {
    throw new Error(
      `Timestamp value is outside CEL's supported instant range: ${value}`
    );
  }
  return new Date(milliseconds).toISOString().replace(".000Z", "Z");
};

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
  | "endsWith";

type MapperTransform = (args: {
  operator: ComparisonOperator;
  value: Value;
}) => SQL;

type MappingConfig = {
  column?: AnyColumn;
  transform?: MapperTransform;
  relation?: RelationMapping;
  valueType?: "timestamp";
  collectionValueType?: "scalar";
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
  ("column" in entry ||
    "transform" in entry ||
    "relation" in entry ||
    "valueType" in entry ||
    "collectionValueType" in entry);

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

// CEL-exact string matching: replace/substr are case-sensitive, interpret no LIKE
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
      // REPLACE is case-sensitive and treats the needle literally on SQLite,
      // PostgreSQL, and MySQL. Unlike LIKE it cannot reinterpret %, _, \, or [
      // from a column-valued needle as pattern syntax. The explicit NULL arm
      // preserves CEL missing-attribute errors under negation, and contains("")
      // remains true for every present receiver.
      return sql`(case when ${receiver} is null or ${needle} is null then null when ${needle} = '' then true else length(replace(${receiver}, ${needle}, '')) < length(${receiver}) end)`;
    case "startsWith":
      return sql`substr(${receiver}, 1, length(${needle})) = ${needle}`;
    case "endsWith":
      return sql`substr(${receiver}, length(${receiver}) - length(${needle}) + 1) = ${needle}`;
  }
};

type ConstantHierarchy = {
  kind: "constant";
  segments: string[];
  delimiter: string;
};

type FieldHierarchy = {
  kind: "field";
  reference: string;
  resolved: ResolvedMapping;
  delimiter: string;
};

type ResolvedHierarchy = ConstantHierarchy | FieldHierarchy;

const resolveHierarchy = (
  operand: PlanExpressionOperand,
  mapper: Mapper
): ResolvedHierarchy => {
  if (!isExpressionOperand(operand) || operand.operator !== "hierarchy") {
    throw new Error("Hierarchy operators require hierarchy(...) operands");
  }
  if (operand.operands.length < 1 || operand.operands.length > 2) {
    throw new Error("'hierarchy' operator requires one or two operands");
  }
  const [pathOperand, delimiterOperand] = operand.operands;
  if (!pathOperand) {
    throw new Error("'hierarchy' operator is missing its path operand");
  }
  let delimiter = ".";
  if (delimiterOperand) {
    if (
      !isValueOperand(delimiterOperand) ||
      typeof delimiterOperand.value !== "string"
    ) {
      throw new Error("Hierarchy delimiter must be a string value");
    }
    delimiter = delimiterOperand.value;
  }

  if (isValueOperand(pathOperand)) {
    if (typeof pathOperand.value !== "string") {
      throw new Error("Hierarchy path must be a string value or field reference");
    }
    return {
      kind: "constant",
      segments: pathOperand.value.split(delimiter),
      delimiter,
    };
  }
  if (isNameOperand(pathOperand)) {
    return {
      kind: "field",
      reference: pathOperand.name,
      resolved: resolveFieldReference(pathOperand.name, mapper),
      delimiter,
    };
  }
  throw new Error(
    "Segmented hierarchy expressions are not supported by the Drizzle adapter"
  );
};

const hierarchyStrictPrefixes = (
  segments: string[],
  delimiter: string
): string[] => {
  const prefixes: string[] = [];
  for (let length = 1; length < segments.length; length += 1) {
    prefixes.push(segments.slice(0, length).join(delimiter));
  }
  return prefixes;
};

const buildHierarchyFilter = (
  operator: "ancestorOf" | "descendentOf" | "overlaps",
  operands: PlanExpressionOperand[],
  mapper: Mapper,
  options?: BuildFilterOptions
): SQL => {
  if (operands.length !== 2) {
    throw new Error(`'${operator}' operator requires exactly two operands`);
  }
  const [leftOperand, rightOperand] = operands;
  if (!leftOperand || !rightOperand) {
    throw new Error(`'${operator}' operator is missing operands`);
  }
  const left = resolveHierarchy(leftOperand, mapper);
  const right = resolveHierarchy(rightOperand, mapper);
  if (left.kind === "field" && right.kind === "field") {
    throw new Error(
      `'${operator}' between two field-backed hierarchies is not supported`
    );
  }
  if (left.kind === "constant" && right.kind === "constant") {
    throw new Error(
      `'${operator}' between two constant hierarchies should be folded by the planner`
    );
  }

  let field: FieldHierarchy;
  let constant: ConstantHierarchy;
  if (left.kind === "field") {
    if (right.kind !== "constant") {
      throw new Error(`'${operator}' requires one constant hierarchy operand`);
    }
    field = left;
    constant = right;
  } else {
    if (right.kind !== "field") {
      throw new Error(`'${operator}' requires one field hierarchy operand`);
    }
    field = right;
    constant = left;
  }
  const fieldExpr = buildColumnExpression(field.resolved.mapping, field.reference);
  const constantPath = constant.segments.join(field.delimiter);
  const fieldIsLeft = left.kind === "field";
  const fieldIsAncestor =
    operator === "ancestorOf"
      ? fieldIsLeft
      : operator === "descendentOf"
        ? !fieldIsLeft
        : false;

  let filter: SQL;
  if (operator === "overlaps") {
    const prefixes = hierarchyStrictPrefixes(
      constant.segments,
      field.delimiter
    );
    const conditions: SQL[] = [sql`${fieldExpr} = ${constantPath}`];
    if (prefixes.length > 0) {
      conditions.unshift(
        sql`${fieldExpr} in ${prefixes.map((prefix) => sql`${prefix}`)}`
      );
    }
    conditions.push(
      buildStringMatchCondition(
        "startsWith",
        fieldExpr,
        sql`${constantPath + field.delimiter}`
      )
    );
    const combined = or(...conditions);
    if (!combined) {
      throw new Error("Unable to combine hierarchy overlap conditions");
    }
    filter = combined;
  } else if (fieldIsAncestor) {
    const prefixes = hierarchyStrictPrefixes(
      constant.segments,
      field.delimiter
    );
    if (prefixes.length === 0) {
      filter = FALSE_CONDITION;
    } else {
      filter = sql`${fieldExpr} in ${prefixes.map((prefix) => sql`${prefix}`)}`;
    }
  } else {
    filter = buildStringMatchCondition(
      "startsWith",
      fieldExpr,
      sql`${constantPath + field.delimiter}`
    );
  }

  return field.resolved.relations.length > 0
    ? wrapWithRelations(
        field.resolved.relations,
        filter,
        field.reference,
        options
      )
    : filter;
};

const CONVERSION_TARGETS: Record<string, string> = {
  string: "TEXT",
  // FLOAT(53) is float8 on PostgreSQL, DOUBLE on MySQL, and receives REAL
  // affinity on SQLite.
  double: "FLOAT(53)",
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
    const leftOperand = operands[0];
    const rightOperand = operands[1];
    if (!leftOperand || !rightOperand) {
      throw new Error(`Arithmetic operator '${operator}' is missing operands`);
    }
    if (
      operator === "div" &&
      isValueOperand(leftOperand) &&
      typeof leftOperand.value === "number" &&
      isValueOperand(rightOperand) &&
      typeof rightOperand.value === "number"
    ) {
      // Fold pure-constant division in JavaScript's IEEE double space. SQL engines
      // disagree on division by zero (SQLite returns NULL, PostgreSQL throws), while
      // CEL produces +/-Infinity or NaN. Binding the folded value preserves ordering
      // semantics; SQLite represents bound NaN as NULL, whose comparisons are never true.
      return sql`${leftOperand.value / rightOperand.value}`;
    }
    const left = buildValueExpression(leftOperand, mapper, options);
    const right = buildValueExpression(rightOperand, mapper, options);
    if (operator === "div") {
      // CEL attribute arithmetic is double-typed: force REAL division so an
      // INTEGER/INTEGER pair does not silently truncate (3 / 2 must be 1.5, not 1).
      // SQLite yields NULL for division by zero — UNKNOWN, excluded under both
      // polarities — matching CEL's NaN comparisons (always false → deny).
      return sql`(cast(${left} as float(53)) / ${right})`;
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

  if (operator === "timestamp") {
    if (operands.length !== 1) {
      throw new Error("'timestamp' operator requires exactly one operand");
    }
    const inner = operands[0];
    if (!inner) {
      throw new Error("'timestamp' operator is missing its operand");
    }
    if (isNameOperand(inner)) {
      const resolved = resolveFieldReference(inner.name, mapper);
      if (
        !isMappingConfig(resolved.mapping) ||
        resolved.mapping.valueType !== "timestamp"
      ) {
        throw new Error(
          `'timestamp' field '${inner.name}' requires a mapping with valueType: "timestamp"`
        );
      }
      return buildValueExpression(inner, mapper, options);
    }
    if (!isValueOperand(inner) || typeof inner.value !== "string") {
      throw new Error(
        "'timestamp' requires an RFC-3339 string value or field reference"
      );
    }
    // Normalize offsets so instant equality is preserved. Removing the zero
    // millisecond suffix matches the canonical UTC strings used by timestamp columns.
    const normalized = normalizeRfc3339Milliseconds(inner.value);
    return sql`${normalized}`;
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
      if (values.length === 0) {
        return FALSE_CONDITION;
      }
      const nonNullValues = values.filter((candidate) => candidate !== null);
      if (nonNullValues.length === 0) {
        return isNull(column);
      }
      const membership = sql`${column} in ${nonNullValues.map(
        (candidate) => new Param(candidate, column)
      )}`;
      if (nonNullValues.length === values.length) {
        return membership;
      }
      const withNull = or(membership, isNull(column));
      if (!withNull) {
        throw new Error("Unable to combine null-aware membership conditions");
      }
      return withNull;
    }
    case "contains":
    case "startsWith":
    case "endsWith":
      if (typeof value !== "string") {
        throw new Error(`The '${operator}' operator requires a string value`);
      }
      return buildStringMatchCondition(operator, sql`${column}`, sql`${value}`);
    default:
      throw new Error(`Unsupported operator: ${operator}`);
  }
};

const buildVariableMembershipFilter = (
  operands: PlanExpressionOperand[],
  mapper: Mapper,
  options?: BuildFilterOptions
): SQL => {
  if (operands.length !== 2) {
    throw new Error("'in' operator requires exactly two operands");
  }
  const [memberOperand, collectionOperand] = operands;
  if (
    !memberOperand ||
    !collectionOperand ||
    !isNameOperand(memberOperand) ||
    !isNameOperand(collectionOperand)
  ) {
    throw new Error(
      "Variable membership requires scalar and collection field references"
    );
  }

  const member = resolveFieldReference(memberOperand.name, mapper);
  const unskippedMemberRelations = member.relations.filter(
    (relation) => !options?.skipRelations?.has(relation)
  );
  if (unskippedMemberRelations.length > 0) {
    throw new Error(
      "Variable membership requires its first operand to resolve to a scalar field"
    );
  }
  const collectionEntry = getMappingEntry(collectionOperand.name, mapper);
  if (
    !collectionEntry ||
    !isMappingConfig(collectionEntry) ||
    collectionEntry.collectionValueType !== "scalar"
  ) {
    throw new Error(
      "Variable membership requires a scalar collection mapping"
    );
  }
  const collection = resolveRelationDefaultField(
    resolveFieldReference(collectionOperand.name, mapper),
    collectionOperand.name
  );
  if (collection.relations.length === 0) {
    throw new Error(
      "Variable membership requires its second operand to resolve to a collection"
    );
  }

  const memberExpr = buildColumnExpression(member.mapping, memberOperand.name);
  const elementExpr = buildColumnExpression(
    collection.mapping,
    collectionOperand.name
  );
  const nullMatch = and(
    sql`${memberExpr} is null`,
    sql`${elementExpr} is null`
  );
  const match = or(sql`${memberExpr} = ${elementExpr}`, nullMatch);
  if (!match) {
    throw new Error("Unable to combine variable membership conditions");
  }
  return wrapWithRelations(
    collection.relations,
    match,
    collectionOperand.name,
    options
  );
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
    wrapOptions?: BuildFilterOptions,
    guardNullProjection = false
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
    const guard = guardNullProjection
      ? nullProjectionGuard(
          normalized.relations,
          normalized.mapping,
          reference,
          wrapOptions
        )
      : undefined;
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
      skipRelations ? { skipRelations } : undefined,
      true
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

// Operators whose second operand is a lambda that binds an iteration variable.
const LAMBDA_BINDING_OPERATORS = new Set([
  "exists",
  "exists_one",
  "all",
  "filter",
  "map",
  "except",
]);

/**
 * Substitute a lambda iteration variable with a concrete collection element
 * inside a lambda body. A bare reference to the variable becomes the element
 * itself; a `variable.path.to.field` reference drills into the element. A
 * nested collection macro whose lambda rebinds the same variable name shadows
 * the outer variable, so substitution only descends into its collection
 * operand.
 */
const substituteLambdaVariable = (
  operand: PlanExpressionOperand,
  variableName: string,
  element: Value
): PlanExpressionOperand => {
  if (isNameOperand(operand)) {
    if (operand.name === variableName) {
      return { value: element };
    }
    if (operand.name.startsWith(`${variableName}.`)) {
      let current: Value = element;
      for (const segment of operand.name
        .slice(variableName.length + 1)
        .split(".")) {
        if (
          current === null ||
          typeof current !== "object" ||
          Array.isArray(current) ||
          !(segment in current)
        ) {
          throw new Error(
            `Cannot resolve "${operand.name}": collection element has no field "${segment}"`
          );
        }
        const next = current[segment];
        if (next === undefined) {
          throw new Error(
            `Cannot resolve "${operand.name}": collection element field "${segment}" is undefined`
          );
        }
        current = next;
      }
      return { value: current };
    }
    return operand;
  }

  if (isExpressionOperand(operand)) {
    if (
      LAMBDA_BINDING_OPERATORS.has(operand.operator) &&
      operand.operands.length === 2
    ) {
      const [nestedCollection, nestedLambda] = operand.operands;
      if (
        nestedCollection !== undefined &&
        nestedLambda !== undefined &&
        isExpressionOperand(nestedLambda) &&
        nestedLambda.operator === "lambda"
      ) {
        const nestedVariable = nestedLambda.operands[1];
        if (
          nestedVariable !== undefined &&
          isNameOperand(nestedVariable) &&
          nestedVariable.name === variableName
        ) {
          // The nested lambda shadows our variable: substitute only in the
          // collection operand.
          return {
            operator: operand.operator,
            operands: [
              substituteLambdaVariable(nestedCollection, variableName, element),
              nestedLambda,
            ],
          };
        }
      }
    }
    return {
      operator: operand.operator,
      operands: operand.operands.map((o) =>
        substituteLambdaVariable(o, variableName, element)
      ),
    };
  }

  return operand;
};

/**
 * Fold a collection macro whose collection operand is a literal value list.
 *
 * The planner emits this shape when a known-value collection (typically a
 * folded principal attribute) has more than 10 elements — at 10 or fewer it
 * unrolls `exists`/`all` into an or/and chain itself (cerbos/cerbos#2570,
 * cerbos/cerbos#2817; `maxItems = 10` in the planner's struct matcher). Apply
 * the same fold here so the translated filter does not depend on which side of
 * that threshold the collection lands: substitute each element into the lambda
 * body and combine the per-element conditions with OR (`exists`) or AND
 * (`all`). The empty collection keeps CEL identity semantics: `exists` over
 * `[]` is false, `all` over `[]` is true.
 */
const buildKnownValueCollectionFilter = (
  operator: CollectionOperator,
  collectionValue: Value,
  lambdaOperand: PlanExpressionOperand,
  mapper: Mapper,
  options: BuildFilterOptions | undefined,
  negated: boolean
): SQL => {
  if (operator !== "exists" && operator !== "all") {
    throw new Error(
      `'${operator}' over a literal collection value is not supported. ` +
        "Only exists() and all() can be folded into a flat filter."
    );
  }

  if (!Array.isArray(collectionValue)) {
    throw new Error(
      `'${operator}' over a literal collection requires a list value`
    );
  }

  const { variable: variableOperand, expression: bodyOperand } =
    extractLambdaComponents(lambdaOperand, `'${operator}' lambda operand`);
  if (!isNameOperand(variableOperand)) {
    throw new Error("Lambda variable must have a name operand");
  }

  if (collectionValue.length === 0) {
    const combinesWithOr = operator === "exists" ? !negated : negated;
    return combinesWithOr ? FALSE_CONDITION : TRUE_CONDITION;
  }

  const filters = collectionValue.map((element) =>
    buildFilterFromExpression(
      substituteLambdaVariable(bodyOperand, variableOperand.name, element),
      mapper,
      options,
      negated
    )
  );

  // Push negation through the macro rather than applying a SQL NOT around it:
  // !exists(body) == all(!body), and !all(body) == exists(!body). This keeps
  // CEL evaluation errors as deny instead of treating UNKNOWN as false.
  const combinesWithOr = operator === "exists" ? !negated : negated;
  const combined = combinesWithOr ? or(...filters) : and(...filters);
  if (!combined) {
    throw new Error(
      `Unable to combine folded '${operator}' collection conditions`
    );
  }
  return combined;
};

/**
 * Collection macros with CEL's three-valued semantics. An element whose lambda condition
 * is UNKNOWN (a NULL element column) is a CEL missing-attribute evaluation error:
 *
 * - exists  = TRUE on a true witness (absorbs errors), else error if any element errors;
 * - all     = FALSE on a false witness (absorbs errors), else error if any element errors;
 * - exists_one errors on ANY erroring element, never absorbed.
 *
 * The generated CASE expressions preserve those three states as TRUE, FALSE, or NULL.
 * SQL NOT can therefore apply the requested polarity without turning an evaluation error
 * into an allow, while IS TRUE/FALSE/NULL keeps witnesses distinct inside each macro.
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
  // A literal value list arrives when the planner could not unroll a macro
  // over a known collection (more than 10 elements) — fold it here instead of
  // requiring a relation mapping that cannot exist for a literal.
  if (isValueOperand(collectionOperand)) {
    return buildKnownValueCollectionFilter(
      operator,
      collectionOperand.value,
      lambdaOperand,
      mapper,
      options,
      negated
    );
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
      {
        const trueWitness = wrapAll(sql`(${rowCondition}) is true`);
        const unknownWitness = wrapAll(sql`(${rowCondition}) is null`);
        const triState = sql`(case when ${trueWitness} then true when ${unknownWitness} then null else false end)`;
        return negated ? not(triState) : triState;
      }
    case "except": {
      const exceptFilter = wrapAll(not(rowCondition));
      return negated ? not(exceptFilter) : exceptFilter;
    }
    case "all":
      {
        const falseWitness = wrapAll(sql`(${rowCondition}) is false`);
        const unknownWitness = wrapAll(sql`(${rowCondition}) is null`);
        const triState = sql`(case when ${falseWitness} then false when ${unknownWitness} then null else true end)`;
        return negated ? not(triState) : triState;
      }
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
      const unknownWitness = wrapWithRelations(
        [primaryRelation],
        sql`(${rowCondition}) is null`,
        collectionName
      );
      const triState = sql`(case when ${unknownWitness} then null when ${countExpr} = 1 then true else false end)`;
      const wrapped = wrapLeading(triState);
      return negated ? not(wrapped) : wrapped;
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

type LeafComparisonOperator = "eq" | "ne" | "lt" | "le" | "gt" | "ge";

type ConstantNumberResolution =
  | { kind: "constant"; value: number }
  | { kind: "dynamic" };

const resolveConstantNumber = (
  operand: PlanExpressionOperand
): ConstantNumberResolution => {
  if (isValueOperand(operand)) {
    return typeof operand.value === "number"
      ? { kind: "constant", value: operand.value }
      : { kind: "dynamic" };
  }
  if (!isExpressionOperand(operand) || operand.operands.length !== 2) {
    return { kind: "dynamic" };
  }
  if (
    operand.operator !== "add" &&
    operand.operator !== "sub" &&
    operand.operator !== "mult" &&
    operand.operator !== "div"
  ) {
    return { kind: "dynamic" };
  }
  const [leftOperand, rightOperand] = operand.operands;
  if (!leftOperand || !rightOperand) {
    return { kind: "dynamic" };
  }
  const left = resolveConstantNumber(leftOperand);
  const right = resolveConstantNumber(rightOperand);
  if (left.kind !== "constant" || right.kind !== "constant") {
    return { kind: "dynamic" };
  }
  switch (operand.operator) {
    case "add":
      return { kind: "constant", value: left.value + right.value };
    case "sub":
      return { kind: "constant", value: left.value - right.value };
    case "mult":
      return { kind: "constant", value: left.value * right.value };
    case "div":
      return { kind: "constant", value: left.value / right.value };
  }
};

const evaluateConstantNumberComparison = (
  operator: LeafComparisonOperator,
  left: number,
  right: number
): boolean => {
  switch (operator) {
    case "eq":
      return left === right;
    case "ne":
      return left !== right;
    case "lt":
      return left < right;
    case "le":
      return left <= right;
    case "gt":
      return left > right;
    case "ge":
      return left >= right;
  }
};

const evaluateScalarValueComparison = (
  operator: LeafComparisonOperator,
  left: Value,
  right: Value
): boolean | undefined => {
  const bothNumbers = typeof left === "number" && typeof right === "number";
  const bothStrings = typeof left === "string" && typeof right === "string";
  const bothBooleans = typeof left === "boolean" && typeof right === "boolean";
  const bothNull = left === null && right === null;
  if (!bothNumbers && !bothStrings && !bothBooleans && !bothNull) {
    return undefined;
  }
  switch (operator) {
    case "eq":
      return left === right;
    case "ne":
      return left !== right;
    case "lt":
      return bothNumbers || bothStrings ? left < right : undefined;
    case "le":
      return bothNumbers || bothStrings ? left <= right : undefined;
    case "gt":
      return bothNumbers || bothStrings ? left > right : undefined;
    case "ge":
      return bothNumbers || bothStrings ? left >= right : undefined;
  }
};

const buildComparisonFilter = (
  operator: LeafComparisonOperator,
  left: PlanExpressionOperand,
  right: PlanExpressionOperand,
  mapper: Mapper,
  options: BuildFilterOptions | undefined,
  negated: boolean
): SQL => {
  const buildTernary = (
    ternary: PlanExpressionOperand & {
      operator: string;
      operands: PlanExpressionOperand[];
    },
    other: PlanExpressionOperand,
    ternaryIsLeft: boolean
  ): SQL => {
    if (ternary.operands.length !== 3) {
      throw new Error("'if' operator requires exactly three operands");
    }
    const [conditionOperand, thenOperand, elseOperand] = ternary.operands;
    if (!conditionOperand || !thenOperand || !elseOperand) {
      throw new Error("'if' operator is missing operands");
    }
    const condition = buildFilterFromExpression(
      conditionOperand,
      mapper,
      options
    );
    const thenFilter = ternaryIsLeft
      ? buildComparisonFilter(
          operator,
          thenOperand,
          other,
          mapper,
          options,
          negated
        )
      : buildComparisonFilter(
          operator,
          other,
          thenOperand,
          mapper,
          options,
          negated
        );
    const elseFilter = ternaryIsLeft
      ? buildComparisonFilter(
          operator,
          elseOperand,
          other,
          mapper,
          options,
          negated
        )
      : buildComparisonFilter(
          operator,
          other,
          elseOperand,
          mapper,
          options,
          negated
        );
    const combined = or(
      and(condition, thenFilter),
      and(not(condition), elseFilter)
    );
    if (!combined) {
      throw new Error("Unable to combine ternary comparison conditions");
    }
    return combined;
  };

  /**
   * Fold a division whose denominator may be zero, at the comparison site.
   *
   * CEL attribute arithmetic is double-typed, so `0/0` is NaN and `x/0` is a
   * signed infinity — neither of which SQL can represent. Lowering the division
   * to NULL (SQLite's division-by-zero result) makes every comparison UNKNOWN.
   * That agrees with CEL for ORDERED comparisons, which is why `cr-div-zero`
   * passed, but it silently denies rows an INEQUALITY allows: `NaN != 1.0` is
   * TRUE in CEL while `NULL != 1.0` is UNKNOWN.
   *
   * Keep the three IEEE cases as CASE arms and fold each against the other
   * operand in JavaScript's own IEEE space, so no NaN or Infinity is ever bound
   * as a driver parameter. Comparing a non-finite against the finite sentinel 0
   * gives the same answer as against any finite comparand, which is the same
   * trick `buildDynamicNaNComparison` uses.
   *
   * A NULL numerator, denominator or comparand leaves the result NULL, so the
   * row stays excluded under BOTH polarities — CEL's missing-attribute deny.
   */
  const buildDivision = (
    division: PlanExpressionOperand & {
      operator: string;
      operands: PlanExpressionOperand[];
    },
    other: PlanExpressionOperand,
    divisionIsLeft: boolean
  ): SQL => {
    const [numeratorOperand, denominatorOperand] = division.operands;
    if (!numeratorOperand || !denominatorOperand) {
      throw new Error("'div' operator is missing operands");
    }
    const numerator = buildValueExpression(numeratorOperand, mapper, options);
    const denominator = buildValueExpression(
      denominatorOperand,
      mapper,
      options
    );
    const otherExpr = buildValueExpression(other, mapper, options);

    const arm = (nonFinite: number): SQL => {
      const result = divisionIsLeft
        ? evaluateConstantNumberComparison(operator, nonFinite, 0)
        : evaluateConstantNumberComparison(operator, 0, nonFinite);
      return result !== negated ? sql`true` : sql`false`;
    };

    const quotient = sql`(cast(${numerator} as float(53)) / ${denominator})`;
    const finite = divisionIsLeft
      ? applyComparisonWithExpression(operator, quotient, otherExpr)
      : applyComparisonWithExpression(operator, otherExpr, quotient);

    return sql`(case
      when ${numerator} is null or ${denominator} is null or ${otherExpr} is null then null
      when ${denominator} = 0 and ${numerator} = 0 then ${arm(Number.NaN)}
      when ${denominator} = 0 and ${numerator} > 0 then ${arm(Number.POSITIVE_INFINITY)}
      when ${denominator} = 0 then ${arm(Number.NEGATIVE_INFINITY)}
      else ${negated ? not(finite) : finite}
    end)`;
  };

  if (isExpressionOperand(left) && left.operator === "if") {
    return buildTernary(left, right, true);
  }
  if (isExpressionOperand(right) && right.operator === "if") {
    return buildTernary(right, left, false);
  }

  // A zero denominator is only reachable when it is not a known non-zero
  // constant; otherwise fall through to the plain arithmetic path.
  const canDivideByZero = (
    operand: PlanExpressionOperand
  ): operand is PlanExpressionOperand & {
    operator: string;
    operands: PlanExpressionOperand[];
  } => {
    if (!isExpressionOperand(operand) || operand.operator !== "div") {
      return false;
    }
    // A division of two constants already folds to an exact IEEE value, and
    // the constant/NaN paths below render it more tightly than a CASE can.
    if (resolveConstantNumber(operand).kind === "constant") {
      return false;
    }
    const denominatorOperand = operand.operands[1];
    if (!denominatorOperand) {
      return false;
    }
    const denominator = resolveConstantNumber(denominatorOperand);
    return denominator.kind !== "constant" || denominator.value === 0;
  };

  if (canDivideByZero(left)) {
    return buildDivision(left, right, true);
  }
  if (canDivideByZero(right)) {
    return buildDivision(right, left, false);
  }

  const leftConstant = resolveConstantNumber(left);
  const rightConstant = resolveConstantNumber(right);
  if (
    leftConstant.kind === "constant" &&
    rightConstant.kind === "constant"
  ) {
    const result = evaluateConstantNumberComparison(
      operator,
      leftConstant.value,
      rightConstant.value
    );
    return result !== negated ? TRUE_CONDITION : FALSE_CONDITION;
  }

  const buildDynamicNaNComparison = (
    dynamicOperand: PlanExpressionOperand,
    nanIsLeft: boolean
  ): SQL => {
    const dynamic = resolveScalarOperand(dynamicOperand, mapper, options);
    // Every IEEE comparison with NaN has the same result for every present value:
    // only != is true. Preserve SQL NULL as UNKNOWN so a missing CEL attribute still
    // denies under either polarity, without ever sending a NaN parameter to a driver.
    const presentResult = nanIsLeft
      ? evaluateConstantNumberComparison(operator, Number.NaN, 0)
      : evaluateConstantNumberComparison(operator, 0, Number.NaN);
    const presentCondition = presentResult ? sql`true` : sql`false`;
    let filter = sql`(case when ${dynamic.expr} is null then null else ${presentCondition} end)`;
    if (dynamic.relations.length) {
      const reference = isNameOperand(dynamicOperand)
        ? dynamicOperand.name
        : `'${operator}' operand`;
      filter = wrapWithRelations(
        dynamic.relations,
        filter,
        reference,
        options
      );
    }
    return negated ? not(filter) : filter;
  };

  if (
    leftConstant.kind === "constant" &&
    Number.isNaN(leftConstant.value) &&
    rightConstant.kind === "dynamic" &&
    !isValueOperand(right)
  ) {
    return buildDynamicNaNComparison(right, true);
  }
  if (
    rightConstant.kind === "constant" &&
    Number.isNaN(rightConstant.value) &&
    leftConstant.kind === "dynamic" &&
    !isValueOperand(left)
  ) {
    return buildDynamicNaNComparison(left, false);
  }

  if (isValueOperand(left) && isValueOperand(right)) {
    const result = evaluateScalarValueComparison(
      operator,
      left.value,
      right.value
    );
    if (result === undefined) {
      throw new Error(
        `'${operator}' cannot compare the provided constant value types`
      );
    }
    return result !== negated ? TRUE_CONDITION : FALSE_CONDITION;
  }

  let filter: SQL;
  if (isExpressionOperand(left) || isExpressionOperand(right)) {
    const leftExpr = buildValueExpression(left, mapper, options);
    const rightExpr = buildValueExpression(right, mapper, options);
    filter = applyComparisonWithExpression(operator, leftExpr, rightExpr);
  } else if (isNameOperand(left) && isNameOperand(right)) {
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
    const mirrored = MIRRORED_OPERATORS[operator];
    if (!mirrored) {
      throw new Error(`Unable to mirror comparison operator '${operator}'`);
    }
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
    throw new Error(`'${operator}' operator requires field or value operands`);
  }
  return negated ? not(filter) : filter;
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
      return buildComparisonFilter(
        operator,
        left,
        right,
        mapper,
        options,
        negated
      );
    }
    case "contains":
    case "startsWith":
    case "endsWith": {
      const filter = buildStringMatchFilter(operator, operands, mapper, options);
      return negated ? not(filter) : filter;
    }
    case "in": {
      if (operands.every(isNameOperand)) {
        const filter = buildVariableMembershipFilter(operands, mapper, options);
        return negated ? not(filter) : filter;
      }
      // Membership: whichever side is the name operand is the column — the planner
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
      const unresolved = resolveFieldReference(fieldOperand.name, mapper);
      const entry = getMappingEntry(fieldOperand.name, mapper);
      const resolved =
        entry &&
        isMappingConfig(entry) &&
        entry.collectionValueType === "scalar"
          ? resolveRelationDefaultField(unresolved, fieldOperand.name)
          : unresolved;
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
      throw new Error(
        "'matches' is not supported because SQL regex dialects do not guarantee CEL/RE2 semantics"
      );
    }
    case "hasIntersection": {
      const filter = buildHasIntersectionFilter(operands, mapper);
      return negated ? not(filter) : filter;
    }
    case "ancestorOf":
    case "descendentOf":
    case "overlaps": {
      const filter = buildHierarchyFilter(operator, operands, mapper, options);
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
