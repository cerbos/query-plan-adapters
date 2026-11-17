import { Column, SQL, Table, getTableColumns, getTableUniqueName, is } from "drizzle-orm";
import type { AnyColumn } from "drizzle-orm";
import { MySqlTable, getTableConfig as mysqlTableConfig } from "drizzle-orm/mysql-core";
import { PgTable, getTableConfig as pgTableConfig } from "drizzle-orm/pg-core";
import { SQLiteTable, getTableConfig as sqliteTableConfig } from "drizzle-orm/sqlite-core";

import { columnTable, legacyRelationsApi, renderRelationsFilter } from "./compat";
import type { LegacyRelation, LegacyRelationalConfig, LegacyRelationsApi } from "./compat";
import { isFunction } from "./narrowing";
import type {
  Mapper,
  MapperEntry,
  DeclaredMappingConfig,
  DeclaredRelation,
  ResolvedMapper,
  ResolvedMapperEntry,
  RelationMapping,
  RelationV2,
  RelationV2Column,
  RelationsSchema,
  RelationsSchemaEntry,
  TableRelationsV2,
} from "./types";

/**
 * Completing the mapper from the schema: one-or-many, the target table and the join columns come
 * from drizzle's relations (`relations()` before 1.0, `defineRelations()` from 1.0) or the tables'
 * foreign keys, and an attribute the mapper leaves out reads the resource table's column or
 * relation of the same name. The caller maps only what the schema cannot say.
 */

interface ForeignKeyReference {
  columns: AnyColumn[];
  foreignTable: Table;
  foreignColumns: AnyColumn[];
}

interface TableConstraints {
  foreignKeys: ForeignKeyReference[];
  uniqueColumns: Set<AnyColumn>;
  primaryKey: AnyColumn | undefined;
}

const tableConstraints = (table: Table): TableConstraints => {
  const config = is(table, PgTable)
    ? pgTableConfig(table)
    : is(table, MySqlTable)
      ? mysqlTableConfig(table)
      : is(table, SQLiteTable)
        ? sqliteTableConfig(table)
        : undefined;

  if (config === undefined) {
    throw new Error("Unable to read the constraints of a table that is not a drizzle table");
  }

  const uniqueColumns = new Set<AnyColumn>();

  const addSingle = <T>(columns: readonly T[]): void => {
    const [only] = columns;

    if (columns.length === 1 && is(only, Column)) uniqueColumns.add(only);
  };

  const primaryKeys: (readonly unknown[])[] = [
    ...config.columns.filter((column) => column.primary).map((column) => [column]),
    ...config.primaryKeys.map((key) => [...key.columns]),
  ];

  for (const key of primaryKeys) addSingle(key);

  for (const column of config.columns) {
    if (column.isUnique) addSingle([column]);
  }

  for (const unique of config.uniqueConstraints) addSingle([...unique.columns]);

  for (const index of config.indexes) {
    const partial = "where" in index.config && index.config.where !== undefined;

    if (index.config.unique && !partial) addSingle([...index.config.columns]);
  }

  const [onlyKey] = primaryKeys;
  const [primaryKey] = primaryKeys.length === 1 && onlyKey?.length === 1 ? onlyKey : [];

  return {
    foreignKeys: config.foreignKeys.map((foreignKey) => foreignKey.reference()),
    uniqueColumns,
    primaryKey: is(primaryKey, Column) ? primaryKey : undefined,
  };
};

const isUniqueColumn = (column: AnyColumn): boolean =>
  tableConstraints(columnTable(column)).uniqueColumns.has(column);

const tableLabel = (table: Table): string => getTableUniqueName(table);

const tableColumns = (table: Table): Record<string, AnyColumn> => getTableColumns(table);

interface Inferred {
  type: "one" | "many";
  table: Table;
  sourceColumn: AnyColumn;
  targetColumn: AnyColumn;
  subqueryFilter?: SQL;
}

const singleColumn = (columns: readonly AnyColumn[], what: string): AnyColumn => {
  const [only] = columns;

  if (columns.length !== 1 || only === undefined) {
    throw new Error(`${what} joins on ${columns.length} columns; only a single-column key can be translated`);
  }

  return only;
};

const toOneType = (target: AnyColumn, what: string): "one" => {
  if (!isUniqueColumn(target)) {
    throw new Error(
      `${what} is a to-one relation, but nothing makes its target column unique, so it can match ` +
        "several rows: add a unique constraint or declare `type` explicitly",
    );
  }

  return "one";
};

// -- relations(), before drizzle 1.0 -------------------------------------------------------------

const legacyConfigs = new WeakMap<RelationsSchema, LegacyRelationalConfig>();

const legacyConfig = (api: LegacyRelationsApi, schema: RelationsSchema): LegacyRelationalConfig => {
  const config =
    legacyConfigs.get(schema) ??
    api.extractTablesRelationalConfig(schema, api.createTableRelationsHelpers);

  legacyConfigs.set(schema, config);

  return config;
};

/** The relations `relations()` declares on `table`, keyed by name. */
const legacyRelationsOf = (
  config: LegacyRelationalConfig,
  schema: RelationsSchema,
  table: Table,
): Record<string, LegacyRelation> => {
  const tsName = Object.entries(schema).find(([, value]) => value === table)?.[0];

  return (tsName === undefined ? undefined : config.tables[tsName]?.relations) ?? {};
};

const inferFromLegacyRelations = (
  api: LegacyRelationsApi,
  source: Table,
  name: string,
  schema: RelationsSchema,
  what: string,
): Inferred => {
  const config = legacyConfig(api, schema);
  const relation = legacyRelationsOf(config, schema, source)[name];

  if (relation === undefined) {
    throw new Error(`${what} is not defined by any relations() in the schema`);
  }

  const normalized = api.normalizeRelation(config.tables, config.tableNamesMap, relation);

  return {
    type: relation instanceof api.One ? "one" : "many",
    table: relation.referencedTable,
    sourceColumn: singleColumn(normalized.fields, what),
    targetColumn: singleColumn(normalized.references, what),
  };
};

// -- defineRelations(), from drizzle 1.0 ---------------------------------------------------------

const isTableRelationsV2 = (entry: RelationsSchemaEntry): entry is TableRelationsV2 =>
  !is(entry, Table) && "relations" in entry;

const relationsV2Of = (schema: RelationsSchema): TableRelationsV2[] =>
  Object.values(schema).filter(isTableRelationsV2);

const relationColumn = (column: RelationV2Column, what: string): AnyColumn => {
  const value = column._.column;

  if (!is(value, Column)) {
    throw new Error(`${what} joins on an expression, not a column`);
  }

  return value;
};

const isFilter = (where: RelationV2["where"]): where is object =>
  typeof where === "object" && Object.keys(where).length > 0;

/**
 * A `defineRelations()` relation. Its `where` is the application's own narrowing of the rows it
 * reads, so it becomes the subquery filter; a filter on the source side, or a many-to-many
 * `through` a junction table, cannot be rendered as one hop and is refused.
 */
const inferFromRelationsV2 = (
  tables: TableRelationsV2[],
  source: Table,
  name: string,
  what: string,
): Inferred => {
  const relation = tables.find((entry) => entry.table === source)?.relations[name];

  if (relation === undefined) {
    throw new Error(`${what} is not defined by defineRelations()`);
  }

  if (relation.through !== undefined) {
    throw new Error(
      `${what} runs through a junction table: map it as two relations, the junction table and then the target`,
    );
  }

  const inferred: Inferred = {
    type: relation.relationType,
    table: relation.targetTable,
    sourceColumn: singleColumn(relation.sourceColumns.map((c) => relationColumn(c, what)), what),
    targetColumn: singleColumn(relation.targetColumns.map((c) => relationColumn(c, what)), what),
  };

  if (!isFilter(relation.where)) return inferred;

  if (relation.isFilterReversed) {
    throw new Error(`${what} is filtered on its source table, which a subquery over its target cannot apply`);
  }

  const filter = renderRelationsFilter(relation.targetTable, relation.where);

  if (filter === undefined) {
    throw new Error(`${what} declares a filter this drizzle version cannot render`);
  }

  inferred.subqueryFilter = filter;

  return inferred;
};

// -- either style --------------------------------------------------------------------------------

interface DeclaredRelations {
  names(table: Table): string[];
  infer(source: Table, name: string, what: string): Inferred;
}

const declaredRelationsCache = new WeakMap<RelationsSchema, DeclaredRelations | undefined>();

/** The relations `schema` declares, in whichever of drizzle's two styles it uses. */
const declaredRelations = (schema: RelationsSchema): DeclaredRelations | undefined => {
  if (declaredRelationsCache.has(schema)) return declaredRelationsCache.get(schema);

  const tablesV2 = relationsV2Of(schema);
  const legacy = tablesV2.length > 0 ? undefined : legacyRelationsApi();

  const relations: DeclaredRelations | undefined =
    tablesV2.length > 0
      ? {
          names: (table) => Object.keys(tablesV2.find((entry) => entry.table === table)?.relations ?? {}),
          infer: (source, name, what) => inferFromRelationsV2(tablesV2, source, name, what),
        }
      : legacy === undefined
        ? undefined
        : {
            names: (table) => Object.keys(legacyRelationsOf(legacyConfig(legacy, schema), schema, table)),
            infer: (source, name, what) => inferFromLegacyRelations(legacy, source, name, schema, what),
          };

  declaredRelationsCache.set(schema, relations);

  return relations;
};

const relationNames = (table: Table, schema: RelationsSchema | undefined): string[] =>
  (schema === undefined ? undefined : declaredRelations(schema)?.names(table)) ?? [];

const inferFromRelations = (
  source: Table,
  name: string,
  schema: RelationsSchema | undefined,
): Inferred => {
  const what = `Relation '${name}' of ${tableLabel(source)}`;

  if (schema === undefined) {
    throw new Error(
      `${what} is declared by name, which needs the \`schema\` option: the object holding your relations()` +
        " or the result of defineRelations()",
    );
  }

  const relations = declaredRelations(schema);

  if (relations === undefined) {
    throw new Error(`${what} is declared by name, but \`schema\` holds no relations`);
  }

  return relations.infer(source, name, what);
};

// -- foreign keys --------------------------------------------------------------------------------

const inferFromForeignKeys = (source: Table, declared: DeclaredRelation, target: Table): Inferred => {
  const what = `The relation from ${tableLabel(source)} to ${tableLabel(target)}`;

  const forward = tableConstraints(source)
    .foreignKeys.filter((key) => key.foreignTable === target)
    .map((key) => ({
      sourceColumns: key.columns,
      targetColumns: key.foreignColumns,
      reverse: false,
    }));

  const reverse = tableConstraints(target)
    .foreignKeys.filter((key) => key.foreignTable === source)
    .map((key) => ({
      sourceColumns: key.foreignColumns,
      targetColumns: key.columns,
      reverse: true,
    }));

  const candidates = [...forward, ...reverse].filter(
    (candidate) =>
      (declared.sourceColumn === undefined || candidate.sourceColumns.includes(declared.sourceColumn)) &&
      (declared.targetColumn === undefined || candidate.targetColumns.includes(declared.targetColumn)),
  );

  const [match] = candidates;

  if (candidates.length !== 1 || match === undefined) {
    throw new Error(
      `${what} ${candidates.length === 0 ? "has no foreign key" : "has several candidate foreign-key joins"}: ` +
        "declare `name` with relations(), or `sourceColumn` and `targetColumn`",
    );
  }

  const sourceColumn = singleColumn(match.sourceColumns, what);
  const targetColumn = singleColumn(match.targetColumns, what);

  return {
    type: !match.reverse || isUniqueColumn(targetColumn) ? "one" : "many",
    table: target,
    sourceColumn,
    targetColumn,
  };
};

// -- completing a declaration --------------------------------------------------------------------

const isComplete = (declared: DeclaredRelation): declared is DeclaredRelation & Inferred =>
  declared.type !== undefined &&
  declared.table !== undefined &&
  declared.sourceColumn !== undefined &&
  declared.targetColumn !== undefined;

const infer = (
  declared: DeclaredRelation,
  source: Table | undefined,
  schema: RelationsSchema | undefined,
): Inferred => {
  if (isComplete(declared)) {
    const { type, table, sourceColumn, targetColumn } = declared;

    return { type, table, sourceColumn, targetColumn };
  }

  const from = (declared.sourceColumn && columnTable(declared.sourceColumn)) ?? source;

  if (from === undefined) {
    throw new Error(
      "A top-level relation that leaves `sourceColumn` undeclared needs the `table` option",
    );
  }

  const inferred =
    declared.name !== undefined
      ? inferFromRelations(from, declared.name, schema)
      : declared.table !== undefined
        ? inferFromForeignKeys(from, declared, declared.table)
        : undefined;

  if (inferred === undefined) {
    throw new Error("A relation must declare `name` or `table` for the rest to be inferred");
  }

  if (declared.table !== undefined && declared.table !== inferred.table) {
    throw new Error(
      `Relation '${declared.name}' of ${tableLabel(from)} targets ${tableLabel(inferred.table)}, not the declared ${tableLabel(declared.table)}`,
    );
  }

  const targetColumn = declared.targetColumn ?? inferred.targetColumn;
  const label = `Relation '${declared.name ?? tableLabel(inferred.table)}' of ${tableLabel(from)}`;

  const completed: Inferred = {
    type: declared.type ?? (inferred.type === "one" ? toOneType(targetColumn, label) : "many"),
    table: inferred.table,
    sourceColumn: declared.sourceColumn ?? inferred.sourceColumn,
    targetColumn,
  };

  if (inferred.subqueryFilter !== undefined) completed.subqueryFilter = inferred.subqueryFilter;

  return completed;
};

const isDeclaredConfig = (entry: MapperEntry): entry is DeclaredMappingConfig =>
  typeof entry === "object" && "relation" in entry && !("kind" in entry);

const ATTR = "request.resource.attr.";

/** The table's single-column primary key, which `request.resource.id` reads unless mapped. */
const primaryKey = (table: Table): AnyColumn | undefined => tableConstraints(table).primaryKey;

/**
 * The caller's mapper completed from the schema. Each declared relation resolves to one
 * `RelationMapping` per source table for the whole call, because the translator tells relations
 * apart by identity.
 */
export const inferRelations = (
  mapper: Mapper,
  root: Table | undefined,
  schema: RelationsSchema | undefined,
): ResolvedMapper => {
  const resolved = new Map<DeclaredRelation, Map<Table | undefined, RelationMapping>>();
  const implicit = new Map<Table, Map<string, DeclaredRelation>>();

  /** The relation `name` of `table` when the mapper does not mention it: inferred whole. */
  const implicitRelation = (table: Table, name: string): DeclaredRelation => {
    const byName = implicit.get(table) ?? new Map<string, DeclaredRelation>();

    implicit.set(table, byName);

    const relation = byName.get(name) ?? { name };

    byName.set(name, relation);

    return relation;
  };

  /**
   * A path segment under `table` that the mapper does not declare: the table's column of that
   * name, else its relation of that name.
   */
  const implicitEntry = (table: Table, segment: string): ResolvedMapperEntry | undefined => {
    const column = tableColumns(table)[segment];

    if (column !== undefined) return column;

    return relationNames(table, schema).includes(segment)
      ? resolveEntry({ relation: implicitRelation(table, segment) }, table)
      : undefined;
  };

  const resolveRelation = (declared: DeclaredRelation, source: Table | undefined): RelationMapping => {
    const bySource = resolved.get(declared) ?? new Map<Table | undefined, RelationMapping>();

    resolved.set(declared, bySource);

    const cached = bySource.get(source);

    if (cached) return cached;

    const { name: _name, field, fields, ...rest } = declared;
    const inferred = infer(declared, source, schema);
    const relation: RelationMapping = { ...rest, ...inferred };

    if (rest.subqueryFilter !== undefined) relation.subqueryFilter = rest.subqueryFilter;

    bySource.set(source, relation);

    if (field !== undefined) relation.field = resolveEntry(field, inferred.table);

    const columns = tableColumns(inferred.table);

    const nested = Object.fromEntries(
      relationNames(inferred.table, schema)
        .filter((name) => fields?.[name] === undefined && columns[name] === undefined)
        .map((name) => [name, resolveEntry({ relation: implicitRelation(inferred.table, name) }, inferred.table)]),
    );

    if (fields !== undefined || Object.keys(nested).length > 0) {
      relation.fields = {
        ...nested,
        ...Object.fromEntries(
          Object.entries(fields ?? {}).map(([key, entry]) => [key, resolveEntry(entry, inferred.table)]),
        ),
      };
    }

    return relation;
  };

  const resolveEntry = (entry: MapperEntry, source: Table | undefined): ResolvedMapperEntry => {
    if (!isDeclaredConfig(entry)) return entry;

    const { relation, ...config } = entry;

    return relation === undefined ? config : { ...config, relation: resolveRelation(relation, source) };
  };

  const implicitReference = (table: Table, reference: string): ResolvedMapperEntry | undefined => {
    if (reference === "request.resource.id") return primaryKey(table);

    const segment = reference.startsWith(ATTR) ? reference.slice(ATTR.length) : undefined;

    return segment === undefined || segment.includes(".") ? undefined : implicitEntry(table, segment);
  };

  const entries = new Map<string, ResolvedMapperEntry | undefined>();

  return (reference: string) => {
    if (entries.has(reference)) return entries.get(reference);

    const declared = isFunction(mapper) ? mapper(reference) : mapper[reference];

    const entry =
      declared !== undefined
        ? resolveEntry(declared, root)
        : root === undefined
          ? undefined
          : implicitReference(root, reference);

    entries.set(reference, entry);

    return entry;
  };
};
