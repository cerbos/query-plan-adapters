import {
  Column,
  One,
  createTableRelationsHelpers,
  extractTablesRelationalConfig,
  getTableUniqueName,
  is,
  normalizeRelation,
} from "drizzle-orm";
import type { AnyColumn, Table, TablesRelationalConfig } from "drizzle-orm";
import { MySqlTable, getTableConfig as mysqlTableConfig } from "drizzle-orm/mysql-core";
import { PgTable, getTableConfig as pgTableConfig } from "drizzle-orm/pg-core";
import { SQLiteTable, getTableConfig as sqliteTableConfig } from "drizzle-orm/sqlite-core";

import { isFunction } from "./narrowing";
import type {
  DeclaredMapper,
  DeclaredMapperEntry,
  DeclaredMappingConfig,
  DeclaredRelation,
  Mapper,
  MapperEntry,
  RelationMapping,
} from "./types";

/**
 * Completing the relations a mapper declares from the schema: one-or-many, the target table and
 * the join columns come from drizzle's `relations()` or the tables' foreign keys, so the caller
 * names the relation and maps only what the schema cannot say (fields, order, visibility).
 */

interface TableConstraints {
  foreignKeys: { columns: AnyColumn[]; foreignTable: Table; foreignColumns: AnyColumn[] }[];
  uniqueColumns: Set<AnyColumn>;
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
  const addSingle = (columns: unknown[]): void => {
    const [only] = columns;
    if (columns.length === 1 && is(only, Column)) uniqueColumns.add(only);
  };

  for (const column of config.columns) {
    if (column.primary || column.isUnique) uniqueColumns.add(column as unknown as AnyColumn);
  }
  for (const key of config.primaryKeys) addSingle(key.columns);
  for (const unique of config.uniqueConstraints) addSingle(unique.columns);
  for (const index of config.indexes) {
    if (index.config.unique && (index.config as { where?: unknown }).where === undefined) addSingle(index.config.columns);
  }

  return {
    foreignKeys: config.foreignKeys.map((foreignKey) => foreignKey.reference()),
    uniqueColumns,
  };
};

const isUniqueColumn = (column: AnyColumn): boolean =>
  tableConstraints(column.table).uniqueColumns.has(column);

const tableLabel = (table: Table): string => getTableUniqueName(table);

interface Inferred {
  type: "one" | "many";
  table: Table;
  sourceColumn: AnyColumn;
  targetColumn: AnyColumn;
}

const singleColumn = (columns: AnyColumn[], what: string): AnyColumn => {
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

type RelationalConfig = { tables: TablesRelationalConfig; tableNamesMap: Record<string, string> };

const relationalConfigs = new WeakMap<Record<string, unknown>, RelationalConfig>();

const relationalConfig = (schema: Record<string, unknown>): RelationalConfig => {
  const cached = relationalConfigs.get(schema);
  if (cached) return cached;

  const config = extractTablesRelationalConfig<TablesRelationalConfig>(
    schema,
    createTableRelationsHelpers,
  );
  relationalConfigs.set(schema, config);

  return config;
};

const inferFromRelations = (
  source: Table,
  name: string,
  schema: Record<string, unknown> | undefined,
): Inferred => {
  const what = `Relation '${name}' of ${tableLabel(source)}`;

  if (schema === undefined) {
    throw new Error(`${what} is declared by name, which needs the drizzle \`schema\` option`);
  }

  const { tables, tableNamesMap } = relationalConfig(schema);
  const tsName = tableNamesMap[getTableUniqueName(source)];
  const relation = tsName === undefined ? undefined : tables[tsName]?.relations[name];

  if (relation === undefined) {
    throw new Error(`${what} is not defined by any relations() in the schema`);
  }

  const normalized = normalizeRelation(tables, tableNamesMap, relation);
  const sourceColumn = singleColumn(normalized.fields, what);
  const targetColumn = singleColumn(normalized.references, what);

  return {
    type: is(relation, One) ? "one" : "many",
    table: relation.referencedTable,
    sourceColumn,
    targetColumn,
  };
};

const inferFromForeignKeys = (
  source: Table,
  declared: DeclaredRelation & { table: Table },
): Inferred => {
  const target = declared.table;
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

const isComplete = (declared: DeclaredRelation): declared is DeclaredRelation & Inferred =>
  declared.type !== undefined &&
  declared.table !== undefined &&
  declared.sourceColumn !== undefined &&
  declared.targetColumn !== undefined;

const infer = (
  declared: DeclaredRelation,
  source: Table | undefined,
  schema: Record<string, unknown> | undefined,
): Inferred => {
  if (isComplete(declared)) {
    const { type, table, sourceColumn, targetColumn } = declared;
    return { type, table, sourceColumn, targetColumn };
  }

  const from = declared.sourceColumn?.table ?? source;

  if (from === undefined) {
    throw new Error(
      "A top-level relation that leaves `sourceColumn` undeclared needs the `table` option",
    );
  }

  const inferred =
    declared.name !== undefined
      ? inferFromRelations(from, declared.name, schema)
      : declared.table !== undefined
        ? inferFromForeignKeys(from, { ...declared, table: declared.table })
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

  return {
    type:
      declared.type ??
      (inferred.type === "one"
        ? toOneType(targetColumn, `Relation '${declared.name ?? tableLabel(inferred.table)}' of ${tableLabel(from)}`)
        : "many"),
    table: inferred.table,
    sourceColumn: declared.sourceColumn ?? inferred.sourceColumn,
    targetColumn,
  };
};

const isDeclaredConfig = (entry: DeclaredMapperEntry): entry is DeclaredMappingConfig =>
  typeof entry === "object" && entry !== null && "relation" in entry && !("kind" in entry);

/**
 * The caller's mapper with every declared relation completed. Each declared relation resolves to
 * one `RelationMapping` per source table for the whole call, because the translator tells
 * relations apart by identity.
 */
export const inferRelations = (
  mapper: DeclaredMapper,
  root: Table | undefined,
  schema: Record<string, unknown> | undefined,
): Mapper => {
  const resolved = new Map<DeclaredRelation, Map<Table | undefined, RelationMapping>>();

  const resolveRelation = (declared: DeclaredRelation, source: Table | undefined): RelationMapping => {
    const bySource = resolved.get(declared) ?? new Map<Table | undefined, RelationMapping>();
    resolved.set(declared, bySource);

    const cached = bySource.get(source);
    if (cached) return cached;

    const { name: _name, field, fields, ...rest } = declared;
    const inferred = infer(declared, source, schema);
    const relation: RelationMapping = { ...rest, ...inferred };
    bySource.set(source, relation);

    if (field !== undefined) relation.field = resolveEntry(field, inferred.table);
    if (fields !== undefined) {
      relation.fields = Object.fromEntries(
        Object.entries(fields).map(([key, entry]) => [key, resolveEntry(entry, inferred.table)]),
      );
    }

    return relation;
  };

  const resolveEntry = (entry: DeclaredMapperEntry, source: Table | undefined): MapperEntry =>
    isDeclaredConfig(entry) && entry.relation !== undefined
      ? { ...entry, relation: resolveRelation(entry.relation, source) }
      : (entry as MapperEntry);

  const entries = new Map<string, MapperEntry | undefined>();

  return (reference: string) => {
    if (entries.has(reference)) return entries.get(reference);

    const declared = isFunction(mapper) ? mapper(reference) : mapper[reference];
    const entry = declared === undefined ? undefined : resolveEntry(declared, root);
    entries.set(reference, entry);

    return entry;
  };
};
