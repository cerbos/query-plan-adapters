import { Column, SQL, Table, getTableUniqueName, is } from "drizzle-orm";
import type { AnyColumn } from "drizzle-orm";
import { MySqlTable, getTableConfig as mysqlTableConfig } from "drizzle-orm/mysql-core";
import { PgTable, getTableConfig as pgTableConfig } from "drizzle-orm/pg-core";
import { SQLiteTable, getTableConfig as sqliteTableConfig } from "drizzle-orm/sqlite-core";

import { columnTable } from "./compat";
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
 * the join columns come from drizzle's relations (`relations()` before 1.0, `defineRelations()`
 * from 1.0) or the tables' foreign keys, so the caller names the relation and maps only what the
 * schema cannot say (fields, order).
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
  tableConstraints(columnTable(column)).uniqueColumns.has(column);

const tableLabel = (table: Table): string => getTableUniqueName(table);

interface Inferred {
  type: "one" | "many";
  table: Table;
  sourceColumn: AnyColumn;
  targetColumn: AnyColumn;
  subqueryFilter?: SQL;
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

interface LegacyRelationalConfig {
  tables: Record<string, { relations: Record<string, { referencedTable: Table }> }>;
  tableNamesMap: Record<string, string>;
}

interface LegacyRelationsApi {
  extractTablesRelationalConfig(schema: Record<string, unknown>, helpers: unknown): LegacyRelationalConfig;
  createTableRelationsHelpers: unknown;
  normalizeRelation(
    tables: LegacyRelationalConfig["tables"],
    tableNamesMap: Record<string, string>,
    relation: object,
  ): { fields: AnyColumn[]; references: AnyColumn[] };
  One: abstract new (...args: never[]) => object;
}

interface RelationsV2Api {
  relationsFilterToSQL(table: Table, filter: unknown): { sql?: unknown[] };
}

const loadModule = <T>(id: string, member: string): T | undefined => {
  try {
    const loaded = require(id) as Record<string, unknown>;
    return typeof loaded[member] === "function" ? (loaded as T) : undefined;
  } catch {
    return undefined;
  }
};

/** `relations()` lives in the root module before 1.0 and in `drizzle-orm/_relations` from it. */
const legacyRelationsApi = (): LegacyRelationsApi | undefined =>
  loadModule<LegacyRelationsApi>("drizzle-orm", "extractTablesRelationalConfig") ??
  loadModule<LegacyRelationsApi>("drizzle-orm/_relations", "extractTablesRelationalConfig");

interface RelationV2Column {
  _: { column: unknown };
}

interface RelationV2 {
  relationType: "one" | "many";
  sourceColumns: RelationV2Column[];
  targetColumns: RelationV2Column[];
  targetTable: Table;
  through?: unknown;
  where?: unknown;
  isFilterReversed?: boolean;
}

interface TableRelationsV2 {
  table: Table;
  relations: Record<string, RelationV2>;
}

const isTableRelationsV2 = (value: unknown): value is TableRelationsV2 =>
  typeof value === "object" &&
  value !== null &&
  "table" in value &&
  "relations" in value &&
  is((value as { table: unknown }).table, Table);

const relationsV2Of = (schema: Record<string, unknown>): TableRelationsV2[] =>
  Object.values(schema).filter(isTableRelationsV2);

const legacyConfigs = new WeakMap<Record<string, unknown>, LegacyRelationalConfig>();

const inferFromLegacyRelations = (
  api: LegacyRelationsApi,
  source: Table,
  name: string,
  schema: Record<string, unknown>,
  what: string,
): Inferred => {
  const config =
    legacyConfigs.get(schema) ??
    api.extractTablesRelationalConfig(schema, api.createTableRelationsHelpers);
  legacyConfigs.set(schema, config);

  const { tables, tableNamesMap } = config;
  const tsName = Object.entries(schema).find(([, value]) => value === source)?.[0];
  const relation = tsName === undefined ? undefined : tables[tsName]?.relations[name];

  if (relation === undefined) {
    throw new Error(`${what} is not defined by any relations() in the schema`);
  }

  const normalized = api.normalizeRelation(tables, tableNamesMap, relation);

  return {
    type: relation instanceof api.One ? "one" : "many",
    table: relation.referencedTable,
    sourceColumn: singleColumn(normalized.fields, what),
    targetColumn: singleColumn(normalized.references, what),
  };
};

const relationColumn = (column: RelationV2Column, what: string): AnyColumn => {
  if (!is(column._.column, Column)) {
    throw new Error(`${what} joins on an expression, not a column`);
  }

  return column._.column as AnyColumn;
};

const hasFilter = (where: unknown): boolean =>
  typeof where === "object" && where !== null && Object.keys(where).length > 0;

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

  if (!hasFilter(relation.where)) return inferred;

  if (relation.isFilterReversed) {
    throw new Error(`${what} is filtered on its source table, which a subquery over its target cannot apply`);
  }

  const api = loadModule<RelationsV2Api>("drizzle-orm", "relationsFilterToSQL");
  const chunks = api?.relationsFilterToSQL(relation.targetTable, relation.where).sql;

  if (chunks === undefined) {
    throw new Error(`${what} declares a filter this drizzle version cannot render`);
  }

  return { ...inferred, subqueryFilter: new SQL(chunks as ConstructorParameters<typeof SQL>[0]) };
};

/** The names of the relations `schema` defines on `table`, in either drizzle's style. */
const relationNames = (table: Table, schema: Record<string, unknown> | undefined): string[] => {
  if (schema === undefined) return [];

  const tablesV2 = relationsV2Of(schema);
  if (tablesV2.length > 0) {
    return Object.keys(tablesV2.find((entry) => entry.table === table)?.relations ?? {});
  }

  const legacy = legacyRelationsApi();
  if (legacy === undefined) return [];

  const config =
    legacyConfigs.get(schema) ??
    legacy.extractTablesRelationalConfig(schema, legacy.createTableRelationsHelpers);
  legacyConfigs.set(schema, config);
  const tsName = Object.entries(schema).find(([, value]) => value === table)?.[0];

  return Object.keys((tsName === undefined ? undefined : config.tables[tsName]?.relations) ?? {});
};

const tableColumns = (table: Table): Record<string, AnyColumn> =>
  Object.fromEntries(Object.entries(table).filter(([, value]) => is(value, Column))) as Record<
    string,
    AnyColumn
  >;

const inferFromRelations = (
  source: Table,
  name: string,
  schema: Record<string, unknown> | undefined,
): Inferred => {
  const what = `Relation '${name}' of ${tableLabel(source)}`;

  if (schema === undefined) {
    throw new Error(
      `${what} is declared by name, which needs the \`schema\` option: the object holding your relations()` +
        " or the result of defineRelations()",
    );
  }

  const tablesV2 = relationsV2Of(schema);
  if (tablesV2.length > 0) return inferFromRelationsV2(tablesV2, source, name, what);

  const legacy = legacyRelationsApi();
  if (legacy === undefined) {
    throw new Error(`${what} is declared by name, but \`schema\` holds no relations`);
  }

  return inferFromLegacyRelations(legacy, source, name, schema, what);
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
    ...(inferred.subqueryFilter && { subqueryFilter: inferred.subqueryFilter }),
  };
};

const isDeclaredConfig = (entry: DeclaredMapperEntry): entry is DeclaredMappingConfig =>
  typeof entry === "object" && entry !== null && "relation" in entry && !("kind" in entry);

/**
 * The caller's mapper with every declared relation completed. Each declared relation resolves to
 * one `RelationMapping` per source table for the whole call, because the translator tells
 * relations apart by identity.
 */
const ATTR = "request.resource.attr.";

/** The table's single-column primary key, which `request.resource.id` reads unless mapped. */
const primaryKey = (table: Table): AnyColumn | undefined => {
  const keys = Object.values(tableColumns(table)).filter((column) => column.primary);
  return keys.length === 1 ? keys[0] : undefined;
};

export const inferRelations = (
  mapper: DeclaredMapper,
  root: Table | undefined,
  schema: Record<string, unknown> | undefined,
): Mapper => {
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
  const implicitEntry = (table: Table, segment: string): MapperEntry | undefined => {
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
    const nested = Object.fromEntries(
      relationNames(inferred.table, schema)
        .filter((name) => fields?.[name] === undefined && tableColumns(inferred.table)[name] === undefined)
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

  const resolveEntry = (entry: DeclaredMapperEntry, source: Table | undefined): MapperEntry =>
    isDeclaredConfig(entry) && entry.relation !== undefined
      ? { ...entry, relation: resolveRelation(entry.relation, source) }
      : (entry as MapperEntry);

  const entries = new Map<string, MapperEntry | undefined>();

  return (reference: string) => {
    if (entries.has(reference)) return entries.get(reference);

    const declared = isFunction(mapper) ? mapper(reference) : mapper[reference];
    const entry =
      declared !== undefined
        ? resolveEntry(declared, root)
        : root === undefined
          ? undefined
          : reference === "request.resource.id"
            ? primaryKey(root)
            : reference.startsWith(ATTR) && !reference.slice(ATTR.length).includes(".")
              ? implicitEntry(root, reference.slice(ATTR.length))
              : undefined;
    entries.set(reference, entry);

    return entry;
  };
};
