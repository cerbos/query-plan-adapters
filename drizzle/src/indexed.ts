import type { PlanExpressionOperand, Value } from "@cerbos/core";
import { and, is, not, sql } from "drizzle-orm";
import type { AnyColumn, SQL } from "drizzle-orm";
import { MySqlColumn } from "drizzle-orm/mysql-core";
import { PgArray, PgColumn } from "drizzle-orm/pg-core";
import { SQLiteColumn } from "drizzle-orm/sqlite-core";

import { UnsupportedQueryPlanError } from "./errors";
import { getMappingEntry, isMappingConfig, resolveFieldReference } from "./mapper";
import { isNameOperand, isValueOperand } from "./operands";
import type { BuildFilterOptions, Mapper } from "./types";
import { UNKNOWN_CONDITION } from "./predicates";

/**
 * Constant positional access — `R.attr.list[0] == x` — over a column whose ordered storage the
 * caller declares with `indexable`. A relation has no positional order, so it is never inferred.
 */

/** The declared representation of an ordered collection, independent of its CEL name. */
export type Indexable = "json" | "pgArray";

type Scalar = string | number | boolean | null;

/**
 * Compare an indexed scalar without conflating an absent element with a null VALUE.
 * Keep the guard inside the predicate: SQL UNKNOWN survives negation, but a sibling OR
 * can still mask it, just as CEL can mask an out-of-range evaluation error.
 */
export function indexedEquality({
  column,
  indexable,
  index,
  value,
}: {
  column: AnyColumn;
  indexable: Indexable;
  index: number;
  value: Scalar;
}): SQL {
  if (indexable === "pgArray") {
    assertPgArrayColumn(column);
    // JSON conversion preserves null elements and addresses POSITIONS, including arrays whose
    // lower bound is not 1. A raw [index + 1] would read a different element in those arrays.
    return postgresEquality(sql`to_jsonb(${column})`, index, value);
  }
  if (indexable !== "json") {
    throw new Error("Unknown indexable storage shape");
  }
  if (is(column, PgColumn)) {
    assertPgJsonColumn(column);
    return postgresEquality(sql`cast(${column} as jsonb)`, index, value);
  }
  const path = `$[${index}]`;
  if (is(column, MySqlColumn)) {
    if (column.dataType !== "json") {
      throw new Error('indexable: "json" requires a MySQL JSON column');
    }
    const element = sql`json_extract(${column}, ${path})`;
    const equality = mysqlElementEquality(element, value);
    // MySQL autowraps scalar JSON as a singleton array for [0]. CEL does not.
    return sql`(case when json_type(${column}) = 'ARRAY' and ${element} is not null then ${equality} end)`;
  }
  if (is(column, SQLiteColumn)) {
    assertSqliteJsonColumn(column);
    const type = sql`json_type(${column}, ${path})`;
    const element = sql`json_extract(${column}, ${path})`;
    const equality = sqliteElementEquality(type, element, value);
    // json_type returns the STRING 'null' for a null element and SQL NULL for a missing one.
    return sql`(case when json_type(${column}) = 'array' and ${type} is not null then ${equality} end)`;
  }
  throw new Error("Indexed JSON columns require PostgreSQL, SQLite or MySQL");
}

/**
 * `R.attr.list == [v0, v1, …]` over declared ordered storage: CEL's list equality — the same
 * length, and each position equal under the typed element comparison `indexedEquality` makes. A
 * length mismatch is false before any element is read; with equal lengths every position is in
 * bounds, so each element comparison is definite. A NULL or non-array column stays UNKNOWN.
 */
export function indexedListEquality({
  column,
  indexable,
  values,
}: {
  column: AnyColumn;
  indexable: Indexable;
  values: readonly Value[];
}): SQL {
  const scalars = values.map((value): Scalar => {
    if (
      (value !== null && typeof value !== "string" && typeof value !== "boolean" &&
        typeof value !== "number") ||
      (typeof value === "number" && !Number.isFinite(value))
    ) {
      throw new UnsupportedQueryPlanError(
        "Whole-list comparison over declared indexed storage supports only finite scalar elements",
      );
    }
    return value;
  });
  let isArray: SQL;
  let length: SQL;
  if (indexable === "pgArray" || is(column, PgColumn)) {
    const source =
      indexable === "pgArray"
        ? (assertPgArrayColumn(column), sql`to_jsonb(${column})`)
        : (assertPgJsonColumn(column as PgColumn), sql`cast(${column} as jsonb)`);
    isArray = sql`jsonb_typeof(${source}) = 'array'`;
    length = sql`jsonb_array_length(${source})`;
  } else if (is(column, MySqlColumn)) {
    isArray = sql`json_type(${column}) = 'ARRAY'`;
    length = sql`json_length(${column})`;
  } else if (is(column, SQLiteColumn)) {
    assertSqliteJsonColumn(column);
    isArray = sql`json_type(${column}) = 'array'`;
    length = sql`json_array_length(${column})`;
  } else {
    throw new Error("Indexed JSON columns require PostgreSQL, SQLite or MySQL");
  }
  const elements = scalars.map((value, index) =>
    indexedEquality({ column, indexable, index, value }),
  );
  const sameLength = sql`${length} = ${scalars.length}`;
  const equal = elements.length === 0 ? sameLength : and(sameLength, ...elements)!;
  return sql`(case when ${isArray} then ${equal} end)`;
}

/**
 * `value in list` and `hasIntersection(list, [values])` over declared ordered storage with no
 * relation: true when ANY element equals ANY of `values` under CEL's heterogeneous equality.
 *
 * Every element is compared with its JSON type checked first, exactly as `indexedEquality` does,
 * because CEL's `"2" == 2` and `"true" == true` are false while a store that reads the element
 * back as SQL can answer them true: SQLite's affinity rules and MySQL's string-to-number
 * conversion both equate '2' with 2, and neither store has a boolean distinct from the integer 1.
 * A NULL or non-array column is SQL UNKNOWN, excluded under either polarity like CEL's error.
 */
export function indexedMembership({
  column,
  indexable,
  values,
}: {
  column: AnyColumn;
  indexable: Indexable;
  values: readonly Value[];
}): SQL {
  const scalars = values.map((value): Scalar => {
    if (
      value !== null && typeof value !== "string" &&
      typeof value !== "boolean" && typeof value !== "number"
    ) {
      throw new UnsupportedQueryPlanError(
        "Membership in declared indexed storage supports only scalar literals",
      );
    }
    if (typeof value === "number" && !Number.isFinite(value)) {
      throw new UnsupportedQueryPlanError("Indexed numeric comparisons require a finite literal");
    }
    return value;
  });
  const anyOf = (equalities: SQL[]): SQL =>
    equalities.length === 0
      ? sql`false`
      : sql`(${sql.join(equalities, sql` or `)})`;

  if (indexable === "pgArray") {
    assertPgArrayColumn(column);
    return postgresMembership(sql`to_jsonb(${column})`, scalars, anyOf);
  }
  if (indexable !== "json") {
    throw new Error("Unknown indexable storage shape");
  }
  if (is(column, PgColumn)) {
    assertPgJsonColumn(column);
    return postgresMembership(sql`cast(${column} as jsonb)`, scalars, anyOf);
  }
  if (is(column, MySqlColumn)) {
    if (column.dataType !== "json") {
      throw new Error('indexable: "json" requires a MySQL JSON column');
    }
    // JSON_TABLE's path must be a literal; `$[*]` is constant, so nothing caller-supplied is
    // inlined. JSON_TABLE reads a JSON null element back as SQL NULL — measured on the pinned
    // server — so a null literal is matched by IS NULL; `$[*]` yields no missing element to
    // confuse it with, and every other comparison against that NULL leaves the row out.
    const element = sql.raw("cerbos_element.v");
    const match = anyOf(
      scalars.map((value) =>
        value === null ? sql`${element} is null` : mysqlElementEquality(element, value),
      ),
    );
    return sql`(case when json_type(${column}) = 'ARRAY' then exists (select 1 from json_table(${column}, '$[*]' columns (v json path '$')) as cerbos_element where ${match}) end)`;
  }
  if (is(column, SQLiteColumn)) {
    assertSqliteJsonColumn(column);
    const match = anyOf(
      scalars.map((value) =>
        sqliteElementEquality(
          sql.raw("cerbos_element.type"),
          sql.raw("cerbos_element.value"),
          value,
        ),
      ),
    );
    return sql`(case when json_type(${column}) = 'array' then exists (select 1 from json_each(${column}) as cerbos_element where ${match}) end)`;
  }
  throw new Error("Indexed JSON columns require PostgreSQL, SQLite or MySQL");
}

/**
 * The declared ordered storage a membership test reads, when the collection has no relation to
 * read instead. A mapping carrying both keeps reading its relation, as it always has.
 */
export const resolveIndexedMembership = (
  reference: string,
  mapper: Mapper,
  options: BuildFilterOptions,
): { column: AnyColumn; indexable: Indexable } | undefined => {
  const direct = getMappingEntry(reference, mapper);
  const resolved =
    direct && isMappingConfig(direct) && direct.indexable
      ? { mapping: direct, relations: [] }
      : resolveFieldReference(reference, mapper);
  const mapping = resolved.mapping;
  if (!isMappingConfig(mapping) || !mapping.indexable || mapping.relation) {
    return undefined;
  }
  if (
    !mapping.column || mapping.transform ||
    resolved.relations.some((relation) => !options.skipRelations?.has(relation))
  ) {
    throw new Error(
      "Membership in declared indexed storage requires a directly addressable column without a transform",
    );
  }
  return { column: mapping.column, indexable: mapping.indexable };
};

/**
 * The declared ordered storage a WHOLE-list comparison reads. Unlike membership, it is used even
 * when the mapping also carries a relation: a relation has no order, so it cannot answer this.
 */
export const resolveIndexedList = (
  reference: string,
  mapper: Mapper,
): { column: AnyColumn; indexable: Indexable } | undefined => {
  const mapping = getMappingEntry(reference, mapper);
  if (!mapping || !isMappingConfig(mapping) || !mapping.indexable || !mapping.column) {
    return undefined;
  }
  if (mapping.transform) {
    throw new Error("A whole-list comparison requires a declared indexed column without a transform");
  }
  return { column: mapping.column, indexable: mapping.indexable };
};

/** Resolve the opt-in column separately from a relation used for collection predicates. */
export const resolveIndexedColumn = (
  operands: PlanExpressionOperand[],
  mapper: Mapper,
  options: BuildFilterOptions,
): { column: AnyColumn; indexable: Indexable; index: number } => {
  const [collection, position] = operands;
  if (
    operands.length !== 2 || !collection || !position || !isNameOperand(collection)
  ) {
    throw new UnsupportedQueryPlanError(
      "Index access requires a mapped collection and a constant position",
    );
  }
  const direct = getMappingEntry(collection.name, mapper);
  const resolved =
    direct && isMappingConfig(direct) && direct.indexable
      ? { mapping: direct, relations: [] }
      : resolveFieldReference(collection.name, mapper);
  const mapping = resolved.mapping;
  if (!isMappingConfig(mapping) || !mapping.indexable || !mapping.column) {
    throw new Error(
      `Index storage shape is undeclared for '${collection.name}': declare a column with indexable: "json" or "pgArray"; a relation has no positional order`,
    );
  }
  if (
    mapping.transform ||
    resolved.relations.some((relation) => !options.skipRelations?.has(relation))
  ) {
    throw new Error(
      "Index access requires a directly addressable column without a transform",
    );
  }
  if (
    !isValueOperand(position) || typeof position.value !== "number" ||
    !Number.isSafeInteger(position.value) || position.value < 0 ||
    position.value > 2147483647
  ) {
    throw new UnsupportedQueryPlanError(
      "Index access requires a constant non-negative 32-bit integer position",
    );
  }
  return {
    column: mapping.column,
    indexable: mapping.indexable,
    index: position.value,
  };
};

/**
 * `index(collection, position) == literal` (or `!=`, or the literal first), under the polarity a
 * `not` pushed down to it.
 */
export const buildIndexedComparison = (
  operator: string,
  indexed: PlanExpressionOperand & { operands: PlanExpressionOperand[] },
  other: PlanExpressionOperand,
  mapper: Mapper,
  options: BuildFilterOptions,
  negated: boolean,
): SQL => {
  // A negative or fractional position is an error in CEL whatever the collection holds — out of
  // bounds for a list, a key no JSON-sourced map carries — and an error denies under both
  // polarities, exactly as the UNKNOWN every out-of-bounds position already yields below.
  const [collection, position] = indexed.operands;
  if (
    collection !== undefined && isNameOperand(collection) &&
    position !== undefined && isValueOperand(position) &&
    typeof position.value === "number" &&
    (position.value < 0 || !Number.isInteger(position.value))
  ) {
    resolveFieldReference(collection.name, mapper);
    return UNKNOWN_CONDITION;
  }
  const resolved = resolveIndexedColumn(indexed.operands, mapper, options);
  if (
    (operator !== "eq" && operator !== "ne") || !isValueOperand(other) ||
    (other.value !== null && typeof other.value !== "string" &&
     typeof other.value !== "boolean" && typeof other.value !== "number")
  ) {
    throw new UnsupportedQueryPlanError(
      "Indexed values support only direct eq/ne comparisons with scalar literals",
    );
  }
  if (typeof other.value === "number" && !Number.isFinite(other.value)) {
    throw new UnsupportedQueryPlanError("Indexed numeric comparisons require a finite literal");
  }
  const equality = indexedEquality({ ...resolved, value: other.value });
  return (operator === "ne") !== negated ? not(equality) : equality;
};

function postgresEquality(source: SQL, index: number, value: Scalar): SQL {
  const element = sql`(${source} -> cast(${index} as integer))`;
  const equality = postgresElementEquality(element, value);
  return sql`(case when jsonb_typeof(${source}) = 'array' and ${element} is not null then ${equality} end)`;
}

function postgresMembership(
  source: SQL,
  values: readonly Scalar[],
  anyOf: (equalities: SQL[]) => SQL,
): SQL {
  const element = sql.raw("cerbos_element.v");
  const match = anyOf(values.map((value) => postgresElementEquality(element, value)));
  return sql`(case when jsonb_typeof(${source}) = 'array' then exists (select 1 from jsonb_array_elements(${source}) as cerbos_element(v) where ${match}) end)`;
}

/** One jsonb element against a literal: a number by its double value, anything else as jsonb. */
function postgresElementEquality(element: SQL, value: Scalar): SQL {
  return typeof value === "number"
    ? sql`(case when jsonb_typeof(${element}) = 'number' then cast(${element} #>> '{}' as float(53)) = cast(${value} as float(53)) else false end)`
    : sql`${element} = cast(${JSON.stringify(value)} as jsonb)`;
}

/**
 * One MySQL JSON element against a literal. Every JSON number type MySQL reports is a CEL number:
 * an integer beyond the signed 64-bit range (1e19) is `UNSIGNED INTEGER`, and an exact decimal is
 * `DECIMAL`. Leaving either out made the equality FALSE for an element the PDP matches — and its
 * negation TRUE (#472). Anything else is compared as JSON, whose comparator keeps the type.
 */
function mysqlElementEquality(element: SQL, value: Scalar): SQL {
  return typeof value === "number"
    ? sql`(case when json_type(${element}) in ('INTEGER', 'UNSIGNED INTEGER', 'DOUBLE', 'DECIMAL') then cast(json_unquote(${element}) as float(53)) = cast(${value} as float(53)) else false end)`
    : sql`${element} = cast(${JSON.stringify(value)} as json)`;
}

/** One SQLite JSON element, given its `json_type` and its extracted SQL value, against a literal. */
function sqliteElementEquality(type: SQL, element: SQL, value: Scalar): SQL {
  if (value === null) {
    return sql`${type} = 'null'`;
  }
  if (typeof value === "boolean") {
    return sql`${type} = ${value ? "true" : "false"}`;
  }
  if (typeof value === "number") {
    return sql`(case when ${type} in ('integer', 'real') then cast(${element} as real) = cast(${value} as real) else false end)`;
  }
  return sql`(case when ${type} = 'text' then ${element} = ${value} else false end)`;
}

function assertPgArrayColumn(column: AnyColumn): void {
  if (!is(column, PgArray)) {
    throw new Error('indexable: "pgArray" requires a PostgreSQL array column');
  }
  if (
    !["PgText", "PgVarchar", "PgBoolean", "PgInteger", "PgSmallInt"]
      .includes(column.baseColumn.columnType)
  ) {
    // Numeric/string-mode, bigint and custom decoders can change the representation of a
    // value (including a SQL NULL element) before the application sends it to check().
    // Floating-point arrays also admit NaN/Infinity, which to_jsonb converts to STRINGS.
    throw new Error(
      "Indexed PostgreSQL arrays require text, varchar, boolean, integer or smallint elements without custom decoding",
    );
  }
}

function assertPgJsonColumn(column: PgColumn): void {
  if (column.dataType !== "json") {
    throw new Error(
      'indexable: "json" requires a PostgreSQL JSON or JSONB column',
    );
  }
}

function assertSqliteJsonColumn(column: SQLiteColumn): void {
  if (column.dataType !== "json" && column.dataType !== "string") {
    throw new Error('indexable: "json" requires a SQLite JSON text column');
  }
}
