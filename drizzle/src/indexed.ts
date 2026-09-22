import type { PlanExpressionOperand } from "@cerbos/core";
import { is, not, sql } from "drizzle-orm";
import type { AnyColumn, SQL } from "drizzle-orm";
import { MySqlColumn } from "drizzle-orm/mysql-core";
import { PgArray, PgColumn } from "drizzle-orm/pg-core";
import { SQLiteColumn } from "drizzle-orm/sqlite-core";

import { getMappingEntry, isMappingConfig, resolveFieldReference } from "./mapper";
import { isNameOperand, isValueOperand } from "./operands";
import type { BuildFilterOptions, Mapper } from "./types";

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
    // JSON conversion preserves null elements and addresses POSITIONS, including arrays whose
    // lower bound is not 1. A raw [index + 1] would read a different element in those arrays.
    return postgresEquality(sql`to_jsonb(${column})`, index, value);
  }
  if (indexable !== "json") {
    throw new Error("Unknown indexable storage shape");
  }
  if (is(column, PgColumn)) {
    if (column.dataType !== "json") {
      throw new Error(
        'indexable: "json" requires a PostgreSQL JSON or JSONB column',
      );
    }
    return postgresEquality(sql`cast(${column} as jsonb)`, index, value);
  }
  const path = `$[${index}]`;
  if (is(column, MySqlColumn)) {
    if (column.dataType !== "json") {
      throw new Error('indexable: "json" requires a MySQL JSON column');
    }
    const element = sql`json_extract(${column}, ${path})`;
    // Every JSON number type MySQL reports is a CEL number: an integer beyond the signed 64-bit
    // range (1e19) is `UNSIGNED INTEGER`, and an exact decimal is `DECIMAL`. Leaving either out
    // made the equality FALSE for an element the PDP matches — and its negation TRUE (#472).
    const equality =
      typeof value === "number"
        ? sql`(case when json_type(${element}) in ('INTEGER', 'UNSIGNED INTEGER', 'DOUBLE', 'DECIMAL') then cast(json_unquote(${element}) as float(53)) = cast(${value} as float(53)) else false end)`
        : sql`${element} = cast(${JSON.stringify(value)} as json)`;
    // MySQL autowraps scalar JSON as a singleton array for [0]. CEL does not.
    return sql`(case when json_type(${column}) = 'ARRAY' and ${element} is not null then ${equality} end)`;
  }
  if (is(column, SQLiteColumn)) {
    if (column.dataType !== "json" && column.dataType !== "string") {
      throw new Error('indexable: "json" requires a SQLite JSON text column');
    }
    const type = sql`json_type(${column}, ${path})`;
    const element = sql`json_extract(${column}, ${path})`;
    let equality: SQL;
    if (value === null) {
      equality = sql`${type} = 'null'`;
    } else if (typeof value === "boolean") {
      equality = sql`${type} = ${value ? "true" : "false"}`;
    } else if (typeof value === "number") {
      equality = sql`(case when ${type} in ('integer', 'real') then cast(${element} as real) = cast(${value} as real) else false end)`;
    } else {
      equality = sql`(case when ${type} = 'text' then ${element} = ${value} else false end)`;
    }
    // json_type returns the STRING 'null' for a null element and SQL NULL for a missing one.
    return sql`(case when json_type(${column}) = 'array' and ${type} is not null then ${equality} end)`;
  }
  throw new Error("Indexed JSON columns require PostgreSQL, SQLite or MySQL");
}

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
    throw new Error(
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
    throw new Error(
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
  const resolved = resolveIndexedColumn(indexed.operands, mapper, options);
  if (
    (operator !== "eq" && operator !== "ne") || !isValueOperand(other) ||
    (other.value !== null && typeof other.value !== "string" &&
     typeof other.value !== "boolean" && typeof other.value !== "number")
  ) {
    throw new Error(
      "Indexed values support only direct eq/ne comparisons with scalar literals",
    );
  }
  if (typeof other.value === "number" && !Number.isFinite(other.value)) {
    throw new Error("Indexed numeric comparisons require a finite literal");
  }
  const equality = indexedEquality({ ...resolved, value: other.value });
  return (operator === "ne") !== negated ? not(equality) : equality;
};

function postgresEquality(source: SQL, index: number, value: Scalar): SQL {
  const element = sql`(${source} -> cast(${index} as integer))`;
  const equality =
    typeof value === "number"
      ? sql`(case when jsonb_typeof(${element}) = 'number' then cast(${element} #>> '{}' as float(53)) = cast(${value} as float(53)) else false end)`
      : sql`${element} = cast(${JSON.stringify(value)} as jsonb)`;
  return sql`(case when jsonb_typeof(${source}) = 'array' and ${element} is not null then ${equality} end)`;
}
