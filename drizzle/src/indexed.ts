import { is, sql } from "drizzle-orm";
import type { AnyColumn, SQL } from "drizzle-orm";
import { MySqlColumn } from "drizzle-orm/mysql-core";
import { PgArray, PgColumn } from "drizzle-orm/pg-core";
import { SQLiteColumn } from "drizzle-orm/sqlite-core";

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
    const equality =
      typeof value === "number"
        ? sql`(case when json_type(${element}) in ('INTEGER', 'DOUBLE') then cast(json_unquote(${element}) as float(53)) = cast(${value} as float(53)) else false end)`
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

function postgresEquality(source: SQL, index: number, value: Scalar): SQL {
  const element = sql`(${source} -> cast(${index} as integer))`;
  const equality =
    typeof value === "number"
      ? sql`(case when jsonb_typeof(${element}) = 'number' then cast(${element} #>> '{}' as float(53)) = cast(${value} as float(53)) else false end)`
      : sql`${element} = cast(${JSON.stringify(value)} as jsonb)`;
  return sql`(case when jsonb_typeof(${source}) = 'array' and ${element} is not null then ${equality} end)`;
}
