import { sql } from "drizzle-orm";
import type { AnyColumn, SQL, SQLWrapper, Table } from "drizzle-orm";

/**
 * The few places drizzle 0.44/0.45 and 1.0 differ at runtime or in their types. Everything else
 * reads a column or builds SQL the same way on both.
 */

export const not = (condition: SQLWrapper): SQL => sql`not ${condition}`;

interface ColumnInternals {
  table: Table;
  dataType: string;
  dimensions?: number;
  baseColumn?: AnyColumn;
}

const internals = (column: AnyColumn): ColumnInternals => column as unknown as ColumnInternals;

export const columnTable = (column: AnyColumn): Table => internals(column).table;

/**
 * The column's data type in 0.45's vocabulary. 1.0 spells it `"<type> <constraint>"` (`"number
 * int32"`, `"object json"`), and gives an array column its element's type plus `dimensions`.
 */
export function dataTypeOf(column: AnyColumn): string;
export function dataTypeOf(column: AnyColumn | undefined): string | undefined;
export function dataTypeOf(column: AnyColumn | undefined): string | undefined {
  if (column === undefined) return undefined;

  const { dataType, dimensions } = internals(column);

  if ((dimensions ?? 0) > 0) return "array";

  const [type = dataType, constraint] = dataType.split(" ");

  return type === "object" && constraint !== undefined ? constraint : type;
}

/**
 * The element column of a one-dimensional PostgreSQL array: 0.45's `PgArray.baseColumn`, or the
 * column itself in 1.0, where an array is its element column with `dimensions: 1`.
 */
export const pgArrayElementColumnType = (column: AnyColumn): string | undefined => {
  const { baseColumn, dimensions } = internals(column);

  if (column.columnType === "PgArray") {
    return baseColumn !== undefined && baseColumn.columnType !== "PgArray"
      ? baseColumn.columnType
      : undefined;
  }

  return dimensions === 1 ? column.columnType : undefined;
};
