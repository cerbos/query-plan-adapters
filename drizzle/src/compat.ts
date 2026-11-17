import { Column, Table, is, sql } from "drizzle-orm";
import type { AnyColumn, SQL, SQLWrapper } from "drizzle-orm";

import { isNumber } from "./narrowing";

/**
 * The few places drizzle 0.44/0.45 and 1.0 differ at runtime or in their types. Everything else
 * reads a column or builds SQL the same way on both.
 */

export const not = (condition: SQLWrapper): SQL => sql`not ${condition}`;

/** 1.0 still sets `column.table` but no longer declares it. */
export const columnTable = (column: AnyColumn): Table => {
  if (!("table" in column) || !is(column.table, Table)) {
    throw new Error(`Column '${column.name}' has no table`);
  }

  return column.table;
};

/** An array column's dimensions in 1.0; 0.45 has none, its arrays are `PgArray` columns. */
const dimensionsOf = (column: AnyColumn): number =>
  "dimensions" in column && isNumber(column.dimensions) ? column.dimensions : 0;

/**
 * The column's data type in 0.45's vocabulary. 1.0 spells it `"<type> <constraint>"` (`"number
 * int32"`, `"object json"`), and gives an array column its element's type plus `dimensions`.
 */
export function dataTypeOf(column: AnyColumn): string;
export function dataTypeOf(column: AnyColumn | undefined): string | undefined;
export function dataTypeOf(column: AnyColumn | undefined): string | undefined {
  if (column === undefined) return undefined;

  if (dimensionsOf(column) > 0) return "array";

  const dataType: string = column.dataType;
  const [type = dataType, constraint] = dataType.split(" ");

  return type === "object" && constraint !== undefined ? constraint : type;
}

/**
 * The element column type of a one-dimensional PostgreSQL array: 0.45's `PgArray.baseColumn`, or
 * the column itself in 1.0, where an array is its element column with `dimensions: 1`.
 */
export const pgArrayElementColumnType = (column: AnyColumn): string | undefined => {
  if (column.columnType === "PgArray") {
    const base = "baseColumn" in column && is(column.baseColumn, Column) ? column.baseColumn : undefined;

    return base !== undefined && base.columnType !== "PgArray" ? base.columnType : undefined;
  }

  return dimensionsOf(column) === 1 ? column.columnType : undefined;
};
