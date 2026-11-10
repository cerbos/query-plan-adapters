import { sql } from "drizzle-orm";
import type { AnyColumn, SQL } from "drizzle-orm";

const MYSQL_TEXT = new Set(["MySqlVarChar", "MySqlChar", "MySqlText", "MySqlEnumColumn", "MySqlEnumObjectColumn"]);
const PG_TEXT = new Set(["PgVarchar", "PgChar", "PgText"]);

/**
 * A string column read the way CEL compares strings: byte for byte. MySQL's default collation,
 * `utf8mb4_0900_ai_ci`, folds case and accents, so every MySQL string column is compared under
 * `utf8mb4_0900_bin`, which is byte-exact and NO PAD; an explicit COLLATE on one operand decides
 * the comparison whatever the other operand's collation. PostgreSQL equality is exact under any
 * deterministic collation, but a linguistic collation orders strings out of code-point order, so
 * an ordering reads a PostgreSQL string column under `"C"`. Other column types (a datetime or
 * decimal in string mode, a custom type) are left as they are.
 */
export const exactString = (column: AnyColumn, expr: SQL = sql`${column}`): SQL =>
  MYSQL_TEXT.has(column.columnType)
    ? sql`(${expr} collate utf8mb4_0900_bin)`
    : expr;

export const exactOrdering = (column: AnyColumn, expr: SQL = sql`${column}`): SQL =>
  PG_TEXT.has(column.columnType)
    ? sql`(${expr} collate "C")`
    : exactString(column, expr);

/** Both sides of an ordering under `"C"`, when either is a PostgreSQL string column. */
export const exactPgOrdering = (
  columns: readonly (AnyColumn | undefined)[],
  left: SQL,
  right: SQL,
): [SQL, SQL] =>
  columns.some((column) => column !== undefined && PG_TEXT.has(column.columnType))
    ? [sql`(${left} collate "C")`, sql`(${right} collate "C")`]
    : [left, right];
