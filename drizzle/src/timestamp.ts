import { is, sql } from "drizzle-orm";
import type { AnyColumn, SQL } from "drizzle-orm";
import { MySqlColumn } from "drizzle-orm/mysql-core";
import { PgColumn } from "drizzle-orm/pg-core";
import { SQLiteColumn } from "drizzle-orm/sqlite-core";

import { UnsupportedQueryPlanError } from "./errors";

/**
 * CEL `timestamp()` literals, validated and normalised to the canonical UTC string timestamp
 * columns hold. Anything that cannot be represented exactly at millisecond precision inside CEL's
 * instant range is refused rather than coerced: a lenient parse compares as some other instant.
 */

const RFC3339_MILLISECOND_TIMESTAMP =
  /^((?!0000)\d{4})-(\d{2})-(\d{2})[Tt](?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.(\d{1,9}))?(?:[Zz]|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/;
const MIN_RFC3339_TIMESTAMP_MILLISECONDS = Date.parse(
  "0001-01-01T00:00:00.000Z",
);
const MAX_RFC3339_TIMESTAMP_MILLISECONDS = Date.parse(
  "9999-12-31T23:59:59.999Z",
);

export const normalizeRfc3339Milliseconds = (value: string): string => {
  const match = RFC3339_MILLISECOND_TIMESTAMP.exec(value);
  const yearText = match?.[1];
  const monthText = match?.[2];
  const dayText = match?.[3];
  const fraction = match?.[4] ?? "";
  if (!yearText || !monthText || !dayText) {
    throw new UnsupportedQueryPlanError(`Invalid RFC-3339 timestamp value: ${value}`);
  }
  if ([...fraction.slice(3)].some((digit) => digit !== "0")) {
    throw new UnsupportedQueryPlanError(`Timestamp value exceeds millisecond precision: ${value}`);
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
    throw new UnsupportedQueryPlanError(`Invalid RFC-3339 timestamp value: ${value}`);
  }

  const milliseconds = Date.parse(value);
  if (Number.isNaN(milliseconds)) {
    throw new UnsupportedQueryPlanError(`Invalid RFC-3339 timestamp value: ${value}`);
  }
  if (
    milliseconds < MIN_RFC3339_TIMESTAMP_MILLISECONDS ||
    milliseconds > MAX_RFC3339_TIMESTAMP_MILLISECONDS
  ) {
    throw new UnsupportedQueryPlanError(
      `Timestamp value is outside CEL's supported instant range: ${value}`,
    );
  }
  return new Date(milliseconds).toISOString().replace(".000Z", "Z");
};

const NANOS_PER_MILLI = 1_000_000n;

/** The digits after the third fractional digit of an RFC-3339 literal, if any is non-zero. */
export const exceedsMillisecondPrecision = (value: string): boolean => {
  const fraction = RFC3339_MILLISECOND_TIMESTAMP.exec(value)?.[4] ?? "";
  return [...fraction.slice(3)].some((digit) => digit !== "0");
};

/**
 * An RFC-3339 literal as nanoseconds since the epoch — CEL's own instant resolution — validated
 * exactly as `normalizeRfc3339Milliseconds` validates it.
 */
export const parseRfc3339Nanoseconds = (value: string): bigint => {
  const match = RFC3339_MILLISECOND_TIMESTAMP.exec(value);
  const fraction = match?.[4];
  if (fraction === undefined) {
    return BigInt(Date.parse(normalizeRfc3339Milliseconds(value))) * NANOS_PER_MILLI;
  }
  const digits = fraction.padEnd(9, "0");
  const millisecondLiteral = value.replace(`.${fraction}`, `.${digits.slice(0, 3)}`);
  const milliseconds = Date.parse(normalizeRfc3339Milliseconds(millisecondLiteral));
  return BigInt(milliseconds) * NANOS_PER_MILLI + BigInt(digits.slice(3));
};

/**
 * The instant `nanoseconds` as a UTC RFC-3339 literal carrying `digits` fractional digits, which
 * must hold everything below them as zero. At millisecond precision or coarser it is exactly the
 * canonical string `normalizeRfc3339Milliseconds` produces.
 */
export const formatRfc3339Nanoseconds = (nanoseconds: bigint, digits: number): string => {
  const remainder = ((nanoseconds % NANOS_PER_MILLI) + NANOS_PER_MILLI) % NANOS_PER_MILLI;
  const milliseconds = (nanoseconds - remainder) / NANOS_PER_MILLI;
  const iso = new Date(Number(milliseconds)).toISOString();
  if (digits <= 3) return iso.replace(".000Z", "Z");
  const fraction = `${iso.slice(20, 23)}${remainder.toString().padStart(6, "0")}`;
  return `${iso.slice(0, 19)}.${fraction.slice(0, digits)}Z`;
};

/**
 * How a `valueType: "timestamp"` column is compared as an instant:
 *
 * - `"native"`: a PostgreSQL `timestamp` or a MySQL `datetime` / `timestamp`. The database parses the
 *   bound RFC-3339 literal into its own temporal type and compares instants.
 * - `"sqlite-text"`: a SQLite `text` column holding RFC-3339 strings. SQLite has no temporal type,
 *   so the stored string is rewritten into one fixed-width UTC form first (`sqliteTextInstant`).
 *
 * Any other column — a SQLite `integer` in `timestamp` or `timestamp_ms` mode, a PostgreSQL `text`
 * or `date`, a custom type — is refused: the database would compare the bound literal with it as
 * something other than an instant (SQLite ranks every integer below every string; a text column
 * compares two spellings of one instant as different strings).
 */
export type TimestampColumnForm = "native" | "sqlite-text";

export const timestampColumnForm = (
  column: AnyColumn,
  reference: string,
): TimestampColumnForm => {
  if (is(column, PgColumn) && column.columnType.startsWith("PgTimestamp")) return "native";
  if (
    is(column, MySqlColumn) &&
    (column.columnType.startsWith("MySqlDateTime") || column.columnType.startsWith("MySqlTimestamp"))
  ) {
    return "native";
  }
  if (is(column, SQLiteColumn) && column.columnType === "SQLiteText") return "sqlite-text";
  throw new UnsupportedQueryPlanError(
    `Cannot compare '${reference}' as a timestamp: its ${column.columnType} column is not a ` +
      "PostgreSQL timestamp, a MySQL datetime or timestamp, or a SQLite text column, so the " +
      "database would compare a timestamp literal with it as something other than an instant " +
      "(SQLite ranks every integer below every string)",
  );
};

const SQLITE_DATE_TIME_PREFIX = sql.raw(
  "'[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]*'",
);

/**
 * A SQLite text column's RFC-3339 value in the fixed-width UTC form `sqliteInstantLiteral` renders,
 * `YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ`, so that comparing two of them as strings compares the instants,
 * whichever spelling the row stored (`…00Z`, `…00.000Z`, an offset, a microsecond fraction).
 *
 * SQLite's date functions apply the offset, but they round the fraction to a millisecond and they
 * accept spellings CEL's `timestamp()` rejects. So only the whole seconds go through `strftime`,
 * and the stored fraction is carried over digit for digit. A value CEL cannot parse becomes NULL,
 * which leaves the comparison UNKNOWN under both polarities, as CEL's error denies the row: no
 * offset, a space, a lower-case `t`, a day or hour that does not exist, more than nine fractional
 * digits, an instant before year 1. So does an offset SQLite itself rejects (beyond ±14:00).
 */
export const sqliteTextInstant = (expr: SQL): SQL => {
  const zone = sql`(case when ${expr} glob '*Z' then 1 when ${expr} glob '*[+-][0-9][0-9]:[0-9][0-9]' then 6 end)`;
  const digits = sql`(length(${expr}) - 20 - ${zone})`;
  const wholeSeconds = sql`strftime('%Y-%m-%dT%H:%M:%S', substr(${expr}, 1, 19) || substr(${expr}, length(${expr}) - ${zone} + 1))`;
  const valid = sql.join(
    [
      sql`${expr} glob ${SQLITE_DATE_TIME_PREFIX}`,
      sql`strftime('%Y-%m-%dT%H:%M:%S', julianday(substr(${expr}, 1, 19))) = substr(${expr}, 1, 19)`,
      sql`(${digits} = -1 or (substr(${expr}, 20, 1) = '.' and ${digits} between 1 and 9 and substr(${expr}, 21, ${digits}) not glob '*[^0-9]*'))`,
      sql`${wholeSeconds} >= '0001-01-01T00:00:00'`,
    ],
    sql` and `,
  );
  return sql`(case when ${valid} then ${wholeSeconds} || '.' || substr(substr(${expr}, 21, max(${digits}, 0)) || '000000000', 1, 9) || 'Z' end)`;
};

/** An RFC-3339 literal in the fixed-width UTC form `sqliteTextInstant` gives a stored value. */
export const sqliteInstantLiteral = (value: string): string =>
  formatRfc3339Nanoseconds(parseRfc3339Nanoseconds(value), 9);
