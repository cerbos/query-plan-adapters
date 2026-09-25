// RFC 3339 timestamp literals, validated to CEL's instant range and Prisma's millisecond precision.

import { UnsupportedQueryPlanError } from "./errors";

const RFC3339_MILLISECOND_TIMESTAMP =
  /^((?!0000)\d{4})-(\d{2})-(\d{2})[Tt](?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.(\d{1,9}))?(?:[Zz]|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/;
const MIN_RFC3339_TIMESTAMP_MILLISECONDS = Date.parse(
  "0001-01-01T00:00:00.000Z"
);
const MAX_RFC3339_TIMESTAMP_MILLISECONDS = Date.parse(
  "9999-12-31T23:59:59.999Z"
);

/**
 * Parses an RFC 3339 literal into the ISO string Prisma binds. Anything a lenient `Date` parse
 * would coerce into some other instant — a missing time part, a day that does not exist, digits
 * past the millisecond, an instant outside CEL's range — is refused instead.
 */
export function normalizeRfc3339Milliseconds(value: string): string {
  const { iso, subMillisecond } = parseRfc3339Instant(value);
  if (subMillisecond) {
    throw new UnsupportedQueryPlanError(
      `Timestamp value exceeds millisecond precision: ${value}`
    );
  }
  return iso;
}

/**
 * Parses an RFC 3339 literal like normalizeRfc3339Milliseconds, except that an instant between
 * two milliseconds is accepted: `iso` is then the NEXT millisecond, and `subMillisecond` says so.
 * A comparison against it has to be rewritten for that rounding (see roundSubMillisecond).
 */
export function parseRfc3339Instant(value: string): {
  iso: string;
  subMillisecond: boolean;
} {
  const match = RFC3339_MILLISECOND_TIMESTAMP.exec(value);
  const yearText = match?.[1];
  const monthText = match?.[2];
  const dayText = match?.[3];
  const fraction = match?.[4] ?? "";
  if (!yearText || !monthText || !dayText) {
    throw new UnsupportedQueryPlanError(`Invalid RFC 3339 timestamp value: ${value}`);
  }
  const subMillisecond = [...fraction.slice(3)].some((digit) => digit !== "0");

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
    throw new UnsupportedQueryPlanError(`Invalid RFC 3339 timestamp value: ${value}`);
  }

  const milliseconds = Date.parse(value);
  if (Number.isNaN(milliseconds)) {
    throw new UnsupportedQueryPlanError(`Invalid RFC 3339 timestamp value: ${value}`);
  }
  if (
    milliseconds < MIN_RFC3339_TIMESTAMP_MILLISECONDS ||
    milliseconds > MAX_RFC3339_TIMESTAMP_MILLISECONDS
  ) {
    throw new UnsupportedQueryPlanError(
      `Timestamp value is outside CEL's supported instant range: ${value}`
    );
  }
  // Date.parse drops the digits past the millisecond, so a sub-millisecond instant lies strictly
  // between `milliseconds` and the next one.
  const rounded = subMillisecond ? milliseconds + 1 : milliseconds;
  if (rounded > MAX_RFC3339_TIMESTAMP_MILLISECONDS) {
    throw new UnsupportedQueryPlanError(
      `Timestamp value is outside CEL's supported instant range: ${value}`
    );
  }
  return { iso: new Date(rounded).toISOString(), subMillisecond };
}

/**
 * A Prisma `DateTime` carries milliseconds, so the attribute an application builds from one is a
 * whole millisecond `a`, truncated from whatever the column stores. Against an instant `T` strictly
 * between two milliseconds, with `C` the millisecond after it: `a < T` and `a <= T` both mean
 * `column < C`, `a > T` and `a >= T` both mean `column >= C`, and `a == T` never holds. Returns the
 * operator to compare against `C`, or null for the equality family.
 */
export function roundSubMillisecond(operator: string): string | null {
  switch (operator) {
    case "lt":
    case "le":
      return "lt";
    case "gt":
    case "ge":
      return "ge";
    default:
      return null;
  }
}
