// RFC 3339 timestamp literals, validated to CEL's instant range and Prisma's millisecond precision.

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
  const match = RFC3339_MILLISECOND_TIMESTAMP.exec(value);
  const yearText = match?.[1];
  const monthText = match?.[2];
  const dayText = match?.[3];
  const fraction = match?.[4] ?? "";
  if (!yearText || !monthText || !dayText) {
    throw new Error(`Invalid RFC 3339 timestamp value: ${value}`);
  }
  if ([...fraction.slice(3)].some((digit) => digit !== "0")) {
    throw new Error(
      `Timestamp value exceeds millisecond precision: ${value}`
    );
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
    throw new Error(`Invalid RFC 3339 timestamp value: ${value}`);
  }

  const milliseconds = Date.parse(value);
  if (Number.isNaN(milliseconds)) {
    throw new Error(`Invalid RFC 3339 timestamp value: ${value}`);
  }
  if (
    milliseconds < MIN_RFC3339_TIMESTAMP_MILLISECONDS ||
    milliseconds > MAX_RFC3339_TIMESTAMP_MILLISECONDS
  ) {
    throw new Error(
      `Timestamp value is outside CEL's supported instant range: ${value}`
    );
  }
  return new Date(milliseconds).toISOString();
}
