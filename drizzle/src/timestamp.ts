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
