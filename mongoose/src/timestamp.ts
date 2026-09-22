const RFC3339_TIMESTAMP_PATTERN =
  /^((?!0000)\d{4})-(\d{2})-(\d{2})[Tt](?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,3})?(?:[Zz]|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/;

// Same source, but for PCRE2 (Mongo) rather than the JS engine: PCRE2 `$`
// also matches immediately before a final newline, so "…Z\n" would pass as
// RFC 3339 and $convert would silently accept it. `\z` pins the absolute end.
export const RFC3339_TIMESTAMP_MONGO_PATTERN =
  RFC3339_TIMESTAMP_PATTERN.source.replace(/\$$/, "\\z");

// CEL's timestamp range, inclusive.
const MIN_CEL_TIMESTAMP_MILLISECONDS = Date.parse("0001-01-01T00:00:00.000Z");
const MAX_CEL_TIMESTAMP_MILLISECONDS = Date.parse("9999-12-31T23:59:59.999Z");
export const MIN_CEL_TIMESTAMP = new Date(MIN_CEL_TIMESTAMP_MILLISECONDS);
export const MAX_CEL_TIMESTAMP = new Date(MAX_CEL_TIMESTAMP_MILLISECONDS);

/** Whether a CEL timestamp literal is an RFC 3339 instant a BSON Date can hold exactly. */
export const isRfc3339Timestamp = (value: string): boolean => {
  const match = RFC3339_TIMESTAMP_PATTERN.exec(value);
  if (!match) {
    return false;
  }
  const [year, month, day] = [match[1], match[2], match[3]].map(Number);
  if (year === undefined || month === undefined || day === undefined) {
    return false;
  }
  const calendarDate = new Date(0);
  calendarDate.setUTCHours(0, 0, 0, 0);
  calendarDate.setUTCFullYear(year, month - 1, day);
  if (
    calendarDate.getUTCFullYear() !== year ||
    calendarDate.getUTCMonth() !== month - 1 ||
    calendarDate.getUTCDate() !== day
  ) {
    return false;
  }

  const instant = new Date(value).getTime();
  return (
    !Number.isNaN(instant) &&
    instant >= MIN_CEL_TIMESTAMP_MILLISECONDS &&
    instant <= MAX_CEL_TIMESTAMP_MILLISECONDS
  );
};
