import type { ComparisonOperator } from "./operands";

// CEL's value semantics, as the in-memory evaluator needs them. Every function here is total: a
// value CEL would raise on comes back as `EVALUATION_ERROR`, which the evaluator propagates the way
// CEL propagates an error — through `&&`/`||` absorption, and to a deny at the root.

export const EVALUATION_ERROR = Symbol("Cerbos evaluation error");

export type EvaluationError = typeof EVALUATION_ERROR;

export const isEvaluationError = (value: unknown): value is EvaluationError =>
  value === EVALUATION_ERROR;

export const asBoolean = (value: unknown): boolean | EvaluationError =>
  typeof value === "boolean" ? value : EVALUATION_ERROR;

export const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/** Reads a dotted path; an absent key at any level is CEL's missing-attribute error. */
export const getNestedValue = (obj: unknown, path: string): unknown => {
  let current: unknown = obj;
  for (const part of path.split(".")) {
    if (
      !isRecord(current) ||
      !Object.prototype.hasOwnProperty.call(current, part)
    ) {
      return EVALUATION_ERROR;
    }
    current = current[part];
  }
  return current;
};

/** A number or an int beyond the safe range: `int()` returns a bigint only there. */
const isNumeric = (value: unknown): value is number | bigint =>
  typeof value === "number" || typeof value === "bigint";

export const valuesEqual = (left: unknown, right: unknown): boolean => {
  if (isNumeric(left) && isNumeric(right)) {
    // JavaScript compares a bigint with a number exactly, so 2^53 + 1 never equals 2^53.
    return (
      !Number.isNaN(left) &&
      !Number.isNaN(right) &&
      !(left < right || left > right)
    );
  }
  if (Object.is(left, right)) return true;
  if (Array.isArray(left) && Array.isArray(right)) {
    return (
      left.length === right.length &&
      left.every((value, index) => valuesEqual(value, right[index]))
    );
  }
  if (isRecord(left) && isRecord(right)) {
    const leftKeys = Object.keys(left).sort();
    const rightKeys = Object.keys(right).sort();
    return (
      leftKeys.length === rightKeys.length &&
      leftKeys.every(
        (key, index) =>
          key === rightKeys[index] && valuesEqual(left[key], right[key]),
      )
    );
  }
  return false;
};

export const compareValues = (
  operator: ComparisonOperator,
  left: unknown,
  right: unknown,
): boolean | EvaluationError => {
  if (isEvaluationError(left) || isEvaluationError(right)) {
    return EVALUATION_ERROR;
  }
  if (operator === "eq" || operator === "ne") {
    const equal = valuesEqual(left, right);
    return operator === "eq" ? equal : !equal;
  }
  if (Number.isNaN(left) || Number.isNaN(right)) return false;
  if (
    !(isNumeric(left) && isNumeric(right)) &&
    (typeof left !== "string" || typeof right !== "string")
  ) {
    return EVALUATION_ERROR;
  }
  switch (operator) {
    case "lt":
      return left < right;
    case "le":
      return left <= right;
    case "gt":
      return left > right;
    case "ge":
      return left >= right;
  }
};

// -- hierarchies ---------------------------------------------------------------------------------

export interface HierarchyValue {
  value: string;
  delimiter: string;
}

export const isHierarchyValue = (value: unknown): value is HierarchyValue =>
  isRecord(value) &&
  typeof value["value"] === "string" &&
  typeof value["delimiter"] === "string";

export const isStrictAncestor = (
  ancestor: HierarchyValue,
  descendent: HierarchyValue,
): boolean =>
  ancestor.delimiter === descendent.delimiter &&
  ancestor.value !== descendent.value &&
  descendent.value.startsWith(ancestor.value + ancestor.delimiter);

// -- timestamps ----------------------------------------------------------------------------------

const RFC3339_TIMESTAMP =
  /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|([+-])(\d{2}):(\d{2}))$/;

const MIN_TIMESTAMP_NANOS = -62135596800000000000n;
const MAX_TIMESTAMP_NANOS = 253402300799999999999n;

const isLeapYear = (year: number): boolean =>
  year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);

const daysInMonth = (year: number, month: number): number => {
  const days = [
    31,
    isLeapYear(year) ? 29 : 28,
    31,
    30,
    31,
    30,
    31,
    31,
    30,
    31,
    30,
    31,
  ];
  return days[month - 1] ?? 0;
};

/** Nanoseconds since the epoch, so the nine fractional digits a plan carries are all compared. */
export const parseRfc3339Timestamp = (
  value: string,
): bigint | EvaluationError => {
  const match = RFC3339_TIMESTAMP.exec(value);
  if (!match) return EVALUATION_ERROR;

  const [
    ,
    yearPart,
    monthPart,
    dayPart,
    hourPart,
    minutePart,
    secondPart,
    fractionPart,
    zonePart,
    offsetSign,
    offsetHourPart,
    offsetMinutePart,
  ] = match;
  if (
    !yearPart ||
    !monthPart ||
    !dayPart ||
    !hourPart ||
    !minutePart ||
    !secondPart ||
    !zonePart
  ) {
    return EVALUATION_ERROR;
  }

  const year = Number(yearPart);
  const month = Number(monthPart);
  const day = Number(dayPart);
  const hour = Number(hourPart);
  const minute = Number(minutePart);
  const second = Number(secondPart);
  if (
    year < 1 ||
    year > 9999 ||
    month < 1 ||
    month > 12 ||
    day < 1 ||
    day > daysInMonth(year, month) ||
    hour > 23 ||
    minute > 59 ||
    second > 59
  ) {
    return EVALUATION_ERROR;
  }

  let offsetMinutes = 0;
  if (zonePart !== "Z") {
    if (!offsetSign || !offsetHourPart || !offsetMinutePart) {
      return EVALUATION_ERROR;
    }
    const offsetHour = Number(offsetHourPart);
    const offsetMinute = Number(offsetMinutePart);
    if (offsetHour > 23 || offsetMinute > 59) return EVALUATION_ERROR;
    offsetMinutes =
      (offsetHour * 60 + offsetMinute) * (offsetSign === "+" ? 1 : -1);
  }

  const instant = new Date(0);
  instant.setUTCFullYear(year, month - 1, day);
  instant.setUTCHours(hour, minute, second, 0);
  const epochMillis = instant.getTime() - offsetMinutes * 60_000;
  if (!Number.isFinite(epochMillis)) return EVALUATION_ERROR;

  const fractionNanos = BigInt((fractionPart ?? "").padEnd(9, "0") || "0");
  const timestampNanos = BigInt(epochMillis) * 1_000_000n + fractionNanos;
  return timestampNanos < MIN_TIMESTAMP_NANOS ||
    timestampNanos > MAX_TIMESTAMP_NANOS
    ? EVALUATION_ERROR
    : timestampNanos;
};

// -- type conversions: string(), double(), int() --------------------------------------------------

const CEL_DOUBLE_STRING = /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/;
const CEL_INT_STRING = /^[+-]?\d+$/;

/**
 * CEL renders a double with Go's `strconv.FormatFloat(d, 'g', -1, 64)`: the shortest digits that
 * round-trip, in exponent form when the decimal exponent is below -4 or at least 6 ("1e+06",
 * "-9.5e+18", "1e-05"), and plainly otherwise ("100000", "0.0001"). JavaScript's `String(n)`
 * switches to exponent form only at 1e21 and 1e-7, and spells the exponent differently, so only
 * the DIGITS are taken from JavaScript — `toExponential()` with no argument yields the same
 * shortest round-trip digits Go does — and the layout is Go's.
 */
const formatGoDouble = (value: number): string => {
  if (Number.isNaN(value)) return "NaN";
  if (!Number.isFinite(value)) return value > 0 ? "+Inf" : "-Inf";
  // JavaScript renders a negative zero as "0" where CEL renders "-0".
  const sign = value < 0 || Object.is(value, -0) ? "-" : "";
  if (value === 0) return `${sign}0`;
  const [mantissa = "", exponentPart = ""] = Math.abs(value)
    .toExponential()
    .split("e");
  const digits = mantissa.replace(".", "");
  const exponent = Number(exponentPart);
  if (exponent < -4 || exponent >= 6) {
    const fraction = digits.length > 1 ? `.${digits.slice(1)}` : "";
    const magnitude = String(Math.abs(exponent)).padStart(2, "0");
    return `${sign}${digits[0]}${fraction}e${exponent < 0 ? "-" : "+"}${magnitude}`;
  }
  const pointAt = exponent + 1;
  if (pointAt <= 0) return `${sign}0.${"0".repeat(-pointAt)}${digits}`;
  if (pointAt >= digits.length) {
    return `${sign}${digits}${"0".repeat(pointAt - digits.length)}`;
  }
  return `${sign}${digits.slice(0, pointAt)}.${digits.slice(pointAt)}`;
};

// A bool renders "true"/"false" in CEL and in JavaScript alike. That is why convex is one of the
// two adapters that lower `string()` over a boolean rather than refusing it — the SQL adapters
// cannot, because SQLite and MySQL store 1/0.
export const convertToString = (value: unknown): string | EvaluationError => {
  if (typeof value === "string") return value;
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "number") return formatGoDouble(value);
  return EVALUATION_ERROR;
};

export const convertToDouble = (value: unknown): number | EvaluationError => {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : EVALUATION_ERROR;
  }
  if (typeof value !== "string" || !CEL_DOUBLE_STRING.test(value)) {
    return EVALUATION_ERROR;
  }
  const converted = Number(value);
  return Number.isFinite(converted) ? converted : EVALUATION_ERROR;
};

const INT64_MIN = -(2n ** 63n);
const INT64_MAX = 2n ** 63n - 1n;

/**
 * An int as the evaluator carries it: a number while it is a safe integer, a bigint past that, so
 * `int("9007199254740993")` is not rounded onto 2^53. A zero is always the unsigned `0` — a CEL
 * int has no negative zero, and `int("-0")` and `int(-0.5)` are both 0.
 */
const celInt = (value: bigint): number | bigint | EvaluationError => {
  if (value < INT64_MIN || value > INT64_MAX) return EVALUATION_ERROR;
  const asNumber = Number(value);
  return Number.isSafeInteger(asNumber) ? asNumber : value;
};

export const convertToInt = (
  value: unknown,
): number | bigint | EvaluationError => {
  if (typeof value === "number") {
    // cel-go refuses a double at or beyond either int64 bound, -2^63 included.
    if (!Number.isFinite(value) || value <= -(2 ** 63) || value >= 2 ** 63) {
      return EVALUATION_ERROR;
    }
    return celInt(BigInt(Math.trunc(value)));
  }
  // Go's strconv.ParseInt: an optional sign, then decimal digits and nothing else. BigInt() alone
  // would also accept surrounding whitespace, which ParseInt rejects.
  if (typeof value !== "string" || !CEL_INT_STRING.test(value)) {
    return EVALUATION_ERROR;
  }
  return celInt(BigInt(value));
};
