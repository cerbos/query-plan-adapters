// CEL's value semantics, as the post-filter needs them. Every function here is total: a value CEL
// would raise on comes back as `EVALUATION_ERROR`, which the evaluator propagates the way CEL
// propagates an error — through `&&`/`||` absorption, and to a deny at the root.
//
// Ported from the convex adapter's evaluator and copied rather than shared: adapters share data, not
// code (docs/adr/0007-adapters-share-data-not-code.md). Where it departs from that evaluator it is
// to follow cel-go more closely: a timestamp is its own type rather than a bare bigint, so it never
// equals or orders against a number; int arithmetic is exact over bigints (`intArithmetic`);
// strings order by code point, as cel-go's byte-wise UTF-8 comparison does, not by UTF-16 code
// unit; and bools order, false before true.

export const EVALUATION_ERROR = Symbol("Cerbos evaluation error");

export type EvaluationError = typeof EVALUATION_ERROR;

export const isEvaluationError = (value: unknown): value is EvaluationError =>
  value === EVALUATION_ERROR;

export const asBoolean = (value: unknown): boolean | EvaluationError =>
  typeof value === "boolean" ? value : EVALUATION_ERROR;

export const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" &&
  value !== null &&
  !Array.isArray(value) &&
  !(value instanceof CelTimestamp);

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

/** A CEL timestamp: nanoseconds since the epoch, so all nine fractional digits are compared. */
export class CelTimestamp {
  constructor(readonly nanos: bigint) {}
}

/** A number, or an int beyond the safe range: `int()` returns a bigint only there. */
const isNumeric = (value: unknown): value is number | bigint =>
  typeof value === "number" || typeof value === "bigint";

/** CEL's heterogeneous equality: numbers compare by value across int and double, anything else by type. */
export const valuesEqual = (left: unknown, right: unknown): boolean => {
  if (isNumeric(left) && isNumeric(right)) {
    // JavaScript compares a bigint with a number exactly, so 2^53 + 1 never equals 2^53.
    return (
      !Number.isNaN(left) &&
      !Number.isNaN(right) &&
      !(left < right || left > right)
    );
  }
  if (left instanceof CelTimestamp || right instanceof CelTimestamp) {
    return (
      left instanceof CelTimestamp &&
      right instanceof CelTimestamp &&
      left.nanos === right.nanos
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

export type ComparisonOperator = "eq" | "ne" | "lt" | "le" | "gt" | "ge";

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
  let a: unknown = left;
  let b: unknown = right;
  if (typeof left === "string" && typeof right === "string") {
    // cel-go orders strings by their UTF-8 bytes, which is code point order. JavaScript's `<`
    // compares UTF-16 code units, which puts an astral character (a surrogate pair, 0xD800…)
    // before U+E000–U+FFFF.
    a = compareCodePoints(left, right);
    b = 0;
  } else if (typeof left === "boolean" && typeof right === "boolean") {
    // CEL orders bools: false < true.
    a = Number(left);
    b = Number(right);
  } else if (left instanceof CelTimestamp && right instanceof CelTimestamp) {
    a = left.nanos;
    b = right.nanos;
  } else if (Number.isNaN(left) || Number.isNaN(right)) {
    return false;
  } else if (
    !(isNumeric(left) && isNumeric(right)) &&
    (typeof left !== "string" || typeof right !== "string")
  ) {
    return EVALUATION_ERROR;
  }
  const x = a as number | bigint | string;
  const y = b as number | bigint | string;
  switch (operator) {
    case "lt":
      return x < y;
    case "le":
      return x <= y;
    case "gt":
      return x > y;
    case "ge":
      return x >= y;
  }
};

/** -1, 0 or 1 as `a` orders before, with or after `b` by Unicode code point. */
const compareCodePoints = (a: string, b: string): number => {
  let i = 0;
  let j = 0;
  while (i < a.length && j < b.length) {
    const x = a.codePointAt(i)!;
    const y = b.codePointAt(j)!;
    if (x !== y) return x < y ? -1 : 1;
    i += x > 0xffff ? 2 : 1;
    j += y > 0xffff ? 2 : 1;
  }
  return Number(i < a.length) - Number(j < b.length);
};

// -- int arithmetic ------------------------------------------------------------------------------

const INT64_MIN = -(2n ** 63n);
const INT64_MAX = 2n ** 63n - 1n;

/**
 * An int as the evaluator carries it: a number while it is a safe integer, a bigint past that, so
 * `int("9007199254740993")` is not rounded onto 2^53. Outside int64 it is CEL's overflow error.
 */
const celInt = (value: bigint): number | bigint | EvaluationError => {
  if (value < INT64_MIN || value > INT64_MAX) return EVALUATION_ERROR;
  const asNumber = Number(value);
  return Number.isSafeInteger(asNumber) ? asNumber : value;
};

const toBigInt = (value: unknown): bigint | undefined => {
  if (typeof value === "bigint") return value;
  if (typeof value === "number" && Number.isInteger(value)) {
    return BigInt(value);
  }
  return undefined;
};

/**
 * CEL int arithmetic, exact over bigints: `/` and `%` truncate toward zero (as bigint division
 * does), a zero divisor and an int64 overflow are errors.
 */
export const intArithmetic = (
  operator: "add" | "sub" | "mult" | "div" | "mod",
  left: unknown,
  right: unknown,
): number | bigint | EvaluationError => {
  const a = toBigInt(left);
  const b = toBigInt(right);
  if (a === undefined || b === undefined) return EVALUATION_ERROR;
  switch (operator) {
    case "add":
      return celInt(a + b);
    case "sub":
      return celInt(a - b);
    case "mult":
      return celInt(a * b);
    case "div":
      // INT64_MIN / -1 overflows, which celInt reports.
      return b === 0n ? EVALUATION_ERROR : celInt(a / b);
    case "mod":
      return b === 0n ? EVALUATION_ERROR : celInt(a % b);
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

export const parseRfc3339Timestamp = (
  value: string,
): CelTimestamp | EvaluationError => {
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
    : new CelTimestamp(timestampNanos);
};

// -- type conversions: string(), double(), int() --------------------------------------------------

const CEL_DOUBLE_STRING = /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/;
const CEL_INT_STRING = /^[+-]?\d+$/;

/**
 * CEL renders a double with Go's `strconv.FormatFloat(d, 'g', -1, 64)`: the shortest digits that
 * round-trip, in exponent form when the decimal exponent is below -4 or at least 6 ("1e+06",
 * "-9.5e+18", "1e-05"), and plainly otherwise ("100000", "0.0001"). Only the DIGITS are taken from
 * JavaScript — `toExponential()` with no argument yields the same shortest round-trip digits Go
 * does — and the layout is Go's.
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

/**
 * `string()` over a value the post-filter can meet. A number is a double unless it came from an
 * int-typed expression, and the two render identically for an integral value ("3"), so the
 * double rendering is exact for both; a bigint is only ever an int.
 */
export const convertToString = (value: unknown): string | EvaluationError => {
  if (typeof value === "string") return value;
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "bigint") return value.toString();
  if (typeof value === "number") return formatGoDouble(value);
  return EVALUATION_ERROR;
};

export const convertToDouble = (value: unknown): number | EvaluationError => {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : EVALUATION_ERROR;
  }
  if (typeof value === "bigint") return Number(value);
  if (typeof value !== "string" || !CEL_DOUBLE_STRING.test(value)) {
    return EVALUATION_ERROR;
  }
  const converted = Number(value);
  return Number.isFinite(converted) ? converted : EVALUATION_ERROR;
};

export const convertToInt = (
  value: unknown,
): number | bigint | EvaluationError => {
  if (typeof value === "bigint") return celInt(value);
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
