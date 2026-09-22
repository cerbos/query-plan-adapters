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

export const valuesEqual = (left: unknown, right: unknown): boolean => {
  if (typeof left === "number" && typeof right === "number") {
    return !Number.isNaN(left) && !Number.isNaN(right) && left === right;
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
    (typeof left !== "number" || typeof right !== "number") &&
    (typeof left !== "string" || typeof right !== "string") &&
    (typeof left !== "bigint" || typeof right !== "bigint")
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
const CEL_INT_STRING = /^-?\d+$/;

/**
 * The magnitude band in which `String(n)` was MEASURED to render a double exactly as CEL does.
 *
 * CEL's number formatting is Go's, and it does not agree with JavaScript's everywhere. Probing the
 * pinned PDP with `string(R.attr.d) == R.attr.js` (the two renderings compared by the PDP itself)
 * puts the disagreements outside this band and nowhere inside it: 0.0000999, 1e15, 1e16, 1e19,
 * 1234567890123456 and `Number.MAX_SAFE_INTEGER` all differ, while 0, 100, 1000, 0.25, 1.3, 16.3,
 * -4.7, -0.6, 0.0001 and 0.0001234 all agree.
 *
 * The band is deliberately narrower than the agreement it is derived from — 1e21 agrees and is
 * still refused — because the rule producing it is Go's and is not restated here. Outside the
 * band the conversion is an evaluation error, which DENIES the row: the same thing CEL does with a
 * conversion it cannot perform, and the safe direction, since the failure that matters is an
 * over-grant (cerbos/query-plan-adapters#376).
 */
const STRING_CAST_MIN_MAGNITUDE = 1e-4;
const STRING_CAST_MAX_MAGNITUDE = 1e15;

// A bool needs no band: CEL renders "true"/"false" and so does JavaScript, exactly. That is why
// convex is one of the two adapters that lower `string()` over a boolean rather than refusing it —
// the SQL adapters cannot, because SQLite and MySQL store 1/0.
export const convertToString = (value: unknown): string | EvaluationError => {
  if (typeof value === "string") return value;
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "number" && Number.isFinite(value)) {
    // JavaScript renders a negative zero as "0" where CEL renders "-0".
    if (Object.is(value, -0)) return EVALUATION_ERROR;
    const magnitude = Math.abs(value);
    if (
      magnitude !== 0 &&
      (magnitude < STRING_CAST_MIN_MAGNITUDE ||
        magnitude >= STRING_CAST_MAX_MAGNITUDE)
    ) {
      return EVALUATION_ERROR;
    }
    return String(value);
  }
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

export const convertToInt = (value: unknown): number | EvaluationError => {
  if (typeof value === "number") {
    if (!Number.isFinite(value)) return EVALUATION_ERROR;
    const converted = Math.trunc(value);
    return Number.isSafeInteger(converted) ? converted : EVALUATION_ERROR;
  }
  if (typeof value !== "string" || !CEL_INT_STRING.test(value)) {
    return EVALUATION_ERROR;
  }
  const converted = Number(value);
  return Number.isSafeInteger(converted) ? converted : EVALUATION_ERROR;
};
