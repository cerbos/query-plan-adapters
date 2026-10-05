// RFC 3339 timestamp literals, validated to CEL's instant range and Prisma's millisecond precision.

import type { PlanExpressionOperand } from "@cerbos/core";

import {
  COMPARISON_OPERATORS,
  isNamedOperand,
  isOperatorOperand,
  isValueOperand,
  mirrorOperator,
} from "./plan";
import type { OperatorOperand } from "./plan";
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

// -- timestamp arithmetic, solved for the column -------------------------------------------------

const MIN_INSTANT = MIN_RFC3339_TIMESTAMP_MILLISECONDS;
const MAX_INSTANT = MAX_RFC3339_TIMESTAMP_MILLISECONDS;

/**
 * A comparison over timestamp arithmetic, rewritten as a comparison of the bare `timestamp(x)`
 * against a literal instant, or undefined for any other shape:
 *
 * - `timeSince(timestamp(x)) CMP duration(d)`: timeSince is `now - x`, read from the clock when
 *   the query runs, so it holds exactly when `x` lies on the mirrored side of `now - d`. Cerbos
 *   computes it with Go's saturating `time.Sub`, which raises no error.
 * - `timestamp(x) ± duration(d) CMP timestamp(t)`: the shift is monotone, so it holds exactly when
 *   `x CMP t ∓ d`. CEL raises an error when the shifted instant leaves 0001-01-01..9999-12-31,
 *   which the comparison alone cannot carry: `positive` says whether the leaf sits under an even
 *   number of negations, and the rows that overflow are then excluded from (positive) or included
 *   in (negative) the leaf, so the enclosing negations never select them.
 *
 * Both sides may come in either order. A duration or instant that is not a whole millisecond, or a
 * bound outside CEL's range, is left alone for the translator to refuse.
 */
export function rewriteTemporalComparison(
  expr: OperatorOperand,
  positive: boolean,
  now: number = Date.now()
): PlanExpressionOperand | undefined {
  if (!COMPARISON_OPERATORS.has(expr.operator) || expr.operands.length !== 2) return undefined;
  let [left, right] = expr.operands as [PlanExpressionOperand, PlanExpressionOperand];
  let operator = expr.operator;
  if (!isTemporalArithmetic(left) && isTemporalArithmetic(right)) {
    [left, right] = [right, left];
    operator = mirrorOperator(operator);
  }
  if (!isOperatorOperand(left)) return undefined;

  if (left.operator === "timeSince") {
    const [column] = left.operands;
    const duration = durationMilliseconds(right);
    if (left.operands.length !== 1 || !isTimestampColumn(column) || duration === undefined) {
      return undefined;
    }
    const bound = instantLiteral(now - duration);
    // now - x CMP d  <=>  x mirror(CMP) now - d
    return bound && compareInstant(mirrorOperator(operator), column, bound);
  }

  if ((left.operator !== "add" && left.operator !== "sub") || left.operands.length !== 2) {
    return undefined;
  }
  const [a, b] = left.operands as [PlanExpressionOperand, PlanExpressionOperand];
  let column: OperatorOperand;
  let duration: number | undefined;
  if (isTimestampColumn(a) && durationMilliseconds(b) !== undefined) {
    [column, duration] = [a, durationMilliseconds(b)];
  } else if (left.operator === "add" && isTimestampColumn(b)) {
    [column, duration] = [b, durationMilliseconds(a)];
  } else {
    return undefined;
  }
  const instant = instantMilliseconds(right);
  if (duration === undefined || instant === undefined) return undefined;
  const shift = left.operator === "add" ? duration : -duration;
  const bound = instantLiteral(instant - shift);
  if (bound === undefined) return undefined;
  const solved = compareInstant(operator, column, bound);
  if (shift === 0) return solved;
  const limit = instantLiteral(shift > 0 ? MAX_INSTANT - shift : MIN_INSTANT - shift);
  if (limit === undefined) return undefined;
  const inRange = compareInstant(shift > 0 ? "le" : "ge", column, limit);
  return positive
    ? { operator: "and", operands: [solved, inRange] }
    : { operator: "or", operands: [solved, { operator: "not", operands: [inRange] }] };
}

function isTemporalArithmetic(operand: PlanExpressionOperand): boolean {
  return (
    isOperatorOperand(operand) &&
    (operand.operator === "timeSince" ||
      ((operand.operator === "add" || operand.operator === "sub") &&
        operand.operands.some(isTimestampColumn)))
  );
}

/** `timestamp(column)`. */
function isTimestampColumn(
  operand: PlanExpressionOperand | undefined
): operand is OperatorOperand {
  return (
    operand !== undefined &&
    isOperatorOperand(operand) &&
    operand.operator === "timestamp" &&
    operand.operands.length === 1 &&
    isNamedOperand(operand.operands[0]!)
  );
}

const PROTOBUF_DURATION = /^(-)?(\d+)(?:\.(\d{1,9}))?s$/;

/** A `duration("...")` literal in the planner's protobuf spelling, in whole milliseconds. */
function durationMilliseconds(operand: PlanExpressionOperand): number | undefined {
  if (
    !isOperatorOperand(operand) ||
    operand.operator !== "duration" ||
    operand.operands.length !== 1
  ) {
    return undefined;
  }
  const [literal] = operand.operands;
  if (!isValueOperand(literal!) || typeof literal.value !== "string") return undefined;
  const match = PROTOBUF_DURATION.exec(literal.value);
  if (!match) return undefined;
  const fraction = (match[3] ?? "").padEnd(9, "0");
  if (fraction.slice(3) !== "000000") return undefined;
  const milliseconds = Number(match[2]) * 1000 + Number(fraction.slice(0, 3));
  if (!Number.isSafeInteger(milliseconds)) return undefined;
  return match[1] ? -milliseconds : milliseconds;
}

/** A `timestamp("...")` literal at a whole millisecond. */
function instantMilliseconds(operand: PlanExpressionOperand): number | undefined {
  if (
    !isOperatorOperand(operand) ||
    operand.operator !== "timestamp" ||
    operand.operands.length !== 1
  ) {
    return undefined;
  }
  const [literal] = operand.operands;
  if (!isValueOperand(literal!) || typeof literal.value !== "string") return undefined;
  try {
    const { iso, subMillisecond } = parseRfc3339Instant(literal.value);
    return subMillisecond ? undefined : Date.parse(iso);
  } catch {
    return undefined;
  }
}

function instantLiteral(milliseconds: number): PlanExpressionOperand | undefined {
  if (
    !Number.isFinite(milliseconds) ||
    milliseconds < MIN_INSTANT ||
    milliseconds > MAX_INSTANT
  ) {
    return undefined;
  }
  return { operator: "timestamp", operands: [{ value: new Date(milliseconds).toISOString() }] };
}

function compareInstant(
  operator: string,
  column: PlanExpressionOperand,
  instant: PlanExpressionOperand
): PlanExpressionOperand {
  return { operator, operands: [column, instant] };
}
