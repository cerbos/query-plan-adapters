// Evaluating operators whose operands are all constants, with CEL's semantics.

import type { Value } from "@cerbos/core";
import { UnsupportedQueryPlanError } from "./errors";

export const ARITHMETIC_OPERATORS = new Set(["add", "sub", "mult", "div"]);

export function evaluateConstantComparison(
  operator: string,
  left: Value,
  right: Value
): boolean {
  switch (operator) {
    case "eq":
      return areValuesEqual(left, right);
    case "ne":
      return !areValuesEqual(left, right);
    case "lt":
    case "le":
    case "gt":
    case "ge": {
      if (
        typeof left === "number" &&
        typeof right === "number" &&
        (Number.isNaN(left) || Number.isNaN(right))
      ) {
        return false;
      }
      if (
        !(
          (typeof left === "number" && typeof right === "number") ||
          (typeof left === "string" && typeof right === "string")
        )
      ) {
        throw new UnsupportedQueryPlanError(
          `${operator} constant comparison requires two numbers or two strings`
        );
      }
      const order = compareConstantValues(left, right);
      switch (operator) {
        case "lt":
          return order < 0;
        case "le":
          return order <= 0;
        case "gt":
          return order > 0;
        case "ge":
          return order >= 0;
      }
    }
  }

  throw new UnsupportedQueryPlanError(`Unsupported constant comparison operator: ${operator}`);
}

/** Orders numbers numerically and strings by Unicode code point, as CEL does. */
function compareConstantValues(
  left: number | string,
  right: number | string
): number {
  if (typeof left === "number" && typeof right === "number") {
    return left === right ? 0 : left < right ? -1 : 1;
  }
  if (typeof left !== "string" || typeof right !== "string") {
    throw new UnsupportedQueryPlanError("Cannot order constant values of different types");
  }

  const leftCodePoints = Array.from(left, (character) =>
    character.codePointAt(0)
  );
  const rightCodePoints = Array.from(right, (character) =>
    character.codePointAt(0)
  );
  const sharedLength = Math.min(leftCodePoints.length, rightCodePoints.length);
  for (let index = 0; index < sharedLength; index++) {
    const leftCodePoint = leftCodePoints[index]!;
    const rightCodePoint = rightCodePoints[index]!;
    if (leftCodePoint !== rightCodePoint) {
      return leftCodePoint < rightCodePoint ? -1 : 1;
    }
  }
  return leftCodePoints.length === rightCodePoints.length
    ? 0
    : leftCodePoints.length < rightCodePoints.length
      ? -1
      : 1;
}

function areValuesEqual(left: Value, right: Value): boolean {
  if (Array.isArray(left)) {
    return (
      Array.isArray(right) &&
      left.length === right.length &&
      left.every((value, index) => areValuesEqual(value, right[index]!))
    );
  }

  if (Array.isArray(right)) {
    return false;
  }

  if (typeof left === "object" && left !== null) {
    if (typeof right !== "object" || right === null) {
      return false;
    }
    const leftKeys = Object.keys(left);
    const rightKeys = Object.keys(right);
    return (
      leftKeys.length === rightKeys.length &&
      leftKeys.every(
        (key) => key in right && areValuesEqual(left[key]!, right[key]!)
      )
    );
  }

  return left === right;
}

export function foldArithmetic(
  operator: string,
  left: Value,
  right: Value
): Value {
  if (
    operator === "add" &&
    (typeof left === "string" || typeof right === "string")
  ) {
    return String(left) + String(right);
  }
  if (typeof left !== "number" || typeof right !== "number") {
    throw new UnsupportedQueryPlanError(`${operator} operator requires string or number operands`);
  }
  switch (operator) {
    case "add":
      return left + right;
    case "sub":
      return left - right;
    case "mult":
      return left * right;
    case "div":
      return left / right;
    default:
      throw new UnsupportedQueryPlanError(`Unsupported operator: ${operator}`);
  }
}
