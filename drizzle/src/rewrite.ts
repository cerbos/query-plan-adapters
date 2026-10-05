import type { PlanExpressionOperand, Value } from "@cerbos/core";

import { getMappingEntry } from "./mapper";
import {
  isExpressionOperand,
  isNameOperand,
  isOperatorCall,
  isValueOperand,
} from "./operands";
import type { ExpressionOperand } from "./operands";
import type { Mapper } from "./types";

/**
 * Plan-to-plan rewrites into shapes the translators already handle, each an identity under CEL's
 * own semantics, errors included. A node that does not match a rewrite exactly is left as it is,
 * so the translators still see — and refuse — every shape no rewrite claims.
 */

const call = (operator: string, ...operands: PlanExpressionOperand[]): ExpressionOperand => ({
  operator,
  operands,
});

const value = (constant: Value): PlanExpressionOperand => ({ value: constant });

/** A fresh lambda variable: undotted, and never a name the planner emits. */
const ELEMENT = "__cerbos_element";

/** A scalar constant: a string, number or boolean (not null, a list or a map). */
const isScalar = (constant: Value): constant is string | number | boolean =>
  typeof constant === "string" || typeof constant === "number" || typeof constant === "boolean";

const isScalarList = (constant: Value): constant is (string | number | boolean)[] =>
  Array.isArray(constant) && constant.every(isScalar);

// -- constant evaluation ---------------------------------------------------------------------------

/** CEL equality over the scalar lists and scalars `evaluateConstant` produces. */
const constantEquals = (left: Value, right: Value): boolean => {
  if (Array.isArray(left) && Array.isArray(right)) {
    return left.length === right.length && left.every((element, i) => constantEquals(element, right[i]!));
  }
  return isScalar(left) && isScalar(right) && typeof left === typeof right && left === right;
};

/**
 * Evaluate an operand built only from constants and list concatenation, membership and equality,
 * or `undefined` where it holds anything else — including any type CEL would reject, so a
 * constant this returns is exactly what CEL computes.
 */
const evaluateConstant = (operand: PlanExpressionOperand): Value | undefined => {
  if (isValueOperand(operand)) {
    return isScalar(operand.value) || isScalarList(operand.value) ? operand.value : undefined;
  }
  if (!isExpressionOperand(operand) || operand.operands.length !== 2) return undefined;
  const [left, right] = operand.operands.map(evaluateConstant);
  if (left === undefined || right === undefined) return undefined;
  switch (operand.operator) {
    case "add":
      return Array.isArray(left) && Array.isArray(right) ? [...left, ...right] : undefined;
    case "eq":
    case "ne": {
      // CEL compares only values of one kind here: a list with a list, a scalar with a scalar of
      // the same type. Anything else is left to the translators.
      if (Array.isArray(left) !== Array.isArray(right)) return undefined;
      if (!Array.isArray(left) && typeof left !== typeof right) return undefined;
      return constantEquals(left, right) === (operand.operator === "eq");
    }
    case "in":
      if (!isScalar(left) || !Array.isArray(right)) return undefined;
      if (right.some((element) => typeof element !== typeof left)) return undefined;
      return right.some((element) => constantEquals(left, element));
    default:
      return undefined;
  }
};

/** The first ternary reachable from `operand` through list concatenation alone. */
const findLiftableTernary = (
  operand: PlanExpressionOperand,
): ExpressionOperand | undefined => {
  if (isOperatorCall(operand, "if")) return operand;
  if (isOperatorCall(operand, "add")) {
    for (const child of operand.operands) {
      const found = findLiftableTernary(child);
      if (found) return found;
    }
  }
  return undefined;
};

const replaceNode = (
  operand: PlanExpressionOperand,
  target: PlanExpressionOperand,
  replacement: PlanExpressionOperand,
): PlanExpressionOperand => {
  if (operand === target) return replacement;
  if (!isExpressionOperand(operand)) return operand;
  return call(
    operand.operator,
    ...operand.operands.map((child) => replaceNode(child, target, replacement)),
  );
};

/**
 * `["a"] + (c ? ["b"] : []) + ["c"] == [...]`, or `"x" in (c ? ["x"] : [])`: a comparison whose
 * every leaf but one ternary's condition is constant. It is lifted to `c ? <then> : <else>`, each
 * branch folded to the boolean CEL computes, which the translators read as a ternary in condition
 * position — UNKNOWN when `c` is, so a missing condition attribute still denies.
 */
const liftConstantTernary = (node: ExpressionOperand): PlanExpressionOperand => {
  if (!["eq", "ne", "in"].includes(node.operator) || node.operands.length !== 2) return node;
  for (const side of node.operands) {
    const ternary = findLiftableTernary(side);
    if (!ternary || ternary.operands.length !== 3) continue;
    const [condition, thenOperand, elseOperand] = ternary.operands as [
      PlanExpressionOperand,
      PlanExpressionOperand,
      PlanExpressionOperand,
    ];
    const thenValue = evaluateConstant(replaceNode(node, ternary, thenOperand));
    const elseValue = evaluateConstant(replaceNode(node, ternary, elseOperand));
    if (typeof thenValue === "boolean" && typeof elseValue === "boolean") {
      return call("if", condition, value(thenValue), value(elseValue));
    }
  }
  return node;
};

// -- list functions compared with an empty list ---------------------------------------------------

/**
 * `L.except(K) == []` is `L.all(e, e in K)`, `intersect(L, K) == []` is `L.all(e, !(e in K))`,
 * and `L.isSubset(K)` is `L.all(e, e in K)`: each is true exactly when every element of `L` passes,
 * is true over an empty `L`, and errors exactly where reading `L` does. `!=` is the negation.
 */
const rewriteListFunction = (node: ExpressionOperand): PlanExpressionOperand => {
  const membershipOver = (
    list: PlanExpressionOperand,
    constants: (string | number | boolean)[],
    negate: boolean,
  ) => {
    // An or of equalities, not `in`: CEL's `==` is false for a null element, never an error.
    const body =
      constants.length === 0
        ? value(false)
        : call("or", ...constants.map((constant) => call("eq", { name: ELEMENT }, value(constant))));
    return call("all", list, call("lambda", negate ? call("not", body) : body, { name: ELEMENT }));
  };
  const listFunctionAll = (operand: PlanExpressionOperand): PlanExpressionOperand | undefined => {
    if (
      (isOperatorCall(operand, "except") || isOperatorCall(operand, "intersect")) &&
      operand.operands.length === 2
    ) {
      const [list, constants] = operand.operands as [PlanExpressionOperand, PlanExpressionOperand];
      if (isNameOperand(list) && isValueOperand(constants) && isScalarList(constants.value)) {
        return membershipOver(list, constants.value, operand.operator === "intersect");
      }
    }
    return undefined;
  };

  if (node.operator === "isSubset" && node.operands.length === 2) {
    const [list, constants] = node.operands as [PlanExpressionOperand, PlanExpressionOperand];
    if (isNameOperand(list) && isValueOperand(constants) && isScalarList(constants.value)) {
      return membershipOver(list, constants.value, false);
    }
  }
  if ((node.operator === "eq" || node.operator === "ne") && node.operands.length === 2) {
    const [left, right] = node.operands as [PlanExpressionOperand, PlanExpressionOperand];
    const isEmptyList = (operand: PlanExpressionOperand) =>
      isValueOperand(operand) && Array.isArray(operand.value) && operand.value.length === 0;
    const rewritten = isEmptyList(right)
      ? listFunctionAll(left)
      : isEmptyList(left)
        ? listFunctionAll(right)
        : undefined;
    if (rewritten) return node.operator === "eq" ? rewritten : call("not", rewritten);
  }
  return node;
};

// -- membership in a derived list -----------------------------------------------------------------

/**
 * `"x" in L.map(t, t.f)` is `hasIntersection(L.map(t, t.f), ["x"])`: both build the whole list
 * first, so an element the projection errors on is an error either way. `x in L + K` is `x in L || x in K`, except that CEL reads `L` even when `x` is
 * in `K`: the `size(L) >= 0` conjunct is UNKNOWN exactly where reading `L` errors.
 */
const rewriteMembership = (node: ExpressionOperand): PlanExpressionOperand => {
  if (node.operator !== "in" || node.operands.length !== 2) return node;
  const [element, collection] = node.operands as [PlanExpressionOperand, PlanExpressionOperand];
  if (isValueOperand(element) && isScalar(element.value) && isOperatorCall(collection, "map")) {
    const [, lambda] = collection.operands;
    // Only a field projection: hasIntersection over map() reads nothing else. Any other
    // projection, and filter(), are translated where `in` is (`filter.ts`).
    if (lambda && isOperatorCall(lambda, "lambda") && lambda.operands.every(isNameOperand)) {
      return call("hasIntersection", collection, value([element.value]));
    }
  }
  if (isOperatorCall(collection, "add") && collection.operands.length === 2) {
    const [list, constants] = collection.operands as [PlanExpressionOperand, PlanExpressionOperand];
    if (isNameOperand(list) && isValueOperand(constants) && isScalarList(constants.value)) {
      return call(
        "or",
        call("in", element, list),
        call(
          "and",
          call("in", element, constants),
          call("ge", call("size", list), value(0)),
        ),
      );
    }
  }
  return node;
};

// -- map access by a constant key -------------------------------------------------------------------

/**
 * `R.attr.obj["inner"]` is `R.attr.obj.inner`: indexing a map by a string key and selecting the
 * field of that name both read the same entry and both error when it is absent. Rewritten only
 * where the dotted reference is mapped directly and the receiver itself is not, so a list or a
 * relation is never read as a map.
 */
const rewriteConstantKeyIndex = (node: ExpressionOperand, mapper: Mapper): PlanExpressionOperand => {
  if (node.operator !== "index" || node.operands.length !== 2) return node;
  const [receiver, key] = node.operands as [PlanExpressionOperand, PlanExpressionOperand];
  if (!isNameOperand(receiver) || !isValueOperand(key) || typeof key.value !== "string") return node;
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key.value)) return node;
  const dotted = `${receiver.name}.${key.value}`;
  if (getMappingEntry(receiver.name, mapper) !== undefined) return node;
  return getMappingEntry(dotted, mapper) !== undefined ? { name: dotted } : node;
};

// -- timestamps shifted by a constant duration -------------------------------------------------------

/** A duration literal the planner prints, in milliseconds: `86400s`, `1.5s`. */
const durationMilliseconds = (operand: PlanExpressionOperand): number | undefined => {
  if (!isOperatorCall(operand, "duration") || operand.operands.length !== 1) return undefined;
  const [literal] = operand.operands;
  if (!literal || !isValueOperand(literal) || typeof literal.value !== "string") return undefined;
  const match = /^(-?)(\d+)(?:\.(\d{1,3}))?s$/.exec(literal.value);
  if (!match) return undefined;
  const milliseconds = Number(match[2]) * 1000 + Number((match[3] ?? "").padEnd(3, "0"));
  return match[1] === "-" ? -milliseconds : milliseconds;
};

/** A `timestamp("...")` literal, at millisecond precision, in epoch milliseconds. */
const timestampLiteralMilliseconds = (operand: PlanExpressionOperand): number | undefined => {
  if (!isOperatorCall(operand, "timestamp") || operand.operands.length !== 1) return undefined;
  const [literal] = operand.operands;
  if (!literal || !isValueOperand(literal) || typeof literal.value !== "string") return undefined;
  const fraction = /\.(\d+)/.exec(literal.value)?.[1] ?? "";
  if ([...fraction.slice(3)].some((digit) => digit !== "0")) return undefined;
  const milliseconds = Date.parse(literal.value);
  return Number.isNaN(milliseconds) ? undefined : milliseconds;
};

const timestampLiteral = (milliseconds: number): PlanExpressionOperand =>
  call("timestamp", value(new Date(milliseconds).toISOString()));

const MIRRORED: Record<string, string> = { lt: "gt", le: "ge", gt: "lt", ge: "le", eq: "eq", ne: "ne" };

/** `timestamp(x) + d`, `d + timestamp(x)` or `timestamp(x) - d`, as the timestamp and the shift. */
const shiftedTimestamp = (
  operand: PlanExpressionOperand,
): { timestamp: PlanExpressionOperand; shift: number } | undefined => {
  if (!isExpressionOperand(operand) || operand.operands.length !== 2) return undefined;
  const [left, right] = operand.operands as [PlanExpressionOperand, PlanExpressionOperand];
  const isConversion = (candidate: PlanExpressionOperand) =>
    isOperatorCall(candidate, "timestamp") && timestampLiteralMilliseconds(candidate) === undefined;
  if (operand.operator === "add") {
    const rightShift = durationMilliseconds(right);
    if (isConversion(left) && rightShift !== undefined) return { timestamp: left, shift: rightShift };
    const leftShift = durationMilliseconds(left);
    if (isConversion(right) && leftShift !== undefined) return { timestamp: right, shift: leftShift };
  }
  if (operand.operator === "sub") {
    const shift = durationMilliseconds(right);
    if (isConversion(left) && shift !== undefined) return { timestamp: left, shift: -shift };
  }
  return undefined;
};

/**
 * `timestamp(x) + d op timestamp(T)` is `timestamp(x) op timestamp(T - d)`, and
 * `timestamp(x).timeSince() op d` is `timestamp(x) mirror(op) timestamp(now - d)`, with `now` read
 * when the plan is translated: the comparison against a constant is the only temporal shape the
 * translators lower, and moving a constant shift across it is exact.
 */
const rewriteShiftedTimestamp = (node: ExpressionOperand): PlanExpressionOperand => {
  if (!(node.operator in MIRRORED) || node.operands.length !== 2) return node;
  const [left, right] = node.operands as [PlanExpressionOperand, PlanExpressionOperand];
  for (const [side, other, operator] of [
    [left, right, node.operator],
    [right, left, MIRRORED[node.operator]!],
  ] as const) {
    const shifted = shiftedTimestamp(side);
    const bound = timestampLiteralMilliseconds(other);
    if (shifted && bound !== undefined) {
      return call(operator, shifted.timestamp, timestampLiteral(bound - shifted.shift));
    }
    if (isOperatorCall(side, "timeSince") && side.operands.length === 1) {
      const [conversion] = side.operands as [PlanExpressionOperand];
      const window = durationMilliseconds(other);
      if (isOperatorCall(conversion, "timestamp") && window !== undefined) {
        return call(MIRRORED[operator]!, conversion, timestampLiteral(Date.now() - window));
      }
    }
  }
  return node;
};

// -- the pass ----------------------------------------------------------------------------------------

/** Apply every rewrite, bottom up. */
export const rewritePlan = (
  operand: PlanExpressionOperand,
  mapper: Mapper,
): PlanExpressionOperand => {
  if (!isExpressionOperand(operand)) return operand;
  let node: PlanExpressionOperand = call(
    operand.operator,
    ...operand.operands.map((child) => rewritePlan(child, mapper)),
  );
  for (const rewrite of [
    (candidate: ExpressionOperand) => rewriteConstantKeyIndex(candidate, mapper),
    liftConstantTernary,
    rewriteListFunction,
    rewriteMembership,
    rewriteShiftedTimestamp,
  ]) {
    if (!isExpressionOperand(node)) break;
    node = rewrite(node);
  }
  return node;
};
