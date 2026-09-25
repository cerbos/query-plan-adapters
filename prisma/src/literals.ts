// A plan-to-plan rewrite for collection macros over a LITERAL list — typically a folded principal
// attribute, which the planner leaves unrolled above ten elements. Each macro becomes the
// comparisons it performs, so no relation is involved and nothing has to be counted in the store.

import type { PlanExpressionOperand, Value } from "@cerbos/core";

import { isNamedOperand, isOperatorOperand, isValueOperand } from "./plan";
import type { NamedOperand, OperatorOperand } from "./plan";
import { substituteLambdaVariable } from "./rewrite";

/** Past this many elements, `exists`/`all` are left to the translator's own fold. */
const MAX_UNROLLED_ELEMENTS = 100;

/**
 * Rewrites, bottom-up:
 *
 * - `exists`/`all` over a literal list into the OR/AND of the body per element. CEL's macros are
 *   Kleene's disjunction and conjunction over the elements, so this is exact under any polarity.
 * - `exists_one`, `size(filter(...))` and `size(except(list, [x]))` over a literal list whose
 *   body is `t == x` or `t != x`, with `x` not mentioning `t`, into membership of `x`. Every
 *   element then compares against the same `x`: all of them error together when `x` is missing,
 *   and otherwise the count of matches is the multiplicity of `x` in the list, so the macro holds
 *   exactly for the values whose multiplicity satisfies it.
 * - `size(except(collection, literals))` against an emptiness threshold into `exists`/`all` over
 *   the collection, which the translator lowers as a relation.
 */
export function expandLiteralCollections(expr: PlanExpressionOperand): PlanExpressionOperand {
  if (!isOperatorOperand(expr)) return expr;
  const node: OperatorOperand = {
    operator: expr.operator,
    operands: expr.operands.map(expandLiteralCollections),
  };
  return expandMacro(node) ?? expandSizeComparison(node) ?? node;
}

function expandMacro(expr: OperatorOperand): PlanExpressionOperand | undefined {
  const { operator } = expr;
  if (operator !== "exists" && operator !== "all" && operator !== "exists_one") return undefined;
  const parts = literalMacro(expr);
  if (parts === undefined) return undefined;
  const { elements, body, variable } = parts;

  if (operator === "exists_one") {
    return countMembership(elements, body, variable, (count) => count === 1);
  }
  if (elements.length > MAX_UNROLLED_ELEMENTS) return undefined;
  if (elements.length === 0) return { value: operator === "all" };
  const substituted = elements.map((element) =>
    substituteLambdaVariable(body, variable.name, element)
  );
  if (substituted.length === 1) return substituted[0]!;
  return { operator: operator === "exists" ? "or" : "and", operands: substituted };
}

/** `size(filter(...)) CMP n`, `size(except(...)) CMP n`. */
function expandSizeComparison(expr: OperatorOperand): PlanExpressionOperand | undefined {
  const comparison = sizeComparison(expr);
  if (comparison === undefined) return undefined;
  const { counted, holds } = comparison;

  if (counted.operator === "filter") {
    const parts = literalMacro(counted);
    return parts && countMembership(parts.elements, parts.body, parts.variable, holds);
  }

  if (counted.operator !== "except" || counted.operands.length !== 2) return undefined;
  const [minuend, subtrahend] = counted.operands as [PlanExpressionOperand, PlanExpressionOperand];
  if (isOperatorOperand(subtrahend) && subtrahend.operator === "lambda") return undefined;

  // except(literals, [x]) keeps every element that is not x.
  const elements = literalList(minuend);
  if (elements !== undefined) {
    const removed = isOperatorOperand(subtrahend) && subtrahend.operator === "list"
      ? subtrahend.operands
      : undefined;
    if (removed?.length !== 1 || literalValue(removed[0]!) !== undefined) return undefined;
    const variable: NamedOperand = { name: "__except" };
    const body: PlanExpressionOperand = { operator: "ne", operands: [variable, removed[0]!] };
    return countMembership(elements, body, variable, holds);
  }

  // except(collection, literals) is non-empty exactly when some element is not among them.
  if (isNamedOperand(minuend) && literalList(subtrahend) !== undefined) {
    const nonEmpty = holds(1) && !holds(0);
    const empty = holds(0) && !holds(1);
    if (!nonEmpty && !empty) return undefined;
    // Only counts 0 and 1 are distinguished, so every count >= 1 must agree with 1.
    if ([2, 3].some((count) => holds(count) !== holds(1))) return undefined;
    const variable: NamedOperand = { name: "__except" };
    const member: PlanExpressionOperand = { operator: "in", operands: [variable, subtrahend] };
    return {
      operator: nonEmpty ? "exists" : "all",
      operands: [
        minuend,
        {
          operator: "lambda",
          operands: [nonEmpty ? { operator: "not", operands: [member] } : member, variable],
        },
      ],
    };
  }
  return undefined;
}

/**
 * The membership test a count over `t == x` (or `t != x`) reduces to, or undefined when the body
 * has another shape.
 */
function countMembership(
  elements: Value[],
  body: PlanExpressionOperand,
  variable: NamedOperand,
  holds: (count: number) => boolean
): PlanExpressionOperand | undefined {
  // Over no elements `x` is never evaluated, so its error cannot surface.
  if (elements.length === 0) return undefined;
  const comparison = uniformComparison(body, variable);
  if (comparison === undefined) return undefined;
  const { operator, path, other } = comparison;

  const projected: Value[] = [];
  for (const element of elements) {
    const value = project(element, path);
    // An element without the field errors on its own; a non-scalar compares structurally.
    if (value === undefined || (value !== null && typeof value === "object")) return undefined;
    projected.push(value);
  }
  const distinct = [...new Set(projected)];
  const multiplicity = (value: Value) => projected.filter((v) => v === value).length;
  const countFor = (matches: number) =>
    operator === "eq" ? matches : projected.length - matches;

  const accepted = distinct.filter((value) => holds(countFor(multiplicity(value))));
  const absentHolds = holds(countFor(0));
  // Decided for every x: only the error of a missing x is left, which an empty IN cannot carry
  // under negation.
  if (absentHolds ? accepted.length === distinct.length : accepted.length === 0) {
    return undefined;
  }
  const membership = (values: Value[]): PlanExpressionOperand =>
    values.length === 1
      ? { operator: "eq", operands: [other, { value: values[0]! }] }
      : { operator: "in", operands: [other, { value: values }] };
  if (!absentHolds) return membership(accepted);
  return {
    operator: "not",
    operands: [membership(distinct.filter((value) => !accepted.includes(value)))],
  };
}

/**
 * `t == x`, `t.path != x` and their mirrors, where `x` does not mention `t`: every element is
 * compared against the same `x`, and equality never raises an error of its own.
 */
function uniformComparison(
  body: PlanExpressionOperand,
  variable: NamedOperand
):
  | { operator: "eq" | "ne"; path: string[]; other: PlanExpressionOperand }
  | undefined {
  if (!isOperatorOperand(body) || (body.operator !== "eq" && body.operator !== "ne")) {
    return undefined;
  }
  if (body.operands.length !== 2) return undefined;
  for (const [element, other] of [
    [body.operands[0]!, body.operands[1]!],
    [body.operands[1]!, body.operands[0]!],
  ] as const) {
    if (!isNamedOperand(element) || mentions(other, variable.name)) continue;
    if (element.name === variable.name) return { operator: body.operator, path: [], other };
    if (element.name.startsWith(`${variable.name}.`)) {
      return {
        operator: body.operator,
        path: element.name.slice(variable.name.length + 1).split("."),
        other,
      };
    }
  }
  return undefined;
}

function mentions(expr: PlanExpressionOperand, name: string): boolean {
  if (isNamedOperand(expr)) return expr.name === name || expr.name.startsWith(`${name}.`);
  return isOperatorOperand(expr) && expr.operands.some((operand) => mentions(operand, name));
}

function project(element: Value, path: string[]): Value | undefined {
  let current: Value | undefined = element;
  for (const segment of path) {
    if (current === null || typeof current !== "object" || Array.isArray(current)) {
      return undefined;
    }
    current = (current as Record<string, Value>)[segment];
  }
  return current;
}

// -- reading the plan ----------------------------------------------------------------------------

/** `size(counted) CMP n`, with `holds` deciding the comparison for a count. */
function sizeComparison(
  expr: OperatorOperand
): { counted: OperatorOperand; holds: (count: number) => boolean } | undefined {
  const mirrored: Record<string, string> = { lt: "gt", gt: "lt", le: "ge", ge: "le" };
  if (!["eq", "ne", "lt", "le", "gt", "ge"].includes(expr.operator)) return undefined;
  if (expr.operands.length !== 2) return undefined;
  const [first, second] = expr.operands as [PlanExpressionOperand, PlanExpressionOperand];
  const [size, threshold, operator] = isValueOperand(first)
    ? [second, first, mirrored[expr.operator] ?? expr.operator]
    : [first, second, expr.operator];
  if (
    !isOperatorOperand(size) ||
    size.operator !== "size" ||
    size.operands.length !== 1 ||
    !isValueOperand(threshold) ||
    typeof threshold.value !== "number" ||
    !Number.isFinite(threshold.value)
  ) {
    return undefined;
  }
  const counted = size.operands[0]!;
  if (!isOperatorOperand(counted)) return undefined;
  const n = threshold.value;
  const holds = (count: number): boolean => {
    switch (operator) {
      case "eq":
        return count === n;
      case "ne":
        return count !== n;
      case "lt":
        return count < n;
      case "le":
        return count <= n;
      case "gt":
        return count > n;
      default:
        return count >= n;
    }
  };
  return { counted, holds };
}

/** The elements, lambda body and variable of `macro(literals, lambda(body, variable))`. */
function literalMacro(
  expr: OperatorOperand
): { elements: Value[]; body: PlanExpressionOperand; variable: NamedOperand } | undefined {
  const [collection, lambda] = expr.operands;
  if (
    expr.operands.length !== 2 ||
    collection === undefined ||
    lambda === undefined ||
    !isOperatorOperand(lambda) ||
    lambda.operator !== "lambda" ||
    lambda.operands.length !== 2
  ) {
    return undefined;
  }
  const elements = literalList(collection);
  const [body, variable] = lambda.operands as [PlanExpressionOperand, PlanExpressionOperand];
  if (elements === undefined || !isNamedOperand(variable)) return undefined;
  return { elements, body, variable };
}

/** A list literal: a value list, or a `list(...)` of literals. */
function literalList(operand: PlanExpressionOperand): Value[] | undefined {
  if (isValueOperand(operand)) {
    return Array.isArray(operand.value) ? operand.value : undefined;
  }
  if (isOperatorOperand(operand) && operand.operator === "list") {
    const elements: Value[] = [];
    for (const element of operand.operands) {
      const value = literalValue(element);
      if (value === undefined) return undefined;
      elements.push(value);
    }
    return elements;
  }
  return undefined;
}

/** A literal operand's value: a value, or a `struct(set-field(...), ...)` of literals. */
function literalValue(operand: PlanExpressionOperand): Value | undefined {
  if (isValueOperand(operand)) return operand.value;
  if (!isOperatorOperand(operand)) return undefined;
  if (operand.operator === "list") return literalList(operand);
  if (operand.operator !== "struct") return undefined;
  const fields: Record<string, Value> = {};
  for (const field of operand.operands) {
    if (!isOperatorOperand(field) || field.operator !== "set-field" || field.operands.length !== 2) {
      return undefined;
    }
    const [key, value] = field.operands as [PlanExpressionOperand, PlanExpressionOperand];
    const literal = literalValue(value);
    if (!isValueOperand(key) || typeof key.value !== "string" || literal === undefined) {
      return undefined;
    }
    fields[key.value] = literal;
  }
  return fields;
}
