// Plan-to-plan rewrites of the CEL functions and constructors that have no Prisma filter of their
// own into the macros and comparisons the translator already lowers. Each rewrite is exact in CEL,
// errors included, so it holds under either polarity and needs no negation of its own.

import type { PlanExpressionOperand } from "@cerbos/core";

import { literalList, literalValue } from "./literals";
import { resolveFieldReference } from "./mapping";
import type { TranslationContext } from "./mapping";
import { isNamedOperand, isOperatorOperand, isValueOperand } from "./plan";
import type { NamedOperand, OperatorOperand } from "./plan";
import { substituteLambdaVariable } from "./rewrite";
import { presence } from "./types";

/** The iteration variable of a macro this module builds; no policy can spell the name. */
const ELEMENT: NamedOperand = { name: "__element" };

/** Rewrites, bottom-up, every shape the functions below recognise. */
export function rewriteFunctionCalls(
  expr: PlanExpressionOperand,
  context: TranslationContext
): PlanExpressionOperand {
  if (!isOperatorOperand(expr)) return expr;
  const node: OperatorOperand = {
    operator: expr.operator,
    operands: expr.operands.map((operand) => rewriteFunctionCalls(operand, context)),
  };
  return (
    memberRead(node, context) ??
    emptinessComparison(node) ??
    subsetTest(node) ??
    membershipOfDerivedList(node, context) ??
    intersectionWithMappedList(node) ??
    hoistTernaryFromConcatenation(node) ??
    macroOverConstructedList(node, context) ??
    node
  );
}

const not = (operand: PlanExpressionOperand): OperatorOperand => ({
  operator: "not",
  operands: [operand],
});

function macro(
  operator: "exists" | "all",
  collection: PlanExpressionOperand,
  body: PlanExpressionOperand,
  variable: NamedOperand
): OperatorOperand {
  return {
    operator,
    operands: [collection, { operator: "lambda", operands: [body, variable] }],
  };
}

/**
 * `x["key"]` is `x.key`: CEL reads a map member the same way through either spelling, and both
 * raise the same error on a missing key. The dotted name then resolves through the mapping as if
 * the policy had written it. A key holding a dot would be read as two segments, so it is left
 * alone, and so is an index into a list-valued mapping, where a string index is an error.
 */
function memberRead(
  node: OperatorOperand,
  context: TranslationContext
): PlanExpressionOperand | undefined {
  if (node.operator !== "index" || node.operands.length !== 2) return undefined;
  const [subject, key] = node.operands as [PlanExpressionOperand, PlanExpressionOperand];
  if (
    !isNamedOperand(subject) ||
    !isValueOperand(key) ||
    typeof key.value !== "string" ||
    key.value === "" ||
    key.value.includes(".")
  ) {
    return undefined;
  }
  const { relations } = resolveFieldReference(subject.name, context);
  if (relations?.some((relation) => relation.type === "many")) return undefined;
  return { name: `${subject.name}.${key.value}` };
}

/**
 * `c.except(L) == []` and `intersect(c, L) == []` (and `!=`), with `L` a literal list. The
 * difference is empty exactly when every element of `c` is in `L`, and the intersection exactly
 * when none is. Cerbos keeps an element by `L`'s membership test, which never raises an error, so
 * the macros below decide every row as the list functions do.
 */
function emptinessComparison(node: OperatorOperand): PlanExpressionOperand | undefined {
  if ((node.operator !== "eq" && node.operator !== "ne") || node.operands.length !== 2) {
    return undefined;
  }
  const [first, second] = node.operands as [PlanExpressionOperand, PlanExpressionOperand];
  const [call, other] = isListFunction(first) ? [first, second] : [second, first];
  if (!isListFunction(call) || literalList(other)?.length !== 0) return undefined;

  let [collection, literals] = call.operands as [PlanExpressionOperand, PlanExpressionOperand];
  // intersect() is symmetric in which elements it shares, so emptiness is too.
  if (call.operator === "intersect" && !isNamedOperand(collection)) {
    [collection, literals] = [literals, collection];
  }
  const values = literalList(literals);
  if (!isNamedOperand(collection) || values === undefined) return undefined;

  const member: PlanExpressionOperand = { operator: "in", operands: [ELEMENT, { value: values }] };
  const isEmpty = node.operator === "eq";
  if (call.operator === "except") {
    return isEmpty
      ? macro("all", collection, member, ELEMENT)
      : macro("exists", collection, not(member), ELEMENT);
  }
  const shared = macro("exists", collection, member, ELEMENT);
  return isEmpty ? not(shared) : shared;
}

function isListFunction(operand: PlanExpressionOperand): operand is OperatorOperand {
  return (
    isOperatorOperand(operand) &&
    (operand.operator === "except" || operand.operator === "intersect") &&
    operand.operands.length === 2
  );
}

/** `a.isSubset(b)` holds exactly when every element of `a` is in `b`. */
function subsetTest(node: OperatorOperand): PlanExpressionOperand | undefined {
  if (node.operator !== "isSubset" || node.operands.length !== 2) return undefined;
  const [subset, superset] = node.operands as [PlanExpressionOperand, PlanExpressionOperand];
  const subsetValues = literalList(subset);
  const supersetValues = literalList(superset);
  if (isNamedOperand(subset) && supersetValues !== undefined) {
    return macro(
      "all",
      subset,
      { operator: "in", operands: [ELEMENT, { value: supersetValues }] },
      ELEMENT
    );
  }
  if (subsetValues !== undefined && isNamedOperand(superset)) {
    return macro(
      "all",
      { value: subsetValues },
      { operator: "in", operands: [ELEMENT, superset] },
      ELEMENT
    );
  }
  return undefined;
}

/**
 * `x in c.map(v, F)` and `x in c.filter(v, P)`, with `x` a literal. The derived list holds `x`
 * exactly when some element maps to `x` (or is `x` and passes `P`), but building it raises an
 * error as soon as ONE element's `F` (or `P`) does, which no witness absorbs. The second macro
 * carries that error: `F == x || !(F == x)` is true for every element whose `F` evaluates, and an
 * error otherwise, so `all` over it holds exactly when the list can be built.
 */
function membershipOfDerivedList(
  node: OperatorOperand,
  context: TranslationContext
): PlanExpressionOperand | undefined {
  if (node.operator !== "in" || node.operands.length !== 2) return undefined;
  const [member, list] = node.operands as [PlanExpressionOperand, PlanExpressionOperand];
  if (
    !isValueOperand(member) ||
    !isOperatorOperand(list) ||
    (list.operator !== "map" && list.operator !== "filter") ||
    list.operands.length !== 2
  ) {
    return undefined;
  }
  const [collection, lambda] = list.operands as [PlanExpressionOperand, PlanExpressionOperand];
  if (
    !isNamedOperand(collection) ||
    !isOperatorOperand(lambda) ||
    lambda.operator !== "lambda" ||
    lambda.operands.length !== 2
  ) {
    return undefined;
  }
  const [body, variable] = lambda.operands as [PlanExpressionOperand, PlanExpressionOperand];
  if (!isNamedOperand(variable)) return undefined;

  const evaluates = (predicate: PlanExpressionOperand): OperatorOperand => ({
    operator: "or",
    operands: [predicate, not(predicate)],
  });
  if (list.operator === "map") {
    const matches: PlanExpressionOperand = { operator: "eq", operands: [body, member] };
    return {
      operator: "and",
      operands: [
        macro("exists", collection, matches, variable),
        macro("all", collection, evaluates(matches), variable),
      ],
    };
  }
  // filter() keeps the element itself, which only a projected scalar list can compare with a
  // literal; the element of a list of related rows is an object.
  const { relations } = resolveFieldReference(collection.name, context);
  if (relations?.[relations.length - 1]?.field === undefined) return undefined;
  return {
    operator: "and",
    operands: [
      macro(
        "exists",
        collection,
        { operator: "and", operands: [{ operator: "eq", operands: [variable, member] }, body] },
        variable
      ),
      macro("all", collection, evaluates(body), variable),
    ],
  };
}

/**
 * `hasIntersection(c.map(v, F), L)` (either order), with `L` a literal list: some element maps
 * into `L`, and, as for membership above, building the mapped list raises an error as soon as one
 * element's `F` does. Spelled as `exists` plus an `all` that carries that error, the negation
 * pushes down through both macros, so a row holding an element whose `F` errors stays denied
 * under `!` instead of the guard's own negation selecting it.
 */
function intersectionWithMappedList(node: OperatorOperand): PlanExpressionOperand | undefined {
  if (node.operator !== "hasIntersection" || node.operands.length !== 2) return undefined;
  const [first, second] = node.operands as [PlanExpressionOperand, PlanExpressionOperand];
  const [mapped, literals] =
    isOperatorOperand(first) && first.operator === "map" ? [first, second] : [second, first];
  const values = literalList(literals);
  if (
    values === undefined ||
    !isOperatorOperand(mapped) ||
    mapped.operator !== "map" ||
    mapped.operands.length !== 2
  ) {
    return undefined;
  }
  const [collection, lambda] = mapped.operands as [PlanExpressionOperand, PlanExpressionOperand];
  if (
    !isNamedOperand(collection) ||
    !isOperatorOperand(lambda) ||
    lambda.operator !== "lambda" ||
    lambda.operands.length !== 2
  ) {
    return undefined;
  }
  const [projection, variable] = lambda.operands as [PlanExpressionOperand, PlanExpressionOperand];
  if (!isNamedOperand(variable)) return undefined;
  const shared: PlanExpressionOperand = {
    operator: "in",
    operands: [projection, { value: values }],
  };
  return {
    operator: "and",
    operands: [
      macro("exists", collection, shared, variable),
      macro("all", collection, { operator: "or", operands: [shared, not(shared)] }, variable),
    ],
  };
}

/**
 * `L1 + (c ? A : B)` (and the mirror), with every list a literal, is `c ? L1 + A : L1 + B`: the
 * ternary evaluates only the branch it selects, and concatenating literals raises no error, so
 * the condition's error is the only one either spelling can raise. The constant folder then
 * concatenates each branch, leaving a ternary over two literal lists.
 */
function hoistTernaryFromConcatenation(
  node: OperatorOperand
): PlanExpressionOperand | undefined {
  if (node.operator !== "add" || node.operands.length !== 2) return undefined;
  const index = node.operands.findIndex(
    (operand) => isOperatorOperand(operand) && operand.operator === "if"
  );
  if (index === -1) return undefined;
  const ternary = node.operands[index] as OperatorOperand;
  const other = node.operands[1 - index]!;
  if (ternary.operands.length !== 3) return undefined;
  const [condition, thenBranch, elseBranch] = ternary.operands as [
    PlanExpressionOperand,
    PlanExpressionOperand,
    PlanExpressionOperand,
  ];
  const lists = [other, thenBranch, elseBranch].map(constantList);
  if (lists.some((list) => list === undefined)) return undefined;
  const [otherList, thenList, elseList] = lists as [
    PlanExpressionOperand,
    PlanExpressionOperand,
    PlanExpressionOperand,
  ];
  const place = (branch: PlanExpressionOperand): OperatorOperand => ({
    operator: "add",
    operands: index === 0 ? [branch, otherList] : [otherList, branch],
  });
  return { operator: "if", operands: [condition, place(thenList), place(elseList)] };
}

/** A literal list as a value operand, or a concatenation of literal lists left for the folder. */
function constantList(operand: PlanExpressionOperand): PlanExpressionOperand | undefined {
  const literal = literalList(operand);
  if (literal !== undefined) return { value: literal };
  if (
    isOperatorOperand(operand) &&
    operand.operator === "add" &&
    operand.operands.length === 2 &&
    operand.operands.every((part) => constantList(part) !== undefined)
  ) {
    return { operator: "add", operands: operand.operands.map((part) => constantList(part)!) };
  }
  return undefined;
}

/**
 * `[a, b].exists(v, P)` and `.all(v, P)` over a list BUILT from attributes: the OR (AND) of `P`
 * per element, as CEL's macros are Kleene's disjunction (conjunction) over the elements. Building
 * the list raises an error when an element is a missing attribute, even where another element
 * would decide the macro, so each such element's presence test is ANDed on, and ORed into the
 * body negated: FALSE for a present value and UNKNOWN for a NULL one, so a missing element leaves
 * the whole rewrite UNKNOWN under both polarities. Only elements that are literals or plain
 * columns are rewritten.
 */
function macroOverConstructedList(
  node: OperatorOperand,
  context: TranslationContext
): PlanExpressionOperand | undefined {
  if ((node.operator !== "exists" && node.operator !== "all") || node.operands.length !== 2) {
    return undefined;
  }
  const [list, lambda] = node.operands as [PlanExpressionOperand, PlanExpressionOperand];
  if (!isOperatorOperand(list) || list.operator !== "list" || literalList(list) !== undefined) {
    return undefined;
  }
  if (!isOperatorOperand(lambda) || lambda.operator !== "lambda" || lambda.operands.length !== 2) {
    return undefined;
  }
  const [body, variable] = lambda.operands as [PlanExpressionOperand, PlanExpressionOperand];
  if (!isNamedOperand(variable)) return undefined;

  const presences: PlanExpressionOperand[] = [];
  for (const element of list.operands) {
    if (literalValue(element) !== undefined) continue;
    if (!isNamedOperand(element)) return undefined;
    const fieldRef = resolveFieldReference(element.name, context);
    if ((fieldRef.relations?.length ?? 0) > 0) return undefined;
    // An explicit null is a value, which builds the list like any other.
    const convention = fieldRef.nullAttributeRepresentation ?? context.nullRepresentation;
    if (convention === "explicit") continue;
    const test = presence(element, fieldRef);
    if (test === undefined) return undefined;
    presences.push(test);
  }

  const bodies: PlanExpressionOperand[] = [];
  for (const element of list.operands) {
    const substituted = substituteElement(body, variable.name, element);
    if (substituted === undefined) return undefined;
    bodies.push(substituted);
  }
  const core: PlanExpressionOperand =
    bodies.length === 1
      ? bodies[0]!
      : { operator: node.operator === "exists" ? "or" : "and", operands: bodies };
  if (presences.length === 0) return core;
  return {
    operator: "and",
    operands: [...presences, { operator: "or", operands: [core, ...presences.map(not)] }],
  };
}

/**
 * `expr` with the lambda variable `name` replaced by `element`, a literal or a column; undefined
 * when a nested lambda rebinds the name.
 */
function substituteElement(
  expr: PlanExpressionOperand,
  name: string,
  element: PlanExpressionOperand
): PlanExpressionOperand | undefined {
  if (isNamedOperand(expr)) {
    if (expr.name !== name && !expr.name.startsWith(`${name}.`)) return expr;
    if (isNamedOperand(element)) return { name: element.name + expr.name.slice(name.length) };
    const value = literalValue(element);
    return value === undefined ? undefined : substituteLambdaVariable(expr, name, value);
  }
  if (!isOperatorOperand(expr)) return expr;
  if (
    expr.operator === "lambda" &&
    expr.operands.slice(1).some((bound) => isNamedOperand(bound) && bound.name === name)
  ) {
    return undefined;
  }
  const operands: PlanExpressionOperand[] = [];
  for (const operand of expr.operands) {
    const substituted = substituteElement(operand, name, element);
    if (substituted === undefined) return undefined;
    operands.push(substituted);
  }
  return { operator: expr.operator, operands };
}
