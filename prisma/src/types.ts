// A plan-to-plan rewrite that settles comparisons whose outcome the mapped column types already
// decide: CEL's heterogeneous equality, and the no-overload errors a mistyped operand raises.
// Runs before constant folding, so every constant it produces folds away like any other.

import type { PlanExpressionOperand, Value } from "@cerbos/core";

import { enterLambdaScope, lookupMapping, resolveFieldReference } from "./mapping";
import type { ResolvedFieldReference, TranslationContext } from "./mapping";
import type { MapperConfig } from "./index";
import { COMPARISON_OPERATORS, isNamedOperand, isOperatorOperand, isValueOperand } from "./plan";
import type { NamedOperand, OperatorOperand } from "./plan";

/**
 * The CEL type an operand is known to have. Numbers are one family: CEL's heterogeneous equality
 * compares an int, a uint and a double numerically, so `1 == 1.0` is true across them.
 */
type CelType = "string" | "number" | "boolean" | "null" | "list" | "map";

/**
 * The values one leaf can take across the rows. `error` is a CEL evaluation error, which Cerbos
 * denies: a no-overload call, or a missing attribute (an omitted NULL column, an absent to-one
 * parent) reached before the comparison could decide.
 */
type Outcomes = { true?: boolean; false?: boolean; error?: boolean };

const ORDERING_OPERATORS = new Set(["lt", "le", "gt", "ge"]);
const STRING_OPERATORS = new Set(["contains", "startsWith", "endsWith"]);
const HIERARCHY_OPERATORS = new Set(["overlaps", "ancestorOf", "descendentOf"]);
const LIST_OPERATORS = new Set(["filter", "map", "except"]);

/**
 * Replaces every leaf whose outcome the mapped value types settle with the constant that decides
 * the same rows, then leaves the rest of the plan to the translator.
 *
 * CEL's `&&`, `||` and `!` are Kleene's strong three-valued logic with an error in the role of
 * UNKNOWN, and `exists`/`all` are its disjunction and conjunction over the elements. Kleene logic
 * is regular — a formula that is TRUE with an UNKNOWN operand is TRUE whatever that operand is —
 * so under an even number of negations a leaf that is only ever FALSE or an error decides every
 * row exactly as a constant FALSE does, and under an odd number a leaf that is only ever TRUE or
 * an error decides every row exactly as a constant TRUE does. A leaf that can be both TRUE and
 * FALSE, or that sits where its polarity is not fixed (a ternary condition, an operand of `==`,
 * the body of `exists_one`), is left for the translator to lower or refuse.
 */
export function settleTypeMismatches(
  expr: PlanExpressionOperand,
  context: TranslationContext,
  positive = true
): PlanExpressionOperand {
  if (!isOperatorOperand(expr)) return expr;

  switch (expr.operator) {
    case "and":
    case "or":
      return {
        operator: expr.operator,
        operands: expr.operands.map((operand) =>
          settleTypeMismatches(operand, context, positive)
        ),
      };
    case "not":
      return {
        operator: "not",
        operands: expr.operands.map((operand) =>
          settleTypeMismatches(operand, context, !positive)
        ),
      };
    case "exists":
    case "all":
      return settleLambdaBody(expr, context, positive);
  }

  const cast =
    rewriteCastComparison(expr, context, positive) ??
    rewriteConcatenation(expr, context) ??
    splitDivision(expr, context);
  if (cast !== undefined) return settleTypeMismatches(cast, context, positive);

  const outcomes = leafOutcomes(expr, context);
  if (outcomes === undefined) return rewriteLiteralLists(expr, context);
  if (positive && !outcomes.true) return { value: false };
  if (!positive && !outcomes.false) return { value: true };
  if (!outcomes.error && !(outcomes.true && outcomes.false)) {
    return { value: outcomes.true === true };
  }
  return expr;
}

/** `exists(c, P)` and `all(c, P)` are monotone in P, so the body keeps the macro's polarity. */
function settleLambdaBody(
  expr: OperatorOperand,
  context: TranslationContext,
  positive: boolean
): PlanExpressionOperand {
  const [collection, lambda] = expr.operands;
  if (
    collection === undefined ||
    !isNamedOperand(collection) ||
    lambda === undefined ||
    !isOperatorOperand(lambda) ||
    lambda.operator !== "lambda"
  ) {
    return expr;
  }
  const [body, variable] = lambda.operands;
  if (body === undefined || variable === undefined || !isNamedOperand(variable)) {
    return expr;
  }
  const scoped = scopedContext(context, collection.name, variable.name);
  if (scoped === undefined) return expr;
  return {
    operator: expr.operator,
    operands: [
      collection,
      { operator: "lambda", operands: [settleTypeMismatches(body, scoped, positive), variable] },
    ],
  };
}

/** The context a lambda body over `collectionName` resolves its element references in. */
function scopedContext(
  context: TranslationContext,
  collectionName: string,
  variableName: string
): TranslationContext | undefined {
  const { relations } = resolveFieldReference(collectionName, context);
  if (!relations || relations.length === 0) return undefined;
  return enterLambdaScope(context, collectionName, {
    variableName,
    relationModel: relations[relations.length - 1]!.model,
    nullableFields: new Set(),
    unknownFilters: [],
  });
}

// -- casts ---------------------------------------------------------------------------------------

/**
 * `string(column) == "lit"` and `int(column) == k` (and their `!=` forms), solved for the column
 * the cast wraps: a Prisma filter has no cast, but these conversions are invertible.
 *
 * - `string(b)` of a boolean is `"true"` or `"false"`.
 * - `string(d)` of a number is cel-go's shortest `%g` rendering, so exactly one double prints as a
 *   given literal — or none does, and the comparison is settled like any type mismatch.
 * - `int(d)` truncates toward zero, and raises an error outside (-2^63, 2^63). A threshold is a
 *   half-open interval that would need that bound spelled out, which an `Int` column rejects, so
 *   only `==` outside a `!` and `!=` under one are translated: the interval `==` solves to already
 *   excludes every value that overflows, and the error then decides the row as the constant does.
 *
 * A column the caller sends as an explicit null is left alone: the cast of a null VALUE is an
 * error, where the comparison it would be rewritten to is definite.
 */
function rewriteCastComparison(
  expr: OperatorOperand,
  context: TranslationContext,
  positive: boolean
): PlanExpressionOperand | undefined {
  const { operator } = expr;
  if ((operator !== "eq" && operator !== "ne") || expr.operands.length !== 2) return undefined;
  const [first, second] = expr.operands as [PlanExpressionOperand, PlanExpressionOperand];
  const [cast, literal] = isOperatorOperand(first) ? [first, second] : [second, first];
  if (
    !isOperatorOperand(cast) ||
    cast.operands.length !== 1 ||
    !isValueOperand(literal) ||
    literal.value === null
  ) {
    return undefined;
  }
  const column = cast.operands[0]!;
  if (!isNamedOperand(column)) return undefined;
  const fieldRef = resolveFieldReference(column.name, context);
  if (fieldRef.relations?.length || fieldRef.nullAttributeRepresentation === "explicit") {
    return undefined;
  }
  const type = scalarType(fieldRef);
  const value = literal.value;
  const compare = (op: string, v: Value): PlanExpressionOperand => ({
    operator: op,
    operands: [column, { value: v }],
  });
  const equality = (v: Value): PlanExpressionOperand => compare(operator, v);
  // No value of the column prints as the literal: the comparison is settled by its type alone.
  const unmatched: PlanExpressionOperand = {
    operator,
    operands: [column, { operator: "struct", operands: [] }],
  };

  if (cast.operator === "string" && typeof value === "string") {
    if (type === "boolean") {
      return value === "true" || value === "false" ? equality(value === "true") : unmatched;
    }
    if (type === "number") {
      const parsed = Number(value);
      return value.trim() !== "" && formatCelDouble(parsed) === value
        ? equality(parsed)
        : unmatched;
    }
    return undefined;
  }

  if (cast.operator === "int" && type === "number" && typeof value === "number") {
    if ((operator === "eq") !== positive) return undefined;
    if (!Number.isInteger(value)) return unmatched;
    const range: PlanExpressionOperand = {
      operator: "and",
      operands:
        value > 0
          ? [compare("ge", value), compare("lt", value + 1)]
          : value < 0
            ? [compare("gt", value - 1), compare("le", value)]
            : [compare("gt", -1), compare("lt", 1)],
    };
    return operator === "eq" ? range : { operator: "not", operands: [range] };
  }
  return undefined;
}

/**
 * A comparison whose only column appears as `x / x` or `x / ±0`, split on the sign of `x`. CEL
 * numbers are doubles, so neither division raises an error: `x / x` is 1, or NaN at zero, and
 * `x / ±0` is an infinity signed by both operands, or NaN at zero. Each arm substitutes that
 * constant, leaving a comparison between constants that folds; the arms' guards are plain
 * comparisons on `x`, UNKNOWN on a NULL column, as the missing attribute's error requires.
 */
function splitDivision(
  expr: OperatorOperand,
  context: TranslationContext
): PlanExpressionOperand | undefined {
  if (!COMPARISON_OPERATORS.has(expr.operator)) return undefined;
  const column = divisionColumn(expr);
  if (column === undefined) return undefined;
  const fieldRef = resolveFieldReference(column.name, context);
  if (
    fieldRef.valueType !== "number" ||
    (fieldRef.relations?.length ?? 0) > 0 ||
    fieldRef.nullAttributeRepresentation === "explicit"
  ) {
    return undefined;
  }
  const sign = (op: string): PlanExpressionOperand => ({
    operator: op,
    operands: [column, { value: 0 }],
  });
  const arms: PlanExpressionOperand[] = [];
  for (const [guard, x] of [
    ["gt", 1],
    ["lt", -1],
    ["eq", 0],
  ] as const) {
    const substituted = substituteDivision(expr, column.name, x);
    if (substituted === undefined) return undefined;
    arms.push({ operator: "and", operands: [sign(guard), substituted] });
  }
  return { operator: "or", operands: arms };
}

/** The one column of a comparison when every reference to it sits in `x / x` or `x / ±0`. */
function divisionColumn(expr: PlanExpressionOperand): NamedOperand | undefined {
  let column: NamedOperand | undefined;
  let divided = false;
  const visit = (node: PlanExpressionOperand): boolean => {
    if (isNamedOperand(node)) {
      if (column !== undefined && column.name !== node.name) return false;
      column = node;
      return false;
    }
    if (!isOperatorOperand(node)) return true;
    if (node.operator === "lambda") return false;
    if (isColumnDivision(node)) {
      const numerator = node.operands[0] as NamedOperand;
      if (column !== undefined && column.name !== numerator.name) return false;
      column = numerator;
      divided = true;
      return true;
    }
    return node.operands.every(visit);
  };
  return visit(expr) && divided ? column : undefined;
}

function isColumnDivision(node: OperatorOperand): boolean {
  if (node.operator !== "div" || node.operands.length !== 2) return false;
  const [numerator, denominator] = node.operands as [PlanExpressionOperand, PlanExpressionOperand];
  if (!isNamedOperand(numerator)) return false;
  if (isNamedOperand(denominator)) return denominator.name === numerator.name;
  return isValueOperand(denominator) && denominator.value === 0;
}

/**
 * `expr` with every division of the column replaced by its value when the column has the sign of
 * `x`, or undefined when the column is still referenced outside one.
 */
function substituteDivision(
  expr: PlanExpressionOperand,
  name: string,
  x: number
): PlanExpressionOperand | undefined {
  if (isNamedOperand(expr)) return undefined;
  if (!isOperatorOperand(expr)) return expr;
  if (isColumnDivision(expr)) {
    const denominator = expr.operands[1]!;
    const d = isNamedOperand(denominator) ? x : ((denominator as { value: number }).value);
    return { value: x / d };
  }
  const operands: PlanExpressionOperand[] = [];
  for (const operand of expr.operands) {
    const substituted = substituteDivision(operand, name, x);
    if (substituted === undefined) return undefined;
    operands.push(substituted);
  }
  return { operator: expr.operator, operands };
}

/** Past this many code points, a literal is not split across a two-column concatenation. */
const MAX_CONCATENATION_SPLITS = 256;

/**
 * `a + b == "lit"` over two string columns: the literal split at every code point, one arm per
 * split, `a == prefix && b == suffix`. Each arm also ORs in `!startsWith(column, "")` for both
 * columns — FALSE for a present value, UNKNOWN for a NULL one — so a missing operand leaves the
 * whole comparison UNKNOWN under both polarities, as the error `+` raises on it does in CEL.
 */
function rewriteConcatenation(
  expr: OperatorOperand,
  context: TranslationContext
): PlanExpressionOperand | undefined {
  const { operator } = expr;
  if ((operator !== "eq" && operator !== "ne") || expr.operands.length !== 2) return undefined;
  const [first, second] = expr.operands as [PlanExpressionOperand, PlanExpressionOperand];
  const [sum, literal] = isOperatorOperand(first) ? [first, second] : [second, first];
  if (
    !isOperatorOperand(sum) ||
    sum.operator !== "add" ||
    sum.operands.length !== 2 ||
    !isValueOperand(literal) ||
    typeof literal.value !== "string"
  ) {
    return undefined;
  }
  const [left, right] = sum.operands as [PlanExpressionOperand, PlanExpressionOperand];
  const columns = [left, right].filter(isNamedOperand);
  if (
    columns.length !== 2 ||
    columns.some((column) => {
      const fieldRef = resolveFieldReference(column.name, context);
      return fieldRef.valueType !== "string" || (fieldRef.relations?.length ?? 0) > 0;
    })
  ) {
    return undefined;
  }
  const codePoints = Array.from(literal.value);
  if (codePoints.length > MAX_CONCATENATION_SPLITS) return undefined;

  const arms: PlanExpressionOperand[] = [];
  for (let split = 0; split <= codePoints.length; split++) {
    arms.push({
      operator: "and",
      operands: [
        { operator: "eq", operands: [left, { value: codePoints.slice(0, split).join("") }] },
        { operator: "eq", operands: [right, { value: codePoints.slice(split).join("") }] },
      ],
    });
  }
  for (const column of columns) {
    arms.push({
      operator: "not",
      operands: [{ operator: "startsWith", operands: [column, { value: "" }] }],
    });
  }
  const equality: PlanExpressionOperand = { operator: "or", operands: arms };
  return operator === "eq" ? equality : { operator: "not", operands: [equality] };
}

/**
 * CEL's string() of a double: cel-go's `fmt.Sprintf("%g", d)`, the shortest digits that
 * round-trip, in exponent form below 1e-4 or from 1e+06.
 */
export function formatCelDouble(x: number): string {
  if (Number.isNaN(x)) return "NaN";
  if (x === Infinity) return "+Inf";
  if (x === -Infinity) return "-Inf";
  if (x === 0) return Object.is(x, -0) ? "-0" : "0";
  const sign = x < 0 ? "-" : "";
  const [mantissa, exponentText] = Math.abs(x).toExponential().split("e") as [string, string];
  const digits = mantissa.replace(".", "");
  const exponent = Number(exponentText);
  if (exponent < -4 || exponent >= 6) {
    const fraction = digits.length > 1 ? `.${digits.slice(1)}` : "";
    const magnitude = String(Math.abs(exponent)).padStart(2, "0");
    return `${sign}${digits[0]}${fraction}e${exponent < 0 ? "-" : "+"}${magnitude}`;
  }
  const point = exponent + 1;
  if (point <= 0) return `${sign}0.${"0".repeat(-point)}${digits}`;
  if (point >= digits.length) return `${sign}${digits}${"0".repeat(point - digits.length)}`;
  return `${sign}${digits.slice(0, point)}.${digits.slice(point)}`;
}

// -- the leaves ----------------------------------------------------------------------------------

function leafOutcomes(
  expr: OperatorOperand,
  context: TranslationContext
): Outcomes | undefined {
  const { operator, operands } = expr;
  // A list where a boolean is required: CEL's logical operators raise a no-overload error on it,
  // and a condition that is not a boolean denies.
  if (LIST_OPERATORS.has(operator)) return { error: true };

  if (COMPARISON_OPERATORS.has(operator) && operands.length === 2) {
    const [left, right] = operands as [PlanExpressionOperand, PlanExpressionOperand];
    const nullComparison = omittedNullComparison(operator, left, right, context);
    if (nullComparison !== undefined) return nullComparison;
    const size = [left, right].find(
      (o): o is OperatorOperand => isOperatorOperand(o) && o.operator === "size"
    );
    if (size !== undefined) {
      // size() has overloads for strings, lists and maps only.
      const [subject] = size.operands;
      const type = subject && scalarFieldType(subject, context);
      return type === "number" || type === "boolean" ? { error: true } : undefined;
    }
    const leftType = operandType(left, context);
    const rightType = operandType(right, context);
    if (
      leftType === undefined ||
      rightType === undefined ||
      leftType.type === rightType.type ||
      leftType.type === "null" ||
      rightType.type === "null"
    ) {
      return undefined;
    }
    const missing = leftType.mayBeMissing || rightType.mayBeMissing;
    if (ORDERING_OPERATORS.has(operator)) return { error: true };
    return { [operator === "eq" ? "false" : "true"]: true, error: missing };
  }

  if (STRING_OPERATORS.has(operator) && operands.length === 2) {
    const mistyped = operands.some((operand) => {
      const type = operandType(operand, context)?.type;
      return type !== undefined && type !== "string" && type !== "null";
    });
    return mistyped ? { error: true } : undefined;
  }

  if (HIERARCHY_OPERATORS.has(operator)) {
    // hierarchy() parses a string; any other scalar is a no-overload error.
    const mistyped = operands.some((operand) => {
      if (!isOperatorOperand(operand) || operand.operator !== "hierarchy") return false;
      const [subject] = operand.operands;
      const type = subject && scalarFieldType(subject, context);
      return type === "number" || type === "boolean";
    });
    return mistyped ? { error: true } : undefined;
  }

  if (operator === "in" && operands.length === 2) {
    const [member, collection] = operands as [PlanExpressionOperand, PlanExpressionOperand];
    // A literal member against a mapped list whose elements can never equal it.
    const memberType = literalType(member);
    const list = collectionElementType(collection, context);
    if (memberType !== undefined && memberType !== "null" && list !== undefined) {
      return memberType === list.type ? undefined : { false: true, error: list.mayBeMissing };
    }
    // A field member against a literal list none of whose elements it can equal.
    const field = operandType(member, context);
    const elements = literalElements(collection);
    if (field !== undefined && elements !== undefined) {
      return elements.every((element) => isMismatch(field.type, element))
        ? { false: true, error: field.mayBeMissing }
        : undefined;
    }
    return undefined;
  }

  if (operator === "hasIntersection" && operands.length === 2) {
    const [left, right] = operands as [PlanExpressionOperand, PlanExpressionOperand];
    for (const [list, literal] of [
      [left, right],
      [right, left],
    ] as const) {
      const elementType = collectionElementType(list, context);
      const elements = literalElements(literal);
      if (elementType !== undefined && elements !== undefined) {
        return elements.every((element) => isMismatch(elementType.type, element))
          ? { false: true, error: elementType.mayBeMissing }
          : undefined;
      }
    }
  }

  return undefined;
}

/**
 * `x == null` / `x != null` over a column the caller omits when NULL: a present value is never
 * null, and an absent one is a missing-attribute error, so neither outcome can select the NULL
 * rows an `IS NULL` would.
 */
function omittedNullComparison(
  operator: string,
  left: PlanExpressionOperand,
  right: PlanExpressionOperand,
  context: TranslationContext
): Outcomes | undefined {
  if (operator !== "eq" && operator !== "ne") return undefined;
  const [field, literal] = isNamedOperand(left) ? [left, right] : [right, left];
  if (!isNamedOperand(field) || !isValueOperand(literal) || literal.value !== null) {
    return undefined;
  }
  const fieldRef = resolveFieldReference(field.name, context);
  if (fieldRef.relations && fieldRef.relations.length > 0) return undefined;
  const convention = fieldRef.nullAttributeRepresentation ?? context.nullRepresentation;
  if (convention !== "omitted") return undefined;
  return { [operator === "eq" ? "false" : "true"]: true, error: true };
}

/**
 * Drops, from a literal list compared by element equality, every element that can never equal
 * the other side's elements: each one contributes a definite false to CEL's membership test, so
 * the list without it decides every row the same way.
 */
function rewriteLiteralLists(
  expr: OperatorOperand,
  context: TranslationContext
): PlanExpressionOperand {
  const { operator, operands } = expr;
  if (operands.length !== 2) return expr;
  const [first, second] = operands as [PlanExpressionOperand, PlanExpressionOperand];

  if (operator === "in") {
    const field = operandType(first, context);
    if (field === undefined) return expr;
    const pruned = pruneLiteralList(second, field.type);
    return pruned === undefined ? expr : { operator, operands: [first, pruned] };
  }

  if (operator === "hasIntersection") {
    const leftList = collectionElementType(first, context);
    if (leftList !== undefined) {
      const pruned = pruneLiteralList(second, leftList.type);
      return pruned === undefined ? expr : { operator, operands: [first, pruned] };
    }
    const rightList = collectionElementType(second, context);
    if (rightList !== undefined) {
      const pruned = pruneLiteralList(first, rightList.type);
      return pruned === undefined ? expr : { operator, operands: [pruned, second] };
    }
  }
  return expr;
}

/** The literal list without its mismatched elements, or undefined when nothing is dropped. */
function pruneLiteralList(
  operand: PlanExpressionOperand,
  type: CelType
): PlanExpressionOperand | undefined {
  if (!isValueOperand(operand) || !Array.isArray(operand.value)) return undefined;
  const kept = operand.value.filter((element) => !isMismatch(type, element));
  if (kept.length === operand.value.length || kept.length === 0) return undefined;
  return { value: kept };
}

// -- operand types -------------------------------------------------------------------------------

type KnownType = { type: CelType; mayBeMissing: boolean };

function valueType(value: Value): CelType | undefined {
  if (value === null) return "null";
  if (Array.isArray(value)) return "list";
  switch (typeof value) {
    case "string":
      return "string";
    case "number":
      return "number";
    case "boolean":
      return "boolean";
    case "object":
      return "map";
    default:
      return undefined;
  }
}

function isMismatch(type: CelType, element: Value): boolean {
  const elementType = valueType(element);
  return elementType !== undefined && elementType !== "null" && elementType !== type;
}

/** The CEL type of a literal operand: a value, or a `list(...)`/`struct(...)` constructor. */
function literalType(operand: PlanExpressionOperand): CelType | undefined {
  if (isValueOperand(operand)) return valueType(operand.value);
  if (isOperatorOperand(operand)) {
    if (operand.operator === "list") return "list";
    if (operand.operator === "struct") return "map";
  }
  return undefined;
}

/** The elements of a literal list operand, with nested constructors standing in for their type. */
function literalElements(operand: PlanExpressionOperand): Value[] | undefined {
  if (isValueOperand(operand)) {
    return Array.isArray(operand.value) ? operand.value : undefined;
  }
  if (isOperatorOperand(operand) && operand.operator === "list") {
    const elements: Value[] = [];
    for (const element of operand.operands) {
      if (isValueOperand(element)) {
        elements.push(element.value);
        continue;
      }
      const type = literalType(element);
      if (type === "list") elements.push([]);
      else if (type === "map") elements.push({});
      else return undefined;
    }
    return elements;
  }
  return undefined;
}

function operandType(
  operand: PlanExpressionOperand,
  context: TranslationContext
): KnownType | undefined {
  const literal = literalType(operand);
  if (literal !== undefined) return { type: literal, mayBeMissing: false };
  if (!isNamedOperand(operand)) return undefined;
  const fieldRef = resolveFieldReference(operand.name, context);
  const type = scalarType(fieldRef);
  if (type === undefined) return undefined;
  // A relation-backed reference that is not a to-one scalar is a collection, not a scalar.
  if (fieldRef.relations?.some((relation) => relation.type !== "one")) return undefined;
  return { type, mayBeMissing: mayBeMissing(fieldRef) };
}

function scalarFieldType(
  operand: PlanExpressionOperand,
  context: TranslationContext
): CelType | undefined {
  return operandType(operand, context)?.type;
}

/** Date-time columns are RFC 3339 strings in CEL but not in the store, so they stay unknown. */
function scalarType(fieldRef: ResolvedFieldReference): CelType | undefined {
  switch (fieldRef.valueType) {
    case "string":
    case "number":
    case "boolean":
      return fieldRef.valueType;
    default:
      return undefined;
  }
}

/**
 * Whether evaluating the reference can raise a missing-attribute error: a NULL column the caller
 * omits, or a to-one hop that may be absent. A column sent as an explicit null holds a null VALUE,
 * which heterogeneous equality compares definitely.
 */
function mayBeMissing(fieldRef: ResolvedFieldReference): boolean {
  if (fieldRef.relations?.some((relation) => relation.type === "one")) return true;
  if (fieldRef.nullAttributeRepresentation === "explicit") return false;
  return (
    fieldRef.nullAttributeRepresentation === "omitted" || fieldRef.nullable === true
  );
}

/**
 * The element type of a mapped list: a relation-backed list reference, or `map()` projecting one
 * element column. Undefined when the mapping does not declare it.
 */
function collectionElementType(
  operand: PlanExpressionOperand,
  context: TranslationContext
): KnownType | undefined {
  if (isNamedOperand(operand)) {
    const fieldRef = resolveFieldReference(operand.name, context);
    const relations = fieldRef.relations;
    if (!relations || relations.length === 0) return undefined;
    const last = relations[relations.length - 1]!;
    if (last.type !== "many" || last.field === undefined) return undefined;
    const declared =
      lookupMapping(context.mapper, operand.name)?.valueType ??
      elementFieldValueType(context, operand.name, last.field);
    const type = declared && scalarType({ path: [], valueType: declared });
    if (type === undefined) return undefined;
    return { type, mayBeMissing: relations.some((relation) => relation.type === "one") };
  }
  if (isOperatorOperand(operand) && operand.operator === "map") {
    const [collection, lambda] = operand.operands;
    if (
      collection === undefined ||
      !isNamedOperand(collection) ||
      lambda === undefined ||
      !isOperatorOperand(lambda) ||
      lambda.operator !== "lambda"
    ) {
      return undefined;
    }
    const [projection, variable] = lambda.operands;
    if (
      projection === undefined ||
      !isNamedOperand(projection) ||
      variable === undefined ||
      !isNamedOperand(variable)
    ) {
      return undefined;
    }
    const scoped = scopedContext(context, collection.name, variable.name);
    if (scoped === undefined) return undefined;
    const type = scalarType(resolveFieldReference(projection.name, scoped));
    if (type === undefined) return undefined;
    // A projected element column can be missing on an element, which errors the whole map().
    return { type, mayBeMissing: true };
  }
  return undefined;
}

/** The declared valueType of the projected element column of a relation-backed list. */
function elementFieldValueType(
  context: TranslationContext,
  reference: string,
  field: string
): MapperConfig["valueType"] {
  const relation = lookupMapping(context.mapper, reference)?.relation;
  return relation?.fields?.[field]?.valueType;
}
