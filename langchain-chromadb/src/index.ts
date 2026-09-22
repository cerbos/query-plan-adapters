import {
  PlanExpression,
  PlanExpressionOperand,
  PlanExpressionValue,
  PlanExpressionVariable,
  PlanKind as PK,
  PlanResourcesResponse,
} from "@cerbos/core";
import type { Where } from "chromadb";

export type PlanKind = PK;
export const PlanKind = PK;

export interface FieldNameMapperConfig {
  field: string;
  required?: boolean;
  numericType?: "integer" | "float";
}

type FieldNameMapperValue = string | FieldNameMapperConfig;

export type FieldMapper =
  | Record<string, FieldNameMapperValue>
  | ((key: string) => FieldNameMapperValue);

export interface QueryPlanToChromaDBArgs {
  queryPlan: PlanResourcesResponse;
  fieldNameMapper: FieldMapper;
}

// Exported so a consumer can name what it is handed, as prisma and drizzle already do for theirs.
// A caller that passes the result to a function of its own — which is what composing the clause
// with an application-owned one looks like — otherwise has to write it out or reach for
// `ReturnType<typeof queryPlanToChromaDB>`. Found by `example/`, which is the only thing here that
// resolves this package through its published surface
// (docs/adr/0002-examples-install-the-packed-artifact.md).
export type QueryPlanToChromaDBResult =
  | { kind: PK.ALWAYS_ALLOWED; filters: Record<string, never> }
  | { kind: PK.ALWAYS_DENIED; filters?: undefined }
  | { kind: PK.CONDITIONAL; filters: Where };

/**
 * A well-formed plan asks for something a Chroma metadata filter cannot express. Thrown so a caller
 * can route that case — a broader search, a per-document `check()`, a deny — without matching on
 * the message, which stays the text `conformance/actions.json` pins
 * (cerbos/query-plan-adapters#228). A malformed plan or a mapper misconfiguration is a plain `Error`.
 *
 * `operator` is the plan operator the refusal is about: the one the message names, after mirroring
 * and negation (`not(eq)` over an optional key reports `ne`); a computed operand's own inside a
 * comparison (`add`, `size`), and the enclosing operator anywhere else (`exists`, not its lambda);
 * the comparison itself for two keys or two literals; `if` for a ternary.
 */
export class UnsupportedOperatorError extends Error {
  readonly operator: string;

  constructor(operator: string, message: string) {
    super(message);
    this.name = "UnsupportedOperatorError";
    this.operator = operator;
  }
}

export function queryPlanToChromaDB({
  queryPlan,
  fieldNameMapper,
}: QueryPlanToChromaDBArgs): QueryPlanToChromaDBResult {
  switch (queryPlan.kind) {
    case PlanKind.ALWAYS_ALLOWED:
      return { kind: PlanKind.ALWAYS_ALLOWED, filters: {} };
    case PlanKind.ALWAYS_DENIED:
      return { kind: PlanKind.ALWAYS_DENIED };
    case PlanKind.CONDITIONAL:
      return {
        kind: PlanKind.CONDITIONAL,
        filters: mapOperand(queryPlan.condition, fieldResolver(fieldNameMapper)),
      };
    default:
      throw Error("Invalid query plan.");
  }
}

// -- the comparisons Chroma can express ------------------------------------------------------------

type ChromaLiteral = string | number | boolean;

interface Comparison {
  /** The Chroma operator the comparison is emitted as. */
  chroma: "$eq" | "$ne" | "$lt" | "$lte" | "$gt" | "$gte" | "$in" | "$nin";
  /** Validates the literal operand, refusing one Chroma metadata cannot be compared with. */
  literal: (value: unknown, operator: string) => ChromaLiteral | ChromaLiteral[];
  /** The operator `not(key <op> literal)` becomes. Absent: the comparison cannot be negated. */
  negated?: string;
  /** The operator `literal <op> key` becomes once the key is moved left. Absent: no mirror. */
  mirrored?: string;
  /**
   * Chroma matches a document that is missing the metadata key, where CEL raises a
   * missing-attribute error and the PDP denies — so it is only sound over a `required` field.
   */
  matchesMissingKey?: true;
  /** An ordered comparison: a fractional threshold needs a field declared `numericType: "float"`. */
  ordered?: true;
  /**
   * Never a plan operator — only ever reached by negating one. A plan node naming it with the
   * wrong operands is therefore a shape this adapter does not know, not a malformed comparison.
   */
  negationOnly?: true;
}

/**
 * Every comparison this adapter emits, keyed by the plan operator it translates. Adding an operator
 * is adding a row here: operand validation, negation, mirroring and the optional-key and
 * fractional-threshold guards are all read from it.
 */
// prettier-ignore
const COMPARISONS: ReadonlyMap<string, Comparison> = new Map(
  Object.entries<Comparison>({
    eq:  { chroma: "$eq",  literal: requireLiteral,     negated: "ne",  mirrored: "eq" },
    ne:  { chroma: "$ne",  literal: requireLiteral,     negated: "eq",  mirrored: "ne", matchesMissingKey: true },
    lt:  { chroma: "$lt",  literal: requireNumber,      negated: "ge",  mirrored: "gt", ordered: true },
    le:  { chroma: "$lte", literal: requireNumber,      negated: "gt",  mirrored: "ge", ordered: true },
    gt:  { chroma: "$gt",  literal: requireNumber,      negated: "le",  mirrored: "lt", ordered: true },
    ge:  { chroma: "$gte", literal: requireNumber,      negated: "lt",  mirrored: "le", ordered: true },
    in:  { chroma: "$in",  literal: requireLiteralList, negated: "nin" },
    nin: { chroma: "$nin", literal: requireLiteralList, matchesMissingKey: true, negationOnly: true },
  }),
);

/** A comparison the planner emits, as opposed to any other operator or one only negation reaches. */
function isPlanComparison(operator: string): boolean {
  const comparison = COMPARISONS.get(operator);
  return comparison !== undefined && !comparison.negationOnly;
}

/** `{ field: { $op: literal } }`, once the literal has passed the comparison's validation. */
function emit(field: string, operator: string, value: unknown): Where {
  const { chroma, literal } = COMPARISONS.get(operator)!;
  return { [field]: { [chroma]: literal(value, operator) } } as Where;
}

function unsupported(operator: string): UnsupportedOperatorError {
  return new UnsupportedOperatorError(operator, `Unsupported operator ${operator}`);
}

function negationOf(operator: string): string {
  const negated = COMPARISONS.get(operator)?.negated;
  if (negated === undefined) {
    throw new UnsupportedOperatorError(
      operator,
      `Cannot negate operator ${operator}`,
    );
  }
  return negated;
}

/** `literal <op> key` as `key <op'> literal`, the only orientation a `Where` clause has. */
function mirrorOf(operator: string): string {
  if (operator === "in") {
    throw new UnsupportedOperatorError(
      operator,
      "ChromaDB filters cannot test whether a literal is contained in a metadata field",
    );
  }
  const mirrored = COMPARISONS.get(operator)?.mirrored;
  if (mirrored === undefined) {
    throw unsupported(operator);
  }
  return mirrored;
}

// -- literal operands ------------------------------------------------------------------------------

function isChromaLiteral(value: unknown): value is ChromaLiteral {
  return (
    typeof value === "string" ||
    typeof value === "boolean" ||
    (typeof value === "number" && Number.isFinite(value))
  );
}

function requireLiteral(value: unknown, operator: string): ChromaLiteral {
  if (!isChromaLiteral(value)) {
    throw new UnsupportedOperatorError(
      operator,
      `${operator} requires a finite number, string, or boolean literal`,
    );
  }
  return value;
}

function requireNumber(value: unknown, operator: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new UnsupportedOperatorError(
      operator,
      `${operator} requires a finite number literal`,
    );
  }
  return value;
}

function requireLiteralList(value: unknown, operator: string): ChromaLiteral[] {
  if (!Array.isArray(value) || value.length === 0) {
    throw new UnsupportedOperatorError(
      operator,
      `${operator} requires a non-empty literal list`,
    );
  }
  if (!value.every(isChromaLiteral)) {
    throw new UnsupportedOperatorError(
      operator,
      `${operator} requires a list containing only finite numbers, strings, or booleans`,
    );
  }
  const firstType = typeof value[0];
  if (!value.every((item) => typeof item === firstType)) {
    throw new UnsupportedOperatorError(
      operator,
      `${operator} requires a list whose values have one scalar type`,
    );
  }
  return value;
}

// -- metadata fields -------------------------------------------------------------------------------

type ResolvedField = {
  name: string;
  numericType?: "integer" | "float";
  required: boolean;
};

type FieldResolver = (key: string) => ResolvedField;

// Fields default to optional: Chroma's $ne/$nin match records where the metadata key is absent,
// while Cerbos denies on a missing attribute. Without an explicit `required: true` assertion from
// the integrator the adapter cannot know the key is always present, so those operators are
// rejected rather than allowed to over-grant.
function fieldResolver(fieldNameMapper: FieldMapper): FieldResolver {
  return (key) => {
    const mapped =
      typeof fieldNameMapper === "function"
        ? fieldNameMapper(key)
        : fieldNameMapper[key];
    const field: ResolvedField =
      typeof mapped === "string"
        ? { name: mapped, required: false }
        : mapped
          ? {
              name: mapped.field,
              numericType: mapped.numericType,
              required: mapped.required ?? false,
            }
          : { name: key, required: false };
    if (!field.name) {
      throw Error("Field name is required");
    }
    return field;
  };
}

function requirePresenceFor(field: ResolvedField, operator: string): void {
  if (!field.required && COMPARISONS.get(operator)?.matchesMissingKey) {
    throw new UnsupportedOperatorError(
      operator,
      `${operator} is unsafe for optional Chroma metadata because missing fields match the filter`,
    );
  }
}

// -- the walk --------------------------------------------------------------------------------------

// Operands are classified by shape, never with `instanceof`. `instanceof` is nominal, so it
// answers "was this built by MY copy of @cerbos/core?" rather than "what kind of operand is
// this?" — and a consumer whose Cerbos client resolves a different copy of core than this
// adapter does is an ordinary npm outcome, not a misconfiguration. No dependency declaration
// prevents it: npm resolves a peer to the highest version satisfying it, not the one that
// dedupes with the rest of the tree, so every range leaves some consumer with two copies
// (cerbos/query-plan-adapters#419). The three operand types have disjoint shapes, so matching
// on them is exact and survives however many copies exist. Same trio as every other adapter.
const isExpression = (e: PlanExpressionOperand): e is PlanExpression =>
  "operator" in e;
const isValue = (e: PlanExpressionOperand): e is PlanExpressionValue =>
  "value" in e;
const isVariable = (e: PlanExpressionOperand): e is PlanExpressionVariable =>
  "name" in e;

function mapOperand(
  operand: PlanExpressionOperand,
  resolveField: FieldResolver,
  negate = false,
): Where {
  if (isVariable(operand)) {
    return mapBooleanVariable(operand, resolveField, negate);
  }
  if (!isExpression(operand)) {
    throw Error(
      `Query plan did not contain an expression for operand ${String(operand)}`,
    );
  }

  const { operator, operands } = operand;

  if (operator === "and" || operator === "or") {
    if (operands.length < 2) throw Error("Expected at least 2 operands");
    const children = operands.map((child) =>
      mapOperand(child, resolveField, negate),
    );
    // De Morgan: under negation a conjunction becomes a disjunction and vice versa.
    return (operator === "and") !== negate
      ? { $and: children }
      : { $or: children };
  }

  if (operator === "not") {
    if (operands.length !== 1 || !operands[0])
      throw Error("Expected exactly one operand");
    return mapOperand(operands[0], resolveField, !negate);
  }

  return mapComparison(operator, operands, resolveField, negate);
}

/** A bare boolean key as a condition: `key == true`, or `key != true` under negation. */
function mapBooleanVariable(
  variable: PlanExpressionVariable,
  resolveField: FieldResolver,
  negate: boolean,
): Where {
  const field = resolveField(variable.name);
  const operator = negate ? "ne" : "eq";
  requirePresenceFor(field, operator);
  return emit(field.name, operator, true);
}

function mapComparison(
  planOperator: string,
  operands: PlanExpressionOperand[],
  resolveField: FieldResolver,
  negate: boolean,
): Where {
  // Checked before the operands are read, so a non-negatable shape keeps its refusal site under
  // negation whatever its operands are.
  if (negate) {
    negationOf(planOperator);
  }
  const { variable, literalFirst, value } = binaryOperands(
    planOperator,
    operands,
  );
  const oriented = literalFirst ? mirrorOf(planOperator) : planOperator;
  const operator = negate ? negationOf(oriented) : oriented;

  const field = resolveField(variable.name);
  const comparison = COMPARISONS.get(operator);
  if (!comparison) {
    throw unsupported(operator);
  }
  requirePresenceFor(field, operator);
  if (
    comparison.ordered &&
    typeof value === "number" &&
    !Number.isInteger(value) &&
    field.numericType !== "float"
  ) {
    throw new UnsupportedOperatorError(
      operator,
      `${operator} cannot safely compare a fractional threshold unless the mapped Chroma metadata field declares numericType: "float"`,
    );
  }
  return emit(field.name, operator, value);
}

type KeyOrLiteral =
  | { variable: PlanExpressionVariable }
  | { value: PlanExpressionValue["value"] };

/**
 * The metadata key and the literal a comparison relates, and which side the literal is on. Anything
 * else — a computed operand, two keys, two literals — is a shape the `Where` grammar cannot hold.
 */
function binaryOperands(
  operator: string,
  operands: PlanExpressionOperand[],
): {
  variable: PlanExpressionVariable;
  literalFirst: boolean;
  value: PlanExpressionValue["value"];
} {
  if (operands.length !== 2) {
    // A known comparison short of an operand is a malformed plan; any other operator — a ternary,
    // in the corpus — is a shape the `Where` grammar has no comparison for.
    if (isPlanComparison(operator)) {
      throw Error("Expected exactly two operands");
    }
    throw new UnsupportedOperatorError(
      operator,
      "Expected exactly two operands",
    );
  }

  const [left, right] = operands.map((operand): KeyOrLiteral => {
    if (isVariable(operand)) return { variable: operand };
    if (isValue(operand)) return { value: operand.value };
    // Inside a comparison, the computed operand is the part Chroma cannot evaluate. Inside
    // anything else — a collection macro, whose second operand is always its lambda — the
    // operator itself is what has no `Where` form, and `lambda` would tell a caller nothing.
    throw new UnsupportedOperatorError(
      isPlanComparison(operator) ? operand.operator : operator,
      "Nested expressions are not supported by ChromaDB filters",
    );
  }) as [KeyOrLiteral, KeyOrLiteral];

  if ("variable" in left && "value" in right) {
    return { variable: left.variable, literalFirst: false, value: right.value };
  }
  if ("value" in left && "variable" in right) {
    return { variable: right.variable, literalFirst: true, value: left.value };
  }
  throw new UnsupportedOperatorError(
    operator,
    "variable" in left
      ? "Variable-to-variable comparisons are not supported by ChromaDB filters"
      : "Value-to-value comparisons are not supported by ChromaDB filters",
  );
}
