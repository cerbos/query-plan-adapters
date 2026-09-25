// Predicates on one resolved column: the leaves every operator handler bottoms out in.

import type { Value } from "@cerbos/core";

import type { PrismaFilter } from "./index";
import {
  assertNullOperandTranslatable,
  getLeafField,
  isExplicitNullReference,
} from "./mapping";
import type { ResolvedFieldReference, TranslationContext } from "./mapping";
import { CERBOS_TO_PRISMA_OPERATOR, assertDefined } from "./plan";
import { wrapInRelations } from "./relations";
import { UnsupportedQueryPlanError } from "./errors";

/**
 * Prisma has no model-agnostic always-false `where` shape — empty logical arrays are ignored —
 * so a predicate that folds to constant false is refused. Real PDP plans fold it to
 * ALWAYS_DENIED before it reaches the adapter.
 */
export function rejectConstantFalse(): never {
  throw new UnsupportedQueryPlanError(
    "A constant-false conditional predicate must be folded by the Cerbos planner"
  );
}

/** `{ field: { [prismaOp]: value } }`, nested through the field's relations. */
export function buildFieldFilter(
  fieldRef: ResolvedFieldReference,
  prismaOp: string,
  value: Value
): PrismaFilter {
  const fieldName = getLeafField(fieldRef.path);
  return wrapInRelations(fieldRef.relations, {
    [fieldName]: { [prismaOp]: value },
  });
}

/** A filter no row satisfies, spelled on a column the model is known to have. */
export function buildImpossibleFilter(
  fieldRef: ResolvedFieldReference
): PrismaFilter {
  return buildFieldFilter(fieldRef, "in", []);
}

function buildDirectOrInLeaf(
  fieldRef: ResolvedFieldReference,
  values: Value[]
): PrismaFilter {
  const fieldName = getLeafField(fieldRef.path);
  return values.length === 1
    ? { [fieldName]: values[0] }
    : { [fieldName]: { in: values } };
}

/**
 * `field IS NOT NULL AND <leaf>`, so a NULL column makes the whole thing definitely FALSE rather
 * than merely unmatched — which is what lets an enclosing `NOT` include the row.
 *
 * Prisma has no three-valued logic to expose: a record either matches a `where` or it does not.
 * That is exactly why the guard has to be explicit — `{ NOT: { field: { equals: "x" } } }` leans on
 * the generated SQL's UNKNOWN, which excludes the record under both polarities, and an
 * explicit-null attribute needs the negation to include it.
 *
 * The guard is ANDed with the leaf BEFORE the relation wrapping, so both halves are evaluated
 * against the same element rather than against two independently-matched ones.
 */
function guardedFieldFilter(
  fieldRef: ResolvedFieldReference,
  leaf: PrismaFilter
): PrismaFilter {
  const fieldName = getLeafField(fieldRef.path);
  return wrapInRelations(fieldRef.relations, {
    AND: [{ [fieldName]: { not: null } }, leaf],
  });
}

function assertScalarValue(
  fieldRef: ResolvedFieldReference,
  value: Value,
  operator: string
): void {
  if (value !== null && typeof value === "object") {
    throw new UnsupportedQueryPlanError(
      `${operator} requires scalar values: Prisma cannot compare list or map elements`
    );
  }
  if (
    value !== null &&
    fieldRef.valueType !== undefined &&
    fieldRef.valueType !== "dateTime" &&
    typeof value !== fieldRef.valueType
  ) {
    throw new UnsupportedQueryPlanError(
      `${operator} value type does not match mapped ${fieldRef.valueType} field`
    );
  }
}

export function assertStringField(
  fieldRef: ResolvedFieldReference,
  operator: string
): void {
  if (fieldRef.valueType !== undefined && fieldRef.valueType !== "string") {
    throw new UnsupportedQueryPlanError(
      `${operator} requires a string field, got ${fieldRef.valueType}`
    );
  }
}

/** `field IN values`, with CEL's semantics for a null element and for an explicit-null column. */
export function buildMembershipFilter(
  context: TranslationContext,
  fieldRef: ResolvedFieldReference,
  values: Value[]
): PrismaFilter {
  for (const value of values) assertScalarValue(fieldRef, value, "in");
  const nonNullValues = values.filter((value) => value !== null);
  const carriesNull = nonNullValues.length !== values.length;
  if (carriesNull) {
    assertNullOperandTranslatable(
      context,
      "a null element in an `in` list",
      fieldRef.nullAttributeRepresentation
    );
  }
  const filters: PrismaFilter[] = [];

  if (nonNullValues.length > 0 || values.length === 0) {
    // Without a null element nothing has made the membership definite yet: a NULL column is
    // dropped by `{ in: [...] }` under BOTH polarities, while CEL compares a null VALUE against
    // each element and gets a definite false, so its negation is TRUE. With a null element the
    // `[null]` disjunct below already settles it.
    const leaf = buildDirectOrInLeaf(fieldRef, nonNullValues);
    filters.push(
      carriesNull || !isExplicitNullReference(fieldRef)
        ? wrapInRelations(fieldRef.relations, leaf)
        : guardedFieldFilter(fieldRef, leaf)
    );
  }
  if (carriesNull) {
    filters.push(
      wrapInRelations(fieldRef.relations, buildDirectOrInLeaf(fieldRef, [null]))
    );
  }

  return filters.length === 1 ? filters[0]! : { OR: filters };
}

/**
 * The two-clause bracket a fractional constant needs, or undefined for an operator that has
 * none. Prisma coerces filter values to the column's type: on an Int column, `gte: 1.5` is
 * silently bound as `gte: 1`, inverting rows like aNumber == 1. Each bracket pairs the exact
 * constant with its integer neighbor so the combination is correct BOTH for Float columns (the
 * extra clause is redundant) and Int columns (whichever way Prisma truncates or floors the
 * fraction).
 */
function fractionalBracket(
  fieldName: string,
  operator: string,
  value: number
): PrismaFilter | undefined {
  const lower = Math.floor(value);
  const upper = Math.ceil(value);
  const equalsBracket = {
    AND: [
      { [fieldName]: { equals: value } },
      { [fieldName]: { gt: lower } },
      { [fieldName]: { lt: upper } },
    ],
  };
  switch (operator) {
    case "gt":
      return {
        OR: [{ [fieldName]: { gt: value } }, { [fieldName]: { gte: upper } }],
      };
    case "ge":
      return {
        AND: [{ [fieldName]: { gte: value } }, { [fieldName]: { gt: lower } }],
      };
    case "lt":
      return {
        OR: [{ [fieldName]: { lt: value } }, { [fieldName]: { lte: lower } }],
      };
    case "le":
      return {
        AND: [{ [fieldName]: { lte: value } }, { [fieldName]: { lt: upper } }],
      };
    case "eq":
      return equalsBracket;
    case "ne":
      return { NOT: equalsBracket };
    default:
      return undefined;
  }
}

/**
 * An ordering against a boolean constant. CEL orders bools, `false < true`, but Prisma's Boolean
 * filter has only `equals` and `not`, so the ordering becomes the set of bools that satisfy it,
 * spelled with `equals` alone. Every spelling reads the column, so a NULL stays UNKNOWN under both
 * polarities, as the missing-attribute error requires: an empty set is a contradiction on the
 * column and the full set a tautology on it. Plain `equals` rather than the explicit-null guard,
 * because ordering a null VALUE is a no-overload error in CEL, not a definite answer.
 */
function buildBooleanOrderingFilter(
  fieldRef: ResolvedFieldReference,
  operator: string,
  value: boolean
): PrismaFilter {
  const satisfied = [false, true].filter((candidate) => {
    const [a, b] = [Number(candidate), Number(value)];
    switch (operator) {
      case "lt":
        return a < b;
      case "le":
        return a <= b;
      case "gt":
        return a > b;
      case "ge":
        return a >= b;
      default:
        throw new UnsupportedQueryPlanError(`Unsupported operator: ${operator}`);
    }
  });
  if (satisfied.length === 1) return buildFieldFilter(fieldRef, "equals", satisfied[0]!);
  const fieldName = getLeafField(fieldRef.path);
  const both = [{ [fieldName]: { equals: false } }, { [fieldName]: { equals: true } }];
  return wrapInRelations(
    fieldRef.relations,
    satisfied.length === 0 ? { AND: both } : { OR: both }
  );
}

/**
 * Builds a field-vs-constant comparison. Fractional constants are emitted as a two-clause
 * bracket (see fractionalBracket).
 */
export function buildComparisonFilter(
  context: TranslationContext,
  fieldRef: ResolvedFieldReference,
  operator: string,
  value: Value
): PrismaFilter {
  const prismaOperator = assertDefined(
    CERBOS_TO_PRISMA_OPERATOR[operator],
    `Unsupported operator: ${operator}`
  );

  assertScalarValue(fieldRef, value, operator);

  if (typeof value === "boolean" && operator !== "eq" && operator !== "ne") {
    return buildBooleanOrderingFilter(fieldRef, operator, value);
  }

  if (value === null) {
    assertNullOperandTranslatable(
      context,
      `\`${operator}\` against a null operand`,
      fieldRef.nullAttributeRepresentation
    );
  }

  // An attribute the caller sends as an explicit null holds a null VALUE in CEL, so equality
  // against a non-null operand is a definite FALSE and inequality a definite TRUE. Prisma leans
  // on SQL's UNKNOWN for both, which drops the row under either polarity, so the presence of the
  // column has to become part of the predicate. Confined to the equality family: an ordering
  // comparison against a null receiver is a no-overload error in CEL, which denies exactly as the
  // dropped row does.
  if (
    value !== null &&
    isExplicitNullReference(fieldRef) &&
    (operator === "eq" || operator === "ne")
  ) {
    const fieldName = getLeafField(fieldRef.path);
    const equality = guardedFieldFilter(fieldRef, {
      [fieldName]: { equals: value },
    });
    return operator === "eq" ? equality : { NOT: equality };
  }

  if (typeof value === "number" && !Number.isInteger(value)) {
    const bracket = fractionalBracket(
      getLeafField(fieldRef.path),
      operator,
      value
    );
    if (bracket) {
      return wrapInRelations(fieldRef.relations, bracket);
    }
  }

  return buildFieldFilter(fieldRef, prismaOperator, value);
}

/** OR of `field IS NULL` checks for every nullable element column the lambda touched. */
export function buildNullWitnessFilter(
  nullableFields: Set<string>
): PrismaFilter {
  const checks = [...nullableFields].map((field) => ({ [field]: null }));
  return checks.length === 1 ? checks[0]! : { OR: checks };
}
