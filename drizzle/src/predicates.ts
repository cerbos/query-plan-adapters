import type { PlanExpressionOperand, Value } from "@cerbos/core";
import { and, is, isNull, not, or, sql } from "drizzle-orm";
import type { AnyColumn, SQL } from "drizzle-orm";
import { MySqlColumn } from "drizzle-orm/mysql-core";
import { Param } from "drizzle-orm/sql";

import { resolveConstantNumber } from "./arithmetic";
import {
  isColumn,
  isMappingConfig,
  isRelationValue,
  mappingNullRepresentation,
} from "./mapper";
import { isValueOperand } from "./operands";
import type {
  BaseMapperEntry,
  BuildFilterOptions,
  ComparisonOperator,
  NullAttributeRepresentation,
} from "./types";

/**
 * The leaf SQL the adapter emits: constants, bound values, null guards, string matching, and a
 * single comparison against one mapping. Nothing here recurses into the plan.
 */

export const FALSE_CONDITION = sql`0 = 1`;
export const TRUE_CONDITION = sql`1 = 1`;

/** Apply the polarity a `not` pushed down to this leaf. */
export const withPolarity = (filter: SQL, negated: boolean): SQL =>
  negated ? not(filter) : filter;

/** A plan-time boolean, as the constant condition that spells it. */
export const constantCondition = (value: boolean): SQL =>
  value ? TRUE_CONDITION : FALSE_CONDITION;

// -- nullability ---------------------------------------------------------------------------------

/**
 * A SQL fragment plus whether the value behind it can be SQL NULL at run time.
 *
 * A bound constant's nullness is settled during translation, so guarding one with `IS NULL` asks
 * the database a question the adapter has already answered. PostgreSQL cannot even answer it: a
 * bare parameter has no type of its own and `IS NULL` gives it no context to infer one, so
 * `$1 IS NULL` is rejected outright rather than merely being redundant
 * (cerbos/query-plan-adapters#320).
 */
export interface NullableExpression {
  expr: SQL;
  canBeNull: boolean;
}

/** The `IS NULL` disjunction over the operands that can actually be NULL, or nothing. */
export const buildNullGuard = (
  ...operands: NullableExpression[]
): SQL | undefined => {
  const guards = operands
    .filter((operand) => operand.canBeNull)
    .map((operand) => sql`${operand.expr} is null`);
  return guards.length ? sql.join(guards, sql` or `) : undefined;
};

/**
 * Classify an operand's rendered SQL by whether it can be NULL.
 *
 * A constant renders as a bound literal, so only an operand that reaches a column can be NULL.
 * Arithmetic over constants folds to one too, which is why it is resolved rather than assumed:
 * `($1 + $2) IS NULL` is as untypeable on PostgreSQL as `$1 IS NULL`.
 */
export const operandExpression = (
  expr: SQL,
  operand: PlanExpressionOperand,
): NullableExpression => ({
  expr,
  canBeNull: isValueOperand(operand)
    ? operand.value === null
    : resolveConstantNumber(operand) === undefined,
});

/** A column expression, which the schema may allow to be NULL. */
export const columnExpression = (expr: SQL): NullableExpression => ({
  expr,
  canBeNull: true,
});

/** A constant already known to be a non-null literal by the time it is rendered. */
export const constantExpression = (expr: SQL): NullableExpression => ({
  expr,
  canBeNull: false,
});

// -- binding constants ---------------------------------------------------------------------------

/** The widest exact integer every numeric SQL type a driver might infer can carry. */
const INT32_MIN = -2147483648;
const INT32_MAX = 2147483647;

/**
 * Whether a constant has to be bound as an explicit double rather than left for the engine to
 * type from whatever it is compared with.
 *
 * CEL numbers are IEEE doubles, but a bound parameter carries no type of its own: PostgreSQL
 * types it from the other side of the comparison — an `integer` column, `length()`'s `integer`,
 * `count()`'s `bigint` — and then rejects a value that does not fit, so `aNumber >= 1.5` and
 * `size(aString) > 4294967296` fail outright there while SQLite compares them numerically. The
 * silent failure mode is worse than the loud one: read as SQL `numeric`, `aNumber * 0.1 == 0.3`
 * is exact decimal arithmetic and allows a row CEL's binary floating point denies
 * (cerbos/query-plan-adapters#320).
 *
 * An exact 32-bit integer is carried by every numeric type SQL might infer and needs no cast, so
 * the common `column = 5` predicate keeps whatever index the column has.
 */
const requiresDoubleTyping = (value: Value): value is number =>
  typeof value === "number" &&
  (!Number.isInteger(value) || value < INT32_MIN || value > INT32_MAX);

/** The constant, typed so the engine reads it as an IEEE double. */
export const bindConstant = (value: Value): SQL =>
  requiresDoubleTyping(value)
    ? sql`cast(${value} as float(53))`
    : sql`${value}`;

/**
 * A constant bound for comparison against `column`.
 *
 * Normally the column's own encoder types it, which is what keeps the comparison index-friendly.
 * A value that type cannot carry goes through `bindConstant` instead, so the engine widens the
 * comparison rather than rejecting the parameter.
 */
const bindAgainstColumn = (value: Value, column: AnyColumn): SQL | Param =>
  requiresDoubleTyping(value) ? bindConstant(value) : new Param(value, column);

// -- string matching -----------------------------------------------------------------------------

export type StringMatchOperator = "contains" | "startsWith" | "endsWith";

/**
 * A string's length in CHARACTERS, the unit CEL's `size()` and every dialect's `substr()` count
 * in. SQLite's and PostgreSQL's `length()` already count characters; MySQL's counts BYTES, so a
 * multi-byte character (`é` is two bytes in utf8mb4) would offset every `substr()` built on it
 * and make `size()` disagree with the PDP (#473). MySQL's character count is `char_length()`,
 * which SQLite does not have — so, as `indexed.ts` does, the dialect is read off the Drizzle
 * class of whichever operand is a column, and the caller still declares none. With no column
 * among the operands there is no MySQL-side value to measure.
 */
export const characterLength = (
  columns: readonly (AnyColumn | undefined)[],
): ((expr: SQL) => SQL) =>
  columns.some((column) => column !== undefined && is(column, MySqlColumn))
    ? (expr) => sql`char_length(${expr})`
    : (expr) => sql`length(${expr})`;

/**
 * CEL-exact string matching: replace/substr are case-sensitive, interpret no LIKE
 * metacharacters (% _ \ in the needle match literally), and propagate NULL as SQL
 * UNKNOWN — which excludes the row under both polarities, mirroring the CEL
 * missing-attribute error (deny). The receiver is ALWAYS the haystack and the needle
 * ALWAYS the pattern; operands are never swapped.
 */
export const buildStringMatchCondition = (
  operator: StringMatchOperator,
  receiver: NullableExpression,
  needle: NullableExpression,
  length: (expr: SQL) => SQL,
): SQL => {
  const receiverExpr = receiver.expr;
  const needleExpr = needle.expr;
  switch (operator) {
    case "contains": {
      // REPLACE is case-sensitive and treats the needle literally on SQLite,
      // PostgreSQL, and MySQL. Unlike LIKE it cannot reinterpret %, _, \, or [
      // from a column-valued needle as pattern syntax. The explicit NULL arm
      // preserves CEL missing-attribute errors under negation, and contains("")
      // remains true for every present receiver.
      const body = sql`when ${needleExpr} = '' then true else length(replace(${receiverExpr}, ${needleExpr}, '')) < length(${receiverExpr}) end`;
      const nullGuard = buildNullGuard(receiver, needle);
      return nullGuard
        ? sql`(case when ${nullGuard} then null ${body})`
        : sql`(case ${body})`;
    }
    case "startsWith":
      return sql`substr(${receiverExpr}, 1, ${length(needleExpr)}) = ${needleExpr}`;
    case "endsWith":
      return sql`substr(${receiverExpr}, ${length(receiverExpr)} - ${length(needleExpr)} + 1) = ${needleExpr}`;
  }
};

// -- comparisons ---------------------------------------------------------------------------------

/**
 * The definite spelling of an equality between operands that can hold an explicit null.
 *
 * Deliberately not `IS [NOT] DISTINCT FROM`. Two reasons, and the second is the load-bearing one:
 * SQLite spells it `IS`/`IS NOT`, MySQL `<=>` and only PostgreSQL takes the standard form, so the
 * adapter would need a dialect it does not otherwise know — and, more importantly, a null-safe
 * equality is SYMMETRIC while this rewrite must not be. When only ONE side declares the
 * convention, the other side's NULL is a MISSING attribute on the check side, so CEL raises an
 * error and denies; only the asymmetric expansion below keeps propagating UNKNOWN for it. A
 * null-safe operator would match the two NULLs and over-grant.
 */
const definiteEquality = (
  leftExpr: SQL,
  rightExpr: SQL,
  leftExplicitNull: boolean,
  rightExplicitNull: boolean,
): SQL => {
  const nullMatches: SQL[] = [];
  const bothPresent: SQL[] = [];
  if (leftExplicitNull) {
    nullMatches.push(sql`${leftExpr} is null`);
    bothPresent.push(sql`${leftExpr} is not null`);
  }
  if (rightExplicitNull) {
    nullMatches.push(sql`${rightExpr} is null`);
    bothPresent.push(sql`${rightExpr} is not null`);
  }
  // Two nulls are EQUAL in CEL, so a row where every explicit-null operand is NULL matches.
  const bothNull = and(...nullMatches);
  const present = and(...bothPresent, sql`${leftExpr} = ${rightExpr}`);
  const combined =
    leftExplicitNull && rightExplicitNull ? or(bothNull, present) : present;
  if (!combined) {
    throw new Error("Unable to combine null-aware equality conditions");
  }
  return combined;
};

/** `left <op> right`, or `undefined` for an operator that is not a binary comparison. */
const binaryComparison = (
  operator: ComparisonOperator,
  left: unknown,
  right: unknown,
): SQL | undefined => {
  switch (operator) {
    case "eq":
      return sql`${left} = ${right}`;
    case "ne":
      return sql`${left} <> ${right}`;
    case "lt":
      return sql`${left} < ${right}`;
    case "le":
      return sql`${left} <= ${right}`;
    case "gt":
      return sql`${left} > ${right}`;
    case "ge":
      return sql`${left} >= ${right}`;
    default:
      return undefined;
  }
};

/** A comparison between two SQL expressions, optionally declared explicit-null on either side. */
export const applyComparisonWithExpression = (
  operator: ComparisonOperator,
  fieldExpr: SQL,
  valueExpr: SQL,
  fieldExplicitNull = false,
  valueExplicitNull = false,
): SQL => {
  const isEquality = operator === "eq" || operator === "ne";
  // Mixing the two conventions across one comparison has no faithful rendering. The declared side
  // needs a definite answer for its NULL (CEL holds a null VALUE); the undeclared side needs
  // UNKNOWN for its NULL (a missing attribute, which CEL denies under both polarities). A definite
  // predicate returns rows the PDP refuses; a plain one drops rows the PDP allows. Refuse it
  // rather than pick a direction — declare both attributes, or neither.
  if (isEquality && fieldExplicitNull !== valueExplicitNull) {
    throw new Error(
      `Cannot translate \`${operator}\` between two columns under mixed null conventions: ` +
        "cannot compare an attribute declared explicit-null with one on the omitted convention: the omitted side is UNKNOWN for a NULL column while the declared side is definite, and no single predicate is both. Declare nullAttributeRepresentation on both mapper entries, or on neither.",
    );
  }
  if (isEquality && fieldExplicitNull && valueExplicitNull) {
    const equality = definiteEquality(fieldExpr, valueExpr, true, true);
    return operator === "eq" ? equality : not(equality);
  }
  const comparison = binaryComparison(operator, fieldExpr, valueExpr);
  if (!comparison) {
    throw new Error(
      `Operator '${operator}' is not supported for expression-valued operands`,
    );
  }
  return comparison;
};

/**
 * Guards every site that would emit a NULL-selecting predicate out of a `null` comparison
 * operand.
 *
 * Under the `"omitted"` representation a NULL column carries no attribute, so CEL raises a
 * missing-attribute error and `check()` denies the row — `IS NULL` would return exactly the rows
 * the PDP refuses. The rejection is deliberately wider than the over-granting shapes: `ne(x,
 * null)` on its own is aligned, but negation is applied by wrapping the built condition rather
 * than by pushing it into the leaf, so a leaf cannot tell whether an enclosing `not` will flip
 * `IS NOT NULL` back into a NULL-selecting predicate. Rejecting every null operand is correct
 * under any nesting; narrowing it requires negation-parity tracking.
 *
 * `declared` is the convention on the mapper entry, which overrides the call-level option.
 */
export const assertNullOperandTranslatable = (
  context: string,
  options: BuildFilterOptions,
  declared?: NullAttributeRepresentation,
): void => {
  if ((declared ?? options.nullRepresentation) === "omitted") {
    throw new Error(
      `Cannot translate ${context} under nullAttributeRepresentation "omitted": a NULL column ` +
        "sends no attribute, so Cerbos evaluates the comparison as a missing-attribute error " +
        "(deny) while a NULL-selecting filter would return those rows. Send NULL columns as " +
        'explicit nulls and use "explicit", or keep this shape out of the policy.',
    );
  }
};

/** A whole relation compared as a value: it is never equal to a constant. */
const applyRelationComparison = (operator: ComparisonOperator): SQL => {
  switch (operator) {
    case "eq":
    case "in":
      return FALSE_CONDITION;
    case "ne":
      return TRUE_CONDITION;
    default:
      throw new Error(
        `Unsupported operator '${operator}' for relation comparison`,
      );
  }
};

/** `column IN (values)`, with CEL's reading of a null element and of an explicit-null column. */
const buildColumnMembership = (
  column: AnyColumn,
  value: Value,
  explicitNull: boolean,
): SQL => {
  const values = Array.isArray(value) ? value : [value];
  if (values.length === 0) {
    return FALSE_CONDITION;
  }
  const nonNullValues = values.filter((candidate) => candidate !== null);
  if (nonNullValues.length === 0) {
    return isNull(column);
  }
  const membership = sql`${column} in ${nonNullValues.map((candidate) =>
    bindAgainstColumn(candidate, column),
  )}`;
  if (nonNullValues.length === values.length) {
    // No null element, so nothing has made the predicate definite yet: `NOT (col IN (…))`
    // over a NULL column is UNKNOWN and drops the row, while CEL compares a null VALUE
    // against each element and gets a definite false. A null element takes the branch
    // below instead, where the `IS NULL` disjunct already settles it.
    if (!explicitNull) {
      return membership;
    }
    const present = and(sql`${column} is not null`, membership);
    if (!present) {
      throw new Error("Unable to combine null-aware membership conditions");
    }
    return present;
  }
  const withNull = or(membership, isNull(column));
  if (!withNull) {
    throw new Error("Unable to combine null-aware membership conditions");
  }
  return withNull;
};

/** A comparison between one column and one constant. */
const applyColumnComparison = (
  column: AnyColumn,
  operator: ComparisonOperator,
  value: Value,
  explicitNull: boolean,
): SQL => {
  const bound = bindAgainstColumn(value, column);

  // An attribute the caller sends as an explicit null holds a null VALUE in CEL, so equality
  // against a non-null operand is FALSE and inequality is TRUE — both definite. SQL's UNKNOWN
  // excludes the row under BOTH polarities, so the predicate has to be made definite here; a
  // fix confined to `ne` would leave `!(col = x)` still dropping the row.
  if (explicitNull && value !== null && (operator === "eq" || operator === "ne")) {
    const equality = definiteEquality(sql`${column}`, sql`${bound}`, true, false);
    return operator === "eq" ? equality : not(equality);
  }

  switch (operator) {
    case "eq":
      return value === null ? isNull(column) : sql`${column} = ${bound}`;
    case "ne":
      return value === null ? not(isNull(column)) : sql`${column} <> ${bound}`;
    case "in":
      return buildColumnMembership(column, value, explicitNull);
    case "contains":
    case "startsWith":
    case "endsWith":
      if (typeof value !== "string") {
        throw new Error(`The '${operator}' operator requires a string value`);
      }
      return buildStringMatchCondition(
        operator,
        columnExpression(sql`${column}`),
        constantExpression(sql`${value}`),
        characterLength([column]),
      );
  }
  const ordering = binaryComparison(operator, column, bound);
  if (!ordering) {
    throw new Error(`Unsupported operator: ${operator}`);
  }
  return ordering;
};

/**
 * Compare a mapped reference with a constant. A transform (bare function or `transform`) owns
 * the whole comparison; a column gets the adapter's own rendering.
 */
export const applyComparison = (
  mapping: BaseMapperEntry,
  operator: ComparisonOperator,
  value: Value,
  options: BuildFilterOptions,
  inherited?: NullAttributeRepresentation,
): SQL => {
  // The declaration lives on the mapper entry, but the entry unwraps to a bare column one frame
  // down, so it has to be carried rather than re-read.
  const declared = mappingNullRepresentation(mapping) ?? inherited;
  if (value === null) {
    assertNullOperandTranslatable(
      `\`${operator}\` against a null operand`,
      options,
      declared,
    );
  } else if (Array.isArray(value) && value.includes(null)) {
    assertNullOperandTranslatable(
      "a null element in an `in` list",
      options,
      declared,
    );
  }
  if (isRelationValue(mapping)) {
    return applyRelationComparison(operator);
  }
  if (typeof mapping === "function") {
    return mapping({ operator, value });
  }
  if (isMappingConfig(mapping)) {
    if (mapping.relation) {
      throw new Error("Relation mappings must be resolved before comparison");
    }
    if (mapping.transform) {
      return mapping.transform({ operator, value });
    }
    if (!mapping.column) {
      throw new Error("Mapping configuration requires a column or transform");
    }
    return applyComparison(mapping.column, operator, value, options, declared);
  }
  if (!isColumn(mapping)) {
    throw new Error("Expected a column mapping");
  }
  return applyColumnComparison(
    mapping,
    operator,
    value,
    declared === "explicit",
  );
};
