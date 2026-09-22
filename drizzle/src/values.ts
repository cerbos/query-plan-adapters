import type { PlanExpressionOperand } from "@cerbos/core";
import { is, sql } from "drizzle-orm";
import type { AnyColumn, SQL } from "drizzle-orm";
import { MySqlColumn } from "drizzle-orm/mysql-core";

import { ARITHMETIC_OPERATORS } from "./arithmetic";
import { buildFilteredCount } from "./collections";
import { buildFilterFromExpression } from "./filter";
import { resolveIndexedColumn } from "./indexed";
import {
  buildColumnExpression,
  columnForOperand,
  isMappingConfig,
  resolveFieldReference,
} from "./mapper";
import {
  isExpressionOperand,
  isNameOperand,
  isOperatorCall,
  isStringConversion,
  isValueOperand,
} from "./operands";
import { bindConstant, characterLength } from "./predicates";
import {
  chainCorrelation,
  requireLeadingHops,
  resolveTableName,
} from "./relations";
import { normalizeRfc3339Milliseconds } from "./timestamp";
import type { BuildFilterOptions, Mapper, RelationMapping } from "./types";

/**
 * Operands in VALUE position — the sides of a comparison: constants, columns, arithmetic,
 * ternaries, `size()`, `timestamp()` and the one conversion the adapter can lower.
 */

/**
 * Every CEL conversion, and why SQL `CAST` cannot reproduce it here. Only one is lowered — `string()`
 * over a boolean column, which needs no cast at all (see `buildBooleanString`). The adapter renders
 * through whichever Drizzle dialect the CALLER hands its query to, which is what lets one
 * translation serve SQLite, PostgreSQL and MySQL — and a cast is exactly the place where those
 * three disagree.
 *
 * `int()` / `double()` (cerbos/query-plan-adapters#311): CEL reads a WHOLE string or raises an
 * error, and an error denies the row. SQL reads whatever prefix parses — `CAST('100%_done' AS
 * INTEGER)` is `100` on SQLite, `0` on MySQL and a hard error on PostgreSQL — so a direct lowering
 * returns rows the PDP denies. The numeric direction is no safer: CEL's `int()` truncates toward
 * zero, SQLite's CAST truncates, but PostgreSQL and MySQL round to nearest, so `int(-0.6)` is `0`
 * to CEL and `-1` to those engines. Nothing in the plan says what type the column holds, so the
 * adapter cannot pick a faithful lowering per row.
 *
 * `string()` (cerbos/query-plan-adapters#340): there is no cast TARGET the three stores share.
 * This one used to be lowered to `CAST(... AS TEXT)` for numeric and text columns, on the stated
 * grounds that the rendering was "measured against the pinned images" — it was measured against
 * two of them. `TEXT` is not a MySQL cast target at all: `CAST(-0.6 AS TEXT)` is `ERROR 1064` on
 * MySQL 8.4, which spells the same conversion `CAST(-0.6 AS CHAR)`. Nor is `VARCHAR`. And `CHAR`
 * is `character(1)` on PostgreSQL, where `CAST(-0.6 AS CHAR)` is `'-'` — a filter that silently
 * matches nothing rather than failing. A BOOLEAN column is the exception, and it is not a cast:
 * SQLite and MySQL hold a boolean as 1/0, so any CAST renders `"1"` where CEL renders `"true"`,
 * but a CASE spells CEL's two words on every store (cerbos/query-plan-adapters#418).
 *
 * `ent` translates `string()` and is not a counter-example: its `render.go` branches on a dialect
 * the caller declares through `WithDialect`, so it emits `CHAR` on MySQL and `TEXT` elsewhere. The
 * limitation here is the absent dialect, not the absent cast.
 */
const NUMERIC_CONVERSION_REFUSAL =
  "SQL CAST does not reproduce CEL conversion semantics — it reads a numeric prefix where CEL " +
  "requires the whole string and raises otherwise, and PostgreSQL and MySQL round where CEL " +
  "truncates toward zero. The adapter rejects the shape instead of returning rows the PDP denies";

const UNSUPPORTED_CONVERSIONS: Record<string, string> = {
  int: NUMERIC_CONVERSION_REFUSAL,
  double: NUMERIC_CONVERSION_REFUSAL,
  string:
    "no SQL CAST target spells it on every store this adapter supports — TEXT and VARCHAR are " +
    "syntax errors on MySQL, which spells it CHAR, and CHAR is character(1) on PostgreSQL, where " +
    "the cast would silently match nothing. The adapter does not know its dialect by design, so " +
    "it rejects the shape instead of emitting a filter that is correct on one store only",
};

/**
 * CEL's `string()` over a boolean column, lowered through a CASE rather than a CAST
 * (cerbos/query-plan-adapters#418):
 *
 *   CASE WHEN col IS NULL THEN NULL WHEN col THEN 'true' ELSE 'false' END
 *
 * A CASE needs no cast target, so the reason every other `string()` is refused does not apply, and
 * SQLite and MySQL (which store 1/0) read a boolean column as a condition exactly as PostgreSQL
 * reads a real `boolean`. One rendering spells CEL's own two words on all three stores, where
 * `CAST(col AS TEXT)` renders `"1"` on two of them.
 *
 * The IS NULL arm is load-bearing. A NULL boolean is a missing attribute — or, declared explicit,
 * a null value — and CEL has no `string()` for either: it raises, and the PDP denies. Without the
 * arm `WHEN col` is UNKNOWN for a NULL column, the CASE falls through to its ELSE and yields
 * `'false'`, so `string(x) != "true"` would return a row the PDP denies. With it the result is
 * NULL, and the row stays out under both polarities.
 *
 * On MySQL the two literals carry `COLLATE utf8mb4_0900_bin`, because a literal compares in the
 * CONNECTION's collation rather than any column's or the server's. mysql2's default is
 * `utf8mb4_unicode_ci`, under which `'true' = 'TRUE'` and `'true' = 'true '` are both TRUE even on
 * a server started case-sensitive — measured against the MySQL 8.4 image the adversarial leg pins
 * (`MYSQL_IMAGE` in `adversarial.test.ts`) through that leg's own client, not inferred.
 * `utf8mb4_0900_bin` is the collation that is byte-exact AND NO PAD: `utf8mb4_bin` is PAD SPACE,
 * and `utf8mb4_0900_as_cs` ignores a soft hyphen, so `'tr­ue' = 'true'` is TRUE under it.
 * The `_utf8mb4` introducer fixes the literals' character set, so the COLLATE is valid whatever
 * the connection's is. It needs MySQL 8.0.17, the floor
 * `CAST(… AS FLOAT(53))` already sets. SQLite compares the literals BINARY and PostgreSQL in its
 * deterministic database collation, so neither needs one.
 *
 * The dialect is read off the column's own Drizzle class, as `indexed.ts` does; the caller still
 * declares none.
 */
const buildBooleanString = (column: AnyColumn, expr: SQL): SQL => {
  const [whenTrue, whenFalse] = is(column, MySqlColumn)
    ? [
        sql`_utf8mb4'true' collate utf8mb4_0900_bin`,
        sql`_utf8mb4'false' collate utf8mb4_0900_bin`,
      ]
    : [sql`'true'`, sql`'false'`];
  return sql`(case when ${expr} is null then null when ${expr} then ${whenTrue} else ${whenFalse} end)`;
};

/**
 * The boolean column a `string()` operand converts, if it converts one; anything else stays
 * refused with the `UNSUPPORTED_CONVERSIONS` message.
 */
const booleanStringColumn = (
  operands: PlanExpressionOperand[],
  mapper: Mapper,
): AnyColumn | undefined => {
  const [inner] = operands;
  if (operands.length !== 1 || !inner) {
    return undefined;
  }
  const column = columnForOperand(inner, mapper);
  return column?.dataType === "boolean" ? column : undefined;
};

/**
 * Whether an `add` is CEL's string overload rather than its numeric one.
 *
 * One string operand settles it: CEL has no mixed-type `+`, so a string on either side means the
 * whole expression is a concatenation.
 */
const isStringConcatenation = (
  operands: PlanExpressionOperand[],
  mapper: Mapper,
): boolean =>
  operands.some((operand) => {
    if (isValueOperand(operand)) {
      return typeof operand.value === "string";
    }
    // `string(flag) + string(flag)` is CEL's string `+`. Read as arithmetic it would coerce both
    // CASE results to 0 on SQLite and MySQL and compare `0` with the other side.
    if (isStringConversion(operand)) {
      return true;
    }
    return columnForOperand(operand, mapper)?.dataType === "string";
  });

const buildArithmeticExpression = (
  operator: string,
  operands: PlanExpressionOperand[],
  mapper: Mapper,
  options: BuildFilterOptions,
): SQL => {
  if (operands.length !== 2) {
    throw new Error(`Arithmetic operator '${operator}' requires two operands`);
  }
  const [leftOperand, rightOperand] = operands;
  if (!leftOperand || !rightOperand) {
    throw new Error(`Arithmetic operator '${operator}' is missing operands`);
  }
  if (
    operator === "div" &&
    isValueOperand(leftOperand) &&
    typeof leftOperand.value === "number" &&
    isValueOperand(rightOperand) &&
    typeof rightOperand.value === "number"
  ) {
    // Fold pure-constant division in JavaScript's IEEE double space. SQL engines
    // disagree on division by zero (SQLite returns NULL, PostgreSQL throws), while
    // CEL produces +/-Infinity or NaN. Binding the folded value preserves ordering
    // semantics; SQLite represents bound NaN as NULL, whose comparisons are never true.
    return sql`${leftOperand.value / rightOperand.value}`;
  }
  const left = buildValueExpression(leftOperand, mapper, options);
  const right = buildValueExpression(rightOperand, mapper, options);
  if (operator === "div") {
    // CEL attribute arithmetic is double-typed: force REAL division so an
    // INTEGER/INTEGER pair does not silently truncate (3 / 2 must be 1.5, not 1).
    // The comparison builder handles non-finite results in separate IEEE arms;
    // this expression supplies its finite branch.
    return sql`(cast(${left} as float(53)) / ${right})`;
  }
  if (operator === "add" && isStringConcatenation(operands, mapper)) {
    // CEL overloads `+` on strings; SQL does not agree on how to spell that. `||` concatenates
    // on SQLite and PostgreSQL but is LOGICAL OR on MySQL unless PIPES_AS_CONCAT is set, and
    // MySQL's own spelling is CONCAT(). This adapter deliberately does not know its dialect
    // (see `definiteEquality`), so there is no rendering it can prove correct everywhere —
    // and the numeric `+` it would otherwise emit is silently wrong rather than a syntax
    // error: SQLite and MySQL coerce 'prefix:' to 0, so the comparison quietly matches
    // nothing (cerbos/query-plan-adapters#376).
    throw new Error(
      "Cannot translate string concatenation: CEL's + over strings has no dialect-independent " +
        "SQL spelling — || concatenates on SQLite and PostgreSQL but is logical OR on MySQL, " +
        "which spells it CONCAT() — and the numeric + this adapter emits for arithmetic would " +
        "coerce the operands to 0 rather than fail",
    );
  }
  return sql`(${left} ${sql.raw(ARITHMETIC_OPERATORS[operator]!)} ${right})`;
};

/** `size()` of a relation (a correlated COUNT), a filtered relation, or a string column. */
const buildSizeExpression = (
  operand: PlanExpressionOperand,
  mapper: Mapper,
  options: BuildFilterOptions,
): SQL => {
  if (isOperatorCall(operand, "filter")) {
    return buildFilteredCount(operand, mapper, options);
  }
  if (!isNameOperand(operand)) {
    throw new Error(
      "'size' operator requires a field reference or filter expression",
    );
  }
  const resolved = resolveFieldReference(operand.name, mapper);
  // Relation: a correlated COUNT subquery over the tail of the relation chain, joining THROUGH
  // every intermediate hop (never straight off the root).
  if (resolved.relations.length > 0) {
    const primary = resolved.relations[resolved.relations.length - 1]!;
    const leading = resolved.relations.slice(0, -1);
    const tableName = resolveTableName(primary.table, operand.name);
    const chainWhere = chainCorrelation(primary, leading, operand.name, options);
    // An absent to-one parent must count as UNKNOWN, not 0: `size(chain) == 0` and
    // `size(chain) >= 0` are both TRUE over an empty count and would return every
    // parentless row (#309).
    return requireLeadingHops(
      leading,
      sql`(select count(*) from ${sql.identifier(tableName)} where ${chainWhere})`,
      operand.name,
      options,
    );
  }
  // A non-string column has no CEL size(): the comparison is a no-overload error, so UNKNOWN.
  const scalarColumn = columnForOperand(operand, mapper);
  if (scalarColumn && scalarColumn.dataType !== "string") return sql`null`;
  return characterLength([scalarColumn])(
    buildColumnExpression(resolved.mapping, operand.name),
  );
};

const buildTimestampExpression = (
  operands: PlanExpressionOperand[],
  mapper: Mapper,
  options: BuildFilterOptions,
): SQL => {
  if (operands.length !== 1) {
    throw new Error("'timestamp' operator requires exactly one operand");
  }
  const inner = operands[0];
  if (!inner) {
    throw new Error("'timestamp' operator is missing its operand");
  }
  if (isNameOperand(inner)) {
    const resolved = resolveFieldReference(inner.name, mapper);
    if (
      !isMappingConfig(resolved.mapping) ||
      resolved.mapping.valueType !== "timestamp"
    ) {
      throw new Error(
        `'timestamp' field '${inner.name}' requires a mapping with valueType: "timestamp"`,
      );
    }
    return buildValueExpression(inner, mapper, options);
  }
  if (!isValueOperand(inner) || typeof inner.value !== "string") {
    throw new Error(
      "'timestamp' requires an RFC-3339 string value or field reference",
    );
  }
  // Normalize offsets so instant equality is preserved. Removing the zero
  // millisecond suffix matches the canonical UTC strings used by timestamp columns.
  return sql`${normalizeRfc3339Milliseconds(inner.value)}`;
};

/** An operand in value position, rendered as a SQL scalar expression. */
export const buildValueExpression = (
  operand: PlanExpressionOperand,
  mapper: Mapper,
  options: BuildFilterOptions,
): SQL => {
  if (isValueOperand(operand)) {
    return bindConstant(operand.value);
  }
  if (isNameOperand(operand)) {
    const resolved = resolveFieldReference(operand.name, mapper);
    // Relations already established by an enclosing lambda subquery (skipRelations) leave
    // the element column directly addressable; anything else cannot be a scalar.
    const unskipped = resolved.relations.filter(
      (relation) => !options.skipRelations?.has(relation),
    );
    if (unskipped.length > 0) {
      throw new Error(
        `Cannot use relation '${operand.name}' as a scalar value expression`,
      );
    }
    return buildColumnExpression(resolved.mapping, operand.name);
  }
  if (!isExpressionOperand(operand)) {
    throw new Error("Invalid value-expression operand");
  }

  const { operator, operands } = operand;

  if (operator in ARITHMETIC_OPERATORS) {
    return buildArithmeticExpression(operator, operands, mapper, options);
  }

  if (operator === "string") {
    const column = booleanStringColumn(operands, mapper);
    if (column !== undefined) {
      return buildBooleanString(
        column,
        buildValueExpression(operands[0]!, mapper, options),
      );
    }
  }

  const unsupportedConversion = UNSUPPORTED_CONVERSIONS[operator];
  if (unsupportedConversion !== undefined) {
    throw new Error(`Cannot translate ${operator}(): ${unsupportedConversion}`);
  }

  switch (operator) {
    case "if": {
      if (operands.length !== 3) {
        throw new Error("'if' operator requires exactly three operands");
      }
      const cond = buildFilterFromExpression(operands[0]!, mapper, options);
      const thenExpr = buildValueExpression(operands[1]!, mapper, options);
      const elseExpr = buildValueExpression(operands[2]!, mapper, options);
      // Two guarded WHEN arms (no bare ELSE): an UNKNOWN condition matches neither arm and
      // the CASE yields NULL, so the enclosing comparison stays UNKNOWN — excluded under
      // both polarities, mirroring the CEL missing-attribute error (deny). A bare ELSE
      // would silently route UNKNOWN rows into the else branch.
      return sql`(case when ${cond} then ${thenExpr} when not (${cond}) then ${elseExpr} end)`;
    }
    case "size":
      if (operands.length !== 1) {
        throw new Error("'size' operator requires exactly one operand");
      }
      return buildSizeExpression(operands[0]!, mapper, options);
    case "timestamp":
      return buildTimestampExpression(operands, mapper, options);
    case "index":
      resolveIndexedColumn(operands, mapper, options);
      throw new Error(
        "Indexed values support only direct eq/ne comparisons with scalar literals; nested value expressions cannot preserve element types and index errors",
      );
    default:
      throw new Error(`Unsupported value-expression operator: ${operator}`);
  }
};

/** A scalar SQL expression plus the relation chain its column lives behind (empty if none). */
export interface ResolvedScalarOperand {
  expr: SQL;
  relations: RelationMapping[];
}

/**
 * Resolve an operand into a scalar SQL expression. Unlike `buildValueExpression`, a field behind
 * a relation is allowed: its chain is returned for the caller to wrap the predicate in.
 */
export const resolveScalarOperand = (
  operand: PlanExpressionOperand,
  mapper: Mapper,
  options: BuildFilterOptions,
): ResolvedScalarOperand => {
  if (isValueOperand(operand)) {
    return { expr: bindConstant(operand.value), relations: [] };
  }
  if (isNameOperand(operand)) {
    const resolved = resolveFieldReference(operand.name, mapper);
    return {
      expr: buildColumnExpression(resolved.mapping, operand.name),
      relations: resolved.relations,
    };
  }
  return {
    expr: buildValueExpression(operand, mapper, options),
    relations: [],
  };
};
