import type { PlanExpressionOperand } from "@cerbos/core";
import { is, sql } from "drizzle-orm";
import type { AnyColumn, SQL } from "drizzle-orm";
import { MySqlColumn } from "drizzle-orm/mysql-core";
import { PgColumn } from "drizzle-orm/pg-core";
import { SQLiteColumn } from "drizzle-orm/sqlite-core";

import { UnsupportedQueryPlanError } from "./errors";
import { ARITHMETIC_OPERATORS, resolveConstantNumber } from "./arithmetic";
import { buildExceptCount, buildFilteredCount } from "./collections";
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
 * ternaries, `size()`, `timestamp()` and the `string()` shapes that need no cast.
 */

/**
 * Every CEL conversion, and why SQL `CAST` cannot reproduce it here. No conversion is lowered to a
 * CAST: the adapter renders through whichever Drizzle dialect the CALLER hands its query to, which
 * is what lets one translation serve SQLite, PostgreSQL and MySQL — and a cast is exactly the place
 * where those three disagree. The `string()` shapes that translate need none (see below).
 *
 * `int()` / `double()` (cerbos/query-plan-adapters#311): CEL reads a WHOLE string or raises an
 * error, and an error denies the row. SQL reads whatever prefix parses — `CAST('100%_done' AS
 * INTEGER)` is `100` on SQLite, `0` on MySQL and a hard error on PostgreSQL — so a direct lowering
 * returns rows the PDP denies. The numeric direction is no safer: CEL's `int()` truncates toward
 * zero, SQLite's CAST truncates, but PostgreSQL and MySQL round to nearest, so `int(-0.6)` is `0`
 * to CEL and `-1` to those engines. So `int()` is lowered only where the column says what it
 * holds: over a whole-number column it is the column (`buildIntegerConversion`); over a double or
 * string column it validates and truncates the way cel-go does, per dialect, and only as a direct
 * comparison with a small constant (`buildCheckedIntComparison`). Every other conversion is refused.
 *
 * `string()` (cerbos/query-plan-adapters#340): there is no cast TARGET the three stores share.
 * `TEXT` is not a MySQL cast target at all: `CAST(-0.6 AS TEXT)` is `ERROR 1064` on MySQL 8.4, which
 * spells the same conversion `CAST(-0.6 AS CHAR)`. Nor is `VARCHAR`. And `CHAR` is `character(1)`
 * on PostgreSQL, where `CAST(-0.6 AS CHAR)` is `'-'`. Even a per-dialect target would render a
 * number in the store's format, not CEL's: SQLite's `CAST(2.0 AS TEXT)` is `'2.0'`, CEL's is `"2"`.
 * So three shapes are lowered WITHOUT a cast, and every other `string()` is refused:
 *
 * - over a string column it is the identity;
 * - over a boolean column it is a CASE spelling CEL's two words (`buildBooleanString`, #418);
 * - over a number column compared for (in)equality with a string constant, the comparison is
 *   inverted into a numeric one against the one double CEL spells that way
 *   (`buildNumberStringComparison` in `comparison.ts`).
 */
const NUMERIC_CONVERSION_REFUSAL =
  "SQL CAST does not reproduce CEL conversion semantics — it reads a numeric prefix where CEL " +
  "requires the whole string and raises otherwise, and PostgreSQL and MySQL round where CEL " +
  "truncates toward zero. The adapter rejects the shape instead of returning rows the PDP denies";

const UNSUPPORTED_CONVERSIONS: Record<string, string> = {
  int: NUMERIC_CONVERSION_REFUSAL,
  double: NUMERIC_CONVERSION_REFUSAL,
  string:
    "only string() over a string or boolean column, or string() over a number column compared " +
    "for equality with a string constant, is lowered — each without a CAST. A CAST would render " +
    "the number in the store's own format rather than CEL's (SQLite spells 2.0 as '2.0' where " +
    "CEL spells it '2'), and no cast target is shared by every store: TEXT is a syntax error on " +
    "MySQL, whose CHAR is character(1) on PostgreSQL",
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
 * The Drizzle column types that can only hold whole numbers inside int64's range, keyed by
 * `columnType`. A `bigint` column in `bigint` mode is not among them: its values are not numbers.
 */
const INTEGER_COLUMN_TYPES = new Set([
  "SQLiteInteger",
  "PgInteger",
  "PgSmallInt",
  "PgBigInt53",
  "PgSerial",
  "PgSmallSerial",
  "PgBigSerial53",
  "MySqlInt",
  "MySqlTinyInt",
  "MySqlSmallInt",
  "MySqlMediumInt",
  "MySqlBigInt53",
  "MySqlSerial",
]);

/**
 * The integer column an `int()` operand converts, if it converts one. CEL's `int()` over a whole
 * number is that number, so no CAST is needed and the rounding every other `int()` would inherit
 * from PostgreSQL and MySQL never arises.
 */
const integerConversionColumn = (
  operand: PlanExpressionOperand,
  mapper: Mapper,
): AnyColumn | undefined => {
  if (!isOperatorCall(operand, "int") || !isExpressionOperand(operand)) return undefined;
  const [inner] = operand.operands;
  if (operand.operands.length !== 1 || inner === undefined) return undefined;
  const column = columnForOperand(inner, mapper);
  return column !== undefined && INTEGER_COLUMN_TYPES.has(column.columnType)
    ? column
    : undefined;
};

/**
 * `int()` over an integer column. On PostgreSQL and MySQL the column's type guarantees a whole
 * number, so it is the column itself. SQLite's INTEGER affinity does not: a value with a fraction
 * is kept as a REAL, so the conversion is spelled `CAST(… AS INTEGER)`, which on SQLite (and only
 * there) truncates toward zero exactly as CEL's `int()` does.
 */
const buildIntegerConversion = (column: AnyColumn, expr: SQL): SQL =>
  is(column, SQLiteColumn) ? sql`cast(${expr} as integer)` : expr;

/** The store a column belongs to, read off its Drizzle class. */
const columnDialect = (column: AnyColumn): "sqlite" | "postgresql" | "mysql" | undefined =>
  is(column, SQLiteColumn)
    ? "sqlite"
    : is(column, PgColumn)
      ? "postgresql"
      : is(column, MySqlColumn)
        ? "mysql"
        : undefined;

/** 2^63, the first double cel-go's `int()` refuses in either direction (`doubleToInt64Checked`). */
const INT64_BOUND = 9223372036854775808;

/**
 * `int()` over a double column: cel-go refuses a value at or beyond ±2^63 (and NaN or an
 * infinity) and otherwise truncates toward zero. Each store has an exact truncation — PostgreSQL's
 * `trunc`, MySQL's `TRUNCATE(x, 0)`, and SQLite's `CAST(… AS INTEGER)`, which truncates there (and
 * only there) — and the CASE leaves the refused values NULL, CEL's error.
 */
const buildDoubleToInt = (column: AnyColumn, expr: SQL): SQL | undefined => {
  const dialect = columnDialect(column);
  const truncated =
    dialect === "postgresql"
      ? sql`trunc(${expr})`
      : dialect === "mysql"
        ? sql`truncate(${expr}, 0)`
        : dialect === "sqlite"
          ? sql`cast(${expr} as integer)`
          : undefined;
  if (truncated === undefined) return undefined;
  return sql`(case when ${expr} > ${bindConstant(-INT64_BOUND)} and ${expr} < ${bindConstant(INT64_BOUND)} then ${truncated} end)`;
};

/**
 * `int()` over a string column: cel-go's `strconv.ParseInt(s, 10, 64)` — an optional sign, then one
 * or more ASCII digits and nothing else, within int64 — or an error. SQL's own CAST reads a
 * numeric prefix instead (`'100%_done'` is 100 on SQLite, 0 on MySQL, an error on PostgreSQL), so
 * the string is validated first and cast only when valid; anything else is NULL, CEL's error.
 *
 * Validation needs no regex: removing every digit from the unsigned part must leave nothing, and
 * once leading zeros are trimmed the digits must be fewer than 19, or exactly 19 and no greater
 * than int64's bound — for equal-length digit strings, string order is numeric order.
 */
const buildStringToInt = (column: AnyColumn, expr: SQL): SQL | undefined => {
  const dialect = columnDialect(column);
  if (dialect === undefined) return undefined;
  const sign = sql`substr(${expr}, 1, 1)`;
  const unsigned = sql`(case when ${sign} in ('+', '-') then substr(${expr}, 2) else ${expr} end)`;
  const nonDigits = [..."0123456789"].reduce<SQL>(
    (rest, digit) => sql`replace(${rest}, ${digit}, '')`,
    unsigned,
  );
  const significant =
    dialect === "mysql" ? sql`trim(leading '0' from ${unsigned})` : sql`ltrim(${unsigned}, '0')`;
  const bound = sql`(case when ${sign} = '-' then '9223372036854775808' else '9223372036854775807' end)`;
  const valid = sql`length(${unsigned}) > 0 and length(${nonDigits}) = 0 and (length(${significant}) < 19 or (length(${significant}) = 19 and ${significant} <= ${bound}))`;
  const cast =
    dialect === "postgresql"
      ? sql`cast(${expr} as bigint)`
      : dialect === "mysql"
        ? sql`cast(${expr} as signed)`
        : sql`cast(${expr} as integer)`;
  return sql`(case when ${valid} then ${cast} end)`;
};

/** 2^53: below it a double holds every integer exactly, so a store's bigint-to-double is exact. */
const EXACT_DOUBLE_INTEGER_BOUND = 9007199254740992;

/**
 * `int(x) <op> constant` over a double or string column, where `x` is read through
 * `buildDoubleToInt` / `buildStringToInt`. Their result can reach ±2^63, so it is lowered ONLY as a
 * comparison with a number constant below 2^53 in magnitude: PostgreSQL and MySQL compare a bigint
 * with a double by converting the bigint, which is exact against such a constant — measured,
 * `int("9223372036854775807") == 9223372036854775808.0` is true on both where CEL says false — and
 * arithmetic on the result could overflow a bigint and fail the whole query. `undefined` when the
 * shape is anything else.
 */
export const buildCheckedIntComparison = (
  operator: "eq" | "ne" | "lt" | "le" | "gt" | "ge",
  conversion: PlanExpressionOperand,
  constant: PlanExpressionOperand,
  mapper: Mapper,
  options: BuildFilterOptions,
): SQL | undefined => {
  if (
    !isOperatorCall(conversion, "int") || !isExpressionOperand(conversion) ||
    conversion.operands.length !== 1 ||
    !isValueOperand(constant) || typeof constant.value !== "number" ||
    !(Math.abs(constant.value) < EXACT_DOUBLE_INTEGER_BOUND)
  ) {
    return undefined;
  }
  const inner = conversion.operands[0]!;
  const column = columnForOperand(inner, mapper);
  if (column === undefined || INTEGER_COLUMN_TYPES.has(column.columnType)) return undefined;
  const expr = () => buildValueExpression(inner, mapper, options);
  const converted =
    column.dataType === "number"
      ? buildDoubleToInt(column, expr())
      : column.dataType === "string"
        ? buildStringToInt(column, expr())
        : undefined;
  if (converted === undefined) return undefined;
  const symbol = { eq: "=", ne: "<>", lt: "<", le: "<=", gt: ">", ge: ">=" }[operator];
  return sql`(${converted} ${sql.raw(symbol)} ${bindConstant(constant.value)})`;
};

/**
 * CEL's `%` is integer-only: over a double it is a no-overload error, which denies the row, so
 * only an `int()` over an integer column is a dividend it can take. The divisor must be a non-zero
 * whole constant: CEL's `x % 0` is an error, which SQLite and MySQL answer NULL but PostgreSQL
 * raises. SQLite, PostgreSQL and MySQL all give the remainder the dividend's sign — truncated
 * division, as CEL does — so `-5 % 2` is `-1` on each.
 */
const buildModulo = (
  leftOperand: PlanExpressionOperand,
  rightOperand: PlanExpressionOperand,
  mapper: Mapper,
  options: BuildFilterOptions,
): SQL => {
  const dividend = integerConversionColumn(leftOperand, mapper);
  const divisor = resolveConstantNumber(rightOperand);
  if (
    dividend === undefined ||
    divisor === undefined ||
    !Number.isInteger(divisor) ||
    divisor === 0
  ) {
    throw new UnsupportedQueryPlanError(
      "Cannot translate '%': CEL's modulo is defined only over integers, so the adapter lowers " +
        "it only for int() of an integer column by a non-zero whole constant. A double operand " +
        "is a no-overload error in CEL, and a zero divisor raises on PostgreSQL",
    );
  }
  const left = buildValueExpression(leftOperand, mapper, options);
  return sql`(${left} % ${bindConstant(divisor)})`;
};

/**
 * Whether an `add` is CEL's string overload rather than its numeric one.
 *
 * One string operand settles it: CEL has no mixed-type `+`, so a string on either side means the
 * whole expression is a concatenation. A nested `+` is read the same way, so
 * `(a + b) + (c + d)` over string columns is never lowered to numeric `+`.
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
    if (isOperatorCall(operand, "add")) {
      return isStringConcatenation(operand.operands, mapper);
    }
    return columnForOperand(operand, mapper)?.dataType === "string";
  });

type ConcatenationDialect = "pipes" | "concat";

/**
 * How the store spells string `+`, read off the Drizzle class of every column the concatenation
 * reaches — as `indexed.ts` and `characterLength` do, so the caller still declares no dialect.
 * `||` concatenates on SQLite and PostgreSQL but is LOGICAL OR on MySQL unless PIPES_AS_CONCAT is
 * set, and MySQL's own spelling is CONCAT(). With no column of a known dialect in the tree — a
 * callback mapping, or columns of two dialects — there is nothing to decide by, so `undefined`.
 */
const concatenationDialect = (
  operands: PlanExpressionOperand[],
  mapper: Mapper,
): ConcatenationDialect | undefined => {
  const dialects = new Set<ConcatenationDialect>();
  const visit = (operand: PlanExpressionOperand): void => {
    if (isExpressionOperand(operand)) {
      operand.operands.forEach(visit);
      return;
    }
    const column = columnForOperand(operand, mapper);
    if (column === undefined) return;
    if (is(column, MySqlColumn)) dialects.add("concat");
    else if (is(column, PgColumn) || is(column, SQLiteColumn)) dialects.add("pipes");
  };
  operands.forEach(visit);
  return dialects.size === 1 ? [...dialects][0] : undefined;
};

/**
 * CEL's string `+`. Both spellings propagate NULL — `NULL || 'x'` and `CONCAT(NULL, 'x')` are
 * both NULL — so a missing operand leaves the enclosing comparison UNKNOWN and the row out under
 * either polarity, as CEL's missing-attribute error denies it. Two constants fold in JavaScript
 * and need no dialect at all.
 */
const buildConcatenation = (
  leftOperand: PlanExpressionOperand,
  rightOperand: PlanExpressionOperand,
  mapper: Mapper,
  options: BuildFilterOptions,
): SQL => {
  if (
    isValueOperand(leftOperand) &&
    typeof leftOperand.value === "string" &&
    isValueOperand(rightOperand) &&
    typeof rightOperand.value === "string"
  ) {
    return bindConstant(leftOperand.value + rightOperand.value);
  }
  const dialect = concatenationDialect([leftOperand, rightOperand], mapper);
  if (dialect === undefined) {
    // The numeric `+` the adapter would otherwise emit is silently wrong rather than a syntax
    // error: SQLite and MySQL coerce 'prefix:' to 0, so the comparison quietly matches nothing
    // (cerbos/query-plan-adapters#376).
    throw new UnsupportedQueryPlanError(
      "Cannot translate string concatenation: no Drizzle column of a single known dialect " +
        "among its operands says how to spell it — || concatenates on SQLite and PostgreSQL " +
        "but is logical OR on MySQL, which spells it CONCAT()",
    );
  }
  const left = buildValueExpression(leftOperand, mapper, options);
  const right = buildValueExpression(rightOperand, mapper, options);
  return dialect === "concat"
    ? sql`concat(${left}, ${right})`
    : sql`(${left} || ${right})`;
};

const buildArithmeticExpression = (
  operator: string,
  operands: PlanExpressionOperand[],
  mapper: Mapper,
  options: BuildFilterOptions,
): SQL => {
  if (operands.length !== 2) {
    throw new UnsupportedQueryPlanError(`Arithmetic operator '${operator}' requires two operands`);
  }
  const [leftOperand, rightOperand] = operands;
  if (!leftOperand || !rightOperand) {
    throw new UnsupportedQueryPlanError(`Arithmetic operator '${operator}' is missing operands`);
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
  if (operator === "add" && isStringConcatenation(operands, mapper)) {
    return buildConcatenation(leftOperand, rightOperand, mapper, options);
  }
  if (operator === "mod") {
    return buildModulo(leftOperand, rightOperand, mapper, options);
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
  if (isOperatorCall(operand, "except") && isExpressionOperand(operand)) {
    return buildExceptCount(operand, mapper, options);
  }
  if (!isNameOperand(operand)) {
    throw new UnsupportedQueryPlanError(
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
    throw new UnsupportedQueryPlanError("'timestamp' operator requires exactly one operand");
  }
  const inner = operands[0];
  if (!inner) {
    throw new UnsupportedQueryPlanError("'timestamp' operator is missing its operand");
  }
  if (isNameOperand(inner)) {
    const resolved = resolveFieldReference(inner.name, mapper);
    if (
      !isMappingConfig(resolved.mapping) ||
      resolved.mapping.valueType !== "timestamp"
    ) {
      throw new UnsupportedQueryPlanError(
        `'timestamp' field '${inner.name}' requires a mapping with valueType: "timestamp"`,
      );
    }
    return buildValueExpression(inner, mapper, options);
  }
  if (!isValueOperand(inner) || typeof inner.value !== "string") {
    throw new UnsupportedQueryPlanError(
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
      throw new UnsupportedQueryPlanError(
        `Cannot use relation '${operand.name}' as a scalar value expression`,
      );
    }
    return buildColumnExpression(resolved.mapping, operand.name);
  }
  if (!isExpressionOperand(operand)) {
    throw new UnsupportedQueryPlanError("Invalid value-expression operand");
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
    // `string()` of a string is the string itself: no cast, so no cast target to disagree on. A
    // NULL column stays NULL, leaving the comparison UNKNOWN under both polarities, as CEL's
    // error over a missing attribute or a null value denies it.
    const [inner] = operands;
    if (
      operands.length === 1 &&
      inner !== undefined &&
      columnForOperand(inner, mapper)?.dataType === "string"
    ) {
      return buildValueExpression(inner, mapper, options);
    }
  }

  const integerColumn = integerConversionColumn(operand, mapper);
  if (integerColumn !== undefined) {
    return buildIntegerConversion(
      integerColumn,
      buildValueExpression(operands[0]!, mapper, options),
    );
  }

  const unsupportedConversion = UNSUPPORTED_CONVERSIONS[operator];
  if (unsupportedConversion !== undefined) {
    throw new UnsupportedQueryPlanError(`Cannot translate ${operator}(): ${unsupportedConversion}`);
  }

  switch (operator) {
    case "if": {
      if (operands.length !== 3) {
        throw new UnsupportedQueryPlanError("'if' operator requires exactly three operands");
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
        throw new UnsupportedQueryPlanError("'size' operator requires exactly one operand");
      }
      return buildSizeExpression(operands[0]!, mapper, options);
    case "timestamp":
      return buildTimestampExpression(operands, mapper, options);
    case "index":
      resolveIndexedColumn(operands, mapper, options);
      throw new UnsupportedQueryPlanError(
        "Indexed values support only direct eq/ne comparisons with scalar literals; nested value expressions cannot preserve element types and index errors",
      );
    default:
      throw new UnsupportedQueryPlanError(`Unsupported value-expression operator: ${operator}`);
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
