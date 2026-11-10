import type { PlanExpressionOperand, Value } from "@cerbos/core";
import { aliasedTableColumn, sql } from "drizzle-orm";
import type { AnyColumn, SQL } from "drizzle-orm";
import { Param } from "drizzle-orm/sql";

import { UnsupportedQueryPlanError } from "./errors";
import {
  buildColumnExpression,
  createCollectionScope,
  getMappingEntry,
  isColumn,
  isMappingConfig,
  mappingNullRepresentation,
  resolveFieldReference,
} from "./mapper";
import type { CollectionScope } from "./mapper";
import { extractLambdaComponents, isNameOperand, isOperatorCall, isValueOperand } from "./operands";
import { constantCondition, UNKNOWN_CONDITION, withPolarity } from "./predicates";
import { chainCorrelation, relationSource, requireLeadingHops } from "./relations";
import type { BuildFilterOptions, Mapper } from "./types";

/**
 * Reads that need the ORDER of a to-many relation's rows — a constant positional access and an
 * ordered list equality — over a relation whose order the caller declares with `position`
 * (`RelationMapping.position`). A relation has no order of its own, so nothing here is inferred.
 */

/** The variable the scope binds the element to; not a name CEL lets a policy bind. */
const ELEMENT = "__cerbos_positioned_element";

type Scalar = string | number | boolean;

/** The lowered element column of a positioned scope, with what the scope says about its NULL. */
interface Element {
  expr: SQL;
  column: AnyColumn;
  explicitNull: boolean;
}

interface PositionedScope {
  scope: CollectionScope;
  reference: string;
  position: SQL;
  source: SQL;
  correlation: SQL;
  /** The mapping's own list column, when it carries one beside the relation. */
  listColumn: AnyColumn | undefined;
}

/** The positioned scope a collection reference iterates, or `undefined` when it declares no order. */
const resolvePositionedScope = (
  reference: string,
  mapper: Mapper,
  options: BuildFilterOptions,
): PositionedScope | undefined => {
  const entry = getMappingEntry(reference, mapper);
  const resolved = resolveFieldReference(reference, mapper);
  const primary = resolved.relations[resolved.relations.length - 1];

  if (primary === undefined || primary.position === undefined || primary.type !== "many") {
    return undefined;
  }

  const scope = createCollectionScope(reference, ELEMENT, mapper, options.openTables);

  const position: AnyColumn =
    scope.alias === undefined
      ? primary.position
      : aliasedTableColumn(primary.position as AnyColumn, scope.alias);

  return {
    scope,
    reference,
    position: sql`${position}`,
    source: relationSource(primary, reference, scope.alias),
    correlation: chainCorrelation(
      primary,
      scope.leadingRelations,
      reference,
      options,
      scope.alias,
    ),
    listColumn:
      entry !== undefined && isMappingConfig(entry) && entry.column !== undefined
        ? entry.column
        : undefined,
  };
};

/**
 * The element, or one of its fields, as a column of the positioned relation's own table. A
 * transform or a further relation hop is not a value a row holds, so it is refused.
 */
const resolveElement = (
  positioned: PositionedScope,
  field: string | undefined,
): Element => {
  const reference = field === undefined ? ELEMENT : `${ELEMENT}.${field}`;
  const resolved = resolveFieldReference(reference, positioned.scope.mapper);

  const unskipped = resolved.relations.filter(
    (relation) => !positioned.scope.skipRelations.has(relation),
  );

  const { mapping } = resolved;

  const column = isColumn(mapping)
    ? mapping
    : isMappingConfig(mapping) && !mapping.transform && !mapping.relation && !mapping.indexable
      ? mapping.column
      : undefined;

  if (unskipped.length > 0 || column === undefined) {
    throw new UnsupportedQueryPlanError(
      `Cannot read '${field ?? "the element"}' of '${positioned.reference}' by position: only a ` +
        "column of the relation's own table has a position, not a transform or a further relation",
    );
  }

  return {
    expr: buildColumnExpression(mapping, reference),
    column,
    explicitNull: mappingNullRepresentation(mapping) === "explicit",
  };
};

const isScalar = (value: Value): value is Scalar =>
  typeof value === "string" || typeof value === "boolean" ||
  (typeof value === "number" && Number.isFinite(value));

/** A constant bound through the element column's own encoder, as a direct comparison binds it. */
const bindElement = (value: Scalar, element: Element): Param => new Param(value, element.column);

/** A list of scalars of the element column's type; anything else has no row to equal. */
const typedList = (values: readonly Value[], element: Element, context: string): Scalar[] =>
  values.map((value) => {
    if (!isScalar(value) || typeof value !== element.column.dataType) {
      throw new UnsupportedQueryPlanError(
        `${context} supports only ${element.column.dataType} literals over '${element.column.name}'`,
      );
    }

    return value;
  });

/** A `get-field(index(coll, k), f)` or `index(coll, k)` read of a positioned relation. */
const positionedRead = (
  operand: PlanExpressionOperand,
  mapper: Mapper,
  options: BuildFilterOptions,
): { positioned: PositionedScope; index: number; field: string | undefined } | undefined => {
  let field: string | undefined;
  let indexed = operand;

  if (isOperatorCall(operand, "get-field")) {
    const [inner, name] = operand.operands;

    if (operand.operands.length !== 2 || inner === undefined || name === undefined) return undefined;

    if (!isNameOperand(name) || !isOperatorCall(inner, "index")) return undefined;
    field = name.name;
    indexed = inner;
  }

  if (!isOperatorCall(indexed, "index") || indexed.operands.length !== 2) return undefined;
  const [collection, position] = indexed.operands;

  if (
    collection === undefined || position === undefined || !isNameOperand(collection) ||
    !isValueOperand(position) || typeof position.value !== "number"
  ) {
    return undefined;
  }

  const entry = getMappingEntry(collection.name, mapper);

  // A declared indexable column answers a bare positional read of a scalar list itself.
  if (field === undefined && entry !== undefined && isMappingConfig(entry) && entry.indexable) {
    return undefined;
  }

  const positioned = resolvePositionedScope(collection.name, mapper, options);

  if (positioned === undefined) return undefined;

  if (!Number.isSafeInteger(position.value) || position.value < 0) {
    throw new UnsupportedQueryPlanError(
      "Index access requires a constant non-negative integer position",
    );
  }

  return { positioned, index: position.value, field };
};

/**
 * `coll[k].f op lit` (or `coll[k] op lit`) over a positioned relation: the field of the row at
 * position `k`, read by a scalar subquery that is NULL — CEL's out-of-range error, denied under
 * both polarities — when no row holds that position, and compared with the constant through the
 * column's own encoder. A NULL field is a missing element attribute, so it stays UNKNOWN; a field
 * declared explicit-null compares as CEL's null VALUE, behind a guard that keeps the absent
 * position an error. A constant of another type equals no field and orders against none.
 */
export const buildPositionedComparison = (
  operator: "eq" | "ne" | "lt" | "le" | "gt" | "ge",
  left: PlanExpressionOperand,
  right: PlanExpressionOperand,
  mapper: Mapper,
  options: BuildFilterOptions,
  negated: boolean,
): SQL | undefined => {
  const leftRead = positionedRead(left, mapper, options);
  const read = leftRead ?? positionedRead(right, mapper, options);

  if (read === undefined) return undefined;
  const other = leftRead === undefined ? left : right;

  const mirrored: Record<typeof operator, typeof operator> = {
    eq: "eq", ne: "ne", lt: "gt", le: "ge", gt: "lt", ge: "le",
  };

  const op = leftRead === undefined ? mirrored[operator] : operator;

  if (!isValueOperand(other) || !isScalar(other.value)) {
    throw new UnsupportedQueryPlanError(
      `Cannot compare '${read.positioned.reference}' read by position with anything but a ` +
        "string, number or boolean constant",
    );
  }

  const { positioned, index, field } = read;
  const element = resolveElement(positioned, field);
  const atPosition = sql`${positioned.correlation} and ${positioned.position} = ${index}`;
  const value = sql`(select ${element.expr} from ${positioned.source} where ${atPosition})`;
  const present = sql`exists (select 1 from ${positioned.source} where ${atPosition})`;
  let filter: SQL;

  if (typeof other.value !== element.column.dataType) {
    if (op !== "eq" && op !== "ne") return UNKNOWN_CONDITION;
    const guard = element.explicitNull ? sql`not ${present}` : sql`${value} is null`;
    filter = sql`(case when ${guard} then null else ${constantCondition(op === "ne")} end)`;
  } else {
    const bound = bindElement(other.value, element);

    const comparison = {
      eq: sql`${value} = ${bound}`,
      ne: sql`${value} <> ${bound}`,
      lt: sql`${value} < ${bound}`,
      le: sql`${value} <= ${bound}`,
      gt: sql`${value} > ${bound}`,
      ge: sql`${value} >= ${bound}`,
    }[op];

    filter =
      element.explicitNull && (op === "eq" || op === "ne")
        ? sql`(case when not ${present} then null when ${value} is null then ${constantCondition(op === "ne")} else ${comparison} end)`
        : sql`(${comparison})`;
  }

  return withPolarity(filter, negated);
};

/** The ordered projection one side of a list equality names, if it names one. */
const orderedProjection = (
  operand: PlanExpressionOperand,
  mapper: Mapper,
  options: BuildFilterOptions,
):
  | { positioned: PositionedScope; element: Element; survivors: SQL | undefined }
  | undefined => {
  if (isOperatorCall(operand, "map") && operand.operands.length === 2) {
    const [collection, lambda] = operand.operands;

    if (collection === undefined || lambda === undefined || !isNameOperand(collection)) {
      return undefined;
    }

    const positioned = resolvePositionedScope(collection.name, mapper, options);

    if (positioned === undefined) return undefined;
    const { variable, expression } = extractLambdaComponents(lambda, "'map' lambda operand");

    if (!isNameOperand(expression)) {
      throw new UnsupportedQueryPlanError(
        "An ordered equality over map() supports only a projection of the element or one of its fields",
      );
    }

    const field =
      expression.name === variable.name
        ? undefined
        : expression.name.startsWith(`${variable.name}.`)
          ? expression.name.slice(variable.name.length + 1)
          : undefined;

    if (field === undefined && expression.name !== variable.name) {
      throw new UnsupportedQueryPlanError(
        "An ordered equality over map() supports only a projection of the element or one of its fields",
      );
    }

    return { positioned, element: resolveElement(positioned, field), survivors: undefined };
  }

  if (isOperatorCall(operand, "except") && operand.operands.length === 2) {
    const [collection, removed] = operand.operands;

    if (
      collection === undefined || removed === undefined || !isNameOperand(collection) ||
      !isValueOperand(removed) || !Array.isArray(removed.value)
    ) {
      return undefined;
    }

    const positioned = resolvePositionedScope(collection.name, mapper, options);

    if (positioned === undefined) return undefined;
    const element = resolveElement(positioned, undefined);
    // Cerbos's except() keeps an element exactly when the removed list does not contain it, so
    // a constant of another type removes nothing and a null removes the null elements.
    const removesNull = removed.value.includes(null);

    const kept = removed.value.filter(
      (value): value is Scalar => isScalar(value) && typeof value === element.column.dataType,
    );

    const notRemoved =
      kept.length === 0
        ? undefined
        : sql`${element.expr} not in (${sql.join(kept.map((value) => bindElement(value, element)), sql`, `)})`;

    const survivors = removesNull
      ? notRemoved === undefined
        ? sql`${element.expr} is not null`
        : sql`(${element.expr} is not null and ${notRemoved})`
      : notRemoved === undefined
        ? undefined
        : sql`(${element.expr} is null or ${notRemoved})`;

    return { positioned, element, survivors };
  }

  return undefined;
};

/**
 * `coll.map(t, t.f) == [v0, …]` and `coll.except([…]) == [v0, …]` (and `!=`) over a positioned
 * relation: CEL's list equality — the same length, and the element at each rank equal to the
 * literal's — over the rows in `position` order, the ranks counted among the rows except() keeps.
 *
 * A NULL projection of a field the caller omits is a missing element attribute, which map()
 * raises on before any comparison, so it makes the whole equality UNKNOWN; an explicit null is
 * CEL's null VALUE, which equals no scalar, so its rank fails to match. The mapping's own list
 * column, when it carries one, being NULL is a null list, an error too.
 */
export const buildOrderedListEquality = (
  operator: "eq" | "ne",
  left: PlanExpressionOperand,
  right: PlanExpressionOperand,
  mapper: Mapper,
  options: BuildFilterOptions,
  negated: boolean,
): SQL | undefined => {
  const leftProjection = orderedProjection(left, mapper, options);
  const projection = leftProjection ?? orderedProjection(right, mapper, options);

  if (projection === undefined) return undefined;
  const other = leftProjection === undefined ? left : right;

  if (!isValueOperand(other) || !Array.isArray(other.value)) {
    throw new UnsupportedQueryPlanError(
      "An ordered list equality over a positioned relation needs a literal list on the other side",
    );
  }

  const { positioned, element, survivors } = projection;
  const values = typedList(other.value, element, "An ordered list equality");

  const where =
    survivors === undefined
      ? positioned.correlation
      : sql`${positioned.correlation} and ${survivors}`;

  const count = sql`(select count(*) from ${positioned.source} where ${where})`;
  let equality: SQL;

  if (values.length === 0) {
    equality = sql`${count} = 0`;
  } else {
    const ranked = sql`(select ${element.expr} as v, row_number() over (order by ${positioned.position}) - 1 as rn from ${positioned.source} where ${where}) cerbos_ranked`;

    const matches = sql.join(
      values.map(
        (value, rank) => sql`(cerbos_ranked.rn = ${rank} and cerbos_ranked.v = ${bindElement(value, element)})`,
      ),
      sql` or `,
    );

    equality = sql`(${count} = ${values.length} and (select count(*) from ${ranked} where ${matches}) = ${values.length})`;
  }

  const guards: SQL[] = [];

  if (positioned.listColumn !== undefined) {
    guards.push(sql`when ${positioned.listColumn} is null then null`);
  }

  if (!element.explicitNull) {
    guards.push(
      sql`when exists (select 1 from ${positioned.source} where ${positioned.correlation} and ${element.expr} is null) then null`,
    );
  }

  const filter =
    guards.length === 0
      ? sql`(${equality})`
      : sql`(case ${sql.join(guards, sql` `)} else ${equality} end)`;

  return withPolarity(
    requireLeadingHops(positioned.scope.leadingRelations, filter, positioned.reference, options),
    (operator === "ne") !== negated,
  );
};
