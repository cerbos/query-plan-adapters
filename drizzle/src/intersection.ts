import type { PlanExpressionOperand, Value } from "@cerbos/core";
import { and, not, sql } from "drizzle-orm";
import type { SQL } from "drizzle-orm";

import {
  buildColumnExpression,
  createCollectionScope,
  isColumn,
  isMappingConfig,
  resolveFieldReference,
  resolveRelationDefaultField,
} from "./mapper";
import { indexedMembership, resolveIndexedMembership } from "./indexed";
import {
  extractArrayValue,
  extractLambdaComponents,
  isNameOperand,
  isOperatorCall,
} from "./operands";
import type { ExpressionOperand } from "./operands";
import { FALSE_CONDITION, applyComparison } from "./predicates";
import {
  requireLeadingHops,
  wrapRelationChain,
  wrapWithRelations,
} from "./relations";
import type {
  BuildFilterOptions,
  Mapper,
  RelationMapping,
  ResolvedMapping,
} from "./types";

/**
 * `hasIntersection(collection, [literals])`, where the collection is a mapped relation or
 * `map(relation, x, x.field)`.
 */

const conjoin = (filter: SQL, guard: SQL | undefined): SQL =>
  guard ? (and(filter, guard) ?? filter) : filter;

/**
 * CEL projects EVERY element before intersecting, so an element whose projected attribute is
 * missing (a NULL column) is an evaluation error — deny — even when another element
 * intersects. Guard with NOT EXISTS(element with NULL projection); a no-op for NOT NULL
 * columns.
 */
const nullProjectionGuard = (
  relations: RelationMapping[],
  mapping: ResolvedMapping["mapping"],
  reference: string,
  options: BuildFilterOptions,
): SQL | undefined => {
  const effective = relations.filter(
    (relation) => !options.skipRelations?.has(relation),
  );
  if (!effective.length) {
    return undefined;
  }
  const isPlainColumn =
    isColumn(mapping) ||
    (isMappingConfig(mapping) &&
      mapping.column !== undefined &&
      !mapping.relation &&
      !mapping.transform);
  if (!isPlainColumn) {
    return undefined;
  }
  const colExpr = buildColumnExpression(mapping, reference);
  return not(
    wrapWithRelations(relations, sql`${colExpr} is null`, reference, options),
  );
};

/** `element IN (values)` evaluated over the chain the element lives behind. */
const buildChainMembership = (
  element: ResolvedMapping,
  values: Value[],
  reference: string,
  options: BuildFilterOptions,
  guardNullProjection: boolean,
): SQL => {
  const filter = applyComparison(element.mapping, "in", values, options);
  const wrapped = wrapRelationChain(element.relations, filter, reference, options);
  return conjoin(
    wrapped,
    guardNullProjection
      ? nullProjectionGuard(element.relations, element.mapping, reference, options)
      : undefined,
  );
};

/** `hasIntersection(map(relation, x, x.field), values)`. */
const buildMappedIntersection = (
  mapOperand: ExpressionOperand,
  values: Value[],
  mapper: Mapper,
  options: BuildFilterOptions,
): SQL => {
  if (mapOperand.operands.length !== 2) {
    throw new Error("'map' operator within hasIntersection requires two operands");
  }
  const [collectionOperand, lambdaOperand] = mapOperand.operands;
  if (!collectionOperand || !lambdaOperand) {
    throw new Error("Map expression is missing operands");
  }
  if (!isNameOperand(collectionOperand)) {
    throw new Error("Map collection operand must be a field reference");
  }
  const { variable, expression: projectionOperand } = extractLambdaComponents(
    lambdaOperand,
    "Map lambda operand",
  );
  if (!isNameOperand(projectionOperand)) {
    throw new Error("Invalid map lambda structure");
  }

  const scope = createCollectionScope(
    collectionOperand.name,
    variable.name,
    mapper,
  );
  if (scope.leadingRelations.length > 0) {
    return FALSE_CONDITION;
  }
  const reference = projectionOperand.name;
  const projection = resolveRelationDefaultField(
    resolveFieldReference(reference, scope.mapper),
    reference,
  );
  const projectedFilter = buildChainMembership(
    projection,
    values,
    reference,
    { ...options, skipRelations: scope.skipRelations },
    true,
  );
  // Exclude rows whose projected element column is NULL (CEL map() errors on a missing
  // element attribute → deny). Only emit this guard when the projection is a direct column
  // of the primary relation's table (no relations beyond the primary one already skipped):
  // when it lives behind further nested relations, buildChainMembership already wrapped an
  // equivalent NULL guard through that chain inside projectedFilter, and re-guarding here with
  // only the primary relation would reference the nested table's column without joining it.
  const projectionBeyondPrimary = projection.relations.filter(
    (relation) => !scope.skipRelations.has(relation),
  );
  return conjoin(
    wrapWithRelations([scope.primaryRelation], projectedFilter, reference),
    projectionBeyondPrimary.length === 0
      ? nullProjectionGuard(
          [scope.primaryRelation],
          projection.mapping,
          reference,
          options,
        )
      : undefined,
  );
};

export const buildHasIntersectionFilter = (
  operands: PlanExpressionOperand[],
  mapper: Mapper,
  options: BuildFilterOptions,
): SQL => {
  if (operands.length !== 2) {
    throw new Error("'hasIntersection' operator requires exactly two operands");
  }
  const [firstOperand, secondOperand] = operands;
  if (!firstOperand || !secondOperand) {
    throw new Error("'hasIntersection' requires exactly two operands");
  }
  // hasIntersection is commutative and the planner preserves source order, so the constant
  // list arrives FIRST when the policy spells it first. The two operands are not
  // interchangeable in the emitted SQL — one is a column the store can index, the other a
  // parameter list — so normalize to collection-first rather than reading them positionally.
  const collectionFirst = extractArrayValue(firstOperand) === undefined;
  const leftOperand = collectionFirst ? firstOperand : secondOperand;
  const rightOperand = collectionFirst ? secondOperand : firstOperand;

  // A missing list is NOT an empty one. Folding the two together used to translate the
  // value-first spelling into `FALSE`, returning no rows for a shape the adapter can express
  // — an emitted filter the corpus forbids, silent because it never threw (#387).
  const values = extractArrayValue(rightOperand);
  if (values === undefined) {
    throw new Error(
      "'hasIntersection' requires a literal list as one of its operands",
    );
  }

  if (isNameOperand(leftOperand)) {
    const indexed = resolveIndexedMembership(leftOperand.name, mapper, options);
    if (indexed) {
      return indexedMembership({ ...indexed, values });
    }
  }

  if (values.length === 0) {
    if (isNameOperand(leftOperand)) {
      const resolved = resolveFieldReference(leftOperand.name, mapper);
      return requireLeadingHops(
        resolved.relations.slice(0, -1),
        sql`false`,
        leftOperand.name,
        options,
      );
    }
    throw new Error(
      "Empty intersection over a computed collection requires preserving its evaluation errors",
    );
  }

  if (isOperatorCall(leftOperand, "map")) {
    return buildMappedIntersection(leftOperand, values, mapper, options);
  }
  if (!isNameOperand(leftOperand)) {
    throw new Error(
      "'hasIntersection' requires a field reference or map expression as the first operand",
    );
  }
  return buildChainMembership(
    resolveRelationDefaultField(
      resolveFieldReference(leftOperand.name, mapper),
      leftOperand.name,
    ),
    values,
    leftOperand.name,
    options,
    false,
  );
};
