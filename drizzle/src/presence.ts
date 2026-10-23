import type { PlanExpressionOperand } from "@cerbos/core";
import { sql } from "drizzle-orm";
import type { SQL } from "drizzle-orm";

import { UnsupportedQueryPlanError } from "./errors";
import {
  buildColumnExpression,
  isRelationValue,
  mappingNullRepresentation,
  resolveFieldReference,
} from "./mapper";
import { isNameOperand } from "./operands";
import { TRUE_CONDITION, withPolarity } from "./predicates";
import { requireLeadingHops, wrapWithRelations } from "./relations";
import type { BuildFilterOptions, Mapper } from "./types";

/**
 * `has()`, the presence test the planner emits from Cerbos 0.57.0 for an attribute it does not
 * fold into a guard: whether the caller's attributes carry the key at all.
 *
 * Presence is a property of the caller's null convention, not of the column. An attribute sent as
 * an explicit null is present on every row; one omitted when its column is NULL is present exactly
 * when the column is not NULL. An element column of an object collection follows the omitted
 * convention unless it declares otherwise, as the collection macros already assume. A to-one
 * relation is present when the related row exists.
 *
 * `has()` never errors on a missing value, so the leaf is two-valued and a NOT over it is exact.
 * The one error it raises is selecting through a missing to-one hop (`has(parent.x)` on a
 * parentless row), which denies under both polarities, so the hops BEFORE the tested key make the
 * result UNKNOWN when absent.
 */
export const buildHasFilter = (
  operands: PlanExpressionOperand[],
  mapper: Mapper,
  options: BuildFilterOptions,
  negated: boolean,
): SQL => {
  const [operand] = operands;
  if (operands.length !== 1 || operand === undefined || !isNameOperand(operand)) {
    throw new UnsupportedQueryPlanError("'has' operator requires exactly one attribute reference");
  }
  const reference = operand.name;
  const { relations, mapping } = resolveFieldReference(reference, mapper);
  const open = relations.filter((relation) => !options.skipRelations?.has(relation));
  if (open.some((relation) => relation.type === "many")) {
    throw new UnsupportedQueryPlanError(
      `Cannot translate has(${reference}): a presence test over a to-many relation has no ` +
        "SQL form that matches what the caller sends for it",
    );
  }

  let presence: SQL;
  let hops = open;
  if (isRelationValue(mapping)) {
    presence = wrapWithRelations(open, TRUE_CONDITION, reference, options);
    hops = open.slice(0, -1);
  } else {
    const element = relations.some((relation) => options.skipRelations?.has(relation));
    const convention =
      mappingNullRepresentation(mapping) ?? (element ? "omitted" : options.nullRepresentation);
    const leaf =
      convention === "omitted"
        ? sql`(${buildColumnExpression(mapping, reference)} is not null)`
        : TRUE_CONDITION;
    presence = wrapWithRelations(open, leaf, reference, options);
  }

  return withPolarity(requireLeadingHops(hops, presence, reference, options), negated);
};
