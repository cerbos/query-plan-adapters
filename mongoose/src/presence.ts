import type { PlanExpressionOperand } from "@cerbos/core";

import { assertCollectionScopedReference } from "./context";
import type { TranslateContext } from "./context";
import { UnsupportedQueryPlanError } from "./errors";
import type { MongooseFilter } from "./index";
import { isNullableReference, resolveFieldReference } from "./mapper";
import { isVariable } from "./operands";

/**
 * `has()`, the presence test the planner emits from Cerbos 0.57.0 for an attribute it does not
 * fold into a guard: whether the caller's attributes carry the key at all.
 *
 * A document field is present when the path exists, and a field declared `nullable` (a stored
 * null is a missing attribute) also needs a non-null value. A to-one parent is present when it is
 * stored and not null. `has()` never errors on a missing value, so its negation is a plain `$nor`;
 * the one error it raises is selecting through a missing to-one parent (`has(parent.x)` on a
 * parentless document), which denies under both polarities, so the negation requires the parent.
 */
export const translateHas = (
  operands: PlanExpressionOperand[],
  ctx: TranslateContext,
  negated: boolean,
): MongooseFilter => {
  const [operand] = operands;
  if (operands.length !== 1 || operand === undefined || !isVariable(operand)) {
    throw new UnsupportedQueryPlanError("has requires exactly one attribute reference");
  }
  const reference = operand.name;
  assertCollectionScopedReference(reference, ctx);
  const { path, relation } = resolveFieldReference(reference, ctx.mapper);
  if (relation?.type === "many") {
    throw new UnsupportedQueryPlanError(
      `Cannot translate has(${reference}): a presence test over a to-many relation is unsupported`,
    );
  }
  const field = path.join(".");
  const namesParent = relation?.type === "one" && field === relation.name;
  const presence =
    namesParent || isNullableReference(reference, ctx.mapper)
      ? { [field]: { $ne: null } }
      : { [field]: { $exists: true } };
  if (!negated) {
    return presence;
  }
  const absence = { $nor: [presence] };
  return relation?.type === "one" && !namesParent
    ? { $and: [{ [relation.name]: { $ne: null } }, absence] }
    : absence;
};
