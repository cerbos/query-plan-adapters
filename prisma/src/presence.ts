// `has()`, the presence test the planner emits from Cerbos 0.57.0 for an attribute it does not
// fold into a guard: whether the caller's attributes carry the key at all.

import type { PlanExpressionOperand } from "@cerbos/core";

import type { PrismaFilter } from "./index";
import { getLeafField, namesToOneRelation, resolveFieldReference } from "./mapping";
import type { TranslationContext } from "./mapping";
import { isNamedOperand } from "./plan";
import type { OperatorOperand } from "./plan";
import { negateFilter, wrapInRelations } from "./relations";
import { UnsupportedQueryPlanError } from "./errors";

/**
 * `has(ref)`, or `!has(ref)` when `negated`.
 *
 * Presence is a property of the caller's null convention, not of the column: an attribute sent as
 * an explicit null is present, so `has()` is true for every row, and one omitted when its column
 * is NULL is present exactly when the column is not NULL. Element columns inside a lambda follow
 * the omitted convention unless they are declared `nullable: false`, as the collection macros
 * already assume. A to-one relation is present when the related row exists.
 *
 * `has()` itself never errors on a missing value, so `!has()` is a plain NOT. The one error it can
 * raise is selecting through a missing to-one hop, `has(parent.x)` on a parentless row, which
 * denies under both polarities, so every hop BEFORE the tested key is required outside the NOT.
 */
export function handleHasOperator(
  operands: PlanExpressionOperand[],
  context: TranslationContext,
  negated: boolean
): PrismaFilter {
  const [operand] = operands;
  if (operands.length !== 1 || operand === undefined || !isNamedOperand(operand)) {
    throw new UnsupportedQueryPlanError("has requires exactly one attribute reference");
  }

  const fieldRef = resolveFieldReference(operand.name, context);
  const relations = fieldRef.relations ?? [];
  if (relations.some((relation) => relation.type === "many")) {
    throw new UnsupportedQueryPlanError(
      `Cannot translate has(${operand.name}): a presence test over a to-many relation has no ` +
        "Prisma filter that matches what the caller sends for it"
    );
  }

  let presence: PrismaFilter;
  let hops = relations;
  if (namesToOneRelation(context.mapper, operand.name)) {
    presence = wrapInRelations(relations, {});
    hops = relations.slice(0, -1);
  } else if (presenceFollowsColumn(operand.name, fieldRef, context)) {
    presence = wrapInRelations(relations, {
      [getLeafField(fieldRef.path)]: { not: null },
    });
  } else {
    presence = wrapInRelations(relations, {});
  }

  if (!negated) return presence;
  const absence = negateFilter(presence);
  return hops.length === 0 ? absence : { AND: [wrapInRelations(hops, {}), absence] };
}

/**
 * `has(R.attr.x)` on an attribute sent as an explicit null is true on every row, so it becomes the
 * constant `true` before translation. Left to the translator it lowers to `{}`, which Prisma drops
 * from an OR or a NOT, losing the rows it admits and, negated, admitting rows it denies.
 */
export function constantPresence(
  node: OperatorOperand,
  context: TranslationContext
): PlanExpressionOperand | undefined {
  const [operand] = node.operands;
  if (
    node.operator !== "has" ||
    node.operands.length !== 1 ||
    operand === undefined ||
    !isNamedOperand(operand) ||
    !operand.name.startsWith("request.") ||
    namesToOneRelation(context.mapper, operand.name)
  ) {
    return undefined;
  }
  const fieldRef = resolveFieldReference(operand.name, context);
  if ((fieldRef.relations ?? []).length > 0) return undefined;
  return presenceFollowsColumn(operand.name, fieldRef, context) ? undefined : { value: true };
}

/** Whether a NULL column means the key is absent, rather than sent as an explicit null. */
function presenceFollowsColumn(
  reference: string,
  fieldRef: ReturnType<typeof resolveFieldReference>,
  context: TranslationContext
): boolean {
  const element = context.scopes.some((scope) =>
    reference.startsWith(scope.variableName + ".")
  );
  if (element && fieldRef.nullAttributeRepresentation === undefined) {
    return fieldRef.nullable !== false;
  }
  return (
    (fieldRef.nullAttributeRepresentation ?? context.nullRepresentation) === "omitted"
  );
}
