import type { PlanExpressionOperand } from "@cerbos/core";

import { buildEvaluationGuards } from "./aggregation";
import type { Mapper, MongooseFilter } from "./index";
import {
  isNullableReference,
  relationOfReference,
  resolveFieldReference,
} from "./mapper";
import { collectVariableNames } from "./operands";

/** `["a", "b"]`, `v` → `{ a: { b: v } }`; an empty path is `v` itself. */
export const buildFieldFilter = (path: string[], value: unknown): any =>
  path.reduceRight((acc: unknown, key) => ({ [key]: acc }), value);

/**
 * A single-field comparison, with the field required to exist (`requireExists`) and/or to be
 * non-null (`nullable`: a stored null is a missing Cerbos attribute) ANDed in front of it.
 */
export const buildGuardedFieldFilter = (
  path: string[],
  value: unknown,
  nullable: boolean,
  requireExists = false,
): MongooseFilter => {
  const filter = buildFieldFilter(path, value);
  if (!nullable && !requireExists) {
    return filter;
  }
  const guards: MongooseFilter[] = [];
  if (requireExists) {
    guards.push(buildFieldFilter(path, { $exists: true }));
  }
  if (nullable) {
    guards.push(buildFieldFilter(path, { $ne: null }));
  }
  return {
    $and: [...guards, filter],
  };
};

/** ANDs `{ field: { $ne: null } }` in front of `filter` for every nullable field the operands read. */
export const withNullableGuards = (
  filter: MongooseFilter,
  operands: PlanExpressionOperand[],
  mapper: Mapper,
): MongooseFilter => {
  const guardedNames = [
    ...new Set(
      operands
        .flatMap(collectVariableNames)
        .filter((name) => isNullableReference(name, mapper)),
    ),
  ];
  if (guardedNames.length === 0) {
    return filter;
  }

  const guards = guardedNames.map((name) => {
    const { path } = resolveFieldReference(name, mapper);
    return buildFieldFilter(path, { $ne: null });
  });
  return { $and: [...guards, filter] };
};

/**
 * "Every to-one parent this expression dots through is present", as a Mongoose filter, or
 * undefined when it dots through none.
 *
 * CEL cannot dot through a list, so each intermediate segment of `a.b.c` is a to-ONE parent:
 * absent, the application sends no attribute at all and CEL raises a missing-path error,
 * which denies. The flattened `a.b` path a Mongo filter matches against cannot see that — an
 * absent parent and a childless parent both fail the match — so any `$nor` over it is TRUE
 * for a parentless document and returns rows the PDP denies.
 *
 * `requiresParent` already carried this for the `$size` aggregation (#309); membership,
 * `hasIntersection` and the negated count spelling reach the same chain without ever passing
 * through it (cerbos/query-plan-adapters#315, #316). Requiring the parent OUTSIDE the `$nor`
 * fixes all of them at once, and is unconditionally faithful to CEL rather than a
 * per-operator patch: a missing parent denies under BOTH polarities regardless of which
 * operator sits above the chain.
 *
 * `<parent>.0` exists exactly when the parent array is non-empty, which is how the document
 * model spells "the to-one parent was serialised" — the same test the `$size` guard makes
 * with `$ifNull`. That `$size` guard alone is not enough: it makes the count `null`, and BSON
 * orders `null` BELOW every number, so `$eq`/`$gt`/`$gte` against it are false but `$lte`/`$lt`
 * are TRUE — `size(chain) <= 0` admitted every parentless document. A filter-level conjunct
 * has no such ordering to get wrong.
 *
 * `requiresParent` only ever appears on a top-level mapping, so the paths produced here are
 * always rooted at the document. A nested `fields` entry that declared one would need its
 * path rebased onto the enclosing `$elemMatch` scope instead.
 *
 * A `type: "one"` relation needs no `requiresParent` to opt in: a to-one hop is BY DEFINITION
 * a level that can be absent, and reaching a scalar through one is a missing-path error when it
 * is. `{ path: { $ne: null } }` is the subdocument spelling of the same requirement — a missing
 * path compares equal to null in MongoDB, so it excludes both the absent and the stored-null
 * case, and on a two-level path it subsumes the level above (cerbos/query-plan-adapters#375).
 */
const buildRequiredParentsFilter = (
  operand: PlanExpressionOperand,
  mapper: Mapper,
): MongooseFilter | undefined => {
  const arrayParents = new Set<string>();
  const toOnePaths = new Set<string>();
  for (const name of collectVariableNames(operand)) {
    const relation = relationOfReference(name, mapper);
    if (relation?.requiresParent !== undefined) {
      arrayParents.add(relation.requiresParent);
    }
    if (relation?.type === "one") {
      toOnePaths.add(relation.name);
    }
  }
  const clauses: MongooseFilter[] = [
    ...[...arrayParents].map((parentPath) => ({
      [`${parentPath}.0`]: { $exists: true },
    })),
    ...[...toOnePaths].map((path) => ({ [path]: { $ne: null } })),
  ];
  if (clauses.length === 0) {
    return undefined;
  }
  return clauses.length === 1 ? clauses[0]! : { $and: clauses };
};

/**
 * Keeps out every document on which an operand of `filter` could not be evaluated: a nullable
 * field that is null, a guarded expression (see `AggregationOperator.guard`) that cannot be
 * evaluated, and an absent to-one parent. Each is ANDed OUTSIDE `filter`.
 */
export const withEvaluationGuards = (
  filter: MongooseFilter,
  operands: PlanExpressionOperand[],
  mapper: Mapper,
): MongooseFilter => {
  const guardedFilter = withNullableGuards(filter, operands, mapper);
  const guards = operands.flatMap((operand) =>
    buildEvaluationGuards(operand, mapper),
  );
  // The absent to-one parent is the other way an operand can be undefined, and it belongs
  // here with the rest: a filter-level conjunct applies to whatever `filter` already is, so
  // both the plain comparison and the `$nor` a negation wraps it in inherit it (#315, #316).
  const requiredParents = operands
    .map((operand) => buildRequiredParentsFilter(operand, mapper))
    .filter((parents): parents is MongooseFilter => parents !== undefined);
  const conjuncts = [...guards, ...requiredParents];
  return conjuncts.length === 0
    ? guardedFilter
    : { $and: [...conjuncts, guardedFilter] };
};
