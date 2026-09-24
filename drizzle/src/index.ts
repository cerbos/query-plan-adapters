import { PlanKind } from "@cerbos/core";

import { UnsupportedQueryPlanError } from "./errors";
import { buildFilterFromExpression, rejectNullConstructors } from "./filter";
import type {
  BuildFilterOptions,
  QueryPlanToDrizzleArgs,
  QueryPlanToDrizzleResult,
} from "./types";

/**
 * The public surface: `queryPlanToDrizzle` and the types a caller needs to build its mapper.
 *
 * The translation itself lives in internal modules, each owning one kind of shape:
 * `filter.ts` dispatches every operator in condition position; `comparison.ts`,
 * `collections.ts`, `intersection.ts`, `hierarchy.ts` and `indexed.ts` translate their operator
 * families; `values.ts` renders operands in value position; `predicates.ts` holds the leaf SQL;
 * `mapper.ts` and `relations.ts` resolve references and build the correlated subqueries.
 */

export type { Indexable } from "./indexed";
export type {
  DrizzleFilter,
  Mapper,
  MapperEntry,
  NullAttributeRepresentation,
  QueryPlanToDrizzleArgs,
  QueryPlanToDrizzleResult,
  RelationMapping,
} from "./types";

export { PlanKind };
export { UnsupportedQueryPlanError };

export function queryPlanToDrizzle({
  queryPlan,
  mapper,
  nullAttributeRepresentation = "explicit",
}: QueryPlanToDrizzleArgs): QueryPlanToDrizzleResult {
  const options: BuildFilterOptions = {
    nullRepresentation: nullAttributeRepresentation,
  };
  switch (queryPlan.kind) {
    case PlanKind.ALWAYS_ALLOWED:
      return { kind: PlanKind.ALWAYS_ALLOWED };
    case PlanKind.ALWAYS_DENIED:
      return { kind: PlanKind.ALWAYS_DENIED };
    case PlanKind.CONDITIONAL:
      rejectNullConstructors(queryPlan.condition, options);
      return {
        kind: PlanKind.CONDITIONAL,
        filter: buildFilterFromExpression(queryPlan.condition, mapper, options),
      };
    default:
      throw new UnsupportedQueryPlanError("Invalid plan kind");
  }
}
