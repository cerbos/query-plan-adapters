import {
  PlanResourcesResponse,
  PlanExpressionOperand,
  PlanExpression,
  PlanKind,
} from "@cerbos/core";

import { UnsupportedQueryPlanError } from "./errors";
import { evaluate } from "./evaluate";
import { isExpression, withOmittedNullDefault } from "./operands";
import { canPushToDb, translateExpression } from "./pushdown";
import type { FilterQ } from "./pushdown";
import {
  assertEveryReferenceMapped,
  assertNoListValuedCondition,
  assertNoNullComparisonOperands,
  validateStructure,
} from "./validate";

export { PlanKind, UnsupportedQueryPlanError };

export type ConvexFilter<Q, R = unknown> = (q: Q) => R;

export type MapperConfig = {
  field?: string;
  /**
   * The mapped path may be absent from a document (CEL's missing-attribute case), so comparisons
   * over it stay with the post-filter. Left undeclared, it follows the call's
   * `nullAttributeRepresentation`: `false` under `"explicit"`, `true` under `"omitted"`.
   */
  nullable?: boolean;
};

export type Mapper =
  | Record<string, MapperConfig>
  | ((key: string) => MapperConfig);

/**
 * How the caller represents a NULL field when building the attributes it sends to `check()`.
 *
 * The planner emits the same `eq(attr, null)` node either way, so the plan cannot reveal which
 * convention is in use and the adapter has to be told.
 *
 * - `"explicit"` (default) — a NULL field is sent as an explicit `null` attribute. CEL compares
 *   `null == null`, so matching null selects exactly the documents `check()` allows.
 * - `"omitted"` — a NULL field sends no attribute at all. CEL then raises a missing-attribute
 *   error, which Cerbos treats as a deny, so a filter that *selects* null documents returns
 *   documents the PDP denies. Null comparison operands are rejected instead of translated, every
 *   mapper entry that does not declare `nullable` is treated as `nullable: true`, and the
 *   post-filter reads a stored `null` as a missing attribute.
 *
 * See https://github.com/cerbos/query-plan-adapters/issues/302 and
 * cerbos/query-plan-adapters#493.
 */
export type NullAttributeRepresentation = "explicit" | "omitted";

export interface QueryPlanToConvexArgs {
  queryPlan: PlanResourcesResponse;
  mapper?: Mapper;
  allowPostFilter?: boolean;
  /**
   * Which NULL-field representation the caller uses when building `check()` attributes.
   * Defaults to `"explicit"`, preserving the historical null-matching translation.
   */
  nullAttributeRepresentation?: NullAttributeRepresentation;
}

type PostFilter = (doc: Record<string, unknown>) => boolean;

type ConditionalResult<Q, R> =
  | {
      kind: PlanKind.CONDITIONAL;
      path: "db";
      filter: ConvexFilter<Q, R>;
      postFilter?: undefined;
    }
  | {
      kind: PlanKind.CONDITIONAL;
      path: "post";
      filter?: undefined;
      postFilter: PostFilter;
    }
  | {
      kind: PlanKind.CONDITIONAL;
      path: "split";
      filter: ConvexFilter<Q, R>;
      postFilter: PostFilter;
    };

export type QueryPlanToConvexResult<Q = unknown, R = unknown> =
  | {
      kind: PlanKind.ALWAYS_ALLOWED | PlanKind.ALWAYS_DENIED;
      path?: undefined;
      filter?: undefined;
      postFilter?: undefined;
    }
  | ConditionalResult<Q, R>;

/**
 * Routes a validated condition to the half of the output that answers it: Convex's filter engine
 * (`db`), the adapter's own evaluator (`post`), or — for a root `and` that mixes the two — both
 * (`split`), the engine narrowing the candidates before the post-filter decides.
 */
const buildFilters = <Q, R>(
  expression: PlanExpressionOperand,
  mapper: Mapper,
  nullIsMissing: boolean,
): ConditionalResult<Q, R> => {
  if (canPushToDb(expression, mapper)) {
    return {
      kind: PlanKind.CONDITIONAL,
      path: "db",
      filter: convexFilter(expression, mapper),
    };
  }

  if (
    isExpression(expression) &&
    expression.operator === "and" &&
    expression.operands.length > 1
  ) {
    const pushable: PlanExpressionOperand[] = [];
    const nonPushable: PlanExpressionOperand[] = [];
    for (const op of expression.operands) {
      (canPushToDb(op, mapper) ? pushable : nonPushable).push(op);
    }

    if (pushable.length > 0 && nonPushable.length > 0) {
      return {
        kind: PlanKind.CONDITIONAL,
        path: "split",
        filter: convexFilter(conjunction(pushable), mapper),
        postFilter: postFilterFor(
          conjunction(nonPushable),
          mapper,
          nullIsMissing,
        ),
      };
    }
  }

  return {
    kind: PlanKind.CONDITIONAL,
    path: "post",
    postFilter: postFilterFor(expression, mapper, nullIsMissing),
  };
};

const conjunction = (
  operands: PlanExpressionOperand[],
): PlanExpressionOperand =>
  operands.length === 1
    ? operands[0]!
    : ({ operator: "and", operands } as PlanExpression);

// The caller's `Q` is Convex's `FilterBuilder`, of which `FilterQ` is the subset the filter calls.
const convexFilter =
  <Q, R>(
    expression: PlanExpressionOperand,
    mapper: Mapper,
  ): ConvexFilter<Q, R> =>
  (q) =>
    translateExpression(expression, q as unknown as FilterQ, mapper) as R;

const postFilterFor =
  (
    expression: PlanExpressionOperand,
    mapper: Mapper,
    nullIsMissing: boolean,
  ): PostFilter =>
  (doc) =>
    evaluate(expression, { doc, mapper, bindings: {}, nullIsMissing }) === true;

export function queryPlanToConvex<Q = unknown, R = unknown>({
  queryPlan,
  mapper = {},
  allowPostFilter = false,
  nullAttributeRepresentation = "explicit",
}: QueryPlanToConvexArgs): QueryPlanToConvexResult<Q, R> {
  switch (queryPlan.kind) {
    case PlanKind.ALWAYS_ALLOWED:
      return { kind: PlanKind.ALWAYS_ALLOWED };
    case PlanKind.ALWAYS_DENIED:
      return { kind: PlanKind.ALWAYS_DENIED };
    case PlanKind.CONDITIONAL: {
      const { condition } = queryPlan;
      const omitted = nullAttributeRepresentation === "omitted";
      assertNoListValuedCondition(condition);
      if (omitted) {
        assertNoNullComparisonOperands(condition);
      }
      validateStructure(condition);
      assertEveryReferenceMapped(condition, mapper);

      const result = buildFilters<Q, R>(
        condition,
        omitted ? withOmittedNullDefault(mapper) : mapper,
        omitted,
      );
      if (result.postFilter && !allowPostFilter) {
        throw new Error(
          "The query plan contains conditions that cannot be evaluated by Convex's " +
            "query engine and require trusted-backend filtering (postFilter). Apply " +
            "postFilter to every candidate before it is serialized or returned. Set " +
            "{ allowPostFilter: true } to opt in to this behavior.",
        );
      }
      return result;
    }
    default:
      throw new UnsupportedQueryPlanError("Invalid query plan.");
  }
}
