import { PlanKind } from "@cerbos/core";
import type { PlanResourcesResponse } from "@cerbos/core";

import { rejectConstantFalse } from "./fields";
import { assertStructuralNulls } from "./mapping";
import type { TranslationContext } from "./mapping";
import { isValueOperand } from "./plan";
import { constantFoldExpression, hoistOuterScopeReferences } from "./rewrite";
import { buildPrismaFilterFromCerbosExpression } from "./translate";
import { expandLiteralCollections } from "./literals";
import { settleTypeMismatches } from "./types";
import { UnsupportedQueryPlanError } from "./errors";

export { PlanKind, UnsupportedQueryPlanError };

export type PrismaFilter = Record<string, any>;

export type MapperConfig = {
  field?: string;
  /**
   * Declares the Prisma scalar kind so incompatible operands are rejected before query
   * execution. Undeclared scalar kinds preserve the historical behavior. Date-time metadata
   * is required for timestamp(field); raw DateTime column comparisons lose lexical spelling.
   */
  valueType?: "dateTime" | "string" | "number" | "boolean";
  /**
   * Marks the mapped column as nullable in the database. Cerbos treats a missing attribute as
   * an evaluation error (deny), which matches SQL three-valued logic for simple predicates —
   * but relation subqueries (some/every/none) collapse UNKNOWN to false at the EXISTS boundary,
   * so collection macros over elements with NULL fields need an explicit guard to stay
   * deny-aligned. An element column is treated as nullable unless this says `false`, because
   * the unguarded filter is the one that over-grants; declare `nullable: false` on every required
   * element column, since Prisma rejects a `null` comparison against one. Scalar hierarchy
   * segments likewise preserve missing-value errors unless nullable: false declares that a
   * segment cannot be NULL.
   */
  nullable?: boolean;
  /**
   * Declares that this column can be SQL NULL **and** how the caller represents that NULL in the
   * attributes it sends to `check()`. Declaring it asserts both facts; leaving it undeclared means
   * "treat this column as NOT NULL", which is the historical rendering.
   *
   * This is per attribute rather than per call because one policy suite can legitimately mix the
   * two conventions — the same column can be mapped twice, sent as an explicit null under one
   * attribute name and omitted under another. The call-level option cannot express that, which is
   * what made cerbos/query-plan-adapters#308 unfixable with #302's option alone.
   *
   * - `"explicit"` — a NULL column is sent as an explicit `null` attribute, so CEL holds a null
   *   VALUE. `null != "x"` is then TRUE and `null == "x"` is FALSE, both definite, while Prisma's
   *   `{ not: "x" }` drops the row under both polarities. The equality family (`equals`, `not`,
   *   `in`) is therefore rendered so it can never depend on SQL's UNKNOWN.
   * - `"omitted"` — a NULL column sends no attribute, so CEL raises a missing-attribute error and
   *   `check()` denies. Dropping the row is already right, so the rendering is unchanged; what the
   *   declaration adds is the same null-operand rejection the call-level `"omitted"` performs,
   *   scoped to this attribute.
   *
   * Only the equality family is affected. `lt`/`lte`/`gt`/`gte` and the string operators raise a
   * no-overload error on a null receiver in CEL, which denies under both polarities exactly as a
   * dropped row does, so they are left alone.
   *
   * Distinct from `nullable` above, which declares whether a *relation element* column can be NULL so
   * collection macros gain their three-valued guards. The two are independent: `nullable` is about
   * what a subquery does with an UNKNOWN element, this is about what the caller sent to the PDP.
   *
   * See https://github.com/cerbos/query-plan-adapters/issues/308.
   */
  nullAttributeRepresentation?: NullAttributeRepresentation;
  relation?: {
    name: string;
    type: "one" | "many";
    /**
     * The Prisma model name of the related record (e.g. "Tag"). Required only for
     * field-to-field comparisons between two columns of the related model, which compile to
     * Prisma field references and need the model name as their container.
     */
    model?: string;
    field?: string;
    fields?: Record<string, MapperConfig>;
    /**
     * The predicate the application itself applies when it reads this relation — a soft-delete
     * flag, a tenant column, a subtype discriminator, whatever narrows the rows that became the
     * resource attributes.
     *
     * Prisma has no schema-level filtered relation. A `where` injected by a client extension or
     * middleware rewrites the *top-level* query, never the nested `some`/`every`/`none` this
     * adapter generates, so a narrowing the application applies to its own reads does not reach
     * the subquery — and the subquery then matches records the application never serialised, a
     * filter that returns rows the PDP denies. Declaring the predicate here restores the
     * equality; the adapter cannot detect the omission, which is why the field is optional and
     * silence is not a warning. See "Mapping hazards" in the README.
     *
     * It is a Prisma where-input over the RELATED model, ANDed into the nested filter, and it
     * constrains every operator reached through this relation under both polarities. `every` is
     * rewritten to the equivalent `none` so the restriction narrows the records examined rather
     * than the records required to match.
     *
     * ```ts
     * relation: { name: "tags", type: "many", subqueryFilter: { deletedAt: null } }
     * ```
     */
    subqueryFilter?: PrismaFilter;
  };
};

export type Mapper =
  | Record<string, MapperConfig>
  | ((key: string) => MapperConfig);

/**
 * How the caller represents a NULL column when building the attributes it sends to `check()`.
 *
 * The planner emits the same `eq(attr, null)` node either way, so the plan cannot reveal which
 * convention is in use and the adapter has to be told.
 *
 * - `"explicit"` (default) — a NULL column is sent as an explicit `null` attribute. CEL compares
 *   `null == null`, so `IS NULL` selects exactly the rows `check()` allows.
 * - `"omitted"` — a NULL column sends no attribute at all. CEL then raises a missing-attribute
 *   error, which Cerbos treats as a deny, so a filter that *selects* NULL rows returns rows the
 *   PDP denies. A null comparison operand is never translated into a NULL-selecting filter.
 *
 * See https://github.com/cerbos/query-plan-adapters/issues/302.
 */
export type NullAttributeRepresentation = "explicit" | "omitted";

export interface QueryPlanToPrismaArgs {
  queryPlan: PlanResourcesResponse;
  mapper?: Mapper;
  /**
   * The Prisma model name the generated filter targets (e.g. "Resource"). Required only for
   * field-to-field comparisons between two root columns, which compile to Prisma field
   * references and need the model name as their container.
   */
  model?: string;
  /**
   * Which NULL-column representation the caller uses when building `check()` attributes.
   * Defaults to `"explicit"`, preserving the historical `IS NULL` translation.
   */
  nullAttributeRepresentation?: NullAttributeRepresentation;
}

export type QueryPlanToPrismaResult =
  | {
      kind: PlanKind.ALWAYS_ALLOWED | PlanKind.ALWAYS_DENIED;
    }
  | {
      kind: PlanKind.CONDITIONAL;
      filters: PrismaFilter;
    };

/**
 * Converts a Cerbos query plan to a Prisma filter.
 */
export function queryPlanToPrisma({
  queryPlan,
  mapper = {},
  model,
  nullAttributeRepresentation = "explicit",
}: QueryPlanToPrismaArgs): QueryPlanToPrismaResult {
  const context: TranslationContext = {
    mapper,
    rootModel: model,
    nullRepresentation: nullAttributeRepresentation,
    scopes: [],
  };
  switch (queryPlan.kind) {
    case PlanKind.ALWAYS_ALLOWED:
      return { kind: PlanKind.ALWAYS_ALLOWED };
    case PlanKind.ALWAYS_DENIED:
      return { kind: PlanKind.ALWAYS_DENIED };
    case PlanKind.CONDITIONAL: {
      const settled = settleTypeMismatches(
        hoistOuterScopeReferences(
          expandLiteralCollections(queryPlan.condition),
          []
        ),
        context
      );
      // After settling: a struct literal compared with a column of another type is decided by
      // the types alone (`aString == {"a": null}` is false, or an error on a missing column), so
      // its null member never reaches a filter and must not refuse the plan.
      assertStructuralNulls(settled, context);
      const condition = constantFoldExpression(settled);
      if (isValueOperand(condition)) {
        // The planner folds a condition that is constant on its own, but not one the mapped
        // column types decide (`R.attr.aNumber == "5"` is false for every row): that surfaces
        // here, and is answered as the planner would have answered it.
        if (condition.value === true) {
          return { kind: PlanKind.CONDITIONAL, filters: {} };
        }
        if (condition.value === false) {
          return { kind: PlanKind.ALWAYS_DENIED };
        }
        rejectConstantFalse();
      }
      return {
        kind: PlanKind.CONDITIONAL,
        filters: buildPrismaFilterFromCerbosExpression(condition, context),
      };
    }
    default:
      throw new UnsupportedQueryPlanError(`Invalid query plan.`);
  }
}
