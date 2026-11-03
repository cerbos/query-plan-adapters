import { PlanResourcesResponse, PlanKind } from "@cerbos/core";

import { UnsupportedQueryPlanError } from "./errors";
import type { TranslateContext } from "./context";
import { translateCondition } from "./filter";
import { withOmittedNullDefault } from "./mapper";

export { PlanKind };
export { UnsupportedQueryPlanError };

export type MongooseFilter = Record<string, any>;

export type MapperConfig = {
  field?: string;
  /**
   * Treat a stored null as a missing Cerbos attribute and exclude it from comparisons. Left
   * undeclared, it follows the call's `nullAttributeRepresentation`: `true` under `"omitted"` (the
   * default), `false` under `"explicit"`.
   */
  nullable?: boolean;
  valueParser?: (value: any) => any;
  /** Stored scalar type; dateTime loses the original CEL timestamp string spelling. */
  valueType?: "number" | "string" | "boolean" | "dateTime";
  relation?: {
    name: string;
    type: "one" | "many";
    field?: string;
    /**
     * The document path of an optional to-ONE parent this collection is reached through.
     *
     * CEL cannot dot through a list, so every intermediate segment of `a.b.c` is a to-one
     * parent: absent, the application sends no attribute at all and CEL raises a
     * missing-path error, which denies. A flattened Mongo path cannot see the difference —
     * an absent parent and a childless parent both give an empty array — so
     * `size(chain) == 0` and `size(chain) >= 0` are TRUE for every parentless document and
     * return records the PDP denies. Declaring the parent makes those comparisons yield
     * null, which loses against every number in BSON order and excludes the document
     * (cerbos/query-plan-adapters#309).
     */
    requiresParent?: string;
    fields?: {
      [key: string]: MapperConfig;
    };
  };
};

export type Mapper =
  | {
      [key: string]: MapperConfig;
    }
  | ((key: string) => MapperConfig);

/**
 * How the caller represents a NULL field when building the attributes it sends to `check()`.
 *
 * The planner emits the same `eq(attr, null)` node either way, so the plan cannot reveal which
 * convention is in use and the adapter has to be told.
 *
 * - `"explicit"` — a NULL field is sent as an explicit `null` attribute. CEL compares
 *   `null == null`, so matching null selects exactly the documents `check()` allows.
 * - `"omitted"` (default) — a NULL field sends no attribute at all. CEL then raises a
 *   missing-attribute error, which Cerbos treats as a deny, so a filter that *selects* null documents returns
 *   documents the PDP denies. Null comparison operands are rejected instead of translated, and
 *   every mapper entry that does not declare `nullable` is treated as `nullable: true`.
 *
 * The default is the convention whose wrong guess fails closed: a caller that actually sends
 * explicit nulls gets a refusal naming this option, never a filter returning documents the PDP
 * denies (docs/adr/0011-denial-fails-closed-in-the-result.md).
 *
 * See https://github.com/cerbos/query-plan-adapters/issues/302 and cerbos/query-plan-adapters#493.
 */
export type NullAttributeRepresentation = "explicit" | "omitted";

export interface QueryPlanToMongooseArgs {
  queryPlan: PlanResourcesResponse;
  mapper?: Mapper;
  /**
   * Which NULL-field representation the caller uses when building `check()` attributes.
   * Defaults to `"omitted"`, which refuses every null comparison operand.
   */
  nullAttributeRepresentation?: NullAttributeRepresentation;
}

/**
 * Only a `CONDITIONAL` result carries `filters`, so a TypeScript caller cannot read one without
 * narrowing on `kind` first: `ALWAYS_DENIED` has no filter to apply by mistake, and the compiler
 * rejects the attempt (docs/adr/0011-denial-fails-closed-in-the-result.md).
 */
export type QueryPlanToMongooseResult =
  | {
      kind: PlanKind.ALWAYS_ALLOWED | PlanKind.ALWAYS_DENIED;
    }
  | {
      kind: PlanKind.CONDITIONAL;
      filters: MongooseFilter;
    };

/**
 * Converts a Cerbos query plan to a Mongoose filter
 */
export function queryPlanToMongoose({
  queryPlan,
  mapper = {},
  nullAttributeRepresentation = "omitted",
}: QueryPlanToMongooseArgs): QueryPlanToMongooseResult {
  const ctx: TranslateContext = {
    mapper:
      nullAttributeRepresentation === "omitted"
        ? withOmittedNullDefault(mapper)
        : mapper,
    nullRepresentation: nullAttributeRepresentation,
    scope: { kind: "root" },
  };
  switch (queryPlan.kind) {
    case PlanKind.ALWAYS_ALLOWED:
      return { kind: PlanKind.ALWAYS_ALLOWED };
    case PlanKind.ALWAYS_DENIED:
      return { kind: PlanKind.ALWAYS_DENIED };
    case PlanKind.CONDITIONAL:
      return {
        kind: PlanKind.CONDITIONAL,
        filters: translateCondition(queryPlan.condition, ctx),
      };
    default:
      throw new UnsupportedQueryPlanError(`Invalid query plan.`);
  }
}
