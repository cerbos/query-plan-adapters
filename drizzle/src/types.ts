import type { PlanKind, PlanResourcesResponse, Value } from "@cerbos/core";
import type { AnyColumn, SQL, Table } from "drizzle-orm";
import type { Indexable } from "./indexed";

/**
 * The adapter's type model: the public types `index.ts` re-exports, plus the internal mapper
 * shapes they are built from. Nothing here has a runtime value except the scoped-entry brand.
 */

export type DrizzleFilter = SQL;

export const SCOPED_RELATION = Symbol("ScopedRelationEntry");

export type ComparisonOperator =
  | "eq"
  | "ne"
  | "lt"
  | "le"
  | "gt"
  | "ge"
  | "in"
  | "contains"
  | "startsWith"
  | "endsWith";

export type MapperTransform = (args: {
  operator: ComparisonOperator;
  value: Value;
}) => SQL;

export type MappingConfig = {
  column?: AnyColumn;
  transform?: MapperTransform;
  relation?: RelationMapping;
  valueType?: "timestamp";
  collectionValueType?: "scalar";
  /** Ordered column storage for constant positional access; never inferred from a relation. */
  indexable?: Indexable;
  /**
   * Declares that this column can be SQL NULL **and** how the caller represents that NULL in
   * the attributes it sends to `check()`. Declaring it asserts both facts; leaving it undeclared
   * means "treat this column as NOT NULL", which is the historical rendering.
   *
   * This is per attribute rather than per call because one policy suite can legitimately mix the
   * two conventions — the same column can be mapped twice, sent as an explicit null under one
   * attribute name and omitted under another. The call-level option cannot express that, which is
   * what made cerbos/query-plan-adapters#308 unfixable with #302's option alone.
   *
   * - `"explicit"` — a NULL column is sent as an explicit `null` attribute, so CEL holds a null
   *   VALUE. `null != "x"` is then TRUE and `null == "x"` is FALSE, both definite. SQL's
   *   `NULL <> 'x'` is UNKNOWN, which excludes the row under both polarities, so the equality
   *   family (`eq`, `ne`, `in`) is rendered so it can never be UNKNOWN.
   * - `"omitted"` — a NULL column sends no attribute, so CEL raises a missing-attribute error and
   *   `check()` denies. UNKNOWN already excludes the row under both polarities, so the rendering
   *   is unchanged; what the declaration adds is the same null-operand rejection the call-level
   *   `"omitted"` performs, scoped to this attribute.
   *
   * Only the equality family is affected. `lt`/`le`/`gt`/`ge` and the string operators raise a
   * no-overload error on a null receiver in CEL, which denies under both polarities exactly as
   * UNKNOWN does, so they keep propagating it.
   *
   * See https://github.com/cerbos/query-plan-adapters/issues/308.
   */
  nullAttributeRepresentation?: NullAttributeRepresentation;
};

export interface RelationValue {
  kind: "relation";
  relation: RelationMapping;
}

export type BaseMapperEntry =
  AnyColumn | MappingConfig | MapperTransform | RelationValue;

/** A reference resolved to its mapping, plus the relation chain that has to be joined to reach it. */
export interface ResolvedMapping {
  relations: RelationMapping[];
  mapping: BaseMapperEntry;
}

export interface ScopedRelationEntry {
  [SCOPED_RELATION]: true;
  resolve: () => {
    relations: RelationMapping[];
    mapping: BaseMapperEntry;
  };
}

export type MapperEntry = BaseMapperEntry | ScopedRelationEntry;

export type Mapper =
  | {
      [key: string]: MapperEntry | undefined;
    }
  | ((reference: string) => MapperEntry | undefined);

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
 *   PDP denies. Null comparison operands are rejected instead of translated.
 *
 * See https://github.com/cerbos/query-plan-adapters/issues/302.
 */
export type NullAttributeRepresentation = "explicit" | "omitted";

export interface QueryPlanToDrizzleArgs {
  queryPlan: PlanResourcesResponse;
  mapper: Mapper;
  /**
   * Which NULL-column representation the caller uses when building `check()` attributes.
   * Defaults to `"explicit"`, preserving the historical `IS NULL` translation.
   */
  nullAttributeRepresentation?: NullAttributeRepresentation;
}

export type QueryPlanToDrizzleResult =
  | {
      kind: PlanKind.ALWAYS_ALLOWED | PlanKind.ALWAYS_DENIED;
    }
  | {
      kind: PlanKind.CONDITIONAL;
      filter: DrizzleFilter;
    };

export interface RelationMapping {
  type: "one" | "many";
  table: Table;
  sourceColumn: AnyColumn;
  targetColumn: AnyColumn;
  field?: MapperEntry;
  fields?: { [key: string]: MapperEntry };
  /**
   * The predicate the application itself applies when it reads this relation — a soft-delete
   * flag, a tenant column, a subtype discriminator, whatever narrows the rows that became the
   * resource attributes.
   *
   * The adapter reads `table` directly. Drizzle has no association metadata to consult, so
   * nothing that narrows the application's own read reaches the generated `EXISTS`, and any
   * such narrowing makes the subquery see rows the application never serialised — a filter that
   * returns rows the PDP denies. Declaring the predicate here restores the equality; the adapter
   * cannot detect the omission, which is why the field is optional and silence is not a warning.
   * See "Mapping hazards" in the README.
   *
   * It is ANDed into the correlated subquery over `table`, so it constrains every operator
   * reached through this relation — `exists`, `all`, membership, counts — under both polarities.
   *
   * ```ts
   * relation: {
   *   type: "many",
   *   table: tags,
   *   sourceColumn: resources.id,
   *   targetColumn: tags.resourceId,
   *   subqueryFilter: isNull(tags.deletedAt),
   * }
   * ```
   */
  subqueryFilter?: SQL;
}

/** State threaded through one translation. Never global: a mapper may re-enter the adapter. */
export type BuildFilterOptions = {
  nullRepresentation: NullAttributeRepresentation;
  /**
   * Relations an enclosing lambda subquery has already joined. Their columns are directly
   * addressable, so they must not be re-joined (or re-required) off the root.
   */
  skipRelations?: Set<RelationMapping>;
  /**
   * The tables enclosing collection-macro subqueries range over, outermost first. A macro over
   * one of them again must alias its own subquery: otherwise its element columns and the
   * enclosing element's render as the same `"table"."column"`, SQL resolves both to the
   * innermost row, and the lambda compares an element with itself (#509).
   */
  openTables?: readonly string[];
};
