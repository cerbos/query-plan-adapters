package dev.cerbos.queryplan.exposed

/**
 * How the caller represents a SQL `NULL` column in the attributes it sends to `check()`.
 *
 * The planner emits the same `eq(attr, null)` node under both conventions, so a plan cannot reveal
 * which one the caller uses and the adapter has to be told
 * (https://github.com/cerbos/query-plan-adapters/issues/302). Set the call-level default on
 * [Options], and override it per attribute on [AttributeMapping.Field] where one policy suite mixes
 * the two (https://github.com/cerbos/query-plan-adapters/issues/308).
 */
public enum class NullAttributeRepresentation {
    /**
     * A NULL column is sent as an explicit `null` attribute. `IS NULL` then selects exactly the
     * rows `check()` allows. The default.
     */
    EXPLICIT,

    /**
     * A NULL column sends no attribute at all, so CEL raises a missing-attribute error (a deny) and
     * a NULL-selecting filter would return rows the PDP denies. Null operands are refused.
     */
    OMITTED,
}
