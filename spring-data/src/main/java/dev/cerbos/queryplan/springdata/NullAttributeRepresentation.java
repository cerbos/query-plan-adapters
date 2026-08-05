package dev.cerbos.queryplan.springdata;

/**
 * How the caller represents a NULL column when building the attributes it sends to
 * {@code check()}.
 *
 * <p>The planner emits the same {@code eq(attr, null)} node either way, so the plan cannot
 * reveal which convention is in use and the adapter has to be told.
 *
 * <p>See <a href="https://github.com/cerbos/query-plan-adapters/issues/302">issue #302</a>.
 */
public enum NullAttributeRepresentation {

    /**
     * A NULL column is sent as an explicit {@code null} attribute. CEL compares
     * {@code null == null}, so {@code IS NULL} selects exactly the rows {@code check()} allows.
     * This is the historical behaviour and the default.
     */
    EXPLICIT,

    /**
     * A NULL column sends no attribute at all. CEL then raises a missing-attribute error, which
     * Cerbos treats as a deny, so a filter that <em>selects</em> NULL rows returns rows the PDP
     * denies. Null comparison operands are rejected instead of translated.
     */
    OMITTED
}
