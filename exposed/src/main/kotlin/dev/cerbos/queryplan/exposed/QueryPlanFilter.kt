package dev.cerbos.queryplan.exposed

import org.jetbrains.exposed.v1.core.Op

/**
 * A translated query plan.
 *
 * The three kinds are kept apart rather than collapsed into one predicate because an `Op` is a
 * value, not a deferred builder: an [AlwaysDenied] plan lets the caller skip the query entirely,
 * which a bare `Op.FALSE` would hide. Callers who always run the query use [toOp].
 */
public sealed interface QueryPlanFilter {
    /**
     * The filter as one predicate: `Op.TRUE` for [AlwaysAllowed], `Op.FALSE` for [AlwaysDenied],
     * and the translated condition for [Conditional]. Compose it with the application's own
     * predicates using Exposed's `and`.
     */
    public fun toOp(): Op<Boolean>

    /** `KIND_ALWAYS_ALLOWED`: every row is accessible; apply no filter. */
    public data object AlwaysAllowed : QueryPlanFilter {
        override fun toOp(): Op<Boolean> = Op.TRUE
    }

    /** `KIND_ALWAYS_DENIED`: no row is accessible; the query can be skipped. */
    public data object AlwaysDenied : QueryPlanFilter {
        override fun toOp(): Op<Boolean> = Op.FALSE
    }

    /**
     * `KIND_CONDITIONAL`: [op] selects exactly the rows the PDP allows.
     *
     * [op] references the mapped columns (and, for relations, correlated subqueries over the mapped
     * tables). It is an ordinary Exposed predicate: pass it to `where { }`, `andWhere { }` or a DAO
     * `find { }`.
     */
    public class Conditional(public val op: Op<Boolean>) : QueryPlanFilter {
        override fun toOp(): Op<Boolean> = op

        override fun toString(): String = "Conditional(op=$op)"
    }
}
