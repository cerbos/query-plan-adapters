package dev.cerbos.queryplan.springdata;

import jakarta.persistence.criteria.Join;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Subquery;

/**
 * The body of a {@link ChainSubquery}, built fresh each time it is asked for.
 *
 * <p>Every invocation re-translates the body, so each occurrence gets a fresh Predicate
 * tree — Hibernate 6 negation is stateful (see {@link TriPredicate#not}), and the macro
 * subqueries consume the same body in both polarities.
 */
@FunctionalInterface
interface SubqueryBodyBuilder {
    /**
     * @param sub          the subquery being built
     * @param tailJoin     the join over the chain's TAIL Relation inside the subquery —
     *                     for a single Relation, the join over its collection; for a
     *                     multi-hop chain, the innermost join of the join chain
     * @param rebasedOuter the enclosing scope re-rooted for use inside {@code sub}
     *                     (see {@link Scope#rebaseAt}) — lambda bodies resolve
     *                     non-lambda variables (e.g. {@code request.resource.attr.x})
     *                     through this so outer references stay legal correlation paths
     */
    Predicate build(Subquery<?> sub, Join<?, ?> tailJoin, Scope rebasedOuter);
}
