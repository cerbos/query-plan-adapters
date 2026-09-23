/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Expression;
import jakarta.persistence.criteria.From;
import jakarta.persistence.criteria.Join;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;
import jakarta.persistence.criteria.Subquery;

import java.util.stream.Collectors;

/**
 * The correlated subqueries every collection operator is built on: EXISTS, COUNT, the
 * tri-state macro score and the strict match count. Callers choose the shape; every shape
 * ranges over the same flattened elements of the relation chain.
 *
 * <p>Relation subqueries throw when the Specification is evaluated outside its own SELECT,
 * because they cannot correlate there (see {@link SpringDataQueryPlanAdapter}).
 */
final class ChainSubqueries {

    private final CriteriaBuilder cb;
    private final TriPredicate tri;
    private final boolean selectInvocation;

    ChainSubqueries(CriteriaBuilder cb, TriPredicate tri, boolean selectInvocation) {
        this.cb = cb;
        this.tri = tri;
        this.selectInvocation = selectInvocation;
    }

    /**
     * Scores each element for {@code exists} (true 2, false 0) and {@code all} (true 0,
     * false 2):
     *
     * <pre>{@code CASE WHEN body THEN trueScore WHEN NOT body THEN falseScore ELSE 1 END}</pre>
     *
     * An UNKNOWN body takes neither branch and scores 1. The subquery selects
     * {@code NULLIF(COALESCE(MAX(score), 0), 1)}: an empty collection gives 0, and an UNKNOWN
     * element that dominates gives NULL. Comparing the result with 2 or 0 then gives TRUE,
     * FALSE or UNKNOWN as the CEL macro does.
     *
     * <p>The body is translated twice, once per polarity (see {@link TriPredicate}), so nested
     * macros grow as {@code 2^depth}.
     */
    Subquery<Integer> macroScoreSubquery(Scope scope, Scope.ResolvedRelation ref,
                                         SubqueryBodyBuilder bodyBuilder,
                                         int trueScore, int falseScore) {
        ChainSubquery<Integer> cs = chainSubquery(Integer.class, scope, ref);
        Expression<Integer> score = cb.<Integer>selectCase()
                .when(cs.body(bodyBuilder), trueScore)
                .when(tri.not(cs.body(bodyBuilder)), falseScore)
                .otherwise(1);
        cs.sub().select(cb.nullif(cb.coalesce(cb.max(score), 0), 1));
        return cs.sub();
    }

    /**
     * Counts elements whose body is true, for {@code exists_one} and
     * {@code size(filter(...))}:
     *
     * <pre>{@code COALESCE(SUM(CASE WHEN body THEN 1 ELSE 0 END), 0) + poisonTerm}</pre>
     *
     * These CEL macros error if any element errors, so {@link #undeterminedPoisonTerm} makes
     * the count NULL when any body is UNKNOWN. An empty collection counts 0.
     */
    Subquery<Long> strictMatchCountSubquery(Scope scope, Scope.ResolvedRelation ref,
                                            SubqueryBodyBuilder bodyBuilder) {
        ChainSubquery<Long> cs = chainSubquery(Long.class, scope, ref);
        Expression<Long> match = cb.<Long>selectCase()
                .when(cs.body(bodyBuilder), 1L)
                .otherwise(0L);
        cs.sub().select(cb.sum(
                cb.coalesce(cb.sum(match), 0L),
                undeterminedPoisonTerm(cs, bodyBuilder)));
        return cs.sub();
    }

    /**
     * Selects only the poison term, for a {@code size(filter(...))} comparison that is decided
     * statically but must still deny when a body is UNKNOWN.
     */
    Subquery<Long> undeterminedPoisonSubquery(Scope scope, Scope.ResolvedRelation ref,
                                              SubqueryBodyBuilder bodyBuilder) {
        ChainSubquery<Long> cs = chainSubquery(Long.class, scope, ref);
        cs.sub().select(undeterminedPoisonTerm(cs, bodyBuilder));
        return cs.sub();
    }

    /**
     * {@code NULLIF(COALESCE(MAX(CASE WHEN body THEN 0 WHEN NOT body THEN 0 ELSE 1 END), 0), 1)}:
     * 0 when every body is determined or the collection is empty, NULL when any is UNKNOWN.
     */
    private Expression<Long> undeterminedPoisonTerm(
            ChainSubquery<?> cs, SubqueryBodyBuilder bodyBuilder) {
        Expression<Long> determined = cb.<Long>selectCase()
                .when(cs.body(bodyBuilder), 0L)
                .when(tri.not(cs.body(bodyBuilder)), 0L)
                .otherwise(1L);
        return cb.nullif(cb.coalesce(cb.max(determined), 0L), 1L);
    }

    @SuppressWarnings("unchecked")
    private static From<?, ?> correlate(Subquery<?> sub, From<?, ?> outerFrom) {
        if (outerFrom instanceof Root<?> r) {
            return sub.correlate(r);
        }
        if (outerFrom instanceof Join<?, ?> j) {
            return sub.correlate((Join<Object, Object>) j);
        }
        // Every Scope is rooted at a Root or a Join; no plan input reaches this.
        throw Refusals.internal("Cannot correlate from non-Root, non-Join scope: " + outerFrom);
    }

    /**
     * Builds the skeleton of every relation subquery. It correlates on
     * {@code ref.owner().from()}, not the current scope's {@code from()}, which inside a lambda
     * is the element join and does not hold outer relations. A multi-hop chain joins through
     * every hop, so the subquery ranges over the flattened tail elements.
     */
    <T> ChainSubquery<T> chainSubquery(Class<T> resultType, Scope scope,
                                       Scope.ResolvedRelation ref) {
        if (!selectInvocation) {
            String chain = ref.chain().stream()
                    .map(AttributeMapping.Relation::joinAttribute)
                    .collect(Collectors.joining("."));
            throw new UnsupportedOperationException(
                    "Relation '" + chain + "' requires a correlated subquery, but this Specification "
                    + "is being evaluated outside its own SELECT query — e.g. via "
                    + "repository.delete(Specification) or a criteria bulk delete/update. "
                    + "Hibernate's multi-table bulk delete first clears @ElementCollection/join "
                    + "tables using this same predicate, which self-invalidates the correlated "
                    + "subquery: 0 entity rows are deleted while their collection rows are "
                    + "silently destroyed. The Cerbos Specification is SELECT-only; fetch the "
                    + "matching ids with findAll(spec) and delete them with deleteAllById(ids).");
        }
        Subquery<T> sub = scope.parentQuery().subquery(resultType);
        From<?, ?> correlated = correlate(sub, ref.owner().from());
        From<?, ?> joinedFrom = correlated;
        Predicate anchor = null;
        if (rejoinsAnEnclosingElement(scope, ref)) {
            // Hibernate 7 gives every correlated copy of a From the original's alias and a fresh
            // join counter, so a chain joined off it here gets the very navigable path the
            // enclosing lambda's element join got in ITS subquery — `tags.exists(t,
            // tags.exists(u, u.name != t.name))` renders `t2_0.name<>t2_0.name` and compares each
            // tag with itself (#509). A fresh range variable over the owner's entity, pinned to
            // the correlated row by identity, starts a path of its own on every Hibernate major.
            if (!(correlated instanceof Root<?> owner)) {
                throw Refusals.unsupported("Cannot nest a collection macro over '"
                        + ref.chain().stream()
                                .map(AttributeMapping.Relation::joinAttribute)
                                .collect(Collectors.joining("."))
                        + "' inside a lambda over the same relation when the relation is owned by "
                        + "a collection element: the inner subquery can only be given its own range "
                        + "variable over an entity root");
            }
            Root<?> fresh = sub.from(owner.getModel());
            anchor = cb.equal(fresh, correlated);
            sub.where(anchor);
            joinedFrom = fresh;
        }
        Join<?, ?> join = joinedFrom.join(ref.chain().get(0).joinAttribute());
        for (int i = 1; i < ref.chain().size(); i++) {
            join = join.join(ref.chain().get(i).joinAttribute());
        }
        Scope rebased = Scope.rebaseAt(scope, ref.owner(), correlated, sub);
        return new ChainSubquery<>(sub, join, rebased, anchor);
    }

    /**
     * Whether a lambda between {@code scope} and the chain's owner iterates an element joined
     * through one of the chain's own attributes — the case where the chain's join, taken off a
     * fresh correlation of the owner, can collide with that element's (see
     * {@link #chainSubquery}). Matched by attribute name, which over-approximates: a false
     * positive costs one redundant self-join, a miss compares an element with itself.
     */
    private static boolean rejoinsAnEnclosingElement(Scope scope, Scope.ResolvedRelation ref) {
        for (Scope level = scope; level != null && level != ref.owner();
             level = level instanceof Scope.LambdaScope ls ? ls.outer() : null) {
            if (level instanceof Scope.LambdaScope ls && ref.chain().stream()
                    .anyMatch(r -> r.joinAttribute().equals(ls.relation().joinAttribute()))) {
                return true;
            }
        }
        return false;
    }

    /** Set {@code cs}'s WHERE to {@code predicate}, conjoined with its fresh-root anchor if any. */
    void restrict(ChainSubquery<?> cs, Predicate predicate) {
        cs.sub().where(cs.anchor() == null ? predicate : cb.and(cs.anchor(), predicate));
    }

    /** A chain subquery selecting {@code COUNT(tailJoin)}. */
    ChainSubquery<Long> countSubquery(Scope scope, Scope.ResolvedRelation ref) {
        ChainSubquery<Long> cs = chainSubquery(Long.class, scope, ref);
        cs.sub().select(cb.count(cs.tailJoin()));
        return cs;
    }

    /**
     * Whether every intermediate hop of a dotted path exists, or {@code null} for a direct
     * relation.
     *
     * <p>Each intermediate segment of {@code a.b.c} is a to-one parent. When it is absent, CEL
     * raises a missing-path error and denies, but a subquery sees the same empty result as a
     * parent with no children.
     */
    Predicate leadingHopsExist(Scope scope, Scope.ResolvedRelation ref) {
        if (!ref.isChained()) {
            return null;
        }
        Scope.ResolvedRelation hops = new Scope.ResolvedRelation(
                ref.owner(), ref.chain().subList(0, ref.chain().size() - 1));
        return existsSubquery(scope, hops, (sub, tailJoin, rebased) -> cb.conjunction());
    }

    /**
     * Returns {@code value}, or SQL NULL when an intermediate hop is absent, so the enclosing
     * comparison is UNKNOWN under both polarities.
     */
    <N> Expression<N> requireLeadingHops(
            Scope scope, Scope.ResolvedRelation ref,
            Expression<N> value, Class<N> type) {
        Predicate guard = leadingHopsExist(scope, ref);
        if (guard == null) {
            return value;
        }
        return cb.<N>selectCase().when(guard, value).otherwise(cb.nullLiteral(type));
    }

    Predicate existsSubquery(Scope scope, Scope.ResolvedRelation ref,
                             SubqueryBodyBuilder bodyBuilder) {
        ChainSubquery<Integer> cs = chainSubquery(Integer.class, scope, ref);
        cs.sub().select(cb.literal(1));
        restrict(cs, cs.body(bodyBuilder));
        return cb.exists(cs.sub());
    }

    /**
     * Whether some element satisfies the body, UNKNOWN when an intermediate hop is absent.
     * Use this rather than {@link #existsSubquery} for any existence test over a chain:
     * {@code NOT EXISTS} is TRUE for an absent parent and would readmit the row. A direct
     * relation keeps the plain {@code EXISTS}.
     */
    Predicate chainContains(Scope scope, Scope.ResolvedRelation ref,
                            SubqueryBodyBuilder bodyBuilder) {
        if (!ref.isChained()) {
            return existsSubquery(scope, ref, bodyBuilder);
        }
        ChainSubquery<Long> cs = countSubquery(scope, ref);
        restrict(cs, cs.body(bodyBuilder));
        return cb.greaterThan(
                requireLeadingHops(scope, ref, cs.sub(), Long.class), 0L);
    }
}
