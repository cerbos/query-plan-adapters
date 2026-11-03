/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.jpa;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

import java.util.Objects;

/**
 * A translated plan, from {@link JpaQueryPlanAdapter#toFilter}. It is immutable and safe to
 * share; each {@link #toPredicate} call builds a new {@link Predicate} for one query.
 *
 * <p>Build a predicate per query, and never reuse one across queries: a predicate is bound to the
 * {@link Root} it was built on, and Hibernate 6 rejects it under another, such as the root of the
 * {@code COUNT} query beside a paged select.
 *
 * <p><strong>The filter is SELECT-only.</strong> Do not use its predicate in a
 * {@code CriteriaDelete} or {@code CriteriaUpdate}: with a {@link AttributeMapping.Relation} in
 * play, Hibernate's bulk delete clears the collection tables first and the correlated subquery
 * then deletes no entity rows. A relation translated outside the SELECT whose root it was given
 * throws {@link UnsupportedOperationException}. Select the ids and delete by id instead.
 */
public final class QueryPlanFilter {

    private enum Kind { ALWAYS_ALLOWED, ALWAYS_DENIED, CONDITIONAL }

    private static final QueryPlanFilter ALWAYS_ALLOWED =
            new QueryPlanFilter(Kind.ALWAYS_ALLOWED, null, null);
    private static final QueryPlanFilter ALWAYS_DENIED =
            new QueryPlanFilter(Kind.ALWAYS_DENIED, null, null);

    private final Kind kind;
    private final Operand condition;
    private final JpaQueryPlanAdapter.Options options;

    private QueryPlanFilter(Kind kind, Operand condition, JpaQueryPlanAdapter.Options options) {
        this.kind = kind;
        this.condition = condition;
        this.options = options;
    }

    static QueryPlanFilter alwaysAllowed() {
        return ALWAYS_ALLOWED;
    }

    static QueryPlanFilter alwaysDenied() {
        return ALWAYS_DENIED;
    }

    static QueryPlanFilter conditional(Operand condition, JpaQueryPlanAdapter.Options options) {
        return new QueryPlanFilter(Kind.CONDITIONAL, condition, options);
    }

    /**
     * Whether the plan allowed every row, so the predicate is always true. A caller that can
     * omit its {@code WHERE} clause may test this; the predicate is correct either way.
     *
     * @return {@code true} for a {@code KIND_ALWAYS_ALLOWED} plan
     */
    public boolean isAlwaysAllowed() {
        return kind == Kind.ALWAYS_ALLOWED;
    }

    /**
     * Builds the predicate selecting the rows the plan permits.
     *
     * @param root the entity root, one of {@code query}'s roots
     * @param query the SELECT the predicate is for; relations correlate subqueries to it
     * @param cb the criteria builder of the {@code EntityManager} that runs {@code query}
     * @return a new predicate; never {@code null}
     * @throws UnsupportedPlanShapeException if the plan has a shape the adapter cannot translate
     * @throws UnmappedAttributeException if the plan names a variable the mapping does not map,
     *         or maps the wrong way round for its operator
     * @throws MalformedPlanException if the plan is not one the PDP emits
     * @throws UnsupportedOperationException if a relation is translated while {@code root} is
     *         not one of {@code query}'s roots, as in a bulk delete
     */
    public Predicate toPredicate(Root<?> root, CriteriaQuery<?> query, CriteriaBuilder cb) {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(cb, "cb");
        return switch (kind) {
            case ALWAYS_ALLOWED -> cb.conjunction();
            case ALWAYS_DENIED -> cb.disjunction();
            case CONDITIONAL -> new PlanWalker(cb, options, isSelectInvocation(root, query))
                    .traverse(condition, Scope.root(root, query, options.mapping()));
        };
    }

    /**
     * Whether the predicate is built for a SELECT. There the {@code Root} is one of
     * {@code query.getRoots()}; Spring Data's {@code delete(Specification)} passes a root from
     * a {@code CriteriaDelete} and an unrelated or null query.
     */
    private static boolean isSelectInvocation(Root<?> root, CriteriaQuery<?> query) {
        return query != null && query.getRoots().contains(root);
    }
}
