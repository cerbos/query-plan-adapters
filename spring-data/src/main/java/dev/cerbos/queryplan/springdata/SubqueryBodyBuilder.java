/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import jakarta.persistence.criteria.Join;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Subquery;

/**
 * Builds the body of a {@link ChainSubquery}. Each call re-translates it, because Hibernate 6
 * negation is stateful and a body used in both polarities needs a fresh tree each time (see
 * {@link TriPredicate}).
 */
@FunctionalInterface
interface SubqueryBodyBuilder {
    /**
     * @param sub          the subquery being built
     * @param tailJoin     the innermost join, over the chain's last Relation
     * @param rebasedOuter the enclosing scope re-rooted inside {@code sub} (see
     *                     {@link Scope#rebaseAt}), used to resolve non-lambda variables
     */
    Predicate build(Subquery<?> sub, Join<?, ?> tailJoin, Scope rebasedOuter);
}
