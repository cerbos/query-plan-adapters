/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import jakarta.persistence.criteria.Join;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Subquery;

/**
 * A correlated subquery over a relation chain: {@code sub} correlates the chain owner's
 * {@code From} and joins through every hop to {@code tailJoin}; {@code rebasedOuter} is the
 * enclosing scope re-rooted inside {@code sub}. Built only by
 * {@link ChainSubqueries#chainSubquery}.
 */
record ChainSubquery<T>(Subquery<T> sub, Join<?, ?> tailJoin, Scope rebasedOuter) {

    /** Translates {@code builder}'s body afresh over this subquery's element join. */
    Predicate body(SubqueryBodyBuilder builder) {
        return builder.build(sub, tailJoin, rebasedOuter);
    }
}
