/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import jakarta.persistence.criteria.Join;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Subquery;

/**
 * A correlated subquery spanning a resolved relation chain: {@code sub} correlates the
 * chain OWNER's {@code From} and joins through every hop to {@code tailJoin}, with
 * {@code rebasedOuter} being the evaluation scope re-rooted inside {@code sub}.
 *
 * <p>This is the one correlated-subquery skeleton. Every collection operator — the macros,
 * {@code in}/{@code hasIntersection} membership, {@code size()} — composes over an instance
 * built by {@link ChainSubqueries#chainSubquery}, which is where the two join-anchoring
 * invariants (owner-anchored correlation, joining through every hop) are enforced once.
 */
record ChainSubquery<T>(Subquery<T> sub, Join<?, ?> tailJoin, Scope rebasedOuter) {

    /** A fresh translation of {@code builder}'s body over this subquery's element join. */
    Predicate body(SubqueryBodyBuilder builder) {
        return builder.build(sub, tailJoin, rebasedOuter);
    }
}
