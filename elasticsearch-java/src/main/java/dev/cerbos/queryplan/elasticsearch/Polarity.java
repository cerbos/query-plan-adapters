/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

/**
 * Which truth value the walk is proving for the sub-tree it is on.
 *
 * <p>Negation is pushed down the tree rather than wrapped around a clause: {@code not(and(a, b))}
 * is walked as {@code or} of the children under {@link #FALSE}, so every leaf lowers the exact
 * predicate it is asked for, and the missing-field cases are decided at the leaf — where the field
 * is known — instead of by a {@code bool.must_not} that would match every document without one.
 * The one walk parameterised by this enum replaces the {@code …False} twin of every traversal
 * method the adapter used to carry.
 */
enum Polarity {
    TRUE,
    FALSE;

    /** {@code true} under {@link #TRUE}: the boolean a bare variable operand is compared to. */
    boolean holds() {
        return this == TRUE;
    }

    /** The polarity a {@code not} flips to. */
    Polarity negate() {
        return this == TRUE ? FALSE : TRUE;
    }
}
