/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

/**
 * Whether the walk is proving a sub-tree true or false.
 *
 * <p>Negation is pushed down to the leaves instead of wrapping a clause in {@code bool.must_not},
 * which would match documents missing the field. Each leaf can then require the field to exist.
 */
enum Polarity {
    TRUE,
    FALSE;

    boolean holds() {
        return this == TRUE;
    }

    Polarity negate() {
        return this == TRUE ? FALSE : TRUE;
    }
}
