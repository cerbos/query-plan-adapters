/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

/**
 * How the caller represents a NULL column in the attributes it sends to {@code check()}.
 *
 * <p>The planner emits the same {@code eq(attr, null)} node either way, so the adapter has to
 * be told.
 */
public enum NullAttributeRepresentation {

    /**
     * A NULL column is sent as an explicit {@code null} attribute, so {@code IS NULL} selects
     * exactly the rows {@code check()} allows. The default.
     */
    EXPLICIT,

    /**
     * A NULL column is sent as no attribute at all, which Cerbos denies as a missing-attribute
     * error. Null comparison operands are then rejected rather than translated to
     * {@code IS NULL}.
     */
    OMITTED
}
