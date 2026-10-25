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
     * exactly the rows {@code check()} allows.
     */
    EXPLICIT,

    /**
     * A NULL column is sent as no attribute at all, which Cerbos denies as a missing-attribute
     * error. Declared on an attribute, {@code eq}/{@code ne} against a bare null is UNKNOWN for
     * a NULL column under both polarities; every other null operand, and every one against an
     * undeclared attribute under this call-level convention, is rejected rather than translated
     * to {@code IS NULL}. The default: its wrong guess is a refusal, where {@link #EXPLICIT}'s is
     * an over-grant (docs/adr/0011-denial-fails-closed-in-the-result.md).
     */
    OMITTED
}
