/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

/**
 * A well-formed plan holds a shape the Elasticsearch Query DSL cannot express without scripts —
 * field-to-field comparison, arithmetic, a conditional value, a count threshold, a polarity that
 * cannot tell a missing collection from an empty one.
 *
 * <p>This is the adapter failing closed: a wrong filter is an authorization bug, a throw is a bug
 * report. The message names the mechanism, and the shared corpus pins it
 * ({@code conformance/actions.json}). Extends {@link IllegalArgumentException} so a caller that
 * catches the documented base type keeps working; catch this subtype to route an inexpressible
 * shape elsewhere (a {@code check()} per row, a different store) without matching on the message.
 */
public final class UnsupportedPlanShapeException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    public UnsupportedPlanShapeException(String message) {
        super(message);
    }

    public UnsupportedPlanShapeException(String message, Throwable cause) {
        super(message, cause);
    }
}
