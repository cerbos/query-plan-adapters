/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

/**
 * A well-formed plan holds a shape the JPA Criteria API cannot express faithfully — a regex
 * match, a type cast, a list index, {@code mod}, {@code except()}, arithmetic composed on a
 * division whose denominator may be zero, a macro nested deeper than the configured bound.
 *
 * <p>This is the adapter failing closed: a wrong filter is an authorization bug that returns
 * rows the PDP denies, a throw is a bug report. The message names the mechanism, and the shared
 * corpus pins it ({@code conformance/actions.json}). Extends {@link IllegalArgumentException}
 * so a caller that catches the documented base type keeps working; catch this subtype to route
 * an inexpressible shape elsewhere (a {@code check()} per row, a different store) without
 * matching on the message. Some of these shapes can be intercepted with an
 * {@link OperatorFunction} override instead — the README's "Not yet supported" table says which.
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
