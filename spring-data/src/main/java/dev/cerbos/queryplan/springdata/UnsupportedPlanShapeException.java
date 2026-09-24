/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

/**
 * A well-formed plan holds a shape the JPA Criteria API cannot express faithfully, such as a
 * regex match, a type cast, a list index, {@code except()}, or a macro nested deeper than the
 * configured bound.
 *
 * <p>The adapter throws rather than emit a filter that could return rows the PDP denies. Catch
 * this type to route such plans elsewhere (for example a {@code check()} per row). Some of these
 * shapes can be handled with an {@link OperatorFunction} override; the README's
 * "Not yet supported" table says which.
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
