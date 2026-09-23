/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

/**
 * The plan violates the planner's wire contract, for example an operator with the wrong arity,
 * a conditional plan with no condition, or a literal CEL's {@code timestamp()} would reject.
 *
 * <p>Planner output should never produce this, so it points to a hand-built plan or an
 * upstream bug rather than a policy to rewrite.
 */
public final class MalformedPlanException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    public MalformedPlanException(String message) {
        super(message);
    }

    public MalformedPlanException(String message, Throwable cause) {
        super(message, cause);
    }
}
