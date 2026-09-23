/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

/**
 * The plan breaks the planner's wire contract, for example an operator with the wrong number of
 * operands or an invalid {@code timestamp()} literal. The Cerbos planner should never produce one.
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
