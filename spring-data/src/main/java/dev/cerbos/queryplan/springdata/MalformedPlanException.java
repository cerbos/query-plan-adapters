/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

/**
 * The plan violates the planner's wire contract — an operator with the wrong arity, a lambda
 * whose second operand is not a variable, a conditional plan with no condition, an unknown
 * filter kind, a protobuf value with no kind, a literal CEL's own {@code timestamp()} would
 * reject, a constant comparison the planner should have folded away.
 *
 * <p>No Cerbos planner output should produce one of these, so a caller seeing it has either a
 * hand-built plan or an upstream bug to report, not a policy shape to rewrite. The shared
 * corpus is planner output and reaches none of these sites; {@code RefusalTypesTest} pins that.
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
