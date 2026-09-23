/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

/**
 * Factories for every exception the adapter raises, plus the refusals raised from more than
 * one place, so each pinned message is spelled once.
 *
 * <p>{@link #internal} is for adapter bugs, not refusals: it returns an
 * {@link IllegalStateException}, so it is never mistaken for a refusal of the plan.
 */
final class Refusals {

    private Refusals() {}

    static UnsupportedPlanShapeException unsupported(String message) {
        return new UnsupportedPlanShapeException(message);
    }

    static MalformedPlanException malformed(String message) {
        return new MalformedPlanException(message);
    }

    static MalformedPlanException malformed(String message, Throwable cause) {
        return new MalformedPlanException(message, cause);
    }

    static UnmappedAttributeException unmapped(String message) {
        return new UnmappedAttributeException(message);
    }

    /** An invariant no plan input can violate; reaching it is an adapter bug, not a refusal. */
    static IllegalStateException internal(String message) {
        return new IllegalStateException(message);
    }

    /** Cerbos {@code except(list, list)} is a list difference, which JPA Criteria cannot express. */
    static UnsupportedPlanShapeException exceptUnsupported() {
        return unsupported(
                "except is not supported: Cerbos except(list, list) computes a list "
                        + "difference, which has no JPA Criteria translation. Rewrite the "
                        + "policy with a collection macro instead — e.g. "
                        + "size(R.attr.tags.except([\"x\"])) > 0 is equivalent to "
                        + "R.attr.tags.exists(t, !(t in [\"x\"])).");
    }

    /** A plan variable the mapping does not name, or names only behind a relation chain. */
    static UnmappedAttributeException unknownAttribute(String cerbosVar) {
        return unmapped("Unknown attribute: " + cerbosVar);
    }

    /** A lambda body references a name the plan never bound. */
    static MalformedPlanException notALambdaReference(String variable, String lambdaVar) {
        return malformed("Variable '" + variable + "' does not start with lambda variable '"
                + lambdaVar + "'");
    }

    /**
     * A null comparison operand under {@link NullAttributeRepresentation#OMITTED}, where
     * {@code check()} denies NULL rows that a NULL-selecting filter would return.
     */
    static UnsupportedPlanShapeException nullOperandUnderOmitted(String operator) {
        return unsupported(
                "Cannot translate `" + operator + "` against a null operand"
                        + " under NullAttributeRepresentation.OMITTED: a NULL column sends no"
                        + " attribute, so Cerbos evaluates the comparison as a"
                        + " missing-attribute error (deny) while a NULL-selecting filter"
                        + " would return those rows. Send NULL columns as explicit nulls and"
                        + " use EXPLICIT, or keep this shape out of the policy.");
    }

    /**
     * Describes an operand for error messages: the variable name, the operator, or for a
     * constant only its protobuf kind, never its value.
     */
    static String describeOperand(Operand o) {
        return switch (o.getNodeCase()) {
            case VARIABLE -> "VARIABLE '" + o.getVariable() + "'";
            case EXPRESSION -> "EXPRESSION " + o.getExpression().getOperator() + "()";
            // Not converted: conversion can throw, and this runs inside error paths.
            case VALUE -> "VALUE (" + o.getValue().getKindCase() + ")";
            default -> o.getNodeCase().toString();
        };
    }
}
