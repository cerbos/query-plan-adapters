package dev.cerbos.queryplan.springdata;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

/**
 * The exception factories, and the refusals more than one collaborator raises by name.
 *
 * <p>Every refusal in the package goes through one of the three factories, which is what makes
 * the classification a property of the walk rather than of the message text: the site that
 * raises a refusal is the site that knows whether the plan was malformed, the mapping was short,
 * or the Criteria API has no shape for it. The named refusals below are the ones raised from
 * more than one place — {@code except} from the walk, the leaf resolver and {@code size()};
 * the unknown attribute from both resolution arms of {@link Scope}; the OMITTED-convention
 * rejection from the pre-walk scan — so the message is spelled once and the corpus pin in
 * {@code conformance/actions.json} has one string to hold.
 *
 * <p>{@link #internal} is deliberately not a refusal. A branch that only an adapter bug can
 * reach — a switch default under a guard that already enumerated its cases, a routing
 * invariant between two collaborators — is an {@link IllegalStateException}, so a caller
 * catching the documented {@link IllegalArgumentException} base type never mistakes one for a
 * classified refusal.
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

    /**
     * Cerbos {@code except()} is a two-list function ({@code list.except(list)}) whose
     * list-difference result has no JPA Criteria translation. PDP-verified arrival shapes:
     * inside {@code size()} ({@code gt(size(except(variable, value-list)), 0)}) and as a
     * comparison operand ({@code eq(except(variable, value-list), value-list)}); the lambda
     * form this adapter once translated never appears on the wire.
     */
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

    /**
     * A lambda body reference that is neither the lambda's own variable nor resolvable further
     * out — a name the plan never bound.
     */
    static MalformedPlanException notALambdaReference(String variable, String lambdaVar) {
        return malformed("Variable '" + variable + "' does not start with lambda variable '"
                + lambdaVar + "'");
    }

    /**
     * A null comparison operand under {@link NullAttributeRepresentation#OMITTED}: a NULL
     * column then sends no attribute, so CEL raises a missing-attribute error and
     * {@code check()} denies the row, while a NULL-selecting filter would return it
     * (cerbos/query-plan-adapters#302).
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
     * Shape-only description of an operand for error messages: node case plus the attribute
     * name (VARIABLE) or inner operator (EXPRESSION). Constant VALUES report their type
     * only — never their content — matching the adapter's no-value-leak discipline.
     */
    static String describeOperand(Operand o) {
        return switch (o.getNodeCase()) {
            case VARIABLE -> "VARIABLE '" + o.getVariable() + "'";
            case EXPRESSION -> "EXPRESSION " + o.getExpression().getOperator() + "()";
            // The protobuf kind, not the converted value: conversion could itself throw on
            // a malformed VALUE, and this helper must stay safe inside error paths.
            case VALUE -> "VALUE (" + o.getValue().getKindCase() + ")";
            default -> o.getNodeCase().toString();
        };
    }
}
