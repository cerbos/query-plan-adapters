/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

/**
 * The exception factories, and the refusals more than one collaborator raises by name.
 *
 * <p>Every throw site in the package goes through one of the three factories, which is what makes
 * the classification a property of the walk rather than of the message text. The named refusals
 * below are the ones raised from more than one place — {@code except} from the walk, the leaf
 * resolver and {@code size()}; the explicit-null convention from the pre-walk scan and the null
 * leaf — so the message is spelled once and the corpus pin in {@code conformance/actions.json}
 * has one string to hold.
 */
final class Refusals {

    private Refusals() {}

    static UnsupportedPlanShapeException unsupported(String message) {
        return new UnsupportedPlanShapeException(message);
    }

    static MalformedPlanException malformed(String message) {
        return new MalformedPlanException(message);
    }

    static UnmappedAttributeException unmapped(String message) {
        return new UnmappedAttributeException(message);
    }

    /**
     * Cerbos {@code except()} is a two-list function ({@code list.except(list)}) whose
     * list-difference result has no Query DSL translation. PDP-verified arrival shapes: inside
     * {@code size()} ({@code gt(size(except(variable, value-list)), 0)}) and as a comparison
     * operand ({@code eq(except(variable, value-list), value-list)}); the lambda form this adapter
     * once translated never appears on the wire.
     */
    static UnsupportedPlanShapeException exceptUnsupported() {
        return unsupported(
                "except is not supported: Cerbos except(list, list) computes a list difference, "
                        + "which has no Elasticsearch Query DSL translation. Rewrite the policy "
                        + "with a collection macro instead — e.g. "
                        + "size(R.attr.tags.except([\"x\"])) > 0 is equivalent to "
                        + "R.attr.tags.exists(t, !(t in [\"x\"])).");
    }

    /**
     * A CEL ternary used as a condition. The Query DSL has no conditional-value expression: a
     * {@code bool} clause combines boolean sub-queries, and there is nowhere to evaluate a
     * condition and choose between two VALUES on the way. A ternary that arrives as a leaf operand
     * rather than as a condition is refused by the leaf resolver instead, with its own message.
     */
    static UnsupportedPlanShapeException ternaryUnsupported() {
        return unsupported("if (CEL ternary) cannot be expressed: Elasticsearch Query DSL has no "
                + "conditional-value expression without scripts");
    }

    static UnsupportedPlanShapeException unsafeExplicitNullComparison() {
        return unsupported(
                "Elasticsearch cannot distinguish an explicit null value from a missing field "
                        + "without an indexed null-value sentinel");
    }
}
