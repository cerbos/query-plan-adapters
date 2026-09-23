/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

/**
 * Exception factories, and the refusals raised from more than one place so each message is
 * written once.
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
     * {@code except(list, list)} is a list difference, which has no Query DSL form. It arrives
     * inside {@code size()} or as a comparison operand.
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
     * A CEL ternary used as a condition. A ternary inside a leaf operand is refused by the leaf
     * translator with its own message.
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
