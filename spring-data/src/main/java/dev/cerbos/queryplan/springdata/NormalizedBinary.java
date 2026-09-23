/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

import java.util.List;
import java.util.Set;

/**
 * A binary expression reordered so the most field-like operand comes first (variable, then
 * expression, then constant). The planner keeps policy source order, so {@code 5 < R.attr.x}
 * arrives value-first and is mirrored to {@code gt}; {@link OperatorFunction} overrides are
 * then looked up under the mirrored operator.
 *
 * <p>Only symmetric operators and {@code lt}/{@code gt}/{@code le}/{@code ge} are reordered.
 * The string-match methods are not: in {@code "a,b".contains(R.attr.x)} the constant is the
 * haystack, and swapping would invert haystack and needle.
 */
record NormalizedBinary(String op, List<Operand> operands) {

    /** Operators whose operands may be reordered without changing meaning. */
    private static final Set<String> ORDER_NORMALIZABLE = Set.of(
            "eq", "ne", "lt", "gt", "le", "ge",
            "in", "hasIntersection", "has_intersection");

    static NormalizedBinary of(String op, List<Operand> operands) {
        if (ORDER_NORMALIZABLE.contains(op)
                && operands.size() == 2
                && rank(operands.get(0)) < rank(operands.get(1))) {
            return new NormalizedBinary(mirror(op), List.of(operands.get(1), operands.get(0)));
        }
        return new NormalizedBinary(op, operands);
    }

    private static int rank(Operand o) {
        return switch (o.getNodeCase()) {
            case VARIABLE -> 2;
            case EXPRESSION -> 1;
            default -> 0;
        };
    }

    /** lt/le/gt/ge mirror when their operands swap sides; symmetric operators are unchanged. */
    static String mirror(String op) {
        return switch (op) {
            case "lt" -> "gt";
            case "gt" -> "lt";
            case "le" -> "ge";
            case "ge" -> "le";
            default -> op;
        };
    }
}
