/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

import java.util.List;
import java.util.Set;

/**
 * A binary expression normalized to field-side-first. The planner preserves policy source
 * order, so a constant may precede the field it constrains ({@code 5 < R.attr.x} arrives
 * as {@code lt(value(5), variable(x))}). Normalizing once here — most field-like operand
 * first (variable > nested expression > constant value), mirroring directional operators
 * when swapping — lets every downstream handler assume field-first order. A consequence
 * is that {@link OperatorFunction} overrides are consulted under the mirrored operator:
 * a value-first {@code lt} is looked up as {@code gt}.
 *
 * <p>Only operators whose semantics survive a swap are reordered: symmetric ones
 * ({@code eq}/{@code ne}/{@code in}/{@code hasIntersection}) and the mirrorable
 * inequalities ({@code lt}/{@code gt}/{@code le}/{@code ge}). The CEL string-match
 * methods ({@code contains}/{@code startsWith}/{@code endsWith}) are RECEIVER-SENSITIVE:
 * {@code "a,b".contains(R.attr.x)} arrives as {@code contains(value, variable)} where
 * the constant is the haystack — swapping it would silently invert haystack and needle
 * (translating {@code x LIKE '%a,b%'} instead of testing whether {@code "a,b"} contains
 * the column value). Those keep planner source order and are handled positionally by
 * the constant-receiver case of {@link ComparisonTranslator#dispatch}.
 *
 * <p>This is the one place operand order is decided: the comparison seam, {@code in} and
 * {@code hasIntersection} all normalize through {@link #of}, and the arithmetic and
 * timestamp paths reuse {@link #mirror} when they have to swap a pair this record could not
 * rank (two EXPRESSION nodes rank equally).
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
