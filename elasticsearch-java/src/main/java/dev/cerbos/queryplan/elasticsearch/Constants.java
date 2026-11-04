/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import static dev.cerbos.queryplan.elasticsearch.Refusals.unsupported;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

import java.util.List;
import java.util.Map;

/**
 * Comparisons between two literals, which a hoisted ternary leaves behind: the planner folds a
 * constant comparison it can see, but {@code (c ? x : 0) > 1} keeps {@code 0 > 1} under the
 * ternary. Each is decided here with CEL's rules; a shape CEL errors on matches nothing.
 */
final class Constants {

    private Constants() {}

    /** {@code match_all} when the comparison holds under the polarity, else {@code match_none}. */
    static Map<String, Object> comparison(String operator, List<Operand> operands, Polarity polarity) {
        if (operands.size() != 2) {
            throw unsupported(operator + " over literal operands cannot be decided: the planner"
                    + " folds a constant comparison, so only a ternary branch leaves one, and it"
                    + " compares two literals");
        }
        Object left = literal(operands.get(0));
        Object right = literal(operands.get(1));
        Boolean holds = switch (operator) {
            case "eq" -> equal(left, right);
            case "ne" -> !equal(left, right);
            case "in" -> contains(right, left);
            case "lt", "le", "gt", "ge" -> {
                Integer order = order(left, right);
                if (order == null) {
                    yield null;
                }
                yield switch (operator) {
                    case "lt" -> order < 0;
                    case "le" -> order <= 0;
                    case "gt" -> order > 0;
                    default -> order >= 0;
                };
            }
            default -> throw unsupported(operator + " over two literals cannot be decided without"
                    + " evaluating CEL: only equality, membership and ordering are folded here");
        };
        if (holds == null) {
            return Queries.matchNone();
        }
        return holds == polarity.holds() ? Queries.matchAll() : Queries.matchNone();
    }

    private static Object literal(Operand operand) {
        if (operand.getNodeCase() == Operand.NodeCase.EXPRESSION) {
            Object value = PlanValues.protoValueToJava(operand.getExpression().getOperands(0).getValue());
            return new Timestamp(PlanValues.canonicalTimestampLiteral(value));
        }
        Object value = PlanValues.protoValueToJava(operand.getValue());
        PlanValues.rejectNonFinite(value);
        return value;
    }

    private record Timestamp(String instant) {}

    /** CEL equality: numbers compare by value across int and double; other types by structure. */
    static boolean equal(Object left, Object right) {
        if (left == null || right == null) {
            return left == right;
        }
        if (left instanceof Number a && right instanceof Number b) {
            return a.doubleValue() == b.doubleValue();
        }
        if (left instanceof List<?> a && right instanceof List<?> b) {
            if (a.size() != b.size()) {
                return false;
            }
            for (int i = 0; i < a.size(); i++) {
                if (!equal(a.get(i), b.get(i))) {
                    return false;
                }
            }
            return true;
        }
        if (left instanceof Map<?, ?> a && right instanceof Map<?, ?> b) {
            return a.keySet().equals(b.keySet())
                    && a.keySet().stream().allMatch(key -> equal(a.get(key), b.get(key)));
        }
        return left.equals(right);
    }

    /** {@code needle in container}: a list's elements, or a map's keys; null for a CEL error. */
    private static Boolean contains(Object container, Object needle) {
        if (container instanceof List<?> list) {
            return list.stream().anyMatch(element -> equal(element, needle));
        }
        if (container instanceof Map<?, ?> map) {
            return map.keySet().stream().anyMatch(key -> equal(key, needle));
        }
        return null;
    }

    /** CEL ordering within one type (code-point order for strings); null for a CEL error. */
    private static Integer order(Object left, Object right) {
        if (left instanceof Number a && right instanceof Number b) {
            // Double.compare orders -0.0 below 0.0, which CEL holds equal.
            return a.doubleValue() < b.doubleValue() ? -1 : a.doubleValue() > b.doubleValue() ? 1 : 0;
        }
        if (left instanceof String a && right instanceof String b) {
            return compareCodePoints(a, b);
        }
        if (left instanceof Boolean a && right instanceof Boolean b) {
            return Boolean.compare(a, b);
        }
        if (left instanceof Timestamp a && right instanceof Timestamp b) {
            return a.instant().compareTo(b.instant());
        }
        return null;
    }

    private static int compareCodePoints(String a, String b) {
        int[] left = a.codePoints().toArray();
        int[] right = b.codePoints().toArray();
        for (int i = 0; i < Math.min(left.length, right.length); i++) {
            if (left[i] != right[i]) {
                return Integer.compare(left[i], right[i]);
            }
        }
        return Integer.compare(left.length, right.length);
    }
}
