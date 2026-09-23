/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import com.google.protobuf.Value;

import java.util.LinkedHashMap;
import java.util.Map;

/** Protobuf-to-Java value conversion, {@code add} folding and solving, and LIKE escaping. */
final class PlanValues {

    private PlanValues() {}

    static Object protoValueToJava(Value value) {
        return switch (value.getKindCase()) {
            case STRING_VALUE -> value.getStringValue();
            case NUMBER_VALUE -> {
                double d = value.getNumberValue();
                // Only whole numbers inside the long range become longs: casting a larger
                // double saturates to Long.MIN/MAX_VALUE and changes the constant.
                if (d == Math.floor(d) && !Double.isInfinite(d)
                        && d >= -0x1p63 && d < 0x1p63) {
                    yield (long) d;
                }
                yield d;
            }
            case BOOL_VALUE -> value.getBoolValue();
            case NULL_VALUE -> null;
            case LIST_VALUE -> value.getListValue().getValuesList().stream()
                    .map(PlanValues::protoValueToJava)
                    .toList();
            case STRUCT_VALUE -> {
                // Not Collectors.toMap: it rejects null values, and struct fields may hold nulls.
                Map<String, Object> struct = new LinkedHashMap<>();
                value.getStructValue().getFieldsMap()
                        .forEach((k, v) -> struct.put(k, protoValueToJava(v)));
                yield struct;
            }
            case KIND_NOT_SET -> throw Refusals.malformed(
                    "Protobuf Value has no kind set — the planner emitted a malformed operand");
            default -> throw Refusals.malformed(
                    "Unsupported protobuf value type: " + value.getKindCase());
        };
    }

    /** Folds {@code add} over two constants: strings concatenate, numbers add. */
    static Object foldAdd(Object left, Object right) {
        if (left == null || right == null) {
            // `null + x` is a CEL no-overload error, so the planner never emits it. Messages
            // report types only: constants can carry principal attribute values.
            throw Refusals.malformed(
                    "add requires non-null operands, got " + typeName(left) + " + "
                            + typeName(right));
        }
        if (left instanceof String || right instanceof String) {
            return String.valueOf(left) + String.valueOf(right);
        }
        if (left instanceof Number ln && right instanceof Number rn) {
            if (left instanceof Long && right instanceof Long) {
                return ln.longValue() + rn.longValue();
            }
            return ln.doubleValue() + rn.doubleValue();
        }
        throw Refusals.malformed(
                "add requires string or numeric operands, got " + typeName(left) + " + "
                        + typeName(right));
    }

    /**
     * The type of a plan constant, for error messages. Never the value: constants can carry
     * principal attributes.
     */
    static String typeName(Object o) {
        return o == null ? "null" : o.getClass().getSimpleName();
    }

    /** 2<sup>53</sup>: every long up to this magnitude is exact as a double. */
    private static final long MAX_EXACT_DOUBLE_LONG = 1L << 53;

    /**
     * Whether {@code field + addConstant == comparisonValue} must be computed in SQL double
     * arithmetic instead of solved in Java.
     *
     * <p>IEEE subtraction does not invert addition ({@code -0.6 + 0.7 != 0.1}), so only
     * long pairs whose values and solution stay within ±2<sup>53</sup> are solved. Non-numeric
     * pairs return {@code false} and are left to {@link #solveAdd}.
     */
    static boolean requiresSqlLowering(Object comparisonValue, Object addConstant) {
        if (!(comparisonValue instanceof Number) || !(addConstant instanceof Number)) {
            return false;
        }
        return !(comparisonValue instanceof Long t && addConstant instanceof Long c
                && isExactLongSolve(t, c));
    }

    /**
     * Whether {@code t}, {@code c} and {@code t - c} are all within ±2<sup>53</sup>. The operands
     * are checked first, so the subtraction cannot overflow.
     */
    private static boolean isExactLongSolve(long t, long c) {
        return withinExactDoubleRange(t) && withinExactDoubleRange(c)
                && withinExactDoubleRange(t - c);
    }

    /** {@code Math.abs}-free range check: safe for {@code Long.MIN_VALUE}. */
    private static boolean withinExactDoubleRange(long v) {
        return -MAX_EXACT_DOUBLE_LONG <= v && v <= MAX_EXACT_DOUBLE_LONG;
    }

    /**
     * Solves {@code field + addConstant == comparisonValue} ({@code addConstant + field} when
     * {@code !fieldIsLeft}) for the value the field must equal. For strings, returns
     * {@code null} when no field value can match. Numeric pairs that
     * {@link #requiresSqlLowering} rejects must not reach here.
     */
    static Object solveAdd(Object comparisonValue, Object addConstant, boolean fieldIsLeft) {
        if (comparisonValue instanceof String compStr && addConstant instanceof String constStr) {
            if (fieldIsLeft) {
                // field + const == comparison  →  field == comparison stripped-of-suffix
                if (!compStr.endsWith(constStr)) return null;
                return compStr.substring(0, compStr.length() - constStr.length());
            }
            // const + field == comparison  →  field == comparison stripped-of-prefix
            if (!compStr.startsWith(constStr)) return null;
            return compStr.substring(constStr.length());
        }
        if (comparisonValue instanceof Number && addConstant instanceof Number) {
            if (comparisonValue instanceof Long t && addConstant instanceof Long c
                    && isExactLongSolve(t, c)) {
                return t - c;
            }
            throw Refusals.internal(
                    "Numeric add-solve is only exact for integer constants within ±2^53; "
                            + "this shape must be lowered to SQL double arithmetic instead");
        }
        // `R.attr.x + 1 == "abc"` is valid CEL over a dyn attribute but has no column translation.
        throw Refusals.unsupported(
                "add comparison type mismatch: " + comparisonValue.getClass() + " vs " + addConstant.getClass());
    }

    /**
     * Escapes {@code LIKE} wildcards; pair with an explicit {@code '\\'} escape character.
     * {@code [} is escaped because SQL Server treats {@code [...]} as a character class; on
     * other dialects {@code \[} is just a literal {@code [}.
     */
    static String escapeLike(String s) {
        return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
                .replace("[", "\\[");
    }
}
