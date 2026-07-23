package dev.cerbos.queryplan.springdata;

import com.google.protobuf.Value;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Conversions between protobuf plan values and plain Java values, plus the constant-folding
 * helpers for the {@code add} operator and shared SQL literal escaping.
 */
final class PlanValues {

    private PlanValues() {}

    static Object protoValueToJava(Value value) {
        return switch (value.getKindCase()) {
            case STRING_VALUE -> value.getStringValue();
            case NUMBER_VALUE -> {
                double d = value.getNumberValue();
                // Whole numbers become longs only inside [-2^63, 2^63): casting a double
                // outside that range saturates to Long.MIN/MAX_VALUE (JLS 5.1.3), silently
                // changing the constant. Out-of-range values stay doubles, which every
                // comparison path already handles in double space.
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
            case KIND_NOT_SET -> throw new IllegalArgumentException(
                    "Protobuf Value has no kind set — the planner emitted a malformed operand");
            default -> throw new IllegalArgumentException(
                    "Unsupported protobuf value type: " + value.getKindCase());
        };
    }

    /**
     * Fold {@code add(left, right)} where both operands are constants. Strings concatenate;
     * numbers add. Used when the planner emits e.g. {@code eq(field, add("prefix:", "123"))}.
     */
    static Object foldAdd(Object left, Object right) {
        if (left == null || right == null) {
            // Reaching here means the planner emitted `add(null, ...)` or `add(..., null)`
            // — neither side could satisfy any string/number equation, so report the shape
            // explicitly rather than NPE'ing on `.getClass()` below.
            throw new IllegalArgumentException(
                    "add requires non-null operands, got " + left + " + " + right);
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
        throw new IllegalArgumentException(
                "add requires string or numeric operands, got " + left.getClass() + " + " + right.getClass());
    }

    /**
     * Largest magnitude at which every long is exactly representable as an IEEE double
     * (2<sup>53</sup>). CEL attribute arithmetic is always double-typed at check time, so
     * beyond this bound the check-time arithmetic has gaps between representable integers
     * and an algebraic long-space solve could disagree with what the PDP evaluates.
     */
    private static final long MAX_EXACT_DOUBLE_LONG = 1L << 53;

    /**
     * Whether {@code field + addConstant eq/ne comparisonValue} must be lowered to SQL-side
     * double arithmetic instead of solved algebraically in Java.
     *
     * <p>IEEE subtraction does not invert IEEE addition: {@code fl(fl(t - c) + c) != t} for
     * many double pairs — e.g. {@code t = 0.1, c = 0.7}: the algebraic solve yields exactly
     * {@code -0.6}, yet {@code -0.6 + 0.7 == 0.09999999999999998 != 0.1}, so a pre-solved
     * {@code field = -0.6} filter returns rows the PDP's {@code check()} denies (and the
     * {@code ne} mirror hides rows it allows). Only long/long pairs where every value —
     * including the solution — stays within ±2<sup>53</sup> remain on the solve path: there
     * both the Java solve and the check-time double arithmetic are exact. Non-numeric
     * pairings return {@code false} so {@link #solveAdd} keeps owning their translation
     * (string concatenation) and their type-mismatch error messages.
     */
    static boolean requiresSqlLowering(Object comparisonValue, Object addConstant) {
        if (!(comparisonValue instanceof Number) || !(addConstant instanceof Number)) {
            return false;
        }
        return !(comparisonValue instanceof Long t && addConstant instanceof Long c
                && isExactLongSolve(t, c));
    }

    /**
     * True when {@code t - c} is exact in both long and double space: {@code t}, {@code c},
     * and the solution all within ±2<sup>53</sup>. The bounds on {@code t} and {@code c} are
     * checked first, capping {@code |t - c|} at 2<sup>54</sup> — so the subtraction cannot
     * overflow before its own range check runs.
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
     * Solve {@code field + addConstant == comparisonValue} (or with operands swapped if
     * {@code !fieldIsLeft}) — for the ALGEBRAICALLY EXACT shapes only. For strings: strip the
     * prefix/suffix and return what the field must equal; return {@code null} if the
     * comparison value doesn't match the constant's shape (which means no field value can
     * satisfy the equation). For numbers: subtract, but only in-range long/long pairs —
     * fractional or oversized numbers are not invertible in IEEE double space and must be
     * lowered to SQL-side double arithmetic by the caller (see {@link #requiresSqlLowering});
     * reaching that case here is an adapter routing bug.
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
            // Both orderings of numeric addition produce the same equation: field = comp - const
            if (comparisonValue instanceof Long t && addConstant instanceof Long c
                    && isExactLongSolve(t, c)) {
                return t - c;
            }
            throw new IllegalArgumentException(
                    "Numeric add-solve is only exact for integer constants within ±2^53; "
                            + "this shape must be lowered to SQL double arithmetic instead");
        }
        throw new IllegalArgumentException(
                "add comparison type mismatch: " + comparisonValue.getClass() + " vs " + addConstant.getClass());
    }

    /**
     * Escape {@code LIKE} wildcards; pair with an explicit {@code '\\'} escape character.
     *
     * <p>{@code [} is escaped because SQL Server / Sybase {@code LIKE} treats {@code [...]}
     * as a character class even when an {@code ESCAPE} clause is declared — an unescaped
     * {@code '[SEC]%'} matches one character from {@code {S,E,C}} instead of the literal
     * prefix {@code [SEC]}. With the escape declared, {@code \[} means a literal {@code [}
     * on every targeted dialect (verified empirically on H2, PostgreSQL, and MySQL, where
     * {@code [} is otherwise inert — the escape is a semantic no-op there). {@code ]} needs
     * no escaping: it is only special on SQL Server as the closer of a class, and no class
     * can open once every {@code [} is escaped.
     */
    static String escapeLike(String s) {
        return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
                .replace("[", "\\[");
    }
}
