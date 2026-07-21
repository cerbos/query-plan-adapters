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
     * Solve {@code field + addConstant == comparisonValue} (or with operands swapped if
     * {@code !fieldIsLeft}). For strings: strip the prefix/suffix and return what the field must
     * equal; return {@code null} if the comparison value doesn't match the constant's
     * shape (which means no field value can satisfy the equation). For numbers: subtract.
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
        if (comparisonValue instanceof Number compNum && addConstant instanceof Number constNum) {
            // Both orderings of numeric addition produce the same equation: field = comp - const
            if (comparisonValue instanceof Long && addConstant instanceof Long) {
                return compNum.longValue() - constNum.longValue();
            }
            return compNum.doubleValue() - constNum.doubleValue();
        }
        throw new IllegalArgumentException(
                "add comparison type mismatch: " + comparisonValue.getClass() + " vs " + addConstant.getClass());
    }

    /** Escape {@code LIKE} wildcards; pair with an explicit {@code '\\'} escape character. */
    static String escapeLike(String s) {
        return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
    }
}
