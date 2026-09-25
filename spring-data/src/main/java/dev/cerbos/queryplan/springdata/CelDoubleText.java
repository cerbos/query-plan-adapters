/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.regex.Pattern;

/**
 * Inverts CEL's {@code string(double)}, which renders Go's {@code %g} with the shortest digits
 * that round-trip: {@code "-0.6"}, {@code "100000"}, {@code "1e+06"}, {@code "1.5e-05"}.
 *
 * <p>SQL has no portable spelling of that rendering, so {@code string(col) == "text"} is solved
 * in Java for the one double that renders as {@code text}, and the column is compared with it.
 */
final class CelDoubleText {

    private CelDoubleText() {}

    /** Every finite, non-zero rendering: fixed notation, or exponent notation. */
    private static final Pattern RENDERING = Pattern.compile(
            "-?(?:(?:0|[1-9][0-9]*)(?:\\.[0-9]*[1-9])?|[1-9](?:\\.[0-9]*[1-9])?e[+-][0-9]{2,3})");

    /** The spellings CEL gives zero, NaN and the infinities, which a column comparison cannot pin. */
    private static final Pattern UNDECIDABLE = Pattern.compile("-?0|NaN|[+-]Inf");

    /** The outcome of {@link #solve}. */
    sealed interface Solution {
        /** {@code text} is exactly the rendering of {@code value}. */
        record Exactly(double value) implements Solution {}

        /** No double renders as {@code text}. */
        record None() implements Solution {}
    }

    /**
     * The double whose CEL rendering is {@code text}, or {@link Solution.None} when there is
     * none.
     *
     * @throws UnsupportedPlanShapeException when {@code text} is CEL's spelling of zero, NaN or
     *         an infinity, whose SQL comparison cannot tell the cases apart
     */
    static Solution solve(String text) {
        if (UNDECIDABLE.matcher(text).matches()) {
            throw Refusals.unsupported("string() of a number compared with \"" + text + "\": SQL"
                    + " cannot tell the zero, NaN or infinity CEL renders that way from its"
                    + " neighbours");
        }
        if (!RENDERING.matcher(text).matches()) {
            return new Solution.None();
        }
        double value = Double.parseDouble(text);
        if (Double.isInfinite(value)) {
            return new Solution.None();
        }
        String rendered = render(value);
        return text.equals(rendered) ? new Solution.Exactly(value) : new Solution.None();
    }

    /** Go's {@code fmt.Sprintf("%g", value)} for a finite, non-zero double. */
    static String render(double value) {
        BigDecimal exact = new BigDecimal(value);
        BigDecimal digits = shortest(value, exact);
        // Go's decision for the shortest precision: exponent notation when the decimal exponent
        // is below -4 or at least 6, so 100000 stays fixed and 1234567 is 1.234567e+06.
        BigDecimal unscaled = digits.stripTrailingZeros();
        int exponent = unscaled.precision() - unscaled.scale() - 1;
        if (exponent < -4 || exponent >= 6) {
            String mantissa = unscaled.movePointLeft(exponent).abs().toPlainString();
            String sign = exponent < 0 ? "-" : "+";
            String exp = String.valueOf(Math.abs(exponent));
            return (value < 0 ? "-" : "") + mantissa + "e" + sign + (exp.length() < 2 ? "0" + exp : exp);
        }
        return unscaled.toPlainString();
    }

    /**
     * The shortest decimal that parses back to {@code value}. At each length both neighbours of
     * the exact value are tried, since near a power of two the rounding interval is lopsided and
     * the nearer neighbour can fall outside it while the farther one does not.
     */
    private static BigDecimal shortest(double value, BigDecimal exact) {
        for (int p = 1; p <= 17; p++) {
            BigDecimal down = exact.round(new MathContext(p, RoundingMode.FLOOR));
            BigDecimal up = exact.round(new MathContext(p, RoundingMode.CEILING));
            boolean downOk = down.doubleValue() == value;
            boolean upOk = up.doubleValue() == value;
            if (downOk && upOk && down.compareTo(up) != 0) {
                int cmp = exact.subtract(down).compareTo(up.subtract(exact));
                if (cmp == 0) {
                    // Exactly halfway: Go rounds its decimal to even.
                    return down.round(new MathContext(p, RoundingMode.HALF_EVEN))
                            .unscaledValue().testBit(0) ? up : down;
                }
                return cmp < 0 ? down : up;
            }
            if (downOk) {
                return down;
            }
            if (upOk) {
                return up;
            }
        }
        throw Refusals.internal("no 17-digit decimal round-trips " + value);
    }
}
