package dev.cerbos.queryplan.exposed

import java.math.BigDecimal
import java.math.MathContext
import java.math.RoundingMode

/**
 * CEL's `string()` over a double: Go's `strconv.FormatFloat(v, 'g', -1, 64)`, the shortest
 * decimal that round-trips, printed in `%e` form when its exponent is below -4 or at least 6
 * (`1e+06`, `-9.5e+18`) and in plain form otherwise (`2`, `0.5`, `123456`).
 */
internal object CelDoubleText {

    /**
     * The one double CEL prints as [text], or `null` when CEL prints no double that way. A
     * non-finite value is never returned: SQL has no literal for one.
     */
    fun parseCanonical(text: String): Double? {
        val value = text.toDoubleOrNull() ?: return null
        if (!value.isFinite()) return null
        return if (format(value) == text) value else null
    }

    /** [value] as CEL's `string()` prints it. [value] must be finite. */
    fun format(value: Double): String {
        require(value.isFinite()) { "CelDoubleText.format needs a finite value" }
        val negative = value < 0.0 || (value == 0.0 && 1.0 / value < 0.0)
        if (value == 0.0) return if (negative) "-0" else "0"
        val (digits, pointPosition) = shortestDigits(Math.abs(value))
        val exponent = pointPosition - 1
        val body = if (exponent < -4 || exponent >= 6) {
            buildString {
                append(digits[0])
                if (digits.length > 1) append('.').append(digits, 1, digits.length)
                append('e').append(if (exponent < 0) '-' else '+')
                val magnitude = Math.abs(exponent)
                if (magnitude < 10) append('0')
                append(magnitude)
            }
        } else if (pointPosition <= 0) {
            "0." + "0".repeat(-pointPosition) + digits
        } else if (pointPosition >= digits.length) {
            digits + "0".repeat(pointPosition - digits.length)
        } else {
            digits.substring(0, pointPosition) + "." + digits.substring(pointPosition)
        }
        return if (negative) "-$body" else body
    }

    /**
     * The shortest significant digits that round-trip to [value] (positive, finite), and the
     * position of the decimal point relative to them: `value = 0.<digits> × 10^pointPosition`.
     * Rounding the EXACT binary value to each precision in turn finds the nearest decimal of that
     * length, which is the one Go picks among equally short candidates.
     */
    private fun shortestDigits(value: Double): Pair<String, Int> {
        val exact = BigDecimal(value)
        for (precision in 1..17) {
            val rounded = exact.round(MathContext(precision, RoundingMode.HALF_EVEN))
            if (rounded.toDouble() == value) return normalise(rounded)
        }
        return normalise(exact.round(MathContext(17, RoundingMode.HALF_EVEN)))
    }

    private fun normalise(decimal: BigDecimal): Pair<String, Int> {
        val stripped = decimal.stripTrailingZeros()
        val digits = stripped.unscaledValue().toString()
        return digits to (digits.length - stripped.scale())
    }
}
