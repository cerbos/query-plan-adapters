package dev.cerbos.queryplan.exposed

import java.math.BigDecimal
import java.math.BigInteger
import java.time.Duration

/**
 * Parses the argument of CEL's `duration()`: Go's duration syntax, an optional sign and a sequence
 * of decimal numbers each with a unit (`"86400s"`, `"1h30m"`, `"-1.5ms"`, `"0"`). The planner sends
 * the canonical seconds form, but a policy's literal can reach the plan as written.
 */
internal object CelDuration {

    private val TERM = Regex("(\\d+(?:\\.\\d*)?|\\.\\d+)(ns|us|µs|μs|ms|s|m|h)")

    private val NANOS_PER_UNIT: Map<String, BigDecimal> = mapOf(
        "ns" to BigDecimal.ONE,
        "us" to BigDecimal.valueOf(1_000L),
        "µs" to BigDecimal.valueOf(1_000L),
        "μs" to BigDecimal.valueOf(1_000L),
        "ms" to BigDecimal.valueOf(1_000_000L),
        "s" to BigDecimal.valueOf(1_000_000_000L),
        "m" to BigDecimal.valueOf(60_000_000_000L),
        "h" to BigDecimal.valueOf(3_600_000_000_000L),
    )

    /** CEL's duration range, about 10000 years either way. */
    private val MAX_SECONDS: BigInteger = BigInteger.valueOf(315_576_000_000L)

    private val NANOS_PER_SECOND: BigInteger = BigInteger.valueOf(1_000_000_000L)

    fun parse(raw: Any?): Duration {
        // CEL's duration() takes a string, so the planner cannot emit anything else.
        if (raw !is String) {
            throw Refusals.malformed("duration() constant must be a string, got ${PlanValues.typeName(raw)}")
        }
        var body = raw
        var negative = false
        if (body.startsWith("-") || body.startsWith("+")) {
            negative = body.startsWith("-")
            body = body.substring(1)
        }
        if (body == "0") return Duration.ZERO
        var nanos = BigDecimal.ZERO
        var end = 0
        for (match in TERM.findAll(body)) {
            if (match.range.first != end) break
            val number = match.groupValues[1].let { if (it.startsWith(".")) "0$it" else it }
            nanos = nanos.add(BigDecimal(number).multiply(NANOS_PER_UNIT.getValue(match.groupValues[2])))
            end = match.range.last + 1
        }
        if (end == 0 || end != body.length) {
            throw Refusals.malformed("duration() constant could not be parsed as a duration")
        }
        // Go truncates a fraction of a nanosecond.
        val (seconds, remainder) = nanos.toBigInteger().divideAndRemainder(NANOS_PER_SECOND)
        if (seconds > MAX_SECONDS) {
            throw Refusals.malformed("duration() constant is outside CEL's duration range")
        }
        val duration = Duration.ofSeconds(seconds.longValueExact(), remainder.longValueExact())
        return if (negative) duration.negated() else duration
    }
}
