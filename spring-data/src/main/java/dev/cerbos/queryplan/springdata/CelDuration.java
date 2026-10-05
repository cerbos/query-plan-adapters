/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import com.google.protobuf.Value;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses the argument of CEL's {@code duration()}: Go's duration syntax, an optional sign and a
 * sequence of decimal numbers each with a unit ({@code "86400s"}, {@code "1h30m"},
 * {@code "-1.5ms"}, {@code "0"}). The planner sends the canonical seconds form, but a policy's
 * literal can reach the plan as written.
 */
final class CelDuration {

    private CelDuration() {}

    private static final Pattern TERM = Pattern.compile("(\\d+(?:\\.\\d*)?|\\.\\d+)(ns|us|µs|μs|ms|s|m|h)");

    private static final Map<String, BigDecimal> NANOS_PER_UNIT = Map.of(
            "ns", BigDecimal.ONE,
            "us", BigDecimal.valueOf(1_000L),
            "µs", BigDecimal.valueOf(1_000L),
            "μs", BigDecimal.valueOf(1_000L),
            "ms", BigDecimal.valueOf(1_000_000L),
            "s", BigDecimal.valueOf(1_000_000_000L),
            "m", BigDecimal.valueOf(60_000_000_000L),
            "h", BigDecimal.valueOf(3_600_000_000_000L));

    /** CEL's duration range, about 10000 years either way. */
    private static final BigInteger MAX_SECONDS = BigInteger.valueOf(315_576_000_000L);

    static Duration parse(Value value) {
        if (value.getKindCase() != Value.KindCase.STRING_VALUE) {
            // CEL's duration() takes a string, so the planner cannot emit anything else.
            throw Refusals.malformed("duration() constant must be a string, got "
                    + value.getKindCase());
        }
        String text = value.getStringValue();
        String body = text;
        boolean negative = false;
        if (body.startsWith("-") || body.startsWith("+")) {
            negative = body.startsWith("-");
            body = body.substring(1);
        }
        if ("0".equals(body)) {
            return Duration.ZERO;
        }
        Matcher m = TERM.matcher(body);
        BigDecimal nanos = BigDecimal.ZERO;
        int end = 0;
        while (m.find()) {
            if (m.start() != end) {
                break;
            }
            nanos = nanos.add(new BigDecimal(m.group(1).startsWith(".")
                    ? "0" + m.group(1) : m.group(1)).multiply(NANOS_PER_UNIT.get(m.group(2))));
            end = m.end();
        }
        if (end == 0 || end != body.length()) {
            throw Refusals.malformed("duration() constant could not be parsed as a duration");
        }
        // Go truncates a fraction of a nanosecond.
        BigInteger whole = nanos.toBigInteger();
        BigInteger[] secondsAndNanos = whole.divideAndRemainder(BigInteger.valueOf(1_000_000_000L));
        if (secondsAndNanos[0].compareTo(MAX_SECONDS) > 0) {
            throw Refusals.malformed("duration() constant is outside CEL's duration range");
        }
        Duration d = Duration.ofSeconds(secondsAndNanos[0].longValueExact(),
                secondsAndNanos[1].longValueExact());
        return negative ? d.negated() : d;
    }
}
