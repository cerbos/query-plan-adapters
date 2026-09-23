/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import static dev.cerbos.queryplan.elasticsearch.Refusals.malformed;
import static dev.cerbos.queryplan.elasticsearch.Refusals.unsupported;

import com.google.protobuf.Value;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Converts plan literals to JDK values and validates them.
 */
final class PlanValues {

    private static final Pattern RFC3339_TIMESTAMP = Pattern.compile(
            "^(?!0000-)[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
                    + "(?:\\.[0-9]+)?(?:Z|[+-][0-9]{2}:[0-9]{2})$");
    private static final Instant CEL_TIMESTAMP_MIN = Instant.parse("0001-01-01T00:00:00Z");
    private static final Instant CEL_TIMESTAMP_MAX =
            Instant.parse("9999-12-31T23:59:59.999999999Z");

    /** Doubles in [-2^63, 2^63) convert to {@code long} without saturating. */
    private static final double LONG_MIN_AS_DOUBLE = -0x1p63;
    private static final double LONG_LIMIT_AS_DOUBLE = 0x1p63;

    private PlanValues() {}

    /**
     * The JDK value a protobuf {@link Value} carries.
     *
     * <p>The planner sends every number as a double. An integral double within {@code long} range
     * becomes a {@code long}; outside it, the cast would saturate, so it stays a double. Struct
     * values may be null, so a {@link LinkedHashMap} is used rather than {@code Map.of}.
     */
    static Object protoValueToJava(Value value) {
        return switch (value.getKindCase()) {
            case STRING_VALUE -> value.getStringValue();
            case NUMBER_VALUE -> {
                double d = value.getNumberValue();
                if (d == Math.rint(d) && d >= LONG_MIN_AS_DOUBLE && d < LONG_LIMIT_AS_DOUBLE) {
                    yield (long) d;
                }
                yield d;
            }
            case BOOL_VALUE -> value.getBoolValue();
            case NULL_VALUE -> null;
            case LIST_VALUE -> {
                List<Object> list = new ArrayList<>();
                value.getListValue().getValuesList()
                        .forEach(element -> list.add(protoValueToJava(element)));
                yield Collections.unmodifiableList(list);
            }
            case STRUCT_VALUE -> {
                Map<String, Object> struct = new LinkedHashMap<>();
                value.getStructValue().getFieldsMap()
                        .forEach((key, field) -> struct.put(key, protoValueToJava(field)));
                yield Collections.unmodifiableMap(struct);
            }
            default -> throw malformed("Unsupported protobuf value type: " + value.getKindCase());
        };
    }

    /**
     * Refuses NaN and infinity (e.g. CEL's {@code 1.0 / 0.0}), which JSON cannot represent.
     */
    static void rejectNonFinite(Object value) {
        if (value instanceof Double number && !Double.isFinite(number)) {
            throw unsupported("a non-finite numeric literal (" + number + ") cannot be bound: JSON "
                    + "has no representation for NaN or infinity, so no Query DSL request body "
                    + "can carry it");
        }
        if (value instanceof List<?> list) {
            list.forEach(PlanValues::rejectNonFinite);
        }
        if (value instanceof Map<?, ?> map) {
            map.values().forEach(PlanValues::rejectNonFinite);
        }
    }

    static void validateTimestampLiteral(Object value) {
        if (!(value instanceof String literal)) {
            throw malformed("timestamp() requires an RFC 3339 string literal");
        }
        if (!RFC3339_TIMESTAMP.matcher(literal).matches()) {
            throw invalidTimestampLiteral(literal, null);
        }
        try {
            Instant instant = OffsetDateTime.parse(literal, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                    .toInstant();
            if (instant.isBefore(CEL_TIMESTAMP_MIN) || instant.isAfter(CEL_TIMESTAMP_MAX)) {
                throw invalidTimestampLiteral(literal, null);
            }
            int nanos = instant.getNano();
            if (nanos % 1_000_000 != 0) {
                throw unsupported(
                        "Sub-millisecond timestamp literals require an explicit date_nanos "
                                + "mapping mode, which this adapter does not configure");
            }
        } catch (DateTimeParseException error) {
            throw invalidTimestampLiteral(literal, error);
        }
    }

    private static MalformedPlanException invalidTimestampLiteral(
            String literal, DateTimeParseException cause) {
        String message = "timestamp() requires a valid RFC 3339 string literal: " + literal;
        return cause == null
                ? new MalformedPlanException(message)
                : new MalformedPlanException(message, cause);
    }
}
