/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import java.util.List;
import java.util.Map;

/**
 * The Query DSL builders: the default lowering of each leaf operator, and the compound shapes
 * ({@code bool}, {@code nested}, {@code exists}) the translators assemble them into.
 *
 * <p>Every emitted clause is a plain JDK {@code Map} built here, so the whole package agrees on
 * one spelling per shape and no client library type ever reaches the result — a property the
 * golden asset asserts.
 */
final class Queries {

    private Queries() {}

    static final Map<String, OperatorFunction> DEFAULT_OPERATORS = Map.ofEntries(
            Map.entry("eq", (field, value) ->
                    Map.of("term", Map.of(field, Map.of("value", value)))),
            Map.entry("ne", (field, value) ->
                    Map.of("bool", Map.of("must_not", List.of(
                            Map.of("term", Map.of(field, Map.of("value", value))))))),
            Map.entry("lt", (field, value) ->
                    Map.of("range", Map.of(field, Map.of("lt", value)))),
            Map.entry("gt", (field, value) ->
                    Map.of("range", Map.of(field, Map.of("gt", value)))),
            Map.entry("le", (field, value) ->
                    Map.of("range", Map.of(field, Map.of("lte", value)))),
            Map.entry("ge", (field, value) ->
                    Map.of("range", Map.of(field, Map.of("gte", value)))),
            Map.entry("in", (field, value) ->
                    Map.of("terms", Map.of(field, value instanceof List<?> l ? l : List.of(value)))),
            Map.entry("contains", (field, value) ->
                    Map.of("wildcard", Map.of(field, Map.of("value", "*" + escapeWildcard(value) + "*")))),
            Map.entry("startsWith", (field, value) ->
                    Map.of("prefix", Map.of(field, Map.of("value", value)))),
            Map.entry("endsWith", (field, value) ->
                    Map.of("wildcard", Map.of(field, Map.of("value", "*" + escapeWildcard(value))))),
            Map.entry("matches", RegexTranslator::matchesQuery),
            Map.entry("hasIntersection", (field, value) ->
                    Map.of("terms", Map.of(field, value instanceof List<?> l ? l : List.of(value))))
    );

    static Map<String, Object> boolMust(List<Map<String, Object>> clauses) {
        return Map.of("bool", Map.of("must", clauses));
    }

    static Map<String, Object> boolShould(List<Map<String, Object>> clauses) {
        return Map.of("bool", Map.of("should", clauses, "minimum_should_match", 1));
    }

    static Map<String, Object> notQuery(Map<String, Object> query) {
        return Map.of("bool", Map.of("must_not", List.of(query)));
    }

    static Map<String, Object> matchAll() {
        return Map.of("match_all", Map.of());
    }

    static Map<String, Object> matchNone() {
        return Map.of("match_none", Map.of());
    }

    static Map<String, Object> exists(String field) {
        return Map.of("exists", Map.of("field", field));
    }

    static Map<String, Object> notExists(String field) {
        return notQuery(exists(field));
    }

    static Map<String, Object> definedAndNot(String field, Map<String, Object> query) {
        return boolMust(List.of(exists(field), notQuery(query)));
    }

    static Map<String, Object> nestedQuery(String path, Map<String, Object> query) {
        return Map.of("nested", Map.of("path", path, "query", query));
    }

    static String escapeWildcard(Object value) {
        return value.toString()
                .replace("\\", "\\\\")
                .replace("*", "\\*")
                .replace("?", "\\?");
    }
}
