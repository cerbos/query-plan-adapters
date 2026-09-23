/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import java.util.List;
import java.util.Map;

/**
 * Query DSL builders. Every clause is a plain JDK {@code Map}, so no client library type reaches
 * the result.
 */
final class Queries {

    private Queries() {}

    /** Default translation of each leaf operator. A caller's override replaces an entry. */
    static final Map<String, OperatorFunction> DEFAULT_OPERATORS = Map.ofEntries(
            Map.entry("eq", Queries::term),
            Map.entry("lt", (field, value) -> range(field, "lt", value)),
            Map.entry("gt", (field, value) -> range(field, "gt", value)),
            Map.entry("le", (field, value) -> range(field, "lte", value)),
            Map.entry("ge", (field, value) -> range(field, "gte", value)),
            Map.entry("in", Queries::terms),
            Map.entry("contains", (field, value) -> wildcard(field, "*" + escapeWildcard(value) + "*")),
            Map.entry("startsWith", Queries::prefix),
            Map.entry("endsWith", (field, value) -> wildcard(field, "*" + escapeWildcard(value))),
            Map.entry("matches", RegexTranslator::matchesQuery),
            Map.entry("hasIntersection", Queries::terms));

    static Map<String, Object> term(String field, Object value) {
        return Map.of("term", Map.of(field, Map.of("value", value)));
    }

    /** A {@code terms} query; a scalar operand is read as a one-element list. */
    static Map<String, Object> terms(String field, Object value) {
        return Map.of("terms", Map.of(field, value instanceof List<?> list ? list : List.of(value)));
    }

    static Map<String, Object> prefix(String field, Object value) {
        return Map.of("prefix", Map.of(field, Map.of("value", value)));
    }

    private static Map<String, Object> range(String field, String bound, Object value) {
        return Map.of("range", Map.of(field, Map.of(bound, value)));
    }

    private static Map<String, Object> wildcard(String field, String pattern) {
        return Map.of("wildcard", Map.of(field, Map.of("value", pattern)));
    }

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

    private static String escapeWildcard(Object value) {
        return value.toString()
                .replace("\\", "\\\\")
                .replace("*", "\\*")
                .replace("?", "\\?");
    }
}
