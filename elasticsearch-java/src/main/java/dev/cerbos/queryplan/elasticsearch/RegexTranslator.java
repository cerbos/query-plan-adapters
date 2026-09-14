/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import static dev.cerbos.queryplan.elasticsearch.Refusals.unsupported;

import java.util.Map;

/**
 * The {@code matches()} lowering: a {@code prefix} query for a plain {@code ^literal}, otherwise a
 * {@code regexp} query over the RE2/Lucene subset the two engines agree on.
 *
 * <p>CEL {@code matches()} uses RE2 partial-match semantics. Elasticsearch's {@code regexp} query
 * uses Lucene regex with whole-field semantics, and Lucene {@code .} includes newlines while RE2
 * {@code .} does not. Only explicitly whole-field patterns in the common syntax subset reach
 * Lucene; simple {@code ^literal} prefixes use {@code prefix}. Optional Lucene operators are
 * disabled at the query site with {@code flags=NONE}. The subset validation is its own class
 * because it is a small parser with its own refusals, and nothing else in the walk needs it.
 */
final class RegexTranslator {

    private RegexTranslator() {}

    static Map<String, Object> matchesQuery(String field, Object value) {
        String pattern = value.toString();
        boolean anchoredStart = pattern.startsWith("^");
        String body = anchoredStart ? pattern.substring(1) : pattern;
        boolean anchoredEnd = body.endsWith("$") && !isEscaped(body, body.length() - 1);
        if (anchoredStart && !anchoredEnd && !body.isEmpty() && isPlainRegexLiteral(body)) {
            return Map.of("prefix", Map.of(field, Map.of("value", body)));
        }
        return Map.of("regexp", Map.of(field, Map.of(
                "value", toLuceneRegex(pattern),
                "flags", "NONE")));
    }

    private static boolean isPlainRegexLiteral(String pattern) {
        for (int index = 0; index < pattern.length(); index++) {
            if ("\\.[](){}?*+|^$".indexOf(pattern.charAt(index)) >= 0) {
                return false;
            }
        }
        return true;
    }

    // CEL `matches()` uses RE2 partial-match semantics. Elasticsearch's `regexp`
    // query uses Lucene regex with whole-field semantics, and Lucene `.` includes
    // newlines while RE2 `.` does not. Only explicitly whole-field patterns in the
    // common syntax subset reach Lucene; simple `^literal` prefixes use `prefix`.
    // Optional Lucene operators are disabled at the query site with flags=NONE.
    static String toLuceneRegex(String celPattern) {
        boolean anchoredStart = celPattern.startsWith("^");
        String body = anchoredStart ? celPattern.substring(1) : celPattern;
        boolean anchoredEnd = body.endsWith("$") && !isEscaped(body, body.length() - 1);
        if (anchoredEnd) {
            body = body.substring(0, body.length() - 1);
        }
        body = validateAndEscapeLuceneRegexBody(body);
        if (!anchoredStart || !anchoredEnd) {
            throw unsupported(
                    "matches regex patterns must be fully anchored unless they are a simple "
                            + "literal prefix");
        }
        if (body.isEmpty()) {
            throw unsupported(
                    "matches regex for only the empty string is not supported by Elasticsearch");
        }
        return body;
    }

    private static String validateAndEscapeLuceneRegexBody(String pattern) {
        StringBuilder translated = new StringBuilder(pattern.length());
        boolean escaped = false;
        boolean inCharacterClass = false;
        int depth = 0;
        for (int index = 0; index < pattern.length(); index++) {
            char current = pattern.charAt(index);
            if (escaped) {
                if (Character.isLetterOrDigit(current)) {
                    throw unsupportedRegexSyntax(pattern, index - 1);
                }
                translated.append('\\').append(current);
                escaped = false;
                continue;
            }
            if (current == '\\') {
                escaped = true;
                continue;
            }
            if (current == '[') {
                if (inCharacterClass) {
                    throw unsupportedRegexSyntax(pattern, index);
                }
                inCharacterClass = true;
                translated.append(current);
                continue;
            }
            if (current == ']') {
                if (!inCharacterClass) {
                    throw unsupportedRegexSyntax(pattern, index);
                }
                inCharacterClass = false;
                translated.append(current);
                continue;
            }
            if (inCharacterClass) {
                translated.append(current == '"' ? "\\\"" : String.valueOf(current));
                continue;
            }
            if (current == '^' || current == '$' || current == '.') {
                throw unsupportedRegexSyntax(pattern, index);
            }
            if (current == '(') {
                if (index + 1 < pattern.length() && pattern.charAt(index + 1) == '?') {
                    throw unsupportedRegexSyntax(pattern, index);
                }
                depth++;
            } else if (current == ')') {
                if (depth == 0) {
                    throw unsupportedRegexSyntax(pattern, index);
                }
                depth--;
            } else if (current == '|' && depth == 0) {
                // RE2 parses `^a|b$` as two alternatives, each anchored on one side only; Lucene
                // matches the whole field against `a|b`. The two languages agree only once the
                // alternation is parenthesised under both anchors.
                throw unsupported("matches regex has a top-level alternation at index " + index
                        + ": RE2 reads ^a|b$ as two separately anchored alternatives, while "
                        + "Lucene matches the whole field against a|b, so the alternation must "
                        + "be parenthesised as ^(a|b)$: " + pattern);
            } else if (current == '{') {
                int close = intervalEnd(pattern, index);
                if (close < 0) {
                    throw unsupported("matches regex has a brace at index " + index
                            + " that does not begin a {n}, {n,} or {n,m} repetition, which "
                            + "Lucene rejects at query time: " + pattern);
                }
                translated.append(pattern, index, close + 1);
                index = close;
                continue;
            }
            if (current == '"') {
                translated.append("\\\"");
            } else {
                translated.append(current);
            }
        }
        if (escaped || inCharacterClass || depth != 0) {
            throw unsupportedRegexSyntax(pattern, pattern.length());
        }
        return translated.toString();
    }

    /**
     * The index of the {@code }} closing a well-formed {@code {n}}, {@code {n,}} or {@code {n,m}}
     * interval opening at {@code open}, or {@code -1} when the brace begins no such interval.
     */
    private static int intervalEnd(String pattern, int open) {
        int cursor = open + 1;
        int digits = 0;
        while (cursor < pattern.length() && Character.isDigit(pattern.charAt(cursor))) {
            cursor++;
            digits++;
        }
        if (digits == 0 || cursor >= pattern.length()) {
            return -1;
        }
        if (pattern.charAt(cursor) == '}') {
            return cursor;
        }
        if (pattern.charAt(cursor) != ',') {
            return -1;
        }
        cursor++;
        while (cursor < pattern.length() && Character.isDigit(pattern.charAt(cursor))) {
            cursor++;
        }
        return cursor < pattern.length() && pattern.charAt(cursor) == '}' ? cursor : -1;
    }

    private static boolean isEscaped(String value, int index) {
        int backslashes = 0;
        for (int cursor = index - 1; cursor >= 0 && value.charAt(cursor) == '\\'; cursor--) {
            backslashes++;
        }
        return backslashes % 2 != 0;
    }

    private static UnsupportedPlanShapeException unsupportedRegexSyntax(String pattern, int index) {
        return unsupported(
                "matches regex uses syntax outside the supported RE2/Lucene subset at index "
                        + index + ": " + pattern);
    }
}
