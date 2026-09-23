/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import static dev.cerbos.queryplan.elasticsearch.Refusals.unsupported;

import java.util.Map;

/**
 * Translates {@code matches()}: a {@code prefix} query for a plain {@code ^literal}, otherwise a
 * {@code regexp} query.
 *
 * <p>CEL uses RE2 partial matching; Lucene matches the whole field, and its {@code .} matches
 * newlines. So only fully anchored patterns in the syntax both engines share are accepted, and
 * Lucene's optional operators are disabled with {@code flags=NONE}.
 */
final class RegexTranslator {

    private RegexTranslator() {}

    static Map<String, Object> matchesQuery(String field, Object value) {
        String pattern = value.toString();
        boolean anchoredStart = pattern.startsWith("^");
        String body = anchoredStart ? pattern.substring(1) : pattern;
        boolean anchoredEnd = body.endsWith("$") && !isEscaped(body, body.length() - 1);
        if (anchoredStart && !anchoredEnd && !body.isEmpty() && isPlainRegexLiteral(body)) {
            return Queries.prefix(field, body);
        }
        if (anchoredEnd) {
            body = body.substring(0, body.length() - 1);
        }
        // Syntax is checked before anchoring, so an unanchored bad pattern reports its syntax.
        String luceneBody = validateAndEscapeLuceneRegexBody(body);
        if (!anchoredStart || !anchoredEnd) {
            throw unsupported(
                    "matches regex patterns must be fully anchored unless they are a simple "
                            + "literal prefix");
        }
        if (luceneBody.isEmpty()) {
            throw unsupported(
                    "matches regex for only the empty string is not supported by Elasticsearch");
        }
        return Map.of("regexp", Map.of(field, Map.of("value", luceneBody, "flags", "NONE")));
    }

    private static boolean isPlainRegexLiteral(String pattern) {
        for (int index = 0; index < pattern.length(); index++) {
            if ("\\.[](){}?*+|^$".indexOf(pattern.charAt(index)) >= 0) {
                return false;
            }
        }
        return true;
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
     * The index of the brace closing a {@code {n}}, {@code {n,}} or {@code {n,m}} interval that
     * opens at {@code open}, or {@code -1} if there is none.
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
