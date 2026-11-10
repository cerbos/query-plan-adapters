/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import static dev.cerbos.queryplan.elasticsearch.Refusals.unsupported;

import java.util.ArrayDeque;
import java.util.Deque;
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

    /** RE2's bound on the copies a counted repetition, nested ones included, may make. */
    private static final int MAX_REPEAT = 1000;

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
        // Go's repeatIsValid divides RE2's budget by each enclosing count, so a pattern is valid
        // when the counts along every nesting path multiply to at most MAX_REPEAT. These track
        // the most copies any atom of the current group makes, and the copies of the last atom.
        Deque<Integer> enclosingCopies = new ArrayDeque<>();
        int groupCopies = 1;
        int atomCopies = 1;
        // The current group's copies before its last atom when that atom is a group, else -1: a
        // {0} after the group copies nothing, and Go's repeatIsValid stops there.
        int copiesBeforeLastGroup = -1;
        // What the last token was, so a repetition with nothing to repeat, a repetition of a
        // repetition and an empty alternative are refused: RE2 rejects the first two and Lucene
        // reads all three differently from RE2.
        Token last = Token.NONE;
        for (int index = 0; index < pattern.length(); index++) {
            char current = pattern.charAt(index);
            if (escaped) {
                if (Character.isLetterOrDigit(current)) {
                    throw unsupportedRegexSyntax(pattern, index - 1);
                }
                translated.append('\\').append(current);
                escaped = false;
                last = Token.ATOM;
                atomCopies = 1;
                copiesBeforeLastGroup = -1;
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
                last = Token.ATOM;
                atomCopies = 1;
                copiesBeforeLastGroup = -1;
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
                enclosingCopies.push(groupCopies);
                groupCopies = 1;
                last = Token.NONE;
            } else if (current == ')') {
                if (depth == 0 || last == Token.NONE) {
                    throw unsupportedRegexSyntax(pattern, index);
                }
                depth--;
                last = Token.ATOM;
                atomCopies = groupCopies;
                copiesBeforeLastGroup = enclosingCopies.pop();
                groupCopies = Math.max(copiesBeforeLastGroup, groupCopies);
                translated.append(current);
                continue;
            } else if (current == '|' && depth == 0) {
                throw unsupported("matches regex has a top-level alternation at index " + index
                        + ": RE2 reads ^a|b$ as two separately anchored alternatives, while "
                        + "Lucene matches the whole field against a|b, so the alternation must "
                        + "be parenthesised as ^(a|b)$: " + pattern);
            } else if (current == '|') {
                if (last == Token.NONE) {
                    throw emptyAlternative(pattern, index);
                }
                last = Token.NONE;
            } else if (current == '{') {
                int close = intervalEnd(pattern, index);
                if (close < 0) {
                    throw unsupported("matches regex has a brace at index " + index
                            + " that does not begin a {n}, {n,} or {n,m} repetition, which "
                            + "Lucene rejects at query time: " + pattern);
                }
                last = repetition(last, pattern, index);
                String interval = pattern.substring(index + 1, close);
                // RE2 reads a count as Go's regexp/syntax parseInt does, without a leading zero.
                if (!interval.matches("(0|[1-9][0-9]*)(,(0|[1-9][0-9]*)?)?")) {
                    throw unsupported("matches regex has a brace at index " + index
                            + " whose count has a leading zero, which RE2 reads as literal text"
                            + " and Lucene as a repetition: " + pattern);
                }
                if (interval.equals("0") || interval.equals("0,0")) {
                    if (copiesBeforeLastGroup >= 0) {
                        groupCopies = copiesBeforeLastGroup;
                    }
                    atomCopies = 0;
                    translated.append(pattern, index, close + 1);
                    index = close;
                    continue;
                }
                atomCopies = atomCopies * repeatCount(interval);
                if (atomCopies > MAX_REPEAT) {
                    throw unsupported("matches regex has a repetition at index " + index
                            + " whose nested copies exceed RE2's bound of " + MAX_REPEAT
                            + ", so RE2 rejects the pattern and CEL's matches() errors; Lucene"
                            + " would accept it: " + pattern);
                }
                groupCopies = Math.max(groupCopies, atomCopies);
                translated.append(pattern, index, close + 1);
                index = close;
                continue;
            }
            if (current == '*' || current == '+') {
                last = repetition(last, pattern, index);
            } else if (current == '?') {
                last = last == Token.REPETITION ? Token.LAZY : repetition(last, pattern, index);
            } else if (current != '(' && current != '|') {
                last = Token.ATOM;
            }
            if (current == '"') {
                translated.append("\\\"");
            } else {
                translated.append(current);
            }
            if (current != '*' && current != '+' && current != '?') {
                atomCopies = 1;
                copiesBeforeLastGroup = -1;
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

    /**
     * The count Go's repeatIsValid charges an interval's body ({@code n}, {@code n,} or
     * {@code n,m}): its maximum, or its minimum when unbounded. A count too long for an int is
     * past the bound anyway.
     */
    private static int repeatCount(String interval) {
        int comma = interval.indexOf(',');
        String count = comma < 0 || comma == interval.length() - 1
                ? interval.substring(0, comma < 0 ? interval.length() : comma)
                : interval.substring(comma + 1);
        return count.length() > 4 ? MAX_REPEAT + 1 : Integer.parseInt(count);
    }

    private static boolean isEscaped(String value, int index) {
        int backslashes = 0;
        for (int cursor = index - 1; cursor >= 0 && value.charAt(cursor) == '\\'; cursor--) {
            backslashes++;
        }
        return backslashes % 2 != 0;
    }

    private enum Token { NONE, ATOM, REPETITION, LAZY }

    private static Token repetition(Token last, String pattern, int index) {
        if (last == Token.NONE) {
            throw unsupported("matches regex has a repetition at index " + index + " with "
                    + "nothing to repeat: RE2 rejects it, or repeats the anchor before it, and "
                    + "Lucene reads it as neither: " + pattern);
        }
        if (last != Token.ATOM) {
            throw unsupported("matches regex has a repetition at index " + index + " with a "
                    + "repetition before it, which RE2 rejects, so CEL's matches() errors, while "
                    + "Lucene reads it as a pattern: " + pattern);
        }
        return Token.REPETITION;
    }

    private static UnsupportedPlanShapeException emptyAlternative(String pattern, int index) {
        return unsupported("matches regex has an empty alternative at index " + index
                + ": RE2 matches the empty string there, while Lucene reads the | differently: "
                + pattern);
    }

    private static UnsupportedPlanShapeException unsupportedRegexSyntax(String pattern, int index) {
        return unsupported(
                "matches regex uses syntax outside the supported RE2/Lucene subset at index "
                        + index + ": " + pattern);
    }
}
