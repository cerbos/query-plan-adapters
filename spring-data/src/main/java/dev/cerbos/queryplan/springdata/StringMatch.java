/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

/**
 * The CEL string-match methods and where each puts the {@code LIKE} wildcards around its needle.
 * The receiver matters, so {@link NormalizedBinary} never reorders their operands.
 */
enum StringMatch {
    CONTAINS("contains", true, true),
    STARTS_WITH("startsWith", false, true),
    ENDS_WITH("endsWith", true, false);

    private final String operator;
    final boolean leadingWildcard;
    final boolean trailingWildcard;

    StringMatch(String operator, boolean leadingWildcard, boolean trailingWildcard) {
        this.operator = operator;
        this.leadingWildcard = leadingWildcard;
        this.trailingWildcard = trailingWildcard;
    }

    /** The method {@code operator} names, or {@code null} for any other operator. */
    static StringMatch of(String operator) {
        for (StringMatch match : values()) {
            if (match.operator.equals(operator)) {
                return match;
            }
        }
        return null;
    }

    /** Wraps an already LIKE-escaped needle in this method's wildcards. */
    String pattern(String escapedNeedle) {
        return (leadingWildcard ? "%" : "") + escapedNeedle + (trailingWildcard ? "%" : "");
    }
}
