package dev.cerbos.queryplan.springdata;

/**
 * The CEL string-match methods — {@code contains}, {@code startsWith}, {@code endsWith} — and
 * where each puts the {@code LIKE} wildcards around its needle.
 *
 * <p>The one place those three operators are named: the constant-needle leaf
 * ({@link LeafTranslator#defaultLeaf}), the column-needle form and the constant-receiver form
 * ({@link ComparisonTranslator}) all read the wildcard layout from here. The methods are
 * RECEIVER-SENSITIVE, which is why {@link NormalizedBinary} never reorders their operands.
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

    /** The string-match method a Cerbos operator names, or {@code null} for any other operator. */
    static StringMatch of(String operator) {
        for (StringMatch match : values()) {
            if (match.operator.equals(operator)) {
                return match;
            }
        }
        return null;
    }

    /** {@code needle} (already LIKE-escaped) wrapped in this method's wildcards. */
    String pattern(String escapedNeedle) {
        return (leadingWildcard ? "%" : "") + escapedNeedle + (trailingWildcard ? "%" : "");
    }
}
