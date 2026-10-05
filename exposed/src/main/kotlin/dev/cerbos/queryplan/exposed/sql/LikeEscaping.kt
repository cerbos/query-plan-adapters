package dev.cerbos.queryplan.exposed.sql

import org.jetbrains.exposed.v1.core.CustomFunction
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.IsNullOp
import org.jetbrains.exposed.v1.core.TextColumnType
import org.jetbrains.exposed.v1.core.stringParam

/**
 * Builds the pattern side of a `LIKE`, for both kinds of needle.
 *
 * The escape character is `\` and is always declared: Exposed's [org.jetbrains.exposed.v1.core.LikeEscapeOp]
 * BINDS it as a parameter rather than inlining it, so no dialect's string-literal backslash
 * handling can change what it means.
 */
internal object LikeEscaping {
    const val ESCAPE_CHAR: Char = '\\'

    /**
     * `wildcards(escape(needleColumn))` — the pattern for a needle that is data rather than a
     * constant, so its metacharacters have to be escaped at query time with nested `REPLACE`
     * (portable across every dialect this adapter targets): `\` first, then `%`, `_` and `[`,
     * mirroring `PlanValues.escapeLike`. Each `REPLACE` argument is bound, so the backslashes are
     * never subject to a dialect's literal parsing.
     *
     * A NULL needle must leave the whole match UNKNOWN, not FALSE. CEL raises a missing-attribute
     * error there, which denies under BOTH polarities, and only UNKNOWN reproduces that: the
     * reference adapter once spelled this `needle IS NOT NULL AND haystack LIKE pattern`, which is
     * a definite FALSE for a NULL needle, and `NOT FALSE` is TRUE — so every negated column-needle
     * match returned exactly the rows the PDP denies
     * (https://github.com/cerbos/query-plan-adapters/issues/387). Nesting the guard in a `CASE`
     * that yields a NULL PATTERN keeps the LIKE itself UNKNOWN, and also defends against a dialect
     * whose concatenation reads NULL as `''` and would otherwise build a match-anything `'%%'`.
     */
    fun columnPattern(needle: Expression<*>, leadingWildcard: Boolean, trailingWildcard: Boolean): Expression<*> {
        var escaped: Expression<*> = needle
        escaped = replace(escaped, "\\", "\\\\")
        escaped = replace(escaped, "%", "\\%")
        escaped = replace(escaped, "_", "\\_")
        escaped = replace(escaped, "[", "\\[")

        val parts = buildList {
            if (leadingWildcard) add(stringParam("%"))
            add(escaped)
            if (trailingWildcard) add(stringParam("%"))
        }
        val pattern: Expression<*> = if (parts.size == 1) parts[0] else ConcatExpression(parts)
        return ScalarCase(listOf(IsNullOp(needle) to NullLiteral), pattern)
    }

    private fun replace(target: Expression<*>, from: String, to: String): Expression<String> =
        CustomFunction("REPLACE", TextColumnType(), target, stringParam(from), stringParam(to))
}
