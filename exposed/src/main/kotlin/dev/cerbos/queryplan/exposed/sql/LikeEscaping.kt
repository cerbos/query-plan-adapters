package dev.cerbos.queryplan.exposed.sql

import org.jetbrains.exposed.v1.core.CustomFunction
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.IsNullOp
import org.jetbrains.exposed.v1.core.TextColumnType
import org.jetbrains.exposed.v1.core.stringParam

/** Builds the pattern side of a `LIKE`, with `\` always declared as the (bound) escape character. */
internal object LikeEscaping {
    const val ESCAPE_CHAR: Char = '\\'

    /**
     * The pattern for a column needle, escaped at query time with nested `REPLACE`. A NULL needle yields a
     * NULL pattern, keeping the match UNKNOWN under both polarities (#387).
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
