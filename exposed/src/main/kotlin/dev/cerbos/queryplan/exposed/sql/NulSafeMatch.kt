package dev.cerbos.queryplan.exposed.sql

import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.QueryBuilder
import org.jetbrains.exposed.v1.core.append
import org.jetbrains.exposed.v1.core.vendors.SQLiteDialect
import org.jetbrains.exposed.v1.core.vendors.currentDialect

/**
 * [like], except on SQLite, whose LIKE reads a string only up to its first NUL: there `instr()`
 * compares every byte and `hex()` keeps a suffix aligned to whole bytes, each case-sensitive as
 * CEL is. The dialect is read at render time, as [ExactString] reads it.
 */
internal class NulSafeMatch(
    private val like: Op<Boolean>,
    private val haystack: Expression<*>,
    private val needle: Expression<*>,
    private val leadingWildcard: Boolean,
    private val trailingWildcard: Boolean,
) : Op<Boolean>() {
    override fun toQueryBuilder(queryBuilder: QueryBuilder): Unit = queryBuilder {
        when {
            currentDialect !is SQLiteDialect -> append(like)
            leadingWildcard && trailingWildcard -> append("(instr(", haystack, ", ", needle, ") > 0)")
            trailingWildcard -> append("(instr(", haystack, ", ", needle, ") = 1)")
            // hex() reads NULL as '', so NULL is propagated by hand.
            leadingWildcard -> append(
                "(CASE WHEN ", haystack, " IS NOT NULL AND ", needle, " IS NOT NULL THEN hex(",
                haystack, ") LIKE '%' || hex(", needle, ") END)",
            )
            else -> append("(", haystack, " = ", needle, ")")
        }
    }
}
