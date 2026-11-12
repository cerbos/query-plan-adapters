package dev.cerbos.queryplan.exposed.sql

import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.Function
import org.jetbrains.exposed.v1.core.LongColumnType
import org.jetbrains.exposed.v1.core.QueryBuilder
import org.jetbrains.exposed.v1.core.TextColumnType
import org.jetbrains.exposed.v1.core.append
import org.jetbrains.exposed.v1.core.stringParam
import org.jetbrains.exposed.v1.core.vendors.MysqlDialect
import org.jetbrains.exposed.v1.core.vendors.PostgreSQLDialect
import org.jetbrains.exposed.v1.core.vendors.SQLiteDialect
import org.jetbrains.exposed.v1.core.vendors.currentDialect

/**
 * CEL's `int()` over a text column: the integer when the whole string is an optional sign and 1 to
 * 18 ASCII digits, which always fits in 64 bits, and NULL otherwise. CEL raises on a malformed
 * string, so NULL keeps the comparison UNKNOWN; a valid 19-digit string is NULL too, an under-grant.
 */
internal class StringIntCast(private val expr: Expression<*>) : Function<Long>(LongColumnType()) {
    override fun toQueryBuilder(queryBuilder: QueryBuilder): Unit = queryBuilder {
        append("CASE WHEN ")
        when (currentDialect) {
            is PostgreSQLDialect -> append("(", expr, " ~ ", stringParam("\\A[+-]?[0-9]{1,18}\\Z"), ")")
            is MysqlDialect -> append("REGEXP_LIKE(", expr, ", ", stringParam("\\A[+-]?[0-9]{1,18}\\z"), ", 'c')")
            is SQLiteDialect -> {
                val body = Body(expr)
                // GLOB and LENGTH stop at a NUL; the byte length of a digit string is its length.
                append(
                    "(LENGTH(", body, ") BETWEEN 1 AND 18 AND NOT (", body, " GLOB '*[^0-9]*') AND ",
                    "LENGTH(CAST(", body, " AS BLOB)) = LENGTH(", body, "))",
                )
            }
            else -> append("REGEXP_LIKE(", expr, ", ", stringParam("\\A[+-]?[0-9]{1,18}\\z"), ")")
        }
        append(" THEN CAST(", expr, if (currentDialect is MysqlDialect) " AS SIGNED" else " AS BIGINT", ") END")
    }

    /** The string without one leading sign. */
    private class Body(private val expr: Expression<*>) : Function<String>(TextColumnType()) {
        override fun toQueryBuilder(queryBuilder: QueryBuilder): Unit = queryBuilder {
            append("(CASE WHEN SUBSTR(", expr, ", 1, 1) IN ('+', '-') THEN SUBSTR(", expr, ", 2) ELSE ", expr, " END)")
        }
    }
}
