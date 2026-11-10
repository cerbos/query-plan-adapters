package dev.cerbos.queryplan.exposed.sql

import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.Function
import org.jetbrains.exposed.v1.core.IntegerColumnType
import org.jetbrains.exposed.v1.core.QueryBuilder
import org.jetbrains.exposed.v1.core.append
import org.jetbrains.exposed.v1.core.vendors.H2Dialect
import org.jetbrains.exposed.v1.core.vendors.SQLiteDialect
import org.jetbrains.exposed.v1.core.vendors.currentDialect

/**
 * The number of characters in a string, as CEL's `size()` counts. MySQL `LENGTH` counts bytes;
 * SQLite has no `CHAR_LENGTH` and its `LENGTH` stops at a NUL, so NULs are counted through the JSON
 * text; H2 counts UTF-16 units, so astral characters are replaced first. Read at render time.
 */
internal class CountChars(private val value: Expression<*>) : Function<Int>(IntegerColumnType()) {
    override fun toQueryBuilder(queryBuilder: QueryBuilder) {
        queryBuilder {
            when (currentDialect) {
                is SQLiteDialect -> append(
                    "LENGTH(JSON_EXTRACT(REPLACE(REPLACE(REPLACE(JSON_QUOTE(", value,
                    "), '\\\\', CHAR(1)), '\\u0000', ' '), CHAR(1), '\\\\'), '\$'))",
                )
                is H2Dialect -> append("CHAR_LENGTH(REGEXP_REPLACE(", value, ", '[\\x{10000}-\\x{10FFFF}]', '_'))")
                else -> append("CHAR_LENGTH(", value, ")")
            }
        }
    }
}
