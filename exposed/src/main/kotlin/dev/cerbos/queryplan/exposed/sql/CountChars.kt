package dev.cerbos.queryplan.exposed.sql

import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.Function
import org.jetbrains.exposed.v1.core.IntegerColumnType
import org.jetbrains.exposed.v1.core.QueryBuilder
import org.jetbrains.exposed.v1.core.vendors.SQLiteDialect
import org.jetbrains.exposed.v1.core.vendors.currentDialect

/**
 * The number of CHARACTERS in a string, which is what CEL's `size()` counts.
 *
 * `LENGTH` is not portable for this: on MySQL it counts BYTES, so every multibyte string would be
 * measured against the wrong threshold and `size(s) > 4` would return rows the PDP denies.
 * `CHAR_LENGTH` is the standard spelling and is what PostgreSQL, MySQL and H2 all accept; SQLite
 * has no `CHAR_LENGTH` at all, and its `LENGTH` already counts characters for a text value.
 *
 * The dialect is only known at RENDER time — an `Op` is built outside any transaction and rendered
 * inside the caller's — so the branch lives here rather than in the translation.
 */
internal class CountChars(private val value: Expression<*>) : Function<Int>(IntegerColumnType()) {
    override fun toQueryBuilder(queryBuilder: QueryBuilder) {
        queryBuilder {
            append(if (currentDialect is SQLiteDialect) "LENGTH(" else "CHAR_LENGTH(")
            append(value)
            append(")")
        }
    }
}
