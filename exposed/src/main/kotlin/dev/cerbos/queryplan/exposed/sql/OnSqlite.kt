package dev.cerbos.queryplan.exposed.sql

import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.QueryBuilder
import org.jetbrains.exposed.v1.core.vendors.SQLiteDialect
import org.jetbrains.exposed.v1.core.vendors.currentDialect

/** [sqlite] when the query renders for SQLite, [other] otherwise. */
internal class OnSqlite(private val sqlite: Op<Boolean>, private val other: Op<Boolean>) : Op<Boolean>() {
    override fun toQueryBuilder(queryBuilder: QueryBuilder) {
        (if (currentDialect is SQLiteDialect) sqlite else other).toQueryBuilder(queryBuilder)
    }
}
