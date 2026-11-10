package dev.cerbos.queryplan.exposed.sql

import org.jetbrains.exposed.v1.core.DoubleColumnType
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.Function
import org.jetbrains.exposed.v1.core.QueryBuilder
import org.jetbrains.exposed.v1.core.append
import org.jetbrains.exposed.v1.core.vendors.MysqlDialect
import org.jetbrains.exposed.v1.core.vendors.SQLiteDialect
import org.jetbrains.exposed.v1.core.vendors.currentDialect

/**
 * `CAST(expr AS <binary floating point type>)`, because CEL arithmetic is IEEE double and H2 and
 * PostgreSQL type `0.1` as NUMERIC. `DOUBLE PRECISION`, MySQL `DOUBLE`, SQLite `REAL`; PostgreSQL
 * `REAL` is single precision. The dialect is read at render time.
 */
internal class IeeeDoubleCast(private val expr: Expression<*>) : Function<Double>(DoubleColumnType()) {
    override fun toQueryBuilder(queryBuilder: QueryBuilder): Unit = queryBuilder {
        append("CAST(", expr, " AS ", targetType(), ")")
    }

    private fun targetType(): String = when (currentDialect) {
        // MariaDBDialect extends MysqlDialect, so this arm covers both.
        is MysqlDialect -> "DOUBLE"
        is SQLiteDialect -> "REAL"
        else -> "DOUBLE PRECISION"
    }
}
