package dev.cerbos.queryplan.exposed.sql

import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.Function
import org.jetbrains.exposed.v1.core.QueryBuilder
import org.jetbrains.exposed.v1.core.TextColumnType
import org.jetbrains.exposed.v1.core.append
import org.jetbrains.exposed.v1.core.vendors.MysqlDialect
import org.jetbrains.exposed.v1.core.vendors.currentDialect

/**
 * `CAST(expr AS <the dialect's character type>)`, for CEL's `string()` over a numeric or text
 * column.
 *
 * MySQL's `CAST` accepts `CHAR` and rejects `VARCHAR` without a length; every other dialect this
 * adapter targets accepts `VARCHAR` unqualified. The dialect is read inside [toQueryBuilder]
 * because translation happens outside any transaction.
 */
internal class TextCastExpression(private val expr: Expression<*>) : Function<String>(TextColumnType()) {
    override fun toQueryBuilder(queryBuilder: QueryBuilder): Unit = queryBuilder {
        append("CAST(", expr, " AS ", targetType(), ")")
    }

    private fun targetType(): String = when (currentDialect) {
        // MariaDBDialect extends MysqlDialect, so this arm covers both.
        is MysqlDialect -> "CHAR"
        else -> "VARCHAR"
    }
}
