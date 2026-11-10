package dev.cerbos.queryplan.exposed.sql

import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.ExpressionWithColumnType
import org.jetbrains.exposed.v1.core.IColumnType
import org.jetbrains.exposed.v1.core.QueryBuilder
import org.jetbrains.exposed.v1.core.StringColumnType
import org.jetbrains.exposed.v1.core.TextColumnType
import org.jetbrains.exposed.v1.core.append
import org.jetbrains.exposed.v1.core.stringParam
import org.jetbrains.exposed.v1.core.vendors.MariaDBDialect
import org.jetbrains.exposed.v1.core.vendors.MysqlDialect
import org.jetbrains.exposed.v1.core.vendors.PostgreSQLDialect
import org.jetbrains.exposed.v1.core.vendors.currentDialect

/**
 * A string under the collation CEL needs: `utf8mb4_0900_bin` on MySQL (#474) and, for an [ordering],
 * code point order on PostgreSQL (#489). Unchanged elsewhere and on MariaDB. Read at render time.
 */
internal class ExactString<T>(
    private val expression: Expression<T>,
    private val ordering: Boolean,
    override val columnType: IColumnType<T & Any>,
) : ExpressionWithColumnType<T>() {
    override fun toQueryBuilder(queryBuilder: QueryBuilder): Unit = queryBuilder {
        val dialect = currentDialect
        when {
            dialect is MysqlDialect && dialect !is MariaDBDialect ->
                append("(CONVERT(", expression, " USING utf8mb4) COLLATE utf8mb4_0900_bin)")
            ordering && dialect is PostgreSQLDialect -> append("(", expression, " COLLATE \"C\")")
            else -> append(expression)
        }
    }

    companion object {
        /** A bound string constant, byte-exact on MySQL. */
        fun param(value: String): ExpressionWithColumnType<String> {
            return ExactString(stringParam(value), ordering = false, columnType = TextColumnType())
        }

        /** [expression] for [operator], when it is a string; any other expression as it is. */
        fun <T> of(expression: Expression<T>, operator: String): Expression<T> {
            val typed = expression as? ExpressionWithColumnType<T> ?: return expression
            if (typed is ExactString<*> || typed.columnType !is StringColumnType) return expression
            return ExactString(typed, operator in ORDERINGS, typed.columnType)
        }

        private val ORDERINGS = setOf("lt", "le", "gt", "ge")
    }
}
