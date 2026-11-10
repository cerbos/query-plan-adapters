package dev.cerbos.queryplan.exposed.sql

import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.Function
import org.jetbrains.exposed.v1.core.QueryBuilder
import org.jetbrains.exposed.v1.core.TextColumnType
import org.jetbrains.exposed.v1.core.append
import org.jetbrains.exposed.v1.core.vendors.MysqlDialect
import org.jetbrains.exposed.v1.core.vendors.currentDialect

/**
 * String concatenation that propagates NULL, as CEL's `+` does. Exposed's `Concat` and PostgreSQL
 * `CONCAT()` skip NULLs; MySQL needs `CONCAT()` because `||` is OR there. Self-parenthesising.
 */
internal class ConcatExpression(private val parts: List<Expression<*>>) : Function<String>(TextColumnType()) {
    init {
        require(parts.size >= 2) { "a concatenation needs at least two operands" }
    }

    override fun toQueryBuilder(queryBuilder: QueryBuilder): Unit = queryBuilder {
        // MariaDBDialect extends MysqlDialect, so this arm covers both.
        if (currentDialect is MysqlDialect) {
            append("CONCAT(")
            parts.forEachIndexed { index, part ->
                if (index > 0) append(", ")
                append(part)
            }
            append(")")
        } else {
            append("(")
            parts.forEachIndexed { index, part ->
                if (index > 0) append(" || ")
                append(part)
            }
            append(")")
        }
    }
}
