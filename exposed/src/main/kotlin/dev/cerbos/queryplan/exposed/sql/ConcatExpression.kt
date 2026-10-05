package dev.cerbos.queryplan.exposed.sql

import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.Function
import org.jetbrains.exposed.v1.core.QueryBuilder
import org.jetbrains.exposed.v1.core.TextColumnType
import org.jetbrains.exposed.v1.core.append
import org.jetbrains.exposed.v1.core.vendors.MysqlDialect
import org.jetbrains.exposed.v1.core.vendors.currentDialect

/**
 * String concatenation that PROPAGATES NULL, which is what CEL's `+` over strings does: an operand
 * that is a missing attribute is an evaluation error, and the row must stay excluded under both
 * polarities.
 *
 * Not Exposed's own [org.jetbrains.exposed.v1.core.Concat], and not PostgreSQL's `CONCAT()`: both
 * SKIP a NULL argument, so `aString + aOptionalString == "oneset"` would compare `"one"` and match
 * a row whose second operand is absent — exactly the rows `check()` denies. MySQL's `CONCAT()`
 * does propagate NULL and is the right spelling there and only there, because `||` is logical OR
 * on MySQL outside `PIPES_AS_CONCAT` and would collapse the whole expression to a boolean.
 *
 * Self-parenthesising: the `||` form is an operator chain, and it lands inside comparisons and
 * LIKE patterns where precedence would otherwise bite.
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
