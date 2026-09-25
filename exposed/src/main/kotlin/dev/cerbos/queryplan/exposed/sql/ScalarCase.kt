package dev.cerbos.queryplan.exposed.sql

import org.jetbrains.exposed.v1.core.ComplexExpression
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.Function
import org.jetbrains.exposed.v1.core.QueryBuilder
import org.jetbrains.exposed.v1.core.TextColumnType
import org.jetbrains.exposed.v1.core.append

/**
 * A text-valued `CASE WHEN … THEN … ELSE … END`.
 *
 * Written out rather than built with Exposed's `case()` builder, whose generics reshaped between
 * the support floor and the current release and which has no arm that yields a NULL of a type the
 * builder cannot infer. Both uses here need exactly that: the NULL-yielding guard around a
 * column-valued LIKE pattern, and the boolean-to-text lowering, where a NULL column has to stay
 * NULL rather than fall through to the `ELSE`.
 *
 * [ComplexExpression] makes Exposed parenthesise it inside AND / OR.
 */
internal class ScalarCase(
    private val branches: List<Pair<Expression<Boolean>, Expression<*>>>,
    private val otherwise: Expression<*>,
) : Function<String>(TextColumnType()), ComplexExpression {
    init {
        require(branches.isNotEmpty()) { "a CASE expression needs at least one WHEN branch" }
    }

    override fun toQueryBuilder(queryBuilder: QueryBuilder): Unit = queryBuilder {
        append("CASE")
        branches.forEach { (condition, result) -> append(" WHEN ", condition, " THEN ", result) }
        append(" ELSE ", otherwise, " END")
    }
}
