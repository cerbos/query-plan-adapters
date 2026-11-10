package dev.cerbos.queryplan.exposed.sql

import org.jetbrains.exposed.v1.core.ComplexExpression
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.Function
import org.jetbrains.exposed.v1.core.QueryBuilder
import org.jetbrains.exposed.v1.core.TextColumnType
import org.jetbrains.exposed.v1.core.append

/**
 * A text-valued `CASE WHEN … THEN … ELSE … END`, written out because Exposed's `case()` cannot yield
 * an untyped NULL and its generics differ across the supported versions.
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
