package dev.cerbos.queryplan.exposed.sql

import org.jetbrains.exposed.v1.core.ComplexExpression
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.QueryBuilder

/**
 * The constant SQL UNKNOWN.
 *
 * Rendered as a comparison against a NULL LITERAL, never a bound parameter: PostgreSQL cannot type
 * a bare `$1` and rejects the statement, and a literal needs no dialect knowledge.
 * [ComplexExpression] makes Exposed parenthesise it inside AND / OR.
 */
internal object UnknownOp : Op<Boolean>(), ComplexExpression {
    override fun toQueryBuilder(queryBuilder: QueryBuilder) {
        queryBuilder.append("1 = NULL")
    }
}
