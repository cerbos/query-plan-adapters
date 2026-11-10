package dev.cerbos.queryplan.exposed.sql

import dev.cerbos.queryplan.exposed.Refusals
import dev.cerbos.queryplan.exposed.ScalarRefusals
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.QueryBuilder
import org.jetbrains.exposed.v1.core.booleanParam
import org.jetbrains.exposed.v1.core.doubleParam
import org.jetbrains.exposed.v1.core.longParam

/**
 * Binds a plan constant by the value's type, never the column's: binding through an integer column
 * turns `1.5` into `1`. NULL is never bound ([UnknownOp], [NullLiteral]).
 */
internal object Params {
    fun of(value: Any): Expression<*> = when (value) {
        is String -> ExactString.param(value)
        is Long -> longParam(value)
        is Int -> longParam(value.toLong())
        // A NaN or infinity is refused rather than bound.
        is Double ->
            if (value.isFinite()) doubleParam(value) else throw ScalarRefusals.nonFiniteConstant()
        is Boolean -> booleanParam(value)
        else -> throw Refusals.internal("No SQL parameter type for a ${value::class.simpleName} constant")
    }
}

/** The SQL `NULL` keyword, rendered rather than bound because PostgreSQL cannot type a bare `$1`. */
internal object NullLiteral : Expression<Any?>() {
    override fun toQueryBuilder(queryBuilder: QueryBuilder) {
        queryBuilder.append("NULL")
    }
}
