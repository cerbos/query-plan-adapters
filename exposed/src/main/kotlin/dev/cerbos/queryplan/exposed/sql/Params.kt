package dev.cerbos.queryplan.exposed.sql

import dev.cerbos.queryplan.exposed.Refusals
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.QueryBuilder
import org.jetbrains.exposed.v1.core.booleanParam
import org.jetbrains.exposed.v1.core.doubleParam
import org.jetbrains.exposed.v1.core.longParam
import org.jetbrains.exposed.v1.core.stringParam

/**
 * Binds a plan constant BY THE VALUE'S TYPE, never by the column's.
 *
 * Exposed's own `column eq value` binds through the column type, which coerces: `1.5` against an
 * integer column becomes `1`, and `aNumber >= 1.5` then returns rows the PDP denies. A constant is
 * what the plan says it is, so its SQL type comes from the constant.
 *
 * NULL is never bound: see [UnknownOp] and [NullLiteral].
 */
internal object Params {
    fun of(value: Any): Expression<*> = when (value) {
        is String -> stringParam(value)
        is Long -> longParam(value)
        is Int -> longParam(value.toLong())
        is Double -> doubleParam(value)
        is Boolean -> booleanParam(value)
        else -> throw Refusals.internal("No SQL parameter type for a ${value::class.simpleName} constant")
    }
}

/**
 * The SQL `NULL` keyword.
 *
 * Rendered, never bound: PostgreSQL cannot infer a type for a bare `$1` in a position that gives
 * it nothing to infer from, and a keyword needs no dialect knowledge. Its one use is a `THEN NULL`
 * arm, where the value has to be absent rather than merely unknown.
 */
internal object NullLiteral : Expression<Any?>() {
    override fun toQueryBuilder(queryBuilder: QueryBuilder) {
        queryBuilder.append("NULL")
    }
}
