package dev.cerbos.queryplan.exposed.sql

import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.ExpressionWithColumnType
import org.jetbrains.exposed.v1.core.IColumnType
import org.jetbrains.exposed.v1.core.QueryBuilder

/** [value] carrying [columnType], so [ExactString] collates a to-one column read through a subquery. */
internal class TypedScalar<T>(
    private val value: Expression<T>,
    override val columnType: IColumnType<T & Any>,
) : ExpressionWithColumnType<T>() {
    override fun toQueryBuilder(queryBuilder: QueryBuilder): Unit = queryBuilder { append(value) }
}
