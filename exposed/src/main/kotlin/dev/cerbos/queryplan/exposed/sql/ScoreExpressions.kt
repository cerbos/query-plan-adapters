package dev.cerbos.queryplan.exposed.sql

import org.jetbrains.exposed.v1.core.ComplexExpression
import org.jetbrains.exposed.v1.core.CustomFunction
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.ExpressionWithColumnType
import org.jetbrains.exposed.v1.core.IColumnType
import org.jetbrains.exposed.v1.core.IntegerColumnType
import org.jetbrains.exposed.v1.core.QueryBuilder
import org.jetbrains.exposed.v1.core.intLiteral

/** `NULLIF(fold, 1)`: only undetermined elements scored (1, the lowest score), which CEL calls an error. */
internal fun nullIfUndetermined(fold: Expression<Int>): ExpressionWithColumnType<Int> =
    CustomFunction("NULLIF", IntegerColumnType(), fold, intLiteral(1))

/**
 * Scores one element: `CASE WHEN (body) THEN <whenTrue> WHEN NOT (body) THEN <whenFalse> ELSE
 * <whenUndetermined> END`. An UNKNOWN body takes the `ELSE` rather than collapsing to FALSE.
 */
internal class ScoreCase(
    private val body: Expression<Boolean>,
    private val whenTrue: Int,
    private val whenFalse: Int,
    private val whenUndetermined: Int,
) : ExpressionWithColumnType<Int>(), ComplexExpression {
    override val columnType: IColumnType<Int> = IntegerColumnType()

    override fun toQueryBuilder(queryBuilder: QueryBuilder) {
        queryBuilder {
            append("CASE WHEN (")
            append(body)
            append(") THEN ")
            append(whenTrue.toString())
            append(" WHEN NOT (")
            append(body)
            append(") THEN ")
            append(whenFalse.toString())
            append(" ELSE ")
            append(whenUndetermined.toString())
            append(" END")
        }
    }
}

/** [value], or SQL NULL when [guard] fails: `CASE WHEN guard THEN value END`, with no `ELSE` on purpose. */
internal class ScoreGuard<T>(
    private val guard: Expression<Boolean>,
    private val value: Expression<T>,
    override val columnType: IColumnType<T & Any>,
) : ExpressionWithColumnType<T>(), ComplexExpression {
    override fun toQueryBuilder(queryBuilder: QueryBuilder) {
        queryBuilder {
            append("CASE WHEN ")
            append(guard)
            append(" THEN ")
            append(value)
            append(" END")
        }
    }
}
