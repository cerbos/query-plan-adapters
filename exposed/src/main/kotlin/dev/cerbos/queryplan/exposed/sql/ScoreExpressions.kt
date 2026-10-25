package dev.cerbos.queryplan.exposed.sql

import org.jetbrains.exposed.v1.core.ComplexExpression
import org.jetbrains.exposed.v1.core.CustomFunction
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.ExpressionWithColumnType
import org.jetbrains.exposed.v1.core.IColumnType
import org.jetbrains.exposed.v1.core.IntegerColumnType
import org.jetbrains.exposed.v1.core.QueryBuilder
import org.jetbrains.exposed.v1.core.intLiteral

/**
 * `NULLIF(fold, 1)`: the state where only undetermined elements scored, mapped to SQL NULL.
 *
 * 1 is the score [ScoreCase] gives an undetermined element, and it is deliberately the LOWEST of
 * the three: it can only dominate a `MAX` when nothing else scored, which is exactly the state CEL
 * calls an error. Exposed has no `NULLIF` of its own, and the spelling is standard on every store
 * this adapter is executed against.
 */
internal fun nullIfUndetermined(fold: Expression<Int>): ExpressionWithColumnType<Int> =
    CustomFunction("NULLIF", IntegerColumnType(), fold, intLiteral(1))

/**
 * Scores ONE element of a collection into the three states CEL distinguishes, in a single pass.
 *
 * ```sql
 * CASE WHEN (body) THEN <whenTrue> WHEN NOT (body) THEN <whenFalse> ELSE <whenUndetermined> END
 * ```
 *
 * An UNKNOWN body takes neither `WHEN` — SQL treats an UNKNOWN condition as not taken — so an
 * element whose lambda body touched a NULL lands in the `ELSE` rather than being collapsed onto
 * the FALSE case. That collapse is the authorization bug the whole shape exists to stop: under a
 * plain `EXISTS` an undetermined element silently fails to match, the macro reads FALSE, and an
 * enclosing `NOT` turns it into a row the PDP denies.
 *
 * The scores are the adapter's own constants, not plan values, so they are rendered inline: a
 * bound parameter here would buy nothing and would give the driver a type to guess at.
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

/**
 * [value], unless [guard] fails, in which case SQL NULL: `CASE WHEN guard THEN value END`.
 *
 * The missing `ELSE` is the whole point. A chained collection whose intermediate to-one parent is
 * absent must stay excluded under BOTH polarities, and only UNKNOWN does that — `NOT NULL` is
 * still NULL, while `NOT FALSE` is TRUE and readmits the row. Guarding the VALUE rather than
 * ANDing a guard beside the comparison is what makes every comparison built on it inherit the
 * requirement, including the negated spellings.
 */
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
