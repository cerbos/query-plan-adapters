package dev.cerbos.queryplan.exposed.sql

import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.Function
import org.jetbrains.exposed.v1.core.LongColumnType
import org.jetbrains.exposed.v1.core.QueryBuilder
import org.jetbrains.exposed.v1.core.append
import org.jetbrains.exposed.v1.core.vendors.MysqlDialect
import org.jetbrains.exposed.v1.core.vendors.PostgreSQLDialect
import org.jetbrains.exposed.v1.core.vendors.SQLiteDialect
import org.jetbrains.exposed.v1.core.vendors.currentDialect

/**
 * The remainder of an integer by a non-zero constant, with the dividend's sign as CEL's `%` has it:
 * `%` on SQLite, PostgreSQL and MySQL, `MOD()` elsewhere.
 */
internal class IntRemainder(private val dividend: Expression<*>, private val divisor: Long) :
    Function<Long>(LongColumnType()) {
    init {
        require(divisor != 0L && divisor != -1L) { "a remainder by 0 or -1 is not lowered" }
    }

    override fun toQueryBuilder(queryBuilder: QueryBuilder): Unit = queryBuilder {
        when (currentDialect) {
            is SQLiteDialect, is PostgreSQLDialect, is MysqlDialect -> append("(", dividend, " % ", divisor.toString(), ")")
            else -> append("MOD(", dividend, ", ", divisor.toString(), ")")
        }
    }
}
