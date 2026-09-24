package dev.cerbos.queryplan.exposed.sql

import org.jetbrains.exposed.v1.core.DoubleColumnType
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.Function
import org.jetbrains.exposed.v1.core.QueryBuilder
import org.jetbrains.exposed.v1.core.append
import org.jetbrains.exposed.v1.core.vendors.MysqlDialect
import org.jetbrains.exposed.v1.core.vendors.SQLiteDialect
import org.jetbrains.exposed.v1.core.vendors.currentDialect

/**
 * `CAST(expr AS <the dialect's binary floating point type>)`.
 *
 * Database decimal arithmetic is NOT IEEE double arithmetic, and CEL's is: an attribute value
 * reaches `check()` as a protobuf number, i.e. always a CEL double, so `aNumber * 0.1 == 0.3` is
 * FALSE there (`0.30000000000000004`) while H2 and PostgreSQL type a bare `0.1` as exact NUMERIC
 * and answer TRUE. Every column entering arithmetic, and every column compared against a
 * fractional constant, is therefore forced through a real cast rather than a type marker.
 *
 * The three spellings are not interchangeable. PostgreSQL's `REAL` is single precision and would
 * silently round a CEL double; MySQL has no `DOUBLE PRECISION` in a `CAST` before 8.0.17 and
 * spells the target `DOUBLE`; SQLite's only floating type is `REAL`, which is 8 bytes there. The
 * dialect is read inside [toQueryBuilder] because translation happens outside any transaction.
 */
internal class IeeeDoubleCast(private val expr: Expression<*>) : Function<Double>(DoubleColumnType()) {
    override fun toQueryBuilder(queryBuilder: QueryBuilder): Unit = queryBuilder {
        append("CAST(", expr, " AS ", targetType(), ")")
    }

    private fun targetType(): String = when (currentDialect) {
        // MariaDBDialect extends MysqlDialect, so this arm covers both.
        is MysqlDialect -> "DOUBLE"
        is SQLiteDialect -> "REAL"
        else -> "DOUBLE PRECISION"
    }
}
