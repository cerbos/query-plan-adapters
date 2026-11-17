package dev.cerbos.queryplan.exposed.sql

import org.jetbrains.exposed.v1.core.Column
import org.jetbrains.exposed.v1.core.EqOp
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.LikeEscapeOp
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.QueryBuilder
import org.jetbrains.exposed.v1.core.StringColumnType
import org.jetbrains.exposed.v1.core.append
import org.jetbrains.exposed.v1.core.stringParam
import org.jetbrains.exposed.v1.core.vendors.MariaDBDialect
import org.jetbrains.exposed.v1.core.vendors.MysqlDialect
import org.jetbrains.exposed.v1.core.vendors.currentDialect

/**
 * [exact] on MySQL, preceded by [prefilter]: the same test under the column's own collation, which
 * every byte-equal value satisfies. The pair has [exact]'s three-valued result, and an index on the
 * column serves the prefilter, with its order for `ORDER BY ... LIMIT`, where the byte-exact form
 * reads every row. [exact] alone on every other dialect. Read at render time.
 */
internal class IndexedOnMysql(val prefilter: Op<Boolean>, val exact: Op<Boolean>) : Op<Boolean>() {
    override fun toQueryBuilder(queryBuilder: QueryBuilder): Unit = queryBuilder {
        val dialect = currentDialect
        if (dialect is MysqlDialect && dialect !is MariaDBDialect) append("(", prefilter, " AND ", exact, ")") else append(exact)
    }

    companion object {
        /** [exact], an `=` between [target] and [value], with an indexable prefilter when [target] is a string column. */
        fun eq(target: Expression<*>, value: String, exact: Op<Boolean>): Op<Boolean> =
            stringColumn(target)?.let { IndexedOnMysql(EqOp(it, stringParam(value)), exact) } ?: exact

        /** [exact], a LIKE [pattern] over [target] with no leading wildcard, likewise. */
        fun like(target: Expression<*>, pattern: String, exact: Op<Boolean>): Op<Boolean> {
            val column = stringColumn(target) ?: return exact
            return IndexedOnMysql(LikeEscapeOp(column, stringParam(pattern), true, LikeEscaping.ESCAPE_CHAR), exact)
        }

        private fun stringColumn(target: Expression<*>): Column<*>? =
            (target as? Column<*>)?.takeIf { it.columnType is StringColumnType }
    }
}
