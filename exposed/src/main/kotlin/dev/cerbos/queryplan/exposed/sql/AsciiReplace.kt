package dev.cerbos.queryplan.exposed.sql

import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.Function
import org.jetbrains.exposed.v1.core.QueryBuilder
import org.jetbrains.exposed.v1.core.TextColumnType
import org.jetbrains.exposed.v1.core.append

/**
 * `REPLACE(expr, 'from', 'to')` for one ASCII letter, the step CEL's `upperAscii()` is spelled
 * from. `REPLACE` matches case-sensitively on every dialect this adapter targets (MySQL's included,
 * whatever the collation), and a NULL argument yields NULL. The letters are rendered inline: each
 * is one ASCII letter, which needs no quoting beyond the quotes and binds nothing.
 */
internal class AsciiReplace(
    private val expr: Expression<*>,
    private val from: String,
    private val to: String,
) : Function<String>(TextColumnType()) {
    init {
        require(from.length == 1 && from[0] in 'a'..'z' && to.length == 1 && to[0] in 'A'..'Z') {
            "AsciiReplace maps one lowercase ASCII letter to one uppercase ASCII letter"
        }
    }

    override fun toQueryBuilder(queryBuilder: QueryBuilder): Unit = queryBuilder {
        append("REPLACE(", expr, ", '", from, "', '", to, "')")
    }
}
