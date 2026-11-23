package dev.cerbos.queryplan.exposed.sql

import dev.cerbos.queryplan.exposed.TriLogic
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.QueryBuilder
import org.jetbrains.exposed.v1.core.append
import org.jetbrains.exposed.v1.core.stringParam
import org.jetbrains.exposed.v1.core.vendors.SQLiteDialect
import org.jetbrains.exposed.v1.core.vendors.currentDialect

/**
 * [predicate], except on SQLite, whose LIKE reads a string only up to its first NUL. There [exact]
 * stands in when given; otherwise a value holding a NUL is UNKNOWN.
 */
internal class SqliteUnknownPastNul(
    private val predicate: Op<Boolean>,
    private val value: Expression<*>,
    private val exact: Op<Boolean>?,
) : Op<Boolean>() {
    override fun toQueryBuilder(queryBuilder: QueryBuilder): Unit = queryBuilder {
        val rendered = when {
            currentDialect !is SQLiteDialect -> predicate
            exact != null -> exact
            else -> TriLogic.baseUnlessUnknown(predicate, HoldsNul(value))
        }
        append("(", rendered, ")")
    }

    private class HoldsNul(private val value: Expression<*>) : Op<Boolean>() {
        override fun toQueryBuilder(queryBuilder: QueryBuilder): Unit = queryBuilder {
            append("(instr(", value, ", char(0)) > 0)")
        }
    }

    /**
     * `hex(value) LIKE head <wildcard> tail` over the UTF-8 bytes. Exact for one wildcard between
     * two anchored parts: each part starts on a byte boundary, so no match is misaligned.
     */
    class HexLike(private val value: Expression<*>, head: String, wildcard: String, tail: String) : Op<Boolean>() {
        private val pattern = hex(head) + (if (wildcard == "%") "%" else "__%") + hex(tail)

        override fun toQueryBuilder(queryBuilder: QueryBuilder): Unit = queryBuilder {
            append("(CASE WHEN ", value, " IS NOT NULL THEN hex(", value, ") LIKE ", stringParam(pattern), " END)")
        }

        private fun hex(s: String): String = s.toByteArray(Charsets.UTF_8).joinToString("") { "%02X".format(it) }
    }
}
