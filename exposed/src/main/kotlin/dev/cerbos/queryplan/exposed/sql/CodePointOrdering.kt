package dev.cerbos.queryplan.exposed.sql

import dev.cerbos.queryplan.exposed.ScalarRefusals
import org.jetbrains.exposed.v1.core.ComplexExpression
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.QueryBuilder
import org.jetbrains.exposed.v1.core.append
import org.jetbrains.exposed.v1.core.vendors.H2Dialect
import org.jetbrains.exposed.v1.core.vendors.MysqlDialect
import org.jetbrains.exposed.v1.core.vendors.PostgreSQLDialect
import org.jetbrains.exposed.v1.core.vendors.SQLiteDialect
import org.jetbrains.exposed.v1.core.vendors.currentDialect

/**
 * A string ordering by code point, for operands that may hold a code unit at or above 0xD800. H2
 * compares UTF-16 units, so both sides compare as UTF-8 bytes there; PostgreSQL `C`, MySQL
 * `utf8mb4_0900_bin` and SQLite BINARY already do. Any other dialect raises at render time.
 */
internal class CodePointOrdering(
    private val left: Expression<*>,
    private val right: Expression<*>,
    private val opSign: String,
) : Op<Boolean>(), ComplexExpression {
    override fun toQueryBuilder(queryBuilder: QueryBuilder): Unit = queryBuilder {
        when (currentDialect) {
            is H2Dialect -> append("STRINGTOUTF8(", left, ") ", opSign, " STRINGTOUTF8(", right, ")")
            is PostgreSQLDialect, is MysqlDialect, is SQLiteDialect -> append(left, " ", opSign, " ", right)
            else -> throw ScalarRefusals.codeUnitOrdering(opSign, "a string attribute")
        }
    }

    companion object {
        /** Whether [text] holds a UTF-16 code unit at or above 0xD800. */
        fun needed(text: String): Boolean = text.any { it.code >= 0xD800 }

        fun sign(operator: String): String = when (operator) {
            "lt" -> "<"
            "le" -> "<="
            "gt" -> ">"
            "ge" -> ">="
            else -> throw IllegalArgumentException("not an ordering: $operator")
        }
    }
}
