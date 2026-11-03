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
 * A string ordering in CEL's order, by CODE POINT, where the two operands can hold a UTF-16 code
 * unit at or above 0xD800.
 *
 * Code point order and UTF-16 code unit order disagree exactly there: an astral character is a
 * surrogate pair from 0xD800, which UTF-16 order puts BEFORE U+E000–U+FFFF. H2 compares strings by
 * code unit (Java's `String.compareTo`), so on H2 both sides are compared as their UTF-8 bytes,
 * `STRINGTOUTF8(a) < STRINGTOUTF8(b)`: UTF-8 preserves code point order and H2 compares binary
 * values as unsigned bytes. PostgreSQL under the `C` collation, MySQL under `utf8mb4_0900_bin` and
 * SQLite's BINARY collation already compare UTF-8 bytes, so the comparison is left as it is there
 * (the collation requirement every ordering already carries). Any other dialect raises when the
 * statement is rendered rather than guess at its collation.
 *
 * The dialect is read inside [toQueryBuilder] because translation happens outside any transaction.
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
