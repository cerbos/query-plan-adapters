package dev.cerbos.queryplan.exposed.sql

import org.jetbrains.exposed.v1.core.AndOp
import org.jetbrains.exposed.v1.core.Column
import org.jetbrains.exposed.v1.core.ComparisonOp
import org.jetbrains.exposed.v1.core.CompoundBooleanOp
import org.jetbrains.exposed.v1.core.Exists
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.IsNotNullOp
import org.jetbrains.exposed.v1.core.IsNullOp
import org.jetbrains.exposed.v1.core.LiteralOp
import org.jetbrains.exposed.v1.core.NotOp
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.OrOp
import org.jetbrains.exposed.v1.core.QueryParameter

/**
 * Whether a predicate is provably never NULL, so a quantifier over it needs no search for an erroring
 * element. Conservative: an unrecognised node may be NULL. Inside an AND, a conjunct may be NULL only
 * where an `IS NOT NULL` before it is false.
 */
internal object NeverNull {
    fun of(node: Expression<*>, known: Set<String> = emptySet()): Boolean = when (node) {
        is Op.TRUE, is Op.FALSE, is Exists, is IsNullOp, is IsNotNullOp -> true
        is NotOp<*> -> of(node.expr, known)
        is IndexedOnMysql -> of(node.prefilter, known) && of(node.exact, known)
        is AndOp -> operands(node)?.let { conjuncts ->
            val guarded = known.toMutableSet()
            conjuncts.all { conjunct ->
                of(conjunct, guarded).also { if (conjunct is IsNotNullOp) columnKey(conjunct.expr)?.let(guarded::add) }
            }
        } ?: false
        is OrOp -> operands(node)?.all { of(it, known) } ?: false
        is ComparisonOp -> value(node.expr1, known) && value(node.expr2, known)
        else -> false
    }

    private fun value(node: Expression<*>, known: Set<String>): Boolean = when (node) {
        is ExactString<*> -> value(node.expression, known)
        is Column<*> -> !node.columnType.nullable || columnKey(node) in known
        is QueryParameter<*> -> node.value != null
        is LiteralOp<*> -> node.value != null
        else -> false
    }

    private fun columnKey(node: Expression<*>): String? = when (node) {
        is ExactString<*> -> columnKey(node.expression)
        is Column<*> -> "${node.table.tableName}.${node.name}"
        else -> null
    }

    // Exposed keeps a compound's operands internal; without them nothing is proved.
    private val expressions = runCatching {
        CompoundBooleanOp::class.java.getDeclaredField("expressions").apply { isAccessible = true }
    }.getOrNull()

    @Suppress("UNCHECKED_CAST")
    private fun operands(node: CompoundBooleanOp): List<Expression<*>>? =
        runCatching { expressions?.get(node) as List<Expression<*>>? }.getOrNull()
}
