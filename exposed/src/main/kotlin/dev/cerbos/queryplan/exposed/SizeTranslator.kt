package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.jetbrains.exposed.v1.core.Op

/**
 * `size(x) op N`: string length over a column, element count over a relation chain, and the strict
 * `size(filter(...))` count. Owns the threshold arithmetic for fractional and out-of-range
 * constants.
 *
 * OWNED BY THE RELATION SIDE. Stub.
 */
internal class SizeTranslator(@Suppress("unused") private val translation: Translation) {
    /**
     * The comparison when one operand is a `size(...)` expression, or `null` when neither is.
     * Receives the NORMALISED operator and operands.
     */
    fun trySizeComparison(operator: String, operands: List<Operand>, scope: Scope): Op<Boolean>? {
        val hasSize = operands.any { it.nodeCase == Operand.NodeCase.EXPRESSION && it.expression.operator == "size" }
        if (hasSize) throw Refusals.notYetImplemented("a size() operand of $operator")
        return null
    }
}
