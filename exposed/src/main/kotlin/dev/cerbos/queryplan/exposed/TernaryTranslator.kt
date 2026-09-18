package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.jetbrains.exposed.v1.core.Op

/**
 * The CEL ternary, `if(condition, whenTrue, whenFalse)`, in both positions. Both are REWRITES, not
 * lowerings to `CASE`: the branches are substituted back into the surrounding shape and walked
 * again, so a branch translates exactly as the same condition written directly, and the three
 * predicates are combined with [TriLogic.ternary].
 *
 * OWNED BY THE SCALAR SIDE. Stub.
 */
internal class TernaryTranslator(@Suppress("unused") private val translation: Translation) {
    /** `if(...)` as the whole condition. */
    fun translateBare(operands: List<Operand>, scope: Scope): Op<Boolean> =
        throw Refusals.notYetImplemented("the ternary operator")

    /**
     * A comparison with a ternary operand, or `null` when neither operand is one. Called FIRST by
     * [ComparisonTranslator.translate], on raw operands, because it must see source order.
     */
    fun tryTernaryComparison(operator: String, operands: List<Operand>, scope: Scope): Op<Boolean>? {
        val hasTernary = operands.any { it.nodeCase == Operand.NodeCase.EXPRESSION && it.expression.operator == "if" }
        if (hasTernary) throw Refusals.notYetImplemented("a ternary operand of $operator")
        return null
    }
}
