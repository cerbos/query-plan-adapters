package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.jetbrains.exposed.v1.core.Op

/**
 * The one walk over the plan's expression tree.
 *
 * Polarity is NOT a walk parameter. SQL is three-valued, so negation is applied around a built
 * predicate ([TriLogic.not]) and every collaborator is responsible for yielding UNKNOWN, never
 * FALSE, where CEL would raise an error.
 */
internal class PlanWalker(private val translation: Translation) {
    private var macroDepth = 0

    fun traverse(operand: Operand, scope: Scope): Op<Boolean> = when (operand.nodeCase) {
        Operand.NodeCase.EXPRESSION -> traverseExpression(operand.expression, scope)
        // A bare boolean attribute in condition position: `R.attr.aBool`.
        Operand.NodeCase.VARIABLE -> translation.leaf.applyLeaf("eq", scope.scalar(operand.variable), true)
        // A boolean constant as a whole condition. The planner never ships one at the root, but a
        // ternary rewrite substitutes one in: `aBool ? true : false` walks each branch through
        // here. Folded rather than routed to a leaf, because there is no column to compare.
        Operand.NodeCase.VALUE -> when (val constant = PlanValues.toKotlin(operand.value)) {
            true -> Op.TRUE
            false -> Op.FALSE
            else -> throw Refusals.malformed(
                "A constant in condition position must be a boolean, got ${PlanValues.typeName(constant)}",
            )
        }
        Operand.NodeCase.NODE_NOT_SET, null ->
            throw Refusals.malformed("Plan operand has no node set")
    }

    fun traverseExpression(expression: Expression, scope: Scope): Op<Boolean> {
        val operator = expression.operator
        val operands = expression.operandsList
        return when (operator) {
            "and" -> TriLogic.and(junction(operator, operands, scope))
            "or" -> TriLogic.or(junction(operator, operands, scope))
            "not" -> {
                if (operands.size != 1) throw Refusals.malformed("not requires exactly 1 operand, got ${operands.size}")
                TriLogic.not(traverse(operands[0], scope))
            }
            "exists", "exists_one", "all" -> translation.collections.translate(operator, operands, scope)
            // filter() and map() return a LIST, not a boolean. `filter(...)` in condition position is
            // not `size(filter(...)) > 0`, and lowering it as if it were over-grants.
            "filter", "map" -> throw Refusals.unsupported(
                "$operator() returns a list, not a boolean, so it cannot be a condition on its " +
                    "own; only size($operator(...)) and hasIntersection($operator(...), [...]) " +
                    "give the list a scalar meaning",
            )
            "except" -> throw ScalarRefusals.exceptUnsupported()
            "hasIntersection", "has_intersection" -> translation.membership.translateHasIntersection(operands, scope)
            "in" -> translation.membership.translateIn(operands, scope)
            "if" -> translation.ternary.translateBare(operands, scope)
            "overlaps", "ancestorOf", "descendentOf" -> translation.hierarchy.translate(operator, operands, scope)
            else -> translation.comparisons.translate(operator, operands, scope)
        }
    }

    private fun junction(operator: String, operands: List<Operand>, scope: Scope): List<Op<Boolean>> {
        if (operands.isEmpty()) throw Refusals.malformed("$operator requires at least 1 operand")
        return operands.map { traverse(it, scope) }
    }

    /**
     * Runs [body] one macro level deeper, refusing past [Options.maxMacroDepth]. Every collection
     * macro wraps its translation in this: each level multiplies the correlated subqueries the
     * filter carries, and the bound is what keeps that finite.
     */
    fun <T> enterMacro(operator: String, body: () -> T): T {
        val limit = translation.options.maxMacroDepth
        if (macroDepth >= limit) {
            throw Refusals.unsupported(
                "$operator is nested ${macroDepth + 1} collection macros deep, past maxMacroDepth=$limit",
            )
        }
        macroDepth++
        try {
            return body()
        } finally {
            macroDepth--
        }
    }
}
