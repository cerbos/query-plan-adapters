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
            "filter" -> throw Refusals.unsupported(
                "filter() returns a list, not a boolean, so it cannot be a condition on its own; " +
                    "only size(filter(...)) gives the list a scalar meaning",
            )
            "map" -> throw Refusals.unsupported(
                "map() returns a list, not a boolean, so it cannot be a condition on its own; " +
                    "only hasIntersection(map(...), [...]) gives the list a scalar meaning",
            )
            "except" -> throw ScalarRefusals.exceptUnsupported()
            "hasIntersection", "has_intersection" -> translation.membership.translateHasIntersection(operands, scope)
            "in" -> translation.membership.translateIn(operands, scope)
            "if" -> translation.ternary.translateBare(operands, scope)
            "overlaps", "ancestorOf", "descendentOf" -> translation.hierarchy.translate(operator, operands, scope)
            "matches" -> matches(operands, scope)
            else -> matchesComparedWithBoolean(operator, operands, scope)
                ?: translation.comparisons.translate(operator, operands, scope)
        }
    }

    /**
     * `matches(attribute, "pattern")`. A pattern that is not a string constant (read from a column,
     * or computed) has nothing to translate at build time and is refused; a non-string constant has
     * no `matches()` overload, so it is UNKNOWN.
     */
    private fun matches(operands: List<Operand>, scope: Scope): Op<Boolean> {
        if (operands.size != 2) throw Refusals.malformed("matches requires exactly 2 operands, got ${operands.size}")
        val (receiver, pattern) = operands
        if (receiver.nodeCase != Operand.NodeCase.VARIABLE || pattern.nodeCase != Operand.NodeCase.VALUE) {
            throw Refusals.unsupported(
                "matches() is translated only between a mapped attribute and a constant pattern: " +
                    "the pattern is compiled into LIKE at translation time",
            )
        }
        val target = scope.scalar(receiver.variable)
        val text = PlanValues.toKotlin(pattern.value) as? String ?: return TriLogic.unknown()
        return translation.regex.matches(target, text)
    }

    /**
     * `matches(...) == true`, `!= false` and their mirrors, which keep their wrapper on the wire: the
     * match itself, or its negation. `null` when the node is not that shape.
     */
    private fun matchesComparedWithBoolean(operator: String, operands: List<Operand>, scope: Scope): Op<Boolean>? {
        if ((operator != "eq" && operator != "ne") || operands.size != 2) return null
        val match = operands.firstOrNull {
            it.nodeCase == Operand.NodeCase.EXPRESSION && it.expression.operator == "matches"
        } ?: return null
        val other = operands.first { it !== match }
        if (other.nodeCase != Operand.NodeCase.VALUE) return null
        val expected = PlanValues.toKotlin(other.value) as? Boolean ?: return null
        val translated = matches(match.expression.operandsList, scope)
        return if (expected == (operator == "eq")) translated else TriLogic.not(translated)
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
