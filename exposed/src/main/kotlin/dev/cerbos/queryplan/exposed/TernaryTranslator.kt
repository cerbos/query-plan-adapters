package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.jetbrains.exposed.v1.core.Op

/**
 * The CEL ternary in both positions, as a rewrite: each branch is substituted into the surrounding
 * shape and walked again, then combined with [TriLogic.ternary].
 */
internal class TernaryTranslator(private val translation: Translation) {

    /** `if(...)` as the whole condition: both branches are themselves boolean. */
    fun translateBare(operands: List<Operand>, scope: Scope): Op<Boolean> =
        translateTernary(operands, scope) { branch -> translation.walker.traverse(branch, scope) }

    /**
     * A comparison or `in` with a ternary operand, or `null`. Runs first, on raw operands, so branches
     * substitute into the comparison as written. Under list or string `+` the substitution is still exact.
     */
    fun tryTernaryComparison(operator: String, operands: List<Operand>, scope: Scope): Op<Boolean>? {
        val distributes = operator in ComparisonTranslator.COMPARISON_OPERATORS || operator == "in" ||
            operator in ComparisonTranslator.STRING_MATCH_OPERATORS
        if (!distributes || operands.size != 2) {
            return null
        }
        for (index in 0..1) {
            val path = ternaryUnderArithmetic(operands[index]) ?: continue
            val ternary = at(operands[index], path)
            return translateTernary(ternary.expression.operandsList, scope) { branch ->
                val substituted = PlanRewrites.foldListConcatenation(replaceAt(operands[index], path, 0, branch))
                val rebuilt = substitute(operator, operands, index, substituted)
                constantStringMatch(operator, rebuilt.operandsList)
                    ?: translation.walker.traverseExpression(rebuilt, scope)
            }
        }
        return null
    }

    /**
     * The operand indices from [operand] down to the first `if` that is [operand] or is reached
     * from it through [DISTRIBUTING] operators alone; `null` when there is none.
     */
    private fun ternaryUnderArithmetic(operand: Operand): List<Int>? {
        if (isTernary(operand)) return emptyList()
        if (operand.nodeCase != Operand.NodeCase.EXPRESSION || operand.expression.operator !in DISTRIBUTING) return null
        operand.expression.operandsList.forEachIndexed { i, nested ->
            ternaryUnderArithmetic(nested)?.let { return listOf(i) + it }
        }
        return null
    }

    private fun at(root: Operand, path: List<Int>): Operand =
        path.fold(root) { current, i -> current.expression.getOperands(i) }

    private fun replaceAt(root: Operand, path: List<Int>, depth: Int, replacement: Operand): Operand {
        if (depth == path.size) return replacement
        val i = path[depth]
        val rebuilt = root.expression.toBuilder()
            .setOperands(i, replaceAt(root.expression.getOperands(i), path, depth + 1, replacement))
        return Operand.newBuilder().setExpression(rebuilt).build()
    }

    /** The rewrite. A constant condition translates only its live branch. */
    private fun translateTernary(
        operands: List<Operand>,
        scope: Scope,
        branch: (Operand) -> Op<Boolean>,
    ): Op<Boolean> {
        if (operands.size != 3) {
            throw Refusals.malformed(
                "if (ternary) requires exactly 3 operands (condition, then, else), got ${operands.size}",
            )
        }
        val condition = operands[0]
        if (condition.nodeCase == Operand.NodeCase.VALUE) {
            val known = PlanValues.toKotlin(condition.value) as? Boolean
                // A non-boolean literal condition is a CEL type error the planner never emits.
                ?: throw Refusals.malformed("if (ternary) condition must be a boolean expression")
            return branch(operands[if (known) 1 else 2])
        }
        return TriLogic.ternary(
            translation.walker.traverse(condition, scope),
            branch(operands[1]),
            branch(operands[2]),
        )
    }

    /** A string match whose receiver and needle a branch left as constants, decided here. */
    private fun constantStringMatch(operator: String, operands: List<Operand>): Op<Boolean>? {
        if (operator !in ComparisonTranslator.STRING_MATCH_OPERATORS) return null
        if (operands.any { it.nodeCase != Operand.NodeCase.VALUE }) return null
        val receiver = PlanValues.toKotlin(operands[0].value) as? String ?: return TriLogic.unknown()
        val needle = PlanValues.toKotlin(operands[1].value) as? String ?: return TriLogic.unknown()
        val holds = when (operator) {
            "contains" -> receiver.contains(needle)
            "startsWith" -> receiver.startsWith(needle)
            else -> receiver.endsWith(needle)
        }
        return if (holds) Op.TRUE else Op.FALSE
    }

    private fun isTernary(operand: Operand): Boolean =
        operand.nodeCase == Operand.NodeCase.EXPRESSION && operand.expression.operator == "if"

    /** Rebuilds `operator(operands...)` with the operand at [index] replaced. */
    private fun substitute(
        operator: String,
        operands: List<Operand>,
        index: Int,
        replacement: Operand,
    ): PlanResourcesFilter.Expression {
        val builder = PlanResourcesFilter.Expression.newBuilder().setOperator(operator)
        operands.forEachIndexed { position, operand ->
            builder.addOperands(if (position == index) replacement else operand)
        }
        return builder.build()
    }

    private companion object {
        /** Strict operators a ternary operand distributes out of: `f(c ? a : b)` is `c ? f(a) : f(b)`. */
        val DISTRIBUTING = setOf("add", "sub", "mult", "div", "mod")
    }
}
