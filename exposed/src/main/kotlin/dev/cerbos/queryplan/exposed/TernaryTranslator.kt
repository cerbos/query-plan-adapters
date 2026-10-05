package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.jetbrains.exposed.v1.core.Op

/**
 * The CEL ternary, `if(condition, whenTrue, whenFalse)`, in both positions. Both are REWRITES, not
 * lowerings to `CASE`: the branches are substituted back into the surrounding shape and walked
 * again, so a branch translates exactly as the same condition written directly, and the three
 * predicates are combined with [TriLogic.ternary].
 *
 * Rewriting rather than emitting `CASE WHEN` is what keeps every typed leaf path — field-first
 * normalisation, the `size()` handling, the `add` fold, double-space comparison — reachable from
 * inside a ternary. Nesting, and a ternary on the other side of the comparison, then come for
 * free through the recursion.
 */
internal class TernaryTranslator(private val translation: Translation) {

    /** `if(...)` as the whole condition: both branches are themselves boolean. */
    fun translateBare(operands: List<Operand>, scope: Scope): Op<Boolean> =
        translateTernary(operands, scope) { branch -> translation.walker.traverse(branch, scope) }

    /**
     * A comparison or `in` with a ternary operand, or `null` when neither operand holds one. Called
     * FIRST, on raw operands, because it must see source order: each branch is substituted back
     * into the comparison AS WRITTEN and walked again, and mirroring first would substitute into a
     * comparison the policy never had.
     *
     * The ternary may sit under list or string `+` (`["a"] + (c ? ["b"] : []) == [...]`): `+`
     * evaluates both its operands, so substituting a branch in place keeps CEL's meaning, and a
     * substituted `add(list, list)` of constants is folded into the list it concatenates.
     */
    fun tryTernaryComparison(operator: String, operands: List<Operand>, scope: Scope): Op<Boolean>? {
        if ((operator !in ComparisonTranslator.COMPARISON_OPERATORS && operator != "in") || operands.size != 2) {
            return null
        }
        for (index in 0..1) {
            val path = ternaryUnderAdd(operands[index]) ?: continue
            val ternary = at(operands[index], path)
            return translateTernary(ternary.expression.operandsList, scope) { branch ->
                val substituted = PlanRewrites.foldListConcatenation(replaceAt(operands[index], path, 0, branch))
                translation.walker.traverseExpression(substitute(operator, operands, index, substituted), scope)
            }
        }
        return null
    }

    /**
     * The operand indices from [operand] down to the first `if` that is [operand] or is reached
     * from it through `add` alone; `null` when there is none.
     */
    private fun ternaryUnderAdd(operand: Operand): List<Int>? {
        if (isTernary(operand)) return emptyList()
        if (operand.nodeCase != Operand.NodeCase.EXPRESSION || operand.expression.operator != "add") return null
        operand.expression.operandsList.forEachIndexed { i, nested ->
            ternaryUnderAdd(nested)?.let { return listOf(i) + it }
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

    /**
     * The rewrite itself. A CONSTANT boolean condition folds to a single branch, and only that
     * branch is translated — so an untranslatable dead branch cannot fail the whole plan.
     *
     * Null semantics and the third arm belong to [TriLogic.ternary]: a NULL condition column is a
     * CEL evaluation error, which denies, so the predicate has to be UNKNOWN there rather than
     * FALSE — two false branches would otherwise collapse it to FALSE and an enclosing negation
     * would readmit the row.
     */
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
}
