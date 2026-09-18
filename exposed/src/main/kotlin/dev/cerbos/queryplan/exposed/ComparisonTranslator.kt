package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.jetbrains.exposed.v1.core.Op

/**
 * Comparisons and string matches: everything the walk does not route by name.
 *
 * The reference adapter fixes the pipeline by CODE ORDER, and this follows it:
 * ternary rewrite (on RAW operands, before mirroring) -> normalise to field-first -> arity check
 * (before the size probe, which would otherwise translate a partial comparison) -> `size()`
 * comparisons -> operand resolution -> dispatch on the resolved pair.
 *
 * OWNED BY THE SCALAR SIDE. This minimal form handles one mapped column against one constant.
 */
internal class ComparisonTranslator(private val translation: Translation) {
    fun translate(operator: String, operands: List<Operand>, scope: Scope): Op<Boolean> {
        if (operator !in COMPARISON_OPERATORS && operator !in STRING_MATCH_OPERATORS) {
            throw Refusals.unsupported("Unsupported operator: $operator")
        }
        translation.ternary.tryTernaryComparison(operator, operands, scope)?.let { return it }

        val normalized = NormalizedBinary.of(operator, operands)
        if (normalized.operands.size != 2) {
            throw Refusals.malformed("$operator requires exactly 2 operands, got ${normalized.operands.size}")
        }
        translation.sizes.trySizeComparison(normalized.operator, normalized.operands, scope)?.let { return it }

        val (left, right) = normalized.operands
        if (left.nodeCase == Operand.NodeCase.VARIABLE && right.nodeCase == Operand.NodeCase.VALUE) {
            return translation.leaf.applyLeaf(
                normalized.operator,
                scope.scalar(left.variable),
                PlanValues.toKotlin(right.value),
            )
        }
        throw Refusals.notYetImplemented("$operator over anything but one column and one constant")
    }

    companion object {
        val COMPARISON_OPERATORS = setOf("eq", "ne", "lt", "le", "gt", "ge")
        val STRING_MATCH_OPERATORS = setOf("contains", "startsWith", "endsWith")
    }
}
