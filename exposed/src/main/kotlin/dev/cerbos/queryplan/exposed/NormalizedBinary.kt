package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand

/**
 * A binary operator with its operands in field-first order.
 *
 * The planner preserves policy source order, so `5 < R.attr.x` arrives as `lt(value, variable)`.
 * Operands are ranked (variable > expression > value) and swapped when the lower rank comes first,
 * MIRRORING the operator: `lt` becomes `gt`. Value-first operand inversion is this repository's
 * canonical bug class; it has shipped to more than one adapter.
 *
 * `contains`, `startsWith` and `endsWith` are RECEIVER-SENSITIVE and deliberately absent from
 * [ORDER_NORMALIZABLE]: `"a,b".contains(R.attr.x)` arrives as `contains(value, variable)` where the
 * constant is the haystack, and swapping would silently invert haystack and needle. They keep
 * planner source order and are handled positionally.
 */
internal class NormalizedBinary private constructor(val operator: String, val operands: List<Operand>) {
    companion object {
        private val ORDER_NORMALIZABLE = setOf(
            "eq", "ne", "lt", "gt", "le", "ge",
            "in", "hasIntersection", "has_intersection",
        )

        fun of(operator: String, operands: List<Operand>): NormalizedBinary =
            if (operator in ORDER_NORMALIZABLE && operands.size == 2 && rank(operands[0]) < rank(operands[1])) {
                NormalizedBinary(mirror(operator), listOf(operands[1], operands[0]))
            } else {
                NormalizedBinary(operator, operands)
            }

        /** `lt`/`le`/`gt`/`ge` mirror when their operands swap sides; symmetric operators do not. */
        fun mirror(operator: String): String = when (operator) {
            "lt" -> "gt"
            "gt" -> "lt"
            "le" -> "ge"
            "ge" -> "le"
            else -> operator
        }

        private fun rank(operand: Operand): Int = when (operand.nodeCase) {
            Operand.NodeCase.VARIABLE -> 2
            Operand.NodeCase.EXPRESSION -> 1
            else -> 0
        }
    }
}
