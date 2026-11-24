package dev.cerbos.queryplan.exposed

import com.google.protobuf.ListValue
import com.google.protobuf.Value
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand

/**
 * Rewrites hasAll, hasAny, hasOnly and hasExactly (CEL list extensions, Cerbos 0.58) into
 * hasIntersection and isSubset, bottom-up. Each rewrite is exact in CEL, errors included; a call
 * without exactly one literal list operand is left for the walk to refuse.
 */
internal object SetFunctions {
    private val OPERATORS = setOf("hasAll", "hasAny", "hasOnly", "hasExactly")

    fun rewrite(called: String, operands: List<Operand>): Operand? {
        if (called !in OPERATORS || operands.size != 2) return null
        val (first, second) = operands
        val firstLiteral = isLiteralList(first)
        if (firstLiteral == isLiteralList(second)) return null
        val (list, literal) = if (firstLiteral) second to first else first to second
        val operator = when {
            firstLiteral && called == "hasAll" -> "hasOnly"
            firstLiteral && called == "hasOnly" -> "hasAll"
            else -> called
        }
        val subset = { PlanRewrites.expression("isSubset", list, literal) }
        return when (operator) {
            "hasAny" -> PlanRewrites.expression("hasIntersection", list, literal)
            "hasAll" -> containsAll(list, literal)
            "hasOnly" -> subset()
            else -> PlanRewrites.expression("and", containsAll(list, literal), subset())
        }
    }

    private fun containsAll(list: Operand, literal: Operand): Operand {
        val values = literal.value.listValue.valuesList
        if (values.isEmpty()) {
            return PlanRewrites.expression("not", PlanRewrites.expression("hasIntersection", list, literalList(emptyList())))
        }
        val tests = values.map { PlanRewrites.expression("hasIntersection", list, literalList(listOf(it))) }
        return tests.singleOrNull() ?: PlanRewrites.expression("and", *tests.toTypedArray())
    }

    private fun isLiteralList(operand: Operand): Boolean =
        operand.nodeCase == Operand.NodeCase.VALUE && operand.value.kindCase == Value.KindCase.LIST_VALUE

    private fun literalList(values: List<Value>): Operand =
        PlanRewrites.constant(Value.newBuilder().setListValue(ListValue.newBuilder().addAllValues(values)).build())
}
