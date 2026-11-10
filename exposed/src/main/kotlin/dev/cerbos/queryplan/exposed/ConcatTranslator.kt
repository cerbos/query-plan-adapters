package dev.cerbos.queryplan.exposed

import dev.cerbos.queryplan.exposed.sql.ExactString
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import dev.cerbos.queryplan.exposed.sql.ConcatExpression
import dev.cerbos.queryplan.exposed.sql.ScalarColumnKind
import dev.cerbos.queryplan.exposed.sql.ScalarColumnTypes
import org.jetbrains.exposed.v1.core.Expression

/**
 * CEL's `+` over strings. One text operand, a string constant or a text column, makes the whole
 * expression a concatenation, since CEL has no mixed-type `+` (#391).
 */
internal object ConcatTranslator {

    /** Whether an `add`-rooted operand is a string concatenation rather than arithmetic. */
    fun isConcatenation(operand: Operand, scope: Scope): Boolean =
        operand.nodeCase == Operand.NodeCase.EXPRESSION &&
            operand.expression.operator == "add" &&
            operand.expression.operandsCount == 2 &&
            hasTextOperand(operand, scope)

    /** One side of a comparison as text: a bound string, a text column, or their NULL-propagating concatenation. */
    fun textExpression(operand: Operand, scope: Scope): Expression<*> = when (operand.nodeCase) {
        Operand.NodeCase.VALUE -> {
            val value = PlanValues.toKotlin(operand.value)
            if (value !is String) {
                throw Refusals.unsupported(
                    "String concatenation requires string operands, got ${PlanValues.typeName(value)}: " +
                        "CEL has no mixed-type `+`, so this comparison errors at check time and denies",
                )
            }
            ExactString.param(value)
        }
        Operand.NodeCase.VARIABLE -> {
            val target = scope.scalar(operand.variable)
            if (ScalarColumnTypes.kindOf(target.column) != ScalarColumnKind.TEXT) {
                throw Refusals.unmapped(
                    "String concatenation over '${operand.variable}' requires a text column, but " +
                        "it maps to a ${ScalarColumnTypes.describe(target.column)} column",
                )
            }
            target.expression
        }
        Operand.NodeCase.EXPRESSION -> {
            val expression = operand.expression
            if (expression.operator != "add") {
                throw Refusals.unsupported(
                    "Unexpected ${expression.operator}() expression inside a string concatenation",
                )
            }
            if (expression.operandsCount != 2) throw Refusals.malformed("add requires exactly 2 operands")
            // Flattened, so `a + b + c` is one CONCAT rather than a nest of them: fewer nodes, and
            // every dialect's two-argument form is the same operator anyway.
            ConcatExpression(expression.operandsList.flatMap { flatten(it, scope) })
        }
        else -> throw Refusals.malformed(
            "Unexpected operand type in a string concatenation: ${operand.nodeCase}",
        )
    }

    private fun flatten(operand: Operand, scope: Scope): List<Expression<*>> =
        if (operand.nodeCase == Operand.NodeCase.EXPRESSION &&
            operand.expression.operator == "add" &&
            operand.expression.operandsCount == 2
        ) {
            operand.expression.operandsList.flatMap { flatten(it, scope) }
        } else {
            listOf(textExpression(operand, scope))
        }

    private fun hasTextOperand(operand: Operand, scope: Scope): Boolean = when (operand.nodeCase) {
        Operand.NodeCase.VALUE -> PlanValues.toKotlin(operand.value) is String
        Operand.NodeCase.VARIABLE ->
            ScalarColumnTypes.kindOf(scope.scalar(operand.variable).column) == ScalarColumnKind.TEXT
        Operand.NodeCase.EXPRESSION ->
            operand.expression.operator == "add" && operand.expression.operandsList.any { hasTextOperand(it, scope) }
        else -> false
    }
}
