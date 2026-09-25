package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand

/**
 * A parsed CEL lambda operand: its body and the name of its iteration variable.
 *
 * Three operator families unpack a lambda — the collection macros, `size(filter(...))` and
 * `hasIntersection(map(...), …)` — and each keeps its own wording for the same three wire-contract
 * violations, so the messages are caller-supplied and only the shape check is shared. The
 * CLASSIFICATION is not caller-supplied: every failure here is the planner's `lambda(body,
 * variable)` contract being violated, which no Cerbos planner output produces.
 */
internal class ParsedLambda private constructor(val body: Operand, val variable: String) {
    companion object {
        fun parse(
            operand: Operand,
            notLambdaMessage: String,
            arityMessage: String,
            variableMessage: String,
        ): ParsedLambda {
            if (operand.nodeCase != Operand.NodeCase.EXPRESSION || operand.expression.operator != "lambda") {
                throw Refusals.malformed(notLambdaMessage)
            }
            val operands = operand.expression.operandsList
            if (operands.size != 2) {
                throw Refusals.malformed(arityMessage)
            }
            val variable = operands[1]
            if (variable.nodeCase != Operand.NodeCase.VARIABLE || variable.variable.isEmpty()) {
                throw Refusals.malformed(variableMessage)
            }
            return ParsedLambda(operands[0], variable.variable)
        }
    }
}
