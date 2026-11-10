package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand

/**
 * A parsed CEL lambda operand: body and iteration variable. Callers supply their own messages; every
 * failure is a malformed plan except a two-variable lambda, which is refused.
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
            if (operands.size == 3) {
                // `list.exists(i, v, body)` and `map.exists(k, v, body)`: CEL's two-variable
                // comprehensions, which the planner emits as a lambda binding two variables. The
                // first is a list index or a map key, and neither has a column: a relation
                // subquery's rows carry no CEL list order, and no mapping names a map's key set.
                throw Refusals.unsupported(
                    "A two-variable lambda binds a list index or a map key, which no mapped column " +
                        "holds: a relation subquery's rows carry no list order, and no mapping " +
                        "enumerates a map's keys",
                )
            }
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
