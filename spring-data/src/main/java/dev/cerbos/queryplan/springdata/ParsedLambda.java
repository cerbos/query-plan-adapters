package dev.cerbos.queryplan.springdata;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

import java.util.List;

/**
 * A parsed CEL lambda operand: its body and the name of its iteration variable.
 *
 * <p>Three operator families unpack a lambda — the collection macros, {@code size(filter(...))}
 * and {@code hasIntersection(map(...), ...)} — and each keeps its own wording for the same
 * three wire-contract violations, so the messages are caller-supplied and only the shape check
 * is shared.
 */
record ParsedLambda(Operand body, String varName) {

    /**
     * Validate and unpack a {@code lambda(body, var)} operand — an EXPRESSION with operator
     * {@code lambda}, exactly two operands, the second a VARIABLE. Error messages are
     * caller-supplied so each operator keeps its exact wording; the classification is not,
     * because every failure here is the wire contract ({@code lambda(body, variable)})
     * being violated.
     */
    static ParsedLambda parse(Operand lambdaOperand, String notLambdaMessage,
                              String arityMessage, String varMessage) {
        if (lambdaOperand.getNodeCase() != Operand.NodeCase.EXPRESSION
                || !"lambda".equals(lambdaOperand.getExpression().getOperator())) {
            throw Refusals.malformed(notLambdaMessage);
        }
        List<Operand> lambdaOps = lambdaOperand.getExpression().getOperandsList();
        if (lambdaOps.size() != 2) {
            throw Refusals.malformed(arityMessage);
        }
        Operand varOp = lambdaOps.get(1);
        if (varOp.getNodeCase() != Operand.NodeCase.VARIABLE) {
            throw Refusals.malformed(varMessage);
        }
        return new ParsedLambda(lambdaOps.get(0), varOp.getVariable());
    }
}
