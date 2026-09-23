/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

import java.util.List;

/** A parsed CEL lambda operand: its body and the name of its iteration variable. */
record ParsedLambda(Operand body, String varName) {

    /**
     * Validates and unpacks a {@code lambda(body, variable)} operand. Each caller supplies its
     * own messages because they are pinned per operator; every failure is a malformed plan.
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
