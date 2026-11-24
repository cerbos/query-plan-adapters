/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.jpa;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

/** Substitutes reads of a bound lambda variable in a plan expression. */
final class LambdaSubstitution {
    private static final Set<String> LAMBDA_BINDING_OPERATORS =
            Set.of("exists", "exists_one", "all", "filter", "map", "except");

    private LambdaSubstitution() {}

    /**
     * Rewrites every read of {@code varName} ({@code var} or {@code var.f}) with
     * {@code replace.apply(suffix)}, where the suffix is empty or starts with a dot. A nested
     * macro that rebinds the name shadows it, so only its collection operand is rewritten.
     */
    static Operand substitute(Operand operand, String varName, Function<String, Operand> replace) {
        switch (operand.getNodeCase()) {
            case VARIABLE -> {
                String name = operand.getVariable();
                if (name.equals(varName)) {
                    return replace.apply("");
                }
                if (name.startsWith(varName + ".")) {
                    return replace.apply(name.substring(varName.length()));
                }
                return operand;
            }
            case EXPRESSION -> {
                Expression expr = operand.getExpression();
                List<Operand> ops = expr.getOperandsList();
                Expression.Builder rebuilt = expr.toBuilder();
                boolean shadows = LAMBDA_BINDING_OPERATORS.contains(expr.getOperator()) && ops.size() == 2
                        && bindsVariable(ops.get(1), varName);
                int count = shadows ? 1 : ops.size();
                for (int i = 0; i < count; i++) {
                    rebuilt.setOperands(i, substitute(ops.get(i), varName, replace));
                }
                return Operand.newBuilder().setExpression(rebuilt).build();
            }
            default -> {
                return operand;
            }
        }
    }

    private static boolean bindsVariable(Operand lambdaOperand, String varName) {
        if (lambdaOperand.getNodeCase() != Operand.NodeCase.EXPRESSION
                || !"lambda".equals(lambdaOperand.getExpression().getOperator())) {
            return false;
        }
        List<Operand> ops = lambdaOperand.getExpression().getOperandsList();
        return ops.subList(1, ops.size()).stream().anyMatch(v ->
                v.getNodeCase() == Operand.NodeCase.VARIABLE && varName.equals(v.getVariable()));
    }
}
