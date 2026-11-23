/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Rewrites hasAll, hasAny, hasOnly and hasExactly (CEL list extensions, Cerbos 0.58) into
 * hasIntersection and isSubset, bottom-up. Each rewrite is exact in CEL, errors included; a call
 * without exactly one literal list operand is left for the translator to refuse, and an operator
 * the caller overrides is left for the override.
 */
final class SetFunctions {
    private static final Set<String> OPERATORS = Set.of("hasAll", "hasAny", "hasOnly", "hasExactly");

    private SetFunctions() {}

    static Operand rewrite(Operand operand, Map<String, ?> overrides) {
        if (operand.getNodeCase() != Operand.NodeCase.EXPRESSION) {
            return operand;
        }
        Expression expression = operand.getExpression();
        List<Operand> operands = new ArrayList<>();
        for (Operand child : expression.getOperandsList()) {
            operands.add(rewrite(child, overrides));
        }
        String operator = expression.getOperator();
        if (OPERATORS.contains(operator) && overrides.get(operator) == null && operands.size() == 2) {
            Operand rewritten = setFunction(operator, operands.get(0), operands.get(1));
            if (rewritten != null) {
                return rewritten;
            }
        }
        return call(operator, operands);
    }

    private static Operand setFunction(String operator, Operand first, Operand second) {
        boolean firstLiteral = isLiteralList(first);
        if (firstLiteral == isLiteralList(second)) {
            return null;
        }
        Operand list = firstLiteral ? second : first;
        Operand literal = firstLiteral ? first : second;
        if (firstLiteral && operator.equals("hasAll")) {
            operator = "hasOnly";
        } else if (firstLiteral && operator.equals("hasOnly")) {
            operator = "hasAll";
        }
        return switch (operator) {
            case "hasAny" -> call("hasIntersection", List.of(list, literal));
            case "hasAll" -> containsAll(list, literal);
            case "hasOnly" -> call("isSubset", List.of(list, literal));
            default -> call("and", List.of(containsAll(list, literal),
                    call("isSubset", List.of(list, literal))));
        };
    }

    private static Operand containsAll(Operand list, Operand literal) {
        List<Value> values = literal.getValue().getListValue().getValuesList();
        if (values.isEmpty()) {
            return call("not", List.of(call("hasIntersection", List.of(list, listOf(List.of())))));
        }
        List<Operand> tests = new ArrayList<>();
        for (Value value : values) {
            tests.add(call("hasIntersection", List.of(list, listOf(List.of(value)))));
        }
        return tests.size() == 1 ? tests.get(0) : call("and", tests);
    }

    private static boolean isLiteralList(Operand operand) {
        return operand.getNodeCase() == Operand.NodeCase.VALUE
                && operand.getValue().getKindCase() == Value.KindCase.LIST_VALUE;
    }

    private static Operand listOf(List<Value> values) {
        return Operand.newBuilder()
                .setValue(Value.newBuilder().setListValue(ListValue.newBuilder().addAllValues(values)))
                .build();
    }

    private static Operand call(String operator, List<Operand> operands) {
        return Operand.newBuilder()
                .setExpression(Expression.newBuilder().setOperator(operator).addAllOperands(operands))
                .build();
    }
}
