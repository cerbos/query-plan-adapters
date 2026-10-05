/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;

import java.util.Map;

/**
 * Folds the literal expressions the planner emits for a list or map it could not write as one
 * value: {@code list(v1, v2)} of constants, {@code struct(set-field("k", v), ...)} for a
 * map literal, and {@code add(list, list)} of two constant lists, which a ternary substitution
 * leaves behind ({@link TernaryTranslator}). Each becomes a constant operand, so every translator sees the literal the policy
 * wrote, whether the planner spelled it as a value or as an expression.
 *
 * <p>{@code hierarchy(list(...))} is left alone: there a list is a path's segments, not a
 * value. A map with a non-string or non-constant key, or any element that is not constant,
 * stays an expression.
 */
final class PlanLiterals {

    private PlanLiterals() {}

    static Operand fold(Operand operand) {
        if (operand.getNodeCase() != Operand.NodeCase.EXPRESSION) {
            return operand;
        }
        PlanResourcesFilter.Expression expr = operand.getExpression();
        if ("hierarchy".equals(expr.getOperator())) {
            return operand;
        }
        PlanResourcesFilter.Expression.Builder rebuilt = expr.toBuilder().clearOperands();
        boolean allConstant = true;
        for (Operand child : expr.getOperandsList()) {
            Operand folded = fold(child);
            rebuilt.addOperands(folded);
            allConstant &= folded.getNodeCase() == Operand.NodeCase.VALUE;
        }
        Value literal = switch (expr.getOperator()) {
            case "list" -> allConstant ? list(rebuilt) : null;
            case "struct" -> map(rebuilt);
            case "add" -> allConstant ? concatenation(rebuilt) : null;
            default -> null;
        };
        return literal != null
                ? Operand.newBuilder().setValue(literal).build()
                : Operand.newBuilder().setExpression(rebuilt).build();
    }

    /**
     * Rewrites {@code index(attr, "key")}, CEL's {@code attr["key"]}, as the attribute
     * {@code attr.key} when the mapping declares {@code attr.key} and not {@code attr} itself.
     * On a map the two spellings are the same lookup, and both error on a missing key, so the
     * declared column carries its own null convention for that error. An attribute mapped
     * whole (a relation, or a column) is left to its translator.
     */
    static Operand selectMapKeys(Operand operand, Map<String, AttributeMapping> mapping) {
        if (operand.getNodeCase() != Operand.NodeCase.EXPRESSION) {
            return operand;
        }
        PlanResourcesFilter.Expression expr = operand.getExpression();
        if ("index".equals(expr.getOperator()) && expr.getOperandsCount() == 2
                && expr.getOperands(0).getNodeCase() == Operand.NodeCase.VARIABLE
                && expr.getOperands(1).getNodeCase() == Operand.NodeCase.VALUE
                && expr.getOperands(1).getValue().getKindCase() == Value.KindCase.STRING_VALUE) {
            String attribute = expr.getOperands(0).getVariable();
            String selected = attribute + "." + expr.getOperands(1).getValue().getStringValue();
            if (!mapping.containsKey(attribute) && mapping.containsKey(selected)) {
                return Operand.newBuilder().setVariable(selected).build();
            }
        }
        PlanResourcesFilter.Expression.Builder rebuilt = expr.toBuilder().clearOperands();
        expr.getOperandsList().forEach(child -> rebuilt.addOperands(selectMapKeys(child, mapping)));
        return Operand.newBuilder().setExpression(rebuilt).build();
    }

    private static Value list(PlanResourcesFilter.Expression.Builder list) {
        ListValue.Builder values = ListValue.newBuilder();
        list.getOperandsList().forEach(element -> values.addValues(element.getValue()));
        return Value.newBuilder().setListValue(values).build();
    }

    /**
     * {@code add(list, list)} of two constant lists, the list CEL's {@code +} concatenates them
     * into, else null: two strings or numbers are left to the comparison that folds them.
     */
    private static Value concatenation(PlanResourcesFilter.Expression.Builder add) {
        if (add.getOperandsCount() != 2
                || add.getOperands(0).getValue().getKindCase() != Value.KindCase.LIST_VALUE
                || add.getOperands(1).getValue().getKindCase() != Value.KindCase.LIST_VALUE) {
            return null;
        }
        ListValue.Builder values = add.getOperands(0).getValue().getListValue().toBuilder();
        values.addAllValues(add.getOperands(1).getValue().getListValue().getValuesList());
        return Value.newBuilder().setListValue(values).build();
    }

    /** The map a {@code struct} of constant {@code set-field} entries spells, else null. */
    private static Value map(PlanResourcesFilter.Expression.Builder struct) {
        Struct.Builder fields = Struct.newBuilder();
        for (Operand entry : struct.getOperandsList()) {
            if (entry.getNodeCase() != Operand.NodeCase.EXPRESSION
                    || !"set-field".equals(entry.getExpression().getOperator())
                    || entry.getExpression().getOperandsCount() != 2) {
                return null;
            }
            Operand key = entry.getExpression().getOperands(0);
            Operand value = entry.getExpression().getOperands(1);
            if (key.getNodeCase() != Operand.NodeCase.VALUE
                    || key.getValue().getKindCase() != Value.KindCase.STRING_VALUE
                    || value.getNodeCase() != Operand.NodeCase.VALUE
                    || fields.containsFields(key.getValue().getStringValue())) {
                return null;
            }
            fields.putFields(key.getValue().getStringValue(), value.getValue());
        }
        return Value.newBuilder().setStructValue(fields).build();
    }
}
