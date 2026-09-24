/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Predicate;

import java.util.List;
import java.util.function.Function;

/**
 * Translates the CEL ternary {@code if(c, a, b)} as a whole condition
 * ({@link #handleBareTernary}) and as a comparison operand ({@link #tryTernaryComparison}).
 *
 * <p>Each branch is substituted back into the surrounding shape and walked again, so it
 * translates exactly like the same condition written directly.
 */
final class TernaryTranslator {

    private final CriteriaBuilder cb;
    private final TriPredicate tri;
    private final PlanWalker walker;

    TernaryTranslator(CriteriaBuilder cb, TriPredicate tri, PlanWalker walker) {
        this.cb = cb;
        this.tri = tri;
        this.walker = walker;
    }

    /**
     * Rewrites {@code cmp(if(c, a, b), other)} as {@code cmp(a, other)} and
     * {@code cmp(b, other)}. Runs on the raw operands, before any mirroring. Nested ternaries
     * and a ternary on the other side are handled by the recursion.
     *
     * @return the rewritten predicate, or {@code null} if this comparison has no ternary
     */
    Predicate tryTernaryComparison(String op, List<Operand> operands, Scope scope) {
        if (!ComparisonTranslator.COMPARISON_OPS.contains(op) || operands.size() != 2) {
            return null;
        }
        int idx;
        if (isIfExpression(operands.get(0))) {
            idx = 0;
        } else if (isIfExpression(operands.get(1))) {
            idx = 1;
        } else {
            return null;
        }
        List<Operand> ifOps = operands.get(idx).getExpression().getOperandsList();
        return translateTernary(ifOps,
                branch -> walker.traverseExpression(substituteOperand(op, operands, idx, branch), scope),
                scope);
    }

    private static boolean isIfExpression(Operand o) {
        return o.getNodeCase() == Operand.NodeCase.EXPRESSION
                && "if".equals(o.getExpression().getOperator());
    }

    /**
     * Rewrites {@code if(c, a, b)} with {@link TriPredicate#ternary}, which keeps the result
     * UNKNOWN when {@code c} is (CEL denies a null condition). A predicate rewrite rather than
     * {@code CASE WHEN} sends each branch through the normal leaf paths.
     *
     * <p>A constant condition translates only the live branch, so an untranslatable dead branch
     * cannot fail the plan.
     */
    private Predicate translateTernary(List<Operand> ifOps,
                                       Function<Operand, Predicate> branchTranslator,
                                       Scope scope) {
        if (ifOps.size() != 3) {
            throw Refusals.malformed(
                    "if (ternary) requires exactly 3 operands (condition, then, else), got "
                            + ifOps.size());
        }
        Operand condition = ifOps.get(0);
        Operand thenBranch = ifOps.get(1);
        Operand elseBranch = ifOps.get(2);

        if (condition.getNodeCase() == Operand.NodeCase.VALUE) {
            Boolean known = constantBooleanOrNull(condition);
            if (known == null) {
                // A non-boolean literal condition is a CEL type error.
                throw Refusals.malformed(
                        "if (ternary) condition must be a boolean expression");
            }
            return branchTranslator.apply(known ? thenBranch : elseBranch);
        }

        return tri.ternary(
                () -> walker.traverse(condition, scope),
                () -> branchTranslator.apply(thenBranch),
                () -> branchTranslator.apply(elseBranch));
    }

    /** {@code if(c, a, b)} used directly as a condition, so both branches are boolean. */
    Predicate handleBareTernary(List<Operand> operands, Scope scope) {
        return translateTernary(operands, branch -> booleanBranchPredicate(branch, scope), scope);
    }

    // A boolean constant branch becomes always-true or always-false.
    private Predicate booleanBranchPredicate(Operand branch, Scope scope) {
        if (branch.getNodeCase() == Operand.NodeCase.VALUE) {
            Boolean constant = constantBooleanOrNull(branch);
            if (constant == null) {
                throw Refusals.malformed(
                        "if (ternary) branch in boolean position must be a boolean");
            }
            return constant ? cb.conjunction() : cb.disjunction();
        }
        return walker.traverse(branch, scope);
    }

    private static PlanResourcesFilter.Expression substituteOperand(
            String op, List<Operand> operands, int idx, Operand replacement) {
        PlanResourcesFilter.Expression.Builder b =
                PlanResourcesFilter.Expression.newBuilder().setOperator(op);
        for (int i = 0; i < operands.size(); i++) {
            b.addOperands(i == idx ? replacement : operands.get(i));
        }
        return b.build();
    }

    private static Boolean constantBooleanOrNull(Operand o) {
        return PlanValues.protoValueToJava(o.getValue()) instanceof Boolean b ? b : null;
    }
}
