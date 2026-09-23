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
 * The CEL ternary, {@code if(c, a, b)}, in both of its positions: as a whole condition
 * ({@link #handleBareTernary}) and as one operand of a comparison
 * ({@link #tryTernaryComparison}).
 *
 * <p>Both are REWRITES, not lowerings: the branches are substituted back into the surrounding
 * shape and walked again, so a ternary branch translates identically to the same condition
 * written directly, and the third (condition-UNKNOWN) arm is owned by
 * {@link TriPredicate#ternary}. The comparison form has to see the RAW operands — before
 * {@link NormalizedBinary} mirrors anything — which is why it is the first step of
 * {@link ComparisonTranslator#translate} rather than a resolved-operand case.
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
     * A comparison wrapping a CEL ternary — {@code cmp(if(c, a, b), other)}. Each branch is
     * substituted back into the comparison and recursed through
     * {@link PlanWalker#traverseExpression}, so a ternary branch behaves identically to the
     * same comparison written directly (see {@link #translateTernary} for the rewrite and its
     * null semantics). Recursion also handles nested ternaries and a ternary on the other side
     * for free.
     *
     * @return the rewritten predicate, or {@code null} if this comparison involves no ternary
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
     * Rewrite a CEL ternary {@code if(c, a, b)} into a pure predicate:
     *
     * <pre>{@code (pred(c) AND branch(a)) OR (NOT pred(c) AND branch(b)) OR NOT(pred(c) OR NOT pred(c))}</pre>
     *
     * where {@code branch} is supplied by the caller — comparison substitution for
     * {@link #tryTernaryComparison}, {@link #booleanBranchPredicate} for
     * {@link #handleBareTernary}. We rewrite instead of emitting {@code CASE WHEN}
     * ({@code cb.selectCase}) because this translator is predicate-only: every existing typed
     * leaf path — field-first normalization, size() handling, add-fold, fractional
     * double-space comparison — operates on comparison predicates, and routing the branches
     * back through those exact paths keeps them identical to the same condition written
     * directly.
     *
     * <p>A constant boolean condition folds to a single branch — only that branch is
     * translated, so an untranslatable dead branch cannot fail the whole plan.
     *
     * <p>Null semantics and the third (condition-UNKNOWN) arm are owned by
     * {@link TriPredicate#ternary}: a null/missing condition in a CEL ternary is an
     * evaluation error and the check denies, so the SQL must evaluate to UNKNOWN — never
     * FALSE — when the condition column is NULL. The condition is passed as a Supplier and
     * translated fresh for each arm (Hibernate 6 negation is stateful — see
     * {@link TriPredicate#not}).
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
                // A non-boolean literal condition is a CEL type error the planner
                // never emits; a column condition is translated below.
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

    /**
     * A CEL ternary in boolean position — {@code if(c, a, b)} used directly as a condition,
     * so both branches are themselves boolean and translate through
     * {@link #booleanBranchPredicate}. Same rewrite, rationale and null semantics as
     * {@link #translateTernary}.
     */
    Predicate handleBareTernary(List<Operand> operands, Scope scope) {
        return translateTernary(operands, branch -> booleanBranchPredicate(branch, scope), scope);
    }

    /**
     * A ternary branch in boolean position: a boolean VALUE folds to the always-true /
     * always-false predicate (the same collapse the unsolvable add-solve cases use); anything
     * else translates as a normal boolean operand (bare variables become {@code path = true}).
     */
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

    /** Rebuild {@code op(operands...)} with the operand at {@code idx} replaced. */
    private static PlanResourcesFilter.Expression substituteOperand(
            String op, List<Operand> operands, int idx, Operand replacement) {
        PlanResourcesFilter.Expression.Builder b =
                PlanResourcesFilter.Expression.newBuilder().setOperator(op);
        for (int i = 0; i < operands.size(); i++) {
            b.addOperands(i == idx ? replacement : operands.get(i));
        }
        return b.build();
    }

    /** The operand's boolean constant, or {@code null} if it is not a boolean VALUE. */
    private static Boolean constantBooleanOrNull(Operand o) {
        return PlanValues.protoValueToJava(o.getValue()) instanceof Boolean b ? b : null;
    }
}
