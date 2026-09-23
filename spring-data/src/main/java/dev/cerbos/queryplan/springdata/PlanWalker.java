/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Path;
import jakarta.persistence.criteria.Predicate;

import java.util.List;
import java.util.function.Supplier;

/**
 * Walks a plan's expression tree. It lowers {@code and}/{@code or}/{@code not} and bare
 * boolean variables itself and dispatches every other operator to its translator.
 *
 * <p>Polarity is not passed down the walk: {@code not} wraps the built predicate (see
 * {@link TriPredicate#not}). Build a new instance per Specification evaluation, because it
 * tracks macro nesting depth.
 */
final class PlanWalker {

    private final CriteriaBuilder cb;
    private final TriPredicate tri;
    private final LeafTranslator leaf;
    private final HierarchyTranslator hierarchy;
    private final TernaryTranslator ternary;
    private final ComparisonTranslator comparisons;
    private final MembershipTranslator membership;
    private final CollectionTranslator collections;
    private final int maxMacroDepth;
    /** Current collection-macro nesting depth; maintained by {@link #enterMacro}. */
    private int macroDepth;

    PlanWalker(CriteriaBuilder cb, SpringDataQueryPlanAdapter.Options options,
               boolean selectInvocation) {
        this.cb = cb;
        this.tri = new TriPredicate(cb);
        this.maxMacroDepth = options.effectiveMaxMacroDepth();
        this.leaf = new LeafTranslator(cb, tri, options.operatorOverrides());
        this.hierarchy = new HierarchyTranslator(cb, tri);
        ChainSubqueries subqueries = new ChainSubqueries(cb, tri, selectInvocation);
        this.ternary = new TernaryTranslator(cb, tri, this);
        this.comparisons = new ComparisonTranslator(cb, tri, leaf, ternary,
                new SizeTranslator(cb, tri, this, subqueries));
        this.membership = new MembershipTranslator(cb, tri, leaf, subqueries);
        this.collections = new CollectionTranslator(cb, this, subqueries);
    }

    /**
     * Runs {@code body} one macro level deeper, throwing when the depth exceeds the configured
     * maximum. Each level multiplies translation work and subqueries.
     */
    Predicate enterMacro(String op, Supplier<Predicate> body) {
        macroDepth++;
        try {
            if (macroDepth > maxMacroDepth) {
                throw Refusals.unsupported(
                        "Collection-macro nesting depth " + macroDepth + " exceeds the maximum of "
                        + maxMacroDepth + " (reached via operator '" + op + "'). "
                        + "Nested relation macros and literal folds "
                        + "multiply translation work and correlated subqueries, so deeply "
                        + "nested macros degrade query latency "
                        + "sharply. If the policy shape is intentional, raise the limit via "
                        + "the '" + SpringDataQueryPlanAdapter.MAX_MACRO_DEPTH_PROPERTY
                        + "' system property.");
            }
            return body.get();
        } finally {
            macroDepth--;
        }
    }

    Predicate traverse(Operand operand, Scope scope) {
        return switch (operand.getNodeCase()) {
            case EXPRESSION -> traverseExpression(operand.getExpression(), scope);
            case VARIABLE -> handleBareVariable(operand.getVariable(), scope);
            default -> throw Refusals.malformed("Unexpected operand type: " + operand.getNodeCase());
        };
    }

    private Predicate handleBareVariable(String variable, Scope scope) {
        Path<?> path = scope.path(variable);
        return leaf.applyLeaf("eq", path, true);
    }

    Predicate traverseExpression(PlanResourcesFilter.Expression expression, Scope scope) {
        String op = expression.getOperator();
        List<Operand> operands = expression.getOperandsList();

        return switch (op) {
            case "and" -> cb.and(operands.stream()
                    .map(o -> traverse(o, scope)).toArray(Predicate[]::new));
            case "or" -> cb.or(operands.stream()
                    .map(o -> traverse(o, scope)).toArray(Predicate[]::new));
            case "not" -> {
                if (operands.size() != 1) {
                    throw Refusals.malformed("not requires exactly 1 operand");
                }
                yield tri.not(traverse(operands.get(0), scope));
            }
            case "exists", "exists_one", "all" ->
                    collections.handleCollectionOperator(op, operands, scope);
            // filter() is a list, not a boolean; size(filter(...)) is handled before this.
            case "filter" -> throw Refusals.unsupported(
                    "filter() returns a list, not a boolean, so it cannot be a condition on "
                            + "its own; only size(filter(...)) has a boolean meaning");
            // List difference has no JPA Criteria translation.
            case "except" -> throw Refusals.exceptUnsupported();
            // has_intersection is a deprecated alias the PDP still accepts.
            case "hasIntersection", "has_intersection" ->
                    membership.handleHasIntersection(operands, scope);
            case "in" -> membership.handleIn(operands, scope);
            case "if" -> ternary.handleBareTernary(operands, scope);
            case "overlaps" -> hierarchy.handleOverlaps(operands, scope);
            case "ancestorOf" -> hierarchy.handleAncestorDescendant(operands, scope, true);
            case "descendentOf" -> hierarchy.handleAncestorDescendant(operands, scope, false);
            default -> comparisons.translate(op, operands, scope);
        };
    }
}
