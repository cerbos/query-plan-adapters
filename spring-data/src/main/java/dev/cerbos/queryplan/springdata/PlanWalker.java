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

import com.google.protobuf.Value;

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
            case "in" -> {
                Operand rewritten = membershipInMappedLiteral(operands);
                yield rewritten != null
                        ? traverse(rewritten, scope)
                        : membership.handleIn(operands, scope);
            }
            case "if" -> ternary.handleBareTernary(operands, scope);
            case "overlaps" -> hierarchy.handleOverlaps(operands, scope);
            case "ancestorOf" -> hierarchy.handleAncestorDescendant(operands, scope, true);
            case "descendentOf" -> hierarchy.handleAncestorDescendant(operands, scope, false);
            case "eq", "ne" -> {
                Operand rewritten = wholeListEquality(op, operands, scope);
                yield rewritten != null
                        ? traverse(rewritten, scope)
                        : comparisons.translate(op, operands, scope);
            }
            default -> comparisons.translate(op, operands, scope);
        };
    }

    /** A lambda variable no plan names, for the element of a rewritten list equality. */
    private static final String LIST_ELEMENT = "__cerbos_list_element";

    /**
     * {@code coll == [v]} or {@code coll == []} over a relation, rewritten as
     * {@code size(coll) == 1 && coll.exists(e, e == v)} or {@code size(coll) == 0}, and
     * {@code !=} as its negation. CEL list equality is ordered, but a list of at most one
     * element has no order to compare, so the rewrite is exact. Returns {@code null} for any
     * other shape, including a longer list, which a JPA collection cannot order.
     */
    private static Operand wholeListEquality(String op, List<Operand> operands, Scope scope) {
        if (operands.size() != 2) {
            return null;
        }
        Operand variable = operands.get(0);
        Operand literal = operands.get(1);
        if (variable.getNodeCase() == Operand.NodeCase.VALUE) {
            variable = operands.get(1);
            literal = operands.get(0);
        }
        if (variable.getNodeCase() != Operand.NodeCase.VARIABLE
                || literal.getNodeCase() != Operand.NodeCase.VALUE
                || literal.getValue().getKindCase() != Value.KindCase.LIST_VALUE
                || literal.getValue().getListValue().getValuesCount() > 1) {
            return null;
        }
        try {
            if (!(scope.resolve(variable.getVariable()) instanceof Scope.ResolvedRelation)) {
                return null;
            }
        } catch (IllegalArgumentException unmapped) {
            // Left to the comparison, which reports the list constant.
            return null;
        }
        List<Value> elements = literal.getValue().getListValue().getValuesList();
        Operand size = expression("size", variable);
        Operand equality = expression("eq", size, number(elements.size()));
        if (!elements.isEmpty()) {
            Operand element = Operand.newBuilder().setVariable(LIST_ELEMENT).build();
            Operand body = expression("eq", element,
                    Operand.newBuilder().setValue(elements.get(0)).build());
            equality = expression("and", equality,
                    expression("exists", variable, expression("lambda", body, element)));
        }
        return "ne".equals(op) ? expression("not", equality) : equality;
    }

    /**
     * {@code x in list.map(t, body)} over a literal list, rewritten as
     * {@code size(list.filter(t, x == body)) > 0}, whose strict count is UNKNOWN when any
     * element's comparison is. That is exact because CEL's {@code map} errors when any element's
     * body does, and {@code x == body} is UNKNOWN only where the body errors or {@code x} is a
     * missing attribute. Returns {@code null} for any other shape, and when {@code x} reads the
     * lambda variable's name, which the rewrite would capture.
     */
    private static Operand membershipInMappedLiteral(List<Operand> operands) {
        if (operands.size() != 2
                || operands.get(1).getNodeCase() != Operand.NodeCase.EXPRESSION) {
            return null;
        }
        PlanResourcesFilter.Expression map = operands.get(1).getExpression();
        if (!"map".equals(map.getOperator()) || map.getOperandsCount() != 2
                || map.getOperands(0).getNodeCase() != Operand.NodeCase.VALUE) {
            return null;
        }
        ParsedLambda lambda = ParsedLambda.parse(map.getOperands(1),
                "map second operand must be a lambda",
                "lambda requires exactly 2 operands",
                "lambda variable must be a variable operand");
        Operand needle = operands.get(0);
        if (readsName(needle, lambda.varName())) {
            return null;
        }
        Operand variable = Operand.newBuilder().setVariable(lambda.varName()).build();
        Operand filter = expression("filter", map.getOperands(0), expression("lambda",
                expression("eq", needle, lambda.body()), variable));
        return expression("gt", expression("size", filter), number(0));
    }

    private static boolean readsName(Operand operand, String name) {
        return switch (operand.getNodeCase()) {
            case VARIABLE -> operand.getVariable().equals(name)
                    || operand.getVariable().startsWith(name + ".");
            case EXPRESSION -> operand.getExpression().getOperandsList().stream()
                    .anyMatch(child -> readsName(child, name));
            default -> false;
        };
    }

    private static Operand expression(String operator, Operand... operands) {
        PlanResourcesFilter.Expression.Builder e =
                PlanResourcesFilter.Expression.newBuilder().setOperator(operator);
        for (Operand operand : operands) {
            e.addOperands(operand);
        }
        return Operand.newBuilder().setExpression(e).build();
    }

    private static Operand number(double value) {
        return Operand.newBuilder().setValue(Value.newBuilder().setNumberValue(value)).build();
    }
}
