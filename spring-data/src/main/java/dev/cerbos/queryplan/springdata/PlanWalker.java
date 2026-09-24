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
    private final RegexTranslator regex;
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
        this.regex = new RegexTranslator(cb, tri);
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
            case "matches" -> {
                if (leaf.overridden("matches") || operands.size() != 2
                        || operands.get(0).getNodeCase() != Operand.NodeCase.VARIABLE
                        || operands.get(1).getNodeCase() != Operand.NodeCase.VALUE
                        || operands.get(1).getValue().getKindCase()
                                != Value.KindCase.STRING_VALUE) {
                    yield leafComparison(op, operands, scope);
                }
                yield regex.matches(scope.path(operands.get(0).getVariable()),
                        operands.get(1).getValue().getStringValue());
            }
            case "eq", "ne" -> {
                Operand predicate = booleanComparison(op, operands);
                if (predicate != null) {
                    yield traverse(predicate, scope);
                }
                Operand rewritten = wholeListEquality(op, operands, scope);
                yield rewritten != null
                        ? traverse(rewritten, scope) : leafComparison(op, operands, scope);
            }
            default -> leafComparison(op, operands, scope);
        };
    }

    /**
     * {@code matches(...) == true} and its spellings, as the predicate or its negation:
     * {@code == true} and {@code != false} keep it, {@code == false} and {@code != true} negate
     * it. A {@code matches()} error stays UNKNOWN under both. {@code null} for any other shape.
     */
    private static Operand booleanComparison(String op, List<Operand> operands) {
        if (operands.size() != 2) {
            return null;
        }
        Operand predicate = operands.get(0);
        Operand literal = operands.get(1);
        if (predicate.getNodeCase() == Operand.NodeCase.VALUE) {
            predicate = operands.get(1);
            literal = operands.get(0);
        }
        if (predicate.getNodeCase() != Operand.NodeCase.EXPRESSION
                || !"matches".equals(predicate.getExpression().getOperator())
                || literal.getNodeCase() != Operand.NodeCase.VALUE
                || literal.getValue().getKindCase() != Value.KindCase.BOOL_VALUE) {
            return null;
        }
        boolean keep = literal.getValue().getBoolValue() == "eq".equals(op);
        return keep ? predicate : expression("not", predicate);
    }

    /** A leaf comparison: a positional list read if an operand is one, else the comparison. */
    private Predicate leafComparison(String op, List<Operand> operands, Scope scope) {
        Predicate positional = collections.tryPositionalRead(op, operands, scope);
        return positional != null ? positional : comparisons.translate(op, operands, scope);
    }

    /** A lambda variable no plan names, for the element of a rewritten list equality. */
    private static final String LIST_ELEMENT = "__cerbos_list_element";

    /**
     * {@code coll == [v1, ..., vn]} with {@code coll} a relation, a {@code map()} projection of
     * one ({@code t} or {@code t.f}), or an {@code except()} of one, rewritten into operators
     * that translate, and {@code !=} as its negation. CEL list equality is ordered:
     * <ul>
     *   <li>at most one element has no order to compare, so it is
     *       {@code size(coll) == n && coll.exists(e, e == v)};</li>
     *   <li>more need the relation's declared position field, and are
     *       {@code size(coll) == n && coll[0] == v1 && ...}, each read in range once the size
     *       matches;</li>
     *   <li>an {@code except()} difference is compared only with at most one element, as
     *       {@code size(diff) == n} and, for one, a kept element equal to it.</li>
     * </ul>
     * Returns {@code null} for any other shape, which the comparison then refuses.
     */
    private static Operand wholeListEquality(String op, List<Operand> operands, Scope scope) {
        if (operands.size() != 2) {
            return null;
        }
        Operand collection = operands.get(0);
        Operand literal = operands.get(1);
        if (collection.getNodeCase() == Operand.NodeCase.VALUE) {
            collection = operands.get(1);
            literal = operands.get(0);
        }
        if (literal.getNodeCase() != Operand.NodeCase.VALUE
                || literal.getValue().getKindCase() != Value.KindCase.LIST_VALUE) {
            return null;
        }
        List<Value> elements = literal.getValue().getListValue().getValuesList();
        Operand equality = switch (collection.getNodeCase()) {
            case VARIABLE -> relationEquality(collection.getVariable(), null, elements, scope);
            case EXPRESSION -> {
                PlanResourcesFilter.Expression e = collection.getExpression();
                if ("map".equals(e.getOperator()) && e.getOperandsCount() == 2
                        && e.getOperands(0).getNodeCase() == Operand.NodeCase.VARIABLE) {
                    ParsedLambda projection = ParsedLambda.parse(e.getOperands(1),
                            "map second operand must be a lambda",
                            "lambda requires exactly 2 operands",
                            "lambda variable must be a variable operand");
                    yield relationEquality(e.getOperands(0).getVariable(), projection,
                            elements, scope);
                }
                if ("except".equals(e.getOperator()) && e.getOperandsCount() == 2
                        && e.getOperands(0).getNodeCase() == Operand.NodeCase.VARIABLE
                        && elements.size() <= 1
                        && isRelation(e.getOperands(0).getVariable(), scope)) {
                    yield differenceEquality(collection, e, elements);
                }
                yield null;
            }
            default -> null;
        };
        if (equality == null) {
            return null;
        }
        return "ne".equals(op) ? expression("not", equality) : equality;
    }

    /**
     * The equality of {@code variable}, or of its {@code projection}, with {@code elements};
     * {@code null} when {@code variable} is no relation, the projection is not {@code t} or
     * {@code t.f}, or the list is longer than one and the relation declares no position.
     */
    private static Operand relationEquality(String variable, ParsedLambda projection,
                                            List<Value> elements, Scope scope) {
        if (!isRelation(variable, scope)) {
            return null;
        }
        String field = null;
        if (projection != null) {
            Operand body = projection.body();
            String name = projection.varName();
            if (body.getNodeCase() != Operand.NodeCase.VARIABLE) {
                return null;
            }
            String projected = body.getVariable();
            if (projected.startsWith(name + ".") && projected.indexOf('.', name.length() + 1) < 0) {
                field = projected.substring(name.length() + 1);
            } else if (!projected.equals(name)) {
                return null;
            }
        }
        Operand listVar = Operand.newBuilder().setVariable(variable).build();
        Operand equality = expression("eq", expression("size", listVar), number(elements.size()));
        if (elements.size() == 1) {
            Operand element = Operand.newBuilder().setVariable(
                    field == null ? LIST_ELEMENT : LIST_ELEMENT + "." + field).build();
            Operand body = expression("eq", element, constant(elements.get(0)));
            return expression("and", equality, expression("exists", listVar, expression("lambda",
                    body, Operand.newBuilder().setVariable(LIST_ELEMENT).build())));
        }
        if (elements.size() > 1) {
            Scope.Resolution resolved = scope.resolve(variable);
            if (!(resolved instanceof Scope.ResolvedRelation ref) || ref.isChained()
                    || ref.tail().positionField() == null) {
                return null;
            }
            PlanResourcesFilter.Expression.Builder all = PlanResourcesFilter.Expression
                    .newBuilder().setOperator("and").addOperands(equality);
            for (int k = 0; k < elements.size(); k++) {
                Operand read = expression("index", listVar, number(k));
                if (field != null) {
                    read = expression("get-field", read,
                            Operand.newBuilder().setVariable(field).build());
                }
                all.addOperands(expression("eq", read, constant(elements.get(k))));
            }
            return Operand.newBuilder().setExpression(all).build();
        }
        return equality;
    }

    /**
     * {@code rel.except(b) == []} or {@code == [v]}: the difference is empty, or holds one
     * element and it is {@code v}.
     */
    private static Operand differenceEquality(Operand difference,
                                              PlanResourcesFilter.Expression except,
                                              List<Value> elements) {
        Operand size = expression("eq", expression("size", difference), number(elements.size()));
        if (elements.isEmpty()) {
            return size;
        }
        ParsedLambda keep = SizeTranslator.exceptKeeps(except);
        Operand element = Operand.newBuilder().setVariable(keep.varName()).build();
        Operand kept = expression("and", keep.body(),
                expression("eq", element, constant(elements.get(0))));
        Operand matches = expression("filter", except.getOperands(0),
                expression("lambda", kept, element));
        return expression("and", size, expression("gt", expression("size", matches), number(0)));
    }

    private static boolean isRelation(String variable, Scope scope) {
        try {
            return scope.resolve(variable) instanceof Scope.ResolvedRelation;
        } catch (IllegalArgumentException unmapped) {
            // Left to the comparison, which reports the list constant.
            return false;
        }
    }

    private static Operand constant(Value value) {
        return Operand.newBuilder().setValue(value).build();
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
