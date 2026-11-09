/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import static dev.cerbos.queryplan.elasticsearch.Refusals.exceptUnsupported;
import static dev.cerbos.queryplan.elasticsearch.Refusals.malformed;
import static dev.cerbos.queryplan.elasticsearch.Refusals.unsupported;

import com.google.protobuf.Value;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Options;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Walks a plan's expression tree. Lowers {@code and}/{@code or}/{@code not}/{@code if} here and
 * hands every other operator to its translator. Hierarchy and {@code size()} operators are only
 * handled at the top level; inside a lambda body the leaf translator refuses them. A collection
 * macro is handled in any scope, so a nested path can hold a nested path.
 *
 * <p>One instance per translation.
 */
final class PlanWalker {

    /** Operators whose operands CEL evaluates lazily, so a ternary operand stays where it is. */
    private static final Set<String> LAZY_OPERATORS = Set.of("and", "or", "not", "if", "lambda");

    /**
     * Operators that evaluate to a boolean or an error, never to another type: compared with a
     * boolean literal, the comparison is the predicate itself or its negation.
     */
    private static final Set<String> PREDICATE_OPERATORS = Set.of(
            "eq", "ne", "lt", "le", "gt", "ge", "in", "contains", "startsWith", "endsWith",
            "matches", "and", "or", "not", "exists", "all", "exists_one", "hasIntersection",
            "isSubset", "has", "ancestorOf", "descendentOf", "overlaps");

    private final Scope.Root root;
    private final Options options;
    private final LeafTranslator leaf;
    private final HierarchyTranslator hierarchy;
    private final SizeTranslator sizes;
    private final CollectionTranslator collections;

    PlanWalker(Options options) {
        this.root = new Scope.Root(options.fieldMap());
        this.options = options;
        this.leaf = new LeafTranslator(options);
        this.hierarchy = new HierarchyTranslator(root, options.scalarTypes());
        this.collections = new CollectionTranslator(options, root, this, leaf);
        this.sizes = new SizeTranslator(options, root, collections);
    }

    /** The Query DSL clause for a conditional plan's condition. */
    Map<String, Object> translate(Operand condition) {
        return operand(condition, root, Polarity.TRUE);
    }

    Map<String, Object> operand(Operand operand, Scope scope, Polarity polarity) {
        return switch (operand.getNodeCase()) {
            case EXPRESSION -> expression(operand.getExpression(), scope, polarity);
            case VARIABLE -> {
                String field = scope.field(operand.getVariable());
                yield leaf.operatorOrDefault("eq").apply(field, polarity.holds());
            }
            // The planner folds a lambda body it can decide to a bare value: `t.name in []` arrives
            // as `false`. A boolean holds or fails for every document alike; any other value where
            // CEL needs a boolean is a no-overload error, which denies under both polarities.
            case VALUE -> operand.getValue().getKindCase() == Value.KindCase.BOOL_VALUE
                    && operand.getValue().getBoolValue() == polarity.holds()
                    ? Queries.matchAll()
                    : Queries.matchNone();
            default -> throw malformed("Unexpected operand type: " + operand.getNodeCase());
        };
    }

    Map<String, Object> expression(Expression expression, Scope scope, Polarity polarity) {
        String operator = expression.getOperator();
        List<Operand> operands = expression.getOperandsList();

        if (!LAZY_OPERATORS.contains(operator)) {
            for (int i = 0; i < operands.size(); i++) {
                if (isExpression(operands.get(i), "if")) {
                    return hoistedTernary(expression, i, scope, polarity);
                }
            }
            if (operands.stream().allMatch(PlanWalker::isLiteral)) {
                return Constants.comparison(operator, operands, polarity);
            }
        }
        return switch (operator) {
            // De Morgan: under FALSE, `and` becomes `should` and `or` becomes `must`.
            case "and", "or" -> {
                List<Map<String, Object>> clauses = operands.stream()
                        .map(o -> operand(o, scope, polarity))
                        .toList();
                boolean conjunction = "and".equals(operator) == polarity.holds();
                yield conjunction ? Queries.boolMust(clauses) : Queries.boolShould(clauses);
            }
            case "not" -> {
                requireUnary("not", operands);
                yield operand(operands.get(0), scope, polarity.negate());
            }
            case "except" -> throw exceptUnsupported();
            case "if" -> {
                if (operands.size() != 3) {
                    throw malformed("if requires exactly 3 operands, got " + operands.size());
                }
                yield ternary(operands.get(0), operands.get(1), operands.get(2), scope, polarity);
            }
            case "has" -> presence(operands, scope, polarity);
            case "exists", "all" -> collections.translateMacro(operator, operands, scope, polarity);
            default -> {
                Map<String, Object> predicate = predicateAgainstBoolean(operator, operands, scope, polarity);
                if (predicate != null) {
                    yield predicate;
                }
                yield scope instanceof Scope.Root
                        ? rootExpression(operator, operands, polarity)
                        : leaf.applyResolvedLeaf(operator, operands, scope, polarity);
            }
        };
    }

    /**
     * {@code c ? p : q}, proved under one polarity. CEL evaluates {@code c} first and errors when
     * it errors, so a document matches when {@code c} holds and {@code p} does under the polarity,
     * or {@code c} fails and {@code q} does; a document where {@code c} errors matches neither
     * side. Inside a nested lambda every operand must read the nested document: a nested query
     * sees no field of the enclosing document.
     */
    private Map<String, Object> ternary(Operand condition, Operand then, Operand otherwise,
                                        Scope scope, Polarity polarity) {
        if (scope instanceof Scope.Lambda lambda) {
            for (Operand operand : List.of(condition, then, otherwise)) {
                if (readsOutside(operand, Set.of(lambda.lambdaVariable()))) {
                    throw outsideNestedScope("if (CEL ternary)");
                }
            }
        }
        return Queries.boolShould(List.of(
                Queries.boolMust(List.of(
                        operand(condition, scope, Polarity.TRUE), operand(then, scope, polarity))),
                Queries.boolMust(List.of(
                        operand(condition, scope, Polarity.FALSE), operand(otherwise, scope, polarity)))));
    }

    /**
     * {@code op(..., c ? t : e, ...)} is {@code c ? op(..., t, ...) : op(..., e, ...)}: CEL
     * evaluates the ternary operand first, so the comparison runs on whichever branch it took.
     */
    private Map<String, Object> hoistedTernary(Expression expression, int at, Scope scope,
                                               Polarity polarity) {
        Expression ternary = expression.getOperands(at).getExpression();
        if (ternary.getOperandsCount() != 3) {
            throw malformed("if requires exactly 3 operands, got " + ternary.getOperandsCount());
        }
        Operand then = Operand.newBuilder()
                .setExpression(expression.toBuilder().setOperands(at, ternary.getOperands(1))).build();
        Operand otherwise = Operand.newBuilder()
                .setExpression(expression.toBuilder().setOperands(at, ternary.getOperands(2))).build();
        return ternary(ternary.getOperands(0), then, otherwise, scope, polarity);
    }

    /**
     * {@code p == true}, {@code p != false} and the other two spellings, where {@code p} is a
     * predicate: the predicate, or its negation. A predicate that errors is an error under both.
     */
    private Map<String, Object> predicateAgainstBoolean(String operator, List<Operand> operands,
                                                        Scope scope, Polarity polarity) {
        if (!("eq".equals(operator) || "ne".equals(operator)) || operands.size() != 2) {
            return null;
        }
        for (int side = 0; side < 2; side++) {
            Operand literal = operands.get(side);
            Operand predicate = operands.get(1 - side);
            if (literal.getNodeCase() == Operand.NodeCase.VALUE
                    && literal.getValue().getKindCase() == Value.KindCase.BOOL_VALUE
                    && predicate.getNodeCase() == Operand.NodeCase.EXPRESSION
                    && PREDICATE_OPERATORS.contains(predicate.getExpression().getOperator())) {
                boolean same = literal.getValue().getBoolValue() == "eq".equals(operator);
                return operand(predicate, scope, same ? polarity : polarity.negate());
            }
        }
        return null;
    }

    private static boolean isExpression(Operand operand, String operator) {
        return operand.getNodeCase() == Operand.NodeCase.EXPRESSION
                && operator.equals(operand.getExpression().getOperator());
    }

    /** A literal, or {@code timestamp()} of one. */
    private static boolean isLiteral(Operand operand) {
        return operand.getNodeCase() == Operand.NodeCase.VALUE
                || (isExpression(operand, "timestamp") && operand.getExpression().getOperandsCount() == 1
                        && operand.getExpression().getOperands(0).getNodeCase() == Operand.NodeCase.VALUE);
    }

    /**
     * Whether the operand names a variable that no lambda in scope binds: a field of the enclosing
     * document, which a nested query cannot read.
     */
    static boolean readsOutside(Operand operand, Set<String> bound) {
        switch (operand.getNodeCase()) {
            case VARIABLE -> {
                String variable = operand.getVariable();
                return bound.stream().noneMatch(b -> variable.equals(b) || variable.startsWith(b + "."));
            }
            case EXPRESSION -> {
                Expression expression = operand.getExpression();
                List<Operand> operands = expression.getOperandsList();
                if ("lambda".equals(expression.getOperator()) && operands.size() >= 2
                        && operands.get(1).getNodeCase() == Operand.NodeCase.VARIABLE) {
                    Set<String> inner = new HashSet<>(bound);
                    inner.add(operands.get(1).getVariable());
                    return readsOutside(operands.get(0), inner);
                }
                return operands.stream().anyMatch(child -> readsOutside(child, bound));
            }
            default -> {
                return false;
            }
        }
    }

    static UnsupportedPlanShapeException outsideNestedScope(String shape) {
        return unsupported(shape + " inside a collection macro reads a field of the enclosing"
                + " document: an Elasticsearch nested query is scoped to one nested document and"
                + " sees no field outside it");
    }

    /** The operators only the top-level scope dispatches; inside a lambda they are leaves. */
    private Map<String, Object> rootExpression(
            String operator, List<Operand> operands, Polarity polarity) {
        return switch (operator) {
            case "exists_one" -> collections.translateExistsOne(operands, polarity);
            case "hasIntersection" -> collections.translateHasIntersection(operands, polarity);
            case "ancestorOf", "descendentOf", "overlaps" ->
                    hierarchy.translate(operator, operands, polarity);
            default -> {
                if ("in".equals(operator) && CollectionTranslator.isMembershipInMapProjection(operands)) {
                    yield collections.translateMapProjectionMembership(operands, polarity);
                }
                CollectionTranslator.rejectUnfoldableValueListMacro(operator, operands);
                Map<String, Object> sizeResult = sizes.tryTranslate(operator, operands, polarity);
                if (sizeResult != null) {
                    yield sizeResult;
                }
                yield leaf.applyResolvedLeaf(operator, operands, root, polarity);
            }
        };
    }

    /**
     * {@code has(x)}, the presence test the planner emits from Cerbos 0.57.0 for an attribute it
     * does not fold into a guard: an {@code exists} query, or its negation.
     *
     * <p>Elasticsearch does not index a JSON {@code null}, so a field is present to the DSL
     * exactly when CEL holds a non-null value there. That is CEL's presence only when a NULL
     * column is omitted: an attribute declared in {@code explicitNullAttributes} is present
     * while null, and is refused. {@code has()} through a missing parent object is an error CEL
     * denies under both polarities, and the DSL cannot prove an object present when every field
     * under it is null, so a negated {@code has()} below another mapped field, and any
     * {@code has()} of a field other mapped fields sit below, is refused.
     */
    private Map<String, Object> presence(List<Operand> operands, Scope scope, Polarity polarity) {
        requireUnary("has", operands);
        if (operands.get(0).getNodeCase() != Operand.NodeCase.VARIABLE) {
            throw malformed("has requires an attribute reference");
        }
        String variable = operands.get(0).getVariable();
        if (options.explicitNullAttributes().contains(variable)) {
            throw unsupported("has(" + variable + ") over an attribute sent as an explicit null: "
                    + "Elasticsearch does not index null, so a present null and a missing field are "
                    + "the same document");
        }
        String field = scope.field(variable);
        if (scope instanceof Scope.Root) {
            for (String mapped : options.fieldMap().values()) {
                if (mapped.startsWith(field + ".")) {
                    throw unsupported("has(" + variable + ") tests the object " + field
                            + ": an object whose fields are all null is not found by exists");
                }
                if (!polarity.holds() && field.startsWith(mapped + ".")) {
                    throw unsupported("!has(" + variable + ") below the object " + mapped
                            + ": a missing parent is an error CEL denies, and the DSL cannot "
                            + "require an object whose fields are all null to be present");
                }
            }
        }
        return polarity.holds() ? Queries.exists(field) : Queries.notExists(field);
    }

    private static void requireUnary(String operator, List<Operand> operands) {
        if (operands.size() != 1) {
            throw malformed(operator + " requires exactly 1 operand, got " + operands.size());
        }
    }
}
