/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import static dev.cerbos.queryplan.elasticsearch.Refusals.exceptUnsupported;
import static dev.cerbos.queryplan.elasticsearch.Refusals.malformed;
import static dev.cerbos.queryplan.elasticsearch.Refusals.ternaryUnsupported;
import static dev.cerbos.queryplan.elasticsearch.Refusals.unsupported;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Options;

import java.util.List;
import java.util.Map;

/**
 * The one walk over a plan's expression tree.
 *
 * <p>It used to exist four times — unscoped and lambda-scoped, each in a true and a false
 * polarity — and the four differed in exactly two ways: how a variable resolves to a field, which
 * {@link Scope} now answers, and which truth value is being proved, which {@link Polarity} now
 * carries. Everything else is dispatch: the boolean connectives are lowered here, and every other
 * operator is handed to the collaborator that owns it. Only the top-level scope reaches the
 * collection, hierarchy and {@code size()} shapes; inside a lambda body those operators fall
 * through to the leaf translator, which refuses them as computed operands, exactly as the scoped
 * traversal always did.
 *
 * <p>One instance per translation, built from the caller's {@link Options}; it holds no state
 * beyond its collaborators, so the public facade stays thread-safe.
 */
final class PlanWalker {

    private final Scope root;
    private final LeafTranslator leaf;
    private final HierarchyTranslator hierarchy;
    private final SizeTranslator sizes;
    private final CollectionTranslator collections;

    PlanWalker(Options options) {
        this.root = Scope.root(options.fieldMap());
        this.leaf = new LeafTranslator(options);
        this.hierarchy = new HierarchyTranslator(root);
        this.sizes = new SizeTranslator(options, root);
        this.collections = new CollectionTranslator(options, root, this, leaf);
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
            default -> throw malformed("Unexpected operand type: " + operand.getNodeCase());
        };
    }

    Map<String, Object> expression(Expression expression, Scope scope, Polarity polarity) {
        String operator = expression.getOperator();
        List<Operand> operands = expression.getOperandsList();

        return switch (operator) {
            // De Morgan: under FALSE, `and` becomes a `should` of the negated children and `or`
            // a `must` of them.
            case "and" -> {
                List<Map<String, Object>> clauses = operands.stream()
                        .map(o -> operand(o, scope, polarity))
                        .toList();
                yield polarity.holds() ? Queries.boolMust(clauses) : Queries.boolShould(clauses);
            }
            case "or" -> {
                List<Map<String, Object>> clauses = operands.stream()
                        .map(o -> operand(o, scope, polarity))
                        .toList();
                yield polarity.holds() ? Queries.boolShould(clauses) : Queries.boolMust(clauses);
            }
            case "not" -> {
                requireUnary("not", operands);
                yield operand(operands.get(0), scope, polarity.negate());
            }
            case "except" -> throw exceptUnsupported();
            case "if" -> throw ternaryUnsupported();
            default -> scope instanceof Scope.Root
                    ? rootExpression(operator, operands, polarity)
                    : leaf.applyResolvedLeaf(operator, operands, scope, polarity);
        };
    }

    /** The operators only the top-level scope dispatches; inside a lambda they are leaves. */
    private Map<String, Object> rootExpression(
            String operator, List<Operand> operands, Polarity polarity) {
        return switch (operator) {
            case "exists", "all" -> collections.translateMacro(operator, operands, polarity);
            case "exists_one" -> {
                CollectionTranslator.rejectUnfoldableValueListMacro(operator, operands);
                throw existsOneUnsupported();
            }
            case "hasIntersection" -> collections.translateHasIntersection(operands, polarity);
            case "ancestorOf", "descendentOf", "overlaps" ->
                    hierarchy.translate(operator, operands, polarity);
            default -> {
                CollectionTranslator.rejectUnfoldableValueListMacro(operator, operands);
                Map<String, Object> sizeResult = sizes.tryTranslate(operator, operands, polarity);
                if (sizeResult != null) {
                    yield sizeResult;
                }
                yield leaf.applyResolvedLeaf(operator, operands, root, polarity);
            }
        };
    }

    private static UnsupportedPlanShapeException existsOneUnsupported() {
        return unsupported(
                "exists_one cannot be expressed by Elasticsearch nested queries without scripts");
    }

    private static void requireUnary(String operator, List<Operand> operands) {
        if (operands.size() != 1) {
            throw malformed(operator + " requires exactly 1 operand, got " + operands.size());
        }
    }
}
