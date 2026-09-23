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
 * Walks a plan's expression tree. Lowers {@code and}/{@code or}/{@code not} here and hands every
 * other operator to its translator. Collection, hierarchy and {@code size()} operators are only
 * handled at the top level; inside a lambda body the leaf translator refuses them.
 *
 * <p>One instance per translation.
 */
final class PlanWalker {

    private final Scope root;
    private final LeafTranslator leaf;
    private final HierarchyTranslator hierarchy;
    private final SizeTranslator sizes;
    private final CollectionTranslator collections;

    PlanWalker(Options options) {
        this.root = new Scope.Root(options.fieldMap());
        this.leaf = new LeafTranslator(options);
        this.hierarchy = new HierarchyTranslator(root, options.scalarTypes());
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
