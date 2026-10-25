/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import static dev.cerbos.queryplan.elasticsearch.Refusals.exceptUnsupported;
import static dev.cerbos.queryplan.elasticsearch.Refusals.malformed;
import static dev.cerbos.queryplan.elasticsearch.Refusals.ternaryUnsupported;
import static dev.cerbos.queryplan.elasticsearch.Refusals.unsupported;

import com.google.protobuf.Value;
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
            case "has" -> presence(operands, scope, polarity);
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
