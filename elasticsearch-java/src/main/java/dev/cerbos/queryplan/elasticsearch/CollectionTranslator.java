/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import static dev.cerbos.queryplan.elasticsearch.Refusals.malformed;
import static dev.cerbos.queryplan.elasticsearch.Refusals.unmapped;
import static dev.cerbos.queryplan.elasticsearch.Refusals.unsupported;

import com.google.protobuf.Value;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Options;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The collection shapes: {@code exists} / {@code all} over a nested path, the fold of either over
 * a literal value list (with the lambda-variable substitution it needs), and {@code
 * hasIntersection} in both its flat and its {@code map}-projected forms.
 *
 * <p>These are the shapes that re-enter the walk — a lambda body is walked under a
 * {@link Scope.Lambda}, a folded value list is walked at the top level — which is why this class
 * holds the {@link PlanWalker} rather than the other way round, and why the missing-versus-empty
 * refusals that make Elasticsearch's unindexed empty array visible all sit here.
 */
final class CollectionTranslator {

    /**
     * Operators whose second operand is a lambda that binds an iteration variable.
     *
     * <p>{@code except} is deliberately absent: Cerbos {@code except(list, list)} is a two-list
     * function returning a list difference, and no lambda form of it exists on the wire. It is
     * refused by name wherever it can appear ({@link Refusals#exceptUnsupported()}).
     */
    private static final Set<String> LAMBDA_BINDING_OPERATORS =
            Set.of("exists", "exists_one", "all", "filter", "map");

    private final Options options;
    private final Scope root;
    private final PlanWalker walker;
    private final LeafTranslator leaf;

    CollectionTranslator(Options options, Scope root, PlanWalker walker, LeafTranslator leaf) {
        this.options = options;
        this.root = root;
        this.walker = walker;
        this.leaf = leaf;
    }

    // --- Collection operators (exists, all) ---

    Map<String, Object> translateMacro(String operator, List<Operand> operands, Polarity polarity) {
        boolean whenTrue = polarity.holds();
        if (operands.size() != 2) {
            throw malformed(operator + " requires exactly 2 operands, got " + operands.size());
        }

        Operand listOperand = operands.get(0);
        Operand lambdaOperand = operands.get(1);

        // A literal value-list collection arrives when the planner could not unroll a macro
        // over a known collection: at <= 10 elements it folds exists/all into an or/and chain
        // itself (cerbos/cerbos#2570, #2817; maxItems = 10 in the planner's struct matcher),
        // above that the lambda ships with the folded value list as its collection operand.
        // Apply the same fold here instead of demanding a nested mapping that cannot exist
        // for a literal.
        if (listOperand.getNodeCase() == Operand.NodeCase.VALUE) {
            return handleKnownValueCollection(
                    operator, listOperand.getValue(), lambdaOperand, polarity);
        }

        if (listOperand.getNodeCase() != Operand.NodeCase.VARIABLE) {
            throw malformed(
                    operator + " first operand must be a variable, got " + listOperand.getNodeCase());
        }

        String cerbosAttr = listOperand.getVariable();
        String esField = root.field(cerbosAttr);

        if (!options.nestedPaths().contains(esField)) {
            throw unmapped("Field '" + esField + "' is not declared in nestedPaths. "
                    + "Collection operators require nested mappings.");
        }

        if (lambdaOperand.getNodeCase() != Operand.NodeCase.EXPRESSION) {
            throw malformed(operator + " second operand must be a lambda expression");
        }

        Expression lambdaExpr = lambdaOperand.getExpression();
        if (!"lambda".equals(lambdaExpr.getOperator())) {
            throw malformed(
                    operator + " second operand must be a lambda, got " + lambdaExpr.getOperator());
        }

        List<Operand> lambdaOperands = lambdaExpr.getOperandsList();
        if (lambdaOperands.size() != 2) {
            throw malformed("lambda requires exactly 2 operands");
        }

        Operand bodyOperand = lambdaOperands.get(0);

        Operand lambdaVarOperand = lambdaOperands.get(1);
        if (lambdaVarOperand.getNodeCase() != Operand.NodeCase.VARIABLE) {
            throw malformed("lambda second operand must be a variable");
        }
        String lambdaVar = lambdaVarOperand.getVariable();
        Scope scope = Scope.lambda(esField, lambdaVar);

        if (whenTrue && "all".equals(operator)) {
            throw unsupported(
                    "all cannot distinguish a missing collection from an empty collection in Elasticsearch");
        }
        if (!whenTrue && "exists".equals(operator)) {
            throw unsupported(
                    "Negated exists cannot distinguish a missing collection from an empty collection in Elasticsearch");
        }

        if (whenTrue) {
            // Only exists reaches here positively.
            Map<String, Object> innerTrue = walker.operand(bodyOperand, scope, Polarity.TRUE);
            return Queries.nestedQuery(esField, innerTrue);
        }

        // Only all reaches here negatively: a document qualifies when it holds an element the
        // predicate is definitely false for. An element for which the lambda is undefined
        // prevents both true and false, preserving CEL errors.
        Map<String, Object> innerFalse = walker.operand(bodyOperand, scope, Polarity.FALSE);
        return Queries.nestedQuery(esField, innerFalse);
    }

    /**
     * Fold a collection macro whose collection operand is a literal value list: substitute each
     * element into the lambda body and combine the per-element expressions with {@code or}
     * ({@code exists}) or {@code and} ({@code all}), then translate the combined expression
     * through the normal traversal — the same fold the planner itself applies to known
     * collections of 10 or fewer elements, so the emitted query does not depend on which side
     * of that threshold the collection lands.
     *
     * <p>Unlike a nested-field collection, a literal list is fully known at plan time: there is
     * no missing-versus-empty ambiguity, so the fold is exact under negation too and none of
     * the nested-query restrictions on {@code all} or negated {@code exists} apply. The empty
     * collection keeps CEL identity semantics: {@code exists} over {@code []} is false,
     * {@code all} over {@code []} is true.
     */
    private Map<String, Object> handleKnownValueCollection(
            String operator,
            Value collectionValue,
            Operand lambdaOperand,
            Polarity polarity) {
        boolean whenTrue = polarity.holds();
        if (!"exists".equals(operator) && !"all".equals(operator)) {
            throw unfoldableValueListMacro(operator);
        }
        if (collectionValue.getKindCase() != Value.KindCase.LIST_VALUE) {
            throw malformed(operator + " over a literal collection requires a list value");
        }

        if (lambdaOperand.getNodeCase() != Operand.NodeCase.EXPRESSION
                || !"lambda".equals(lambdaOperand.getExpression().getOperator())) {
            throw malformed(operator + " second operand must be a lambda expression");
        }
        List<Operand> lambdaOperands = lambdaOperand.getExpression().getOperandsList();
        if (lambdaOperands.size() != 2) {
            throw malformed(operator
                    + " over a literal collection supports single-variable lambdas only");
        }
        Operand bodyOperand = lambdaOperands.get(0);
        Operand lambdaVarOperand = lambdaOperands.get(1);
        if (lambdaVarOperand.getNodeCase() != Operand.NodeCase.VARIABLE) {
            throw malformed("lambda second operand must be a variable");
        }
        String lambdaVar = lambdaVarOperand.getVariable();

        List<Value> elements = collectionValue.getListValue().getValuesList();
        if (elements.isEmpty()) {
            boolean holds = "all".equals(operator);
            return holds == whenTrue ? Queries.matchAll() : Queries.matchNone();
        }

        Expression.Builder combined = Expression.newBuilder()
                .setOperator("exists".equals(operator) ? "or" : "and");
        for (Value element : elements) {
            combined.addOperands(substituteLambdaVariable(bodyOperand, lambdaVar, element));
        }
        Expression folded = combined.build();
        return walker.expression(folded, root, polarity);
    }

    /**
     * Fail closed, by name, for a collection macro over a literal value list that has no flat
     * translation. {@code filter} and {@code map} reach the leaf traversal rather than
     * {@link #translateMacro}, so without this they would surface an unrelated operand-shape
     * error instead of naming the real limitation.
     */
    static void rejectUnfoldableValueListMacro(String operator, List<Operand> operands) {
        if (LAMBDA_BINDING_OPERATORS.contains(operator)
                && operands.size() == 2
                && operands.get(0).getNodeCase() == Operand.NodeCase.VALUE) {
            throw unfoldableValueListMacro(operator);
        }
    }

    private static UnsupportedPlanShapeException unfoldableValueListMacro(String operator) {
        return unsupported(operator + " over a literal collection value is not supported. "
                + "Only exists() and all() can be folded into a flat query.");
    }

    /**
     * Substitute a lambda iteration variable with a concrete collection element inside a lambda
     * body. A bare reference to the variable becomes the element itself; a
     * {@code variable.path.to.field} reference drills into the element (failing closed when the
     * path is missing — the CEL evaluation of that element would error). A nested macro whose
     * lambda rebinds the same variable name shadows the outer variable, so substitution only
     * descends into its collection operand.
     */
    private static Operand substituteLambdaVariable(
            Operand operand, String varName, Value element) {
        switch (operand.getNodeCase()) {
            case VARIABLE -> {
                String name = operand.getVariable();
                if (name.equals(varName)) {
                    return Operand.newBuilder().setValue(element).build();
                }
                if (name.startsWith(varName + ".")) {
                    return Operand.newBuilder()
                            .setValue(resolveElementPath(
                                    name, name.substring(varName.length() + 1), element))
                            .build();
                }
                return operand;
            }
            case EXPRESSION -> {
                Expression expr = operand.getExpression();
                List<Operand> ops = expr.getOperandsList();
                Expression.Builder rebuilt = expr.toBuilder();
                if (LAMBDA_BINDING_OPERATORS.contains(expr.getOperator()) && ops.size() == 2
                        && shadowsVariable(ops.get(1), varName)) {
                    // The nested lambda rebinds our variable: substitute only in the
                    // collection operand.
                    rebuilt.setOperands(0, substituteLambdaVariable(ops.get(0), varName, element));
                    return Operand.newBuilder().setExpression(rebuilt).build();
                }
                for (int i = 0; i < ops.size(); i++) {
                    rebuilt.setOperands(i, substituteLambdaVariable(ops.get(i), varName, element));
                }
                return Operand.newBuilder().setExpression(rebuilt).build();
            }
            default -> {
                return operand;
            }
        }
    }

    /** True when {@code lambdaOperand} is a lambda whose iteration variable is {@code varName}. */
    private static boolean shadowsVariable(Operand lambdaOperand, String varName) {
        if (lambdaOperand.getNodeCase() != Operand.NodeCase.EXPRESSION
                || !"lambda".equals(lambdaOperand.getExpression().getOperator())) {
            return false;
        }
        List<Operand> ops = lambdaOperand.getExpression().getOperandsList();
        return ops.size() == 2
                && ops.get(1).getNodeCase() == Operand.NodeCase.VARIABLE
                && varName.equals(ops.get(1).getVariable());
    }

    /** Drill a dotted path into a struct element, failing closed on a missing field. */
    private static Value resolveElementPath(String fullRef, String path, Value element) {
        Value current = element;
        for (String segment : path.split("\\.")) {
            if (current.getKindCase() != Value.KindCase.STRUCT_VALUE
                    || !current.getStructValue().containsFields(segment)) {
                throw unsupported("Cannot resolve \"" + fullRef
                        + "\": collection element has no field \"" + segment + "\"");
            }
            current = current.getStructValue().getFieldsOrThrow(segment);
        }
        return current;
    }

    // --- hasIntersection (flat + nested/map) ---

    /**
     * {@code hasIntersection} in the TRUE direction. The false direction throws before any
     * operand is examined: a {@code bool.must_not} over the {@code terms} query it lowers to
     * would match a document whose collection is missing, which CEL errors on.
     */
    Map<String, Object> translateHasIntersection(List<Operand> operands, Polarity polarity) {
        if (!polarity.holds()) {
            throw unsupported(
                    "Negated hasIntersection cannot distinguish a missing collection from an empty collection in Elasticsearch");
        }
        if (operands.size() != 2) {
            throw malformed("hasIntersection requires exactly 2 operands");
        }

        Operand first = operands.get(0);
        Operand second = operands.get(1);

        // hasIntersection is symmetric, so the map() projection may sit on either side. The
        // operands are mirrored so both spellings take the one nested lowering.
        if (isMapProjection(first)) {
            return handleMapHasIntersection(first.getExpression(), second);
        }
        if (isMapProjection(second)) {
            return handleMapHasIntersection(second.getExpression(), first);
        }

        return leaf.applyResolvedLeaf("hasIntersection", operands, root, Polarity.TRUE);
    }

    private static boolean isMapProjection(Operand operand) {
        return operand.getNodeCase() == Operand.NodeCase.EXPRESSION
                && "map".equals(operand.getExpression().getOperator());
    }

    private Map<String, Object> handleMapHasIntersection(
            Expression mapExpr, Operand valuesOperand) {
        List<Operand> mapOperands = mapExpr.getOperandsList();
        if (mapOperands.size() != 2) {
            throw malformed("map requires exactly 2 operands");
        }

        Operand listOperand = mapOperands.get(0);
        if (listOperand.getNodeCase() != Operand.NodeCase.VARIABLE) {
            throw malformed("map first operand must be a variable");
        }

        String cerbosAttr = listOperand.getVariable();
        String esField = root.field(cerbosAttr);

        if (!options.nestedPaths().contains(esField)) {
            throw unmapped("Field '" + esField + "' is not declared in nestedPaths. "
                    + "map+hasIntersection requires nested mappings.");
        }

        Operand lambdaOperand = mapOperands.get(1);
        if (lambdaOperand.getNodeCase() != Operand.NodeCase.EXPRESSION
                || !"lambda".equals(lambdaOperand.getExpression().getOperator())) {
            throw malformed("map second operand must be a lambda");
        }

        Expression lambdaExpr = lambdaOperand.getExpression();
        List<Operand> lambdaOperands = lambdaExpr.getOperandsList();
        if (lambdaOperands.size() != 2) {
            throw malformed("lambda requires exactly 2 operands");
        }

        Operand projectionOperand = lambdaOperands.get(0);
        Operand lambdaVarOperand = lambdaOperands.get(1);
        if (lambdaVarOperand.getNodeCase() != Operand.NodeCase.VARIABLE) {
            throw malformed("lambda second operand must be a variable");
        }
        String lambdaVar = lambdaVarOperand.getVariable();

        if (projectionOperand.getNodeCase() != Operand.NodeCase.VARIABLE) {
            throw unsupported("map lambda body must be a simple variable projection");
        }

        String projectionVar = projectionOperand.getVariable();
        String suffix = Scope.extractLambdaSuffix(projectionVar, lambdaVar);
        String nestedField = esField + "." + suffix;

        if (valuesOperand.getNodeCase() != Operand.NodeCase.VALUE) {
            throw unsupported("hasIntersection second operand must be a value list");
        }

        Object values = PlanValues.protoValueToJava(valuesOperand.getValue());
        List<?> valueList = values instanceof List<?> l ? l : List.of(values);
        LeafTranslator.rejectNullIntersection(valueList);
        LeafTranslator.rejectNonScalarElements("hasIntersection", valueList);

        Map<String, Object> matchingValue = Queries.nestedQuery(
                esField, Map.of("terms", Map.of(nestedField, valueList)));
        Map<String, Object> missingProjection = Queries.nestedQuery(esField, Queries.notExists(nestedField));
        return Queries.boolMust(List.of(matchingValue, Queries.notQuery(missingProjection)));
    }
}
