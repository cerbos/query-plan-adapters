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
 * Translates collection shapes: {@code exists}/{@code all} over a nested path or a literal list,
 * and {@code hasIntersection}, flat or over a {@code map()} projection.
 *
 * <p>Elasticsearch does not index an empty array, so a missing collection and an empty one look
 * the same. Shapes that depend on the difference are refused.
 */
final class CollectionTranslator {

    /** Operators whose second operand is a lambda binding an iteration variable. */
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

    /** A {@code lambda(body, variable)} operand. */
    private record Lambda(Operand body, String variable) {

        static Lambda of(Expression lambda, String arityMessage) {
            List<Operand> operands = lambda.getOperandsList();
            if (operands.size() != 2) {
                throw malformed(arityMessage);
            }
            if (operands.get(1).getNodeCase() != Operand.NodeCase.VARIABLE) {
                throw malformed("lambda second operand must be a variable");
            }
            return new Lambda(operands.get(0), operands.get(1).getVariable());
        }
    }

    Map<String, Object> translateMacro(String operator, List<Operand> operands, Polarity polarity) {
        boolean whenTrue = polarity.holds();
        if (operands.size() != 2) {
            throw malformed(operator + " requires exactly 2 operands, got " + operands.size());
        }

        Operand listOperand = operands.get(0);
        Operand lambdaOperand = operands.get(1);

        // The planner unrolls a macro over a literal list of up to 10 elements itself; a longer
        // list arrives as the collection operand, so fold it here the same way.
        if (listOperand.getNodeCase() == Operand.NodeCase.VALUE) {
            return handleKnownValueCollection(
                    operator, listOperand.getValue(), lambdaOperand, polarity);
        }

        if (listOperand.getNodeCase() != Operand.NodeCase.VARIABLE) {
            throw unsupported(operator + " over a computed collection cannot be lowered to a nested query");
        }

        String cerbosAttr = listOperand.getVariable();
        String esField = root.field(cerbosAttr);

        if (!options.nestedPaths().contains(esField)) {
            if (options.collectionFields().contains(esField)) {
                Map<String, Object> equality = flatScalarExistsEquality(
                        operator, lambdaOperand, esField, polarity);
                if (equality != null) return equality;
                throw unsupported("Collection macros over flat scalar arrays cannot preserve per-element predicates without a nested mapping");
            }
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
        Lambda lambda = Lambda.of(lambdaExpr, "lambda requires exactly 2 operands");

        if (whenTrue && "all".equals(operator)) {
            throw unsupported(
                    "all cannot distinguish a missing collection from an empty collection in Elasticsearch");
        }
        if (!whenTrue && "exists".equals(operator)) {
            throw unsupported(
                    "Negated exists cannot distinguish a missing collection from an empty collection in Elasticsearch");
        }

        // Only positive exists and negated all reach here. Either way a document matches when
        // one nested element satisfies the body under this polarity.
        Scope scope = new Scope.Lambda(esField, lambda.variable());
        return Queries.nestedQuery(esField, walker.operand(lambda.body(), scope, polarity));
    }

    /**
     * {@code exists(x, x == v)} over a flat array is a single term match. Returns {@code null}
     * for any other shape.
     */
    private Map<String, Object> flatScalarExistsEquality(
            String operator, Operand lambdaOperand, String field, Polarity polarity) {
        if (!"exists".equals(operator) || !polarity.holds()
                || lambdaOperand.getNodeCase() != Operand.NodeCase.EXPRESSION) return null;
        Expression lambda = lambdaOperand.getExpression();
        if (!"lambda".equals(lambda.getOperator()) || lambda.getOperandsCount() != 2
                || lambda.getOperands(1).getNodeCase() != Operand.NodeCase.VARIABLE
                || lambda.getOperands(0).getNodeCase() != Operand.NodeCase.EXPRESSION) return null;
        Expression body = lambda.getOperands(0).getExpression();
        if (!"eq".equals(body.getOperator()) || body.getOperandsCount() != 2) return null;
        String variable = lambda.getOperands(1).getVariable();
        Operand left = body.getOperands(0);
        Operand right = body.getOperands(1);
        boolean variableFirst = left.getNodeCase() == Operand.NodeCase.VARIABLE
                && left.getVariable().equals(variable)
                && right.getNodeCase() == Operand.NodeCase.VALUE;
        boolean valueFirst = right.getNodeCase() == Operand.NodeCase.VARIABLE
                && right.getVariable().equals(variable)
                && left.getNodeCase() == Operand.NodeCase.VALUE;
        if (!variableFirst && !valueFirst) return null;
        return leaf.applyResolvedLeaf("eq", body.getOperandsList(),
                new Scope.Root(Map.of(variable, field)), Polarity.TRUE);
    }

    /**
     * Folds {@code exists}/{@code all} over a literal list into an {@code or}/{@code and} of the
     * body with each element substituted, then walks the result. A literal list is never missing,
     * so this is exact under negation too. Over {@code []}, {@code exists} is false and
     * {@code all} is true.
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
        Lambda lambda = Lambda.of(lambdaOperand.getExpression(),
                operator + " over a literal collection supports single-variable lambdas only");

        List<Value> elements = collectionValue.getListValue().getValuesList();
        if (elements.isEmpty()) {
            boolean holds = "all".equals(operator);
            return holds == whenTrue ? Queries.matchAll() : Queries.matchNone();
        }

        Expression.Builder combined = Expression.newBuilder()
                .setOperator("exists".equals(operator) ? "or" : "and");
        for (Value element : elements) {
            combined.addOperands(substituteLambdaVariable(lambda.body(), lambda.variable(), element));
        }
        Expression folded = combined.build();
        return walker.expression(folded, root, polarity);
    }

    /**
     * Refuses a macro over a literal list that cannot be folded. Without this, {@code filter} and
     * {@code map} would reach the leaf translator and fail with an unrelated message.
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
     * Replaces the lambda variable with {@code element} in a lambda body. {@code v.a.b} reads a
     * field of the element and throws if it is missing. A nested lambda that rebinds the same
     * name shadows it, so only that macro's collection operand is substituted.
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

    /**
     * {@code hasIntersection}. The negated form is refused: {@code bool.must_not} would match a
     * document whose collection is missing, which CEL errors on.
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

        // hasIntersection is symmetric, so the map() projection may be on either side.
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

        Lambda lambda = Lambda.of(lambdaOperand.getExpression(), "lambda requires exactly 2 operands");
        if (lambda.body().getNodeCase() != Operand.NodeCase.VARIABLE) {
            throw unsupported("map lambda body must be a simple variable projection");
        }
        String nestedField = new Scope.Lambda(esField, lambda.variable())
                .field(lambda.body().getVariable());

        if (valuesOperand.getNodeCase() != Operand.NodeCase.VALUE) {
            throw unsupported("hasIntersection second operand must be a value list");
        }

        Object values = PlanValues.protoValueToJava(valuesOperand.getValue());
        List<?> valueList = values instanceof List<?> l ? l : List.of(values);
        LeafTranslator.rejectNullIntersection(valueList);
        LeafTranslator.rejectNonScalarElements("hasIntersection", valueList);
        // The default `terms` query is used here, not an override, so always filter by type.
        List<?> members = leaf.typedIntersection(nestedField, valueList, List.of());
        if (members.isEmpty()) {
            return Queries.matchNone();
        }

        Map<String, Object> matchingValue =
                Queries.nestedQuery(esField, Queries.terms(nestedField, members));
        Map<String, Object> missingProjection = Queries.nestedQuery(esField, Queries.notExists(nestedField));
        return Queries.boolMust(List.of(matchingValue, Queries.notQuery(missingProjection)));
    }
}
