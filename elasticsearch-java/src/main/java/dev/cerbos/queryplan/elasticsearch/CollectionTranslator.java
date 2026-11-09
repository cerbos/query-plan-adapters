/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import static dev.cerbos.queryplan.elasticsearch.Refusals.malformed;
import static dev.cerbos.queryplan.elasticsearch.Refusals.unmapped;
import static dev.cerbos.queryplan.elasticsearch.Refusals.unsupported;

import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Options;

import java.util.ArrayList;
import java.util.LinkedHashMap;
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

    Map<String, Object> translateMacro(String operator, List<Operand> operands, Scope scope,
                                       Polarity polarity) {
        boolean whenTrue = polarity.holds();
        if (operands.size() != 2) {
            throw malformed(operator + " requires exactly 2 operands, got " + operands.size());
        }

        Operand listOperand = operands.get(0);
        Operand lambdaOperand = operands.get(1);

        if (scope instanceof Scope.Lambda enclosing
                && operands.stream().anyMatch(o -> PlanWalker.readsOutside(o, Set.of(enclosing.lambdaVariable())))) {
            throw PlanWalker.outsideNestedScope(operator + "()");
        }
        if (lambdaOperand.getNodeCase() == Operand.NodeCase.EXPRESSION
                && "lambda".equals(lambdaOperand.getExpression().getOperator())
                && lambdaOperand.getExpression().getOperandsCount() == 3) {
            throw unsupported(operator + " with two variables binds each element's index, or each"
                    + " map key, beside its value: Elasticsearch indexes an array as an unordered"
                    + " set of terms and holds no term for an object's keys, so no Query DSL query"
                    + " reads either");
        }

        if (listOperand.getNodeCase() == Operand.NodeCase.EXPRESSION
                && "list".equals(listOperand.getExpression().getOperator())) {
            return handleListExpression(operator, listOperand.getExpression(), lambdaOperand, scope, polarity);
        }

        // The planner unrolls a macro over a literal list of up to 10 elements itself; a longer
        // list arrives as the collection operand, so fold it here the same way.
        if (listOperand.getNodeCase() == Operand.NodeCase.VALUE) {
            return handleKnownValueCollection(
                    operator, listOperand.getValue(), lambdaOperand, scope, polarity);
        }

        if (listOperand.getNodeCase() != Operand.NodeCase.VARIABLE) {
            throw unsupported(operator + " over a computed collection cannot be lowered to a nested query");
        }

        String cerbosAttr = listOperand.getVariable();
        String esField = scope.field(cerbosAttr);

        if (!options.nestedPaths().contains(esField)) {
            if (options.collectionFields().contains(esField)) {
                Map<String, Object> predicate = flatScalarExists(
                        operator, lambdaOperand, esField, polarity);
                if (predicate != null) return predicate;
                throw unsupported("Collection macros over flat scalar arrays cannot preserve per-element predicates without a nested mapping");
            }
            if (isObjectPath(esField)) {
                // CEL ranges a macro over a map's keys. The field is an object whose sub-fields
                // the field map names, and a query matches values, never the key set of an
                // object, so no query answers the macro.
                throw unsupported(operator + " over the object field '" + esField
                        + "' ranges over its keys, as CEL iterates a map, and a Query DSL query"
                        + " cannot enumerate the keys an object holds");
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
        Scope element = new Scope.Lambda(esField, lambda.variable());
        return Queries.nestedQuery(esField, walker.operand(lambda.body(), element, polarity));
    }

    /** Positive predicates of one element against a literal that a flat array answers as a whole. */
    private static final Set<String> FLAT_ELEMENT_OPERATORS = Set.of(
            "eq", "in", "lt", "le", "gt", "ge", "contains", "startsWith", "endsWith", "matches");

    /**
     * {@code exists(x, x op v)} over a flat array: a query on the array field matches a document
     * holding one element that satisfies it, which is what {@code exists} asks. Only a positive
     * predicate of the element against a literal is exact: {@code exists(x, x != v)} needs an
     * element the term does not match, which a {@code must_not} over the whole array cannot find
     * in an array that also holds {@code v}. A null element, which Elasticsearch does not index,
     * errors in CEL only where no other element holds, and that denies too. Returns {@code null}
     * for any other shape.
     */
    private Map<String, Object> flatScalarExists(
            String operator, Operand lambdaOperand, String field, Polarity polarity) {
        if (!"exists".equals(operator) || !polarity.holds()
                || lambdaOperand.getNodeCase() != Operand.NodeCase.EXPRESSION) return null;
        Expression lambda = lambdaOperand.getExpression();
        if (!"lambda".equals(lambda.getOperator()) || lambda.getOperandsCount() != 2
                || lambda.getOperands(1).getNodeCase() != Operand.NodeCase.VARIABLE
                || lambda.getOperands(0).getNodeCase() != Operand.NodeCase.EXPRESSION) return null;
        Expression body = lambda.getOperands(0).getExpression();
        if (!FLAT_ELEMENT_OPERATORS.contains(body.getOperator()) || body.getOperandsCount() != 2) return null;
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
        if (valueFirst && "in".equals(body.getOperator())) return null;
        return leaf.applyResolvedLeaf(body.getOperator(), body.getOperandsList(),
                new Scope.Root(Map.of(variable, field)), Polarity.TRUE);
    }

    /**
     * {@code exists_one(t, t == f)} over a literal list, with {@code f} a document field: exactly
     * one element equals the field, so the field is one of the elements that occur once. The
     * negation is the field present and not one of them. Each element's comparison errors
     * exactly when the field is missing, so the error is the same for every element, as
     * {@code exists_one} requires: it errors when any element does.
     */
    Map<String, Object> translateExistsOne(List<Operand> operands, Polarity polarity) {
        if (operands.size() != 2) {
            throw malformed("exists_one requires exactly 2 operands, got " + operands.size());
        }
        Operand listOperand = operands.get(0);
        if (listOperand.getNodeCase() != Operand.NodeCase.VALUE) {
            throw unsupported("exists_one cannot be expressed by Elasticsearch nested queries without"
                    + " scripts: the Query DSL counts no nested matches");
        }
        if (listOperand.getValue().getKindCase() != Value.KindCase.LIST_VALUE) {
            throw malformed("exists_one over a literal collection requires a list value");
        }
        if (operands.get(1).getNodeCase() != Operand.NodeCase.EXPRESSION
                || !"lambda".equals(operands.get(1).getExpression().getOperator())) {
            throw malformed("exists_one second operand must be a lambda expression");
        }
        Lambda lambda = Lambda.of(operands.get(1).getExpression(),
                "exists_one over a literal collection supports single-variable lambdas only");
        Operand field = elementEqualsField(lambda);
        if (field == null) {
            throw unsupported("exists_one over a literal list folds only when its body compares the"
                    + " element with one document field for equality: any other body is decided"
                    + " per element, which the Query DSL cannot count");
        }
        Map<Object, Integer> occurrences = new LinkedHashMap<>();
        for (Value element : listOperand.getValue().getListValue().getValuesList()) {
            Object value = PlanValues.protoValueToJava(element);
            if (value == null || value instanceof List<?> || value instanceof Map<?, ?>) {
                // No indexed field equals a null, a list or a map.
                continue;
            }
            occurrences.merge(value, 1, Integer::sum);
        }
        ListValue.Builder singles = ListValue.newBuilder();
        occurrences.forEach((value, count) -> {
            if (count == 1) {
                singles.addValues(javaToProtoValue(value));
            }
        });
        Operand members = Operand.newBuilder().setValue(Value.newBuilder().setListValue(singles)).build();
        return walker.expression(Expression.newBuilder().setOperator("in")
                .addOperands(field).addOperands(members).build(), root, polarity);
    }

    /** The field operand of a {@code t == f} / {@code f == t} lambda body, or null. */
    private static Operand elementEqualsField(Lambda lambda) {
        if (lambda.body().getNodeCase() != Operand.NodeCase.EXPRESSION) return null;
        Expression body = lambda.body().getExpression();
        if (!"eq".equals(body.getOperator()) || body.getOperandsCount() != 2) return null;
        for (int side = 0; side < 2; side++) {
            Operand element = body.getOperands(side);
            Operand other = body.getOperands(1 - side);
            if (element.getNodeCase() == Operand.NodeCase.VARIABLE
                    && element.getVariable().equals(lambda.variable())
                    && other.getNodeCase() == Operand.NodeCase.VARIABLE) {
                return other;
            }
        }
        return null;
    }

    private static Value javaToProtoValue(Object value) {
        Value.Builder builder = Value.newBuilder();
        if (value instanceof String s) return builder.setStringValue(s).build();
        if (value instanceof Number n) return builder.setNumberValue(n.doubleValue()).build();
        if (value instanceof Boolean b) return builder.setBoolValue(b).build();
        throw malformed("Unsupported literal element: " + value.getClass().getSimpleName());
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
            Scope scope,
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
            combined.addOperands(substituteLambdaVariable(
                    lambda.body(), lambda.variable(), Operand.newBuilder().setValue(element).build()));
        }
        Expression folded = combined.build();
        return walker.expression(folded, scope, polarity);
    }

    /**
     * Folds {@code exists}/{@code all} over a list built in the policy, such as
     * {@code [R.attr.a, R.attr.b].exists(s, s == "x")}, the same way as a literal list. CEL builds
     * the list before the macro runs, so a missing element attribute errors the whole expression:
     * every field element must exist, under either polarity. A {@code struct()} literal element,
     * which the planner leaves unfolded when the list holds a field, is a map literal.
     */
    private Map<String, Object> handleListExpression(
            String operator, Expression list, Operand lambdaOperand, Scope scope, Polarity polarity) {
        if (lambdaOperand.getNodeCase() != Operand.NodeCase.EXPRESSION
                || !"lambda".equals(lambdaOperand.getExpression().getOperator())) {
            throw malformed(operator + " second operand must be a lambda expression");
        }
        Lambda lambda = Lambda.of(lambdaOperand.getExpression(),
                operator + " over a list expression supports single-variable lambdas only");
        List<Map<String, Object>> clauses = new ArrayList<>();
        Expression.Builder combined = Expression.newBuilder()
                .setOperator("exists".equals(operator) ? "or" : "and");
        for (Operand element : list.getOperandsList()) {
            if (isStructLiteral(element)) {
                element = Operand.newBuilder().setValue(structLiteral(element.getExpression())).build();
            }
            switch (element.getNodeCase()) {
                case VALUE -> { }
                case VARIABLE -> {
                    String field = scope.field(element.getVariable());
                    if (!options.scalarTypes().containsKey(field)
                            || options.explicitNullAttributes().contains(element.getVariable())) {
                        throw unsupported(operator + " over a list holding the attribute '"
                                + element.getVariable() + "' folds only when the field is a"
                                + " declared scalar whose null is a missing field: Elasticsearch"
                                + " indexes no JSON null, and a collection or object element has no"
                                + " single-term comparison");
                    }
                    clauses.add(Queries.exists(field));
                }
                default -> throw unsupported(operator + " over a list holding a computed element"
                        + " cannot be folded: the Query DSL compares a field against a literal, and"
                        + " computing the element needs a script");
            }
            combined.addOperands(substituteLambdaVariable(lambda.body(), lambda.variable(), element));
        }
        if (list.getOperandsCount() == 0) {
            boolean holds = "all".equals(operator);
            return holds == polarity.holds() ? Queries.matchAll() : Queries.matchNone();
        }
        clauses.add(walker.expression(combined.build(), scope, polarity));
        return clauses.size() == 1 ? clauses.get(0) : Queries.boolMust(clauses);
    }

    private static boolean isStructLiteral(Operand operand) {
        return operand.getNodeCase() == Operand.NodeCase.EXPRESSION
                && "struct".equals(operand.getExpression().getOperator());
    }

    /** A {@code struct(set-field(k, v), ...)} of literals as one map value. */
    private static Value structLiteral(Expression struct) {
        Struct.Builder fields = Struct.newBuilder();
        for (Operand entry : struct.getOperandsList()) {
            if (entry.getNodeCase() != Operand.NodeCase.EXPRESSION) {
                throw malformed("struct requires set-field operands");
            }
            Expression setField = entry.getExpression();
            if (!"set-field".equals(setField.getOperator()) || setField.getOperandsCount() != 2
                    || setField.getOperands(0).getNodeCase() != Operand.NodeCase.VALUE
                    || setField.getOperands(0).getValue().getKindCase() != Value.KindCase.STRING_VALUE
                    || setField.getOperands(1).getNodeCase() != Operand.NodeCase.VALUE) {
                throw unsupported("A map literal with a computed key or value cannot be folded: the"
                        + " Query DSL compares a field against a literal, and computing the entry"
                        + " needs a script");
            }
            fields.putFields(setField.getOperands(0).getValue().getStringValue(),
                    setField.getOperands(1).getValue());
        }
        return Value.newBuilder().setStructValue(fields).build();
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
     * Replaces the lambda variable with {@code element}, a literal or a field, in a lambda body.
     * Over a literal, {@code v.a.b} reads a field of the element and throws if it is missing; over
     * a field, it names the sub-field. A nested lambda that rebinds the same
     * name shadows it, so only that macro's collection operand is substituted.
     */
    private static Operand substituteLambdaVariable(
            Operand operand, String varName, Operand element) {
        switch (operand.getNodeCase()) {
            case VARIABLE -> {
                String name = operand.getVariable();
                if (name.equals(varName)) {
                    return element;
                }
                if (name.startsWith(varName + ".")) {
                    String path = name.substring(varName.length() + 1);
                    if (element.getNodeCase() == Operand.NodeCase.VARIABLE) {
                        return Operand.newBuilder()
                                .setVariable(element.getVariable() + "." + path).build();
                    }
                    return Operand.newBuilder()
                            .setValue(resolveElementPath(name, path, element.getValue()))
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

    /** {@code "x" in c.map(t, t.f)}: a literal needle tested against a {@code map()} projection. */
    static boolean isMembershipInMapProjection(List<Operand> operands) {
        return operands.size() == 2
                && operands.get(0).getNodeCase() == Operand.NodeCase.VALUE
                && isMapProjection(operands.get(1))
                && operands.get(1).getExpression().getOperandsCount() == 2
                && operands.get(1).getExpression().getOperands(0).getNodeCase()
                        == Operand.NodeCase.VARIABLE;
    }

    /**
     * {@code "x" in c.map(t, t.f)} is {@code hasIntersection(c.map(t, t.f), ["x"])}: both hold
     * when one element's projection equals the needle, and both error when an element lacks the
     * projected field. The negated form is refused for the same missing-versus-empty reason.
     */
    Map<String, Object> translateMapProjectionMembership(List<Operand> operands, Polarity polarity) {
        if (!polarity.holds()) {
            throw unsupported("Negated membership in a map() projection cannot distinguish a missing"
                    + " collection from an empty collection in Elasticsearch");
        }
        Expression map = operands.get(1).getExpression();
        String esField = root.field(map.getOperands(0).getVariable());
        Operand lambda = map.getOperands(1);
        boolean plainProjection = lambda.getNodeCase() == Operand.NodeCase.EXPRESSION
                && lambda.getExpression().getOperandsCount() == 2
                && lambda.getExpression().getOperands(0).getNodeCase() == Operand.NodeCase.VARIABLE
                && lambda.getExpression().getOperands(1).getNodeCase() == Operand.NodeCase.VARIABLE
                && lambda.getExpression().getOperands(0).getVariable()
                        .startsWith(lambda.getExpression().getOperands(1).getVariable() + ".");
        if (!options.nestedPaths().contains(esField) || !plainProjection) {
            throw unsupported("Membership in a map() projection reduces to a nested query only when"
                    + " the collection is a nested path and each element projects one of its"
                    + " fields; any other projection is computed per element, which the Query DSL"
                    + " cannot do without scripts");
        }
        Value needle = Value.newBuilder().setListValue(
                ListValue.newBuilder().addValues(operands.get(0).getValue())).build();
        return handleMapHasIntersection(
                operands.get(1).getExpression(), Operand.newBuilder().setValue(needle).build());
    }

    /** Whether the field map names a sub-field of {@code esField}, so it is an object in the index. */
    private boolean isObjectPath(String esField) {
        String prefix = esField + ".";
        return options.fieldMap().values().stream().anyMatch(f -> f.startsWith(prefix));
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
