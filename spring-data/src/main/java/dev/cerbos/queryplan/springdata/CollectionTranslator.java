/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Expression;
import jakarta.persistence.criteria.Predicate;

import com.google.protobuf.Value;

import java.util.List;
import java.util.Set;

/**
 * Translates {@code exists}, {@code exists_one} and {@code all} over a relation chain, and
 * folds them over a literal list. The subqueries come from {@link ChainSubqueries}. {@code filter} in boolean position is refused by the walk, and
 * {@code size(filter(...))} belongs to {@link SizeTranslator}.
 */
final class CollectionTranslator {

    private static final Set<String> LAMBDA_BINDING_OPERATORS =
            Set.of("exists", "exists_one", "all", "filter", "map", "except");

    private final CriteriaBuilder cb;
    private final PlanWalker walker;
    private final ChainSubqueries subqueries;

    CollectionTranslator(CriteriaBuilder cb, PlanWalker walker, ChainSubqueries subqueries) {
        this.cb = cb;
        this.walker = walker;
        this.subqueries = subqueries;
    }

    /**
     * Translates a macro over a relation as a tri-state predicate. A NULL element column is a
     * missing attribute, so a body that reads it is a CEL error, mapped to SQL UNKNOWN:
     * <ul>
     *   <li>{@code exists}: true if any element matches, else UNKNOWN if any errors, else
     *       false;</li>
     *   <li>{@code all}: false if any element fails, else UNKNOWN if any errors, else
     *       true;</li>
     *   <li>{@code exists_one}: UNKNOWN if any element errors, else true if exactly one
     *       matches.</li>
     * </ul>
     * A plain EXISTS would turn an error into FALSE, and its negation would return rows the
     * PDP denies.
     *
     * <p>The body is translated twice per macro (three times for {@code exists_one}), so
     * nesting multiplies subqueries. {@link PlanWalker#enterMacro} bounds the depth
     * ({@link SpringDataQueryPlanAdapter#MAX_MACRO_DEPTH_PROPERTY}, default
     * {@value SpringDataQueryPlanAdapter#DEFAULT_MAX_MACRO_DEPTH}).
     */
    Predicate handleCollectionOperator(String op, List<Operand> operands, Scope scope) {
        if (operands.size() != 2) {
            throw Refusals.malformed(op + " requires exactly 2 operands");
        }
        Operand listOperand = operands.get(0);
        Operand lambdaOperand = operands.get(1);

        // The planner unrolls a macro over a known list of up to 10 elements itself; above
        // that the list arrives as the collection operand, so fold it here the same way.
        if (listOperand.getNodeCase() == Operand.NodeCase.VALUE) {
            return walker.enterMacro(op,
                    () -> handleKnownValueCollection(op, listOperand.getValue(), lambdaOperand, scope));
        }

        if (listOperand.getNodeCase() != Operand.NodeCase.VARIABLE) {
            // e.g. `tags.map(...).exists(...)`: legal CEL with no join chain.
            throw Refusals.unsupported(op + " first operand must be a variable");
        }
        if (lambdaOperand.getNodeCase() != Operand.NodeCase.EXPRESSION
                || !"lambda".equals(lambdaOperand.getExpression().getOperator())) {
            throw Refusals.malformed(op + " second operand must be a lambda");
        }

        String collectionVar = listOperand.getVariable();
        if (!(scope.resolve(collectionVar) instanceof Scope.ResolvedRelation ref)) {
            throw Refusals.unmapped(
                    op + " requires a Relation mapping for " + collectionVar);
        }

        ParsedLambda lambda = ParsedLambda.parse(lambdaOperand,
                op + " second operand must be a lambda",
                "lambda requires exactly 2 operands",
                "lambda variable must be a variable operand");
        Operand body = lambda.body();
        String lambdaVarName = lambda.varName();

        // Each call builds a fresh Predicate tree: Hibernate 6 negation is stateful.
        SubqueryBodyBuilder bodyBuilder = (sub, tailJoin, rebased) -> walker.traverse(body,
                Scope.lambda(tailJoin, sub, ref.tail(), lambdaVarName, rebased));

        return walker.enterMacro(op, () -> switch (op) {
            case "exists" ->
                    cb.equal(subqueries.requireLeadingHops(scope, ref,
                            subqueries.macroScoreSubquery(scope, ref, bodyBuilder, 2, 0),
                            Integer.class), 2);
            case "all" ->
                    cb.equal(subqueries.requireLeadingHops(scope, ref,
                            subqueries.macroScoreSubquery(scope, ref, bodyBuilder, 0, 2),
                            Integer.class), 0);
            case "exists_one" ->
                    cb.equal(subqueries.requireLeadingHops(scope, ref,
                            subqueries.strictMatchCountSubquery(scope, ref, bodyBuilder),
                            Long.class), 1L);
            default -> throw Refusals.internal("Unsupported collection operator: " + op);
        });
    }

    /**
     * Folds a macro over a literal list: substitutes each element into the body and joins the
     * results with {@code or} ({@code exists}) or {@code and} ({@code all}), as the planner
     * does for short lists. An empty list gives false for {@code exists} and true for
     * {@code all}. {@code exists_one} is the strict count ({@link #literalStrictCount}) equal
     * to 1.
     */
    private Predicate handleKnownValueCollection(String op, Value collectionValue,
                                                 Operand lambdaOperand, Scope scope) {
        if (!"exists".equals(op) && !"all".equals(op) && !"exists_one".equals(op)) {
            throw Refusals.unsupported(op
                    + " over a literal collection value is not supported. "
                    + "Only exists(), all() and exists_one() can be folded into a flat filter.");
        }
        if (collectionValue.getKindCase() != Value.KindCase.LIST_VALUE) {
            // CEL cannot iterate a scalar, so the planner cannot emit this.
            throw Refusals.malformed(op
                    + " over a literal collection requires a list value");
        }
        ParsedLambda lambda = ParsedLambda.parse(lambdaOperand,
                op + " second operand must be a lambda",
                op + " over a literal collection supports single-variable lambdas only",
                "lambda variable must be a variable operand");

        if ("exists_one".equals(op)) {
            return cb.equal(literalStrictCount(cb, walker, collectionValue, lambda, scope), 1L);
        }
        List<Value> elements = collectionValue.getListValue().getValuesList();
        if (elements.isEmpty()) {
            return "exists".equals(op) ? cb.disjunction() : cb.conjunction();
        }

        PlanResourcesFilter.Expression.Builder combined = PlanResourcesFilter.Expression
                .newBuilder()
                .setOperator("exists".equals(op) ? "or" : "and");
        for (Value element : elements) {
            combined.addOperands(
                    substituteLambdaVariable(lambda.body(), lambda.varName(), element));
        }
        return walker.traverse(Operand.newBuilder().setExpression(combined).build(), scope);
    }

    /** A lambda variable no plan names, bound to the element a positional read selects. */
    private static final String POSITIONAL_ELEMENT = "__cerbos_positional_element";

    /**
     * A leaf that reads one list element by position, {@code R.attr.list[i] op v} or
     * {@code R.attr.list[i].field op v}, over a relation that declares its
     * {@linkplain AttributeMapping.Relation#withPositionField position field}. Returns
     * {@code null} when no operand is such a read, so the leaf is translated as usual.
     *
     * <p>It is translated as {@code list.exists(e, position(e) == i && e op v)}, whose score
     * subquery is TRUE when the element at {@code i} satisfies the leaf, FALSE when it fails
     * it, and UNKNOWN when its comparison is (every other element is FALSE). CEL errors when
     * {@code i} is not a non-negative integer or no element sits at {@code i}, so those are
     * UNKNOWN too, under both polarities.
     */
    Predicate tryPositionalRead(String op, List<Operand> operands, Scope scope) {
        int readAt = -1;
        for (int i = 0; i < operands.size(); i++) {
            if (positionalRead(operands.get(i)) != null) {
                if (readAt >= 0) {
                    throw Refusals.unsupported(op + " between two positional list reads is not"
                            + " supported: each is a separate correlated element");
                }
                readAt = i;
            }
        }
        if (readAt < 0) {
            return null;
        }
        PlanResourcesFilter.Expression index = positionalRead(operands.get(readAt));
        Operand read = operands.get(readAt);
        String field = read.getExpression().getOperator().equals("get-field")
                ? read.getExpression().getOperands(1).getVariable() : null;
        String listVar = index.getOperands(0).getVariable();
        if (!(scope.resolve(listVar) instanceof Scope.ResolvedRelation ref)
                || ref.isChained() || ref.tail().positionField() == null) {
            throw Refusals.unsupported("Positional access into " + listVar + " requires a"
                    + " direct Relation mapping that declares its position field"
                    + " (AttributeMapping.Relation#withPositionField): a JPA collection has no"
                    + " order a plan can name otherwise");
        }
        TriPredicate tri = new TriPredicate(cb);
        Value position = index.getOperands(1).getValue();
        double at = position.getKindCase() == Value.KindCase.NUMBER_VALUE
                ? position.getNumberValue() : -1;
        if (at < 0 || at != Math.rint(at) || at > Integer.MAX_VALUE) {
            // A non-integral, negative or non-numeric index is a CEL error.
            return tri.unknown();
        }
        int atIndex = (int) at;
        String positionField = ref.tail().positionField();

        Operand element = Operand.newBuilder().setVariable(field == null
                ? POSITIONAL_ELEMENT : POSITIONAL_ELEMENT + "." + field).build();
        PlanResourcesFilter.Expression.Builder leaf = PlanResourcesFilter.Expression.newBuilder()
                .setOperator(op).addAllOperands(operands).setOperands(readAt, element);
        Operand body = Operand.newBuilder().setExpression(leaf).build();
        SubqueryBodyBuilder bodyBuilder = (sub, tailJoin, rebased) -> cb.and(
                cb.equal(tailJoin.get(positionField), atIndex),
                walker.traverse(body, Scope.lambda(tailJoin, sub, ref.tail(),
                        POSITIONAL_ELEMENT, rebased)));
        return walker.enterMacro(op, () -> tri.baseUnlessUnknown(
                cb.equal(subqueries.macroScoreSubquery(scope, ref, bodyBuilder, 2, 0), 2),
                () -> tri.not(subqueries.existsSubquery(scope, ref, (sub, tailJoin, rebased) ->
                        cb.equal(tailJoin.get(positionField), atIndex)))));
    }

    /** The {@code index(list, i)} {@code operand} reads, directly or under a get-field. */
    private static PlanResourcesFilter.Expression positionalRead(Operand operand) {
        if (operand.getNodeCase() != Operand.NodeCase.EXPRESSION) {
            return null;
        }
        PlanResourcesFilter.Expression e = operand.getExpression();
        if ("get-field".equals(e.getOperator()) && e.getOperandsCount() == 2
                && e.getOperands(1).getNodeCase() == Operand.NodeCase.VARIABLE) {
            PlanResourcesFilter.Expression inner = positionalRead(e.getOperands(0));
            return inner != null && "index".equals(e.getOperands(0).getExpression().getOperator())
                    ? inner : null;
        }
        return "index".equals(e.getOperator()) && e.getOperandsCount() == 2
                && e.getOperands(0).getNodeCase() == Operand.NodeCase.VARIABLE
                && e.getOperands(1).getNodeCase() == Operand.NodeCase.VALUE ? e : null;
    }

    /**
     * The number of elements of a literal list whose body holds, or NULL when any body is
     * UNKNOWN: {@code exists_one} and {@code filter} evaluate every element and error if any
     * does. Each element contributes {@code CASE WHEN p THEN 1 WHEN NOT p THEN 0 END}, which is
     * NULL for an UNKNOWN body, and SQL {@code +} carries that NULL into the sum.
     */
    static Expression<Long> literalStrictCount(CriteriaBuilder cb, PlanWalker walker,
                                               Value list, ParsedLambda lambda, Scope scope) {
        if (list.getKindCase() != Value.KindCase.LIST_VALUE) {
            // CEL cannot iterate a scalar, so the planner cannot emit this.
            throw Refusals.malformed("a macro over a literal collection requires a list value");
        }
        TriPredicate tri = new TriPredicate(cb);
        Expression<Long> count = cb.literal(0L);
        for (Value element : list.getListValue().getValuesList()) {
            Operand body = substituteLambdaVariable(lambda.body(), lambda.varName(), element);
            // Built twice: a predicate node must not be used in both polarities.
            Expression<Long> matched = cb.<Long>selectCase()
                    .when(walker.traverse(body, scope), 1L)
                    .when(tri.not(walker.traverse(body, scope)), 0L)
                    .otherwise(cb.nullLiteral(Long.class));
            count = cb.sum(count, matched);
        }
        return count;
    }

    /**
     * Replaces the lambda variable with {@code element} in a body. {@code var.a.b} reads a
     * field of the element. A nested macro that rebinds the same name shadows it, so only its
     * collection operand is substituted.
     */
    private static Operand substituteLambdaVariable(Operand operand, String varName,
                                                    Value element) {
        switch (operand.getNodeCase()) {
            case VARIABLE -> {
                String name = operand.getVariable();
                if (name.equals(varName)) {
                    return Operand.newBuilder().setValue(element).build();
                }
                if (name.startsWith(varName + ".")) {
                    return Operand.newBuilder()
                            .setValue(resolveElementPath(name,
                                    name.substring(varName.length() + 1), element))
                            .build();
                }
                return operand;
            }
            case EXPRESSION -> {
                PlanResourcesFilter.Expression expr = operand.getExpression();
                List<Operand> ops = expr.getOperandsList();
                PlanResourcesFilter.Expression.Builder rebuilt = expr.toBuilder();
                if (LAMBDA_BINDING_OPERATORS.contains(expr.getOperator()) && ops.size() == 2
                        && shadowsVariable(ops.get(1), varName)) {
                    rebuilt.setOperands(0,
                            substituteLambdaVariable(ops.get(0), varName, element));
                    return Operand.newBuilder().setExpression(rebuilt).build();
                }
                for (int i = 0; i < ops.size(); i++) {
                    rebuilt.setOperands(i,
                            substituteLambdaVariable(ops.get(i), varName, element));
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
                // CEL would error on this element, and the fold cannot express a
                // per-element error, so refuse.
                throw Refusals.unsupported("Cannot resolve \"" + fullRef
                        + "\": collection element has no field \"" + segment + "\"");
            }
            current = current.getStructValue().getFieldsOrThrow(segment);
        }
        return current;
    }
}
