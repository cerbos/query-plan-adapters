/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import static dev.cerbos.queryplan.elasticsearch.Refusals.exceptUnsupported;
import static dev.cerbos.queryplan.elasticsearch.Refusals.malformed;
import static dev.cerbos.queryplan.elasticsearch.Refusals.unmapped;
import static dev.cerbos.queryplan.elasticsearch.Refusals.unsafeExplicitNullComparison;
import static dev.cerbos.queryplan.elasticsearch.Refusals.unsupported;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Options;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.ScalarType;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The leaf operators: one document field against one literal.
 *
 * <p>Owns operand resolution (which side is the field, which the value, and the {@code timestamp()}
 * wrapper), operator mirroring when the value comes first, the null leaf and null-aware
 * membership, the routing of each operator through the caller's override or its default lowering,
 * and the refusals of a non-scalar where a term or range query needs a scalar. It is the one
 * place a field is compared against a value, so every rule about WHICH comparisons the Query DSL
 * can answer honestly sits here.
 */
final class LeafTranslator {

    /**
     * The leaf operators whose literal operand must be a scalar. A term or range query compares a
     * field against one value; CEL's whole-list and whole-map equality have no Query DSL form.
     */
    private static final Set<String> SCALAR_OPERAND_OPERATORS = Set.of(
            "eq", "ne", "lt", "gt", "le", "ge", "contains", "startsWith", "endsWith", "matches");

    /** The string operators, which a field declared as anything but a string can never satisfy. */
    private static final Set<String> STRING_OPERATORS =
            Set.of("contains", "startsWith", "endsWith", "matches");

    /** The mirror of each ordering operator, which is what its negation lowers to. */
    private static final Map<String, String> NEGATED_RANGE = Map.of(
            "lt", "ge", "le", "gt", "gt", "le", "ge", "lt");

    private sealed interface ResolvedOperand {
        record Field(String variable) implements ResolvedOperand {}
        record Literal(Object value) implements ResolvedOperand {}
    }

    /** A leaf read with the field on the left, whatever order the plan carried it in. */
    private record Comparison(String variable, Object value, boolean variableFirst) {}

    private final Options options;

    LeafTranslator(Options options) {
        this.options = options;
    }

    Map<String, Object> applyResolvedLeaf(
            String operator, List<Operand> operands, Scope scope, Polarity polarity) {
        boolean whenTrue = polarity.holds();
        if (operands.size() != 2) {
            throw malformed(operator + " requires exactly 2 operands, got " + operands.size());
        }
        Comparison comparison = resolveComparison(operands.get(0), operands.get(1));
        String field = scope.field(comparison.variable());
        Object value = comparison.value();
        boolean variableFirst = comparison.variableFirst();

        String normalizedOperator = normalizeLeafOperator(operator, variableFirst);
        if (!whenTrue && "in".equals(normalizedOperator) && !variableFirst) {
            throw unsupported(
                    "Negated membership in a document collection cannot distinguish a missing "
                            + "collection from an empty collection in Elasticsearch");
        }
        rejectNonScalarOperand(normalizedOperator, value, variableFirst);
        if (value == null) {
            return nullLeafQuery(normalizedOperator, field, variableFirst, whenTrue);
        }
        Map<String, Object> typeMismatch =
                typeMismatchQuery(normalizedOperator, field, value, operands, whenTrue);
        if (typeMismatch != null) {
            return typeMismatch;
        }
        if ("in".equals(normalizedOperator) && variableFirst && value instanceof List<?> values) {
            List<?> members = typedMembers(field, values, operands);
            if (members.isEmpty()) {
                // No element can equal the field: CEL's `in` is heterogeneous equality against
                // each element, so it is false wherever the field is present.
                return whenTrue ? Queries.matchNone() : Queries.exists(field);
            }
            value = members;
        }
        if (!variableFirst && "in".equals(normalizedOperator)
                && !options.operatorOverrides().containsKey("in")
                && !inhabits(declaredElementType(field, "in", operands), value)) {
            // A value of the wrong type is never an element: CEL's `in` is heterogeneous equality
            // against each element, and the negated form was refused above.
            return Queries.matchNone();
        }
        if ("hasIntersection".equals(normalizedOperator) && value instanceof List<?> values) {
            rejectNullIntersection(values);
            if (!options.operatorOverrides().containsKey("hasIntersection")) {
                List<?> members = typedIntersection(field, values, operands);
                if (members.isEmpty()) {
                    return Queries.matchNone();
                }
                value = members;
            }
        }
        if ("in".equals(normalizedOperator) && value instanceof List<?> values
                && values.stream().anyMatch(Objects::isNull)) {
            return nullAwareMembershipQuery(field, values, whenTrue);
        }

        Map<String, Object> positive = positiveQuery(normalizedOperator, field, value);
        return whenTrue ? positive : negatedQuery(normalizedOperator, field, value, positive);
    }

    /** Which operand is the document field and which the literal, or a refusal when neither is. */
    private static Comparison resolveComparison(Operand leftOperand, Operand rightOperand) {
        ResolvedOperand left = resolveLeafOperand(leftOperand);
        ResolvedOperand right = resolveLeafOperand(rightOperand);
        if (left instanceof ResolvedOperand.Field field
                && right instanceof ResolvedOperand.Literal literal) {
            return new Comparison(field.variable(), literal.value(), true);
        }
        if (left instanceof ResolvedOperand.Literal literal
                && right instanceof ResolvedOperand.Field field) {
            return new Comparison(field.variable(), literal.value(), false);
        }
        if (left instanceof ResolvedOperand.Field) {
            throw unsupported(
                    "Elasticsearch Query DSL cannot compare two document fields without scripts");
        }
        throw malformed("Leaf expression must contain exactly one document field");
    }

    /**
     * The answer for a comparison against a field whose declared {@link ScalarType} the literal
     * cannot inhabit, or {@code null} when the declaration does not decide it (an override owns
     * the operator, or the types agree).
     *
     * <p>CEL raises a no-overload error for a comparison across types, and an error is neither
     * true nor false — so it matches nothing in either polarity. {@code eq} and {@code ne} are the
     * exception: CEL's heterogeneous equality is simply false, so {@code ne} holds wherever the
     * field is present.
     *
     * @throws UnmappedAttributeException when the field carries no declaration; see
     *         {@link #declaredType(String, String)}
     */
    private Map<String, Object> typeMismatchQuery(String operator, String field, Object value,
                                                  List<Operand> operands, boolean whenTrue) {
        if (options.operatorOverrides().containsKey(operator)
                || !SCALAR_OPERAND_OPERATORS.contains(operator)) {
            return null;
        }
        ScalarType type = declaredType(field, operator);
        if (type == ScalarType.TIMESTAMP && !hasTimestampWrapper(operands)) {
            throw bareTemporalComparison();
        }
        boolean compatible = inhabits(type, value);
        if (compatible && !(STRING_OPERATORS.contains(operator) && type != ScalarType.STRING)) {
            return null;
        }
        if ("eq".equals(operator) || "ne".equals(operator)) {
            boolean matches = "ne".equals(operator) == whenTrue;
            return matches ? Queries.exists(field) : Queries.matchNone();
        }
        return Queries.matchNone();
    }

    /**
     * The elements of a {@code field in [...]} list that can equal the field, or the list
     * unchanged when an {@code in} override owns the operator. A {@code terms} query coerces each
     * term onto the field's mapped type exactly as a {@code term} query does, so an element of the
     * wrong type is dropped here rather than left to match; a {@code null} element is kept for
     * the null-aware membership lowering to answer.
     */
    private List<?> typedMembers(String field, List<?> values, List<Operand> operands) {
        if (options.operatorOverrides().containsKey("in")) {
            return values;
        }
        ScalarType type = declaredType(field, "in");
        if (type == ScalarType.TIMESTAMP && !hasTimestampWrapper(operands)) {
            throw bareTemporalComparison();
        }
        return values.stream().filter(element -> element == null || inhabits(type, element)).toList();
    }

    /**
     * The elements of a {@code hasIntersection} literal list that can equal an element of the
     * collection {@code field}. A {@code terms} query coerces each term onto the field's mapped
     * type, so an element of the wrong type is dropped here rather than left to match; an empty
     * result means the intersection is false. The collection's declaration is its ELEMENT type.
     */
    List<?> typedIntersection(String field, List<?> values, List<Operand> operands) {
        ScalarType type = declaredElementType(field, "hasIntersection", operands);
        return values.stream().filter(element -> inhabits(type, element)).toList();
    }

    /** {@link #declaredType(String, String)} for a collection, whose declaration is per element. */
    private ScalarType declaredElementType(String field, String operator, List<Operand> operands) {
        ScalarType type = declaredType(field, operator);
        if (type == ScalarType.TIMESTAMP && !hasTimestampWrapper(operands)) {
            throw bareTemporalComparison();
        }
        return type;
    }

    /**
     * The caller's declared {@link ScalarType} for {@code field}, or a refusal when there is none.
     *
     * <p>Every comparison this class lowers without an override is a term-level query, and
     * Elasticsearch coerces a query term onto the field's MAPPED type: {@code "5"} matches the
     * number {@code 5}, {@code "true"} matches the boolean {@code true}, and {@code 5} matches the
     * keyword {@code "5"}. CEL's cross-type equality is {@code false}, so the untyped lowering
     * returns rows the PDP denies. The adapter is handed a plan, never a mapping, and cannot tell
     * which type a field holds — so the declaration is required rather than assumed
     * (<a href="https://github.com/cerbos/query-plan-adapters/issues/496">#496</a>).
     */
    private ScalarType declaredType(String field, String operator) {
        ScalarType type = options.scalarTypes().get(field);
        if (type == null) {
            throw unmapped("Field '" + field + "' has no declared scalar type: " + operator
                    + " lowers to a term or range query, and Elasticsearch coerces the query term "
                    + "onto the field's mapped type where CEL's cross-type equality is false. "
                    + "Declare it in Options.withScalarTypes");
        }
        return type;
    }

    private static boolean inhabits(ScalarType type, Object value) {
        return switch (type) {
            case STRING, TIMESTAMP -> value instanceof String;
            case NUMBER -> value instanceof Number;
            case BOOLEAN -> value instanceof Boolean;
        };
    }

    private static boolean hasTimestampWrapper(List<Operand> operands) {
        return operands.stream().anyMatch(operand ->
                operand.getNodeCase() == Operand.NodeCase.EXPRESSION
                        && "timestamp".equals(operand.getExpression().getOperator()));
    }

    private static UnsupportedPlanShapeException bareTemporalComparison() {
        return unsupported("Bare temporal comparison cannot preserve CEL string equality; use timestamp() explicitly");
    }

    /**
     * The query for the operator holding. A positive {@code ne} with no {@code ne} override is
     * {@code exists AND NOT eq}, with the caller's {@code eq} override inside it.
     */
    private Map<String, Object> positiveQuery(String operator, String field, Object value) {
        if ("ne".equals(operator) && !options.operatorOverrides().containsKey("ne")) {
            return Queries.definedAndNot(field, operatorOrDefault("eq").apply(field, value));
        }
        OperatorFunction function = operatorOrDefault(operator);
        if (function == null) {
            throw unsupported("Unknown operator: " + operator);
        }
        return function.apply(field, value);
    }

    /**
     * The query for the operator NOT holding. Every negation requires the field to exist, because
     * CEL errors (and the PDP denies) on a missing one, while a bare {@code bool.must_not} would
     * match it.
     */
    private Map<String, Object> negatedQuery(
            String operator, String field, Object value, Map<String, Object> positive) {
        return switch (operator) {
            case "eq", "in", "contains", "startsWith", "endsWith", "matches" ->
                    Queries.definedAndNot(field, positive);
            case "ne" -> operatorOrDefault("eq").apply(field, value);
            // The negation of an ordering operator is its mirror, so the mirror's override is the
            // one a caller expects to see applied.
            case "lt", "le", "gt", "ge" ->
                    operatorOrDefault(NEGATED_RANGE.get(operator)).apply(field, value);
            default -> throw unsupported(
                    "Cannot safely negate operator without scripts: " + operator);
        };
    }

    /**
     * A term or range query compares a field against ONE scalar. CEL's list and map literals have
     * other meanings — whole-list equality, key membership — that the Query DSL has no operand
     * for, so a non-scalar where a scalar is expected is refused by name rather than serialised
     * into a query Elasticsearch would either reject or, worse, read as a different predicate.
     */
    private static void rejectNonScalarOperand(String operator, Object value, boolean variableFirst) {
        boolean nonScalar = value instanceof List<?> || value instanceof Map<?, ?>;
        if (SCALAR_OPERAND_OPERATORS.contains(operator) && nonScalar) {
            throw unsupported(operator + " against a " + kindOf(value) + " literal cannot be "
                    + "expressed: a term or range query compares a field against a scalar, and "
                    + "the Query DSL has no whole-" + kindOf(value) + " comparison");
        }
        if ("in".equals(operator) && variableFirst && value instanceof Map<?, ?>) {
            throw unsupported("in over a map literal cannot be expressed: CEL map membership "
                    + "tests the map's keys, and the Query DSL has no such operand");
        }
        if ("in".equals(operator) && !variableFirst && nonScalar) {
            throw unsupported("in with a " + kindOf(value) + " element cannot be expressed: a "
                    + "terms query matches scalar terms only");
        }
        if (("in".equals(operator) || "hasIntersection".equals(operator))
                && value instanceof List<?> values) {
            rejectNonScalarElements(operator, values);
        }
    }

    static void rejectNonScalarElements(String operator, List<?> values) {
        for (Object element : values) {
            if (element instanceof List<?> || element instanceof Map<?, ?>) {
                throw unsupported(operator + " over a list holding a " + kindOf(element)
                        + " element cannot be expressed: a terms query matches scalar terms only");
            }
        }
    }

    private static String kindOf(Object value) {
        return value instanceof Map<?, ?> ? "map" : "list";
    }

    private static ResolvedOperand resolveLeafOperand(Operand operand) {
        return switch (operand.getNodeCase()) {
            case VARIABLE -> new ResolvedOperand.Field(operand.getVariable());
            case VALUE -> {
                Object value = PlanValues.protoValueToJava(operand.getValue());
                PlanValues.rejectNonFinite(value);
                yield new ResolvedOperand.Literal(value);
            }
            case EXPRESSION -> {
                Expression expression = operand.getExpression();
                if ("except".equals(expression.getOperator())) {
                    throw exceptUnsupported();
                }
                if (!"timestamp".equals(expression.getOperator())
                        || expression.getOperandsCount() != 1) {
                    throw unsupported(
                            "Unexpected " + expression.getOperator() + " expression in leaf operand");
                }
                ResolvedOperand resolved = resolveLeafOperand(expression.getOperands(0));
                if (resolved instanceof ResolvedOperand.Literal literal) {
                    PlanValues.validateTimestampLiteral(literal.value());
                }
                yield resolved;
            }
            default -> throw malformed(
                    "Unexpected operand type in leaf expression: " + operand.getNodeCase());
        };
    }

    /**
     * The operator as it reads with the field on the left. The planner preserves policy source
     * order, so {@code 5 < R.attr.x} arrives value-first and the ordering has to be mirrored; a
     * string operator whose RECEIVER is the constant has no mirror and is refused.
     */
    static String normalizeLeafOperator(String operator, boolean variableFirst) {
        if (variableFirst) {
            return operator;
        }
        return switch (operator) {
            case "eq", "ne", "in" -> operator;
            case "lt" -> "gt";
            case "le" -> "ge";
            case "gt" -> "lt";
            case "ge" -> "le";
            case "contains", "startsWith", "endsWith", "matches" ->
                    throw unsupported(
                            operator + " with a document field as the receiver argument "
                                    + "cannot be expressed without scripts");
            default -> operator;
        };
    }

    private static Map<String, Object> nullLeafQuery(
            String operator,
            String field,
            boolean variableFirst,
            boolean whenTrue) {
        return switch (operator) {
            case "eq" -> {
                if (whenTrue) {
                    throw unsafeExplicitNullComparison();
                }
                yield Queries.exists(field);
            }
            case "ne" -> {
                if (!whenTrue) {
                    throw unsafeExplicitNullComparison();
                }
                yield Queries.exists(field);
            }
            case "in" -> {
                if (!variableFirst) {
                    throw unsupported(
                            "null membership in a document array requires an explicit null-value mapping");
                }
                throw unsafeExplicitNullComparison();
            }
            default -> throw unsupported(
                    "Null values are only supported with eq, ne, and scalar in operators");
        };
    }

    private Map<String, Object> nullAwareMembershipQuery(
            String field, List<?> values, boolean whenTrue) {
        if (whenTrue) {
            throw unsafeExplicitNullComparison();
        }
        List<?> nonNull = values.stream().filter(Objects::nonNull).toList();
        if (nonNull.isEmpty()) {
            return Queries.exists(field);
        }
        return Queries.definedAndNot(field, operatorOrDefault("in").apply(field, nonNull));
    }

    /**
     * A {@code null} element in an intersection has no indexed counterpart — Elasticsearch does
     * not index a JSON null — so whichever side carries it, the shape is refused before a
     * {@code terms} query is built that would silently drop the element.
     */
    static void rejectNullIntersection(List<?> values) {
        if (values.stream().anyMatch(Objects::isNull)) {
            throw unsupported("hasIntersection with null requires an explicit null-value mapping");
        }
    }

    /** The caller's override for {@code operator}, or its default lowering. */
    OperatorFunction operatorOrDefault(String operator) {
        return options.operatorOverrides().getOrDefault(operator, Queries.DEFAULT_OPERATORS.get(operator));
    }
}
