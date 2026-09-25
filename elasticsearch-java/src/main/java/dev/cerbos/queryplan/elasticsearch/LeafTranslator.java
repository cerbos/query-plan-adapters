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
 * Translates leaf comparisons: one document field against one literal.
 */
final class LeafTranslator {

    /** Operators whose literal must be a scalar: term and range queries take one value. */
    private static final Set<String> SCALAR_OPERAND_OPERATORS = Set.of(
            "eq", "ne", "lt", "gt", "le", "ge", "contains", "startsWith", "endsWith", "matches");

    private static final Set<String> STRING_OPERATORS =
            Set.of("contains", "startsWith", "endsWith", "matches");

    /** The negation of each ordering operator. */
    private static final Map<String, String> NEGATED_RANGE = Map.of(
            "lt", "ge", "le", "gt", "gt", "le", "ge", "lt");

    private sealed interface ResolvedOperand {
        record Field(String variable) implements ResolvedOperand {}
        record Literal(Object value) implements ResolvedOperand {}
    }

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
        if ("in".equals(normalizedOperator) && !variableFirst && isObjectPath(field)) {
            // CEL's `in` over a map tests its keys. An object field's key set is not indexed:
            // Elasticsearch indexes no JSON null, so a key held with a null value is
            // indistinguishable from an absent key, and no query answers the membership.
            throw unsupported("in over the object field '" + field + "' tests its keys, as CEL"
                    + " does over a map, and Elasticsearch indexes no key whose value is null,"
                    + " so a query cannot tell a key held with a null from an absent one");
        }
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
                // No element has the field's type, so CEL's `in` is false wherever the field is
                // present.
                return whenTrue ? Queries.matchNone() : Queries.exists(field);
            }
            value = members;
        }
        if (!variableFirst && "in".equals(normalizedOperator)
                && !options.operatorOverrides().containsKey("in")
                && !inhabits(declaredElementType(field, "in", operands), value)) {
            // A value of the wrong type is never an element. The negated form was refused above.
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

    /** Whether the field map names a sub-field of {@code field}, so it is an object in the index. */
    private boolean isObjectPath(String field) {
        String prefix = field + ".";
        return options.fieldMap().values().stream().anyMatch(f -> f.startsWith(prefix));
    }

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
     * The query for a literal whose type does not match the field's declared type, or
     * {@code null} when the types match or an override handles the operator.
     *
     * <p>A cross-type comparison is a CEL error, so it matches nothing in either polarity. The
     * exception is {@code eq}/{@code ne}: cross-type equality is false, so {@code ne} matches
     * wherever the field exists.
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
     * The elements of {@code field in [...]} that have the field's type. Elasticsearch would
     * coerce the others and match them, so they are dropped. Null elements are kept for the
     * null-aware path. Unchanged when an {@code in} override is set.
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
     * The {@code hasIntersection} literals that have the collection's declared element type. An
     * empty result means the intersection is false.
     */
    List<?> typedIntersection(String field, List<?> values, List<Operand> operands) {
        ScalarType type = declaredElementType(field, "hasIntersection", operands);
        return values.stream().filter(element -> inhabits(type, element)).toList();
    }

    private ScalarType declaredElementType(String field, String operator, List<Operand> operands) {
        ScalarType type = declaredType(field, operator);
        if (type == ScalarType.TIMESTAMP && !hasTimestampWrapper(operands)) {
            throw bareTemporalComparison();
        }
        return type;
    }

    /**
     * The declared {@link ScalarType} for {@code field}. Required because Elasticsearch coerces a
     * query term to the mapped type ({@code "5"} matches the number {@code 5}) while CEL's
     * cross-type equality is false, and the adapter cannot see the mapping.
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

    /** Without an {@code ne} override, {@code ne} is {@code exists AND NOT eq}. */
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
     * The query for the operator not holding. Requires the field to exist: CEL errors on a missing
     * field, but a bare {@code bool.must_not} would match it.
     */
    private Map<String, Object> negatedQuery(
            String operator, String field, Object value, Map<String, Object> positive) {
        return switch (operator) {
            case "eq", "in", "contains", "startsWith", "endsWith", "matches" ->
                    Queries.definedAndNot(field, positive);
            case "ne" -> operatorOrDefault("eq").apply(field, value);
            // Uses the override of the negated operator, if any.
            case "lt", "le", "gt", "ge" ->
                    operatorOrDefault(NEGATED_RANGE.get(operator)).apply(field, value);
            default -> throw unsupported(
                    "Cannot safely negate operator without scripts: " + operator);
        };
    }

    /**
     * Refuses a list or map literal where a scalar is needed. CEL gives them meanings (whole-list
     * equality, key membership) that the Query DSL cannot express.
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
     * The operator rewritten with the field on the left: {@code 5 < x} becomes {@code x > 5}. A
     * string operator whose receiver is the literal has no mirror and is refused.
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

    /** Elasticsearch does not index null, so a null intersection element cannot match. */
    static void rejectNullIntersection(List<?> values) {
        if (values.stream().anyMatch(Objects::isNull)) {
            throw unsupported("hasIntersection with null requires an explicit null-value mapping");
        }
    }

    OperatorFunction operatorOrDefault(String operator) {
        return options.operatorOverrides().getOrDefault(operator, Queries.DEFAULT_OPERATORS.get(operator));
    }
}
