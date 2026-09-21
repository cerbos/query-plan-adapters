/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import static dev.cerbos.queryplan.elasticsearch.Refusals.exceptUnsupported;
import static dev.cerbos.queryplan.elasticsearch.Refusals.malformed;
import static dev.cerbos.queryplan.elasticsearch.Refusals.unsafeExplicitNullComparison;
import static dev.cerbos.queryplan.elasticsearch.Refusals.unsupported;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Options;

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

    /** The mirror of each ordering operator, which is what its negation lowers to. */
    private static final Map<String, String> NEGATED_RANGE = Map.of(
            "lt", "ge", "le", "gt", "gt", "le", "ge", "lt");

    private sealed interface ResolvedOperand {
        record Field(String variable) implements ResolvedOperand {}
        record Literal(Object value) implements ResolvedOperand {}
    }

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

        ResolvedOperand left = resolveLeafOperand(operands.get(0));
        ResolvedOperand right = resolveLeafOperand(operands.get(1));
        String variable;
        Object value;
        boolean variableFirst;
        if (left instanceof ResolvedOperand.Field leftField
                && right instanceof ResolvedOperand.Literal rightValue) {
            variable = leftField.variable();
            value = rightValue.value();
            variableFirst = true;
        } else if (left instanceof ResolvedOperand.Literal leftValue
                && right instanceof ResolvedOperand.Field rightField) {
            variable = rightField.variable();
            value = leftValue.value();
            variableFirst = false;
        } else if (left instanceof ResolvedOperand.Field) {
            throw unsupported(
                    "Elasticsearch Query DSL cannot compare two document fields without scripts");
        } else {
            throw malformed("Leaf expression must contain exactly one document field");
        }
        String field = scope.field(variable);

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
        ElasticsearchQueryPlanAdapter.ScalarType type = options.scalarTypes().get(field);
        if (type != null && !options.operatorOverrides().containsKey(normalizedOperator)
                && SCALAR_OPERAND_OPERATORS.contains(normalizedOperator)) {
            boolean timestamp = operands.stream().anyMatch(operand ->
                    operand.getNodeCase() == Operand.NodeCase.EXPRESSION
                            && "timestamp".equals(operand.getExpression().getOperator()));
            if (type == ElasticsearchQueryPlanAdapter.ScalarType.TIMESTAMP && !timestamp) {
                throw unsupported("Bare temporal comparison cannot preserve CEL string equality; use timestamp() explicitly");
            }
            boolean compatible = switch (type) {
                case STRING, TIMESTAMP -> value instanceof String;
                case NUMBER -> value instanceof Number;
                case BOOLEAN -> value instanceof Boolean;
            };
            boolean stringOperator = Set.of("contains", "startsWith", "endsWith", "matches")
                    .contains(normalizedOperator);
            if (!compatible || stringOperator && type != ElasticsearchQueryPlanAdapter.ScalarType.STRING) {
                if ("eq".equals(normalizedOperator) || "ne".equals(normalizedOperator)) {
                    boolean matches = "ne".equals(normalizedOperator) == whenTrue;
                    return matches ? Queries.exists(field) : Queries.matchNone();
                }
                // A type error is neither true nor false, including under negation.
                return Queries.matchNone();
            }
        }
        if ("hasIntersection".equals(normalizedOperator) && value instanceof List<?> values) {
            rejectNullIntersection(values);
        }
        if ("in".equals(normalizedOperator) && value instanceof List<?> values
                && values.stream().anyMatch(Objects::isNull)) {
            return nullAwareMembershipQuery(field, values, whenTrue);
        }

        Map<String, Object> positive;
        if ("in".equals(normalizedOperator)) {
            positive = membershipQuery(field, value);
        } else if ("ne".equals(normalizedOperator) && !options.operatorOverrides().containsKey("ne")) {
            positive = Queries.definedAndNot(field, operatorOrDefault("eq").apply(field, value));
        } else {
            OperatorFunction function = options.operatorOverrides().getOrDefault(
                    normalizedOperator, Queries.DEFAULT_OPERATORS.get(normalizedOperator));
            if (function == null) {
                throw unsupported("Unknown operator: " + normalizedOperator);
            }
            positive = function.apply(field, value);
        }
        if (whenTrue) {
            return positive;
        }

        return switch (normalizedOperator) {
            case "eq" -> Queries.definedAndNot(field, positive);
            case "ne" -> operatorOrDefault("eq").apply(field, value);
            // The negation of an ordering operator is its mirror, so the mirror's override is the
            // one a caller expects to see applied.
            case "lt", "le", "gt", "ge" ->
                    operatorOrDefault(NEGATED_RANGE.get(normalizedOperator))
                            .apply(field, value);
            case "in", "contains", "startsWith", "endsWith", "matches" ->
                    Queries.definedAndNot(field, positive);
            default -> throw unsupported(
                    "Cannot safely negate operator without scripts: " + normalizedOperator);
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

    /** The {@code in} lowering, through the caller's {@code in} override when there is one. */
    private Map<String, Object> membershipQuery(String field, Object value) {
        return operatorOrDefault("in").apply(field, value);
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
