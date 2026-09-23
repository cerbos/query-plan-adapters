/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import com.google.protobuf.Value;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.sdk.PlanResourcesResult;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Translates a Cerbos {@code PlanResources} response into an Elasticsearch Query DSL clause.
 *
 * <p>The result is a {@code Map<String, Object>} of plain JDK values that serialises to a Query DSL
 * clause. The adapter fails closed: a shape the Query DSL cannot express throws
 * {@link UnsupportedPlanShapeException}, an undeclared variable throws
 * {@link UnmappedAttributeException}, and a malformed plan throws {@link MalformedPlanException}.
 * All three extend {@link IllegalArgumentException}.
 */
public class ElasticsearchQueryPlanAdapter {

    /** The translated plan: always allowed, always denied, or a conditional query. */
    public sealed interface Result permits Result.AlwaysAllowed, Result.AlwaysDenied, Result.Conditional {
        record AlwaysAllowed() implements Result {}
        record AlwaysDenied() implements Result {}
        record Conditional(Map<String, Object> query) implements Result {}
    }

    /** The CEL scalar type stored in a mapped Elasticsearch field. */
    public enum ScalarType { STRING, NUMBER, BOOLEAN, TIMESTAMP }

    /**
     * What the caller declares about the index. Immutable; each {@code with…} method returns a new
     * instance. Start from {@link #of(Map)}.
     *
     * @param fieldMap maps each plan variable to an Elasticsearch field; an unmapped variable throws
     *        {@link UnmappedAttributeException}
     * @param operatorOverrides replacement translations, keyed by plan operator (see the README)
     * @param nestedPaths field paths mapped as {@code nested}, which collection macros walk
     * @param collectionFields field paths holding a flat array of scalars. {@code size()} over a
     *        field declared neither here nor in {@code nestedPaths} is refused, because the plan
     *        does not say whether the field is a string or an array
     * @param explicitNullAttributes plan variables the caller sends to {@code check()} as explicit
     *        nulls; see {@link ElasticsearchQueryPlanAdapter#toElasticsearchQuery(PlanResourcesResult, Options)}
     * @param scalarTypes the CEL scalar type of each compared field, keyed by Elasticsearch field
     *        (element type for a flat array, full path for a nested sub-field). Elasticsearch coerces
     *        a query term to the mapped type, unlike CEL, so comparing a field with no declared type
     *        throws {@link UnmappedAttributeException} unless an operator override handles it
     */
    public record Options(
            Map<String, String> fieldMap,
            Map<String, OperatorFunction> operatorOverrides,
            Set<String> nestedPaths,
            Set<String> collectionFields,
            Set<String> explicitNullAttributes,
            Map<String, ScalarType> scalarTypes) {

        public Options(Map<String, String> fieldMap, Map<String, OperatorFunction> operatorOverrides,
                       Set<String> nestedPaths, Set<String> collectionFields,
                       Set<String> explicitNullAttributes) {
            this(fieldMap, operatorOverrides, nestedPaths, collectionFields, explicitNullAttributes, Map.of());
        }

        public Options {
            scalarTypes = Map.copyOf(Objects.requireNonNull(scalarTypes, "scalarTypes"));
            fieldMap = Map.copyOf(Objects.requireNonNull(fieldMap, "fieldMap"));
            operatorOverrides = Map.copyOf(
                    Objects.requireNonNull(operatorOverrides, "operatorOverrides"));
            nestedPaths = Set.copyOf(Objects.requireNonNull(nestedPaths, "nestedPaths"));
            collectionFields = Set.copyOf(
                    Objects.requireNonNull(collectionFields, "collectionFields"));
            explicitNullAttributes = Set.copyOf(
                    Objects.requireNonNull(explicitNullAttributes, "explicitNullAttributes"));
        }

        /**
         * Options with only a field map. Add {@link #withScalarTypes(Map)} before translating a
         * plan that compares a field.
         *
         * @param fieldMap plan variable to Elasticsearch field
         * @return new Options
         */
        public static Options of(Map<String, String> fieldMap) {
            return new Options(fieldMap, Map.of(), Set.of(), Set.of(), Set.of());
        }

        public Options withFieldMap(Map<String, String> fieldMap) {
            return new Options(
                    fieldMap, operatorOverrides, nestedPaths, collectionFields, explicitNullAttributes, scalarTypes);
        }

        public Options withOperatorOverrides(Map<String, OperatorFunction> operatorOverrides) {
            return new Options(
                    fieldMap, operatorOverrides, nestedPaths, collectionFields, explicitNullAttributes, scalarTypes);
        }

        public Options withNestedPaths(Set<String> nestedPaths) {
            return new Options(
                    fieldMap, operatorOverrides, nestedPaths, collectionFields, explicitNullAttributes, scalarTypes);
        }

        public Options withCollectionFields(Set<String> collectionFields) {
            return new Options(
                    fieldMap, operatorOverrides, nestedPaths, collectionFields, explicitNullAttributes, scalarTypes);
        }

        public Options withExplicitNullAttributes(Set<String> explicitNullAttributes) {
            return new Options(
                    fieldMap, operatorOverrides, nestedPaths, collectionFields, explicitNullAttributes, scalarTypes);
        }

        /**
         * Declare the CEL scalar type of each Elasticsearch field a plan compares.
         *
         * @param scalarTypes Elasticsearch field to {@link ScalarType}
         * @return new Options with the scalar types replaced
         */
        public Options withScalarTypes(Map<String, ScalarType> scalarTypes) {
            return new Options(
                    fieldMap, operatorOverrides, nestedPaths, collectionFields, explicitNullAttributes, scalarTypes);
        }
    }

    /** The operators CEL evaluates to a definite boolean over a null value. */
    private static final Set<String> EQUALITY_FAMILY = Set.of("eq", "ne", "in");

    private ElasticsearchQueryPlanAdapter() {}

    /**
     * Translates a plan into an Elasticsearch query.
     *
     * <p>Elasticsearch does not index a JSON {@code null}, so an explicit null and a missing field
     * look the same to every query. For an attribute in {@link Options#explicitNullAttributes()},
     * CEL evaluates {@code null != "x"} to true, which no query can reproduce, so an
     * {@code eq}/{@code ne}/{@code in} comparison of that attribute against a value throws
     * {@link UnsupportedPlanShapeException}.
     *
     * @param planResult the SDK plan result
     * @param options the caller's declarations about the index
     * @return the translated query
     * @throws UnsupportedPlanShapeException if the plan holds a shape the Query DSL cannot express
     * @throws UnmappedAttributeException if the plan names a variable the options do not declare
     * @throws MalformedPlanException if the plan violates the planner's wire contract
     */
    public static Result toElasticsearchQuery(PlanResourcesResult planResult, Options options) {
        Objects.requireNonNull(planResult, "planResult");
        Objects.requireNonNull(options, "options");
        if (planResult.isAlwaysAllowed()) {
            return new Result.AlwaysAllowed();
        }
        if (planResult.isAlwaysDenied()) {
            return new Result.AlwaysDenied();
        }

        Operand condition = planResult.getCondition()
                .orElseThrow(() -> Refusals.malformed("Conditional plan has no condition"));
        return translateCondition(condition, options);
    }

    /**
     * Same as {@link #toElasticsearchQuery(PlanResourcesResult, Options)}, for a raw
     * {@link PlanResourcesResponse}.
     *
     * @param response the raw {@code PlanResources} RPC response
     * @param options the caller's declarations about the index
     * @return the translated query
     */
    public static Result toElasticsearchQuery(PlanResourcesResponse response, Options options) {
        Objects.requireNonNull(response, "response");
        Objects.requireNonNull(options, "options");
        PlanResourcesFilter filter = response.getFilter();
        return switch (filter.getKind()) {
            case KIND_ALWAYS_ALLOWED -> new Result.AlwaysAllowed();
            case KIND_ALWAYS_DENIED -> new Result.AlwaysDenied();
            case KIND_CONDITIONAL -> {
                Operand condition = filter.getCondition();
                if (condition.getNodeCase() == Operand.NodeCase.NODE_NOT_SET) {
                    throw Refusals.malformed("Conditional plan has no condition");
                }
                yield translateCondition(condition, options);
            }
            default -> throw Refusals.malformed("Unknown filter kind: " + filter.getKind());
        };
    }

    private static Result translateCondition(Operand condition, Options options) {
        assertNoExplicitNullAttributeComparisons(condition, options);
        return new Result.Conditional(new PlanWalker(options).translate(condition));
    }

    // Convenience overloads: the Options form with the other declarations empty.

    /** Convenience form of {@link #toElasticsearchQuery(PlanResourcesResult, Options)}. */
    public static Result toElasticsearchQuery(
            PlanResourcesResult planResult,
            Map<String, String> fieldMap) {
        return toElasticsearchQuery(planResult, Options.of(fieldMap));
    }

    /** Convenience form of {@link #toElasticsearchQuery(PlanResourcesResult, Options)}. */
    public static Result toElasticsearchQuery(
            PlanResourcesResult planResult,
            Map<String, String> fieldMap,
            Map<String, OperatorFunction> operatorOverrides) {
        return toElasticsearchQuery(planResult,
                Options.of(fieldMap).withOperatorOverrides(operatorOverrides));
    }

    /** Convenience form of {@link #toElasticsearchQuery(PlanResourcesResult, Options)}. */
    public static Result toElasticsearchQuery(
            PlanResourcesResult planResult,
            Map<String, String> fieldMap,
            Set<String> nestedPaths) {
        return toElasticsearchQuery(planResult, Options.of(fieldMap).withNestedPaths(nestedPaths));
    }

    /** Convenience form of {@link #toElasticsearchQuery(PlanResourcesResult, Options)}. */
    public static Result toElasticsearchQuery(
            PlanResourcesResult planResult,
            Map<String, String> fieldMap,
            Map<String, OperatorFunction> operatorOverrides,
            Set<String> nestedPaths) {
        return toElasticsearchQuery(planResult, Options.of(fieldMap)
                .withOperatorOverrides(operatorOverrides)
                .withNestedPaths(nestedPaths));
    }

    /** Convenience form of {@link #toElasticsearchQuery(PlanResourcesResult, Options)}. */
    public static Result toElasticsearchQuery(
            PlanResourcesResult planResult,
            Map<String, String> fieldMap,
            Map<String, OperatorFunction> operatorOverrides,
            Set<String> nestedPaths,
            Set<String> explicitNullAttributes) {
        return toElasticsearchQuery(planResult, Options.of(fieldMap)
                .withOperatorOverrides(operatorOverrides)
                .withNestedPaths(nestedPaths)
                .withExplicitNullAttributes(explicitNullAttributes));
    }

    /** Convenience form of {@link #toElasticsearchQuery(PlanResourcesResponse, Options)}. */
    public static Result toElasticsearchQuery(
            PlanResourcesResponse response,
            Map<String, String> fieldMap) {
        return toElasticsearchQuery(response, Options.of(fieldMap));
    }

    /** Convenience form of {@link #toElasticsearchQuery(PlanResourcesResponse, Options)}. */
    public static Result toElasticsearchQuery(
            PlanResourcesResponse response,
            Map<String, String> fieldMap,
            Map<String, OperatorFunction> operatorOverrides) {
        return toElasticsearchQuery(response,
                Options.of(fieldMap).withOperatorOverrides(operatorOverrides));
    }

    /** Convenience form of {@link #toElasticsearchQuery(PlanResourcesResponse, Options)}. */
    public static Result toElasticsearchQuery(
            PlanResourcesResponse response,
            Map<String, String> fieldMap,
            Set<String> nestedPaths) {
        return toElasticsearchQuery(response, Options.of(fieldMap).withNestedPaths(nestedPaths));
    }

    /** Convenience form of {@link #toElasticsearchQuery(PlanResourcesResponse, Options)}. */
    public static Result toElasticsearchQuery(
            PlanResourcesResponse response,
            Map<String, String> fieldMap,
            Map<String, OperatorFunction> operatorOverrides,
            Set<String> nestedPaths) {
        return toElasticsearchQuery(response, Options.of(fieldMap)
                .withOperatorOverrides(operatorOverrides)
                .withNestedPaths(nestedPaths));
    }

    /** Convenience form of {@link #toElasticsearchQuery(PlanResourcesResponse, Options)}. */
    public static Result toElasticsearchQuery(
            PlanResourcesResponse response,
            Map<String, String> fieldMap,
            Map<String, OperatorFunction> operatorOverrides,
            Set<String> nestedPaths,
            Set<String> explicitNullAttributes) {
        return toElasticsearchQuery(response, Options.of(fieldMap)
                .withOperatorOverrides(operatorOverrides)
                .withNestedPaths(nestedPaths)
                .withExplicitNullAttributes(explicitNullAttributes));
    }

    /**
     * Rejects an equality-family comparison between a declared explicit-null attribute and a
     * non-null value. Other operators error in CEL on null, which denies, matching what a missing
     * field does here. A null literal is handled by the leaf translator.
     */
    private static void assertNoExplicitNullAttributeComparisons(Operand operand, Options options) {
        Set<String> explicitNullAttributes = options.explicitNullAttributes();
        if (explicitNullAttributes.isEmpty() || operand.getNodeCase() != Operand.NodeCase.EXPRESSION) {
            return;
        }
        Expression expression = operand.getExpression();
        List<Operand> operands = expression.getOperandsList();
        if (EQUALITY_FAMILY.contains(expression.getOperator()) && operands.size() == 2) {
            Operand left = operands.get(0);
            Operand right = operands.get(1);
            boolean leftDeclared = isDeclaredAttribute(left, explicitNullAttributes);
            boolean rightDeclared = isDeclaredAttribute(right, explicitNullAttributes);
            Operand other = leftDeclared ? right : left;
            if ((leftDeclared || rightDeclared) && comparesAgainstAValue(other, options.fieldMap())) {
                throw Refusals.unsafeExplicitNullComparison();
            }
        }
        operands.forEach(child -> assertNoExplicitNullAttributeComparisons(child, options));
    }

    private static boolean isDeclaredAttribute(Operand operand, Set<String> explicitNullAttributes) {
        return operand.getNodeCase() == Operand.NodeCase.VARIABLE
                && explicitNullAttributes.contains(operand.getVariable());
    }

    /**
     * Whether the other operand is a non-null value. An unmapped variable counts: it is a lambda
     * variable that the value-list fold later replaces with a literal. A mapped variable is another
     * document field and is refused elsewhere with its own message.
     */
    private static boolean comparesAgainstAValue(Operand other, Map<String, String> fieldMap) {
        return switch (other.getNodeCase()) {
            case VALUE -> !carriesNullLiteral(other.getValue());
            case VARIABLE -> !fieldMap.containsKey(other.getVariable());
            default -> false;
        };
    }

    private static boolean carriesNullLiteral(Value value) {
        if (value.getKindCase() == Value.KindCase.NULL_VALUE) {
            return true;
        }
        return value.getKindCase() == Value.KindCase.LIST_VALUE
                && value.getListValue().getValuesList().stream()
                        .anyMatch(v -> v.getKindCase() == Value.KindCase.NULL_VALUE);
    }
}
