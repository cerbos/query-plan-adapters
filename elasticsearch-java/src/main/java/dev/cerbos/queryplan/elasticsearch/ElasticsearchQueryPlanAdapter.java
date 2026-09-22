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
 * <p>The adapter walks the plan's expression tree, resolves every attribute reference through
 * {@link Options#fieldMap()}, and emits a {@code Map<String, Object>} that serialises to a Query
 * DSL clause. It fails closed: a shape the Query DSL cannot express without scripts throws
 * {@link UnsupportedPlanShapeException} rather than emitting a best-effort filter, a variable the
 * caller has not declared throws {@link UnmappedAttributeException}, and a plan that violates the
 * planner's wire contract throws {@link MalformedPlanException}. All three extend
 * {@link IllegalArgumentException}, which remains the documented base type.
 *
 * <p>This class is the package's only public entry point. The walk itself is {@link PlanWalker},
 * and each family of shapes has a package-private translator of its own; what stays here is the
 * public API, the {@link Options} and {@link Result} types, and the explicit-null attribute scan
 * that runs over the whole tree before any query is built.
 */
public class ElasticsearchQueryPlanAdapter {

    public sealed interface Result permits Result.AlwaysAllowed, Result.AlwaysDenied, Result.Conditional {
        record AlwaysAllowed() implements Result {}
        record AlwaysDenied() implements Result {}
        record Conditional(Map<String, Object> query) implements Result {}
    }

    /** The CEL scalar type stored in a mapped Elasticsearch field. */
    public enum ScalarType { STRING, NUMBER, BOOLEAN, TIMESTAMP }

    /**
     * Everything a caller tells the adapter about the index a plan is translated against.
     *
     * <p>Immutable: every collection is defensively copied on construction, and each
     * {@code with…} method returns a new instance. Start from {@link #of(Map)} — the field map is
     * the one declaration every plan needs — and add the rest as the index requires.
     *
     * @param fieldMap maps each plan variable to an Elasticsearch field name; a variable the map
     *        does not name throws {@link UnmappedAttributeException}
     * @param operatorOverrides per-operator replacement translations, keyed by plan operator; see
     *        the README for which operators and polarities an override reaches
     * @param nestedPaths Elasticsearch field paths mapped as {@code nested} documents — the arrays
     *        of objects a collection macro walks
     * @param collectionFields Elasticsearch field paths that hold a flat array of scalars (a
     *        {@code keyword} array, say). The adapter is handed a plan, never a mapping, so it
     *        cannot tell {@code size(aString)} from {@code size(tagNames)}: a {@code size()} over
     *        a field declared neither here nor in {@code nestedPaths} is refused
     * @param scalarTypes the CEL scalar type of each compared field, keyed by mapped Elasticsearch
     *        field name (a flat array declares its element type, a nested sub-field its full
     *        path). Elasticsearch coerces a query term onto the field's mapped type where CEL's
     *        cross-type equality is false, and the adapter cannot see the mapping, so a comparison
     *        against a field declared here in no type throws {@link UnmappedAttributeException}
     *        unless an operator override owns the operator
     * @param explicitNullAttributes plan variables the caller sends to {@code check()} as explicit
     *        nulls when their column is NULL; see
     *        {@link ElasticsearchQueryPlanAdapter#toElasticsearchQuery(PlanResourcesResult, Options)}
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
         * Options holding only a field map; every other declaration is empty. Add
         * {@link #withScalarTypes(Map)} before translating a plan that compares a field.
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
         * Declare the CEL scalar type of each mapped Elasticsearch field; required for every field
         * a plan compares. See {@link Options}.
         */
        public Options withScalarTypes(Map<String, ScalarType> scalarTypes) {
            return new Options(
                    fieldMap, operatorOverrides, nestedPaths, collectionFields, explicitNullAttributes, scalarTypes);
        }
    }

    /**
     * The operators CEL evaluates to a definite boolean over a null value, and so the only ones an
     * attribute's declared explicit-null convention can settle.
     */
    private static final Set<String> EQUALITY_FAMILY = Set.of("eq", "ne", "in");

    private ElasticsearchQueryPlanAdapter() {}

    // --- Public API: the Options form ---

    /**
     * Translates a plan under the caller's {@link Options}.
     *
     * <p>On {@link Options#explicitNullAttributes()}: Elasticsearch cannot represent the explicit
     * null convention. A JSON {@code null} is not indexed, so an explicitly-null value and a
     * missing field are the same document to every query the DSL can express. The adapter already
     * refuses a comparison against a null LITERAL for that reason; what this declaration adds is
     * the other half of the same limitation
     * (<a href="https://github.com/cerbos/query-plan-adapters/issues/308">#308</a>). Under the
     * explicit convention CEL holds a null VALUE, so {@code null != "x"} is TRUE and the PDP allows
     * the row, while every Elasticsearch spelling of {@code != "x"} either requires the field to
     * exist (dropping it) or matches every document missing the field (over-granting some other
     * shape). Neither is the decision, so a comparison of a declared attribute against a non-null
     * operand is refused rather than answered narrowly.
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
     * The {@link PlanResourcesResponse} form of {@link #toElasticsearchQuery(PlanResourcesResult, Options)}.
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

    // --- Public API: the convenience overloads ---
    //
    // Each one is the Options form with the remaining declarations empty. They exist so the
    // one-argument call in the README keeps working; a caller declaring more than a field map and
    // one other thing should build an Options instead of finding the right positional overload.

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

    // --- Explicit-null attribute scan ---

    /**
     * Rejects every equality-family comparison between a declared explicit-null attribute and a
     * non-null literal, before any query is built.
     *
     * <p>Scoped to the equality family on purpose: those are the operators CEL evaluates to a
     * definite boolean over a null value, and therefore the only ones whose Elasticsearch
     * translation can disagree with the decision. Ordering and string operators raise a
     * no-overload error in CEL, which denies — exactly what a document missing the field already
     * does here. A comparison against a null LITERAL is left to the existing guard, whose message
     * this shares.
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
     * Whether the other side of the comparison carries a non-null VALUE at evaluation time.
     *
     * <p>A plan constant is one; so is a lambda-bound name, which a value-list macro fold
     * substitutes with a literal element after this scan has run — that is how the fold reaches
     * the same unrepresentable convention (#308). A name the field map knows is another DOCUMENT
     * field, which the Query DSL refuses for its own reason, and a null literal is the existing
     * guard's business; neither is claimed here, so both keep the message that actually fires.
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
