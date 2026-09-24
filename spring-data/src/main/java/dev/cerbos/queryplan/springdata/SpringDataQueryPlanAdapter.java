/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.sdk.PlanResourcesResult;

import org.springframework.data.jpa.domain.Specification;

import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Root;

import com.google.protobuf.Value;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;

/**
 * Translates a Cerbos {@code PlanResources} response into a Spring Data JPA
 * {@link Specification} for a {@code JpaSpecificationExecutor}.
 *
 * <p>Every plan kind yields a Specification: {@code KIND_ALWAYS_ALLOWED} gives
 * {@link Specification#unrestricted()}, {@code KIND_ALWAYS_DENIED} an always-false predicate,
 * and a conditional plan the translated predicate. To skip the query on a denied plan, check
 * {@code planResult.isAlwaysDenied()} first. Hand the Specification to a repository method;
 * calling {@link Specification#toPredicate} yourself is not supported.
 *
 * <p>A shape the adapter cannot translate faithfully throws rather than returning a filter:
 * {@link UnsupportedPlanShapeException}, {@link UnmappedAttributeException} or
 * {@link MalformedPlanException}, all subtypes of {@link IllegalArgumentException}. Most are
 * raised when the Specification is first evaluated, not by {@code toSpecification}.
 *
 * <p><strong>The Specification is SELECT-only.</strong> Do not pass it to
 * {@code delete(Specification)} or another criteria bulk operation: with a
 * {@link AttributeMapping.Relation} in play, Hibernate's bulk delete clears the collection
 * tables first and the correlated subquery then deletes no entity rows. The adapter throws
 * {@link UnsupportedOperationException} in that case. Select the ids and delete by id instead:
 *
 * <pre>{@code
 * Specification<MyEntity> spec = SpringDataQueryPlanAdapter.toSpecification(plan, MAPPING);
 * List<Long> ids = repository.findAll(spec).stream().map(MyEntity::getId).toList();
 * repository.deleteAllById(ids);
 * }</pre>
 */
public final class SpringDataQueryPlanAdapter {

    /**
     * System property bounding collection-macro nesting depth ({@code exists}, {@code all},
     * {@code filter} and so on). Each level multiplies the correlated subqueries in the filter.
     * Deeper plans throw {@link UnsupportedPlanShapeException}.
     *
     * <p>{@link Options#withMaxMacroDepth(int)} takes precedence; without either,
     * {@value #DEFAULT_MAX_MACRO_DEPTH} applies. A value that is not a positive integer throws
     * {@link IllegalArgumentException}.
     */
    public static final String MAX_MACRO_DEPTH_PROPERTY =
            "dev.cerbos.queryplan.springdata.maxMacroDepth";

    /** Default value of {@link #MAX_MACRO_DEPTH_PROPERTY}. */
    public static final int DEFAULT_MAX_MACRO_DEPTH = 5;

    /**
     * Translation options. Immutable and safe to share: collections are copied and each
     * {@code with…} method returns a new instance. Start from {@link #of(Map)}.
     *
     * @param mapping maps each plan variable to a JPA path or relation; see
     *        {@link AttributeMapping}
     * @param operatorOverrides replacement translations keyed by Cerbos operator name; see
     *        {@link OperatorFunction} for where they apply
     * @param nullAttributeRepresentation the NULL-column convention for attributes whose mapping
     *        does not declare one
     * @param maxMacroDepth the collection-macro nesting bound for this call; when empty,
     *        {@link #MAX_MACRO_DEPTH_PROPERTY} and then {@link #DEFAULT_MAX_MACRO_DEPTH} apply
     */
    public record Options(
            Map<String, AttributeMapping> mapping,
            Map<String, OperatorFunction> operatorOverrides,
            NullAttributeRepresentation nullAttributeRepresentation,
            OptionalInt maxMacroDepth) {

        public Options {
            mapping = Map.copyOf(Objects.requireNonNull(mapping, "mapping"));
            operatorOverrides = Map.copyOf(
                    Objects.requireNonNull(operatorOverrides, "operatorOverrides"));
            Objects.requireNonNull(nullAttributeRepresentation, "nullAttributeRepresentation");
            Objects.requireNonNull(maxMacroDepth, "maxMacroDepth");
            if (maxMacroDepth.isPresent() && maxMacroDepth.getAsInt() < 1) {
                throw new IllegalArgumentException(
                        "maxMacroDepth must be a positive integer, got " + maxMacroDepth.getAsInt());
            }
        }

        /**
         * Options with only a mapping: no overrides, {@link NullAttributeRepresentation#EXPLICIT},
         * and no macro-depth bound of their own.
         *
         * @param mapping maps each plan variable to a JPA path or relation
         * @return the options
         */
        public static Options of(Map<String, AttributeMapping> mapping) {
            return new Options(mapping, Map.of(), NullAttributeRepresentation.EXPLICIT,
                    OptionalInt.empty());
        }

        public Options withMapping(Map<String, AttributeMapping> mapping) {
            return new Options(mapping, operatorOverrides, nullAttributeRepresentation, maxMacroDepth);
        }

        public Options withOperatorOverrides(Map<String, OperatorFunction> operatorOverrides) {
            return new Options(mapping, operatorOverrides, nullAttributeRepresentation, maxMacroDepth);
        }

        public Options withNullAttributeRepresentation(
                NullAttributeRepresentation nullAttributeRepresentation) {
            return new Options(mapping, operatorOverrides, nullAttributeRepresentation, maxMacroDepth);
        }

        /**
         * Bounds collection-macro nesting for this call, overriding
         * {@link #MAX_MACRO_DEPTH_PROPERTY}.
         *
         * @param maxMacroDepth a positive integer
         * @return new options with the bound set
         * @throws IllegalArgumentException if {@code maxMacroDepth} is less than 1
         */
        public Options withMaxMacroDepth(int maxMacroDepth) {
            return new Options(mapping, operatorOverrides, nullAttributeRepresentation,
                    OptionalInt.of(maxMacroDepth));
        }

        /** The declared bound, else the system property, else the default. */
        int effectiveMaxMacroDepth() {
            return maxMacroDepth.orElseGet(SpringDataQueryPlanAdapter::readMaxMacroDepth);
        }
    }

    private SpringDataQueryPlanAdapter() {}

    /**
     * Translates a plan from the Java SDK's {@code CerbosBlockingClient.plan(...)}.
     *
     * @param <T> the entity type
     * @param planResult the SDK plan result
     * @param options the translation options
     * @return a SELECT-only Specification selecting the rows the plan permits
     * @throws MalformedPlanException if a conditional plan has no condition
     * @throws UnsupportedPlanShapeException if the plan has a null comparison operand under
     *         {@link NullAttributeRepresentation#OMITTED}. Other refusals are raised when the
     *         Specification is evaluated.
     */
    public static <T> Specification<T> toSpecification(
            PlanResourcesResult planResult, Options options) {
        Objects.requireNonNull(options, "options");
        if (planResult.isAlwaysAllowed()) {
            return alwaysAllowed();
        }
        if (planResult.isAlwaysDenied()) {
            return alwaysDenied();
        }
        Operand condition = planResult.getCondition()
                .orElseThrow(() -> Refusals.malformed("Conditional plan has no condition"));
        return conditional(condition, options);
    }

    /**
     * Translates a raw {@link PlanResourcesResponse}, for responses obtained without the SDK
     * client.
     *
     * @param <T> the entity type
     * @param response the raw {@code PlanResources} response
     * @param options the translation options
     * @return a SELECT-only Specification selecting the rows the plan permits
     * @throws MalformedPlanException if the filter kind is unknown or a conditional filter has
     *         no condition
     * @throws UnsupportedPlanShapeException if the plan has a null comparison operand under
     *         {@link NullAttributeRepresentation#OMITTED}. Other refusals are raised when the
     *         Specification is evaluated.
     */
    public static <T> Specification<T> toSpecification(
            PlanResourcesResponse response, Options options) {
        Objects.requireNonNull(options, "options");
        PlanResourcesFilter filter = response.getFilter();
        return switch (filter.getKind()) {
            case KIND_ALWAYS_ALLOWED -> alwaysAllowed();
            case KIND_ALWAYS_DENIED -> alwaysDenied();
            case KIND_CONDITIONAL -> {
                if (filter.getCondition().getNodeCase() == Operand.NodeCase.NODE_NOT_SET) {
                    throw Refusals.malformed("Conditional plan has no condition");
                }
                yield conditional(filter.getCondition(), options);
            }
            default -> throw Refusals.malformed("Unknown filter kind: " + filter.getKind());
        };
    }

    /**
     * Translates a plan from the Java SDK with the default operator translations.
     *
     * @param <T> the entity type
     * @param planResult the SDK plan result
     * @param mapper maps each plan variable to a JPA path or relation; see
     *        {@link AttributeMapping}
     * @return a SELECT-only Specification selecting the rows the plan permits
     * @throws MalformedPlanException if a conditional plan has no condition. Other refusals are
     *         raised when the Specification is evaluated.
     */
    public static <T> Specification<T> toSpecification(
            PlanResourcesResult planResult, Map<String, AttributeMapping> mapper) {
        return toSpecification(planResult, mapper, Map.of());
    }

    /**
     * Translates a plan from the Java SDK with operator overrides.
     *
     * @param <T> the entity type
     * @param planResult the SDK plan result
     * @param mapper maps each plan variable to a JPA path or relation
     * @param overrides replacement translations keyed by Cerbos operator name; see
     *        {@link OperatorFunction}
     * @return a SELECT-only Specification selecting the rows the plan permits
     * @throws MalformedPlanException if a conditional plan has no condition. Other refusals are
     *         raised when the Specification is evaluated.
     */
    public static <T> Specification<T> toSpecification(
            PlanResourcesResult planResult,
            Map<String, AttributeMapping> mapper,
            Map<String, OperatorFunction> overrides) {
        return toSpecification(
                planResult, mapper, overrides, NullAttributeRepresentation.EXPLICIT);
    }

    /**
     * Translates a plan from the Java SDK, declaring how the caller sends NULL columns to
     * {@code check()}. Under {@link NullAttributeRepresentation#OMITTED}, null comparison
     * operands are rejected by this call.
     *
     * @param <T> the entity type
     * @param planResult the SDK plan result
     * @param mapper maps each plan variable to a JPA path or relation
     * @param overrides replacement translations keyed by Cerbos operator name
     * @param nullAttributeRepresentation the caller's NULL-column convention
     * @return a SELECT-only Specification selecting the rows the plan permits
     * @throws MalformedPlanException if a conditional plan has no condition
     * @throws UnsupportedPlanShapeException if the plan has a null comparison operand under
     *         {@link NullAttributeRepresentation#OMITTED}
     */
    public static <T> Specification<T> toSpecification(
            PlanResourcesResult planResult,
            Map<String, AttributeMapping> mapper,
            Map<String, OperatorFunction> overrides,
            NullAttributeRepresentation nullAttributeRepresentation) {
        return toSpecification(planResult, Options.of(mapper)
                .withOperatorOverrides(overrides)
                .withNullAttributeRepresentation(nullAttributeRepresentation));
    }

    /**
     * Translates a raw {@link PlanResourcesResponse} with the default operator translations.
     *
     * @param <T> the entity type
     * @param response the raw {@code PlanResources} response
     * @param mapper maps each plan variable to a JPA path or relation; see
     *        {@link AttributeMapping}
     * @return a SELECT-only Specification selecting the rows the plan permits
     * @throws MalformedPlanException if the filter kind is unknown or a conditional filter has
     *         no condition. Other refusals are raised when the Specification is evaluated.
     */
    public static <T> Specification<T> toSpecification(
            PlanResourcesResponse response, Map<String, AttributeMapping> mapper) {
        return toSpecification(response, mapper, Map.of());
    }

    /**
     * Translates a raw {@link PlanResourcesResponse} with operator overrides.
     *
     * @param <T> the entity type
     * @param response the raw {@code PlanResources} response
     * @param mapper maps each plan variable to a JPA path or relation
     * @param overrides replacement translations keyed by Cerbos operator name; see
     *        {@link OperatorFunction}
     * @return a SELECT-only Specification selecting the rows the plan permits
     * @throws MalformedPlanException if the filter kind is unknown or a conditional filter has
     *         no condition. Other refusals are raised when the Specification is evaluated.
     */
    public static <T> Specification<T> toSpecification(
            PlanResourcesResponse response,
            Map<String, AttributeMapping> mapper,
            Map<String, OperatorFunction> overrides) {
        return toSpecification(
                response, mapper, overrides, NullAttributeRepresentation.EXPLICIT);
    }

    /**
     * Translates a raw {@link PlanResourcesResponse}, declaring how the caller sends NULL
     * columns to {@code check()}. Under {@link NullAttributeRepresentation#OMITTED}, null
     * comparison operands are rejected by this call.
     *
     * @param <T> the entity type
     * @param response the raw {@code PlanResources} response
     * @param mapper maps each plan variable to a JPA path or relation
     * @param overrides replacement translations keyed by Cerbos operator name
     * @param nullAttributeRepresentation the caller's NULL-column convention
     * @return a SELECT-only Specification selecting the rows the plan permits
     * @throws MalformedPlanException if the filter kind is unknown or a conditional filter has
     *         no condition
     * @throws UnsupportedPlanShapeException if the plan has a null comparison operand under
     *         {@link NullAttributeRepresentation#OMITTED}
     */
    public static <T> Specification<T> toSpecification(
            PlanResourcesResponse response,
            Map<String, AttributeMapping> mapper,
            Map<String, OperatorFunction> overrides,
            NullAttributeRepresentation nullAttributeRepresentation) {
        return toSpecification(response, Options.of(mapper)
                .withOperatorOverrides(overrides)
                .withNullAttributeRepresentation(nullAttributeRepresentation));
    }

    /** Its predicate is null, so no {@code WHERE} is emitted. Needs spring-data-jpa 3.5.2+. */
    private static <T> Specification<T> alwaysAllowed() {
        return Specification.unrestricted();
    }

    private static <T> Specification<T> alwaysDenied() {
        return (root, query, cb) -> cb.disjunction();
    }

    /**
     * Rebuilds the predicate on every invocation: a paged {@code findAll} runs a separate
     * {@code COUNT} query with its own {@code Root}, and Hibernate 6 rejects a predicate built
     * on another {@code Root}. The OMITTED scan always runs, because an attribute can declare
     * OMITTED when the call does not.
     */
    private static <T> Specification<T> conditional(Operand condition, Options options) {
        assertNoNullComparisonOperands(condition, options.mapping(),
                options.operatorOverrides(), options.nullAttributeRepresentation());
        return (root, query, cb) ->
                new PlanWalker(cb, options, isSelectInvocation(root, query))
                        .traverse(condition, Scope.root(root, query, options.mapping()));
    }

    /**
     * Rejects every null literal operand governed by {@link NullAttributeRepresentation#OMITTED},
     * where {@code check()} denies NULL rows that {@code IS NULL} would return.
     *
     * <p>It scans operands rather than a list of operators, because a null constant becomes
     * {@code IS NULL} in several places. It also rejects {@code ne(x, null)}: an enclosing
     * {@code not} could turn it back into a NULL-selecting predicate.
     */
    private static void assertNoNullComparisonOperands(
            Operand operand, Map<String, AttributeMapping> mapper,
            Map<String, OperatorFunction> overrides, NullAttributeRepresentation fallback) {
        if (operand.getNodeCase() != Operand.NodeCase.EXPRESSION) {
            return;
        }
        var expression = operand.getExpression();
        List<Operand> operands = expression.getOperandsList();

        // An attribute-vs-literal comparison uses the attribute's own declaration. Any other
        // null, e.g. inside a macro over a literal list, uses the call-level fallback.
        NullAttributeRepresentation declared =
                declaredForComparedAttribute(expression.getOperator(), operands, mapper);
        NullAttributeRepresentation governing = declared != null ? declared : fallback;
        // eq/ne of an attribute declared OMITTED against a bare null is translated three-valued
        // (never true for eq, never false for ne); an override would receive the null instead.
        if (declared == NullAttributeRepresentation.OMITTED
                && ("eq".equals(expression.getOperator()) || "ne".equals(expression.getOperator()))
                && overrides.get(expression.getOperator()) == null
                && operands.stream().anyMatch(o -> o.getNodeCase() == Operand.NodeCase.VALUE
                        && o.getValue().getKindCase() == Value.KindCase.NULL_VALUE)) {
            return;
        }
        if (governing == NullAttributeRepresentation.OMITTED
                && operands.stream().anyMatch(SpringDataQueryPlanAdapter::carriesNull)) {
            throw Refusals.nullOperandUnderOmitted(expression.getOperator());
        }
        if (declared == null) {
            operands.forEach(child ->
                    assertNoNullComparisonOperands(child, mapper, overrides, fallback));
        }
    }

    /** The operators CEL evaluates to a definite boolean over a null value. */
    private static final Set<String> EQUALITY_FAMILY = Set.of("eq", "ne", "in");

    /**
     * The declared NULL convention of the attribute in an attribute-vs-literal comparison, or
     * {@code null} for any other node.
     */
    private static NullAttributeRepresentation declaredForComparedAttribute(
            String operator, List<Operand> operands, Map<String, AttributeMapping> mapper) {
        if (!EQUALITY_FAMILY.contains(operator) || operands.size() != 2) {
            return null;
        }
        Operand left = operands.get(0);
        Operand right = operands.get(1);
        Operand variable = left;
        Operand literal = right;
        if (right.getNodeCase() == Operand.NodeCase.VARIABLE) {
            variable = right;
            literal = left;
        }
        if (variable.getNodeCase() != Operand.NodeCase.VARIABLE
                || literal.getNodeCase() != Operand.NodeCase.VALUE) {
            return null;
        }
        return mapper.get(variable.getVariable()) instanceof AttributeMapping.Field f
                ? f.nullAttributeRepresentation()
                : null;
    }

    private static boolean carriesNull(Operand operand) {
        if (operand.getNodeCase() != Operand.NodeCase.VALUE) {
            return false;
        }
        Value value = operand.getValue();
        return switch (value.getKindCase()) {
            case NULL_VALUE -> true;
            case LIST_VALUE -> value.getListValue().getValuesList().stream()
                    .anyMatch(element -> element.getKindCase() == Value.KindCase.NULL_VALUE);
            default -> false;
        };
    }

    /**
     * Whether the Specification is evaluated for a SELECT. There the {@code Root} is one of
     * {@code query.getRoots()}; in {@code delete(Specification)} it comes from a
     * {@code CriteriaDelete}, and the query is unrelated or null.
     */
    private static boolean isSelectInvocation(Root<?> root, CriteriaQuery<?> query) {
        return query != null && query.getRoots().contains(root);
    }

    /**
     * {@link #MAX_MACRO_DEPTH_PROPERTY} as an integer, or the default when unset. A bad value
     * is a configuration error, so it throws a plain {@link IllegalArgumentException}.
     */
    private static int readMaxMacroDepth() {
        String raw = System.getProperty(MAX_MACRO_DEPTH_PROPERTY);
        if (raw == null) {
            return DEFAULT_MAX_MACRO_DEPTH;
        }
        int value;
        try {
            value = Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    MAX_MACRO_DEPTH_PROPERTY + " must be a positive integer, got '" + raw + "'", e);
        }
        if (value < 1) {
            throw new IllegalArgumentException(
                    MAX_MACRO_DEPTH_PROPERTY + " must be a positive integer, got " + value);
        }
        return value;
    }
}
