/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.queryplan.jpa.AttributeMapping;
import dev.cerbos.queryplan.jpa.JpaQueryPlanAdapter;
import dev.cerbos.queryplan.jpa.JpaQueryPlanAdapter.Options;
import dev.cerbos.queryplan.jpa.MalformedPlanException;
import dev.cerbos.queryplan.jpa.NullAttributeRepresentation;
import dev.cerbos.queryplan.jpa.OperatorFunction;
import dev.cerbos.queryplan.jpa.QueryPlanFilter;
import dev.cerbos.queryplan.jpa.UnmappedAttributeException;
import dev.cerbos.queryplan.jpa.UnsupportedPlanShapeException;
import dev.cerbos.sdk.PlanResourcesResult;

import org.springframework.data.jpa.domain.Specification;

import java.util.Map;

/**
 * Translates a Cerbos {@code PlanResources} response into a Spring Data JPA
 * {@link Specification} for a {@code JpaSpecificationExecutor}. The translation is
 * {@link JpaQueryPlanAdapter}'s; this class adapts its {@link QueryPlanFilter} to Spring Data.
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
        return of(JpaQueryPlanAdapter.toFilter(planResult, options));
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
        return of(JpaQueryPlanAdapter.toFilter(response, options));
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
                planResult, mapper, overrides, NullAttributeRepresentation.OMITTED);
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
                response, mapper, overrides, NullAttributeRepresentation.OMITTED);
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

    /**
     * An always-allowed filter becomes {@link Specification#unrestricted()} (spring-data-jpa
     * 3.5.2+), whose null predicate omits the {@code WHERE} clause. Any other filter builds its
     * predicate on every invocation: a paged {@code findAll} runs a separate {@code COUNT} query
     * with its own {@code Root}.
     */
    private static <T> Specification<T> of(QueryPlanFilter filter) {
        if (filter.isAlwaysAllowed()) {
            return Specification.unrestricted();
        }
        return filter::toPredicate;
    }
}
