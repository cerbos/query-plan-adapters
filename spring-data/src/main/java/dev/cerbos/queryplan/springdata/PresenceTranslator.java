/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Join;
import jakarta.persistence.criteria.Path;
import jakarta.persistence.criteria.Predicate;

import java.util.List;

/**
 * Lowers {@code has(x)}, the presence test the planner emits from Cerbos 0.57.0 for an
 * attribute it does not fold into a guard: whether the caller's attributes carry the key at all.
 *
 * <p>Presence is a property of the NULL convention, not of the column. An attribute sent as an
 * explicit null is present on every row; one omitted when its column is NULL is present exactly
 * when the column is not NULL. A lambda element's field follows
 * {@link NullAttributeRepresentation#OMITTED} unless its mapping declares otherwise, as the
 * collection macros already assume.
 *
 * <p>{@code has()} never errors on a missing value, so the predicate is two-valued and
 * {@link TriPredicate#not} over it is exact. The one error it raises is selecting through a
 * missing to-one hop ({@code has(parent.x)} on a parentless row), which denies under both
 * polarities, so a missing hop gives UNKNOWN.
 */
final class PresenceTranslator {

    private final CriteriaBuilder cb;
    private final TriPredicate tri;
    private final NullAttributeRepresentation fallback;

    PresenceTranslator(CriteriaBuilder cb, TriPredicate tri, NullAttributeRepresentation fallback) {
        this.cb = cb;
        this.tri = tri;
        this.fallback = fallback;
    }

    Predicate handleHas(List<Operand> operands, Scope scope) {
        if (operands.size() != 1 || operands.get(0).getNodeCase() != Operand.NodeCase.VARIABLE) {
            throw Refusals.malformed("has requires exactly 1 attribute reference");
        }
        String variable = operands.get(0).getVariable();
        if (!(scope.resolve(variable) instanceof Scope.ResolvedScalar scalar)
                || !(scalar.mapping() instanceof AttributeMapping.Field field)) {
            throw Refusals.unsupported("has(" + variable + ") tests a relation: a presence test "
                    + "over one has no JPA Criteria form that matches what the caller sends for it");
        }
        Path<?> path = scope.path(variable);
        NullAttributeRepresentation convention = field.nullAttributeRepresentation() != null
                ? field.nullAttributeRepresentation()
                : isElementReference(scope, variable) ? NullAttributeRepresentation.OMITTED
                : fallback;
        Predicate present = convention == NullAttributeRepresentation.OMITTED
                ? path.isNotNull() : cb.conjunction();
        if (!(path.getParentPath() instanceof Join<?, ?> hop) || hop == scope.from()) {
            return present;
        }
        // The LEFT-joined hop the column is read through: absent, the row is UNKNOWN.
        return cb.or(cb.and(hop.isNotNull(), present), cb.and(tri.not(hop.isNotNull()), tri.unknown()));
    }

    /** Whether {@code variable} reads a field of an element bound by an enclosing lambda. */
    private static boolean isElementReference(Scope scope, String variable) {
        for (Scope s = scope; s instanceof Scope.LambdaScope lambda; s = lambda.outer()) {
            if (variable.startsWith(lambda.lambdaVar() + ".")) {
                return true;
            }
        }
        return false;
    }
}
