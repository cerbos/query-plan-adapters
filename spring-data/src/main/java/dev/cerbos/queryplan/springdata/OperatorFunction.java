package dev.cerbos.queryplan.springdata;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Expression;
import jakarta.persistence.criteria.Predicate;

/**
 * Override hook for translating a Cerbos operator + (field, value) pair into a JPA {@link Predicate}.
 * The {@code field} expression is already resolved to a typed JPA path (or join) under the current scope.
 */
@FunctionalInterface
public interface OperatorFunction {
    Predicate apply(CriteriaBuilder cb, Expression<?> field, Object value);
}
