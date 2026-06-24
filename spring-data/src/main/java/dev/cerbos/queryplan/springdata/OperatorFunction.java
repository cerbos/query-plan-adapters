package dev.cerbos.queryplan.springdata;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Expression;
import jakarta.persistence.criteria.Predicate;

/**
 * Override hook for translating a Cerbos operator + (field, value) pair into a JPA {@link Predicate}.
 * The {@code field} expression is already resolved to a typed JPA path (or join) under the current scope.
 *
 * <p>Overrides are keyed by Cerbos operator name and are consulted for every <em>scalar leaf</em>
 * translation of that operator: {@code eq}, {@code ne}, {@code lt}, {@code gt}, {@code le},
 * {@code ge}, {@code contains}, {@code startsWith}, {@code endsWith} (including the {@code add}-folded
 * forms such as {@code field == "p:" + R.id}, and the null-RHS form where {@code value} is
 * {@code null}), the bare-boolean attribute (looked up as {@code eq}), {@code isSet} (where
 * {@code value} is the {@link Boolean} flag), and the scalar {@code in} (where {@code value} is the
 * resolved value or {@link java.util.List}).
 *
 * <p>Overrides are <em>not</em> consulted for operators that translate to correlated {@code EXISTS}
 * subqueries against a {@code Relation} mapping — {@code exists}/{@code exists_one}/{@code all}/
 * {@code except}/{@code filter}, {@code hasIntersection} over a relation, {@code size(...)}, and the
 * relation form of {@code in} — because those have no single resolved (field, value) pair.
 */
@FunctionalInterface
public interface OperatorFunction {
    Predicate apply(CriteriaBuilder cb, Expression<?> field, Object value);
}
