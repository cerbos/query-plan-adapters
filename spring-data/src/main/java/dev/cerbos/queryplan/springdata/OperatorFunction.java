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
 * <p>Operand order is normalized before overrides are consulted: a value-first comparison such as
 * {@code 5 < R.attr.x} is mirrored to field-first form, so the override is looked up (and invoked)
 * under {@code gt}, matching the semantics of the predicate being built.
 *
 * <p>Arithmetic comparisons ({@code R.attr.n + 1.0 > 2.0}) also consult the override when the
 * other side of the comparison is a plan constant: the {@code field} argument is the composed
 * arithmetic SQL expression (not a bare path) and {@code value} is the constant — always a
 * {@link Double}, because the arithmetic path evaluates in IEEE double space end to end.
 *
 * <p>Overrides are <em>not</em> consulted for operators that translate to correlated {@code EXISTS}
 * subqueries against a {@code Relation} mapping — {@code exists}/{@code exists_one}/{@code all}/
 * {@code except}/{@code filter}, {@code hasIntersection} over a relation, {@code size(...)}, and the
 * relation form of {@code in} — because those have no single resolved (field, value) pair. The same
 * applies to {@code size(string)} length comparisons, field-to-field comparisons
 * ({@code R.attr.a == R.attr.b}) and arithmetic-vs-expression comparisons, where the right-hand
 * side is a column or composed expression, not a value, and to constant-receiver string matches
 * ({@code "a,b".contains(R.attr.x)}), where the COLUMN is the needle and the constant the
 * haystack — invoking a {@code contains} override there would silently invert the semantics.
 */
@FunctionalInterface
public interface OperatorFunction {
    Predicate apply(CriteriaBuilder cb, Expression<?> field, Object value);
}
