/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

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
 * {@code null} — the planner has no existence operator, so {@code IS NULL} / {@code IS NOT NULL}
 * arrive as {@code eq} / {@code ne} against a null value), the bare-boolean attribute (looked up as
 * {@code eq}), and the scalar {@code in} (where {@code value} is the resolved value or
 * {@link java.util.List}).
 *
 * <p>Operand order is normalized before overrides are consulted: a value-first comparison such as
 * {@code 5 < R.attr.x} is mirrored to field-first form, so the override is looked up (and invoked)
 * under {@code gt}, matching the semantics of the predicate being built.
 *
 * <p>Arithmetic comparisons ({@code R.attr.n + 1.0 > 2.0}) also consult the override when the
 * other side of the comparison is a plan constant: the {@code field} argument is the composed
 * arithmetic SQL expression (not a bare path) and {@code value} is the constant — always a
 * {@link Double}, because the arithmetic path evaluates in IEEE double space end to end.
 * {@code string()} over a boolean column compared with {@code "true"} or {@code "false"}
 * ({@code string(R.attr.flag) == "true"}) consults the {@code eq}/{@code ne} override as the
 * bare boolean attribute does: {@code field} is the column and {@code value} the
 * {@link Boolean} the constant names. Any other string constant matches no boolean's text,
 * so that comparison is decided without an override.
 *
 * <p>Overrides are <em>not</em> consulted for operators that translate to correlated {@code EXISTS}
 * subqueries against a {@code Relation} mapping — {@code exists}/{@code exists_one}/{@code all}/
 * {@code except}/{@code filter}, {@code hasIntersection} over a relation, {@code size(...)}, the
 * relation form of {@code in} and the attribute-in-attribute form
 * ({@code R.attr.x in R.attr.coll}) — because those have no single resolved (field, value) pair.
 * The same applies to {@code size(string)} length comparisons, field-to-field comparisons
 * ({@code R.attr.a == R.attr.b}) and arithmetic-vs-expression comparisons, where the right-hand
 * side is a column or composed expression, not a value, and to constant-receiver string matches
 * ({@code "a,b".contains(R.attr.x)}), where the COLUMN is the needle and the constant the
 * haystack — invoking a {@code contains} override there would silently invert the semantics.
 * Nor is {@code hasIntersection} over a scalar {@code Field} mapping consulted: it is built as
 * {@code path IN (values)} directly, not through the {@code in} hook, so an {@code in} override
 * does not reach it (the README's "Database collation requirements" section says why that
 * matters). Negation never changes any of this: {@code not} is applied around the built
 * predicate, so an override reaches its operator under both polarities.
 *
 * <p>An operator with no default translation — {@code matches} is the policy-reachable one —
 * is consulted under its own name before the adapter refuses it with
 * {@link UnsupportedPlanShapeException}, as is a {@code timestamp()} comparison over a column
 * type the default translation rejects (the override receives the parsed
 * {@link java.time.Instant}); the constructs the README's "Not yet supported" table marks as
 * not overridable are refused while an operand is being resolved, before any lookup.
 */
@FunctionalInterface
public interface OperatorFunction {
    Predicate apply(CriteriaBuilder cb, Expression<?> field, Object value);
}
