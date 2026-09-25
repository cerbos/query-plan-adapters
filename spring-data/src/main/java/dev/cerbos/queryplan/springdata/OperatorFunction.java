/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Expression;
import jakarta.persistence.criteria.Predicate;

/**
 * Replaces the default translation of one Cerbos operator applied to a resolved
 * (field, value) pair. Overrides are keyed by operator name.
 *
 * <p>An override is consulted for:
 * <ul>
 *   <li>scalar {@code eq}, {@code ne}, {@code lt}, {@code gt}, {@code le}, {@code ge},
 *       {@code contains}, {@code startsWith} and {@code endsWith}, including {@code add}-folded
 *       forms and null values ({@code IS NULL} arrives as {@code eq} against {@code null});</li>
 *   <li>a bare boolean attribute, as {@code eq} with {@code true};</li>
 *   <li>scalar {@code in}, where {@code value} is the value or a {@link java.util.List};</li>
 *   <li>arithmetic compared with a constant, where {@code field} is the arithmetic expression
 *       and {@code value} a {@link Double};</li>
 *   <li>{@code string(boolColumn) == "true"} or {@code "false"}, as {@code eq}/{@code ne} with
 *       a {@link Boolean}, and {@code string(stringColumn) == "text"} with the {@link String};</li>
 *   <li>{@code matches}, and a {@code timestamp()} comparison over a column type the default
 *       translation rejects (with a {@link java.time.Instant} value), before the adapter
 *       refuses them.</li>
 * </ul>
 *
 * <p>A value-first comparison is mirrored first, so {@code 5 < R.attr.x} is looked up as
 * {@code gt}. {@code not} wraps the built predicate, so an override applies under both
 * polarities.
 *
 * <p>An override is not consulted where there is no single (field, value) pair: relation
 * operators ({@code exists}, {@code all}, {@code size(...)}, relation {@code in} and
 * {@code hasIntersection}, and so on), field-to-field comparisons, and constant-receiver string
 * matches such as {@code "a,b".contains(R.attr.x)}. Scalar {@code hasIntersection} is built as
 * {@code IN} directly and does not use the {@code in} override. The README's
 * "Not yet supported" table lists what else is refused before any lookup.
 */
@FunctionalInterface
public interface OperatorFunction {
    /**
     * @param cb    the query's criteria builder
     * @param field the resolved column or expression
     * @param value the constant operand
     * @return the predicate to use in place of the default translation
     */
    Predicate apply(CriteriaBuilder cb, Expression<?> field, Object value);
}
