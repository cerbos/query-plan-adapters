/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.jpa;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Expression;

import java.util.Set;

/**
 * Puts the string side of a comparison under the collation CEL's semantics need, where
 * {@link ExactStringFunctionContributor} registered one: byte-exact on MySQL, code-point order
 * for an ordering on PostgreSQL. One collated side decides the comparison's collation, so the
 * other side, often a bound value, is left alone. Elsewhere the expression is returned as is.
 */
final class ExactStrings {

    static final String EXACT = "cerbos_exact_string";
    static final String CODE_POINT_ORDER = "cerbos_code_point_order";

    private static final Set<String> ORDERINGS = Set.of("lt", "gt", "le", "ge");

    private ExactStrings() {}

    /** {@code e} for {@code ==}, {@code in} or a string match. */
    static <T> Expression<T> exact(CriteriaBuilder cb, Expression<T> e) {
        return wrap(cb, EXACT, e);
    }

    /** {@code e} for {@code op}: an ordering also runs in code-point order. */
    static <T> Expression<T> forOperator(CriteriaBuilder cb, String op, Expression<T> e) {
        if (ORDERINGS.contains(op) && isString(e)
                && RegisteredFunctions.isRegistered(cb, CODE_POINT_ORDER)) {
            return wrap(cb, CODE_POINT_ORDER, e);
        }
        return exact(cb, e);
    }

    private static boolean isString(Expression<?> e) {
        return String.class.equals(e.getJavaType());
    }

    @SuppressWarnings("unchecked")
    private static <T> Expression<T> wrap(CriteriaBuilder cb, String function, Expression<T> e) {
        if (!isString(e) || !RegisteredFunctions.isRegistered(cb, function)) {
            return e;
        }
        return (Expression<T>) cb.function(function, String.class, e);
    }
}
