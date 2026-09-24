/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import jakarta.persistence.criteria.CriteriaBuilder;

/**
 * Checks whether the {@code cerbos_ieee_double} function from
 * {@link MySqlDoubleCastFunctionContributor} is registered. Hibernate types are touched only in
 * {@link Probe}, which loads only when {@code hibernate-core} is on the classpath; on other JPA
 * providers {@link #isRegistered} is always {@code false}.
 */
final class IeeeDoubleCast {

    private IeeeDoubleCast() {}

    private static final boolean HIBERNATE_PRESENT = detectHibernate();

    private static boolean detectHibernate() {
        try {
            Class.forName("org.hibernate.query.sqm.NodeBuilder", false,
                    IeeeDoubleCast.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException | LinkageError e) {
            return false;
        }
    }

    /** Whether {@code cb}'s session factory has the function registered. */
    static boolean isRegistered(CriteriaBuilder cb) {
        return HIBERNATE_PRESENT && Probe.isRegistered(cb);
    }

    /** The only code that references Hibernate types. */
    private static final class Probe {
        static boolean isRegistered(CriteriaBuilder cb) {
            return cb instanceof org.hibernate.query.sqm.NodeBuilder nb
                    && nb.getQueryEngine().getSqmFunctionRegistry().findFunctionDescriptor(
                            MySqlDoubleCastFunctionContributor.FUNCTION_NAME) != null;
        }
    }
}
