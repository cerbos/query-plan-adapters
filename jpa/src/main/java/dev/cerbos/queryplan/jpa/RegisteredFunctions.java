/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.jpa;

import jakarta.persistence.criteria.CriteriaBuilder;

/**
 * Checks whether a function from one of this library's {@code FunctionContributor}s is
 * registered. Hibernate types are touched only in {@link Probe}, which loads only when
 * {@code hibernate-core} is on the classpath; on other JPA providers {@link #isRegistered} is
 * always {@code false}.
 */
final class RegisteredFunctions {

    private RegisteredFunctions() {}

    private static final boolean HIBERNATE_PRESENT = detectHibernate();

    private static boolean detectHibernate() {
        try {
            Class.forName("org.hibernate.query.sqm.NodeBuilder", false,
                    RegisteredFunctions.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException | LinkageError e) {
            return false;
        }
    }

    /** Whether {@code cb}'s session factory has {@code name} registered. */
    static boolean isRegistered(CriteriaBuilder cb, String name) {
        return HIBERNATE_PRESENT && Probe.isRegistered(cb, name);
    }

    /** The only code that references Hibernate types. */
    private static final class Probe {
        static boolean isRegistered(CriteriaBuilder cb, String name) {
            return cb instanceof org.hibernate.query.sqm.NodeBuilder nb
                    && nb.getQueryEngine().getSqmFunctionRegistry()
                            .findFunctionDescriptor(name) != null;
        }
    }
}
