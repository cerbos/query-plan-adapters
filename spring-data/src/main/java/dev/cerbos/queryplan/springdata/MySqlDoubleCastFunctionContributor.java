/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import org.hibernate.boot.model.FunctionContributions;
import org.hibernate.boot.model.FunctionContributor;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MariaDBDialect;
import org.hibernate.dialect.MySQLDialect;
import org.hibernate.type.StandardBasicTypes;

/**
 * Registers the {@value #FUNCTION_NAME} function, {@code cast(?1 as double)}, on MySQL 8.0.17+
 * so the adapter's arithmetic is evaluated in IEEE double, as the PDP does. Discovered through
 * {@code META-INF/services/org.hibernate.boot.model.FunctionContributor}.
 *
 * <p>Without it, Hibernate's {@code MySQLDialect} casts to {@code decimal(53,20)} and
 * Connector/J inlines double parameters as DECIMAL literals, so {@code 3 * 0.1 = 0.3} is TRUE
 * in SQL but FALSE in CEL.
 *
 * <p>Nothing is registered on other databases, on MySQL before 8.0.17, or on MariaDB; the
 * adapter then uses {@code cb.toDouble}. See the README, "MySQL: keeping arithmetic
 * IEEE-faithful".
 */
public final class MySqlDoubleCastFunctionContributor implements FunctionContributor {

    /**
     * Name of the registered function. Other classes may use it only as an inlined
     * compile-time constant or from Hibernate-guarded code, so this class never loads without
     * Hibernate.
     */
    static final String FUNCTION_NAME = "cerbos_ieee_double";

    @Override
    public void contributeFunctions(FunctionContributions functionContributions) {
        Dialect dialect = functionContributions.getDialect();
        if (!(dialect instanceof MySQLDialect) || dialect instanceof MariaDBDialect) {
            return;
        }
        if (!dialect.getVersion().isSameOrAfter(8, 0, 17)) {
            // CAST(... AS DOUBLE) needs MySQL 8.0.17. Without JDBC metadata access the dialect
            // reports its minimum version, so registration is skipped.
            return;
        }
        functionContributions.getFunctionRegistry()
                .patternDescriptorBuilder(FUNCTION_NAME, "cast(?1 as double)")
                .setExactArgumentCount(1)
                .setInvariantType(functionContributions.getTypeConfiguration()
                        .getBasicTypeRegistry().resolve(StandardBasicTypes.DOUBLE))
                .register();
    }

    @Override
    public int ordinal() {
        return 600; // library range (500–1000)
    }
}
