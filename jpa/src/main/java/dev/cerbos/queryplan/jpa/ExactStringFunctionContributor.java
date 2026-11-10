/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.jpa;

import org.hibernate.boot.model.FunctionContributions;
import org.hibernate.boot.model.FunctionContributor;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MariaDBDialect;
import org.hibernate.dialect.MySQLDialect;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.type.StandardBasicTypes;

/**
 * Registers the collations CEL's string semantics need, whatever the column's: on MySQL
 * 8.0.17+, {@value ExactStrings#EXACT} reads a string under {@code utf8mb4_0900_bin}, since the
 * default {@code utf8mb4_0900_ai_ci} folds case and accents; on PostgreSQL,
 * {@value ExactStrings#CODE_POINT_ORDER} orders a string under {@code "C"}, since a linguistic
 * collation does not order by code point. Discovered through
 * {@code META-INF/services/org.hibernate.boot.model.FunctionContributor}.
 */
public final class ExactStringFunctionContributor implements FunctionContributor {

    @Override
    public void contributeFunctions(FunctionContributions functionContributions) {
        Dialect dialect = functionContributions.getDialect();
        String name;
        String pattern;
        if (dialect instanceof MySQLDialect && !(dialect instanceof MariaDBDialect)
                && dialect.getVersion().isSameOrAfter(8, 0, 17)) {
            name = ExactStrings.EXACT;
            pattern = "(convert(?1 using utf8mb4) collate utf8mb4_0900_bin)";
        } else if (dialect instanceof PostgreSQLDialect) {
            name = ExactStrings.CODE_POINT_ORDER;
            pattern = "(?1 collate \"C\")";
        } else {
            return;
        }
        functionContributions.getFunctionRegistry()
                .patternDescriptorBuilder(name, pattern)
                .setExactArgumentCount(1)
                .setInvariantType(functionContributions.getTypeConfiguration()
                        .getBasicTypeRegistry().resolve(StandardBasicTypes.STRING))
                .register();
    }

    @Override
    public int ordinal() {
        return 600;
    }
}
