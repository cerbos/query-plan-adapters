/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.jpa;

import org.hibernate.boot.model.FunctionContributions;
import org.hibernate.boot.model.FunctionContributor;
import org.hibernate.dialect.H2Dialect;
import org.hibernate.type.StandardBasicTypes;

/**
 * Registers the {@value #FUNCTION_NAME} function on H2, whose {@code CHAR_LENGTH} counts UTF-16
 * code units where CEL's {@code size()} counts code points. Each astral character is replaced by
 * one unit before counting. Discovered through
 * {@code META-INF/services/org.hibernate.boot.model.FunctionContributor}.
 */
public final class H2CodePointLengthFunctionContributor implements FunctionContributor {

    /** Name of the registered function; see {@link MySqlDoubleCastFunctionContributor#FUNCTION_NAME}. */
    static final String FUNCTION_NAME = "cerbos_code_point_length";

    @Override
    public void contributeFunctions(FunctionContributions functionContributions) {
        if (!(functionContributions.getDialect() instanceof H2Dialect)) {
            return;
        }
        functionContributions.getFunctionRegistry()
                .patternDescriptorBuilder(FUNCTION_NAME,
                        "char_length(regexp_replace(?1, '[\\x{10000}-\\x{10FFFF}]', '_'))")
                .setExactArgumentCount(1)
                .setInvariantType(functionContributions.getTypeConfiguration()
                        .getBasicTypeRegistry().resolve(StandardBasicTypes.INTEGER))
                .register();
    }

    @Override
    public int ordinal() {
        return 600;
    }
}
