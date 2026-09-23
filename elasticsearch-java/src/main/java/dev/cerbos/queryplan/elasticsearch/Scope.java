/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import java.util.Map;

/**
 * Resolves a plan variable to an Elasticsearch field. At the top level the caller's field map is
 * used; inside a lambda, {@code t.name} becomes {@code <nested path>.name}.
 */
sealed interface Scope permits Scope.Root, Scope.Lambda {

    /**
     * The Elasticsearch field {@code variable} refers to in this scope.
     *
     * @throws UnmappedAttributeException at the top level, when the field map does not name it
     * @throws MalformedPlanException inside a lambda, when it does not start with the lambda
     *         variable
     */
    String field(String variable);

    record Root(Map<String, String> fieldMap) implements Scope {
        @Override
        public String field(String variable) {
            String field = fieldMap.get(variable);
            if (field == null) {
                throw Refusals.unmapped("Unknown attribute: " + variable);
            }
            return field;
        }
    }

    /** A lambda body: {@code <lambdaVariable>.<suffix>} maps to {@code <nestedPath>.<suffix>}. */
    record Lambda(String nestedPath, String lambdaVariable) implements Scope {
        @Override
        public String field(String variable) {
            String prefix = lambdaVariable + ".";
            if (!variable.startsWith(prefix)) {
                throw Refusals.malformed("Variable '" + variable
                        + "' does not start with lambda variable '" + lambdaVariable + "'");
            }
            return nestedPath + "." + variable.substring(prefix.length());
        }
    }
}
