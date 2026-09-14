/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import java.util.Map;

/**
 * Where in the plan a variable is being resolved, and therefore which Elasticsearch field it
 * denotes.
 *
 * <p>At the top level a variable is looked up in the caller's field map. Inside a collection
 * macro's lambda it is the iteration variable plus a suffix ({@code t.name}), and it denotes a
 * field of the nested document the macro walks ({@code <nested path>.name}). The two rules are
 * the only thing that differed between the adapter's unscoped and lambda-scoped traversals, so
 * they live here and the walk is written once.
 */
sealed interface Scope permits Scope.Root, Scope.Lambda {

    /**
     * The Elasticsearch field {@code variable} denotes in this scope.
     *
     * @throws UnmappedAttributeException at the top level, when the field map does not name it
     * @throws MalformedPlanException inside a lambda, when it is not prefixed by the iteration
     *         variable
     */
    String field(String variable);

    /** The top-level scope: every variable resolves through the caller's field map. */
    static Scope root(Map<String, String> fieldMap) {
        return new Root(fieldMap);
    }

    /** The scope of a lambda body: {@code <lambdaVariable>.<suffix>} is {@code <nestedPath>.<suffix>}. */
    static Scope lambda(String nestedPath, String lambdaVariable) {
        return new Lambda(nestedPath, lambdaVariable);
    }

    /** The top-level scope. */
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

    /** The scope inside a collection macro's lambda over a nested path. */
    record Lambda(String nestedPath, String lambdaVariable) implements Scope {
        @Override
        public String field(String variable) {
            String suffix = extractLambdaSuffix(variable, lambdaVariable);
            return nestedPath + "." + suffix;
        }
    }

    /** The part of {@code variable} after {@code <lambdaVar>.}, or a refusal when it has none. */
    static String extractLambdaSuffix(String variable, String lambdaVar) {
        String prefix = lambdaVar + ".";
        if (!variable.startsWith(prefix)) {
            throw Refusals.malformed(
                    "Variable '" + variable + "' does not start with lambda variable '" + lambdaVar + "'");
        }
        return variable.substring(prefix.length());
    }
}
