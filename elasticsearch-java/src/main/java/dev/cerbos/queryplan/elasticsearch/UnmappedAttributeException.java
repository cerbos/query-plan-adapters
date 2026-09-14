/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

/**
 * The plan names an attribute the caller's declarations do not cover: a variable absent from
 * {@link ElasticsearchQueryPlanAdapter.Options#fieldMap()}, or a collection a macro walks that is
 * not declared in {@link ElasticsearchQueryPlanAdapter.Options#nestedPaths()}.
 *
 * <p>The adapter is handed a plan, never an index mapping, so it cannot look the field up itself
 * and it never uses a plan variable verbatim as a field name. This is a configuration gap on the
 * caller's side rather than a limitation of the Query DSL, which is why it is a type of its own.
 */
public final class UnmappedAttributeException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    public UnmappedAttributeException(String message) {
        super(message);
    }
}
