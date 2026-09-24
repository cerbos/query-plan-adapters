/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

/**
 * The plan uses an attribute the caller's {@link ElasticsearchQueryPlanAdapter.Options} do not
 * declare: a variable missing from the field map, a collection missing from the nested paths, or a
 * compared field with no scalar type. Fix the options; the policy is fine.
 */
public final class UnmappedAttributeException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    public UnmappedAttributeException(String message) {
        super(message);
    }
}
