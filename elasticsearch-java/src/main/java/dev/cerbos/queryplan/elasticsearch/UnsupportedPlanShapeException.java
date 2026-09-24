/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

/**
 * The plan is valid but uses a shape the Elasticsearch Query DSL cannot express without scripts,
 * such as a field-to-field comparison or a count threshold. The adapter throws rather than emit a
 * filter that could return rows the PDP denies.
 */
public final class UnsupportedPlanShapeException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    public UnsupportedPlanShapeException(String message) {
        super(message);
    }

    public UnsupportedPlanShapeException(String message, Throwable cause) {
        super(message, cause);
    }
}
