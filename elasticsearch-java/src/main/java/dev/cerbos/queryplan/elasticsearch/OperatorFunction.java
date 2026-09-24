/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import java.util.Map;

/**
 * A caller-supplied translation of one plan operator, used in place of the default.
 */
@FunctionalInterface
public interface OperatorFunction {
    /**
     * Build the query for one comparison.
     *
     * @param field the Elasticsearch field
     * @param value the literal operand, as a plain JDK value
     * @return the Query DSL clause
     */
    Map<String, Object> apply(String field, Object value);
}
