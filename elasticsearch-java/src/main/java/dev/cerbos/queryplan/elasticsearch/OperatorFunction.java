/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import java.util.Map;

@FunctionalInterface
public interface OperatorFunction {
    Map<String, Object> apply(String field, Object value);
}
