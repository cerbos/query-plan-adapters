/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

/**
 * The plan uses an attribute in a way the caller's {@link AttributeMapping mapping} does not
 * cover: a variable the mapping does not name, a {@link AttributeMapping.Relation} where a
 * scalar column is needed or a {@link AttributeMapping.Field} where a relation is needed, a
 * temporal column whose Java type does not pin an absolute instant, or two sides of one
 * comparison declared under different NULL conventions.
 *
 * <p>The fix is a mapping change, not a policy rewrite.
 */
public final class UnmappedAttributeException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    public UnmappedAttributeException(String message) {
        super(message);
    }
}
