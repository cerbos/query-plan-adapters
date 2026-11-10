/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.jpa;

import dev.cerbos.queryplan.jpa.JpaQueryPlanAdapter.Options;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests {@link Options}: it copies its collections and rejects a non-positive macro depth. */
class OptionsTest {

    /**
     * The default null convention is the one whose wrong guess is a refusal rather than an
     * over-grant (docs/adr/0011-denial-fails-closed-in-the-result.md).
     */
    @Test
    void theDefaultNullConventionIsOmitted() {
        assertSame(NullAttributeRepresentation.OMITTED,
                Options.of(Corpus.MAPPING).nullAttributeRepresentation());
    }

    @Nested
    class Immutability {

        @Test
        void theCollectionsAreCopiedNotCaptured() {
            Map<String, AttributeMapping> mapping = new HashMap<>(Corpus.MAPPING);
            Map<String, OperatorFunction> overrides = new HashMap<>();
            Options options = Options.of(mapping).withOperatorOverrides(overrides);

            mapping.put("request.resource.attr.late", AttributeMapping.field("aString"));
            overrides.put("eq", (cb, field, value) -> cb.disjunction());

            assertFalse(options.mapping().containsKey("request.resource.attr.late"));
            assertTrue(options.operatorOverrides().isEmpty());
            assertThrows(UnsupportedOperationException.class,
                    () -> options.mapping().put("x", AttributeMapping.field("aString")));
            assertThrows(UnsupportedOperationException.class,
                    () -> options.operatorOverrides().put("eq", (cb, field, value) -> null));
        }

        /** A plain {@link IllegalArgumentException}, not a refusal type: no plan was refused. */
        @Test
        void aNonPositiveMacroDepthIsRejectedAtConstruction() {
            for (int depth : new int[] {0, -1}) {
                IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                        () -> Options.of(Corpus.MAPPING).withMaxMacroDepth(depth));
                assertSame(IllegalArgumentException.class, ex.getClass(), ex.getMessage());
                assertTrue(ex.getMessage().contains("must be a positive integer"), ex.getMessage());
            }
        }
    }
}
