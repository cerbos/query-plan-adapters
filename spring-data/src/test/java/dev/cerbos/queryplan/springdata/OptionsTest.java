/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import com.google.protobuf.NullValue;
import com.google.protobuf.Value;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.queryplan.springdata.SpringDataQueryPlanAdapter.Options;
import dev.cerbos.queryplan.springdata.testmodel.ResourceEntity;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.jpa.domain.Specification;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link Options}: it copies its collections and is immutable, and the macro-depth limit
 * comes from the option, then the system property, then the default. Runs offline.
 */
class OptionsTest {

    private static final String DEPTH_PROPERTY = SpringDataQueryPlanAdapter.MAX_MACRO_DEPTH_PROPERTY;

    private static EntityManagerFactory emf;

    @BeforeAll
    static void setUp() {
        emf = Persistence.createEntityManagerFactory("translator-pu");
    }

    @AfterAll
    static void tearDown() {
        emf.close();
    }

    @Nested
    class Immutability {

        @Test
        void ofHoldsOnlyTheMapping() {
            Options options = Options.of(Corpus.MAPPING);

            assertEquals(Corpus.MAPPING, options.mapping());
            assertEquals(Map.of(), options.operatorOverrides());
            assertEquals(NullAttributeRepresentation.EXPLICIT, options.nullAttributeRepresentation());
            assertEquals(OptionalInt.empty(), options.maxMacroDepth());
        }

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

        @Test
        void eachWithReturnsANewInstanceAndLeavesTheOriginalUnchanged() {
            Options original = Options.of(Corpus.MAPPING);
            Options changed = original
                    .withOperatorOverrides(Map.of("eq", (cb, field, value) -> cb.disjunction()))
                    .withNullAttributeRepresentation(NullAttributeRepresentation.OMITTED)
                    .withMaxMacroDepth(3)
                    .withMapping(Corpus.MAPPING_WITHOUT_NULL_CONVENTIONS);

            assertNotSame(original, changed);
            assertEquals(Options.of(Corpus.MAPPING), original);
            assertEquals(1, changed.operatorOverrides().size());
            assertEquals(NullAttributeRepresentation.OMITTED, changed.nullAttributeRepresentation());
            assertEquals(OptionalInt.of(3), changed.maxMacroDepth());
            assertEquals(Corpus.MAPPING_WITHOUT_NULL_CONVENTIONS, changed.mapping());
        }

        @Test
        void everyComponentIsRequired() {
            assertThrows(NullPointerException.class, () -> Options.of(null));
            assertThrows(NullPointerException.class,
                    () -> Options.of(Corpus.MAPPING).withOperatorOverrides(null));
            assertThrows(NullPointerException.class,
                    () -> Options.of(Corpus.MAPPING).withNullAttributeRepresentation(null));
            assertThrows(NullPointerException.class,
                    () -> new Options(Corpus.MAPPING, Map.of(),
                            NullAttributeRepresentation.EXPLICIT, null));
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

    @Nested
    class MacroDepthPrecedence {

        /** Alternates subCategories and labels to build a deep join chain. */
        private static final Map<String, AttributeMapping> DEEP_MAPPER = Map.of(
                "request.resource.attr.categories", AttributeMapping.relation("categories", Map.of(
                        "name", AttributeMapping.field("name"),
                        "subCategories", AttributeMapping.relation("subCategories", Map.of(
                                "name", AttributeMapping.field("name"),
                                "labels", AttributeMapping.relation("labels", Map.of(
                                        "name", AttributeMapping.field("name"),
                                        "subCategories", AttributeMapping.relation("subCategories", Map.of(
                                                "name", AttributeMapping.field("name"),
                                                "labels", AttributeMapping.relation("labels", Map.of(
                                                        "name", AttributeMapping.field("name"),
                                                        "subCategories", AttributeMapping.relation(
                                                                "subCategories", Map.of(
                                                                        "name", AttributeMapping.field("name")
                                                                ))
                                                ))
                                        ))
                                ))
                        ))
                )));

        private static final String[] HOPS = {"subCategories", "labels", "subCategories",
                "labels", "subCategories"};

        private final Options deep = Options.of(DEEP_MAPPER);

        @Test
        void anExplicitOptionWinsOverTheProperty() {
            System.setProperty(DEPTH_PROPERTY, "6");
            try {
                assertTranslates(existsChain(2), deep.withMaxMacroDepth(2));
                IllegalArgumentException ex = assertRefused(existsChain(3), deep.withMaxMacroDepth(2));
                assertInstanceOf(UnsupportedPlanShapeException.class, ex);
                assertTrue(ex.getMessage().contains("nesting depth 3 exceeds the maximum of 2"),
                        ex.getMessage());
            } finally {
                System.clearProperty(DEPTH_PROPERTY);
            }
        }

        @Test
        void thePropertyAppliesWhenTheOptionsDeclareNothing() {
            System.setProperty(DEPTH_PROPERTY, "2");
            try {
                assertTranslates(existsChain(2), deep);
                IllegalArgumentException ex = assertRefused(existsChain(3), deep);
                assertTrue(ex.getMessage().contains("nesting depth 3 exceeds the maximum of 2"),
                        ex.getMessage());
            } finally {
                System.clearProperty(DEPTH_PROPERTY);
            }
        }

        @Test
        void theDefaultAppliesWhenNeitherIsSet() {
            System.clearProperty(DEPTH_PROPERTY);
            assertTranslates(existsChain(SpringDataQueryPlanAdapter.DEFAULT_MAX_MACRO_DEPTH), deep);
            IllegalArgumentException ex = assertRefused(
                    existsChain(SpringDataQueryPlanAdapter.DEFAULT_MAX_MACRO_DEPTH + 1), deep);
            assertTrue(ex.getMessage().contains("exceeds the maximum of "
                    + SpringDataQueryPlanAdapter.DEFAULT_MAX_MACRO_DEPTH), ex.getMessage());
        }

        @Test
        void anExplicitOptionRaisesTheLimitWithoutTheProperty() {
            System.clearProperty(DEPTH_PROPERTY);
            assertTranslates(existsChain(6), deep.withMaxMacroDepth(6));
        }

        /** The property is read each time the Specification builds a predicate. */
        @Test
        void thePropertyIsReadPerTranslation() {
            Specification<ResourceEntity> spec = SpringDataQueryPlanAdapter.toSpecification(
                    response(existsChain(3)), deep);
            System.setProperty(DEPTH_PROPERTY, "2");
            try {
                assertThrows(UnsupportedPlanShapeException.class, () -> predicateOf(spec));
                System.setProperty(DEPTH_PROPERTY, "3");
                assertNotNull(predicateOf(spec));
            } finally {
                System.clearProperty(DEPTH_PROPERTY);
            }
        }

        /** {@code categories.exists(v1, v1.subCategories.exists(v2, ... vd.name == "x"))}. */
        private Operand existsChain(int depth) {
            return existsLevel(1, depth, "request.resource.attr.categories");
        }

        private Operand existsLevel(int level, int depth, String collection) {
            String v = "v" + level;
            Operand body = level == depth
                    ? expr("eq", var(v + ".name"), string("x"))
                    : existsLevel(level + 1, depth, v + "." + HOPS[level - 1]);
            return expr("exists", var(collection), expr("lambda", body, var(v)));
        }
    }

    /** The positional overloads behave the same as the {@link Options} form. */
    @Nested
    class PositionalOverloadsDelegate {

        @Test
        void theNullRepresentationOverloadIsTheOptionsForm() {
            Operand eqNull = expr("eq", var("request.resource.attr.aOptionalString"),
                    Operand.newBuilder().setValue(
                            Value.newBuilder().setNullValue(NullValue.NULL_VALUE)).build());
            PlanResourcesResponse plan = response(eqNull);

            IllegalArgumentException positional = assertThrows(IllegalArgumentException.class,
                    () -> SpringDataQueryPlanAdapter.toSpecification(plan,
                            Corpus.MAPPING_WITHOUT_NULL_CONVENTIONS, Map.of(),
                            NullAttributeRepresentation.OMITTED));
            IllegalArgumentException viaOptions = assertThrows(IllegalArgumentException.class,
                    () -> SpringDataQueryPlanAdapter.toSpecification(plan,
                            Options.of(Corpus.MAPPING_WITHOUT_NULL_CONVENTIONS)
                                    .withNullAttributeRepresentation(
                                            NullAttributeRepresentation.OMITTED)));

            assertEquals(positional.getClass(), viaOptions.getClass());
            assertEquals(positional.getMessage(), viaOptions.getMessage());
        }

        @Test
        void theOverridesOverloadIsTheOptionsForm() {
            Map<String, OperatorFunction> overrides = Map.of("eq", (cb, field, value) -> {
                throw new IllegalStateException("override reached");
            });
            Operand condition = expr("eq", var("request.resource.attr.aString"), string("x"));

            IllegalStateException positional = assertThrows(IllegalStateException.class,
                    () -> predicateOf(SpringDataQueryPlanAdapter.toSpecification(
                            response(condition), Corpus.MAPPING, overrides)));
            IllegalStateException viaOptions = assertThrows(IllegalStateException.class,
                    () -> predicateOf(SpringDataQueryPlanAdapter.toSpecification(
                            response(condition),
                            Options.of(Corpus.MAPPING).withOperatorOverrides(overrides))));

            assertEquals(positional.getMessage(), viaOptions.getMessage());
        }
    }

    // -- helpers -----------------------------------------------------------------------------------

    private static void assertTranslates(Operand condition, Options options) {
        assertNotNull(predicateOf(SpringDataQueryPlanAdapter.toSpecification(
                response(condition), options)));
    }

    private static IllegalArgumentException assertRefused(Operand condition, Options options) {
        Specification<ResourceEntity> spec =
                SpringDataQueryPlanAdapter.toSpecification(response(condition), options);
        return assertThrows(IllegalArgumentException.class, () -> predicateOf(spec));
    }

    private static Predicate predicateOf(Specification<ResourceEntity> spec) {
        EntityManager em = emf.createEntityManager();
        try {
            CriteriaBuilder cb = em.getCriteriaBuilder();
            CriteriaQuery<String> cq = cb.createQuery(String.class);
            Root<ResourceEntity> root = cq.from(ResourceEntity.class);
            cq.select(root.get("id"));
            return spec.toPredicate(root, cq, cb);
        } finally {
            em.close();
        }
    }

    private static PlanResourcesResponse response(Operand condition) {
        return PlanResourcesResponse.newBuilder()
                .setFilter(PlanResourcesFilter.newBuilder()
                        .setKind(PlanResourcesFilter.Kind.KIND_CONDITIONAL)
                        .setCondition(condition))
                .build();
    }

    private static Operand expr(String op, Operand... operands) {
        Expression.Builder e = Expression.newBuilder().setOperator(op);
        for (Operand o : operands) {
            e.addOperands(o);
        }
        return Operand.newBuilder().setExpression(e).build();
    }

    private static Operand var(String name) {
        return Operand.newBuilder().setVariable(name).build();
    }

    private static Operand string(String v) {
        return Operand.newBuilder().setValue(Value.newBuilder().setStringValue(v)).build();
    }
}
