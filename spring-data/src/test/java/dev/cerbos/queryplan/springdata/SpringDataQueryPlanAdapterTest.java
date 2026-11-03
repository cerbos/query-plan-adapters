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
import dev.cerbos.queryplan.jpa.AttributeMapping;
import dev.cerbos.queryplan.jpa.MalformedPlanException;
import dev.cerbos.queryplan.jpa.NullAttributeRepresentation;
import dev.cerbos.queryplan.jpa.UnsupportedPlanShapeException;
import dev.cerbos.queryplan.springdata.testmodel.ResourceEntity;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Root;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.support.SimpleJpaRepository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Spring Data call contract the wrapper adds over jpa's {@code QueryPlanFilter}: an
 * always-allowed plan is {@link Specification#unrestricted()}, plan-level refusals are raised by
 * {@code toSpecification}, and {@code repository.delete(Specification)} is refused for a relation.
 * jpa's suites prove the translation itself.
 */
class SpringDataQueryPlanAdapterTest {

    private static final Map<String, AttributeMapping> MAPPER = Map.of(
            "request.resource.attr.createdBy", AttributeMapping.field("createdBy"),
            "request.resource.attr.aOptionalString", AttributeMapping.field("aOptionalString"),
            "request.resource.attr.ownedBy", AttributeMapping.relation("ownedBy"));

    private static EntityManagerFactory emf;

    @BeforeAll
    static void setUp() {
        emf = Persistence.createEntityManagerFactory("test-pu");
    }

    @AfterAll
    static void tearDown() {
        if (emf != null) emf.close();
    }

    private static PlanResourcesResponse buildResponse(PlanResourcesFilter.Kind kind, Operand cond) {
        PlanResourcesFilter.Builder b = PlanResourcesFilter.newBuilder().setKind(kind);
        if (cond != null) b.setCondition(cond);
        return PlanResourcesResponse.newBuilder().setFilter(b).build();
    }

    private static Operand exprOp(String op, Operand... operands) {
        Expression.Builder e = Expression.newBuilder().setOperator(op);
        for (Operand o : operands) e.addOperands(o);
        return Operand.newBuilder().setExpression(e).build();
    }

    private static Operand var(String name) {
        return Operand.newBuilder().setVariable(name).build();
    }

    private static Operand sval(String v) {
        return Operand.newBuilder().setValue(Value.newBuilder().setStringValue(v)).build();
    }

    private static Specification<ResourceEntity> spec(Operand condition) {
        return SpringDataQueryPlanAdapter.toSpecification(
                buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition), MAPPER);
    }

    /** Persist {@code entity}, run {@code body}, then always delete the row again. */
    private static void withResource(ResourceEntity entity, Runnable body) {
        EntityManager em = emf.createEntityManager();
        em.getTransaction().begin();
        em.persist(entity);
        em.getTransaction().commit();
        em.close();
        try {
            body.run();
        } finally {
            EntityManager cleanup = emf.createEntityManager();
            cleanup.getTransaction().begin();
            ResourceEntity managed = cleanup.find(ResourceEntity.class, entity.getId());
            if (managed != null) {
                cleanup.remove(managed);
            }
            cleanup.getTransaction().commit();
            cleanup.close();
        }
    }

    /** A null predicate makes Spring Data omit the WHERE clause. */
    @Test
    void alwaysAllowedIsUnrestricted() {
        Specification<ResourceEntity> spec = SpringDataQueryPlanAdapter.toSpecification(
                buildResponse(PlanResourcesFilter.Kind.KIND_ALWAYS_ALLOWED, null), MAPPER);
        EntityManager em = emf.createEntityManager();
        try {
            CriteriaBuilder cb = em.getCriteriaBuilder();
            CriteriaQuery<ResourceEntity> cq = cb.createQuery(ResourceEntity.class);
            Root<ResourceEntity> root = cq.from(ResourceEntity.class);
            assertNull(spec.toPredicate(root, cq, cb));
        } finally {
            em.close();
        }
    }

    @Test
    void alwaysDeniedSelectsNothing() {
        withResource(new ResourceEntity("denied-1"), () -> {
            Specification<ResourceEntity> spec = SpringDataQueryPlanAdapter.toSpecification(
                    buildResponse(PlanResourcesFilter.Kind.KIND_ALWAYS_DENIED, null), MAPPER);
            EntityManager em = emf.createEntityManager();
            try {
                SimpleJpaRepository<ResourceEntity, String> repository =
                        new SimpleJpaRepository<>(ResourceEntity.class, em);
                assertEquals(0, repository.count(spec));
                assertEquals(1, repository.count());
            } finally {
                em.close();
            }
        });
    }

    /** Refusals that depend on the plan alone are raised by toSpecification, not on first use. */
    @Test
    void planLevelRefusalsAreRaisedByToSpecification() {
        assertThrows(MalformedPlanException.class, () -> SpringDataQueryPlanAdapter.toSpecification(
                buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, null), MAPPER));
        Operand nullEq = exprOp("eq", var("request.resource.attr.aOptionalString"),
                Operand.newBuilder().setValue(
                        Value.newBuilder().setNullValue(NullValue.NULL_VALUE)).build());
        UnsupportedPlanShapeException ex = assertThrows(UnsupportedPlanShapeException.class,
                () -> SpringDataQueryPlanAdapter.toSpecification(
                        buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, nullEq),
                        MAPPER, Map.of(), NullAttributeRepresentation.OMITTED));
        assertTrue(ex.getMessage().contains("NullAttributeRepresentation.OMITTED"), ex.getMessage());
    }

    /**
     * {@code repository.delete(Specification)} guard. Hibernate's bulk delete first clears the
     * collection tables using the same predicate, which breaks a correlated subquery over them:
     * the entity survives and its collection rows are deleted. So a relation-mapped Specification
     * throws {@link UnsupportedOperationException} before any statement runs.
     */
    @Nested
    class BulkDeleteGuard {

        // The plan for `P.id in R.attr.ownedBy`: the folded principal id comes first.
        private final Operand ownedByUser1 =
                exprOp("in", sval("user1"), var("request.resource.attr.ownedBy"));

        /**
         * {@code delete(Specification)} exists in Spring Data JPA 3.x only; 4.0 removed it. It is
         * looked up by reflection so this source compiles on both, and tests that need it are
         * skipped on 4.x.
         */
        private static java.lang.reflect.Method specificationDelete() {
            try {
                return SimpleJpaRepository.class.getMethod("delete", Specification.class);
            } catch (NoSuchMethodException e) {
                return null;
            }
        }

        /** Skips the test on Spring Data JPA 4.x. Call it before any assertThrows. */
        private static void assumeSpecificationDeleteIsOnTheClasspath() {
            org.junit.jupiter.api.Assumptions.assumeTrue(specificationDelete() != null,
                    "JpaSpecificationExecutor.delete(Specification) is a Spring Data JPA 3.x "
                            + "method and is not on this classpath");
        }

        private static long deleteBySpecification(
                SimpleJpaRepository<ResourceEntity, String> repository,
                Specification<ResourceEntity> spec) {
            assumeSpecificationDeleteIsOnTheClasspath();
            java.lang.reflect.Method delete = specificationDelete();
            try {
                return (long) delete.invoke(repository, spec);
            } catch (java.lang.reflect.InvocationTargetException e) {
                if (e.getCause() instanceof RuntimeException runtime) {
                    throw runtime;
                }
                if (e.getCause() instanceof Error error) {
                    throw error;
                }
                throw new IllegalStateException(e.getCause());
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
        }

        @Test
        void deleteWithRelationSpecThrowsBeforeAnyDeletion() {
            // Must run before assertThrows, which would report the skip as a wrong exception.
            assumeSpecificationDeleteIsOnTheClasspath();
            ResourceEntity r = new ResourceEntity("bulk-del-1");
            r.setOwnedBy(new ArrayList<>(List.of("user1", "user2")));
            withResource(r, () -> {
                Specification<ResourceEntity> spec = spec(ownedByUser1);
                EntityManager em = emf.createEntityManager();
                try {
                    SimpleJpaRepository<ResourceEntity, String> repository =
                            new SimpleJpaRepository<>(ResourceEntity.class, em);

                    // SELECTs are unaffected.
                    assertEquals(1, repository.findAll(spec).size());
                    assertEquals(1, repository.count(spec));

                    em.getTransaction().begin();
                    try {
                        UnsupportedOperationException ex = assertThrows(
                                UnsupportedOperationException.class,
                                () -> deleteBySpecification(repository, spec));
                        assertTrue(ex.getMessage().contains("ownedBy"),
                                "message should name the relation, was: " + ex.getMessage());
                        assertTrue(ex.getMessage().contains("SELECT"),
                                "message should state the SELECT-only contract, was: " + ex.getMessage());
                        assertTrue(ex.getMessage().contains("deleteAllById"),
                                "message should point at the safe alternative, was: " + ex.getMessage());
                    } finally {
                        em.getTransaction().rollback();
                    }
                } finally {
                    em.close();
                }

                // Nothing was deleted: the entity and its collection rows survive.
                EntityManager check = emf.createEntityManager();
                try {
                    ResourceEntity reloaded = check.find(ResourceEntity.class, "bulk-del-1");
                    assertNotNull(reloaded, "entity row must survive");
                    assertEquals(Set.of("user1", "user2"), Set.copyOf(reloaded.getOwnedBy()),
                            "collection rows must survive untouched");
                } finally {
                    check.close();
                }
            });
        }

        @Test
        void deleteWithFieldOnlySpecStillDeletes() {
            // A field-only predicate has no subquery, so the guard must allow it.
            ResourceEntity r = new ResourceEntity("bulk-del-2");
            r.setCreatedBy("alice");
            withResource(r, () -> {
                Specification<ResourceEntity> spec =
                        spec(exprOp("eq", var("request.resource.attr.createdBy"), sval("alice")));
                EntityManager em = emf.createEntityManager();
                try {
                    SimpleJpaRepository<ResourceEntity, String> repository =
                            new SimpleJpaRepository<>(ResourceEntity.class, em);
                    em.getTransaction().begin();
                    long deleted = deleteBySpecification(repository, spec);
                    em.getTransaction().commit();
                    assertEquals(1, deleted);
                } finally {
                    em.close();
                }
                EntityManager check = emf.createEntityManager();
                try {
                    assertNull(check.find(ResourceEntity.class, "bulk-del-2"),
                            "field-only delete(Specification) must still work");
                } finally {
                    check.close();
                }
            });
        }
    }
}
