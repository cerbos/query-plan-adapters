/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

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
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Truth tables for {@link TriPredicate}, run against one H2 row with hand-built TRUE, FALSE and
 * UNKNOWN predicates. A count of 0 under both the predicate and its negation means UNKNOWN, which
 * must deny either way. Runs offline.
 */
class TriPredicateTest {

    private static final String SEED_ID = "tri-predicate-seed";

    private static EntityManagerFactory emf;

    @BeforeAll
    static void setUp() {
        emf = Persistence.createEntityManagerFactory("test-pu");
        EntityManager em = emf.createEntityManager();
        em.getTransaction().begin();
        ResourceEntity seed = new ResourceEntity(SEED_ID);
        seed.setaString("seed");
        seed.setaOptionalString(null);
        em.persist(seed);
        em.getTransaction().commit();
        em.close();
    }

    @AfterAll
    static void tearDown() {
        if (emf == null) {
            return;
        }
        EntityManager em = emf.createEntityManager();
        em.getTransaction().begin();
        ResourceEntity seed = em.find(ResourceEntity.class, SEED_ID);
        if (seed != null) {
            em.remove(seed);
        }
        em.getTransaction().commit();
        em.close();
        emf.close();
    }

    @FunctionalInterface
    private interface PredicateFactory {
        Predicate build(CriteriaBuilder cb, TriPredicate tri, Root<ResourceEntity> root);
    }

    /** 1 when the predicate is TRUE for the seed row, 0 when FALSE or UNKNOWN. */
    private static int count(PredicateFactory factory) {
        EntityManager em = emf.createEntityManager();
        try {
            CriteriaBuilder cb = em.getCriteriaBuilder();
            TriPredicate tri = new TriPredicate(cb);
            CriteriaQuery<Long> cq = cb.createQuery(Long.class);
            Root<ResourceEntity> root = cq.from(ResourceEntity.class);
            cq.select(cb.count(root));
            cq.where(factory.build(cb, tri, root));
            return em.createQuery(cq).getSingleResult().intValue();
        } finally {
            em.close();
        }
    }

    /** {@link #count} under {@code tri.not(...)}. */
    private static int countNegated(PredicateFactory factory) {
        return count((cb, tri, root) -> tri.not(factory.build(cb, tri, root)));
    }

    // -- primitive predicates over the seed row --

    private static Predicate knownTrue(CriteriaBuilder cb, Root<ResourceEntity> root) {
        return cb.equal(root.get("aString"), "seed");
    }

    private static Predicate knownFalse(CriteriaBuilder cb, Root<ResourceEntity> root) {
        return cb.equal(root.get("aString"), "something-else");
    }

    /** UNKNOWN: a comparison against a NULL column. */
    private static Predicate unknownLeaf(CriteriaBuilder cb, Root<ResourceEntity> root) {
        return cb.equal(root.get("aOptionalString"), "anything");
    }

    // -- unknown(): the UNKNOWN constant --

    @Test
    void unknownConstantExcludedUnderBothPolarities() {
        assertEquals(0, count((cb, tri, root) -> tri.unknown()));
        assertEquals(0, countNegated((cb, tri, root) -> tri.unknown()));
    }

    @Test
    void nullDerivedLeafExcludedUnderBothPolarities() {
        // Checks the fixture: the NULL comparison is UNKNOWN, not FALSE.
        assertEquals(0, count((cb, tri, root) -> unknownLeaf(cb, root)));
        assertEquals(0, countNegated((cb, tri, root) -> unknownLeaf(cb, root)));
    }

    // -- not(): the junction barrier --

    @Test
    void notFlipsKnownPredicates() {
        assertEquals(0, count((cb, tri, root) -> tri.not(knownTrue(cb, root))));
        assertEquals(1, count((cb, tri, root) -> tri.not(knownFalse(cb, root))));
    }

    @Test
    void doubleNegationComposesThroughJunctionBarrier() {
        // Hibernate 6 collapses cb.not(cb.not(eq)) to a single NOT. tri.not must not.
        assertEquals(1, count((cb, tri, root) -> tri.not(tri.not(knownTrue(cb, root)))));
        assertEquals(0, count((cb, tri, root) -> tri.not(tri.not(knownFalse(cb, root)))));
        assertEquals(0, count((cb, tri, root) -> tri.not(tri.not(tri.not(knownTrue(cb, root))))));
    }

    // -- determined(): the two-polarity determinedness probe --

    @Test
    void determinedIsTrueForKnownBodiesAndUnknownForUnknownBodies() {
        assertEquals(1, count((cb, tri, root) -> tri.determined(() -> knownTrue(cb, root))));
        assertEquals(1, count((cb, tri, root) -> tri.determined(() -> knownFalse(cb, root))));
        // UNKNOWN OR NOT UNKNOWN is UNKNOWN.
        assertEquals(0, count((cb, tri, root) -> tri.determined(() -> unknownLeaf(cb, root))));
        assertEquals(0, countNegated((cb, tri, root) -> tri.determined(() -> unknownLeaf(cb, root))));
    }

    // -- ternary(): condition-unknown arm --

    @Test
    void ternaryWithKnownConditionSelectsTheBranch() {
        // TRUE condition: the then-branch decides.
        assertEquals(1, count((cb, tri, root) -> tri.ternary(
                () -> knownTrue(cb, root), () -> knownTrue(cb, root), () -> knownFalse(cb, root))));
        assertEquals(0, count((cb, tri, root) -> tri.ternary(
                () -> knownTrue(cb, root), () -> knownFalse(cb, root), () -> knownTrue(cb, root))));
        assertEquals(1, countNegated((cb, tri, root) -> tri.ternary(
                () -> knownTrue(cb, root), () -> knownFalse(cb, root), () -> knownTrue(cb, root))));
        // FALSE condition: the else-branch decides.
        assertEquals(1, count((cb, tri, root) -> tri.ternary(
                () -> knownFalse(cb, root), () -> knownFalse(cb, root), () -> knownTrue(cb, root))));
        assertEquals(0, count((cb, tri, root) -> tri.ternary(
                () -> knownFalse(cb, root), () -> knownTrue(cb, root), () -> knownFalse(cb, root))));
        assertEquals(1, countNegated((cb, tri, root) -> tri.ternary(
                () -> knownFalse(cb, root), () -> knownTrue(cb, root), () -> knownFalse(cb, root))));
    }

    @Test
    void ternaryWithUnknownConditionIsUnknownNotFalse() {
        // Without the unknown arm this would be FALSE, and NOT would admit the row.
        assertEquals(0, count((cb, tri, root) -> tri.ternary(
                () -> unknownLeaf(cb, root), () -> knownTrue(cb, root), () -> knownTrue(cb, root))));
        assertEquals(0, countNegated((cb, tri, root) -> tri.ternary(
                () -> unknownLeaf(cb, root), () -> knownTrue(cb, root), () -> knownTrue(cb, root))));
    }

    // -- baseUnlessUnknown(): the map-intersection strict table --

    @Test
    void baseUnlessUnknownWithFalseBaseAndUnknownWitnessIsUnknownNotFalse() {
        // Must be UNKNOWN, not FALSE, or NOT(...) would admit rows the PDP denies.
        assertEquals(0, count((cb, tri, root) ->
                tri.baseUnlessUnknown(knownFalse(cb, root), () -> knownTrue(cb, root))));
        assertEquals(0, countNegated((cb, tri, root) ->
                tri.baseUnlessUnknown(knownFalse(cb, root), () -> knownTrue(cb, root))));
    }

    @Test
    void baseUnlessUnknownTruthTable() {
        // No unknown witness: the base passes through.
        assertEquals(1, count((cb, tri, root) ->
                tri.baseUnlessUnknown(knownTrue(cb, root), () -> knownFalse(cb, root))));
        assertEquals(0, countNegated((cb, tri, root) ->
                tri.baseUnlessUnknown(knownTrue(cb, root), () -> knownFalse(cb, root))));
        assertEquals(0, count((cb, tri, root) ->
                tri.baseUnlessUnknown(knownFalse(cb, root), () -> knownFalse(cb, root))));
        assertEquals(1, countNegated((cb, tri, root) ->
                tri.baseUnlessUnknown(knownFalse(cb, root), () -> knownFalse(cb, root))));
        // An unknown witness makes even a TRUE base UNKNOWN.
        assertEquals(0, count((cb, tri, root) ->
                tri.baseUnlessUnknown(knownTrue(cb, root), () -> knownTrue(cb, root))));
        assertEquals(0, countNegated((cb, tri, root) ->
                tri.baseUnlessUnknown(knownTrue(cb, root), () -> knownTrue(cb, root))));
    }

    // -- structural invariant: multi-polarity inputs are rebuilt per occurrence --

    @Test
    void multiPolarityInputsAreBuiltFreshPerOccurrence() {
        EntityManager em = emf.createEntityManager();
        try {
            CriteriaBuilder cb = em.getCriteriaBuilder();
            TriPredicate tri = new TriPredicate(cb);
            CriteriaQuery<Long> cq = cb.createQuery(Long.class);
            Root<ResourceEntity> root = cq.from(ResourceEntity.class);

            AtomicInteger calls = new AtomicInteger();
            Supplier<Predicate> fresh = () -> {
                calls.incrementAndGet();
                return cb.equal(root.get("aString"), "seed");
            };

            // determined builds the body twice, once per polarity.
            tri.determined(fresh);
            assertEquals(2, calls.getAndSet(0));

            // baseUnlessUnknown builds the witness twice.
            tri.baseUnlessUnknown(cb.conjunction(), fresh);
            assertEquals(2, calls.getAndSet(0));

            // ternary builds the condition twice in the branches and twice in the unknown arm.
            tri.ternary(fresh, cb::conjunction, cb::conjunction);
            assertEquals(4, calls.getAndSet(0));
        } finally {
            em.close();
        }
    }
}
