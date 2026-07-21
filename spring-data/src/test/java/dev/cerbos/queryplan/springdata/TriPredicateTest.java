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
 * Pins the {@link TriPredicate} algebra directly at its own seam, against Hibernate 6 + H2 —
 * the same fixture pattern as {@link SpringDataQueryPlanAdapterTest} but with tiny hand-built
 * predicates instead of query plans.
 *
 * <p>One row is seeded with {@code aString = "seed"} and {@code aOptionalString = NULL}, giving
 * three primitive predicates over it: a known-TRUE one, a known-FALSE one, and an UNKNOWN one
 * (a comparison against the NULL column). Every truth table is asserted under BOTH polarities:
 * a count of 0 for the positive query AND 0 for the {@code tri.not(...)}-wrapped query is the
 * signature of UNKNOWN (excluded either way — the error→deny contract), while FALSE flips to 1
 * under negation.
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

    /** Builds the predicate under test from a fresh (cb, tri, root) triple. */
    @FunctionalInterface
    private interface PredicateFactory {
        Predicate build(CriteriaBuilder cb, TriPredicate tri, Root<ResourceEntity> root);
    }

    /** Rows matched by the factory's predicate: 1 = TRUE for the seed row, 0 = FALSE or UNKNOWN. */
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

    /** {@link #count} of the factory's predicate under {@code tri.not(...)} — the other polarity. */
    private static int countNegated(PredicateFactory factory) {
        return count((cb, tri, root) -> tri.not(factory.build(cb, tri, root)));
    }

    // -- primitive predicates over the seed row --

    /** TRUE for the seed row. */
    private static Predicate knownTrue(CriteriaBuilder cb, Root<ResourceEntity> root) {
        return cb.equal(root.get("aString"), "seed");
    }

    /** FALSE for the seed row. */
    private static Predicate knownFalse(CriteriaBuilder cb, Root<ResourceEntity> root) {
        return cb.equal(root.get("aString"), "something-else");
    }

    /** UNKNOWN for the seed row: comparison against its NULL column. */
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
        // Control for the fixture itself: a comparison against the NULL column really is
        // UNKNOWN, not FALSE — the raw material every guarded composition is built for.
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
        // Hibernate 6's raw cb.not(cb.not(eq)) collapses to a single NOT; the barrier must
        // restore boolean algebra: NOT NOT p = p, NOT NOT NOT p = NOT p.
        assertEquals(1, count((cb, tri, root) -> tri.not(tri.not(knownTrue(cb, root)))));
        assertEquals(0, count((cb, tri, root) -> tri.not(tri.not(knownFalse(cb, root)))));
        assertEquals(0, count((cb, tri, root) -> tri.not(tri.not(tri.not(knownTrue(cb, root))))));
    }

    // -- determined(): the two-polarity determinedness probe --

    @Test
    void determinedIsTrueForKnownBodiesAndUnknownForUnknownBodies() {
        assertEquals(1, count((cb, tri, root) -> tri.determined(() -> knownTrue(cb, root))));
        assertEquals(1, count((cb, tri, root) -> tri.determined(() -> knownFalse(cb, root))));
        // UNKNOWN body: UNKNOWN OR NOT UNKNOWN = UNKNOWN — excluded under both polarities.
        assertEquals(0, count((cb, tri, root) -> tri.determined(() -> unknownLeaf(cb, root))));
        assertEquals(0, countNegated((cb, tri, root) -> tri.determined(() -> unknownLeaf(cb, root))));
    }

    // -- ternary(): condition-unknown arm --

    @Test
    void ternaryWithKnownConditionSelectsTheBranch() {
        // TRUE condition → then-branch decides.
        assertEquals(1, count((cb, tri, root) -> tri.ternary(
                () -> knownTrue(cb, root), () -> knownTrue(cb, root), () -> knownFalse(cb, root))));
        assertEquals(0, count((cb, tri, root) -> tri.ternary(
                () -> knownTrue(cb, root), () -> knownFalse(cb, root), () -> knownTrue(cb, root))));
        assertEquals(1, countNegated((cb, tri, root) -> tri.ternary(
                () -> knownTrue(cb, root), () -> knownFalse(cb, root), () -> knownTrue(cb, root))));
        // FALSE condition → else-branch decides.
        assertEquals(1, count((cb, tri, root) -> tri.ternary(
                () -> knownFalse(cb, root), () -> knownFalse(cb, root), () -> knownTrue(cb, root))));
        assertEquals(0, count((cb, tri, root) -> tri.ternary(
                () -> knownFalse(cb, root), () -> knownTrue(cb, root), () -> knownFalse(cb, root))));
        assertEquals(1, countNegated((cb, tri, root) -> tri.ternary(
                () -> knownFalse(cb, root), () -> knownTrue(cb, root), () -> knownFalse(cb, root))));
    }

    @Test
    void ternaryWithUnknownConditionIsUnknownNotFalse() {
        // Both branches TRUE, condition UNKNOWN: the two branch arms alone would collapse to
        // FALSE (leaking the row under NOT); the third arm must force UNKNOWN — excluded under
        // BOTH polarities.
        assertEquals(0, count((cb, tri, root) -> tri.ternary(
                () -> unknownLeaf(cb, root), () -> knownTrue(cb, root), () -> knownTrue(cb, root))));
        assertEquals(0, countNegated((cb, tri, root) -> tri.ternary(
                () -> unknownLeaf(cb, root), () -> knownTrue(cb, root), () -> knownTrue(cb, root))));
    }

    // -- anyTrueOrUnknown(): the exists/filter/except absorption table --

    @Test
    void anyTrueOrUnknownTruthTable() {
        // True witness absorbs an unknown sibling → TRUE.
        assertEquals(1, count((cb, tri, root) ->
                tri.anyTrueOrUnknown(knownTrue(cb, root), knownTrue(cb, root))));
        assertEquals(0, countNegated((cb, tri, root) ->
                tri.anyTrueOrUnknown(knownTrue(cb, root), knownTrue(cb, root))));
        // No true witness, unknown witness → UNKNOWN (deny) under both polarities.
        assertEquals(0, count((cb, tri, root) ->
                tri.anyTrueOrUnknown(knownFalse(cb, root), knownTrue(cb, root))));
        assertEquals(0, countNegated((cb, tri, root) ->
                tri.anyTrueOrUnknown(knownFalse(cb, root), knownTrue(cb, root))));
        // No true witness, no unknown witness → plain FALSE (flips under NOT).
        assertEquals(0, count((cb, tri, root) ->
                tri.anyTrueOrUnknown(knownFalse(cb, root), knownFalse(cb, root))));
        assertEquals(1, countNegated((cb, tri, root) ->
                tri.anyTrueOrUnknown(knownFalse(cb, root), knownFalse(cb, root))));
        // True witness, no unknown → plain TRUE.
        assertEquals(1, count((cb, tri, root) ->
                tri.anyTrueOrUnknown(knownTrue(cb, root), knownFalse(cb, root))));
    }

    // -- allTrueOrUnknown(): the all absorption table --

    @Test
    void allTrueOrUnknownTruthTable() {
        // False witness absorbs an unknown sibling → FALSE (flips under NOT).
        assertEquals(0, count((cb, tri, root) ->
                tri.allTrueOrUnknown(knownTrue(cb, root), knownTrue(cb, root))));
        assertEquals(1, countNegated((cb, tri, root) ->
                tri.allTrueOrUnknown(knownTrue(cb, root), knownTrue(cb, root))));
        // No false witness, unknown witness → UNKNOWN (deny) under both polarities.
        assertEquals(0, count((cb, tri, root) ->
                tri.allTrueOrUnknown(knownFalse(cb, root), knownTrue(cb, root))));
        assertEquals(0, countNegated((cb, tri, root) ->
                tri.allTrueOrUnknown(knownFalse(cb, root), knownTrue(cb, root))));
        // No false witness, no unknown witness → TRUE.
        assertEquals(1, count((cb, tri, root) ->
                tri.allTrueOrUnknown(knownFalse(cb, root), knownFalse(cb, root))));
        assertEquals(0, countNegated((cb, tri, root) ->
                tri.allTrueOrUnknown(knownFalse(cb, root), knownFalse(cb, root))));
    }

    // -- baseUnlessUnknown(): the exists_one / size(filter) / map-intersection strict table --

    @Test
    void baseUnlessUnknownWithFalseBaseAndUnknownWitnessIsUnknownNotFalse() {
        // The load-bearing case: base FALSE + unknown witness must be UNKNOWN, not FALSE —
        // otherwise NOT(...) would include exactly the rows the PDP denies.
        assertEquals(0, count((cb, tri, root) ->
                tri.baseUnlessUnknown(knownFalse(cb, root), () -> knownTrue(cb, root))));
        assertEquals(0, countNegated((cb, tri, root) ->
                tri.baseUnlessUnknown(knownFalse(cb, root), () -> knownTrue(cb, root))));
    }

    @Test
    void baseUnlessUnknownTruthTable() {
        // No unknown witness → base passes through, both polarities.
        assertEquals(1, count((cb, tri, root) ->
                tri.baseUnlessUnknown(knownTrue(cb, root), () -> knownFalse(cb, root))));
        assertEquals(0, countNegated((cb, tri, root) ->
                tri.baseUnlessUnknown(knownTrue(cb, root), () -> knownFalse(cb, root))));
        assertEquals(0, count((cb, tri, root) ->
                tri.baseUnlessUnknown(knownFalse(cb, root), () -> knownFalse(cb, root))));
        assertEquals(1, countNegated((cb, tri, root) ->
                tri.baseUnlessUnknown(knownFalse(cb, root), () -> knownFalse(cb, root))));
        // Strictness: an unknown witness poisons even a TRUE base — no absorption.
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

            // determined: body appears in two polarities → built exactly twice.
            tri.determined(fresh);
            assertEquals(2, calls.getAndSet(0));

            // baseUnlessUnknown: unknown witness appears in two polarities → built exactly twice.
            tri.baseUnlessUnknown(cb.conjunction(), fresh);
            assertEquals(2, calls.getAndSet(0));

            // ternary: condition appears twice directly plus twice in the unknown arm → four.
            tri.ternary(fresh, cb::conjunction, cb::conjunction);
            assertEquals(4, calls.getAndSet(0));
        } finally {
            em.close();
        }
    }
}
