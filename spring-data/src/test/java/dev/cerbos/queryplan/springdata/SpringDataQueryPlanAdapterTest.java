package dev.cerbos.queryplan.springdata;

import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.queryplan.springdata.testmodel.CategoryEntity;
import dev.cerbos.queryplan.springdata.testmodel.ResourceEntity;
import dev.cerbos.queryplan.springdata.testmodel.SubCategoryEntity;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaDelete;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests that exercise the adapter without a live Cerbos PDP. They build protobuf operands
 * directly and verify the produced Specification by executing it against an H2 schema —
 * Hibernate translates to SQL, which catches mapping/type errors. Tests whose names promise row
 * semantics seed rows and assert row identities; the remainder run against an empty table and
 * check that translation succeeds (or throws the pinned error).
 */
class SpringDataQueryPlanAdapterTest {

    private static final Map<String, AttributeMapping> MAPPER = Map.ofEntries(
            Map.entry("request.resource.attr.aBool", AttributeMapping.field("aBool")),
            Map.entry("request.resource.attr.aString", AttributeMapping.field("aString")),
            Map.entry("request.resource.attr.aNumber", AttributeMapping.field("aNumber")),
            Map.entry("request.resource.attr.aDouble", AttributeMapping.field("aDouble")),
            Map.entry("request.resource.attr.aOptionalString", AttributeMapping.field("aOptionalString")),
            Map.entry("request.resource.attr.createdBy", AttributeMapping.field("createdBy")),
            Map.entry("request.resource.attr.createdAt", AttributeMapping.field("createdAt")),
            Map.entry("request.resource.attr.updatedAt", AttributeMapping.field("updatedAt")),
            Map.entry("request.resource.attr.localCreatedAt", AttributeMapping.field("localCreatedAt")),
            Map.entry("request.resource.attr.ownedBy", AttributeMapping.relation("ownedBy")),
            Map.entry("request.resource.attr.tags", AttributeMapping.relation("tags", Map.of(
                    "id", AttributeMapping.field("id"),
                    "name", AttributeMapping.field("name")
            )))
    );

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

    private static Operand nval(double v) {
        return Operand.newBuilder().setValue(Value.newBuilder().setNumberValue(v)).build();
    }

    private static Operand bval(boolean v) {
        return Operand.newBuilder().setValue(Value.newBuilder().setBoolValue(v)).build();
    }

    private static Operand nullVal() {
        return Operand.newBuilder().setValue(Value.newBuilder().setNullValue(NullValue.NULL_VALUE)).build();
    }

    private static Operand listOp(String... values) {
        ListValue.Builder list = ListValue.newBuilder();
        for (String v : values) list.addValues(Value.newBuilder().setStringValue(v));
        return Operand.newBuilder().setValue(Value.newBuilder().setListValue(list)).build();
    }

    private static Operand listOpNumbers(double... values) {
        ListValue.Builder list = ListValue.newBuilder();
        for (double v : values) list.addValues(Value.newBuilder().setNumberValue(v));
        return Operand.newBuilder().setValue(Value.newBuilder().setListValue(list)).build();
    }

    private static Operand lambda(String varName, Operand body) {
        return exprOp("lambda", body, var(varName));
    }

    /**
     * Assert that {@link #runCount} for {@code condition} throws {@link IllegalArgumentException}
     * whose message contains every {@code messageFragments} entry. Pins error contracts so a
     * future refactor can't silently regress to a less-helpful message or different exception
     * type.
     */
    private static void assertConditionThrows(Operand condition, String... messageFragments) {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> runCount(condition));
        for (String fragment : messageFragments) {
            assertTrue(ex.getMessage().contains(fragment),
                    "expected message to contain '" + fragment + "' but was: " + ex.getMessage());
        }
    }

    /**
     * Build a Specification, translate to a predicate, and run the query — returns the row count.
     * Exercises the full path so any IllegalArgumentException during predicate building surfaces.
     */
    private static int runCount(Operand condition) {
        return runCount(condition, Map.of());
    }

    /** {@link #runCount(Operand)} with per-operator overrides. */
    private static int runCount(Operand condition, Map<String, OperatorFunction> overrides) {
        return runCount(condition, MAPPER, overrides);
    }

    /** {@link #runCount(Operand)} against a caller-supplied attribute mapper. */
    private static int runCount(Operand condition, Map<String, AttributeMapping> mapper,
                                Map<String, OperatorFunction> overrides) {
        PlanResourcesResponse resp =
                buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);
        Result<ResourceEntity> result =
                SpringDataQueryPlanAdapter.toSpecification(resp, mapper, overrides);
        assertInstanceOf(Result.Conditional.class, result);
        Specification<ResourceEntity> spec = ((Result.Conditional<ResourceEntity>) result).specification();

        EntityManager em = emf.createEntityManager();
        try {
            CriteriaBuilder cb = em.getCriteriaBuilder();
            CriteriaQuery<Long> cq = cb.createQuery(Long.class);
            Root<ResourceEntity> root = cq.from(ResourceEntity.class);
            cq.select(cb.count(root));
            Predicate p = spec.toPredicate(root, cq, cb);
            if (p != null) cq.where(p);
            return em.createQuery(cq).getSingleResult().intValue();
        } finally {
            em.close();
        }
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

    /** Thrown by {@link #THROWING_OVERRIDE} to prove an override hook was actually invoked. */
    private static final class OverrideInvoked extends RuntimeException {
        OverrideInvoked() {
            super("override invoked");
        }
    }

    /** An override that fails loudly when reached, so a test can assert the override path is taken. */
    private static final OperatorFunction THROWING_OVERRIDE = (cb, field, value) -> {
        throw new OverrideInvoked();
    };

    @Test
    void alwaysAllowedResult() {
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_ALWAYS_ALLOWED, null);
        Result<ResourceEntity> result =
                SpringDataQueryPlanAdapter.toSpecification(resp, MAPPER);
        assertInstanceOf(Result.AlwaysAllowed.class, result);
    }

    @Test
    void alwaysAllowedSpecificationReturnsNullPredicate() {
        // Contract: AlwaysAllowed.toSpecification() must produce a Specification whose
        // toPredicate returns null — Spring Data's SimpleJpaRepository skips the WHERE
        // clause entirely in that case. Pins B2 against regression to cb.conjunction().
        Specification<ResourceEntity> spec = new Result.AlwaysAllowed<ResourceEntity>().toSpecification();
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
    void alwaysDeniedResult() {
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_ALWAYS_DENIED, null);
        Result<ResourceEntity> result =
                SpringDataQueryPlanAdapter.toSpecification(resp, MAPPER);
        assertInstanceOf(Result.AlwaysDenied.class, result);
    }

    @Test
    void alwaysDeniedSpecificationReturnsDisjunction() {
        // Symmetric pin: AlwaysDenied.toSpecification() must produce a non-null predicate.
        Specification<ResourceEntity> spec = new Result.AlwaysDenied<ResourceEntity>().toSpecification();
        EntityManager em = emf.createEntityManager();
        try {
            CriteriaBuilder cb = em.getCriteriaBuilder();
            CriteriaQuery<ResourceEntity> cq = cb.createQuery(ResourceEntity.class);
            Root<ResourceEntity> root = cq.from(ResourceEntity.class);
            assertNotNull(spec.toPredicate(root, cq, cb),
                    "AlwaysDenied must emit an explicit predicate, not null");
        } finally {
            em.close();
        }
    }

    @Test
    void eqOnString() {
        assertEquals(0, runCount(exprOp("eq", var("request.resource.attr.aString"), sval("foo"))));
    }

    @Test
    void neOnString() {
        assertEquals(0, runCount(exprOp("ne", var("request.resource.attr.aString"), sval("foo"))));
    }

    @Test
    void ltOnNumber() {
        assertEquals(0, runCount(exprOp("lt", var("request.resource.attr.aNumber"), nval(10))));
    }

    @Test
    void gtOnNumber() {
        assertEquals(0, runCount(exprOp("gt", var("request.resource.attr.aNumber"), nval(0))));
    }

    @Test
    void leOnNumber() {
        assertEquals(0, runCount(exprOp("le", var("request.resource.attr.aNumber"), nval(10))));
    }

    @Test
    void geOnNumber() {
        assertEquals(0, runCount(exprOp("ge", var("request.resource.attr.aNumber"), nval(0))));
    }

    @Test
    void inOnString() {
        assertEquals(0, runCount(exprOp("in", var("request.resource.attr.aString"), listOp("a", "b"))));
    }

    @Test
    void containsBuildsLike() {
        assertEquals(0, runCount(exprOp("contains", var("request.resource.attr.aString"), sval("foo"))));
    }

    @Test
    void startsWithBuildsLike() {
        assertEquals(0, runCount(exprOp("startsWith", var("request.resource.attr.aString"), sval("foo"))));
    }

    @Test
    void endsWithBuildsLike() {
        assertEquals(0, runCount(exprOp("endsWith", var("request.resource.attr.aString"), sval("foo"))));
    }

    @Test
    void andOr() {
        assertEquals(0, runCount(exprOp("and",
                exprOp("eq", var("request.resource.attr.aBool"), bval(true)),
                exprOp("or",
                        exprOp("ne", var("request.resource.attr.aString"), sval("x")),
                        exprOp("gt", var("request.resource.attr.aNumber"), nval(5))))));
    }

    @Test
    void notBareBool() {
        assertEquals(0, runCount(exprOp("not", var("request.resource.attr.aBool"))));
    }

    @Test
    void bareBoolBuildsEquals() {
        assertEquals(0, runCount(var("request.resource.attr.aBool")));
    }

    @Test
    void isSetTrueBuildsIsNotNull() {
        assertEquals(0, runCount(exprOp("isSet",
                var("request.resource.attr.aOptionalString"), bval(true))));
    }

    @Test
    void isSetFalseBuildsIsNull() {
        assertEquals(0, runCount(exprOp("isSet",
                var("request.resource.attr.aOptionalString"), bval(false))));
    }

    @Test
    void eqNullBuildsIsNull() {
        assertEquals(0, runCount(exprOp("eq",
                var("request.resource.attr.aOptionalString"), nullVal())));
    }

    @Test
    void neNullBuildsIsNotNull() {
        assertEquals(0, runCount(exprOp("ne",
                var("request.resource.attr.aOptionalString"), nullVal())));
    }

    @Test
    void hasIntersectionOnFlatCollection() {
        assertEquals(0, runCount(exprOp("hasIntersection",
                var("request.resource.attr.ownedBy"), listOp("user1", "user2"))));
    }

    @Test
    void inMembershipOnCollection() {
        // "public" in tags  (right operand is the collection)
        assertEquals(0, runCount(exprOp("in",
                sval("public"), var("request.resource.attr.ownedBy"))));
    }

    @Test
    void sizeGtZeroBuildsExists() {
        assertEquals(0, runCount(exprOp("gt",
                exprOp("size", var("request.resource.attr.ownedBy")),
                nval(0))));
    }

    @Test
    void sizeEqZeroBuildsNotExists() {
        assertEquals(0, runCount(exprOp("eq",
                exprOp("size", var("request.resource.attr.ownedBy")),
                nval(0))));
    }

    // -- size(collection) compared with arbitrary N → correlated (SELECT COUNT(...)) <op> N.
    // Seeds a real row because an empty table cannot distinguish count thresholds.

    @Nested
    class SizeCountComparisons {

        private ResourceEntity seeded() {
            ResourceEntity r = new ResourceEntity("size-seed-1");
            r.setOwnedBy(new ArrayList<>(List.of("user1", "user2")));
            r.addTag("tagX", "x");
            return r;
        }

        @Test
        void sizeComparedWithArbitraryN() {
            // Seeded row has 2 owners (@ElementCollection) and 1 tag (@OneToMany).
            withResource(seeded(), () -> {
                assertEquals(1, runCount(exprOp("eq",
                        exprOp("size", var("request.resource.attr.ownedBy")), nval(2))));
                assertEquals(0, runCount(exprOp("eq",
                        exprOp("size", var("request.resource.attr.ownedBy")), nval(3))));
                assertEquals(1, runCount(exprOp("gt",
                        exprOp("size", var("request.resource.attr.ownedBy")), nval(1))));
                assertEquals(0, runCount(exprOp("gt",
                        exprOp("size", var("request.resource.attr.ownedBy")), nval(2))));
                assertEquals(1, runCount(exprOp("le",
                        exprOp("size", var("request.resource.attr.ownedBy")), nval(2))));
                assertEquals(0, runCount(exprOp("lt",
                        exprOp("size", var("request.resource.attr.ownedBy")), nval(2))));
                assertEquals(0, runCount(exprOp("ge",
                        exprOp("size", var("request.resource.attr.ownedBy")), nval(3))));
                assertEquals(0, runCount(exprOp("ne",
                        exprOp("size", var("request.resource.attr.ownedBy")), nval(2))));
                // Entity relation (@OneToMany), not just element collections.
                assertEquals(1, runCount(exprOp("eq",
                        exprOp("size", var("request.resource.attr.tags")), nval(1))));
            });
        }

        @Test
        void sizeValueFirstWithArbitraryNIsMirrored() {
            // 3 > size(ownedBy) → size < 3, with 2 owners → match. The naive (unmirrored)
            // translation `size > 3` would return 0.
            withResource(seeded(), () -> {
                assertEquals(1, runCount(exprOp("gt",
                        nval(3),
                        exprOp("size", var("request.resource.attr.ownedBy")))));
                assertEquals(1, runCount(exprOp("lt",
                        nval(1),
                        exprOp("size", var("request.resource.attr.ownedBy")))));
            });
        }
    }

    // -- Fractional size() thresholds: COUNT/LENGTH are integral, so a fractional constant f
    // can never be hit exactly. Correct semantics: eq → always-false; ne → always-true (but a
    // NULL string column is a missing attribute → CEL error → deny); ge/gt f → ge ceil(f);
    // le/lt f → le floor(f). Truncation (`>= 1.5` becoming `>= 1`) over-included.

    @Nested
    class FractionalSizeThresholds {

        private ResourceEntity seeded() {
            ResourceEntity r = new ResourceEntity("size-frac-seed-1");
            r.setOwnedBy(new ArrayList<>(List.of("user1", "user2")));
            return r;
        }

        private Operand sizeCmp(String op, double threshold) {
            return exprOp(op,
                    exprOp("size", var("request.resource.attr.ownedBy")),
                    nval(threshold));
        }

        @Test
        void eqFractionalIsAlwaysFalse() {
            // size == 2.5 can never hold for an integral count; truncation made it eq 2.
            withResource(seeded(), () -> assertEquals(0, runCount(sizeCmp("eq", 2.5))));
        }

        @Test
        void neFractionalIsAlwaysTrue() {
            // size != 2.5 always holds; truncation made it ne 2 (false for the seeded row).
            withResource(seeded(), () -> assertEquals(1, runCount(sizeCmp("ne", 2.5))));
        }

        @Test
        void geFractionalRoundsUp() {
            // size >= 2.5 ⇔ size >= 3; truncation made it >= 2 (over-inclusive).
            withResource(seeded(), () -> {
                assertEquals(0, runCount(sizeCmp("ge", 2.5)));
                assertEquals(1, runCount(sizeCmp("ge", 1.5)));
            });
        }

        @Test
        void gtFractionalRoundsUp() {
            // gt f ⇔ ge ceil(f) for integral counts.
            withResource(seeded(), () -> {
                assertEquals(1, runCount(sizeCmp("gt", 1.5)));
                assertEquals(0, runCount(sizeCmp("gt", 2.5)));
            });
        }

        @Test
        void ltFractionalRoundsDown() {
            // size < 2.5 ⇔ size <= 2; truncation made it lt 2 (under-inclusive).
            withResource(seeded(), () -> {
                assertEquals(1, runCount(sizeCmp("lt", 2.5)));
                assertEquals(0, runCount(sizeCmp("lt", 1.5)));
            });
        }

        @Test
        void leFractionalRoundsDown() {
            withResource(seeded(), () -> {
                assertEquals(0, runCount(sizeCmp("le", 1.5)));
                assertEquals(1, runCount(sizeCmp("le", 2.5)));
            });
        }

        @Test
        void fractionalEmptinessShortcutsStillRoute() {
            // ge 0.5 ⇔ ge 1 → EXISTS; lt 0.5 ⇔ le 0 → NOT EXISTS.
            withResource(seeded(), () -> {
                assertEquals(1, runCount(sizeCmp("ge", 0.5)));
                assertEquals(0, runCount(sizeCmp("lt", 0.5)));
            });
        }

        @Test
        void stringSizeFractionalNeExcludesNullColumn() {
            // size(string) != 1.5 is vacuously true for any PRESENT string, but a NULL
            // column is a missing attribute → CEL error → deny. Always-true would leak it.
            Operand cond = exprOp("ne",
                    exprOp("size", var("request.resource.attr.aOptionalString")),
                    nval(1.5));
            ResourceEntity withValue = new ResourceEntity("size-frac-str-1");
            withValue.setaOptionalString("ab");
            ResourceEntity withNull = new ResourceEntity("size-frac-str-2");
            withNull.setaOptionalString(null);
            withResource(withValue, () -> withResource(withNull, () ->
                    assertEquals(1, runCount(cond))));
            // eq fractional over a string length is always false, NULL or not.
            Operand eqCond = exprOp("eq",
                    exprOp("size", var("request.resource.attr.aOptionalString")),
                    nval(1.5));
            ResourceEntity another = new ResourceEntity("size-frac-str-3");
            another.setaOptionalString("ab");
            withResource(another, () -> assertEquals(0, runCount(eqCond)));
        }
    }

    // -- size(string) thresholds outside int range: cb.length is Expression<Integer>, so an
    // unguarded (int) narrowing cast wrapped them (2147483648 → −2147483648, 4294967296 → 0),
    // silently flipping the filter: `size(s) > 4294967296` became `LENGTH(s) > 0` — always-true
    // over-inclusion while check() denies every row. No string's length leaves int range, so
    // these comparisons must fold statically: gt/ge/eq huge → always-false; lt/le/ne huge →
    // true for a PRESENT string only (NULL column = missing attribute → CEL error → deny).

    @Nested
    class HugeStringSizeThresholds {

        private static final double TWO_POW_31 = 2147483648.0; // Integer.MAX_VALUE + 1
        private static final double TWO_POW_32 = 4294967296.0;

        private Operand strSize(String op, double threshold) {
            return exprOp(op,
                    exprOp("size", var("request.resource.attr.aOptionalString")),
                    nval(threshold));
        }

        private ResourceEntity present() {
            ResourceEntity r = new ResourceEntity("size-huge-1");
            r.setaOptionalString("abc");
            return r;
        }

        private ResourceEntity emptyString() {
            ResourceEntity r = new ResourceEntity("size-huge-2");
            r.setaOptionalString("");
            return r;
        }

        private ResourceEntity nullString() {
            ResourceEntity r = new ResourceEntity("size-huge-3");
            r.setaOptionalString(null);
            return r;
        }

        @Test
        void gtGeEqAboveIntMaxAreAlwaysFalse() {
            // No string has >= 2^31 chars, so gt/ge/eq can never hold. The wrap made
            // gt 2^32 into LENGTH > 0 (matched every non-empty row) and gt 2^31 into
            // LENGTH > −2^31 (matched every present row).
            withResource(present(), () -> {
                assertEquals(0, runCount(strSize("gt", TWO_POW_32)));
                assertEquals(0, runCount(strSize("gt", TWO_POW_31)));
                assertEquals(0, runCount(strSize("ge", TWO_POW_32)));
                assertEquals(0, runCount(strSize("ge", TWO_POW_31)));
            });
            // eq 2^32 wrapped to LENGTH = 0, wrongly matching the empty string.
            withResource(emptyString(), () ->
                    assertEquals(0, runCount(strSize("eq", TWO_POW_32))));
        }

        @Test
        void ltLeAboveIntMaxIncludePresentAndExcludeNull() {
            // Every present string satisfies lt/le a huge threshold — but a NULL column is
            // a missing attribute → CEL error → deny. The wrap made lt 2^32 into
            // LENGTH < 0 (excluded everything).
            withResource(present(), () -> withResource(nullString(), () -> {
                assertEquals(1, runCount(strSize("lt", TWO_POW_32)));
                assertEquals(1, runCount(strSize("le", TWO_POW_32)));
                assertEquals(1, runCount(strSize("lt", TWO_POW_31)));
            }));
        }

        @Test
        void neAboveIntMaxIncludesEmptyStringAndExcludesNull() {
            // size("") != 2^32 is TRUE in CEL. The wrap made it LENGTH <> 0, wrongly
            // excluding the empty string; NULL must stay excluded either way.
            withResource(emptyString(), () -> withResource(nullString(), () ->
                    assertEquals(1, runCount(strSize("ne", TWO_POW_32)))));
        }

        @Test
        void belowIntMinThresholdsFoldMirrored() {
            // LENGTH(s) >= 0 > any threshold below int range: gt/ge/ne always hold for a
            // present string (incl. the empty string — the wrap made gt −2^32 into
            // LENGTH > 0, wrongly excluding it); eq/lt/le can never hold.
            withResource(emptyString(), () -> withResource(nullString(), () -> {
                assertEquals(1, runCount(strSize("gt", -TWO_POW_32)));
                assertEquals(1, runCount(strSize("ge", -TWO_POW_32)));
                assertEquals(1, runCount(strSize("ne", -TWO_POW_32)));
                assertEquals(0, runCount(strSize("lt", -TWO_POW_32)));
                assertEquals(0, runCount(strSize("le", -TWO_POW_32)));
                assertEquals(0, runCount(strSize("eq", -TWO_POW_32)));
            }));
        }

        @Test
        void fractionalHugeThresholdRoundsThenFolds() {
            // ge 2^32 + 0.5 → ceil → 4294967297 → still above int range → always-false;
            // le → floor → 4294967296 → present strings satisfy it.
            withResource(present(), () -> {
                assertEquals(0, runCount(strSize("ge", TWO_POW_32 + 0.5)));
                assertEquals(1, runCount(strSize("le", TWO_POW_32 + 0.5)));
            });
        }

        @Test
        void boundaryIntegerMaxStillComparesExactly() {
            // Integer.MAX_VALUE itself is in range and must keep producing a real LENGTH
            // comparison, not a fold.
            withResource(present(), () -> {
                assertEquals(0, runCount(strSize("gt", 2147483647.0)));
                assertEquals(1, runCount(strSize("lt", 2147483647.0)));
                assertEquals(1, runCount(strSize("le", 2147483647.0)));
                assertEquals(0, runCount(strSize("ge", 2147483647.0)));
            });
        }
    }

    @Test
    void existsOnNestedRelation() {
        assertEquals(0, runCount(exprOp("exists",
                var("request.resource.attr.tags"),
                lambda("t",
                        exprOp("eq", var("t.id"), sval("tag1"))))));
    }

    @Test
    void existsMultiCondition() {
        assertEquals(0, runCount(exprOp("exists",
                var("request.resource.attr.tags"),
                lambda("t",
                        exprOp("and",
                                exprOp("eq", var("t.id"), sval("tag1")),
                                exprOp("eq", var("t.name"), sval("public")))))));
    }

    @Test
    void allOnNestedRelation() {
        assertEquals(0, runCount(exprOp("all",
                var("request.resource.attr.tags"),
                lambda("t",
                        exprOp("eq", var("t.name"), sval("public"))))));
    }

    @Test
    void exceptOnNestedRelation() {
        assertEquals(0, runCount(exprOp("except",
                var("request.resource.attr.tags"),
                lambda("t",
                        exprOp("eq", var("t.name"), sval("public"))))));
    }

    @Test
    void hasIntersectionWithMap() {
        Operand mapExpr = exprOp("map",
                var("request.resource.attr.tags"),
                lambda("t", var("t.name")));
        assertEquals(0, runCount(exprOp("hasIntersection", mapExpr, listOp("public", "private"))));
    }

    @Test
    void existsOneOnNestedRelation() {
        // exists_one → correlated (SELECT COUNT(...)) = 1. Exercises the manual-correlation path
        // that the other collection operators do not.
        assertEquals(0, runCount(exprOp("exists_one",
                var("request.resource.attr.tags"),
                lambda("t",
                        exprOp("eq", var("t.name"), sval("public"))))));
    }

    @Test
    void existsOneWithCompoundBody() {
        assertEquals(0, runCount(exprOp("exists_one",
                var("request.resource.attr.tags"),
                lambda("t",
                        exprOp("or",
                                exprOp("eq", var("t.id"), sval("tag1")),
                                exprOp("eq", var("t.name"), sval("public")))))));
    }

    // -- empty-list intersection short-circuits (no dialect-dependent `IN ()`) --

    @Test
    void hasIntersectionScalarEmptyListCompiles() {
        // hasIntersection(field, []) is always false and must not emit an empty `IN ()`.
        assertEquals(0, runCount(exprOp("hasIntersection",
                var("request.resource.attr.aString"), listOp())));
    }

    @Test
    void hasIntersectionRelationEmptyListCompiles() {
        assertEquals(0, runCount(exprOp("hasIntersection",
                var("request.resource.attr.tags"), listOp())));
    }

    @Test
    void hasIntersectionMapEmptyListCompiles() {
        Operand mapExpr = exprOp("map",
                var("request.resource.attr.tags"),
                lambda("t", var("t.name")));
        assertEquals(0, runCount(exprOp("hasIntersection", mapExpr, listOp())));
    }

    // -- override hook is consulted on every scalar-leaf path, not just the direct comparison --

    @Test
    void overrideAppliesToDirectComparison() {
        Operand cond = exprOp("eq", var("request.resource.attr.aString"), sval("foo"));
        assertThrows(OverrideInvoked.class,
                () -> runCount(cond, Map.of("eq", THROWING_OVERRIDE)));
    }

    @Test
    void overrideAppliesToAddFoldedComparison() {
        // field == "prefix:" + "123" folds to a constant then compares — must hit the same override.
        Operand cond = exprOp("eq",
                var("request.resource.attr.aString"),
                exprOp("add", sval("prefix:"), sval("123")));
        assertThrows(OverrideInvoked.class,
                () -> runCount(cond, Map.of("eq", THROWING_OVERRIDE)));
    }

    @Test
    void overrideAppliesToNullRhs() {
        // eq(field, null) must route through a registered override rather than forcing IS NULL.
        Operand cond = exprOp("eq", var("request.resource.attr.aOptionalString"), nullVal());
        assertThrows(OverrideInvoked.class,
                () -> runCount(cond, Map.of("eq", THROWING_OVERRIDE)));
    }

    @Test
    void overrideAppliesToBareBoolean() {
        Operand cond = var("request.resource.attr.aBool");
        assertThrows(OverrideInvoked.class,
                () -> runCount(cond, Map.of("eq", THROWING_OVERRIDE)));
    }

    @Test
    void overrideAppliesToScalarIn() {
        Operand cond = exprOp("in",
                var("request.resource.attr.aString"), listOp("a", "b"));
        assertThrows(OverrideInvoked.class,
                () -> runCount(cond, Map.of("in", THROWING_OVERRIDE)));
    }

    @Test
    void overrideAppliesToIsSet() {
        Operand cond = exprOp("isSet", var("request.resource.attr.aOptionalString"), bval(true));
        assertThrows(OverrideInvoked.class,
                () -> runCount(cond, Map.of("isSet", THROWING_OVERRIDE)));
    }

    @Test
    void unknownAttributeThrows() {
        Operand cond = exprOp("eq", var("request.resource.attr.nonexistent"), sval("v"));
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> runCount(cond));
        assertTrue(ex.getMessage().contains("Unknown attribute"));
    }

    @Test
    void unknownOperatorThrows() {
        Operand cond = exprOp("unsupported_op",
                var("request.resource.attr.aString"), sval("v"));
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> runCount(cond));
        assertTrue(ex.getMessage().contains("Unsupported operator"));
    }

    // -- eq/ne against a structured (list/map) constant: named error, not a raw Hibernate one --

    /**
     * PDP-verified wire shapes (Cerbos {@code :latest}, 2026-07-23): {@code R.attr.tags ==
     * ["a", "b"]} arrives as {@code eq(variable, value-list)} verbatim — in BOTH operand
     * orders — and {@code ne} likewise. Without the guard, {@code cb.equal(stringPath, List)}
     * dies inside Hibernate with a raw coercion error ("Could not convert
     * java.util.ImmutableCollections$ListN to java.lang.String"), violating the README's
     * contract that unsupported constructs throw {@link IllegalArgumentException} naming the
     * operator. These tests pin the named error AND that no element values leak into it.
     */
    @Nested
    class StructuredConstantComparison {

        /** Distinctive element values so the no-leak assertions cannot false-negative. */
        private static final String ELEM_A = "leak-canary-alpha";
        private static final String ELEM_B = "leak-canary-beta";

        private static IllegalArgumentException assertNamedError(Operand cond, String op,
                                                                 String attribute, String shape) {
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> runCount(cond));
            String msg = ex.getMessage();
            assertTrue(msg.contains(op), "expected operator '" + op + "' in: " + msg);
            assertTrue(msg.contains(attribute), "expected attribute '" + attribute + "' in: " + msg);
            assertTrue(msg.contains(shape), "expected shape '" + shape + "' in: " + msg);
            assertTrue(msg.contains("hasIntersection"),
                    "expected the supported alternative in: " + msg);
            assertFalse(msg.contains(ELEM_A), "element value leaked into: " + msg);
            assertFalse(msg.contains(ELEM_B), "element value leaked into: " + msg);
            return ex;
        }

        @Test
        void eqFieldAgainstListConstantThrowsNamedError() {
            assertNamedError(
                    exprOp("eq", var("request.resource.attr.aString"), listOp(ELEM_A, ELEM_B)),
                    "eq", "request.resource.attr.aString", "list of 2 elements");
        }

        @Test
        void neFieldAgainstListConstantThrowsNamedError() {
            assertNamedError(
                    exprOp("ne", var("request.resource.attr.aString"), listOp(ELEM_A, ELEM_B)),
                    "ne", "request.resource.attr.aString", "list of 2 elements");
        }

        @Test
        void eqValueFirstListConstantThrowsNamedError() {
            // ["a", "b"] == R.attr.x — source order is preserved on the wire; NormalizedBinary
            // mirrors it back to field-first, so the same named error must surface.
            assertNamedError(
                    exprOp("eq", listOp(ELEM_A), var("request.resource.attr.aString")),
                    "eq", "request.resource.attr.aString", "list of 1 element");
        }

        @Test
        void neValueFirstListConstantThrowsNamedError() {
            assertNamedError(
                    exprOp("ne", listOp(ELEM_A, ELEM_B), var("request.resource.attr.aString")),
                    "ne", "request.resource.attr.aString", "list of 2 elements");
        }

        @Test
        void eqRelationAgainstListConstantThrowsNamedError() {
            // Relation-mapped attribute: previously surfaced the generic "is a Relation;
            // cannot resolve as a scalar path" — the structured-constant guard runs before
            // path resolution so this shape gets the same actionable message.
            assertNamedError(
                    exprOp("eq", var("request.resource.attr.tags"), listOp(ELEM_A, ELEM_B)),
                    "eq", "request.resource.attr.tags", "list of 2 elements");
        }

        @Test
        void eqFieldAgainstStructConstantThrowsNamedError() {
            // Defensive: the planner emits map literals as struct() expressions (which throw a
            // named error via leafOperandError), but protoValueToJava can produce a Map from a
            // STRUCT_VALUE — pin the same contract for that shape.
            Operand structConstant = Operand.newBuilder()
                    .setValue(Value.newBuilder().setStructValue(Struct.newBuilder()
                            .putFields("k", Value.newBuilder().setStringValue(ELEM_A).build())))
                    .build();
            assertNamedError(
                    exprOp("eq", var("request.resource.attr.aString"), structConstant),
                    "eq", "request.resource.attr.aString", "map of 1 entry");
        }
    }

    // -- add operator --

    @Test
    void addFoldedTwoConstants() {
        // eq(R.attr.aString, add("hello", "-world"))  →  field == "hello-world"
        Operand cond = exprOp("eq",
                var("request.resource.attr.aString"),
                exprOp("add", sval("hello"), sval("-world")));
        assertEquals(0, runCount(cond));
    }

    @Test
    void addSolveStringPrefixStrip() {
        // eq("projects:123", add("projects:", R.attr.aString))
        //   → "projects:123".stripPrefix("projects:") == "123"
        //   → aString == "123"
        Operand cond = exprOp("eq",
                sval("projects:123"),
                exprOp("add", sval("projects:"), var("request.resource.attr.aString")));
        assertEquals(0, runCount(cond));

        // Concatenation is exact — the solve path must keep working: aString="123" row in,
        // aString="456" row out.
        ResourceEntity match = new ResourceEntity("add-str-1");
        match.setaString("123");
        ResourceEntity miss = new ResourceEntity("add-str-2");
        miss.setaString("456");
        withResource(match, () -> withResource(miss, () -> assertEquals(1, runCount(cond))));
    }

    @Test
    void addSolveStringSuffixStrip() {
        // eq("foo.bar", add(R.attr.aString, ".bar"))
        //   → "foo.bar".stripSuffix(".bar") == "foo"
        //   → aString == "foo"
        Operand cond = exprOp("eq",
                sval("foo.bar"),
                exprOp("add", var("request.resource.attr.aString"), sval(".bar")));
        assertEquals(0, runCount(cond));
    }

    @Test
    void addSolveNumeric() {
        // eq(10, add(3, R.attr.aNumber))  →  aNumber == 7. Long/long solves within ±2^53 are
        // algebraically exact and must keep solving in Java: seeded rows prove the filter
        // keeps the aNumber=7 row and drops the aNumber=8 row.
        Operand cond = exprOp("eq",
                nval(10),
                exprOp("add", nval(3), var("request.resource.attr.aNumber")));
        assertEquals(0, runCount(cond));

        ResourceEntity match = new ResourceEntity("add-long-1");
        match.setaNumber(7);
        ResourceEntity miss = new ResourceEntity("add-long-2");
        miss.setaNumber(8);
        withResource(match, () -> withResource(miss, () -> assertEquals(1, runCount(cond))));
    }

    @Test
    void addSolveFractionalEqIsIeeeFaithful() {
        // Policy `R.attr.aDouble + 0.7 == 0.1` arrives as eq(add(variable, 0.7), 0.1) —
        // wire shape verified against a live PDP. The old algebraic solve computed
        // 0.1 - 0.7 = exactly -0.6 in Java and emitted `WHERE a_double = -0.6`, but IEEE
        // subtraction does not invert IEEE addition: check(aDouble=-0.6) DENIES because
        // -0.6 + 0.7 == 0.09999999999999998 != 0.1 (verified against a live PDP). The row
        // must be EXCLUDED for eq (over-inclusion = authz bypass) and INCLUDED for ne
        // (the mirror under-inclusion). The fix lowers the comparison to SQL-side
        // fl(a_double + 0.7) == 0.1 in double space.
        Operand eqCond = exprOp("eq",
                exprOp("add", var("request.resource.attr.aDouble"), nval(0.7)),
                nval(0.1));
        Operand neCond = exprOp("ne",
                exprOp("add", var("request.resource.attr.aDouble"), nval(0.7)),
                nval(0.1));

        ResourceEntity trap = new ResourceEntity("add-frac-1");
        trap.setaDouble(-0.6);
        withResource(trap, () -> {
            assertEquals(0, runCount(eqCond), "aDouble=-0.6 must be excluded for eq: "
                    + "-0.6 + 0.7 != 0.1 in IEEE double space, the PDP denies it");
            assertEquals(1, runCount(neCond), "aDouble=-0.6 must be included for ne: "
                    + "-0.6 + 0.7 != 0.1 holds, the PDP allows it");
        });
    }

    @Test
    void addSolveFractionalEqKeepsExactlySatisfiedRow() {
        // 0.25 + 0.5 == 0.75 is EXACT in binary floating point, so the SQL lowering must not
        // degenerate to always-false: the aDouble=0.25 row stays in (PDP-verified ALLOW).
        Operand cond = exprOp("eq",
                exprOp("add", var("request.resource.attr.aDouble"), nval(0.5)),
                nval(0.75));

        ResourceEntity match = new ResourceEntity("add-frac-2");
        match.setaDouble(0.25);
        ResourceEntity miss = new ResourceEntity("add-frac-3");
        miss.setaDouble(-0.6);
        withResource(match, () -> withResource(miss, () -> assertEquals(1, runCount(cond))));
    }

    @Test
    void addSolveOversizedLongRoutesToSqlArithmetic() {
        // 2^54 is outside the ±2^53 exactly-representable range: the check-time double
        // arithmetic has gaps there, so the long-space solve must NOT fire — the shape
        // routes through SQL double arithmetic instead (and must not throw).
        Operand cond = exprOp("eq",
                exprOp("add", var("request.resource.attr.aNumber"), nval(1)),
                nval(0x1p54));

        ResourceEntity row = new ResourceEntity("add-big-1");
        row.setaNumber(5);
        withResource(row, () -> assertEquals(0, runCount(cond)));
    }

    @Test
    void addNoSolutionEqProducesImpossibleFilter() {
        // eq("nope", add("projects:", R.attr.aString))
        //   "nope" doesn't start with "projects:" → no solution → eq becomes 1=0
        Operand cond = exprOp("eq",
                sval("nope"),
                exprOp("add", sval("projects:"), var("request.resource.attr.aString")));
        // 1=0 filter → 0 results expected (table is empty anyway, this just confirms no exception)
        assertEquals(0, runCount(cond));
    }

    @Test
    void addNoSolutionNeExcludesNullRows() {
        // ne("abc", add("users:", R.attr.aOptionalString)): no field value can make the
        // concatenation equal "abc", BUT a missing attribute makes `"users:" + null` a CEL
        // evaluation error → deny. An always-true collapse would leak the NULL row; the
        // correct translation is IS NOT NULL (non-NULL rows in, NULL rows out).
        Operand neCond = exprOp("ne",
                sval("abc"),
                exprOp("add", sval("users:"), var("request.resource.attr.aOptionalString")));
        Operand eqCond = exprOp("eq",
                sval("abc"),
                exprOp("add", sval("users:"), var("request.resource.attr.aOptionalString")));

        ResourceEntity withValue = new ResourceEntity("ne-add-1");
        withValue.setaOptionalString("x");
        ResourceEntity withNull = new ResourceEntity("ne-add-2");
        withNull.setaOptionalString(null);

        withResource(withValue, () -> withResource(withNull, () -> {
            // Only the non-NULL row survives ne; the NULL row is a CEL error → deny.
            assertEquals(1, runCount(neCond));
            // eq stays always-false: neither row matches (NULL row denied there too).
            assertEquals(0, runCount(eqCond));
        }));
    }

    // -- DeMorgan / negated operator wrappers (PR #222) --

    @Nested
    class DeMorganNegation {

        @Test
        void notAnd() {
            // !(aBool == true && aString != "string")
            assertEquals(0, runCount(exprOp("not",
                    exprOp("and",
                            exprOp("eq", var("request.resource.attr.aBool"), bval(true)),
                            exprOp("ne", var("request.resource.attr.aString"), sval("string"))))));
        }

        @Test
        void notOr() {
            assertEquals(0, runCount(exprOp("not",
                    exprOp("or",
                            exprOp("eq", var("request.resource.attr.aBool"), bval(true)),
                            exprOp("ne", var("request.resource.attr.aString"), sval("string"))))));
        }

        @Test
        void notGt() {
            assertEquals(0, runCount(exprOp("not",
                    exprOp("gt", var("request.resource.attr.aNumber"), nval(1)))));
        }

        @Test
        void notLt() {
            assertEquals(0, runCount(exprOp("not",
                    exprOp("lt", var("request.resource.attr.aNumber"), nval(2)))));
        }

        @Test
        void notContains() {
            assertEquals(0, runCount(exprOp("not",
                    exprOp("contains", var("request.resource.attr.aString"), sval("str")))));
        }

        @Test
        void notStartsWith() {
            assertEquals(0, runCount(exprOp("not",
                    exprOp("startsWith", var("request.resource.attr.aString"), sval("str")))));
        }
    }

    // -- CEL primitives (PR #223): only empty-collection is natively supported; the rest throw --

    @Nested
    class CelPrimitives {

        @Test
        void emptyCollectionBuildsNotExists() {
            // size(R.attr.tags) == 0 — tags mapped as Relation → not-exists subquery.
            assertEquals(0, runCount(exprOp("eq",
                    exprOp("size", var("request.resource.attr.tags")),
                    nval(0))));
        }

        // add/sub/mult/div appearing as a comparison operand are supported (double-space SQL
        // arithmetic) — see ArithmeticComparisons. Only mod remains rejected.

        @Test
        void arithModThrows() {
            assertConditionThrows(
                    exprOp("eq",
                            exprOp("mod", var("request.resource.attr.aNumber"), nval(2)),
                            nval(0)),
                    "mod");
        }

        @Test
        void matchesRegexThrows() {
            assertConditionThrows(
                    exprOp("matches",
                            var("request.resource.attr.aString"), sval("^str.*")),
                    "Unsupported operator", "matches");
        }

        @Test
        void indexListThrows() {
            // ownedBy[0] == "user1" — array indexing not supported.
            assertConditionThrows(
                    exprOp("eq",
                            exprOp("index", var("request.resource.attr.ownedBy"), nval(0)),
                            sval("user1")),
                    "index");
        }

        @Test
        void convertStringThrows() {
            assertConditionThrows(
                    exprOp("eq",
                            exprOp("string", var("request.resource.attr.aNumber")),
                            sval("1")),
                    "string");
        }

        @Test
        void convertDoubleThrows() {
            assertConditionThrows(
                    exprOp("gt",
                            exprOp("double", var("request.resource.attr.aNumber")),
                            nval(1.5)),
                    "double");
        }

        @Test
        void convertIntThrows() {
            assertConditionThrows(
                    exprOp("gt",
                            exprOp("int", var("request.resource.attr.aString")),
                            nval(0)),
                    "int");
        }

        @Test
        void stringSizeComparesLength() {
            // size(aString) on a Field mapping → LENGTH(a_string) <op> N.
            // Seeded aString = "seededString" (12 chars).
            ResourceEntity r = new ResourceEntity("string-size-seed-1");
            r.setaString("seededString");
            withResource(r, () -> {
                assertEquals(1, runCount(exprOp("eq",
                        exprOp("size", var("request.resource.attr.aString")), nval(12))));
                assertEquals(0, runCount(exprOp("eq",
                        exprOp("size", var("request.resource.attr.aString")), nval(5))));
                assertEquals(1, runCount(exprOp("gt",
                        exprOp("size", var("request.resource.attr.aString")), nval(0))));
                assertEquals(0, runCount(exprOp("gt",
                        exprOp("size", var("request.resource.attr.aString")), nval(20))));
                // Value-first is mirrored: 5 < size(aString) → length > 5 → match.
                assertEquals(1, runCount(exprOp("lt",
                        nval(5),
                        exprOp("size", var("request.resource.attr.aString")))));
            });
        }
    }

    // -- Minor operator/comparison shapes (PR #234) --

    @Nested
    class MinorOperators {

        @Test
        void isNotSetBuildsIsNull() {
            // aOptionalString == null — adapter routes eq(field, null) to cb.isNull.
            assertEquals(0, runCount(exprOp("eq",
                    var("request.resource.attr.aOptionalString"), nullVal())));
        }

        @Test
        void fieldToFieldEquality() {
            // eq/ne over two variables compares the two columns directly.
            // Seeded: aString == createdBy == "same"; aOptionalString differs.
            ResourceEntity r = new ResourceEntity("f2f-seed-1");
            r.setaString("same");
            r.setCreatedBy("same");
            r.setaOptionalString("different");
            withResource(r, () -> {
                assertEquals(1, runCount(exprOp("eq",
                        var("request.resource.attr.aString"),
                        var("request.resource.attr.createdBy"))));
                assertEquals(0, runCount(exprOp("eq",
                        var("request.resource.attr.aString"),
                        var("request.resource.attr.aOptionalString"))));
                assertEquals(0, runCount(exprOp("ne",
                        var("request.resource.attr.aString"),
                        var("request.resource.attr.createdBy"))));
                assertEquals(1, runCount(exprOp("ne",
                        var("request.resource.attr.aString"),
                        var("request.resource.attr.aOptionalString"))));
            });
        }

        @Test
        void fieldToFieldOrderingKeepsOperandDirection() {
            // lt/gt over two variables must honor source order: createdBy < aString
            // with createdBy = "abc", aString = "xyz" → match; the swapped form must not.
            ResourceEntity r = new ResourceEntity("f2f-seed-2");
            r.setaString("xyz");
            r.setCreatedBy("abc");
            r.setaNumber(5);
            withResource(r, () -> {
                assertEquals(1, runCount(exprOp("lt",
                        var("request.resource.attr.createdBy"),
                        var("request.resource.attr.aString"))));
                assertEquals(0, runCount(exprOp("lt",
                        var("request.resource.attr.aString"),
                        var("request.resource.attr.createdBy"))));
                assertEquals(1, runCount(exprOp("le",
                        var("request.resource.attr.aNumber"),
                        var("request.resource.attr.aNumber"))));
                assertEquals(0, runCount(exprOp("gt",
                        var("request.resource.attr.aNumber"),
                        var("request.resource.attr.aNumber"))));
            });
        }

        @Test
        void fieldToFieldUnsupportedOperatorStillThrows() {
            // contains/startsWith/endsWith(var, var) are supported (see FieldToFieldStringMatch);
            // anything else without a column-to-column translation keeps the specific message.
            assertConditionThrows(
                    exprOp("matches",
                            var("request.resource.attr.aString"),
                            var("request.resource.attr.createdBy")),
                    "Field-to-field", "matches");
        }

        @Test
        void equalBoolFalse() {
            assertEquals(0, runCount(exprOp("eq",
                    var("request.resource.attr.aBool"), bval(false))));
        }

        @Test
        void inNumberList() {
            assertEquals(0, runCount(exprOp("in",
                    var("request.resource.attr.aNumber"),
                    listOpNumbers(1, 2, 3))));
        }

        @Test
        void orLeafExists() {
            // aBool == true OR tags.exists(t, t.name == "public")
            Operand cond = exprOp("or",
                    exprOp("eq", var("request.resource.attr.aBool"), bval(true)),
                    exprOp("exists", var("request.resource.attr.tags"),
                            lambda("t", exprOp("eq", var("t.name"), sval("public")))));
            assertEquals(0, runCount(cond));
        }
    }

    // -- Collection macro composition (PR #235) --

    @Nested
    class CollectionMacroComposition {

        @Test
        void allWithNestedAnd() {
            // tags.all(t, t.name == "public" && t.id != "tag1")
            Operand cond = exprOp("all",
                    var("request.resource.attr.tags"),
                    lambda("t", exprOp("and",
                            exprOp("eq", var("t.name"), sval("public")),
                            exprOp("ne", var("t.id"), sval("tag1")))));
            assertEquals(0, runCount(cond));
        }

        @Test
        void mapComparedToLiteralListThrows() {
            // tags.map(t, t.id) == ["tag1", "tag2"] — adapter only handles map() inside hasIntersection.
            Operand mapExpr = exprOp("map",
                    var("request.resource.attr.tags"),
                    lambda("t", var("t.id")));
            assertConditionThrows(
                    exprOp("eq", mapExpr, listOp("tag1", "tag2")),
                    "map(...)", "hasIntersection");
        }

        @Test
        void sizeOfFilterCountsMatchingElements() {
            // size(tags.filter(t, t.name == "public")) <op> N → correlated
            // (SELECT COUNT(...) WHERE lambda) <op> N. Seeded row: tags [public, public, x].
            ResourceEntity r = new ResourceEntity("size-filter-seed-1");
            r.addTag("tagA", "public");
            r.addTag("tagB", "public");
            r.addTag("tagC", "x");
            Operand filterExpr = exprOp("filter",
                    var("request.resource.attr.tags"),
                    lambda("t", exprOp("eq", var("t.name"), sval("public"))));
            withResource(r, () -> {
                assertEquals(1, runCount(exprOp("eq", exprOp("size", filterExpr), nval(2))));
                assertEquals(0, runCount(exprOp("eq", exprOp("size", filterExpr), nval(3))));
                assertEquals(1, runCount(exprOp("gt", exprOp("size", filterExpr), nval(1))));
                assertEquals(0, runCount(exprOp("gt", exprOp("size", filterExpr), nval(2))));
                // Emptiness checks work through the same path.
                assertEquals(1, runCount(exprOp("gt", exprOp("size", filterExpr), nval(0))));
                assertEquals(0, runCount(exprOp("eq", exprOp("size", filterExpr), nval(0))));
                // Value-first is mirrored: 3 > size(filter) → count < 3 → match.
                assertEquals(1, runCount(exprOp("gt", nval(3), exprOp("size", filterExpr))));
            });
        }
    }

    // -- NULL element columns under collection macros (three-valued lambda bodies) --
    // CEL semantics (cel-spec macro definitions; a NULL element column is a missing attribute,
    // so touching it is an evaluation error → deny):
    //   exists     — OR with error absorption: true if ANY element is true; error if no true
    //                and ≥1 error; false otherwise.
    //   all        — AND with error absorption: false if ANY element is false; error if no
    //                false and ≥1 error; true otherwise.
    //   exists_one — errors if ANY element errors; else true iff exactly one matches.
    // The SQL translation must map ERROR to UNKNOWN (excluded under BOTH polarities), never
    // FALSE — NOT(FALSE) = TRUE would leak rows the PDP denies.

    @Nested
    class CollectionMacroNullElements {

        private Operand existsPublic() {
            return exprOp("exists", var("request.resource.attr.tags"),
                    lambda("t", exprOp("eq", var("t.name"), sval("public"))));
        }

        private Operand allNotX() {
            return exprOp("all", var("request.resource.attr.tags"),
                    lambda("t", exprOp("ne", var("t.name"), sval("x"))));
        }

        private Operand existsOnePublic() {
            return exprOp("exists_one", var("request.resource.attr.tags"),
                    lambda("t", exprOp("eq", var("t.name"), sval("public"))));
        }

        /**
         * Probe for the UNKNOWN boolean constant the macro translations compose with: the
         * predicate {@link TriPredicate#unknown()} produces must render as a genuinely UNKNOWN
         * predicate in Hibernate 6 — matching no rows under EITHER polarity
         * (NOT(UNKNOWN) = UNKNOWN). Asserted directly against the module seam the adapter
         * composes through; the full algebra truth tables live in {@link TriPredicateTest}.
         */
        @Test
        void unknownBooleanConstantProbe() {
            withResource(new ResourceEntity("null-elem-probe"), () -> {
                EntityManager em = emf.createEntityManager();
                try {
                    CriteriaBuilder cb = em.getCriteriaBuilder();
                    TriPredicate tri = new TriPredicate(cb);
                    CriteriaQuery<Long> positive = cb.createQuery(Long.class);
                    positive.select(cb.count(positive.from(ResourceEntity.class)));
                    positive.where(tri.unknown());
                    assertEquals(0, em.createQuery(positive).getSingleResult().intValue());

                    // Junction-barriered negation — the module's own not().
                    CriteriaQuery<Long> negated = cb.createQuery(Long.class);
                    negated.select(cb.count(negated.from(ResourceEntity.class)));
                    negated.where(tri.not(tri.unknown()));
                    assertEquals(0, em.createQuery(negated).getSingleResult().intValue());
                } finally {
                    em.close();
                }
            });
        }

        @Test
        void notExistsWithNullElementExcludesRow() {
            // Single NULL-name element: exists = error (no true, one error) → deny.
            // The leak: EXISTS(name = 'public') is FALSE for the NULL element, and
            // NOT(FALSE) = TRUE would include the row.
            ResourceEntity r = new ResourceEntity("null-elem-exists-1");
            r.addTag("ne1", null);
            withResource(r, () -> {
                assertEquals(0, runCount(existsPublic()));
                assertEquals(0, runCount(exprOp("not", existsPublic())));
            });
        }

        @Test
        void existsAbsorbsErrorWhenAnotherElementIsTrue() {
            // Positive control: CEL exists absorbs errors through a true witness, so the
            // row IS included even though a sibling element is NULL.
            ResourceEntity r = new ResourceEntity("null-elem-exists-2");
            r.addTag("ne2a", null);
            r.addTag("ne2b", "public");
            withResource(r, () -> {
                assertEquals(1, runCount(existsPublic()));
                assertEquals(0, runCount(exprOp("not", existsPublic())));
            });
        }

        @Test
        void allWithNullElementAndNoFalseExcludesRow() {
            // No false element, one NULL element: all = error → deny under BOTH polarities.
            // The leak: NOT EXISTS(NOT(name != 'x')) is TRUE (the UNKNOWN body never matches).
            ResourceEntity lone = new ResourceEntity("null-elem-all-1");
            lone.addTag("na1", null);
            withResource(lone, () -> {
                assertEquals(0, runCount(allNotX()));
                assertEquals(0, runCount(exprOp("not", allNotX())));
            });

            // Mixed collection: a determined-true sibling must not mask the unknown element.
            ResourceEntity mixed = new ResourceEntity("null-elem-all-2");
            mixed.addTag("na2a", null);
            mixed.addTag("na2b", "ok");
            withResource(mixed, () -> {
                assertEquals(0, runCount(allNotX()));
                assertEquals(0, runCount(exprOp("not", allNotX())));
            });
        }

        @Test
        void allFalseElementDominatesEvenWithNullElement() {
            // CEL all absorbs errors through a false witness: all = false (not error), so
            // NOT(all) must still include the row.
            ResourceEntity r = new ResourceEntity("null-elem-all-3");
            r.addTag("na3a", "x");
            r.addTag("na3b", null);
            withResource(r, () -> {
                assertEquals(0, runCount(allNotX()));
                assertEquals(1, runCount(exprOp("not", allNotX())));
            });
        }

        @Test
        void existsOneWithNullElementExcludesRow() {
            // exists_one has NO error absorption: one true + one NULL element still errors.
            ResourceEntity oneTrueOneNull = new ResourceEntity("null-elem-one-1");
            oneTrueOneNull.addTag("no1a", "public");
            oneTrueOneNull.addTag("no1b", null);
            withResource(oneTrueOneNull, () -> {
                assertEquals(0, runCount(existsOnePublic()));
                assertEquals(0, runCount(exprOp("not", existsOnePublic())));
            });

            // Zero true + one NULL element: COUNT(...) = 1 is FALSE, and NOT would leak.
            ResourceEntity onlyNull = new ResourceEntity("null-elem-one-2");
            onlyNull.addTag("no2a", null);
            withResource(onlyNull, () -> {
                assertEquals(0, runCount(existsOnePublic()));
                assertEquals(0, runCount(exprOp("not", existsOnePublic())));
            });

            // Control: exactly one true, no NULLs — unchanged behaviour.
            ResourceEntity clean = new ResourceEntity("null-elem-one-3");
            clean.addTag("no3a", "public");
            clean.addTag("no3b", "other");
            withResource(clean, () -> {
                assertEquals(1, runCount(existsOnePublic()));
                assertEquals(0, runCount(exprOp("not", existsOnePublic())));
            });
        }

        @Test
        void filterAndExceptFollowExistsFamilySemantics() {
            Operand filterPublic = exprOp("filter", var("request.resource.attr.tags"),
                    lambda("t", exprOp("eq", var("t.name"), sval("public"))));
            Operand exceptPublic = exprOp("except", var("request.resource.attr.tags"),
                    lambda("t", exprOp("eq", var("t.name"), sval("public"))));

            ResourceEntity r = new ResourceEntity("null-elem-fe-1");
            r.addTag("fe1", null);
            withResource(r, () -> {
                assertEquals(0, runCount(filterPublic));
                assertEquals(0, runCount(exprOp("not", filterPublic)));
                assertEquals(0, runCount(exceptPublic));
                assertEquals(0, runCount(exprOp("not", exceptPublic)));
            });
        }

        @Test
        void mapIntersectionWithNullProjectionExcludesRow() {
            // CEL map() has no error absorption: a NULL projected column errors the whole
            // hasIntersection even when another element would intersect.
            Operand mapNames = exprOp("hasIntersection",
                    exprOp("map", var("request.resource.attr.tags"),
                            lambda("t", var("t.name"))),
                    listOp("public"));

            ResourceEntity withNull = new ResourceEntity("null-elem-map-1");
            withNull.addTag("nm1a", "public");
            withNull.addTag("nm1b", null);
            withResource(withNull, () -> {
                assertEquals(0, runCount(mapNames));
                assertEquals(0, runCount(exprOp("not", mapNames)));
            });

            // Control: no NULL projections — the plain intersection still matches.
            ResourceEntity clean = new ResourceEntity("null-elem-map-2");
            clean.addTag("nm2a", "public");
            withResource(clean, () -> {
                assertEquals(1, runCount(mapNames));
                assertEquals(0, runCount(exprOp("not", mapNames)));
            });
        }
    }

    @Nested
    class HierarchyOperators {

        // Helpers: a hierarchy(...) wrapper and a list(...) of segments.
        private Operand hierarchy(Operand inner, String delimiter) {
            return exprOp("hierarchy", inner, sval(delimiter));
        }

        private Operand hierarchy(Operand inner) {
            return exprOp("hierarchy", inner);
        }

        private Operand segList(Operand... segs) {
            return exprOp("list", segs);
        }

        /**
         * The standard scope fixture, chosen so a correct translation and every plausible
         * regression return DIFFERENT row sets:
         * <ul>
         *   <li>{@code "a:b"} — equal to the ancestor constant (strict-vs-inclusive
         *       discriminator: strict operators must NOT match the equal path),</li>
         *   <li>{@code "a:b:c"} — equal to the descendant constant (off-by-one strict-prefix
         *       discriminator: an IN list wrongly including the full path would match it),</li>
         *   <li>{@code "a:bb:c"} — shares the STRING prefix {@code "a:b"} but not the PATH
         *       prefix (separator-mishandling discriminator: {@code LIKE 'a:b%'} without the
         *       trailing delimiter would match it),</li>
         *   <li>{@code "a:b:c:d"} — multi-level descendant,</li>
         *   <li>{@code "x:y"} — unrelated control.</li>
         * </ul>
         * Verified against a live PDP (Cerbos 0.54.0): {@code check()} treats
         * ancestorOf/descendentOf as STRICT (the equal path is denied) and overlaps as
         * inclusive; sibling string prefixes are denied.
         */
        private static final List<String> SCOPES =
                List.of("a", "a:b", "a:b:c", "a:b:c:d", "a:bb:c", "x:y");

        /**
         * Seed one row per path — the row's ID doubles as its {@code aString} scope path —
         * run {@code body}, then delete the rows. Row-identity assertions then read
         * naturally: the expected set IS the set of matching paths.
         */
        private void withScopeRows(List<String> paths, Runnable body) {
            EntityManager em = emf.createEntityManager();
            em.getTransaction().begin();
            for (String path : paths) {
                ResourceEntity r = new ResourceEntity(path);
                r.setaString(path);
                em.persist(r);
            }
            em.getTransaction().commit();
            em.close();
            try {
                body.run();
            } finally {
                EntityManager cleanup = emf.createEntityManager();
                cleanup.getTransaction().begin();
                for (String path : paths) {
                    ResourceEntity managed = cleanup.find(ResourceEntity.class, path);
                    if (managed != null) {
                        cleanup.remove(managed);
                    }
                }
                cleanup.getTransaction().commit();
                cleanup.close();
            }
        }

        /** Translate {@code condition}, run it, and return the matched row IDs (= scope paths). */
        private Set<String> runIds(Operand condition) {
            PlanResourcesResponse resp =
                    buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);
            Result<ResourceEntity> result =
                    SpringDataQueryPlanAdapter.toSpecification(resp, MAPPER, Map.of());
            assertInstanceOf(Result.Conditional.class, result);
            Specification<ResourceEntity> spec =
                    ((Result.Conditional<ResourceEntity>) result).specification();
            EntityManager em = emf.createEntityManager();
            try {
                CriteriaBuilder cb = em.getCriteriaBuilder();
                CriteriaQuery<String> cq = cb.createQuery(String.class);
                Root<ResourceEntity> root = cq.from(ResourceEntity.class);
                cq.select(root.get("id"));
                Predicate p = spec.toPredicate(root, cq, cb);
                if (p != null) {
                    cq.where(p);
                }
                return Set.copyOf(em.createQuery(cq).getResultList());
            } finally {
                em.close();
            }
        }

        @Test
        void ancestorOfConstantPrefixOfField() {
            // ancestorOf(hierarchy("a:b", ":"), hierarchy(field, ":")) → field LIKE 'a:b:%' —
            // strict descendants only: NOT the equal path "a:b", NOT the string-prefix
            // sibling "a:bb:c" (the trailing delimiter in the pattern excludes it).
            Operand cond = exprOp("ancestorOf",
                    hierarchy(sval("a:b"), ":"),
                    hierarchy(var("request.resource.attr.aString"), ":"));
            withScopeRows(SCOPES, () ->
                    assertEquals(Set.of("a:b:c", "a:b:c:d"), runIds(cond)));
        }

        @Test
        void ancestorOfFieldPrefixOfConstant() {
            // ancestorOf(hierarchy(field, ":"), hierarchy("a:b:c", ":")) → field IN ('a', 'a:b')
            // — the strict-prefix IN list. An off-by-one regression including the full path
            // would wrongly add the "a:b:c" row (a path is not its own strict ancestor).
            Operand cond = exprOp("ancestorOf",
                    hierarchy(var("request.resource.attr.aString"), ":"),
                    hierarchy(sval("a:b:c"), ":"));
            withScopeRows(SCOPES, () ->
                    assertEquals(Set.of("a", "a:b"), runIds(cond)));
        }

        @Test
        void descendentOfFieldUnderConstant() {
            // descendentOf(hierarchy(field, ":"), hierarchy("a:b", ":")) → field LIKE 'a:b:%'.
            // Same discriminators as ancestorOfConstantPrefixOfField (mirrored operator).
            Operand cond = exprOp("descendentOf",
                    hierarchy(var("request.resource.attr.aString"), ":"),
                    hierarchy(sval("a:b"), ":"));
            withScopeRows(SCOPES, () ->
                    assertEquals(Set.of("a:b:c", "a:b:c:d"), runIds(cond)));
        }

        @Test
        void descendentOfConstantUnderField() {
            // Constant-first operand order: descendentOf(hierarchy("a:b:c", ":"),
            // hierarchy(field, ":")) — the FIELD is the ancestor side, routing to the
            // strict-prefix IN-list branch: field IN ('a', 'a:b').
            Operand cond = exprOp("descendentOf",
                    hierarchy(sval("a:b:c"), ":"),
                    hierarchy(var("request.resource.attr.aString"), ":"));
            withScopeRows(SCOPES, () ->
                    assertEquals(Set.of("a", "a:b"), runIds(cond)));
        }

        @Test
        void overlapsFieldHierarchyWithConstant() {
            // overlaps(hierarchy(field, ":"), hierarchy("a:b", ":"))
            //   → field IN ('a') OR field = 'a:b' OR field LIKE 'a:b:%' — inclusive in both
            // directions (ancestors, the equal path, and descendants), but never the
            // string-prefix sibling "a:bb:c" or the unrelated "x:y".
            Operand cond = exprOp("overlaps",
                    hierarchy(var("request.resource.attr.aString"), ":"),
                    hierarchy(sval("a:b"), ":"));
            withScopeRows(SCOPES, () ->
                    assertEquals(Set.of("a", "a:b", "a:b:c", "a:b:c:d"), runIds(cond)));
        }

        @Test
        void overlapsConstantHierarchyWithField() {
            // Constant-first operand order: overlaps is symmetric, so the same union must
            // come back with the operands mirrored.
            Operand cond = exprOp("overlaps",
                    hierarchy(sval("a:b"), ":"),
                    hierarchy(var("request.resource.attr.aString"), ":"));
            withScopeRows(SCOPES, () ->
                    assertEquals(Set.of("a", "a:b", "a:b:c", "a:b:c:d"), runIds(cond)));
        }

        @Test
        void ancestorOfSingleSegmentConstantMatchesNothing() {
            // ancestorOf(field, "a") — a single-segment path has NO strict ancestors, so the
            // translation is always-false: even the row whose scope is exactly "a" must not
            // match (a path is not its own ancestor).
            Operand cond = exprOp("ancestorOf",
                    hierarchy(var("request.resource.attr.aString"), ":"),
                    hierarchy(sval("a"), ":"));
            withScopeRows(SCOPES, () ->
                    assertEquals(Set.of(), runIds(cond)));
        }

        @Test
        void descendentOfSingleSegmentConstant() {
            // descendentOf(field, "a") → field LIKE 'a:%': every path under the root —
            // including the sibling branch "a:bb:c" (a genuine descendant of "a") — but not
            // the root itself and not "x:y".
            Operand cond = exprOp("descendentOf",
                    hierarchy(var("request.resource.attr.aString"), ":"),
                    hierarchy(sval("a"), ":"));
            withScopeRows(SCOPES, () ->
                    assertEquals(Set.of("a:b", "a:b:c", "a:b:c:d", "a:bb:c"), runIds(cond)));
        }

        @Test
        void ancestorOfMetacharacterSegments() {
            // Field-first with LIKE metacharacters in the constant path: the strict-prefix
            // IN list ('50%', '50%:a_b') compares by equality — no pattern matching, so the
            // '50x'/'50_' rows must not match.
            Operand cond = exprOp("ancestorOf",
                    hierarchy(var("request.resource.attr.aString"), ":"),
                    hierarchy(sval("50%:a_b:x"), ":"));
            withScopeRows(List.of("50%", "50%:a_b", "50x", "50_", "50%:a_b:x"), () ->
                    assertEquals(Set.of("50%", "50%:a_b"), runIds(cond)));
        }

        @Test
        void descendentOfMetacharacterPrefixIsEscaped() {
            // Constant-first LIKE branch: the prefix '50%:a_b:' must be escaped so '%' and
            // '_' are literals. Unescaped, LIKE '50%:a_b:%' would match the '50x:a_b:y'
            // (via %) and '50%:aXb:y' (via _) traps; the equal path '50%:a_b' pins strictness.
            Operand cond = exprOp("descendentOf",
                    hierarchy(var("request.resource.attr.aString"), ":"),
                    hierarchy(sval("50%:a_b"), ":"));
            withScopeRows(List.of("50%:a_b:y", "50x:a_b:y", "50%:aXb:y", "50%:a_b"), () ->
                    assertEquals(Set.of("50%:a_b:y"), runIds(cond)));
        }

        @Test
        void descendentOfBracketPrefixMatchesLiteralBracketPaths() {
            // '[' in a path segment: the prefix pattern must escape it as '\[' because SQL
            // Server LIKE treats '[...]' as a character class even under ESCAPE. On H2 the
            // escaped pattern must keep matching the LITERAL '[env]:prod' descendants and
            // must never match 'e:prod:eu' — the row an unescaped '[env]' class would match
            // one character of on SQL Server. The equal path pins strictness.
            Operand cond = exprOp("descendentOf",
                    hierarchy(var("request.resource.attr.aString"), ":"),
                    hierarchy(sval("[env]:prod"), ":"));
            withScopeRows(List.of("[env]:prod:eu", "e:prod:eu", "[env]:prod"), () ->
                    assertEquals(Set.of("[env]:prod:eu"), runIds(cond)));
        }

        @Test
        void overlapsSegmentedWithField() {
            // The policy shape: hierarchy("projects:123", ":").overlaps(hierarchy(["projects", R.id]))
            //   → segment-wise: const "projects" matches, then field == "123".
            Operand cond = exprOp("overlaps",
                    hierarchy(sval("projects:123"), ":"),
                    hierarchy(segList(sval("projects"), var("request.resource.attr.aString"))));
            withScopeRows(List.of("123", "456", "projects"), () ->
                    assertEquals(Set.of("123"), runIds(cond)));
        }

        @Test
        void overlapsConstantsMatchingPrefixIsAlwaysTrue() {
            // overlaps("a", "a:b") — "a" is a prefix of "a:b", all constant → unconditionally
            // true: EVERY seeded row must come back (a regression to always-false returns none).
            Operand cond = exprOp("overlaps",
                    hierarchy(sval("a"), ":"),
                    hierarchy(sval("a:b"), ":"));
            withScopeRows(SCOPES, () ->
                    assertEquals(Set.copyOf(SCOPES), runIds(cond)));
        }

        @Test
        void ancestorOfConstantsSatisfied() {
            // ancestorOf("a", "a:b") — satisfied by constants alone → unconditionally true:
            // every seeded row comes back regardless of its own scope value.
            Operand cond = exprOp("ancestorOf",
                    hierarchy(sval("a"), ":"),
                    hierarchy(sval("a:b"), ":"));
            withScopeRows(SCOPES, () ->
                    assertEquals(Set.copyOf(SCOPES), runIds(cond)));

            // A trailing delimiter is a real (empty) segment: "a:b:" splits to ["a","b",""],
            // so "a:b" is still a strict prefix. If splitLiteral dropped trailing empties this
            // would throw "do not satisfy" instead of translating to always-true.
            Operand trailing = exprOp("ancestorOf",
                    hierarchy(sval("a:b"), ":"),
                    hierarchy(sval("a:b:"), ":"));
            withScopeRows(SCOPES, () ->
                    assertEquals(Set.copyOf(SCOPES), runIds(trailing)));
        }

        @Test
        void overlapsIncompatibleConstantsWithoutFieldThrows() {
            // overlaps("a:b", "x:y") — no prefix relationship and no field to constrain → planner bug.
            assertConditionThrows(
                    exprOp("overlaps",
                            hierarchy(sval("a:b"), ":"),
                            hierarchy(sval("x:y"), ":")),
                    "Cannot determine hierarchy overlap");
        }

        @Test
        void ancestorOfConstantsNotSatisfiedThrows() {
            assertConditionThrows(
                    exprOp("ancestorOf",
                            hierarchy(sval("x"), ":"),
                            hierarchy(sval("a:b"), ":")),
                    "ancestorOf", "do not satisfy");
        }

        @Test
        void nonHierarchyOperandThrows() {
            assertConditionThrows(
                    exprOp("overlaps",
                            var("request.resource.attr.aString"),
                            sval("a:b")),
                    "overlaps", "hierarchy(...) operands");
        }

        @Test
        void twoFieldHierarchiesInOverlapThrows() {
            assertConditionThrows(
                    exprOp("overlaps",
                            hierarchy(var("request.resource.attr.aString"), ":"),
                            hierarchy(var("request.resource.attr.createdBy"), ":")),
                    "two field-reference hierarchies");
        }
    }

    // -- Operand order: the planner preserves policy source order, so a value (or folded
    // constant) can appear BEFORE the field. Directional operators must mirror or results are
    // silently inverted. These tests seed a real row because an empty table cannot distinguish
    // `x < 3` from `x > 3`.

    @Nested
    class OperandOrderSemantics {

        private ResourceEntity seeded() {
            ResourceEntity r = new ResourceEntity("seed-1");
            r.setaBool(true);
            r.setaString("seededString");
            r.setaNumber(5);
            r.setOwnedBy(new ArrayList<>(List.of("user1")));
            r.addTag("tagX", "x");
            return r;
        }

        @Test
        void ltValueFirstMeansFieldGreaterThan() {
            // 3 < aNumber, with aNumber = 5 → must match. The naive (unmirrored) translation
            // `aNumber < 3` would return 0.
            withResource(seeded(), () -> {
                assertEquals(1, runCount(exprOp("lt", nval(3), var("request.resource.attr.aNumber"))));
                // Control: field-first form keeps its meaning.
                assertEquals(0, runCount(exprOp("lt", var("request.resource.attr.aNumber"), nval(3))));
            });
        }

        @Test
        void gtValueFirstMeansFieldLessThan() {
            // 10 > aNumber, with aNumber = 5 → match.
            withResource(seeded(), () ->
                    assertEquals(1, runCount(exprOp("gt", nval(10), var("request.resource.attr.aNumber")))));
        }

        @Test
        void leGeValueFirstAreMirrored() {
            withResource(seeded(), () -> {
                // 5 <= aNumber → aNumber >= 5 → match
                assertEquals(1, runCount(exprOp("le", nval(5), var("request.resource.attr.aNumber"))));
                // 4 >= aNumber → aNumber <= 4 → no match
                assertEquals(0, runCount(exprOp("ge", nval(4), var("request.resource.attr.aNumber"))));
            });
        }

        @Test
        void sizeValueFirstNonEmptyCheck() {
            // 0 < size(ownedBy) → EXISTS; seeded row has one owner.
            withResource(seeded(), () ->
                    assertEquals(1, runCount(exprOp("lt",
                            nval(0),
                            exprOp("size", var("request.resource.attr.ownedBy"))))));
        }

        @Test
        void sizeValueFirstEmptinessCheck() {
            // 1 > size(ownedBy) → size < 1 → NOT EXISTS; seeded row is non-empty → 0.
            withResource(seeded(), () ->
                    assertEquals(0, runCount(exprOp("gt",
                            nval(1),
                            exprOp("size", var("request.resource.attr.ownedBy"))))));
        }

        @Test
        void addFoldedConstantValueFirstIsMirrored() {
            // (1 + 2) < aNumber → aNumber > 3, with aNumber = 5 → match.
            withResource(seeded(), () ->
                    assertEquals(1, runCount(exprOp("lt",
                            exprOp("add", nval(1), nval(2)),
                            var("request.resource.attr.aNumber")))));
        }

        @Test
        void hasIntersectionValueFirstIsSymmetric() {
            // hasIntersection(["user1","other"], R.attr.ownedBy) — value list first.
            withResource(seeded(), () ->
                    assertEquals(1, runCount(exprOp("hasIntersection",
                            listOp("user1", "other"),
                            var("request.resource.attr.ownedBy")))));
        }

        @Test
        void hasIntersectionSnakeCaseAliasIsAccepted() {
            // The PDP still accepts the deprecated has_intersection spelling in policies.
            withResource(seeded(), () ->
                    assertEquals(1, runCount(exprOp("has_intersection",
                            var("request.resource.attr.ownedBy"),
                            listOp("user1")))));
        }

        @Test
        void overrideIsConsultedUnderMirroredOperator() {
            // 3 < aNumber builds a gt predicate — the override must be looked up as "gt".
            Operand cond = exprOp("lt", nval(3), var("request.resource.attr.aNumber"));
            assertThrows(OverrideInvoked.class,
                    () -> runCount(cond, Map.of("gt", THROWING_OVERRIDE)));
        }
    }

    // -- CEL ternary: `if(cond, then, else)` is rewritten into pure
    // predicates — cmp(if(c,a,b), other) → (c AND cmp(a, other)) OR (NOT c AND cmp(b, other)).
    // Seeds real rows because an empty table cannot distinguish the branch predicates.

    @Nested
    class TernaryIfExpressions {

        /** gt(if(aBool, aNumber, 0), 0) — the canonical `(R.attr.aBool ? R.attr.aNumber : 0) > 0`. */
        private Operand canonicalPlan() {
            return exprOp("gt",
                    exprOp("if",
                            var("request.resource.attr.aBool"),
                            var("request.resource.attr.aNumber"),
                            nval(0)),
                    nval(0));
        }

        @Test
        void comparisonWrappingTernary() {
            // aBool = true → then-branch compares aNumber > 0.
            ResourceEntity match = new ResourceEntity("ternary-seed-1");
            match.setaBool(true);
            match.setaNumber(10);
            withResource(match, () -> assertEquals(1, runCount(canonicalPlan())));

            ResourceEntity zeroThen = new ResourceEntity("ternary-seed-2");
            zeroThen.setaBool(true);
            zeroThen.setaNumber(0);
            withResource(zeroThen, () -> assertEquals(0, runCount(canonicalPlan())));

            // aBool = false → else branch folds to gt(0, 0) → always false, whatever aNumber is.
            ResourceEntity elseBranch = new ResourceEntity("ternary-seed-3");
            elseBranch.setaBool(false);
            elseBranch.setaNumber(10);
            withResource(elseBranch, () -> assertEquals(0, runCount(canonicalPlan())));
        }

        @Test
        void bareBooleanTernary() {
            // aBool ? aString == "x" : aNumber > 5 — the ternary IS the condition, both
            // branches are boolean expressions.
            Operand plan = exprOp("if",
                    var("request.resource.attr.aBool"),
                    exprOp("eq", var("request.resource.attr.aString"), sval("x")),
                    exprOp("gt", var("request.resource.attr.aNumber"), nval(5)));

            ResourceEntity thenMatch = new ResourceEntity("ternary-bare-1");
            thenMatch.setaBool(true);
            thenMatch.setaString("x");
            thenMatch.setaNumber(0);
            withResource(thenMatch, () -> assertEquals(1, runCount(plan)));

            // then-branch active but not satisfied — the else branch must NOT rescue the row.
            ResourceEntity thenMiss = new ResourceEntity("ternary-bare-2");
            thenMiss.setaBool(true);
            thenMiss.setaString("y");
            thenMiss.setaNumber(10);
            withResource(thenMiss, () -> assertEquals(0, runCount(plan)));

            ResourceEntity elseMatch = new ResourceEntity("ternary-bare-3");
            elseMatch.setaBool(false);
            elseMatch.setaString("x");
            elseMatch.setaNumber(10);
            withResource(elseMatch, () -> assertEquals(1, runCount(plan)));

            ResourceEntity elseMiss = new ResourceEntity("ternary-bare-4");
            elseMiss.setaBool(false);
            elseMiss.setaString("x");
            elseMiss.setaNumber(1);
            withResource(elseMiss, () -> assertEquals(0, runCount(plan)));
        }

        @Test
        void bareBooleanTernaryWithConstantBranch() {
            // aBool ? true : aNumber > 5 — a boolean VALUE branch folds to 1=1 / 1=0.
            Operand plan = exprOp("if",
                    var("request.resource.attr.aBool"),
                    bval(true),
                    exprOp("gt", var("request.resource.attr.aNumber"), nval(5)));

            ResourceEntity thenMatch = new ResourceEntity("ternary-bare-const-1");
            thenMatch.setaBool(true);
            thenMatch.setaNumber(0);
            withResource(thenMatch, () -> assertEquals(1, runCount(plan)));

            ResourceEntity elseMiss = new ResourceEntity("ternary-bare-const-2");
            elseMiss.setaBool(false);
            elseMiss.setaNumber(1);
            withResource(elseMiss, () -> assertEquals(0, runCount(plan)));

            // aBool ? false : aNumber > 5 — a false then-branch excludes matching-condition rows.
            Operand planFalse = exprOp("if",
                    var("request.resource.attr.aBool"),
                    bval(false),
                    exprOp("gt", var("request.resource.attr.aNumber"), nval(5)));
            ResourceEntity falseThen = new ResourceEntity("ternary-bare-const-3");
            falseThen.setaBool(true);
            falseThen.setaNumber(10);
            withResource(falseThen, () -> assertEquals(0, runCount(planFalse)));
        }

        @Test
        void valueFirstComparisonIsMirrored() {
            // 3 < (aBool ? aNumber : 0) — planner preserves source order, so the constant sits
            // on the LEFT. Branch substitution keeps positions, and NormalizedBinary mirrors the
            // recursed comparisons: lt(3, aNumber) → aNumber > 3.
            Operand plan = exprOp("lt",
                    nval(3),
                    exprOp("if",
                            var("request.resource.attr.aBool"),
                            var("request.resource.attr.aNumber"),
                            nval(0)));

            ResourceEntity match = new ResourceEntity("ternary-mirror-1");
            match.setaBool(true);
            match.setaNumber(5);
            withResource(match, () -> assertEquals(1, runCount(plan)));

            // Naive (unmirrored) translation `aNumber < 3` would wrongly match this row.
            ResourceEntity below = new ResourceEntity("ternary-mirror-2");
            below.setaBool(true);
            below.setaNumber(2);
            withResource(below, () -> assertEquals(0, runCount(plan)));

            // else branch: 3 < 0 folds to always-false — aNumber must not leak in.
            ResourceEntity elseRow = new ResourceEntity("ternary-mirror-3");
            elseRow.setaBool(false);
            elseRow.setaNumber(99);
            withResource(elseRow, () -> assertEquals(0, runCount(plan)));
        }

        @Test
        void nestedTernaryInBranch() {
            // (aBool ? (aString == "x" ? aNumber : 1) : 0) > 2 — the then-branch is itself a
            // ternary; substitution recurses until no `if` remains.
            Operand plan = exprOp("gt",
                    exprOp("if",
                            var("request.resource.attr.aBool"),
                            exprOp("if",
                                    exprOp("eq", var("request.resource.attr.aString"), sval("x")),
                                    var("request.resource.attr.aNumber"),
                                    nval(1)),
                            nval(0)),
                    nval(2));

            ResourceEntity innerThen = new ResourceEntity("ternary-nested-1");
            innerThen.setaBool(true);
            innerThen.setaString("x");
            innerThen.setaNumber(5);
            withResource(innerThen, () -> assertEquals(1, runCount(plan)));

            // Inner else: 1 > 2 → always false, even though aNumber would match.
            ResourceEntity innerElse = new ResourceEntity("ternary-nested-2");
            innerElse.setaBool(true);
            innerElse.setaString("y");
            innerElse.setaNumber(5);
            withResource(innerElse, () -> assertEquals(0, runCount(plan)));

            // Outer else: 0 > 2 → always false.
            ResourceEntity outerElse = new ResourceEntity("ternary-nested-3");
            outerElse.setaBool(false);
            outerElse.setaString("x");
            outerElse.setaNumber(5);
            withResource(outerElse, () -> assertEquals(0, runCount(plan)));
        }

        @Test
        void ternaryUnderLogicalOperators() {
            // The rewrite produces an OR-of-ANDs; it must compose under not/and/or like any
            // other predicate (negation goes through the junction-barrier helper).
            Operand comparison = exprOp("gt",
                    exprOp("if",
                            var("request.resource.attr.aBool"),
                            var("request.resource.attr.aNumber"),
                            nval(0)),
                    nval(0));

            ResourceEntity truthy = new ResourceEntity("ternary-logic-1");
            truthy.setaBool(true);
            truthy.setaString("x");
            truthy.setaNumber(10);
            withResource(truthy, () -> {
                assertEquals(0, runCount(exprOp("not", comparison)));
                // Double negation must toggle back (junction barrier, not raw cb.not).
                assertEquals(1, runCount(exprOp("not", exprOp("not", comparison))));
                assertEquals(1, runCount(exprOp("and", comparison,
                        exprOp("eq", var("request.resource.attr.aString"), sval("x")))));
                assertEquals(0, runCount(exprOp("and", comparison,
                        exprOp("eq", var("request.resource.attr.aString"), sval("z")))));
                assertEquals(1, runCount(exprOp("or", comparison,
                        exprOp("eq", var("request.resource.attr.aString"), sval("z")))));
            });

            // Row where the ternary comparison is false: NOT must select it, OR must rescue it
            // only through the other arm.
            ResourceEntity falsy = new ResourceEntity("ternary-logic-2");
            falsy.setaBool(false);
            falsy.setaString("x");
            falsy.setaNumber(10);
            withResource(falsy, () -> {
                assertEquals(1, runCount(exprOp("not", comparison)));
                assertEquals(0, runCount(exprOp("not", exprOp("not", comparison))));
                assertEquals(1, runCount(exprOp("or", comparison,
                        exprOp("eq", var("request.resource.attr.aString"), sval("x")))));
                assertEquals(0, runCount(exprOp("or", comparison,
                        exprOp("eq", var("request.resource.attr.aString"), sval("z")))));
            });
        }

        @Test
        void ternaryWithExpressionCondition() {
            // (aNumber > 5 ? aString : "none") == "x" — condition is a boolean EXPRESSION,
            // not a bare variable.
            Operand plan = exprOp("eq",
                    exprOp("if",
                            exprOp("gt", var("request.resource.attr.aNumber"), nval(5)),
                            var("request.resource.attr.aString"),
                            sval("none")),
                    sval("x"));

            ResourceEntity condTrue = new ResourceEntity("ternary-exprcond-1");
            condTrue.setaNumber(10);
            condTrue.setaString("x");
            withResource(condTrue, () -> assertEquals(1, runCount(plan)));

            // Condition false → eq("none", "x") folds to always-false.
            ResourceEntity condFalse = new ResourceEntity("ternary-exprcond-2");
            condFalse.setaNumber(1);
            condFalse.setaString("x");
            withResource(condFalse, () -> assertEquals(0, runCount(plan)));
        }

        @Test
        void eqNeWithTernary() {
            // (aBool ? aString : "none") == "x"  /  != "x"
            Operand ternary = exprOp("if",
                    var("request.resource.attr.aBool"),
                    var("request.resource.attr.aString"),
                    sval("none"));
            Operand eqPlan = exprOp("eq", ternary, sval("x"));
            Operand nePlan = exprOp("ne", ternary, sval("x"));

            ResourceEntity thenX = new ResourceEntity("ternary-eqne-1");
            thenX.setaBool(true);
            thenX.setaString("x");
            withResource(thenX, () -> {
                assertEquals(1, runCount(eqPlan));
                assertEquals(0, runCount(nePlan));
            });

            ResourceEntity thenY = new ResourceEntity("ternary-eqne-2");
            thenY.setaBool(true);
            thenY.setaString("y");
            withResource(thenY, () -> {
                assertEquals(0, runCount(eqPlan));
                assertEquals(1, runCount(nePlan));
            });

            // else branch folds: eq("none", "x") → always false; ne("none", "x") → always true.
            ResourceEntity elseRow = new ResourceEntity("ternary-eqne-3");
            elseRow.setaBool(false);
            elseRow.setaString("x");
            withResource(elseRow, () -> {
                assertEquals(0, runCount(eqPlan));
                assertEquals(1, runCount(nePlan));
            });
        }

        @Test
        void constantVersusConstantComparisonsFold() {
            // (aBool ? 1 : 0) > 0 — BOTH branches collapse to constant comparisons, leaving
            // only the condition predicate. Seeded row: matches iff aBool is true.
            Operand allConstBranches = exprOp("gt",
                    exprOp("if", var("request.resource.attr.aBool"), nval(1), nval(0)),
                    nval(0));

            ResourceEntity boolTrue = new ResourceEntity("ternary-const-1");
            boolTrue.setaBool(true);
            withResource(boolTrue, () -> {
                assertEquals(1, runCount(allConstBranches));

                // Direct value-vs-value plans exercise the fold through the public seam:
                // numbers compare in double space (1.0 == 1, 0.5 < 1), strings via compareTo,
                // mixed incomparable types are eq → false / ne → true.
                assertEquals(1, runCount(exprOp("eq", nval(1.0), nval(1))));
                assertEquals(1, runCount(exprOp("lt", nval(0.5), nval(1))));
                assertEquals(0, runCount(exprOp("gt", nval(0), nval(0))));
                assertEquals(1, runCount(exprOp("ge", nval(2), nval(2))));
                assertEquals(1, runCount(exprOp("lt", sval("a"), sval("b"))));
                assertEquals(0, runCount(exprOp("eq", sval("a"), nval(1))));
                assertEquals(1, runCount(exprOp("ne", sval("a"), nval(1))));
                assertEquals(1, runCount(exprOp("eq", bval(true), bval(true))));
                // Whole-number constants beyond the long range must stay doubles: a
                // saturating (long) cast collapses 1.0e19 and 9.3e18 both to
                // Long.MAX_VALUE, inverting these comparisons.
                assertEquals(1, runCount(exprOp("gt", nval(1.0e19), nval(9.3e18))));
                assertEquals(1, runCount(exprOp("ne", nval(1.0e19), nval(9.3e18))));
                assertEquals(0, runCount(exprOp("eq", nval(-1.0e19), nval(-9.3e18))));
                // Ordering incomparable constant types is a planner bug and must throw.
                assertConditionThrows(exprOp("lt", sval("a"), nval(1)),
                        "Cannot order", "lt");
            });

            ResourceEntity boolFalse = new ResourceEntity("ternary-const-2");
            boolFalse.setaBool(false);
            withResource(boolFalse, () -> assertEquals(0, runCount(allConstBranches)));
        }

        @Test
        void negatedTernaryWithNullConditionExcludesRow() {
            // !((aOptionalString != "x" ? aNumber : 0.0) > 1.0) — a NULL condition column is
            // a CEL evaluation error (deny), so the SQL must be UNKNOWN under BOTH polarities.
            Operand comparison = exprOp("gt",
                    exprOp("if",
                            exprOp("ne", var("request.resource.attr.aOptionalString"), sval("x")),
                            var("request.resource.attr.aNumber"),
                            nval(0.0)),
                    nval(1.0));

            // Both branch comparisons false: (NULL AND FALSE) OR (NULL AND FALSE) must not
            // collapse to FALSE — NOT(FALSE) = TRUE would leak the row.
            ResourceEntity bothBranchesFalse = new ResourceEntity("ternary-nullcond-1");
            bothBranchesFalse.setaOptionalString(null);
            bothBranchesFalse.setaNumber(0);
            withResource(bothBranchesFalse, () -> {
                assertEquals(0, runCount(comparison));
                assertEquals(0, runCount(exprOp("not", comparison)));
            });

            // Then-branch true: still UNKNOWN (the PDP denies), excluded either way.
            ResourceEntity thenBranchTrue = new ResourceEntity("ternary-nullcond-2");
            thenBranchTrue.setaOptionalString(null);
            thenBranchTrue.setaNumber(5);
            withResource(thenBranchTrue, () -> {
                assertEquals(0, runCount(comparison));
                assertEquals(0, runCount(exprOp("not", comparison)));
            });

            // Known-condition control: the UNKNOWN arm must vanish for non-NULL conditions.
            ResourceEntity knownCondition = new ResourceEntity("ternary-nullcond-3");
            knownCondition.setaOptionalString("y");
            knownCondition.setaNumber(5);
            withResource(knownCondition, () -> {
                assertEquals(1, runCount(comparison));
                assertEquals(0, runCount(exprOp("not", comparison)));
            });
        }

        @Test
        void negatedBareTernaryWithNullConditionExcludesRow() {
            // aOptionalString != "x" ? aNumber > 1 : aBool — bare boolean-position ternary
            // with a NULL condition column: same UNKNOWN-not-FALSE contract as above.
            Operand plan = exprOp("if",
                    exprOp("ne", var("request.resource.attr.aOptionalString"), sval("x")),
                    exprOp("gt", var("request.resource.attr.aNumber"), nval(1)),
                    var("request.resource.attr.aBool"));

            ResourceEntity nullCondition = new ResourceEntity("ternary-barenull-1");
            nullCondition.setaOptionalString(null);
            nullCondition.setaNumber(0);
            nullCondition.setaBool(false);
            withResource(nullCondition, () -> {
                assertEquals(0, runCount(plan));
                assertEquals(0, runCount(exprOp("not", plan)));
            });
        }

        @Test
        void ternaryWithWrongOperandCountThrows() {
            // if() with 2 operands inside a comparison — malformed plan, not a silent drop.
            assertConditionThrows(
                    exprOp("gt",
                            exprOp("if", var("request.resource.attr.aBool"), nval(1)),
                            nval(0)),
                    "if (ternary) requires exactly 3 operands", "got 2");
            // Same contract for a bare-boolean-position ternary.
            assertConditionThrows(
                    exprOp("if", var("request.resource.attr.aBool"), bval(true)),
                    "if (ternary) requires exactly 3 operands", "got 2");
        }

        @Test
        void ternaryUnderUnsupportedWrapperNamesOperator() {
            // contains(if(...), "x") — only eq/ne/lt/gt/le/ge accept a ternary operand; the
            // error must name the offending wrapper operator.
            assertConditionThrows(
                    exprOp("contains",
                            exprOp("if",
                                    var("request.resource.attr.aBool"),
                                    var("request.resource.attr.aString"),
                                    sval("none")),
                            sval("x")),
                    "if()", "contains");
        }
    }

    // -- Lambda bodies referencing outer (non-lambda) resource attributes --

    @Nested
    class OuterReferencesInsideLambda {

        @Test
        void outerAttributeInsideExistsLambda() {
            // R.attr.tags.exists(t, t.name == "x" && R.attr.aBool) — the PDP keeps the residual
            // R.attr.aBool INSIDE the lambda body; it must resolve against the correlated outer
            // entity, not the joined tag.
            ResourceEntity r = new ResourceEntity("seed-2");
            r.setaBool(true);
            r.setaNumber(1);
            r.setaString("s");
            r.addTag("tagX", "x");

            withResource(r, () -> {
                Operand cond = exprOp("exists",
                        var("request.resource.attr.tags"),
                        lambda("t", exprOp("and",
                                exprOp("eq", var("t.name"), sval("x")),
                                var("request.resource.attr.aBool"))));
                assertEquals(1, runCount(cond));

                // Same shape with a non-matching outer comparison → excluded.
                Operand condNoMatch = exprOp("exists",
                        var("request.resource.attr.tags"),
                        lambda("t", exprOp("and",
                                exprOp("eq", var("t.name"), sval("x")),
                                exprOp("eq", var("request.resource.attr.aBool"), bval(false)))));
                assertEquals(0, runCount(condNoMatch));
            });
        }
    }

    /**
     * Structural join-anchoring defects:
     *
     * <p>W1 — a dotted relation CHAIN ({@code categories.subCategories}) must join through
     * every intermediate hop. Resolving only the tail Relation and joining its attribute off
     * the root either fails at query-build time (the root has no such attribute) or — worse —
     * silently joins a same-named collection on the wrong entity. Chain semantics are the
     * FLATTENED union of tail elements across all intermediate hops, which is exactly what a
     * correlated join chain expresses for exists/in/hasIntersection and a JOIN-through COUNT
     * expresses for size().
     *
     * <p>W2 — a subquery for a relation referenced inside a lambda body must correlate the
     * From that OWNS the relation attribute. {@code R.attr.tags} inside a
     * {@code categories.exists(c, ...)} lambda resolves through the outer scope against the
     * ROOT entity; anchoring the tags join to the lambda's category join instead is a wrong
     * From — build-time failure or a silent wrong join if the element entity had a same-named
     * collection.
     */
    @Nested
    class MultiHopRelationChains {

        private static final String CHAIN = "request.resource.attr.categories.subCategories";

        private final Map<String, AttributeMapping> chainMapper = Map.ofEntries(
                Map.entry("request.resource.attr.aString", AttributeMapping.field("aString")),
                Map.entry("request.resource.attr.tags", AttributeMapping.relation("tags", Map.of(
                        "id", AttributeMapping.field("id"),
                        "name", AttributeMapping.field("name")))),
                Map.entry("request.resource.attr.categories", AttributeMapping.relation("categories", Map.of(
                        "name", AttributeMapping.field("name"),
                        "subCategories", AttributeMapping.relation("subCategories", "name", Map.of(
                                "name", AttributeMapping.field("name")))))));

        private int runChainCount(Operand condition) {
            return runCount(condition, chainMapper, Map.of());
        }

        /**
         * Persist a resource plus its (non-cascaded) category/sub-category graph, run
         * {@code body}, then delete everything again — the shared in-memory schema must stay
         * empty for the other tests.
         */
        private void withCategoryGraph(ResourceEntity resource,
                                       List<CategoryEntity> categories,
                                       List<SubCategoryEntity> subCategories,
                                       Runnable body) {
            EntityManager em = emf.createEntityManager();
            em.getTransaction().begin();
            subCategories.forEach(em::persist);
            categories.forEach(em::persist);
            em.persist(resource);
            em.getTransaction().commit();
            em.close();
            try {
                body.run();
            } finally {
                EntityManager cleanup = emf.createEntityManager();
                cleanup.getTransaction().begin();
                ResourceEntity managed = cleanup.find(ResourceEntity.class, resource.getId());
                if (managed != null) {
                    cleanup.remove(managed);
                }
                for (CategoryEntity c : categories) {
                    CategoryEntity mc = cleanup.find(CategoryEntity.class, c.getId());
                    if (mc != null) {
                        cleanup.remove(mc);
                    }
                }
                for (SubCategoryEntity s : subCategories) {
                    SubCategoryEntity ms = cleanup.find(SubCategoryEntity.class, s.getId());
                    if (ms != null) {
                        cleanup.remove(ms);
                    }
                }
                cleanup.getTransaction().commit();
                cleanup.close();
            }
        }

        @Test
        void existsOverTwoHopChainJoinsThroughIntermediateHop() {
            var fin = new SubCategoryEntity("chain-sub-e1", "finance");
            var biz = new CategoryEntity("chain-cat-e1", "business");
            biz.setSubCategories(List.of(fin));
            ResourceEntity r = new ResourceEntity("chain-r-e1");
            r.setCategories(List.of(biz));

            withCategoryGraph(r, List.of(biz), List.of(fin), () -> {
                Operand matching = exprOp("exists", var(CHAIN),
                        lambda("s", exprOp("eq", var("s.name"), sval("finance"))));
                assertEquals(1, runChainCount(matching));

                Operand nonMatching = exprOp("exists", var(CHAIN),
                        lambda("s", exprOp("eq", var("s.name"), sval("nope"))));
                assertEquals(0, runChainCount(nonMatching));
            });
        }

        @Test
        void inOverTwoHopChainJoinsThroughIntermediateHop() {
            var fin = new SubCategoryEntity("chain-sub-i1", "finance");
            var biz = new CategoryEntity("chain-cat-i1", "business");
            biz.setSubCategories(List.of(fin));
            ResourceEntity r = new ResourceEntity("chain-r-i1");
            r.setCategories(List.of(biz));

            withCategoryGraph(r, List.of(biz), List.of(fin), () -> {
                // "finance" in R.attr.categories.subCategories — value-first, as the planner
                // preserves source order; membership tests the tail's defaultMemberField (name).
                assertEquals(1, runChainCount(exprOp("in", sval("finance"), var(CHAIN))));
                assertEquals(0, runChainCount(exprOp("in", sval("nope"), var(CHAIN))));
            });
        }

        @Test
        void hasIntersectionOverTwoHopChainJoinsThroughIntermediateHop() {
            var fin = new SubCategoryEntity("chain-sub-h1", "finance");
            var biz = new CategoryEntity("chain-cat-h1", "business");
            biz.setSubCategories(List.of(fin));
            ResourceEntity r = new ResourceEntity("chain-r-h1");
            r.setCategories(List.of(biz));

            withCategoryGraph(r, List.of(biz), List.of(fin), () -> {
                assertEquals(1, runChainCount(
                        exprOp("hasIntersection", var(CHAIN), listOp("finance", "zz"))));
                assertEquals(0, runChainCount(
                        exprOp("hasIntersection", var(CHAIN), listOp("zz"))));
            });
        }

        @Test
        void sizeOverTwoHopChainCountsFlattenedElements() {
            // Two categories with one sub-category each: the FLATTENED chain count is 2 — a
            // tail join anchored to the wrong parent could never produce it.
            var s1 = new SubCategoryEntity("chain-sub-s1", "finance");
            var s2 = new SubCategoryEntity("chain-sub-s2", "tech");
            var c1 = new CategoryEntity("chain-cat-s1", "business");
            var c2 = new CategoryEntity("chain-cat-s2", "development");
            c1.setSubCategories(List.of(s1));
            c2.setSubCategories(List.of(s2));
            ResourceEntity r = new ResourceEntity("chain-r-s1");
            r.setCategories(List.of(c1, c2));

            withCategoryGraph(r, List.of(c1, c2), List.of(s1, s2), () -> {
                // Non-empty shortcut (EXISTS through the chain).
                assertEquals(1, runChainCount(
                        exprOp("gt", exprOp("size", var(CHAIN)), nval(0))));
                // Arbitrary-N JOIN-through COUNT: 2 flattened elements.
                assertEquals(1, runChainCount(
                        exprOp("ge", exprOp("size", var(CHAIN)), nval(2))));
                assertEquals(0, runChainCount(
                        exprOp("gt", exprOp("size", var(CHAIN)), nval(2))));
            });
        }

        @Test
        void rootRelationSubqueryInsideLambdaAnchorsToOwningEntity() {
            // W2: R.attr.categories.exists(c, c.name == "business" && R.attr.tags.exists(u, ...))
            // — the inner tags subquery must correlate the ROOT entity (owner of "tags"), not
            // the category join the lambda scope is rooted at.
            var fin = new SubCategoryEntity("chain-sub-w1", "finance");
            var biz = new CategoryEntity("chain-cat-w1", "business");
            biz.setSubCategories(List.of(fin));
            ResourceEntity r = new ResourceEntity("chain-r-w1");
            r.setCategories(List.of(biz));
            r.addTag("chain-tag-w1", "public");

            withCategoryGraph(r, List.of(biz), List.of(fin), () -> {
                Operand matching = exprOp("exists", var("request.resource.attr.categories"),
                        lambda("c", exprOp("and",
                                exprOp("eq", var("c.name"), sval("business")),
                                exprOp("exists", var("request.resource.attr.tags"),
                                        lambda("u", exprOp("eq", var("u.name"), sval("public")))))));
                assertEquals(1, runChainCount(matching));

                Operand nonMatching = exprOp("exists", var("request.resource.attr.categories"),
                        lambda("c", exprOp("and",
                                exprOp("eq", var("c.name"), sval("business")),
                                exprOp("exists", var("request.resource.attr.tags"),
                                        lambda("u", exprOp("eq", var("u.name"), sval("private")))))));
                assertEquals(0, runChainCount(nonMatching));
            });
        }
    }

    // -- Malformed / hostile operand shapes --

    @Test
    void mapLambdaWithWrongArityThrowsCleanly() {
        // A malformed lambda inside map() must produce IllegalArgumentException, not
        // IndexOutOfBoundsException.
        Operand mapExpr = exprOp("map",
                var("request.resource.attr.tags"),
                exprOp("lambda", var("t")));
        assertConditionThrows(
                exprOp("hasIntersection", mapExpr, listOp("x")),
                "map lambda requires exactly 2 operands");
    }

    @Test
    void structValueWithNullEntryDoesNotThrow() {
        // Struct fields may hold nulls; Collectors.toMap would NPE on them.
        com.google.protobuf.Struct struct = com.google.protobuf.Struct.newBuilder()
                .putFields("a", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .putFields("b", Value.newBuilder().setStringValue("x").build())
                .build();
        Object converted = PlanValues.protoValueToJava(
                Value.newBuilder().setStructValue(struct).build());
        assertInstanceOf(Map.class, converted);
        Map<?, ?> map = (Map<?, ?>) converted;
        assertEquals(2, map.size());
        assertNull(map.get("a"));
        assertEquals("x", map.get("b"));
    }

    @Test
    void operatorOverrideIsUsed() {
        Operand cond = exprOp("eq", var("request.resource.attr.aString"), sval("foo"));
        // Override eq to always produce IS NULL — result count stays 0 and the override path
        // is exercised end-to-end (runCount asserts the Conditional kind internally).
        Map<String, OperatorFunction> overrides = Map.of(
                "eq", (cb, field, value) -> cb.isNull(field));
        assertEquals(0, runCount(cond, overrides));
    }

    // -- Field-to-field contains/startsWith/endsWith --
    // The needle is a COLUMN, so LIKE metacharacters it holds must be escaped dynamically
    // (nested REPLACE) before being wrapped in wildcards. CEL semantics: case-sensitive
    // literal substring; a NULL needle is a missing attribute → deny (row excluded).

    @Nested
    class FieldToFieldStringMatch {

        private ResourceEntity row(String id, String aString, String createdBy) {
            ResourceEntity r = new ResourceEntity(id);
            r.setaString(aString);
            r.setCreatedBy(createdBy);
            return r;
        }

        private int count(String op) {
            return runCount(exprOp(op,
                    var("request.resource.attr.aString"),
                    var("request.resource.attr.createdBy")));
        }

        @Test
        void metacharactersInNeedleColumnAreEscaped() {
            // "oneXtwo" does NOT literally contain/start-with/end-with "one_two", but an
            // UNESCAPED pattern ('%one_two%' / 'one_two%' / '%one_two') would match all
            // three ways because '_' matches the 'X'. All three must be no-match.
            withResource(row("f2f-like-1", "oneXtwo", "one_two"), () -> {
                assertEquals(0, count("contains"));
                assertEquals(0, count("startsWith"));
                assertEquals(0, count("endsWith"));
            });
        }

        @Test
        void containsColumnLiteralMatch() {
            withResource(row("f2f-like-2", "a_one_two_b", "one_two"), () ->
                    assertEquals(1, count("contains")));
        }

        @Test
        void startsWithColumn() {
            withResource(row("f2f-like-3", "one_twoTail", "one_two"), () -> {
                assertEquals(1, count("startsWith"));
                assertEquals(1, count("contains"));
                assertEquals(0, count("endsWith"));
            });
        }

        @Test
        void endsWithColumn() {
            withResource(row("f2f-like-4", "Headone_two", "one_two"), () -> {
                assertEquals(1, count("endsWith"));
                assertEquals(0, count("startsWith"));
            });
        }

        @Test
        void percentAndBackslashInNeedleColumn() {
            withResource(row("f2f-like-5", "50%_off", "%_o"), () ->
                    assertEquals(1, count("contains")));
            withResource(row("f2f-like-6", "back\\slash", "k\\s"), () ->
                    assertEquals(1, count("contains")));
            // A literal backslash in the needle must not act as an escape prefix.
            withResource(row("f2f-like-7", "backXslash", "k\\s"), () ->
                    assertEquals(0, count("contains")));
        }

        @Test
        void bracketInNeedleColumn() {
            // The REPLACE chain rewrites '[' to '\[' (SQL Server character-class guard);
            // with ESCAPE '\' declared that must still be a literal '[' on H2 — the literal
            // match keeps working and 'Secret' (the row an unescaped '[SEC]' class would
            // match on SQL Server) never matches.
            withResource(row("f2f-like-10", "x[SEC]y", "[SEC]"), () -> {
                assertEquals(1, count("contains"));
                assertEquals(0, count("startsWith"));
            });
            withResource(row("f2f-like-11", "[SEC]ret", "[SEC]"), () -> {
                assertEquals(1, count("startsWith"));
                assertEquals(1, count("contains"));
            });
            withResource(row("f2f-like-12", "Secret", "[SEC]"), () -> {
                assertEquals(0, count("contains"));
                assertEquals(0, count("startsWith"));
                assertEquals(0, count("endsWith"));
            });
        }

        @Test
        void nullNeedleColumnExcludesRow() {
            // CEL: missing attribute → error → deny. Guarded explicitly because some
            // dialects' CONCAT treats NULL as '' which would turn the pattern into
            // match-anything '%%'.
            withResource(row("f2f-like-8", "anything", null), () -> {
                assertEquals(0, count("contains"));
                assertEquals(0, count("startsWith"));
                assertEquals(0, count("endsWith"));
            });
        }

        @Test
        void emptyNeedleColumnMatchesLikeCel() {
            // CEL: "x".contains("") / startsWith("") / endsWith("") are all true.
            withResource(row("f2f-like-9", "x", ""), () -> {
                assertEquals(1, count("contains"));
                assertEquals(1, count("startsWith"));
                assertEquals(1, count("endsWith"));
            });
        }
    }

    // -- SQL Server '[' LIKE escaping --
    // T-SQL LIKE treats '[...]' as a character class EVEN WITH an ESCAPE clause declared, so
    // every '[' in a generated pattern must arrive as '\['. On H2 (and PostgreSQL/MySQL —
    // covered by the differential oracle legs) '[' is inert and '\[' under ESCAPE '\' is
    // still a literal '[', so the escape is a semantic no-op there: the row-behavior tests
    // below pin that no-op, while the pattern assertions pin the escape itself (they are the
    // tests that FAIL when the '[' rewrite is removed — H2 row behavior cannot distinguish).

    @Nested
    class BracketLikeEscaping {

        @Test
        void escapeLikeEscapesOpeningBracket() {
            // The pattern-level contract for the constant contains/startsWith/endsWith forms
            // AND the hierarchy prefix LIKE (both build their patterns via escapeLike).
            assertEquals("\\[SEC]", PlanValues.escapeLike("[SEC]"));
            assertEquals("50\\%\\[a]\\_b", PlanValues.escapeLike("50%[a]_b"));
            // Backslash is escaped FIRST, so a literal '\[' becomes '\\' + '\['.
            assertEquals("\\\\\\[", PlanValues.escapeLike("\\["));
            // ']' is intentionally unescaped: it is only special on SQL Server as the closer
            // of a character class, and no class can open once every '[' is escaped.
            assertEquals("]", PlanValues.escapeLike("]"));
        }

        private ResourceEntity row(String id, String aString) {
            ResourceEntity r = new ResourceEntity(id);
            r.setaString(aString);
            return r;
        }

        @Test
        void bracketConstantsMatchLiterallyOnH2() {
            // '\[' + ESCAPE stays a literal '[': literal-bracket rows keep matching.
            withResource(row("br-1", "[SEC]ret"), () -> {
                assertEquals(1, runCount(exprOp("startsWith",
                        var("request.resource.attr.aString"), sval("[SEC]"))));
                assertEquals(1, runCount(exprOp("contains",
                        var("request.resource.attr.aString"), sval("[SEC]"))));
            });
            withResource(row("br-2", "a[x]b"), () ->
                    assertEquals(1, runCount(exprOp("contains",
                            var("request.resource.attr.aString"), sval("[x]")))));
            withResource(row("br-3", "tail[end]"), () ->
                    assertEquals(1, runCount(exprOp("endsWith",
                            var("request.resource.attr.aString"), sval("[end]")))));
        }

        @Test
        void bracketConstantsDoNotMatchClassMembers() {
            // The SQL Server over-match scenario: 'Secret' starts with a member of the
            // {S,E,C} class an unescaped '[SEC]' pattern denotes. It must not match on any
            // dialect (H2 here; PostgreSQL/MySQL via the oracle legs; SQL Server by the
            // escaped pattern).
            withResource(row("br-4", "Secret"), () -> {
                assertEquals(0, runCount(exprOp("startsWith",
                        var("request.resource.attr.aString"), sval("[SEC]"))));
                assertEquals(0, runCount(exprOp("contains",
                        var("request.resource.attr.aString"), sval("[SEC]"))));
            });
        }
    }

    // -- Constant-receiver string matches: `"a,b".contains(R.attr.x)` --
    // CEL string-match methods are receiver-sensitive and the planner preserves policy source
    // order, so the constant RECEIVER arrives FIRST: contains(value, variable). The constant is
    // the haystack and the COLUMN is the needle — operand-order normalization must not swap
    // them (that silently inverts the match), and the column needle's LIKE metacharacters must
    // be escaped dynamically.

    @Nested
    class ConstantReceiverStringMatch {

        private ResourceEntity row(String id, String aString) {
            ResourceEntity r = new ResourceEntity(id);
            r.setaString(aString);
            return r;
        }

        private Operand plan(String op, String constant) {
            // Receiver (constant) first — exactly as the planner emits it.
            return exprOp(op, sval(constant), var("request.resource.attr.aString"));
        }

        @Test
        void constantReceiverContains() {
            // "role1,role2".contains(aString): aString="role1" IS contained → match.
            // The inverted translation (aString LIKE '%role1,role2%') would return 0.
            withResource(row("cr-1", "role1"), () -> {
                assertEquals(1, runCount(plan("contains", "role1,role2")));
                // Control: the column-receiver form keeps its meaning.
                assertEquals(0, runCount(exprOp("contains",
                        var("request.resource.attr.aString"), sval("role1,role2"))));
            });
            withResource(row("cr-2", "admin"), () ->
                    assertEquals(0, runCount(plan("contains", "role1,role2"))));
        }

        @Test
        void constantReceiverStartsWith() {
            // "one_two,three".startsWith(aString): aString="one_two" is a prefix → match.
            withResource(row("cr-3", "one_two"), () ->
                    assertEquals(1, runCount(plan("startsWith", "one_two,three"))));
            withResource(row("cr-4", "three"), () ->
                    assertEquals(0, runCount(plan("startsWith", "one_two,three"))));
        }

        @Test
        void constantReceiverEndsWith() {
            withResource(row("cr-5", "one_two"), () ->
                    assertEquals(1, runCount(plan("endsWith", "three,one_two"))));
            withResource(row("cr-6", "three"), () ->
                    assertEquals(0, runCount(plan("endsWith", "three,one_two"))));
        }

        @Test
        void columnNeedleMetacharactersAreEscaped() {
            // Column holds "a_b"; the constant "aXb-list" does NOT literally contain it, but
            // an UNESCAPED needle pattern ('%a_b%') would match the 'X'. Same for the
            // startsWith/endsWith shapes.
            withResource(row("cr-7", "a_b"), () -> {
                assertEquals(0, runCount(plan("contains", "aXb-list")));
                assertEquals(0, runCount(plan("startsWith", "aXb-list")));
                assertEquals(0, runCount(plan("endsWith", "list-aXb")));
            });
            // Literal metacharacter matches only work when the escape is correct.
            withResource(row("cr-8", "a_b"), () -> {
                assertEquals(1, runCount(plan("contains", "xa_by")));
                assertEquals(1, runCount(plan("startsWith", "a_b-tail")));
                assertEquals(1, runCount(plan("endsWith", "head-a_b")));
            });
        }

        @Test
        void nullColumnNeedleExcludesRow() {
            // A NULL column is a missing attribute → CEL error → deny for all three ops.
            withResource(row("cr-9", null), () -> {
                assertEquals(0, runCount(plan("contains", "anything")));
                assertEquals(0, runCount(plan("startsWith", "anything")));
                assertEquals(0, runCount(plan("endsWith", "anything")));
            });
        }

        @Test
        void emptyColumnNeedleMatchesLikeCel() {
            // CEL: "x".contains("") / startsWith("") / endsWith("") are all true.
            withResource(row("cr-10", ""), () -> {
                assertEquals(1, runCount(plan("contains", "x")));
                assertEquals(1, runCount(plan("startsWith", "x")));
                assertEquals(1, runCount(plan("endsWith", "x")));
            });
        }

        @Test
        void addFoldedConstantReceiver() {
            // ("role1," + "role2").contains(aString) — if the planner ever ships the concat
            // unfolded, the receiver arrives as add(value, value) and must fold into the same
            // constant-haystack translation, not the inverted column-haystack one.
            Operand cond = exprOp("contains",
                    exprOp("add", sval("role1,"), sval("role2")),
                    var("request.resource.attr.aString"));
            withResource(row("cr-11", "role1"), () -> assertEquals(1, runCount(cond)));
            withResource(row("cr-12", "admin"), () -> assertEquals(0, runCount(cond)));
        }
    }

    // -- Leaf operand-count guard: extra operands must fail loudly, not drop silently --

    @Test
    void leafWithExtraOperandThrows() {
        // A 3-operand eq previously kept the field-to-field comparison and silently DROPPED
        // the value operand. Malformed plans must throw instead.
        assertConditionThrows(
                exprOp("eq",
                        var("request.resource.attr.aString"),
                        var("request.resource.attr.createdBy"),
                        sval("x")),
                "eq", "2 operands");
    }

    // -- Arithmetic (add/sub/mult/div) as a comparison operand --
    // Cerbos attribute values are ALWAYS CEL doubles (protobuf Value numbers), so the only
    // arithmetic that can evaluate at check time is double-typed — verified against a live
    // PDP: `R.attr.n + 1 > 2` (int literal) is a no-overload error → deny, `+ 1.0` works,
    // and `/ 2.0` is true double division (5/2.0 == 2.5). The adapter therefore computes
    // the whole comparison in double space; integer truncation is never observable.
    // `mod` stays unsupported: CEL `%` is int-only, so it always errors on attributes.

    @Nested
    class ArithmeticComparisons {

        private ResourceEntity seeded() {
            ResourceEntity r = new ResourceEntity("arith-seed-1");
            r.setaNumber(5);
            return r;
        }

        private Operand numVar() {
            return var("request.resource.attr.aNumber");
        }

        @Test
        void addInGtComparison() {
            withResource(seeded(), () -> {
                assertEquals(1, runCount(exprOp("gt",
                        exprOp("add", numVar(), nval(1)), nval(2))));
                assertEquals(0, runCount(exprOp("gt",
                        exprOp("add", numVar(), nval(1)), nval(6))));
            });
        }

        @Test
        void subInLtComparison() {
            withResource(seeded(), () -> {
                assertEquals(1, runCount(exprOp("lt",
                        exprOp("sub", numVar(), nval(1)), nval(10))));
                assertEquals(0, runCount(exprOp("lt",
                        exprOp("sub", numVar(), nval(1)), nval(2))));
                // Constant-minus-field keeps direction: 10 - 5 = 5 <= 5.
                assertEquals(1, runCount(exprOp("le",
                        exprOp("sub", nval(10), numVar()), nval(5))));
            });
        }

        @Test
        void multInComparisonIncludingNegativeConstant() {
            withResource(seeded(), () -> {
                assertEquals(1, runCount(exprOp("gt",
                        exprOp("mult", numVar(), nval(2)), nval(9))));
                assertEquals(0, runCount(exprOp("gt",
                        exprOp("mult", numVar(), nval(2)), nval(10))));
                // Negative multiplier: arithmetic is emitted on the SQL side, so no
                // inequality flipping is needed: 5 * -2 = -10 < 3.
                assertEquals(1, runCount(exprOp("lt",
                        exprOp("mult", numVar(), nval(-2)), nval(3))));
                assertEquals(1, runCount(exprOp("gt",
                        exprOp("mult", numVar(), nval(-2)), nval(-11))));
            });
        }

        @Test
        void divIsDoubleDivision() {
            // CEL semantics on attributes are double: 5 / 2.0 == 2.5, NOT 2 (int
            // truncation is a CEL runtime error on double attrs, verified vs live PDP).
            withResource(seeded(), () -> {
                assertEquals(1, runCount(exprOp("eq",
                        exprOp("div", numVar(), nval(2)), nval(2.5))));
                assertEquals(0, runCount(exprOp("eq",
                        exprOp("div", numVar(), nval(2)), nval(2))));
                assertEquals(1, runCount(exprOp("ge",
                        exprOp("div", numVar(), nval(2)), nval(2.5))));
                assertEquals(0, runCount(exprOp("ge",
                        exprOp("div", numVar(), nval(2)), nval(2.6))));
            });
        }

        @Test
        void valueFirstComparisonIsMirrored() {
            // 2 < aNumber + 1 → NormalizedBinary mirrors to (aNumber + 1) > 2.
            withResource(seeded(), () -> {
                assertEquals(1, runCount(exprOp("lt",
                        nval(2), exprOp("add", numVar(), nval(1)))));
                assertEquals(0, runCount(exprOp("lt",
                        nval(6), exprOp("add", numVar(), nval(1)))));
            });
        }

        @Test
        void arithmeticOnBothSides() {
            // aNumber + 1 <op> aNumber * 2 → 6 vs 10.
            withResource(seeded(), () -> {
                assertEquals(1, runCount(exprOp("lt",
                        exprOp("add", numVar(), nval(1)),
                        exprOp("mult", numVar(), nval(2)))));
                assertEquals(0, runCount(exprOp("gt",
                        exprOp("add", numVar(), nval(1)),
                        exprOp("mult", numVar(), nval(2)))));
            });
        }

        @Test
        void nestedArithmetic() {
            // (aNumber + 1) * 2 > 11 → 12 > 11.
            withResource(seeded(), () -> {
                assertEquals(1, runCount(exprOp("gt",
                        exprOp("mult", exprOp("add", numVar(), nval(1)), nval(2)),
                        nval(11))));
                assertEquals(0, runCount(exprOp("gt",
                        exprOp("mult", exprOp("add", numVar(), nval(1)), nval(2)),
                        nval(12))));
            });
        }

        @Test
        void divByZeroColumnDivisorDoesNotAbortQuery() {
            // gt(div(aNumber, aNumber), 0.5): a zero-valued row makes the divisor 0. SQL
            // division by zero would abort the WHOLE query; CEL 0.0/0.0 is NaN, whose
            // comparisons are all false → deny. The divisor is guarded with NULLIF so the
            // zero-divisor row becomes UNKNOWN → excluded, and the query survives.
            Operand cond = exprOp("gt",
                    exprOp("div", numVar(), numVar()),
                    nval(0.5));
            ResourceEntity nonZero = new ResourceEntity("div-zero-1");
            nonZero.setaNumber(5);
            ResourceEntity zero = new ResourceEntity("div-zero-2");
            zero.setaNumber(0);
            withResource(nonZero, () -> withResource(zero, () ->
                    // 5/5 = 1.0 > 0.5 → only the non-zero-divisor row matches.
                    assertEquals(1, runCount(cond))));
        }

        @Test
        void fractionalMultiplicationComparesInIeeeDoubleSpace() {
            // IEEE doubles: 3 * 0.1 = 0.30000000000000004 != 0.3, so CEL (and the PDP)
            // exclude the row. Decimal-exact DB arithmetic would wrongly include it.
            ResourceEntity r = new ResourceEntity("frac-mult-1");
            r.setaNumber(3);
            withResource(r, () -> assertEquals(0, runCount(exprOp("eq",
                    exprOp("mult", numVar(), nval(0.1)),
                    nval(0.3)))));
        }

        @Test
        void overrideAppliesToArithmeticComparison() {
            // OperatorFunction contract: overrides win on EVERY scalar path. The arithmetic
            // expression is passed as the field argument; the plan constant as the value.
            Operand cond = exprOp("gt",
                    exprOp("add", numVar(), nval(1.0)),
                    nval(2.0));
            assertThrows(OverrideInvoked.class,
                    () -> runCount(cond, Map.of("gt", THROWING_OVERRIDE)));
        }

        @Test
        void modStillThrows() {
            // CEL `%` has no double overload and attribute values are always doubles, so a
            // mod comparison can never be satisfied at check time — translating it to SQL
            // MOD would fabricate rows the PDP denies. It must keep throwing.
            assertConditionThrows(
                    exprOp("eq",
                            exprOp("mod", numVar(), nval(2)), nval(1)),
                    "mod");
        }

        @Test
        void nonNumericOperandThrows() {
            // lt over string concatenation has no numeric translation.
            assertConditionThrows(
                    exprOp("lt",
                            exprOp("add", var("request.resource.attr.aString"), sval("x")),
                            sval("z")),
                    "numeric");
        }
    }

    // -- Constant NaN / ±Infinity ordering --
    // CEL/IEEE define EVERY ordering comparison involving NaN as false. The planner does
    // NOT fold div(0,0) (verified vs live PDP: `(R.attr.aBool ? 1.0 : 0.0/0.0) > 0.5`
    // arrives as gt(if(aBool, 1, div(0,0)), 0.5)), so resolveNumericOperand folds it to
    // NaN in Java and constantComparison must order with primitive IEEE operators.
    // Double.compare's total order ranks NaN above every number (and -0.0 below 0.0),
    // which would collapse gt/ge against a NaN constant to always-true — over-inclusion.

    @Nested
    class ConstantNanInfinityOrdering {

        /** {@code div(0, 0)} — folds to NaN in Java, exactly as delivered on the wire. */
        private Operand nan() {
            return exprOp("div", nval(0), nval(0));
        }

        private Operand posInf() {
            return exprOp("div", nval(1), nval(0));
        }

        private Operand negInf() {
            return exprOp("div", nval(-1), nval(0));
        }

        /** An arithmetic subtree folding to 0.5, so both sides rank as expressions. */
        private Operand half() {
            return exprOp("div", nval(1), nval(2));
        }

        @Test
        void nanOnLeftExcludesForAllOrderingOperators() {
            // Expression-vs-value keeps source order: constantComparison sees (NaN, 0.5).
            ResourceEntity r = new ResourceEntity("nan-ord-1");
            withResource(r, () -> {
                for (String op : List.of("gt", "ge", "lt", "le")) {
                    assertEquals(0, runCount(exprOp(op, nan(), nval(0.5))),
                            op + "(NaN, 0.5) must exclude every row");
                }
            });
        }

        @Test
        void nanOnRightExcludesForAllOrderingOperators() {
            // Arithmetic on BOTH sides so normalization cannot mirror the NaN to the left:
            // constantComparison sees (0.5, NaN).
            ResourceEntity r = new ResourceEntity("nan-ord-2");
            withResource(r, () -> {
                for (String op : List.of("gt", "ge", "lt", "le")) {
                    assertEquals(0, runCount(exprOp(op, half(), nan())),
                            op + "(0.5, NaN) must exclude every row");
                }
            });
        }

        @Test
        void infinityOrderingFollowsIeee() {
            // ±Infinity is ORDERED normally in IEEE space — it must NOT be excluded the
            // way NaN is.
            ResourceEntity r = new ResourceEntity("nan-ord-3");
            withResource(r, () -> {
                assertEquals(1, runCount(exprOp("gt", posInf(), nval(0.5))));
                assertEquals(1, runCount(exprOp("ge", posInf(), nval(0.5))));
                assertEquals(0, runCount(exprOp("lt", posInf(), nval(0.5))));
                assertEquals(0, runCount(exprOp("le", posInf(), nval(0.5))));
                assertEquals(1, runCount(exprOp("lt", negInf(), nval(0.5))));
                assertEquals(1, runCount(exprOp("le", negInf(), nval(0.5))));
                assertEquals(0, runCount(exprOp("gt", negInf(), nval(0.5))));
                assertEquals(1, runCount(exprOp("lt", negInf(), posInf())));
            });
        }

        @Test
        void negativeZeroOrderingFollowsIeee() {
            // mult(-1, 0) folds to -0.0 in Java. IEEE: -0.0 == 0.0, so lt is false and
            // ge is true — Double.compare(-0.0, 0.0) = -1 would invert both (the same
            // total-order defect as NaN, on the same line).
            Operand negZero = exprOp("mult", nval(-1), nval(0));
            Operand zero = exprOp("mult", nval(1), nval(0));
            ResourceEntity r = new ResourceEntity("nan-ord-4");
            withResource(r, () -> {
                assertEquals(0, runCount(exprOp("lt", negZero, zero)));
                assertEquals(1, runCount(exprOp("le", negZero, zero)));
                assertEquals(1, runCount(exprOp("ge", negZero, zero)));
                assertEquals(0, runCount(exprOp("gt", negZero, zero)));
            });
        }

        @Test
        void ternaryNanElseArmExcludesRows() {
            // The live-PDP reproduction: `(R.attr.aBool ? 1.0 : 0.0/0.0) > 0.5` arrives
            // as gt(if(aBool, 1, div(0,0)), 0.5). check() denies every aBool=false
            // resource (NaN > 0.5 is false), so the rewritten else arm must contribute
            // exclusion — old code produced (NOT aBool AND 1=1), returning the row.
            Operand plan = exprOp("gt",
                    exprOp("if", var("request.resource.attr.aBool"), nval(1), nan()),
                    nval(0.5));

            ResourceEntity allowed = new ResourceEntity("nan-tern-1");
            allowed.setaBool(true);
            withResource(allowed, () -> assertEquals(1, runCount(plan)));

            ResourceEntity denied = new ResourceEntity("nan-tern-2");
            denied.setaBool(false);
            withResource(denied, () -> assertEquals(0, runCount(plan)));
        }
    }

    /**
     * {@code repository.delete(Specification)} guard. Relation-mapped operators translate to
     * correlated subqueries over collection tables; Hibernate's multi-table bulk delete first
     * clears {@code @ElementCollection}/join tables with the same predicate, which
     * self-invalidates the correlated subquery — empirically (Hibernate 6.6.18/H2, plan
     * {@code in(value "user1", variable ownedBy)}): SELECT returns the row, {@code delete()}
     * returns 0, the entity survives, and ALL its {@code resource_owned_by} collection rows are
     * destroyed. The adapter now detects the bulk-delete invocation context (the {@code Root}
     * comes from a {@code CriteriaDelete} and is not a member of the throwaway
     * {@code CriteriaQuery}'s root set) and throws {@link UnsupportedOperationException} from
     * {@code toPredicate} — i.e. BEFORE any statement executes.
     */
    @Nested
    class BulkDeleteGuard {

        /**
         * Exact wire shape the PDP emits for policy {@code P.id in request.resource.attr.ownedBy}
         * (operand order is source order, so the constant-folded principal id comes first).
         */
        private final Operand ownedByUser1 =
                exprOp("in", sval("user1"), var("request.resource.attr.ownedBy"));

        private Specification<ResourceEntity> spec(Operand condition) {
            PlanResourcesResponse resp =
                    buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);
            Result<ResourceEntity> result =
                    SpringDataQueryPlanAdapter.toSpecification(resp, MAPPER, Map.of());
            return ((Result.Conditional<ResourceEntity>) result).specification();
        }

        @Test
        void deleteWithRelationSpecThrowsBeforeAnyDeletion() {
            ResourceEntity r = new ResourceEntity("bulk-del-1");
            r.setOwnedBy(new ArrayList<>(List.of("user1", "user2")));
            withResource(r, () -> {
                Specification<ResourceEntity> spec = spec(ownedByUser1);
                EntityManager em = emf.createEntityManager();
                try {
                    SimpleJpaRepository<ResourceEntity, String> repository =
                            new SimpleJpaRepository<>(ResourceEntity.class, em);

                    // SELECT paths through the real Spring Data repository are unaffected.
                    assertEquals(1, repository.findAll(spec).size());
                    assertEquals(1, repository.count(spec));

                    em.getTransaction().begin();
                    try {
                        UnsupportedOperationException ex = assertThrows(
                                UnsupportedOperationException.class,
                                () -> repository.delete(spec));
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

                // The guard fired inside toPredicate, before Hibernate built or ran any DELETE:
                // the entity row AND every one of its collection rows survive. (Without the
                // guard: entity survives but resource_owned_by is emptied — data corruption.)
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
            // Field-only predicates involve no correlated subquery and no collection-table
            // pre-clear hazard — the guard must not block them.
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
                    long deleted = repository.delete(spec);
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

        @Test
        void criteriaDeleteInvocationContextIsDetected() {
            // Pins the detection mechanism itself, independent of the Spring Data version:
            // SimpleJpaRepository.delete(Specification) calls
            // spec.toPredicate(delete.from(cls), builder.createQuery(cls), builder) — a Root
            // created on a CriteriaDelete plus a throwaway CriteriaQuery whose root set does
            // not contain it. Every Spring Data SELECT path creates the Root via
            // query.from(...), so membership in query.getRoots() distinguishes the two.
            Specification<ResourceEntity> spec = spec(ownedByUser1);
            EntityManager em = emf.createEntityManager();
            try {
                CriteriaBuilder cb = em.getCriteriaBuilder();
                CriteriaDelete<ResourceEntity> delete = cb.createCriteriaDelete(ResourceEntity.class);
                Root<ResourceEntity> deleteRoot = delete.from(ResourceEntity.class);
                assertThrows(UnsupportedOperationException.class,
                        () -> spec.toPredicate(deleteRoot, cb.createQuery(ResourceEntity.class), cb));
            } finally {
                em.close();
            }
        }

        @Test
        void hasIntersectionAndSizeAreGuardedToo() {
            // All correlated-subquery construction funnels through the same choke point
            // (chainSubquery) — spot-check two more Relation-mapped operator families.
            Operand hasIntersection = exprOp("hasIntersection",
                    var("request.resource.attr.ownedBy"), listOp("user1", "user2"));
            Operand sizeGt = exprOp("gt",
                    exprOp("size", var("request.resource.attr.ownedBy")), nval(1));
            EntityManager em = emf.createEntityManager();
            try {
                CriteriaBuilder cb = em.getCriteriaBuilder();
                for (Operand cond : List.of(hasIntersection, sizeGt)) {
                    Specification<ResourceEntity> spec = spec(cond);
                    CriteriaDelete<ResourceEntity> delete = cb.createCriteriaDelete(ResourceEntity.class);
                    Root<ResourceEntity> deleteRoot = delete.from(ResourceEntity.class);
                    assertThrows(UnsupportedOperationException.class,
                            () -> spec.toPredicate(deleteRoot, cb.createQuery(ResourceEntity.class), cb));
                }
            } finally {
                em.close();
            }
        }
    }

    // -- timestamp(field) vs timestamp(constant) comparisons --
    // Wire shape (PDP-verified): `timestamp(R.attr.createdAt) < now() - duration("24h")`
    // arrives as lt(timestamp(variable), timestamp(value "<RFC-3339>")) — the planner folds
    // now()-duration to a constant instant and RE-WRAPS it in timestamp(); a bare string
    // constant never appears. Value-first policies keep source order (both operands are
    // EXPRESSION nodes, so NormalizedBinary cannot reorder them) and must be MIRRORED.

    @Nested
    class TimestampComparisons {

        private static final String CONST = "2025-01-01T00:00:00Z";

        private Operand tsVar(String attr) {
            return exprOp("timestamp", var("request.resource.attr." + attr));
        }

        private Operand tsVal(String iso) {
            return exprOp("timestamp", sval(iso));
        }

        /** Seed rows: two before {@link #CONST}, one exactly at it, one after, one NULL. */
        private void withTimestampRows(Runnable body) {
            ResourceEntity old1 = new ResourceEntity("ts-old1");
            old1.setCreatedAt(java.time.Instant.parse("2024-03-01T00:00:00Z"));
            old1.setUpdatedAt(java.time.OffsetDateTime.parse("2024-03-01T00:00:00Z"));
            old1.setaBool(true);
            ResourceEntity old2 = new ResourceEntity("ts-old2");
            old2.setCreatedAt(java.time.Instant.parse("2024-06-01T00:00:00.123456Z"));
            old2.setUpdatedAt(java.time.OffsetDateTime.parse("2024-06-01T00:00:00.123456Z"));
            old2.setaBool(false);
            ResourceEntity exact = new ResourceEntity("ts-exact");
            exact.setCreatedAt(java.time.Instant.parse(CONST));
            exact.setUpdatedAt(java.time.OffsetDateTime.parse(CONST));
            exact.setaBool(false);
            ResourceEntity newer = new ResourceEntity("ts-new");
            newer.setCreatedAt(java.time.Instant.parse("2026-02-01T00:00:00Z"));
            newer.setUpdatedAt(java.time.OffsetDateTime.parse("2026-02-01T00:00:00Z"));
            newer.setaBool(false);
            ResourceEntity nul = new ResourceEntity("ts-null"); // createdAt/updatedAt NULL
            nul.setaBool(false);
            withResource(old1, () -> withResource(old2, () -> withResource(exact,
                    () -> withResource(newer, () -> withResource(nul, body)))));
        }

        @Test
        void allSixOperatorsFieldFirstOnInstantColumn() {
            withTimestampRows(() -> {
                // Row set: old1, old2 < CONST; exact == CONST; new > CONST; null excluded
                // everywhere (SQL three-valued logic == CEL missing-attribute deny).
                assertEquals(2, runCount(exprOp("lt", tsVar("createdAt"), tsVal(CONST))));
                assertEquals(3, runCount(exprOp("le", tsVar("createdAt"), tsVal(CONST))));
                assertEquals(1, runCount(exprOp("gt", tsVar("createdAt"), tsVal(CONST))));
                assertEquals(2, runCount(exprOp("ge", tsVar("createdAt"), tsVal(CONST))));
                assertEquals(1, runCount(exprOp("eq", tsVar("createdAt"), tsVal(CONST))));
                assertEquals(3, runCount(exprOp("ne", tsVar("createdAt"), tsVal(CONST))));
            });
        }

        @Test
        void allSixOperatorsValueFirstAreMirroredNotInverted() {
            withTimestampRows(() -> {
                // `CONST < field` selects rows AFTER the instant (1 row) — an inversion bug
                // (treating it as `field < CONST`) would return the 2 older rows instead.
                assertEquals(1, runCount(exprOp("lt", tsVal(CONST), tsVar("createdAt"))));
                assertEquals(2, runCount(exprOp("le", tsVal(CONST), tsVar("createdAt"))));
                assertEquals(2, runCount(exprOp("gt", tsVal(CONST), tsVar("createdAt"))));
                assertEquals(3, runCount(exprOp("ge", tsVal(CONST), tsVar("createdAt"))));
                assertEquals(1, runCount(exprOp("eq", tsVal(CONST), tsVar("createdAt"))));
                assertEquals(3, runCount(exprOp("ne", tsVal(CONST), tsVar("createdAt"))));
            });
        }

        @Test
        void offsetDateTimeColumnSupportsAllSixOperators() {
            withTimestampRows(() -> {
                assertEquals(2, runCount(exprOp("lt", tsVar("updatedAt"), tsVal(CONST))));
                assertEquals(3, runCount(exprOp("le", tsVar("updatedAt"), tsVal(CONST))));
                assertEquals(1, runCount(exprOp("gt", tsVar("updatedAt"), tsVal(CONST))));
                assertEquals(2, runCount(exprOp("ge", tsVar("updatedAt"), tsVal(CONST))));
                assertEquals(1, runCount(exprOp("eq", tsVar("updatedAt"), tsVal(CONST))));
                assertEquals(3, runCount(exprOp("ne", tsVar("updatedAt"), tsVal(CONST))));
                // Value-first mirror on the OffsetDateTime column too.
                assertEquals(1, runCount(exprOp("lt", tsVal(CONST), tsVar("updatedAt"))));
            });
        }

        @Test
        void nonUtcOffsetConstantNormalizesToTheSameInstant() {
            // 2025-01-01T02:00:00+02:00 IS 2025-01-01T00:00:00Z: eq must match the `exact`
            // row (CEL timestamp equality is instant equality — PDP-verified), and lt must
            // select the same two older rows as the Z-offset constant.
            withTimestampRows(() -> {
                assertEquals(1, runCount(exprOp("eq",
                        tsVar("createdAt"), tsVal("2025-01-01T02:00:00+02:00"))));
                assertEquals(2, runCount(exprOp("lt",
                        tsVar("createdAt"), tsVal("2025-01-01T02:00:00+02:00"))));
            });
        }

        @Test
        void subSecondPrecisionConstantDiscriminates() {
            // The folded now()-duration constant carries nanosecond precision on the wire.
            // A threshold BETWEEN old2 (…00.123456Z) and its whole second must split them.
            withTimestampRows(() -> {
                assertEquals(1, runCount(exprOp("le",
                        tsVar("createdAt"), tsVal("2024-06-01T00:00:00.000001Z"))));
                assertEquals(2, runCount(exprOp("le",
                        tsVar("createdAt"), tsVal("2024-06-01T00:00:00.123456Z"))));
            });
        }

        @Test
        void nullColumnIsExcludedByEveryOperator() {
            // Only the NULL row seeded: every comparison is UNKNOWN → zero rows, matching
            // check() denying on the missing attribute (PDP-verified).
            ResourceEntity nul = new ResourceEntity("ts-only-null");
            withResource(nul, () -> {
                for (String op : List.of("lt", "le", "gt", "ge", "eq", "ne")) {
                    assertEquals(0, runCount(exprOp(op, tsVar("createdAt"), tsVal(CONST))),
                            "NULL column must be excluded for " + op);
                    assertEquals(0, runCount(exprOp(op, tsVal(CONST), tsVar("createdAt"))),
                            "NULL column must be excluded for value-first " + op);
                }
            });
        }

        @Test
        void constantVsConstantFoldsViaTernarySubstitution() {
            // (aBool ? timestamp(A) : timestamp(B)) == timestamp(A) — substitution yields
            // timestamp-constant vs timestamp-constant comparisons; the fold must select
            // exactly the aBool=true row (old1).
            withTimestampRows(() -> assertEquals(1, runCount(exprOp("eq",
                    exprOp("if",
                            var("request.resource.attr.aBool"),
                            tsVal("2024-01-01T00:00:00Z"),
                            tsVal("2030-01-01T00:00:00Z")),
                    tsVal("2024-01-01T00:00:00Z")))));
        }

        @Test
        void localDateTimeColumnThrowsNamedError() {
            // LocalDateTime has no zone: the stored wall-clock could denote any instant, and
            // guessing UTC could silently include rows check() denies. Fail closed, by name.
            assertConditionThrows(
                    exprOp("lt", tsVar("localCreatedAt"), tsVal(CONST)),
                    "timestamp() comparison", "LocalDateTime", "localCreatedAt");
        }

        @Test
        void stringColumnThrowsNamedError() {
            assertConditionThrows(
                    exprOp("lt", tsVar("createdBy"), tsVal(CONST)),
                    "timestamp() comparison", "String", "createdBy");
        }

        @Test
        void overrideIsConsultedBeforeColumnTypeCheck() {
            // The README's OperatorFunction escape hatch must be REACHABLE for timestamp
            // comparisons — including on column types the default translation rejects.
            assertThrows(OverrideInvoked.class, () -> runCount(
                    exprOp("lt", tsVar("localCreatedAt"), tsVal(CONST)),
                    Map.of("lt", THROWING_OVERRIDE)));
        }

        @Test
        void valueFirstOverrideIsConsultedUnderTheMirroredOperator() {
            // Same contract as NormalizedBinary: a value-first lt is looked up as gt.
            assertThrows(OverrideInvoked.class, () -> runCount(
                    exprOp("lt", tsVal(CONST), tsVar("createdAt")),
                    Map.of("gt", THROWING_OVERRIDE)));
        }

        @Test
        void bareStringConstantStillThrows() {
            // The PDP never emits timestamp(variable) vs a bare string (verified against a
            // live PDP: even folded now()-duration constants are re-wrapped in timestamp()).
            // Unverifiable shape → keep failing closed.
            assertConditionThrows(
                    exprOp("lt", tsVar("createdAt"), sval(CONST)),
                    "Unexpected timestamp() expression in leaf operand of lt");
        }

        @Test
        void numberConstantAgainstTimestampFieldThrows() {
            assertConditionThrows(
                    exprOp("gt", tsVar("createdAt"), nval(5)),
                    "Unexpected timestamp() expression in leaf operand of gt");
        }

        @Test
        void timestampOverNestedExpressionThrows() {
            // timestamp(<expression>) has no verified wire shape → Opaque → named error.
            assertConditionThrows(
                    exprOp("lt",
                            exprOp("timestamp", exprOp("add", sval("a"), sval("b"))),
                            tsVal(CONST)),
                    "Unexpected timestamp() expression in leaf operand of lt");
        }

        @Test
        void timestampInsideArithmeticStillThrows() {
            // Nested shapes the numeric machinery routes through resolveNumericOperand keep
            // their named error — no partial support for shapes the oracle cannot verify.
            assertConditionThrows(
                    exprOp("lt",
                            exprOp("add", tsVar("createdAt"), nval(1)),
                            nval(5)),
                    "timestamp() expression inside an arithmetic");
        }

        @Test
        void malformedConstantThrowsNamedError() {
            assertConditionThrows(
                    exprOp("lt", tsVar("createdAt"), tsVal("not-a-timestamp")),
                    "timestamp() constant could not be parsed");
        }

        @Test
        void nonStringConstantInsideTimestampThrows() {
            assertConditionThrows(
                    exprOp("lt", tsVar("createdAt"),
                            exprOp("timestamp", nval(1735689600))),
                    "timestamp() constant must be an RFC-3339 string");
        }
    }
}
