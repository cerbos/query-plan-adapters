package dev.cerbos.queryplan.springdata;

import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Value;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
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

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests that exercise the adapter without a live Cerbos PDP. They build protobuf operands
 * directly and verify the produced Specification by executing it against an empty H2 schema —
 * Hibernate translates to SQL, which catches mapping/type errors. We assert via empty result
 * lists (the schema is empty), so the tests really check that no exception is thrown and the
 * query compiles correctly.
 */
class SpringDataQueryPlanAdapterTest {

    private static final Map<String, AttributeMapping> MAPPER = Map.ofEntries(
            Map.entry("request.resource.attr.aBool", AttributeMapping.field("aBool")),
            Map.entry("request.resource.attr.aString", AttributeMapping.field("aString")),
            Map.entry("request.resource.attr.aNumber", AttributeMapping.field("aNumber")),
            Map.entry("request.resource.attr.aOptionalString", AttributeMapping.field("aOptionalString")),
            Map.entry("request.resource.attr.createdBy", AttributeMapping.field("createdBy")),
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
        PlanResourcesResponse resp =
                buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);
        Result<ResourceEntity> result =
                SpringDataQueryPlanAdapter.toSpecification(resp, MAPPER, overrides);
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
            r.setOwnedBy(new java.util.ArrayList<>(java.util.List.of("user1", "user2")));
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
        // eq(10, add(3, R.attr.aNumber))  →  aNumber == 7
        Operand cond = exprOp("eq",
                nval(10),
                exprOp("add", nval(3), var("request.resource.attr.aNumber")));
        assertEquals(0, runCount(cond));
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

        @Test
        void arithAddInComparisonThrows() {
            // gt(add(field, 1.0), 2.0) — handleAddComparison rejects non-eq/ne ops.
            assertConditionThrows(
                    exprOp("gt",
                            exprOp("add", var("request.resource.attr.aNumber"), nval(1)),
                            nval(2)),
                    "add", "gt");
        }

        @Test
        void arithSubThrows() {
            assertConditionThrows(
                    exprOp("lt",
                            exprOp("sub", var("request.resource.attr.aNumber"), nval(1)),
                            nval(2)),
                    "sub");
        }

        @Test
        void arithMultThrows() {
            assertConditionThrows(
                    exprOp("gt",
                            exprOp("mult", var("request.resource.attr.aNumber"), nval(2)),
                            nval(2)),
                    "mult");
        }

        @Test
        void arithDivThrows() {
            assertConditionThrows(
                    exprOp("gt",
                            exprOp("div", var("request.resource.attr.aNumber"), nval(2)),
                            nval(0)),
                    "div");
        }

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
        void fieldToFieldNonComparisonOperatorStillThrows() {
            // contains(var, var) has no portable JPA translation — the specific message stays.
            assertConditionThrows(
                    exprOp("contains",
                            var("request.resource.attr.aString"),
                            var("request.resource.attr.createdBy")),
                    "Field-to-field", "contains");
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

        @Test
        void ancestorOfConstantPrefixOfField() {
            // ancestorOf(hierarchy("a:b", ":"), hierarchy(field, ":")) → field LIKE 'a:b:%'
            Operand cond = exprOp("ancestorOf",
                    hierarchy(sval("a:b"), ":"),
                    hierarchy(var("request.resource.attr.aString"), ":"));
            assertEquals(0, runCount(cond));
        }

        @Test
        void ancestorOfFieldPrefixOfConstant() {
            // ancestorOf(hierarchy(field, ":"), hierarchy("a:b:c", ":")) → field IN ('a', 'a:b')
            Operand cond = exprOp("ancestorOf",
                    hierarchy(var("request.resource.attr.aString"), ":"),
                    hierarchy(sval("a:b:c"), ":"));
            assertEquals(0, runCount(cond));
        }

        @Test
        void descendentOfFieldUnderConstant() {
            // descendentOf(hierarchy(field, ":"), hierarchy("a:b", ":")) → field LIKE 'a:b:%'
            Operand cond = exprOp("descendentOf",
                    hierarchy(var("request.resource.attr.aString"), ":"),
                    hierarchy(sval("a:b"), ":"));
            assertEquals(0, runCount(cond));
        }

        @Test
        void overlapsFieldHierarchyWithConstant() {
            // overlaps(hierarchy(field, ":"), hierarchy("a:b", ":"))
            //   → field IN ('a') OR field = 'a:b' OR field LIKE 'a:b:%'
            Operand cond = exprOp("overlaps",
                    hierarchy(var("request.resource.attr.aString"), ":"),
                    hierarchy(sval("a:b"), ":"));
            assertEquals(0, runCount(cond));
        }

        @Test
        void overlapsSegmentedWithField() {
            // The policy shape: hierarchy("projects:123", ":").overlaps(hierarchy(["projects", R.id]))
            //   → segment-wise: const "projects" matches, then field == "123"
            Operand cond = exprOp("overlaps",
                    hierarchy(sval("projects:123"), ":"),
                    hierarchy(segList(sval("projects"), var("request.resource.attr.aString"))));
            assertEquals(0, runCount(cond));
        }

        @Test
        void overlapsConstantsMatchingPrefixIsAlwaysTrue() {
            // overlaps("a", "a:b") — "a" is a prefix of "a:b", all constant → unconditionally true.
            Operand cond = exprOp("overlaps",
                    hierarchy(sval("a"), ":"),
                    hierarchy(sval("a:b"), ":"));
            assertEquals(0, runCount(cond));
        }

        @Test
        void ancestorOfConstantsSatisfied() {
            // ancestorOf("a", "a:b") — satisfied by constants alone → unconditionally true.
            Operand cond = exprOp("ancestorOf",
                    hierarchy(sval("a"), ":"),
                    hierarchy(sval("a:b"), ":"));
            assertEquals(0, runCount(cond));
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
            r.setOwnedBy(new java.util.ArrayList<>(java.util.List.of("user1")));
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

    // -- CEL ternary (PR: ternary support): `if(cond, then, else)` is rewritten into pure
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
                // Ordering incomparable constant types is a planner bug and must throw.
                assertConditionThrows(exprOp("lt", sval("a"), nval(1)),
                        "Cannot order", "lt");
            });

            ResourceEntity boolFalse = new ResourceEntity("ternary-const-2");
            boolFalse.setaBool(false);
            withResource(boolFalse, () -> assertEquals(0, runCount(allConstBranches)));
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
}
