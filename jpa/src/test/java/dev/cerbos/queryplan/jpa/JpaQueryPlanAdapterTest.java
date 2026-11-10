/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.jpa;

import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.queryplan.jpa.testmodel.CategoryEntity;
import dev.cerbos.queryplan.jpa.testmodel.ResourceEntity;
import dev.cerbos.queryplan.jpa.testmodel.SubCategoryEntity;

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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests that hand-build plans and run the resulting filter against in-memory H2. No
 * Docker. Each test sits under one of the three banners from CODING_STANDARDS.md, "What a
 * translator unit test may pin": a branch CEL cannot reach, a caller-supplied argument the corpus
 * cannot vary, or a corpus gap (tracked by #668, deleted when the corpus action lands).
 */
class JpaQueryPlanAdapterTest {

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
            ))),
            // Scalar projection of the tags relation.
            Map.entry("request.resource.attr.tagNames", AttributeMapping.relation("tags", "name"))
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

    /** Like {@link #listOp} but {@code null} entries become protobuf NULL_VALUE elements. */
    private static Operand listOpNullable(String... values) {
        ListValue.Builder list = ListValue.newBuilder();
        for (String v : values) {
            if (v == null) {
                list.addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE));
            } else {
                list.addValues(Value.newBuilder().setStringValue(v));
            }
        }
        return Operand.newBuilder().setValue(Value.newBuilder().setListValue(list)).build();
    }

    private static Operand lambda(String varName, Operand body) {
        return exprOp("lambda", body, var(varName));
    }

    /** Asserts that translating {@code condition} throws with every fragment in the message. */
    private static void assertConditionThrows(Operand condition, String... messageFragments) {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> runCount(condition));
        for (String fragment : messageFragments) {
            assertTrue(ex.getMessage().contains(fragment),
                    "expected message to contain '" + fragment + "' but was: " + ex.getMessage());
        }
    }

    /** Translates {@code condition}, runs it, and returns the row count. */
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
        // EXPLICIT, the convention these vary from; the default is pinned in
        // NullAttributeRepresentationTest.
        QueryPlanFilter filter = JpaQueryPlanAdapter.toFilter(resp, JpaQueryPlanAdapter.Options.of(mapper).withOperatorOverrides(overrides)
                    .withNullAttributeRepresentation(NullAttributeRepresentation.EXPLICIT));

        EntityManager em = emf.createEntityManager();
        try {
            CriteriaBuilder cb = em.getCriteriaBuilder();
            CriteriaQuery<Long> cq = cb.createQuery(Long.class);
            Root<ResourceEntity> root = cq.from(ResourceEntity.class);
            cq.select(cb.count(root));
            Predicate p = filter.toPredicate(root, cq, cb);
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

    /** Thrown by {@link #THROWING_OVERRIDE}, so a test can assert the override was called. */
    private static final class OverrideInvoked extends RuntimeException {
        OverrideInvoked() {
            super("override invoked");
        }
    }

    private static final OperatorFunction THROWING_OVERRIDE = (cb, field, value) -> {
        throw new OverrideInvoked();
    };

    // Distinctive values, so a leak into an error message is easy to detect.
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

    // Scope paths that tell a correct hierarchy translation from the likely bugs: "a:bb:c"
    // shares the string prefix "a:b" but not the path prefix. In check(), ancestorOf and
    // descendentOf are strict (a path is not its own ancestor); overlaps is inclusive.
    private static final List<String> SCOPES =
            List.of("a", "a:b", "a:b:c", "a:b:c:d", "a:bb:c", "x:y");

    /** Seeds one row per path, using the path as id and {@code aString}; cleans up after. */
    private static void withScopeRows(List<String> paths, Runnable body) {
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

    /** Translates {@code condition}, runs it, and returns the matched ids (the scope paths). */
    private static Set<String> scopeIds(Operand condition) {
        PlanResourcesResponse resp =
                buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);
        QueryPlanFilter filter =
                JpaQueryPlanAdapter.toFilter(resp, MAPPER);
        EntityManager em = emf.createEntityManager();
        try {
            CriteriaBuilder cb = em.getCriteriaBuilder();
            CriteriaQuery<String> cq = cb.createQuery(String.class);
            Root<ResourceEntity> root = cq.from(ResourceEntity.class);
            cq.select(root.get("id"));
            Predicate p = filter.toPredicate(root, cq, cb);
            if (p != null) {
                cq.where(p);
            }
            return Set.copyOf(em.createQuery(cq).getResultList());
        } finally {
            em.close();
        }
    }

    private static ResourceEntity orderSeed() {
        ResourceEntity r = new ResourceEntity("seed-1");
        r.setaBool(true);
        r.setaString("seededString");
        r.setaNumber(5);
        r.setOwnedBy(new ArrayList<>(List.of("user1")));
        r.addTag("tagX", "x");
        return r;
    }

    private static final String CHAIN = "request.resource.attr.categories.subCategories";

    private static final Map<String, AttributeMapping> CHAIN_MAPPER = Map.ofEntries(
            Map.entry("request.resource.attr.aString", AttributeMapping.field("aString")),
            Map.entry("request.resource.attr.tags", AttributeMapping.relation("tags", Map.of(
                    "id", AttributeMapping.field("id"),
                    "name", AttributeMapping.field("name")))),
            Map.entry("request.resource.attr.categories", AttributeMapping.relation("categories", Map.of(
                    "name", AttributeMapping.field("name"),
                    "subCategories", AttributeMapping.relation("subCategories", "name", Map.of(
                            "name", AttributeMapping.field("name")))))));

    private static int runChainCount(Operand condition) {
        return runCount(condition, CHAIN_MAPPER, Map.of());
    }

    /** Persists a resource and its category graph (not cascaded), runs {@code body}, cleans up. */
    private static void withCategoryGraph(ResourceEntity resource,
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

    private static ResourceEntity row(String id, String aString) {
        ResourceEntity r = new ResourceEntity(id);
        r.setaString(aString);
        return r;
    }

    private static final String TS_CONST = "2025-01-01T00:00:00Z";

    private static Operand tsVar(String attr) {
        return exprOp("timestamp", var("request.resource.attr." + attr));
    }

    private static Operand tsVal(String iso) {
        return exprOp("timestamp", sval(iso));
    }

    /** Seed rows: two before {@link #TS_CONST}, one exactly at it, one after, one NULL. */
    private static void withTimestampRows(Runnable body) {
        ResourceEntity old1 = new ResourceEntity("ts-old1");
        old1.setCreatedAt(java.time.Instant.parse("2024-03-01T00:00:00Z"));
        old1.setUpdatedAt(java.time.OffsetDateTime.parse("2024-03-01T00:00:00Z"));
        old1.setaBool(true);
        ResourceEntity old2 = new ResourceEntity("ts-old2");
        old2.setCreatedAt(java.time.Instant.parse("2024-06-01T00:00:00.123456Z"));
        old2.setUpdatedAt(java.time.OffsetDateTime.parse("2024-06-01T00:00:00.123456Z"));
        old2.setaBool(false);
        ResourceEntity exact = new ResourceEntity("ts-exact");
        exact.setCreatedAt(java.time.Instant.parse(TS_CONST));
        exact.setUpdatedAt(java.time.OffsetDateTime.parse(TS_CONST));
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

    // ============================================================================================
    // KIND 1 — a branch CEL itself cannot reach
    //
    // No policy compiles to these, or the planner never emits them. Permanent.
    // ============================================================================================

    // CEL rejects an undeclared function, so no plan carries one.
    @Test
    void unknownOperatorThrows() {
        Operand cond = exprOp("unsupported_op",
                var("request.resource.attr.aString"), sval("v"));
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> runCount(cond));
        assertTrue(ex.getMessage().contains("Unsupported operator"));
    }

    // isSet is not a CEL function, so a policy using it does not compile. Every operand shape
    // must fail as an unsupported operator. Existence arrives as eq/ne against null.
    @Test
    void isSetReportsUnsupportedOperator() {
        assertConditionThrows(
                exprOp("isSet", var("request.resource.attr.aOptionalString"), bval(true)),
                "Unsupported operator", "isSet");
        assertConditionThrows(
                exprOp("isSet",
                        var("request.resource.attr.aString"),
                        var("request.resource.attr.createdBy")),
                "isSet");
        assertConditionThrows(
                exprOp("isSet", bval(true), bval(false)),
                "isSet");
    }

    /**
     * Hierarchy shapes no plan carries. The planner folds a comparison of two constant
     * hierarchies to ALWAYS_ALLOWED or ALWAYS_DENIED, and CEL's checker rejects {@code overlaps}
     * on a plain string.
     */
    @Nested
    class HierarchyShapesNoPlanCarries {

        private Operand hierarchy(Operand inner, String delimiter) {
            return exprOp("hierarchy", inner, sval(delimiter));
        }

        private Operand hierarchy(Operand inner) {
            return exprOp("hierarchy", inner);
        }

        @Test
        void overlapsConstantsMatchingPrefixIsAlwaysTrue() {
            // "a" is a prefix of "a:b", so every row comes back.
            Operand cond = exprOp("overlaps",
                    hierarchy(sval("a"), ":"),
                    hierarchy(sval("a:b"), ":"));
            withScopeRows(SCOPES, () ->
                    assertEquals(Set.copyOf(SCOPES), scopeIds(cond)));
        }

        @Test
        void ancestorOfConstantsSatisfied() {
            // Satisfied by the constants alone, so every row comes back.
            Operand cond = exprOp("ancestorOf",
                    hierarchy(sval("a"), ":"),
                    hierarchy(sval("a:b"), ":"));
            withScopeRows(SCOPES, () ->
                    assertEquals(Set.copyOf(SCOPES), scopeIds(cond)));

            // A trailing delimiter is an empty segment: "a:b:" is ["a", "b", ""], so "a:b" is
            // still a strict ancestor.
            Operand trailing = exprOp("ancestorOf",
                    hierarchy(sval("a:b"), ":"),
                    hierarchy(sval("a:b:"), ":"));
            withScopeRows(SCOPES, () ->
                    assertEquals(Set.copyOf(SCOPES), scopeIds(trailing)));
        }

        @Test
        void overlapsIncompatibleConstantsWithoutFieldThrows() {
            // No prefix relation and no column to constrain.
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

    }

    /**
     * {@code timestamp()} shapes no plan carries. The planner folds constants to an instant and
     * wraps it in {@code timestamp()}, so a bare string, number or concatenation never arrives;
     * {@code timestamp(x) > 5} and {@code timestamp(x) + 1} fail CEL's type checker. The
     * reachable cases are in {@link TimestampComparisons}.
     */
    @Nested
    class TimestampShapesNoPlanCarries {

        @Test
        void bareStringConstantStillThrows() {
            assertConditionThrows(
                    exprOp("lt", tsVar("createdAt"), sval(TS_CONST)),
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
            assertConditionThrows(
                    exprOp("lt",
                            exprOp("timestamp", exprOp("add", sval("a"), sval("b"))),
                            tsVal(TS_CONST)),
                    "Unexpected timestamp() expression in leaf operand of lt");
        }

        @Test
        void timestampInsideArithmeticStillThrows() {
            assertConditionThrows(
                    exprOp("lt",
                            exprOp("add", tsVar("createdAt"), nval(1)),
                            nval(5)),
                    "timestamp() expression inside an arithmetic");
        }

        @Test
        void nonStringConstantInsideTimestampThrows() {
            assertConditionThrows(
                    exprOp("lt", tsVar("createdAt"),
                            exprOp("timestamp", nval(1735689600))),
                    "timestamp() constant must be an RFC-3339 string");
        }

    }

    // Constant-only sub-expressions: the planner folds these before sending the plan (see the
    // string/starts-with/concatenated-literal-needle golden).

    @Test
    void addFoldedTwoConstants() {
        Operand cond = exprOp("eq",
                var("request.resource.attr.aString"),
                exprOp("add", sval("hello"), sval("-world")));
        withResource(row("fold-1", "hello-world"), () -> assertEquals(1, runCount(cond)));
        withResource(row("fold-2", "hello"), () -> assertEquals(0, runCount(cond)));
    }

    @Test
    void addFoldedConstantValueFirstIsMirrored() {
        // (1 + 2) < aNumber, with aNumber = 5.
        withResource(orderSeed(), () ->
                assertEquals(1, runCount(exprOp("lt",
                        exprOp("add", nval(1), nval(2)),
                        var("request.resource.attr.aNumber")))));
    }

    @Test
    void addFoldedConstantReceiver() {
        // ("role1," + "role2").contains(aString): the folded constant stays the haystack.
        Operand cond = exprOp("contains",
                exprOp("add", sval("role1,"), sval("role2")),
                var("request.resource.attr.aString"));
        withResource(row("cr-11", "role1"), () -> assertEquals(1, runCount(cond)));
        withResource(row("cr-12", "admin"), () -> assertEquals(0, runCount(cond)));
    }

    @Test
    void bothOperandsAddExpressionsReportsShapeNotArity() {
        assertConditionThrows(
                exprOp("contains",
                        exprOp("add", sval("a"), sval("b")),
                        exprOp("add", sval("c"), sval("d"))),
                "contains", "two add() expressions");
    }

    // Malformed operand shapes the planner never emits. Each must fail with a named
    // IllegalArgumentException, not a raw runtime error or a silently dropped operand.

    @Test
    void mapLambdaWithWrongArityThrowsCleanly() {
        Operand mapExpr = exprOp("map",
                var("request.resource.attr.tags"),
                exprOp("lambda", var("t")));
        assertConditionThrows(
                exprOp("hasIntersection", mapExpr, listOp("x")),
                "map lambda requires exactly 2 operands");
    }

    @Test
    void structValueWithNullEntryDoesNotThrow() {
        // Collectors.toMap throws on null values.
        Struct struct = Struct.newBuilder()
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
    void ternaryWithWrongOperandCountThrows() {
        assertConditionThrows(
                exprOp("gt",
                        exprOp("if", var("request.resource.attr.aBool"), nval(1)),
                        nval(0)),
                "if (ternary) requires exactly 3 operands", "got 2");
        // Same for a ternary in boolean position.
        assertConditionThrows(
                exprOp("if", var("request.resource.attr.aBool"), bval(true)),
                "if (ternary) requires exactly 3 operands", "got 2");
    }

    @Test
    void leafWithExtraOperandThrows() {
        assertConditionThrows(
                exprOp("eq",
                        var("request.resource.attr.aString"),
                        var("request.resource.attr.createdBy"),
                        sval("x")),
                "eq", "2 operands");
    }

    @Test
    void sizeComparisonWithExtraOperandThrows() {
        // The arity check must run before the size() path, or the extra operand is dropped.
        assertConditionThrows(
                exprOp("eq",
                        exprOp("size", var("request.resource.attr.tags")),
                        var("request.resource.attr.aString"),
                        nval(2)),
                "eq", "2 operands");
    }

    @Test
    void sizeArityErrorReportsOperandCount() {
        assertConditionThrows(
                exprOp("gt",
                        exprOp("size",
                                var("request.resource.attr.tags"),
                                var("request.resource.attr.tagNames")),
                        nval(0)),
                "size() takes exactly 1 argument, got 2");
    }

    // ============================================================================================
    // KIND 2 — a caller-supplied argument the corpus structurally cannot vary
    //
    // The harness translates every case with one mapping, so overrides, null
    // conventions, other column types and the call contract have no corpus spelling.
    // ============================================================================================

    /** Both unconditional kinds return a predicate, never null, so a caller can always apply it. */
    @Test
    void unconditionalPlansSelectEveryRowOrNone() {
        ResourceEntity r = new ResourceEntity("unconditional-1");
        withResource(r, () -> {
            QueryPlanFilter allowed = JpaQueryPlanAdapter.toFilter(
                    buildResponse(PlanResourcesFilter.Kind.KIND_ALWAYS_ALLOWED, null), MAPPER);
            QueryPlanFilter denied = JpaQueryPlanAdapter.toFilter(
                    buildResponse(PlanResourcesFilter.Kind.KIND_ALWAYS_DENIED, null), MAPPER);
            assertTrue(allowed.isAlwaysAllowed());
            assertFalse(denied.isAlwaysAllowed());
            EntityManager em = emf.createEntityManager();
            try {
                CriteriaBuilder cb = em.getCriteriaBuilder();
                CriteriaQuery<Long> all = cb.createQuery(Long.class);
                all.select(cb.count(all.from(ResourceEntity.class)));
                long total = em.createQuery(all).getSingleResult();
                assertTrue(total > 0);
                for (var expected : Map.of(allowed, total, denied, 0L).entrySet()) {
                    CriteriaQuery<Long> cq = cb.createQuery(Long.class);
                    Root<ResourceEntity> root = cq.from(ResourceEntity.class);
                    cq.select(cb.count(root)).where(expected.getKey().toPredicate(root, cq, cb));
                    assertEquals(expected.getValue(), em.createQuery(cq).getSingleResult());
                }
            } finally {
                em.close();
            }
        });
    }

    // An OperatorFunction override must be used on every scalar comparison path.

    @Test
    void overrideAppliesToDirectComparison() {
        Operand cond = exprOp("eq", var("request.resource.attr.aString"), sval("foo"));
        assertThrows(OverrideInvoked.class,
                () -> runCount(cond, Map.of("eq", THROWING_OVERRIDE)));
    }

    @Test
    void overrideAppliesToAddFoldedComparison() {
        Operand cond = exprOp("eq",
                var("request.resource.attr.aString"),
                exprOp("add", sval("prefix:"), sval("123")));
        assertThrows(OverrideInvoked.class,
                () -> runCount(cond, Map.of("eq", THROWING_OVERRIDE)));
    }

    @Test
    void overrideAppliesToNullRhs() {
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
    void overrideAppliesToNeNull() {
        Operand cond = exprOp("ne", var("request.resource.attr.aOptionalString"), nullVal());
        assertThrows(OverrideInvoked.class,
                () -> runCount(cond, Map.of("ne", THROWING_OVERRIDE)));
    }

    @Test
    void operatorOverrideIsUsed() {
        // The query must run the override's predicate: the default excludes this row, the
        // override's IS NOT NULL includes it.
        Operand cond = exprOp("eq", var("request.resource.attr.aString"), sval("foo"));
        Map<String, OperatorFunction> overrides = Map.of(
                "eq", (cb, field, value) -> cb.isNotNull(field));
        withResource(row("override-used-1", "bar"), () -> {
            assertEquals(0, runCount(cond));
            assertEquals(1, runCount(cond, overrides));
        });
    }

    @Test
    void overrideStillOwnsInWithNullElement() {
        // The override receives the list as sent, nulls included.
        Operand cond = exprOp("in",
                var("request.resource.attr.aOptionalString"), listOpNullable("a", null));
        assertThrows(OverrideInvoked.class,
                () -> runCount(cond, Map.of("in", THROWING_OVERRIDE)));
    }

    @Test
    void overrideIsConsultedUnderMirroredOperator() {
        // 3 < aNumber becomes aNumber > 3, so the override is looked up as "gt".
        Operand cond = exprOp("lt", nval(3), var("request.resource.attr.aNumber"));
        assertThrows(OverrideInvoked.class,
                () -> runCount(cond, Map.of("gt", THROWING_OVERRIDE)));
    }

    @Test
    void overrideAppliesToArithmeticComparison() {
        // The arithmetic expression is passed as the field argument.
        Operand cond = exprOp("gt",
                exprOp("add", var("request.resource.attr.aNumber"), nval(1.0)),
                nval(2.0));
        assertThrows(OverrideInvoked.class,
                () -> runCount(cond, Map.of("gt", THROWING_OVERRIDE)));
    }

    @Test
    void unknownCollectionAttributeThrows() {
        assertConditionThrows(
                exprOp("in",
                        var("request.resource.attr.aString"),
                        var("request.resource.attr.nonexistent")),
                "Unknown attribute");
    }

    /**
     * Both null conventions send the same {@code eq(attr, null)}, so the caller must say which one
     * it uses. Under {@code OMITTED}, {@code check()} denies a NULL column, so {@code IS NULL}
     * would return denied rows.
     */
    @Nested
    class NullAttributeRepresentationTest {

        private QueryPlanFilter translate(
                Operand condition, NullAttributeRepresentation representation) {
            PlanResourcesResponse resp =
                    buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);
            return JpaQueryPlanAdapter.toFilter(resp, JpaQueryPlanAdapter.Options.of(MAPPER)
                    .withNullAttributeRepresentation(representation));
        }

        private Operand nullEq() {
            return exprOp("eq", var("request.resource.attr.aOptionalString"), nullVal());
        }

        @Test
        void omittedIsTheDefaultAndExplicitKeepsIsNull() {
            // The three-argument overload refuses the null operand: OMITTED is the default,
            // because its wrong guess is a refusal rather than an over-grant (ADR 0011).
            PlanResourcesResponse resp =
                    buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, nullEq());
            assertThrows(UnsupportedPlanShapeException.class,
                    () -> JpaQueryPlanAdapter.toFilter(resp, MAPPER));
            ResourceEntity nullColumn = new ResourceEntity("explicit-default-1");
            nullColumn.setaOptionalString(null);
            withResource(nullColumn, () -> assertEquals(1, runCount(nullEq())));
            assertDoesNotThrow(() -> translate(nullEq(), NullAttributeRepresentation.EXPLICIT));
        }

        @Test
        void omittedLeavesNullFreeComparisonsUntouched() {
            Operand condition =
                    exprOp("eq", var("request.resource.attr.aString"), sval("x"));
            assertDoesNotThrow(() -> translate(condition, NullAttributeRepresentation.OMITTED));
        }
    }

    /**
     * The null convention declared per attribute mapping, which overrides the call-level default.
     * This lets one call mix both conventions.
     */
    @Nested
    class PerAttributeNullRepresentationTest {

        private final Map<String, AttributeMapping> mapper = Map.of(
                "request.resource.attr.owner",
                AttributeMapping.field("aOptionalString", NullAttributeRepresentation.EXPLICIT),
                "request.resource.attr.coOwner",
                AttributeMapping.field("aString", NullAttributeRepresentation.EXPLICIT),
                "request.resource.attr.plain",
                AttributeMapping.field("aOptionalString"));

        private int count(Operand condition) {
            return runCount(condition, mapper, Map.of());
        }

        private ResourceEntity nullOwner() {
            ResourceEntity e = new ResourceEntity();
            e.setId("null-owner");
            e.setaOptionalString(null);
            e.setaString(null);
            return e;
        }

        @Test
        void explicitNullDeclarationDoesNotEnableHeterogeneousCoercion() {
            ResourceEntity row = new ResourceEntity("typed-explicit-owner");
            row.setaOptionalString("0");
            withResource(row, () -> {
                assertEquals(0, count(exprOp("eq", var("request.resource.attr.owner"), nval(0))));
                assertEquals(1, count(exprOp("ne", var("request.resource.attr.owner"), nval(0))));
            });
            withResource(nullOwner(), () -> {
                assertEquals(0, count(exprOp("eq", var("request.resource.attr.owner"), nval(0))));
                assertEquals(1, count(exprOp("ne", var("request.resource.attr.owner"), nval(0))));
            });
        }

        // An EXPLICIT declaration only changes eq/ne. Ordering a null is a CEL error that
        // denies under both polarities, which SQL UNKNOWN already gives.
        @Test
        void orderingComparisonsStayUnknown() {
            withResource(nullOwner(), () -> assertEquals(0, count(
                    exprOp("gt", var("request.resource.attr.owner"), sval("x")))));
        }

        /**
         * An OMITTED declaration wins over the call-level EXPLICIT default: a null comparison is
         * never true for eq nor false for ne, and a NULL column stays UNKNOWN under negation.
         */
        @Test
        void declaringOmittedMakesANullOperandThreeValuedUnderTheExplicitDefault() {
            Map<String, AttributeMapping> omitted = Map.of("request.resource.attr.omitted",
                    AttributeMapping.field("aOptionalString", NullAttributeRepresentation.OMITTED));
            Operand eqNull = exprOp("eq", var("request.resource.attr.omitted"), nullVal());
            Operand neNull = exprOp("ne", var("request.resource.attr.omitted"), nullVal());
            withResource(nullOwner(), () -> {
                assertEquals(0, runCount(eqNull, omitted, Map.of()));
                assertEquals(0, runCount(exprOp("not", eqNull), omitted, Map.of()));
                assertEquals(0, runCount(neNull, omitted, Map.of()));
                assertEquals(0, runCount(exprOp("not", neNull), omitted, Map.of()));
            });
            ResourceEntity present = new ResourceEntity("present-omitted");
            present.setaOptionalString("x");
            withResource(present, () -> {
                assertEquals(0, runCount(eqNull, omitted, Map.of()));
                assertEquals(1, runCount(exprOp("not", eqNull), omitted, Map.of()));
                assertEquals(1, runCount(neNull, omitted, Map.of()));
                assertEquals(0, runCount(exprOp("not", neNull), omitted, Map.of()));
            });
        }

        /** An override would receive the null and could select NULL rows, so it is refused. */
        @Test
        void declaringOmittedRejectsANullOperandUnderAnOverride() {
            PlanResourcesResponse resp = buildResponse(
                    PlanResourcesFilter.Kind.KIND_CONDITIONAL,
                    exprOp("eq", var("request.resource.attr.omitted"), nullVal()));
            IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                    () -> JpaQueryPlanAdapter.toFilter(resp, JpaQueryPlanAdapter.Options.of(
                            Map.of("request.resource.attr.omitted", AttributeMapping.field(
                                    "aOptionalString", NullAttributeRepresentation.OMITTED)))
                            .withOperatorOverrides(Map.of("eq", THROWING_OVERRIDE))
                            .withNullAttributeRepresentation(NullAttributeRepresentation.EXPLICIT)));
            assertTrue(thrown.getMessage().contains("missing-attribute error"));
        }

        /** #308: an EXPLICIT declaration wins over a call-level OMITTED; an undeclared one does not. */
        @Test
        void declaringExplicitTranslatesANullOperandUnderACallLevelOmitted() {
            PlanResourcesResponse declared = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL,
                    exprOp("eq", var("request.resource.attr.owner"), nullVal()));
            assertDoesNotThrow(() -> JpaQueryPlanAdapter.toFilter(declared, JpaQueryPlanAdapter.Options.of(mapper)
                    .withNullAttributeRepresentation(NullAttributeRepresentation.OMITTED)));

            PlanResourcesResponse undeclared = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL,
                    exprOp("eq", var("request.resource.attr.plain"), nullVal()));
            IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                    () -> JpaQueryPlanAdapter.toFilter(undeclared, JpaQueryPlanAdapter.Options.of(mapper)
                    .withNullAttributeRepresentation(NullAttributeRepresentation.OMITTED)));
            assertTrue(thrown.getMessage().contains("null operand"), thrown.getMessage());
        }

    }

    // Each nested collection macro adds a correlated subquery, so nesting beyond the limit must
    // throw at translation time rather than emit a filter that may not finish.
    @Nested
    class MacroDepthGuard {

        private static final String DEPTH_PROPERTY = "dev.cerbos.queryplan.jpa.maxMacroDepth";

        // The subCategories/labels many-to-many pair allows join chains of any depth.
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

        /** {@code categories.exists(v1, v1.subCategories.exists(v2, ... vd.name == "x"))}. */
        private Operand existsChain(int depth) {
            return existsLevel(1, depth, "request.resource.attr.categories");
        }

        private Operand existsLevel(int level, int depth, String collection) {
            String v = "v" + level;
            Operand body = level == depth
                    ? exprOp("eq", var(v + ".name"), sval("x"))
                    : existsLevel(level + 1, depth, v + "." + HOPS[level - 1]);
            return exprOp("exists", var(collection), lambda(v, body));
        }

        private int runDeep(Operand cond) {
            return runCount(cond, DEEP_MAPPER, Map.of());
        }

        @Test
        void literalFoldCountsAgainstTheCallLevelDepthBound() {
            PlanResourcesResponse plan = Corpus.plan("principal/exists/shadowed-lambda-variable");
            EntityManager em = emf.createEntityManager();
            try {
                CriteriaBuilder cb = em.getCriteriaBuilder();
                CriteriaQuery<ResourceEntity> query = cb.createQuery(ResourceEntity.class);
                Root<ResourceEntity> root = query.from(ResourceEntity.class);
                JpaQueryPlanAdapter.Options options =
                        JpaQueryPlanAdapter.Options.of(MAPPER);
                QueryPlanFilter shallow = JpaQueryPlanAdapter.toFilter(
                        plan, options.withMaxMacroDepth(1));
                UnsupportedPlanShapeException error = assertThrows(UnsupportedPlanShapeException.class,
                        () -> shallow.toPredicate(root, query, cb));
                assertTrue(error.getMessage().contains("nesting depth 2 exceeds the maximum of 1"));
                QueryPlanFilter admitted = JpaQueryPlanAdapter.toFilter(
                        plan, options.withMaxMacroDepth(2));
                assertNotNull(admitted.toPredicate(root, query, cb));
            } finally {
                em.close();
            }
        }

        @Test
        void depthAtDefaultLimitTranslates() {
            assertEquals(0, runDeep(existsChain(5)));
        }

        @Test
        void depthBeyondDefaultLimitThrowsNamedError() {
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> runDeep(existsChain(6)));
            assertTrue(ex.getMessage().contains("nesting depth 6 exceeds the maximum of 5")
                            && ex.getMessage().contains("exists")
                            && ex.getMessage().contains(DEPTH_PROPERTY),
                    "unexpected message: " + ex.getMessage());
        }

        @Test
        void propertyRaisesAndLowersTheLimit() {
            System.setProperty(DEPTH_PROPERTY, "2");
            try {
                assertEquals(0, runDeep(existsChain(2)));
                IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                        () -> runDeep(existsChain(3)));
                assertTrue(ex.getMessage().contains("nesting depth 3 exceeds the maximum of 2"),
                        "unexpected message: " + ex.getMessage());
                System.setProperty(DEPTH_PROPERTY, "6");
                assertEquals(0, runDeep(existsChain(6)));
            } finally {
                System.clearProperty(DEPTH_PROPERTY);
            }
        }

        @Test
        void sizeFilterCountsAsAMacroLevel() {
            // filter plus the nested exists is depth 2, over a limit of 1.
            Operand filtered = exprOp("filter",
                    var("request.resource.attr.categories"),
                    lambda("v1", existsLevel(2, 2, "v1.subCategories")));
            Operand cond = exprOp("gt", exprOp("size", filtered), nval(0));
            System.setProperty(DEPTH_PROPERTY, "1");
            try {
                IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                        () -> runDeep(cond));
                assertTrue(ex.getMessage().contains("nesting depth 2 exceeds the maximum of 1"),
                        "unexpected message: " + ex.getMessage());
            } finally {
                System.clearProperty(DEPTH_PROPERTY);
            }
        }

        @Test
        void invalidPropertyValuesThrowNamedErrors() {
            System.setProperty(DEPTH_PROPERTY, "not-a-number");
            try {
                IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                        () -> runDeep(existsChain(1)));
                assertTrue(ex.getMessage().contains(DEPTH_PROPERTY)
                                && ex.getMessage().contains("positive integer"),
                        "unexpected message: " + ex.getMessage());
                System.setProperty(DEPTH_PROPERTY, "0");
                IllegalArgumentException zero = assertThrows(IllegalArgumentException.class,
                        () -> runDeep(existsChain(1)));
                assertTrue(zero.getMessage().contains("positive integer"),
                        "unexpected message: " + zero.getMessage());
            } finally {
                System.clearProperty(DEPTH_PROPERTY);
            }
        }
    }

    /**
     * Timestamp column types the corpus does not map (it uses {@link java.time.Instant}), and
     * overrides on timestamp comparisons.
     */
    @Nested
    class TimestampColumnMappings {

        @Test
        void offsetDateTimeColumnSupportsAllSixOperators() {
            withTimestampRows(() -> {
                assertEquals(2, runCount(exprOp("lt", tsVar("updatedAt"), tsVal(TS_CONST))));
                assertEquals(3, runCount(exprOp("le", tsVar("updatedAt"), tsVal(TS_CONST))));
                assertEquals(1, runCount(exprOp("gt", tsVar("updatedAt"), tsVal(TS_CONST))));
                assertEquals(2, runCount(exprOp("ge", tsVar("updatedAt"), tsVal(TS_CONST))));
                assertEquals(1, runCount(exprOp("eq", tsVar("updatedAt"), tsVal(TS_CONST))));
                assertEquals(3, runCount(exprOp("ne", tsVar("updatedAt"), tsVal(TS_CONST))));
                // Value-first is mirrored.
                assertEquals(1, runCount(exprOp("lt", tsVal(TS_CONST), tsVar("updatedAt"))));
            });
        }

        @Test
        void localDateTimeColumnThrowsNamedError() {
            // LocalDateTime has no zone, so it cannot be compared to an instant. Guessing UTC
            // could return rows check() denies.
            assertConditionThrows(
                    exprOp("lt", tsVar("localCreatedAt"), tsVal(TS_CONST)),
                    "timestamp() comparison", "LocalDateTime", "localCreatedAt");
        }

        @Test
        void overrideIsConsultedBeforeColumnTypeCheck() {
            // An override still works on a column type the default translation rejects.
            assertThrows(OverrideInvoked.class, () -> runCount(
                    exprOp("lt", tsVar("localCreatedAt"), tsVal(TS_CONST)),
                    Map.of("lt", THROWING_OVERRIDE)));
        }

        @Test
        void valueFirstOverrideIsConsultedUnderTheMirroredOperator() {
            // A value-first lt is looked up as gt.
            assertThrows(OverrideInvoked.class, () -> runCount(
                    exprOp("lt", tsVal(TS_CONST), tsVar("createdAt")),
                    Map.of("gt", THROWING_OVERRIDE)));
        }

    }

    /**
     * {@code AttributeMapping} and {@code toFilter} copy the caller's maps, so changing a
     * map afterwards cannot change which columns the filter uses.
     */
    @Nested
    class DefensiveCopies {

        @Test
        void nullConstructorArgumentsThrowNamedNpe() {
            NullPointerException f = assertThrows(NullPointerException.class,
                    () -> AttributeMapping.field(null));
            assertTrue(f.getMessage().contains("jpaPath"), "message: " + f.getMessage());

            NullPointerException j = assertThrows(NullPointerException.class,
                    () -> new AttributeMapping.Relation(null, null, Map.of()));
            assertTrue(j.getMessage().contains("joinAttribute"), "message: " + j.getMessage());

            NullPointerException fields = assertThrows(NullPointerException.class,
                    () -> new AttributeMapping.Relation("tags", null, null));
            assertTrue(fields.getMessage().contains("fields"), "message: " + fields.getMessage());
        }

        @Test
        void relationFieldsMapIsCopiedAndImmutable() {
            java.util.HashMap<String, AttributeMapping> fields = new java.util.HashMap<>();
            fields.put("name", AttributeMapping.field("name"));
            AttributeMapping.Relation rel = AttributeMapping.relation("tags", fields);

            fields.put("name", AttributeMapping.field("id"));
            assertEquals(AttributeMapping.field("name"), rel.fields().get("name"),
                    "mutating the caller's fields map must not affect the Relation");
            assertThrows(UnsupportedOperationException.class,
                    () -> rel.fields().put("x", AttributeMapping.field("x")));
        }

        @Test
        void mutatingCallerMapperAfterToFilterDoesNotAffectFilter() {
            // Different values, so reading the wrong column changes the count.
            ResourceEntity r = new ResourceEntity("copy-1");
            r.setaString("match");
            r.setaOptionalString("other");
            withResource(r, () -> {
                java.util.HashMap<String, AttributeMapping> mapper = new java.util.HashMap<>();
                mapper.put("request.resource.attr.aString", AttributeMapping.field("aString"));
                Operand cond = exprOp("eq", var("request.resource.attr.aString"), sval("match"));
                PlanResourcesResponse resp =
                        buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, cond);
                QueryPlanFilter filter =
                        JpaQueryPlanAdapter.toFilter(resp, mapper);

                assertEquals(1, countWithFilter(filter));
                // Remap after construction; the filter must keep the original column.
                mapper.put("request.resource.attr.aString", AttributeMapping.field("aOptionalString"));
                assertEquals(1, countWithFilter(filter),
                        "post-construction mapper mutation changed the captured filter");
            });
        }

        private int countWithFilter(QueryPlanFilter filter) {
            EntityManager em = emf.createEntityManager();
            try {
                CriteriaBuilder cb = em.getCriteriaBuilder();
                CriteriaQuery<Long> cq = cb.createQuery(Long.class);
                Root<ResourceEntity> root = cq.from(ResourceEntity.class);
                cq.select(cb.count(root));
                Predicate p = filter.toPredicate(root, cq, cb);
                if (p != null) cq.where(p);
                return em.createQuery(cq).getSingleResult().intValue();
            } finally {
                em.close();
            }
        }
    }

    /**
     * Bulk-delete guard. Hibernate's bulk delete first clears the collection tables using the
     * same predicate, which breaks a correlated subquery over them: the entity survives and its
     * collection rows are deleted. So a relation-mapped filter throws
     * {@link UnsupportedOperationException} from {@code toPredicate} when called for a delete,
     * before any statement runs. spring-data's suite drives the same guard through
     * {@code repository.delete(Specification)}.
     */
    @Nested
    class BulkDeleteGuard {

        // The plan for `P.id in R.attr.ownedBy`: the folded principal id comes first.
        private final Operand ownedByUser1 =
                exprOp("in", sval("user1"), var("request.resource.attr.ownedBy"));

        private QueryPlanFilter filter(Operand condition) {
            PlanResourcesResponse resp =
                    buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);
            return JpaQueryPlanAdapter.toFilter(resp, MAPPER);
        }

        @Test
        void criteriaDeleteInvocationContextIsDetected() {
            // A delete passes a Root from a CriteriaDelete with an unrelated or null
            // CriteriaQuery; SELECT paths pass a Root from query.from(...). The guard tells them
            // apart by whether the Root is in query.getRoots().
            QueryPlanFilter filter = filter(ownedByUser1);
            EntityManager em = emf.createEntityManager();
            try {
                CriteriaBuilder cb = em.getCriteriaBuilder();
                CriteriaDelete<ResourceEntity> delete = cb.createCriteriaDelete(ResourceEntity.class);
                Root<ResourceEntity> deleteRoot = delete.from(ResourceEntity.class);
                UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
                        () -> filter.toPredicate(deleteRoot, cb.createQuery(ResourceEntity.class), cb));
                assertTrue(ex.getMessage().contains("ownedBy"),
                        "message should name the relation, was: " + ex.getMessage());
                assertTrue(ex.getMessage().contains("SELECT-only"),
                        "message should state the SELECT-only contract, was: " + ex.getMessage());
                assertThrows(UnsupportedOperationException.class,
                        () -> filter.toPredicate(deleteRoot, null, cb));
            } finally {
                em.close();
            }
        }

        @Test
        void criteriaDeleteWithAFieldOnlyFilterStillDeletes() {
            // A field-only predicate has no subquery, so the guard must allow it.
            ResourceEntity r = new ResourceEntity("bulk-del-2");
            r.setCreatedBy("alice");
            withResource(r, () -> {
                QueryPlanFilter filter =
                        filter(exprOp("eq", var("request.resource.attr.createdBy"), sval("alice")));
                EntityManager em = emf.createEntityManager();
                try {
                    CriteriaBuilder cb = em.getCriteriaBuilder();
                    CriteriaDelete<ResourceEntity> delete =
                            cb.createCriteriaDelete(ResourceEntity.class);
                    Root<ResourceEntity> deleteRoot = delete.from(ResourceEntity.class);
                    delete.where(filter.toPredicate(deleteRoot, null, cb));
                    em.getTransaction().begin();
                    int deleted = em.createQuery(delete).executeUpdate();
                    em.getTransaction().commit();
                    assertEquals(1, deleted);
                } finally {
                    em.close();
                }
                EntityManager check = emf.createEntityManager();
                try {
                    assertNull(check.find(ResourceEntity.class, "bulk-del-2"),
                            "a field-only criteria delete must still work");
                } finally {
                    check.close();
                }
            });
        }

        @Test
        void hasIntersectionAndSizeAreGuardedToo() {
            // Spot-check two more relation operators; all subqueries go through chainSubquery.
            Operand hasIntersection = exprOp("hasIntersection",
                    var("request.resource.attr.ownedBy"), listOp("user1", "user2"));
            Operand sizeGt = exprOp("gt",
                    exprOp("size", var("request.resource.attr.ownedBy")), nval(1));
            EntityManager em = emf.createEntityManager();
            try {
                CriteriaBuilder cb = em.getCriteriaBuilder();
                for (Operand cond : List.of(hasIntersection, sizeGt)) {
                    QueryPlanFilter filter = filter(cond);
                    CriteriaDelete<ResourceEntity> delete = cb.createCriteriaDelete(ResourceEntity.class);
                    Root<ResourceEntity> deleteRoot = delete.from(ResourceEntity.class);
                    assertThrows(UnsupportedOperationException.class,
                            () -> filter.toPredicate(deleteRoot, cb.createQuery(ResourceEntity.class), cb));
                }
            } finally {
                em.close();
            }
        }
    }

    // ============================================================================================
    // KIND 3 — a policy can reach these, and the corpus does not carry them yet
    //
    // Each test is a corpus gap tracked by #668. Delete it when its corpus action lands.
    // ============================================================================================

    /**
     * <strong>Corpus gap.</strong> #668: {@code collection/exists-one/several-matches} carries a single-equality body; a
     * disjunctive body is not carried.
     */
    @Test
    void existsOneWithCompoundBody() {
        assertEquals(0, runCount(exprOp("exists_one",
                var("request.resource.attr.tags"),
                lambda("t",
                        exprOp("or",
                                exprOp("eq", var("t.id"), sval("tag1")),
                                exprOp("eq", var("t.name"), sval("public")))))));
    }

    // hasIntersection with an empty list is always false and must not emit `IN ()`, which some
    // databases reject.

    /**
     * <strong>Corpus gap.</strong> #668: {@code hasIntersection(x, [])} against a scalar column is
     * not carried. It is not known whether the planner folds it as it folds {@code membership/in/empty-list}.
     */
    @Test
    void hasIntersectionScalarEmptyListCompiles() {
        assertEquals(0, runCount(exprOp("hasIntersection",
                var("request.resource.attr.aString"), listOp())));
    }

    /**
     * <strong>Corpus gap.</strong> #668: The same empty constant list, over a relation.
     */
    @Test
    void hasIntersectionRelationEmptyListCompiles() {
        assertEquals(0, runCount(exprOp("hasIntersection",
                var("request.resource.attr.tags"), listOp())));
    }

    /**
     * <strong>Corpus gap.</strong> #668: The same empty constant list, over a {@code map()}
     * projection.
     */
    @Test
    void hasIntersectionMapEmptyListCompiles() {
        Operand mapExpr = exprOp("map",
                var("request.resource.attr.tags"),
                lambda("t", var("t.name")));
        assertEquals(0, runCount(exprOp("hasIntersection", mapExpr, listOp())));
    }

    /**
     * {@code eq}/{@code ne} against a list constant, in either operand order. CEL equality across
     * types is false, so a scalar column never equals a list; a relation must throw a named
     * {@link IllegalArgumentException} rather than a raw Hibernate conversion error, whose message
     * must not contain the element values.
     */
    @Nested
    class StructuredConstantComparison {

        /** Rows whose aString is present: {@code aString != canary} holds for every one. */
        private int presentStrings() {
            return runCount(exprOp("ne", var("request.resource.attr.aString"), sval(ELEM_A)));
        }

        /**
         * <strong>Corpus gap.</strong> #668: the corpus carries {@code aString == [...]} field
         * first ({@code type-mismatch/equals/string-field-against-list-literal}); the value-first
         * order and {@code !=} are not carried.
         */
        @Test
        void aScalarFieldNeverEqualsAListConstant() {
            assertEquals(0, runCount(
                    exprOp("eq", listOp(ELEM_A), var("request.resource.attr.aString"))));
            assertEquals(presentStrings(), runCount(
                    exprOp("ne", var("request.resource.attr.aString"), listOp(ELEM_A, ELEM_B))));
            assertEquals(presentStrings(), runCount(
                    exprOp("ne", listOp(ELEM_A, ELEM_B), var("request.resource.attr.aString"))));
        }

        /**
         * <strong>Corpus gap.</strong> #668: {@code comparison/equals/whole-list-literal} carries this shape with a one-element
         * list; a multi-element list is not carried.
         */
        @Test
        void eqRelationAgainstListConstantThrowsNamedError() {
            assertNamedError(
                    exprOp("eq", var("request.resource.attr.tags"), listOp(ELEM_A, ELEM_B)),
                    "eq", "request.resource.attr.tags", "list of 2 elements");
        }

    }

    @Nested
    class CollectionMacroComposition {

        /**
         * <strong>Corpus gap.</strong> #668: {@code all} with a conjunctive body is not carried.
         */
        @Test
        void allWithNestedAnd() {
            Operand cond = exprOp("all",
                    var("request.resource.attr.tags"),
                    lambda("t", exprOp("and",
                            exprOp("eq", var("t.name"), sval("public")),
                            exprOp("ne", var("t.id"), sval("tag1")))));
            assertEquals(0, runCount(cond));
        }

        /**
         * <strong>Corpus gap.</strong> #668: {@code size/equals/filtered-collection} carries {@code == 1}; the
         * other operators and the value-first mirror are not carried.
         */
        @Test
        void sizeOfFilterCountsMatchingElements() {
            // Translates to a correlated COUNT with the lambda as its WHERE clause.
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
                assertEquals(1, runCount(exprOp("gt", exprOp("size", filterExpr), nval(0))));
                assertEquals(0, runCount(exprOp("eq", exprOp("size", filterExpr), nval(0))));
                // Value-first is mirrored.
                assertEquals(1, runCount(exprOp("gt", nval(3), exprOp("size", filterExpr))));
            });
        }
    }

    /**
     * Null in membership tests. CEL's {@code null in ["a", null]} is true, but SQL's
     * {@code IN ('a', NULL)} never matches a NULL, so a null is translated as an extra
     * {@code IS NULL} condition.
     */
    @Nested
    class InListNullElements {

        /** Seeds rows whose aOptionalString is "a", "b" and NULL. */
        private void withOwnerRows(Runnable body) {
            ResourceEntity a = new ResourceEntity("in-null-a");
            a.setaOptionalString("a");
            ResourceEntity b = new ResourceEntity("in-null-b");
            b.setaOptionalString("b");
            ResourceEntity nul = new ResourceEntity("in-null-nul");
            nul.setaOptionalString(null);
            withResource(a, () -> withResource(b, () -> withResource(nul, body)));
        }

        /** Seeds rows with a tag named "x", a tag with a NULL name, and no tags. */
        private void withTagRows(Runnable body) {
            ResourceEntity withX = new ResourceEntity("in-null-tag-x");
            withX.addTag("int1", "x");
            ResourceEntity withNullName = new ResourceEntity("in-null-tag-nul");
            withNullName.addTag("int2", null);
            ResourceEntity noTags = new ResourceEntity("in-null-tag-none");
            withResource(withX, () -> withResource(withNullName, () -> withResource(noTags, body)));
        }

        /** Translates {@code condition}, runs it, and returns the matched ids. */
        private Set<String> runIds(Operand condition) {
            PlanResourcesResponse resp =
                    buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);
            QueryPlanFilter filter = JpaQueryPlanAdapter.toFilter(resp, JpaQueryPlanAdapter.Options.of(MAPPER)
                    .withNullAttributeRepresentation(NullAttributeRepresentation.EXPLICIT));
            EntityManager em = emf.createEntityManager();
            try {
                CriteriaBuilder cb = em.getCriteriaBuilder();
                CriteriaQuery<String> cq = cb.createQuery(String.class);
                Root<ResourceEntity> root = cq.from(ResourceEntity.class);
                cq.select(root.get("id")).distinct(true);
                Predicate p = filter.toPredicate(root, cq, cb);
                if (p != null) {
                    cq.where(p);
                }
                return Set.copyOf(em.createQuery(cq).getResultList());
            } finally {
                em.close();
            }
        }

        /**
         * <strong>Corpus gap.</strong> #668: {@code null in R.attr.x} over a scalar column is not
         * carried; the corpus's null needles are over relations ({@code null/in/null-literal-in-resource-list}).
         */
        @Test
        void nullNeedleAgainstScalarFieldIsIsNull() {
            // Membership in a scalar is equality, so this is IS NULL.
            Operand cond = exprOp("in",
                    nullVal(), var("request.resource.attr.aOptionalString"));
            withOwnerRows(() ->
                    assertEquals(Set.of("in-null-nul"), runIds(cond)));
        }

    }

    @Nested
    class HierarchyOperators {

        private Operand hierarchy(Operand inner, String delimiter) {
            return exprOp("hierarchy", inner, sval(delimiter));
        }

        private Operand hierarchy(Operand inner) {
            return exprOp("hierarchy", inner);
        }

        /**
         * <strong>Corpus gap.</strong> #668: The corpus's hierarchy constants are two segments or
         * more; a single-segment ancestor, which has no strict ancestors at all, is not carried.
         */
        @Test
        void ancestorOfSingleSegmentConstantMatchesNothing() {
            // "a" has no strict ancestors, and a path is not its own ancestor: no rows.
            Operand cond = exprOp("ancestorOf",
                    hierarchy(var("request.resource.attr.aString"), ":"),
                    hierarchy(sval("a"), ":"));
            withScopeRows(SCOPES, () ->
                    assertEquals(Set.of(), scopeIds(cond)));
        }

        /**
         * <strong>Corpus gap.</strong> #668: A single-segment descendant prefix, under which the
         * sibling branch is a genuine descendant, is not carried.
         */
        @Test
        void descendentOfSingleSegmentConstant() {
            // LIKE 'a:%': everything under "a", including "a:bb:c", but not "a" itself.
            Operand cond = exprOp("descendentOf",
                    hierarchy(var("request.resource.attr.aString"), ":"),
                    hierarchy(sval("a"), ":"));
            withScopeRows(SCOPES, () ->
                    assertEquals(Set.of("a:b", "a:b:c", "a:b:c:d", "a:bb:c"), scopeIds(cond)));
        }

        /**
         * <strong>Corpus gap.</strong> #668: {@code overlaps} between two column hierarchies is not
         * carried.
         */
        @Test
        void twoFieldHierarchiesInOverlapThrows() {
            assertConditionThrows(
                    exprOp("overlaps",
                            hierarchy(var("request.resource.attr.aString"), ":"),
                            hierarchy(var("request.resource.attr.createdBy"), ":")),
                    "two field-reference hierarchies");
        }

    }

    // The planner keeps policy source order, so a constant can come before the column.
    // Ordering operators must be mirrored.
    @Nested
    class OperandOrderSemantics {

        /**
         * <strong>Corpus gap.</strong> #668: The corpus carries {@code comparison/less-or-equal/value-first}, {@code comparison/greater-or-equal/value-first},
         * {@code comparison/less-than/value-first} and {@code comparison/not-equals/value-first}; value-first {@code gt} is not carried.
         */
        @Test
        void gtValueFirstMeansFieldLessThan() {
            // 10 > aNumber, with aNumber = 5.
            withResource(orderSeed(), () ->
                    assertEquals(1, runCount(exprOp("gt", nval(10), var("request.resource.attr.aNumber")))));
        }

        /**
         * <strong>Corpus gap.</strong> #668: {@code size/less-than/value-first} spells {@code 0 < size(...)}; the
         * {@code 1 > size(...)} mirror, which lowers to NOT EXISTS, is not carried.
         */
        @Test
        void sizeValueFirstEmptinessCheck() {
            // 1 > size(ownedBy) is NOT EXISTS; the seeded row has owners.
            withResource(orderSeed(), () ->
                    assertEquals(0, runCount(exprOp("gt",
                            nval(1),
                            exprOp("size", var("request.resource.attr.ownedBy"))))));
        }

        /**
         * <strong>Corpus gap.</strong> #668: The deprecated {@code has_intersection} spelling the
         * PDP still accepts is not carried.
         */
        @Test
        void hasIntersectionSnakeCaseAliasIsAccepted() {
            withResource(orderSeed(), () ->
                    assertEquals(1, runCount(exprOp("has_intersection",
                            var("request.resource.attr.ownedBy"),
                            listOp("user1")))));
        }

    }

    /**
     * <strong>Corpus gap.</strong> #668: {@code hasIntersection} between two attributes is not
     * carried.
     */
    @Test
    void hasIntersectionVariableVariableReportsBothOperands() {
        // The message names both operands and lists the supported shapes.
        assertConditionThrows(
                exprOp("hasIntersection",
                        var("request.resource.attr.tagNames"),
                        var("request.resource.attr.ownedBy")),
                "VARIABLE 'request.resource.attr.tagNames'",
                "VARIABLE 'request.resource.attr.ownedBy'",
                "Supported shapes");
    }

    // in(variable, variable): the second attribute must be a relation.
    @Nested
    class InVariableVariable {

        /**
         * <strong>Corpus gap.</strong> #668: {@code in} whose second attribute is a scalar is not
         * carried; {@code membership/in/field-in-resource-list} maps a relation.
         */
        @Test
        void scalarSecondOperandThrowsNamedError() {
            assertConditionThrows(
                    exprOp("in",
                            var("request.resource.attr.aString"),
                            var("request.resource.attr.createdBy")),
                    "request.resource.attr.createdBy", "Relation", "scalar Field mapping");
        }

    }

}
