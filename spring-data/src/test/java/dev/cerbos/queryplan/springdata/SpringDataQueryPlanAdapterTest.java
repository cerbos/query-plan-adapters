/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests that hand-build plans and run the resulting Specification against in-memory H2. No
 * Docker. Each test sits under one of the three banners from CLAUDE.md, "What a translator unit
 * test may pin": a branch CEL cannot reach, a caller-supplied argument the corpus cannot vary, or
 * a corpus gap (tracked by #414, deleted when the corpus action lands).
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
        Specification<ResourceEntity> spec =
                SpringDataQueryPlanAdapter.toSpecification(resp, mapper, overrides);

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
        Specification<ResourceEntity> spec =
                SpringDataQueryPlanAdapter.toSpecification(resp, MAPPER, Map.of());
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

    // The planner sends map literals as struct() expressions, never as a STRUCT_VALUE constant.
    @Test
    void eqFieldAgainstStructConstantThrowsNamedError() {
        Operand structConstant = Operand.newBuilder()
                .setValue(Value.newBuilder().setStructValue(Struct.newBuilder()
                        .putFields("k", Value.newBuilder().setStringValue(ELEM_A).build())))
                .build();
        assertNamedError(
                exprOp("eq", var("request.resource.attr.aString"), structConstant),
                "eq", "request.resource.attr.aString", "map of 1 entry");
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
    // p-startswith-concat wire fixture).

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
    // actions.json classifies each action against one mapping per adapter, so overrides, null
    // conventions, other column types and the Spring Data call contract have no corpus spelling.
    // ============================================================================================

    @Test
    void alwaysAllowedSpecificationReturnsNullPredicate() {
        // A null predicate makes Spring Data omit the WHERE clause.
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_ALWAYS_ALLOWED, null);
        Specification<ResourceEntity> spec =
                SpringDataQueryPlanAdapter.toSpecification(resp, MAPPER);
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

        private Specification<ResourceEntity> translate(
                Operand condition, NullAttributeRepresentation representation) {
            PlanResourcesResponse resp =
                    buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);
            return SpringDataQueryPlanAdapter.toSpecification(
                    resp, MAPPER, Map.of(), representation);
        }

        private Operand nullEq() {
            return exprOp("eq", var("request.resource.attr.aOptionalString"), nullVal());
        }

        @Test
        void explicitIsTheDefaultAndKeepsIsNull() {
            // The three-argument overload gives IS NULL; an explicit EXPLICIT also translates.
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

        @Test
        void declaringOmittedRejectsANullOperandUnderTheExplicitDefault() {
            PlanResourcesResponse resp = buildResponse(
                    PlanResourcesFilter.Kind.KIND_CONDITIONAL,
                    exprOp("eq", var("request.resource.attr.omitted"), nullVal()));
            IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                    () -> SpringDataQueryPlanAdapter.toSpecification(resp,
                            Map.of("request.resource.attr.omitted", AttributeMapping.field(
                                    "aOptionalString", NullAttributeRepresentation.OMITTED)),
                            Map.of(), NullAttributeRepresentation.EXPLICIT));
            assertTrue(thrown.getMessage().contains("missing-attribute error"));
        }

    }

    // Each nested collection macro adds a correlated subquery, so nesting beyond the limit must
    // throw at translation time rather than emit a filter that may not finish.
    @Nested
    class MacroDepthGuard {

        private static final String DEPTH_PROPERTY = "dev.cerbos.queryplan.springdata.maxMacroDepth";

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
            PlanResourcesResponse plan = Corpus.planFromWireFixture("pv-shadow");
            EntityManager em = emf.createEntityManager();
            try {
                CriteriaBuilder cb = em.getCriteriaBuilder();
                CriteriaQuery<ResourceEntity> query = cb.createQuery(ResourceEntity.class);
                Root<ResourceEntity> root = query.from(ResourceEntity.class);
                SpringDataQueryPlanAdapter.Options options =
                        SpringDataQueryPlanAdapter.Options.of(MAPPER);
                Specification<ResourceEntity> shallow = SpringDataQueryPlanAdapter.toSpecification(
                        plan, options.withMaxMacroDepth(1));
                UnsupportedPlanShapeException error = assertThrows(UnsupportedPlanShapeException.class,
                        () -> shallow.toPredicate(root, query, cb));
                assertTrue(error.getMessage().contains("nesting depth 2 exceeds the maximum of 1"));
                Specification<ResourceEntity> admitted = SpringDataQueryPlanAdapter.toSpecification(
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
     * {@code AttributeMapping} and {@code toSpecification} copy the caller's maps, so changing a
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
        void mutatingCallerMapperAfterToSpecificationDoesNotAffectSpecification() {
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
                Specification<ResourceEntity> spec =
                        SpringDataQueryPlanAdapter.toSpecification(resp, mapper);

                assertEquals(1, countWithSpec(spec));
                // Remap after construction; the Specification must keep the original column.
                mapper.put("request.resource.attr.aString", AttributeMapping.field("aOptionalString"));
                assertEquals(1, countWithSpec(spec),
                        "post-construction mapper mutation changed the captured Specification");
            });
        }

        private int countWithSpec(Specification<ResourceEntity> spec) {
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
    }

    /**
     * {@code repository.delete(Specification)} guard. Hibernate's bulk delete first clears the
     * collection tables using the same predicate, which breaks a correlated subquery over them:
     * the entity survives and its collection rows are deleted. So a relation-mapped
     * Specification throws {@link UnsupportedOperationException} from {@code toPredicate} when
     * called for a delete, before any statement runs.
     */
    @Nested
    class BulkDeleteGuard {

        // The plan for `P.id in R.attr.ownedBy`: the folded principal id comes first.
        private final Operand ownedByUser1 =
                exprOp("in", sval("user1"), var("request.resource.attr.ownedBy"));

        private Specification<ResourceEntity> spec(Operand condition) {
            PlanResourcesResponse resp =
                    buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);
            return SpringDataQueryPlanAdapter.toSpecification(resp, MAPPER, Map.of());
        }

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

        @Test
        void criteriaDeleteInvocationContextIsDetected() {
            // Spring Data's delete passes a Root from a CriteriaDelete with an unrelated
            // CriteriaQuery; SELECT paths pass a Root from query.from(...). The guard tells them
            // apart by whether the Root is in query.getRoots(). Runs on any Spring Data version.
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
            // Spot-check two more relation operators; all subqueries go through chainSubquery.
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

    // ============================================================================================
    // KIND 3 — a policy can reach these, and the corpus does not carry them yet
    //
    // Each test is a corpus gap tracked by #414. Delete it when its corpus action lands.
    // ============================================================================================

    // size(collection) <op> N translates to a correlated COUNT subquery.
    @Nested
    class SizeCountComparisons {

        private ResourceEntity seeded() {
            ResourceEntity r = new ResourceEntity("size-seed-1");
            r.setOwnedBy(new ArrayList<>(List.of("user1", "user2")));
            r.addTag("tagX", "x");
            return r;
        }

        /**
         * <strong>Corpus gap.</strong> #414: The corpus counts a collection against 1 alone
         * ({@code size-threshold}, {@code size-filter-count}); an arbitrary threshold under every
         * operator, over an element collection and an entity relation, is not carried.
         */
        @Test
        void sizeComparedWithArbitraryN() {
            // 2 owners (@ElementCollection) and 1 tag (@OneToMany).
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
                assertEquals(1, runCount(exprOp("eq",
                        exprOp("size", var("request.resource.attr.tags")), nval(1))));
            });
        }

        /**
         * <strong>Corpus gap.</strong> #414: {@code vf-size} mirrors the emptiness check alone; a
         * value-first arbitrary threshold is not carried.
         */
        @Test
        void sizeValueFirstWithArbitraryNIsMirrored() {
            // 3 > size(ownedBy) means size < 3; not mirroring it would give size > 3.
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

    /**
     * Fractional {@code size()} equality. CEL's type checker rejects int-vs-double
     * {@code ==}/{@code !=}, but {@code size(x) != dyn(1.5)} compiles and the planner drops the
     * {@code dyn()}, so these are policy-reachable. The corpus carries the negated string-length
     * half ({@code size-frac-ne-not}, {@code size-frac-eq-not}); the rest is not carried.
     */
    @Nested
    class FractionalSizeEquality {

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

        /** <strong>Corpus gap.</strong> #414: fractional {@code ==} over a collection. */
        @Test
        void eqFractionalIsAlwaysFalse() {
            // A count is never 2.5; truncating the threshold to 2 would match.
            withResource(seeded(), () -> assertEquals(0, runCount(sizeCmp("eq", 2.5))));
        }

        /** <strong>Corpus gap.</strong> #414: fractional {@code !=} over a collection. */
        @Test
        void neFractionalIsAlwaysTrue() {
            // Always true; truncating to ne 2 would exclude the seeded row.
            withResource(seeded(), () -> assertEquals(1, runCount(sizeCmp("ne", 2.5))));
        }

        /**
         * <strong>Corpus gap.</strong> #414: {@code size-frac-ne-not} and {@code size-frac-eq-not}
         * carry the negated string-length forms; the positive polarity is not carried.
         */
        @Test
        void stringSizeFractionalNeExcludesNullColumn() {
            // True for any present string, but a NULL column is a missing attribute, which
            // CEL denies. A plain always-true would return it.
            Operand cond = exprOp("ne",
                    exprOp("size", var("request.resource.attr.aOptionalString")),
                    nval(1.5));
            ResourceEntity withValue = new ResourceEntity("size-frac-str-1");
            withValue.setaOptionalString("ab");
            ResourceEntity withNull = new ResourceEntity("size-frac-str-2");
            withNull.setaOptionalString(null);
            withResource(withValue, () -> withResource(withNull, () ->
                    assertEquals(1, runCount(cond))));
            // eq is always false, NULL or not.
            Operand eqCond = exprOp("eq",
                    exprOp("size", var("request.resource.attr.aOptionalString")),
                    nval(1.5));
            ResourceEntity another = new ResourceEntity("size-frac-str-3");
            another.setaOptionalString("ab");
            withResource(another, () -> assertEquals(0, runCount(eqCond)));
        }

    }

    /**
     * <strong>Corpus gap.</strong> #414: fractional equality over a chain, reachable through
     * {@code dyn()} (see {@link FractionalSizeEquality}) and not carried yet. A count is never fractional, but a row with no parent is still a CEL
     * error that denies under both polarities, so the collapsed result must stay UNKNOWN for it,
     * including under {@code not}.
     */
    @Test
    void fractionalCollapseOverTwoHopChainStaysUnknownForAnAbsentParent() {
        var fin = new SubCategoryEntity("chain-sub-f1", "finance");
        var biz = new CategoryEntity("chain-cat-f1", "business");
        biz.setSubCategories(List.of(fin));
        ResourceEntity parented = new ResourceEntity("chain-r-f1");
        parented.setCategories(List.of(biz));
        // No categories: CEL denies this row whatever the comparison.
        ResourceEntity orphan = new ResourceEntity("chain-r-f2");

        Operand size = exprOp("size", var(CHAIN));
        Operand matching = exprOp("size",
                exprOp("filter", var(CHAIN),
                        lambda("s", exprOp("eq", var("s.name"), sval("finance")))));

        withCategoryGraph(parented, List.of(biz), List.of(fin), () ->
                withResource(orphan, () -> {
                    // ne is true for the parented row only, never the orphan.
                    assertEquals(1, runChainCount(exprOp("ne", size, nval(1.5))));
                    assertEquals(1, runChainCount(exprOp("ne", matching, nval(1.5))));
                    // eq is false for both.
                    assertEquals(0, runChainCount(exprOp("eq", size, nval(1.5))));
                    assertEquals(0, runChainCount(exprOp("eq", matching, nval(1.5))));

                    // Under not, the orphan must stay excluded.
                    assertEquals(0, runChainCount(
                            exprOp("not", exprOp("ne", size, nval(1.5)))));
                    assertEquals(0, runChainCount(
                            exprOp("not", exprOp("ne", matching, nval(1.5)))));
                    assertEquals(1, runChainCount(
                            exprOp("not", exprOp("eq", size, nval(1.5)))));
                    assertEquals(1, runChainCount(
                            exprOp("not", exprOp("eq", matching, nval(1.5)))));
                }));
    }


    // A count is an integer, so against a fractional f: ge/gt f means ge ceil(f), and le/lt f
    // means le floor(f). Truncating f would return extra rows.
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

        /**
         * <strong>Corpus gap.</strong> #414: {@code cr-size-frac-ge} and {@code w1-size-frac-chain}
         * carry the inclusive {@code >=}; the strict {@code >} rounding is not carried.
         */
        @Test
        void gtFractionalRoundsUp() {
            withResource(seeded(), () -> {
                assertEquals(1, runCount(sizeCmp("gt", 1.5)));
                assertEquals(0, runCount(sizeCmp("gt", 2.5)));
            });
        }

        /**
         * <strong>Corpus gap.</strong> #414: {@code w1-size-frac-le-chain} carries the inclusive
         * {@code <=}; the strict {@code <} rounding is not carried.
         */
        @Test
        void ltFractionalRoundsDown() {
            withResource(seeded(), () -> {
                assertEquals(1, runCount(sizeCmp("lt", 2.5)));
                assertEquals(0, runCount(sizeCmp("lt", 1.5)));
            });
        }

        /**
         * <strong>Corpus gap.</strong> #414: A fractional threshold below 1 folds into the
         * emptiness shortcuts, which no corpus action reaches.
         */
        @Test
        void fractionalEmptinessShortcutsStillRoute() {
            // ge 0.5 becomes EXISTS; lt 0.5 becomes NOT EXISTS.
            withResource(seeded(), () -> {
                assertEquals(1, runCount(sizeCmp("ge", 0.5)));
                assertEquals(0, runCount(sizeCmp("lt", 0.5)));
            });
        }

    }

    // size(string) against a threshold outside int range. cb.length is an Integer expression, so
    // casting the threshold to int would wrap it (2^32 becomes 0). No string is that long, so the
    // comparison is decided without SQL: gt/ge/eq are false, and lt/le/ne are true for a present
    // string. A NULL column is a missing attribute, which CEL denies.
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

        /**
         * <strong>Corpus gap.</strong> #414: {@code size-huge-gt} carries {@code >} at 2^32 alone;
         * {@code ge}, {@code eq} and the 2^31 boundary are not carried.
         */
        @Test
        void gtGeEqAboveIntMaxAreAlwaysFalse() {
            withResource(present(), () -> {
                assertEquals(0, runCount(strSize("gt", TWO_POW_32)));
                assertEquals(0, runCount(strSize("gt", TWO_POW_31)));
                assertEquals(0, runCount(strSize("ge", TWO_POW_32)));
                assertEquals(0, runCount(strSize("ge", TWO_POW_31)));
            });
            // A wrapped eq 2^32 would be LENGTH = 0 and match the empty string.
            withResource(emptyString(), () ->
                    assertEquals(0, runCount(strSize("eq", TWO_POW_32))));
        }

        /**
         * <strong>Corpus gap.</strong> #414: {@code size-huge-lt} carries {@code <} at 2^32 and
         * {@code size-huge-lt-not} its negation over the nullable aOptionalString; {@code le} and
         * the 2^31 boundary are not carried.
         */
        @Test
        void ltLeAboveIntMaxIncludePresentAndExcludeNull() {
            withResource(present(), () -> withResource(nullString(), () -> {
                assertEquals(1, runCount(strSize("lt", TWO_POW_32)));
                assertEquals(1, runCount(strSize("le", TWO_POW_32)));
                assertEquals(1, runCount(strSize("lt", TWO_POW_31)));
            }));
        }

        /**
         * <strong>Corpus gap.</strong> #414: {@code ne} above int range is not carried.
         */
        @Test
        void neAboveIntMaxIncludesEmptyStringAndExcludesNull() {
            // size("") != 2^32 is true in CEL; the NULL row stays excluded.
            withResource(emptyString(), () -> withResource(nullString(), () ->
                    assertEquals(1, runCount(strSize("ne", TWO_POW_32)))));
        }

        /**
         * <strong>Corpus gap.</strong> #414: Thresholds below int range are not carried.
         */
        @Test
        void belowIntMinThresholdsFoldMirrored() {
            // A length is never negative: gt/ge/ne hold for any present string, including the
            // empty one; eq/lt/le never hold.
            withResource(emptyString(), () -> withResource(nullString(), () -> {
                assertEquals(1, runCount(strSize("gt", -TWO_POW_32)));
                assertEquals(1, runCount(strSize("ge", -TWO_POW_32)));
                assertEquals(1, runCount(strSize("ne", -TWO_POW_32)));
                assertEquals(0, runCount(strSize("lt", -TWO_POW_32)));
                assertEquals(0, runCount(strSize("le", -TWO_POW_32)));
                assertEquals(0, runCount(strSize("eq", -TWO_POW_32)));
            }));
        }

        /**
         * <strong>Corpus gap.</strong> #414: A fractional threshold outside int range is not
         * carried.
         */
        @Test
        void fractionalHugeThresholdRoundsThenFolds() {
            // Rounded first (ceil for ge, floor for le), then still out of int range.
            withResource(present(), () -> {
                assertEquals(0, runCount(strSize("ge", TWO_POW_32 + 0.5)));
                assertEquals(1, runCount(strSize("le", TWO_POW_32 + 0.5)));
            });
        }

        /**
         * <strong>Corpus gap.</strong> #414: The exact {@code Integer.MAX_VALUE} boundary is not
         * carried.
         */
        @Test
        void boundaryIntegerMaxStillComparesExactly() {
            // Integer.MAX_VALUE is in range, so this is a real LENGTH comparison.
            withResource(present(), () -> {
                assertEquals(0, runCount(strSize("gt", 2147483647.0)));
                assertEquals(1, runCount(strSize("lt", 2147483647.0)));
                assertEquals(1, runCount(strSize("le", 2147483647.0)));
                assertEquals(0, runCount(strSize("ge", 2147483647.0)));
            });
        }
    }

    /**
     * <strong>Corpus gap.</strong> #414: {@code exists-one-multi} carries a single-equality body; a
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
     * <strong>Corpus gap.</strong> #414: {@code hasIntersection(x, [])} against a scalar column is
     * not carried. It is not known whether the planner folds it as it folds {@code in-empty}.
     */
    @Test
    void hasIntersectionScalarEmptyListCompiles() {
        assertEquals(0, runCount(exprOp("hasIntersection",
                var("request.resource.attr.aString"), listOp())));
    }

    /**
     * <strong>Corpus gap.</strong> #414: The same empty constant list, over a relation.
     */
    @Test
    void hasIntersectionRelationEmptyListCompiles() {
        assertEquals(0, runCount(exprOp("hasIntersection",
                var("request.resource.attr.tags"), listOp())));
    }

    /**
     * <strong>Corpus gap.</strong> #414: The same empty constant list, over a {@code map()}
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
     * {@code eq}/{@code ne} against a list constant, in either operand order, must throw a named
     * {@link IllegalArgumentException} rather than a raw Hibernate conversion error. The message
     * must not contain the element values.
     */
    @Nested
    class StructuredConstantComparison {

        /**
         * <strong>Corpus gap.</strong> #414: {@code eq-list} carries a relation-mapped attribute; a
         * scalar column against a list constant is not carried.
         */
        @Test
        void eqFieldAgainstListConstantThrowsNamedError() {
            assertNamedError(
                    exprOp("eq", var("request.resource.attr.aString"), listOp(ELEM_A, ELEM_B)),
                    "eq", "request.resource.attr.aString", "list of 2 elements");
        }

        /**
         * <strong>Corpus gap.</strong> #414: {@code ne-list} carries a relation-mapped attribute; a
         * scalar column against a list constant is not carried.
         */
        @Test
        void neFieldAgainstListConstantThrowsNamedError() {
            assertNamedError(
                    exprOp("ne", var("request.resource.attr.aString"), listOp(ELEM_A, ELEM_B)),
                    "ne", "request.resource.attr.aString", "list of 2 elements");
        }

        /**
         * <strong>Corpus gap.</strong> #414: The value-first spelling of the same gap.
         */
        @Test
        void eqValueFirstListConstantThrowsNamedError() {
            assertNamedError(
                    exprOp("eq", listOp(ELEM_A), var("request.resource.attr.aString")),
                    "eq", "request.resource.attr.aString", "list of 1 element");
        }

        /**
         * <strong>Corpus gap.</strong> #414: The value-first {@code ne} spelling of the same gap.
         */
        @Test
        void neValueFirstListConstantThrowsNamedError() {
            assertNamedError(
                    exprOp("ne", listOp(ELEM_A, ELEM_B), var("request.resource.attr.aString")),
                    "ne", "request.resource.attr.aString", "list of 2 elements");
        }

        /**
         * <strong>Corpus gap.</strong> #414: {@code eq-list} carries this shape with a one-element
         * list; a multi-element list is not carried.
         */
        @Test
        void eqRelationAgainstListConstantThrowsNamedError() {
            assertNamedError(
                    exprOp("eq", var("request.resource.attr.tags"), listOp(ELEM_A, ELEM_B)),
                    "eq", "request.resource.attr.tags", "list of 2 elements");
        }

    }

    /**
     * <strong>Corpus gap.</strong> #414: {@code id-concat-vf} solves a PREFIX concatenation back to
     * a key equality; the suffix form is not carried.
     */
    @Test
    void addSolveStringSuffixStrip() {
        // "foo.bar" == aString + ".bar" solves to aString == "foo".
        Operand cond = exprOp("eq",
                sval("foo.bar"),
                exprOp("add", var("request.resource.attr.aString"), sval(".bar")));
        assertEquals(0, runCount(cond));
    }

    /**
     * <strong>Corpus gap.</strong> #414: {@code arith-add-eq-frac} and its siblings go through SQL
     * arithmetic; the exact whole-number solve in Java is not carried. With int literals the policy
     * is a CEL no-overload error at check time, since attribute values are doubles.
     */
    @Test
    void addSolveNumeric() {
        // 10 == 3 + aNumber solves to aNumber == 7; exact within ±2^53.
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

    /**
     * <strong>Corpus gap.</strong> #414: A constant beyond 2^53 is not carried.
     */
    @Test
    void addSolveOversizedLongRoutesToSqlArithmetic() {
        // Beyond ±2^53 a double is not exact, so this must use SQL double arithmetic instead
        // of the Java solve.
        Operand cond = exprOp("eq",
                exprOp("add", var("request.resource.attr.aNumber"), nval(1)),
                nval(0x1p54));

        ResourceEntity row = new ResourceEntity("add-big-1");
        row.setaNumber(5);
        withResource(row, () -> assertEquals(0, runCount(cond)));
    }

    @Nested
    class CelPrimitives {

        /**
         * <strong>Corpus gap.</strong> #414: {@code string-size} and {@code string-size-gt0} carry
         * {@code >}; equality and the value-first mirror over a string length are not carried.
         */
        @Test
        void stringSizeComparesLength() {
            // size() of a string column is LENGTH; "seededString" has 12 characters.
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
                // Value-first is mirrored.
                assertEquals(1, runCount(exprOp("lt",
                        nval(5),
                        exprOp("size", var("request.resource.attr.aString")))));
            });
        }
    }

    @Nested
    class MinorOperators {

        /**
         * <strong>Corpus gap.</strong> #414: The corpus orders a column against a constant
         * ({@code rel-lt-hop} and siblings) and compares two columns for equality
         * ({@code field-to-field}); a two-column ORDERING is not carried.
         */
        @Test
        void fieldToFieldOrderingKeepsOperandDirection() {
            // Two-column ordering keeps the source operand order.
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

        /**
         * <strong>Corpus gap.</strong> #414: {@code matches()} between two columns is not carried;
         * {@code p-matches} refuses the constant form.
         */
        @Test
        void fieldToFieldUnsupportedOperatorStillThrows() {
            // contains/startsWith/endsWith between two columns translate to LIKE; matches has
            // no column-to-column translation.
            assertConditionThrows(
                    exprOp("matches",
                            var("request.resource.attr.aString"),
                            var("request.resource.attr.createdBy")),
                    "Field-to-field", "matches");
        }

        /**
         * <strong>Corpus gap.</strong> #414: {@code R.attr.aBool == false} is not carried; the
         * corpus reaches the boolean column bare ({@code root-bare-bool}).
         */
        @Test
        void equalBoolFalse() {
            assertEquals(0, runCount(exprOp("eq",
                    var("request.resource.attr.aBool"), bval(false))));
        }

    }

    @Nested
    class CollectionMacroComposition {

        /**
         * <strong>Corpus gap.</strong> #414: {@code all} with a conjunctive body is not carried.
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
         * <strong>Corpus gap.</strong> #414: {@code size-filter-count} carries {@code == 1}; the
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
            Specification<ResourceEntity> spec =
                    SpringDataQueryPlanAdapter.toSpecification(resp, MAPPER, Map.of());
            EntityManager em = emf.createEntityManager();
            try {
                CriteriaBuilder cb = em.getCriteriaBuilder();
                CriteriaQuery<String> cq = cb.createQuery(String.class);
                Root<ResourceEntity> root = cq.from(ResourceEntity.class);
                cq.select(root.get("id")).distinct(true);
                Predicate p = spec.toPredicate(root, cq, cb);
                if (p != null) {
                    cq.where(p);
                }
                return Set.copyOf(em.createQuery(cq).getResultList());
            } finally {
                em.close();
            }
        }

        /**
         * <strong>Corpus gap.</strong> #414: {@code null in R.attr.x} over a scalar column is not
         * carried; the corpus's null needles are over relations ({@code in-null-elem-rel}).
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
         * <strong>Corpus gap.</strong> #414: The corpus's hierarchy constants are two segments or
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
         * <strong>Corpus gap.</strong> #414: A single-segment descendant prefix, under which the
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
         * <strong>Corpus gap.</strong> #414: {@code overlaps} between two column hierarchies is not
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
         * <strong>Corpus gap.</strong> #414: The corpus carries {@code vf-le}, {@code vf-ge},
         * {@code vf-lt} and {@code vf-ne}; value-first {@code gt} is not carried.
         */
        @Test
        void gtValueFirstMeansFieldLessThan() {
            // 10 > aNumber, with aNumber = 5.
            withResource(orderSeed(), () ->
                    assertEquals(1, runCount(exprOp("gt", nval(10), var("request.resource.attr.aNumber")))));
        }

        /**
         * <strong>Corpus gap.</strong> #414: {@code vf-size} spells {@code 0 < size(...)}; the
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
         * <strong>Corpus gap.</strong> #414: The deprecated {@code has_intersection} spelling the
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

    // A comparison against if(c, a, b) is rewritten into the two branch comparisons, each
    // guarded by c or not c.
    @Nested
    class TernaryIfExpressions {

        /**
         * <strong>Corpus gap.</strong> #414: {@code ternary-bare} has two comparison branches; a
         * constant boolean branch is not carried.
         */
        @Test
        void bareBooleanTernaryWithConstantBranch() {
            // aBool ? true : aNumber > 5
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

            // aBool ? false : aNumber > 5
            Operand planFalse = exprOp("if",
                    var("request.resource.attr.aBool"),
                    bval(false),
                    exprOp("gt", var("request.resource.attr.aNumber"), nval(5)));
            ResourceEntity falseThen = new ResourceEntity("ternary-bare-const-3");
            falseThen.setaBool(true);
            falseThen.setaNumber(10);
            withResource(falseThen, () -> assertEquals(0, runCount(planFalse)));
        }

        /**
         * <strong>Corpus gap.</strong> #414: {@code ternary-negated} carries the negation; the
         * ternary comparison under {@code and}, {@code or} and a double negation is not carried.
         */
        @Test
        void ternaryUnderLogicalOperators() {
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
                // Hibernate collapses cb.not(cb.not(p)); double negation must still toggle back.
                assertEquals(1, runCount(exprOp("not", exprOp("not", comparison))));
                assertEquals(1, runCount(exprOp("and", comparison,
                        exprOp("eq", var("request.resource.attr.aString"), sval("x")))));
                assertEquals(0, runCount(exprOp("and", comparison,
                        exprOp("eq", var("request.resource.attr.aString"), sval("z")))));
                assertEquals(1, runCount(exprOp("or", comparison,
                        exprOp("eq", var("request.resource.attr.aString"), sval("z")))));
            });

            // Here the ternary comparison is false.
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

        /**
         * <strong>Corpus gap.</strong> #414: {@code eq}/{@code ne} against a ternary with a string
         * branch is not carried; the corpus compares its ternaries by ordering.
         */
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

            // The else branch compares two constants: "none" vs "x".
            ResourceEntity elseRow = new ResourceEntity("ternary-eqne-3");
            elseRow.setaBool(false);
            elseRow.setaString("x");
            withResource(elseRow, () -> {
                assertEquals(0, runCount(eqPlan));
                assertEquals(1, runCount(nePlan));
            });
        }

        /**
         * <strong>Corpus gap.</strong> #414: {@code nan-ord-inf} reaches the constant-vs-constant
         * fold through a ternary; the other fold cases are not carried. They are spelled here as
         * direct constant comparisons, which the planner itself would fold; only the ternary form
         * could reach the adapter.
         */
        @Test
        void constantVersusConstantComparisonsFold() {
            // Both branches are constant comparisons, so only the condition remains.
            Operand allConstBranches = exprOp("gt",
                    exprOp("if", var("request.resource.attr.aBool"), nval(1), nval(0)),
                    nval(0));

            ResourceEntity boolTrue = new ResourceEntity("ternary-const-1");
            boolTrue.setaBool(true);
            withResource(boolTrue, () -> {
                assertEquals(1, runCount(allConstBranches));

                // Numbers compare as doubles; mixed types are unequal.
                assertEquals(1, runCount(exprOp("eq", nval(1.0), nval(1))));
                assertEquals(1, runCount(exprOp("lt", nval(0.5), nval(1))));
                assertEquals(0, runCount(exprOp("gt", nval(0), nval(0))));
                assertEquals(1, runCount(exprOp("ge", nval(2), nval(2))));
                assertEquals(1, runCount(exprOp("lt", sval("a"), sval("b"))));
                assertEquals(0, runCount(exprOp("eq", sval("a"), nval(1))));
                assertEquals(1, runCount(exprOp("ne", sval("a"), nval(1))));
                assertEquals(1, runCount(exprOp("eq", bval(true), bval(true))));
                // Beyond the long range these must stay doubles; a (long) cast makes both
                // Long.MAX_VALUE.
                assertEquals(1, runCount(exprOp("gt", nval(1.0e19), nval(9.3e18))));
                assertEquals(1, runCount(exprOp("ne", nval(1.0e19), nval(9.3e18))));
                assertEquals(0, runCount(exprOp("eq", nval(-1.0e19), nval(-9.3e18))));
                // Ordering a string against a number cannot be answered, so it throws.
                assertConditionThrows(exprOp("lt", sval("a"), nval(1)),
                        "Cannot order", "lt");
            });

            ResourceEntity boolFalse = new ResourceEntity("ternary-const-2");
            boolFalse.setaBool(false);
            withResource(boolFalse, () -> assertEquals(0, runCount(allConstBranches)));
        }

        /**
         * <strong>Corpus gap.</strong> #414: {@code p-not-ternary-null} negates a ternary
         * COMPARISON; the negated bare ternary with a NULL condition column is not carried.
         */
        @Test
        void negatedBareTernaryWithNullConditionExcludesRow() {
            // A NULL condition column is a CEL error, so the row is excluded both with and
            // without not.
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

        /**
         * <strong>Corpus gap.</strong> #414: A ternary as the receiver of a string match is not
         * carried.
         */
        @Test
        void ternaryUnderUnsupportedWrapperNamesOperator() {
            // Only eq/ne/lt/gt/le/ge accept a ternary operand.
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

    /**
     * A relation chain such as {@code categories.subCategories} must join through every hop. Its
     * value is the flattened list of tail elements across all intermediate rows.
     */
    @Nested
    class MultiHopRelationChains {

        /**
         * <strong>Corpus gap.</strong> #414: {@code w1-size-chain} carries the emptiness shortcut
         * over the chain; an arbitrary count of flattened elements is not carried.
         */
        @Test
        void sizeOverTwoHopChainCountsFlattenedElements() {
            // Two categories with one sub-category each: the flattened count is 2.
            var s1 = new SubCategoryEntity("chain-sub-s1", "finance");
            var s2 = new SubCategoryEntity("chain-sub-s2", "tech");
            var c1 = new CategoryEntity("chain-cat-s1", "business");
            var c2 = new CategoryEntity("chain-cat-s2", "development");
            c1.setSubCategories(List.of(s1));
            c2.setSubCategories(List.of(s2));
            ResourceEntity r = new ResourceEntity("chain-r-s1");
            r.setCategories(List.of(c1, c2));

            withCategoryGraph(r, List.of(c1, c2), List.of(s1, s2), () -> {
                // Emptiness check (EXISTS).
                assertEquals(1, runChainCount(
                        exprOp("gt", exprOp("size", var(CHAIN)), nval(0))));
                // Arbitrary threshold (COUNT through the joins).
                assertEquals(1, runChainCount(
                        exprOp("ge", exprOp("size", var(CHAIN)), nval(2))));
                assertEquals(0, runChainCount(
                        exprOp("gt", exprOp("size", var(CHAIN)), nval(2))));
            });
        }

    }

    /**
     * <strong>Corpus gap.</strong> #414: Concatenating a null principal attribute is not carried;
     * the corpus principal has no null attribute.
     */
    @Test
    void foldAddNullOperandErrorDoesNotLeakConstantValues() {
        // The folded principal value may be personal data and error messages get logged, so
        // the message names operand types only.
        Operand cond = exprOp("eq",
                var("request.resource.attr.aString"),
                exprOp("add", nullVal(), sval("canary-secret-value")));
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> runCount(cond));
        assertTrue(ex.getMessage().contains("add requires non-null operands"),
                "unexpected message: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("null") && ex.getMessage().contains("String"),
                "expected operand types in message: " + ex.getMessage());
        assertFalse(ex.getMessage().contains("canary-secret-value"),
                "constant value leaked into the error message: " + ex.getMessage());

        // The null on the right.
        IllegalArgumentException ex2 = assertThrows(IllegalArgumentException.class,
                () -> runCount(exprOp("eq",
                        var("request.resource.attr.aString"),
                        exprOp("add", sval("canary-secret-value"), nullVal()))));
        assertFalse(ex2.getMessage().contains("canary-secret-value"),
                "constant value leaked into the error message: " + ex2.getMessage());
    }

    /**
     * <strong>Corpus gap.</strong> #414: {@code hasIntersection} between two attributes is not
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

    /**
     * <strong>Corpus gap.</strong> #414: {@code size()} over a {@code map()} projection is not
     * carried. The string-literal case is folded by the planner; it is here because it hits the
     * same error.
     */
    @Test
    void sizeBadArgumentErrorNamesTheOffendingShape() {
        assertConditionThrows(
                exprOp("gt", exprOp("size", sval("x")), nval(0)),
                "size() argument must be a collection attribute or filter(...)",
                "VALUE (STRING_VALUE)");
        assertConditionThrows(
                exprOp("gt",
                        exprOp("size", exprOp("map",
                                var("request.resource.attr.tags"), lambda("t", var("t.name")))),
                        nval(0)),
                "size() argument must be a collection attribute or filter(...)",
                "EXPRESSION map()");
    }

    @Nested
    class BracketLikeEscaping {

        /**
         * <strong>Corpus gap.</strong> #414: {@code like-bracket} and {@code hier-bracket} carry
         * the shape, but no CI leg runs SQL Server, where {@code [} starts a character class even
         * with an ESCAPE clause. On the other databases the escape changes nothing, so it is
         * checked on the pattern.
         */
        @Test
        void escapeLikeEscapesOpeningBracket() {
            assertEquals("\\[SEC]", PlanValues.escapeLike("[SEC]"));
            assertEquals("50\\%\\[a]\\_b", PlanValues.escapeLike("50%[a]_b"));
            // Backslash is escaped first.
            assertEquals("\\\\\\[", PlanValues.escapeLike("\\["));
            // ']' only closes a class, and none can open once '[' is escaped.
            assertEquals("]", PlanValues.escapeLike("]"));
        }
    }

    // `"a,b".contains(R.attr.x)` arrives as contains(value, variable): the constant is the
    // haystack and the column is the needle, so the operands must not be swapped.
    @Nested
    class ConstantReceiverStringMatch {

        private Operand plan(String op, String constant) {
            return exprOp(op, sval(constant), var("request.resource.attr.aString"));
        }

        /**
         * <strong>Corpus gap.</strong> #414: {@code cr-contains} and its siblings run over a corpus
         * whose aString is never NULL, so the NULL-needle denial has no discriminating seed there.
         */
        @Test
        void nullColumnNeedleExcludesRow() {
            // A NULL column is a missing attribute, which CEL denies.
            withResource(row("cr-9", null), () -> {
                assertEquals(0, runCount(plan("contains", "anything")));
                assertEquals(0, runCount(plan("startsWith", "anything")));
                assertEquals(0, runCount(plan("endsWith", "anything")));
            });
        }

    }

    // in(variable, variable): the second attribute must be a relation.
    @Nested
    class InVariableVariable {

        /**
         * <strong>Corpus gap.</strong> #414: {@code in} whose second attribute is a scalar is not
         * carried; {@code in-var-var} maps a relation.
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

    // Arithmetic as a comparison operand. Attribute values are CEL doubles, so the adapter
    // computes in double arithmetic. mod is refused: CEL's % is int-only, so it always fails
    // on an attribute.
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

        /**
         * <strong>Corpus gap.</strong> #414: {@code arith-sub} carries one subtraction;
         * constant-minus-column under an ordering is not carried.
         */
        @Test
        void subInLtComparison() {
            withResource(seeded(), () -> {
                assertEquals(1, runCount(exprOp("lt",
                        exprOp("sub", numVar(), nval(1)), nval(10))));
                assertEquals(0, runCount(exprOp("lt",
                        exprOp("sub", numVar(), nval(1)), nval(2))));
                // Constant minus column: 10 - 5 <= 5.
                assertEquals(1, runCount(exprOp("le",
                        exprOp("sub", nval(10), numVar()), nval(5))));
            });
        }

        /**
         * <strong>Corpus gap.</strong> #414: Arithmetic nested inside arithmetic is not carried;
         * {@code arith-both} puts one operation on each side.
         */
        @Test
        void nestedArithmetic() {
            // (aNumber + 1) * 2 with aNumber = 5 is 12.
            withResource(seeded(), () -> {
                assertEquals(1, runCount(exprOp("gt",
                        exprOp("mult", exprOp("add", numVar(), nval(1)), nval(2)),
                        nval(11))));
                assertEquals(0, runCount(exprOp("gt",
                        exprOp("mult", exprOp("add", numVar(), nval(1)), nval(2)),
                        nval(12))));
            });
        }

    }

    // Every ordering comparison with NaN is false. The planner sends div(0, 0) unfolded, and the
    // adapter must compare with IEEE operators: Double.compare ranks NaN above every number and
    // -0.0 below 0.0, which would return extra rows.
    @Nested
    class ConstantNanInfinityOrdering {

        /** {@code div(0, 0)}, which the adapter folds to NaN. */
        private Operand nan() {
            return exprOp("div", nval(0), nval(0));
        }

        private Operand posInf() {
            return exprOp("div", nval(1), nval(0));
        }

        private Operand negInf() {
            return exprOp("div", nval(-1), nval(0));
        }

        /** 0.5 as an expression, so the operands are not reordered. */
        private Operand half() {
            return exprOp("div", nval(1), nval(2));
        }

        /**
         * <strong>Corpus gap.</strong> #414: {@code nan-ord-ternary} reaches {@code gt} with NaN on
         * the left through a ternary; {@code ge}, {@code lt} and {@code le} are not carried. The
         * direct constant form here would be folded by the planner; only a ternary could send it.
         */
        @Test
        void nanOnLeftExcludesForAllOrderingOperators() {
            ResourceEntity r = new ResourceEntity("nan-ord-1");
            withResource(r, () -> {
                for (String op : List.of("gt", "ge", "lt", "le")) {
                    assertEquals(0, runCount(exprOp(op, nan(), nval(0.5))),
                            op + "(NaN, 0.5) must exclude every row");
                }
            });
        }

        /**
         * <strong>Corpus gap.</strong> #414: {@code nan-ord-le} carries {@code le} with NaN on the
         * right; the other three operators are not carried.
         */
        @Test
        void nanOnRightExcludesForAllOrderingOperators() {
            // Expressions on both sides, so NaN stays on the right.
            ResourceEntity r = new ResourceEntity("nan-ord-2");
            withResource(r, () -> {
                for (String op : List.of("gt", "ge", "lt", "le")) {
                    assertEquals(0, runCount(exprOp(op, half(), nan())),
                            op + "(0.5, NaN) must exclude every row");
                }
            });
        }

        /**
         * <strong>Corpus gap.</strong> #414: {@code nan-ord-inf} carries {@code gt} against an
         * infinity; the remaining operators and the infinity-versus-infinity ordering are not
         * carried.
         */
        @Test
        void infinityOrderingFollowsIeee() {
            // Unlike NaN, infinities order normally.
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

        /**
         * <strong>Corpus gap.</strong> #414: A negative zero constant is not carried.
         */
        @Test
        void negativeZeroOrderingFollowsIeee() {
            // mult(-1, 0) is -0.0, which IEEE treats as equal to 0.0.
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

    }

    // timestamp(column) against timestamp(constant). Both operands are expressions, so a
    // value-first comparison keeps its order and must be mirrored.
    @Nested
    class TimestampComparisons {

        /**
         * <strong>Corpus gap.</strong> #414: {@code ts-window}, {@code ts-eq}, {@code ts-eq-offset}
         * and {@code ts-ne} carry {@code lt}, {@code eq} and {@code ne}; {@code le}, {@code gt} and
         * {@code ge} are not carried.
         */
        @Test
        void allSixOperatorsFieldFirstOnInstantColumn() {
            withTimestampRows(() -> {
                // The NULL row is excluded by every operator, as CEL denies a missing attribute.
                assertEquals(2, runCount(exprOp("lt", tsVar("createdAt"), tsVal(TS_CONST))));
                assertEquals(3, runCount(exprOp("le", tsVar("createdAt"), tsVal(TS_CONST))));
                assertEquals(1, runCount(exprOp("gt", tsVar("createdAt"), tsVal(TS_CONST))));
                assertEquals(2, runCount(exprOp("ge", tsVar("createdAt"), tsVal(TS_CONST))));
                assertEquals(1, runCount(exprOp("eq", tsVar("createdAt"), tsVal(TS_CONST))));
                assertEquals(3, runCount(exprOp("ne", tsVar("createdAt"), tsVal(TS_CONST))));
            });
        }

        /**
         * <strong>Corpus gap.</strong> #414: {@code ts-vf} carries value-first {@code gt} alone.
         */
        @Test
        void allSixOperatorsValueFirstAreMirroredNotInverted() {
            withTimestampRows(() -> {
                // TS_CONST < column selects the 1 later row, not the 2 earlier ones.
                assertEquals(1, runCount(exprOp("lt", tsVal(TS_CONST), tsVar("createdAt"))));
                assertEquals(2, runCount(exprOp("le", tsVal(TS_CONST), tsVar("createdAt"))));
                assertEquals(2, runCount(exprOp("gt", tsVal(TS_CONST), tsVar("createdAt"))));
                assertEquals(3, runCount(exprOp("ge", tsVal(TS_CONST), tsVar("createdAt"))));
                assertEquals(1, runCount(exprOp("eq", tsVal(TS_CONST), tsVar("createdAt"))));
                assertEquals(3, runCount(exprOp("ne", tsVal(TS_CONST), tsVar("createdAt"))));
            });
        }

        /**
         * <strong>Corpus gap.</strong> #414: The corpus's timestamp constants are whole seconds; a
         * sub-second threshold inside seed a5's microseconds is not carried.
         */
        @Test
        void subSecondPrecisionConstantDiscriminates() {
            // A folded now() - duration constant has sub-second precision. These thresholds
            // fall inside old2's second.
            withTimestampRows(() -> {
                assertEquals(1, runCount(exprOp("le",
                        tsVar("createdAt"), tsVal("2024-06-01T00:00:00.000001Z"))));
                assertEquals(2, runCount(exprOp("le",
                        tsVar("createdAt"), tsVal("2024-06-01T00:00:00.123456Z"))));
            });
        }

        /**
         * <strong>Corpus gap.</strong> #414: A ternary over two timestamp constants is not carried.
         */
        @Test
        void constantVsConstantFoldsViaTernarySubstitution() {
            // Each branch compares two constants, so only the aBool = true row (old1) matches.
            withTimestampRows(() -> assertEquals(1, runCount(exprOp("eq",
                    exprOp("if",
                            var("request.resource.attr.aBool"),
                            tsVal("2024-01-01T00:00:00Z"),
                            tsVal("2030-01-01T00:00:00Z")),
                    tsVal("2024-01-01T00:00:00Z")))));
        }

    }

}
