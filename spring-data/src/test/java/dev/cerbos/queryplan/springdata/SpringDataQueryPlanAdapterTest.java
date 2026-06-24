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
        PlanResourcesResponse resp =
                buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);
        Result<ResourceEntity> result =
                SpringDataQueryPlanAdapter.toSpecification(resp, MAPPER);
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

    @Test
    void unsupportedSizeComparisonThrows() {
        // Only emptiness checks (size > 0, size == 0) are supported; size > 5 must throw.
        assertConditionThrows(
                exprOp("gt",
                        exprOp("size", var("request.resource.attr.ownedBy")),
                        nval(5)),
                "size", "Unsupported size comparison");
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
        void ternaryThrows() {
            // The CEL planner emits ternary as `if(cond, then, else)` in the AST.
            Operand ternary = exprOp("if",
                    var("request.resource.attr.aBool"),
                    var("request.resource.attr.aNumber"),
                    nval(0));
            assertConditionThrows(exprOp("gt", ternary, nval(0)), "if()");
        }

        @Test
        void stringSizeThrows() {
            // size(aString) > 0 — size() requires a Relation mapping; aString is a Field.
            assertConditionThrows(
                    exprOp("gt",
                            exprOp("size", var("request.resource.attr.aString")),
                            nval(0)),
                    "size()", "Relation");
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
        void equalFieldToFieldThrows() {
            // eq(var, var) — adapter rejects two-variable comparisons with a specific message.
            assertConditionThrows(
                    exprOp("eq",
                            var("request.resource.attr.aString"),
                            var("request.resource.attr.createdBy")),
                    "Field-to-field", "eq");
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
        void sizeOfFilterThrows() {
            // size(tags.filter(t, t.name == "public")) > 0 — size() operand must be a variable.
            Operand filterExpr = exprOp("filter",
                    var("request.resource.attr.tags"),
                    lambda("t", exprOp("eq", var("t.name"), sval("public"))));
            assertConditionThrows(
                    exprOp("gt", exprOp("size", filterExpr), nval(0)),
                    "size()");
        }
    }

    @Test
    void operatorOverrideIsUsed() {
        Operand cond = exprOp("eq", var("request.resource.attr.aString"), sval("foo"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, cond);

        // Override eq to always produce IS NULL — so result count is 0 (no nulls in empty table either, still 0).
        Map<String, OperatorFunction> overrides = Map.of(
                "eq", (cb, field, value) -> cb.isNull(field));

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
            cq.where(p);
            assertEquals(0L, em.createQuery(cq).getSingleResult().longValue());
        } finally {
            em.close();
        }
    }
}
