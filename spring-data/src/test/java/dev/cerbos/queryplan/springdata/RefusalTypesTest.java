/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import com.google.protobuf.ListValue;
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
import jakarta.persistence.criteria.Root;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.jpa.domain.Specification;

import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks which exception type each refusal uses: {@link UnsupportedPlanShapeException},
 * {@link UnmappedAttributeException} or {@link MalformedPlanException}. Corpus refusals come from
 * the wire fixtures; malformed plans are hand-built because the planner never emits them. Runs
 * offline.
 */
class RefusalTypesTest {

    private static final Map<String, String> THROWING =
            Corpus.throwingActions(Corpus.actionsFile(), Corpus.ADAPTER);

    private static final Options OPTIONS = Options.of(Corpus.MAPPING);

    private static EntityManagerFactory emf;

    @BeforeAll
    static void setUp() {
        emf = Persistence.createEntityManagerFactory("translator-pu");
    }

    @AfterAll
    static void tearDown() {
        emf.close();
    }

    // -- the corpus ------------------------------------------------------------------------------

    /**
     * The expected type for every corpus action this adapter refuses. The
     * {@link UnmappedAttributeException} entries are fixed by changing the mapping:
     * {@code createdBy} is a String column, and {@code null-value-f2f-mixed} compares two
     * different null conventions.
     */
    private static final Map<String, Class<? extends IllegalArgumentException>> CLASSIFIED =
            Map.ofEntries(
                    Map.entry("regex-final-newline", UnsupportedPlanShapeException.class),
                    Map.entry("regex-eq-true", UnsupportedPlanShapeException.class),
                    Map.entry("regex-lookahead", UnsupportedPlanShapeException.class),
                    Map.entry("index-negative", UnsupportedPlanShapeException.class),
                    Map.entry("index-fractional", UnsupportedPlanShapeException.class),
                    Map.entry("index-not-oob", UnsupportedPlanShapeException.class),
                    Map.entry("cast-not-int", UnsupportedPlanShapeException.class),
                    Map.entry("cast-not-string-missing", UnsupportedPlanShapeException.class),
                    Map.entry("cast-not-string-null", UnsupportedPlanShapeException.class),
                    Map.entry("cast-not-timestamp", UnmappedAttributeException.class),
                    Map.entry("cast-not-double", UnsupportedPlanShapeException.class),
                    Map.entry("regex-digit", UnsupportedPlanShapeException.class),
                    Map.entry("regex-case", UnsupportedPlanShapeException.class),
                    Map.entry("regex-posix", UnsupportedPlanShapeException.class),
                    Map.entry("regex-unanchored", UnsupportedPlanShapeException.class),
                    Map.entry("regex-dot", UnsupportedPlanShapeException.class),
                    Map.entry("regex-alternation", UnsupportedPlanShapeException.class),
                    Map.entry("regex-grouped", UnsupportedPlanShapeException.class),
                    Map.entry("regex-brace", UnsupportedPlanShapeException.class),
                    Map.entry("regex-repetition", UnsupportedPlanShapeException.class),
                    Map.entry("regex-optional-operators", UnsupportedPlanShapeException.class),
                    Map.entry("except-root", UnsupportedPlanShapeException.class),
                    Map.entry("except-size", UnsupportedPlanShapeException.class),
                    Map.entry("except-eq", UnsupportedPlanShapeException.class),
                    Map.entry("pv-structs", UnsupportedPlanShapeException.class),
                    Map.entry("pv-structs-null", UnsupportedPlanShapeException.class),
                    Map.entry("pv-exists-one", UnsupportedPlanShapeException.class),
                    Map.entry("pv-filter", UnsupportedPlanShapeException.class),
                    Map.entry("pv-map", UnsupportedPlanShapeException.class),
                    Map.entry("pv-except", UnsupportedPlanShapeException.class),
                    Map.entry("temporal-raw-eq", UnsupportedPlanShapeException.class),
                    Map.entry("eq-list", UnsupportedPlanShapeException.class),
                    Map.entry("ne-list", UnsupportedPlanShapeException.class),
                    Map.entry("eq-map", UnsupportedPlanShapeException.class),
                    Map.entry("ne-map", UnsupportedPlanShapeException.class),
                    Map.entry("eq-map-null", UnsupportedPlanShapeException.class),
                    Map.entry("in-nested-list", UnsupportedPlanShapeException.class),
                    Map.entry("hasint-map-element", UnsupportedPlanShapeException.class),
                    Map.entry("arith-mod", UnsupportedPlanShapeException.class),
                    Map.entry("cast-double-string", UnsupportedPlanShapeException.class),
                    Map.entry("cast-int-double", UnsupportedPlanShapeException.class),
                    Map.entry("cast-int-string", UnsupportedPlanShapeException.class),
                    Map.entry("cast-string-double", UnsupportedPlanShapeException.class),
                    Map.entry("concat-f2f", UnsupportedPlanShapeException.class),
                    Map.entry("cr-div-then-add", UnsupportedPlanShapeException.class),
                    Map.entry("cr-div-then-add-ne", UnsupportedPlanShapeException.class),
                    Map.entry("filter-as-condition", UnsupportedPlanShapeException.class),
                    Map.entry("filter-as-conjunct", UnsupportedPlanShapeException.class),
                    Map.entry("hier-empty-delim", UnsupportedPlanShapeException.class),
                    Map.entry("id-concat", UnsupportedPlanShapeException.class),
                    Map.entry("index-scalar-list", UnsupportedPlanShapeException.class),
                    Map.entry("index-scalar-list-not-eq", UnsupportedPlanShapeException.class),
                    Map.entry("index-scalar-list-null", UnsupportedPlanShapeException.class),
                    Map.entry("index-number-list", UnsupportedPlanShapeException.class),
                    Map.entry("index-number-list-not-eq", UnsupportedPlanShapeException.class),
                    Map.entry("index-bool-list", UnsupportedPlanShapeException.class),
                    Map.entry("index-bool-list-not-eq", UnsupportedPlanShapeException.class),
                    Map.entry("index-bool-list-vs-number", UnsupportedPlanShapeException.class),
                    Map.entry("index-number-list-vs-bool", UnsupportedPlanShapeException.class),
                    Map.entry("map-as-condition", UnsupportedPlanShapeException.class),
                    Map.entry("map-eq-list", UnsupportedPlanShapeException.class),
                    Map.entry("matches-alt", UnsupportedPlanShapeException.class),
                    Map.entry("null-value-f2f-mixed", UnmappedAttributeException.class),
                    Map.entry("p-index", UnsupportedPlanShapeException.class),
                    Map.entry("p-matches", UnsupportedPlanShapeException.class),
                    Map.entry("p-timestamp", UnmappedAttributeException.class));

    @Test
    void everyThrowingCorpusActionIsClassified() {
        assertEquals(new TreeMap<>(THROWING).keySet(), new TreeMap<>(CLASSIFIED).keySet(),
                "the classified set must be exactly the actions actions.json says this adapter refuses");
    }

    @Test
    void everyThrowingCorpusActionIsRefusedAsItsClassifiedType() {
        for (Map.Entry<String, String> entry : THROWING.entrySet()) {
            String action = entry.getKey();
            IllegalArgumentException ex = refusal(Corpus.planFromWireFixture(action), OPTIONS);
            assertInstanceOf(CLASSIFIED.get(action), ex,
                    () -> action + " was refused with \"" + ex.getMessage() + "\"");
            // The right type is not enough; the message must match too.
            assertTrue(ex.getMessage().contains(entry.getValue()),
                    () -> action + " was refused with \"" + ex.getMessage()
                            + "\", not the message actions.json pins");
        }
    }

    /** How many corpus refusals use each type. */
    @Test
    void theRefusalTypesAreDistributedInTheseNumbers() {
        Map<String, Integer> counts = new TreeMap<>();
        for (String action : THROWING.keySet()) {
            IllegalArgumentException ex = refusal(Corpus.planFromWireFixture(action), OPTIONS);
            counts.merge(ex.getClass().getSimpleName(), 1, Integer::sum);
        }
        assertEquals(new TreeMap<>(Map.of(
                        "UnsupportedPlanShapeException", 63,
                        "UnmappedAttributeException", 3)),
                counts);
        assertEquals(THROWING.size(), counts.values().stream().mapToInt(Integer::intValue).sum());
    }

    /**
     * Planner output is never malformed. A failure here is either an upstream planner bug or a
     * refusal raised with the wrong type.
     */
    @Test
    void noCorpusActionIsRefusedAsMalformed() {
        for (String action : THROWING.keySet()) {
            IllegalArgumentException ex = refusal(Corpus.planFromWireFixture(action), OPTIONS);
            assertTrue(!(ex instanceof MalformedPlanException),
                    () -> action + " is planner output but was refused as malformed: "
                            + ex.getMessage());
        }
    }

    /**
     * Under OMITTED, a null comparison is refused by {@code toSpecification} itself, before any
     * predicate is built, because any NULL-matching filter would over-grant.
     */
    @Test
    void theOmittedConventionRefusalIsAnUnsupportedShapeRaisedEagerly() {
        for (Corpus.NullRepresentationOmitted probe
                : Corpus.nullRepresentationThrows(Corpus.actionsFile())) {
            PlanResourcesResponse plan = Corpus.planFromWireFixture(probe.action());
            Options omitted = Options.of(Corpus.MAPPING_WITHOUT_NULL_CONVENTIONS)
                    .withNullAttributeRepresentation(NullAttributeRepresentation.OMITTED);
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> SpringDataQueryPlanAdapter.toSpecification(plan, omitted));
            assertInstanceOf(UnsupportedPlanShapeException.class, ex, probe.action());
            assertTrue(ex.getMessage().contains(Corpus.nullOmittedMessage(probe, Corpus.ADAPTER)),
                    ex.getMessage());
        }
    }

    // -- hand-built plans: the shapes a fixture cannot supply ------------------------------------

    /** Plans that break the wire contract. */
    @Nested
    class MalformedPlans {

        @Test
        void aConditionalPlanWithNoConditionIsMalformed() {
            PlanResourcesResponse plan = response(PlanResourcesFilter.Kind.KIND_CONDITIONAL, null);
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> SpringDataQueryPlanAdapter.toSpecification(plan, OPTIONS));
            assertRefusal(ex, MalformedPlanException.class, "Conditional plan has no condition");
        }

        @Test
        void anUnknownFilterKindIsMalformed() {
            PlanResourcesResponse plan = response(PlanResourcesFilter.Kind.KIND_UNSPECIFIED, null);
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> SpringDataQueryPlanAdapter.toSpecification(plan, OPTIONS));
            assertRefusal(ex, MalformedPlanException.class, "Unknown filter kind");
        }

        @Test
        void theWrongArityIsMalformed() {
            assertRefusal(refusal(expr("not", var("request.resource.attr.aBool"),
                            var("request.resource.attr.aBool")), OPTIONS),
                    MalformedPlanException.class, "not requires exactly 1 operand");
            assertRefusal(refusal(expr("eq", var("request.resource.attr.aString")), OPTIONS),
                    MalformedPlanException.class, "eq requires exactly 2 operands");
            assertRefusal(refusal(expr("in", var("request.resource.attr.aString")), OPTIONS),
                    MalformedPlanException.class, "in requires exactly 2 operands");
        }

        @Test
        void aMacroWhoseSecondOperandIsNotALambdaIsMalformed() {
            assertRefusal(refusal(expr("exists", var("request.resource.attr.tags"),
                            string("x")), OPTIONS),
                    MalformedPlanException.class, "exists second operand must be a lambda");
        }

        @Test
        void aLambdaWithoutAVariableIsMalformed() {
            Operand lambda = expr("lambda",
                    expr("eq", var("t.name"), string("x")), string("not a variable"));
            assertRefusal(refusal(expr("exists", var("request.resource.attr.tags"), lambda),
                            OPTIONS),
                    MalformedPlanException.class, "lambda variable must be a variable operand");
        }

        /**
         * A {@code map()} projection must use its own lambda variable. Inside an {@code exists}
         * body the same name would resolve outward and fail as an unknown attribute instead.
         */
        @Test
        void aProjectionOfAnUnboundNameIsMalformed() {
            Operand projection = expr("lambda", var("x.name"), var("t"));
            assertRefusal(refusal(expr("hasIntersection",
                            expr("map", var("request.resource.attr.tags"), projection),
                            list("a")), OPTIONS),
                    MalformedPlanException.class, "does not start with lambda variable");
        }

        /** The planner would have folded two constants. */
        @Test
        void aConstantComparisonThePlannerWouldHaveFoldedIsMalformed() {
            assertRefusal(refusal(expr("contains", string("a"), string("b")), OPTIONS),
                    MalformedPlanException.class, "Missing variable operand for contains");
        }

        @Test
        void aTimestampLiteralCelWouldRejectIsMalformed() {
            assertRefusal(refusal(expr("lt",
                            expr("timestamp", var("request.resource.attr.createdAt")),
                            expr("timestamp", string("yesterday"))), OPTIONS),
                    MalformedPlanException.class, "could not be parsed as an RFC-3339 instant");
        }

        @Test
        void aMacroOverAScalarLiteralIsMalformed() {
            Operand lambda = expr("lambda",
                    expr("eq", var("request.resource.attr.aString"), var("t")), var("t"));
            assertRefusal(refusal(expr("exists", string("not a list"), lambda), OPTIONS),
                    MalformedPlanException.class,
                    "exists over a literal collection requires a list value");
        }

        @Test
        void aValueWithNoKindIsMalformed() {
            Operand unset = Operand.newBuilder().setValue(Value.newBuilder()).build();
            assertRefusal(refusal(expr("eq", var("request.resource.attr.aString"), unset), OPTIONS),
                    MalformedPlanException.class, "Protobuf Value has no kind set");
        }
    }

    /** Well-formed plans the caller's mapping does not cover. */
    @Nested
    class UnmappedAttributes {

        @Test
        void aVariableTheMappingDoesNotNameIsUnmapped() {
            assertRefusal(refusal(expr("eq", var("request.resource.attr.nonexistent"),
                            string("x")), OPTIONS),
                    UnmappedAttributeException.class, "Unknown attribute");
        }

        @Test
        void aRelationUsedWhereAScalarIsNeededIsUnmapped() {
            assertRefusal(refusal(expr("eq", var("request.resource.attr.tags"), string("x")),
                            OPTIONS),
                    UnmappedAttributeException.class, "is a Relation; cannot resolve as a scalar path");
        }

        @Test
        void aFieldUsedWhereACollectionIsNeededIsUnmapped() {
            Operand lambda = expr("lambda", expr("eq", var("t"), string("x")), var("t"));
            assertRefusal(refusal(expr("exists", var("request.resource.attr.aString"), lambda),
                            OPTIONS),
                    UnmappedAttributeException.class, "exists requires a Relation mapping for");
            assertRefusal(refusal(expr("in", var("request.resource.attr.aString"),
                            var("request.resource.attr.aNumber")), OPTIONS),
                    UnmappedAttributeException.class,
                    "requires the second attribute to be mapped as a Relation");
            // The bare lambda element is not a Field, so size() is not a string length here.
            Operand sizeOfElement = expr("lambda",
                    expr("gt", expr("size", var("t")), number(0)), var("t"));
            assertRefusal(refusal(expr("exists", var("request.resource.attr.tagNames"),
                            sizeOfElement), OPTIONS),
                    UnmappedAttributeException.class, "size() requires a collection (Relation) mapping");
        }

        /** A Field reachable only through a relation chain has no column at the root. */
        @Test
        void aFieldBehindARelationChainUsedAtTheRootIsUnmapped() {
            assertRefusal(refusal(expr("eq", var("request.resource.attr.tags.name"), string("x")),
                            OPTIONS),
                    UnmappedAttributeException.class, "Unknown attribute");
        }
    }

    // -- helpers -----------------------------------------------------------------------------------

    private static void assertRefusal(IllegalArgumentException ex,
                                      Class<? extends IllegalArgumentException> type,
                                      String fragment) {
        assertInstanceOf(type, ex, () -> "refused as " + ex.getClass().getSimpleName()
                + " with \"" + ex.getMessage() + "\"");
        assertTrue(ex.getMessage().contains(fragment),
                () -> "expected \"" + fragment + "\" in \"" + ex.getMessage() + "\"");
    }

    private static IllegalArgumentException refusal(Operand condition, Options options) {
        return refusal(response(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition), options);
    }

    /** Builds the Specification's predicate and returns the refusal it throws. */
    private static IllegalArgumentException refusal(PlanResourcesResponse plan, Options options) {
        Specification<ResourceEntity> spec = SpringDataQueryPlanAdapter.toSpecification(plan, options);
        EntityManager em = emf.createEntityManager();
        try {
            CriteriaBuilder cb = em.getCriteriaBuilder();
            CriteriaQuery<String> cq = cb.createQuery(String.class);
            Root<ResourceEntity> root = cq.from(ResourceEntity.class);
            cq.select(root.get("id"));
            return assertThrows(IllegalArgumentException.class,
                    () -> spec.toPredicate(root, cq, cb));
        } finally {
            em.close();
        }
    }

    private static PlanResourcesResponse response(PlanResourcesFilter.Kind kind, Operand condition) {
        PlanResourcesFilter.Builder filter = PlanResourcesFilter.newBuilder().setKind(kind);
        if (condition != null) {
            filter.setCondition(condition);
        }
        return PlanResourcesResponse.newBuilder().setFilter(filter).build();
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

    private static Operand number(double v) {
        return Operand.newBuilder().setValue(Value.newBuilder().setNumberValue(v)).build();
    }

    private static Operand list(String... values) {
        ListValue.Builder list = ListValue.newBuilder();
        for (String v : values) {
            list.addValues(Value.newBuilder().setStringValue(v));
        }
        return Operand.newBuilder().setValue(Value.newBuilder().setListValue(list)).build();
    }
}
