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

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks which exception type each refusal uses: {@link UnsupportedPlanShapeException},
 * {@link UnmappedAttributeException} or {@link MalformedPlanException}. Corpus refusals are the
 * {@code unsupported} entries of {@code conformance-ledger.json}, replayed from the current PDP's
 * goldens; malformed plans are hand-built because the planner never emits them. Runs offline.
 */
class RefusalTypesTest {

    private static final Options OPTIONS = Options.of(Corpus.MAPPING);

    /** The case ids the ledger says this adapter refuses under the current PDP. */
    private static final Set<String> UNSUPPORTED = Corpus.ledger().entrySet().stream()
            .filter(e -> "unsupported".equals(e.getValue().status())
                    && e.getValue().appliesTo(Corpus.currentTag()))
            .map(Map.Entry::getKey)
            .collect(Collectors.toCollection(TreeSet::new));

    /**
     * The corpus refusals that are the mapping's to fix rather than a Criteria limit:
     * {@code createdBy} is a String column, which does not pin an instant. Every other corpus
     * refusal is an {@link UnsupportedPlanShapeException}.
     */
    private static final Set<String> UNMAPPED = Set.of(
            "cast/timestamp/malformed-string",
            "cast/timestamp/negated-malformed-string");

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

    @Test
    void everyUnsupportedCaseIsRefusedAsItsClassifiedType() {
        assertTrue(UNSUPPORTED.containsAll(UNMAPPED), () -> "not ledgered unsupported: " + UNMAPPED);
        for (String caseId : UNSUPPORTED) {
            IllegalArgumentException ex = refusal(Corpus.plan(caseId), OPTIONS);
            Class<? extends IllegalArgumentException> expected = UNMAPPED.contains(caseId)
                    ? UnmappedAttributeException.class : UnsupportedPlanShapeException.class;
            // Planner output is never malformed, so MalformedPlanException always fails here.
            assertInstanceOf(expected, ex,
                    () -> caseId + " was refused as " + ex.getClass().getSimpleName()
                            + " with \"" + ex.getMessage() + "\"");
        }
    }

    /**
     * Under a call-level OMITTED, a null comparison against an undeclared attribute is refused by
     * {@code toSpecification} itself, before any predicate is built, because any NULL-matching
     * filter would over-grant.
     */
    @Test
    void theOmittedConventionRefusalIsAnUnsupportedShapeRaisedEagerly() {
        PlanResourcesResponse plan = Corpus.plan("null/equals/null-literal-on-missing-attribute");
        Options callLevelOmitted = Options.of(Corpus.MAPPING_WITHOUT_NULL_CONVENTIONS)
                .withNullAttributeRepresentation(NullAttributeRepresentation.OMITTED);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> SpringDataQueryPlanAdapter.toSpecification(plan, callLevelOmitted));
        assertInstanceOf(UnsupportedPlanShapeException.class, ex);
        assertTrue(ex.getMessage().contains("NullAttributeRepresentation.OMITTED"), ex.getMessage());
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

    /**
     * Builds the Specification and its predicate and returns the refusal either step throws: a
     * refusal under the OMITTED convention is raised by {@code toSpecification} itself.
     */
    private static IllegalArgumentException refusal(PlanResourcesResponse plan, Options options) {
        EntityManager em = emf.createEntityManager();
        try {
            CriteriaBuilder cb = em.getCriteriaBuilder();
            CriteriaQuery<String> cq = cb.createQuery(String.class);
            Root<ResourceEntity> root = cq.from(ResourceEntity.class);
            cq.select(root.get("id"));
            return assertThrows(IllegalArgumentException.class, () -> SpringDataQueryPlanAdapter
                    .<ResourceEntity>toSpecification(plan, options).toPredicate(root, cq, cb));
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
