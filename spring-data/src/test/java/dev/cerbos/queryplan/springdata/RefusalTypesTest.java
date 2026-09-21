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

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Which of the three refusal types each refusal is raised as — a property of WHERE in the walk
 * the plan was refused, which neither {@code conformance/actions.json} (a message substring per
 * action) nor {@link SpringDataTranslatorTest} (the SQL of the shapes that translate) can state.
 *
 * <p>Offline like the translator unit test: the {@code translator-pu} persistence unit carries
 * no JDBC connection, and a refusal is raised while the Specification builds its predicate,
 * one step before any SQL exists.
 *
 * <p>Two sources of plans, deliberately. The corpus's throwing actions are read from
 * {@code conformance/wire-fixtures/} — planner output, never hand-built — and each is pinned to
 * its type, so a translator change that moves a refusal from one site to another shows up as a
 * diff even though both sites throw and {@code actions.json} is unchanged. The malformed plans
 * are hand-built because that is the one thing a fixture cannot supply: a planner never emits a
 * wire-contract violation, which is also why {@link #noCorpusActionIsRefusedAsMalformed} holds.
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
     * Every throwing corpus action, classified. The key set is asserted equal to what
     * {@code actions.json} declares for this adapter, so a new throwing action fails here until
     * someone decides which kind of refusal it is.
     *
     * <p>The three {@link UnmappedAttributeException} entries are the judgement calls:
     * {@code p-timestamp} and {@code cast-not-timestamp} are refused because the MAPPING binds
     * {@code createdBy} to a String column that does not pin the instant it stores, and
     * {@code null-value-f2f-mixed} because the two mappings it compares declare different NULL
     * conventions. Both mechanisms are resolved by
     * changing a declaration rather than the policy, which is the line the type draws.
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
                    Map.entry("cast-string-bool", UnsupportedPlanShapeException.class),
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
            // The type and the pinned message travel together: a refusal of the right type for
            // an undeclared reason would be the #326 accident wearing a new label.
            assertTrue(ex.getMessage().contains(entry.getValue()),
                    () -> action + " was refused with \"" + ex.getMessage()
                            + "\", not the message actions.json pins");
        }
    }

    /**
     * The distribution over the corpus. A count, not only a per-action type, so the SHAPE of
     * this adapter's refusals is pinned: three declaration-dependent actions, the rest shapes
     * the Criteria API has no faithful form for.
     */
    @Test
    void theRefusalTypesAreDistributedInTheseNumbers() {
        Map<String, Integer> counts = new TreeMap<>();
        for (String action : THROWING.keySet()) {
            IllegalArgumentException ex = refusal(Corpus.planFromWireFixture(action), OPTIONS);
            counts.merge(ex.getClass().getSimpleName(), 1, Integer::sum);
        }
        assertEquals(new TreeMap<>(Map.of(
                        "UnsupportedPlanShapeException", 64,
                        "UnmappedAttributeException", 3)),
                counts);
        assertEquals(THROWING.size(), counts.values().stream().mapToInt(Integer::intValue).sum());
    }

    /**
     * The corpus is planner output, and the planner honours its own wire contract — so no corpus
     * action may land on a malformed-plan site. One doing so would mean either the planner shipped
     * a shape this adapter reads as malformed (an upstream bug to report) or a well-formed shape
     * is refused at a site that mislabels it.
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
     * The OMITTED-convention probe is refused from {@code toSpecification} itself, before any
     * predicate is built, and it is an unsupported SHAPE: the plan is fine, the convention makes
     * every NULL-selecting rendering of it an over-grant.
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

    // -- the documented base type ----------------------------------------------------------------

    /** A caller catching the base type the adapter always documented keeps working unchanged. */
    @Test
    void everyRefusalTypeExtendsIllegalArgumentException() {
        for (Class<?> type : List.of(UnsupportedPlanShapeException.class,
                UnmappedAttributeException.class, MalformedPlanException.class)) {
            assertTrue(IllegalArgumentException.class.isAssignableFrom(type), type.getName());
        }
    }

    // -- hand-built plans: the shapes a fixture cannot supply ------------------------------------

    /**
     * Wire-contract violations. No planner emits these, so they are built by hand; each is
     * pinned to the message it already carried and to the type that message now travels under.
     */
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
         * A {@code map()} projection must project ITS lambda's variable; a projection of some
         * other name is a plan whose lambda never bound it. (Inside an {@code exists} body the
         * same unbound name delegates outward and is an unknown attribute instead.)
         */
        @Test
        void aProjectionOfAnUnboundNameIsMalformed() {
            Operand projection = expr("lambda", var("x.name"), var("t"));
            assertRefusal(refusal(expr("hasIntersection",
                            expr("map", var("request.resource.attr.tags"), projection),
                            list("a")), OPTIONS),
                    MalformedPlanException.class, "does not start with lambda variable");
        }

        /** Two constants under an operator the constant fold does not cover. */
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

    /**
     * Declaration gaps: the plan is well-formed and the Criteria API could express it, but the
     * caller's mapping does not say how.
     */
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
            // The bare lambda element is the relation's scalar projection, not a Field, so
            // size() of it is not a string length either.
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
     * Translates {@code plan} the way a repository would — by asking the Specification for its
     * predicate — and returns the refusal that raises.
     */
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
