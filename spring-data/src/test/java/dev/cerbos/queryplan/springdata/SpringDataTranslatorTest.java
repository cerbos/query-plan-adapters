package dev.cerbos.queryplan.springdata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.queryplan.springdata.Corpus.ControlPlane;
import dev.cerbos.queryplan.springdata.Corpus.RepresentationDependentRejection;
import dev.cerbos.queryplan.springdata.testmodel.ResourceEntity;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;
import jakarta.persistence.criteria.Subquery;

import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.H2Dialect;
import org.hibernate.dialect.MySQLDialect;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.query.spi.QueryParameterBindings;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.tree.select.SqmSelectStatement;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.data.jpa.domain.Specification;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Translator unit test: for every action in the shared {@code ../conformance/} corpus, the SQL
 * this adapter emits. Offline — no Cerbos sidecar, no container, and no database: the
 * {@code translator-pu} persistence unit carries no JDBC connection at all, and Hibernate
 * renders the Criteria tree against a dialect it is told about rather than one it discovers.
 *
 * <p>This suite asserts ONE thing, and the three assertions its predecessors braided around it
 * belong elsewhere now:
 *
 * <table border="1">
 *   <caption>Who owns which assertion</caption>
 *   <tr><th>assertion</th><th>owner</th></tr>
 *   <tr><td>the plan the PDP produces for a policy</td>
 *       <td>{@code conformance/wire-fixtures/}, replanned and diffed by the
 *           {@code Conformance Corpus} workflow</td></tr>
 *   <tr><td>which shapes this adapter must refuse, and with what message</td>
 *       <td>{@code adapterctl.json} — read below, never restated</td></tr>
 *   <tr><td>the rows a filter returns</td>
 *       <td>{@link AdversarialConformanceTest}, against real H2/PostgreSQL/MySQL with
 *           {@code check()} as the oracle</td></tr>
 *   <tr><td><strong>the SQL this adapter emits for a plan</strong></td>
 *       <td><strong>here</strong></td></tr>
 * </table>
 *
 * <p><strong>The plans are read, not written.</strong> A hand-built plan is a BELIEF about what
 * the planner emits, and this repository keeps golden fixtures because that belief has been
 * wrong before
 * ({@code docs/adr/0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md}). The
 * hand-built plans that remain in {@link SpringDataQueryPlanAdapterTest} are there for shapes no
 * policy can produce — malformed operands, caller-supplied overrides, mapping validation — which
 * is the one thing a fixture cannot supply.
 *
 * <p><strong>The expectations are data, not literals.</strong> The SQL this adapter is pinned to
 * emit lives in {@code spring-data/golden/expectations.json}, a golden expectation file this
 * adapter owns — never under {@code conformance/}, where every adapter workflow triggers and one
 * adapter re-pinning one statement would re-run all the others. It is regenerated with
 * {@code gradle goldenUpdate} and reviewed as a diff, exactly like the wire fixtures it is
 * asserted against ({@code conformance/README.md}, "Golden expectations").
 *
 * <p><strong>What a pinned statement buys over the harness.</strong> The harness proves the query
 * returns the right rows AGAINST THE ROWS IT SEEDS. Two different queries can agree on all 21 of
 * them and disagree on the row a consumer has, so a rewrite that quietly changes the emitted SQL
 * passes there and shows up here as a diff a reviewer reads.
 */
class SpringDataTranslatorTest {

    /**
     * The dialects the asset records, and the server version each one is rendered at.
     *
     * <p>All three are executed by CI: H2 is the default {@link AdversarialConformanceTest} leg,
     * PostgreSQL and MySQL are the {@code test-database} legs. A dialect nothing executes would
     * be a rendering nobody has ever proved returns the right rows.
     *
     * <p><strong>The version is load-bearing, not decoration.</strong> Told only a dialect class,
     * Hibernate reports that dialect's MINIMUM supported version, and
     * {@link MySqlDoubleCastFunctionContributor} then declines to register — so MySQL would
     * render {@code cast(x as decimal(53,20))} here while the MySQL leg executes
     * {@code cast(x as double)}, and the asset would pin SQL no database in this repository runs.
     * Each version below is the one {@link DatabaseTestImages} pins for that leg (H2 is a driver
     * on the classpath rather than a container, so its version comes from the build file).
     */
    private static final Map<String, Dialect> DIALECTS = new LinkedHashMap<>();

    static {
        DIALECTS.put("h2", new H2Dialect(DatabaseVersion.make(2, 4)));
        DIALECTS.put("postgresql", new PostgreSQLDialect(DatabaseVersion.make(16)));
        DIALECTS.put("mysql", new MySQLDialect(DatabaseVersion.make(8, 4)));
    }

    private static final ObjectMapper JSON = new ObjectMapper();

    private static final ControlPlane ACTIONS = Corpus.actionsFile();

    /**
     * The shapes {@code adapterctl.json} says this adapter must refuse, each with the message it
     * must refuse them with. Identical to the classification the harness asserts against a live
     * PDP; asserting it here as well is what lets the completeness guard below be total, and it
     * costs a millisecond rather than a container.
     *
     * <p>A throwing action needs no golden expectation of its own: the message is already pinned in
     * this adapter's {@code adapterctl.json} manifest. Writing it into this adapter's asset too would
     * create two places to change one string with nothing to say which is authoritative.
     */
    private static final Map<String, String> THROWING =
            Corpus.throwingActions(ACTIONS, Corpus.ADAPTER);

    private static final Map<String, EntityManagerFactory> FACTORIES = new LinkedHashMap<>();

    private static Map<String, ObjectNode> recorded;
    private static List<String> recordedActions;

    /**
     * Every emitted statement, rendered once per action per dialect and read by everything
     * below — the comparison against the asset, the rules, and the regeneration that writes it.
     *
     * <p>One pass, deliberately: the rules are about what the translator emits RIGHT NOW rather
     * than about the pinned bytes, and a second pass would let those two answers drift apart
     * within a single run.
     */
    private static Map<String, Map<String, String>> emitted;

    @BeforeAll
    static void setUp() {
        assumeTrue(System.getenv("ADAPTERCTL_ACTION") == null
                        || System.getenv("ADAPTERCTL_ACTION").isBlank(),
                "action-scoped runs execute the live conformance harness only");
        DIALECTS.forEach((name, dialect) -> FACTORIES.put(name,
                Persistence.createEntityManagerFactory(
                        "translator-pu", Map.of("hibernate.dialect", dialect))));

        emitted = new LinkedHashMap<>();
        for (String action : Corpus.wireFixtureActions()) {
            // A throwing action is never rendered: its message is corpus data, and asking the
            // translator for SQL it must refuse would fail here rather than in the throw suite
            // that owns the question.
            if (!THROWING.containsKey(action)) {
                emitted.put(action, statementsFor(action));
            }
        }

        // `gradle goldenUpdate` rewrites the file from what the translator emits today and
        // preserves every note. That is the same deliberate act as regenerating the wire
        // fixtures, and the safety is identical: the diff is what a reviewer reads. CI never
        // sets the property, so a translator change that moves the emitted SQL fails there
        // whatever anyone ran locally. Skipping the throwing actions above is also what keeps
        // regeneration from papering over a misclassification — an action moved into
        // `rejected` that this adapter still translates fails the throw suite, and one
        // moved out of it that this adapter still refuses fails regeneration itself.
        if (Boolean.getBoolean("golden.update")) {
            Map<String, ObjectNode> expectations = new TreeMap<>();
            emitted.forEach((action, statements) ->
                    expectations.put(action, expectationOf(statements)));
            Corpus.writeGoldenExpectations(expectations);
            System.out.printf("==> rewrote %s (%d expectations)%n",
                    Corpus.goldenFile(), expectations.size());
        }

        recorded = Corpus.readGoldenExpectations();
        recordedActions = List.copyOf(recorded.keySet());
    }

    @AfterAll
    static void tearDown() {
        FACTORIES.values().forEach(EntityManagerFactory::close);
        FACTORIES.clear();
    }

    // -- translating one corpus action ----------------------------------------------------------

    /**
     * The whole translator output for one action, in the shape the golden file records.
     *
     * <p>{@code joins} is present only for the shapes that emit one, which keeps the common entry
     * to a single line per dialect and makes a join APPEARING a visible diff. It is not
     * decoration: a dotted {@code jpaPath} through a to-one association is rendered as a root
     * {@code LEFT JOIN}, and whether that join is LEFT or INNER decides whether a row with an
     * absent parent survives a disjunction — a consumer-visible behaviour change once already
     * (cerbos/query-plan-adapters#375).
     */
    private static ObjectNode expectationOf(Map<String, String> statements) {
        Map<String, Rendered> rendered = new LinkedHashMap<>();
        statements.forEach((dialect, statement) -> rendered.put(dialect, split(statement)));

        ObjectNode entry = JSON.createObjectNode();
        if (rendered.values().stream().anyMatch(r -> r.joins() != null)) {
            ObjectNode joins = entry.putObject("joins");
            rendered.forEach((dialect, r) -> put(joins, dialect, r.joins()));
        }
        ObjectNode where = entry.putObject("where");
        rendered.forEach((dialect, r) -> put(where, dialect, r.where()));
        return entry;
    }

    private static void put(ObjectNode node, String key, String value) {
        if (value == null) {
            node.putNull(key);
        } else {
            node.put(key, value);
        }
    }

    private static Map<String, String> statementsFor(String action) {
        Map<String, String> statements = new LinkedHashMap<>();
        Specification<ResourceEntity> spec = specificationFor(action);
        DIALECTS.keySet().forEach(dialect -> statements.put(dialect, statementOf(dialect, spec)));
        return statements;
    }

    private static Specification<ResourceEntity> specificationFor(String action) {
        return specificationFor(action, Corpus.MAPPING, NullAttributeRepresentation.EXPLICIT,
                Corpus.PLANNED_AT);
    }

    private static Specification<ResourceEntity> specificationFor(
            String action, Map<String, AttributeMapping> mapping,
            NullAttributeRepresentation representation, String plannedAt) {
        PlanResourcesResponse plan = Corpus.planFromWireFixture(action, plannedAt);
        return SpringDataQueryPlanAdapter.toSpecification(
                plan, mapping, Map.of(), representation);
    }

    /**
     * Renders one Specification into the statement the repository would execute.
     *
     * <p>The query is the one {@link AdversarialConformanceTest} runs — {@code select distinct
     * id}, so the preamble the asset strips is that harness's own — and the translation stops one
     * step before JDBC: Hibernate's SQM is converted to a SQL AST and rendered by the dialect,
     * which is everything a database would see except the connection.
     *
     * <p>Criteria literals are INLINED rather than bound ({@code hibernate.criteria.value_handling_mode}
     * in {@code translator-pu}). A parameterised rendering would record {@code a_number>=?} and
     * leave the operand — the half of a filter an authorization bug hides in — out of the asset
     * entirely. What a consumer's database receives is the same statement with those literals
     * bound; a rule below asserts no placeholder survives, so the asset cannot silently become
     * the parameterised rendering.
     */
    private static String statementOf(String dialect, Specification<ResourceEntity> spec) {
        EntityManagerFactory factory = FACTORIES.get(dialect);
        assertNotNull(factory, () -> "no EntityManagerFactory for dialect " + dialect);
        EntityManager em = factory.createEntityManager();
        try {
            CriteriaBuilder cb = em.getCriteriaBuilder();
            CriteriaQuery<String> cq = cb.createQuery(String.class);
            Root<ResourceEntity> root = cq.from(ResourceEntity.class);
            cq.select(root.get("id")).distinct(true);
            Predicate predicate = spec.toPredicate(root, cq, cb);
            if (predicate != null) {
                cq.where(predicate);
            }
            return render(factory, cq);
        } finally {
            em.close();
        }
    }

    private static String render(EntityManagerFactory factory, CriteriaQuery<?> query) {
        SessionFactoryImplementor sf = factory.unwrap(SessionFactoryImplementor.class);
        SelectStatement ast = sf.getQueryEngine().getSqmTranslatorFactory()
                .createSelectTranslator(
                        (SqmSelectStatement<?>) query,
                        QueryOptions.NONE,
                        DomainParameterXref.EMPTY,
                        QueryParameterBindings.NO_PARAM_BINDINGS,
                        new LoadQueryInfluencers(sf),
                        sf,
                        true)
                .translate()
                .getSqlAst();
        return sf.getJdbcServices().getJdbcEnvironment().getSqlAstTranslatorFactory()
                .buildSelectTranslator(sf, ast)
                .translate(JdbcParameterBindings.NO_BINDINGS, QueryOptions.NONE)
                .getSqlString();
    }

    /** The SELECT every emitted statement starts with — everything the asset does not record. */
    private static final String PREAMBLE = "select distinct re1_0.id from resources re1_0";

    private static final String WHERE = " where ";

    /** One emitted statement, minus the preamble: the root joins and the filter. */
    private record Rendered(String joins, String where) {}

    /**
     * Splits a statement into what the asset records. Both halves are {@code null} when absent —
     * no root join, or no filter at all.
     */
    private static Rendered split(String statement) {
        assertTrue(statement.startsWith(PREAMBLE),
                () -> "statement does not start with the corpus preamble: " + statement);
        String rest = statement.substring(PREAMBLE.length());
        int where = rest.indexOf(WHERE);
        String joins = where < 0 ? rest : rest.substring(0, where);
        String clause = where < 0 ? null : rest.substring(where + WHERE.length());
        return new Rendered(joins.isEmpty() ? null : joins.substring(1), clause);
    }

    /** The statement a recorded entry reassembles into. */
    private static String statementFrom(Rendered rendered) {
        return PREAMBLE
                + (rendered.joins() == null ? "" : " " + rendered.joins())
                + (rendered.where() == null ? "" : WHERE + rendered.where());
    }

    // -- @MethodSource feeds --------------------------------------------------------------------

    static Stream<String> recordedActions() {
        return recordedActions.stream();
    }

    static Stream<Arguments> throwingActions() {
        return THROWING.entrySet().stream().map(e -> Arguments.of(e.getKey(), e.getValue()));
    }

    // -- the corpus, action by action -----------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("recordedActions")
    void emitsTheGoldenExpectation(String action) {
        Map<String, String> statements = emitted.get(action);
        assertNotNull(statements, () -> "the asset records '" + action + "', which this adapter "
                + "refuses or the corpus no longer carries — see the completeness guard");
        assertEquals(recorded.get(action), expectationOf(statements),
                () -> "the SQL emitted for '" + action + "' is not the SQL "
                        + Corpus.goldenFile() + " pins; run `" + Corpus.GOLDEN_REGENERATE_COMMAND
                        + "` and review the diff");
    }

    /**
     * The message, not just the throw: a mapper typo or an unrelated validation satisfies a bare
     * {@code assertThrows} just as well as the limitation the corpus documents
     * (cerbos/query-plan-adapters#326). The harness makes the same assertion against a live PDP;
     * here it costs a millisecond, which is what lets the completeness guard below be total.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("throwingActions")
    void isRefusedWithTheMessageActionsJsonPins(String action, String message) {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> statementOf("h2", specificationFor(action)));
        assertTrue(ex.getMessage().contains(message),
                "action '" + action + "' was rejected for a reason adapterctl.json does not declare: "
                        + ex.getMessage());
    }

    /**
     * Adding a throwing action without pinning its message must fail this suite rather than
     * silently degrade the throw assertions to a bare "it threw" (#326).
     */
    @Test
    void throwingActionWithNoPinnedMessageFailsClassification() {
        for (String absent : new String[] {null, ""}) {
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> Corpus.requireMessage("synthetic-entry", absent));
            assertTrue(ex.getMessage().contains("pins no throw message"), ex.getMessage());
        }
    }

    @Test
    void selectedMissingOrUnassessedActionProvisionallyUsesOracle() {
        for (Map<String, Corpus.Outcome> outcomes : List.<Map<String, Corpus.Outcome>>of(
                Map.of(),
                Map.of("new-action", new Corpus.Outcome("unassessed", null, null)))) {
            ControlPlane controlPlane = new ControlPlane(
                    List.of(new Corpus.CatalogAction(
                            "new-action", new Corpus.OracleExpectation("proper-subset", null))),
                    outcomes,
                    "new-action");

            assertEquals(List.of("new-action"),
                    Corpus.oracleActions(controlPlane, Corpus.ADAPTER).toList());
        }
    }

    @Test
    void selectedAssessedActionKeepsItsClassification() {
        ControlPlane controlPlane = new ControlPlane(
                List.of(new Corpus.CatalogAction(
                        "known-action", new Corpus.OracleExpectation("proper-subset", null))),
                Map.of("known-action", new Corpus.Outcome(
                        "rejected", "unsupported", "cannot translate")),
                "known-action");

        assertEquals(Map.of("known-action", "cannot translate"),
                Corpus.throwingActions(controlPlane, Corpus.ADAPTER));
    }

    @Test
    void unscopedRunStillRequiresCompleteOutcomes() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new ControlPlane(
                        List.of(new Corpus.CatalogAction(
                                "new-action",
                                new Corpus.OracleExpectation("proper-subset", null))),
                        Map.of(),
                        ""));

        assertTrue(ex.getMessage().contains("outcomes must cover the catalog exactly"),
                ex.getMessage());
    }

    @Test
    void everyCorpusActionIsAccountedForHereExactlyOnce() {
        List<String> classified = Stream.concat(recordedActions.stream(), THROWING.keySet().stream())
                .sorted()
                .toList();

        // Total: a corpus action with no golden expectation and no pinned throw lands as a
        // failure rather than as silence. This is the assertion that makes the asset
        // self-maintaining — adding a hostile shape to the corpus forces someone to look at the
        // SQL this adapter emits for it, and `goldenUpdate` refuses to invent one for a shape
        // that throws.
        assertEquals(Corpus.wireFixtureActions(), classified,
                "every wire fixture must be accounted for exactly once");
        // Disjoint: an action carrying a golden expectation AND declared unsupported would
        // satisfy the union above while asserting two contradictory things.
        assertEquals(classified.size(), Set.copyOf(classified).size(),
                "an action is either recorded or thrown, never both");
        // The asset is written sorted, so a translator change reads as the list of shapes it
        // moved.
        assertEquals(new ArrayList<>(new TreeSet<>(recordedActions)), recordedActions,
                "golden/expectations.json must stay sorted by action");

    }

    /** Actions whose emitted statement carries no filter at all, on any dialect. */
    private static List<String> unconditionalActions() {
        return recordedActions.stream()
                .filter(action -> emitted.get(action).values().stream()
                        .allMatch(statement -> split(statement).where() == null))
                .toList();
    }

    @Test
    void theUnconditionalActionIsThePlannerFoldTheCorpusDeclares() {
        // `p-has` is the corpus's one `upstream-blocked` outcome: the planner folds has() on a
        // missing attribute to ALWAYS_ALLOWED while check() denies those rows. The adapter must
        // translate that faithfully — an unfiltered SELECT — and this is the assertion that says
        // the empty WHERE belongs to that shape rather than to a translation that quietly stopped
        // emitting a filter.
        assertEquals(List.of("p-has"), unconditionalActions());
        assertTrue(ACTIONS.skippedDivergences(Corpus.ADAPTER).contains("p-has"));
    }

    /**
     * The asset is one renderer's rendering of the adapter's Criteria trees, so it records which
     * one — {@code conformance/README.md}, "When the generator is an input". Unlike SQLAlchemy's
     * two majors there is only ever one Hibernate on this classpath, so there is no second leg to
     * assert a divergence list against; what there is instead is this, which fails the moment a
     * dependency bump makes the recorded bytes somebody else's.
     */
    @Test
    void theAssetDeclaresTheRendererThatWroteIt() throws Exception {
        assertEquals(Corpus.HIBERNATE_MINOR,
                JSON.readTree(Corpus.goldenFile().toFile()).get("hibernate").asText());
        assertTrue(org.hibernate.Version.getVersionString().startsWith(Corpus.HIBERNATE_MINOR + "."),
                () -> "golden/expectations.json was rendered by Hibernate " + Corpus.HIBERNATE_MINOR
                        + " and this build runs " + org.hibernate.Version.getVersionString()
                        + ": re-record the asset deliberately rather than editing the header");
    }

    /**
     * "Offline" is a property of the persistence unit, so it is asserted rather than described.
     *
     * <p>Adding a JDBC url to {@code translator-pu} would make this suite quietly start needing a
     * database — it would still pass, because Hibernate would simply have a connection it never
     * uses, and the claim in this file's javadoc would become false with nothing to say so.
     */
    @Test
    void theTranslatorPersistenceUnitDeclaresNoDatabase() throws Exception {
        String persistenceXml = new String(Objects.requireNonNull(
                        getClass().getResourceAsStream("/META-INF/persistence.xml"),
                        "persistence.xml is not on the test classpath")
                .readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        int start = persistenceXml.indexOf("<persistence-unit name=\"translator-pu\"");
        assertTrue(start > 0, "no translator-pu persistence unit");
        String unit = persistenceXml.substring(
                start, persistenceXml.indexOf("</persistence-unit>", start));
        assertFalse(unit.contains("jakarta.persistence.jdbc."), unit);
        assertFalse(unit.contains("hbm2ddl"), unit);
        // ...and the sibling units DO declare one, so the assertion above is about this unit
        // rather than about a spelling that appears nowhere in the file.
        assertTrue(persistenceXml.contains("jakarta.persistence.jdbc.url"));
    }

    /**
     * The asset carries the command that rewrites it, so a reader who opens the file after a
     * failing assertion is told how to look at the difference. That is only useful while the
     * command exists.
     */
    @Test
    void theAssetNamesACommandThisBuildDefines() throws Exception {
        String[] parts = Corpus.GOLDEN_REGENERATE_COMMAND.split(" ");
        assertEquals("gradle", parts[0]);
        assertTrue(Files.readString(java.nio.file.Path.of(
                        System.getProperty("user.dir"), "build.gradle.kts"))
                .contains("tasks.register<Test>(\"" + parts[1] + "\")"),
                () -> "build.gradle.kts defines no task named " + parts[1]);
    }

    /**
     * The properties a regenerated asset must not silently accept.
     *
     * <p>Pinned bytes do not survive {@code gradle goldenUpdate} being run and committed unread;
     * rules do. So each of these is stated over every translated corpus action rather than over a
     * chosen shape, and each carries an anti-vacuity assertion. They read what the translator
     * emits RIGHT NOW rather than what the asset pins.
     */
    @Nested
    class WhatTheEmittedStatementContains {

        @Test
        void everyStatementIsTheCorpusSelectPlusItsJoinsAndFilter() {
            // The asset records only what follows the preamble, which is lossless exactly while
            // this holds — and the preamble is the query the harness executes, so a statement that
            // stopped starting with it would mean the two suites had stopped describing one query.
            for (String action : recordedActions) {
                emitted.get(action).forEach((dialect, statement) -> assertTrue(
                        statement.startsWith(PREAMBLE),
                        () -> action + " (" + dialect + "): " + statement));
            }
            assertTrue(PREAMBLE.endsWith("from resources re1_0"));
            assertFalse(recordedActions.isEmpty());
        }

        @Test
        void aRecordedEntryReassemblesIntoTheStatementThatProducedIt() {
            // The other half of "recording only the tail is lossless": every entry reassembles
            // into exactly the statement the adapter emitted, preamble included. Without this the
            // asset could be a faithful record of something the adapter never built.
            for (String action : recordedActions) {
                ObjectNode expectation = recorded.get(action);
                for (String dialect : DIALECTS.keySet()) {
                    JsonNode where = expectation.path("where").get(dialect);
                    assertNotNull(where, () -> action + " records no where for " + dialect);
                    JsonNode joins = expectation.path("joins").get(dialect);
                    assertEquals(
                            statementFrom(new Rendered(
                                    joins == null || joins.isNull() ? null : joins.asText(),
                                    where.isNull() ? null : where.asText())),
                            emitted.get(action).get(dialect), action + " (" + dialect + ")");
                }
            }
        }

        @Test
        void noStatementCarriesABindPlaceholder() {
            // The asset is only a complete record of the filter while the operands are IN it.
            // A rendering that started binding them would still reassemble, still pass every
            // other rule here, and quietly stop pinning the half of a filter that decides which
            // rows come back.
            List<String> offenders = new ArrayList<>();
            for (String action : recordedActions) {
                emitted.get(action).forEach((dialect, statement) -> {
                    if (statement.contains("?")) {
                        offenders.add(action + " (" + dialect + "): " + statement);
                    }
                });
            }
            assertEquals(List.of(), offenders);
            // Anti-vacuity: satisfied by a corpus whose statements carry no operands at all.
            assertTrue(emitted.get("cs-eq").get("h2").contains("'one'"),
                    emitted.get("cs-eq").get("h2"));
        }

        @Test
        void everyLikeCarriesANonEmptyEscapeClause() {
            // LIKE metacharacters in a needle are the corpus's founding bug class (#258/#259): an
            // unescaped `%` in a value turns an equality into a wildcard match and returns rows
            // the PDP denies. The adapter escapes them and declares the escape character, and a
            // LIKE that reached the database without one would read those backslashes as literal
            // text.
            //
            // NON-EMPTY is the load-bearing half, and it is not obvious. Hibernate renders the
            // two-argument `cb.like(path, pattern)` as `... escape ''` — a clause that is present
            // and declares nothing — so a rule counting ESCAPE clauses passes while every escaped
            // metacharacter has quietly become literal text. Verified by mutation: dropping the
            // escape argument from the `startsWith` lowering and regenerating the asset produces
            // `like '100\%%' escape ''`, which the count-only version of this rule accepted.
            List<String> unescaped = new ArrayList<>();
            int withLike = 0;
            for (String action : recordedActions) {
                for (Map.Entry<String, String> e : emitted.get(action).entrySet()) {
                    String statement = e.getValue();
                    int likes = count(statement, " like ");
                    if (likes == 0) {
                        continue;
                    }
                    withLike++;
                    if (likes != count(statement, " escape '")
                            || count(statement, " escape ''") > 0) {
                        unescaped.add(action + " (" + e.getKey() + "): " + statement);
                    }
                }
            }
            assertEquals(List.of(), unescaped);
            // Anti-vacuity, in two parts: satisfied by a corpus that emits no LIKE at all, and by
            // one whose escape character never has to do anything.
            assertTrue(withLike > 0);
            assertTrue(emitted.get("like-percent").get("h2").contains("'100\\%%' escape '\\'"),
                    emitted.get("like-percent").get("h2"));
        }

        @Test
        void theResourceTableIsNamedInExactlyOneFromClause() {
            // A correlated subquery that lost its correlation lists the outer table in its OWN
            // FROM and then compares against every row of it — silent wrongness, and the class of
            // bug no row-level oracle catches while the seeded data happens to agree.
            List<String> offenders = new ArrayList<>();
            for (String action : recordedActions) {
                emitted.get(action).forEach((dialect, statement) -> {
                    if (fromClausesNamingTheResource(statement) != 1) {
                        offenders.add(action + " (" + dialect + "): " + statement);
                    }
                });
            }
            assertEquals(List.of(), offenders);

            // Anti-vacuity, in two parts because the rule needs both to say anything. The corpus
            // must still emit subqueries at all: these are the shapes that do — a chained
            // collection macro, a direct EXISTS, and a counted filter().
            for (String action : List.of("w1-all-chain", "exists-on-empty", "size-filter-count")) {
                assertTrue(emitted.get(action).get("h2").contains("(select "),
                        () -> action + ": " + emitted.get(action).get("h2"));
            }
            // And the detector must recognise the thing it is looking for. This is the broken
            // rendering, built here rather than hoped for.
            assertEquals(2, fromClausesNamingTheResource(uncorrelatedRendering()));
        }

        /** A subquery over the same association WITHOUT correlating it to the outer root. */
        private String uncorrelatedRendering() {
            EntityManagerFactory factory = FACTORIES.get("h2");
            EntityManager em = factory.createEntityManager();
            try {
                CriteriaBuilder cb = em.getCriteriaBuilder();
                CriteriaQuery<String> cq = cb.createQuery(String.class);
                Root<ResourceEntity> root = cq.from(ResourceEntity.class);
                cq.select(root.get("id")).distinct(true);
                Subquery<String> sub = cq.subquery(String.class);
                Root<ResourceEntity> uncorrelated = sub.from(ResourceEntity.class);
                sub.select(uncorrelated.join("tags").get("name"));
                cq.where(cb.exists(sub));
                return render(factory, cq);
            } finally {
                em.close();
            }
        }

        @Test
        void everyQualifiedIdentifierNamesAColumnTheModelDeclares() {
            // An identifier the model does not carry is a mapping that would fail at execution
            // time — or worse, resolve against a column that happens to exist. The harness cannot
            // catch the second: it seeds the same schema this maps against.
            Set<String> declared = declaredColumns();
            Set<String> stray = new TreeSet<>();
            Pattern qualified = Pattern.compile("\\b[a-z]+\\d*_\\d+\\.([a-z_]+)\\b");
            for (String action : recordedActions) {
                emitted.get(action).forEach((dialect, statement) -> {
                    Matcher m = qualified.matcher(statement);
                    while (m.find()) {
                        if (!declared.contains(m.group(1))) {
                            stray.add(action + ": " + m.group());
                        }
                    }
                });
            }
            assertEquals(Set.of(), stray);
            // Anti-vacuity, in two parts. The column set must be populated at all...
            assertTrue(declared.contains("a_optional_string"), declared.toString());
            // ...and the detector must reject something, which a rule reading its column set from
            // the same metamodel that rendered the SQL would otherwise be too close to tautology
            // to prove. This is a statement the model does NOT declare, matched here rather than
            // hoped for.
            Matcher stranger = qualified.matcher("re1_0.no_such_column='x'");
            assertTrue(stranger.find(), "the detector matches no qualified identifier at all");
            assertFalse(declared.contains(stranger.group(1)), stranger.group(1));
        }

        @Test
        void everyRootJoinIsALeftJoin() {
            // #375, in the SQL. A dotted jpaPath through a to-one association is joined at the
            // ROOT of the query, and the Criteria API's default there is an INNER join — which
            // removes the row from the WHOLE query when the association is absent. That is right
            // for a standalone predicate and wrong under a disjunction: a row whose parent is
            // missing but whose OTHER branch holds is one the PDP allows. The harness proves the
            // rows for the shapes the corpus carries; this is the property, over every shape.
            List<String> offenders = new ArrayList<>();
            int withJoins = 0;
            for (String action : recordedActions) {
                for (Map.Entry<String, String> e : emitted.get(action).entrySet()) {
                    String joins = split(e.getValue()).joins();
                    if (joins == null) {
                        continue;
                    }
                    withJoins++;
                    // Every `join` in the clause must be spelled `left join`: an INNER one renders
                    // as a bare `join`, and a cross join as `cross join`.
                    for (int at = joins.indexOf("join "); at >= 0;
                            at = joins.indexOf("join ", at + 1)) {
                        if (at < "left ".length()
                                || !joins.startsWith("left ", at - "left ".length())) {
                            offenders.add(action + " (" + e.getKey() + "): " + joins);
                        }
                    }
                }
            }
            assertEquals(List.of(), offenders);
            // Anti-vacuity: satisfied by a corpus that emits no root join at all.
            assertTrue(withJoins > 0);
        }

        @Test
        void mysqlRendersTheIeeeDoubleCastTheOtherDialectsGetForFree() {
            // The README's "MySQL: keeping arithmetic IEEE-faithful" gotcha, pinned in the SQL
            // for the first time. MySQLDialect renders a to-double cast as decimal(53,20), which
            // evaluates CEL's double arithmetic in EXACT decimal and returns rows check() denies;
            // MySqlDoubleCastFunctionContributor replaces it with cast(x as double) on 8.0.17+.
            // This is also the anti-vacuity assertion for the dialect VERSIONS above — told only
            // a dialect class, Hibernate reports its minimum version and the contributor declines
            // to register, so this fails rather than the asset silently pinning decimal.
            assertTrue(emitted.get("p-double-frac").get("mysql").contains("as double)"),
                    emitted.get("p-double-frac").get("mysql"));
            assertTrue(emitted.get("p-double-frac").get("h2").contains("as float(53))"),
                    emitted.get("p-double-frac").get("h2"));
        }

        @Test
        void theFoldedNowLiteralKeepsThePrecisionThePdpEmits() {
            // The one operand a wire fixture cannot pin: `now() - duration("24h")` differs on
            // every capture, so the fixture carries a placeholder and this adapter's reader
            // chooses a value (Corpus.PLANNED_AT). The choice is load-bearing — the PDP emits
            // NANOSECONDS, and a tidy millisecond substitution would pin a comparison the PDP
            // never produces against a column that carries them.
            assertTrue(emitted.get("ts-window").get("h2").contains(".123456789"),
                    emitted.get("ts-window").get("h2"));
            // ...and the dialects that cannot carry nanoseconds truncate rather than round,
            // which is a rendering difference worth seeing in the asset.
            assertTrue(emitted.get("ts-window").get("postgresql").contains(".123456"),
                    emitted.get("ts-window").get("postgresql"));
        }

        private int count(String haystack, String needle) {
            int count = 0;
            for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + 1)) {
                count++;
            }
            return count;
        }

        /**
         * How many of a statement's FROM lists name the resource table. A FROM list is one or
         * more table references — which is exactly how an uncorrelated subquery pulls the outer
         * table in.
         */
        private int fromClausesNamingTheResource(String statement) {
            int count = 0;
            Matcher m = Pattern.compile("from ([a-z_]+ [a-z]+\\d*_\\d+(?:,[a-z_]+ [a-z]+\\d*_\\d+)*)")
                    .matcher(statement);
            while (m.find()) {
                for (String reference : m.group(1).split(",")) {
                    if (reference.trim().startsWith("resources ")) {
                        count++;
                    }
                }
            }
            return count;
        }

        /** Every column name the JPA model declares, read from Hibernate's own mapping. */
        private Set<String> declaredColumns() {
            SessionFactoryImplementor sf =
                    FACTORIES.get("h2").unwrap(SessionFactoryImplementor.class);
            Set<String> columns = new TreeSet<>();
            sf.getMappingMetamodel().forEachEntityDescriptor(descriptor -> {
                descriptor.forEachSelectable((index, selectable) ->
                        columns.add(selectable.getSelectionExpression()));
                // The identifier is not part of forEachSelectable's attribute walk, and it is the
                // column every one of these statements selects.
                descriptor.getIdentifierMapping().forEachSelectable((index, selectable) ->
                        columns.add(selectable.getSelectionExpression()));
            });
            sf.getMappingMetamodel().forEachCollectionDescriptor(descriptor -> {
                descriptor.getAttributeMapping().forEachSelectable((index, selectable) ->
                        columns.add(selectable.getSelectionExpression()));
                // A collection's key and index columns are named in the correlated subqueries but
                // belong to neither entity's attribute walk.
                descriptor.getAttributeMapping().getKeyDescriptor()
                        .forEachSelectable((index, selectable) ->
                                columns.add(selectable.getSelectionExpression()));
            });
            return columns;
        }
    }

    /**
     * The corpus's {@code representationDependentRejection} probe, which has no store in it at all.
     *
     * <p>{@code null-eq-missing} compares {@code aOptionalString == null}, and the planner emits
     * the same {@code eq(attr, null)} node whichever convention the caller uses — so the adapter
     * has to be TOLD, and what it does when it is told is a pure translator property. The harness
     * asserts the same pair against a live PDP and proves the over-grant with real rows; here it
     * costs a millisecond and pins the SQL each option produces.
     */
    @Nested
    class NullAttributeRepresentationOption {

        private final RepresentationDependentRejection probe =
                Corpus.representationDependentRejections(ACTIONS).get(0);

        @Test
        void explicitEmitsAnIsNullFilter() {
            assertEquals("null-eq-missing", probe.action());
            assertTrue(statementOf("h2", specificationFor(probe.action()))
                            .endsWith("where re1_0.a_optional_string is null"),
                    statementOf("h2", specificationFor(probe.action())));
        }

        @Test
        void omittedRefusesTheSamePlan() {
            // A NULL column then sends no attribute, so check() denies on a missing-attribute
            // error while the filter above returns exactly those rows (#302).
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> specificationFor(probe.action(), Corpus.MAPPING,
                            NullAttributeRepresentation.OMITTED, Corpus.PLANNED_AT));
            assertTrue(ex.getMessage().contains(
                    Corpus.nullOmittedMessage(probe, Corpus.ADAPTER)), ex.getMessage());
        }

        @Test
        void aPerAttributeDeclarationOverridesTheCallLevelOption() {
            // #308. `owner` declares EXPLICIT in the corpus mapping, so `null-eq` — which probes
            // it — must still translate under a call-level OMITTED...
            assertEquals(statementOf("h2", specificationFor("null-eq")),
                    statementOf("h2", specificationFor("null-eq", Corpus.MAPPING,
                            NullAttributeRepresentation.OMITTED, Corpus.PLANNED_AT)));

            // ...and stripping the declaration must reject the same action under the same option,
            // so the override above is doing work rather than being quietly equivalent.
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> specificationFor("null-eq", Corpus.MAPPING_WITHOUT_NULL_CONVENTIONS,
                            NullAttributeRepresentation.OMITTED, Corpus.PLANNED_AT));
            assertTrue(ex.getMessage().contains("null operand"), ex.getMessage());
        }
    }
}
