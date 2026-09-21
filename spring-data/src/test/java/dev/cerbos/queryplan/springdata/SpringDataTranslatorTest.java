package dev.cerbos.queryplan.springdata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.queryplan.springdata.Corpus.ActionsFile;
import dev.cerbos.queryplan.springdata.Corpus.NullRepresentationOmitted;
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
import org.hibernate.sql.ast.spi.SqlAstCreationContext;
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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
 *       <td>{@code conformance/actions.json} — read below, never restated</td></tr>
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
 * returns the right rows AGAINST THE ROWS IT SEEDS. Two different queries can agree on all 22 of
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

    private static final ActionsFile ACTIONS = Corpus.actionsFile();

    /**
     * The shapes {@code actions.json} says this adapter must refuse, each with the message it
     * must refuse them with. Identical to the classification the harness asserts against a live
     * PDP; asserting it here as well is what lets the completeness guard below be total, and it
     * costs a millisecond rather than a container.
     *
     * <p>A throwing action needs no golden expectation of its own: the message is already corpus
     * data, pinned once in {@code actions.json} and read by every adapter. Writing it into this
     * adapter's asset too would create two places to change one string with nothing to say which
     * is authoritative.
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
        // `adapterUnsupported` that this adapter still translates fails the throw suite, and one
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

    /**
     * The {@code SqlAstCreationContext} the SQM translator renders under. On Hibernate 6.6 the
     * {@code SessionFactoryImplementor} IS that context; Hibernate 7 moved the role to
     * {@code SqlTranslationEngine}, reached through {@code getSqlTranslationEngine()}, a method 6.6
     * does not have. Resolved reflectively so one source compiles against both majors — this
     * suite runs under both ({@code ADAPTER_TEST_ORM}), and a second copy of the renderer per
     * major would be the drift the divergence list exists to catch.
     */
    private static SqlAstCreationContext sqlAstCreationContext(SessionFactoryImplementor sf) {
        try {
            return (SqlAstCreationContext) SessionFactoryImplementor.class
                    .getMethod("getSqlTranslationEngine").invoke(sf);
        } catch (NoSuchMethodException e) {
            return (SqlAstCreationContext) sf;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("cannot resolve Hibernate's SqlAstCreationContext", e);
        }
    }

    private static String render(EntityManagerFactory factory, CriteriaQuery<?> query) {
        SessionFactoryImplementor sf = factory.unwrap(SessionFactoryImplementor.class);
        SelectStatement ast = sf.getQueryEngine().getSqmTranslatorFactory()
                .createSelectTranslator(
                        (SqmSelectStatement<?>) query,
                        QueryOptions.NONE,
                        DomainParameterXref.EMPTY,
                        QueryParameterBindings.empty(),
                        new LoadQueryInfluencers(sf),
                        sqlAstCreationContext(sf),
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
        if (!onTheRendererThatWroteTheAsset() && RENDERING_DIFFERS_ON_HIBERNATE_7.contains(action)) {
            // On the forward-compatibility leg a listed shape is asserted to DIFFER from the asset
            // (divergesFromTheAssetOnExactlyTheShapesTheListNames pins the set and its shape); a
            // byte match here would mean the list is stale in the other direction.
            assertNotEquals(recorded.get(action), expectationOf(statements),
                    () -> "'" + action + "' is listed as diverging on Hibernate "
                            + org.hibernate.Version.getVersionString()
                            + " but renders byte-identically; shrink the list deliberately");
            return;
        }
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
                "action '" + action + "' was rejected for a reason actions.json does not declare: "
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

        // Update these tripwires only after replaying new actions against the oracle.
        assertEquals(
                Map.of("conditional", 231, "unconditional", 3, "throwing", 67),
                Map.of("conditional", conditionalActions().size(),
                        "unconditional", unconditionalActions().size(),
                        "throwing", THROWING.size()));
    }

    /** Actions whose emitted statement carries no filter at all, on any dialect. */
    private static List<String> unconditionalActions() {
        return recordedActions.stream()
                .filter(action -> emitted.get(action).values().stream()
                        .allMatch(statement -> split(statement).where() == null))
                .toList();
    }

    private static List<String> conditionalActions() {
        List<String> unconditional = unconditionalActions();
        return recordedActions.stream().filter(a -> !unconditional.contains(a)).toList();
    }

    @Test
    void theUnconditionalActionIsThePlannerFoldTheCorpusDeclares() {
        // `p-has` is the corpus's one knownDivergences entry: the planner folds has() on a
        // missing attribute to ALWAYS_ALLOWED while check() denies those rows. The adapter must
        // translate that faithfully — an unfiltered SELECT — and this is the assertion that says
        // the empty WHERE belongs to that shape rather than to a translation that quietly stopped
        // emitting a filter.
        assertEquals(List.of("p-has", "pv-empty-all", "pv-empty-not-exists"), unconditionalActions());
        assertTrue(ACTIONS.skippedDivergences(Corpus.ADAPTER).contains("p-has"));
    }

    // -- the renderer, and the other one -------------------------------------------------------

    /**
     * The Hibernate major the forward-compatibility leg runs: the one after the major the asset
     * was rendered under, by definition, so it is derived from {@link Corpus#HIBERNATE_MINOR}
     * rather than declared a second time. {@code build.gradle.kts} selects the leg
     * ({@code ADAPTER_TEST_ORM=next}) and forwards the choice as {@code adapter.test.orm}.
     */
    private static final int NEXT_HIBERNATE_MAJOR =
            Integer.parseInt(Corpus.HIBERNATE_MINOR.substring(0, Corpus.HIBERNATE_MINOR.indexOf('.')))
                    + 1;

    /**
     * Corpus actions Hibernate 7 renders differently from the 6.6 the asset is generated under,
     * from the SAME Criteria tree — which is why they are pinned as a list rather than as a
     * second asset.
     *
     * <p>ONE renderer change accounts for every one of them, and it is asserted below rather than
     * left to this comment: Hibernate 7's {@code MySQLDialect} renders a boolean literal as
     * {@code true}/{@code false} where 6.6 rendered {@code 1}/{@code 0}, so every action whose
     * MySQL statement compares a boolean column diverges and nothing else does. H2 and PostgreSQL
     * render every recorded shape byte-identically on both majors. It is not a translation
     * decision: {@link AdversarialConformanceTest} runs the same corpus against a real PDP on both
     * majors, and every one of these actions is an oracle comparison there.
     *
     * <p>The list is asserted in BOTH directions, so an action that stops diverging fails just as
     * loudly as one that starts: a shrinking list is a renderer change worth knowing about, and a
     * growing one lands here rather than silently widening an exemption.
     */
    static final List<String> RENDERING_DIFFERS_ON_HIBERNATE_7 = List.of(
            "double-negation",
            "lambda-ternary",
            "nan-ord-inf",
            "nan-ord-le",
            "nan-ord-ternary",
            "nan-ord-ternary-vf",
            "nary-and",
            "not-and",
            "not-nan-ord-le",
            "not-nan-order-string",
            "not-ternary-parent",
            "or-eq-exists",
            "or-eq-in",
            "outer-attr-depth2",
            "p-deep-nest",
            "p-ternary-in-exists",
            "p-ternary-of-ternaries",
            "p-ternary-under-all",
            "p-ternary-vs-ternary",
            "rel-bool-hop",
            "rel-bool-hop2",
            "rel-hop-and-root",
            "rel-hop2-or-exists",
            "rel-not-bool-hop",
            "root-bare-bool",
            "root-not-bool",
            "root-or",
            "ternary-bare",
            "ternary-cmp",
            "ternary-negated",
            "ternary-nested",
            "ternary-value-first",
            "triple-negation",
            "w1-ternary-chain-cond");

    /** True on the leg the asset was generated under; false on the forward-compatibility leg. */
    static boolean onTheRendererThatWroteTheAsset() {
        return org.hibernate.Version.getVersionString().startsWith(Corpus.HIBERNATE_MINOR + ".");
    }

    /**
     * The asset is one renderer's rendering of the adapter's Criteria trees, so it records which
     * one — {@code conformance/README.md}, "When the generator is an input". CI runs this suite
     * under two Hibernate majors ({@code build.gradle.kts}, {@code ADAPTER_TEST_ORM}): the one the
     * asset was rendered under, where every recorded byte is asserted, and the next one, where
     * {@link #RENDERING_DIFFERS_ON_HIBERNATE_7} is asserted instead. Which leg this is comes from
     * the build, not the classpath: a resolution that quietly drifted to a third major would
     * otherwise read as whichever leg it happened to match, and a dependency bump that makes the
     * recorded bytes somebody else's fails here before it fails on the shapes.
     */
    @Test
    void theAssetDeclaresTheRendererThatWroteIt() throws Exception {
        assertEquals(Corpus.HIBERNATE_MINOR,
                JSON.readTree(Corpus.goldenFile().toFile()).get("hibernate").asText());
        String running = org.hibernate.Version.getVersionString();
        String selected = System.getProperty("adapter.test.orm", "baseline");
        switch (selected) {
            case "baseline" -> assertTrue(running.startsWith(Corpus.HIBERNATE_MINOR + "."),
                    () -> "golden/expectations.json was rendered by Hibernate "
                            + Corpus.HIBERNATE_MINOR + " and the baseline build runs " + running
                            + ": re-record the asset deliberately rather than editing the header");
            case "next" -> assertTrue(running.startsWith(NEXT_HIBERNATE_MAJOR + "."),
                    () -> "the `next` ORM set must resolve Hibernate " + NEXT_HIBERNATE_MAJOR
                            + ".x, the major after the " + Corpus.HIBERNATE_MINOR
                            + " the asset declares; this build runs " + running);
            default -> throw new AssertionError(
                    "adapter.test.orm must be `baseline` or `next`, got `" + selected + "`");
        }
        assertEquals(selected.equals("baseline"), onTheRendererThatWroteTheAsset());
    }

    /**
     * The other half of the divergence list. On the leg the asset was NOT generated under, every
     * action outside the list must still render byte-identically — otherwise the list is stale in
     * the other direction and that leg is proving nothing about the emitted SQL — and every action
     * inside it must differ, in the one way the list's javadoc claims.
     */
    @Test
    void divergesFromTheAssetOnExactlyTheShapesTheListNames() {
        // A name in the list that is not a recorded action can never fire, on either leg. Checked
        // before the leg split because the baseline is the leg that runs on every push.
        assertTrue(recordedActions.containsAll(RENDERING_DIFFERS_ON_HIBERNATE_7),
                () -> "not recorded actions: " + RENDERING_DIFFERS_ON_HIBERNATE_7.stream()
                        .filter(action -> !recordedActions.contains(action)).toList());
        assertEquals(new ArrayList<>(new TreeSet<>(RENDERING_DIFFERS_ON_HIBERNATE_7)),
                RENDERING_DIFFERS_ON_HIBERNATE_7,
                "the divergence list must stay sorted and free of duplicates");
        // The reason the list is allowed to be a list rather than a second pinned asset: an entry
        // on it is a shape whose ROWS the harness proves against check() on both majors, so what
        // the bytes do not cover, the oracle does. Runs on both legs, since the claim is about the
        // list rather than about either renderer.
        Set<String> oracle = Corpus.oracleActions(ACTIONS, Corpus.ADAPTER)
                .collect(java.util.stream.Collectors.toSet());
        assertEquals(List.of(), RENDERING_DIFFERS_ON_HIBERNATE_7.stream()
                .filter(action -> !oracle.contains(action)).toList(),
                "every shape the renderers disagree on must still be an oracle comparison");

        org.junit.jupiter.api.Assumptions.assumeFalse(onTheRendererThatWroteTheAsset(),
                "the divergence set is empty on the renderer the asset was generated under");

        List<String> diverging = recordedActions.stream()
                .filter(action -> !recorded.get(action).equals(expectationOf(emitted.get(action))))
                .toList();
        assertEquals(RENDERING_DIFFERS_ON_HIBERNATE_7, diverging,
                "Hibernate " + org.hibernate.Version.getVersionString() + " diverges from the asset"
                        + " on a different set of shapes than the list pins");

        // The characterisation, not just the membership: MySQL differs, H2 and PostgreSQL do not.
        // A listed shape whose H2 or PostgreSQL rendering moved is a second renderer change, and
        // it has to be triaged into the javadoc above rather than absorbed by the list.
        for (String action : RENDERING_DIFFERS_ON_HIBERNATE_7) {
            ObjectNode pinned = recorded.get(action);
            ObjectNode rendered = expectationOf(emitted.get(action));
            for (String dialect : List.of("h2", "postgresql")) {
                assertEquals(pinned.path("joins").path(dialect), rendered.path("joins").path(dialect),
                        action + " (" + dialect + " joins)");
                assertEquals(pinned.path("where").path(dialect), rendered.path("where").path(dialect),
                        action + " (" + dialect + " where)");
            }
            assertEquals(pinned.path("joins").path("mysql"), rendered.path("joins").path("mysql"),
                    action + " (mysql joins)");
            assertFalse(pinned.path("where").path("mysql").equals(rendered.path("where").path("mysql")),
                    action + " (mysql where) no longer diverges; shrink the list deliberately");
        }
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
     * Where in the walk each rejection happens, and how many corpus shapes reach each site.
     *
     * <p>{@code actions.json} pins a substring of the message per action, so the throw suite above
     * proves every refusal is the declared one. It cannot say anything about the SHAPE of the
     * refusals taken together: whether the 21 shapes this reference refuses land on a dozen
     * distinct mechanisms or on one catch-all, and whether a translator change moved a shape from
     * one to another. That is a property no corpus action can state, because a corpus action asks
     * which rows come back.
     *
     * <p>Three things are asserted. <strong>Total</strong> — every refusal matches a site this
     * adapter actually has, so a shape rejected by an accident cannot pass as a declared
     * limitation, which is the #326 trap at corpus scale. <strong>Pinned counts</strong> — a
     * translator change that moves a shape from one site to another shows up as a diff even though
     * both sites throw and {@code actions.json} is unchanged; a later split of the translator
     * ({@code SpringDataQueryPlanAdapter} is one file today) has this table to prove it moved
     * nothing. <strong>No unmapped field</strong> — {@code Scope}'s "Unknown attribute" and
     * "Cannot resolve" family is not a limitation of the Criteria API at all, it is this suite's
     * own mapping coming up short, and it is the exact accident #326 was filed for.
     */
    @Nested
    class WhereTheRefusalsHappen {

        /**
         * One entry per {@code throw} site the corpus reaches, named for the mechanism rather
         * than for the message. The substrings are the ones {@code actions.json} pins, narrowed
         * to the part that identifies the site rather than the action.
         */
        private final Map<String, String> sites = Map.ofEntries(
                Map.entry("two-list difference", "except is not supported:"),
                Map.entry("computed macro collection", "exists first operand must be a variable"),
                Map.entry("literal exists-one", "exists_one over a literal collection value"),
                Map.entry("computed filter size", "Unsupported size(filter(...)) expression"),
                Map.entry("computed membership", "Unsupported in operand combination:"),
                Map.entry("bare temporal comparison", "Bare temporal comparison cannot preserve"),
                Map.entry("whole-list comparison", "comparison against a list"),
                Map.entry("computed intersection", "Unsupported hasIntersection operand shape:"),

                // leafOperandError: the operand slot of a comparison holds a computed
                // sub-expression the resolver has no case for — a cast, a positional read, a
                // struct member access, a lambda. A Criteria predicate compares a path against a
                // literal, another path, or the arithmetic and ternary forms the resolver does
                // lower; everything else is Opaque and refused here by the operator it sits in.
                Map.entry("computed leaf operand", " expression in leaf operand of "),
                // The operator dispatch's default: an operator the reference never translates.
                // The corpus reaches it through matches() alone — regular expressions have no
                // dialect-independent SQL form.
                Map.entry("operator the reference never translates", "Unsupported operator: "),
                // filter() at the root of the condition or one conjunct below it: a list where a
                // boolean is required, refused by name before any predicate is built (#387).
                Map.entry("filter() in boolean position", "filter() returns a list, not a boolean"),
                // resolveNumericOperand: CEL's `+` over strings arrives as the same `add` node as
                // numeric addition, and the reference lowers `add` as arithmetic only, so a
                // string operand — a constant, the primary key, or a second column — is refused
                // rather than concatenated (#376, #391).
                Map.entry("non-numeric arithmetic operand",
                        "Arithmetic comparison requires numeric operands"),
                // Arithmetic composed on top of a division whose denominator may be zero: CEL
                // carries the NaN or infinity through the outer operation and SQL has no value
                // that does (#311).
                Map.entry("division inside further arithmetic",
                        "arithmetic composed on a division whose denominator may be zero"),
                // CEL `%` is integer-only while attribute values are doubles, and the int() cast
                // that would make it satisfiable has no faithful lowering.
                Map.entry("modulo", "mod is not supported in comparisons"),
                // map() translates only as the collection operand of hasIntersection; compared
                // directly to a value it is a whole-list equality no scalar column can answer.
                Map.entry("map projection compared directly",
                        "Direct comparison of map(...) to a value is not supported"),
                // HierarchyTranslator: an empty delimiter splits the path per character, and the
                // prefix LIKE this adapter emits would then match the path itself.
                Map.entry("empty hierarchy delimiter",
                        "hierarchy delimiter must be a non-empty string"),
                // Two columns under different null conventions: the omitted side is UNKNOWN for
                // a NULL column and the explicit side is definite, and no single predicate is
                // both (#308).
                Map.entry("mixed null conventions across two columns",
                        "between two columns under mixed null conventions"),
                // timestamp() over a column whose Java type does not denote an absolute instant:
                // the adapter would have to guess a zone to compare it.
                Map.entry("ambiguous temporal column",
                        "timestamp() comparison requires a column mapped to java.time.Instant"));

        private String siteOf(String action) {
            String raised;
            try {
                statementOf("h2", specificationFor(action));
                return "<did not throw>";
            } catch (IllegalArgumentException error) {
                raised = String.valueOf(error.getMessage());
            }
            String message = raised;
            List<String> matched = sites.entrySet().stream()
                    .filter(site -> message.contains(site.getValue()))
                    .map(Map.Entry::getKey)
                    .toList();
            assertEquals(1, matched.size(),
                    () -> action + " is refused with \"" + message + "\", which matches "
                            + matched.size() + " of this adapter's known rejection sites");
            return matched.get(0);
        }

        @Test
        void everyRefusedShapeLandsOnExactlyOneOfThemInTheseNumbers() {
            Map<String, Integer> counts = new TreeMap<>();
            for (String action : THROWING.keySet()) {
                counts.merge(siteOf(action), 1, Integer::sum);
            }

            assertEquals(new TreeMap<>(Map.ofEntries(
                            Map.entry("two-list difference", 4),
                            Map.entry("computed macro collection", 2),
                            Map.entry("literal exists-one", 1),
                            Map.entry("computed filter size", 1),
                            Map.entry("computed membership", 2),
                            Map.entry("bare temporal comparison", 1),
                            Map.entry("whole-list comparison", 2),
                            Map.entry("computed intersection", 1),

                            Map.entry("computed leaf operand", 27),
                            Map.entry("operator the reference never translates", 14),
                            Map.entry("filter() in boolean position", 2),
                            Map.entry("non-numeric arithmetic operand", 2),
                            Map.entry("division inside further arithmetic", 2),
                            Map.entry("modulo", 1),
                            Map.entry("map projection compared directly", 1),
                            Map.entry("empty hierarchy delimiter", 1),
                            Map.entry("mixed null conventions across two columns", 1),
                            Map.entry("ambiguous temporal column", 2))),
                    counts);
            assertEquals(THROWING.size(),
                    counts.values().stream().mapToInt(Integer::intValue).sum());
        }

        /**
         * The substrings raised when the MAPPING or the DATA, not the plan shape, is what fell
         * short: a reference the mapping does not name, a scalar reference to a Relation (both
         * from {@code Scope}), and a struct element of a literal collection that lacks the field
         * a lambda reads. None of them is a limitation of the Criteria API, so none may be the
         * reason a corpus shape is refused.
         */
        private static final List<String> MAPPING_SHORTFALLS =
                List.of("Unknown attribute", "cannot resolve as a scalar path", "Cannot resolve");

        /**
         * The #326 assertion, stated over the whole corpus. An unmapped field makes an action throw
         * from {@code Scope} — which is the mapping coming up short, not a limitation of the
         * Criteria API — and on elasticsearch-java it once let six actions pass the throw suite
         * while never reaching the mechanism their {@code actions.json} reasons claim.
         */
        @Test
        void noRefusalIsTheMappingComingUpShort() {
            List<String> unmapped = new ArrayList<>();
            for (String action : THROWING.keySet()) {
                try {
                    statementOf("h2", specificationFor(action));
                } catch (IllegalArgumentException error) {
                    String message = String.valueOf(error.getMessage());
                    if (MAPPING_SHORTFALLS.stream().anyMatch(message::contains)) {
                        unmapped.add(action + ": " + message);
                    }
                }
            }
            assertEquals(List.of(), unmapped);

            // Anti-vacuity: the detector must recognise the messages it is looking for, built
            // here rather than hoped for — the corpus mapping with one entry removed, and a bare
            // boolean whose attribute is redirected at a Relation (an equality against one would
            // translate as membership instead). The third substring needs a literal collection
            // of struct elements, which no wire fixture carries; it is pinned by
            // SpringDataQueryPlanAdapterTest.missingElementFieldFailsClosed.
            assertTrue(refusal("cs-eq", Map.of()).contains("Unknown attribute"));
            assertTrue(refusal("root-bare-bool", Map.of("request.resource.attr.aBool",
                            AttributeMapping.relation("tags")))
                    .contains("cannot resolve as a scalar path"));
        }

        private String refusal(String action, Map<String, AttributeMapping> mapping) {
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> statementOf("h2", specificationFor(action, mapping,
                            NullAttributeRepresentation.EXPLICIT, Corpus.PLANNED_AT)));
            return String.valueOf(ex.getMessage());
        }
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
                if (!onTheRendererThatWroteTheAsset()
                        && RENDERING_DIFFERS_ON_HIBERNATE_7.contains(action)) {
                    // The asset holds the 6.6 rendering; on the other major a listed shape
                    // reassembles into the statement 6.6 emitted, not this one. The invariant is
                    // still asserted for every shape the two renderers agree on.
                    continue;
                }
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
                    if (statement.replaceAll("'([^']|'')*'", "").contains("?")) {
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
     * The corpus's {@code nullRepresentationOmitted} probe, which has no store in it at all.
     *
     * <p>{@code null-eq-missing} compares {@code aOptionalString == null}, and the planner emits
     * the same {@code eq(attr, null)} node whichever convention the caller uses — so the adapter
     * has to be TOLD, and what it does when it is told is a pure translator property. The harness
     * asserts the same pair against a live PDP and proves the over-grant with real rows; here it
     * costs a millisecond and pins the SQL each option produces.
     */
    @Nested
    class NullAttributeRepresentationOption {

        private final NullRepresentationOmitted probe =
                Corpus.nullRepresentationThrows(ACTIONS).get(0);

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
