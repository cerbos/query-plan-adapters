/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

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
 * Translator unit test: for every corpus action, the SQL this adapter emits on H2, PostgreSQL and
 * MySQL, asserted against {@code golden/expectations.json}. Plans come from
 * {@code conformance/wire-fixtures/}. Offline: no PDP, no Docker, no database connection.
 *
 * <p>Regenerate the asset with {@code ./gradlew goldenUpdate}; see {@code conformance/README.md},
 * "Golden expectations".
 */
class SpringDataTranslatorTest {

    /**
     * The dialects the asset records, each at the server version its CI leg runs. Without a
     * version Hibernate assumes the dialect's minimum, and
     * {@link MySqlDoubleCastFunctionContributor} would not register for MySQL.
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
     * Actions this adapter must refuse, mapped to the message actions.json pins. They get no
     * golden entry.
     */
    private static final Map<String, String> THROWING =
            Corpus.throwingActions(ACTIONS, Corpus.ADAPTER);

    private static final Map<String, EntityManagerFactory> FACTORIES = new LinkedHashMap<>();

    private static Map<String, ObjectNode> recorded;
    private static List<String> recordedActions;

    /**
     * Every statement, rendered once per action and dialect, and read by the golden comparison,
     * the rules below and regeneration.
     */
    private static Map<String, Map<String, String>> emitted;

    @BeforeAll
    static void setUp() {
        DIALECTS.forEach((name, dialect) -> FACTORIES.put(name,
                Persistence.createEntityManagerFactory(
                        "translator-pu", Map.of("hibernate.dialect", dialect))));

        emitted = new LinkedHashMap<>();
        for (String action : Corpus.wireFixtureActions()) {
            // Refused actions are not rendered; the throw test covers them.
            if (!THROWING.containsKey(action)) {
                emitted.put(action, statementsFor(action));
            }
        }

        // `./gradlew goldenUpdate` sets golden.update and rewrites the asset from what the
        // translator emits now. CI never sets it.
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
     * The golden entry for one action. {@code joins} appears only when a root join is emitted:
     * whether that join is LEFT or INNER decides whether a row with an absent parent survives a
     * disjunction (#375).
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
     * Renders a Specification into the SQL a repository would run, using the
     * {@code select distinct id} query {@link AdversarialConformanceTest} runs. Literals are
     * inlined ({@code hibernate.criteria.value_handling_mode} in {@code translator-pu}) so the
     * asset records operands rather than {@code ?}.
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
     * On Hibernate 6.6 the SessionFactoryImplementor is the SqlAstCreationContext; Hibernate 7
     * moved it to {@code getSqlTranslationEngine()}. Resolved reflectively so this compiles
     * against both.
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

    /** Splits a statement into root joins and WHERE clause; either is null when absent. */
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
     * Asserts the message too: a bare throw would also pass for a mapping typo or an unrelated
     * validation (#326).
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

    /** A throwing action with no pinned message must fail classification (#326). */
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

        // Total: every wire fixture has a golden entry or a pinned throw.
        assertEquals(Corpus.wireFixtureActions(), classified,
                "every wire fixture must be accounted for exactly once");
        // Disjoint: never both.
        assertEquals(classified.size(), Set.copyOf(classified).size(),
                "an action is either recorded or thrown, never both");
        // Sorted, so a diff lists the shapes that moved.
        assertEquals(new ArrayList<>(new TreeSet<>(recordedActions)), recordedActions,
                "golden/expectations.json must stay sorted by action");

        // Update these tripwires only after replaying new actions against the oracle.
        assertEquals(
                Map.of("conditional", 264, "unconditional", 3, "throwing", 66),
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
        // `p-has` is the planner's fold to ALWAYS_ALLOWED (knownDivergences), and the two
        // pv-empty-* actions fold on an empty principal list. An empty WHERE on any other action
        // means the translation stopped emitting a filter.
        assertEquals(List.of("p-has", "pv-empty-all", "pv-empty-not-exists"), unconditionalActions());
        assertTrue(ACTIONS.skippedDivergences(Corpus.ADAPTER).contains("p-has"));
    }

    // -- Hibernate 6.6 and 7 -------------------------------------------------------------------

    /**
     * The Hibernate major of the {@code ADAPTER_TEST_ORM=next} leg: one after
     * {@link Corpus#HIBERNATE_MINOR}.
     */
    private static final int NEXT_HIBERNATE_MAJOR =
            Integer.parseInt(Corpus.HIBERNATE_MINOR.substring(0, Corpus.HIBERNATE_MINOR.indexOf('.')))
                    + 1;

    /**
     * Actions Hibernate 7 renders differently from the 6.6 asset. The only cause is MySQL boolean
     * literals ({@code true}/{@code false} instead of {@code 1}/{@code 0}). The list is asserted
     * in both directions by {@link #divergesFromTheAssetOnExactlyTheShapesTheListNames}.
     */
    static final List<String> RENDERING_DIFFERS_ON_HIBERNATE_7 = List.of(
            "cast-string-bool",
            "compose-allow-deny",
            "compose-derived-deny",
            "compose-derived-role",
            "compose-multi-allow",
            "compose-multi-allow-deny",
            "compose-or-not",
            "compose-two-deny",
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
     * The asset's {@code hibernate} header matches the baseline leg, and {@code adapter.test.orm}
     * resolves the expected major. The leg comes from the build rather than the classpath, so a
     * drift to a third major fails here.
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
     * On the {@code next} leg, exactly the listed actions differ from the asset, and only in the
     * MySQL WHERE clause.
     */
    @Test
    void divergesFromTheAssetOnExactlyTheShapesTheListNames() {
        // Checked on both legs: every listed name must be a recorded action.
        assertTrue(recordedActions.containsAll(RENDERING_DIFFERS_ON_HIBERNATE_7),
                () -> "not recorded actions: " + RENDERING_DIFFERS_ON_HIBERNATE_7.stream()
                        .filter(action -> !recordedActions.contains(action)).toList());
        assertEquals(new ArrayList<>(new TreeSet<>(RENDERING_DIFFERS_ON_HIBERNATE_7)),
                RENDERING_DIFFERS_ON_HIBERNATE_7,
                "the divergence list must stay sorted and free of duplicates");
        // Listed shapes must stay oracle-compared, so the harness covers their rows on both
        // majors.
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

        // Only MySQL may differ; any other change is a second renderer change to triage.
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
     * {@code translator-pu} declares no JDBC connection. With one, this suite would quietly start
     * needing a database.
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
        // The sibling units do declare one, so the check above is not vacuous.
        assertTrue(persistenceXml.contains("jakarta.persistence.jdbc.url"));
    }

    /** The regeneration command the asset names must be a task this build defines. */
    @Test
    void theAssetNamesACommandThisBuildDefines() throws Exception {
        String[] parts = Corpus.GOLDEN_REGENERATE_COMMAND.split(" ");
        assertEquals("./gradlew", parts[0]);
        assertTrue(Files.readString(java.nio.file.Path.of(
                        System.getProperty("user.dir"), "build.gradle.kts"))
                .contains("tasks.register<Test>(\"" + parts[1] + "\")"),
                () -> "build.gradle.kts defines no task named " + parts[1]);
    }

    /**
     * Maps every refusal to the throw site that raised it and pins the count per site, so a
     * change that moves a shape between sites shows up even when {@code actions.json} does not
     * change. No refusal may come from a mapping gap (#326).
     */
    @Nested
    class WhereTheRefusalsHappen {

        /** One entry per throw site, keyed by mechanism, valued by a message substring. */
        private final Map<String, String> sites = Map.ofEntries(
                Map.entry("two-list difference", "except is not supported:"),
                Map.entry("computed macro collection", "exists first operand must be a variable"),
                Map.entry("literal exists-one", "exists_one over a literal collection value"),
                Map.entry("computed filter size", "Unsupported size(filter(...)) expression"),
                Map.entry("computed membership", "Unsupported in operand combination:"),
                Map.entry("bare temporal comparison", "Bare temporal comparison cannot preserve"),
                Map.entry("whole-list comparison", "comparison against a list"),
                Map.entry("computed intersection", "Unsupported hasIntersection operand shape:"),

                // ComparisonTranslator.leafOperandError: an operand the resolver cannot lower,
                // such as a cast, a positional read, a struct access or a lambda.
                Map.entry("computed leaf operand", " expression in leaf operand of "),
                // The operator dispatch's default; the corpus reaches it only through matches().
                Map.entry("operator the reference never translates", "Unsupported operator: "),
                // filter() used where a boolean is required (#387).
                Map.entry("filter() in boolean position", "filter() returns a list, not a boolean"),
                // CEL's string `+` arrives as `add`, which this adapter lowers as arithmetic only
                // (#376, #391).
                Map.entry("non-numeric arithmetic operand",
                        "Arithmetic comparison requires numeric operands"),
                // CEL carries NaN or infinity through the outer arithmetic; SQL cannot (#311).
                Map.entry("division inside further arithmetic",
                        "arithmetic composed on a division whose denominator may be zero"),
                // CEL `%` is integer-only, and int() has no faithful lowering.
                Map.entry("modulo", "mod is not supported in comparisons"),
                // map() translates only as the collection operand of hasIntersection.
                Map.entry("map projection compared directly",
                        "Direct comparison of map(...) to a value is not supported"),
                // An empty delimiter would make the prefix LIKE match the path itself.
                Map.entry("empty hierarchy delimiter",
                        "hierarchy delimiter must be a non-empty string"),
                // The omitted side is UNKNOWN for NULL and the explicit side is definite; no one
                // predicate is both (#308).
                Map.entry("mixed null conventions across two columns",
                        "between two columns under mixed null conventions"),
                // The adapter would have to guess a time zone.
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

                            Map.entry("computed leaf operand", 26),
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
         * Messages raised when the mapping or the data falls short rather than the Criteria API:
         * an unmapped attribute, a scalar reference to a Relation, and a literal struct element
         * missing the field a lambda reads.
         */
        private static final List<String> MAPPING_SHORTFALLS =
                List.of("Unknown attribute", "cannot resolve as a scalar path", "Cannot resolve");

        /** No corpus refusal may come from a mapping gap (#326). */
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

            // Anti-vacuity: provoke the first two messages. The third needs a literal
            // collection of structs, which no wire fixture carries.
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
     * Rules over what the translator emits now, for every translated action, each with an
     * anti-vacuity check. They catch what a regenerated asset committed unread would not.
     */
    @Nested
    class WhatTheEmittedStatementContains {

        @Test
        void everyStatementIsTheCorpusSelectPlusItsJoinsAndFilter() {
            // The asset records only what follows the preamble, which is the harness's query.
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
            // Every entry reassembles into the emitted statement, so recording the tail is
            // lossless.
            for (String action : recordedActions) {
                if (!onTheRendererThatWroteTheAsset()
                        && RENDERING_DIFFERS_ON_HIBERNATE_7.contains(action)) {
                    // The asset holds the 6.6 rendering of listed shapes.
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
            // Operands must be inlined; a bound `?` would drop them from the asset.
            List<String> offenders = new ArrayList<>();
            for (String action : recordedActions) {
                emitted.get(action).forEach((dialect, statement) -> {
                    if (statement.replaceAll("'([^']|'')*'", "").contains("?")) {
                        offenders.add(action + " (" + dialect + "): " + statement);
                    }
                });
            }
            assertEquals(List.of(), offenders);
            // Anti-vacuity: statements do carry operands.
            assertTrue(emitted.get("cs-eq").get("h2").contains("'one'"),
                    emitted.get("cs-eq").get("h2"));
        }

        @Test
        void everyLikeCarriesANonEmptyEscapeClause() {
            // An unescaped `%` or `_` turns equality into a wildcard match (#258, #259). Every
            // LIKE needs a non-empty ESCAPE: Hibernate renders two-argument cb.like(...) as
            // `escape ''`, which declares nothing.
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
            // Anti-vacuity: a LIKE is emitted, and its escape character is exercised.
            assertTrue(withLike > 0);
            assertTrue(emitted.get("like-percent").get("h2").contains("'100\\%%' escape '\\'"),
                    emitted.get("like-percent").get("h2"));
        }

        @Test
        void theResourceTableIsNamedInExactlyOneFromClause() {
            // A subquery that lost its correlation names the outer table in its own FROM and
            // compares against every row of it.
            List<String> offenders = new ArrayList<>();
            for (String action : recordedActions) {
                emitted.get(action).forEach((dialect, statement) -> {
                    if (fromClausesNamingTheResource(statement) != 1) {
                        offenders.add(action + " (" + dialect + "): " + statement);
                    }
                });
            }
            assertEquals(List.of(), offenders);

            // Anti-vacuity: these actions emit subqueries...
            for (String action : List.of("w1-all-chain", "exists-on-empty", "size-filter-count")) {
                assertTrue(emitted.get(action).get("h2").contains("(select "),
                        () -> action + ": " + emitted.get(action).get("h2"));
            }
            // ...and the detector catches the broken rendering.
            assertEquals(2, fromClausesNamingTheResource(uncorrelatedRendering()));
            // The pinned fresh range variable the exemption above admits is exercised by the
            // corpus, and only there: without the pin it would count.
            String nested = emitted.get("nest-same-exists").get("h2");
            assertTrue(nested.contains("from resources re2_0 join tags")
                    && nested.contains("re2_0.id=re1_0.id"), nested);
            // One pinned root per polarity the body is translated in, each counted once unpinned.
            assertEquals(3, fromClausesNamingTheResource(
                    nested.replaceAll("re\\d+_0\\.id=re1_0\\.id", "")));
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
            // Every qualified column must be one the JPA model declares.
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
            // Anti-vacuity: the column set is populated...
            assertTrue(declared.contains("a_optional_string"), declared.toString());
            // ...and the detector rejects an undeclared column.
            Matcher stranger = qualified.matcher("re1_0.no_such_column='x'");
            assertTrue(stranger.find(), "the detector matches no qualified identifier at all");
            assertFalse(declared.contains(stranger.group(1)), stranger.group(1));
        }

        @Test
        void everyRootJoinIsALeftJoin() {
            // #375. A dotted jpaPath through a to-one association is joined at the root. An
            // INNER join would drop a row with an absent parent even when another OR branch
            // allows it.
            List<String> offenders = new ArrayList<>();
            int withJoins = 0;
            for (String action : recordedActions) {
                for (Map.Entry<String, String> e : emitted.get(action).entrySet()) {
                    String joins = split(e.getValue()).joins();
                    if (joins == null) {
                        continue;
                    }
                    withJoins++;
                    // An INNER join renders as a bare `join`, a cross join as `cross join`.
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
            // MySQLDialect casts to decimal(53,20), which makes double arithmetic exact;
            // MySqlDoubleCastFunctionContributor renders cast(x as double) instead. This also
            // fails if the dialect versions in DIALECTS are dropped.
            assertTrue(emitted.get("p-double-frac").get("mysql").contains("as double)"),
                    emitted.get("p-double-frac").get("mysql"));
            assertTrue(emitted.get("p-double-frac").get("h2").contains("as float(53))"),
                    emitted.get("p-double-frac").get("h2"));
        }

        @Test
        void theFoldedNowLiteralKeepsThePrecisionThePdpEmits() {
            // `now() - duration("24h")` changes on every capture, so the fixture holds a
            // placeholder and Corpus.PLANNED_AT fills it at nanosecond precision, as the PDP does.
            assertTrue(emitted.get("ts-window").get("h2").contains(".123456789"),
                    emitted.get("ts-window").get("h2"));
            // PostgreSQL truncates to microseconds.
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
         * Counts the FROM lists naming the resource table; an uncorrelated subquery adds one. A
         * reference pinned to the outer row by identity ({@code re2_0.id=re1_0.id}) is the fresh
         * range variable a macro nested over the same relation takes (#509), so it is not counted.
         */
        private int fromClausesNamingTheResource(String statement) {
            int count = 0;
            Matcher m = Pattern.compile("from ([a-z_]+ [a-z]+\\d*_\\d+(?:,[a-z_]+ [a-z]+\\d*_\\d+)*)")
                    .matcher(statement);
            while (m.find()) {
                for (String reference : m.group(1).split(",")) {
                    String trimmed = reference.trim();
                    if (trimmed.startsWith("resources ")
                            && !statement.contains(trimmed.substring("resources ".length())
                                    + ".id=re1_0.id")) {
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
                // The identifier is not part of the attribute walk.
                descriptor.getIdentifierMapping().forEachSelectable((index, selectable) ->
                        columns.add(selectable.getSelectionExpression()));
            });
            sf.getMappingMetamodel().forEachCollectionDescriptor(descriptor -> {
                descriptor.getAttributeMapping().forEachSelectable((index, selectable) ->
                        columns.add(selectable.getSelectionExpression()));
                // Collection key columns belong to no entity's attribute walk.
                descriptor.getAttributeMapping().getKeyDescriptor()
                        .forEachSelectable((index, selectable) ->
                                columns.add(selectable.getSelectionExpression()));
            });
            return columns;
        }
    }

    /**
     * The {@code nullRepresentationOmitted} probe, offline. The planner emits the same
     * {@code eq(attr, null)} under either convention, so the caller's option decides the SQL.
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
            // check() denies every row on a missing attribute, while IS NULL returns rows (#302).
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> specificationFor(probe.action(), Corpus.MAPPING,
                            NullAttributeRepresentation.OMITTED, Corpus.PLANNED_AT));
            assertTrue(ex.getMessage().contains(
                    Corpus.nullOmittedMessage(probe, Corpus.ADAPTER)), ex.getMessage());
        }

        @Test
        void aPerAttributeDeclarationOverridesTheCallLevelOption() {
            // #308. `owner` declares EXPLICIT, so `null-eq` still translates under a call-level
            // OMITTED...
            assertEquals(statementOf("h2", specificationFor("null-eq")),
                    statementOf("h2", specificationFor("null-eq", Corpus.MAPPING,
                            NullAttributeRepresentation.OMITTED, Corpus.PLANNED_AT)));

            // ...and without the declaration the same call is refused.
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> specificationFor("null-eq", Corpus.MAPPING_WITHOUT_NULL_CONVENTIONS,
                            NullAttributeRepresentation.OMITTED, Corpus.PLANNED_AT));
            assertTrue(ex.getMessage().contains("null operand"), ex.getMessage());
        }
    }
}
