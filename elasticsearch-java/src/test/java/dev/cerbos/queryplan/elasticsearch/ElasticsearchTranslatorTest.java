/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import dev.cerbos.queryplan.elasticsearch.Corpus.ActionsFile;
import dev.cerbos.queryplan.elasticsearch.Corpus.NullRepresentationOmitted;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Result;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Translator unit test: checks the Query DSL this adapter emits for every corpus action against
 * {@code golden/expectations.json}, and that every refused action throws the message
 * {@code conformance/actions.json} pins. Plans come from {@code conformance/wire-fixtures/}.
 * Needs no Docker, PDP or Elasticsearch.
 *
 * <p>Most of the corpus is refused here, because the Query DSL only compares a field against a
 * literal. Every wire fixture must be either a golden entry or a pinned throw, so a new corpus
 * action fails this suite until it is classified.
 */
class ElasticsearchTranslatorTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private static final ActionsFile ACTIONS = Corpus.actionsFile();

    /**
     * Actions this adapter must refuse, with their pinned messages. Includes the
     * {@code nullRepresentationOmitted} probe, which this adapter refuses under either null
     * convention (see {@link #theNullRepresentationProbeIsRefusedRegardless}).
     */
    private static final Map<String, String> THROWING = throwingActions();

    private static Map<String, String> throwingActions() {
        Map<String, String> throwing = new TreeMap<>(Corpus.throwingActions(ACTIONS, Corpus.ADAPTER));
        for (NullRepresentationOmitted probe : Corpus.nullRepresentationThrows(ACTIONS)) {
            throwing.put(probe.action(), Corpus.nullOmittedMessage(probe, Corpus.ADAPTER));
        }
        // Keep the sorted order so parameterised cases are stable; Map.copyOf's order is not.
        return Collections.unmodifiableMap(throwing);
    }

    /** Every emitted query, translated once per action and shared by all tests below. */
    private static Map<String, Result> emitted;

    private static Map<String, ObjectNode> recorded;
    private static List<String> recordedActions;

    @BeforeAll
    static void setUp() {
        emitted = new LinkedHashMap<>();
        for (String action : Corpus.wireFixtureActions()) {
            // Throwing actions are checked by the throw test, not translated here.
            if (!THROWING.containsKey(action)) {
                emitted.put(action, Corpus.translate(action));
            }
        }

        // `./gradlew goldenUpdate` rewrites the file from current output, keeping notes. CI never
        // sets the property. Because throwing actions are skipped above, an action wrongly
        // classified as unsupported fails the throw test, and one wrongly classified as supported
        // fails here during regeneration.
        if (Boolean.getBoolean("golden.update")) {
            Map<String, ObjectNode> expectations = new TreeMap<>();
            emitted.forEach((action, result) -> expectations.put(action, expectationOf(result)));
            Corpus.writeGoldenExpectations(expectations);
            System.out.printf("==> rewrote %s (%d expectations)%n",
                    Corpus.goldenFile(), expectations.size());
        }

        recorded = Corpus.readGoldenExpectations();
        recordedActions = List.copyOf(recorded.keySet());
    }

    // -- the golden value -----------------------------------------------------------------------

    private static final String KIND = "kind";
    private static final String QUERY = "query";

    /**
     * The translator output for one action as the golden file records it: the plan kind, plus the
     * query for a conditional plan. The query is plain JDK maps and lists, so it is recorded
     * as-is with no generator.
     */
    private static ObjectNode expectationOf(Result result) {
        ObjectNode entry = JSON.createObjectNode();
        entry.put(KIND, kindOf(result));
        if (result instanceof Result.Conditional conditional) {
            entry.set(QUERY, Corpus.canonicalJson(conditional.query()));
        }
        return entry;
    }

    private static String kindOf(Result result) {
        if (result instanceof Result.AlwaysAllowed) {
            return "ALWAYS_ALLOWED";
        }
        if (result instanceof Result.AlwaysDenied) {
            return "ALWAYS_DENIED";
        }
        if (result instanceof Result.Conditional) {
            return "CONDITIONAL";
        }
        // Unreachable while `Result` is sealed over three kinds. A new kind must fail here rather
        // than be recorded as CONDITIONAL.
        throw new IllegalStateException("unrecognised plan kind: " + result.getClass());
    }

    /** The query the asset pins for one action; {@code null} on an unconditional plan kind. */
    private static JsonNode recordedQuery(String action) {
        return recorded.get(action).get(QUERY);
    }

    // -- @MethodSource feeds --------------------------------------------------------------------

    static Stream<String> recordedActions() {
        return recordedActions.stream();
    }

    static Stream<Arguments> throwingActionsWithMessages() {
        return THROWING.entrySet().stream().map(e -> Arguments.of(e.getKey(), e.getValue()));
    }

    // -- the corpus, action by action -----------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("recordedActions")
    void emitsTheGoldenExpectation(String action) {
        Result result = emitted.get(action);
        assertNotNull(result, () -> "the asset records '" + action + "', which this adapter "
                + "refuses or the corpus no longer carries — see the completeness guard");
        assertEquals(recorded.get(action), expectationOf(result),
                () -> "the query emitted for '" + action + "' is not the query "
                        + Corpus.goldenFile() + " pins; run `" + Corpus.GOLDEN_REGENERATE_COMMAND
                        + "` and review the diff");
    }

    /**
     * Checks the message, not just the throw, so an unrelated error (for example a mapper typo)
     * cannot pass for the declared limitation.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("throwingActionsWithMessages")
    void isRefusedWithTheMessageActionsJsonPins(String action, String message) {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> Corpus.translate(action));
        assertTrue(ex.getMessage().contains(message),
                "action '" + action + "' was rejected for a reason actions.json does not declare: "
                        + ex.getMessage());
    }

    /** A throwing action with no pinned message fails classification. */
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

        // Every wire fixture has a golden entry or a pinned throw.
        assertEquals(Corpus.wireFixtureActions(), classified,
                "every wire fixture must be accounted for exactly once");
        // An action cannot be both.
        assertEquals(classified.size(), Set.copyOf(classified).size(),
                "an action is either recorded or thrown, never both");
        // Sorted, so a translator change diffs as the list of actions it moved.
        assertEquals(new ArrayList<>(new TreeSet<>(recordedActions)), recordedActions,
                "golden/expectations.json must stay sorted by action");
        // The actions.json manifest names the same set as the fixtures.
        assertEquals(new TreeSet<>(Corpus.wireFixtureActions()), ACTIONS.manifestActions());

        // Update these tripwires only after replaying new actions against the oracle.
        assertEquals(
                Map.of("conditional", 135, "unconditional", 7, "throwing", 191),
                Map.of("conditional", actionsOfKind("CONDITIONAL").size(),
                        "unconditional", unconditionalActions().size(),
                        "throwing", THROWING.size()));
    }

    private static List<String> actionsOfKind(String kind) {
        return recordedActions.stream()
                .filter(action -> kind.equals(recorded.get(action).get(KIND).asText()))
                .toList();
    }

    /** Actions whose plan carries no condition at all, so the adapter emits no query. */
    private static List<String> unconditionalActions() {
        return recordedActions.stream()
                .filter(action -> !recorded.get(action).has(QUERY))
                .toList();
    }

    @Test
    void theUnconditionalActionsAreThePlanKindsTheCorpusDeclares() {
        // An entry with no `query` could also mean a translation that stopped emitting a filter,
        // so the plan kind is recorded and pinned here. `p-has` is a knownDivergences entry: the
        // planner folds it to ALWAYS_ALLOWED while check() denies some rows.
        assertEquals(List.of("in-empty", "p-has", "pv-empty-all", "pv-empty-exists", "pv-empty-not-all",
                "pv-empty-not-exists", "pv-structs-missing"), unconditionalActions());
        assertEquals(List.of("p-has", "pv-empty-all", "pv-empty-not-exists"), actionsOfKind("ALWAYS_ALLOWED"));
        assertTrue(ACTIONS.skippedDivergences(Corpus.ADAPTER).contains("p-has"));
        assertEquals(List.of("in-empty", "pv-empty-exists", "pv-empty-not-all", "pv-structs-missing"), actionsOfKind("ALWAYS_DENIED"));
    }

    /**
     * {@code null-eq-missing} produces the same plan under either null convention, so other
     * adapters need an option to choose one. Elasticsearch does not index an explicit null, so this
     * adapter refuses the probe whether or not the attribute is declared explicit-null.
     */
    @Test
    void theNullRepresentationProbeIsRefusedRegardless() {
        List<NullRepresentationOmitted> probes = Corpus.nullRepresentationThrows(ACTIONS);
        assertEquals(List.of("null-eq-missing"), probes.stream()
                .map(NullRepresentationOmitted::action).toList());
        for (NullRepresentationOmitted probe : probes) {
            String message = Corpus.nullOmittedMessage(probe, Corpus.ADAPTER);
            // Refused with the explicit-null attribute declared and without it.
            IllegalArgumentException declared = assertThrows(IllegalArgumentException.class,
                    () -> Corpus.translate(probe.action()));
            assertTrue(declared.getMessage().contains(message), declared.getMessage());

            IllegalArgumentException undeclared = assertThrows(IllegalArgumentException.class,
                    () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                            Corpus.planFromWireFixture(probe.action()),
                            Corpus.OPTIONS.withExplicitNullAttributes(Set.of())));
            assertTrue(undeclared.getMessage().contains(message), undeclared.getMessage());
        }
    }

    /**
     * {@code ts-window} and {@code ts-vf} compare against a folded {@code now()} that the fixtures
     * store as a placeholder. At the PDP's nanosecond precision they are refused, as
     * {@code actions.json} declares; at millisecond precision they translate. This pins
     * {@link Corpus#PLANNED_AT} to nanoseconds.
     */
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"ts-window", "ts-vf"})
    void theRuntimeTimestampActionsAreRefusedForTheirPrecision(String action) {
        assertTrue(THROWING.containsKey(action), action);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> Corpus.translate(action));
        assertTrue(ex.getMessage().contains(THROWING.get(action)), ex.getMessage());
        assertTrue(ex.getMessage().contains("Sub-millisecond"), ex.getMessage());

        assertInstanceOf(Result.Conditional.class,
                Corpus.translate(Corpus.planFromWireFixture(action, "2026-08-11T09:13:39.123Z")),
                action + " no longer translates at millisecond precision, so Corpus.PLANNED_AT's "
                        + "nanoseconds are not what refuses it");
    }

    /** The golden file names its regenerate command, so that command must exist in the build. */
    @Test
    void theAssetNamesACommandThisBuildDefines() throws Exception {
        String[] parts = Corpus.GOLDEN_REGENERATE_COMMAND.split(" ");
        assertEquals("./gradlew", parts[0]);
        assertTrue(Files.readString(Path.of(System.getProperty("user.dir"), "build.gradle.kts"))
                        .contains("tasks.register<Test>(\"" + parts[1] + "\")"),
                () -> "build.gradle.kts defines no task named " + parts[1]);
    }

    /** This file and the test helpers it uses; none may reach a PDP or a container. */
    private static final List<String> OFFLINE_SOURCES =
            List.of("ElasticsearchTranslatorTest", "Corpus");

    /** The suite stays offline: neither it nor {@link Corpus} imports a PDP or container client. */
    @Test
    void thisSuiteReachesNoPdpAndNoContainer() {
        List<String> forbidden = List.of("org.testcontainers.", "dev.cerbos.sdk.", "java.net.http.");
        // Check import lines, not the whole file, since these names appear here as literals.
        for (String source : OFFLINE_SOURCES) {
            assertEquals(List.of(), importsOf(source).stream()
                            .filter(imported -> forbidden.stream().anyMatch(imported::startsWith))
                            .toList(),
                    source + " reaches a PDP or a container, so this suite is no longer offline");
        }
        // Every sibling this file uses in code must be in OFFLINE_SOURCES.
        assertEquals(Set.of("Corpus"), siblingsReferencedBy("ElasticsearchTranslatorTest"),
                "this suite reaches a test helper OFFLINE_SOURCES does not scan");
        assertEquals(Set.of(), siblingsReferencedBy("Corpus"),
                "Corpus reaches a test helper OFFLINE_SOURCES does not scan");
        // The container-backed siblings do import these, so the prefixes are real.
        List<String> siblings = Stream.of("ElasticsearchAdversarialConformanceTest",
                        "ElasticsearchSurfaceTest", "TestElasticsearch")
                .flatMap(name -> importsOf(name).stream())
                .toList();
        for (String prefix : forbidden) {
            assertTrue(siblings.stream().anyMatch(imported -> imported.startsWith(prefix)), prefix);
        }
    }

    private static final Path TEST_SOURCES = Path.of(System.getProperty("user.dir"), "src",
            "test", "java", "dev", "cerbos", "queryplan", "elasticsearch");

    /** The fully-qualified names one test source imports. */
    private static List<String> importsOf(String simpleName) {
        try (Stream<String> lines = Files.lines(TEST_SOURCES.resolve(simpleName + ".java"))) {
            return lines.map(String::strip)
                    .filter(line -> line.startsWith("import ") && line.endsWith(";"))
                    .map(line -> line.substring("import ".length(), line.length() - 1)
                            .replace("static ", "").strip())
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Sibling test classes this source names in code. Comments and string literals are stripped
     * first, since this file names its siblings in both without using them.
     */
    private static Set<String> siblingsReferencedBy(String simpleName) {
        String code;
        try (Stream<Path> files = Files.list(TEST_SOURCES)) {
            code = Files.readString(TEST_SOURCES.resolve(simpleName + ".java"))
                    .replaceAll("(?s)/\\*.*?\\*/", "")
                    .replaceAll("//[^\\n]*", "")
                    .replaceAll("\"(?:\\\\.|[^\"\\\\])*\"", "\"\"");
            Set<String> referenced = new TreeSet<>();
            for (Path file : files.toList()) {
                String sibling = file.getFileName().toString().replace(".java", "");
                if (!sibling.equals(simpleName)
                        && Pattern.compile("\\b" + Pattern.quote(sibling) + "\\b").matcher(code).find()) {
                    referenced.add(sibling);
                }
            }
            return referenced;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Maps every refusal to the adapter site that raised it and pins the count per site. Every
     * refusal must match exactly one site, a change that moves a shape between sites shows up as
     * a diff, and no refusal may be an unmapped field.
     */
    @Nested
    class WhereTheRefusalsHappen {

        /** Message fragments for each refusal site the corpus reaches, named by mechanism. */
        private final Map<String, String> sites = Map.ofEntries(
                Map.entry("two-list difference", "except is not supported:"),
                Map.entry("whole-list comparison", " against a list literal cannot be expressed:"),
                Map.entry("list-valued member", "in with a list element cannot be expressed:"),
                Map.entry("computed collection macro", "over a computed collection cannot be lowered"),
                Map.entry("flat scalar collection macro", "Collection macros over flat scalar arrays"),
                Map.entry("literal exists-one", "exists_one over a literal collection value"),
                Map.entry("regex brace syntax", "matches regex has a brace"),
                Map.entry("regex dialect syntax", "matches regex uses syntax outside"),
                Map.entry("unanchored regex", "matches regex patterns must be fully anchored"),

                // resolveLeafOperand's default: the operand is computed (arithmetic, cast, ternary,
                // index, projection, count, lambda), and a term or range query needs a literal.
                Map.entry("computed leaf operand", " expression in leaf operand"),
                // A hierarchy path built by list() from a document field; the Query DSL cannot
                // concatenate.
                Map.entry("hierarchy path built from a field",
                        "hierarchy path constructed by list() from a document field"),
                // applyResolvedLeaf: both operands resolve to document fields.
                Map.entry("field-to-field", "cannot compare two document fields without scripts"),
                // normalizeLeafOperator: a string operator whose receiver is the constant.
                Map.entry("constant receiver", " with a document field as the receiver argument"),
                // Elasticsearch does not index an explicit null.
                Map.entry("explicit null",
                        "cannot distinguish an explicit null value from a missing field"),
                Map.entry("null in a document array",
                        "null membership in a document array requires an explicit null-value mapping"),
                Map.entry("null in an intersection",
                        "hasIntersection with null requires an explicit null-value mapping"),
                // Elasticsearch does not index an empty array, so polarities that would read a
                // missing collection as an allow are refused.
                Map.entry("positive all over a collection",
                        "all cannot distinguish a missing collection from an empty collection"),
                Map.entry("negated exists over a collection",
                        "Negated exists cannot distinguish a missing collection"),
                Map.entry("negated hasIntersection over a collection",
                        "Negated hasIntersection cannot distinguish a missing collection"),
                Map.entry("negated membership in a collection",
                        "Negated membership in a document collection cannot distinguish"),
                Map.entry("collection emptiness", " emptiness cannot distinguish a missing collection"),
                // exists_one needs a count of matching nested documents.
                Map.entry("exists_one", "exists_one cannot be expressed by Elasticsearch nested queries"),
                // A count comparison that is not an emptiness check.
                Map.entry("count threshold", "Unsupported size comparison:"),
                // size() over a field declared as neither nested nor a flat collection: the adapter
                // cannot tell a string's length from an array count.
                Map.entry("count over an undeclared collection",
                        "size() over a field not declared as a collection"),
                // size() over a computed collection, such as a filter().
                Map.entry("count over a computed collection", "Unsupported size() expression"),
                // A ternary used as the condition itself.
                Map.entry("conditional value as a condition",
                        "if (CEL ternary) cannot be expressed"),
                // An Elasticsearch date field cannot hold sub-millisecond precision.
                Map.entry("sub-millisecond timestamp", "Sub-millisecond timestamp literals"),
                // An empty delimiter leaves no segment boundary to compare against.
                Map.entry("empty hierarchy delimiter", "hierarchy delimiter is empty"),
                // RE2 parses `^a|b$` as two alternatives; Lucene's whole-field `a|b` does not.
                Map.entry("top-level regex alternation",
                        "matches regex has a top-level alternation"));

        private String siteOf(String action) {
            String raised;
            try {
                Corpus.translate(action);
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
                            Map.entry("unanchored regex", 1),
                            Map.entry("regex dialect syntax", 5),
                            Map.entry("regex brace syntax", 1),
                            Map.entry("computed collection macro", 2),
                            Map.entry("literal exists-one", 1),
                            Map.entry("flat scalar collection macro", 3),
                            Map.entry("list-valued member", 1),
                            Map.entry("two-list difference", 4),
                            Map.entry("whole-list comparison", 2),
                            Map.entry("computed leaf operand", 83),
                            Map.entry("field-to-field", 22),
                            Map.entry("explicit null", 8),
                            Map.entry("count threshold", 5),
                            Map.entry("constant receiver", 4),
                            Map.entry("negated exists over a collection", 5),
                            Map.entry("positive all over a collection", 5),
                            Map.entry("count over an undeclared collection", 12),
                            Map.entry("collection emptiness", 2),
                            Map.entry("conditional value as a condition", 3),
                            Map.entry("exists_one", 2),
                            Map.entry("negated membership in a collection", 3),
                            Map.entry("sub-millisecond timestamp", 2),
                            Map.entry("count over a computed collection", 2),
                            Map.entry("hierarchy path built from a field", 2),
                            Map.entry("negated hasIntersection over a collection", 2),
                            Map.entry("null in a document array", 2),
                            Map.entry("null in an intersection", 4),
                            Map.entry("empty hierarchy delimiter", 1),
                            Map.entry("top-level regex alternation", 2))),
                    counts);
            assertEquals(THROWING.size(),
                    counts.values().stream().mapToInt(Integer::intValue).sum());
        }

        /**
         * No refusal is {@code "Unknown attribute"}: that means the corpus field map is incomplete,
         * not that the Query DSL cannot express the shape.
         */
        @Test
        void noRefusalIsAnUnmappedField() {
            List<String> unmapped = new ArrayList<>();
            for (String action : THROWING.keySet()) {
                try {
                    Corpus.translate(action);
                } catch (IllegalArgumentException error) {
                    if (String.valueOf(error.getMessage()).contains("Unknown attribute")) {
                        unmapped.add(action + ": " + error.getMessage());
                    }
                }
            }
            assertEquals(List.of(), unmapped);
            // Anti-vacuity: the detector recognises a real unmapped-field message.
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                            Corpus.planFromWireFixture("cs-eq"),
                            Corpus.OPTIONS.withFieldMap(Map.of())));
            assertTrue(ex.getMessage().contains("Unknown attribute"), ex.getMessage());
        }
    }

    /**
     * Rules over every translated corpus action, each with an anti-vacuity check. These still
     * hold if a regenerated golden file is committed without review.
     */
    @Nested
    class WhatTheEmittedQueryContains {

        /** One leaf clause of an emitted query: {@code {"term": {"aBool": {"value": true}}}}. */
        record Comparison(String action, String clause, String field, JsonNode operand) {}

        /** One {@code nested} query: the action that emitted it, its path, and its inner query. */
        record NestedScope(String action, String path, JsonNode query) {}

        private List<Comparison> comparisons;
        private List<NestedScope> nested;

        private List<Comparison> comparisons() {
            if (comparisons == null) {
                comparisons = new ArrayList<>();
                nested = new ArrayList<>();
                for (String action : recordedActions) {
                    JsonNode query = recordedQuery(action);
                    if (query != null) {
                        walk(action, query, comparisons, nested);
                    }
                }
            }
            return comparisons;
        }

        /**
         * Splits an emitted query into leaf clauses and nested scopes. An unknown clause fails, so
         * a new emission shape cannot slip past the rules.
         */
        private void walk(String action, JsonNode query, List<Comparison> leaves,
                          List<NestedScope> scopes) {
            for (Map.Entry<String, JsonNode> clause : query.properties()) {
                String name = clause.getKey();
                JsonNode body = clause.getValue();
                switch (name) {
                    case "bool" -> body.properties().stream()
                            .filter(occur -> !"minimum_should_match".equals(occur.getKey()))
                            .forEach(occur -> occur.getValue()
                                    .forEach(child -> walk(action, child, leaves, scopes)));
                    case "nested" -> {
                        scopes.add(new NestedScope(
                                action, body.get("path").asText(), body.get("query")));
                        walk(action, body.get("query"), leaves, scopes);
                    }
                    case "match_all", "match_none" -> { }
                    case "exists" -> leaves.add(new Comparison(
                            action, name, body.get("field").asText(), null));
                    case "term", "terms", "range", "prefix", "wildcard", "regexp" ->
                            body.properties().forEach(field -> leaves.add(new Comparison(
                                    action, name, field.getKey(), field.getValue())));
                    default -> throw new AssertionError(
                            action + " emits a clause this rule does not recognise: " + name);
                }
            }
        }

        /**
         * The adapter never sees the index, so a misspelled field would match nothing and silently
         * deny. The harness cannot catch that when the oracle also allows nothing.
         */
        @Test
        void everyFieldNamedIsOneTheCorpusMapsOrANestedScopeOfOne() {
            Set<String> declared = Set.copyOf(Corpus.FIELD_MAP.values());
            List<String> stray = comparisons().stream()
                    .map(Comparison::field)
                    .filter(field -> !declared.contains(field)
                            && Corpus.NESTED_PATHS.stream()
                                    .noneMatch(path -> field.startsWith(path + ".")))
                    .distinct()
                    .sorted()
                    .toList();

            assertEquals(List.of(), stray);
            // Anti-vacuity: comparisons exist, including some inside a nested scope.
            assertFalse(comparisons().isEmpty());
            assertTrue(comparisons().stream().anyMatch(leaf -> Corpus.NESTED_PATHS.stream()
                            .anyMatch(path -> leaf.field().startsWith(path + "."))),
                    "no comparison is made inside a nested scope");
        }

        /**
         * Inside a {@code nested} query only fields under that path are visible, so an inner clause
         * naming any other field silently matches nothing.
         */
        @Test
        void everyNestedQueryOnlyNamesFieldsInsideItsOwnPath() {
            comparisons();
            List<String> offenders = new ArrayList<>();
            for (NestedScope scope : nested) {
                List<Comparison> inner = new ArrayList<>();
                walk(scope.action(), scope.query(), inner, new ArrayList<>());
                inner.stream()
                        .filter(leaf -> !leaf.field().startsWith(scope.path() + "."))
                        .forEach(leaf -> offenders.add(
                                scope.action() + ": " + scope.path() + " -> " + leaf.field()));
            }

            assertEquals(List.of(), offenders);
            // Anti-vacuity: nested scopes exist, and each path is mapped as `nested` (Elasticsearch
            // would reject one that is not only at search time).
            assertFalse(nested.isEmpty());
            nested.forEach(scope -> assertTrue(
                    Corpus.NESTED_PATHS.contains(scope.path()), scope.path()));
        }

        /**
         * No emitted query contains a null literal. Elasticsearch does not index nulls, so a
         * {@code term} on null matches nothing and its {@code must_not} matches everything.
         */
        @Test
        void noEmittedQueryBindsANullLiteral() {
            List<String> offenders = recordedActions.stream()
                    .filter(action -> recordedQuery(action) != null
                            && containsNull(recordedQuery(action)))
                    .map(action -> action + ": " + recordedQuery(action))
                    .toList();

            assertEquals(List.of(), offenders);
            // Anti-vacuity: the null comparisons are still in the corpus and lower to `exists`.
            for (String action : List.of("null-ne", "null-not-eq", "vf-null-ne")) {
                assertTrue(recordedActions.contains(action), action);
            }
            assertEquals("{\"exists\":{\"field\":\"owner\"}}", recordedQuery("null-ne").toString());
        }

        private boolean containsNull(JsonNode node) {
            if (node.isNull()) {
                return true;
            }
            for (JsonNode child : node) {
                if (containsNull(child)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * {@code *} and {@code ?} in a {@code wildcard} value must be escaped, apart from the
         * anchors the adapter adds; otherwise a substring test becomes a pattern match. {@code %},
         * {@code _} and {@code [} are literal in a wildcard query.
         */
        @Test
        void everyWildcardNeedleEscapesItsMetacharacters() {
            List<Comparison> wildcards = comparisons().stream()
                    .filter(leaf -> "wildcard".equals(leaf.clause()))
                    .toList();
            List<String> offenders = wildcards.stream()
                    .filter(leaf -> hasUnescapedMetacharacter(
                            stripAnchors(leaf.operand().get("value").asText())))
                    .map(leaf -> leaf.action() + ": " + leaf.operand())
                    .toList();

            assertEquals(List.of(), offenders);
            // Anti-vacuity: wildcards are emitted, `like-backslash`'s trailing backslash is
            // doubled, and the detector rejects an unescaped `*`.
            assertFalse(wildcards.isEmpty());
            assertEquals("*\\\\", recordedQuery("like-backslash")
                    .get("wildcard").get("aString").get("value").asText());
            assertTrue(hasUnescapedMetacharacter("a*b"));
            assertFalse(hasUnescapedMetacharacter("a\\*b"));
        }

        /** Drop the {@code *} anchors the adapter adds for {@code contains} / {@code endsWith}. */
        private String stripAnchors(String value) {
            String body = value.startsWith("*") ? value.substring(1) : value;
            return body.endsWith("*") && !endsEscaped(body)
                    ? body.substring(0, body.length() - 1) : body;
        }

        private boolean endsEscaped(String value) {
            int backslashes = 0;
            for (int i = value.length() - 2; i >= 0 && value.charAt(i) == '\\'; i--) {
                backslashes++;
            }
            return backslashes % 2 != 0;
        }

        private boolean hasUnescapedMetacharacter(String value) {
            boolean escaped = false;
            for (int i = 0; i < value.length(); i++) {
                char current = value.charAt(i);
                if (escaped) {
                    escaped = false;
                } else if (current == '\\') {
                    escaped = true;
                } else if (current == '*' || current == '?') {
                    return true;
                }
            }
            return false;
        }

        /**
         * Every emitted value is a plain JDK type. A library type with its own serializer would
         * make that library's version an input to the golden file.
         */
        @Test
        void everyEmittedValueIsAPlainJdkType() {
            List<String> exotic = new ArrayList<>();
            for (String action : recordedActions) {
                Result result = emitted.get(action);
                if (result instanceof Result.Conditional conditional) {
                    assertPlain(action, conditional.query(), exotic);
                }
            }
            assertEquals(List.of(), exotic);
            assertFalse(recordedActions.isEmpty());
        }

        private void assertPlain(String action, Object value, List<String> exotic) {
            if (value instanceof Map<?, ?> map) {
                map.forEach((key, child) -> {
                    if (!(key instanceof String)) {
                        exotic.add(action + ": non-string key "
                                + (key == null ? "null" : key.getClass().getName()));
                    }
                    assertPlain(action, child, exotic);
                });
            } else if (value instanceof List<?> list) {
                list.forEach(child -> assertPlain(action, child, exotic));
            } else if (!(value instanceof String || value instanceof Boolean
                    || value instanceof Long || value instanceof Integer
                    || value instanceof Double)) {
                exotic.add(action + ": " + (value == null ? "null" : value.getClass().getName()));
            }
        }

        /**
         * {@link Corpus#canonicalJson} rejects NaN, which Jackson would otherwise write as the
         * string {@code "NaN"}. The corpus actions whose arithmetic could produce one are refused
         * first.
         */
        @Test
        void aValueJsonCannotCarryIsRefusedRatherThanRecorded() {
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> Corpus.canonicalJson(Map.of("range",
                            Map.of("aDouble", Map.of("gt", Double.NaN)))));
            assertTrue(ex.getMessage().contains("which JSON cannot carry"), ex.getMessage());

            for (String action : List.of(
                    "cr-div-zero", "cr-div-neg-zero", "nan-ord-inf", "nan-ord-le")) {
                assertTrue(THROWING.containsKey(action), action);
            }
        }
    }
}
