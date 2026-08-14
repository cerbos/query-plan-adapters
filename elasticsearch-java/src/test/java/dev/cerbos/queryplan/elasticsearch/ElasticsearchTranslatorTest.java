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
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Translator unit test: for every action in the shared {@code ../conformance/} corpus, the
 * Elasticsearch Query DSL this adapter emits. Offline — no Cerbos sidecar, no Elasticsearch, no
 * Docker.
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
 *   <tr><td>the documents a query returns</td>
 *       <td>{@link ElasticsearchAdversarialConformanceTest}, against a real Elasticsearch with
 *           {@code check()} as the oracle</td></tr>
 *   <tr><td><strong>the query this adapter emits for a plan</strong></td>
 *       <td><strong>here</strong></td></tr>
 * </table>
 *
 * <p><strong>The plans are read, not written.</strong> A hand-built plan is a BELIEF about what the
 * planner emits, and this repository keeps golden fixtures because that belief has been wrong
 * before ({@code docs/adr/0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md}). The
 * hand-built plans that remain in {@link ElasticsearchQueryPlanAdapterTest} are there for shapes no
 * policy can produce — malformed operands, caller-supplied arguments, literal validation — which is
 * the one thing a fixture cannot supply.
 *
 * <p><strong>The expectations are data, not literals.</strong> The queries this adapter is pinned
 * to emit live in {@code elasticsearch-java/golden/expectations.json}, a golden expectation file
 * this adapter owns — never under {@code conformance/}, where every adapter workflow triggers and
 * one adapter re-pinning one query would re-run all the others. It is regenerated with
 * {@code gradle goldenUpdate} and reviewed as a diff, exactly like the wire fixtures it is asserted
 * against ({@code conformance/README.md}, "Golden expectations").
 *
 * <p><strong>This file reads as mostly-throws, and that is the adapter.</strong> The Elasticsearch
 * Query DSL compares a FIELD against a literal: it has no arithmetic, no casts, no conditional
 * values, no field-to-field comparison, no hierarchy relation, no count threshold, and no way to
 * index an explicit null or an empty array. Most of the corpus is therefore fail-closed here, and
 * every refusal is asserted against the message {@code actions.json} pins rather than a bare "it
 * threw" — which for an adapter with this ratio is the difference between a suite and a formality
 * (cerbos/query-plan-adapters#326). {@link WhereTheRefusalsHappen} states the property the
 * per-action assertions cannot: the shape of those refusals taken together.
 *
 * <p><strong>Adding a corpus action fails this file.</strong> Every wire fixture must be accounted
 * for here exactly once — a golden expectation or a throw carrying the message
 * {@code actions.json} pins — and the completeness guard below is what makes a new action land as a
 * failure rather than as silence.
 */
class ElasticsearchTranslatorTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private static final ActionsFile ACTIONS = Corpus.actionsFile();

    /**
     * The shapes {@code actions.json} says this adapter must refuse, each with the message it must
     * refuse them with. Identical to the classification the harness asserts against a live PDP;
     * asserting it here as well is what lets the completeness guard below be total, and it costs a
     * millisecond rather than two containers.
     *
     * <p>The {@code nullRepresentationOmitted} probe is folded in on THIS adapter's terms.
     * Elsewhere it is a refusal under one convention and a translation under the other, so the
     * corpus keeps it a group of its own and the harness keeps it separate. Here it is
     * unconditional: Elasticsearch cannot index an explicit null distinguishably from a missing
     * field, so there is no representation option to flip.
     * {@link #theNullRepresentationProbeIsRefusedRegardless} is where that is stated rather than
     * assumed.
     *
     * <p>A throwing action needs no golden expectation of its own: the message is already corpus
     * data, pinned once in {@code actions.json} and read by every adapter. Writing it into this
     * adapter's asset too would create two places to change one string with nothing to say which is
     * authoritative — and on an adapter that refuses most of the corpus, the asset would be largely
     * a restatement of shared data.
     */
    private static final Map<String, String> THROWING = throwingActions();

    private static Map<String, String> throwingActions() {
        Map<String, String> throwing = new TreeMap<>(Corpus.throwingActions(ACTIONS, Corpus.ADAPTER));
        for (NullRepresentationOmitted probe : Corpus.nullRepresentationThrows(ACTIONS)) {
            throwing.put(probe.action(), Corpus.nullOmittedMessage(probe, Corpus.ADAPTER));
        }
        // Unmodifiable rather than Map.copyOf: the sort order is what makes the throw suite's
        // parameterised cases read the same way twice, and Map.copyOf's iteration order is
        // unspecified.
        return Collections.unmodifiableMap(throwing);
    }

    /**
     * Every emitted query, translated once per action and read by everything below — the comparison
     * against the asset, the rules, and the regeneration that writes it.
     *
     * <p>One pass, deliberately: the rules are about what the translator emits RIGHT NOW rather
     * than about the pinned bytes, and a second pass would let those two answers drift apart within
     * a single run.
     */
    private static Map<String, Result> emitted;

    private static Map<String, ObjectNode> recorded;
    private static List<String> recordedActions;

    @BeforeAll
    static void setUp() {
        emitted = new LinkedHashMap<>();
        for (String action : Corpus.wireFixtureActions()) {
            // A throwing action is never translated here: its message is corpus data, and asking
            // the translator for a query it must refuse would fail in this loop rather than in the
            // throw suite that owns the question.
            if (!THROWING.containsKey(action)) {
                emitted.put(action, Corpus.translate(action));
            }
        }

        // `gradle goldenUpdate` rewrites the file from what the translator emits today and
        // preserves every note. That is the same deliberate act as regenerating the wire fixtures,
        // and the safety is identical: the diff is what a reviewer reads. CI never sets the
        // property, so a translator change that moves the emitted query fails there whatever anyone
        // ran locally. Skipping the throwing actions above is also what keeps regeneration from
        // papering over a misclassification — an action moved into `adapterUnsupported` that this
        // adapter still translates fails the throw suite, and one moved out of it that this adapter
        // still refuses fails regeneration itself.
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
     * The whole translator output for one action, in the shape the golden file records.
     *
     * <p><strong>Serialised verbatim, because the Query DSL already is JSON.</strong> A
     * {@code Result.Conditional} carries a {@code Map<String, Object>} of plain JDK values — there
     * is no Elasticsearch client object to render, and no client library on the classpath to render
     * it with — so unlike sqlalchemy (a compiled expression) and spring-data (a rendered Criteria
     * tree) this asset has no generator to declare in its header ({@code conformance/README.md},
     * "When the generator is an input").
     * {@link WhatTheEmittedQueryContains#everyEmittedValueIsAPlainJdkType} is that claim asserted
     * rather than stated.
     *
     * <p>The plan kind is recorded alongside, because it is the other half of the translator's
     * answer: an {@code ALWAYS_ALLOWED} plan produces no query at all, and a consumer switches on
     * the kind before it ever looks at one.
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
        // `Result` is sealed over exactly those three, so this is unreachable today — and it is
        // written as a failure rather than a default because a FOURTH kind silently recorded as
        // CONDITIONAL would be an entry claiming a query that a consumer never receives.
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
     * The message, not just the throw: a mapper typo or an unrelated validation satisfies a bare
     * {@code assertThrows} just as well as the limitation the corpus documents
     * (cerbos/query-plan-adapters#326). The harness makes the same assertion against a live PDP;
     * here it costs a millisecond, which is what lets the completeness guard below be total.
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

        // Total: a corpus action with no golden expectation and no pinned throw lands as a failure
        // rather than as silence. This is the assertion that makes the asset self-maintaining —
        // adding a hostile shape to the corpus forces someone to look at the query this adapter
        // emits for it, and `goldenUpdate` refuses to invent one for a shape that throws.
        assertEquals(Corpus.wireFixtureActions(), classified,
                "every wire fixture must be accounted for exactly once");
        // Disjoint: an action carrying a golden expectation AND declared unsupported would satisfy
        // the union above while asserting two contradictory things.
        assertEquals(classified.size(), Set.copyOf(classified).size(),
                "an action is either recorded or thrown, never both");
        // The asset is written sorted, so a translator change reads as the list of shapes it moved.
        assertEquals(new ArrayList<>(new TreeSet<>(recordedActions)), recordedActions,
                "golden/expectations.json must stay sorted by action");
        // ...and the corpus manifest is the same set the fixtures are, so a fixture nobody
        // classified cannot hide behind an actions.json that never named it.
        assertEquals(new TreeSet<>(Corpus.wireFixtureActions()), ACTIONS.manifestActions());

        // Tripwires. Bump them deliberately: a count that moves without anyone noticing is how a
        // shape gets dropped from an asset nobody reads end to end.
        assertEquals(
                Map.of("conditional", 75, "unconditional", 2, "throwing", 122),
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
        // Two corpus actions carry no condition at all, and an entry with no `query` is only
        // lossless while it is one of them — a translation that quietly stopped emitting a filter
        // would record exactly the same absence. They mean OPPOSITE things, which is why the plan
        // kind is recorded next to the query rather than inferred from it:
        //
        //   `p-has` is the corpus's one knownDivergences entry — the planner folds has() on a
        //   missing attribute to ALWAYS_ALLOWED while check() denies those rows, so a caller
        //   searches unfiltered and sees every document.
        //
        //   `in-empty` is `R.attr.aString in []`, which the planner decides statically: nothing is
        //   a member of the empty list, so the plan is ALWAYS_DENIED and a caller runs no search.
        assertEquals(List.of("in-empty", "p-has"), unconditionalActions());
        assertEquals(List.of("p-has"), actionsOfKind("ALWAYS_ALLOWED"));
        assertTrue(ACTIONS.skippedDivergences(Corpus.ADAPTER).contains("p-has"));
        assertEquals(List.of("in-empty"), actionsOfKind("ALWAYS_DENIED"));
    }

    /**
     * The corpus's {@code nullRepresentationOmitted} probe, and why this adapter needs no option.
     *
     * <p>{@code null-eq-missing} compares {@code aOptionalString == null}, and the planner emits
     * the same {@code eq(attr, null)} node whichever convention the caller uses — so every other
     * adapter has to be TOLD which one it is. Elasticsearch cannot index an explicit null
     * distinguishably from a missing field, so every null-SELECTING direction already fails closed
     * and only the {@code exists}-shaped ones translate. That is why the classification is folded
     * into {@link #THROWING} above rather than parameterised over a representation, and this is the
     * assertion that says so: if a future change starts emitting a null-selecting query here, the
     * adapter acquires a representation dependency it must then declare.
     */
    @Test
    void theNullRepresentationProbeIsRefusedRegardless() {
        List<NullRepresentationOmitted> probes = Corpus.nullRepresentationThrows(ACTIONS);
        assertEquals(List.of("null-eq-missing"), probes.stream()
                .map(NullRepresentationOmitted::action).toList());
        for (NullRepresentationOmitted probe : probes) {
            String message = Corpus.nullOmittedMessage(probe, Corpus.ADAPTER);
            // Refused with the declared message whether or not the caller declares an
            // explicit-null attribute at all — the two ends of the option every other adapter
            // carries, both landing on the same refusal here.
            IllegalArgumentException declared = assertThrows(IllegalArgumentException.class,
                    () -> Corpus.translate(probe.action()));
            assertTrue(declared.getMessage().contains(message), declared.getMessage());

            IllegalArgumentException undeclared = assertThrows(IllegalArgumentException.class,
                    () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                            Corpus.planFromWireFixture(probe.action()), Corpus.FIELD_MAP,
                            Map.of(), Corpus.NESTED_PATHS, Set.of()));
            assertTrue(undeclared.getMessage().contains(message), undeclared.getMessage());
        }
    }

    /**
     * The one operand a wire fixture cannot pin, and the assertion that the reader's choice of
     * instant is a real input to this adapter's classification rather than a tidy default.
     *
     * <p>{@code regenerate-wire-fixtures.sh} rewrites {@code ts-window}'s folded
     * {@code now() - duration("24h")} literal to a placeholder because it differs on every capture,
     * so reading the fixture back means choosing an instant. The PDP emits NANOSECONDS, and this
     * adapter refuses a timestamp literal an ordinary Elasticsearch {@code date} field cannot
     * preserve — which is exactly the reason {@code actions.json} gives for these two actions. At
     * millisecond precision the same fixture TRANSLATES, so a tidier substitution in
     * {@link Corpus#PLANNED_AT} would quietly contradict the corpus, and nothing else in this
     * repository would notice.
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

    /**
     * The asset carries the command that rewrites it, so a reader who opens the file after a
     * failing assertion is told how to look at the difference. That is only useful while the
     * command exists.
     */
    @Test
    void theAssetNamesACommandThisBuildDefines() throws Exception {
        String[] parts = Corpus.GOLDEN_REGENERATE_COMMAND.split(" ");
        assertEquals("gradle", parts[0]);
        assertTrue(Files.readString(Path.of(System.getProperty("user.dir"), "build.gradle.kts"))
                        .contains("tasks.register<Test>(\"" + parts[1] + "\")"),
                () -> "build.gradle.kts defines no task named " + parts[1]);
    }

    /**
     * "Offline" is a property of this class, so it is asserted rather than described.
     *
     * <p>A suite that quietly acquired a PDP or a container would still PASS — that is exactly the
     * drift worth catching — so the check is on what this file may reach rather than on what it
     * does, with the complement asserted so it is about this file rather than about a spelling that
     * appears nowhere in the tree.
     */
    @Test
    void thisSuiteReachesNoPdpAndNoContainer() {
        List<String> forbidden = List.of("org.testcontainers.", "dev.cerbos.sdk.", "java.net.http.");
        // The IMPORT LINES, not a substring of the file: the names below appear in this method as
        // string literals, and a substring scan would match itself.
        assertEquals(List.of(), importsOf("ElasticsearchTranslatorTest").stream()
                        .filter(imported -> forbidden.stream().anyMatch(imported::startsWith))
                        .toList(),
                "this suite reaches a PDP or a container, so it is no longer offline");
        // ...and the container-backed siblings DO import each of them, so the assertion above is
        // about this file rather than about a package prefix nothing in the tree uses.
        List<String> siblings = Stream.of("ElasticsearchAdversarialConformanceTest",
                        "ElasticsearchSurfaceTest", "TestElasticsearch")
                .flatMap(name -> importsOf(name).stream())
                .toList();
        for (String prefix : forbidden) {
            assertTrue(siblings.stream().anyMatch(imported -> imported.startsWith(prefix)), prefix);
        }
    }

    /** The fully-qualified names one test source imports. */
    private static List<String> importsOf(String simpleName) {
        Path source = Path.of(System.getProperty("user.dir"), "src", "test", "java", "dev",
                "cerbos", "queryplan", "elasticsearch", simpleName + ".java");
        try (Stream<String> lines = Files.lines(source)) {
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
     * Where in the walk each rejection happens, and how many corpus shapes reach each site.
     *
     * <p>{@code actions.json} pins a substring of the message per action, so the throw suite above
     * proves every refusal is the declared one. It cannot say anything about the SHAPE of the
     * refusals taken together, and on an adapter that refuses most of the corpus that is the more
     * interesting property: it matters whether that happens at a dozen sites or at one catch-all.
     *
     * <p>Three things are asserted. <strong>Total</strong> — every refusal matches a site this
     * adapter actually has, so a shape rejected by an accident cannot pass as a declared
     * limitation, which is the #326 trap at corpus scale. <strong>Pinned counts</strong> — a
     * translator change that moves a shape from one site to another shows up as a diff even though
     * both sites throw and {@code actions.json} is unchanged. <strong>No unmapped field</strong> —
     * {@code "Unknown attribute"} is not a limitation of the Query DSL at all, it is this suite's
     * own field map coming up short, and it is the exact accident #326 was filed for.
     */
    @Nested
    class WhereTheRefusalsHappen {

        /**
         * One entry per {@code throw} site in {@link ElasticsearchQueryPlanAdapter} the corpus
         * reaches, named for the mechanism rather than for the message.
         */
        private final Map<String, String> sites = Map.ofEntries(
                // resolveLeafOperand's default: the operand slot holds a computed sub-expression —
                // arithmetic, a cast, a ternary, an index, a projection, a hierarchy relation, a
                // count, a lambda. A term or range query compares a FIELD against a literal, and
                // there is nowhere to evaluate anything on the way.
                Map.entry("computed leaf operand", " expression in leaf operand"),
                // applyResolvedLeaf: both operands resolve to document fields.
                Map.entry("field-to-field", "cannot compare two document fields without scripts"),
                // normalizeLeafOperator: a string operator whose RECEIVER is the constant, so the
                // document field is the needle rather than the haystack.
                Map.entry("constant receiver", " with a document field as the receiver argument"),
                // The explicit-null convention Elasticsearch cannot index (#302, #308).
                Map.entry("explicit null",
                        "cannot distinguish an explicit null value from a missing field"),
                Map.entry("null in a document array",
                        "null membership in a document array requires an explicit null-value mapping"),
                Map.entry("null in an intersection",
                        "hasIntersection with null requires an explicit null-value mapping"),
                // Elasticsearch does not index an empty array, so the polarities that would read a
                // missing collection as an allow are refused (#309).
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
                // A count comparison that is not an emptiness check, or not over a collection.
                Map.entry("count threshold", "Unsupported size comparison:"),
                Map.entry("count over a non-collection", "Unsupported size() expression"),
                // A ternary reaches the leaf path with three operands rather than two.
                Map.entry("operand arity", " requires exactly 2 operands, got "),
                // An ordinary Elasticsearch date field cannot preserve sub-millisecond precision.
                Map.entry("sub-millisecond timestamp", "Sub-millisecond timestamp literals"));

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
                            Map.entry("computed leaf operand", 65),
                            Map.entry("field-to-field", 15),
                            Map.entry("count threshold", 8),
                            Map.entry("explicit null", 8),
                            Map.entry("constant receiver", 4),
                            Map.entry("negated exists over a collection", 4),
                            Map.entry("positive all over a collection", 4),
                            Map.entry("collection emptiness", 2),
                            Map.entry("exists_one", 2),
                            Map.entry("negated membership in a collection", 2),
                            Map.entry("operand arity", 2),
                            Map.entry("sub-millisecond timestamp", 2),
                            Map.entry("count over a non-collection", 1),
                            Map.entry("negated hasIntersection over a collection", 1),
                            Map.entry("null in a document array", 1),
                            Map.entry("null in an intersection", 1))),
                    counts);
            assertEquals(THROWING.size(),
                    counts.values().stream().mapToInt(Integer::intValue).sum());
        }

        /**
         * The #326 assertion, stated over the whole corpus. An unmapped field makes an action throw
         * {@code "Unknown attribute"} — which is the field map coming up short, not a limitation of
         * the Query DSL — and it once let six actions pass the throw suite while never reaching the
         * mechanism their {@code actions.json} reasons claim.
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
            // Anti-vacuity: the detector must recognise the message it is looking for, built here
            // rather than hoped for.
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                            Corpus.planFromWireFixture("cs-eq"), Map.of(), Map.of(),
                            Corpus.NESTED_PATHS, Set.of()));
            assertTrue(ex.getMessage().contains("Unknown attribute"), ex.getMessage());
        }
    }

    /**
     * The properties a regenerated asset must not silently accept.
     *
     * <p>Pinned bytes do not survive {@code gradle goldenUpdate} being run and committed unread;
     * rules do. So each of these is stated over every translated corpus action rather than over a
     * chosen shape, and each carries an anti-vacuity assertion.
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
         * Decompose an emitted query into the leaf clauses it makes and the nested scopes it opens.
         *
         * <p>An unrecognised clause is a failure rather than something skipped: a rule that
         * silently ignores a node it does not know is a rule a new emission shape walks past.
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
                    // term / terms / range / prefix / wildcard / regexp: one field, one operand.
                    case "term", "terms", "range", "prefix", "wildcard", "regexp" ->
                            body.properties().forEach(field -> leaves.add(new Comparison(
                                    action, name, field.getKey(), field.getValue())));
                    default -> throw new AssertionError(
                            action + " emits a clause this rule does not recognise: " + name);
                }
            }
        }

        /**
         * The one rule the harness cannot make. The adapter is handed a plan, never an index, so a
         * field name it emits is never checked against anything — and a query naming a field no
         * document holds is not an error, it is a query that matches nothing and silently denies.
         * The harness cannot catch that either: a query returning no document agrees with an oracle
         * that allows none.
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
            // Anti-vacuity, in two parts: the corpus must still emit comparisons at all, and it
            // must still reach INSIDE a nested scope, which a root-only rule would miss.
            assertFalse(comparisons().isEmpty());
            assertTrue(comparisons().stream().anyMatch(leaf -> Corpus.NESTED_PATHS.stream()
                            .anyMatch(path -> leaf.field().startsWith(path + "."))),
                    "no comparison is made inside a nested scope");
        }

        /**
         * A {@code nested} query evaluates its inner clause against ONE inner document, and a field
         * outside the path is not visible there — so an inner clause naming one matches nothing,
         * silently. This is the Elasticsearch spelling of a subquery that lost its correlation, and
         * it is the class of bug no row-level oracle catches while the seeded data happens to
         * agree.
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
            // Anti-vacuity, in two parts: the corpus must open nested scopes, and every path they
            // name must be one the corpus index maps as `nested` — a `nested` query against a plain
            // object path is rejected by Elasticsearch at search time rather than here.
            assertFalse(nested.isEmpty());
            nested.forEach(scope -> assertTrue(
                    Corpus.NESTED_PATHS.contains(scope.path()), scope.path()));
        }

        /**
         * No emitted query ever asks Elasticsearch about a null.
         *
         * <p>Elasticsearch does not index a JSON null, so a {@code term} query against one matches
         * nothing and a {@code must_not} around it matches everything — both silently wrong. The
         * adapter refuses every null-selecting direction and lowers the presence-selecting ones to
         * {@code exists}; a regenerated asset that acquired a null literal would be an
         * authorization bug rather than a diff.
         */
        @Test
        void noEmittedQueryBindsANullLiteral() {
            List<String> offenders = recordedActions.stream()
                    .filter(action -> recordedQuery(action) != null
                            && containsNull(recordedQuery(action)))
                    .map(action -> action + ": " + recordedQuery(action))
                    .toList();

            assertEquals(List.of(), offenders);
            // Anti-vacuity: the corpus must still drive the null comparisons this rule polices, and
            // they must still translate — into `exists`, the only shape that survives.
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
         * Wildcard metacharacters in a needle are this repository's founding bug class (#258/#259)
         * in its Elasticsearch spelling. A {@code wildcard} query reads {@code *} and {@code ?} as
         * operators, so an unescaped one in a value turns a substring test into a pattern match and
         * returns documents the PDP denies. The adapter escapes them and adds the anchors itself,
         * so the only unescaped metacharacters a value may carry are those anchors.
         *
         * <p>{@code %}, {@code _} and {@code [} are deliberately NOT in this rule: a wildcard query
         * is term-level and matches them literally, which is why this adapter needs none of the
         * {@code LIKE ... ESCAPE} machinery the SQL adapters do.
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
            // Anti-vacuity, in three parts: the corpus must emit wildcard queries at all; the
            // escaping must have something to do — `like-backslash` ends with a single backslash,
            // which the adapter doubles; and the detector must reject something.
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
         * The serialisation decision, asserted.
         *
         * <p>This adapter emits a {@code Map<String, Object>} of plain JDK values, which IS the
         * Query DSL — so the asset records the translator's return value verbatim and declares no
         * generator. That claim holds only while nothing on the way out is a library type with a
         * serialiser of its own, whose version would then be an input to the recorded bytes exactly
         * as SQLAlchemy's compiler and Hibernate's renderer are to those adapters'
         * ({@code conformance/README.md}, "When the generator is an input").
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
         * A query leaves this adapter as part of a JSON request body, so a value JSON cannot carry
         * is a value the deployed adapter could not have sent either.
         *
         * <p>The interesting half is that a round trip does NOT catch it. Jackson quotes a
         * non-finite number by default, so {@code NaN} is written as the string {@code "NaN"},
         * parses back cleanly, and has silently stopped being a number — a {@code range} query
         * against a string rather than a broken one. {@link Corpus#canonicalJson} therefore
         * refuses it explicitly, and this is the assertion that the refusal is live rather than
         * unreachable: the detector is proved against a value built here, and the corpus actions
         * whose arithmetic would produce one are confirmed refused before a literal is ever built.
         */
        @Test
        void aValueJsonCannotCarryIsRefusedRatherThanRecorded() throws Exception {
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> Corpus.canonicalJson(Map.of("range",
                            Map.of("aDouble", Map.of("gt", Double.NaN)))));
            assertTrue(ex.getMessage().contains("which JSON cannot carry"), ex.getMessage());
            // ...and the round trip alone would have accepted it, quietly, as a string.
            assertEquals("\"NaN\"", JSON.writeValueAsString(Double.NaN));

            for (String action : List.of(
                    "cr-div-zero", "cr-div-neg-zero", "nan-ord-inf", "nan-ord-le")) {
                assertTrue(THROWING.containsKey(action), action);
            }
        }
    }
}
