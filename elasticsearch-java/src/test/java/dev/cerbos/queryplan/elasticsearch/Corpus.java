package dev.cerbos.queryplan.elasticsearch;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.core.util.Separators;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.google.protobuf.util.JsonFormat;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This adapter's reader for the shared {@code ../conformance/} corpus, and for the golden
 * expectations that belong to it alone.
 *
 * <p><strong>Deliberately duplicated.</strong> Every adapter carries its own loader —
 * {@code prisma/src/corpus.ts}, {@code sqlalchemy/tests/corpus.py}, {@code pgx/corpus_test.go},
 * spring-data's {@code Corpus.java}, this one — so that each stays standalone and none of them can
 * break another by changing. Do not extract a shared one, and do not add a drift check between the
 * copies: they are allowed to differ ({@code docs/adr/0007-adapters-share-data-not-code.md}).
 *
 * <p>What lives here is what BOTH of this adapter's corpus suites must agree on: the classification
 * in {@code actions.json}, the wire-fixture decoding, and the three call arguments the corpus is
 * translated through — {@link #FIELD_MAP}, {@link #NESTED_PATHS} and
 * {@link #EXPLICIT_NULL_ATTRIBUTES}. That last group is the load-bearing part:
 * {@link ElasticsearchTranslatorTest} pins the Query DSL this adapter emits for a corpus action and
 * {@link ElasticsearchAdversarialConformanceTest} proves the documents that same query returns, and
 * the two statements are only about the same query while both are built from these arguments.
 *
 * <p>The seeds, the derived fields, the {@code check()} oracle and the coverage guards over all
 * three stay in the harness, which is the only thing that consumes them.
 */
final class Corpus {

    private Corpus() {}

    /** The corpus key for this adapter — its directory name, as every other harness uses. */
    static final String ADAPTER = "elasticsearch-java";

    private static final ObjectMapper JSON = new ObjectMapper();

    static Path conformanceDir() {
        return Path.of(System.getProperty("user.dir"), "..", "conformance").normalize();
    }

    // -- conformance/actions.json ---------------------------------------------------------------

    /**
     * An {@code expectedUnsupported} entry. {@code messages} carries one entry per adapter that
     * must reject the shape, keyed by adapter name; {@code validate-corpus.sh} asserts that key
     * set is exactly the roster minus the adapters that promoted the shape.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record UnsupportedShape(String action, String shape, Map<String, String> messages) {}

    /**
     * A {@code nullRepresentationOmitted} entry. Every adapter must reject these — the two NULL
     * conventions are indistinguishable on the wire — so {@code messages} names the whole roster
     * with no promotions to subtract.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record NullRepresentationOmitted(String action, String reason, Map<String, String> messages) {}

    /**
     * An {@code adapterUnsupported} / {@code adapterSupportedExpected} entry. {@code message} is
     * the substring this adapter's error must contain — present on the first, absent on the
     * second, which does not throw.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record AdapterUnsupported(String action, String reason, String message) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record KnownDivergence(String action, String reason, List<String> adapters) {}

    /**
     * Every group in actions.json must be named here: Jackson silently drops a field this
     * record does not declare, and a dropped group makes its actions vanish from every count
     * and every parameterised case at once — the projection trap conformance/README.md warns
     * about. The manifest tripwire in each suite is what makes an undropped group load-bearing.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record ActionsFile(
            List<String> conformance,
            Map<String, List<AdapterUnsupported>> adapterUnsupported,
            Map<String, List<AdapterUnsupported>> adapterSupportedExpected,
            List<UnsupportedShape> expectedUnsupported,
            List<NullRepresentationOmitted> nullRepresentationOmitted,
            List<KnownDivergence> knownDivergences) {

        List<AdapterUnsupported> adapterUnsupportedFor(String adapter) {
            return adapterUnsupported == null
                    ? List.of()
                    : adapterUnsupported.getOrDefault(adapter, List.of());
        }

        List<AdapterUnsupported> adapterSupportedExpectedFor(String adapter) {
            return adapterSupportedExpected == null
                    ? List.of()
                    : adapterSupportedExpected.getOrDefault(adapter, List.of());
        }

        /** Every action the corpus declares, in any group. */
        Set<String> manifestActions() {
            Set<String> manifest = new TreeSet<>(conformance);
            expectedUnsupported.forEach(u -> manifest.add(u.action()));
            nullRepresentationOmitted.forEach(n -> manifest.add(n.action()));
            knownDivergences.forEach(d -> manifest.add(d.action()));
            return manifest;
        }

        Set<String> skippedDivergences(String adapter) {
            return knownDivergences.stream()
                    .filter(d -> d.adapters().contains(adapter))
                    .map(KnownDivergence::action)
                    .collect(Collectors.toCollection(TreeSet::new));
        }
    }

    static ActionsFile actionsFile() {
        try {
            return JSON.readValue(
                    conformanceDir().resolve("actions.json").toFile(), ActionsFile.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * The substring this adapter's error must contain, or a loud failure. The message is what
     * turns "it threw" into "it threw for the declared reason": without it a mapper typo or an
     * unrelated validation satisfies the throw suite just as well as the documented limitation
     * (cerbos/query-plan-adapters#326).
     */
    static String requireMessage(String label, String message) {
        if (message == null || message.isEmpty()) {
            throw new IllegalStateException("actions.json pins no throw message for " + label
                    + ": the throw suite would accept a failure for any reason");
        }
        return message;
    }

    /** Actions this adapter oracle-compares: conformance minus its own unsupported, plus promotions. */
    static Stream<String> oracleActions(ActionsFile actions, String adapter) {
        Set<String> unsupported = actions.adapterUnsupportedFor(adapter).stream()
                .map(AdapterUnsupported::action)
                .collect(Collectors.toSet());
        return Stream.concat(
                actions.conformance().stream().filter(a -> !unsupported.contains(a)),
                actions.adapterSupportedExpectedFor(adapter).stream()
                        .map(AdapterUnsupported::action).sorted());
    }

    /**
     * Every action this adapter must refuse, each with the message it must refuse it with:
     * {@code adapterUnsupported[me]} plus {@code expectedUnsupported} minus its own promotions.
     *
     * <p>{@code nullRepresentationOmitted} is deliberately NOT folded in here — the harness keeps
     * it a separate classification because the corpus does ({@link #nullRepresentationThrows}).
     * On this adapter it happens to throw unconditionally, which is a fact about Elasticsearch
     * rather than about the group, and {@link ElasticsearchTranslatorTest} states it as one.
     */
    static Map<String, String> throwingActions(ActionsFile actions, String adapter) {
        Set<String> promoted = actions.adapterSupportedExpectedFor(adapter).stream()
                .map(AdapterUnsupported::action)
                .collect(Collectors.toSet());
        Map<String, String> throwing = new TreeMap<>();
        for (AdapterUnsupported entry : actions.adapterUnsupportedFor(adapter)) {
            throwing.put(entry.action(), requireMessage(
                    "adapterUnsupported." + adapter + "." + entry.action(), entry.message()));
        }
        for (UnsupportedShape entry : actions.expectedUnsupported()) {
            if (promoted.contains(entry.action())) {
                continue;
            }
            throwing.put(entry.action(), requireMessage(
                    "expectedUnsupported." + entry.action() + ".messages." + adapter,
                    entry.messages() == null ? null : entry.messages().get(adapter)));
        }
        return throwing;
    }

    /** The {@code nullRepresentationOmitted} probes, each with the message its rejection must carry. */
    static List<NullRepresentationOmitted> nullRepresentationThrows(ActionsFile actions) {
        return actions.nullRepresentationOmitted();
    }

    static String nullOmittedMessage(NullRepresentationOmitted entry, String adapter) {
        return requireMessage(
                "nullRepresentationOmitted." + entry.action() + ".messages." + adapter,
                entry.messages() == null ? null : entry.messages().get(adapter));
    }

    // -- conformance/wire-fixtures/ -------------------------------------------------------------

    /**
     * The instant {@code regenerate-wire-fixtures.sh} substitutes for the one operand it cannot
     * pin.
     *
     * <p>{@code ts-window} and {@code ts-vf} compare against {@code now() - duration("24h")},
     * which the planner folds to a literal timestamp: a different value on every capture, so the
     * script rewrites it to {@code __NOW_MINUS_24H__} to keep the drift check deterministic.
     * Reading a fixture back therefore means CHOOSING a value, and on THIS adapter the choice
     * decides the classification rather than only the recorded bytes: the adapter refuses a
     * timestamp literal carrying sub-millisecond precision, which is exactly what
     * {@code actions.json} declares those two actions unsupported for. A tidy millisecond
     * substitution would translate cleanly and quietly contradict the corpus.
     *
     * <p>Nanosecond precision, deliberately, and the same instant {@code sqlalchemy/tests/corpus.py}
     * and spring-data's loader chose: the PDP emits nanoseconds.
     * {@link ElasticsearchTranslatorTest} asserts BOTH directions of that dependency, so this
     * stays a decision rather than an accident.
     */
    static final String PLANNED_AT = "2026-08-11T09:13:39.123456789Z";

    private static final String NOW_MINUS_24H = "__NOW_MINUS_24H__";

    /** Every action the corpus has a golden wire fixture for, sorted. */
    static List<String> wireFixtureActions() {
        try (Stream<Path> files = Files.list(conformanceDir().resolve("wire-fixtures"))) {
            return files.map(p -> p.getFileName().toString())
                    .filter(name -> name.endsWith(".json"))
                    .map(name -> name.substring(0, name.length() - ".json".length()))
                    .sorted()
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * The plan the pinned PDP produced for {@code action}, decoded into the protobuf response
     * the SDK hands a caller.
     *
     * <p>The fixture IS the PDP's HTTP response body, so the decoding here is protobuf's own
     * canonical JSON mapping ({@link JsonFormat}) — the same mapping the PDP's HTTP API writes.
     * It is deliberately not a hand-built plan: a plan somebody typed is a BELIEF about what the
     * planner emits, and this repository keeps fixtures precisely because that belief has been
     * wrong before. See {@code docs/adr/0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md}.
     */
    static PlanResourcesResponse planFromWireFixture(String action) {
        return planFromWireFixture(action, PLANNED_AT);
    }

    static PlanResourcesResponse planFromWireFixture(String action, String plannedAt) {
        Path fixture = conformanceDir().resolve("wire-fixtures").resolve(action + ".json");
        try {
            JsonNode filter = JSON.readTree(fixture.toFile()).get("filter");
            if (filter == null) {
                throw new IllegalStateException(fixture + " carries no filter");
            }
            PlanResourcesFilter.Builder builder = PlanResourcesFilter.newBuilder();
            JsonFormat.parser().merge(
                    filter.toString().replace(NOW_MINUS_24H, plannedAt), builder);
            return PlanResourcesResponse.newBuilder().setFilter(builder).build();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // -- the corpus mapped onto the index --------------------------------------------------------

    /**
     * The corpus's field map. Read by BOTH corpus suites: the harness executes the queries it
     * produces against a seeded index, and the translator suite pins them.
     *
     * <p>Every level of every path is mapped even where the action is fail-closed. An unmapped
     * field throws {@code "Unknown attribute"} instead of the mechanism {@code actions.json}
     * names, which passes the throw test for the wrong reason
     * (cerbos/query-plan-adapters#326).
     */
    static final Map<String, String> FIELD_MAP = Map.ofEntries(
            // The primary key, reached as `request.resource.id` rather than through `attr` (the
            // `id-*` actions). It maps onto the indexed `id` keyword field rather than
            // Elasticsearch's own `_id` metadata field: an adapter that resolves references by
            // stripping a `request.resource.attr.` prefix never sees this name at all.
            Map.entry("request.resource.id", "id"),
            Map.entry("request.resource.attr.aBool", "aBool"),
            Map.entry("request.resource.attr.aString", "aString"),
            Map.entry("request.resource.attr.aNumber", "aNumber"),
            Map.entry("request.resource.attr.aDouble", "aDouble"),
            Map.entry("request.resource.attr.aOptionalString", "aOptionalString"),
            Map.entry("request.resource.attr.createdBy", "createdBy"),
            Map.entry("request.resource.attr.createdAt", "createdAt"),
            Map.entry("request.resource.attr.owner", "owner"),
            // `coOwner` is the explicit-null alias of the `scope` field, the second half of
            // `null-value-f2f`: `scope` itself is omitted when NULL, so the corpus carries the
            // same field under both conventions (cerbos/query-plan-adapters#308).
            Map.entry("request.resource.attr.coOwner", "coOwner"),
            Map.entry("request.resource.attr.scope", "scope"),
            Map.entry("request.resource.attr.obj.inner", "obj.inner"),
            Map.entry("request.resource.attr.tags", "tags"),
            Map.entry("request.resource.attr.tagNames", "tagNames"),
            Map.entry("request.resource.attr.categories", "categories"),
            Map.entry("request.resource.attr.mainCategory.subCategories", "mainCategory.subCategories"),
            Map.entry("request.resource.attr.mainCategory.subNames", "mainCategory.subNames"),
            // The corpus's one REAL to-one chain (the `rel-*` actions), indexed as plain objects
            // because Elasticsearch has no join.
            Map.entry("request.resource.attr.parent.aBool", "parent.aBool"),
            Map.entry("request.resource.attr.parent.aString", "parent.aString"),
            Map.entry("request.resource.attr.parent.aNumber", "parent.aNumber"),
            Map.entry("request.resource.attr.parent.aOptionalString", "parent.aOptionalString"),
            Map.entry("request.resource.attr.parent.inner.aBool", "parent.inner.aBool"),
            Map.entry("request.resource.attr.parent.inner.aString", "parent.inner.aString"),
            Map.entry("request.resource.attr.parent.inner.aNumber", "parent.inner.aNumber"),
            Map.entry("request.resource.attr.parent.inner.aOptionalString", "parent.inner.aOptionalString"));

    /**
     * The attributes the corpus sends to {@code check()} as EXPLICIT nulls
     * (cerbos/query-plan-adapters#308). Elasticsearch cannot represent that convention — a JSON
     * null is not indexed, so an explicitly-null value and a missing field are the same document
     * — so declaring them here is what turns a narrow answer into a refusal.
     */
    static final Set<String> EXPLICIT_NULL_ATTRIBUTES = Set.of(
            "request.resource.attr.owner", "request.resource.attr.coOwner");

    /** The field paths the corpus index maps as Elasticsearch {@code nested} documents. */
    static final Set<String> NESTED_PATHS = Set.of(
            "tags", "mainCategory.subCategories",
            "categories", "categories.subCategories", "categories.subCategories.labels");

    /** Translates one corpus action exactly as the harness does. */
    static ElasticsearchQueryPlanAdapter.Result translate(String action) {
        return translate(planFromWireFixture(action));
    }

    static ElasticsearchQueryPlanAdapter.Result translate(PlanResourcesResponse plan) {
        return ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                plan, FIELD_MAP, Map.of(), NESTED_PATHS, EXPLICIT_NULL_ATTRIBUTES);
    }

    // -- elasticsearch-java/golden/expectations.json ---------------------------------------------

    /** The reserved key an entry may carry alongside its expectation; never compared. */
    static final String NOTE_KEY = "note";

    /** The command that rewrites the asset. Documentation that travels with the data. */
    static final String GOLDEN_REGENERATE_COMMAND = "gradle goldenUpdate";

    static Path goldenFile() {
        return Path.of(System.getProperty("user.dir"), "golden", "expectations.json").normalize();
    }

    /**
     * The emitted query as JSON, with every object's keys sorted.
     *
     * <p><strong>There is no rendering step here, and that is the whole serialisation decision.</strong>
     * This adapter's {@code Result.Conditional} carries a {@code Map<String, Object>} of plain JDK
     * values — the Elasticsearch Query DSL IS JSON, and no Elasticsearch client library is on the
     * classpath to turn it into anything else. So the entry records the translator's return value
     * verbatim: no dialect, no compiler, no generator to declare in the header the way sqlalchemy
     * and spring-data must ({@code conformance/README.md}, "When the generator is an input").
     * {@link ElasticsearchTranslatorTest} asserts that claim rather than only stating it.
     *
     * <p>Sorting is the one normalisation, and it is about the FILE rather than the value: the
     * adapter builds its queries with {@link Map#of}, whose iteration order is randomised per JVM
     * run, so unsorted bytes would produce a fresh diff on every regeneration. A JSON object is
     * unordered, and the comparison itself is order-independent either way — Jackson's
     * {@code ObjectNode} equality is a map comparison. Arrays keep their order untouched, which is
     * what matters: {@code bool.must} clause order is part of the value.
     */
    static JsonNode canonicalJson(Object value) {
        rejectNonFinite(value, value);
        try {
            // Written and READ BACK, rather than converted in memory: a JSON text is what the
            // asset holds and what a deployed caller puts in a request body, so the recorded node
            // is the one a parser produces rather than the one an in-memory conversion would.
            return sortKeys(JSON.readTree(JSON.writeValueAsString(value)));
        } catch (IOException e) {
            throw new IllegalStateException("the emitted query does not survive a JSON round trip,"
                    + " so neither this adapter's golden asset nor the request body a deployed"
                    + " caller sends could carry it: " + value, e);
        }
    }

    /**
     * Refuse a non-finite number before it is written, because the round trip would NOT catch it.
     *
     * <p>JSON has no NaN and no infinity, and Jackson's {@code QUOTE_NON_NUMERIC_NUMBERS} is on by
     * default — so writing one produces the STRING {@code "NaN"}, which parses back cleanly and
     * has silently stopped being a number. The asset would record a term query against a string,
     * and so would the request body a deployed caller sends. This adapter refuses every arithmetic
     * shape today, so nothing in the corpus reaches it; the guard is here for the lowering that
     * one day does.
     */
    private static void rejectNonFinite(Object node, Object whole) {
        if (node instanceof Map<?, ?> map) {
            map.values().forEach(child -> rejectNonFinite(child, whole));
        } else if (node instanceof List<?> list) {
            list.forEach(child -> rejectNonFinite(child, whole));
        } else if (node instanceof Double number && !Double.isFinite(number)) {
            throw new IllegalStateException("the emitted query binds " + number + ", which JSON"
                    + " cannot carry: it would be written as a quoted string and stop being a"
                    + " number in this adapter's golden asset and in a caller's request body"
                    + " alike: " + whole);
        }
    }

    private static JsonNode sortKeys(JsonNode node) {
        if (node.isObject()) {
            ObjectNode sorted = JSON.createObjectNode();
            node.properties().stream().map(Map.Entry::getKey).sorted()
                    .forEach(field -> sorted.set(field, sortKeys(node.get(field))));
            return sorted;
        }
        if (node.isArray()) {
            ArrayNode rebuilt = JSON.createArrayNode();
            node.forEach(element -> rebuilt.add(sortKeys(element)));
            return rebuilt;
        }
        return node;
    }

    /**
     * The golden expectations, keyed by action, in file order, each with its {@code note}
     * removed — commentary is carried across regeneration and never compared.
     *
     * <p>{@code adapter} is checked rather than ignored: the file is a flat map of action names,
     * so a copy taken from another adapter parses cleanly and would be compared against this
     * adapter's output with only the diff to say something went wrong.
     */
    static Map<String, ObjectNode> readGoldenExpectations() {
        JsonNode contents;
        try {
            contents = JSON.readTree(goldenFile().toFile());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        String adapter = contents.path("adapter").asText(null);
        if (!ADAPTER.equals(adapter)) {
            throw new IllegalStateException(goldenFile() + " declares adapter \"" + adapter
                    + "\", not \"" + ADAPTER + "\"");
        }
        Map<String, ObjectNode> recorded = new LinkedHashMap<>();
        for (Map.Entry<String, JsonNode> entry : contents.get("expectations").properties()) {
            ObjectNode value = ((ObjectNode) entry.getValue()).deepCopy();
            value.remove(NOTE_KEY);
            recorded.put(entry.getKey(), value);
        }
        return recorded;
    }

    /**
     * Rewrite the golden expectations, carrying every existing {@code note} across.
     *
     * <p>Only ever called under {@code -Dgolden.update=true} ({@code gradle goldenUpdate}).
     * Regeneration is the same deliberate act as {@code conformance/scripts/regenerate-wire-fixtures.sh},
     * with the same safety: the diff is what a reviewer reads, which is why the entries are
     * written sorted and one action per key. CI never sets the property.
     *
     * <p>A missing file is not an error here, and only here — that is how a new adapter
     * bootstraps one. Reading a missing file for an assertion stays an error, because a suite
     * that quietly asserts nothing is the failure mode the completeness guard exists to prevent.
     */
    static void writeGoldenExpectations(Map<String, ObjectNode> expectations) {
        // Notes are read WITHOUT the header validation `readGoldenExpectations` applies. The file
        // about to be overwritten may legitimately carry an older header — that is what a header
        // change looks like — and refusing to carry the commentary across because of one would
        // make every such change silently drop it.
        Map<String, String> notes = new LinkedHashMap<>();
        if (Files.exists(goldenFile())) {
            JsonNode existing;
            try {
                existing = JSON.readTree(goldenFile().toFile());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            for (Map.Entry<String, JsonNode> entry : existing.path("expectations").properties()) {
                if (entry.getValue().has(NOTE_KEY)) {
                    notes.put(entry.getKey(), entry.getValue().get(NOTE_KEY).asText());
                }
            }
        }

        ObjectNode root = JSON.createObjectNode();
        root.put("adapter", ADAPTER);
        root.put("regenerate", GOLDEN_REGENERATE_COMMAND);
        ObjectNode body = root.putObject("expectations");
        for (String action : new TreeSet<>(expectations.keySet())) {
            ObjectNode entry = JSON.createObjectNode();
            if (notes.containsKey(action)) {
                entry.put(NOTE_KEY, notes.get(action));
            }
            entry.setAll(expectations.get(action));
            body.set(action, entry);
        }

        try {
            Files.createDirectories(goldenFile().getParent());
            Files.writeString(goldenFile(), JSON.writer(prettyPrinter()).writeValueAsString(root)
                    + "\n", StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Two-space indent, no space before a colon, LF line endings — the other assets' shape. */
    private static DefaultPrettyPrinter prettyPrinter() {
        DefaultIndenter indenter = new DefaultIndenter("  ", "\n");
        return new DefaultPrettyPrinter()
                .withObjectIndenter(indenter)
                .withArrayIndenter(indenter)
                .withSeparators(new Separators()
                        .withObjectFieldValueSpacing(Separators.Spacing.AFTER));
    }
}
