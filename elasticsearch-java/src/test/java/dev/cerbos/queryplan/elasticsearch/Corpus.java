/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

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
 * Reads the shared {@code ../conformance/} corpus and this adapter's golden expectations.
 *
 * <p>Every adapter keeps its own copy of this loader; do not extract a shared one (ADR 0007).
 * {@link ElasticsearchTranslatorTest} and {@link ElasticsearchAdversarialConformanceTest} both
 * translate through {@link #OPTIONS}, so they test the same query.
 */
final class Corpus {

    private Corpus() {}

    /** This adapter's key in the corpus files: its directory name. */
    static final String ADAPTER = "elasticsearch-java";

    private static final ObjectMapper JSON = new ObjectMapper();

    static Path conformanceDir() {
        return Path.of(System.getProperty("user.dir"), "..", "conformance").normalize();
    }

    // -- conformance/actions.json ---------------------------------------------------------------

    /**
     * An {@code expectedUnsupported} entry. {@code messages} is keyed by adapter name and holds
     * the message each adapter that rejects the shape must raise.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record UnsupportedShape(String action, String shape, Map<String, String> messages) {}

    /**
     * A {@code nullRepresentationOmitted} entry. Every adapter must reject these, because the two
     * NULL conventions look the same on the wire.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record NullRepresentationOmitted(String action, String reason, Map<String, String> messages) {}

    /**
     * An {@code adapterUnsupported} or {@code adapterSupportedExpected} entry. {@code message} is
     * the substring this adapter's error must contain; entries of the second kind do not throw
     * and carry none.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record AdapterUnsupported(String action, String reason, String message) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record KnownDivergence(String action, String reason, List<String> adapters) {}

    /**
     * A {@code degenerateOracles} entry: an action whose {@code check()} oracle is empty or total
     * by construction. {@code oracle} is {@code "empty"} or {@code "total"}.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record DegenerateOracle(String action, String oracle, String reason) {}

    /**
     * Maps each declared degenerate action to {@code "empty"} or {@code "total"}. A listed action
     * must have exactly that oracle; every other compared action must have a non-empty,
     * non-total one.
     */
    static Map<String, String> degenerateOracleShapes(List<DegenerateOracle> entries) {
        if (entries == null) {
            throw new IllegalStateException(
                    "actions.json declares no degenerateOracles: the degeneracy sweep would"
                            + " exempt nothing and fail every by-construction oracle");
        }
        Map<String, String> shapes = new TreeMap<>();
        for (DegenerateOracle entry : entries) {
            if (!"empty".equals(entry.oracle()) && !"total".equals(entry.oracle())) {
                throw new IllegalStateException("degenerateOracles." + entry.action()
                        + " declares oracle '" + entry.oracle()
                        + "': it must be \"empty\" or \"total\"");
            }
            if (shapes.put(entry.action(), entry.oracle()) != null) {
                throw new IllegalStateException(
                        "degenerateOracles lists '" + entry.action() + "' twice");
            }
        }
        return shapes;
    }

    /**
     * Declares every group in actions.json. Jackson silently drops an undeclared field, which
     * would remove that group's actions from every count and test case.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record ActionsFile(
            List<String> conformance,
            Map<String, List<AdapterUnsupported>> adapterUnsupported,
            Map<String, List<AdapterUnsupported>> adapterSupportedExpected,
            List<UnsupportedShape> expectedUnsupported,
            List<NullRepresentationOmitted> nullRepresentationOmitted,
            List<KnownDivergence> knownDivergences,
            List<DegenerateOracle> degenerateOracles) {

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
     * Returns the pinned message, or throws if there is none. Without a message, any exception
     * (for example a mapper typo) would pass the throw test.
     */
    static String requireMessage(String label, String message) {
        if (message == null || message.isEmpty()) {
            throw new IllegalStateException("actions.json pins no throw message for " + label
                    + ": the throw suite would accept a failure for any reason");
        }
        return message;
    }

    /** Actions this adapter compares against the oracle: conformance minus its unsupported, plus promotions. */
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
     * Every action this adapter must refuse, with its message: {@code adapterUnsupported[me]}
     * plus {@code expectedUnsupported} minus its own promotions.
     *
     * <p>{@code nullRepresentationOmitted} stays separate, as it is in the corpus; see
     * {@link #nullRepresentationThrows}.
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
     * The instant substituted for {@code __NOW_MINUS_24H__} in the wire fixtures.
     *
     * <p>{@code ts-window} and {@code ts-vf} compare against {@code now() - duration("24h")}, which
     * the regenerate script replaces with that placeholder. The value must have nanosecond
     * precision, as the PDP emits: this adapter refuses sub-millisecond timestamps, and that
     * refusal is why {@code actions.json} marks those two actions unsupported.
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
     * The plan the pinned PDP produced for {@code action}. The fixture is the PDP's HTTP response
     * body, decoded with protobuf's JSON mapping ({@link JsonFormat}). See ADR 0006.
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
     * The corpus field map, used by both corpus suites.
     *
     * <p>Every path is mapped, including ones only fail-closed actions read. An unmapped field
     * throws {@code "Unknown attribute"} instead of the message {@code actions.json} pins.
     */
    static final Map<String, String> FIELD_MAP = Map.ofEntries(
            // The primary key (`request.resource.id`), mapped to the indexed `id` keyword field
            // rather than Elasticsearch's `_id` metadata field.
            Map.entry("request.resource.id", "id"),
            Map.entry("request.resource.attr.aBool", "aBool"),
            Map.entry("request.resource.attr.aString", "aString"),
            Map.entry("request.resource.attr.aNumber", "aNumber"),
            Map.entry("request.resource.attr.aDouble", "aDouble"),
            Map.entry("request.resource.attr.aOptionalString", "aOptionalString"),
            Map.entry("request.resource.attr.createdBy", "createdBy"),
            Map.entry("request.resource.attr.createdAt", "createdAt"),
            Map.entry("request.resource.attr.updatedAt", "updatedAt"),
            Map.entry("request.resource.attr.owner", "owner"),
            // `coOwner` is the explicit-null alias of `scope`, which is omitted when NULL, so the
            // corpus carries the same field under both conventions (`null-value-f2f`).
            Map.entry("request.resource.attr.coOwner", "coOwner"),
            Map.entry("request.resource.attr.scope", "scope"),
            Map.entry("request.resource.attr.obj.inner", "obj.inner"),
            Map.entry("request.resource.attr.tags", "tags"),
            Map.entry("request.resource.attr.tagNames", "tagNames"),
            // Flat number and boolean arrays, like tagNames.
            Map.entry("request.resource.attr.aNumberList", "aNumberList"),
            Map.entry("request.resource.attr.aBoolList", "aBoolList"),
            Map.entry("request.resource.attr.categories", "categories"),
            Map.entry("request.resource.attr.mainCategory.subCategories", "mainCategory.subCategories"),
            Map.entry("request.resource.attr.mainCategory.subNames", "mainCategory.subNames"),
            // The corpus's to-one relation (`rel-*` actions), indexed as plain objects because
            // Elasticsearch has no join.
            Map.entry("request.resource.attr.parent.aBool", "parent.aBool"),
            Map.entry("request.resource.attr.parent.aString", "parent.aString"),
            Map.entry("request.resource.attr.parent.aNumber", "parent.aNumber"),
            Map.entry("request.resource.attr.parent.aOptionalString", "parent.aOptionalString"),
            Map.entry("request.resource.attr.parent.inner.aBool", "parent.inner.aBool"),
            Map.entry("request.resource.attr.parent.inner.aString", "parent.inner.aString"),
            Map.entry("request.resource.attr.parent.inner.aNumber", "parent.inner.aNumber"),
            Map.entry("request.resource.attr.parent.inner.aOptionalString", "parent.inner.aOptionalString"));

    /**
     * Attributes the corpus sends to {@code check()} as explicit nulls. Elasticsearch does not
     * index a JSON null, so an explicit null and a missing field look the same; declaring these
     * makes the adapter refuse instead of answering.
     */
    static final Set<String> EXPLICIT_NULL_ATTRIBUTES = Set.of(
            "request.resource.attr.owner", "request.resource.attr.coOwner");

    /** The field paths the corpus index maps as Elasticsearch {@code nested} documents. */
    static final Set<String> NESTED_PATHS = Set.of(
            "tags", "mainCategory.subCategories",
            "categories", "categories.subCategories", "categories.subCategories.labels");

    /**
     * The field paths the corpus index maps as flat arrays of scalars. The adapter cannot tell
     * {@code size(aString)} from {@code size(tagNames)} by itself, so it refuses {@code size()} on
     * a field declared neither here nor in {@link #NESTED_PATHS}.
     */
    static final Set<String> COLLECTION_FIELDS = Set.of("tagNames", "aNumberList", "aBoolList");

    /**
     * The CEL scalar type of every scalar field in the corpus index, keyed by Elasticsearch field
     * name. The adapter refuses a comparison against an undeclared field, because Elasticsearch
     * coerces a query term to the field's mapped type where CEL's cross-type equality is false.
     */
    static final Map<String, ElasticsearchQueryPlanAdapter.ScalarType> SCALAR_TYPES = scalarTypes();

    private static Map<String, ElasticsearchQueryPlanAdapter.ScalarType> scalarTypes() {
        ElasticsearchQueryPlanAdapter.ScalarType string = ElasticsearchQueryPlanAdapter.ScalarType.STRING;
        ElasticsearchQueryPlanAdapter.ScalarType number = ElasticsearchQueryPlanAdapter.ScalarType.NUMBER;
        ElasticsearchQueryPlanAdapter.ScalarType bool = ElasticsearchQueryPlanAdapter.ScalarType.BOOLEAN;
        ElasticsearchQueryPlanAdapter.ScalarType timestamp = ElasticsearchQueryPlanAdapter.ScalarType.TIMESTAMP;
        Map<String, ElasticsearchQueryPlanAdapter.ScalarType> types = new LinkedHashMap<>();
        types.put("id", string);
        types.put("aString", string);
        types.put("aOptionalString", string);
        types.put("aNumber", number);
        types.put("aDouble", number);
        types.put("aBool", bool);
        types.put("createdAt", timestamp);
        types.put("updatedAt", timestamp);
        types.put("createdBy", string);
        types.put("scope", string);
        types.put("owner", string);
        types.put("coOwner", string);
        types.put("obj.inner", string);
        types.put("tagNames", string);
        types.put("aNumberList", number);
        types.put("aBoolList", bool);
        types.put("mainCategory.subNames", string);
        types.put("mainCategory.subCategories.name", string);
        types.put("tags.id", string);
        types.put("tags.name", string);
        types.put("categories.name", string);
        types.put("categories.subCategories.name", string);
        types.put("categories.subCategories.labels.name", string);
        for (String level : List.of("parent.", "parent.inner.")) {
            types.put(level + "aBool", bool);
            types.put(level + "aString", string);
            types.put(level + "aNumber", number);
            types.put(level + "aOptionalString", string);
        }
        return Map.copyOf(types);
    }

    /** The options both corpus suites translate through. */
    static final ElasticsearchQueryPlanAdapter.Options OPTIONS =
            ElasticsearchQueryPlanAdapter.Options.of(FIELD_MAP)
                    .withNestedPaths(NESTED_PATHS)
                    .withScalarTypes(SCALAR_TYPES)
                    .withCollectionFields(COLLECTION_FIELDS)
                    .withExplicitNullAttributes(EXPLICIT_NULL_ATTRIBUTES);

    /** Translates one corpus action exactly as the harness does. */
    static ElasticsearchQueryPlanAdapter.Result translate(String action) {
        return translate(planFromWireFixture(action));
    }

    static ElasticsearchQueryPlanAdapter.Result translate(PlanResourcesResponse plan) {
        return ElasticsearchQueryPlanAdapter.toElasticsearchQuery(plan, OPTIONS);
    }

    // -- elasticsearch-java/golden/expectations.json ---------------------------------------------

    /** Optional commentary key on a golden entry; never compared. */
    static final String NOTE_KEY = "note";

    /** The command that rewrites the golden file. */
    static final String GOLDEN_REGENERATE_COMMAND = "./gradlew goldenUpdate";

    static Path goldenFile() {
        return Path.of(System.getProperty("user.dir"), "golden", "expectations.json").normalize();
    }

    /**
     * The emitted query as JSON, with every object's keys sorted.
     *
     * <p>No rendering is needed: the adapter returns plain JDK maps and lists, which are already
     * Query DSL JSON. Keys are sorted because {@link Map#of} iteration order changes per JVM run.
     * Array order is kept, since it is part of the value.
     */
    static JsonNode canonicalJson(Object value) {
        rejectNonFinite(value, value);
        try {
            // Round-trip through JSON text, so the result is what a parser of the request body sees.
            return sortKeys(JSON.readTree(JSON.writeValueAsString(value)));
        } catch (IOException e) {
            throw new IllegalStateException("the emitted query does not survive a JSON round trip,"
                    + " so neither this adapter's golden asset nor the request body a deployed"
                    + " caller sends could carry it: " + value, e);
        }
    }

    /**
     * Rejects NaN and infinity. Jackson writes them as quoted strings, which parse back cleanly as
     * strings, so the round trip alone would not catch them.
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
     * The golden expectations keyed by action, in file order, with each {@code note} removed.
     * Throws if the file's {@code adapter} is not this adapter, to catch a copy from another one.
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
     * Rewrites the golden expectations, keeping every existing {@code note}. Called only under
     * {@code -Dgolden.update=true} ({@code ./gradlew goldenUpdate}); CI never sets it.
     *
     * <p>A missing file is allowed here, so a new file can be bootstrapped.
     */
    static void writeGoldenExpectations(Map<String, ObjectNode> expectations) {
        // Read notes without the header check, since the file being replaced may have an older
        // header.
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

    /** Two-space indent, no space before a colon, LF line endings. */
    private static DefaultPrettyPrinter prettyPrinter() {
        DefaultIndenter indenter = new DefaultIndenter("  ", "\n");
        return new DefaultPrettyPrinter()
                .withObjectIndenter(indenter)
                .withArrayIndenter(indenter)
                .withSeparators(new Separators()
                        .withObjectFieldValueSpacing(Separators.Spacing.AFTER));
    }
}
