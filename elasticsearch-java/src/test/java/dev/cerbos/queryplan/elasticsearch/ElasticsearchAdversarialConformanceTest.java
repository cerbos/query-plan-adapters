/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Result;
import dev.cerbos.sdk.CerbosBlockingClient;
import dev.cerbos.sdk.PlanResourcesResult;
import dev.cerbos.sdk.builders.AttributeValue;
import dev.cerbos.sdk.builders.Principal;
import dev.cerbos.sdk.builders.Resource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.images.builder.Transferable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Differential suite over the shared corpus. A pinned PDP supplies both the query plan and the
 * per-row {@code check()} oracle, and the emitted query runs on a real Elasticsearch. Needs Docker.
 */
class ElasticsearchAdversarialConformanceTest {

    private static final String INDEX = "adversarial";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // The translation options live in Corpus.OPTIONS, shared with ElasticsearchTranslatorTest, so
    // both suites test the same query.

    private static Path conformanceDir() {
        return Path.of(System.getProperty("user.dir"), "..", "conformance").normalize();
    }

    private record Tag(String id, String name) {}

    /**
     * One corpus row. {@code note} is documentation and never read.
     *
     * <p>{@code aNumberList} and {@code aBoolList} elements are boxed so a null element survives:
     * CEL treats {@code [null, 2]} differently from {@code [2]}.
     */
    private record Seed(String id, boolean aBool, String aString, int aNumber,
                        String aOptionalString, List<Double> aNumberList, List<Boolean> aBoolList,
                        List<Tag> tags, List<String> subCategoryNames,
                        String parentSeedId, String note) {}

    /**
     * {@code attr} is raw JSON so the corpus may carry scalars, lists and structs;
     * {@link #principal()} converts each value by its JSON type.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record PrincipalSpec(String id, List<String> roles, Map<String, Object> attr) {}

    /**
     * conformance/seeds.json. Every key is named and unknown keys are rejected, so a new seed field
     * cannot be silently dropped from both the index and the oracle.
     */
    private record SeedsFile(@JsonProperty("$schema") String schema, String description,
                             PrincipalSpec principal, String resourceKind, String principalNote,
                             String relationNote, List<Seed> seeds) {}

    /** One seed's derived fields, exactly as conformance/derived-fields.json carries them. */
    private record DerivedEntry(String createdBy, Double aDouble, String createdAt, String updatedAt, String scope,
                                List<String> labels) {}

    private record DerivedFile(@JsonProperty("$schema") String schema, String description,
                               List<String> fields, Map<String, DerivedEntry> derived) {}

    /**
     * An {@code expectedUnsupported} entry. {@code messages} is keyed by adapter name and holds
     * the message each adapter that rejects the shape must raise.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record UnsupportedShape(String action, Map<String, String> messages) {}

    /**
     * An {@code adapterUnsupported}, {@code adapterSupportedExpected} or
     * {@code nullRepresentationOmitted} entry. {@code message} is this adapter's pinned substring on
     * the first; the last uses {@code messages}, keyed by adapter.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record AdapterOutcome(String action, String reason, String message,
                                 Map<String, String> messages) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record KnownDivergence(String action, List<String> adapters) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record ActionsFile(List<String> conformance,
                               Map<String, List<AdapterOutcome>> adapterUnsupported,
                               Map<String, List<AdapterOutcome>> adapterSupportedExpected,
                               List<UnsupportedShape> expectedUnsupported,
                               List<AdapterOutcome> nullRepresentationOmitted,
                               List<KnownDivergence> knownDivergences,
                               List<Corpus.DegenerateOracle> degenerateOracles) {}

    // -- corpus coverage guards -----------------------------------------------------------------
    //
    // The same parsed data feeds the index and the check() oracle, so a corpus field this harness
    // ignores would be dropped from both and the differential would still agree. These key lists
    // must equal the corpus's keys exactly.

    private static final List<String> SEED_KEYS = List.of(
            "id", "aBool", "aString", "aNumber", "aOptionalString", "aNumberList", "aBoolList",
            "tags", "subCategoryNames", "parentSeedId");

    /** Corpus prose, never read: the one key excluded from SEED_KEYS. */
    private static final String SEED_NOTE_KEY = "note";

    /** Keys of each element of a seed's {@code tags} array. */
    private static final List<String> TAG_KEYS = List.of("id", "name");

    private static final List<String> DERIVED_KEYS =
            List.of("createdBy", "aDouble", "createdAt", "updatedAt", "scope", "labels");

    // The principal feeds both the plan and the oracle, so a dropped attribute would vanish from
    // both sides too (conformance/README.md, "Adding a new hostile shape"). PrincipalSpec ignores
    // unknown properties so that this guard, not a decode error, reports an added key.

    private static final List<String> PRINCIPAL_KEYS = List.of("id", "roles", "attr");

    private static final List<String> PRINCIPAL_ATTR_KEYS =
            List.of("allowedTags", "context", "fewTeams", "manyTeams", "zero", "emptyTeams",
                    "manyStructs", "nullableStructs", "missingStructs");

    /** This adapter's key in the corpus files: its directory name. */
    private static final String ADAPTER = "elasticsearch-java";

    private static SeedsFile seedsFile;
    private static ActionsFile actionsFile;
    /** {@code degenerateOracles} from actions.json: action to {@code "empty"} or {@code "total"}. */
    private static Map<String, String> degenerateOracles;
    private static DerivedFile derivedFile;
    private static List<Seed> seeds;
    private static List<String> oracleActions;
    private static List<String> throwingActions;
    private static Map<String, String> throwingMessages;
    private static List<String> nullRepresentationOmittedActions;
    private static Map<String, String> nullRepresentationOmittedMessages;

    /**
     * Returns the pinned message, or throws if there is none. Without a message, any exception
     * (for example a mapper typo) would pass the throw test.
     */
    private static String requireMessage(String label, String message) {
        if (message == null || message.isEmpty()) {
            throw new IllegalStateException("actions.json pins no throw message for " + label
                    + ": the throw suite would accept a failure for any reason");
        }
        return message;
    }

    private static GenericContainer<?> cerbos;
    private static ElasticsearchContainer elasticsearch;
    private static CerbosBlockingClient client;
    private static TestElasticsearch es;

    static Stream<String> oracleActions() {
        return oracleActions.stream();
    }

    static Stream<String> throwingActions() {
        return throwingActions.stream();
    }

    static Stream<String> nullRepresentationOmittedActions() {
        return nullRepresentationOmittedActions.stream();
    }

    @BeforeAll
    static void setUp() throws Exception {
        Path conformance = conformanceDir();
        seedsFile = MAPPER.readValue(conformance.resolve("seeds.json").toFile(), SeedsFile.class);
        actionsFile = MAPPER.readValue(conformance.resolve("actions.json").toFile(), ActionsFile.class);
        degenerateOracles = Corpus.degenerateOracleShapes(actionsFile.degenerateOracles());
        derivedFile = MAPPER.readValue(
                conformance.resolve("derived-fields.json").toFile(), DerivedFile.class);
        seeds = seedsFile.seeds();
        assertCorpusCoverage(conformance);
        classifyActions();

        cerbos = new GenericContainer<>(CerbosTestImage.IMAGE)
                .withExposedPorts(3593)
                .withCommand("server", "--set=storage.disk.directory=/policies",
                        "--set=engine.strictEvaluation=" + CerbosTestImage.strictEvaluation())
                .withEnv("CERBOS_NO_TELEMETRY", "1")
                .waitingFor(Wait.forLogMessage(".*Starting gRPC server.*", 1));
        // Copy the whole policy directory, so a policy file added later is not silently missing.
        List<Path> policies = policyFiles(conformance.resolve("policies"));
        assertFalse(policies.isEmpty(), "conformance/policies/ holds no policy file");
        for (Path policy : policies) {
            String relative = conformance.resolve("policies").relativize(policy).toString();
            try {
                cerbos.withCopyToContainer(
                        Transferable.of(Files.readAllBytes(policy)), "/policies/" + relative);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        cerbos.start();
        CerbosTestImage.assertPinned(cerbos);
        client = CerbosTestImage.client(cerbos);

        elasticsearch = new ElasticsearchContainer(ElasticsearchTestImage.IMAGE)
                .withEnv("xpack.security.enabled", "false");
        elasticsearch.start();
        es = new TestElasticsearch(elasticsearch.getHttpHostAddress());
        createIndex();
        seedIndex();
        es.refresh(INDEX);
    }

    /** Every regular file under the policy directory, in a stable order. */
    private static List<Path> policyFiles(Path directory) throws IOException {
        try (Stream<Path> files = Files.walk(directory)) {
            return files.filter(Files::isRegularFile).sorted().toList();
        }
    }

    private static void classifyActions() {
        Set<String> conformance = Set.copyOf(actionsFile.conformance());
        Set<String> expected = actionsFile.expectedUnsupported().stream()
                .map(UnsupportedShape::action).collect(java.util.stream.Collectors.toSet());
        Set<String> unsupported = actionsFile.adapterUnsupported()
                .getOrDefault(ADAPTER, List.of()).stream()
                .map(AdapterOutcome::action).collect(java.util.stream.Collectors.toSet());
        Set<String> supportedExpected = actionsFile.adapterSupportedExpected()
                .getOrDefault(ADAPTER, List.of()).stream()
                .map(AdapterOutcome::action).collect(java.util.stream.Collectors.toSet());
        Set<String> divergences = actionsFile.knownDivergences().stream()
                .filter(divergence -> divergence.adapters().contains(ADAPTER))
                .map(KnownDivergence::action).collect(java.util.stream.Collectors.toSet());
        // `== null` probes on an attribute the oracle omits when NULL. Every adapter must reject
        // these, so each carries a message for this adapter.
        nullRepresentationOmittedActions = actionsFile.nullRepresentationOmitted().stream()
                .map(AdapterOutcome::action).sorted().toList();
        Map<String, String> nullMessages = new LinkedHashMap<>();
        actionsFile.nullRepresentationOmitted().forEach(outcome ->
                nullMessages.put(outcome.action(), requireMessage(
                        "nullRepresentationOmitted." + outcome.action() + ".messages." + ADAPTER,
                        outcome.messages() == null ? null : outcome.messages().get(ADAPTER))));
        nullRepresentationOmittedMessages = Map.copyOf(nullMessages);

        assertTrue(conformance.containsAll(unsupported),
                "adapterUnsupported.elasticsearch-java contains non-conformance actions");
        assertTrue(expected.containsAll(supportedExpected),
                "adapterSupportedExpected.elasticsearch-java contains non-expected actions");
        assertEquals(178, unsupported.size(),
                "Elasticsearch unsupported coverage changed without updating the ledger assertion");
        assertEquals(2, supportedExpected.size(),
                "Elasticsearch supported-expected coverage changed without updating the ledger assertion");
        assertEquals(Set.of("p-has"), divergences,
                "Elasticsearch planner divergences changed without updating the tripwire");

        TreeSet<String> oracle = new TreeSet<>(conformance);
        oracle.removeAll(unsupported);
        oracle.addAll(supportedExpected);
        oracleActions = List.copyOf(oracle);

        // Resolve each throwing action's pinned message now, so a missing one fails the suite.
        Map<String, String> messages = new LinkedHashMap<>();
        actionsFile.adapterUnsupported().getOrDefault(ADAPTER, List.of()).forEach(outcome ->
                messages.put(outcome.action(), requireMessage(
                        "adapterUnsupported." + ADAPTER + "." + outcome.action(),
                        outcome.message())));
        actionsFile.expectedUnsupported().stream()
                .filter(shape -> !supportedExpected.contains(shape.action()))
                .forEach(shape -> messages.put(shape.action(), requireMessage(
                        "expectedUnsupported." + shape.action() + ".messages." + ADAPTER,
                        shape.messages() == null ? null : shape.messages().get(ADAPTER))));
        throwingMessages = Map.copyOf(messages);

        TreeSet<String> throwing = new TreeSet<>(unsupported);
        throwing.addAll(expected);
        throwing.removeAll(supportedExpected);
        throwingActions = List.copyOf(throwing);
        assertEquals(throwing, throwingMessages.keySet(),
                "every throwing action must pin the message that names its mechanism");

        Set<String> classified = new LinkedHashSet<>();
        classified.addAll(oracleActions);
        classified.addAll(throwingActions);
        classified.addAll(nullRepresentationOmittedActions);
        classified.addAll(divergences);
        Set<String> manifest = new LinkedHashSet<>();
        manifest.addAll(conformance);
        manifest.addAll(expected);
        manifest.addAll(nullRepresentationOmittedActions);
        manifest.addAll(divergences);
        assertEquals(141, oracleActions.size());
        assertEquals(187, throwingActions.size());
        assertEquals(1, nullRepresentationOmittedActions.size());
        assertEquals(330, classified.size());
        assertEquals(manifest, classified, "every manifest action must be classified locally");
    }

    @AfterAll
    static void tearDown() {
        if (elasticsearch != null) elasticsearch.stop();
        if (cerbos != null) cerbos.stop();
    }

    private static void createIndex() throws Exception {
        Map<String, Object> tagProperties = Map.of(
                "id", Map.of("type", "keyword"),
                "name", Map.of("type", "keyword"));
        Map<String, Object> labelProperties = Map.of("name", Map.of("type", "keyword"));
        Map<String, Object> subCategoryProperties = Map.of(
                "name", Map.of("type", "keyword"),
                "labels", Map.of("type", "nested", "properties", labelProperties));
        Map<String, Object> categoryProperties = Map.of(
                "name", Map.of("type", "keyword"),
                "subCategories", Map.of("type", "nested", "properties", subCategoryProperties));
        Map<String, Object> mainCategoryProperties = Map.of(
                "name", Map.of("type", "keyword"),
                "subNames", Map.of("type", "keyword"),
                "subCategories", Map.of("type", "nested", "properties", Map.of(
                        "name", Map.of("type", "keyword"))));

        Map<String, Object> properties = new LinkedHashMap<>();
        // The corpus id as a keyword field, alongside `_id`. `_id` is metadata that term queries
        // cannot filter on, and the `id-*` actions need a real field.
        properties.put("id", Map.of("type", "keyword"));
        properties.put("aBool", Map.of("type", "boolean"));
        properties.put("aString", Map.of("type", "keyword"));
        properties.put("aNumber", Map.of("type", "integer"));
        properties.put("aDouble", Map.of("type", "double"));
        properties.put("aOptionalString", Map.of("type", "keyword"));
        properties.put("owner", Map.of("type", "keyword"));
        properties.put("coOwner", Map.of("type", "keyword"));
        properties.put("tagNames", Map.of("type", "keyword"));
        // Flat scalar lists. `double` because check() receives CEL doubles, and an integer mapping
        // would coerce a fractional element.
        properties.put("aNumberList", Map.of("type", "double"));
        properties.put("aBoolList", Map.of("type", "boolean"));
        // Keep malformed strings in _source but unindexed. CEL timestamp() errors on those rows,
        // so range predicates and their guarded negations must both deny them.
        properties.put("createdBy", Map.of("type", "date", "format", "strict_date_optional_time_nanos",
                "ignore_malformed", true));
        properties.put("updatedAt", Map.of("type", "date", "format", "strict_date_optional_time_nanos"));
        properties.put("createdAt", Map.of("type", "date", "format", "strict_date_optional_time_nanos"));
        properties.put("scope", Map.of("type", "keyword"));
        properties.put("obj", Map.of("properties", Map.of("inner", Map.of("type", "keyword"))));
        properties.put("tags", Map.of("type", "nested", "properties", tagProperties));
        properties.put("categories", Map.of("type", "nested", "properties", categoryProperties));
        properties.put("mainCategory", Map.of("properties", mainCategoryProperties));
        // The corpus's to-one relation, two levels deep, indexed as plain objects because
        // Elasticsearch has no join.
        Map<String, Object> relationLevel = Map.of(
                "aBool", Map.of("type", "boolean"),
                "aString", Map.of("type", "keyword"),
                "aNumber", Map.of("type", "integer"),
                "aOptionalString", Map.of("type", "keyword"));
        Map<String, Object> parentProperties = new LinkedHashMap<>(relationLevel);
        parentProperties.put("inner", Map.of("properties", relationLevel));
        properties.put("parent", Map.of("properties", parentProperties));

        es.createIndex(INDEX, Map.of("properties", properties));
    }

    private static void seedIndex() throws Exception {
        for (Seed seed : seeds) {
            Map<String, Object> document = new LinkedHashMap<>();
            document.put("id", seed.id());
            document.put("aBool", seed.aBool());
            document.put("aString", seed.aString());
            document.put("aNumber", seed.aNumber());
            if (doubleFor(seed) != null) document.put("aDouble", doubleFor(seed));
            if (seed.aOptionalString() != null) {
                document.put("aOptionalString", seed.aOptionalString());
                document.put("owner", seed.aOptionalString());
            } else {
                document.put("owner", null);
            }
            document.put("coOwner", scopeFor(seed));
            document.put("tagNames", seed.tags().stream().map(Tag::name).toList());
            // Stored as written, null elements included; Elasticsearch indexes the non-null
            // values as an unordered set of terms, so positional reads cannot translate.
            document.put("aNumberList", seed.aNumberList());
            document.put("aBoolList", seed.aBoolList());
            document.put("createdBy", isoFor(seed));
            if (timestampFor(seed) != null) document.put("createdAt", derivedFor(seed).createdAt());
            if (derivedFor(seed).updatedAt() != null) document.put("updatedAt", derivedFor(seed).updatedAt());
            if (scopeFor(seed) != null) document.put("scope", scopeFor(seed));
            document.put("obj", Map.of("inner", seed.aString()));
            document.put("tags", seed.tags().stream().map(tag -> {
                Map<String, Object> value = new LinkedHashMap<>();
                value.put("id", tag.id());
                value.put("name", tag.name());
                return value;
            }).toList());
            document.put("categories", categoriesFor(seed));
            if (!seed.subCategoryNames().isEmpty()) {
                document.put("mainCategory", Map.of(
                        "name", "business",
                        "subNames", seed.subCategoryNames(),
                        "subCategories", seed.subCategoryNames().stream()
                                .map(name -> Map.of("name", name)).toList()));
            }
            // The to-one chain. A seed with no parent gets no `parent` field, matching the
            // missing attribute check() receives.
            Seed parentSeed = parentSeedOf(seed);
            if (parentSeed != null) {
                Map<String, Object> parent = relationDocument(parentSeed);
                Seed innerSeed = parentSeedOf(parentSeed);
                if (innerSeed != null) parent.put("inner", relationDocument(innerSeed));
                document.put("parent", parent);
            }
            es.index(INDEX, seed.id(), document);
        }
    }

    private static List<Map<String, Object>> categoriesFor(Seed seed) {
        List<Map<String, Object>> categories = new ArrayList<>();
        for (String subName : seed.subCategoryNames()) {
            List<Map<String, Object>> labels = labelsFor(seed).stream().map(name -> {
                Map<String, Object> label = new LinkedHashMap<>();
                label.put("name", name);
                return label;
            }).toList();
            categories.add(Map.of(
                    "name", "business",
                    "subCategories", List.of(Map.of("name", subName, "labels", labels))));
        }
        return categories;
    }

    /** Searches the whole index, since Elasticsearch returns 10 hits by default. */
    private static String searchPath() {
        return "/" + INDEX + "/_search?size=" + seeds.size();
    }

    private static List<String> search(Map<String, Object> query) throws Exception {
        return es.ids(searchPath(), Map.of("query", Map.of(
                "bool", Map.of("filter", List.of(query)))));
    }

    private static List<String> allIds() {
        return seeds.stream().map(Seed::id).sorted().toList();
    }

    private static Principal principal() {
        PrincipalSpec spec = seedsFile.principal();
        Principal principal = Principal.newInstance(spec.id(), spec.roles().toArray(String[]::new));
        for (Map.Entry<String, Object> entry : spec.attr().entrySet()) {
            principal = principal.withAttribute(entry.getKey(),
                    principalAttribute(entry.getKey(), entry.getValue()));
        }
        return principal;
    }

    /**
     * Converts one principal attribute by its JSON type, recursively, so the plan and the oracle
     * receive the same principal.
     */
    private static AttributeValue principalAttribute(String key, Object value) {
        if (value == null) return nullAttributeValue();
        if (value instanceof String text) return AttributeValue.stringValue(text);
        if (value instanceof Number number) return AttributeValue.doubleValue(number.doubleValue());
        if (value instanceof Boolean bool) return AttributeValue.boolValue(bool);
        if (value instanceof List<?> list) {
            return AttributeValue.listValue(list.stream()
                    .map(element -> principalAttribute(key, element)).toList());
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, AttributeValue> fields = new LinkedHashMap<>();
            map.forEach((name, element) -> {
                if (!(name instanceof String field)) {
                    throw new IllegalStateException("Non-string principal field: " + key);
                }
                fields.put(field, principalAttribute(key + "." + field, element));
            });
            return AttributeValue.mapValue(fields);
        }
        throw new IllegalStateException("Unsupported principal attribute: " + key);
    }

    // -- the real to-one relation (conformance/README.md, "The real to-one relation") -----------
    //
    // `parentSeedId` names the seed whose scalars a row's `parent` carries, and that seed's own
    // `parentSeedId` fills `parent.inner`. The chain stops at two levels. An absent level is a
    // missing field here and a missing attribute in check().

    /** The seed one hop out, or null when this level has no parent. A null argument returns null. */
    private static Seed parentSeedOf(Seed seed) {
        if (seed == null || seed.parentSeedId() == null) return null;
        return seeds.stream()
                .filter(candidate -> candidate.id().equals(seed.parentSeedId()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "seeds.json: \"" + seed.id() + "\" names parent \"" + seed.parentSeedId()
                                + "\", which is not a seed id"));
    }

    /** One level of the chain as an indexed object. A NULL column is an absent field. */
    private static Map<String, Object> relationDocument(Seed seed) {
        Map<String, Object> level = new LinkedHashMap<>();
        level.put("aBool", seed.aBool());
        level.put("aString", seed.aString());
        level.put("aNumber", seed.aNumber());
        if (seed.aOptionalString() != null) level.put("aOptionalString", seed.aOptionalString());
        return level;
    }

    /** The same level as check() attributes. A NULL column is a missing attribute. */
    private static Map<String, AttributeValue> relationAttribute(Seed seed) {
        Map<String, AttributeValue> level = new LinkedHashMap<>();
        level.put("aBool", AttributeValue.boolValue(seed.aBool()));
        level.put("aString", AttributeValue.stringValue(seed.aString()));
        level.put("aNumber", AttributeValue.doubleValue(seed.aNumber()));
        if (seed.aOptionalString() != null) {
            level.put("aOptionalString", AttributeValue.stringValue(seed.aOptionalString()));
        }
        return level;
    }

    private static Resource checkResource(Seed seed) {
        Resource resource = Resource.newInstance(seedsFile.resourceKind(), seed.id())
                .withAttribute("aBool", AttributeValue.boolValue(seed.aBool()))
                .withAttribute("aString", AttributeValue.stringValue(seed.aString()))
                .withAttribute("aNumber", AttributeValue.doubleValue(seed.aNumber()))
                .withAttribute("createdBy", AttributeValue.stringValue(isoFor(seed)))
                .withAttribute("obj", AttributeValue.mapValue(Map.of(
                        "inner", AttributeValue.stringValue(seed.aString()))))
                .withAttribute("tags", AttributeValue.listValue(seed.tags().stream()
                        .map(ElasticsearchAdversarialConformanceTest::tagAttribute).toList()))
                .withAttribute("categories", AttributeValue.listValue(seed.subCategoryNames().stream()
                        .map(name -> AttributeValue.mapValue(Map.of(
                                "name", AttributeValue.stringValue("business"),
                                "subCategories", AttributeValue.listValue(AttributeValue.mapValue(Map.of(
                                        "name", AttributeValue.stringValue(name),
                                        "labels", AttributeValue.listValue(labelsFor(seed).stream()
                                                .map(ElasticsearchAdversarialConformanceTest::labelAttribute)
                                                .toList())))))))
                        .toList()));
        if (seed.aOptionalString() != null) {
            resource = resource.withAttribute("aOptionalString",
                    AttributeValue.stringValue(seed.aOptionalString()));
        }
        resource = resource.withAttribute("owner", seed.aOptionalString() == null
                ? nullAttributeValue() : AttributeValue.stringValue(seed.aOptionalString()));
        // The explicit-null alias of `scope`, for `null-value-f2f`.
        resource = resource.withAttribute("coOwner", scopeFor(seed) == null
                ? nullAttributeValue() : AttributeValue.stringValue(scopeFor(seed)));
        resource = resource.withAttribute("tagNames", AttributeValue.listValue(seed.tags().stream()
                .map(tag -> tag.name() == null
                        ? nullAttributeValue() : AttributeValue.stringValue(tag.name()))
                .toList()));
        // Null elements are sent as explicit nulls, not dropped: CEL treats `[null, 2]` and `[2]`
        // differently.
        resource = resource.withAttribute("aNumberList", AttributeValue.listValue(
                seed.aNumberList().stream()
                        .map(number -> number == null
                                ? nullAttributeValue() : AttributeValue.doubleValue(number))
                        .toList()));
        resource = resource.withAttribute("aBoolList", AttributeValue.listValue(
                seed.aBoolList().stream()
                        .map(bool -> bool == null
                                ? nullAttributeValue() : AttributeValue.boolValue(bool))
                        .toList()));
        if (doubleFor(seed) != null) {
            resource = resource.withAttribute("aDouble", AttributeValue.doubleValue(doubleFor(seed)));
        }
        if (scopeFor(seed) != null) {
            resource = resource.withAttribute("scope", AttributeValue.stringValue(scopeFor(seed)));
        }
        if (timestampFor(seed) != null) {
            resource = resource.withAttribute("createdAt",
                    AttributeValue.stringValue(derivedFor(seed).createdAt()));
        }
        if (derivedFor(seed).updatedAt() != null) {
            resource = resource.withAttribute("updatedAt", AttributeValue.stringValue(derivedFor(seed).updatedAt()));
        }
        if (!seed.subCategoryNames().isEmpty()) {
            resource = resource.withAttribute("mainCategory", AttributeValue.mapValue(Map.of(
                    "name", AttributeValue.stringValue("business"),
                    "subCategories", AttributeValue.listValue(seed.subCategoryNames().stream()
                            .map(name -> AttributeValue.mapValue(Map.of(
                                    "name", AttributeValue.stringValue(name)))).toList()),
                    "subNames", AttributeValue.listValue(seed.subCategoryNames().stream()
                            .map(AttributeValue::stringValue).toList()))));
        }
        // Mirrors the indexed document: no parent means no `parent` attribute, which CEL denies
        // as a missing path.
        Seed parentSeed = parentSeedOf(seed);
        if (parentSeed != null) {
            Map<String, AttributeValue> parent = relationAttribute(parentSeed);
            Seed innerSeed = parentSeedOf(parentSeed);
            if (innerSeed != null) {
                parent.put("inner", AttributeValue.mapValue(relationAttribute(innerSeed)));
            }
            resource = resource.withAttribute("parent", AttributeValue.mapValue(parent));
        }
        return resource;
    }

    private static AttributeValue tagAttribute(Tag tag) {
        Map<String, AttributeValue> value = new LinkedHashMap<>();
        value.put("id", AttributeValue.stringValue(tag.id()));
        if (tag.name() != null) value.put("name", AttributeValue.stringValue(tag.name()));
        return AttributeValue.mapValue(value);
    }

    private static AttributeValue labelAttribute(String name) {
        Map<String, AttributeValue> value = new LinkedHashMap<>();
        if (name != null) value.put("name", AttributeValue.stringValue(name));
        return AttributeValue.mapValue(value);
    }

    private static AttributeValue nullAttributeValue() {
        try {
            var constructor = AttributeValue.class.getDeclaredConstructor(com.google.protobuf.Value.class);
            constructor.setAccessible(true);
            return constructor.newInstance(com.google.protobuf.Value.newBuilder()
                    .setNullValue(com.google.protobuf.NullValue.NULL_VALUE).build());
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Unable to construct explicit null AttributeValue", e);
        }
    }

    private static List<String> oracleAllowedIds(String action) {
        return seeds.stream()
                .filter(seed -> client.check(principal(), checkResource(seed), action).isAllowed(action))
                .map(Seed::id).sorted().toList();
    }

    /**
     * The plan for one action. Uses the multi-action {@code plan} overload, since the
     * single-action one is deprecated.
     */
    private static PlanResourcesResult plan(String action) {
        return client.plan(
                principal(), Resource.newInstance(seedsFile.resourceKind()), List.of(action));
    }

    private static List<String> adapterFilteredIds(String action) throws Exception {
        // The same options ElasticsearchTranslatorTest uses.
        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                plan(action), Corpus.OPTIONS);
        if (result instanceof Result.AlwaysAllowed) {
            return allIds();
        }
        if (result instanceof Result.AlwaysDenied) {
            return List.of();
        }
        return search(((Result.Conditional) result).query());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("oracleActions")
    void adapterMatchesCheckOracle(String action) throws Exception {
        List<String> oracle = oracleAllowedIds(action);
        assertOracleShape(action, oracle);
        assertEquals(oracle, adapterFilteredIds(action),
                "adapter result diverges from check() oracle for action '" + action + "'");
    }

    @Test
    void malformedTimestampRemainsDeniedUnderBothPolarities() throws Exception {
        Seed malformed = seeds.stream().filter(seed -> seed.id().equals("h5")).findFirst().orElseThrow();
        assertEquals("not-a-timestamp", isoFor(malformed),
                "the oracle must receive the original malformed string, not a normalized date");
        for (String action : List.of("p-timestamp", "cast-not-timestamp")) {
            assertFalse(oracleAllowedIds(action).contains("h5"), action);
            assertFalse(adapterFilteredIds(action).contains("h5"), action);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("throwingActions")
    void unsupportedShapesThrow(String action) throws Exception {
        // Fetch the plan outside assertThrows so a PDP failure fails the test. No search runs: an
        // inexpressible shape must throw during translation.
        PlanResourcesResult plan = plan(action);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(plan, Corpus.OPTIONS),
                "unsupported action must fail during translation: " + action);
        // Checking the pinned message also catches an "Unknown attribute" from an unmapped field.
        assertTrue(ex.getMessage().contains(throwingMessages.get(action)),
                "action '" + action + "' was rejected for a reason actions.json does not declare: "
                        + ex.getMessage());
    }

    /**
     * Elasticsearch does not index an explicit null, so it needs no null-representation option:
     * every null-selecting shape already fails closed. {@code null-eq-missing} must fail the same
     * way.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("nullRepresentationOmittedActions")
    void nullRepresentationOmittedIsRejectedRegardless(String action) throws Exception {
        assertEquals(List.of(), oracleAllowedIds(action),
                "the omitted representation must deny every seed for " + action);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> adapterFilteredIds(action));
        assertTrue(ex.getMessage().contains(nullRepresentationOmittedMessages.get(action)),
                ex.getMessage());
    }

    /**
     * {@code filter-as-conjunct} puts {@code filter()} below the root. Its oracle is empty by
     * construction, so this checks that refusing it is required: the other conjunct,
     * {@code R.attr.aBool} ({@code root-bare-bool}), matches documents the PDP denies, so dropping
     * the untranslatable half would over-grant.
     */
    @Test
    void filterAsConjunctMustBeRefusedBecauseDroppingItsUntranslatableHalfOverGrants() throws Exception {
        assertEquals(List.of(), oracleAllowedIds("filter-as-conjunct"),
                "check() must deny every seed: a filter() in boolean position is not evaluable");

        List<String> survivingHalf = adapterFilteredIds("root-bare-bool");
        assertFalse(survivingHalf.isEmpty(),
                "root-bare-bool must match documents, else dropping the other conjunct would cost nothing");
        assertTrue(survivingHalf.size() < seeds.size(), "root-bare-bool must not match every document");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> adapterFilteredIds("filter-as-conjunct"));
        assertTrue(ex.getMessage().contains(throwingMessages.get("filter-as-conjunct")), ex.getMessage());
    }

    /** A throwing action with no pinned message fails classification. */
    @Test
    void throwingActionWithNoPinnedMessageFailsClassification() {
        for (String absent : new String[] {null, ""}) {
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> requireMessage("synthetic-entry", absent));
            assertTrue(ex.getMessage().contains("pins no throw message"), ex.getMessage());
        }
    }

    /**
     * {@code p-has} is the corpus's known planner divergence: the planner folds
     * {@code has(R.attr.aOptionalString)} to {@code ALWAYS_ALLOWED}. This measures the cost: an
     * unfiltered search returns every document {@code check()} denies.
     */
    @Test
    void upstreamHasFoldOverGrantTripwire() throws Exception {
        assertTrue(plan("p-has").isAlwaysAllowed(),
                "p-has should remain the documented planner divergence");
        assertInstanceOf(Result.AlwaysAllowed.class,
                ElasticsearchQueryPlanAdapter.toElasticsearchQuery(plan("p-has"), Corpus.OPTIONS));

        List<String> oracle = oracleAllowedIds("p-has");
        // A non-empty oracle rules out a broken PDP or policy load.
        assertFalse(oracle.isEmpty(), "p-has check() oracle must still allow the seeds holding the attr");
        assertTrue(oracle.size() < seeds.size(), "p-has check() oracle must still deny missing attrs");
        assertTrue(oracle.contains("a1"), "p-has: a1 holds aOptionalString");

        // With ALWAYS_ALLOWED the caller searches with no filter, so every denied id comes back.
        Set<String> denied = new TreeSet<>(allIds());
        denied.removeAll(oracle);
        assertFalse(denied.isEmpty(), "p-has: check() must deny at least one seed, or there is"
                + " no over-grant for this tripwire to see");
        List<String> unfiltered = es.ids(searchPath(), Map.of("query", Map.of("match_all", Map.of())));
        assertTrue(unfiltered.containsAll(denied), "the unfiltered search must return every"
                + " document the PDP denies for p-has; denied " + denied + ", got " + unfiltered);
        assertEquals(allIds(), unfiltered);
    }

    /**
     * Actions this adapter refuses whose group has no oracle-compared member. Each is still
     * checked for a non-degenerate oracle, as a PDP and policy liveness probe.
     */
    private static final List<String> DEGENERACY_LIVENESS_PROBES = List.of(
            "cast-not-double", "cast-not-int", "cast-not-string-missing",
            "cast-not-string-null", "index-fractional", "index-negative",
            "index-not-oob", "regex-eq-true", "regex-lookahead",
            "projection-exists-not-eq",
            "regex-digit", "regex-case", "regex-posix", "regex-unanchored", "regex-dot", "regex-alternation", "regex-brace", "except-size", "except-eq", "pv-structs", "pv-exists-one", "pv-filter", "pv-map", "pv-except", "lambda-in-literal", "lambda-in-literal-neg", "lambda-ternary", "in-var-var-omitted", "in-var-var-omitted-neg", "not-concat-unsolvable", "not-concat-unsolvable-ne", "hier-overlaps-list-prefix", "not-hasint-empty-chain", "div-by-division", "temporal-raw-eq", "not-nan-ord-le", "not-ternary-parent", "not-nan-order-string", "hasint-null-vf", "hasint-map-null", "hasint-map-null-vf", "eq-list", "ne-list",
            // size() over a string, a top-level regex alternation, and an empty hierarchy
            // delimiter.
            "string-size-gt0",
            "matches-alt",
            "hier-empty-delim",
            // Elasticsearch does not index an empty nested array, so a positive all() cannot tell
            // an empty collection (true) from a missing one (CEL error).
            "all-on-empty",
            // The relation reached through a ternary condition and a fractional count threshold.
            // The Query DSL has neither.
            "w1-ternary-chain-cond",
            "w1-size-frac-le-chain",
            // An explicit null cannot be told from a missing field, so `== null` is refused. The
            // negated forms stay compared.
            "null-eq",
            // Comparing an explicit-null attribute with a non-null constant: every Query DSL
            // spelling either requires the field or matches every document missing it.
            "null-value-ne-const",
            "null-value-not-eq-const",
            "null-value-not-in-const",
            "null-value-f2f",
            "null-value-pv-not-exists",
            // The id-* group's refused forms: a second document field or a computed operand on
            // the value side. string() is the same computed-operand refusal through a cast.
            "id-f2f-ne",
            "id-concat",
            "cast-string-bool",
            // A concatenation of two document fields is a computed operand.
            "concat-f2f",
            // Computed leaf operands the Query DSL cannot express without scripts: a negated
            // LIKE whose needle is a field, modulo, positional reads, and list equality over map().
            "not-contains",
            "arith-mod",
            "index-scalar-list",
            "index-scalar-list-not-eq",
            "index-scalar-list-null",
            "map-eq-list",
            // Positional reads over the number and boolean lists, refused at the same site. The
            // two cross-type probes are refused as a whole disjunction.
            "index-number-list",
            "index-number-list-not-eq",
            "index-bool-list",
            "index-bool-list-not-eq",
            "index-bool-list-vs-number",
            "index-number-list-vs-bool",
            // Its path is built by list() from a constant and the primary key, so there is no
            // stored path to query.
            "hier-list-id",
            // A null element of a flat list is not indexed, so null membership is refused in both
            // polarities.
            "null-in-number-list",
            "not-null-in-number-list");

    /**
     * Fails if an oracle is trivially empty or total, which would let the comparison pass
     * whatever the adapter emits. The only exemptions are {@code degenerateOracles} in
     * {@code conformance/actions.json}, which must match exactly.
     */
    private static void assertOracleShape(String action, List<String> oracle) {
        String declared = degenerateOracles.get(action);
        if ("empty".equals(declared)) {
            assertEquals(List.of(), oracle, "'" + action + "' is listed as an empty oracle in"
                    + " degenerateOracles (conformance/actions.json) but its oracle allows seeds;"
                    + " it discriminates now, so remove it from that list");
        } else if ("total".equals(declared)) {
            assertEquals(allSeedIds(), oracle, "'" + action + "' is listed as a total oracle in"
                    + " degenerateOracles (conformance/actions.json) but its oracle denies seeds;"
                    + " it discriminates now, so remove it from that list");
        } else {
            assertTrue(!oracle.isEmpty() && oracle.size() < seeds.size(),
                    "oracle for '" + action + "' is degenerate (" + oracle + "): the differential"
                            + " cannot fail for a degenerate oracle, so either the corpus lost its"
                            + " discriminating seed or the action belongs in degenerateOracles in"
                            + " conformance/actions.json with a reason");
        }
    }

    private static List<String> allSeedIds() {
        return seeds.stream().map(Seed::id).sorted().toList();
    }

    /**
     * Every {@code degenerateOracles} entry has exactly the declared oracle, whether this adapter
     * compares, refuses or rejects it, so an entry that starts discriminating is caught.
     */
    @Test
    void everyDegenerateOracleIsExactlyAsDeclared() {
        List<String> wrong = new ArrayList<>();
        for (Map.Entry<String, String> entry : degenerateOracles.entrySet()) {
            List<String> ids = oracleAllowedIds(entry.getKey());
            List<String> want = "empty".equals(entry.getValue()) ? List.of() : allSeedIds();
            if (!want.equals(ids)) {
                wrong.add(entry.getKey() + " (declared " + entry.getValue() + "): " + ids);
            }
        }
        assertEquals(List.of(), wrong, "these degenerateOracles entries in"
                + " conformance/actions.json do not have the oracle they declare");
    }

    /**
     * Liveness probes are never oracle-compared. An action that becomes supported must move out
     * of the probe list and into the sweep.
     */
    @Test
    void livenessProbesAreRefusedAndNonDegenerate() {
        Set<String> compared = Set.copyOf(oracleActions);
        for (String action : DEGENERACY_LIVENESS_PROBES) {
            assertFalse(compared.contains(action),
                    "'" + action + "' is now oracle-compared: remove it from the liveness probes");
            assertNonDegenerateOracle(action);
        }
    }

    /**
     * Reads both hops of the to-one chain back from the index and compares them with the corpus,
     * so a seeder that wrote the root's own values one hop out is caught.
     */
    @Test
    @SuppressWarnings("unchecked")
    void seededToOneChainMatchesTheCorpusRelation() throws Exception {
        long withParent = seeds.stream().filter(s -> parentSeedOf(s) != null).count();
        long withInner = seeds.stream()
                .filter(s -> parentSeedOf(parentSeedOf(s)) != null).count();
        assertTrue(withParent > 0, "no seed has a parent");
        assertTrue(withInner > 0, "no seed reaches parent.inner");
        assertTrue(withParent < seeds.size(), "every seed has a parent");

        Map<String, List<String>> want = new LinkedHashMap<>();
        for (Seed seed : seeds) {
            Seed parent = parentSeedOf(seed);
            Seed inner = parentSeedOf(parent);
            want.put(seed.id(), java.util.Arrays.asList(
                    parent == null ? null : parent.aString(),
                    inner == null ? null : inner.aString()));
        }

        Map<String, List<String>> got = new LinkedHashMap<>();
        for (Map<String, Object> hit :
                es.hits(searchPath(), Map.of("query", Map.of("match_all", Map.of())))) {
            Map<String, Object> source = (Map<String, Object>) hit.get("_source");
            Map<String, Object> parent = (Map<String, Object>) source.get("parent");
            Map<String, Object> inner = parent == null
                    ? null : (Map<String, Object>) parent.get("inner");
            got.put((String) hit.get("_id"), java.util.Arrays.asList(
                    parent == null ? null : (String) parent.get("aString"),
                    inner == null ? null : (String) inner.get("aString")));
        }
        assertEquals(want, got);
    }

    private static void assertNonDegenerateOracle(String action) {
        List<String> ids = oracleAllowedIds(action);
        assertTrue(!ids.isEmpty() && ids.size() < seeds.size(),
                "oracle for '" + action + "' is degenerate: " + ids);
    }

    /**
     * Checks that this harness consumes exactly the seed keys, principal keys and derived fields
     * the corpus defines. Rejecting unknown properties on decode only catches added keys, not
     * removed ones.
     */
    private static void assertCorpusCoverage(Path conformance) throws IOException {
        JsonNode rawSeedsFile = MAPPER.readTree(conformance.resolve("seeds.json").toFile());
        JsonNode rawSeeds = rawSeedsFile.get("seeds");
        assertEquals(seeds.size(), rawSeeds.size(), "seeds.json rows lost in decoding");
        for (int i = 0; i < rawSeeds.size(); i++) {
            String label = "seeds.json seeds[" + i + "]";
            assertKeys(label, keysOf(rawSeeds.get(i)), SEED_KEYS, List.of(SEED_NOTE_KEY));
            assertScalarList(label + ".aNumberList", rawSeeds.get(i).get("aNumberList"),
                    JsonNode::isNumber);
            assertScalarList(label + ".aBoolList", rawSeeds.get(i).get("aBoolList"),
                    JsonNode::isBoolean);
            JsonNode rawTags = rawSeeds.get(i).get("tags");
            for (int j = 0; j < rawTags.size(); j++) {
                assertKeys(label + ".tags[" + j + "]", keysOf(rawTags.get(j)), TAG_KEYS,
                        List.of());
            }
        }

        assertPrincipalCoverage(rawSeedsFile.get("principal"));

        assertKeys("derived-fields.json fields", derivedFile.fields(), DERIVED_KEYS, List.of());
        assertEquals(seeds.stream().map(Seed::id).collect(Collectors.toCollection(TreeSet::new)),
                new TreeSet<>(derivedFile.derived().keySet()),
                "derived-fields.json must carry exactly one entry per seeds.json id");
        JsonNode rawDerived =
                MAPPER.readTree(conformance.resolve("derived-fields.json").toFile()).get("derived");
        for (Map.Entry<String, JsonNode> entry : rawDerived.properties()) {
            assertKeys("derived-fields.json derived[\"" + entry.getKey() + "\"]",
                    keysOf(entry.getValue()), DERIVED_KEYS, List.of());
        }
    }

    /**
     * Checks the principal's keys, attribute keys and value types against the raw JSON, since
     * {@link #principal()} rebuilds it from {@link PrincipalSpec} and could only report keys this
     * harness already names.
     */
    private static void assertPrincipalCoverage(JsonNode principal) {
        assertKeys("seeds.json principal", keysOf(principal), PRINCIPAL_KEYS, List.of());
        JsonNode attr = principal.get("attr");
        assertKeys("seeds.json principal.attr", keysOf(attr), PRINCIPAL_ATTR_KEYS, List.of());
        for (Map.Entry<String, JsonNode> entry : attr.properties()) {
            String label = "seeds.json principal.attr." + entry.getKey();
            JsonNode value = entry.getValue();
            switch (entry.getKey()) {
                case "context" -> assertTrue(value.isTextual(), label);
                case "zero" -> assertTrue(value.isNumber(), label);
                case "manyStructs", "nullableStructs", "missingStructs" -> {
                    assertTrue(value.isArray(), label);
                    for (JsonNode element : value) {
                        assertTrue(element.isObject(), label);
                        assertKeys(label + "[]", keysOf(element),
                                entry.getKey().equals("missingStructs") ? List.of() : List.of("name"),
                                List.of());
                        if (!entry.getKey().equals("missingStructs")) {
                            assertTrue(element.get("name").isTextual() || element.get("name").isNull(), label);
                        }
                    }
                }
                default -> {
                    assertTrue(value.isArray(), label);
                    for (JsonNode element : value) assertTrue(element.isTextual(), label);
                }
            }
        }
    }

    /**
     * Checks a scalar list's element types in the raw JSON, because Jackson would coerce a quoted
     * {@code "2"} into a number. Null elements are allowed.
     */
    private static void assertScalarList(String label, JsonNode list,
                                         Predicate<JsonNode> elementType) {
        assertTrue(list.isArray(), () -> label + " is not an array: " + list);
        for (JsonNode element : list) {
            assertTrue(element.isNull() || elementType.test(element),
                    () -> label + " holds " + element + ", which is not its declared element type");
        }
    }

    private static void assertKeys(String label, Collection<String> got, Collection<String> want,
                                   Collection<String> optional) {
        Set<String> allowed = new LinkedHashSet<>(want);
        allowed.addAll(optional);
        for (String key : got) {
            assertTrue(allowed.contains(key), () -> label + " carries \"" + key
                    + "\", which this harness does not consume: an unconsumed corpus field is"
                    + " dropped from the indexed document and the check() oracle at once");
        }
        Set<String> missing = new LinkedHashSet<>(want);
        missing.removeAll(got);
        assertTrue(missing.isEmpty(),
                () -> label + " is missing " + missing + ", which this harness consumes");
    }

    private static List<String> keysOf(JsonNode node) {
        return node.properties().stream().map(Map.Entry::getKey).toList();
    }

    // -- deterministic derived fields (conformance/README.md, "Deterministic derived fields") -----
    //
    // Read from conformance/derived-fields.json, never recomputed here.

    private static DerivedEntry derivedFor(Seed seed) {
        DerivedEntry entry = derivedFile.derived().get(seed.id());
        assertNotNull(entry,
                () -> "derived-fields.json has no entry for seed \"" + seed.id() + "\"");
        return entry;
    }

    private static String isoFor(Seed seed) {
        return derivedFor(seed).createdBy();
    }

    private static Double doubleFor(Seed seed) {
        return derivedFor(seed).aDouble();
    }

    private static Instant timestampFor(Seed seed) {
        String value = derivedFor(seed).createdAt();
        return value == null ? null : Instant.parse(value);
    }

    private static List<String> labelsFor(Seed seed) {
        return derivedFor(seed).labels();
    }

    private static String scopeFor(Seed seed) {
        return derivedFor(seed).scope();
    }
}
