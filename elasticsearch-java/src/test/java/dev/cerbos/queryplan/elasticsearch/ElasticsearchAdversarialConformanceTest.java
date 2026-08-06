package dev.cerbos.queryplan.elasticsearch;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Result;
import dev.cerbos.sdk.CerbosBlockingClient;
import dev.cerbos.sdk.CerbosClientBuilder;
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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Differential suite over the shared hostile corpus. A real, pinned PDP supplies both the query
 * plan and the row-by-row {@code check()} oracle; generated DSL is executed by real Elasticsearch.
 */
class ElasticsearchAdversarialConformanceTest {

    private static final String INDEX = "adversarial";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Map<String, String> FIELD_MAP = Map.ofEntries(
            Map.entry("request.resource.attr.aBool", "aBool"),
            Map.entry("request.resource.attr.aString", "aString"),
            Map.entry("request.resource.attr.aNumber", "aNumber"),
            Map.entry("request.resource.attr.aDouble", "aDouble"),
            Map.entry("request.resource.attr.aOptionalString", "aOptionalString"),
            Map.entry("request.resource.attr.createdBy", "createdBy"),
            Map.entry("request.resource.attr.createdAt", "createdAt"),
            Map.entry("request.resource.attr.owner", "owner"),
            Map.entry("request.resource.attr.scope", "scope"),
            Map.entry("request.resource.attr.obj.inner", "obj.inner"),
            Map.entry("request.resource.attr.tags", "tags"),
            Map.entry("request.resource.attr.tagNames", "tagNames"),
            // `categories` must be mapped even though every action touching it is fail-closed:
            // an unmapped field makes those actions throw "Unknown attribute" instead of the
            // mechanism their actions.json reasons name, so the throw tests pass for the wrong
            // reason and the claimed limitation is never actually exercised.
            Map.entry("request.resource.attr.categories", "categories"),
            Map.entry("request.resource.attr.mainCategory.subCategories", "mainCategory.subCategories"),
            Map.entry("request.resource.attr.mainCategory.subNames", "mainCategory.subNames"));

    private static final Set<String> NESTED_PATHS = Set.of(
            "tags", "mainCategory.subCategories",
            "categories", "categories.subCategories", "categories.subCategories.labels");

    private static Path conformanceDir() {
        return Path.of(System.getProperty("user.dir"), "..", "conformance").normalize();
    }

    private record Tag(String id, String name) {}

    /**
     * One hostile row. {@code note} is corpus documentation this harness never reads; it is named
     * so that strict decoding accepts it, and it is the one seed key {@link #SEED_KEYS} omits.
     */
    private record Seed(String id, boolean aBool, String aString, int aNumber,
                        String aOptionalString, List<Tag> tags, List<String> subCategoryNames,
                        String note) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record PrincipalSpec(String id, List<String> roles, Map<String, List<String>> attr) {}

    /**
     * conformance/seeds.json. Every key the file carries is named, including the prose ones,
     * because unknown properties are rejected rather than ignored: a seed field this harness does
     * not consume would be dropped from the indexed document AND the check() oracle at once, and
     * the differential would agree for the wrong reason.
     */
    private record SeedsFile(@JsonProperty("$schema") String schema, String description,
                             PrincipalSpec principal, String resourceKind, String principalNote,
                             List<Seed> seeds) {}

    /** One seed's derived fields, exactly as conformance/derived-fields.json carries them. */
    private record DerivedEntry(String createdBy, Double aDouble, String createdAt, String scope,
                                List<String> labels) {}

    private record DerivedFile(@JsonProperty("$schema") String schema, String description,
                               List<String> fields, Map<String, DerivedEntry> derived) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record UnsupportedShape(String action) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record AdapterOutcome(String action, String reason) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record KnownDivergence(String action, List<String> adapters) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record ActionsFile(List<String> conformance,
                               Map<String, List<AdapterOutcome>> adapterUnsupported,
                               Map<String, List<AdapterOutcome>> adapterSupportedExpected,
                               List<UnsupportedShape> expectedUnsupported,
                               List<AdapterOutcome> nullRepresentationOmitted,
                               List<KnownDivergence> knownDivergences) {}

    // -- corpus coverage guards -----------------------------------------------------------------
    //
    // The same parsed seed feeds the indexed document AND the check() oracle, so a corpus field
    // this harness does not consume is dropped from both sides at once and the differential agrees
    // for the wrong reason — the projection trap conformance/README.md describes for actions.json,
    // applied to the seeds. Asserting set equality catches both directions: a corpus key nothing
    // here reads, and a key this harness reads that the corpus no longer carries.

    private static final List<String> SEED_KEYS = List.of(
            "id", "aBool", "aString", "aNumber", "aOptionalString", "tags", "subCategoryNames");

    /** Corpus prose, never read by a harness: the one documented exclusion from SEED_KEYS. */
    private static final String SEED_NOTE_KEY = "note";

    /**
     * The one nested object array a seed carries. A key added inside an element is dropped from
     * both sides of the differential just as silently as a top-level one, so it is guarded the
     * same way.
     */
    private static final List<String> TAG_KEYS = List.of("id", "name");

    private static final List<String> DERIVED_KEYS =
            List.of("createdBy", "aDouble", "createdAt", "scope", "labels");

    private static SeedsFile seedsFile;
    private static ActionsFile actionsFile;
    private static DerivedFile derivedFile;
    private static List<Seed> seeds;
    private static List<String> oracleActions;
    private static List<String> throwingActions;
    private static List<String> nullRepresentationOmittedActions;

    private static GenericContainer<?> cerbos;
    private static ElasticsearchContainer elasticsearch;
    private static CerbosBlockingClient client;
    private static HttpClient httpClient;
    private static String esBaseUrl;

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
        derivedFile = MAPPER.readValue(
                conformance.resolve("derived-fields.json").toFile(), DerivedFile.class);
        seeds = seedsFile.seeds();
        assertCorpusCoverage(conformance);
        classifyActions();

        cerbos = new GenericContainer<>(CerbosTestImage.IMAGE)
                .withExposedPorts(3593)
                .withCommand("server", "--set=storage.disk.directory=/policies")
                .withEnv("CERBOS_NO_TELEMETRY", "1")
                .waitingFor(Wait.forLogMessage(".*Starting gRPC server.*", 1));
        try {
            byte[] policy = Files.readAllBytes(conformance.resolve("policies/adversarial.yaml"));
            cerbos.withCopyToContainer(Transferable.of(policy), "/policies/adversarial.yaml");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        cerbos.start();
        System.out.printf("==> Elasticsearch conformance PDP: %s (digest %s)%n",
                CerbosTestImage.IMAGE, CerbosTestImage.resolvedDigest(cerbos));
        client = new CerbosClientBuilder(cerbos.getHost() + ":" + cerbos.getMappedPort(3593))
                .withPlaintext().buildBlockingClient();

        elasticsearch = new ElasticsearchContainer(
                "docker.elastic.co/elasticsearch/elasticsearch:8.15.3")
                .withEnv("xpack.security.enabled", "false");
        elasticsearch.start();
        httpClient = HttpClient.newHttpClient();
        esBaseUrl = "http://" + elasticsearch.getHttpHostAddress();
        createIndex();
        seedIndex();
        esRequest("POST", "/" + INDEX + "/_refresh", null);
    }

    private static void classifyActions() {
        Set<String> conformance = Set.copyOf(actionsFile.conformance());
        Set<String> expected = actionsFile.expectedUnsupported().stream()
                .map(UnsupportedShape::action).collect(java.util.stream.Collectors.toSet());
        Set<String> unsupported = actionsFile.adapterUnsupported()
                .getOrDefault("elasticsearch-java", List.of()).stream()
                .map(AdapterOutcome::action).collect(java.util.stream.Collectors.toSet());
        Set<String> supportedExpected = actionsFile.adapterSupportedExpected()
                .getOrDefault("elasticsearch-java", List.of()).stream()
                .map(AdapterOutcome::action).collect(java.util.stream.Collectors.toSet());
        Set<String> divergences = actionsFile.knownDivergences().stream()
                .filter(divergence -> divergence.adapters().contains("elasticsearch-java"))
                .map(KnownDivergence::action).collect(java.util.stream.Collectors.toSet());
        // Actions whose `== null` probe targets an attribute the oracle OMITS for NULL columns.
        // Elasticsearch needs no representation option — it cannot index an explicit null
        // distinguishably from a missing field, so every null-SELECTING direction already fails
        // closed — but the action still has to be classified somewhere (#302).
        nullRepresentationOmittedActions = actionsFile.nullRepresentationOmitted().stream()
                .map(AdapterOutcome::action).sorted().toList();

        assertTrue(conformance.containsAll(unsupported),
                "adapterUnsupported.elasticsearch-java contains non-conformance actions");
        assertTrue(expected.containsAll(supportedExpected),
                "adapterSupportedExpected.elasticsearch-java contains non-expected actions");
        assertEquals(92, unsupported.size(),
                "Elasticsearch unsupported coverage changed without updating the ledger assertion");
        assertEquals(2, supportedExpected.size(),
                "Elasticsearch supported-expected coverage changed without updating the ledger assertion");
        assertEquals(Set.of("p-has"), divergences,
                "Elasticsearch planner divergences changed without updating the tripwire");

        TreeSet<String> oracle = new TreeSet<>(conformance);
        oracle.removeAll(unsupported);
        oracle.addAll(supportedExpected);
        oracleActions = List.copyOf(oracle);

        TreeSet<String> throwing = new TreeSet<>(unsupported);
        throwing.addAll(expected);
        throwing.removeAll(supportedExpected);
        throwingActions = List.copyOf(throwing);

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
        assertEquals(43, oracleActions.size());
        assertEquals(98, throwingActions.size());
        assertEquals(1, nullRepresentationOmittedActions.size());
        assertEquals(143, classified.size());
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
        properties.put("aBool", Map.of("type", "boolean"));
        properties.put("aString", Map.of("type", "keyword"));
        properties.put("aNumber", Map.of("type", "integer"));
        properties.put("aDouble", Map.of("type", "double"));
        properties.put("aOptionalString", Map.of("type", "keyword"));
        properties.put("owner", Map.of("type", "keyword"));
        properties.put("tagNames", Map.of("type", "keyword"));
        properties.put("createdBy", Map.of("type", "date", "format", "strict_date_optional_time_nanos"));
        properties.put("createdAt", Map.of("type", "date", "format", "strict_date_optional_time_nanos"));
        properties.put("scope", Map.of("type", "keyword"));
        properties.put("obj", Map.of("properties", Map.of("inner", Map.of("type", "keyword"))));
        properties.put("tags", Map.of("type", "nested", "properties", tagProperties));
        properties.put("categories", Map.of("type", "nested", "properties", categoryProperties));
        properties.put("mainCategory", Map.of("properties", mainCategoryProperties));

        esRequest("PUT", "/" + INDEX, MAPPER.writeValueAsString(
                Map.of("mappings", Map.of("properties", properties))));
    }

    private static void seedIndex() throws Exception {
        for (Seed seed : seeds) {
            Map<String, Object> document = new LinkedHashMap<>();
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
            document.put("tagNames", seed.tags().stream().map(Tag::name).toList());
            document.put("createdBy", isoFor(seed));
            if (timestampFor(seed) != null) document.put("createdAt", timestampFor(seed).toString());
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
            esRequest("PUT", "/" + INDEX + "/_doc/" + seed.id(),
                    MAPPER.writeValueAsString(document));
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

    private static String esRequest(String method, String path, String body) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(esBaseUrl + path))
                .header("Content-Type", "application/json");
        builder.method(method, body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body));
        HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 400) {
            throw new IllegalStateException(
                    "Elasticsearch request failed (" + response.statusCode() + "): " + response.body());
        }
        return response.body();
    }

    @SuppressWarnings("unchecked")
    private static List<String> search(Map<String, Object> query) throws Exception {
        String json = esRequest("POST", "/" + INDEX + "/_search?size=" + seeds.size(),
                MAPPER.writeValueAsString(Map.of("query", Map.of(
                        "bool", Map.of("filter", List.of(query))))));
        Map<String, Object> response = MAPPER.readValue(json, new TypeReference<>() {});
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        return ((List<Map<String, Object>>) hits.get("hits")).stream()
                .map(hit -> (String) hit.get("_id")).sorted().toList();
    }

    private static List<String> allIds() {
        return seeds.stream().map(Seed::id).sorted().toList();
    }

    private static Principal principal() {
        PrincipalSpec spec = seedsFile.principal();
        Principal principal = Principal.newInstance(spec.id(), spec.roles().toArray(String[]::new));
        for (Map.Entry<String, List<String>> entry : spec.attr().entrySet()) {
            principal = principal.withAttribute(entry.getKey(), AttributeValue.listValue(
                    entry.getValue().stream().map(AttributeValue::stringValue).toList()));
        }
        return principal;
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
        resource = resource.withAttribute("tagNames", AttributeValue.listValue(seed.tags().stream()
                .map(tag -> tag.name() == null
                        ? nullAttributeValue() : AttributeValue.stringValue(tag.name()))
                .toList()));
        if (doubleFor(seed) != null) {
            resource = resource.withAttribute("aDouble", AttributeValue.doubleValue(doubleFor(seed)));
        }
        if (scopeFor(seed) != null) {
            resource = resource.withAttribute("scope", AttributeValue.stringValue(scopeFor(seed)));
        }
        if (timestampFor(seed) != null) {
            resource = resource.withAttribute("createdAt",
                    AttributeValue.stringValue(timestampFor(seed).toString()));
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

    private static List<String> adapterFilteredIds(String action) throws Exception {
        PlanResourcesResult plan = client.plan(
                principal(), Resource.newInstance(seedsFile.resourceKind()), action);
        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(plan, FIELD_MAP, NESTED_PATHS);
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
        assertEquals(oracleAllowedIds(action), adapterFilteredIds(action),
                "adapter result diverges from check() oracle for action '" + action + "'");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("throwingActions")
    void unsupportedShapesThrow(String action) throws Exception {
        // The plan is fetched OUTSIDE the assertion (a PDP failure fails the test rather than
        // passing it) and no search executes: the invariant is that an inexpressible shape
        // must throw during translation, before any query exists.
        PlanResourcesResult plan = client.plan(
                principal(), Resource.newInstance(seedsFile.resourceKind()), action);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(plan, FIELD_MAP, NESTED_PATHS),
                "unsupported action must fail during translation: " + action);
        // "Unknown attribute" is the one rejection that indicts the HARNESS (a FIELD_MAP gap)
        // rather than the adapter: an unmapped `categories` once let six actions throw here
        // while never reaching the mechanism their actions.json reasons claim. Every real
        // limitation throws a different message, so pin the distinction.
        assertFalse(ex.getMessage().startsWith("Unknown attribute:"),
                "action '" + action + "' was rejected by an unmapped field, not the declared "
                        + "mechanism: " + ex.getMessage());
    }

    /**
     * #302. Elasticsearch is one of two adapters that need no NULL-representation option: it
     * cannot index an explicit null distinguishably from a missing field, so every shape that
     * would SELECT null documents already fails closed and only the {@code exists}-shaped
     * directions translate. {@code null-eq} (explicit null) is already in
     * {@code adapterUnsupported} for that reason; {@code null-eq-missing} must fail the same way.
     * If Elasticsearch ever gains a null sentinel, this stops throwing and the adapter acquires a
     * representation dependency it must then declare.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("nullRepresentationOmittedActions")
    void nullRepresentationOmittedIsRejectedRegardless(String action) throws Exception {
        assertEquals(List.of(), oracleAllowedIds(action),
                "the omitted representation must deny every seed for " + action);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> adapterFilteredIds(action));
        assertTrue(ex.getMessage().contains("explicit null value from a missing field"),
                ex.getMessage());
    }

    @Test
    void upstreamHasFoldOverGrantTripwire() throws Exception {
        PlanResourcesResult plan = client.plan(
                principal(), Resource.newInstance(seedsFile.resourceKind()), "p-has");
        List<String> oracle = oracleAllowedIds("p-has");
        assertTrue(plan.isAlwaysAllowed(), "p-has should remain the documented planner divergence");
        assertTrue(oracle.size() < seeds.size(), "p-has check() oracle must still deny missing attrs");
        assertEquals(allIds(), adapterFilteredIds("p-has"));
    }

    @Test
    void oracleIsNotDegenerate() {
        for (String action : List.of("vf-le", "like-percent", "all-on-empty", "pv-exists", "pv-all", "null-eq", "null-ne")) {
            List<String> ids = oracleAllowedIds(action);
            assertTrue(!ids.isEmpty() && ids.size() < seeds.size(),
                    "oracle for '" + action + "' is degenerate: " + ids);
        }
    }

    /**
     * Proves this harness consumes every seed key and every derived field the corpus defines, and
     * nothing it does not. Rejecting unknown properties on decode cannot do this alone: it catches
     * an added key but says nothing about one that disappears, and a disappeared key decodes to its
     * default on both sides of the differential.
     */
    private static void assertCorpusCoverage(Path conformance) throws IOException {
        JsonNode rawSeeds = MAPPER.readTree(conformance.resolve("seeds.json").toFile()).get("seeds");
        assertEquals(seeds.size(), rawSeeds.size(), "seeds.json rows lost in decoding");
        for (int i = 0; i < rawSeeds.size(); i++) {
            String label = "seeds.json seeds[" + i + "]";
            assertKeys(label, keysOf(rawSeeds.get(i)), SEED_KEYS, List.of(SEED_NOTE_KEY));
            JsonNode rawTags = rawSeeds.get(i).get("tags");
            for (int j = 0; j < rawTags.size(); j++) {
                assertKeys(label + ".tags[" + j + "]", keysOf(rawTags.get(j)), TAG_KEYS,
                        List.of());
            }
        }

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
    // Read from conformance/derived-fields.json rather than restated here. The same value feeds the
    // indexed document and the check() oracle, so a transcription error would be self-consistent
    // and invisible to the differential; one machine-readable definition makes that impossible.

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
