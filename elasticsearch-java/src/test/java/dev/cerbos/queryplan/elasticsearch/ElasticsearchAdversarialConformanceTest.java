package dev.cerbos.queryplan.elasticsearch;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Differential suite over the shared hostile corpus. A real, pinned PDP supplies both the query
 * plan and the row-by-row {@code check()} oracle; generated DSL is executed by real Elasticsearch.
 */
class ElasticsearchAdversarialConformanceTest {

    private static final String INDEX = "adversarial";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // The three arguments the corpus is translated through live in Corpus, not here, because
    // ElasticsearchTranslatorTest pins the query this adapter emits for each corpus action and
    // THIS suite proves the documents that same query returns. Those two statements are only about
    // one query while both are built from one set of arguments; two copies could drift and each
    // suite would keep passing.

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
                        String parentSeedId, String note) {}

    /**
     * {@code attr} is typed as raw JSON rather than {@code Map<String, List<String>>}: the corpus
     * carries scalar principal attributes as well as lists, and a narrower type would reject the
     * file rather than silently drop one — but it would still be this harness deciding what the
     * corpus may contain. {@link #principal()} converts each value by its actual JSON type.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record PrincipalSpec(String id, List<String> roles, Map<String, Object> attr) {}

    /**
     * conformance/seeds.json. Every key the file carries is named, including the prose ones,
     * because unknown properties are rejected rather than ignored: a seed field this harness does
     * not consume would be dropped from the indexed document AND the check() oracle at once, and
     * the differential would agree for the wrong reason.
     */
    private record SeedsFile(@JsonProperty("$schema") String schema, String description,
                             PrincipalSpec principal, String resourceKind, String principalNote,
                             String relationNote, List<Seed> seeds) {}

    /** One seed's derived fields, exactly as conformance/derived-fields.json carries them. */
    private record DerivedEntry(String createdBy, Double aDouble, String createdAt, String scope,
                                List<String> labels) {}

    private record DerivedFile(@JsonProperty("$schema") String schema, String description,
                               List<String> fields, Map<String, DerivedEntry> derived) {}

    // -- corpus coverage guards -----------------------------------------------------------------
    //
    // The same parsed seed feeds the indexed document AND the check() oracle, so a corpus field
    // this harness does not consume is dropped from both sides at once and the differential agrees
    // for the wrong reason — the projection trap conformance/README.md describes for adapterctl.json,
    // applied to the seeds. Asserting set equality catches both directions: a corpus key nothing
    // here reads, and a key this harness reads that the corpus no longer carries.

    private static final List<String> SEED_KEYS = List.of(
            "id", "aBool", "aString", "aNumber", "aOptionalString", "tags", "subCategoryNames",
            "parentSeedId");

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

    // The corpus principal is guarded the same way and for the same reason. It feeds the PLAN under
    // test AND the check() oracle, so an attribute dropped on the way in vanishes from both sides
    // at once: the plan folds to ALWAYS_DENIED and the oracle, built from the same principal,
    // agrees. That is how langchain-chromadb's hardcoded attribute allowlist let `pv-exists` pass
    // while testing nothing (conformance/README.md, "Adding a new hostile shape", step 7).
    // principal() iterates the whole attr map, which is correct; the guard is what proves it does.
    //
    // `id` and `roles` are deliberately IN scope, guarded by PRINCIPAL_KEYS one level above the
    // attributes — the same two-level shape SEED_KEYS and TAG_KEYS use for a row and its `tags[]`
    // elements. A role dropped on the way in changes every policy decision at once; that it is less
    // likely to be projected away than an attribute is a reason to expect the assertion to stay
    // quiet, not a reason to omit it. PrincipalSpec ignores unknown properties so that this
    // assertion, not a Jackson decode error, is what names an added key.

    private static final List<String> PRINCIPAL_KEYS = List.of("id", "roles", "attr");

    private static final List<String> PRINCIPAL_ATTR_KEYS =
            List.of("allowedTags", "context", "fewTeams", "manyTeams");

    /** The corpus key for this adapter — its directory name, as every other harness uses. */
    private static final String ADAPTER = "elasticsearch-java";

    private static SeedsFile seedsFile;
    private static Corpus.ControlPlane actionsFile;
    private static Corpus.CheckResourcesFile checkResources;
    private static DerivedFile derivedFile;
    private static List<Seed> seeds;
    private static List<String> oracleActions;
    private static List<String> throwingActions;
    private static Map<String, String> throwingMessages;
    private static List<String> representationDependentRejectionActions;
    private static Map<String, String> representationDependentRejectionMessages;

    /**
     * The substring this adapter's error must contain, or a loud failure. The message is what
     * turns "it threw" into "it threw for the declared reason": without it a mapper typo or an
     * unrelated validation satisfies the throw suite just as well as the documented limitation
     * (cerbos/query-plan-adapters#326).
     */
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

    static Stream<String> representationDependentRejectionActions() {
        return representationDependentRejectionActions.stream();
    }

    @BeforeAll
    static void setUp() throws Exception {
        Path conformance = conformanceDir();
        seedsFile = MAPPER.readValue(conformance.resolve("seeds.json").toFile(), SeedsFile.class);
        actionsFile = Corpus.actionsFile();
        checkResources = Corpus.checkResourcesFile();
        derivedFile = MAPPER.readValue(
                conformance.resolve("derived-fields.json").toFile(), DerivedFile.class);
        seeds = seedsFile.seeds();
        assertCorpusCoverage(conformance);
        Set<String> seedIds = new TreeSet<>(seeds.stream().map(Seed::id).toList());
        Set<String> checkResourceIds = new TreeSet<>(checkResources.resources().stream()
                .map(Corpus.CheckResource::id).toList());
        assertEquals(seedIds, checkResourceIds,
                "canonical check resources must cover every seed exactly once");
        assertEquals(seedsFile.principal().id(), checkResources.principal().id());
        assertEquals(seedsFile.principal().roles(), checkResources.principal().roles());
        assertEquals(seedsFile.principal().attr(), checkResources.principal().attr());
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

        elasticsearch = new ElasticsearchContainer(ElasticsearchTestImage.IMAGE)
                .withEnv("xpack.security.enabled", "false");
        elasticsearch.start();
        es = new TestElasticsearch(elasticsearch.getHttpHostAddress());
        createIndex();
        seedIndex();
        es.refresh(INDEX);
    }

    private static void classifyActions() {
        Set<String> divergences = actionsFile.skippedDivergences(ADAPTER);
        // Actions whose `== null` probe targets an attribute the oracle OMITS for NULL columns.
        // Elasticsearch needs no representation option — it cannot index an explicit null
        // distinguishably from a missing field, so every null-SELECTING direction already fails
        // closed — but the action still has to be classified somewhere (#302).
        // Every adapter must reject these, so the message map names the whole roster and this
        // harness resolves its own entry exactly as it does for a throwing action.
        List<Corpus.RepresentationDependentRejection> nullProbes =
                Corpus.representationDependentRejections(actionsFile);
        representationDependentRejectionActions = nullProbes.stream()
                .map(Corpus.RepresentationDependentRejection::action).sorted().toList();
        Map<String, String> nullMessages = new LinkedHashMap<>();
        nullProbes.forEach(outcome -> nullMessages.put(outcome.action(), outcome.message()));
        representationDependentRejectionMessages = Map.copyOf(nullMessages);

        oracleActions = Corpus.oracleActions(actionsFile, ADAPTER).sorted().toList();

        // The substring each throwing action's error must contain, resolved once here so a
        // classification with no pinned message fails the whole suite rather than degrading its
        // case to a bare "it threw" (cerbos/query-plan-adapters#326).
        Map<String, String> messages = new LinkedHashMap<>();
        Corpus.rejectedOutcomes(actionsFile).forEach(outcome ->
                messages.put(outcome.action(), outcome.message()));
        throwingMessages = Map.copyOf(messages);

        throwingActions = List.copyOf(messages.keySet());
        assertEquals(new TreeSet<>(throwingActions), new TreeSet<>(throwingMessages.keySet()),
                "every throwing action must pin the message that names its mechanism");

        Set<String> classified = new LinkedHashSet<>();
        classified.addAll(oracleActions);
        classified.addAll(throwingActions);
        classified.addAll(representationDependentRejectionActions);
        classified.addAll(divergences);
        assertEquals(actionsFile.manifestActions(), classified,
                "every manifest action must be classified locally");
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
        // The corpus id, indexed as an ordinary keyword field as well as being the document's
        // `_id`. Elasticsearch's `_id` is metadata addressed by the `ids` query rather than a
        // term query, so the `id-*` actions need a real field to filter on; leaving it unindexed
        // would make them return NOTHING against a non-empty oracle, which reads as an adapter
        // defect and is a harness gap.
        properties.put("id", Map.of("type", "keyword"));
        properties.put("aBool", Map.of("type", "boolean"));
        properties.put("aString", Map.of("type", "keyword"));
        properties.put("aNumber", Map.of("type", "integer"));
        properties.put("aDouble", Map.of("type", "double"));
        properties.put("aOptionalString", Map.of("type", "keyword"));
        properties.put("owner", Map.of("type", "keyword"));
        properties.put("coOwner", Map.of("type", "keyword"));
        properties.put("tagNames", Map.of("type", "keyword"));
        properties.put("createdBy", Map.of("type", "date", "format", "strict_date_optional_time_nanos"));
        properties.put("createdAt", Map.of("type", "date", "format", "strict_date_optional_time_nanos"));
        properties.put("scope", Map.of("type", "keyword"));
        properties.put("obj", Map.of("properties", Map.of("inner", Map.of("type", "keyword"))));
        properties.put("tags", Map.of("type", "nested", "properties", tagProperties));
        properties.put("categories", Map.of("type", "nested", "properties", categoryProperties));
        properties.put("mainCategory", Map.of("properties", mainCategoryProperties));
        // The corpus's real to-one relation. `obj.inner` above is a flat alias of aString; this
        // is a genuine two-level chain, indexed as objects because Elasticsearch has no join.
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
            // The to-one chain, one nested object per level. A seed with no parent gets no
            // `parent` field at all, which is what makes the absent-parent hazard reachable
            // through a SCALAR rather than only through mainCategory's collection.
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

    /** The whole seeded index, so a filter is never silently truncated to Elasticsearch's page. */
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
        Corpus.CheckPrincipal spec = checkResources.principal();
        Principal principal = Principal.newInstance(spec.id(), spec.roles().toArray(String[]::new));
        for (Map.Entry<String, Object> entry : spec.attr().entrySet()) {
            principal = principal.withAttribute(entry.getKey(),
                    principalAttribute(entry.getKey(), entry.getValue()));
        }
        return principal;
    }

    /**
     * One principal attribute, converted by the JSON type the corpus actually carries. Strings and
     * lists of strings are the two shapes today; anything else fails loudly rather than being
     * coerced, because a silently reshaped principal attribute feeds the plan and the oracle at
     * once and they would agree for the wrong reason.
     */
    private static AttributeValue principalAttribute(String key, Object value) {
        if (value instanceof String s) return AttributeValue.stringValue(s);
        if (value instanceof List<?> list) {
            return AttributeValue.listValue(list.stream().map(element -> {
                if (element instanceof String s) return AttributeValue.stringValue(s);
                throw new IllegalStateException(
                        "seeds.json principal.attr." + key + " holds a non-string element");
            }).toList());
        }
        throw new IllegalStateException(
                "seeds.json principal.attr." + key + " is neither a string nor a list of strings");
    }

    // -- the real to-one relation (conformance/README.md, "The real to-one relation") -----------
    //
    // `parentSeedId` names the seed whose four scalars a row's `parent` carries, and that seed's
    // own `parentSeedId` names the ones `parent.inner` carries. The chain is cut at two levels.
    // Elasticsearch has no join, so both levels are indexed as nested objects — but the SHAPE is
    // the same to-one chain every other store carries, and an absent level is a missing path here
    // exactly as it is a missing row there.

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

    /** One level of the chain as an indexed object. A NULL column is an ABSENT field. */
    private static Map<String, Object> relationDocument(Seed seed) {
        Map<String, Object> level = new LinkedHashMap<>();
        level.put("aBool", seed.aBool());
        level.put("aString", seed.aString());
        level.put("aNumber", seed.aNumber());
        if (seed.aOptionalString() != null) level.put("aOptionalString", seed.aOptionalString());
        return level;
    }

    /** The same four as check() attributes: a NULL column is a MISSING attribute, one hop out. */
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

    private static Resource checkResource(Corpus.CheckResource input) {
        Resource resource = Resource.newInstance(input.kind(), input.id());
        for (Map.Entry<String, Object> attribute : input.attr().entrySet()) {
            resource = resource.withAttribute(
                    attribute.getKey(), principalAttribute(attribute.getKey(), attribute.getValue()));
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
        return checkResources.resources().stream()
                .filter(resource -> client.check(
                        principal(), checkResource(resource), action).isAllowed(action))
                .map(Corpus.CheckResource::id).sorted().toList();
    }

    private static List<String> adapterFilteredIds(String action) throws Exception {
        PlanResourcesResult plan = client.plan(
                principal(), Resource.newInstance(seedsFile.resourceKind()), action);
        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                plan, Corpus.FIELD_MAP, Map.of(), Corpus.NESTED_PATHS,
                Corpus.EXPLICIT_NULL_ATTRIBUTES);
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
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                plan, Corpus.FIELD_MAP, Map.of(), Corpus.NESTED_PATHS,
                Corpus.EXPLICIT_NULL_ATTRIBUTES),
                "unsupported action must fail during translation: " + action);
        // The corpus pins the exact mechanism, which subsumes the old "Unknown attribute" guard:
        // an unmapped Corpus.FIELD_MAP entry (which once let six actions throw here while never
        // reaching the mechanism their adapterctl.json reasons claim) now fails this assertion with
        // every other wrong-reason rejection (cerbos/query-plan-adapters#326).
        assertTrue(ex.getMessage().contains(throwingMessages.get(action)),
                "action '" + action + "' was rejected for a reason adapterctl.json does not declare: "
                        + ex.getMessage());
    }

    /**
     * #302. Elasticsearch is one of two adapters that need no NULL-representation option: it
     * cannot index an explicit null distinguishably from a missing field, so every shape that
     * would SELECT null documents already fails closed and only the {@code exists}-shaped
     * directions translate. {@code null-eq} (explicit null) is already in
     * {@code rejected} for that reason; {@code null-eq-missing} must fail the same way.
     * If Elasticsearch ever gains a null sentinel, this stops throwing and the adapter acquires a
     * representation dependency it must then declare.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("representationDependentRejectionActions")
    void representationDependentRejectionIsRejectedRegardless(String action) throws Exception {
        assertEquals(List.of(), oracleAllowedIds(action),
                "the omitted representation must deny every seed for " + action);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> adapterFilteredIds(action));
        assertTrue(ex.getMessage().contains(representationDependentRejectionMessages.get(action)),
                ex.getMessage());
    }

    /**
     * #387. {@code filter-as-conjunct} puts a filter() one level below the root, where the guard
     * that refuses {@code filter-as-condition} does not look. Its oracle is empty BY CONSTRUCTION
     * — check() cannot evaluate a non-boolean conjunction — so it belongs to neither
     * degeneracy-guard list, and the throw suite on its own would say nothing about whether
     * refusing it is REQUIRED.
     *
     * <p>This is that argument. The other conjunct is {@code R.attr.aBool}, which this adapter
     * certainly can express and which {@code root-bare-bool} spells on its own; an adapter that
     * dropped the conjunct it could not translate would emit exactly that query and return every
     * document it matches, all of which the PDP denies for this action.
     */
    @Test
    void filterAsConjunctMustBeRefusedBecauseDroppingItsUntranslatableHalfOverGrants() throws Exception {
        assumeTrue(actionsFile.selected("filter-as-conjunct"));
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

    /**
     * Adding a throwing action without pinning its message must fail this harness rather than
     * silently degrade the throw suite to a bare "it threw" (cerbos/query-plan-adapters#326).
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
    void upstreamHasFoldOverGrantTripwire() throws Exception {
        assumeTrue(actionsFile.selected("p-has"));
        PlanResourcesResult plan = client.plan(
                principal(), Resource.newInstance(seedsFile.resourceKind()), "p-has");
        List<String> oracle = oracleAllowedIds("p-has");
        assertTrue(plan.isAlwaysAllowed(), "p-has should remain the documented planner divergence");
        // Both halves: an empty oracle is a silently broken PDP or policy load, which the
        // non-total assertion alone would pass.
        assertFalse(oracle.isEmpty(), "p-has check() oracle must still allow the seeds holding the attr");
        assertTrue(oracle.size() < seeds.size(), "p-has check() oracle must still deny missing attrs");
        assertTrue(oracle.contains("a1"), "p-has: a1 holds aOptionalString");
        assertEquals(allIds(), adapterFilteredIds("p-has"));
    }

    @Test
    void oracleMatchesCatalogLiveness() {
        for (Map.Entry<String, Corpus.OracleExpectation> entry :
                actionsFile.oracleExpectations().entrySet()) {
            List<String> ids = oracleAllowedIds(entry.getKey());
            switch (entry.getValue().kind()) {
                case "proper-subset" -> assertTrue(
                        !ids.isEmpty() && ids.size() < checkResources.resources().size(),
                        "oracle for '" + entry.getKey() + "' is degenerate: " + ids);
                case "empty" -> assertTrue(ids.isEmpty(),
                        "oracle for '" + entry.getKey() + "' must be empty: " + ids);
                case "total" -> assertEquals(checkResources.resources().size(), ids.size(),
                        "oracle for '" + entry.getKey() + "' must be total: " + ids);
                default -> throw new IllegalStateException(
                        "unknown oracle expectation " + entry.getValue().kind());
            }
        }
    }

    /**
     * The to-one relation carries no corpus action yet — this is the expand half of
     * cerbos/query-plan-adapters#372's expand–contract — so nothing else in this class would
     * notice a seeder that indexed no chain at all, or one that wrote the root's own fields one
     * hop out. Read the two hops back out of the indexed documents rather than counting them: a
     * count cannot tell the corpus's values from the root's, which is exactly the flat-alias
     * failure this relation exists to make visible.
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
     * Proves this harness consumes every seed key, every principal key and every derived field the
     * corpus defines, and nothing it does not. Rejecting unknown properties on decode cannot do
     * this alone: it catches an added key but says nothing about one that disappears, and a
     * disappeared key decodes to its default on both sides of the differential.
     */
    private static void assertCorpusCoverage(Path conformance) throws IOException {
        JsonNode rawSeedsFile = MAPPER.readTree(conformance.resolve("seeds.json").toFile());
        JsonNode rawSeeds = rawSeedsFile.get("seeds");
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
     * Guards the corpus principal the way {@link #assertCorpusCoverage} guards a seed row: the
     * top-level keys, then the keys one level in.
     *
     * <p>Asserted against the RAW JSON because {@link #principal()} rebuilds the principal from
     * {@link PrincipalSpec} — a rebuilt object could only ever report the keys this harness already
     * names. The attribute VALUES are asserted too: a key-set guard says nothing about a change
     * inside one, and three of the four attributes are lists. {@link #principalAttribute} accepts
     * exactly a string and a list of strings, so a third shape fails here, next to the
     * declaration, rather than deep in the conversion. It is the same reason the seed guard
     * descends into {@code tags[]}.
     */
    private static void assertPrincipalCoverage(JsonNode principal) {
        assertKeys("seeds.json principal", keysOf(principal), PRINCIPAL_KEYS, List.of());
        JsonNode attr = principal.get("attr");
        assertKeys("seeds.json principal.attr", keysOf(attr), PRINCIPAL_ATTR_KEYS, List.of());
        for (Map.Entry<String, JsonNode> entry : attr.properties()) {
            String label = "seeds.json principal.attr." + entry.getKey();
            JsonNode value = entry.getValue();
            boolean listOfStrings = value.isArray();
            for (JsonNode element : value) {
                listOfStrings &= element.isTextual();
            }
            assertTrue(value.isTextual() || listOfStrings, () -> label
                    + " is neither a string nor a list of strings, the only two shapes this harness"
                    + " consumes: a reshaped principal attribute feeds the plan and the check()"
                    + " oracle at once");
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
