package dev.cerbos.queryplan.elasticsearch;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.cerbos.sdk.CerbosBlockingClient;
import dev.cerbos.sdk.CerbosClientBuilder;
import dev.cerbos.sdk.PlanResourcesResult;
import dev.cerbos.sdk.builders.Principal;
import dev.cerbos.sdk.builders.Resource;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class ElasticsearchIntegrationTest {

    private static final String INDEX = "resources";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Container
    static GenericContainer<?> cerbos = createCerbosContainer();

    @Container
    static ElasticsearchContainer elasticsearch = new ElasticsearchContainer(
            "docker.elastic.co/elasticsearch/elasticsearch:8.15.3")
            .withEnv("xpack.security.enabled", "false");

    private static CerbosBlockingClient cerbosClient;
    private static HttpClient httpClient;
    private static String esBaseUrl;

    private static final Map<String, String> FIELD_MAP = Map.ofEntries(
            Map.entry("request.resource.attr.aBool", "aBool"),
            Map.entry("request.resource.attr.aString", "aString"),
            Map.entry("request.resource.attr.aNumber", "aNumber"),
            Map.entry("request.resource.attr.id", "id"),
            Map.entry("request.resource.attr.tags", "tags"),
            Map.entry("request.resource.attr.ownedBy", "ownedBy"),
            Map.entry("request.resource.attr.createdBy", "createdBy"),
            Map.entry("request.resource.attr.aOptionalString", "aOptionalString"),
            Map.entry("request.resource.attr.nested.aBool", "nested.aBool"),
            Map.entry("request.resource.attr.nested.aString", "nested.aString"),
            Map.entry("request.resource.attr.nested.aNumber", "nested.aNumber"),
            Map.entry("request.resource.attr.nested.nextlevel.aBool", "nested.nextlevel.aBool"),
            Map.entry("request.resource.attr.nested.nextlevel.aString", "nested.nextlevel.aString"),
            Map.entry("request.resource.attr.tagObjects", "tagObjects")
    );

    private static final Set<String> NESTED_PATHS = Set.of("tagObjects");

    private static GenericContainer<?> createCerbosContainer() {
        GenericContainer<?> container = new GenericContainer<>("ghcr.io/cerbos/cerbos:dev")
                .withExposedPorts(3593)
                .withCommand("server", "--set=storage.disk.directory=/policies", "--set=schema.enforcement=reject")
                .withEnv("CERBOS_NO_TELEMETRY", "1")
                .waitingFor(Wait.forLogMessage(".*Starting gRPC server.*", 1));
        try {
            byte[] policyBytes = Files.readAllBytes(
                    Path.of(System.getProperty("user.dir"), "..", "policies", "resource.yaml"));
            container.withCopyToContainer(Transferable.of(policyBytes), "/policies/resource.yaml");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return container;
    }

    @BeforeAll
    static void setUp() throws Exception {
        cerbosClient = new CerbosClientBuilder(
                cerbos.getHost() + ":" + cerbos.getMappedPort(3593))
                .withPlaintext().buildBlockingClient();

        httpClient = HttpClient.newHttpClient();
        esBaseUrl = "http://" + elasticsearch.getHttpHostAddress();

        createIndex();
        seedData();
        refreshIndex();
    }

    private static void createIndex() throws Exception {
        var nestedNextlevelProps = Map.of(
                "aBool", Map.of("type", "boolean"),
                "aString", Map.of("type", "keyword"));

        var nestedProps = Map.of(
                "aBool", Map.of("type", "boolean"),
                "aString", Map.of("type", "keyword"),
                "aNumber", Map.of("type", "integer"),
                "nextlevel", Map.of("type", "object", "properties", nestedNextlevelProps));

        var tagObjectProps = Map.of(
                "id", Map.of("type", "keyword"),
                "name", Map.of("type", "keyword"));

        var topProps = Map.ofEntries(
                Map.entry("aBool", Map.of("type", "boolean")),
                Map.entry("aString", Map.of("type", "keyword")),
                Map.entry("aNumber", Map.of("type", "integer")),
                Map.entry("id", Map.of("type", "keyword")),
                Map.entry("tags", Map.of("type", "keyword")),
                Map.entry("ownedBy", Map.of("type", "keyword")),
                Map.entry("createdBy", Map.of("type", "keyword")),
                Map.entry("aOptionalString", Map.of("type", "keyword")),
                Map.entry("nested", Map.of("type", "object", "properties", nestedProps)),
                Map.entry("tagObjects", Map.of("type", "nested", "properties", tagObjectProps)));

        String body = MAPPER.writeValueAsString(Map.of("mappings", Map.of("properties", topProps)));
        esRequest("PUT", "/" + INDEX, body);
    }

    private static void seedData() throws Exception {
        indexDoc("1", mapOf(
                "aBool", true, "aString", "string", "aNumber", 1,
                "id", "507f1f77bcf86cd799439011",
                "tags", List.of("public", "featured"),
                "ownedBy", List.of("user1", "user2"),
                "createdBy", "user1",
                "aOptionalString", "hello",
                "nested", Map.of(
                        "aBool", true, "aString", "substring", "aNumber", 2,
                        "nextlevel", Map.of("aBool", true, "aString", "strDeep")),
                "tagObjects", List.of(
                        Map.of("id", "tag1", "name", "public"),
                        Map.of("id", "tag2", "name", "private"))));

        indexDoc("2", mapOf(
                "aBool", false, "aString", "amIAString?", "aNumber", 2,
                "id", "507f1f77bcf86cd799439012",
                "tags", List.of("private"),
                "ownedBy", List.of("user2"),
                "createdBy", "user2",
                "nested", Map.of(
                        "aBool", false, "aString", "noMatch", "aNumber", 1,
                        "nextlevel", Map.of("aBool", false, "aString", "deepValue")),
                "tagObjects", List.of(
                        Map.of("id", "tag3", "name", "private"))));

        indexDoc("3", mapOf(
                "aBool", true, "aString", "anotherString", "aNumber", 3,
                "id", "507f1f77bcf86cd799439013",
                "tags", List.of("public"),
                "ownedBy", List.of("user1"),
                "createdBy", "user3",
                "aOptionalString", "world",
                "nested", Map.of(
                        "aBool", true, "aString", "testString", "aNumber", 3,
                        "nextlevel", Map.of("aBool", false, "aString", "strValue")),
                "tagObjects", List.of(
                        Map.of("id", "tag1", "name", "public"))));
    }

    private static Map<String, Object> mapOf(Object... keyValues) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put((String) keyValues[i], keyValues[i + 1]);
        }
        return map;
    }

    private static void indexDoc(String id, Map<String, Object> doc) throws Exception {
        esRequest("PUT", "/" + INDEX + "/_doc/" + id, MAPPER.writeValueAsString(doc));
    }

    private static void refreshIndex() throws Exception {
        esRequest("POST", "/" + INDEX + "/_refresh", null);
    }

    private static String esRequest(String method, String path, String body) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(esBaseUrl + path))
                .header("Content-Type", "application/json");
        if (body != null) {
            builder.method(method, HttpRequest.BodyPublishers.ofString(body));
        } else {
            builder.method(method, HttpRequest.BodyPublishers.noBody());
        }
        HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 400) {
            throw new RuntimeException("ES request failed (" + response.statusCode() + "): " + response.body());
        }
        return response.body();
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> searchRaw(Map<String, Object> body) throws Exception {
        String responseBody = esRequest("POST", "/" + INDEX + "/_search", MAPPER.writeValueAsString(body));
        Map<String, Object> result = MAPPER.readValue(responseBody, new TypeReference<>() {});
        Map<String, Object> hits = (Map<String, Object>) result.get("hits");
        return (List<Map<String, Object>>) hits.get("hits");
    }

    private static List<String> search(Map<String, Object> filterClause) throws Exception {
        Map<String, Object> body = Map.of("query", Map.of(
                "bool", Map.of("filter", List.of(filterClause))));
        return searchRaw(body).stream()
                .map(h -> (String) h.get("_id"))
                .sorted()
                .collect(Collectors.toList());
    }

    private static List<String> searchAll() throws Exception {
        return searchRaw(Map.of("query", Map.of("match_all", Map.of()))).stream()
                .map(h -> (String) h.get("_id"))
                .sorted()
                .collect(Collectors.toList());
    }

    private static PlanResourcesResult plan(String action) throws Exception {
        return cerbosClient.plan(
                Principal.newInstance("user1", "USER"),
                Resource.newInstance("resource"),
                action
        );
    }

    private static List<String> executeQuery(String action) throws Exception {
        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(plan(action), FIELD_MAP);
        if (result instanceof Result.AlwaysAllowed) {
            return searchAll();
        } else if (result instanceof Result.AlwaysDenied) {
            return List.of();
        } else {
            return search(((Result.Conditional) result).query());
        }
    }

    // --- Filter context best practice ---

    @Test
    void filterContextProducesZeroScores() throws Exception {
        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(plan("equal"), FIELD_MAP);
        assertInstanceOf(Result.Conditional.class, result);
        Map<String, Object> filterClause = ((Result.Conditional) result).query();

        Map<String, Object> body = Map.of("query", Map.of(
                "bool", Map.of("filter", List.of(filterClause))));
        List<Map<String, Object>> hits = searchRaw(body);

        assertFalse(hits.isEmpty());
        for (Map<String, Object> hit : hits) {
            double score = ((Number) hit.get("_score")).doubleValue();
            assertEquals(0.0, score, "filter context should produce zero scores");
        }
    }

    @Test
    void combinedWithUserQueryInBoolMust() throws Exception {
        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(plan("equal"), FIELD_MAP);
        assertInstanceOf(Result.Conditional.class, result);
        Map<String, Object> filterClause = ((Result.Conditional) result).query();

        Map<String, Object> body = Map.of("query", Map.of(
                "bool", Map.of(
                        "must", List.of(Map.of("match_all", Map.of())),
                        "filter", List.of(filterClause))));
        List<Map<String, Object>> hits = searchRaw(body);

        List<String> ids = hits.stream()
                .map(h -> (String) h.get("_id"))
                .sorted()
                .toList();
        assertEquals(List.of("1", "3"), ids);
    }

    // --- Always allow / deny ---

    @Test
    void alwaysAllowed() throws Exception {
        assertEquals(List.of("1", "2", "3"), executeQuery("always-allow"));
    }

    @Test
    void alwaysDenied() throws Exception {
        assertEquals(List.of(), executeQuery("always-deny"));
    }

    // --- Equality ---

    @Test
    void equal() throws Exception {
        // aBool == true → docs 1, 3
        assertEquals(List.of("1", "3"), executeQuery("equal"));
    }

    @Test
    void equalOid() throws Exception {
        // id == "507f1f77bcf86cd799439011" → doc 1
        assertEquals(List.of("1"), executeQuery("equal-oid"));
    }

    @Test
    void notEquals() throws Exception {
        // aString != "string" → docs 2, 3
        assertEquals(List.of("2", "3"), executeQuery("ne"));
    }

    @Test
    void explicitDeny() throws Exception {
        // DENY if aBool==true, ALLOW otherwise → not(eq(aBool, true)) → doc 2
        assertEquals(List.of("2"), executeQuery("explicit-deny"));
    }

    // --- Bare booleans (PDP normalizes to eq/not-eq) ---

    @Test
    void bareBool() throws Exception {
        // request.resource.attr.aBool → eq(aBool, true) → docs 1, 3
        assertEquals(List.of("1", "3"), executeQuery("bare-bool"));
    }

    @Test
    void bareBoolNegated() throws Exception {
        // !request.resource.attr.aBool → not(eq(aBool, true)) → doc 2
        assertEquals(List.of("2"), executeQuery("bare-bool-negated"));
    }

    @Test
    void bareBoolNested() throws Exception {
        // request.resource.attr.nested.aBool → eq(nested.aBool, true) → docs 1, 3
        assertEquals(List.of("1", "3"), executeQuery("bare-bool-nested"));
    }

    @Test
    void bareBoolNestedNegated() throws Exception {
        // !request.resource.attr.nested.aBool → not(eq(nested.aBool, true)) → doc 2
        assertEquals(List.of("2"), executeQuery("bare-bool-nested-negated"));
    }

    // --- Logical operators ---

    @Test
    void and() throws Exception {
        // aBool==true AND aString!="string" → doc 3
        assertEquals(List.of("3"), executeQuery("and"));
    }

    @Test
    void or() throws Exception {
        // aBool==true OR aString!="string" → all 3
        assertEquals(List.of("1", "2", "3"), executeQuery("or"));
    }

    @Test
    void nand() throws Exception {
        // NOT(aBool==true AND aString!="string") → docs 1, 2
        assertEquals(List.of("1", "2"), executeQuery("nand"));
    }

    @Test
    void nor() throws Exception {
        // NOT(aBool==true OR aString!="string") → none
        assertEquals(List.of(), executeQuery("nor"));
    }

    // --- Set membership ---

    @Test
    void in() throws Exception {
        // aString in ["string", "anotherString"] → docs 1, 3
        assertEquals(List.of("1", "3"), executeQuery("in"));
    }

    // --- Range operators (top-level) ---

    @Test
    void greaterThan() throws Exception {
        // aNumber > 1 → docs 2, 3
        assertEquals(List.of("2", "3"), executeQuery("gt"));
    }

    @Test
    void lessThan() throws Exception {
        // aNumber < 2 → doc 1
        assertEquals(List.of("1"), executeQuery("lt"));
    }

    @Test
    void greaterThanOrEqual() throws Exception {
        // aNumber >= 1 → all 3
        assertEquals(List.of("1", "2", "3"), executeQuery("gte"));
    }

    @Test
    void lessThanOrEqual() throws Exception {
        // aNumber <= 2 → docs 1, 2
        assertEquals(List.of("1", "2"), executeQuery("lte"));
    }

    // --- String operators (top-level) ---

    @Test
    void contains() throws Exception {
        // aString.contains("str") → wildcard *str* on keyword → doc 1 ("string")
        assertEquals(List.of("1"), executeQuery("contains"));
    }

    @Test
    void startsWith() throws Exception {
        // aString.startsWith("str") → prefix "str" → doc 1 ("string")
        assertEquals(List.of("1"), executeQuery("starts-with"));
    }

    @Test
    void endsWith() throws Exception {
        // aString.endsWith("ing") → wildcard *ing → docs 1 ("string"), 3 ("anotherString")
        assertEquals(List.of("1", "3"), executeQuery("ends-with"));
    }

    // --- Nested field equality ---

    @Test
    void equalNested() throws Exception {
        // nested.aBool == true → docs 1, 3
        assertEquals(List.of("1", "3"), executeQuery("equal-nested"));
    }

    @Test
    void equalDeeplyNested() throws Exception {
        // nested.nextlevel.aBool == true → doc 1
        assertEquals(List.of("1"), executeQuery("equal-deeply-nested"));
    }

    // --- Nested field range operators ---

    @Test
    void nestedEqNumber() throws Exception {
        // nested.aNumber == 1 → doc 2 (nested.aNumber=1)
        assertEquals(List.of("2"), executeQuery("relation-eq-number"));
    }

    @Test
    void nestedLtNumber() throws Exception {
        // nested.aNumber < 2 → doc 2 (nested.aNumber=1)
        assertEquals(List.of("2"), executeQuery("relation-lt-number"));
    }

    @Test
    void nestedLteNumber() throws Exception {
        // nested.aNumber <= 2 → docs 1 (2), 2 (1)
        assertEquals(List.of("1", "2"), executeQuery("relation-lte-number"));
    }

    @Test
    void nestedGteNumber() throws Exception {
        // nested.aNumber >= 1 → all 3
        assertEquals(List.of("1", "2", "3"), executeQuery("relation-gte-number"));
    }

    @Test
    void nestedGtNumber() throws Exception {
        // nested.aNumber > 1 → docs 1 (2), 3 (3)
        assertEquals(List.of("1", "3"), executeQuery("relation-gt-number"));
    }

    // --- Nested combined range ---

    @Test
    void nestedMultipleAll() throws Exception {
        // nested.aNumber > 1 AND nested.aNumber < 3 → doc 1 (nested.aNumber=2)
        assertEquals(List.of("1"), executeQuery("relation-multiple-all"));
    }

    // --- Nested string operators ---

    @Test
    void nestedContains() throws Exception {
        // nested.aString.contains("str") → wildcard *str* → doc 1 ("substring" has "str")
        assertEquals(List.of("1"), executeQuery("nested-contains"));
    }

    @Test
    void deeplyNestedStartsWith() throws Exception {
        // nested.nextlevel.aString.startsWith("str") → docs 1 ("strDeep"), 3 ("strValue")
        assertEquals(List.of("1", "3"), executeQuery("deeply-nested-starts-with"));
    }

    // --- Null checks (field existence) ---

    @Test
    void isSet() throws Exception {
        // aOptionalString != null → docs 1, 3 (field present)
        assertEquals(List.of("1", "3"), executeQuery("is-set"));
    }

    // --- Array membership ---

    @Test
    void hasTag() throws Exception {
        // "public" in tags → docs 1, 3
        assertEquals(List.of("1", "3"), executeQuery("has-tag"));
    }

    @Test
    void hasNoTag() throws Exception {
        // !("private" in tags) → docs 1, 3
        assertEquals(List.of("1", "3"), executeQuery("has-no-tag"));
    }

    // --- Principal references ---

    @Test
    void relationIs() throws Exception {
        // createdBy == P.id ("user1") → doc 1
        assertEquals(List.of("1"), executeQuery("relation-is"));
    }

    @Test
    void relationIsNot() throws Exception {
        // !(createdBy == P.id) → docs 2, 3
        assertEquals(List.of("2", "3"), executeQuery("relation-is-not"));
    }

    @Test
    void relationSome() throws Exception {
        // P.id in ownedBy → docs 1, 3
        assertEquals(List.of("1", "3"), executeQuery("relation-some"));
    }

    @Test
    void relationNone() throws Exception {
        // !(P.id in ownedBy) → doc 2
        assertEquals(List.of("2"), executeQuery("relation-none"));
    }

    @Test
    void relationMultipleOr() throws Exception {
        // createdBy == P.id OR P.id in ownedBy → docs 1, 3
        assertEquals(List.of("1", "3"), executeQuery("relation-multiple-or"));
    }

    @Test
    void relationMultipleNone() throws Exception {
        // not(createdBy == P.id) AND not("public" in tags) → doc 2
        assertEquals(List.of("2"), executeQuery("relation-multiple-none"));
    }

    // --- Array intersection ---

    @Test
    void hasIntersectionDirect() throws Exception {
        // hasIntersection(tags, ["public", "draft"]) → docs 1 (["public","featured"]), 3 (["public"])
        assertEquals(List.of("1", "3"), executeQuery("has-intersection-direct"));
    }

    // --- Size comparisons ---

    @Test
    void relationHasMembers() throws Exception {
        // size(ownedBy) > 0 → all docs have non-empty ownedBy
        assertEquals(List.of("1", "2", "3"), executeQuery("relation-has-members"));
    }

    @Test
    void relationHasNoMembers() throws Exception {
        // DENY if size(ownedBy) > 0, ALLOW otherwise → not(exists) → no docs
        assertEquals(List.of(), executeQuery("relation-has-no-members"));
    }

    // --- Cross-level combined ---

    @Test
    void combinedAnd() throws Exception {
        // aBool == true AND nested.aString.contains("test") → doc 3 ("testString" has "test")
        assertEquals(List.of("3"), executeQuery("combined-and"));
    }

    // --- Nested object (collection operator) integration tests ---
    // These go through the real Cerbos PDP using policy actions that reference
    // R.attr.tags as objects with {id, name}. In ES, that data lives in the
    // "tagObjects" nested field, so we use a field map that routes tags -> tagObjects.

    private static final Map<String, String> NESTED_FIELD_MAP = Map.ofEntries(
            Map.entry("request.resource.attr.aBool", "aBool"),
            Map.entry("request.resource.attr.aString", "aString"),
            Map.entry("request.resource.attr.aNumber", "aNumber"),
            Map.entry("request.resource.attr.id", "id"),
            Map.entry("request.resource.attr.tags", "tagObjects"),
            Map.entry("request.resource.attr.ownedBy", "ownedBy"),
            Map.entry("request.resource.attr.createdBy", "createdBy"),
            Map.entry("request.resource.attr.aOptionalString", "aOptionalString"),
            Map.entry("request.resource.attr.nested.aBool", "nested.aBool"),
            Map.entry("request.resource.attr.nested.aString", "nested.aString"),
            Map.entry("request.resource.attr.nested.aNumber", "nested.aNumber"),
            Map.entry("request.resource.attr.nested.nextlevel.aBool", "nested.nextlevel.aBool"),
            Map.entry("request.resource.attr.nested.nextlevel.aString", "nested.nextlevel.aString"),
            Map.entry("request.resource.attr.tagObjects", "tagObjects")
    );

    private static List<String> executeNestedQuery(String action) throws Exception {
        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                plan(action), NESTED_FIELD_MAP, NESTED_PATHS);
        if (result instanceof Result.AlwaysAllowed) {
            return searchAll();
        } else if (result instanceof Result.AlwaysDenied) {
            return List.of();
        } else {
            return search(((Result.Conditional) result).query());
        }
    }

    @Nested
    class NestedCollectionOperators {

        @Test
        void existsSingleCondition() throws Exception {
            // R.attr.tags.exists(tag, tag.id == "tag1")
            // Doc 1: [{id:"tag1",name:"public"},{id:"tag2",name:"private"}] → has tag1
            // Doc 3: [{id:"tag1",name:"public"}] → has tag1
            assertEquals(List.of("1", "3"), executeNestedQuery("exists-single"));
        }

        @Test
        void existsMultiCondition() throws Exception {
            // R.attr.tags.exists(tag, tag.id == "tag1" && tag.name == "public")
            assertEquals(List.of("1", "3"), executeNestedQuery("exists-multiple"));
        }

        @Test
        void existsByName() throws Exception {
            // R.attr.tags.exists(tag, tag.name == "public")
            assertEquals(List.of("1", "3"), executeNestedQuery("exists"));
        }

        @Test
        void allMatchingCondition() throws Exception {
            // R.attr.tags.all(tag, tag.name == "public")
            // Only doc 3 has all tagObjects with name=="public"
            assertEquals(List.of("3"), executeNestedQuery("all"));
        }

        @Test
        void hasIntersectionWithMap() throws Exception {
            // hasIntersection(request.resource.attr.tags.map(tag, tag.name), ["public", "private"])
            // All docs have tagObjects with name in ["public","private"]
            assertEquals(List.of("1", "2", "3"), executeNestedQuery("map-collection"));
        }
    }
}
