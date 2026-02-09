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
import java.util.List;
import java.util.Map;
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
            Map.entry("request.resource.attr.nested.aBool", "nested.aBool"),
            Map.entry("request.resource.attr.nested.aString", "nested.aString"),
            Map.entry("request.resource.attr.nested.aNumber", "nested.aNumber"),
            Map.entry("request.resource.attr.nested.nextlevel.aBool", "nested.nextlevel.aBool"),
            Map.entry("request.resource.attr.nested.nextlevel.aString", "nested.nextlevel.aString")
    );

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

        var topProps = Map.ofEntries(
                Map.entry("aBool", Map.of("type", "boolean")),
                Map.entry("aString", Map.of("type", "keyword")),
                Map.entry("aNumber", Map.of("type", "integer")),
                Map.entry("id", Map.of("type", "keyword")),
                Map.entry("nested", Map.of("type", "object", "properties", nestedProps)));

        String body = MAPPER.writeValueAsString(Map.of("mappings", Map.of("properties", topProps)));
        esRequest("PUT", "/" + INDEX, body);
    }

    // Doc 1: aBool=true, aString="string", aNumber=1
    //   nested: aBool=true, aString="substring", aNumber=2, nextlevel: aBool=true, aString="strDeep"
    // Doc 2: aBool=false, aString="amIAString?", aNumber=2
    //   nested: aBool=false, aString="noMatch", aNumber=1, nextlevel: aBool=false, aString="deepValue"
    // Doc 3: aBool=true, aString="anotherString", aNumber=3
    //   nested: aBool=true, aString="testString", aNumber=3, nextlevel: aBool=false, aString="strValue"
    private static void seedData() throws Exception {
        indexDoc("1", Map.of(
                "aBool", true, "aString", "string", "aNumber", 1,
                "id", "507f1f77bcf86cd799439011",
                "nested", Map.of(
                        "aBool", true, "aString", "substring", "aNumber", 2,
                        "nextlevel", Map.of("aBool", true, "aString", "strDeep"))));

        indexDoc("2", Map.of(
                "aBool", false, "aString", "amIAString?", "aNumber", 2,
                "id", "507f1f77bcf86cd799439012",
                "nested", Map.of(
                        "aBool", false, "aString", "noMatch", "aNumber", 1,
                        "nextlevel", Map.of("aBool", false, "aString", "deepValue"))));

        indexDoc("3", Map.of(
                "aBool", true, "aString", "anotherString", "aNumber", 3,
                "id", "507f1f77bcf86cd799439013",
                "nested", Map.of(
                        "aBool", true, "aString", "testString", "aNumber", 3,
                        "nextlevel", Map.of("aBool", false, "aString", "strValue"))));
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
    private static List<String> search(Map<String, Object> query) throws Exception {
        String body = MAPPER.writeValueAsString(Map.of("query", query));
        String responseBody = esRequest("POST", "/" + INDEX + "/_search", body);
        Map<String, Object> result = MAPPER.readValue(responseBody, new TypeReference<>() {});
        Map<String, Object> hits = (Map<String, Object>) result.get("hits");
        List<Map<String, Object>> hitList = (List<Map<String, Object>>) hits.get("hits");
        return hitList.stream()
                .map(h -> (String) h.get("_id"))
                .sorted()
                .collect(Collectors.toList());
    }

    private static List<String> searchAll() throws Exception {
        return search(Map.of("match_all", Map.of()));
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

    // --- Cross-level combined ---

    @Test
    void combinedAnd() throws Exception {
        // aBool == true AND nested.aString.contains("test") → doc 3 ("testString" has "test")
        assertEquals(List.of("3"), executeQuery("combined-and"));
    }
}
