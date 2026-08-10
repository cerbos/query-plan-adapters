package dev.cerbos.queryplan.elasticsearch;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Value;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.sdk.CerbosBlockingClient;
import dev.cerbos.sdk.CerbosClientBuilder;
import dev.cerbos.sdk.PlanResourcesResult;
import dev.cerbos.sdk.builders.AttributeValue;
import dev.cerbos.sdk.builders.Principal;
import dev.cerbos.sdk.builders.Resource;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
    private static final String SEMANTIC_SAFETY_INDEX = "semantic-safety";
    private static final String ANALYZED_MAPPING_INDEX = "analyzed-mapping";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Container
    static GenericContainer<?> cerbos = createCerbosContainer();

    @Container
    static ElasticsearchContainer elasticsearch =
            new ElasticsearchContainer(ElasticsearchTestImage.IMAGE)
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
        GenericContainer<?> container = new GenericContainer<>(CerbosTestImage.IMAGE)
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

        String semanticSafetyBody = MAPPER.writeValueAsString(Map.of(
                "mappings", Map.of("properties", Map.ofEntries(
                        Map.entry("scenario", Map.of("type", "keyword")),
                        Map.entry("tagObjects", Map.of(
                                "type", "nested", "properties", tagObjectProps)),
                        Map.entry("owner", Map.of("type", "keyword")),
                        Map.entry("regexValue", Map.of("type", "keyword")),
                        Map.entry("timestampValue", Map.of(
                                "type", "date", "format", "strict_date_optional_time_nanos"))))));
        esRequest("PUT", "/" + SEMANTIC_SAFETY_INDEX, semanticSafetyBody);

        // The one index in this harness whose string field is NOT `keyword`. Every other index
        // here maps strings exactly, which is precisely the condition under which the adapter is
        // safe — so without this index the README's "use keyword" advice is unenforced, and a
        // caller who ignores it gets an over-grant no test in the repository would notice
        // (cerbos/query-plan-adapters#322). `aString` uses the Elasticsearch default multi-field
        // shape: an analyzed `text` parent with an exact `keyword` sub-field, so the same
        // documents can be queried both ways and the two row sets compared directly.
        String analyzedMappingBody = MAPPER.writeValueAsString(Map.of(
                "mappings", Map.of("properties", Map.of(
                        "aString", Map.of(
                                "type", "text",
                                "fields", Map.of("keyword", Map.of("type", "keyword")))))));
        esRequest("PUT", "/" + ANALYZED_MAPPING_INDEX, analyzedMappingBody);
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

        indexDoc(SEMANTIC_SAFETY_INDEX, "empty", Map.of(
                "scenario", "collection", "tagObjects", List.of()));
        indexDoc(SEMANTIC_SAFETY_INDEX, "missing", Map.of("scenario", "collection"));
        indexDoc(SEMANTIC_SAFETY_INDEX, "present", Map.of(
                "scenario", "collection",
                "tagObjects", List.of(Map.of("id", "tag1", "name", "public"))));

        indexDoc(SEMANTIC_SAFETY_INDEX, "explicit-null-owner", mapOf(
                "scenario", "null", "marker", "same", "owner", null));
        indexDoc(SEMANTIC_SAFETY_INDEX, "missing-owner", Map.of(
                "scenario", "null", "marker", "same"));
        indexDoc(SEMANTIC_SAFETY_INDEX, "other-owner", Map.of(
                "scenario", "null", "marker", "same", "owner", "other"));

        indexDoc(SEMANTIC_SAFETY_INDEX, "regex-at", Map.of(
                "scenario", "regex", "regexValue", "@"));
        indexDoc(SEMANTIC_SAFETY_INDEX, "regex-other", Map.of(
                "scenario", "regex", "regexValue", "anything"));
        indexDoc(SEMANTIC_SAFETY_INDEX, "regex-containing", Map.of(
                "scenario", "regex", "regexValue", "prefix@suffix"));
        indexDoc(SEMANTIC_SAFETY_INDEX, "regex-newline", Map.of(
                "scenario", "regex", "regexValue", "a\nb"));

        indexDoc(SEMANTIC_SAFETY_INDEX, "timestamp-millis", Map.of(
                "scenario", "timestamp", "timestampValue", "2024-06-01T00:00:00.123Z"));
        indexDoc(SEMANTIC_SAFETY_INDEX, "timestamp-nanos", Map.of(
                "scenario", "timestamp", "timestampValue", "2024-06-01T00:00:00.123456Z"));

        // Four values that the standard analyzer and an exact mapping disagree about. "exact" is
        // the only one a caller means by aString == "string"; the other three are what tokenising
        // hands back as well.
        indexDoc(ANALYZED_MAPPING_INDEX, "exact", Map.of("aString", "string"));
        indexDoc(ANALYZED_MAPPING_INDEX, "phrase", Map.of("aString", "a string of words"));
        indexDoc(ANALYZED_MAPPING_INDEX, "casing", Map.of("aString", "STRING"));
        indexDoc(ANALYZED_MAPPING_INDEX, "unrelated", Map.of("aString", "stringent"));
    }

    private static Map<String, Object> mapOf(Object... keyValues) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put((String) keyValues[i], keyValues[i + 1]);
        }
        return map;
    }

    private static void indexDoc(String id, Map<String, Object> doc) throws Exception {
        indexDoc(INDEX, id, doc);
    }

    private static void indexDoc(String index, String id, Map<String, Object> doc) throws Exception {
        esRequest("PUT", "/" + index + "/_doc/" + id, MAPPER.writeValueAsString(doc));
    }

    private static void refreshIndex() throws Exception {
        esRequest("POST", "/" + INDEX + "/_refresh", null);
        esRequest("POST", "/" + SEMANTIC_SAFETY_INDEX + "/_refresh", null);
        esRequest("POST", "/" + ANALYZED_MAPPING_INDEX + "/_refresh", null);
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
        return search(INDEX, filterClause);
    }

    private static List<String> search(String index, Map<String, Object> filterClause) throws Exception {
        Map<String, Object> body = Map.of("query", Map.of(
                "bool", Map.of("filter", List.of(filterClause))));
        String responseBody = esRequest(
                "POST", "/" + index + "/_search", MAPPER.writeValueAsString(body));
        Map<String, Object> result = MAPPER.readValue(responseBody, new TypeReference<>() {});
        @SuppressWarnings("unchecked")
        Map<String, Object> hits = (Map<String, Object>) result.get("hits");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> hitList = (List<Map<String, Object>>) hits.get("hits");
        return hitList.stream()
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

    private static PlanResourcesResponse conditionalPlan(Operand condition) {
        return PlanResourcesResponse.newBuilder()
                .setFilter(PlanResourcesFilter.newBuilder()
                        .setKind(PlanResourcesFilter.Kind.KIND_CONDITIONAL)
                        .setCondition(condition))
                .build();
    }

    private static Operand expressionOperand(String operator, Operand... operands) {
        Expression.Builder expression = Expression.newBuilder().setOperator(operator);
        for (Operand operand : operands) {
            expression.addOperands(operand);
        }
        return Operand.newBuilder().setExpression(expression).build();
    }

    private static Operand variableOperand(String variable) {
        return Operand.newBuilder().setVariable(variable).build();
    }

    private static Operand stringValueOperand(String value) {
        return Operand.newBuilder()
                .setValue(Value.newBuilder().setStringValue(value))
                .build();
    }

    private static Operand nullValueOperand() {
        return Operand.newBuilder()
                .setValue(Value.newBuilder().setNullValue(NullValue.NULL_VALUE))
                .build();
    }

    private static Operand listValueOperandWithNull(String... values) {
        ListValue.Builder list = ListValue.newBuilder();
        for (String value : values) {
            list.addValues(Value.newBuilder().setStringValue(value));
        }
        list.addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE));
        return Operand.newBuilder()
                .setValue(Value.newBuilder().setListValue(list))
                .build();
    }

    private static Map<String, Object> inScenario(
            String scenario, Map<String, Object> query) {
        return Map.of("bool", Map.of("must", List.of(
                Map.of("term", Map.of("scenario", Map.of("value", scenario))),
                query)));
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

    // --- DeMorgan / negated operator wrappers ---

    @Test
    void notAnd() throws Exception {
        // !(aBool==true AND aString!="string") → aBool!=true OR aString=="string"
        // doc 1: aString=="string" → match
        // doc 2: aBool=false → match
        // doc 3: aBool=true AND aString!="string" → excluded
        assertEquals(List.of("1", "2"), executeQuery("not-and"));
    }

    @Test
    void notOr() throws Exception {
        // !(aBool==true OR aString!="string") → aBool!=true AND aString=="string"
        // No doc satisfies both (only doc 1 has aString=="string" but its aBool is true)
        assertEquals(List.of(), executeQuery("not-or"));
    }

    @Test
    void notGt() throws Exception {
        // !(aNumber > 1) → aNumber <= 1 → doc 1 (aNumber=1)
        assertEquals(List.of("1"), executeQuery("not-gt"));
    }

    @Test
    void notLt() throws Exception {
        // !(aNumber < 2) → aNumber >= 2 → docs 2 (aNumber=2), 3 (aNumber=3)
        assertEquals(List.of("2", "3"), executeQuery("not-lt"));
    }

    @Test
    void notContains() throws Exception {
        // !aString.contains("str") → keyword wildcard *str* negated
        // doc 1: "string" matches "str" → excluded
        // doc 2: "amIAString?" (case-sensitive, no lowercase "str") → match
        // doc 3: "anotherString" (case-sensitive, no lowercase "str") → match
        assertEquals(List.of("2", "3"), executeQuery("not-contains"));
    }

    @Test
    void notStartsWith() throws Exception {
        // !aString.startsWith("str") → keyword prefix "str" negated
        // doc 1: "string" → starts with "str" → excluded
        // doc 2: "amIAString?" → does not start with "str" → match
        // doc 3: "anotherString" → does not start with "str" → match
        assertEquals(List.of("2", "3"), executeQuery("not-starts-with"));
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
    void isNotNull() throws Exception {
        // aOptionalString != null → ne(field, null) → docs 1, 3 (field present)
        assertEquals(List.of("1", "3"), executeQuery("is-set"));
    }

    // --- Array membership ---

    @Test
    void hasTag() throws Exception {
        // "public" in tags → docs 1, 3
        assertEquals(List.of("1", "3"), executeQuery("has-tag"));
    }

    @Test
    void hasNoTagFailsClosed() {
        assertThrows(IllegalArgumentException.class,
                () -> executeQuery("has-no-tag"));
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
    void relationNoneFailsClosed() {
        assertThrows(IllegalArgumentException.class,
                () -> executeQuery("relation-none"));
    }

    @Test
    void relationMultipleOr() throws Exception {
        // createdBy == P.id OR P.id in ownedBy → docs 1, 3
        assertEquals(List.of("1", "3"), executeQuery("relation-multiple-or"));
    }

    @Test
    void relationMultipleNoneFailsClosed() {
        assertThrows(IllegalArgumentException.class,
                () -> executeQuery("relation-multiple-none"));
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
    void relationHasNoMembersFailsClosed() {
        assertThrows(IllegalArgumentException.class,
                () -> executeQuery("relation-has-no-members"));
    }

    // --- Cross-level combined ---

    @Test
    void combinedAnd() throws Exception {
        // aBool == true AND nested.aString.contains("test") → doc 3 ("testString" has "test")
        assertEquals(List.of("3"), executeQuery("combined-and"));
    }

    // --- Arithmetic (unsupported in ES query DSL without painless scripts) ---

    @Test
    void arithAddThrows() {
        // aNumber + 1 > 2 — arithmetic on document fields not natively supported.
        assertThrows(IllegalArgumentException.class, () -> executeQuery("arith-add"));
    }

    @Test
    void arithSubThrows() {
        assertThrows(IllegalArgumentException.class, () -> executeQuery("arith-sub"));
    }

    @Test
    void arithMultThrows() {
        assertThrows(IllegalArgumentException.class, () -> executeQuery("arith-mult"));
    }

    @Test
    void arithDivThrows() {
        assertThrows(IllegalArgumentException.class, () -> executeQuery("arith-div"));
    }

    @Test
    void arithModThrows() {
        assertThrows(IllegalArgumentException.class, () -> executeQuery("arith-mod"));
    }

    // --- Regex (supported via ES regexp query on keyword fields) ---

    @Test
    void matchesRegexWithDotFailsClosed() {
        assertThrows(IllegalArgumentException.class,
                () -> executeQuery("matches-regex"));
    }

    // --- List indexing (unsupported: ES treats arrays as multivalued, no ordered access) ---

    @Test
    void indexListThrows() {
        // ownedBy[0] == "user1" — array indexing not expressible in ES query DSL.
        assertThrows(IllegalArgumentException.class, () -> executeQuery("index-list"));
    }

    // --- Type conversions (unsupported: CAST not natively in ES query DSL) ---

    @Test
    void convertStringThrows() {
        // string(aNumber) == "1"
        assertThrows(IllegalArgumentException.class, () -> executeQuery("convert-string"));
    }

    @Test
    void convertDoubleThrows() {
        // double(aNumber) > 1.5
        assertThrows(IllegalArgumentException.class, () -> executeQuery("convert-double"));
    }

    @Test
    void convertIntThrows() {
        // int(aString) > 0
        assertThrows(IllegalArgumentException.class, () -> executeQuery("convert-int"));
    }

    // --- Ternary (unsupported: conditional expressions not in ES query DSL) ---

    @Test
    void ternaryThrows() {
        // (aBool ? aNumber : 0) > 0
        assertThrows(IllegalArgumentException.class, () -> executeQuery("ternary"));
    }

    // --- size() over strings ---
    // Adapter cannot distinguish string vs array fields, so size(str) > 0 is
    // translated as "exists str" — semantically equivalent for "non-empty"
    // emptiness checks. Other comparisons (e.g. size(str) > 5) throw.

    @Test
    void stringSizeGtZeroMatchesAllWithField() throws Exception {
        // size(aString) > 0 — all docs have aString set.
        assertEquals(List.of("1", "2", "3"), executeQuery("string-size"));
    }

    // --- Empty collection (size(arr) == 0 → must_not exists) ---

    @Test
    void emptyCollectionFailsClosed() {
        assertThrows(IllegalArgumentException.class,
                () -> executeQuery("empty-collection"));
    }

    @Test
    void ElasticsearchCannotDistinguishEmptyAndMissingCollections() throws Exception {
        Map<String, Object> nestedElement = Map.of("nested", Map.of(
                "path", "tagObjects",
                "query", Map.of("match_all", Map.of())));
        Map<String, Object> noNestedElement = Map.of("bool", Map.of(
                "must", List.of(Map.of("term", Map.of(
                        "scenario", Map.of("value", "collection")))),
                "must_not", List.of(nestedElement)));

        assertEquals(List.of("present"), search(SEMANTIC_SAFETY_INDEX,
                inScenario("collection", nestedElement)));
        assertEquals(List.of("empty", "missing"),
                search(SEMANTIC_SAFETY_INDEX, noNestedElement));

        assertThrows(IllegalArgumentException.class,
                () -> executeNestedQuery("all"));
        assertThrows(IllegalArgumentException.class,
                () -> executeQuery("empty-collection"));
    }

    @Test
    void ElasticsearchCannotDistinguishExplicitNullAndMissingFields() throws Exception {
        Map<String, Object> absentOwner = Map.of("bool", Map.of(
                "must", List.of(Map.of("term", Map.of(
                        "scenario", Map.of("value", "null")))),
                "must_not", List.of(Map.of("exists", Map.of("field", "owner")))));
        assertEquals(List.of("explicit-null-owner", "missing-owner"),
                search(SEMANTIC_SAFETY_INDEX, absentOwner));

        Operand positiveNull = expressionOperand("eq",
                variableOperand("request.resource.attr.owner"),
                nullValueOperand());
        assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                        conditionalPlan(positiveNull),
                        Map.of("request.resource.attr.owner", "owner")));

        Operand negatedMembership = expressionOperand("not",
                expressionOperand("in",
                        variableOperand("request.resource.attr.owner"),
                        listValueOperandWithNull("blocked")));
        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                conditionalPlan(negatedMembership),
                Map.of("request.resource.attr.owner", "owner"));
        assertEquals(List.of("other-owner"), search(
                SEMANTIC_SAFETY_INDEX,
                inScenario("null", ((Result.Conditional) result).query())));
    }

    // --- Analyzed (`text`) mappings ---
    //
    // The adapter is handed a plan, never an index, so it cannot see how a field is mapped. It
    // emits `term`, `prefix` and `wildcard`, which are exact against `keyword` and per-token
    // against `text` — so pointing `fieldMap` at an analyzed field silently widens every string
    // comparison. That is a caller-owned precondition, recorded as a mapping hazard in this
    // adapter's README, and these two tests are what make it a measured fact rather than a claim:
    // the same plan, the same documents, two mappings, two row sets
    // (cerbos/query-plan-adapters#322).

    @Test
    void analyzedMappingWidensEqualityAndKeywordSubFieldRestoresIt() throws Exception {
        Operand condition = expressionOperand("eq",
                variableOperand("request.resource.attr.aString"),
                stringValueOperand("string"));

        Result analyzed = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                conditionalPlan(condition),
                Map.of("request.resource.attr.aString", "aString"));
        // "casing" tokenises to [string] because the standard analyzer lowercases; "phrase"
        // tokenises to [a, string, of, words] and matches on one of them. Neither document has an
        // aString the policy's `== "string"` is true of, so both are rows the PDP denies.
        assertEquals(List.of("casing", "exact", "phrase"), search(
                ANALYZED_MAPPING_INDEX, ((Result.Conditional) analyzed).query()));

        // The documented remedy: point the field map at the exact sub-field, not at the parent.
        Result exact = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                conditionalPlan(condition),
                Map.of("request.resource.attr.aString", "aString.keyword"));
        assertEquals(List.of("exact"), search(
                ANALYZED_MAPPING_INDEX, ((Result.Conditional) exact).query()));
    }

    @Test
    void analyzedMappingWidensStartsWith() throws Exception {
        Operand condition = expressionOperand("startsWith",
                variableOperand("request.resource.attr.aString"),
                stringValueOperand("str"));

        Result analyzed = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                conditionalPlan(condition),
                Map.of("request.resource.attr.aString", "aString"));
        // `prefix` is per-token on `text`: "a string of words" starts with "a", not "str", but it
        // carries a token that does.
        assertEquals(List.of("casing", "exact", "phrase", "unrelated"), search(
                ANALYZED_MAPPING_INDEX, ((Result.Conditional) analyzed).query()));

        Result exact = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                conditionalPlan(condition),
                Map.of("request.resource.attr.aString", "aString.keyword"));
        assertEquals(List.of("exact", "unrelated"), search(
                ANALYZED_MAPPING_INDEX, ((Result.Conditional) exact).query()));
    }

    @Test
    void matchesTreatsAtAsLiteralInRealElasticsearch() throws Exception {
        Operand condition = expressionOperand("matches",
                variableOperand("request.resource.attr.regexValue"),
                stringValueOperand("^@$"));
        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                conditionalPlan(condition),
                Map.of("request.resource.attr.regexValue", "regexValue"));

        assertEquals(List.of("regex-at"), search(
                SEMANTIC_SAFETY_INDEX,
                inScenario("regex", ((Result.Conditional) result).query())));
    }

    @Test
    void matchesRejectsDotBecauseLuceneWouldMatchANewline() throws Exception {
        Map<String, Object> luceneDot = Map.of("regexp", Map.of(
                "regexValue", Map.of("value", "a.b", "flags", "NONE")));
        assertEquals(List.of("regex-newline"), search(
                SEMANTIC_SAFETY_INDEX,
                inScenario("regex", luceneDot)));

        Operand condition = expressionOperand("matches",
                variableOperand("request.resource.attr.regexValue"),
                stringValueOperand("^a.b$"));
        assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                        conditionalPlan(condition),
                        Map.of("request.resource.attr.regexValue", "regexValue")));
    }

    @Test
    void ordinaryDateMappingCollapsesSubMillisecondPrecision() throws Exception {
        Map<String, Object> exactMillisecond = Map.of(
                "term", Map.of("timestampValue", Map.of(
                        "value", "2024-06-01T00:00:00.123Z")));
        assertEquals(List.of("timestamp-millis", "timestamp-nanos"), search(
                SEMANTIC_SAFETY_INDEX,
                inScenario("timestamp", exactMillisecond)));

        Operand subMillisecond = expressionOperand("eq",
                expressionOperand("timestamp",
                        variableOperand("request.resource.attr.timestampValue")),
                expressionOperand("timestamp",
                        stringValueOperand("2024-06-01T00:00:00.123456Z")));
        assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                        conditionalPlan(subMillisecond),
                        Map.of("request.resource.attr.timestampValue", "timestampValue")));
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
        void allMatchingConditionFailsClosed() {
            assertThrows(IllegalArgumentException.class,
                    () -> executeNestedQuery("all"));
        }

        @Test
        void hasIntersectionWithMap() throws Exception {
            // hasIntersection(request.resource.attr.tags.map(tag, tag.name), ["public", "private"])
            // All docs have tagObjects with name in ["public","private"]
            assertEquals(List.of("1", "2", "3"), executeNestedQuery("map-collection"));
        }

        // --- Issue #232: collection macro composition ---

        @Test
        void allWithNestedLambdaBodyFailsClosed() {
            assertThrows(IllegalArgumentException.class,
                    () -> executeNestedQuery("all-nested"));
        }

        // TODO(#232): map(...) compared directly to a list literal is
        // unsupported by the ES adapter — the relational handler treats the
        // map expression as an unexpected operand type.
        @Test
        void mapComparedToLiteralListThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> executeNestedQuery("map-compared"));
        }

        // TODO(#232): size(filter(...)) > 0 is unsupported — the size handler
        // requires its operand to be a direct collection variable, not a
        // filter() expression.
        @Test
        void sizeOfFilterThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> executeNestedQuery("filter-count-gt"));
        }
    }

    // --- Issue #229: locked-in operator/comparison shapes ---

    @Test
    void isNotSetFailsClosedWithoutNullValueSentinel() {
        assertThrows(IllegalArgumentException.class,
                () -> executeQuery("is-not-set"));
    }

    @Test
    void equalFieldToFieldFailsClosed() {
        assertThrows(IllegalArgumentException.class,
                () -> executeQuery("equal-field-to-field"));
    }

    @Test
    void equalBoolFalse() throws Exception {
        // aBool == false → doc 2 only
        assertEquals(List.of("2"), executeQuery("equal-bool-false"));
    }

    @Test
    void inNumber() throws Exception {
        // aNumber in [1, 2, 3] → all docs (aNumbers are 1, 2, 3)
        assertEquals(List.of("1", "2", "3"), executeQuery("in-number"));
    }

    @Test
    void orLeafExists() throws Exception {
        // aBool == true OR R.attr.tags.exists(t, t.name == "public")
        // With NESTED_FIELD_MAP routing R.attr.tags → tagObjects:
        //   Doc 1: aBool=true OR public-tag → match
        //   Doc 2: aBool=false AND tagObjects=[{tag3,private}] → no match
        //   Doc 3: aBool=true OR public-tag → match
        assertEquals(List.of("1", "3"), executeNestedQuery("or-leaf-exists"));
    }

    // -- known-value principal collections across the planner's 10-item unroll cliff --

    /**
     * {@code P.attr.teams} is folded to a known value at plan time, so the planner unrolls
     * {@code principal-exists}/{@code principal-all} into an or/and chain at <= 10 elements
     * (cerbos/cerbos#2570, #2817) and ships the lambda with a literal value-list collection
     * above that. These tests straddle the cliff (9/10/11 elements) so both wire shapes stay
     * exercised against the pinned PDP — and keep returning identical document sets.
     */
    @Nested
    class KnownValuePrincipalCollections {

        private Principal principalWithTeams(int size) {
            List<AttributeValue> teams = new java.util.ArrayList<>(
                    List.of(AttributeValue.stringValue("string"),
                            AttributeValue.stringValue("anotherString")));
            while (teams.size() < size) {
                teams.add(AttributeValue.stringValue("filler-" + teams.size()));
            }
            return Principal.newInstance("user1", "USER")
                    .withAttribute("teams", AttributeValue.listValue(
                            teams.toArray(AttributeValue[]::new)));
        }

        private PlanResourcesResult planFor(int size, String action) {
            return cerbosClient.plan(
                    principalWithTeams(size), Resource.newInstance("resource"), action);
        }

        /**
         * Pin the wire shape each leg exercises: supported PDPs are >= 0.54, where both macros
         * unroll at <= 10 elements ({@code or}/{@code and} chain) and ship the value-list
         * lambda ({@code exists}/{@code all}) above that. Without this, a planner that moves
         * the threshold would silently leave one side of the cliff untested while the document
         * assertions stay green.
         */
        private void assertPlanShape(int size, String action,
                                     String unrolledOperator, String macroOperator) {
            String operator = planFor(size, action)
                    .getCondition()
                    .orElseThrow(() -> new AssertionError(action + " plan has no condition"))
                    .getExpression()
                    .getOperator();
            assertEquals(size <= 10 ? unrolledOperator : macroOperator, operator);
        }

        private List<String> execute(int size, String action) throws Exception {
            Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                    planFor(size, action), FIELD_MAP);
            assertInstanceOf(Result.Conditional.class, result);
            return search(((Result.Conditional) result).query());
        }

        @ParameterizedTest
        @ValueSource(ints = {9, 10, 11})
        void principalExistsMatchesAnyTeam(int size) throws Exception {
            assertPlanShape(size, "principal-exists", "or", "exists");
            // doc 1 aString = "string", doc 3 aString = "anotherString" — both in teams.
            assertEquals(List.of("1", "3"), execute(size, "principal-exists"));
        }

        @ParameterizedTest
        @ValueSource(ints = {9, 10, 11})
        void principalAllExcludesEveryTeam(int size) throws Exception {
            assertPlanShape(size, "principal-all", "and", "all");
            // doc 2 aString = "amIAString?" — the only document matching none of the teams.
            assertEquals(List.of("2"), execute(size, "principal-all"));
        }
    }
}
