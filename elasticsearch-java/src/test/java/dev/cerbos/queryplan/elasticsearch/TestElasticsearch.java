package dev.cerbos.queryplan.elasticsearch;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

/**
 * The HTTP transport the two container-backed suites share:
 * {@link ElasticsearchAdversarialConformanceTest} and {@link ElasticsearchSurfaceTest}.
 *
 * <p>Only the transport. What each suite indexes, what it asks for and what it asserts are
 * different questions with different answers — the harness executes one query per corpus action
 * over the shared seeds and compares ids with {@code check()}, the surface suite executes hand-made
 * documents to measure Elasticsearch's own behaviour — so neither the mappings nor the search
 * semantics live here.
 *
 * <p>This is a duplication ADR 0007 does NOT license: that rule is about the per-adapter corpus
 * loader, where the copies belong to different adapters and are allowed to differ. Two suites in
 * one adapter sending byte-identical HTTP requests are not that.
 */
final class TestElasticsearch {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final String baseUrl;

    TestElasticsearch(String httpHostAddress) {
        this.baseUrl = "http://" + httpHostAddress;
    }

    /** One request, with a non-2xx response raised rather than returned. */
    String request(String method, String path, String body) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .header("Content-Type", "application/json");
        builder.method(method, body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body));
        HttpResponse<String> response =
                httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 400) {
            throw new IllegalStateException(
                    "Elasticsearch request failed (" + response.statusCode() + "): "
                            + response.body());
        }
        return response.body();
    }

    void createIndex(String index, Map<String, Object> mappings) throws Exception {
        request("PUT", "/" + index, MAPPER.writeValueAsString(Map.of("mappings", mappings)));
    }

    void index(String index, String id, Map<String, Object> document) throws Exception {
        request("PUT", "/" + index + "/_doc/" + id, MAPPER.writeValueAsString(document));
    }

    void refresh(String index) throws Exception {
        request("POST", "/" + index + "/_refresh", null);
    }

    /**
     * The raw hits for a search body, so a caller can read {@code _score} as well as {@code _id}.
     *
     * <p>{@code searchPath} is the whole path rather than an index name, because the harness has to
     * raise {@code size} past Elasticsearch's default page of 10 to see all its seeds.
     */
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> hits(String searchPath, Map<String, Object> body) throws Exception {
        Map<String, Object> response = MAPPER.readValue(
                request("POST", searchPath, MAPPER.writeValueAsString(body)),
                new TypeReference<>() {});
        return (List<Map<String, Object>>) ((Map<String, Object>) response.get("hits")).get("hits");
    }

    /** The document ids a search body selects, sorted. */
    List<String> ids(String searchPath, Map<String, Object> body) throws Exception {
        return hits(searchPath, body).stream().map(hit -> (String) hit.get("_id")).sorted().toList();
    }
}
