/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

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
 * Minimal HTTP client for Elasticsearch, shared by {@link ElasticsearchAdversarialConformanceTest}
 * and {@link ElasticsearchSurfaceTest}. Mappings and queries stay in each suite.
 */
final class TestElasticsearch {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final String baseUrl;

    TestElasticsearch(String httpHostAddress) {
        this.baseUrl = "http://" + httpHostAddress;
    }

    /** Sends one request and throws on a 4xx or 5xx response. */
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
     * Returns the raw hits, so callers can read {@code _score} as well as {@code _id}.
     * {@code searchPath} is a full path so a caller can add parameters such as {@code size}.
     *
     * <p>Throws on a timed-out search or a failed shard. Elasticsearch returns HTTP 200 with
     * partial hits in both cases, which would look like a filter that matched fewer documents.
     */
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> hits(String searchPath, Map<String, Object> body) throws Exception {
        Map<String, Object> response = MAPPER.readValue(
                request("POST", searchPath, MAPPER.writeValueAsString(body)),
                new TypeReference<>() {});
        if (Boolean.TRUE.equals(response.get("timed_out"))) {
            throw new IllegalStateException(
                    "Elasticsearch search timed out and returned partial hits: " + response);
        }
        Map<String, Object> shards = (Map<String, Object>) response.get("_shards");
        int failed = shards == null ? 0 : ((Number) shards.getOrDefault("failed", 0)).intValue();
        if (failed > 0) {
            throw new IllegalStateException("Elasticsearch search failed on " + failed
                    + " shard(s) and returned partial hits: " + response);
        }
        return (List<Map<String, Object>>) ((Map<String, Object>) response.get("hits")).get("hits");
    }

    /** Returns the matching document ids, sorted. */
    List<String> ids(String searchPath, Map<String, Object> body) throws Exception {
        return hits(searchPath, body).stream().map(hit -> (String) hit.get("_id")).sorted().toList();
    }
}
