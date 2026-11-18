package dev.cerbos.queryplan.elasticsearch.domains;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.util.JsonFormat;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Result;
import dev.cerbos.queryplan.elasticsearch.MalformedPlanException;
import dev.cerbos.queryplan.elasticsearch.UnmappedAttributeException;
import dev.cerbos.queryplan.elasticsearch.UnsupportedPlanShapeException;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class DomainHarnessTest {

    static final List<EsDomain> DOMAINS = List.of(
            new Healthcare(), new Fintech(), new Logistics(), new Education(), new Legal(),
            new Retail(), new Hr(), new Media(), new Iot(), new Government());

    static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    final String baseUrl = env("ADAPTER_DOMAIN_ES_URL", "http://localhost:49200");
    final String store = env("ADAPTER_DOMAIN_STORE", "elasticsearch");
    final Path domainsDir = Path.of(System.getProperty("user.dir"), "..", "..").normalize();
    final HttpClient http = HttpClient.newHttpClient();

    static String env(String name, String fallback) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? fallback : v;
    }

    @Test
    void replayDomains() throws Exception {
        Set<String> only = new TreeSet<>(List.of(env("ADAPTER_DOMAIN_ONLY", "").split(",")));
        only.remove("");
        List<ObjectNode> results = new ArrayList<>();
        for (EsDomain d : DOMAINS) {
            if (!only.isEmpty() && !only.contains(d.name())) {
                continue;
            }
            String index = "domain-" + d.name();
            load(d, index);
            try (Stream<Path> files = Files.walk(domainsDir.resolve(d.name()).resolve("golden"))) {
                for (Path p : files.filter(f -> f.toString().endsWith(".json")).sorted().toList()) {
                    results.add(replay(d, index, JSON.readTree(p.toFile())));
                }
            }
        }
        results.sort(Comparator.comparing((ObjectNode n) -> n.get("domain").asText())
                .thenComparing(n -> n.get("pdp").asText())
                .thenComparing(n -> n.get("principal").asText())
                .thenComparing(n -> n.get("action").asText()));
        ObjectNode out = JSON.createObjectNode();
        out.put("adapter", "elasticsearch-java");
        out.put("store", store);
        ArrayNode arr = out.putArray("results");
        results.forEach(arr::add);
        Path dir = domainsDir.resolve("results/elasticsearch-java");
        Files.createDirectories(dir);
        JSON.writeValue(dir.resolve(store + (only.isEmpty() ? "" : ".partial") + ".json").toFile(), out);
        results.stream().collect(Collectors.groupingBy(n -> n.get("verdict").asText(), Collectors.counting()))
                .forEach((k, v) -> System.out.println("==> " + store + " " + k + ": " + v));
    }

    void load(EsDomain d, String index) throws Exception {
        request("DELETE", "/" + index + "?ignore_unavailable=true", null);
        request("PUT", "/" + index, "{\"mappings\":{\"dynamic\":\"strict\",\"properties\":"
                + d.properties() + "}}");
        JsonNode resources = JSON.readTree(domainsDir.resolve(d.name()).resolve("generated/resources.json").toFile());
        StringBuilder bulk = new StringBuilder();
        for (JsonNode r : resources.get("resources")) {
            ObjectNode doc = ((ObjectNode) r.get("attr")).deepCopy();
            doc.put("id", r.get("id").asText());
            bulk.append("{\"index\":{\"_id\":").append(JSON.writeValueAsString(r.get("id").asText())).append("}}\n");
            bulk.append(new ObjectMapper().writeValueAsString(doc)).append('\n');
        }
        String resp = request("POST", "/" + index + "/_bulk?refresh=true", bulk.toString());
        if (JSON.readTree(resp).path("errors").asBoolean()) {
            throw new IllegalStateException("bulk load failed for " + d.name() + ": " + resp);
        }
    }

    ObjectNode replay(EsDomain d, String index, JsonNode golden) {
        ObjectNode r = JSON.createObjectNode();
        r.put("domain", d.name());
        r.put("pdp", golden.get("pdp").asText());
        r.put("principal", golden.get("principalName").asText());
        r.put("action", golden.get("action").asText());
        Set<String> allowed = new TreeSet<>();
        golden.get("allowed").forEach(n -> allowed.add(n.asText()));
        Set<String> returned = new TreeSet<>();
        String verdict;
        String detail = "";
        String body = null;
        try {
            Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(plan(golden), d.options());
            Map<String, Object> query = null;
            if (result instanceof Result.AlwaysAllowed) {
                query = Map.of("match_all", Map.of());
            } else if (result instanceof Result.Conditional c) {
                query = Map.of("bool", Map.of("filter", List.of(c.query())));
            }
            if (query != null) {
                body = new ObjectMapper().writeValueAsString(Map.of("size", 1000, "_source", false, "query", query));
                JsonNode resp = JSON.readTree(request("POST", "/" + index + "/_search", body));
                if (resp.path("_shards").path("failed").asInt() > 0 || resp.path("timed_out").asBoolean()) {
                    throw new IllegalStateException("partial search: " + resp);
                }
                resp.path("hits").path("hits").forEach(h -> returned.add(h.get("_id").asText()));
            }
            if (!allowed.containsAll(returned)) {
                verdict = "over-grant";
                Set<String> extra = new TreeSet<>(returned);
                extra.removeAll(allowed);
                detail = "extra " + extra + "; query: " + body;
            } else if (returned.equals(allowed)) {
                verdict = "pass";
            } else {
                verdict = "under-grant";
                Set<String> missing = new TreeSet<>(allowed);
                missing.removeAll(returned);
                detail = "missing " + missing + "; query: " + body;
            }
        } catch (UnsupportedPlanShapeException | UnmappedAttributeException | MalformedPlanException e) {
            verdict = "refused";
            detail = e.getClass().getSimpleName() + ": " + e.getMessage();
        } catch (Exception e) {
            verdict = "error";
            detail = e.getClass().getSimpleName() + ": " + e.getMessage() + (body != null ? "; query: " + body : "");
        }
        r.put("verdict", verdict);
        ArrayNode ret = r.putArray("returned");
        returned.forEach(ret::add);
        ArrayNode all = r.putArray("allowed");
        allowed.forEach(all::add);
        r.put("detail", detail);
        return r;
    }

    static PlanResourcesResponse plan(JsonNode golden) throws Exception {
        PlanResourcesFilter.Builder filter = PlanResourcesFilter.newBuilder();
        JsonFormat.parser().merge(golden.get("plan").toString(), filter);
        return PlanResourcesResponse.newBuilder().setFilter(filter).build();
    }

    String request(String method, String path, String body) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder().uri(URI.create(baseUrl + path))
                .header("Content-Type", path.contains("_bulk") ? "application/x-ndjson" : "application/json");
        b.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        HttpResponse<String> resp = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() >= 400 && !(method.equals("DELETE") && resp.statusCode() == 404)) {
            throw new IllegalStateException(method + " " + path + " -> " + resp.statusCode() + ": " + resp.body());
        }
        return resp.body();
    }
}
