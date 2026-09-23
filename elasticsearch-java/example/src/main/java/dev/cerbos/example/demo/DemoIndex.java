/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.example.demo;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.mapping.DynamicMapping;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

/**
 * One Elasticsearch index holding the demo seed rows, reached through the official Java client.
 */
final class DemoIndex implements AutoCloseable {

    private static final String INDEX = "demo-documents";

    /** Pagination sorts on this field, because {@code _id} is not sortable without fielddata. */
    private static final String SORT_FIELD = "id";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** One indexed document. {@code isPublic} holds {@code request.resource.attr.public}. */
    record IndexedDocument(String id, String ownerId, boolean isPublic, String region,
                           boolean archived) {}

    private final RestClient restClient;
    private final RestClientTransport transport;
    private final ElasticsearchClient client;

    DemoIndex(String url) {
        this.restClient = RestClient.builder(HttpHost.create(url)).build();
        this.transport = new RestClientTransport(restClient, new JacksonJsonpMapper(MAPPER));
        this.client = new ElasticsearchClient(transport);
    }

    /**
     * Recreates the index and loads the rows.
     *
     * <p>The mapping is explicit and strict: dynamic mapping would make strings {@code text}
     * fields, which are tokenized, and the adapter's {@code term} queries would then compare
     * against tokens instead of the stored value.
     */
    void recreate(List<DemoSeeds.Document> rows) throws IOException {
        if (client.indices().exists(e -> e.index(INDEX)).value()) {
            client.indices().delete(d -> d.index(INDEX));
        }
        client.indices().create(c -> c.index(INDEX).mappings(m -> m
                .dynamic(DynamicMapping.Strict)
                .properties(SORT_FIELD, p -> p.keyword(k -> k))
                .properties("ownerId", p -> p.keyword(k -> k))
                .properties("isPublic", p -> p.boolean_(b -> b))
                .properties("region", p -> p.keyword(k -> k))
                .properties("archived", p -> p.boolean_(b -> b))));

        for (DemoSeeds.Document row : rows) {
            IndexedDocument document = new IndexedDocument(
                    row.id(), row.ownerId(), row.isPublic(), row.region(), row.archived());
            client.index(i -> i.index(INDEX).id(row.id()).document(document));
        }
        client.indices().refresh(r -> r.index(INDEX));
    }

    /**
     * The sorted ids matching every clause in {@code filters}. Uses {@code bool.filter}, since
     * authorization needs no scoring.
     */
    List<String> search(List<Query> filters, int size) throws IOException {
        return ids(client.search(s -> s
                .index(INDEX)
                .size(size)
                .query(q -> q.bool(b -> b.filter(filters))), IndexedDocument.class));
    }

    /** One page of {@link #search}. Sorted, so pages do not repeat or skip documents. */
    List<String> page(List<Query> filters, int from, int size) throws IOException {
        return ids(client.search(s -> s
                .index(INDEX)
                .from(from)
                .size(size)
                .sort(so -> so.field(f -> f.field(SORT_FIELD).order(SortOrder.Asc)))
                .query(q -> q.bool(b -> b.filter(filters))), IndexedDocument.class));
    }

    /**
     * Converts the adapter's map to a client {@link Query} by serialising it and parsing it with
     * {@code withJson}.
     */
    static Query query(Map<String, Object> clause) {
        String json;
        try {
            json = MAPPER.writeValueAsString(clause);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException("Cannot serialise the adapter's clause: " + clause, e);
        }
        return Query.of(q -> q.withJson(new StringReader(json)));
    }

    private static List<String> ids(SearchResponse<IndexedDocument> response) {
        return response.hits().hits().stream()
                .map(hit -> {
                    IndexedDocument source = hit.source();
                    if (source == null) {
                        throw new IllegalStateException("hit " + hit.id() + " carried no _source");
                    }
                    return source.id();
                })
                .sorted()
                .toList();
    }

    @Override
    public void close() throws IOException {
        transport.close();
        restClient.close();
    }
}
