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
 * The store: one Elasticsearch index holding the demo domain's seed rows, reached through the
 * official Elasticsearch Java client.
 *
 * <p>The client is the point. This adapter hands back a {@code Map<String, Object>} of plain JDK
 * values with no Elasticsearch type in its signature, so the step a consumer actually has to make
 * work — turning that map into something the client will send — happens nowhere in the adapter's own
 * suites: {@code ElasticsearchTranslatorTest} compares the map to a golden asset, and
 * {@code ElasticsearchAdversarialConformanceTest} posts it as raw JSON over a bare
 * {@link java.net.http.HttpClient}. Neither ever asks the client library to parse it. That is what
 * {@link #query(Map)} does here, the way {@code elasticsearch-java/README.md} documents it under
 * "Sending the query to Elasticsearch".
 */
final class DemoIndex implements AutoCloseable {

    /** The index the demo rows live in. Created here, so its mapping is this example's own. */
    private static final String INDEX = "demo-documents";

    /**
     * The keyword field pagination sorts on. It is a real field rather than Elasticsearch's
     * {@code _id} metadata, which is not sortable without fielddata.
     */
    private static final String SORT_FIELD = "id";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * One indexed document.
     *
     * <p>The field names are Elasticsearch's, and they are deliberately not the Cerbos attribute
     * names: {@code isPublic} carries {@code request.resource.attr.public}. That is what makes
     * {@link DemoShapes}'s field map necessary rather than decorative.
     */
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
     * Drops and recreates the index, then indexes the corpus rows and refreshes so a search sees
     * them.
     *
     * <p>The mapping is written out rather than left to Elasticsearch's dynamic mapping, and
     * {@code dynamic: strict} makes that a requirement rather than a preference. Both halves are
     * this adapter's own hazard, catalogued in its README under "Analyzed ({@code text}) field
     * mapping": dynamic mapping gives a string field the {@code text} type, which is tokenized and
     * lowercased before it is indexed, and the {@code term} query the adapter emits for
     * {@code ownerId == "alice"} would then run against those tokens rather than against the stored
     * value. The adapter is handed a plan and never an index, so it cannot see the difference — the
     * mapping is the consumer's half of the contract, and an example is the right place to show a
     * consumer writing it.
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
     * The ids every clause in {@code filters} selects, sorted.
     *
     * <p>{@code bool.filter} rather than {@code bool.must}: an authorization condition is an access
     * control filter, not a relevance signal, so it belongs in a filter context where Elasticsearch
     * skips scoring and can cache the result. It is also what makes composition in usage shape 5 a
     * list append — one more clause in the same array.
     *
     * <p>{@code size} is passed in rather than left to Elasticsearch's default page of 10, which
     * would silently truncate a filter that selected more.
     */
    List<String> search(List<Query> filters, int size) throws IOException {
        return ids(client.search(s -> s
                .index(INDEX)
                .size(size)
                .query(q -> q.bool(b -> b.filter(filters))), IndexedDocument.class));
    }

    /**
     * One page of the same query — usage shape 4, expressed as Elasticsearch's {@code from}/{@code
     * size}.
     *
     * <p>The {@code sort} is required for the paging itself to be correct: without a total order,
     * successive pages may repeat or omit documents. How the RESULT is asserted is a separate
     * question, and {@code demo/cases.json} asserts page sizes plus the sorted union rather than
     * per-page order, because several of the stores behind the shared expectations have no total
     * order to paginate by.
     */
    List<String> page(List<Query> filters, int from, int size) throws IOException {
        return ids(client.search(s -> s
                .index(INDEX)
                .from(from)
                .size(size)
                .sort(so -> so.field(f -> f.field(SORT_FIELD).order(SortOrder.Asc)))
                .query(q -> q.bool(b -> b.filter(filters))), IndexedDocument.class));
    }

    /**
     * The adapter's emitted clause, as a client {@link Query}.
     *
     * <p>This is the plumbing step an example exists to execute. The adapter returns plain JDK
     * collections, so the client has to parse them, and {@code withJson} is the documented way in.
     * It is not a formality: the client models the Query DSL as generated types, and a construct the
     * adapter emits that no generated type accepts would fail here and nowhere else in this
     * repository. {@code bool.minimum_should_match} is the concrete instance — the adapter emits it
     * as the integer {@code 1}, which is what Elasticsearch's own JSON accepts, while the client
     * models the field as a string.
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
