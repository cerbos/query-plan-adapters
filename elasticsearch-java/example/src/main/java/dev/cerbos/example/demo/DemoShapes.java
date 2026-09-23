/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.example.demo;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Options;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Result;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.ScalarType;
import dev.cerbos.sdk.CerbosBlockingClient;
import dev.cerbos.sdk.PlanResourcesResult;
import dev.cerbos.sdk.builders.Principal;
import dev.cerbos.sdk.builders.Resource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The five demo usage shapes, run against a real Elasticsearch index through the official Java
 * client. See {@code demo/README.md}.
 *
 * <p>Each result reports the plan kind, read from the adapter's {@link Result}, alongside the ids.
 */
final class DemoShapes {

    private static final String RESOURCE_KIND = "document";

    /**
     * Cerbos attribute to Elasticsearch field. {@code region} and {@code archived} are absent
     * because only the application filter uses them.
     */
    private static final Map<String, String> DOCUMENT_FIELDS = Map.of(
            "request.resource.attr.ownerId", "ownerId",
            "request.resource.attr.public", "isPublic");

    /**
     * The field map plus each field's CEL type, matching the mapping in {@link DemoIndex}. The
     * adapter refuses to compare a field with no declared type.
     */
    private static final Options OPTIONS = Options.of(DOCUMENT_FIELDS)
            .withScalarTypes(Map.of(
                    "ownerId", ScalarType.STRING,
                    "isPublic", ScalarType.BOOLEAN));

    /** One shape's answer, as {@code demo/expected.json} spells it. */
    record ShapeResult(String kind, List<String> ids) {}

    /** Shape 4 also reports the requested page size and the size of each page read. */
    record PaginatedShapeResult(String kind, List<String> ids, int pageSize,
                                List<Integer> pageSizes) {}

    private final CerbosBlockingClient cerbos;
    private final DemoIndex index;
    private final DemoSeeds seeds;

    DemoShapes(CerbosBlockingClient cerbos, DemoIndex index, DemoSeeds seeds) {
        this.cerbos = cerbos;
        this.index = index;
        this.seeds = seeds;
    }

    Map<String, Object> run() throws IOException {
        index.recreate(seeds.documents());

        return Map.of(
                "filtered", Map.of(
                        "alice/view", filtered("alice", "view"),
                        "bob/view", filtered("bob", "view")),
                "alwaysAllowed", Map.of(
                        "admin/admin-view", filtered("admin", "admin-view")),
                "alwaysDenied", Map.of(
                        "alice/publish", filtered("alice", "publish")),
                "paginated", Map.of(
                        "alice/view", paginated("alice", "view", 2),
                        "admin/admin-view", paginated("admin", "admin-view", 3)),
                "composed", Map.of(
                        "alice/view", composed("alice", "view"),
                        "bob/view", composed("bob", "view"),
                        "admin/admin-view", composed("admin", "admin-view"),
                        "alice/publish", composed("alice", "publish")));
    }

    /** Shapes 1, 2 and 3: the adapter's clause is the whole query. */
    private ShapeResult filtered(String principalId, String action) throws IOException {
        Authorization authorization = authorize(principalId, action);
        return new ShapeResult(
                authorization.kind(),
                index.search(List.of(authorization.clause()), everyRow()));
    }

    /**
     * Shape 4: pagination with {@code from} and {@code size}. Reports page sizes and the sorted
     * ids, not per-page order, because some example stores have no total order.
     */
    private PaginatedShapeResult paginated(String principalId, String action, int pageSize)
            throws IOException {
        Authorization authorization = authorize(principalId, action);
        List<Query> filters = List.of(authorization.clause());

        List<Integer> pageSizes = new ArrayList<>();
        List<String> ids = new ArrayList<>();
        for (int from = 0; ; from += pageSize) {
            List<String> page = index.page(filters, from, pageSize);
            if (page.isEmpty()) {
                break;
            }
            pageSizes.add(page.size());
            ids.addAll(page);
            if (page.size() < pageSize) {
                break;
            }
        }

        return new PaginatedShapeResult(
                authorization.kind(), ids.stream().sorted().toList(), pageSize, pageSizes);
    }

    /**
     * Shape 5: the adapter's clause ANDed with the application's own in one {@code bool.filter}.
     * Run for all three plan kinds, so a denial must not be undone by the application filter.
     */
    private ShapeResult composed(String principalId, String action) throws IOException {
        Authorization authorization = authorize(principalId, action);
        List<Query> filters = new ArrayList<>();
        filters.add(authorization.clause());
        filters.addAll(applicationFilter());
        return new ShapeResult(authorization.kind(), index.search(filters, everyRow()));
    }

    /**
     * The application's own predicate, read from {@code demo/seeds.json} and built with the
     * client's typed builders.
     */
    private List<Query> applicationFilter() {
        DemoSeeds.ApplicationFilter filter = seeds.applicationFilter();
        return List.of(
                Query.of(q -> q.term(t -> t.field("archived").value(filter.archived()))),
                Query.of(q -> q.term(t -> t.field("region").value(filter.region()))));
    }

    /** Actions are passed as a list: {@code plan(Principal, Resource, String)} is deprecated. */
    private PlanResourcesResult plan(String principalId, String action) {
        DemoSeeds.Principal principal = seeds.principal(principalId);
        return cerbos.plan(
                Principal.newInstance(principal.id(), principal.roles().toArray(String[]::new)),
                Resource.newInstance(RESOURCE_KIND),
                List.of(action));
    }

    /**
     * The adapter's answer for one shape.
     *
     * @param kind the plan kind, as {@code demo/expected.json} spells it
     * @param clause the authorization half of the query
     */
    private record Authorization(String kind, Query clause) {}

    /**
     * Translates a plan into its kind and clause. The unconditional kinds get {@code match_all} and
     * {@code match_none} so shape 5 still runs the search. A real application could skip the
     * search on a denial.
     *
     * <p>An if-chain, not a pattern {@code switch}, because the example targets Java 17.
     */
    private Authorization authorize(String principalId, String action) {
        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                plan(principalId, action), OPTIONS);
        if (result instanceof Result.Conditional conditional) {
            return new Authorization("KIND_CONDITIONAL", DemoIndex.query(conditional.query()));
        }
        if (result instanceof Result.AlwaysAllowed) {
            return new Authorization("KIND_ALWAYS_ALLOWED", Query.of(q -> q.matchAll(m -> m)));
        }
        if (result instanceof Result.AlwaysDenied) {
            return new Authorization("KIND_ALWAYS_DENIED", Query.of(q -> q.matchNone(m -> m)));
        }
        throw new IllegalStateException("Unknown adapter result: " + result);
    }

    /** A page size covering every row, since Elasticsearch returns only 10 hits by default. */
    private int everyRow() {
        return seeds.documents().size();
    }
}
