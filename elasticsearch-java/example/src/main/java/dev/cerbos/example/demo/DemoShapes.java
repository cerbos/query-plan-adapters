package dev.cerbos.example.demo;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Result;
import dev.cerbos.sdk.CerbosBlockingClient;
import dev.cerbos.sdk.PlanResourcesResult;
import dev.cerbos.sdk.builders.Principal;
import dev.cerbos.sdk.builders.Resource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The five usage shapes of the shared demo domain, against a real Elasticsearch index reached
 * through the official Elasticsearch Java client.
 *
 * <p>This is not a test of what the adapter translates —
 * {@code ../src/test/java/.../ElasticsearchAdversarialConformanceTest.java} proves that against a
 * hostile corpus with a live PDP as the oracle. It proves the two things that harness structurally
 * cannot:
 *
 * <ol>
 *   <li><b>Packaging.</b> The adapter imports above resolve through its real Maven coordinate rather
 *       than through its source set, so its POM and Gradle module metadata are executed —
 *       {@code settings.gradle.kts} and {@code README.md} are where that argument lives.</li>
 *   <li><b>Usage shape.</b> A harness runs one flat filtered query, and posts it as raw JSON over a
 *       bare HTTP client. Consumers paginate, compose the adapter's clause with clauses of their
 *       own, and hand the result to the client library. Shape 5 below is the one that earns the
 *       exercise.</li>
 * </ol>
 *
 * <p>The plan kind is reported alongside the ids because {@code demo/expected.json} pins it: that is
 * what stops this program returning all the rows for {@code admin-view} without ever having reached
 * the PDP. It is read off the adapter's own {@link Result}, not off the SDK's plan predicates,
 * because the {@code Result} variant is what the rest of this class actually branches on — deriving
 * it from the plan instead would leave the adapter free to classify a plan one way and this program
 * to report it another.
 */
final class DemoShapes {

    private static final String RESOURCE_KIND = "document";

    /**
     * Cerbos attribute names are not Elasticsearch field names, so a consumer always writes one of
     * these. Both entries are load-bearing: an unmapped attribute makes the adapter throw rather
     * than guess a field, and {@code public} maps to {@code isPublic} because the policy's name for
     * the attribute and the index's name for the field are allowed to differ — which is the point of
     * having a map at all.
     *
     * <p>{@code region} and {@code archived} are deliberately absent: no rule in
     * {@code demo/policies/document.yaml} names them, and they exist so the application can own a
     * predicate the policy never sees.
     */
    private static final Map<String, String> DOCUMENT_FIELDS = Map.of(
            "request.resource.attr.ownerId", "ownerId",
            "request.resource.attr.public", "isPublic");

    /**
     * One shape's answer, as {@code demo/expected.json} spells it: the plan kind, and the ids the
     * search returned. Typed rather than a {@code Map<String, Object>} so a mistyped key is a
     * compile error here instead of a diff in the shared runner.
     */
    record ShapeResult(String kind, List<String> ids) {}

    /** Shape 4 additionally reports the page size it asked for and the size of each page read. */
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

    /** Seeds the index, runs every shape, and returns the {@code shapes} object to emit. */
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

    // -- the five usage shapes --

    /** Shapes 1, 2 and 3: a plain filtered list — the adapter's clause is the whole query. */
    private ShapeResult filtered(String principalId, String action) throws IOException {
        Authorization authorization = authorize(principalId, action);
        return new ShapeResult(
                authorization.kind(),
                index.search(List.of(authorization.clause()), everyRow()));
    }

    /**
     * Shape 4: pagination applied on top of the filter, through Elasticsearch's {@code from} and
     * {@code size}.
     *
     * <p>Reported as page SIZES plus the sorted union of the ids, never as per-page order:
     * {@code demo/expected.json} is shared by every example and several of the stores behind it have
     * no total order to paginate by. Pages are read until one comes back short, which is what stops
     * the loop on a total that is an exact multiple of the page size as well as on one that is not.
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
     * Shape 5: the adapter's clause ANDed with the application's own, as two more entries in the
     * same {@code bool.filter} array.
     *
     * <p>All three plan kinds go through here on purpose. An {@code ALWAYS_ALLOWED} plan has no
     * clause to AND with, and an {@code ALWAYS_DENIED} one must not have its denial undone — the two
     * cases that break first.
     */
    private ShapeResult composed(String principalId, String action) throws IOException {
        Authorization authorization = authorize(principalId, action);
        List<Query> filters = new ArrayList<>();
        filters.add(authorization.clause());
        filters.addAll(applicationFilter());
        return new ShapeResult(authorization.kind(), index.search(filters, everyRow()));
    }

    /**
     * The APPLICATION's own predicate — {@code archived == false AND region == 'emea'} — read from
     * {@code demo/seeds.json} rather than written out here, so it cannot drift from the expectations
     * {@code demo/scripts/validate-demo.sh} recomputes from the same field.
     *
     * <p>Written with the client's own typed builders, unlike the adapter's clause, which arrives as
     * a map and is parsed. That asymmetry is the shape of the composition a consumer actually makes,
     * and it is why shape 5 is the one that earns the exercise: nothing in
     * {@code demo/policies/document.yaml} mentions either field.
     */
    private List<Query> applicationFilter() {
        DemoSeeds.ApplicationFilter filter = seeds.applicationFilter();
        return List.of(
                Query.of(q -> q.term(t -> t.field("archived").value(filter.archived()))),
                Query.of(q -> q.term(t -> t.field("region").value(filter.region()))));
    }

    // -- plumbing --

    /**
     * The plan for one principal and one action.
     *
     * <p>The action is passed as a single-element list because {@code plan(Principal, Resource,
     * String)} is deprecated in cerbos-sdk-java 0.19.0 — a detail an example is exactly the wrong
     * place to hide, since it is the code a consumer copies.
     */
    private PlanResourcesResult plan(String principalId, String action) {
        DemoSeeds.Principal principal = seeds.principal(principalId);
        return cerbos.plan(
                Principal.newInstance(principal.id(), principal.roles().toArray(String[]::new)),
                Resource.newInstance(RESOURCE_KIND),
                List.of(action));
    }

    /**
     * The adapter's answer, in the two forms every shape here needs: the plan kind as
     * {@code demo/expected.json} spells it, and one {@code bool.filter} entry.
     *
     * @param kind the plan kind, for the emitted document
     * @param clause the authorization half of the query
     */
    private record Authorization(String kind, Query clause) {}

    /**
     * Translates a plan into both halves at once.
     *
     * <p>One cascade over the sealed {@link Result} rather than two — a kind and a clause read off the
     * same variant in one place cannot disagree about which variant it was.
     *
     * <p>Both unconditional kinds are given an explicit clause rather than being special-cased out of
     * the search. {@code ALWAYS_ALLOWED} could equally be an empty filter list — Elasticsearch accepts
     * {@code {"bool":{"filter":[]}}} and returns every document — and a denial could skip the round
     * trip entirely, which is the optimisation the adapter's README shows. Neither is what shape 5
     * needs: the property it exists to check is that the application's own predicate cannot resurrect
     * a denied document, and only a search that actually runs with both halves in place demonstrates
     * that.
     *
     * <p>An if-chain rather than an exhaustive {@code switch} over the sealed type: pattern matching
     * for {@code switch} is standard from Java 21, and this example is compiled against the adapter's
     * declared floor of Java 17 (see {@code build.gradle.kts}).
     */
    private Authorization authorize(String principalId, String action) {
        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                plan(principalId, action), DOCUMENT_FIELDS);
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

    /**
     * A page big enough for the whole index, so an unpaginated shape is never silently truncated to
     * Elasticsearch's default page of 10. Derived from the corpus rather than a constant, so a seed
     * row added to {@code demo/seeds.json} does not quietly start being dropped.
     */
    private int everyRow() {
        return seeds.documents().size();
    }
}
