package dev.cerbos.example.demo;

import dev.cerbos.queryplan.springdata.AttributeMapping;
import dev.cerbos.queryplan.springdata.SpringDataQueryPlanAdapter;
import dev.cerbos.sdk.CerbosBlockingClient;
import dev.cerbos.sdk.PlanResourcesResult;
import dev.cerbos.sdk.builders.Principal;
import dev.cerbos.sdk.builders.Resource;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The five usage shapes of the shared demo domain, against a real
 * {@code JpaSpecificationExecutor}.
 *
 * <p>This is not a test of what the adapter translates —
 * {@code ../src/test/java/.../AdversarialConformanceTest.java} proves that against a hostile
 * corpus with a live PDP as the oracle, on H2, PostgreSQL and MySQL. It proves the two things
 * that harness structurally cannot:
 *
 * <ol>
 *   <li><b>Packaging.</b> The imports above resolve through the adapter's real Maven coordinate,
 *       published to mavenLocal by {@code run.sh}, so its POM and Gradle module metadata are
 *       executed — dependency scopes included. The harness compiles against the adapter's own
 *       source set and touches none of that. See
 *       {@code docs/adr/0002-examples-install-the-packed-artifact.md}.</li>
 *   <li><b>Usage shape.</b> A harness runs one flat filtered query. Consumers also paginate, and
 *       compose the adapter's Specification with Specifications of their own. Shape 5 below is
 *       the one that earns the exercise.</li>
 * </ol>
 *
 * <p>None of the five methods switches on the plan kind, and that is the adapter's design rather
 * than an omission: every {@code toSpecification} overload returns a Specification covering all
 * three kinds — {@code Specification.unrestricted()}, an always-false predicate, or the
 * translated tree — so composition in shape 5 is the same line of code for all of them. The
 * kind is reported alongside the ids because {@code demo/expected.json} pins it: that is what
 * stops this program returning all eight rows for {@code admin-view} without ever having
 * reached the PDP.
 *
 * <p>An {@code ALWAYS_DENIED} plan still runs its query here. Skipping the database on
 * {@code planResult.isAlwaysDenied()} is a documented and supported optimisation, but executing
 * {@code 1=0 AND <application predicate>} is what actually demonstrates the property shape 5
 * exists to check — that the application's own predicate cannot resurrect a denied row.
 */
@Component
class DemoShapes {

    private static final String RESOURCE_KIND = "document";

    /**
     * Cerbos attribute names are not JPA paths, so a consumer always writes one of these. Both
     * entries are load-bearing: an unmapped attribute makes the adapter throw rather than guess a
     * column, and {@code public} maps to {@code isPublic} because the policy's name for the
     * attribute and the entity's name for the field are allowed to differ — which is the point
     * of having a mapping at all.
     *
     * <p>{@code region} and {@code archived} are deliberately absent: no rule in
     * {@code demo/policies/document.yaml} names them, and they exist so the application can own a
     * predicate the policy never sees.
     */
    private static final Map<String, AttributeMapping> DOCUMENT_ATTRS = Map.of(
            "request.resource.attr.ownerId", AttributeMapping.field("ownerId"),
            "request.resource.attr.public", AttributeMapping.field("isPublic"));

    /**
     * One shape's answer, as {@code demo/expected.json} spells it: the plan kind, and the ids the
     * query returned. Typed rather than a {@code Map<String, Object>} so a mistyped key is a
     * compile error here instead of a diff in the shared runner.
     */
    record ShapeResult(String kind, List<String> ids) {}

    /** Shape 4 additionally reports the page size it asked for and the size of each page read. */
    record PaginatedShapeResult(String kind, List<String> ids, int pageSize,
                               List<Integer> pageSizes) {}

    private final CerbosBlockingClient cerbos;
    private final DemoDocumentRepository repository;
    private final DemoSeeds seeds;

    DemoShapes(CerbosBlockingClient cerbos, DemoDocumentRepository repository, DemoSeeds seeds) {
        this.cerbos = cerbos;
        this.repository = repository;
        this.seeds = seeds;
    }

    /** Seeds the store, runs every shape, and returns the {@code shapes} object to emit. */
    Map<String, Object> run() {
        seed();

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

    /** Shapes 1, 2 and 3: a plain filtered list — the adapter's Specification is the whole query. */
    private ShapeResult filtered(String principalId, String action) {
        PlanResourcesResult plan = plan(principalId, action);
        return new ShapeResult(kind(plan), ids(repository.findAll(authorization(plan))));
    }

    /**
     * Shape 4: pagination applied on top of the filter, through
     * {@code findAll(Specification, Pageable)} — which fires a separate COUNT query with its own
     * {@code CriteriaQuery} and {@code Root}, so the Specification is evaluated twice per page
     * against different roots.
     *
     * <p>Reported as page SIZES plus the sorted union of the ids, never as per-page order:
     * {@code demo/expected.json} is shared by ten stores and several of them have no total order
     * to paginate by. The {@code Sort} is still required for the paging itself to be correct —
     * without a total order, successive pages may repeat or omit rows — which is a separate
     * concern from how the result is asserted.
     */
    private PaginatedShapeResult paginated(String principalId, String action, int pageSize) {
        PlanResourcesResult plan = plan(principalId, action);
        Specification<DemoDocument> specification = authorization(plan);

        List<Integer> pageSizes = new ArrayList<>();
        List<String> ids = new ArrayList<>();
        for (int pageNumber = 0; ; pageNumber++) {
            Page<DemoDocument> page = repository.findAll(
                    specification, PageRequest.of(pageNumber, pageSize, Sort.by("id")));
            if (page.getNumberOfElements() == 0) {
                break;
            }
            pageSizes.add(page.getNumberOfElements());
            page.forEach(document -> ids.add(document.getId()));
            if (!page.hasNext()) {
                break;
            }
        }

        return new PaginatedShapeResult(
                kind(plan), ids.stream().sorted().toList(), pageSize, pageSizes);
    }

    /**
     * Shape 5: the adapter's Specification ANDed with the application's own. All three plan kinds
     * go through here on purpose — an {@code ALWAYS_ALLOWED} plan has no predicate to AND with
     * (the adapter returns {@code Specification.unrestricted()}, a true identity for
     * {@code and()}), and an {@code ALWAYS_DENIED} one must not have its denial undone.
     */
    private ShapeResult composed(String principalId, String action) {
        PlanResourcesResult plan = plan(principalId, action);
        Specification<DemoDocument> specification = authorization(plan).and(applicationFilter());
        return new ShapeResult(kind(plan), ids(repository.findAll(specification)));
    }

    /**
     * The APPLICATION's own predicate — {@code archived == false AND region == 'emea'} — read
     * from {@code demo/seeds.json} rather than written out here, so it cannot drift from the
     * expectations {@code demo/scripts/validate-demo.sh} recomputes from the same field.
     *
     * <p>Nothing in {@code demo/policies/document.yaml} mentions either column. That is what
     * makes shape 5 a composition rather than a second copy of the authorization filter.
     */
    private Specification<DemoDocument> applicationFilter() {
        DemoSeeds.ApplicationFilter filter = seeds.applicationFilter();
        return (root, query, cb) -> cb.and(
                cb.equal(root.get("archived"), filter.archived()),
                cb.equal(root.get("region"), filter.region()));
    }

    // -- plumbing --

    private PlanResourcesResult plan(String principalId, String action) {
        DemoSeeds.Principal principal = seeds.principal(principalId);
        return cerbos.plan(
                Principal.newInstance(principal.id(), principal.roles().toArray(String[]::new)),
                Resource.newInstance(RESOURCE_KIND),
                action);
    }

    private Specification<DemoDocument> authorization(PlanResourcesResult plan) {
        return SpringDataQueryPlanAdapter.toSpecification(plan, DOCUMENT_ATTRS);
    }

    private void seed() {
        repository.deleteAll();
        repository.saveAll(seeds.documents().stream()
                .map(document -> new DemoDocument(
                        document.id(),
                        document.ownerId(),
                        document.isPublic(),
                        document.region(),
                        document.archived()))
                .toList());
    }

    private static List<String> ids(List<DemoDocument> documents) {
        return documents.stream().map(DemoDocument::getId).sorted().toList();
    }

    /**
     * The plan kind as {@code demo/expected.json} spells it, derived from the SDK's own
     * predicates rather than from {@code getRaw()}. Reading the protobuf would make this example
     * compile against protobuf types a consumer of the adapter never has to name.
     */
    private static String kind(PlanResourcesResult plan) {
        if (plan.isAlwaysAllowed()) {
            return "KIND_ALWAYS_ALLOWED";
        }
        if (plan.isAlwaysDenied()) {
            return "KIND_ALWAYS_DENIED";
        }
        if (plan.isConditional()) {
            return "KIND_CONDITIONAL";
        }
        throw new IllegalStateException(
                "PDP returned a plan that is neither allowed, denied nor conditional");
    }
}
