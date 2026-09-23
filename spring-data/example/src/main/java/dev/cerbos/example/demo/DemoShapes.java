/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

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
 * The five usage shapes of the shared demo domain, run against a real
 * {@code JpaSpecificationExecutor}. The adapter comes from its published Maven artifact, so this
 * also checks packaging.
 *
 * <p>No shape switches on the plan kind: {@code toSpecification} returns a Specification for
 * every kind. An always-denied plan still runs its query, to show the application's own filter
 * cannot bring a denied row back.
 */
@Component
class DemoShapes {

    private static final String RESOURCE_KIND = "document";

    // Maps policy attributes to entity fields. The adapter throws on an unmapped attribute.
    // region and archived are left out because no policy rule reads them.
    private static final Map<String, AttributeMapping> DOCUMENT_ATTRS = Map.of(
            "request.resource.attr.ownerId", AttributeMapping.field("ownerId"),
            "request.resource.attr.public", AttributeMapping.field("isPublic"));

    /** One shape's result as {@code demo/expected.json} spells it. */
    record ShapeResult(String kind, List<String> ids) {}

    /** Shape 4's result, which also reports the requested page size and each page's size. */
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

    /** Seeds the store, runs every shape and returns the {@code shapes} object to print. */
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

    /** Shapes 1, 2 and 3: the adapter's Specification is the whole query. */
    private ShapeResult filtered(String principalId, String action) {
        PlanResourcesResult plan = plan(principalId, action);
        return new ShapeResult(kind(plan), ids(repository.findAll(authorization(plan))));
    }

    /**
     * Shape 4: {@code findAll(Specification, Pageable)}, which also runs a COUNT query that
     * evaluates the Specification against a second root.
     *
     * <p>The result is page sizes plus the sorted ids, not per-page contents, because
     * {@code demo/expected.json} is shared with stores that have no total order. The sort is
     * still needed so pages do not repeat or skip rows.
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
     * Shape 5: the adapter's Specification ANDed with the application's own, for every plan
     * kind.
     */
    private ShapeResult composed(String principalId, String action) {
        PlanResourcesResult plan = plan(principalId, action);
        Specification<DemoDocument> specification = authorization(plan).and(applicationFilter());
        return new ShapeResult(kind(plan), ids(repository.findAll(specification)));
    }

    // The application's own filter, read from demo/seeds.json so it matches what
    // validate-demo.sh checks. The policy never reads these columns.
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
                List.of(action));
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

    // Uses the SDK's predicates rather than getRaw(), so the example needs no protobuf types.
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
