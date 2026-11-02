/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.example.demo;

import dev.cerbos.queryplan.jpa.AttributeMapping;
import dev.cerbos.queryplan.jpa.JpaQueryPlanAdapter;
import dev.cerbos.queryplan.jpa.QueryPlanFilter;
import dev.cerbos.sdk.CerbosBlockingClient;
import dev.cerbos.sdk.PlanResourcesResult;
import dev.cerbos.sdk.builders.Principal;
import dev.cerbos.sdk.builders.Resource;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The five usage shapes of the shared demo domain, run as criteria queries on a plain
 * {@code EntityManager}. The adapter comes from its published Maven artifact, so this also checks
 * packaging.
 *
 * <p>No shape switches on the plan kind: {@code toFilter} returns a filter for every kind. An
 * always-denied plan still runs its query, to show the application's own filter cannot bring a
 * denied row back.
 */
final class DemoShapes {

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
    private final EntityManagerFactory emf;
    private final DemoSeeds seeds;

    DemoShapes(CerbosBlockingClient cerbos, EntityManagerFactory emf, DemoSeeds seeds) {
        this.cerbos = cerbos;
        this.emf = emf;
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

    /** Shapes 1, 2 and 3: the adapter's predicate is the whole WHERE clause. */
    private ShapeResult filtered(String principalId, String action) {
        PlanResourcesResult plan = plan(principalId, action);
        QueryPlanFilter filter = JpaQueryPlanAdapter.toFilter(plan, DOCUMENT_ATTRS);
        return new ShapeResult(kind(plan), select(filter::toPredicate, -1, -1));
    }

    /**
     * Shape 4: {@code setFirstResult}/{@code setMaxResults} over the filter, plus the
     * {@code COUNT} query a pager runs beside it. The count builds its own predicate on its own
     * root: a predicate belongs to the query it was built for.
     *
     * <p>The result is page sizes plus the sorted ids, not per-page contents, because
     * {@code demo/expected.json} is shared with stores that have no total order. The sort is
     * still needed so pages do not repeat or skip rows.
     */
    private PaginatedShapeResult paginated(String principalId, String action, int pageSize) {
        PlanResourcesResult plan = plan(principalId, action);
        QueryPlanFilter filter = JpaQueryPlanAdapter.toFilter(plan, DOCUMENT_ATTRS);
        long total = count(filter);

        List<Integer> pageSizes = new ArrayList<>();
        List<String> ids = new ArrayList<>();
        for (int first = 0; first < total; first += pageSize) {
            List<String> page = select(filter::toPredicate, first, pageSize);
            if (page.isEmpty()) {
                break;
            }
            pageSizes.add(page.size());
            ids.addAll(page);
        }

        return new PaginatedShapeResult(
                kind(plan), ids.stream().sorted().toList(), pageSize, pageSizes);
    }

    /** Shape 5: the adapter's predicate ANDed with the application's own, for every plan kind. */
    private ShapeResult composed(String principalId, String action) {
        PlanResourcesResult plan = plan(principalId, action);
        QueryPlanFilter filter = JpaQueryPlanAdapter.toFilter(plan, DOCUMENT_ATTRS);
        DemoSeeds.ApplicationFilter own = seeds.applicationFilter();
        return new ShapeResult(kind(plan), select((root, query, cb) -> cb.and(
                filter.toPredicate(root, query, cb),
                cb.equal(root.get("archived"), own.archived()),
                cb.equal(root.get("region"), own.region())), -1, -1));
    }

    // -- plumbing --

    /** Builds a shape's WHERE clause for one query. */
    @FunctionalInterface
    private interface Where {
        Predicate on(Root<DemoDocument> root, CriteriaQuery<?> query, CriteriaBuilder cb);
    }

    /** The ids {@code where} selects, ordered by id; one page of them when {@code first >= 0}. */
    private List<String> select(Where where, int first, int max) {
        EntityManager em = emf.createEntityManager();
        try {
            CriteriaBuilder cb = em.getCriteriaBuilder();
            CriteriaQuery<String> query = cb.createQuery(String.class);
            Root<DemoDocument> root = query.from(DemoDocument.class);
            query.select(root.get("id"))
                    .where(where.on(root, query, cb))
                    .orderBy(cb.asc(root.get("id")));
            var typed = em.createQuery(query);
            if (first >= 0) {
                typed.setFirstResult(first).setMaxResults(max);
            }
            return typed.getResultList().stream().sorted().toList();
        } finally {
            em.close();
        }
    }

    private long count(QueryPlanFilter filter) {
        EntityManager em = emf.createEntityManager();
        try {
            CriteriaBuilder cb = em.getCriteriaBuilder();
            CriteriaQuery<Long> query = cb.createQuery(Long.class);
            Root<DemoDocument> root = query.from(DemoDocument.class);
            query.select(cb.count(root)).where(filter.toPredicate(root, query, cb));
            return em.createQuery(query).getSingleResult();
        } finally {
            em.close();
        }
    }

    private PlanResourcesResult plan(String principalId, String action) {
        DemoSeeds.Principal principal = seeds.principal(principalId);
        return cerbos.plan(
                Principal.newInstance(principal.id(), principal.roles().toArray(String[]::new)),
                Resource.newInstance(RESOURCE_KIND),
                List.of(action));
    }

    private void seed() {
        EntityManager em = emf.createEntityManager();
        try {
            em.getTransaction().begin();
            seeds.documents().forEach(document -> em.persist(new DemoDocument(
                    document.id(),
                    document.ownerId(),
                    document.isPublic(),
                    document.region(),
                    document.archived())));
            em.getTransaction().commit();
        } finally {
            em.close();
        }
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
