/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.queryplan.springdata.testmodel.NestedEmbeddable;
import dev.cerbos.queryplan.springdata.testmodel.OwnerEntity;
import dev.cerbos.queryplan.springdata.testmodel.ResourceEntity;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.support.SimpleJpaRepository;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the adapter's Specification through a real Spring Data repository: {@code findAll},
 * {@code count}, pagination, de-duplication and composition. Plans come from the wire fixtures and
 * rows are seeded in H2, so no Docker is needed. Assertions compare the repository's answers with
 * each other rather than with expected ids.
 */
class RepositorySurfaceTest {

    private static EntityManagerFactory emf;

    /**
     * Adds JPA shapes the corpus mapping does not use: a flat {@code @ElementCollection}, an
     * {@code @Embedded} path and a {@code @ManyToOne} path.
     */
    private static final Map<String, AttributeMapping> MAPPING = Map.of(
            // @OneToMany association.
            "request.resource.attr.tags", AttributeMapping.relation("tags", Map.of(
                    "id", AttributeMapping.field("id"),
                    "name", AttributeMapping.field("name"))),
            // Flat @ElementCollection of strings.
            "request.resource.attr.tagNames", AttributeMapping.relation("tagNames"),
            "request.resource.attr.aBool", AttributeMapping.field("aBool"),
            "request.resource.attr.aNumber", AttributeMapping.field("aNumber"),
            // @Embedded path: stays on the resource's own table.
            "request.resource.attr.aString", AttributeMapping.field("nested.aString"),
            // @ManyToOne path: a join.
            "request.resource.attr.aOptionalString", AttributeMapping.field("creator.id"));

    @BeforeAll
    static void setUp() {
        emf = Persistence.createEntityManagerFactory("repository-surface-pu");
        seed();
    }

    @AfterAll
    static void tearDown() {
        if (emf != null) {
            emf.close();
        }
    }

    /**
     * Four rows. "r1" matches both collection filters through two elements each, so a join-based
     * translation would return it twice.
     */
    private static void seed() {
        EntityManager em = emf.createEntityManager();
        em.getTransaction().begin();

        OwnerEntity alice = new OwnerEntity("alice", "Alice", "engineering");
        em.persist(alice);

        ResourceEntity r1 = row("r1", true, "one", 1, alice);
        r1.addTag("r1-t1", "public");
        r1.addTag("r1-t2", "100%_x");
        r1.setTagNames(new ArrayList<>(List.of("public", "other")));

        ResourceEntity r2 = row("r2", false, "two", 2, null);
        r2.addTag("r2-t1", "public");
        r2.setTagNames(new ArrayList<>(List.of("public")));

        ResourceEntity r3 = row("r3", true, "three", 3, null);
        r3.addTag("r3-t1", "internal");
        r3.setTagNames(new ArrayList<>(List.of("internal")));

        // Third match for vf-hasint, so its second size-2 page is partial.
        ResourceEntity r4 = row("r4", false, "one", 4, null);
        r4.setTagNames(new ArrayList<>(List.of("other")));

        for (ResourceEntity r : List.of(r1, r2, r3, r4)) {
            em.persist(r);
        }
        em.getTransaction().commit();
        em.close();
    }

    private static ResourceEntity row(String id, boolean aBool, String aString, int aNumber,
                                      OwnerEntity creator) {
        ResourceEntity r = new ResourceEntity(id);
        r.setaBool(aBool);
        r.setaString(aString);
        r.setaNumber(aNumber);
        NestedEmbeddable nested = new NestedEmbeddable();
        nested.setaBool(aBool);
        nested.setaString(aString);
        nested.setaNumber(aNumber);
        r.setNested(nested);
        r.setCreator(creator);
        return r;
    }

    private static Specification<ResourceEntity> specFor(String action) {
        PlanResourcesResponse plan = Corpus.planFromWireFixture(action);
        return SpringDataQueryPlanAdapter.toSpecification(plan, MAPPING, Map.of());
    }

    private static SimpleJpaRepository<ResourceEntity, String> repository(EntityManager em) {
        return new SimpleJpaRepository<>(ResourceEntity.class, em);
    }

    private static List<String> idsInOrder(List<ResourceEntity> entities) {
        return entities.stream().map(ResourceEntity::getId).toList();
    }

    private static List<String> sortedIds(List<ResourceEntity> entities) {
        return entities.stream().map(ResourceEntity::getId).sorted().toList();
    }

    /**
     * Checks that {@code findAll}, {@code count}, a full page and a sorted {@code findAll} agree,
     * all on one Specification instance. Reuse matters: a cached {@code Predicate} makes
     * Hibernate 6 throw {@code SqlTreeCreationException} on the second query.
     */
    private void assertRepositorySurface(Specification<ResourceEntity> spec) {
        EntityManager em = emf.createEntityManager();
        try {
            SimpleJpaRepository<ResourceEntity, String> repository = repository(em);

            List<ResourceEntity> found = repository.findAll(spec);
            List<String> ids = sortedIds(found);
            assertEquals(new ArrayList<>(new LinkedHashSet<>(ids)), ids,
                    "findAll(spec) must return one row per matching entity");
            assertEquals(ids.size(), repository.count(spec), "count(spec) must agree with findAll");

            // A page exactly full, so Spring Data runs the separate COUNT query.
            Page<ResourceEntity> page =
                    repository.findAll(spec, PageRequest.of(0, ids.size(), Sort.by("id")));
            assertEquals(ids, idsInOrder(page.getContent()), "page content identities");
            assertEquals(ids.size(), page.getTotalElements(),
                    "getTotalElements must agree with the page content");
            assertEquals(1, page.getTotalPages(), "everything fits on one page");

            assertEquals(ids, idsInOrder(repository.findAll(spec, Sort.by("id"))),
                    "findAll(spec, Sort) identities");

            assertFalse(ids.isEmpty(), "the filter matched no row: every relation above is vacuous");
            assertTrue(ids.size() < repository.count(),
                    "the filter matched every row: every relation above is vacuous");
        } finally {
            em.close();
        }
    }

    /** One action per translation shape. */
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {
            // eq on the @Embedded path.
            "cs-eq",
            // exists over the @OneToMany.
            "exists-on-empty",
            // hasIntersection over the @ElementCollection, value first.
            "vf-hasint",
            // 0 < size(tags).
            "vf-size",
            // A bare boolean attribute as the whole condition.
            "root-bare-bool"})
    void theRepositoryContractHoldsForEveryTranslationShape(String action) {
        assertRepositorySurface(specFor(action));
    }

    /**
     * Paging calls {@code toPredicate} again on the same instance for the COUNT query, so the
     * adapter must rebuild its predicate on every call.
     */
    @Test
    void pageableFindAllInvokesToPredicateTwiceOnOneSpecification() {
        CountingSpecification spec = new CountingSpecification(specFor("vf-hasint"));
        EntityManager em = emf.createEntityManager();
        try {
            SimpleJpaRepository<ResourceEntity, String> repository = repository(em);
            List<String> all = sortedIds(repository.findAll(spec));
            assertEquals(3, all.size(), "the fixture must match more rows than fit on one page");
            spec.invocations.set(0);

            // A full first page, so Spring Data runs the COUNT query.
            Page<ResourceEntity> page0 = repository.findAll(spec, PageRequest.of(0, 2, Sort.by("id")));
            assertEquals(all.subList(0, 2), idsInOrder(page0.getContent()));
            assertEquals(all.size(), page0.getTotalElements());
            assertEquals(2, spec.invocations.get(),
                    "findAll(spec, Pageable) with a full page must invoke toPredicate exactly "
                            + "twice (content query + count query) on one instance");

            // A partial last page: Spring Data computes the total without a COUNT query.
            Page<ResourceEntity> page1 = repository.findAll(spec, PageRequest.of(1, 2, Sort.by("id")));
            assertEquals(all.subList(2, 3), idsInOrder(page1.getContent()));
            assertEquals(all.size(), page1.getTotalElements(),
                    "page totals must stay consistent across pages");
            assertFalse(page1.hasNext(), "three matching rows fill exactly two size-2 pages");
            assertEquals(3, spec.invocations.get(),
                    "the same Specification instance is re-invoked for every execution");
        } finally {
            em.close();
        }
    }

    /**
     * "r1" matches both collection filters through two elements (see {@link #seed()}). A join
     * would give one SQL row per matching element. Hibernate de-duplicates {@code findAll} in
     * memory, so the checks that catch it are {@code count(spec)} and a page sized exactly to the
     * match count, whose LIMIT would cut off an entity. Both collection mappings are covered.
     *
     * <p>The first assertion guards against vacuity: r1 must have at least two matching elements.
     */
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"p-hasintersection-map", "vf-hasint"})
    void aMultiElementMatchDoesNotDuplicateTheEntity(String action) {
        // The policy's literal lists (conformance/policies/adversarial.yaml) and the element the
        // mapping above resolves each action's collection to.
        String elementJpql = switch (action) {
            case "p-hasintersection-map" -> "select count(t) from ResourceEntity r join r.tags t "
                    + "where r.id = 'r1' and t.name in ('public', 'héllo🚀', '100%_x')";
            case "vf-hasint" -> "select count(t) from ResourceEntity r join r.tagNames t "
                    + "where r.id = 'r1' and t in ('public', 'other')";
            default -> throw new IllegalArgumentException(action);
        };
        EntityManager em = emf.createEntityManager();
        try {
            long matchingElements = em.createQuery(elementJpql, Long.class).getSingleResult();
            assertTrue(matchingElements >= 2,
                    "r1 must match " + action + " through at least two elements, else a join could "
                            + "not duplicate it and nothing here is a duplication test; it has "
                            + matchingElements);

            SimpleJpaRepository<ResourceEntity, String> repository = repository(em);
            Specification<ResourceEntity> spec = specFor(action);

            List<ResourceEntity> found = repository.findAll(spec);
            assertEquals(1, found.stream().filter(r -> "r1".equals(r.getId())).count(),
                    "the two-element row must come back exactly once");
            assertEquals(Set.copyOf(sortedIds(found)).size(), found.size(),
                    "an entity with several matching collection elements must come back once");

            assertEquals(found.size(), repository.count(spec),
                    "count(spec) must count entities, not matching collection rows");

            // Page size == the matching count, so the page is exactly full and Spring Data must
            // fire the separate COUNT query to compute the total.
            Page<ResourceEntity> page =
                    repository.findAll(spec, PageRequest.of(0, found.size(), Sort.by("id")));
            assertEquals(sortedIds(found), idsInOrder(page.getContent()));
            assertEquals(found.size(), page.getTotalElements(),
                    "the pagination COUNT query must count entities, not matching collection rows");
        } finally {
            em.close();
        }
    }

    /** {@code .and(...)} with a caller Specification returns the intersection, in either order. */
    @Test
    void composesWithACallerSpecificationInBothOrders() {
        Specification<ResourceEntity> cerbos = specFor("vf-hasint");
        Specification<ResourceEntity> caller =
                (root, query, cb) -> cb.equal(root.get("aBool"), true);

        EntityManager em = emf.createEntityManager();
        try {
            SimpleJpaRepository<ResourceEntity, String> repository = repository(em);
            Set<String> fromCerbos = Set.copyOf(sortedIds(repository.findAll(cerbos)));
            Set<String> fromCaller = Set.copyOf(sortedIds(repository.findAll(caller)));
            Set<String> intersection = new LinkedHashSet<>(fromCerbos);
            intersection.retainAll(fromCaller);

            // Each side must exclude a row the other admits, or dropping one operand would pass.
            assertFalse(intersection.isEmpty(), "the two filters must overlap");
            assertTrue(intersection.size() < fromCerbos.size(), "the caller filter must narrow");
            assertTrue(intersection.size() < fromCaller.size(), "the adapter filter must narrow");

            assertEquals(intersection, Set.copyOf(sortedIds(repository.findAll(cerbos.and(caller)))));
            assertEquals(intersection, Set.copyOf(sortedIds(repository.findAll(caller.and(cerbos)))));
        } finally {
            em.close();
        }
    }

    /** Dotted field paths through an {@code @Embedded} value and a {@code @ManyToOne} join. */
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"cs-eq", "optional-ne"})
    void aDottedFieldPathTraversesEmbeddablesAndToOneAssociations(String action) {
        // cs-eq reads aString, mapped here to the @Embedded nested.aString; optional-ne reads
        // aOptionalString, mapped to the @ManyToOne creator.id.
        EntityManager em = emf.createEntityManager();
        try {
            List<String> ids = sortedIds(repository(em).findAll(specFor(action)));
            assertFalse(ids.isEmpty(), "the dotted path resolved to no row at all");
            assertTrue(ids.size() < 4, "the dotted path matched every row");
        } finally {
            em.close();
        }
    }

    /** Counts {@code toPredicate} calls. */
    private static final class CountingSpecification implements Specification<ResourceEntity> {
        private final Specification<ResourceEntity> delegate;
        final AtomicInteger invocations = new AtomicInteger();

        CountingSpecification(Specification<ResourceEntity> delegate) {
            this.delegate = delegate;
        }

        @Override
        public Predicate toPredicate(Root<ResourceEntity> root, CriteriaQuery<?> query,
                                     CriteriaBuilder criteriaBuilder) {
            invocations.incrementAndGet();
            return delegate.toPredicate(root, query, criteriaBuilder);
        }
    }
}
