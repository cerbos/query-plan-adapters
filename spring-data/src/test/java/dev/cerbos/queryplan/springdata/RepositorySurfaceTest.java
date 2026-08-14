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
 * The Spring Data glue, executed: what a real {@code JpaSpecificationExecutor} does with the
 * Specification this adapter returns.
 *
 * <p>Nothing here is a claim about translation — {@link SpringDataTranslatorTest} pins the SQL and
 * {@link AdversarialConformanceTest} proves the rows against {@code check()}. What this covers is
 * the layer between: {@code findAll} / {@code count} / {@code findAll(spec, Pageable)} on ONE
 * Specification instance, the separate COUNT query pagination fires, entity de-duplication over a
 * multi-element collection match, and composition with the caller's own Specification. Every
 * corpus harness in this repository runs its query with an explicit {@code distinct(true)}, which
 * is exactly what would mask a join-based translation duplicating an entity.
 *
 * <p><strong>Why this is not a third classification bucket.</strong> #372's binary triage — every
 * surviving shape becomes a corpus action, there is no "unit-test-only" shape — is about SHAPES:
 * what the planner can emit and what the adapter translates. Nothing here is a shape. "One
 * Specification instance survives being invoked twice" is a fact about Spring Data, and the corpus
 * has no way to ask it: every conformance harness calls {@code toPredicate} once, through a query
 * it builds itself. {@code demo/} exercises pagination and composition through the published
 * artifact and overlaps this deliberately; what it does not do is count invocations, compare
 * {@code count(spec)} with {@code findAll(spec)}, or seed a row that matches through two elements.
 *
 * <p><strong>Plans from fixtures, expectations from invariants.</strong> The plans come from
 * {@code conformance/wire-fixtures/} like every other suite here, so no PDP and no policy file is
 * involved. The rows are seeded locally because they are INPUTS — this suite needs a row matching
 * through two collection elements, which no corpus seed is obliged to carry — and no assertion
 * names an expected id: each one relates the repository's answers to each other, with a
 * non-degenerate check that the filter matched some rows and not all of them. Which rows a filter
 * ought to return is the {@code check()} oracle's question, and it is answered elsewhere.
 */
class RepositorySurfaceTest {

    private static EntityManagerFactory emf;

    /**
     * A mapping of this suite's own, over the SAME wire fixtures. The corpus mapping reaches the
     * {@code @OneToMany} tag entities; this one adds the two JPA shapes the corpus model does not
     * use and this adapter's README documents — a flat {@code @ElementCollection} and an
     * {@code @Embedded} dotted path — plus a {@code @ManyToOne} traversal.
     */
    private static final Map<String, AttributeMapping> MAPPING = Map.of(
            // @OneToMany association, reached as a correlated subquery.
            "request.resource.attr.tags", AttributeMapping.relation("tags", Map.of(
                    "id", AttributeMapping.field("id"),
                    "name", AttributeMapping.field("name"))),
            // Flat @ElementCollection of strings — the other collection shape a caller can map.
            "request.resource.attr.tagNames", AttributeMapping.relation("tagNames"),
            "request.resource.attr.aBool", AttributeMapping.field("aBool"),
            "request.resource.attr.aNumber", AttributeMapping.field("aNumber"),
            // @Embedded dotted path (README "Field mapping"): resolved segment by segment against
            // the embeddable, so it stays on the resource's own table.
            "request.resource.attr.aString", AttributeMapping.field("nested.aString"),
            // @ManyToOne traversal: the same dotted spelling, but a join.
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
     * Four rows, of which "r1" matches the collection filters through TWO elements — the row a
     * join-based translation would return twice.
     */
    private static void seed() {
        EntityManager em = emf.createEntityManager();
        em.getTransaction().begin();

        OwnerEntity alice = new OwnerEntity("alice", "Alice", "engineering");
        em.persist(alice);

        ResourceEntity r1 = row("r1", true, "one", 1, alice);
        r1.addTag("r1-t1", "public");
        r1.addTag("r1-t2", "private");
        r1.setTagNames(new ArrayList<>(List.of("public", "private")));

        ResourceEntity r2 = row("r2", false, "two", 2, null);
        r2.addTag("r2-t1", "public");
        r2.setTagNames(new ArrayList<>(List.of("public")));

        ResourceEntity r3 = row("r3", true, "three", 3, null);
        r3.addTag("r3-t1", "internal");
        r3.setTagNames(new ArrayList<>(List.of("internal")));

        // A third row matching the flat @ElementCollection filter, so that filter's last page is
        // a PARTIAL one — which is what makes the count-query assertion below discriminate.
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

    /** The Specification this adapter returns for one corpus action, read from its wire fixture. */
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
     * The whole repository contract for ONE Specification instance, related to itself rather than
     * to a written-down row set: {@code findAll} agrees with {@code count}, a full page agrees
     * with both and reports the same total, and {@code findAll(spec, Sort)} returns the same
     * identities. Reusing one instance across all four executions also exercises the
     * re-invocation contract — a cached {@code Predicate} makes Hibernate 6 throw
     * {@code SqlTreeCreationException} on the second query.
     *
     * <p>The anti-vacuity check is the last line: a filter matching nothing, or everything, would
     * satisfy every relation above while proving none of them.
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

            // Page size == the matching count, so the page comes back exactly full and Spring Data
            // must fire the separate COUNT query to compute the total.
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

    /**
     * One representative per translation shape, all through the real repository glue: a scalar
     * column, a correlated EXISTS over an association, a flat {@code @ElementCollection}, a
     * correlated COUNT, and a lambda body.
     */
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {
            // eq on a scalar — here an @Embedded dotted path.
            "cs-eq",
            // exists(tags, t.name == "public") — a correlated subquery over the @OneToMany.
            "exists-on-empty",
            // hasIntersection over the flat @ElementCollection, value-first.
            "vf-hasint",
            // 0 < size(tags) — a correlated COUNT.
            "vf-size",
            // R.attr.aBool at the root of the condition.
            "root-bare-bool"})
    void theRepositoryContractHoldsForEveryTranslationShape(String action) {
        assertRepositorySurface(specFor(action));
    }

    /**
     * {@code findAll(spec, Pageable)} fires a separate COUNT query and re-invokes the SAME
     * Specification instance for it — the reason the adapter rebuilds the whole predicate tree
     * from the {@code Root}/{@code CriteriaQuery} on every call. Nothing else in this repository
     * executes that path: every conformance harness calls {@code toPredicate} exactly once.
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

            // A full first page, so Spring Data MUST run the separate COUNT query.
            Page<ResourceEntity> page0 = repository.findAll(spec, PageRequest.of(0, 2, Sort.by("id")));
            assertEquals(all.subList(0, 2), idsInOrder(page0.getContent()));
            assertEquals(all.size(), page0.getTotalElements());
            assertEquals(2, spec.invocations.get(),
                    "findAll(spec, Pageable) with a full page must invoke toPredicate exactly "
                            + "twice (content query + count query) on one instance");

            // The same instance again for the last page, which comes back PARTIAL: content query
            // only — Spring Data derives the total from offset + content size without a COUNT.
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
     * "r1" matches both collection filters through TWO elements. This pins the correlated-EXISTS
     * translation strategy: a regression to a root join would return it once per matching element,
     * {@code findAll} would yield more entities than there are matches, and {@code getTotalElements}
     * would disagree with the de-duplicated content — breaking page math. Both collection shapes a
     * caller can map are covered, because the hazard is the join, not the mapping.
     */
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"p-hasintersection-map", "vf-hasint"})
    void aMultiElementMatchDoesNotDuplicateTheEntity(String action) {
        EntityManager em = emf.createEntityManager();
        try {
            SimpleJpaRepository<ResourceEntity, String> repository = repository(em);
            Specification<ResourceEntity> spec = specFor(action);

            List<ResourceEntity> found = repository.findAll(spec);
            assertTrue(found.stream().anyMatch(r -> "r1".equals(r.getId())),
                    "the two-element row must match, else nothing here is a duplication test");
            assertEquals(Set.copyOf(sortedIds(found)).size(), found.size(),
                    "an entity with several matching collection elements must come back once");

            Page<ResourceEntity> page =
                    repository.findAll(spec, PageRequest.of(0, 10, Sort.by("id")));
            assertEquals(sortedIds(found), idsInOrder(page.getContent()));
            assertEquals(found.size(), page.getTotalElements(),
                    "the count query must count entities, not matching collection rows");
        } finally {
            em.close();
        }
    }

    /**
     * The README promises the returned Specification composes with the caller's own via
     * {@code .and(...)}. Asserted as set intersection rather than against a written-down row set,
     * in both composition orders — a composition that dropped one side would show up as the
     * intersection being wrong rather than as an id list needing maintenance.
     */
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

            // Non-degenerate: each side must exclude a row the other admits, or an `and` that
            // dropped either operand would still produce the intersection.
            assertFalse(intersection.isEmpty(), "the two filters must overlap");
            assertTrue(intersection.size() < fromCerbos.size(), "the caller filter must narrow");
            assertTrue(intersection.size() < fromCaller.size(), "the adapter filter must narrow");

            assertEquals(intersection, Set.copyOf(sortedIds(repository.findAll(cerbos.and(caller)))));
            assertEquals(intersection, Set.copyOf(sortedIds(repository.findAll(caller.and(cerbos)))));
        } finally {
            em.close();
        }
    }

    /**
     * The two dotted {@code AttributeMapping.field} spellings a caller can write, which resolve to
     * different SQL: an {@code @Embedded} path stays on the resource's own table, a
     * {@code @ManyToOne} path is a join. Both are documented in the README's "Field mapping"
     * section, and the corpus model uses neither — its to-one hops go through a {@code @OneToOne}.
     */
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

    /** Executes the raw predicate, so a translation failure surfaces as itself. */
    @Test
    void theSpecificationIsRebuiltFromTheRootItIsHanded() {
        Specification<ResourceEntity> spec = specFor("exists-on-empty");
        EntityManager em = emf.createEntityManager();
        try {
            for (int i = 0; i < 2; i++) {
                CriteriaBuilder cb = em.getCriteriaBuilder();
                CriteriaQuery<String> cq = cb.createQuery(String.class);
                Root<ResourceEntity> root = cq.from(ResourceEntity.class);
                cq.select(root.get("id"));
                Predicate predicate = spec.toPredicate(root, cq, cb);
                cq.where(predicate);
                assertFalse(em.createQuery(cq).getResultList().isEmpty());
            }
        } finally {
            em.close();
        }
    }

    /**
     * Wraps a Specification and counts {@code toPredicate} invocations, so a test can prove Spring
     * Data really re-invokes ONE instance once per query execution.
     */
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
