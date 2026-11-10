/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

import dev.cerbos.queryplan.jpa.JpaQueryPlanAdapter;
import dev.cerbos.queryplan.jpa.NullAttributeRepresentation;
import dev.cerbos.queryplan.jpa.UnmappedAttributeException;
import dev.cerbos.queryplan.jpa.UnsupportedPlanShapeException;
import dev.cerbos.queryplan.springdata.Corpus.LedgerEntry;
import dev.cerbos.queryplan.springdata.testmodel.AdversarialInnerEntity;
import dev.cerbos.queryplan.springdata.testmodel.AdversarialParentEntity;
import dev.cerbos.queryplan.springdata.testmodel.CategoryEntity;
import dev.cerbos.queryplan.springdata.testmodel.LabelEntity;
import dev.cerbos.queryplan.springdata.testmodel.ResourceEntity;
import dev.cerbos.queryplan.springdata.testmodel.SubCategoryEntity;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.springframework.data.jpa.domain.Specification;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The conformance harness. It implements {@code conformance/README.md}, "The harness contract":
 *
 * <ol>
 *   <li>Store the dataset ({@code seeds.json} + {@code derived-fields.json}) and map it
 *       ({@link Corpus#MAPPING}).</li>
 *   <li>For each PDP and each recorded golden file, translate the plan, run the query, and compare
 *       the ids with the ones {@code check()} allowed. {@code conformance-ledger.json} lists the
 *       exceptions: {@code unsupported} must throw one of the adapter's refusal types, and
 *       {@code divergent} must still give a wrong answer.</li>
 *   <li>Fail if the ledger names a case that has no golden file.</li>
 * </ol>
 *
 * <p>Needs no PDP: the plans and decisions are recorded. It runs on H2 only, through
 * {@code toSpecification}: the translator and its store legs are proved by jpa's harness, and this
 * one proves the Specification the wrapper hands Spring Data selects the same rows.
 */
class AdversarialConformanceTest {

    private static final Map<String, LedgerEntry> LEDGER = Corpus.ledger();
    /** The seeds this store cannot hold: each requires a capability the ledger lacks. */
    private static final Set<String> UNHELD = unheldSeeds();

    private static EntityManagerFactory emf;

    /** (tag, tier, outcome) to count, printed after the run. */
    private static final Map<String, Integer> TALLY = new TreeMap<>();

    /** Every fuzz case's verdict, written once the run ends (conformance/README.md, "Fuzzing"). */
    private static final List<Corpus.FuzzVerdict> FUZZ_RESULTS = Collections.synchronizedList(new ArrayList<>());

    @BeforeAll
    static void setUp() {
        emf = Persistence.createEntityManagerFactory("adversarial-pu");
        seed();
    }

    @AfterAll
    static void tearDown() {
        TALLY.forEach((key, count) -> System.out.printf("conformance %s: %d%n", key, count));
        Corpus.writeFuzzResults("spring-data", "h2", FUZZ_RESULTS);
        if (emf != null) emf.close();
    }

    // -- the contract -------------------------------------------------------------------------

    @TestFactory
    Stream<DynamicTest> goldens() {
        return Corpus.pdpTags().stream().flatMap(tag -> Corpus.goldens(tag).stream().map(golden ->
                DynamicTest.dynamicTest(tag + " " + golden.get("tier").asText() + ": "
                        + golden.get("id").asText(), () -> replay(tag, golden))));
    }

    private static void replay(String tag, JsonNode golden) {
        String id = golden.get("id").asText();
        String tier = golden.get("tier").asText();
        assertEquals(tag, golden.path("pdp").asText(), id + ": golden filed under the wrong PDP tag");
        // A golden file carries `plannerDivergence` only for the PDP tags it applies to. The key
        // must be present: a missing one would otherwise read as non-null and skip the case.
        JsonNode divergence = golden.get("plannerDivergence");
        assertTrue(divergence != null, id + ": golden has no plannerDivergence key");
        if (!divergence.isNull()) {
            tally(tag, tier, "planner divergence");
            Assumptions.abort("planner divergence: " + divergence.get("reason"));
        }
        List<String> allowed = new ArrayList<>();
        golden.get("allowed").forEach(node -> {
            if (!UNHELD.contains(node.asText())) allowed.add(node.asText());
        });
        allowed.sort(null);

        if (Corpus.fuzzing()) {
            // The adapter's two refusal types, as an `unsupported` ledger entry expects.
            Corpus.FuzzVerdict verdict = Corpus.fuzzVerdict(tag, id, allowed, () -> ids(golden),
                    e -> e instanceof UnsupportedPlanShapeException || e instanceof UnmappedAttributeException);
            FUZZ_RESULTS.add(verdict);
            assertTrue(verdict.ok(), verdict::toString);
            return;
        }
        LedgerEntry entry = LEDGER.get(id);
        String status = entry != null && entry.appliesTo(tag) ? entry.status() : "pass";
        switch (status) {
            case "pass" -> assertEquals(allowed, ids(golden), id);
            case "unsupported" -> {
                // The adapter's two refusal types. MalformedPlanException is not one: the
                // planner never emits a malformed plan.
                IllegalArgumentException refusal =
                        assertThrows(IllegalArgumentException.class, () -> ids(golden), id);
                assertTrue(refusal instanceof UnsupportedPlanShapeException
                        || refusal instanceof UnmappedAttributeException, () -> id
                        + " was refused as " + refusal.getClass().getSimpleName() + ": " + refusal.getMessage());
            }
            case "divergent" -> assertNotEquals(allowed, ids(golden),
                    id + " now matches the PDP: remove its divergent ledger entry");
            default -> fail(id + ": unknown ledger status " + status);
        }
        tally(tag, tier, status);
    }

    @Test
    void everyLedgerEntryNamesAGoldenCase() {
        Assumptions.assumeFalse(Corpus.fuzzing(), "a fuzz run is judged without the ledger");
        Set<String> recorded = new HashSet<>();
        Corpus.pdpTags().forEach(tag -> Corpus.goldens(tag).forEach(g -> recorded.add(g.get("id").asText())));
        assertEquals(Set.of(), LEDGER.keySet().stream()
                .filter(id -> !recorded.contains(id)).collect(Collectors.toSet()));
        List<String> tags = Corpus.pdpTags();
        LEDGER.forEach((id, entry) -> {
            assertTrue(Set.of("unsupported", "divergent").contains(entry.status()), id);
            // A `pdp` scope naming a tag no longer tested is stale, and an empty one is a typo.
            assertTrue(entry.pdp() == null || (!entry.pdp().isEmpty() && tags.containsAll(entry.pdp())),
                    () -> id + ": pdp scope " + entry.pdp() + " is not within " + tags);
            assertFalse(entry.reason() == null || entry.reason().isBlank(), id);
            assertTrue(!"divergent".equals(entry.status()) || entry.issue() != null, id);
        });
    }

    private static void tally(String tag, String tier, String outcome) {
        synchronized (TALLY) {
            TALLY.merge(tag + " " + tier + " " + outcome, 1, Integer::sum);
        }
    }

    // -- the query ----------------------------------------------------------------------------

    /**
     * Every column the dataset omits when NULL is declared OMITTED in {@link Corpus#MAPPING}. The
     * call-level convention covers what is left, list elements and principal-side literals, which
     * the dataset sends as values; the adapter's own default is pinned in OptionsTest.
     */
    private static final JpaQueryPlanAdapter.Options OPTIONS =
            JpaQueryPlanAdapter.Options.of(Corpus.MAPPING)
                    .withNullAttributeRepresentation(NullAttributeRepresentation.EXPLICIT);

    private static List<String> ids(JsonNode golden) {
        return executeIds(SpringDataQueryPlanAdapter.toSpecification(Corpus.plan(golden), OPTIONS));
    }

    /** The ids a {@code select distinct id} returns under {@code spec}, sorted. */
    private static List<String> executeIds(Specification<ResourceEntity> spec) {
        EntityManager em = emf.createEntityManager();
        try {
            CriteriaBuilder cb = em.getCriteriaBuilder();
            CriteriaQuery<String> cq = cb.createQuery(String.class);
            Root<ResourceEntity> root = cq.from(ResourceEntity.class);
            cq.select(root.get("id")).distinct(true);
            Predicate p = spec.toPredicate(root, cq, cb);
            if (p != null) {
                cq.where(p);
            }
            // Sorted in Java, as `allowed` is: the database's collation must not decide the order.
            return em.createQuery(cq).getResultList().stream().sorted().toList();
        } finally {
            em.close();
        }
    }

    // -- the store ----------------------------------------------------------------------------

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Tag(String id, String name) {}

    /** One {@code seeds.json} row. Scalars and list elements are boxed: the corpus carries nulls. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Seed(String id, Boolean aBool, String aString, Integer aNumber,
                        String aOptionalString, List<Double> aNumberList, List<Boolean> aBoolList,
                        List<Tag> tags, List<String> subCategoryNames, String parentSeedId,
                        List<String> requires) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record SeedsFile(List<Seed> seeds) {}

    /** One seed's {@code derived-fields.json} entry. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Derived(String createdBy, Double aDouble, String createdAt, String updatedAt,
                           String scope, List<String> labels) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record DerivedFile(Map<String, Derived> derived) {}

    private static List<Seed> allSeeds() {
        return Corpus.readJson(Corpus.conformanceDir().resolve("seeds.json"), SeedsFile.class).seeds();
    }

    private static Set<String> unheldSeeds() {
        Set<String> lacks = Corpus.lacks("h2");
        return allSeeds().stream()
                .filter(s -> s.requires() != null && s.requires().stream().anyMatch(lacks::contains))
                .map(Seed::id)
                .collect(Collectors.toSet());
    }

    private static void seed() {
        List<Seed> seeds = allSeeds().stream().filter(s -> !UNHELD.contains(s.id())).toList();
        Map<String, Derived> derived = Corpus.readJson(
                Corpus.conformanceDir().resolve("derived-fields.json"), DerivedFile.class).derived();
        Map<String, Seed> byId = seeds.stream().collect(Collectors.toMap(Seed::id, Function.identity()));
        Function<Seed, Seed> parentOf = s -> s == null || s.parentSeedId() == null
                ? null : byId.get(s.parentSeedId());

        EntityManager em = emf.createEntityManager();
        em.getTransaction().begin();
        // Distinct category graphs per seed, so no two rows share a relation by accident.
        int catSeq = 0;
        for (Seed s : seeds) {
            Derived d = derived.get(s.id());
            ResourceEntity r = new ResourceEntity(s.id());
            r.setaBool(s.aBool());
            r.setaString(s.aString());
            r.setaNumber(s.aNumber());
            r.setaDouble(d.aDouble());
            r.setaOptionalString(s.aOptionalString());
            r.setCreatedBy(d.createdBy());
            r.setScope(d.scope());
            r.setCreatedAt(d.createdAt() == null ? null : Instant.parse(d.createdAt()));
            r.setUpdatedAt(d.updatedAt() == null ? null : OffsetDateTime.parse(d.updatedAt()));
            s.tags().forEach(tag -> r.addTag(tag.id(), tag.name()));
            // One related row per element; a null element becomes a NULL column.
            s.aNumberList().forEach(r::addNumberListElement);
            s.aBoolList().forEach(r::addBoolListElement);
            // One category holding every subcategory name (conformance/README.md, "The dataset").
            List<CategoryEntity> cats = new ArrayList<>();
            if (!s.subCategoryNames().isEmpty()) {
                catSeq++;
                List<SubCategoryEntity> subs = new ArrayList<>();
                for (String subName : s.subCategoryNames()) {
                    String subId = "adv-sub-" + catSeq + "-" + (subs.size() + 1);
                    SubCategoryEntity sub = new SubCategoryEntity(subId, subName);
                    List<LabelEntity> labels = new ArrayList<>();
                    for (String labelName : d.labels()) {
                        LabelEntity label = new LabelEntity(
                                subId + "-lab-" + (labels.size() + 1), labelName);
                        em.persist(label);
                        labels.add(label);
                    }
                    sub.setLabels(labels);
                    em.persist(sub);
                    subs.add(sub);
                }
                CategoryEntity cat = new CategoryEntity("adv-cat-" + catSeq, "business");
                cat.setSubCategories(subs);
                em.persist(cat);
                cats.add(cat);
            }
            r.setCategories(cats);
            em.persist(r);

            // The to-one chain, one owned row per level: a seed with no parent gets no row, so an
            // absent parent is a missing attribute rather than a NULL value.
            Seed parentSeed = parentOf.apply(s);
            if (parentSeed != null) {
                AdversarialParentEntity parent = new AdversarialParentEntity();
                parent.setId(s.id() + "-parent");
                parent.setaBool(parentSeed.aBool());
                parent.setaString(parentSeed.aString());
                parent.setaNumber(parentSeed.aNumber());
                parent.setaOptionalString(parentSeed.aOptionalString());
                parent.setResource(r);
                em.persist(parent);

                Seed innerSeed = parentOf.apply(parentSeed);
                if (innerSeed != null) {
                    AdversarialInnerEntity inner = new AdversarialInnerEntity();
                    inner.setId(s.id() + "-parent-inner");
                    inner.setaBool(innerSeed.aBool());
                    inner.setaString(innerSeed.aString());
                    inner.setaNumber(innerSeed.aNumber());
                    inner.setaOptionalString(innerSeed.aOptionalString());
                    inner.setParent(parent);
                    em.persist(inner);
                }
            }
        }
        em.getTransaction().commit();
        em.close();
    }
}
