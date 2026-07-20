package dev.cerbos.queryplan.springdata;

import dev.cerbos.queryplan.springdata.testmodel.CategoryEntity;
import dev.cerbos.queryplan.springdata.testmodel.ResourceEntity;
import dev.cerbos.queryplan.springdata.testmodel.SubCategoryEntity;
import dev.cerbos.sdk.CerbosBlockingClient;
import dev.cerbos.sdk.CerbosClientBuilder;
import dev.cerbos.sdk.PlanResourcesResult;
import dev.cerbos.sdk.builders.AttributeValue;
import dev.cerbos.sdk.builders.Principal;
import dev.cerbos.sdk.builders.Resource;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.EntityTransaction;
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
import org.slf4j.LoggerFactory;
import org.springframework.data.jpa.domain.Specification;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Adversarial differential suite: every action in {@code adversarial-policy.yaml} is planned
 * against a REAL Cerbos PDP, translated by the adapter, and executed against seeded rows — then
 * the filtered id set is compared against an <em>oracle</em> computed by calling the PDP's
 * check API for each row with attributes mirroring that row exactly.
 *
 * <p>No hand-computed expectations: if the adapter's SQL semantics diverge from Cerbos's own
 * evaluation for any row, the mismatch surfaces mechanically. Seed rows deliberately hold
 * hostile data — empty collections, LIKE metacharacters ({@code % _ \}), unicode, empty strings,
 * negative numbers — and the policies use planner shapes the conformance policies don't
 * (value-first comparisons, empty {@code in} lists, fractional thresholds against integer
 * columns, outer attribute references two lambda levels deep).
 */
class AdversarialConformanceTest {

    private static final Map<String, AttributeMapping> MAPPING = Map.ofEntries(
            Map.entry("request.resource.attr.aBool", AttributeMapping.field("aBool")),
            Map.entry("request.resource.attr.aString", AttributeMapping.field("aString")),
            Map.entry("request.resource.attr.aNumber", AttributeMapping.field("aNumber")),
            Map.entry("request.resource.attr.aOptionalString", AttributeMapping.field("aOptionalString")),
            Map.entry("request.resource.attr.tags", AttributeMapping.relation("tags", Map.of(
                    "id", AttributeMapping.field("id"),
                    "name", AttributeMapping.field("name")
            ))),
            Map.entry("request.resource.attr.categories", AttributeMapping.relation("categories", Map.of(
                    "name", AttributeMapping.field("name"),
                    "subCategories", AttributeMapping.relation("subCategories", Map.of(
                            "name", AttributeMapping.field("name")
                    ))
            )))
    );

    private record Tag(String id, String name) {}

    /** One seeded row; the single source of truth for BOTH the DB entity and the oracle attributes. */
    private record Seed(String id, boolean aBool, String aString, int aNumber,
                        String aOptionalString, List<Tag> tags, List<String> subCategoryNames) {}

    private static final List<Seed> SEEDS = List.of(
            new Seed("a1", true, "one", 5, "set",
                    List.of(new Tag("t1a", "public")), List.of("finance")),
            new Seed("a2", false, "100%_done", -2, null,
                    List.of(), List.of()),
            new Seed("a3", true, "100xdone", 2, "x",
                    List.of(new Tag("t3a", "public"), new Tag("t3b", "public")), List.of()),
            new Seed("a4", true, "xa_by", 1, null,
                    List.of(new Tag("t4a", "private")), List.of()),
            new Seed("a5", false, "xaXby", -5, "y",
                    List.of(new Tag("t5a", "public")), List.of()),
            new Seed("a6", true, "héllo🚀", 3, "",
                    List.of(new Tag("t6a", "public"), new Tag("t6b", "private")), List.of("finance")),
            new Seed("a7", true, "tail\\", 0, "z",
                    List.of(new Tag("t7a", "other")), List.of()),
            new Seed("a8", true, "", 2, null,
                    List.of(new Tag("t8a", "public")), List.of("tech")),
            // Field-to-field witness: aString == aOptionalString == a tag name, so the
            // field-to-field and lambda-field-to-field oracles are non-degenerate.
            new Seed("a9", true, "same", 4, "same",
                    List.of(new Tag("t9a", "same")), List.of())
    );

    private static GenericContainer<?> cerbos;
    private static CerbosBlockingClient client;
    private static EntityManagerFactory emf;

    @BeforeAll
    static void setUp() throws Exception {
        cerbos = new GenericContainer<>("ghcr.io/cerbos/cerbos:latest")
                .withExposedPorts(3593)
                .withCommand("server", "--set=storage.disk.directory=/policies")
                .withEnv("CERBOS_NO_TELEMETRY", "1")
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("cerbos-adversarial-pdp")))
                .waitingFor(Wait.forLogMessage(".*Starting gRPC server.*", 1));
        try (InputStream policy = AdversarialConformanceTest.class
                .getResourceAsStream("/adversarial-policy.yaml")) {
            cerbos.withCopyToContainer(
                    Transferable.of(policy.readAllBytes()), "/policies/adversarial.yaml");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        cerbos.start();
        client = new CerbosClientBuilder(cerbos.getHost() + ":" + cerbos.getMappedPort(3593))
                .withPlaintext().buildBlockingClient();

        emf = Persistence.createEntityManagerFactory("adversarial-pu");
        seed();
    }

    @AfterAll
    static void tearDown() {
        if (emf != null) emf.close();
        if (cerbos != null) cerbos.stop();
    }

    private static void seed() {
        EntityManager em = emf.createEntityManager();
        EntityTransaction tx = em.getTransaction();
        tx.begin();

        // Distinct sub-category/category graphs per seed so no rows share relations by accident.
        int catSeq = 0;
        for (Seed s : SEEDS) {
            ResourceEntity r = new ResourceEntity(s.id());
            r.setaBool(s.aBool());
            r.setaString(s.aString());
            r.setaNumber(s.aNumber());
            r.setaOptionalString(s.aOptionalString());
            for (Tag tag : s.tags()) {
                r.addTag(tag.id(), tag.name());
            }
            List<CategoryEntity> cats = new ArrayList<>();
            for (String subName : s.subCategoryNames()) {
                catSeq++;
                SubCategoryEntity sub = new SubCategoryEntity("adv-sub-" + catSeq, subName);
                em.persist(sub);
                CategoryEntity cat = new CategoryEntity("adv-cat-" + catSeq, "business");
                cat.setSubCategories(new ArrayList<>(List.of(sub)));
                em.persist(cat);
                cats.add(cat);
            }
            r.setCategories(cats);
            em.persist(r);
        }
        tx.commit();
        em.close();
    }

    // -- oracle: ask the PDP itself, row by row --

    private static Principal principal() {
        return Principal.newInstance("u1", "USER")
                .withAttribute("allowedTags", AttributeValue.listValue(
                        AttributeValue.stringValue("public"),
                        AttributeValue.stringValue("special")));
    }

    /** Cerbos attributes mirroring exactly what the seeded DB row holds. */
    private static Resource asCheckResource(Seed s) {
        Resource r = Resource.newInstance("adversarial", s.id())
                .withAttribute("aBool", AttributeValue.boolValue(s.aBool()))
                .withAttribute("aString", AttributeValue.stringValue(s.aString()))
                .withAttribute("aNumber", AttributeValue.doubleValue(s.aNumber()))
                .withAttribute("tags", AttributeValue.listValue(s.tags().stream()
                        .map(t -> AttributeValue.mapValue(Map.of(
                                "id", AttributeValue.stringValue(t.id()),
                                "name", AttributeValue.stringValue(t.name()))))
                        .toList()))
                .withAttribute("categories", AttributeValue.listValue(s.subCategoryNames().stream()
                        .map(subName -> AttributeValue.mapValue(Map.of(
                                "name", AttributeValue.stringValue("business"),
                                "subCategories", AttributeValue.listValue(
                                        AttributeValue.mapValue(Map.of(
                                                "name", AttributeValue.stringValue(subName)))))))
                        .toList()));
        // A DB NULL is a missing attribute on the check side — conditions touching it must
        // deny (CEL error), matching SQL three-valued logic excluding the row.
        if (s.aOptionalString() != null) {
            r = r.withAttribute("aOptionalString", AttributeValue.stringValue(s.aOptionalString()));
        }
        return r;
    }

    private static List<String> oracleAllowedIds(String action) {
        return SEEDS.stream()
                .filter(s -> client.check(principal(), asCheckResource(s), action).isAllowed(action))
                .map(Seed::id)
                .sorted()
                .toList();
    }

    // -- adapter execution through the public Specification path --

    private static List<String> adapterFilteredIds(String action) {
        PlanResourcesResult plan = client.plan(principal(), Resource.newInstance("adversarial"), action);
        Specification<ResourceEntity> spec =
                SpringDataQueryPlanAdapter.<ResourceEntity>toSpecification(plan, MAPPING).toSpecification();

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
            cq.orderBy(cb.asc(root.get("id")));
            return em.createQuery(cq).getResultList();
        } finally {
            em.close();
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {
            "vf-le", "vf-ge", "vf-ne",
            "in-single", "in-empty",
            "like-percent", "like-underscore", "like-backslash",
            "unicode-eq", "empty-string-eq",
            "neg-number", "double-threshold",
            "all-on-empty", "exists-on-empty", "exists-one-multi", "not-exists",
            "outer-attr-depth2", "lambda-in-principal",
            "nary-and", "double-negation", "triple-negation", "not-empty",
            "optional-ne",
            "lambda-field-to-field", "field-to-field",
            "size-threshold", "size-filter-count", "string-size",
            "ternary-cmp", "ternary-expr-cond", "ternary-nested", "ternary-negated",
            "ternary-bare", "ternary-value-first", "ternary-null-cond",
    })
    void adapterMatchesCheckOracle(String action) {
        List<String> oracle = oracleAllowedIds(action);
        List<String> filtered = adapterFilteredIds(action);
        assertEquals(oracle, filtered,
                "adapter result diverges from check-API oracle for action '" + action + "'");
    }

    @Test
    void oracleIsNotDegenerate() {
        // Guard the guard: at least one action must produce a non-empty, non-total oracle set,
        // otherwise the differential comparison could pass vacuously (e.g. PDP denying all).
        Map<String, List<String>> samples = new LinkedHashMap<>();
        samples.put("vf-le", oracleAllowedIds("vf-le"));
        samples.put("like-percent", oracleAllowedIds("like-percent"));
        samples.put("all-on-empty", oracleAllowedIds("all-on-empty"));
        samples.forEach((action, ids) -> assertTrue(
                !ids.isEmpty() && ids.size() < SEEDS.size(),
                "oracle for '" + action + "' is degenerate: " + ids));
    }
}
