package dev.cerbos.queryplan.springdata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.LoggerFactory;
import org.springframework.data.jpa.domain.Specification;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Adversarial differential suite: every action in the shared {@code ../conformance/} corpus is
 * planned against a REAL Cerbos PDP, translated by the adapter, and executed against seeded rows
 * — then the filtered id set is compared against an <em>oracle</em> computed by calling the check
 * API for each row with attributes mirroring that row exactly.
 *
 * <p>No hand-computed expectations: if the adapter's SQL semantics diverge from Cerbos's own
 * evaluation for any row, the mismatch surfaces mechanically. Seed rows deliberately hold hostile
 * data — empty collections, LIKE metacharacters ({@code % _ \}), unicode, empty strings, negative
 * numbers — and the policies use planner shapes the conformance policies don't (value-first
 * comparisons, empty {@code in} lists, fractional thresholds against integer columns, outer
 * attribute references two lambda levels deep).
 *
 * <p>The policy, seed data, and action list are NOT owned by this module — they live in the
 * repo-level {@code conformance/} corpus (see {@code conformance/README.md}) so prisma and
 * sqlalchemy can run the same hostile shapes against their own oracle harnesses. Only the
 * JPA-specific translation (seeding entities, executing the {@link Specification}) belongs here.
 */
class AdversarialConformanceTest {

    private static final Map<String, AttributeMapping> MAPPING = Map.ofEntries(
            Map.entry("request.resource.attr.aBool", AttributeMapping.field("aBool")),
            Map.entry("request.resource.attr.aString", AttributeMapping.field("aString")),
            Map.entry("request.resource.attr.aNumber", AttributeMapping.field("aNumber")),
            Map.entry("request.resource.attr.aOptionalString", AttributeMapping.field("aOptionalString")),
            // ISO-date string column + flattened struct member for the p-* probes
            Map.entry("request.resource.attr.createdBy", AttributeMapping.field("createdBy")),
            Map.entry("request.resource.attr.obj.inner", AttributeMapping.field("aString")),
            Map.entry("request.resource.attr.tags", AttributeMapping.relation("tags", Map.of(
                    "id", AttributeMapping.field("id"),
                    "name", AttributeMapping.field("name")
            ))),
            Map.entry("request.resource.attr.categories", AttributeMapping.relation("categories", Map.of(
                    "name", AttributeMapping.field("name"),
                    "subCategories", AttributeMapping.relation("subCategories", Map.of(
                            "name", AttributeMapping.field("name")
                    ))
            ))),
            // Multi-hop chain probe (W1): mainCategory is a SINGLE nested object on the check
            // side (every seed holds at most one category), so CEL evaluates dotted chains
            // like R.attr.mainCategory.subCategories naturally — while the ADAPTER maps the
            // same path through TWO collection hops (categories JOIN subCategories), pinning
            // that chained variables join through every intermediate hop, never off the root.
            Map.entry("request.resource.attr.mainCategory", AttributeMapping.relation("categories", Map.of(
                    "name", AttributeMapping.field("name"),
                    "subCategories", AttributeMapping.relation("subCategories", Map.of(
                            "name", AttributeMapping.field("name")
                    )),
                    // subNames: the same 2-hop chain but with a defaultMemberField, so plain
                    // `in` membership compares the flattened tail's name column.
                    "subNames", AttributeMapping.relation("subCategories", "name")
            )))
    );

    // -- shared corpus (../conformance/): policy, seed data, and action list are read from disk
    // rather than duplicated here. See conformance/README.md for the recipe these implement.

    private static Path conformanceDir() {
        return Path.of(System.getProperty("user.dir"), "..", "conformance").normalize();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Tag(String id, String name) {}

    /** One seeded row; the single source of truth for BOTH the DB entity and the oracle attributes. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Seed(String id, boolean aBool, String aString, int aNumber,
                        String aOptionalString, List<Tag> tags, List<String> subCategoryNames) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record PrincipalSpec(String id, List<String> roles, Map<String, List<String>> attr) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record SeedsFile(PrincipalSpec principal, String resourceKind, List<Seed> seeds) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record UnsupportedShape(String action, String shape, String springDataMessage) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record ActionsFile(List<String> conformance, List<UnsupportedShape> expectedUnsupported) {}

    private static SeedsFile seedsFile;
    private static ActionsFile actionsFile;
    private static List<Seed> SEEDS;

    static Stream<String> conformanceActions() {
        return actionsFile.conformance().stream();
    }

    static Stream<Arguments> unsupportedShapes() {
        return actionsFile.expectedUnsupported().stream()
                .map(u -> Arguments.of(u.action(), u.springDataMessage()));
    }

    /** Deterministic ISO instant per seed for the timestamp probe: split around 2025-01-01. */
    private static String isoFor(Seed s) {
        return s.aNumber() >= 2 ? "2024-06-01T00:00:00Z" : "2026-06-01T00:00:00Z";
    }

    private static GenericContainer<?> cerbos;
    private static CerbosBlockingClient client;
    private static EntityManagerFactory emf;

    @BeforeAll
    static void setUp() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Path conformance = conformanceDir();
        seedsFile = mapper.readValue(conformance.resolve("seeds.json").toFile(), SeedsFile.class);
        actionsFile = mapper.readValue(conformance.resolve("actions.json").toFile(), ActionsFile.class);
        SEEDS = seedsFile.seeds();
        String cerbosVersion = Files.readString(conformance.resolve("CERBOS_VERSION")).strip();

        cerbos = new GenericContainer<>("ghcr.io/cerbos/cerbos:" + cerbosVersion)
                .withExposedPorts(3593)
                .withCommand("server", "--set=storage.disk.directory=/policies")
                .withEnv("CERBOS_NO_TELEMETRY", "1")
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("cerbos-adversarial-pdp")))
                .waitingFor(Wait.forLogMessage(".*Starting gRPC server.*", 1));
        try {
            byte[] policy = Files.readAllBytes(conformance.resolve("policies").resolve("adversarial.yaml"));
            cerbos.withCopyToContainer(Transferable.of(policy), "/policies/adversarial.yaml");
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
            r.setCreatedBy(isoFor(s));
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
        PrincipalSpec spec = seedsFile.principal();
        Principal p = Principal.newInstance(spec.id(), spec.roles().toArray(new String[0]));
        for (Map.Entry<String, List<String>> attr : spec.attr().entrySet()) {
            p = p.withAttribute(attr.getKey(), AttributeValue.listValue(attr.getValue().stream()
                    .map(AttributeValue::stringValue)
                    .toArray(AttributeValue[]::new)));
        }
        return p;
    }

    /** Cerbos attributes mirroring exactly what the seeded DB row holds. */
    private static Resource asCheckResource(Seed s) {
        Resource r = Resource.newInstance(seedsFile.resourceKind(), s.id())
                .withAttribute("aBool", AttributeValue.boolValue(s.aBool()))
                .withAttribute("aString", AttributeValue.stringValue(s.aString()))
                .withAttribute("aNumber", AttributeValue.doubleValue(s.aNumber()))
                .withAttribute("createdBy", AttributeValue.stringValue(isoFor(s)))
                .withAttribute("obj", AttributeValue.mapValue(Map.of(
                        "inner", AttributeValue.stringValue(s.aString()))))
                .withAttribute("tags", AttributeValue.listValue(s.tags().stream()
                        .map(AdversarialConformanceTest::asTagAttribute)
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
        // mainCategory mirrors the row's single category as ONE nested object (the seeder
        // creates at most one category per seed), so direct dotted-chain CEL expressions
        // evaluate cleanly; rows without a category get NO attribute — a CEL missing-attr
        // error (deny), matching the adapter's empty join chain excluding the row.
        if (!s.subCategoryNames().isEmpty()) {
            r = r.withAttribute("mainCategory", AttributeValue.mapValue(Map.of(
                    "name", AttributeValue.stringValue("business"),
                    "subCategories", AttributeValue.listValue(s.subCategoryNames().stream()
                            .map(n -> AttributeValue.mapValue(Map.of(
                                    "name", AttributeValue.stringValue(n))))
                            .toList()),
                    "subNames", AttributeValue.listValue(s.subCategoryNames().stream()
                            .map(AttributeValue::stringValue)
                            .toList()))));
        }
        return r;
    }

    /** A NULL tag name in the DB is a missing element attribute on the check side. */
    private static AttributeValue asTagAttribute(Tag t) {
        Map<String, AttributeValue> attrs = new LinkedHashMap<>();
        attrs.put("id", AttributeValue.stringValue(t.id()));
        if (t.name() != null) {
            attrs.put("name", AttributeValue.stringValue(t.name()));
        }
        return AttributeValue.mapValue(attrs);
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
        PlanResourcesResult plan = client.plan(
                principal(), Resource.newInstance(seedsFile.resourceKind()), action);
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
    @MethodSource("conformanceActions")
    void adapterMatchesCheckOracle(String action) {
        List<String> oracle = oracleAllowedIds(action);
        List<String> filtered = adapterFilteredIds(action);
        assertEquals(oracle, filtered,
                "adapter result diverges from check-API oracle for action '" + action + "'");
    }

    /**
     * Probe shapes the adapter does not support: the translation must fail loudly (never a
     * silently-wrong filter). Messages pinned so a regression to silent acceptance is caught.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("unsupportedShapes")
    void unsupportedShapesThrow(String action, String expectedMessage) {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, () -> adapterFilteredIds(action));
        assertEquals(expectedMessage, ex.getMessage());
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
