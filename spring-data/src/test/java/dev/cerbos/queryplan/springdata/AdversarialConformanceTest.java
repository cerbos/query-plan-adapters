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
import org.junit.jupiter.params.provider.CsvSource;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
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
                    List.of(new Tag("t9a", "same")), List.of()),
            // Field-to-field LIKE witnesses: the NEEDLE column (aOptionalString) holds LIKE
            // metacharacters. b1 discriminates escaping for all three ops ("oneXtwo" does not
            // literally contain "one_two", but an unescaped '_' wildcard would match the 'X');
            // b2/b3 are literal % and \ matches that only work when the escape is correct.
            new Seed("b1", true, "oneXtwo", 7, "one_two", List.of(), List.of()),
            new Seed("b2", false, "50%_off", 6, "%_o", List.of(), List.of()),
            new Seed("b3", true, "back\\slash", -4, "k\\s", List.of(), List.of()),
            // Probe witness: a tag whose id equals its name (lambda-inner field-to-field).
            new Seed("b4", false, "mirror", 8, "mirror",
                    List.of(new Tag("mirror", "mirror")), List.of()),
            // NULL element columns: a NULL tag name is a missing element attribute on the
            // check side, so lambda bodies touching it are CEL evaluation errors. b5 holds
            // ONLY a NULL-name tag (no true/false witness → macro errors); b6 mixes a
            // NULL-name tag with a "public" one (exists absorbs the error, all/exists_one
            // and map() do not).
            new Seed("b5", true, "nulltag", 9, "nt",
                    List.of(new Tag("t10a", null)), List.of()),
            new Seed("b6", false, "mixed", 10, "mx",
                    List.of(new Tag("t11a", null), new Tag("t11b", "public")), List.of())
    );

    /** Deterministic ISO instant per seed for the timestamp probe: split around 2025-01-01. */
    private static String isoFor(Seed s) {
        return s.aNumber() >= 2 ? "2024-06-01T00:00:00Z" : "2026-06-01T00:00:00Z";
    }

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
            "f2f-contains", "f2f-startswith", "f2f-endswith",
            "arith-add", "arith-vf", "arith-sub", "arith-mult-neg",
            "arith-div", "arith-div-frac", "arith-both",
            // clean-room probes (GROUP A)
            "p-ternary-in-exists", "p-arith-in-lambda", "p-lambda-inner-f2f",
            "p-lambda-f2f-like", "p-size-nested",
            "p-ternary-of-ternaries", "p-ternary-vs-ternary", "p-ternary-under-all",
            "p-hasintersection-map", "p-deep-nest",
            "p-in-null-single", "p-in-null-multi", "p-startswith-concat",
            // clean-room probes (GROUP B). Shapes that THROW are covered by
            // unsupportedShapesThrow below. Known divergences excluded from the oracle run:
            //   p-has            — planner folds has(unknown attr) to KIND_ALWAYS_ALLOWED, so the
            //                      adapter returns all rows while check() denies NULL rows.
            // p-double-frac is IN the run: the adapter forces IEEE double space (CAST columns,
            // double bind parameters), so 3*0.1 == 0.3 is false in SQL exactly as in CEL.
            "p-struct", "p-not-exists-empty", "p-not-ternary-null", "p-double-frac",
            // NULL element columns under collection macros (b5/b6 witnesses)
            "n-not-exists-one-null", "n-all-mixed-null", "n-not-all-null", "n-not-all-absorb",
            // constant-receiver string matches (the constant is the haystack; escaping
            // discriminators live in the a2/a4/a7 seeds)
            "cr-contains", "cr-startswith", "cr-endswith", "cr-startswith-concat",
            // arithmetic + size edges: zero column divisor (NaN → deny), fractional count
            // threshold (only ordering ops compile in CEL — int vs double eq/ne does not)
            "cr-div-zero", "cr-size-frac-ge",
            // constant NaN / ±Infinity ordering: unfolded div(0,0) → NaN; every NaN
            // ordering is false in CEL/IEEE, while ±Infinity orders normally
            "nan-ord-ternary", "nan-ord-ternary-vf", "nan-ord-le", "nan-ord-inf",
            // multi-hop relation chains via DIRECT dotted syntax (W1) and a root relation
            // subquery anchored from inside a lambda body (W2)
            "w1-exists-chain", "w1-size-chain", "w1-in-chain", "w2-outer-relation",
    })
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
    @CsvSource({
            "p-timestamp, Unexpected timestamp() expression in leaf operand of lt",
            "p-matches, Unsupported operator: matches",
            "p-index, Unexpected get-field() expression in leaf operand of eq",
    })
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
