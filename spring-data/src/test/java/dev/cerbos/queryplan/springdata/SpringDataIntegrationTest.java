package dev.cerbos.queryplan.springdata;

import dev.cerbos.queryplan.springdata.testmodel.CategoryEntity;
import dev.cerbos.queryplan.springdata.testmodel.LabelEntity;
import dev.cerbos.queryplan.springdata.testmodel.NestedEmbeddable;
import dev.cerbos.queryplan.springdata.testmodel.NextLevelEmbeddable;
import dev.cerbos.queryplan.springdata.testmodel.OwnerEntity;
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
import jakarta.persistence.criteria.Order;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.jpa.domain.Specification;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end test against a real Cerbos PDP.
 *
 * <p>Two modes:
 * <ul>
 *   <li><b>Self-managed (default)</b>: a {@code ghcr.io/cerbos/cerbos:dev} container is started
 *       by Testcontainers, with the shared {@code /policies/resource.yaml} mounted in.</li>
 *   <li><b>External (Prisma-style sidecar)</b>: if {@code CERBOS_HOST} and {@code CERBOS_PORT}
 *       are set in the environment, the suite skips Testcontainers and connects to an
 *       externally-managed PDP. See {@code docker-compose.yml} and {@code scripts/run-e2e.sh}.</li>
 * </ul>
 */
class SpringDataIntegrationTest {

    private static final String EXTERNAL_HOST = System.getenv("CERBOS_HOST");
    private static final String EXTERNAL_PORT = System.getenv("CERBOS_PORT");
    private static final boolean USE_EXTERNAL_PDP = EXTERNAL_HOST != null && !EXTERNAL_HOST.isBlank();

    private static GenericContainer<?> cerbos;
    private static CerbosBlockingClient cerbosClient;
    private static EntityManagerFactory emf;

    private static final Map<String, AttributeMapping> FIELD_MAP = Map.ofEntries(
            Map.entry("request.resource.attr.aBool", AttributeMapping.field("aBool")),
            Map.entry("request.resource.attr.aString", AttributeMapping.field("aString")),
            Map.entry("request.resource.attr.aNumber", AttributeMapping.field("aNumber")),
            Map.entry("request.resource.attr.id", AttributeMapping.field("oid")),
            Map.entry("request.resource.attr.aOptionalString", AttributeMapping.field("aOptionalString")),
            Map.entry("request.resource.attr.createdBy", AttributeMapping.field("createdBy")),
            Map.entry("request.resource.attr.ownedBy", AttributeMapping.relation("ownedBy")),
            Map.entry("request.resource.attr.tags", AttributeMapping.relation("tagNames")),
            Map.entry("request.resource.attr.nested.aBool", AttributeMapping.field("nested.aBool")),
            Map.entry("request.resource.attr.nested.aString", AttributeMapping.field("nested.aString")),
            Map.entry("request.resource.attr.nested.aNumber", AttributeMapping.field("nested.aNumber")),
            Map.entry("request.resource.attr.nested.aOptionalString", AttributeMapping.field("nested.aOptionalString")),
            Map.entry("request.resource.attr.nested.nextlevel.aBool", AttributeMapping.field("nested.nextlevel.aBool")),
            Map.entry("request.resource.attr.nested.nextlevel.aString", AttributeMapping.field("nested.nextlevel.aString"))
    );

    // FIELD_MAP, but with tags as the @OneToMany TagEntity collection (for exists/all/filter etc.)
    // instead of the flat tagNames element collection.
    private static final Map<String, AttributeMapping> NESTED_FIELD_MAP = merge(FIELD_MAP,
            Map.of("request.resource.attr.tags", AttributeMapping.relation("tags", Map.of(
                    "id", AttributeMapping.field("id"),
                    "name", AttributeMapping.field("name")
            ))));

    private static final Map<String, AttributeMapping> CATEGORIES_MAP = Map.ofEntries(
            Map.entry("request.resource.attr.categories", AttributeMapping.relation("categories", Map.of(
                    "name", AttributeMapping.field("name"),
                    "subCategories", AttributeMapping.relation("subCategories", Map.of(
                            "name", AttributeMapping.field("name"),
                            "labels", AttributeMapping.relation("labels", Map.of(
                                    "name", AttributeMapping.field("name")
                            ))
                    ))
            )))
    );

    // Kitchen-sink map for tests that mix nested.*, TagEntity tags, and categories (combined-or,
    // kitchensink, principal-attribute actions).
    private static final Map<String, AttributeMapping> COMBINED_MAP = merge(NESTED_FIELD_MAP, CATEGORIES_MAP);

    /** Right-biased union of attribute maps. */
    private static Map<String, AttributeMapping> merge(Map<String, AttributeMapping> base,
                                                       Map<String, AttributeMapping> overlay) {
        java.util.HashMap<String, AttributeMapping> m = new java.util.HashMap<>(base);
        m.putAll(overlay);
        return Map.copyOf(m);
    }

    /** Records every SQL statement Hibernate executes, so a test can assert on query shape. */
    public static final class SqlCapture implements org.hibernate.resource.jdbc.spi.StatementInspector {
        static final java.util.List<String> STATEMENTS =
                java.util.Collections.synchronizedList(new java.util.ArrayList<>());

        @Override
        public String inspect(String sql) {
            STATEMENTS.add(sql);
            return sql;
        }
    }

    // Hierarchy policies reference R.id (the resource id) and R.attr.scope (a delimited path column).
    private static final Map<String, AttributeMapping> HIERARCHY_MAP = Map.ofEntries(
            Map.entry("request.resource.id", AttributeMapping.field("id")),
            Map.entry("request.resource.attr.scope", AttributeMapping.field("scope"))
    );

    private static GenericContainer<?> createCerbosContainer() {
        GenericContainer<?> container = new GenericContainer<>("ghcr.io/cerbos/cerbos:latest")
                .withExposedPorts(3593)
                .withCommand("server",
                        "--set=storage.disk.directory=/policies",
                        "--set=schema.enforcement=reject",
                        "--set=audit.enabled=true",
                        "--set=audit.accessLogsEnabled=true",
                        "--set=audit.decisionLogsEnabled=true",
                        "--set=audit.backend=file",
                        "--set=audit.file.path=stdout")
                .withEnv("CERBOS_NO_TELEMETRY", "1")
                .waitingFor(Wait.forLogMessage(".*Starting gRPC server.*", 1));
        try {
            byte[] policyBytes = Files.readAllBytes(
                    Path.of(System.getProperty("user.dir"), "..", "policies", "resource.yaml"));
            container.withCopyToContainer(Transferable.of(policyBytes), "/policies/resource.yaml");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return container;
    }

    @BeforeAll
    static void setUp() throws Exception {
        String host;
        int port;
        if (USE_EXTERNAL_PDP) {
            host = EXTERNAL_HOST;
            port = EXTERNAL_PORT != null && !EXTERNAL_PORT.isBlank()
                    ? Integer.parseInt(EXTERNAL_PORT)
                    : 3593;
            System.out.printf("==> Using externally-managed Cerbos PDP at %s:%d%n", host, port);
        } else {
            cerbos = createCerbosContainer();
            // Stream the cerbos container's stdout (including audit/decision-log JSON lines) into
            // the test JVM's logger so PlanResources calls are visibly logged alongside test runs.
            cerbos.withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("cerbos-pdp")));
            cerbos.start();
            host = cerbos.getHost();
            port = cerbos.getMappedPort(3593);
            System.out.printf(
                    "==> Started Testcontainers-managed Cerbos PDP (ghcr.io/cerbos/cerbos:latest) at %s:%d%n",
                    host, port);
        }

        cerbosClient = new CerbosClientBuilder(host + ":" + port)
                .withPlaintext().buildBlockingClient();

        // Install a StatementInspector so tests can assert on the generated SQL (e.g. that deeply
        // nested correlated EXISTS subqueries don't degrade into a cartesian/cross join).
        emf = Persistence.createEntityManagerFactory("test-pu",
                Map.of("hibernate.session_factory.statement_inspector", SqlCapture.class.getName()));
        seedData();
    }

    @AfterAll
    static void tearDown() {
        if (emf != null) emf.close();
        if (cerbos != null) {
            cerbos.stop();
        }
    }

    private static void seedData() {
        EntityManager em = emf.createEntityManager();
        EntityTransaction tx = em.getTransaction();
        tx.begin();

        // Labels
        LabelEntity label1 = new LabelEntity("label1", "important");
        LabelEntity label2 = new LabelEntity("label2", "archived");
        LabelEntity label3 = new LabelEntity("label3", "flagged");
        em.persist(label1);
        em.persist(label2);
        em.persist(label3);

        // SubCategories
        SubCategoryEntity sub1 = new SubCategoryEntity("sub1", "finance");
        sub1.setLabels(new java.util.ArrayList<>(List.of(label1, label2)));
        SubCategoryEntity sub2 = new SubCategoryEntity("sub2", "tech");
        sub2.setLabels(new java.util.ArrayList<>(List.of(label2, label3)));
        em.persist(sub1);
        em.persist(sub2);

        // Categories
        CategoryEntity cat1 = new CategoryEntity("cat1", "business");
        cat1.setSubCategories(new java.util.ArrayList<>(List.of(sub1)));
        CategoryEntity cat2 = new CategoryEntity("cat2", "development");
        cat2.setSubCategories(new java.util.ArrayList<>(List.of(sub2)));
        em.persist(cat1);
        em.persist(cat2);

        // Owners
        OwnerEntity user1 = new OwnerEntity("user1", "Alice", "engineering");
        OwnerEntity user2 = new OwnerEntity("user2", "Bob", "marketing");
        OwnerEntity user3 = new OwnerEntity("user3", "Carol", "sales");
        em.persist(user1);
        em.persist(user2);
        em.persist(user3);

        ResourceEntity r1 = new ResourceEntity("1");
        r1.setOid("507f1f77bcf86cd799439011");
        r1.setaBool(true);
        r1.setaString("string");
        r1.setaNumber(1);
        r1.setaOptionalString("hello");
        r1.setCreatedBy("user1");
        r1.setScope("a.b.c");
        r1.setOwnedBy(new java.util.ArrayList<>(List.of("user1", "user2")));
        r1.setTagNames(new java.util.ArrayList<>(List.of("public", "featured")));
        r1.addTag("tag1", "public");
        r1.addTag("tag2", "private");
        r1.setCategories(new java.util.ArrayList<>(List.of(cat1)));
        r1.setCreator(user1);
        NestedEmbeddable n1 = new NestedEmbeddable();
        n1.setaBool(true);
        n1.setaString("substring");
        n1.setaNumber(2);
        NextLevelEmbeddable nl1 = new NextLevelEmbeddable();
        nl1.setaBool(true);
        nl1.setaString("strDeep");
        n1.setNextlevel(nl1);
        r1.setNested(n1);
        em.persist(r1);

        ResourceEntity r2 = new ResourceEntity("2");
        r2.setOid("507f1f77bcf86cd799439012");
        r2.setaBool(false);
        r2.setaString("amIAString?");
        r2.setaNumber(2);
        r2.setCreatedBy("user2");
        r2.setScope("a.x");
        r2.setOwnedBy(new java.util.ArrayList<>(List.of("user2")));
        r2.setTagNames(new java.util.ArrayList<>(List.of("private")));
        r2.addTag("tag3", "private");
        r2.setCategories(new java.util.ArrayList<>(List.of(cat2)));
        r2.setCreator(user2);
        NestedEmbeddable n2 = new NestedEmbeddable();
        n2.setaBool(false);
        n2.setaString("noMatch");
        n2.setaNumber(1);
        NextLevelEmbeddable nl2 = new NextLevelEmbeddable();
        nl2.setaBool(false);
        nl2.setaString("deepValue");
        n2.setNextlevel(nl2);
        r2.setNested(n2);
        em.persist(r2);

        ResourceEntity r3 = new ResourceEntity("3");
        r3.setOid("507f1f77bcf86cd799439013");
        r3.setaBool(true);
        r3.setaString("anotherString");
        r3.setaNumber(3);
        r3.setaOptionalString("world");
        r3.setCreatedBy("user3");
        r3.setScope("a.b");
        r3.setOwnedBy(new java.util.ArrayList<>(List.of("user1")));
        r3.setTagNames(new java.util.ArrayList<>(List.of("public")));
        r3.addTag("tag1", "public");
        r3.setCategories(new java.util.ArrayList<>(List.of(cat1, cat2)));
        r3.setCreator(user3);
        NestedEmbeddable n3 = new NestedEmbeddable();
        n3.setaBool(true);
        n3.setaString("testString");
        n3.setaNumber(3);
        NextLevelEmbeddable nl3 = new NextLevelEmbeddable();
        nl3.setaBool(false);
        nl3.setaString("strValue");
        n3.setNextlevel(nl3);
        r3.setNested(n3);
        em.persist(r3);

        tx.commit();
        em.close();
    }

    private static PlanResourcesResult plan(String action) {
        return plan(Principal.newInstance("user1", "USER"), action);
    }

    private static PlanResourcesResult plan(Principal principal, String action) {
        return cerbosClient.plan(
                principal,
                Resource.newInstance("resource"),
                action);
    }

    private static List<String> runWithMapping(String action, Map<String, AttributeMapping> mapping) {
        return runWithPrincipalAndMapping(Principal.newInstance("user1", "USER"), action, mapping);
    }

    private static List<String> runWithPrincipalAndMapping(
            Principal principal, String action, Map<String, AttributeMapping> mapping) {
        PlanResourcesResult planResult = plan(principal, action);
        Result<ResourceEntity> result =
                SpringDataQueryPlanAdapter.toSpecification(planResult, mapping);

        if (result instanceof Result.AlwaysDenied<ResourceEntity>) {
            return List.of();
        }

        EntityManager em = emf.createEntityManager();
        try {
            CriteriaBuilder cb = em.getCriteriaBuilder();
            CriteriaQuery<String> cq = cb.createQuery(String.class);
            Root<ResourceEntity> root = cq.from(ResourceEntity.class);
            cq.select(root.get("id")).distinct(true);

            if (result instanceof Result.Conditional<ResourceEntity> conditional) {
                Specification<ResourceEntity> spec = conditional.specification();
                Predicate p = spec.toPredicate(root, cq, cb);
                if (p != null) {
                    cq.where(p);
                }
            }
            cq.orderBy(cb.asc(root.get("id")));
            return em.createQuery(cq).getResultList();
        } finally {
            em.close();
        }
    }

    private static List<String> run(String action) {
        return runWithMapping(action, FIELD_MAP);
    }

    private static List<String> runNested(String action) {
        return runWithMapping(action, NESTED_FIELD_MAP);
    }

    /**
     * Assert that translating {@code action} throws an {@link IllegalArgumentException} whose
     * message contains every one of {@code messageFragments}. Pins the error contract so a future
     * refactor can't silently regress to a less-helpful message (or to a different exception type).
     */
    private static void assertActionThrows(String action,
                                           Map<String, AttributeMapping> mapping,
                                           String... messageFragments) {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> runWithMapping(action, mapping));
        for (String fragment : messageFragments) {
            assertTrue(ex.getMessage().contains(fragment),
                    "expected message to contain '" + fragment + "' but was: " + ex.getMessage());
        }
    }

    // -- always allow/deny --

    @Test
    void alwaysAllowed() {
        assertEquals(List.of("1", "2", "3"), run("always-allow"));
    }

    @Test
    void alwaysDenied() {
        assertEquals(List.of(), run("always-deny"));
    }

    // -- equality --

    @Test
    void equal() {
        assertEquals(List.of("1", "3"), run("equal"));
    }

    @Test
    void equalOid() {
        assertEquals(List.of("1"), run("equal-oid"));
    }

    @Test
    void notEquals() {
        assertEquals(List.of("2", "3"), run("ne"));
    }

    @Test
    void explicitDeny() {
        assertEquals(List.of("2"), run("explicit-deny"));
    }

    // -- bare bool --

    @Test
    void bareBool() {
        assertEquals(List.of("1", "3"), run("bare-bool"));
    }

    @Test
    void bareBoolNegated() {
        assertEquals(List.of("2"), run("bare-bool-negated"));
    }

    @Test
    void bareBoolNested() {
        assertEquals(List.of("1", "3"), run("bare-bool-nested"));
    }

    @Test
    void bareBoolNestedNegated() {
        assertEquals(List.of("2"), run("bare-bool-nested-negated"));
    }

    // -- logical --

    @Test
    void and() {
        assertEquals(List.of("3"), run("and"));
    }

    @Test
    void or() {
        assertEquals(List.of("1", "2", "3"), run("or"));
    }

    @Test
    void nand() {
        assertEquals(List.of("1", "2"), run("nand"));
    }

    @Test
    void nor() {
        assertEquals(List.of(), run("nor"));
    }

    // -- set membership --

    @Test
    void in() {
        assertEquals(List.of("1", "3"), run("in"));
    }

    // -- range --

    @Test
    void greaterThan() {
        assertEquals(List.of("2", "3"), run("gt"));
    }

    @Test
    void lessThan() {
        assertEquals(List.of("1"), run("lt"));
    }

    @Test
    void greaterThanOrEqual() {
        assertEquals(List.of("1", "2", "3"), run("gte"));
    }

    @Test
    void lessThanOrEqual() {
        assertEquals(List.of("1", "2"), run("lte"));
    }

    // -- string operators --

    @Test
    void contains() {
        assertEquals(List.of("1"), run("contains"));
    }

    @Test
    void startsWith() {
        assertEquals(List.of("1"), run("starts-with"));
    }

    @Test
    void endsWith() {
        assertEquals(List.of("1", "3"), run("ends-with"));
    }

    // -- nested equality --

    @Test
    void equalNested() {
        assertEquals(List.of("1", "3"), run("equal-nested"));
    }

    @Test
    void equalDeeplyNested() {
        assertEquals(List.of("1"), run("equal-deeply-nested"));
    }

    // -- nested range --

    @Test
    void nestedEqNumber() {
        assertEquals(List.of("2"), run("relation-eq-number"));
    }

    @Test
    void nestedLtNumber() {
        assertEquals(List.of("2"), run("relation-lt-number"));
    }

    @Test
    void nestedLteNumber() {
        assertEquals(List.of("1", "2"), run("relation-lte-number"));
    }

    @Test
    void nestedGteNumber() {
        assertEquals(List.of("1", "2", "3"), run("relation-gte-number"));
    }

    @Test
    void nestedGtNumber() {
        assertEquals(List.of("1", "3"), run("relation-gt-number"));
    }

    @Test
    void nestedMultipleAll() {
        assertEquals(List.of("1"), run("relation-multiple-all"));
    }

    // -- nested string operators --

    @Test
    void nestedContains() {
        assertEquals(List.of("1"), run("nested-contains"));
    }

    @Test
    void deeplyNestedStartsWith() {
        assertEquals(List.of("1", "3"), run("deeply-nested-starts-with"));
    }

    // -- null checks --

    @Test
    void isSet() {
        assertEquals(List.of("1", "3"), run("is-set"));
    }

    // -- array membership (flat element collection) --

    @Test
    void hasTag() {
        assertEquals(List.of("1", "3"), run("has-tag"));
    }

    @Test
    void hasNoTag() {
        assertEquals(List.of("1", "3"), run("has-no-tag"));
    }

    // -- principal references --

    @Test
    void relationIs() {
        assertEquals(List.of("1"), run("relation-is"));
    }

    @Test
    void relationIsNot() {
        assertEquals(List.of("2", "3"), run("relation-is-not"));
    }

    @Test
    void relationSome() {
        assertEquals(List.of("1", "3"), run("relation-some"));
    }

    @Test
    void relationNone() {
        assertEquals(List.of("2"), run("relation-none"));
    }

    @Test
    void relationMultipleOr() {
        assertEquals(List.of("1", "3"), run("relation-multiple-or"));
    }

    @Test
    void relationMultipleNone() {
        assertEquals(List.of("2"), run("relation-multiple-none"));
    }

    // -- intersection --

    @Test
    void hasIntersectionDirect() {
        assertEquals(List.of("1", "3"), run("has-intersection-direct"));
    }

    // -- size --

    @Test
    void relationHasMembers() {
        assertEquals(List.of("1", "2", "3"), run("relation-has-members"));
    }

    @Test
    void relationHasNoMembers() {
        assertEquals(List.of(), run("relation-has-no-members"));
    }

    // -- combined --

    @Test
    void combinedAnd() {
        assertEquals(List.of("3"), run("combined-and"));
    }

    // -- nested object collection ops (use NESTED_FIELD_MAP for tags) --

    @Nested
    class NestedCollectionOperators {

        @Test
        void existsSingle() {
            assertEquals(List.of("1", "3"), runNested("exists-single"));
        }

        @Test
        void existsMultiple() {
            assertEquals(List.of("1", "3"), runNested("exists-multiple"));
        }

        @Test
        void existsByName() {
            assertEquals(List.of("1", "3"), runNested("exists"));
        }

        @Test
        void existsOne() {
            // r1: tags=[public, private] → exactly 1 public ✓
            // r2: tags=[private]        → 0 public ✗
            // r3: tags=[public]         → exactly 1 public ✓
            assertEquals(List.of("1", "3"), runNested("exists-one"));
        }

        @Test
        void filter() {
            assertEquals(List.of("1", "3"), runNested("filter"));
        }

        @Test
        void allMatching() {
            assertEquals(List.of("3"), runNested("all"));
        }

        @Test
        void hasIntersectionWithMap() {
            assertEquals(List.of("1", "2", "3"), runNested("map-collection"));
        }
    }

    // -- Deeply nested many-to-many relations: categories → subCategories → labels --
    // Resources: r1 → cat1(business→sub1=finance→[important,archived])
    //            r2 → cat2(development→sub2=tech→[archived,flagged])
    //            r3 → cat1, cat2 → all of the above
    @Nested
    class DeepNestedRelations {

        @Test
        void deepNestedCategoryLabel() {
            // categories.exists(cat, cat.subCategories.exists(sub, sub.labels.exists(label, label.name == "important")))
            // r1 → cat1 → sub1 → labels=[important, archived] ✓
            // r2 → cat2 → sub2 → labels=[archived, flagged]   ✗
            // r3 → cat1, cat2 → cat1 path matches              ✓
            assertEquals(List.of("1", "3"),
                    runWithMapping("deep-nested-category-label", CATEGORIES_MAP));
        }

        @Test
        void filterDeeplyNested() {
            // same expression as deep-nested-category-label
            assertEquals(List.of("1", "3"),
                    runWithMapping("filter-deeply-nested", CATEGORIES_MAP));
        }

        @Test
        void deepNestedExists() {
            // categories.exists(cat, cat.name == "business" && cat.subCategories.exists(sub, sub.name == "finance"))
            // r1, r3 have cat1=business→sub1=finance ✓; r2 has cat2=development ✗
            assertEquals(List.of("1", "3"),
                    runWithMapping("deep-nested-exists", CATEGORIES_MAP));
        }

        @Test
        void existsNestedCollection() {
            // same expression as deep-nested-exists
            assertEquals(List.of("1", "3"),
                    runWithMapping("exists-nested-collection", CATEGORIES_MAP));
        }

        @Test
        void combinedNot() {
            // !categories.exists(cat, cat.subCategories.exists(sub, sub.name == "finance"))
            // r1: has cat1→sub1=finance → exists, negated → ✗
            // r2: cat2→sub2=tech, no finance → ✓
            // r3: has cat1→sub1=finance → ✗
            assertEquals(List.of("2"),
                    runWithMapping("combined-not", CATEGORIES_MAP));
        }

        @Test
        void mapDeeplyNested() {
            // hasIntersection(categories.subCategories.map(sub, sub.name), ["finance", "tech"])
            // All three resources hit at least one of finance/tech via their categories chain
            assertEquals(List.of("1", "2", "3"),
                    runWithMapping("map-deeply-nested", CATEGORIES_MAP));
        }

        @Test
        void hasIntersectionNested() {
            // same shape as map-deeply-nested
            assertEquals(List.of("1", "2", "3"),
                    runWithMapping("has-intersection-nested", CATEGORIES_MAP));
        }

        @Test
        void threeLevelNestingIsCorrelatedNotCrossJoined() {
            // categories.exists(c, c.subCategories.exists(s, s.labels.exists(l, l.name == "important")))
            // — three relation hops, each a correlated EXISTS one level deeper than the last. This pins
            // the generated SQL shape: a cross/cartesian join would still return rows but would wrongly
            // pair unrelated subcategories/labels, so we assert directly on the SQL, not just the result.
            SqlCapture.STATEMENTS.clear();
            assertEquals(List.of("1", "3"),
                    runWithMapping("deep-nested-category-label", CATEGORIES_MAP));

            String sql = SqlCapture.STATEMENTS.stream()
                    .filter(s -> s.toLowerCase().contains("exists"))
                    .reduce("", (a, b) -> a.length() >= b.length() ? a : b)
                    .toLowerCase();
            assertFalse(sql.isEmpty(), "expected a SELECT with EXISTS to be captured");
            // At least one correlated EXISTS per relation hop (categories -> subCategories ->
            // labels). The tri-state macro translation re-translates each hop's body inside its
            // unknown-element COUNT subqueries, so the total EXISTS count exceeds three — the
            // nesting guarantee is the lower bound plus the cross-join ban and the exact result
            // rows asserted above.
            assertTrue(countOccurrences(sql, "exists") >= 3,
                    "expected at least three nested EXISTS subqueries, SQL was:\n" + sql);
            // Correlated subqueries must not collapse into a cartesian product.
            assertFalse(sql.contains("cross join"),
                    "nested correlation degraded into a cross join, SQL was:\n" + sql);
        }

        private int countOccurrences(String haystack, String needle) {
            int count = 0;
            for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + needle.length())) {
                count++;
            }
            return count;
        }
    }

    // -- Single-valued (@ManyToOne) relation: resource.creator.{name,department} via dotted Field --
    // No special Relation declaration needed — Field("creator.name") traverses the JPA path naturally.
    @Nested
    class SingleValuedRelations {

        @Test
        void manyToOneTraversal() {
            // Synthetic: bare-bool action on aBool — we just confirm the dotted-path Field works
            // by reusing an existing simple test. The is-set test below covers the real value.
            Map<String, AttributeMapping> mapping = new java.util.HashMap<>(FIELD_MAP);
            mapping.put("request.resource.attr.createdBy", AttributeMapping.field("creator.id"));

            // relation-is policy: createdBy == P.id ("user1") → r1
            // With our remapping, createdBy now resolves through the @ManyToOne creator → id column.
            assertEquals(List.of("1"), runWithMapping("relation-is", mapping));
        }

        @Test
        void isSetNested() {
            // request.resource.attr.nested.aOptionalString != null
            // No seeded row sets nested.aOptionalString, so the result is empty: this verifies
            // the nested-path predicate translates and executes, not a positive match.
            assertEquals(List.of(), runWithMapping("is-set-nested", FIELD_MAP));
        }
    }

    // -- Combined: mixing nested + categories in a single OR expression --
    @Nested
    class CombinedExpressions {

        @Test
        void combinedOr() {
            // nested.nextlevel.aBool == true  OR  categories.exists(cat, cat.name == "business")
            // r1: nextlevel.aBool=true → matches
            // r2: nextlevel.aBool=false, categories=[development] → no match
            // r3: nextlevel.aBool=false, categories=[business, development] → matches via business
            assertEquals(List.of("1", "3"), runWithMapping("combined-or", COMBINED_MAP));
        }
    }

    // -- Add operator: string concatenation with constant folding/solving --
    @Nested
    class AddOperator {

        @Test
        void stringConcatPrincipal() {
            // Policy:
            //   any:
            //     - P.attr.context == "projects"
            //     - P.attr.context == "projects:" + R.attr.id
            //
            // With principal.context = "projects:507f1f77bcf86cd799439011":
            //   1st branch is false at plan time → dropped
            //   2nd branch becomes: "projects:507f1f77bcf86cd799439011" == "projects:" + R.attr.id
            //     → adapter solves to: R.attr.id == "507f1f77bcf86cd799439011" → matches r1
            Principal principal = Principal.newInstance("user1", "USER")
                    .withAttribute("context",
                            AttributeValue.stringValue("projects:507f1f77bcf86cd799439011"));

            assertEquals(List.of("1"), runWithPrincipalAndMapping(
                    principal, "string-concat-principal", COMBINED_MAP));
        }

        @Test
        void stringConcatPrincipalNoMatch() {
            // P.attr.context = "projects:does-not-exist" → solves to R.attr.id == "does-not-exist"
            // → no resource matches
            Principal principal = Principal.newInstance("user1", "USER")
                    .withAttribute("context", AttributeValue.stringValue("projects:does-not-exist"));

            assertEquals(List.of(), runWithPrincipalAndMapping(
                    principal, "string-concat-principal", COMBINED_MAP));
        }

        @Test
        void stringConcatPrincipalShortCircuit() {
            // P.attr.context = "projects" → first branch is TRUE at plan time → always-allowed
            Principal principal = Principal.newInstance("user1", "USER")
                    .withAttribute("context", AttributeValue.stringValue("projects"));

            assertEquals(List.of("1", "2", "3"), runWithPrincipalAndMapping(
                    principal, "string-concat-principal", COMBINED_MAP));
        }
    }

    // -- Principal attributes: actions that read request.principal.attr.* --
    @Nested
    class PrincipalAttributes {

        @Test
        void hasIntersectionWithPrincipalTags() {
            // hasIntersection(R.attr.tags.map(t, t.name), P.attr.tags)
            // P.attr.tags = ["public", "private"] → planner substitutes:
            //   hasIntersection(map(R.attr.tags, t, t.name), ["public", "private"])
            // Adapter emits a correlated EXISTS over tags where tags.name IN ["public","private"]:
            //   r1 has [public, private] → ✓
            //   r2 has [private]         → ✓
            //   r3 has [public]          → ✓
            Principal principal = Principal.newInstance("user1", "USER")
                    .withAttribute("tags", AttributeValue.listValue(
                            AttributeValue.stringValue("public"),
                            AttributeValue.stringValue("private")));

            assertEquals(List.of("1", "2", "3"), runWithPrincipalAndMapping(
                    principal, "has-intersection", COMBINED_MAP));
        }

        @Test
        void hasIntersectionPrincipalTagsNoMatch() {
            // P.attr.tags = ["nonexistent"] → no resource has a tag with that name → []
            Principal principal = Principal.newInstance("user1", "USER")
                    .withAttribute("tags",
                            AttributeValue.listValue(AttributeValue.stringValue("nonexistent")));

            assertEquals(List.of(), runWithPrincipalAndMapping(
                    principal, "has-intersection", COMBINED_MAP));
        }

        @Test
        void kitchensink() {
            // The kitchensink action AND-combines:
            //   1. R.attr.tags.filter(tag, tag.name == "public")        (treated as exists)
            //   2. any-of {aOptionalString!=null, aBool==true, exists(tag.id=="tag1" && tag.name=="public"),
            //              nested.aNumber>1, endsWith("ing"), startsWith("ing"), contains("ing")}
            //   3. all-of {hasIntersection(tags.map(t, t.name), P.attr.tags),
            //              "public" in P.attr.tags,                 (folded at plan time)
            //              nested.nextlevel.aBool == true}
            //
            // With P.attr.tags = ["public"]:
            //   3's "public" in P.attr.tags → TRUE at plan time → dropped
            //   3 simplifies to: hasIntersection(tags.map, ["public"]) AND nested.nextlevel.aBool==true
            //
            // Per resource:
            //   r1: filter(public)=hit ✓; any: aOptional!=null ✓; hasIntersection ✓ (tag name "public"); nextlevel.aBool=true ✓ → MATCH
            //   r2: filter(public) → no public tag → ✗
            //   r3: filter(public)=hit ✓; any: aOptional!=null ✓; hasIntersection ✓; nextlevel.aBool=false → ✗ on cond 3
            Principal principal = Principal.newInstance("user1", "USER")
                    .withAttribute("tags",
                            AttributeValue.listValue(AttributeValue.stringValue("public")));

            assertEquals(List.of("1"), runWithPrincipalAndMapping(
                    principal, "kitchensink", COMBINED_MAP));
        }
    }

    // -- DeMorgan / negated operator wrappers (PR #222) --
    // The adapter handles `not` through TriPredicate.not() (junction-barriered negation)
    // around the inner predicate; every supported inner operator composes without source changes.

    @Nested
    class DeMorganNegation {

        @Test
        void notAnd() {
            // !(aBool == true && aString != "string")
            //   r1: !(true && false)  → true  ✓
            //   r2: !(false && _)     → true  ✓
            //   r3: !(true && true)   → false ✗
            assertEquals(List.of("1", "2"), run("not-and"));
        }

        @Test
        void notOr() {
            // !(aBool == true || aString != "string")
            //   r1: !(true || false)  → false ✗
            //   r2: !(false || true)  → false ✗
            //   r3: !(true || true)   → false ✗
            assertEquals(List.of(), run("not-or"));
        }

        @Test
        void notGt() {
            // !(aNumber > 1) → aNumber <= 1; only r1 (aNumber=1)
            assertEquals(List.of("1"), run("not-gt"));
        }

        @Test
        void notLt() {
            // !(aNumber < 2) → aNumber >= 2; r2 (2), r3 (3)
            assertEquals(List.of("2", "3"), run("not-lt"));
        }

        @Test
        void notContains() {
            // !aString.contains("str") — H2 LIKE is case-sensitive by default.
            //   r1: "string" contains "str"   → excluded
            //   r2: "amIAString?"             → match (capital 'S')
            //   r3: "anotherString"           → match (capital 'S')
            assertEquals(List.of("2", "3"), run("not-contains"));
        }

        @Test
        void notStartsWith() {
            //   r1: "string" → excluded
            //   r2: "amIAString?" → match
            //   r3: "anotherString" → match
            assertEquals(List.of("2", "3"), run("not-starts-with"));
        }
    }

    // -- CEL primitives (PR #223) --
    // `empty-collection` (size(coll) == 0) is natively supported via the existing emptiness
    // path in trySizeComparison, and the CEL ternary (`if(cond, then, else)`) is rewritten into
    // an OR of guarded branch predicates. Arithmetic (add/sub/mult/div) in comparisons is
    // translated as double-space SQL arithmetic. mod, regex, casts, and list indexing still
    // throw — the Spring Data adapter has no shape for them in its Criteria-based predicate
    // builder.

    @Nested
    class CelPrimitives {

        @Test
        void emptyCollection() {
            // size(R.attr.tags) == 0 — every resource has non-empty tagNames.
            assertEquals(List.of(), run("empty-collection"));
        }

        @Test
        void arithAdd() {
            // aNumber + 1.0 > 2.0 → aNumber > 1 (double-space SQL arithmetic).
            //   r1: 1+1=2 > 2 ✗   r2: 3 > 2 ✓   r3: 4 > 2 ✓
            assertEquals(List.of("2", "3"), run("arith-add"));
        }

        @Test
        void arithSub() {
            // aNumber - 1.0 < 2.0 → aNumber < 3 → r1 (1), r2 (2).
            assertEquals(List.of("1", "2"), run("arith-sub"));
        }

        @Test
        void arithMult() {
            // aNumber * 2.0 > 2.0 → aNumber > 1 → r2, r3.
            assertEquals(List.of("2", "3"), run("arith-mult"));
        }

        @Test
        void arithDiv() {
            // aNumber / 2.0 > 0.0 → aNumber > 0 → all rows. CEL division on attributes is
            // double division, so the adapter divides in double space (no truncation).
            assertEquals(List.of("1", "2", "3"), run("arith-div"));
        }

        @Test
        void arithModThrows() {
            // int(aNumber) % 2 == 0 — CEL % is integer-only while attribute values are
            // doubles, so mod comparisons stay unsupported (see tryArithmeticComparison).
            assertActionThrows("arith-mod", FIELD_MAP, "mod");
        }

        @Test
        void matchesRegexThrows() {
            assertActionThrows("matches-regex", FIELD_MAP, "Unsupported operator", "matches");
        }

        @Test
        void indexListThrows() {
            assertActionThrows("index-list", FIELD_MAP, "index");
        }

        @Test
        void convertStringThrows() {
            assertActionThrows("convert-string", FIELD_MAP, "string");
        }

        @Test
        void convertDoubleThrows() {
            assertActionThrows("convert-double", FIELD_MAP, "double");
        }

        @Test
        void convertIntThrows() {
            assertActionThrows("convert-int", FIELD_MAP, "int");
        }

        @Test
        void ternarySelectsThenBranchRows() {
            // Policy: (R.attr.aBool ? R.attr.aNumber : 0) > 0 — the planner emits the ternary as
            // if(cond, then, else); the adapter rewrites cmp(if(c,a,b), v) into
            // OR(AND(c, a cmp v), AND(!c, b cmp v)).
            //   r1: aBool=true  → then-branch: aNumber=1 > 0 → ✓
            //   r2: aBool=false → else-branch: 0 > 0        → ✗
            //   r3: aBool=true  → then-branch: aNumber=3 > 0 → ✓
            assertEquals(List.of("1", "3"), run("ternary"));
        }

        @Test
        void stringSizeComparesLength() {
            // size(R.attr.aString) > 0 → LENGTH(a_string) > 0; every row has a non-empty aString.
            assertEquals(List.of("1", "2", "3"), run("string-size"));
        }
    }

    // -- Minor operator/comparison shapes (PR #234) --

    @Nested
    class MinorOperators {

        @Test
        void isNotSet() {
            // aOptionalString == null → only r2 (others have "hello"/"world")
            assertEquals(List.of("2"), run("is-not-set"));
        }

        @Test
        void equalFieldToField() {
            // aString == id (mapped to oid) — no row's aString equals its oid.
            assertEquals(List.of(), run("equal-field-to-field"));
            // The ne direction is non-degenerate: every row's aString differs from its oid.
            assertEquals(List.of("1", "2", "3"), run("not-equal-field-to-field"));
        }

        @Test
        void sizeCountThreshold() {
            // size(ownedBy) >= 2 → COUNT subquery; only r1 has two owners.
            assertEquals(List.of("1"), run("size-count-threshold"));
        }

        @Test
        void equalBoolFalse() {
            // aBool == false → only r2
            assertEquals(List.of("2"), run("equal-bool-false"));
        }

        @Test
        void inNumber() {
            // aNumber in [1, 2, 3] → all three rows have aNumber ∈ {1, 2, 3}.
            assertEquals(List.of("1", "2", "3"), run("in-number"));
        }

        @Test
        void orLeafExists() {
            // aBool == true OR tags.exists(t, t.name == "public") — needs tags mapped as
            // a Relation with id/name fields, so route through NESTED_FIELD_MAP.
            //   r1: aBool=true OR tag1:public → ✓
            //   r2: aBool=false OR tags=[tag3:private] → ✗
            //   r3: aBool=true OR tag1:public → ✓
            assertEquals(List.of("1", "3"), runNested("or-leaf-exists"));
        }
    }

    // -- Collection macro composition (PR #235) --

    @Nested
    class CollectionMacroComposition {

        @Test
        void allWithNestedAnd() {
            // tags.all(t, t.name == "public" && t.id != "tag1") — every resource has at least one
            // tag that fails the inner predicate (r1 has tag1, r2 has tag3 (name=private), r3 has tag1),
            // so the ALL clause is false for all three.
            assertEquals(List.of(), runNested("all-nested"));
        }

        // TODO(#232): the adapter's handleHasIntersection is the only path that accepts a map()
        // expression. A bare `eq(map(...), [...])` is rejected with a hint at the supported shape.
        @Test
        void mapComparedToLiteralListThrows() {
            assertActionThrows("map-compared", NESTED_FIELD_MAP,
                    "map(...)", "hasIntersection");
        }

        @Test
        void sizeOfFilterCountsMatchingElements() {
            // size(tags.filter(t, t.name == "public")) > 0 → r1 and r3 have a "public" tag.
            assertEquals(List.of("1", "3"), runNested("filter-count-gt"));
        }
    }

    // -- Operand order: value-first comparisons and outer references inside lambdas --
    // The planner preserves policy source order; these actions pin the shapes end-to-end.

    @Nested
    class OperandOrder {

        @Test
        void valueFirstLt() {
            // 1 < aNumber → aNumber > 1 → r2 (2), r3 (3). An unmirrored translation
            // (aNumber < 1) would return [].
            assertEquals(List.of("2", "3"), run("value-first-lt"));
        }

        @Test
        void valueFirstSize() {
            // 0 < size(ownedBy) → non-empty → all three.
            assertEquals(List.of("1", "2", "3"), run("value-first-size"));
        }

        @Test
        void valueFirstIntersect() {
            // hasIntersection(["user1","userX"], ownedBy) → r1 [user1,user2], r3 [user1].
            assertEquals(List.of("1", "3"), run("value-first-intersect"));
        }

        @Test
        void outerAttributeInsideLambda() {
            // tags.exists(tag, tag.name == "public" && R.attr.aBool)
            //   r1: aBool=true, has public tag → ✓
            //   r2: aBool=false             → ✗
            //   r3: aBool=true, has public tag → ✓
            assertEquals(List.of("1", "3"), runNested("outer-attr-in-lambda"));
        }
    }

    @Nested
    class HierarchyOperators {

        // Resource scopes seeded above: r1="a.b.c", r2="a.x", r3="a.b".

        @Test
        void overlaps() {
            // Policy: hierarchy(P.attr.context, ":").overlaps(hierarchy(["projects", R.id]))
            // With context "projects:1" the segments are ["projects","1"] and the field segment is
            // R.id, so the overlap reduces to id == "1".
            Principal principal = Principal.newInstance("user1", "USER")
                    .withAttribute("context", AttributeValue.stringValue("projects:1"));
            assertEquals(List.of("1"), runWithPrincipalAndMapping(
                    principal, "hierarchy-overlaps", HIERARCHY_MAP));
        }

        @Test
        void ancestorOf() {
            // Policy: hierarchy(P.attr.scope).ancestorOf(hierarchy(R.attr.scope))
            // P.attr.scope "a.b" must be a strict prefix of R.attr.scope → scope LIKE 'a.b.%'.
            // Only r1 ("a.b.c") is a strict descendant; r3 ("a.b") is equal (not strict).
            Principal principal = Principal.newInstance("user1", "USER")
                    .withAttribute("scope", AttributeValue.stringValue("a.b"));
            assertEquals(List.of("1"), runWithPrincipalAndMapping(
                    principal, "hierarchy-ancestor-of", HIERARCHY_MAP));
        }

        @Test
        void descendentOf() {
            // Policy: hierarchy(R.attr.scope).descendentOf(hierarchy(P.attr.scope))
            // R.attr.scope must be a strict descendant of P.attr.scope "a.b" — same result as
            // ancestorOf above (the relation is symmetric across the two operators).
            Principal principal = Principal.newInstance("user1", "USER")
                    .withAttribute("scope", AttributeValue.stringValue("a.b"));
            assertEquals(List.of("1"), runWithPrincipalAndMapping(
                    principal, "hierarchy-descendent-of", HIERARCHY_MAP));
        }
    }
}
