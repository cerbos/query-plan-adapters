package dev.cerbos.queryplan.springdata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.queryplan.springdata.testmodel.CategoryEntity;
import dev.cerbos.queryplan.springdata.testmodel.LabelEntity;
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
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import com.google.protobuf.Value;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Adversarial differential suite: every action from the repo-level {@code ../conformance/}
 * corpus is planned against a real Cerbos PDP, translated by the adapter, and executed against
 * seeded rows. The filtered id set is compared with an oracle computed by calling the check API
 * for each row with matching attributes.
 *
 * <p>No hand-computed expectations: if the adapter's SQL semantics diverge from Cerbos's own
 * evaluation for any row, the mismatch surfaces mechanically. See {@code conformance/README.md}
 * for the shared seed, NULL, and degeneracy conventions.
 *
 * <p><strong>Database selection.</strong> By default the suite runs on in-memory H2. Set the
 * {@code adapter.test.db} system property (forwarded from {@code ADAPTER_TEST_DB}) to
 * {@code postgres} or {@code mysql} to run the same oracle against a real Testcontainers
 * database. The MySQL leg defaults to the case-sensitive {@code utf8mb4_0900_as_cs} collation;
 * using {@code -Dadapter.test.mysql.collation=utf8mb4_0900_ai_ci} reproduces the documented
 * case-insensitive authorization over-grant.
 */
class AdversarialConformanceTest {

    private static final Map<String, AttributeMapping> MAPPING = Map.ofEntries(
            Map.entry("request.resource.attr.aBool", AttributeMapping.field("aBool")),
            Map.entry("request.resource.attr.aString", AttributeMapping.field("aString")),
            Map.entry("request.resource.attr.aNumber", AttributeMapping.field("aNumber")),
            Map.entry("request.resource.attr.aDouble", AttributeMapping.field("aDouble")),
            Map.entry("request.resource.attr.aOptionalString", AttributeMapping.field("aOptionalString")),
            // ISO-date string column + flattened struct member for the p-* probes
            Map.entry("request.resource.attr.createdBy", AttributeMapping.field("createdBy")),
            // Delimited hierarchy path column for the hier-* actions
            Map.entry("request.resource.attr.scope", AttributeMapping.field("scope")),
            // Instant column for the ts-* timestamp() comparison actions
            Map.entry("request.resource.attr.createdAt", AttributeMapping.field("createdAt")),
            Map.entry("request.resource.attr.obj.inner", AttributeMapping.field("aString")),
            // in-null-elem-*: same column as aOptionalString, but the oracle sends an
            // EXPLICIT null attribute for NULL columns (aOptionalString is OMITTED instead)
            // — pinning the adapter's convention that a DB NULL is the explicitly-null
            // attribute (eq-null → IS NULL, and `x in [..., null]` → OR IS NULL).
            Map.entry("request.resource.attr.owner", AttributeMapping.field("aOptionalString")),
            // Scalar projection of tags (defaultMemberField=name) for `null in R.attr.tagNames`;
            // NULL name columns become explicit null list elements on the check side.
            Map.entry("request.resource.attr.tagNames", AttributeMapping.relation("tags", "name")),
            Map.entry("request.resource.attr.tags", AttributeMapping.relation("tags", Map.of(
                    "id", AttributeMapping.field("id"),
                    "name", AttributeMapping.field("name")
            ))),
            Map.entry("request.resource.attr.categories", AttributeMapping.relation("categories", Map.of(
                    "name", AttributeMapping.field("name"),
                    "subCategories", AttributeMapping.relation("subCategories", Map.of(
                            "name", AttributeMapping.field("name"),
                            // Third macro level for the macro-depth3-* actions.
                            "labels", AttributeMapping.relation("labels", Map.of(
                                    "name", AttributeMapping.field("name")
                            ))
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
    private record NullRepresentationOmitted(String action, String reason) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record AdapterUnsupported(String action, String reason) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record KnownDivergence(String action, String reason, List<String> adapters) {}

    /**
     * Every group in actions.json must be named here: Jackson silently drops a field this
     * record does not declare, and a dropped group makes its actions vanish from every count
     * and every parameterised case at once — the projection trap conformance/README.md warns
     * about. The manifest tripwire test below is what makes an undropped group load-bearing.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record ActionsFile(
            List<String> conformance,
            Map<String, List<AdapterUnsupported>> adapterUnsupported,
            Map<String, List<AdapterUnsupported>> adapterSupportedExpected,
            List<UnsupportedShape> expectedUnsupported,
            List<NullRepresentationOmitted> nullRepresentationOmitted,
            List<KnownDivergence> knownDivergences) {}

    /** The corpus key for this adapter — its directory name, as every other harness uses. */
    private static final String ADAPTER = "spring-data";

    private static SeedsFile seedsFile;
    private static ActionsFile actionsFile;
    private static List<Seed> SEEDS;

    /**
     * Conformance actions this adapter cannot express and must reject loudly instead.
     *
     * <p>Spring Data is the reference implementation, so this list is normally empty — a shape it
     * translates is what puts an action in {@code conformance} in the first place. An entry here
     * means the reference itself proved unable to express the shape faithfully and now fails
     * closed, which is still the required outcome: a wrong filter is an authorization bug, a
     * throw is a bug report.
     */
    private static List<AdapterUnsupported> adapterUnsupported() {
        return actionsFile.adapterUnsupported() == null
                ? List.of()
                : actionsFile.adapterUnsupported().getOrDefault(ADAPTER, List.of());
    }

    /** Reference-unsupported shapes this adapter deliberately translates anyway (normally empty). */
    private static List<AdapterUnsupported> adapterSupportedExpected() {
        return actionsFile.adapterSupportedExpected() == null
                ? List.of()
                : actionsFile.adapterSupportedExpected().getOrDefault(ADAPTER, List.of());
    }

    private static Set<String> adapterSupportedExpectedActions() {
        return adapterSupportedExpected().stream()
                .map(AdapterUnsupported::action).collect(java.util.stream.Collectors.toSet());
    }

    static Stream<String> conformanceActions() {
        Set<String> unsupported = adapterUnsupported().stream()
                .map(AdapterUnsupported::action).collect(java.util.stream.Collectors.toSet());
        return Stream.concat(
                actionsFile.conformance().stream().filter(a -> !unsupported.contains(a)),
                adapterSupportedExpectedActions().stream().sorted());
    }

    static Stream<Arguments> adapterUnsupportedActions() {
        return adapterUnsupported().stream().map(u -> Arguments.of(u.action(), u.reason()));
    }

    static Stream<Arguments> unsupportedShapes() {
        Set<String> promoted = adapterSupportedExpectedActions();
        return actionsFile.expectedUnsupported().stream()
                .filter(u -> !u.action().equals("p-timestamp"))
                .filter(u -> !promoted.contains(u.action()))
                .map(u -> Arguments.of(u.action(), u.springDataMessage()));
    }

    /**
     * Actions whose {@code == null} probe targets an attribute the oracle OMITS for NULL
     * columns. They carry no oracle comparison: under the omitted representation check() denies
     * every row, so the adapter must reject the shape rather than emit a filter (#302).
     */
    static Stream<Arguments> nullRepresentationOmitted() {
        return actionsFile.nullRepresentationOmitted().stream()
                .map(n -> Arguments.of(n.action(), n.reason()));
    }
    /**
     * Deterministic label names per seed for the {@code macro-depth3-*} actions — the third
     * macro level (categories → subCategories → labels). A {@code null} entry seeds a label
     * whose {@code name} column is NULL: a missing element attribute on the check side, so the
     * innermost lambda body touching it is a CEL evaluation error that must propagate up
     * through BOTH enclosing macro levels (deny) — and SQL UNKNOWN through the nested scoring
     * subqueries on the adapter side. a1 is the true witness ("gold"), a6 the error witness
     * (no true sibling to absorb the NULL-name error), a8 the determined-false witness, and
     * c1 the collation witness ("Gold" vs "gold"). Only consulted for seeds that hold a
     * category/subCategory chain.
     */
    private static List<String> labelsFor(Seed s) {
        return switch (s.id()) {
            case "a1" -> java.util.Arrays.asList("gold", "silver");
            case "a6" -> java.util.Arrays.asList(null, "silver");
            case "a8" -> List.of("silver");
            case "c1" -> List.of("Gold");
            default -> List.of();
        };
    }

    /** Deterministic ISO instant per seed for the timestamp probe: split around 2025-01-01. */
    private static String isoFor(Seed s) {
        return s.aNumber() >= 2 ? "2024-06-01T00:00:00Z" : "2026-06-01T00:00:00Z";
    }

    /**
     * Deterministic {@link Instant} per seed for the {@code ts-*} timestamp() comparison
     * actions. The split matters: a1/a5 and the {@code aNumber < 2} seeds are firmly in the
     * past (the {@code ts-window} retention cutoff, {@code now() - 24h}, must include them),
     * a2 and the {@code aNumber >= 2} seeds are far enough in the future to stay AFTER any
     * plan-time {@code now()} yet inside MySQL's {@code TIMESTAMP} range (which ends
     * 2038-01-19 — the CI MySQL leg stores Instant as {@code timestamp}), a3 is NULL
     * (missing attribute → CEL error → {@code check()} denies; SQL NULL comparison →
     * UNKNOWN → excluded — both sides must agree), a4 is the {@code ts-eq} witness, and a5
     * carries sub-second (microsecond) precision — exactly representable on H2, PostgreSQL,
     * and MySQL {@code timestamp(6)} columns.
     */
    private static java.time.Instant tsFor(Seed s) {
        return switch (s.id()) {
            case "a1" -> java.time.Instant.parse("2020-03-15T10:30:00Z");
            case "a2" -> java.time.Instant.parse("2037-01-01T00:00:00Z");
            case "a3" -> null;
            case "a4" -> java.time.Instant.parse("2024-06-01T00:00:00Z");
            case "a5" -> java.time.Instant.parse("2020-03-15T10:30:00.123456Z");
            default -> s.aNumber() >= 2
                    ? java.time.Instant.parse("2036-06-06T06:06:06Z")
                    : java.time.Instant.parse("2021-05-05T05:05:05Z");
        };
    }

    /**
     * Deterministic fractional double per seed for the IEEE add-solve probes
     * ({@code arith-add-*-frac*}). a1 carries the algebraic-solve trap: {@code -0.6} is
     * EXACTLY what solving {@code aDouble + 0.7 == 0.1} yields in Java double space, yet
     * {@code check()} denies it ({@code -0.6 + 0.7 == 0.09999999999999998 != 0.1}) — so a
     * pre-solved filter diverges from the oracle on this row. a2 is the exact-arithmetic
     * agreement witness ({@code 0.25 + 0.5 == 0.75} holds bit-for-bit: both filter and
     * oracle INCLUDE it). a3 has NO aDouble (missing attribute → CEL error → deny; SQL NULL
     * arithmetic → UNKNOWN → excluded). The rest get an unremarkable fractional value both
     * sides agree to exclude.
     */
    private static Double doubleFor(Seed s) {
        return switch (s.id()) {
            case "a1" -> -0.6;
            case "a2" -> 0.25;
            case "a3" -> null;
            default -> s.aNumber() + 0.3;
        };
    }

    /**
     * Deterministic hierarchy path per seed for the {@code hier-*} actions. The paths
     * triangulate the translator's branches: strict-prefix IN lists (ancestor-side fields),
     * prefix LIKE (descendant-side fields), the EQUAL path (ancestorOf/descendentOf are
     * strict — verified against a live PDP — while overlaps is inclusive), sibling STRING
     * prefixes that are not PATH prefixes ({@code "dept.engineering"},
     * {@code "dept.eng.platform2"}), LIKE metacharacters in segments (b2 is the
     * unescaped-{@code %} trap, b3 the unescaped-{@code _} trap, b4 the equal-path
     * strictness trap for the colon-delimited metachar actions), a trailing-delimiter empty
     * segment (c2), a case variant for the collation legs (c1), and a NULL (a7: missing
     * attribute → CEL error → deny on the check side vs SQL NULL → excluded on the SQL side).
     */
    private static String scopeFor(Seed s) {
        return switch (s.id()) {
            case "a1" -> "dept";
            case "a2" -> "dept.eng";
            case "a3" -> "dept.eng.platform";
            case "a4" -> "dept.eng.platform.obs";
            case "a5" -> "dept.engineering";
            case "a6" -> "dept.sales";
            case "a8" -> "";
            case "a9" -> "50%";
            case "b1" -> "50%:a_b:x";
            case "b2" -> "50x:a_b:y";
            case "b3" -> "50%:aXb:y";
            case "b4" -> "50%:a_b";
            case "b5" -> "dept.eng.platform2";
            case "b6" -> "50%.a_b";
            case "c1" -> "Dept.Eng";
            case "c2" -> "dept.eng.";
            case "d1" -> "[env]:prod:eu"; // literal-bracket descendant for hier-bracket
            case "d2" -> "e:prod:eu"; // SQL Server char-class trap sibling for hier-bracket
            default -> null; // a7: NULL scope — a missing attribute on the check side
        };
    }

    private static GenericContainer<?> cerbos;
    private static CerbosBlockingClient client;
    private static EntityManagerFactory emf;
    /** Non-null only when {@code adapter.test.db} selects a real database. */
    private static JdbcDatabaseContainer<?> database;

    @BeforeAll
    static void setUp() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Path conformance = conformanceDir();
        seedsFile = mapper.readValue(conformance.resolve("seeds.json").toFile(), SeedsFile.class);
        actionsFile = mapper.readValue(conformance.resolve("actions.json").toFile(), ActionsFile.class);
        SEEDS = seedsFile.seeds();

        // Pinned PDP image — see CerbosTestImage for the pin rationale and bump policy.
        cerbos = new GenericContainer<>(CerbosTestImage.IMAGE)
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
        System.out.printf("==> Adversarial-oracle Cerbos PDP image: %s (digest %s)%n",
                CerbosTestImage.IMAGE, CerbosTestImage.resolvedDigest(cerbos));
        client = new CerbosClientBuilder(cerbos.getHost() + ":" + cerbos.getMappedPort(3593))
                .withPlaintext().buildBlockingClient();

        emf = createEntityManagerFactory();
        seed();
    }

    /**
     * Builds the EntityManagerFactory for the database selected by {@code adapter.test.db}:
     * the H2-backed persistence unit as-is (default), or the same unit with its JDBC
     * connection properties overridden to point at a Testcontainers-managed PostgreSQL or
     * MySQL instance.
     */
    private static EntityManagerFactory createEntityManagerFactory() {
        String db = System.getProperty("adapter.test.db", "h2");
        switch (db) {
            case "h2":
                return Persistence.createEntityManagerFactory("adversarial-pu");
            case "postgres": {
                PostgreSQLContainer<?> pg = new PostgreSQLContainer<>("postgres:16");
                pg.start();
                database = pg;
                return Persistence.createEntityManagerFactory(
                        "adversarial-pu", jdbcOverrides(pg, "org.hibernate.dialect.PostgreSQLDialect"));
            }
            case "mysql": {
                // Case-sensitive server collation by default, per the README's
                // "Database collation requirements" section. Overriding this with MySQL's
                // default utf8mb4_0900_ai_ci reproduces the collation over-grant: the
                // mixed-case seeds (c1/c2) then diverge from the check() oracle.
                String collation = System.getProperty(
                        "adapter.test.mysql.collation", "utf8mb4_0900_as_cs");
                MySQLContainer<?> my = new MySQLContainer<>("mysql:8.4")
                        .withCommand("--character-set-server=utf8mb4",
                                "--collation-server=" + collation);
                // The leg runs with Connector/J's DEFAULT client-side prepared statements,
                // which interpolate double bind parameters as DECIMAL literals. Hibernate's
                // MySQLDialect renders to-double casts as decimal(53,20), so without the
                // adapter's own `cast(... as double)` rendering (registered by
                // MySqlDoubleCastFunctionContributor) the double-space arithmetic would
                // evaluate in exact decimal — 3 * 0.1 == 0.3 becomes TRUE, diverging from
                // CEL IEEE semantics; p-double-frac is the witness. Running client-side by
                // default makes the oracle pin the DOUBLE-cast fix; set
                // -Dadapter.test.mysql.serverPrepStmts=true (env var
                // ADAPTER_TEST_MYSQL_SERVER_PREP_STMTS) to verify the server-side prepared
                // statement mode too — both
                // modes must agree with the check() oracle. Verified empirically on
                // MySQL 8.4.
                if (Boolean.getBoolean("adapter.test.mysql.serverPrepStmts")) {
                    my.withUrlParam("useServerPrepStmts", "true");
                }
                my.start();
                database = my;
                return Persistence.createEntityManagerFactory(
                        "adversarial-pu", jdbcOverrides(my, "org.hibernate.dialect.MySQLDialect"));
            }
            default:
                throw new IllegalArgumentException(
                        "Unknown adapter.test.db '" + db + "' (expected h2, postgres, or mysql)");
        }
    }

    private static Map<String, Object> jdbcOverrides(JdbcDatabaseContainer<?> c, String dialect) {
        return Map.of(
                "jakarta.persistence.jdbc.url", c.getJdbcUrl(),
                "jakarta.persistence.jdbc.driver", c.getDriverClassName(),
                "jakarta.persistence.jdbc.user", c.getUsername(),
                "jakarta.persistence.jdbc.password", c.getPassword(),
                "hibernate.dialect", dialect);
    }

    @AfterAll
    static void tearDown() {
        if (emf != null) emf.close();
        if (database != null) database.stop();
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
            r.setaDouble(doubleFor(s));
            r.setaOptionalString(s.aOptionalString());
            r.setCreatedBy(isoFor(s));
            r.setScope(scopeFor(s));
            r.setCreatedAt(tsFor(s));
            for (Tag tag : s.tags()) {
                r.addTag(tag.id(), tag.name());
            }
            List<CategoryEntity> cats = new ArrayList<>();
            for (String subName : s.subCategoryNames()) {
                catSeq++;
                SubCategoryEntity sub = new SubCategoryEntity("adv-sub-" + catSeq, subName);
                List<LabelEntity> labels = new ArrayList<>();
                int labSeq = 0;
                for (String labelName : labelsFor(s)) {
                    labSeq++;
                    LabelEntity label = new LabelEntity("adv-lab-" + catSeq + "-" + labSeq, labelName);
                    em.persist(label);
                    labels.add(label);
                }
                sub.setLabels(labels);
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
                                                "name", AttributeValue.stringValue(subName),
                                                "labels", AttributeValue.listValue(labelsFor(s).stream()
                                                        .map(AdversarialConformanceTest::asLabelAttribute)
                                                        .toList())))))))
                        .toList()));
        // A DB NULL is a missing attribute on the check side — conditions touching it must
        // deny (CEL error), matching SQL three-valued logic excluding the row.
        if (s.aOptionalString() != null) {
            r = r.withAttribute("aOptionalString", AttributeValue.stringValue(s.aOptionalString()));
        }
        // `owner` reads the SAME column under the OTHER null convention: a DB NULL is the
        // EXPLICITLY-null attribute. This is the convention the adapter's null translations
        // implement (eq-null → IS NULL; a null in-list element → OR IS NULL), and the two
        // check() verdicts genuinely differ: `null in ["x", null]` is TRUE (allow) while a
        // MISSING owner is a CEL error (deny). SQL cannot distinguish the two — the adapter
        // follows the planner, which itself folds `x in [null]` to eq(x, null).
        r = r.withAttribute("owner", s.aOptionalString() != null
                ? AttributeValue.stringValue(s.aOptionalString())
                : nullAttributeValue());
        // tagNames: the scalar name projection of tags, with NULL name columns as explicit
        // null elements — the representation under which `null in R.attr.tagNames` is TRUE
        // exactly when a related row's member column IS NULL.
        r = r.withAttribute("tagNames", AttributeValue.listValue(s.tags().stream()
                .map(t -> t.name() != null
                        ? AttributeValue.stringValue(t.name())
                        : nullAttributeValue())
                .toList()));
        if (doubleFor(s) != null) {
            r = r.withAttribute("aDouble", AttributeValue.doubleValue(doubleFor(s)));
        }
        if (scopeFor(s) != null) {
            r = r.withAttribute("scope", AttributeValue.stringValue(scopeFor(s)));
        }
        // A NULL created_at column is a missing attribute on the check side: timestamp()
        // over it is a CEL evaluation error → deny, matching SQL NULL exclusion.
        if (tsFor(s) != null) {
            r = r.withAttribute("createdAt", AttributeValue.stringValue(tsFor(s).toString()));
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

    /**
     * An explicit protobuf NULL attribute value. The SDK's {@link AttributeValue} exposes no
     * null factory (string/double/bool/list/map only), so the private constructor is reached
     * reflectively — the null attribute is exactly what the in-null-elem-* actions exist to
     * exercise, and check() verdicts differ between an explicit null and a missing attribute.
     */
    private static AttributeValue nullAttributeValue() {
        try {
            var ctor = AttributeValue.class.getDeclaredConstructor(com.google.protobuf.Value.class);
            ctor.setAccessible(true);
            return ctor.newInstance(com.google.protobuf.Value.newBuilder()
                    .setNullValue(com.google.protobuf.NullValue.NULL_VALUE).build());
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "cerbos-sdk-java AttributeValue no longer has a (Value) constructor", e);
        }
    }

    /** A NULL label name in the DB is a missing element attribute on the check side. */
    private static AttributeValue asLabelAttribute(String name) {
        Map<String, AttributeValue> attrs = new LinkedHashMap<>();
        if (name != null) {
            attrs.put("name", AttributeValue.stringValue(name));
        }
        return AttributeValue.mapValue(attrs);
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
        return adapterFilteredIds(action, NullAttributeRepresentation.EXPLICIT);
    }

    private static List<String> adapterFilteredIds(
            String action, NullAttributeRepresentation representation) {
        PlanResourcesResult plan = client.plan(
                principal(), Resource.newInstance(seedsFile.resourceKind()), action);
        Specification<ResourceEntity> spec =
                SpringDataQueryPlanAdapter.<ResourceEntity>toSpecification(
                        plan, MAPPING, Map.of(), representation).toSpecification();

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

    /**
     * Conformance actions the reference itself cannot express (see {@link #adapterUnsupported()}).
     * They are excluded from the oracle comparison and must fail loudly instead — the invariant is
     * absolute either way: an inexpressible shape throws before its filter can be used.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("adapterUnsupportedActions")
    void adapterUnsupportedActionsThrow(String action, String reason) {
        assertThrows(IllegalArgumentException.class, () -> adapterFilteredIds(action), reason);
    }

    /**
     * #302. {@code null-eq-missing} probes {@code aOptionalString == null}, and
     * {@code aOptionalString} follows the corpus default: a NULL column sends NO attribute. Both
     * halves are asserted because the rejection alone would pass vacuously if the adapter threw
     * for an unrelated reason — the over-grant under the default representation is what makes the
     * rejection necessary.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("nullRepresentationOmitted")
    void nullRepresentationOmittedIsRejected(String action, String reason) {
        assertEquals(List.of(), oracleAllowedIds(action), reason);

        // The default translation emits IS NULL and returns exactly the rows the PDP denies.
        assertFalse(adapterFilteredIds(action).isEmpty(), reason);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> adapterFilteredIds(action, NullAttributeRepresentation.OMITTED));
        assertTrue(ex.getMessage().contains("missing-attribute error"), ex.getMessage());
    }

    /**
     * #302 completeness guard. The rejection must key off the null OPERAND, not off a list of
     * operators: {@code hasIntersection(tagNames, ["public", null])} carries one in its value
     * list, and an allowlist of eq/ne/in silently misses it. Enumerating the corpus rather than
     * naming shapes means a newly added action carrying a null constant is covered automatically.
     */
    @Test
    void everyNullCarryingActionIsRejectedUnderOmitted() {
        Set<String> manifest = new LinkedHashSet<>(actionsFile.conformance());
        actionsFile.expectedUnsupported().forEach(u -> manifest.add(u.action()));
        actionsFile.nullRepresentationOmitted().forEach(n -> manifest.add(n.action()));

        List<String> nullCarrying = new ArrayList<>();
        for (String action : manifest.stream().sorted().toList()) {
            PlanResourcesResult plan = client.plan(
                    principal(), Resource.newInstance(seedsFile.resourceKind()), action);
            plan.getCondition()
                    .filter(AdversarialConformanceTest::planCarriesNullLiteral)
                    .ifPresent(c -> nullCarrying.add(action));
        }

        // Guard the guard: if the walk stopped finding null operands the loop below is vacuous.
        assertTrue(nullCarrying.contains("null-eq-missing"), nullCarrying.toString());
        assertTrue(nullCarrying.contains("in-null-elem-hasint"), nullCarrying.toString());

        List<String> notRejected = new ArrayList<>();
        for (String action : nullCarrying) {
            try {
                adapterFilteredIds(action, NullAttributeRepresentation.OMITTED);
                notRejected.add(action);
            } catch (IllegalArgumentException expected) {
                // the shape must be rejected under this representation
            }
        }
        assertEquals(List.of(), notRejected);
    }

    /** Whether any operand anywhere in the plan is a literal null, or a list containing one. */
    private static boolean planCarriesNullLiteral(Operand operand) {
        return switch (operand.getNodeCase()) {
            case VALUE -> {
                Value value = operand.getValue();
                yield value.getKindCase() == Value.KindCase.NULL_VALUE
                        || (value.getKindCase() == Value.KindCase.LIST_VALUE
                                && value.getListValue().getValuesList().stream()
                                        .anyMatch(e -> e.getKindCase()
                                                == Value.KindCase.NULL_VALUE));
            }
            case EXPRESSION -> operand.getExpression().getOperandsList().stream()
                    .anyMatch(AdversarialConformanceTest::planCarriesNullLiteral);
            default -> false;
        };
    }

    /**
     * {@code p-timestamp} compares {@code timestamp(R.attr.createdBy)} where {@code createdBy}
     * maps to a STRING column: timestamp() comparisons are supported only on columns that
     * unambiguously denote an absolute instant (Instant / OffsetDateTime), so this must keep
     * failing closed — with the column-type error, not the old pre-support operand error.
     */
    @Test
    void timestampOnNonTemporalColumnThrowsNamedError() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, () -> adapterFilteredIds("p-timestamp"));
        assertTrue(ex.getMessage().contains("timestamp() comparison requires a column mapped to")
                        && ex.getMessage().contains("String")
                        && ex.getMessage().contains("request.resource.attr.createdBy"),
                "unexpected message: " + ex.getMessage());
    }

    /**
     * Pins the MySQL IEEE double-cast wiring. On the MySQL leg the ServiceLoader-discovered
     * {@link MySqlDoubleCastFunctionContributor} must have registered the
     * {@code cerbos_ieee_double} function — without it the adapter's arithmetic silently
     * evaluates in exact decimal under Connector/J's default client-side prepared statements
     * ({@code p-double-frac} catches the semantics; this test names the mechanism when it
     * breaks, e.g. the META-INF/services entry going missing). On H2/PostgreSQL the function
     * must NOT be registered: those dialects render IEEE-correct casts already, and the
     * adapter must keep their SQL on the untouched {@code cb.toDouble} path.
     */
    @Test
    void ieeeDoubleCastRegistrationMatchesDatabase() {
        org.hibernate.query.sqm.NodeBuilder nb =
                (org.hibernate.query.sqm.NodeBuilder) emf.getCriteriaBuilder();
        boolean registered = nb.getQueryEngine().getSqmFunctionRegistry()
                .findFunctionDescriptor(MySqlDoubleCastFunctionContributor.FUNCTION_NAME) != null;
        boolean mysqlLeg = "mysql".equals(System.getProperty("adapter.test.db", "h2"));
        assertEquals(mysqlLeg, registered, mysqlLeg
                ? "cerbos_ieee_double must be registered on MySQL (is the "
                        + "META-INF/services FunctionContributor entry intact?)"
                : "cerbos_ieee_double must not be registered off-MySQL — H2/PostgreSQL "
                        + "keep the cb.toDouble cast path");
    }

    /**
     * Tripwire pinning the known UPSTREAM planner over-grant on the {@code has(...)} macro.
     *
     * <p>The Cerbos query planner constant-folds {@code has(R.attr.aOptionalString)} (action
     * {@code p-has}) to {@code KIND_ALWAYS_ALLOWED} — "return every row" — even though the
     * {@code check()} API denies resources that lack the attribute. The fold happens at the
     * PLANNER, so every query-plan adapter is affected equally; this adapter translates the
     * always-allowed plan faithfully. That is why {@code p-has} is excluded from the
     * shared conformance action list: the differential comparison
     * cannot pass while the planner itself over-grants. (Tracked in the Cerbos team's
     * internal issue tracker as of 2026-07; no public cerbos/cerbos issue exists.)
     *
     * <p>This test asserts BOTH halves of the divergence — the plan kind AND the check()
     * denials — so that the moment an upstream image stops folding, the test fails with
     * explicit re-inclusion instructions instead of the coverage hole silently becoming
     * permanent. NOTE: the suite runs against the pinned image in {@link CerbosTestImage},
     * so this "fires when upstream fixes the fold" property is dormant between image bumps —
     * the tripwire is re-evaluated on every deliberate bump of that pin (see the bump policy
     * in {@code CerbosTestImage}), which is when an upstream fix would surface here.
     *
     * <p>README "Gotchas" documents the policy-author workaround:
     * {@code R.attr.aOptionalString != null} plans as a conditional {@code ne(variable, null)}
     * that this adapter translates to {@code IS NOT NULL} (PDP-verified).
     */
    @Test
    void upstreamHasFoldOverGrantTripwire() {
        PlanResourcesResult plan =
                client.plan(principal(), Resource.newInstance("adversarial"), "p-has");
        List<String> allIds = SEEDS.stream().map(Seed::id).sorted().toList();
        List<String> oracle = oracleAllowedIds("p-has");

        String upstreamChanged = String.format(
                """

                UPSTREAM CHANGE DETECTED: the Cerbos planner's has() -> KIND_ALWAYS_ALLOWED \
                over-grant no longer reproduces on the image under test.

                Until now, has(R.attr.aOptionalString) (action 'p-has') planned as \
                KIND_ALWAYS_ALLOWED while check() denied rows without the attribute — a known \
                upstream planner fold this adapter translated faithfully into "return all \
                rows". 'p-has' is therefore EXCLUDED from the adapterMatchesCheckOracle \
                @MethodSource. This tripwire exists to keep that exclusion honest.

                The exclusion is no longer justified. Do ALL of the following:
                  1. Classify "p-has" as a shared conformance action and delete its known-divergence
                     entry — the differential oracle then owns
                     has() semantics and will catch any mistranslation of the new residual
                     plan shape mechanically.
                  2. Run the oracle. If the adapter cannot translate the residual shape the
                     planner now emits for has() (fetch it with: curl -s <pdp>/api/plan/resources
                     -d '{"principal":{"id":"u1","roles":["USER"]},"resource":{"kind":
                     "adversarial","attr":{}},"action":"p-has"}'), implement or fail-closed
                     route that shape before re-including.
                  3. Update the README "Gotchas" entry on has() (the over-grant caveat and the
                     != null workaround) to reflect the fixed planner behaviour.
                  4. Delete this tripwire test.

                Observed on this run:
                  plan kind for 'p-has': %s (pinned while broken: KIND_ALWAYS_ALLOWED)
                  check() allowed %d of %d seeded rows: %s
                """,
                plan.getRaw().getFilter().getKind(), oracle.size(), allIds.size(), oracle);

        // Pinned fact 1: the planner still folds has(...) to an unconditional allow-all plan.
        assertTrue(plan.isAlwaysAllowed(), upstreamChanged);
        // Pinned fact 2: the check() oracle diverges from that plan — at least one seeded row
        // (a2/a4/a8/c2 hold NULL aOptionalString) is denied while the plan admits everything.
        assertTrue(oracle.size() < allIds.size(), upstreamChanged);
        assertTrue(oracle.contains("a1"),
                "sanity: check() must still allow rows whose aOptionalString is set; oracle="
                        + oracle);

        // Executable record of the over-grant itself: the adapter translates the always-allowed
        // plan faithfully, so the filtered set is EVERY row — including the ones check() denies.
        assertEquals(allIds, adapterFilteredIds("p-has"),
                "the adapter is expected to translate KIND_ALWAYS_ALLOWED faithfully into all "
                        + "rows — if this fails the adapter started second-guessing plan kinds");
    }

    /**
     * The corpus pins two count spellings over the chain — {@code size(...) == 0} and
     * {@code !(size(...) > 0)} — but the guard has to be a property of the COUNT rather than
     * of the two spellings that happen to be pinned. These synthesise the remaining
     * threshold/polarity combinations onto the same seeded store and assert the parentless
     * rows stay out of every one, including an arbitrary-N threshold that neither corpus
     * action reaches (cerbos/query-plan-adapters#316).
     */
    @Test
    void everyCountThresholdOverTheChainInheritsTheAbsentParentGuard() {
        Operand chain = Operand.newBuilder()
                .setVariable("request.resource.attr.mainCategory.subCategories").build();
        Operand size = expression("size", chain);

        // Every seed that HAS a mainCategory holds exactly one subCategory, and the 16 without
        // it are CEL missing-path errors — so each of these is empty unless the guard leaks.
        Map<String, Operand> emptyByConstruction = new LinkedHashMap<>();
        emptyByConstruction.put("size(chain) == 0", compare("eq", size, 0));
        emptyByConstruction.put("size(chain) <= 0", compare("le", size, 0));
        emptyByConstruction.put("size(chain) < 1", compare("lt", size, 1));
        emptyByConstruction.put("size(chain) >= 2", compare("ge", size, 2));
        emptyByConstruction.put("!(size(chain) > 0)", expression("not", compare("gt", size, 0)));
        emptyByConstruction.put("!(size(chain) >= 1)", expression("not", compare("ge", size, 1)));
        emptyByConstruction.put("!(size(chain) < 2)", expression("not", compare("lt", size, 2)));
        emptyByConstruction.forEach((shape, condition) ->
                assertEquals(List.of(), filteredIdsFor(condition),
                        "absent-parent guard leaked for " + shape));

        // The mirror image, so the loop above cannot pass by denying everything: `>= 0` and
        // `< 2` are TRUE for exactly the rows that HAVE the parent.
        List<String> withParent = oracleAllowedIds("w1-size-nonneg-chain");
        assertFalse(withParent.isEmpty(), "sanity: some seed must carry a mainCategory");
        assertTrue(withParent.size() < SEEDS.size(), "sanity: not every seed carries one");
        assertEquals(withParent, filteredIdsFor(compare("ge", size, 0)));
        assertEquals(withParent, filteredIdsFor(compare("lt", size, 2)));
    }

    private static Operand expression(String operator, Operand... operands) {
        Expression.Builder e = Expression.newBuilder().setOperator(operator);
        for (Operand operand : operands) {
            e.addOperands(operand);
        }
        return Operand.newBuilder().setExpression(e).build();
    }

    private static Operand compare(String operator, Operand left, double threshold) {
        return expression(operator, left,
                Operand.newBuilder().setValue(Value.newBuilder().setNumberValue(threshold)).build());
    }

    /** Translate a synthesised CONDITIONAL plan and execute it against the seeded store. */
    private static List<String> filteredIdsFor(Operand condition) {
        PlanResourcesResponse response = PlanResourcesResponse.newBuilder()
                .setFilter(PlanResourcesFilter.newBuilder()
                        .setKind(PlanResourcesFilter.Kind.KIND_CONDITIONAL)
                        .setCondition(condition))
                .build();
        Result<ResourceEntity> result =
                SpringDataQueryPlanAdapter.toSpecification(response, MAPPING, Map.of());
        Specification<ResourceEntity> spec =
                ((Result.Conditional<ResourceEntity>) result).specification();

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

    /**
     * Corpus-size tripwire and exactly-once partition. A corpus edit must bump the pinned
     * counts in the same change — without this, a new hostile action silently joins the
     * oracle run, and a group dropped by the {@code ActionsFile} parser above would make its
     * actions vanish from every parameterised case with nothing failing.
     */
    @Test
    void manifestAssignsEveryActionExactlyOneOutcome() {
        Set<String> supportedExpected = adapterSupportedExpectedActions();
        Set<String> oracle = conformanceActions().collect(java.util.stream.Collectors.toSet());
        Set<String> throwing = adapterUnsupported().stream()
                .map(AdapterUnsupported::action)
                .collect(java.util.stream.Collectors.toCollection(java.util.HashSet::new));
        actionsFile.expectedUnsupported().stream()
                .map(UnsupportedShape::action)
                .filter(a -> !supportedExpected.contains(a))
                .forEach(throwing::add);
        Set<String> nullOmitted = actionsFile.nullRepresentationOmitted().stream()
                .map(NullRepresentationOmitted::action)
                .collect(java.util.stream.Collectors.toSet());
        Set<String> skipped = actionsFile.knownDivergences().stream()
                .filter(d -> d.adapters().contains(ADAPTER))
                .map(KnownDivergence::action)
                .collect(java.util.stream.Collectors.toSet());

        Set<String> manifest = new java.util.TreeSet<>(actionsFile.conformance());
        actionsFile.expectedUnsupported().forEach(u -> manifest.add(u.action()));
        actionsFile.nullRepresentationOmitted().forEach(n -> manifest.add(n.action()));
        actionsFile.knownDivergences().forEach(d -> manifest.add(d.action()));

        List<String> misclassified = manifest.stream()
                .filter(action -> Stream.of(
                                oracle.contains(action),
                                throwing.contains(action),
                                nullOmitted.contains(action),
                                skipped.contains(action))
                        .filter(Boolean::booleanValue).count() != 1)
                .toList();

        assertEquals(143, manifest.size(),
                "corpus size changed; triage the new action(s) before bumping this pin");
        assertEquals(20, SEEDS.size(), "seed count changed");
        assertEquals(List.of(), misclassified,
                "every manifest action must have exactly one spring-data outcome");
        assertTrue(actionsFile.expectedUnsupported().stream()
                        .map(UnsupportedShape::action)
                        .collect(java.util.stream.Collectors.toSet())
                        .containsAll(supportedExpected),
                "every promoted action must exist in expectedUnsupported");
    }

    @Test
    void oracleIsNotDegenerate() {
        // Guard the guard: at least one action must produce a non-empty, non-total oracle set,
        // otherwise the differential comparison could pass vacuously (e.g. PDP denying all).
        Map<String, List<String>> samples = new LinkedHashMap<>();
        samples.put("vf-le", oracleAllowedIds("vf-le"));
        samples.put("like-percent", oracleAllowedIds("like-percent"));
        samples.put("all-on-empty", oracleAllowedIds("all-on-empty"));
        samples.put("null-eq", oracleAllowedIds("null-eq"));
        samples.put("null-ne", oracleAllowedIds("null-ne"));
        // #309/#312/#311/#315/#316. w1-size-zero-chain, w1-not-size-chain and the two
        // string-cast actions are deliberately absent: their oracles are empty by CONSTRUCTION
        // (no seed holds a to-one parent with zero children; every seed's aString raises in
        // int()/double()), so they cannot satisfy this guard. cast-int-double is the cast
        // group's non-degenerate stand-in.
        samples.put("w1-all-chain", oracleAllowedIds("w1-all-chain"));
        samples.put("w1-not-exists-chain", oracleAllowedIds("w1-not-exists-chain"));
        samples.put("w1-size-nonneg-chain", oracleAllowedIds("w1-size-nonneg-chain"));
        samples.put("w1-not-in-chain", oracleAllowedIds("w1-not-in-chain"));
        samples.put("w1-not-hasint-chain", oracleAllowedIds("w1-not-hasint-chain"));
        samples.put("cr-div-neg-zero", oracleAllowedIds("cr-div-neg-zero"));
        samples.put("cr-div-other-column", oracleAllowedIds("cr-div-other-column"));
        samples.put("cr-div-then-add", oracleAllowedIds("cr-div-then-add"));
        samples.put("cr-div-then-add-ne", oracleAllowedIds("cr-div-then-add-ne"));
        samples.put("cast-int-double", oracleAllowedIds("cast-int-double"));
        samples.forEach((action, ids) -> assertTrue(
                !ids.isEmpty() && ids.size() < SEEDS.size(),
                "oracle for '" + action + "' is degenerate: " + ids));
    }
}
