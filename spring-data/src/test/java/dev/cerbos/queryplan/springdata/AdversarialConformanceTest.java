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
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
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
 *
 * <p><strong>Database selection.</strong> By default the suite runs on in-memory H2. Set the
 * {@code adapter.test.db} system property (forwarded from the {@code ADAPTER_TEST_DB} env var
 * by the Gradle build) to {@code postgres} or {@code mysql} to run the same differential oracle
 * against a real database started via Testcontainers — the same pattern used for the Cerbos PDP
 * container above. The MySQL leg creates its schema with the case-sensitive
 * {@code utf8mb4_0900_as_cs} collation the README requires; running it with MySQL's default
 * case/accent-insensitive collation ({@code -Dadapter.test.mysql.collation=utf8mb4_0900_ai_ci})
 * makes the mixed-case seeds (c1/c2) fail the oracle comparison, reproducing the silent
 * authorization over-grant documented in the README's "Database collation requirements" section.
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
                    List.of(new Tag("t11a", null), new Tag("t11b", "public")), List.of()),
            // Mixed-case collation witnesses: CEL string comparison at the PDP is exact and
            // case-sensitive, so check() DENIES these rows for the lowercase constants the
            // policies use ("one", "public", "finance", "a_b") — but a case-insensitive
            // database collation (MySQL default utf8mb4_0900_ai_ci, SQL Server *_CI_*)
            // matches them anyway, and the differential oracle catches the over-grant.
            // c1 discriminates eq/in ("One" vs "one"), relation membership and
            // hasIntersection ("Public" vs the principal's "public"), and nested-relation
            // equality ("Finance" vs "finance"); c2 discriminates the LIKE family
            // ("xA_by" wrongly matches contains("a_b") under a CI collation).
            new Seed("c1", true, "One", 11, "Set",
                    List.of(new Tag("tc1", "Public")), List.of("Finance")),
            new Seed("c2", false, "xA_by", 12, null, List.of(), List.of()),
            // SQL Server '[' escaping witnesses. d1 is the literal-bracket match: every
            // generated pattern escapes '[' as '\[', which under ESCAPE '\' must STILL be a
            // literal '[' on H2/PostgreSQL/MySQL (like-bracket startsWith, the f2f-* actions
            // — its aOptionalString "[SEC]" is a bracket NEEDLE through the REPLACE chain —
            // and hier-bracket via scopeFor). d2 is the character-class trap: on SQL Server
            // an UNESCAPED '[SEC]%' would match "Secret" (one character from {S,E,C}); it
            // must never match on any dialect.
            new Seed("d1", true, "[SEC]ret", 13, "[SEC]", List.of(), List.of()),
            new Seed("d2", false, "Secret", 14, "xSECy", List.of(), List.of())
    );

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
            "like-percent", "like-underscore", "like-backslash", "like-bracket",
            "unicode-eq", "empty-string-eq",
            // explicit case-sensitivity witness (c1/c2 seeds also discriminate in-single,
            // like-underscore, lambda-in-principal, and p-hasintersection-map on
            // case-insensitive database collations — see the README collation section)
            "cs-eq",
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
            // IEEE add-solve probes: fractional eq/ne must lower to SQL double arithmetic,
            // never to a Java-side algebraic solve (a1 aDouble=-0.6 is the divergence
            // witness; a2 aDouble=0.25 pins the exact-arithmetic inclusion).
            "arith-add-eq-frac", "arith-add-ne-frac", "arith-add-eq-frac-exact",
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
            // size(string) thresholds outside int range: giant constants arrive verbatim;
            // the old (int) narrowing cast wrapped 4294967296 to 0, so gt returned every
            // non-empty row (check() denies all) and lt returned nothing (check() allows all)
            "size-huge-gt", "size-huge-lt",
            // constant NaN / ±Infinity ordering: unfolded div(0,0) → NaN; every NaN
            // ordering is false in CEL/IEEE, while ±Infinity orders normally
            "nan-ord-ternary", "nan-ord-ternary-vf", "nan-ord-le", "nan-ord-inf",
            // multi-hop relation chains via DIRECT dotted syntax (W1) and a root relation
            // subquery anchored from inside a lambda body (W2)
            "w1-exists-chain", "w1-size-chain", "w1-in-chain", "w2-outer-relation",
            // hierarchy operators: both operand orders per operator (field-first ancestorOf
            // and constant-first descendentOf route to the strict-prefix IN-list branch;
            // the mirrored orders route to the prefix-LIKE branch) plus LIKE-metacharacter
            // paths — seeds are documented on scopeFor(...)
            "hier-ancestor-ff", "hier-ancestor-cf", "hier-descendent-ff", "hier-descendent-cf",
            "hier-overlaps-ff", "hier-overlaps-cf", "hier-meta-like", "hier-meta-in",
            "hier-bracket",
            // timestamp(field) vs folded constant instants against the Instant column
            // (created_at). ts-window is the retention-cutoff shape (now()-duration folds
            // at plan time); ts-vf pins value-first MIRRORING (an inversion bug flips the
            // included set); ts-eq/ts-eq-offset pin instant equality incl. non-UTC offset
            // normalization; ts-ne pins NULL exclusion (a3 has no created_at: check()
            // denies on the missing attribute AND SQL excludes the NULL row).
            "ts-window", "ts-vf", "ts-eq", "ts-eq-offset", "ts-ne",
            // in-lists containing null: the null element arrives VERBATIM and check()
            // allows an explicitly-null `owner` (`null in [..., null]` is true) — the
            // translation must be IN (nonNulls) OR IS NULL, with three-valued-safe
            // negation; `in [null]` is planner-folded to eq-null; the rel variants pin
            // EXISTS(member IS NULL); hasint pins the same null-element semantics through
            // the shared intersection translation.
            "in-null-elem-mixed", "in-null-elem-neg",
            "in-null-elem-only", "in-null-elem-only-neg",
            "in-null-elem-rel", "in-null-elem-rel-neg",
            "in-null-elem-hasint",
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
            "p-matches, Unsupported operator: matches",
            "p-index, Unexpected get-field() expression in leaf operand of eq",
    })
    void unsupportedShapesThrow(String action, String expectedMessage) {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, () -> adapterFilteredIds(action));
        assertEquals(expectedMessage, ex.getMessage());
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
