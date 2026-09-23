/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.queryplan.springdata.Corpus.ActionsFile;
import dev.cerbos.queryplan.springdata.Corpus.AdapterUnsupported;
import dev.cerbos.queryplan.springdata.Corpus.KnownDivergence;
import dev.cerbos.queryplan.springdata.Corpus.NullRepresentationOmitted;
import dev.cerbos.queryplan.springdata.Corpus.UnsupportedShape;
import dev.cerbos.queryplan.springdata.testmodel.AdversarialInnerEntity;
import dev.cerbos.queryplan.springdata.testmodel.AdversarialParentEntity;
import dev.cerbos.queryplan.springdata.testmodel.CategoryEntity;
import dev.cerbos.queryplan.springdata.testmodel.LabelEntity;
import dev.cerbos.queryplan.springdata.testmodel.ResourceEntity;
import dev.cerbos.queryplan.springdata.testmodel.SubCategoryEntity;
import dev.cerbos.sdk.CerbosBlockingClient;
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
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import com.google.protobuf.Value;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Differential suite: plans every {@code conformance/} action against a real Cerbos PDP, runs the
 * translated Specification against seeded rows, and compares the ids with per-row
 * {@code check()} decisions. Needs Docker.
 *
 * <p>Runs on H2 by default. Set {@code adapter.test.db} to {@code postgres} or {@code mysql} to
 * use a Testcontainers database; {@code adapter.test.mysql.collation} overrides the MySQL
 * collation (default {@code utf8mb4_0900_bin}).
 */
class AdversarialConformanceTest {

    /**
     * The corpus mapping, with and without its per-attribute null conventions. Shared with
     * {@link SpringDataTranslatorTest} so both suites describe the same query.
     */
    private static final Map<String, AttributeMapping> MAPPING = Corpus.MAPPING;

    private static final Map<String, AttributeMapping> MAPPING_WITHOUT_NULL_CONVENTIONS =
            Corpus.MAPPING_WITHOUT_NULL_CONVENTIONS;

    private record Tag(String id, String name) {}

    /**
     * One seeded row, feeding both the persisted entity and the check() oracle. {@code note} is
     * corpus prose and never read. List elements are boxed because the corpus carries null
     * elements.
     */
    private record Seed(String id, boolean aBool, String aString, int aNumber,
                        String aOptionalString, List<Double> aNumberList, List<Boolean> aBoolList,
                        List<Tag> tags, List<String> subCategoryNames, String parentSeedId,
                        String note) {}

    /**
     * {@code attr} is raw JSON because the corpus carries scalar, list and struct attributes;
     * {@link #principal()} converts each by its JSON type.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record PrincipalSpec(String id, List<String> roles, Map<String, Object> attr) {}

    /**
     * conformance/seeds.json. Unknown properties are rejected, so a new seed field fails decoding
     * instead of being dropped from both sides of the differential.
     */
    private record SeedsFile(@JsonProperty("$schema") String schema, String description,
                             PrincipalSpec principal, String resourceKind, String principalNote,
                             String relationNote, List<Seed> seeds) {}

    /** One seed's derived fields, exactly as conformance/derived-fields.json carries them. */
    private record DerivedEntry(String createdBy, Double aDouble, String createdAt, String updatedAt, String scope,
                                List<String> labels) {}

    private record DerivedFile(@JsonProperty("$schema") String schema, String description,
                               List<String> fields, Map<String, DerivedEntry> derived) {}

    // The actions.json records and helpers live in Corpus, shared with SpringDataTranslatorTest.

    /** This adapter's key in the corpus. */
    private static final String ADAPTER = Corpus.ADAPTER;

    // -- corpus coverage guards -----------------------------------------------------------------
    //
    // One parsed seed feeds both the entity and the check() oracle, so a corpus key this harness
    // ignored would drop from both sides and the differential would still agree. These key lists
    // are asserted equal to the corpus.

    private static final List<String> SEED_KEYS = List.of(
            "id", "aBool", "aString", "aNumber", "aOptionalString", "aNumberList", "aBoolList",
            "tags", "subCategoryNames", "parentSeedId");

    /** Corpus prose, never read. */
    private static final String SEED_NOTE_KEY = "note";

    /** Keys of each {@code tags[]} element, guarded like the top-level seed keys. */
    private static final List<String> TAG_KEYS = List.of("id", "name");

    private static final List<String> DERIVED_KEYS =
            List.of("createdBy", "aDouble", "createdAt", "updatedAt", "scope", "labels");

    // The principal feeds both the plan and the oracle, so a dropped role or attribute would
    // vanish from both sides too. PrincipalSpec ignores unknown properties so that the coverage
    // assertion, not a decode error, names an added key.

    private static final List<String> PRINCIPAL_KEYS = List.of("id", "roles", "attr");

    private static final List<String> PRINCIPAL_ATTR_KEYS =
            List.of("allowedTags", "context", "fewTeams", "manyTeams", "zero", "emptyTeams",
                    "manyStructs", "nullableStructs", "missingStructs");

    private static SeedsFile seedsFile;
    private static ActionsFile actionsFile;
    /** {@code degenerateOracles} from actions.json: action to {@code "empty"} or {@code "total"}. */
    private static Map<String, String> DEGENERATE_ORACLES;
    private static DerivedFile derivedFile;
    private static List<Seed> SEEDS;

    /** Actions this adapter cannot express and must refuse ({@code adapterUnsupported}). */
    private static List<AdapterUnsupported> adapterUnsupported() {
        return actionsFile.adapterUnsupportedFor(ADAPTER);
    }

    /** {@code expectedUnsupported} actions this adapter translates anyway. */
    private static List<AdapterUnsupported> adapterSupportedExpected() {
        return actionsFile.adapterSupportedExpectedFor(ADAPTER);
    }

    private static Set<String> adapterSupportedExpectedActions() {
        return adapterSupportedExpected().stream()
                .map(AdapterUnsupported::action).collect(java.util.stream.Collectors.toSet());
    }

    static Stream<String> conformanceActions() {
        return Corpus.oracleActions(actionsFile, ADAPTER);
    }

    static Stream<Arguments> adapterUnsupportedActions() {
        return adapterUnsupported().stream().map(u -> Arguments.of(
                u.action(),
                u.reason(),
                Corpus.requireMessage("adapterUnsupported." + ADAPTER + "." + u.action(), u.message())));
    }

    static Stream<Arguments> unsupportedShapes() {
        Set<String> promoted = adapterSupportedExpectedActions();
        return actionsFile.expectedUnsupported().stream()
                .filter(u -> !promoted.contains(u.action()))
                .map(u -> Arguments.of(u.action(), Corpus.requireMessage(
                        "expectedUnsupported." + u.action() + ".messages." + ADAPTER,
                        u.messages() == null ? null : u.messages().get(ADAPTER))));
    }

    /**
     * Actions whose {@code == null} probe reads an attribute the corpus omits when NULL. check()
     * denies every row, so under {@code OMITTED} the adapter must refuse the shape (#302).
     */
    static Stream<Arguments> nullRepresentationOmitted() {
        return actionsFile.nullRepresentationOmitted().stream()
                .map(n -> Arguments.of(n.action(), n.reason(), nullOmittedMessage(n)));
    }

    private static String nullOmittedMessage(NullRepresentationOmitted entry) {
        return Corpus.nullOmittedMessage(entry, ADAPTER);
    }

    /**
     * Label names for the {@code macro-depth3-*} actions. A {@code null} seeds a NULL
     * {@code name}, which check() sees as a missing attribute.
     */
    private static List<String> labelsFor(Seed s) {
        return derivedFor(s).labels();
    }

    /** {@code createdBy}: a timestamp string per seed, split around 2025-01-01. */
    private static String isoFor(Seed s) {
        return derivedFor(s).createdBy();
    }

    /**
     * One seed's derived fields from conformance/derived-fields.json, so the entity and the
     * oracle share one definition.
     */
    private static DerivedEntry derivedFor(Seed s) {
        DerivedEntry entry = derivedFile.derived().get(s.id());
        assertNotNull(entry, () -> "derived-fields.json has no entry for seed \"" + s.id() + "\"");
        return entry;
    }

    /**
     * {@code createdAt} for the {@code ts-*} actions. Future values stay before 2038-01-19, where
     * MySQL's {@code TIMESTAMP} range ends.
     */
    private static java.time.Instant tsFor(Seed s) {
        String value = derivedFor(s).createdAt();
        return value == null ? null : java.time.Instant.parse(value);
    }

    /**
     * {@code aDouble} for the {@code arith-add-*-frac*} actions. a1 holds {@code -0.6}, which is
     * what solving {@code aDouble + 0.7 == 0.1} gives, yet CEL computes
     * {@code -0.6 + 0.7 == 0.09999999999999998}, so a pre-solved filter diverges on that row.
     */
    private static Double doubleFor(Seed s) {
        return derivedFor(s).aDouble();
    }

    /**
     * Hierarchy path for the {@code hier-*} actions. The values include LIKE metacharacters,
     * string prefixes that are not path prefixes, a case variant and a NULL.
     */
    private static String scopeFor(Seed s) {
        return derivedFor(s).scope();
    }

    /**
     * Asserts this harness consumes exactly the seed, principal and derived keys the corpus
     * defines. Rejecting unknown properties on decode catches added keys but not removed ones.
     */
    private static void assertCorpusCoverage(ObjectMapper mapper, Path conformance)
            throws IOException {
        JsonNode rawSeedsFile = mapper.readTree(conformance.resolve("seeds.json").toFile());
        JsonNode rawSeeds = rawSeedsFile.get("seeds");
        assertEquals(SEEDS.size(), rawSeeds.size(), "seeds.json rows lost in decoding");
        for (int i = 0; i < rawSeeds.size(); i++) {
            String label = "seeds.json seeds[" + i + "]";
            assertKeys(label, keysOf(rawSeeds.get(i)), SEED_KEYS, List.of(SEED_NOTE_KEY));
            JsonNode rawTags = rawSeeds.get(i).get("tags");
            for (int j = 0; j < rawTags.size(); j++) {
                assertKeys(label + ".tags[" + j + "]", keysOf(rawTags.get(j)), TAG_KEYS,
                        List.of());
            }
        }

        assertPrincipalCoverage(rawSeedsFile.get("principal"));

        assertKeys("derived-fields.json fields", derivedFile.fields(), DERIVED_KEYS, List.of());
        assertEquals(SEEDS.stream().map(Seed::id).collect(Collectors.toCollection(TreeSet::new)),
                new TreeSet<>(derivedFile.derived().keySet()),
                "derived-fields.json must carry exactly one entry per seeds.json id");
        JsonNode rawDerived =
                mapper.readTree(conformance.resolve("derived-fields.json").toFile()).get("derived");
        for (Map.Entry<String, JsonNode> entry : rawDerived.properties()) {
            assertKeys("derived-fields.json derived[\"" + entry.getKey() + "\"]",
                    keysOf(entry.getValue()), DERIVED_KEYS, List.of());
        }
    }

    /**
     * Guards the principal's keys and attribute value types. Reads the raw JSON, because
     * {@link PrincipalSpec} could only report keys this harness already names.
     */
    private static void assertPrincipalCoverage(JsonNode principal) {
        assertKeys("seeds.json principal", keysOf(principal), PRINCIPAL_KEYS, List.of());
        JsonNode attr = principal.get("attr");
        assertKeys("seeds.json principal.attr", keysOf(attr), PRINCIPAL_ATTR_KEYS, List.of());
        for (Map.Entry<String, JsonNode> entry : attr.properties()) {
            String label = "seeds.json principal.attr." + entry.getKey();
            JsonNode value = entry.getValue();
            switch (entry.getKey()) {
                case "context" -> assertTrue(value.isTextual(), label);
                case "zero" -> assertTrue(value.isNumber(), label);
                case "manyStructs", "nullableStructs", "missingStructs" -> {
                    assertTrue(value.isArray(), label);
                    for (JsonNode element : value) {
                        assertTrue(element.isObject(), label);
                        assertKeys(label + "[]", keysOf(element),
                                entry.getKey().equals("missingStructs") ? List.of() : List.of("name"),
                                List.of());
                        if (!entry.getKey().equals("missingStructs")) {
                            assertTrue(element.get("name").isTextual() || element.get("name").isNull(), label);
                        }
                    }
                }
                default -> {
                    assertTrue(value.isArray(), label);
                    for (JsonNode element : value) assertTrue(element.isTextual(), label);
                }
            }
        }
    }

    private static void assertKeys(String label, Collection<String> got, Collection<String> want,
                                   Collection<String> optional) {
        Set<String> allowed = new LinkedHashSet<>(want);
        allowed.addAll(optional);
        for (String key : got) {
            assertTrue(allowed.contains(key), () -> label + " carries \"" + key
                    + "\", which this harness does not consume: an unconsumed corpus field is"
                    + " dropped from the persisted entity and the check() oracle at once");
        }
        Set<String> missing = new LinkedHashSet<>(want);
        missing.removeAll(got);
        assertTrue(missing.isEmpty(),
                () -> label + " is missing " + missing + ", which this harness consumes");
    }

    private static List<String> keysOf(JsonNode node) {
        return node.properties().stream().map(Map.Entry::getKey).toList();
    }

    private static GenericContainer<?> cerbos;
    private static CerbosBlockingClient client;
    private static EntityManagerFactory emf;
    /** Non-null only when {@code adapter.test.db} selects a real database. */
    private static JdbcDatabaseContainer<?> database;

    @BeforeAll
    static void setUp() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Path conformance = Corpus.conformanceDir();
        seedsFile = mapper.readValue(conformance.resolve("seeds.json").toFile(), SeedsFile.class);
        actionsFile = mapper.readValue(conformance.resolve("actions.json").toFile(), ActionsFile.class);
        DEGENERATE_ORACLES = actionsFile.degenerateOracleShapes();
        derivedFile = mapper.readValue(
                conformance.resolve("derived-fields.json").toFile(), DerivedFile.class);
        SEEDS = seedsFile.seeds();
        assertCorpusCoverage(mapper, conformance);

        // Pinned PDP image; see CerbosTestImage.
        cerbos = new GenericContainer<>(CerbosTestImage.IMAGE)
                .withExposedPorts(3593)
                .withCommand("server", "--set=storage.disk.directory=/policies",
                        "--set=engine.strictEvaluation=" + CerbosTestImage.strictEvaluation())
                .withEnv("CERBOS_NO_TELEMETRY", "1")
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("cerbos-adversarial-pdp")))
                .waitingFor(Wait.forLogMessage(".*Starting gRPC server.*", 1));
        // Copy the whole policy directory, so a policy file added later is loaded too.
        List<Path> policies = policyFiles(conformance.resolve("policies"));
        assertFalse(policies.isEmpty(), "conformance/policies/ holds no policy file");
        for (Path policy : policies) {
            String relative = conformance.resolve("policies").relativize(policy).toString();
            try {
                cerbos.withCopyToContainer(
                        Transferable.of(Files.readAllBytes(policy)), "/policies/" + relative);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        cerbos.start();
        CerbosTestImage.assertPinned(cerbos);
        client = CerbosTestImage.client(cerbos);

        emf = createEntityManagerFactory();
        seed();
    }

    /** Every regular file under the policy directory, in a stable order. */
    private static List<Path> policyFiles(Path directory) throws IOException {
        try (Stream<Path> files = Files.walk(directory)) {
            return files.filter(Files::isRegularFile).sorted().toList();
        }
    }

    /**
     * The H2 persistence unit by default, or the same unit pointed at a PostgreSQL or MySQL
     * container, chosen by {@code adapter.test.db}.
     */
    private static EntityManagerFactory createEntityManagerFactory() {
        String db = System.getProperty("adapter.test.db", "h2");
        switch (db) {
            case "h2":
                return Persistence.createEntityManagerFactory("adversarial-pu");
            case "postgres": {
                PostgreSQLContainer pg = new PostgreSQLContainer(DatabaseTestImages.POSTGRES);
                pg.start();
                database = pg;
                return Persistence.createEntityManagerFactory(
                        "adversarial-pu", jdbcOverrides(pg, "org.hibernate.dialect.PostgreSQLDialect"));
            }
            case "mysql": {
                // Byte-exact collation by default. MySQL's default utf8mb4_0900_ai_ci makes `=`
                // case-insensitive (seeds c1/c2 diverge), and utf8mb4_0900_as_cs ignores the soft
                // hyphen in seed h6 (#474).
                String collation = System.getProperty(
                        "adapter.test.mysql.collation", "utf8mb4_0900_bin");
                MySQLContainer my = new MySQLContainer(DatabaseTestImages.MYSQL)
                        .withCommand("--character-set-server=utf8mb4",
                                "--collation-server=" + collation);
                // Connector/J defaults to client-side prepared statements, which send double
                // parameters as DECIMAL literals, and MySQLDialect casts to decimal(53,20). Double
                // arithmetic would then be exact (3 * 0.1 == 0.3), unlike CEL, so the adapter
                // casts with MySqlDoubleCastFunctionContributor; p-double-frac witnesses it. Set
                // adapter.test.mysql.serverPrepStmts=true to test server-side statements too.
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
            r.setUpdatedAt(derivedFor(s).updatedAt() == null ? null
                    : java.time.OffsetDateTime.parse(derivedFor(s).updatedAt()));
            for (Tag tag : s.tags()) {
                r.addTag(tag.id(), tag.name());
            }
            // One related row per element; a null element becomes a NULL column.
            s.aNumberList().forEach(r::addNumberListElement);
            s.aBoolList().forEach(r::addBoolListElement);
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

            // A seed with no parent gets no parent row, so an absent parent is reachable
            // through a scalar path.
            Seed parentSeed = parentSeedOf(s);
            if (parentSeed != null) {
                AdversarialParentEntity parent = new AdversarialParentEntity();
                parent.setId(s.id() + "-parent");
                parent.setaBool(parentSeed.aBool());
                parent.setaString(parentSeed.aString());
                parent.setaNumber(parentSeed.aNumber());
                parent.setaOptionalString(parentSeed.aOptionalString());
                parent.setResource(r);
                em.persist(parent);

                Seed innerSeed = parentSeedOf(parentSeed);
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
        tx.commit();
        em.close();
    }

    // -- oracle: ask the PDP itself, row by row --

    private static Principal principal() {
        PrincipalSpec spec = seedsFile.principal();
        Principal p = Principal.newInstance(spec.id(), spec.roles().toArray(new String[0]));
        for (Map.Entry<String, Object> attr : spec.attr().entrySet()) {
            p = p.withAttribute(attr.getKey(), asPrincipalAttribute(attr.getKey(), attr.getValue()));
        }
        return p;
    }

    /**
     * Converts a principal attribute by its JSON type, recursively, so the plan and the oracle
     * get the same principal.
     */
    private static AttributeValue asPrincipalAttribute(String key, Object value) {
        if (value == null) return nullAttributeValue();
        if (value instanceof String text) return AttributeValue.stringValue(text);
        if (value instanceof Number number) return AttributeValue.doubleValue(number.doubleValue());
        if (value instanceof Boolean bool) return AttributeValue.boolValue(bool);
        if (value instanceof List<?> list) {
            return AttributeValue.listValue(list.stream()
                    .map(element -> asPrincipalAttribute(key, element)).toList());
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, AttributeValue> fields = new LinkedHashMap<>();
            map.forEach((name, element) -> {
                if (!(name instanceof String field)) {
                    throw new IllegalStateException("Non-string principal field: " + key);
                }
                fields.put(field, asPrincipalAttribute(key + "." + field, element));
            });
            return AttributeValue.mapValue(fields);
        }
        throw new IllegalStateException("Unsupported principal attribute: " + key);
    }

    // -- the to-one relation (conformance/README.md, "The real to-one relation") ----------------
    //
    // `parentSeedId` names the seed whose scalars a row's `parent` carries; that seed's own
    // `parentSeedId` fills `parent.inner`. Each resource owns fresh parent rows, so a filter that
    // returned the parent instead of the child cannot match the oracle by accident.

    /** The seed one hop out, or null when there is none or {@code s} is null. */
    private static Seed parentSeedOf(Seed s) {
        if (s == null || s.parentSeedId() == null) {
            return null;
        }
        return SEEDS.stream()
                .filter(candidate -> candidate.id().equals(s.parentSeedId()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "seeds.json: \"" + s.id() + "\" names parent \"" + s.parentSeedId()
                                + "\", which is not a seed id"));
    }

    /** One level of the chain as check() attributes; a NULL column is a missing attribute. */
    private static Map<String, AttributeValue> relationAttr(Seed s) {
        Map<String, AttributeValue> attrs = new LinkedHashMap<>();
        attrs.put("aBool", AttributeValue.boolValue(s.aBool()));
        attrs.put("aString", AttributeValue.stringValue(s.aString()));
        attrs.put("aNumber", AttributeValue.doubleValue(s.aNumber()));
        if (s.aOptionalString() != null) {
            attrs.put("aOptionalString", AttributeValue.stringValue(s.aOptionalString()));
        }
        return attrs;
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
        // A NULL column is a missing attribute: conditions on it deny, as SQL excludes the row.
        if (s.aOptionalString() != null) {
            r = r.withAttribute("aOptionalString", AttributeValue.stringValue(s.aOptionalString()));
        }
        // `owner` is the same column with a NULL sent as an explicit null, the convention the
        // adapter's null translations assume (eq-null becomes IS NULL). The verdicts differ:
        // `null in ["x", null]` allows, while a missing attribute denies.
        r = r.withAttribute("owner", s.aOptionalString() != null
                ? AttributeValue.stringValue(s.aOptionalString())
                : nullAttributeValue());
        // `coOwner` is the `scope` column as an explicit null, so `null-value-f2f` compares two
        // explicit nulls. `scope` itself is omitted when NULL.
        r = r.withAttribute("coOwner", scopeFor(s) != null
                ? AttributeValue.stringValue(scopeFor(s))
                : nullAttributeValue());
        // NULL tag names as null elements, so `null in tagNames` is true exactly when a tag
        // row's name IS NULL.
        r = r.withAttribute("tagNames", AttributeValue.listValue(s.tags().stream()
                .map(t -> t.name() != null
                        ? AttributeValue.stringValue(t.name())
                        : nullAttributeValue())
                .toList()));
        // Sent verbatim, a null element as an explicit null: `[null, 2][0] == 2` is false in
        // CEL, not an error (seed a6). Persisted as one related row per element.
        r = r.withAttribute("aNumberList", AttributeValue.listValue(s.aNumberList().stream()
                .map(n -> n != null ? AttributeValue.doubleValue(n) : nullAttributeValue())
                .toList()));
        r = r.withAttribute("aBoolList", AttributeValue.listValue(s.aBoolList().stream()
                .map(b -> b != null ? AttributeValue.boolValue(b) : nullAttributeValue())
                .toList()));
        if (doubleFor(s) != null) {
            r = r.withAttribute("aDouble", AttributeValue.doubleValue(doubleFor(s)));
        }
        if (scopeFor(s) != null) {
            r = r.withAttribute("scope", AttributeValue.stringValue(scopeFor(s)));
        }
        // A NULL created_at is a missing attribute: timestamp() over it errors and check() denies.
        if (tsFor(s) != null) {
            r = r.withAttribute("createdAt", AttributeValue.stringValue(derivedFor(s).createdAt()));
        }
        if (derivedFor(s).updatedAt() != null) {
            r = r.withAttribute("updatedAt", AttributeValue.stringValue(derivedFor(s).updatedAt()));
        }
        // The row's single category as one object. Rows without one get no attribute (deny),
        // matching the empty join.
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
        // A row with no parent sends no `parent` attribute (deny), matching a join that finds
        // nothing; likewise for `parent.inner`.
        Seed parentSeed = parentSeedOf(s);
        if (parentSeed != null) {
            Map<String, AttributeValue> parent = relationAttr(parentSeed);
            Seed innerSeed = parentSeedOf(parentSeed);
            if (innerSeed != null) {
                parent.put("inner", AttributeValue.mapValue(relationAttr(innerSeed)));
            }
            r = r.withAttribute("parent", AttributeValue.mapValue(parent));
        }
        return r;
    }

    /**
     * An explicit null attribute. {@link AttributeValue} has no null factory, so its private
     * constructor is called reflectively. check() treats an explicit null differently from a
     * missing attribute.
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

    /** The plan for one action, through the non-deprecated multi-action overload. */
    private static PlanResourcesResult plan(String action) {
        return client.plan(
                principal(), Resource.newInstance(seedsFile.resourceKind()), List.of(action));
    }

    // -- adapter execution through the public Specification path --

    private static List<String> adapterFilteredIds(String action) {
        return adapterFilteredIds(action, NullAttributeRepresentation.EXPLICIT);
    }

    private static List<String> adapterFilteredIds(
            String action, NullAttributeRepresentation representation) {
        return adapterFilteredIds(action, representation, MAPPING);
    }

    private static List<String> adapterFilteredIds(
            String action, NullAttributeRepresentation representation,
            Map<String, AttributeMapping> mapping) {
        Specification<ResourceEntity> spec =
                SpringDataQueryPlanAdapter.toSpecification(
                        plan(action), mapping, Map.of(), representation);
        return executeIds(spec);
    }

    /**
     * The ids the query returns under {@code spec}, or with no filter when {@code spec} is null,
     * as for an always-allowed plan.
     */
    private static List<String> executeIds(Specification<ResourceEntity> spec) {
        EntityManager em = emf.createEntityManager();
        try {
            CriteriaBuilder cb = em.getCriteriaBuilder();
            CriteriaQuery<String> cq = cb.createQuery(String.class);
            Root<ResourceEntity> root = cq.from(ResourceEntity.class);
            cq.select(root.get("id")).distinct(true);
            Predicate p = spec == null ? null : spec.toPredicate(root, cq, cb);
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
        assertOracleShape(action, oracle);
        List<String> filtered = adapterFilteredIds(action);
        assertEquals(oracle, filtered,
                "adapter result diverges from check-API oracle for action '" + action + "'");
    }

    /** {@code expectedUnsupported} shapes must throw with the message actions.json pins. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("unsupportedShapes")
    void unsupportedShapesThrow(String action, String expectedMessage) {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, () -> adapterFilteredIds(action));
        assertTrue(ex.getMessage().contains(expectedMessage),
                "action '" + action + "' was rejected for a reason actions.json does not declare: "
                        + ex.getMessage());
    }

    /** Shapes this adapter cannot express must throw with the message actions.json pins. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("adapterUnsupportedActions")
    void adapterUnsupportedActionsThrow(String action, String reason, String expectedMessage) {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, () -> adapterFilteredIds(action), reason);
        assertTrue(ex.getMessage().contains(expectedMessage),
                "action '" + action + "' was rejected for a reason actions.json does not declare: "
                        + ex.getMessage());
    }

    /**
     * #302. Asserts the over-grant under {@code EXPLICIT} as well as the refusal under
     * {@code OMITTED}, so the refusal cannot pass because of an unrelated error.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("nullRepresentationOmitted")
    void nullRepresentationOmittedIsRejected(String action, String reason, String message) {
        assertEquals(List.of(), oracleAllowedIds(action), reason);

        // EXPLICIT emits IS NULL and returns rows the PDP denies.
        assertFalse(adapterFilteredIds(action).isEmpty(), reason);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> adapterFilteredIds(action, NullAttributeRepresentation.OMITTED));
        assertTrue(ex.getMessage().contains(message), ex.getMessage());
    }

    /**
     * #308. A per-attribute null convention overrides the call-level option. Without the
     * declaration the same call throws, which also shows the stripped mapping differs from
     * {@code MAPPING}.
     */
    @Test
    void perAttributeDeclarationOverridesTheCallLevelRepresentation() {
        // `owner` declares EXPLICIT, so the call-level OMITTED does not apply.
        assertEquals(oracleAllowedIds("null-eq"),
                adapterFilteredIds("null-eq", NullAttributeRepresentation.OMITTED));

        // Without the declaration the same call is refused.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> adapterFilteredIds("null-eq", NullAttributeRepresentation.OMITTED,
                        MAPPING_WITHOUT_NULL_CONVENTIONS));
        assertTrue(ex.getMessage().contains("null operand"), ex.getMessage());
    }

    /**
     * #302. Under {@code OMITTED}, every action whose plan carries a null literal, including one
     * inside a list such as {@code hasIntersection(tagNames, ["public", null])}, must be refused.
     * The corpus is enumerated so new actions are covered.
     */
    @Test
    void everyNullCarryingActionIsRejectedUnderOmitted() {
        Set<String> manifest = new LinkedHashSet<>(actionsFile.conformance());
        actionsFile.expectedUnsupported().forEach(u -> manifest.add(u.action()));
        actionsFile.nullRepresentationOmitted().forEach(n -> manifest.add(n.action()));

        List<String> nullCarrying = new ArrayList<>();
        for (String action : manifest.stream().sorted().toList()) {
            plan(action).getCondition()
                    .filter(AdversarialConformanceTest::planCarriesNullLiteral)
                    .ifPresent(c -> nullCarrying.add(action));
        }

        // Guard against a walk that finds nothing.
        assertTrue(nullCarrying.contains("null-eq-missing"), nullCarrying.toString());
        assertTrue(nullCarrying.contains("in-null-elem-hasint"), nullCarrying.toString());

        List<String> notRejected = new ArrayList<>();
        for (String action : nullCarrying) {
            try {
                adapterFilteredIds(action, NullAttributeRepresentation.OMITTED,
                        MAPPING_WITHOUT_NULL_CONVENTIONS);
                notRejected.add(action);
            } catch (IllegalArgumentException expected) {
                // The refusal must come from the null-operand check, not an unrelated error.
                if (!expected.getMessage().contains(nullOmittedMessage(
                        actionsFile.nullRepresentationOmitted().get(0)))) {
                    notRejected.add(action + " (rejected for the wrong reason: "
                            + expected.getMessage() + ")");
                }
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
     * On MySQL the {@code cerbos_ieee_double} function must be registered (through
     * META-INF/services). Elsewhere it must not be, and the adapter keeps the plain
     * {@code cb.toDouble} cast.
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
     * Pins the upstream planner fold behind the {@code p-has} known divergence: the planner plans
     * {@code has(R.attr.aOptionalString)} as always-allowed while check() denies rows without the
     * attribute. Fails when a PDP image bump stops reproducing it.
     *
     * <p>Policy authors can use {@code R.attr.aOptionalString != null} instead, which plans as
     * {@code IS NOT NULL} (README, "Gotchas").
     */
    @Test
    void upstreamHasFoldOverGrantTripwire() {
        PlanResourcesResult plan = plan("p-has");
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
        // check() denies the rows with a NULL aOptionalString.
        assertTrue(oracle.size() < allIds.size(), upstreamChanged);
        assertTrue(oracle.contains("a1"),
                "sanity: check() must still allow rows whose aOptionalString is set; oracle="
                        + oracle);

        // An unfiltered query returns every row by construction. The over-grant is that the set
        // the PDP denies is non-empty and all of it comes back.
        Set<String> denied = new TreeSet<>(allIds);
        denied.removeAll(oracle);
        assertFalse(denied.isEmpty(), "p-has: check() must deny at least one seed, or there is"
                + " no over-grant for this tripwire to see");
        List<String> unfiltered = executeIds(null);
        assertTrue(unfiltered.containsAll(denied), "the unfiltered query must return every"
                + " row the PDP denies for p-has; denied " + denied + ", got " + unfiltered);
        assertEquals(allIds, unfiltered);

        // The adapter translates the always-allowed plan as no filter.
        assertEquals(unfiltered, adapterFilteredIds("p-has"),
                "the adapter is expected to translate KIND_ALWAYS_ALLOWED faithfully into all "
                        + "rows — if this fails the adapter started second-guessing plan kinds");
    }

    /**
     * The corpus pins two count spellings over the chain. These add the other thresholds and
     * polarities, including one no corpus action reaches, and assert rows without a
     * {@code mainCategory} stay out of every one (#316).
     */
    @Test
    void everyCountThresholdOverTheChainInheritsTheAbsentParentGuard() {
        Operand chain = Operand.newBuilder()
                .setVariable("request.resource.attr.mainCategory.subCategories").build();
        Operand size = expression("size", chain);

        // Seeds with a mainCategory have exactly one subCategory and the rest are CEL
        // missing-path errors, so each of these must be empty.
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

        // The mirror image, so the loop above cannot pass by denying everything.
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
        Specification<ResourceEntity> spec =
                SpringDataQueryPlanAdapter.toSpecification(response, MAPPING, Map.of());

        return executeIds(spec);
    }

    /**
     * #387. {@code filter-as-conjunct} has an empty oracle by construction, so it is never
     * oracle-compared. This shows why refusing it is required: dropping the filter() conjunct
     * leaves {@code root-bare-bool}, which returns rows the PDP denies.
     */
    @Test
    void filterAsConjunctMustBeRefusedBecauseDroppingItsUntranslatableHalfOverGrants() {
        assertEquals(List.of(), oracleAllowedIds("filter-as-conjunct"),
                "check() must deny every seed: a filter() in boolean position is not evaluable");

        List<String> survivingHalf = adapterFilteredIds("root-bare-bool");
        assertFalse(survivingHalf.isEmpty(),
                "root-bare-bool must return rows, else dropping the other conjunct would cost nothing");
        assertTrue(survivingHalf.size() < SEEDS.size(), "root-bare-bool must not return every seed");

        String message = unsupportedShapes()
                .filter(args -> "filter-as-conjunct".equals(args.get()[0]))
                .map(args -> (String) args.get()[1])
                .findFirst()
                .orElseThrow(() -> new AssertionError("filter-as-conjunct pins no throw message"));
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> adapterFilteredIds("filter-as-conjunct"));
        assertTrue(ex.getMessage().contains(message), ex.getMessage());
    }

    /** A throwing action with no pinned message must fail classification (#326). */
    @Test
    void throwingActionWithNoPinnedMessageFailsClassification() {
        for (String absent : new String[] {null, ""}) {
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> Corpus.requireMessage("synthetic-entry", absent));
            assertTrue(ex.getMessage().contains("pins no throw message"), ex.getMessage());
        }
    }

    /** Corpus-size tripwires, and every action gets exactly one outcome. */
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

        // Every group, so a group missing from ActionsFile fails here.
        Set<String> manifest = actionsFile.manifestActions();

        List<String> misclassified = manifest.stream()
                .filter(action -> Stream.of(
                                oracle.contains(action),
                                throwing.contains(action),
                                nullOmitted.contains(action),
                                skipped.contains(action))
                        .filter(Boolean::booleanValue).count() != 1)
                .toList();

        assertEquals(324, manifest.size(),
                "corpus size changed; triage the new action(s) before bumping this pin");
        assertEquals(29, SEEDS.size(), "seed count changed");
        // Each throwing action carries a pinned message; re-triage when this count changes.
        assertEquals(66, throwing.size(), "throwing action count changed");
        assertEquals(throwing.size(),
                adapterUnsupportedActions().count() + unsupportedShapes().count(),
                "every throwing action must reach a parameterised throw case");
        assertEquals(List.of(), misclassified,
                "every manifest action must have exactly one spring-data outcome");
        assertTrue(actionsFile.expectedUnsupported().stream()
                        .map(UnsupportedShape::action)
                        .collect(java.util.stream.Collectors.toSet())
                        .containsAll(supportedExpected),
                "every promoted action must exist in expectedUnsupported");
    }

    /**
     * Refused actions whose group has no oracle-compared member. Their oracles are still checked
     * for discrimination, so the policy stays live for that group.
     */
    private static final List<String> DEGENERACY_LIVENESS_PROBES = List.of(
            "regex-final-newline", "regex-eq-true", "regex-lookahead",
            "index-negative", "index-fractional", "index-not-oob",
            "cast-not-int", "cast-not-string-missing", "cast-not-string-null",
            "cast-not-timestamp", "cast-not-double",
            "regex-digit", "regex-case", "regex-posix", "regex-unanchored", "regex-dot", "regex-alternation", "regex-grouped", "regex-brace", "regex-repetition", "regex-optional-operators", "except-size", "except-eq", "pv-structs", "pv-exists-one", "pv-filter", "pv-map", "pv-except", "temporal-raw-eq", "eq-list", "ne-list",
            // Division inside further arithmetic: SQL cannot carry CEL's NaN or infinity.
            "cr-div-then-add", "cr-div-then-add-ne",
            // int() over a number: CAST rounds where CEL truncates.
            "cast-int-double",
            // string() over a double has no Criteria form; cast-string-bool is compared.
            "cast-string-double",
            // String `+` against the key; `+` is lowered as arithmetic only.
            "id-concat",
            // The same, with both operands columns (#391).
            "concat-f2f",
            // #387: modulo, a positional read of a scalar list, and list equality over map().
            "arith-mod", "index-scalar-list", "map-eq-list",
            // Index errors and explicit-null elements under negation.
            "index-scalar-list-not-eq", "index-scalar-list-null",
            // Positional reads over number and bool lists, and two cross-type probes.
            "index-number-list", "index-number-list-not-eq", "index-bool-list",
            "index-bool-list-not-eq", "index-bool-list-vs-number", "index-number-list-vs-bool",
            // An empty hierarchy delimiter, and matches() with a top-level alternation.
            "hier-empty-delim", "matches-alt");

    /**
     * Fails when an oracle-compared action's oracle is empty or total, since the comparison would
     * then pass whatever the adapter emits. {@code degenerateOracles} in actions.json is the only
     * exemption, and a listed action must be exactly as declared.
     */
    private static void assertOracleShape(String action, List<String> oracle) {
        String declared = DEGENERATE_ORACLES.get(action);
        if ("empty".equals(declared)) {
            assertEquals(List.of(), oracle, "'" + action + "' is listed as an empty oracle in"
                    + " degenerateOracles (conformance/actions.json) but its oracle allows seeds;"
                    + " it discriminates now, so remove it from that list");
        } else if ("total".equals(declared)) {
            assertEquals(allSeedIds(), oracle, "'" + action + "' is listed as a total oracle in"
                    + " degenerateOracles (conformance/actions.json) but its oracle denies seeds;"
                    + " it discriminates now, so remove it from that list");
        } else {
            assertTrue(!oracle.isEmpty() && oracle.size() < SEEDS.size(),
                    "oracle for '" + action + "' is degenerate (" + oracle + "): the differential"
                            + " cannot fail for a degenerate oracle, so either the corpus lost its"
                            + " discriminating seed or the action belongs in degenerateOracles in"
                            + " conformance/actions.json with a reason");
        }
    }

    private static List<String> allSeedIds() {
        return SEEDS.stream().map(Seed::id).sorted().toList();
    }

    /**
     * Every {@code degenerateOracles} entry has exactly its declared oracle, so the list cannot
     * become a blanket exemption.
     */
    @Test
    void everyDegenerateOracleIsExactlyAsDeclared() {
        List<String> wrong = new ArrayList<>();
        for (Map.Entry<String, String> entry : DEGENERATE_ORACLES.entrySet()) {
            List<String> ids = oracleAllowedIds(entry.getKey());
            List<String> want = "empty".equals(entry.getValue()) ? List.of() : allSeedIds();
            if (!want.equals(ids)) {
                wrong.add(entry.getKey() + " (declared " + entry.getValue() + "): " + ids);
            }
        }
        assertEquals(List.of(), wrong, "these degenerateOracles entries in"
                + " conformance/actions.json do not have the oracle they declare");
    }

    /** Liveness probes must not be oracle-compared, and their oracles must discriminate. */
    @Test
    void livenessProbesAreRefusedAndNonDegenerate() {
        Set<String> compared = conformanceActions().collect(java.util.stream.Collectors.toSet());
        for (String action : DEGENERACY_LIVENESS_PROBES) {
            assertFalse(compared.contains(action),
                    "'" + action + "' is now oracle-compared: remove it from the liveness probes");
            List<String> ids = oracleAllowedIds(action);
            assertTrue(!ids.isEmpty() && ids.size() < SEEDS.size(),
                    "oracle for '" + action + "' is degenerate: " + ids);
        }
    }

    /**
     * Reads the seeded to-one chain back through a join and compares it with the corpus. A row
     * count could not tell a correct parent row from one attached to the wrong resource.
     */
    @Test
    void seededToOneChainMatchesTheCorpusRelation() {
        long withParent = SEEDS.stream().filter(s -> parentSeedOf(s) != null).count();
        long withInner = SEEDS.stream()
                .filter(s -> parentSeedOf(parentSeedOf(s)) != null).count();
        assertTrue(withParent > 0, "no seed has a parent");
        assertTrue(withInner > 0, "no seed reaches parent.inner");
        assertTrue(withParent < SEEDS.size(), "every seed has a parent");

        Map<String, List<String>> want = new LinkedHashMap<>();
        for (Seed s : SEEDS) {
            Seed parent = parentSeedOf(s);
            Seed inner = parentSeedOf(parent);
            want.put(s.id(), java.util.Arrays.asList(
                    parent == null ? null : parent.aString(),
                    inner == null ? null : inner.aString()));
        }

        EntityManager em = emf.createEntityManager();
        try {
            Map<String, List<String>> got = new LinkedHashMap<>();
            for (Object[] row : em.createQuery("""
                    select r.id, p.aString, i.aString
                    from ResourceEntity r
                    left join AdversarialParentEntity p on p.resource = r
                    left join AdversarialInnerEntity i on i.parent = p
                    """, Object[].class).getResultList()) {
                got.put((String) row[0], java.util.Arrays.asList((String) row[1], (String) row[2]));
            }
            assertEquals(want, got);
        } finally {
            em.close();
        }
    }
}
