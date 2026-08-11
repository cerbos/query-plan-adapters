package dev.cerbos.queryplan.springdata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.queryplan.springdata.testmodel.AdversarialInnerEntity;
import dev.cerbos.queryplan.springdata.testmodel.AdversarialParentEntity;
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
            // The corpus's one REAL to-one chain (the `rel-*` actions). obj.inner above is a flat
            // column wearing a dotted name; these are a genuine association, and a dotted jpaPath
            // through a to-ONE association is an implicit INNER JOIN in the Criteria API. That is
            // what makes the absent parent deny under BOTH polarities without a separate guard:
            // a row with no parent produces no join row at all, so it is excluded from the query
            // rather than being readmitted by a negation (cerbos/query-plan-adapters#375).
            Map.entry("request.resource.attr.parent.aBool", AttributeMapping.field("parent.aBool")),
            Map.entry("request.resource.attr.parent.aString", AttributeMapping.field("parent.aString")),
            Map.entry("request.resource.attr.parent.aNumber", AttributeMapping.field("parent.aNumber")),
            Map.entry("request.resource.attr.parent.aOptionalString",
                    AttributeMapping.field("parent.aOptionalString")),
            Map.entry("request.resource.attr.parent.inner.aBool",
                    AttributeMapping.field("parent.inner.aBool")),
            Map.entry("request.resource.attr.parent.inner.aString",
                    AttributeMapping.field("parent.inner.aString")),
            Map.entry("request.resource.attr.parent.inner.aNumber",
                    AttributeMapping.field("parent.inner.aNumber")),
            Map.entry("request.resource.attr.parent.inner.aOptionalString",
                    AttributeMapping.field("parent.inner.aOptionalString")),
            // in-null-elem-*: same column as aOptionalString, but the oracle sends an
            // EXPLICIT null attribute for NULL columns (aOptionalString is OMITTED instead)
            // — pinning the adapter's convention that a DB NULL is the explicitly-null
            // attribute (eq-null → IS NULL, and `x in [..., null]` → OR IS NULL).
            // `owner` and `coOwner` alias columns that `aOptionalString` and `scope` also map,
            // under the OTHER null convention: the oracle sends a real null attribute for them
            // rather than omitting it. Declaring that here is what makes the equality family
            // definite for these two attributes and leaves it untouched for every other
            // mapping (cerbos/query-plan-adapters#308).
            Map.entry("request.resource.attr.owner",
                    AttributeMapping.field("aOptionalString", NullAttributeRepresentation.EXPLICIT)),
            Map.entry("request.resource.attr.coOwner",
                    AttributeMapping.field("scope", NullAttributeRepresentation.EXPLICIT)),
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

    /**
     * The same mapping with every per-attribute null convention stripped, so the call-level
     * option is the only thing governing null operands.
     *
     * <p>The #302 completeness guard is a statement about that option: every corpus action
     * carrying a null literal must be rejected under OMITTED. Declaring {@code owner}/
     * {@code coOwner} as explicit-null (#308) deliberately overrides the option for those two
     * attributes — which would otherwise read as the guard going quiet, when in fact it is the
     * per-attribute declaration doing exactly its job. Stripping the declarations keeps the
     * guard testing what it was written to test.
     */
    private static final Map<String, AttributeMapping> MAPPING_WITHOUT_NULL_CONVENTIONS =
            MAPPING.entrySet().stream().collect(java.util.stream.Collectors.toUnmodifiableMap(
                    Map.Entry::getKey,
                    e -> e.getValue() instanceof AttributeMapping.Field f
                            ? AttributeMapping.field(f.jpaPath())
                            : e.getValue()));

    // -- shared corpus (../conformance/): policy, seed data, and action list are read from disk
    // rather than duplicated here. See conformance/README.md for the recipe these implement.

    private static Path conformanceDir() {
        return Path.of(System.getProperty("user.dir"), "..", "conformance").normalize();
    }

    private record Tag(String id, String name) {}

    /**
     * One seeded row; the single source of truth for BOTH the DB entity and the oracle attributes.
     * {@code note} is corpus documentation this harness never reads; it is named so that strict
     * decoding accepts it, and it is the one seed key {@link #SEED_KEYS} omits.
     */
    private record Seed(String id, boolean aBool, String aString, int aNumber,
                        String aOptionalString, List<Tag> tags, List<String> subCategoryNames,
                        String parentSeedId, String note) {}

    /**
     * {@code attr} is typed as raw JSON rather than {@code Map<String, List<String>>}: the corpus
     * carries scalar principal attributes as well as lists, and a narrower type would reject the
     * file rather than silently drop one — but it would still be this harness deciding what the
     * corpus may contain. {@link #principal()} converts each value by its actual JSON type.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record PrincipalSpec(String id, List<String> roles, Map<String, Object> attr) {}

    /**
     * conformance/seeds.json. Every key the file carries is named, including the prose ones,
     * because unknown properties are rejected rather than ignored: a seed field this harness does
     * not consume would be dropped from the entity AND the check() oracle at once, and the
     * differential would agree for the wrong reason.
     */
    private record SeedsFile(@JsonProperty("$schema") String schema, String description,
                             PrincipalSpec principal, String resourceKind, String principalNote,
                             String relationNote, List<Seed> seeds) {}

    /** One seed's derived fields, exactly as conformance/derived-fields.json carries them. */
    private record DerivedEntry(String createdBy, Double aDouble, String createdAt, String scope,
                                List<String> labels) {}

    private record DerivedFile(@JsonProperty("$schema") String schema, String description,
                               List<String> fields, Map<String, DerivedEntry> derived) {}

    /**
     * An {@code expectedUnsupported} entry. {@code messages} carries one entry per adapter that
     * must reject the shape, keyed by adapter name; {@code validate-corpus.sh} asserts that key
     * set is exactly the roster minus the adapters that promoted the shape.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record UnsupportedShape(String action, String shape, Map<String, String> messages) {}

    /**
     * A {@code nullRepresentationOmitted} entry. Every adapter must reject these — the two NULL
     * conventions are indistinguishable on the wire — so {@code messages} names the whole roster
     * with no promotions to subtract.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record NullRepresentationOmitted(String action, String reason,
                                             Map<String, String> messages) {}

    /**
     * An {@code adapterUnsupported} / {@code adapterSupportedExpected} entry. {@code message} is
     * the substring this adapter's error must contain — present on the first, absent on the
     * second, which does not throw.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record AdapterUnsupported(String action, String reason, String message) {}

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

    // -- corpus coverage guards -----------------------------------------------------------------
    //
    // The same parsed seed feeds the persisted entity AND the check() oracle, so a corpus field
    // this harness does not consume is dropped from both sides at once and the differential agrees
    // for the wrong reason — the projection trap conformance/README.md describes for actions.json,
    // applied to the seeds. Asserting set equality catches both directions: a corpus key nothing
    // here reads, and a key this harness reads that the corpus no longer carries.

    private static final List<String> SEED_KEYS = List.of(
            "id", "aBool", "aString", "aNumber", "aOptionalString", "tags", "subCategoryNames",
            "parentSeedId");

    /** Corpus prose, never read by a harness: the one documented exclusion from SEED_KEYS. */
    private static final String SEED_NOTE_KEY = "note";

    /**
     * The one nested object array a seed carries. A key added inside an element is dropped from
     * both sides of the differential just as silently as a top-level one, so it is guarded the
     * same way.
     */
    private static final List<String> TAG_KEYS = List.of("id", "name");

    private static final List<String> DERIVED_KEYS =
            List.of("createdBy", "aDouble", "createdAt", "scope", "labels");

    private static SeedsFile seedsFile;
    private static ActionsFile actionsFile;
    private static DerivedFile derivedFile;
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
        return adapterUnsupported().stream().map(u -> Arguments.of(
                u.action(),
                u.reason(),
                requireMessage("adapterUnsupported." + ADAPTER + "." + u.action(), u.message())));
    }

    static Stream<Arguments> unsupportedShapes() {
        Set<String> promoted = adapterSupportedExpectedActions();
        return actionsFile.expectedUnsupported().stream()
                .filter(u -> !promoted.contains(u.action()))
                .map(u -> Arguments.of(u.action(), requireMessage(
                        "expectedUnsupported." + u.action() + ".messages." + ADAPTER,
                        u.messages() == null ? null : u.messages().get(ADAPTER))));
    }

    /**
     * The substring this adapter's error must contain, or a loud failure. The message is what
     * turns "it threw" into "it threw for the declared reason": without it a mapper typo or an
     * unrelated validation satisfies the throw suite just as well as the documented limitation
     * (cerbos/query-plan-adapters#326).
     */
    private static String requireMessage(String label, String message) {
        if (message == null || message.isEmpty()) {
            throw new IllegalStateException("actions.json pins no throw message for " + label
                    + ": the throw suite would accept a failure for any reason");
        }
        return message;
    }

    /**
     * Actions whose {@code == null} probe targets an attribute the oracle OMITS for NULL
     * columns. They carry no oracle comparison: under the omitted representation check() denies
     * every row, so the adapter must reject the shape rather than emit a filter (#302).
     */
    static Stream<Arguments> nullRepresentationOmitted() {
        return actionsFile.nullRepresentationOmitted().stream()
                .map(n -> Arguments.of(n.action(), n.reason(), nullOmittedMessage(n)));
    }

    /** The substring this adapter's rejection under the omitted representation must contain. */
    private static String nullOmittedMessage(NullRepresentationOmitted entry) {
        return requireMessage(
                "nullRepresentationOmitted." + entry.action() + ".messages." + ADAPTER,
                entry.messages() == null ? null : entry.messages().get(ADAPTER));
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
        return derivedFor(s).labels();
    }

    /** Deterministic ISO instant per seed for the timestamp probe: split around 2025-01-01. */
    private static String isoFor(Seed s) {
        return derivedFor(s).createdBy();
    }

    /**
     * The deterministic derived fields for one seed, read from conformance/derived-fields.json
     * rather than restated here. The same value feeds the persisted entity and the check() oracle,
     * so a transcription error would be self-consistent and invisible to the differential; one
     * machine-readable definition is what makes that impossible. The JavaDoc on the accessors below
     * explains what each value witnesses; conformance/README.md states the rules the file
     * materialises, and validate-corpus.sh re-derives them.
     */
    private static DerivedEntry derivedFor(Seed s) {
        DerivedEntry entry = derivedFile.derived().get(s.id());
        assertNotNull(entry, () -> "derived-fields.json has no entry for seed \"" + s.id() + "\"");
        return entry;
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
        String value = derivedFor(s).createdAt();
        return value == null ? null : java.time.Instant.parse(value);
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
        return derivedFor(s).aDouble();
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
        return derivedFor(s).scope();
    }

    /**
     * Proves this harness consumes every seed key and every derived field the corpus defines, and
     * nothing it does not. Rejecting unknown properties on decode cannot do this alone: it catches
     * an added key but says nothing about one that disappears, and a disappeared key decodes to its
     * default on both sides of the differential.
     */
    private static void assertCorpusCoverage(ObjectMapper mapper, Path conformance)
            throws IOException {
        JsonNode rawSeeds = mapper.readTree(conformance.resolve("seeds.json").toFile()).get("seeds");
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
        Path conformance = conformanceDir();
        seedsFile = mapper.readValue(conformance.resolve("seeds.json").toFile(), SeedsFile.class);
        actionsFile = mapper.readValue(conformance.resolve("actions.json").toFile(), ActionsFile.class);
        derivedFile = mapper.readValue(
                conformance.resolve("derived-fields.json").toFile(), DerivedFile.class);
        SEEDS = seedsFile.seeds();
        assertCorpusCoverage(mapper, conformance);

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
                PostgreSQLContainer<?> pg = new PostgreSQLContainer<>(DatabaseTestImages.POSTGRES);
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
                MySQLContainer<?> my = new MySQLContainer<>(DatabaseTestImages.MYSQL)
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

            // The to-one chain, one owned row per level. A seed with no parent gets no row at
            // all, which is what makes the absent-parent hazard reachable through a SCALAR
            // rather than only through mainCategory's collection.
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
     * One principal attribute, converted by the JSON type the corpus actually carries. Strings
     * and lists of strings are the two shapes today; anything else fails loudly rather than being
     * coerced, because a silently reshaped principal attribute feeds the plan and the oracle at
     * once and they would agree for the wrong reason.
     */
    private static AttributeValue asPrincipalAttribute(String key, Object value) {
        if (value instanceof String s) {
            return AttributeValue.stringValue(s);
        }
        if (value instanceof List<?> list) {
            return AttributeValue.listValue(list.stream()
                    .map(element -> {
                        if (element instanceof String s) {
                            return AttributeValue.stringValue(s);
                        }
                        throw new IllegalStateException(
                                "seeds.json principal.attr." + key + " holds a non-string element");
                    })
                    .toList());
        }
        throw new IllegalStateException(
                "seeds.json principal.attr." + key + " is neither a string nor a list of strings");
    }

    // -- the real to-one relation (conformance/README.md, "The real to-one relation") -----------
    //
    // `parentSeedId` names the seed whose four scalars a row's `parent` carries, and that seed's
    // own `parentSeedId` names the ones `parent.inner` carries. The chain is cut at two levels.
    // Every resource owns a FRESH parent (and inner) row rather than pointing at the named seed's
    // own row, so no two resources share one and a filter that returned the parent instead of the
    // child cannot agree with the oracle by accident.

    /** The seed one hop out, or null when this level has no parent. A null argument returns null. */
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

    /**
     * One level of the chain as check() attributes. A NULL column is a MISSING attribute one hop
     * out, exactly as it is on the resource row itself.
     */
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
        // `coOwner` is the explicit-null alias of the `scope` column, the second half of
        // `null-value-f2f`: `scope` itself is omitted when NULL (below), so the corpus carries
        // the same column under both conventions and the field-to-field probe has two explicit
        // nulls to compare.
        r = r.withAttribute("coOwner", scopeFor(s) != null
                ? AttributeValue.stringValue(scopeFor(s))
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
        // The real to-one chain, mirroring the seeded rows exactly. A row with no parent sends NO
        // `parent` attribute — a CEL missing-path error (deny) — matching a join that finds
        // nothing; the same holds one level down for `parent.inner`.
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
        return adapterFilteredIds(action, representation, MAPPING);
    }

    private static List<String> adapterFilteredIds(
            String action, NullAttributeRepresentation representation,
            Map<String, AttributeMapping> mapping) {
        PlanResourcesResult plan = client.plan(
                principal(), Resource.newInstance(seedsFile.resourceKind()), action);
        Specification<ResourceEntity> spec =
                SpringDataQueryPlanAdapter.toSpecification(
                        plan, mapping, Map.of(), representation);

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
     *
     * <p>{@code p-timestamp} runs here like every other shape. It used to be routed around this
     * case because {@code actions.json} still carried the pre-support operand error; the corpus
     * now pins the column-type error the adapter actually raises, which is the one that must keep
     * firing.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("unsupportedShapes")
    void unsupportedShapesThrow(String action, String expectedMessage) {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, () -> adapterFilteredIds(action));
        assertTrue(ex.getMessage().contains(expectedMessage),
                "action '" + action + "' was rejected for a reason actions.json does not declare: "
                        + ex.getMessage());
    }

    /**
     * Conformance actions the reference itself cannot express (see {@link #adapterUnsupported()}).
     * They are excluded from the oracle comparison and must fail loudly instead — the invariant is
     * absolute either way: an inexpressible shape throws before its filter can be used.
     */
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
     * #302. {@code null-eq-missing} probes {@code aOptionalString == null}, and
     * {@code aOptionalString} follows the corpus default: a NULL column sends NO attribute. Both
     * halves are asserted because the rejection alone would pass vacuously if the adapter threw
     * for an unrelated reason — the over-grant under the default representation is what makes the
     * rejection necessary.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("nullRepresentationOmitted")
    void nullRepresentationOmittedIsRejected(String action, String reason, String message) {
        assertEquals(List.of(), oracleAllowedIds(action), reason);

        // The default translation emits IS NULL and returns exactly the rows the PDP denies.
        assertFalse(adapterFilteredIds(action).isEmpty(), reason);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> adapterFilteredIds(action, NullAttributeRepresentation.OMITTED));
        assertTrue(ex.getMessage().contains(message), ex.getMessage());
    }

    /**
     * #308. The per-attribute declaration overrides the call-level option, which is the
     * property that makes a suite mixing both conventions expressible at all. Asserted in both
     * directions against the SAME action and the SAME call-level option, varying only whether
     * the mapping declares the convention — so a declaration that did nothing would show up here
     * as the two runs agreeing. It also proves the completeness guard below is not quietly
     * running against the same mapping.
     */
    @Test
    void perAttributeDeclarationOverridesTheCallLevelRepresentation() {
        // `owner` declares EXPLICIT, so the call-level OMITTED does not reach it.
        assertEquals(oracleAllowedIds("null-eq"),
                adapterFilteredIds("null-eq", NullAttributeRepresentation.OMITTED));

        // Strip the declaration and the same action under the same option is rejected — so the
        // stripped mapping the completeness guard uses is not quietly equivalent to MAPPING.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> adapterFilteredIds("null-eq", NullAttributeRepresentation.OMITTED,
                        MAPPING_WITHOUT_NULL_CONVENTIONS));
        assertTrue(ex.getMessage().contains("null operand"), ex.getMessage());
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
                adapterFilteredIds(action, NullAttributeRepresentation.OMITTED,
                        MAPPING_WITHOUT_NULL_CONVENTIONS);
                notRejected.add(action);
            } catch (IllegalArgumentException expected) {
                // The rejection must be the null-operand check talking, not an incidental
                // failure: a mapper typo counting as the required rejection is the silent pass
                // the corpus README warns about.
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
        Specification<ResourceEntity> spec =
                SpringDataQueryPlanAdapter.toSpecification(response, MAPPING, Map.of());

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
     * Adding a throwing action without pinning its message must fail this harness rather than
     * silently degrade the throw suite to a bare "it threw" (cerbos/query-plan-adapters#326).
     */
    @Test
    void throwingActionWithNoPinnedMessageFailsClassification() {
        for (String absent : new String[] {null, ""}) {
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> requireMessage("synthetic-entry", absent));
            assertTrue(ex.getMessage().contains("pins no throw message"), ex.getMessage());
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

        assertEquals(167, manifest.size(),
                "corpus size changed; triage the new action(s) before bumping this pin");
        assertEquals(21, SEEDS.size(), "seed count changed");
        // Throwing-count tripwire: each of these carries a pinned message, so a shape gained or
        // lost has to be re-triaged here rather than joining the throw suite unnoticed. The two
        // @MethodSource streams that feed the throw cases are what resolve those messages, and
        // both fail loudly on a missing one.
        assertEquals(11, throwing.size(), "throwing action count changed");
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
     * A representative sample of the actions this adapter ORACLE-COMPARES, one per hostile group
     * it can express. Asserted against {@link #conformanceActions()} so moving one into
     * {@code adapterUnsupported} fails here rather than silently going inert
     * (cerbos/query-plan-adapters#324).
     *
     * <p>{@code w1-size-zero-chain}, {@code w1-not-size-chain}, {@code w1-size-frac-chain} and
     * the two string-cast actions are deliberately absent: their oracles are empty by
     * CONSTRUCTION (no seed holds a to-one parent with zero children, nor one with two or more;
     * every seed's aString raises in {@code int()}/{@code double()}), so they cannot satisfy
     * this guard.
     */
    private static final List<String> DEGENERACY_GUARD_ACTIONS = List.of(
            "vf-le", "like-percent", "all-on-empty", "null-eq", "null-ne",
            // The explicit-null convention against a non-null operand (#308). All five are
            // compared rather than thrown, because the mapper declares the convention per
            // attribute; every one of them under-granted by exactly the NULL-column rows
            // before that declaration existed.
            "null-value-ne-const", "null-value-not-eq-const", "null-value-not-in-const",
            "null-value-f2f", "null-value-pv-not-exists",
            // The absent to-one parent (#309/#315/#316/#333/#334).
            "w1-all-chain", "w1-not-exists-chain", "w1-size-nonneg-chain",
            "w1-not-in-chain", "w1-not-hasint-chain",
            "w1-ternary-chain-cond", "w1-size-frac-le-chain",
            // Column arithmetic under a division (#311). The two shapes that nest further
            // arithmetic on top of the division are liveness probes below.
            "cr-div-neg-zero", "cr-div-other-column",
            // The real to-one join (#375): one per hazard — the negated hop, the null comparison,
            // two-level depth, the root conjunction, and the disjunction, whose failure
            // direction is an under-grant.
            "rel-not-bool-hop", "rel-ne-null-hop", "rel-bool-hop2",
            "rel-hop-and-root", "rel-hop2-or-exists");

    /**
     * Shapes this adapter refuses to translate: they have no oracle comparison to guard, and stay
     * here as PDP/policy liveness probes for a group the list above cannot cover.
     */
    private static final List<String> DEGENERACY_LIVENESS_PROBES = List.of(
            // A division nested inside further arithmetic fails closed: SQL has no value that
            // carries CEL's NaN or signed infinity through the sum.
            "cr-div-then-add", "cr-div-then-add-ne",
            // int() over a numeric column: truncation-versus-rounding, unsupported for every
            // adapter but convex, which promotes it in adapterSupportedExpected.
            "cast-int-double");

    @Test
    void oracleIsNotDegenerate() {
        // Guard the guard: each of these actions must produce a non-empty, non-total oracle set,
        // otherwise the differential comparison could pass vacuously (e.g. PDP denying all).
        Set<String> compared = conformanceActions().collect(Collectors.toSet());
        Map<String, List<String>> samples = new LinkedHashMap<>();
        for (String action : DEGENERACY_GUARD_ACTIONS) {
            assertTrue(compared.contains(action),
                    "'" + action + "' guards nothing: this adapter does not oracle-compare it");
            samples.put(action, oracleAllowedIds(action));
        }
        // Asserting the complement keeps the split honest — an action this adapter gains support
        // for must move up into the guard proper.
        for (String action : DEGENERACY_LIVENESS_PROBES) {
            assertFalse(compared.contains(action),
                    "'" + action + "' is now oracle-compared: move it into the guard proper");
            samples.put(action, oracleAllowedIds(action));
        }
        samples.forEach((action, ids) -> assertTrue(
                !ids.isEmpty() && ids.size() < SEEDS.size(),
                "oracle for '" + action + "' is degenerate: " + ids));
    }

    /**
     * The to-one relation carries no corpus action yet — this is the expand half of
     * cerbos/query-plan-adapters#372's expand–contract — so nothing else in this class would
     * notice a seeder that stored no chain at all, or one that attached every parent to the wrong
     * resource. Read the two hops back through a real join rather than counting rows: a count
     * cannot tell an inner row carrying the corpus's values from one carrying the root's own
     * columns, which is exactly the flat-column-alias failure this relation exists to make
     * visible.
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
