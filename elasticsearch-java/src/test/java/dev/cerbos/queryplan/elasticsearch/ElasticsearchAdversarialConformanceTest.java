/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.cerbos.queryplan.elasticsearch.Corpus.Golden;
import dev.cerbos.queryplan.elasticsearch.Corpus.LedgerEntry;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Result;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.DynamicContainer.dynamicContainer;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

/**
 * The conformance harness. It implements conformance/README.md, "The harness contract":
 *
 * <ol>
 *   <li>Index the dataset ({@code seeds.json} + {@code derived-fields.json}) in a real
 *       Elasticsearch, mapped by {@link Corpus#OPTIONS}.</li>
 *   <li>For each PDP and each recorded golden file, translate the plan, run the query, and compare
 *       the ids with the ones {@code check()} allowed. {@code conformance-ledger.json} lists the
 *       exceptions: {@code unsupported} must throw {@link UnsupportedPlanShapeException}, and
 *       {@code divergent} must still give a wrong answer.</li>
 *   <li>Fail if the ledger names a case that has no golden file, or carries an entry the contract
 *       does not allow (an unknown status, no reason, a divergence with no issue, or a
 *       {@code pdp} tag that is not tested).</li>
 * </ol>
 *
 * <p>Needs no PDP: the plans and decisions are recorded. Needs Docker for Elasticsearch.
 */
class ElasticsearchAdversarialConformanceTest {

    private static final String INDEX = "conformance";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // -- the dataset ------------------------------------------------------------------------------

    private record Tag(String id, String name) {}

    /** One seed row. List elements are boxed so a null element survives. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Seed(String id, boolean aBool, String aString, int aNumber,
                        String aOptionalString, List<Double> aNumberList, List<Boolean> aBoolList,
                        List<Tag> tags, List<String> subCategoryNames, String parentSeedId) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record SeedsFile(List<Seed> seeds) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record DerivedEntry(String createdBy, Double aDouble, String createdAt,
                                String updatedAt, String scope, List<String> labels) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record DerivedFile(Map<String, DerivedEntry> derived) {}

    private static final List<Seed> SEEDS = readJson("seeds.json", SeedsFile.class).seeds();
    private static final Map<String, DerivedEntry> DERIVED =
            readJson("derived-fields.json", DerivedFile.class).derived();

    private static <T> T readJson(String name, Class<T> type) {
        try {
            return MAPPER.readValue(Corpus.conformanceDir().resolve(name).toFile(), type);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static ElasticsearchContainer elasticsearch;
    private static TestElasticsearch es;

    /** Passed / total per PDP tag and tier, printed once the run ends. */
    private static final Map<String, int[]> TALLY = new TreeMap<>();

    @BeforeAll
    static void setUp() throws Exception {
        elasticsearch = new ElasticsearchContainer(ElasticsearchTestImage.IMAGE)
                .withEnv("xpack.security.enabled", "false");
        elasticsearch.start();
        es = new TestElasticsearch(elasticsearch.getHttpHostAddress());
        createIndex();
        seedIndex();
        es.refresh(INDEX);
    }

    @AfterAll
    static void tearDown() {
        TALLY.forEach((key, counts) ->
                System.out.printf("conformance %s: %d / %d pass%n", key, counts[0], counts[1]));
        if (elasticsearch != null) elasticsearch.stop();
    }

    // -- the contract -----------------------------------------------------------------------------

    @TestFactory
    Stream<DynamicNode> conformance() {
        DynamicNode ledger = dynamicTest("every ledger entry is well formed", () -> {
            List<String> malformed = new ArrayList<>();
            Corpus.LEDGER.forEach((id, entry) -> {
                if (!"unsupported".equals(entry.status()) && !"divergent".equals(entry.status())) {
                    malformed.add(id + ": unknown status " + entry.status());
                }
                if (entry.reason() == null || entry.reason().isBlank()) {
                    malformed.add(id + ": no reason");
                }
                if ("divergent".equals(entry.status()) && (entry.issue() == null || entry.issue().isBlank())) {
                    malformed.add(id + ": divergent without an issue");
                }
                // An entry scoped to a PDP that is no longer tested can never apply again.
                if (entry.pdp() != null && (entry.pdp().isEmpty() || !Corpus.PDP_TAGS.containsAll(entry.pdp()))) {
                    malformed.add(id + ": pdp " + entry.pdp() + " is not a subset of " + Corpus.PDP_TAGS);
                }
            });
            assertEquals(List.of(), malformed);
        });
        return Stream.concat(Stream.of(ledger), Corpus.PDP_TAGS.stream().map(tag -> {
            List<Golden> goldens = Corpus.goldens(tag);
            List<DynamicNode> tests = new ArrayList<>();
            tests.add(dynamicTest("has a golden file for every case in the ledger", () -> {
                TreeSet<String> stale = new TreeSet<>(Corpus.LEDGER.keySet());
                goldens.forEach(golden -> stale.remove(golden.id()));
                assertEquals(new TreeSet<String>(), stale, "ledger entries with no golden file");
            }));
            goldens.forEach(golden -> tests.add(
                    dynamicTest(golden.tier() + ": " + golden.id(), () -> check(tag, golden))));
            return dynamicContainer("PDP " + tag, tests);
        }));
    }

    private static void check(String tag, Golden golden) throws Exception {
        // The total counts every golden in the tier, skipped ones included.
        int[] tally = TALLY.computeIfAbsent(tag + " " + golden.tier(), key -> new int[2]);
        tally[1]++;
        Assumptions.assumeFalse(golden.skipped(tag), "planner divergence");
        List<String> allowed = golden.allowed().stream().sorted().toList();
        LedgerEntry entry = Corpus.ledgerEntry(golden.id(), tag);
        String status = entry == null ? null : entry.status();
        if (status == null) {
            assertEquals(allowed, ids(golden));
            tally[0]++;
        } else if (status.equals("unsupported")) {
            assertThrows(UnsupportedPlanShapeException.class, () -> ids(golden));
        } else if (status.equals("divergent")) {
            assertNotEquals(allowed, ids(golden), "fixed: remove the divergent ledger entry");
        } else {
            throw new IllegalStateException(golden.id() + ": unknown ledger status " + status);
        }
    }

    /** Translates the recorded plan and runs it, returning the matching seed ids sorted. */
    private static List<String> ids(Golden golden) throws Exception {
        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(golden.plan(), Corpus.OPTIONS);
        if (result instanceof Result.AlwaysAllowed) return SEEDS.stream().map(Seed::id).sorted().toList();
        if (result instanceof Result.AlwaysDenied) return List.of();
        Map<String, Object> query = ((Result.Conditional) result).query();
        // Elasticsearch returns 10 hits by default.
        List<String> ids = es.ids("/" + INDEX + "/_search?size=" + SEEDS.size(),
                Map.of("query", Map.of("bool", Map.of("filter", List.of(query)))));
        return ids.stream().sorted().toList();
    }

    // -- the index --------------------------------------------------------------------------------

    private static void createIndex() throws Exception {
        Map<String, Object> tagProperties = Map.of(
                "id", Map.of("type", "keyword"),
                "name", Map.of("type", "keyword"));
        Map<String, Object> labelProperties = Map.of("name", Map.of("type", "keyword"));
        Map<String, Object> subCategoryProperties = Map.of(
                "name", Map.of("type", "keyword"),
                "labels", Map.of("type", "nested", "properties", labelProperties));
        Map<String, Object> categoryProperties = Map.of(
                "name", Map.of("type", "keyword"),
                "subCategories", Map.of("type", "nested", "properties", subCategoryProperties));
        Map<String, Object> mainCategoryProperties = Map.of(
                "name", Map.of("type", "keyword"),
                "subNames", Map.of("type", "keyword"),
                "subCategories", Map.of("type", "nested", "properties", Map.of(
                        "name", Map.of("type", "keyword"))));

        Map<String, Object> properties = new LinkedHashMap<>();
        // The corpus id as a keyword field, alongside `_id`. `_id` is metadata that term queries
        // cannot filter on, and the `identifier/*` cases need a real field.
        properties.put("id", Map.of("type", "keyword"));
        properties.put("aBool", Map.of("type", "boolean"));
        properties.put("aString", Map.of("type", "keyword"));
        properties.put("aNumber", Map.of("type", "integer"));
        properties.put("aDouble", Map.of("type", "double"));
        properties.put("aOptionalString", Map.of("type", "keyword"));
        properties.put("owner", Map.of("type", "keyword"));
        properties.put("coOwner", Map.of("type", "keyword"));
        properties.put("tagNames", Map.of("type", "keyword"));
        // Flat scalar lists. `double` because check() receives CEL doubles, and an integer mapping
        // would coerce a fractional element.
        properties.put("aNumberList", Map.of("type", "double"));
        properties.put("aBoolList", Map.of("type", "boolean"));
        // Keep malformed strings in _source but unindexed. CEL timestamp() errors on those rows,
        // so range predicates and their guarded negations must both deny them.
        properties.put("createdBy", Map.of("type", "date", "format", "strict_date_optional_time_nanos",
                "ignore_malformed", true));
        properties.put("updatedAt", Map.of("type", "date", "format", "strict_date_optional_time_nanos"));
        properties.put("createdAt", Map.of("type", "date", "format", "strict_date_optional_time_nanos"));
        properties.put("scope", Map.of("type", "keyword"));
        properties.put("obj", Map.of("properties", Map.of("inner", Map.of("type", "keyword"))));
        properties.put("tags", Map.of("type", "nested", "properties", tagProperties));
        properties.put("categories", Map.of("type", "nested", "properties", categoryProperties));
        properties.put("mainCategory", Map.of("properties", mainCategoryProperties));
        // The corpus's to-one relation, two levels deep, indexed as plain objects because
        // Elasticsearch has no join.
        Map<String, Object> relationLevel = Map.of(
                "aBool", Map.of("type", "boolean"),
                "aString", Map.of("type", "keyword"),
                "aNumber", Map.of("type", "integer"),
                "aOptionalString", Map.of("type", "keyword"));
        Map<String, Object> parentProperties = new LinkedHashMap<>(relationLevel);
        parentProperties.put("inner", Map.of("properties", relationLevel));
        properties.put("parent", Map.of("properties", parentProperties));

        es.createIndex(INDEX, Map.of("properties", properties));
    }

    private static void seedIndex() throws Exception {
        for (Seed seed : SEEDS) {
            Map<String, Object> document = new LinkedHashMap<>();
            document.put("id", seed.id());
            document.put("aBool", seed.aBool());
            document.put("aString", seed.aString());
            document.put("aNumber", seed.aNumber());
            if (derivedFor(seed).aDouble() != null) document.put("aDouble", derivedFor(seed).aDouble());
            if (seed.aOptionalString() != null) {
                document.put("aOptionalString", seed.aOptionalString());
                document.put("owner", seed.aOptionalString());
            } else {
                document.put("owner", null);
            }
            document.put("coOwner", derivedFor(seed).scope());
            document.put("tagNames", seed.tags().stream().map(Tag::name).toList());
            // Stored as written, null elements included; Elasticsearch indexes the non-null
            // values as an unordered set of terms, so positional reads cannot translate.
            document.put("aNumberList", seed.aNumberList());
            document.put("aBoolList", seed.aBoolList());
            document.put("createdBy", derivedFor(seed).createdBy());
            if (derivedFor(seed).createdAt() != null) document.put("createdAt", derivedFor(seed).createdAt());
            if (derivedFor(seed).updatedAt() != null) document.put("updatedAt", derivedFor(seed).updatedAt());
            if (derivedFor(seed).scope() != null) document.put("scope", derivedFor(seed).scope());
            document.put("obj", Map.of("inner", seed.aString()));
            document.put("tags", seed.tags().stream().map(tag -> {
                Map<String, Object> value = new LinkedHashMap<>();
                value.put("id", tag.id());
                value.put("name", tag.name());
                return value;
            }).toList());
            document.put("categories", categoriesFor(seed));
            if (!seed.subCategoryNames().isEmpty()) {
                document.put("mainCategory", Map.of(
                        "name", "business",
                        "subNames", seed.subCategoryNames(),
                        "subCategories", seed.subCategoryNames().stream()
                                .map(name -> Map.of("name", name)).toList()));
            }
            // The to-one chain. A seed with no parent gets no `parent` field, matching the
            // missing attribute check() receives.
            Seed parentSeed = parentSeedOf(seed);
            if (parentSeed != null) {
                Map<String, Object> parent = relationDocument(parentSeed);
                Seed innerSeed = parentSeedOf(parentSeed);
                if (innerSeed != null) parent.put("inner", relationDocument(innerSeed));
                document.put("parent", parent);
            }
            es.index(INDEX, seed.id(), document);
        }
    }

    private static List<Map<String, Object>> categoriesFor(Seed seed) {
        List<Map<String, Object>> categories = new ArrayList<>();
        for (String subName : seed.subCategoryNames()) {
            List<Map<String, Object>> labels = derivedFor(seed).labels().stream().map(name -> {
                Map<String, Object> label = new LinkedHashMap<>();
                label.put("name", name);
                return label;
            }).toList();
            categories.add(Map.of(
                    "name", "business",
                    "subCategories", List.of(Map.of("name", subName, "labels", labels))));
        }
        return categories;
    }

    // -- the real to-one relation (conformance/README.md, "The dataset") --------------------------
    //
    // `parentSeedId` names the seed whose scalars a row's `parent` carries, and that seed's own
    // `parentSeedId` fills `parent.inner`. An absent level is a missing field.

    /** The seed one hop out, or null when this level has no parent. A null argument returns null. */
    private static Seed parentSeedOf(Seed seed) {
        if (seed == null || seed.parentSeedId() == null) return null;
        return SEEDS.stream()
                .filter(candidate -> candidate.id().equals(seed.parentSeedId()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "seeds.json: \"" + seed.id() + "\" names parent \"" + seed.parentSeedId()
                                + "\", which is not a seed id"));
    }

    /** One level of the chain as an indexed object. A NULL column is an absent field. */
    private static Map<String, Object> relationDocument(Seed seed) {
        Map<String, Object> level = new LinkedHashMap<>();
        level.put("aBool", seed.aBool());
        level.put("aString", seed.aString());
        level.put("aNumber", seed.aNumber());
        if (seed.aOptionalString() != null) level.put("aOptionalString", seed.aOptionalString());
        return level;
    }

    private static DerivedEntry derivedFor(Seed seed) {
        DerivedEntry entry = DERIVED.get(seed.id());
        if (entry == null) {
            throw new IllegalStateException("derived-fields.json has no entry for seed " + seed.id());
        }
        return entry;
    }
}
