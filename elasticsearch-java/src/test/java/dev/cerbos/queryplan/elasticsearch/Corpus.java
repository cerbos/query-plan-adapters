/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Reads the shared {@code ../conformance/} corpus: the recorded goldens, the PDP tags they were
 * recorded against, and this adapter's ledger. Also holds the one mapping every case translates
 * through. See conformance/README.md, "The harness contract".
 *
 * <p>Every adapter keeps its own copy of this loader; do not extract a shared one (ADR 0007).
 */
final class Corpus {

    private Corpus() {}

    private static final ObjectMapper JSON = new ObjectMapper();

    static Path conformanceDir() {
        return Path.of(System.getProperty("user.dir"), "..", "conformance").normalize();
    }

    private static JsonNode read(Path path) {
        try {
            return JSON.readTree(path.toFile());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // -- conformance/pdp-versions.json and conformance/golden/ ----------------------------------

    /** The PDP tags the goldens were recorded against: current first, then previous. */
    static final List<String> PDP_TAGS = Stream.of("current", "previous")
            .map(which -> read(conformanceDir().resolve("pdp-versions.json")).get(which).get("tag").asText())
            .toList();

    static final String CURRENT_TAG = PDP_TAGS.get(0);

    /**
     * The planner folds {@code now() - duration("24h")} into a literal, which the goldens record
     * as this placeholder.
     */
    static final String NOW_PLACEHOLDER = "__NOW_MINUS_24H__";

    /**
     * The real instant that goes back in, at the nanosecond precision the planner emits. The
     * fractional digits are fixed, so a run never lands on a whole millisecond by chance.
     */
    static final String NOW_MINUS_24H = Instant.now().minus(24, ChronoUnit.HOURS)
            .truncatedTo(ChronoUnit.SECONDS).plusNanos(123_456_789).toString();

    /** One golden file: a recorded plan and the seed ids {@code check()} allowed. */
    record Golden(String id, String tier, PlanResourcesResponse plan, List<String> allowed,
                  JsonNode plannerDivergence) {

        /** A planner bug for this tag: the plan and {@code check()} disagree, so no adapter can pass. */
        boolean skipped(String tag) {
            return plannerDivergence != null && !plannerDivergence.isNull()
                    && (!plannerDivergence.has("pdp") || strings(plannerDivergence.get("pdp")).contains(tag));
        }
    }

    /** Every golden recorded against one PDP tag, sorted by case id. */
    static List<Golden> goldens(String tag) {
        Path directory = conformanceDir().resolve("golden").resolve(tag);
        try (Stream<Path> files = Files.walk(directory)) {
            return files.filter(path -> path.toString().endsWith(".json")).sorted()
                    .map(path -> golden(path, NOW_MINUS_24H)).toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The recorded plan for one case under the current PDP, with the placeholder filled. */
    static PlanResourcesResponse plan(String caseId) {
        return plan(caseId, NOW_MINUS_24H);
    }

    static PlanResourcesResponse plan(String caseId, String nowMinus24h) {
        return golden(conformanceDir().resolve("golden").resolve(CURRENT_TAG)
                .resolve(caseId + ".json"), nowMinus24h).plan();
    }

    private static Golden golden(Path path, String nowMinus24h) {
        JsonNode golden;
        try {
            golden = JSON.readTree(Files.readString(path).replace(NOW_PLACEHOLDER, nowMinus24h));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        // The plan is the PlanResources `filter`, in protobuf's JSON mapping.
        PlanResourcesFilter.Builder filter = PlanResourcesFilter.newBuilder();
        try {
            JsonFormat.parser().merge(golden.get("plan").toString(), filter);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException(path + " carries a plan protobuf cannot decode", e);
        }
        return new Golden(golden.get("id").asText(), golden.get("tier").asText(),
                PlanResourcesResponse.newBuilder().setFilter(filter).build(),
                strings(golden.get("allowed")), golden.get("plannerDivergence"));
    }

    private static List<String> strings(JsonNode array) {
        return JSON.convertValue(array, new TypeReference<List<String>>() {});
    }

    // -- conformance-ledger.json ----------------------------------------------------------------

    /** One ledger entry: {@code unsupported} or {@code divergent}, optionally limited to some tags. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record LedgerEntry(String status, String reason, String issue, List<String> pdp) {}

    static final Map<String, LedgerEntry> LEDGER = JSON.convertValue(
            read(Path.of(System.getProperty("user.dir"), "conformance-ledger.json")).get("cases"),
            new TypeReference<LinkedHashMap<String, LedgerEntry>>() {});

    /** The ledger entry for a case under one PDP tag, or null. */
    static LedgerEntry ledgerEntry(String caseId, String tag) {
        LedgerEntry entry = LEDGER.get(caseId);
        return entry != null && (entry.pdp() == null || entry.pdp().contains(tag)) ? entry : null;
    }

    // -- the corpus mapped onto the index --------------------------------------------------------

    /**
     * The corpus field map. Every path is mapped, including ones only refused cases read, so an
     * unmapped field throws {@link UnmappedAttributeException} rather than passing for a refusal.
     */
    static final Map<String, String> FIELD_MAP = Map.ofEntries(
            // The primary key (`request.resource.id`), mapped to the indexed `id` keyword field
            // rather than Elasticsearch's `_id` metadata field.
            Map.entry("request.resource.id", "id"),
            Map.entry("request.resource.attr.aBool", "aBool"),
            Map.entry("request.resource.attr.aString", "aString"),
            Map.entry("request.resource.attr.aNumber", "aNumber"),
            Map.entry("request.resource.attr.aDouble", "aDouble"),
            Map.entry("request.resource.attr.aOptionalString", "aOptionalString"),
            Map.entry("request.resource.attr.createdBy", "createdBy"),
            Map.entry("request.resource.attr.createdAt", "createdAt"),
            Map.entry("request.resource.attr.updatedAt", "updatedAt"),
            Map.entry("request.resource.attr.owner", "owner"),
            // `coOwner` is the explicit-null alias of `scope`, which is omitted when NULL, so the
            // corpus carries the same field under both conventions.
            Map.entry("request.resource.attr.coOwner", "coOwner"),
            Map.entry("request.resource.attr.scope", "scope"),
            Map.entry("request.resource.attr.obj.inner", "obj.inner"),
            Map.entry("request.resource.attr.tags", "tags"),
            Map.entry("request.resource.attr.tagNames", "tagNames"),
            // Flat number and boolean arrays, like tagNames.
            Map.entry("request.resource.attr.aNumberList", "aNumberList"),
            Map.entry("request.resource.attr.aBoolList", "aBoolList"),
            Map.entry("request.resource.attr.categories", "categories"),
            Map.entry("request.resource.attr.mainCategory.subCategories", "mainCategory.subCategories"),
            Map.entry("request.resource.attr.mainCategory.subNames", "mainCategory.subNames"),
            // The corpus's to-one relation, indexed as plain objects because
            // Elasticsearch has no join.
            Map.entry("request.resource.attr.parent.aBool", "parent.aBool"),
            Map.entry("request.resource.attr.parent.aString", "parent.aString"),
            Map.entry("request.resource.attr.parent.aNumber", "parent.aNumber"),
            Map.entry("request.resource.attr.parent.aOptionalString", "parent.aOptionalString"),
            Map.entry("request.resource.attr.parent.inner.aBool", "parent.inner.aBool"),
            Map.entry("request.resource.attr.parent.inner.aString", "parent.inner.aString"),
            Map.entry("request.resource.attr.parent.inner.aNumber", "parent.inner.aNumber"),
            Map.entry("request.resource.attr.parent.inner.aOptionalString", "parent.inner.aOptionalString"));

    /**
     * Attributes the corpus sends to {@code check()} as explicit nulls ({@code resources.json}
     * shows {@code "owner": null}). Every other attribute follows the omitted convention: a NULL
     * column is a missing attribute, which is this adapter's default. Elasticsearch does not index
     * a JSON null, so declaring these makes the adapter refuse instead of answering.
     */
    static final Set<String> EXPLICIT_NULL_ATTRIBUTES = Set.of(
            "request.resource.attr.owner", "request.resource.attr.coOwner");

    /** The field paths the corpus index maps as Elasticsearch {@code nested} documents. */
    static final Set<String> NESTED_PATHS = Set.of(
            "tags", "mainCategory.subCategories",
            "categories", "categories.subCategories", "categories.subCategories.labels");

    /**
     * The field paths the corpus index maps as flat arrays of scalars. The adapter cannot tell
     * {@code size(aString)} from {@code size(tagNames)} by itself, so it refuses {@code size()} on
     * a field declared neither here nor in {@link #NESTED_PATHS}.
     */
    static final Set<String> COLLECTION_FIELDS = Set.of("tagNames", "aNumberList", "aBoolList");

    /**
     * The CEL scalar type of every scalar field in the corpus index, keyed by Elasticsearch field
     * name. The adapter refuses a comparison against an undeclared field, because Elasticsearch
     * coerces a query term to the field's mapped type where CEL's cross-type equality is false.
     */
    static final Map<String, ElasticsearchQueryPlanAdapter.ScalarType> SCALAR_TYPES = scalarTypes();

    private static Map<String, ElasticsearchQueryPlanAdapter.ScalarType> scalarTypes() {
        ElasticsearchQueryPlanAdapter.ScalarType string = ElasticsearchQueryPlanAdapter.ScalarType.STRING;
        ElasticsearchQueryPlanAdapter.ScalarType number = ElasticsearchQueryPlanAdapter.ScalarType.NUMBER;
        ElasticsearchQueryPlanAdapter.ScalarType bool = ElasticsearchQueryPlanAdapter.ScalarType.BOOLEAN;
        ElasticsearchQueryPlanAdapter.ScalarType timestamp = ElasticsearchQueryPlanAdapter.ScalarType.TIMESTAMP;
        Map<String, ElasticsearchQueryPlanAdapter.ScalarType> types = new LinkedHashMap<>();
        types.put("id", string);
        types.put("aString", string);
        types.put("aOptionalString", string);
        types.put("aNumber", number);
        types.put("aDouble", number);
        types.put("aBool", bool);
        types.put("createdAt", timestamp);
        types.put("updatedAt", timestamp);
        types.put("createdBy", string);
        types.put("scope", string);
        types.put("owner", string);
        types.put("coOwner", string);
        types.put("obj.inner", string);
        types.put("tagNames", string);
        types.put("aNumberList", number);
        types.put("aBoolList", bool);
        types.put("mainCategory.subNames", string);
        types.put("mainCategory.subCategories.name", string);
        types.put("tags.id", string);
        types.put("tags.name", string);
        types.put("categories.name", string);
        types.put("categories.subCategories.name", string);
        types.put("categories.subCategories.labels.name", string);
        for (String level : List.of("parent.", "parent.inner.")) {
            types.put(level + "aBool", bool);
            types.put(level + "aString", string);
            types.put(level + "aNumber", number);
            types.put(level + "aOptionalString", string);
        }
        return Map.copyOf(types);
    }

    /** The one mapping every case translates through. */
    static final ElasticsearchQueryPlanAdapter.Options OPTIONS =
            ElasticsearchQueryPlanAdapter.Options.of(FIELD_MAP)
                    .withNestedPaths(NESTED_PATHS)
                    .withScalarTypes(SCALAR_TYPES)
                    .withCollectionFields(COLLECTION_FIELDS)
                    .withExplicitNullAttributes(EXPLICIT_NULL_ATTRIBUTES);

    /** Translates one case's current-PDP plan through {@link #OPTIONS}. */
    static ElasticsearchQueryPlanAdapter.Result translate(String caseId) {
        return ElasticsearchQueryPlanAdapter.toElasticsearchQuery(plan(caseId), OPTIONS);
    }
}
